package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Auth verifies AWS Signature Version 4 header authentication.
type Auth struct {
	AccessKey   string
	SecretKey   string
	Sessions    *SessionIssuer
	VHostDomain string
	Now         func() time.Time
}

// AuthInfo describes how a request was authenticated.
type AuthInfo struct {
	Session bool
	Mode    string
	Bucket  string
	Service string
}

const (
	emptySHA256     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	maxSkew         = 15 * time.Minute
)

// Verify checks the request signature and, for non-unsigned payloads, wraps
// the body so its SHA-256 is checked as it is consumed. It does not read the
// body. S3 Express session tokens select the temporary session secret.
func (a *Auth) Verify(r *http.Request) (*AuthInfo, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		if r.URL.Query().Get("X-Amz-Algorithm") != "" {
			return a.verifyPresigned(r)
		}
		return nil, errAccessDenied("missing Authorization header")
	}
	if !strings.HasPrefix(h, "AWS4-HMAC-SHA256 ") {
		return nil, errAccessDenied("unsupported authorization algorithm")
	}
	fields := map[string]string{}
	for _, part := range strings.Split(strings.TrimPrefix(h, "AWS4-HMAC-SHA256 "), ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if ok {
			fields[k] = v
		}
	}
	cred := fields["Credential"]
	signedHeaders := fields["SignedHeaders"]
	signature := fields["Signature"]
	if cred == "" || signedHeaders == "" || signature == "" {
		return nil, errAuthorizationHeaderMalformed()
	}
	scopeParts := strings.Split(cred, "/")
	if len(scopeParts) != 5 || scopeParts[4] != "aws4_request" ||
		(scopeParts[3] != "s3" && scopeParts[3] != "s3express") {
		return nil, errAuthorizationHeaderMalformed()
	}
	accessKey, date, region, service := scopeParts[0], scopeParts[1], scopeParts[2], scopeParts[3]
	secret := a.SecretKey
	info := &AuthInfo{Service: service}
	if token := r.Header.Get("x-amz-s3session-token"); token != "" {
		if a.Sessions == nil {
			return nil, errAccessDenied("session authentication is not configured")
		}
		bucket, _, _ := splitPath(r, a.VHostDomain)
		sess, err := a.Sessions.Validate(token, bucket)
		if err != nil {
			return nil, errAccessDenied(err.Error())
		}
		if subtle.ConstantTimeCompare([]byte(accessKey), []byte(sess.AccessKey)) != 1 {
			return nil, &APIError{Code: "InvalidAccessKeyId", Message: "The AWS access key ID you provided does not exist.", Status: 403}
		}
		if sess.Mode == "ReadOnly" && r.Method != http.MethodGet && r.Method != http.MethodHead {
			return nil, errAccessDenied("read-only session cannot modify the bucket")
		}
		secret = sess.SecretKey
		info = &AuthInfo{Session: true, Mode: sess.Mode, Bucket: sess.Bucket}
	} else if subtle.ConstantTimeCompare([]byte(accessKey), []byte(a.AccessKey)) != 1 {
		return nil, &APIError{Code: "InvalidAccessKeyId", Message: "The AWS access key ID you provided does not exist.", Status: 403}
	}
	amzDate := r.Header.Get("x-amz-date")
	if amzDate == "" {
		amzDate = date + "T000000Z"
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return nil, &APIError{Code: "AccessDenied", Message: "invalid x-amz-date", Status: 403}
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	if d := now().Sub(t); d > maxSkew || d < -maxSkew {
		return nil, &APIError{Code: "RequestTimeTooSkewed", Message: "The difference between the request time and the server's time is too large.", Status: 403}
	}
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete {
			// Older clients omit the header on bodyless requests; S3 treats the
			// canonical payload as UNSIGNED-PAYLOAD. Try the empty hash too.
			if a.check(r, secret, signedHeaders, amzDate, region, service, emptySHA256, signature) != nil &&
				a.check(r, secret, signedHeaders, amzDate, region, service, unsignedPayload, signature) != nil {
				return nil, &APIError{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", Status: 403}
			}
			return info, nil
		}
		return nil, &APIError{Code: "InvalidRequest", Message: "missing x-amz-content-sha256", Status: 400}
	}
	if err := a.check(r, secret, signedHeaders, amzDate, region, service, payloadHash, signature); err != nil {
		return nil, err
	}
	switch {
	case payloadHash == unsignedPayload:
		return info, nil
	case strings.HasPrefix(payloadHash, "STREAMING-"):
		return info, a.wrapStreaming(r, secret, amzDate, region, service, signature)
	case len(payloadHash) == 64:
		r.Body = &payloadHashReader{r: r.Body, h: sha256.New(), want: payloadHash}
		return info, nil
	default:
		return nil, &APIError{Code: "InvalidRequest", Message: "unsupported x-amz-content-sha256", Status: 400}
	}
}

func (a *Auth) check(r *http.Request, secret, signedHeaders, amzDate, region, service, payloadHash, signature string) error {
	canonical, err := canonicalRequest(r, signedHeaders, payloadHash)
	if err != nil {
		return err
	}
	scope := amzDate[:8] + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonical))
	key := signingKey(secret, amzDate[:8], region, service)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(stringToSign))
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(signature)) != 1 {
		return &APIError{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", Status: 403}
	}
	return nil
}

func canonicalRequest(r *http.Request, signedHeaders, payloadHash string) (string, error) {
	return canonicalRequestQuery(r, signedHeaders, payloadHash, r.URL.RawQuery)
}

func canonicalRequestQuery(r *http.Request, signedHeaders, payloadHash, rawQuery string) (string, error) {
	names := strings.Split(signedHeaders, ";")
	for i := range names {
		names[i] = strings.ToLower(strings.TrimSpace(names[i]))
	}
	// The signer sorts the signed headers; reject a malformed order rather
	// than silently canonicalising a different request.
	if !sort.StringsAreSorted(names) {
		return "", &APIError{Code: "AuthorizationHeaderMalformed", Message: "SignedHeaders must be sorted", Status: 400}
	}
	var headers strings.Builder
	for _, name := range names {
		values := headerValues(r, name)
		if len(values) == 0 {
			return "", &APIError{Code: "SignatureDoesNotMatch", Message: "signed header missing: " + name, Status: 403}
		}
		for i, v := range values {
			values[i] = strings.Join(strings.Fields(v), " ")
		}
		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(strings.Join(values, ","))
		headers.WriteByte('\n')
	}
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	return strings.Join([]string{
		r.Method,
		uri,
		canonicalQuery(rawQuery),
		headers.String(),
		strings.Join(names, ";"),
		payloadHash,
	}, "\n"), nil
}

func headerValues(r *http.Request, name string) []string {
	switch name {
	case "host":
		if r.Host == "" {
			return nil
		}
		return []string{r.Host}
	case "content-length":
		if v := r.Header.Get("Content-Length"); v != "" {
			return []string{v}
		}
		if r.ContentLength >= 0 {
			return []string{strconv.FormatInt(r.ContentLength, 10)}
		}
		return nil
	case "content-type":
		if v := r.Header.Get("Content-Type"); v != "" {
			return []string{v}
		}
		return nil
	default:
		return r.Header.Values(name)
	}
}

func canonicalQuery(raw string) string { return canonicalQueryExcept(raw, "") }

func canonicalQueryExcept(raw, exclude string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		dk, err := url.QueryUnescape(k)
		if err != nil {
			dk = k
		}
		if exclude != "" && dk == exclude {
			continue
		}
		dv, err := url.QueryUnescape(v)
		if err != nil {
			dv = v
		}
		pairs = append(pairs, pair{awsEncode(dk, true), awsEncode(dv, true)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func awsEncode(s string, encodeSlash bool) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hexChars[c>>4])
			b.WriteByte(hexChars[c&15])
		}
	}
	return b.String()
}

// verifyPresigned validates an S3 presigned-URL query signature. Payloads are
// UNSIGNED-PAYLOAD; S3 presigned URLs do not sign the body.
func (a *Auth) verifyPresigned(r *http.Request) (*AuthInfo, error) {
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
		return nil, errAccessDenied("unsupported presigned algorithm")
	}
	scopeParts := strings.Split(q.Get("X-Amz-Credential"), "/")
	if len(scopeParts) != 5 || scopeParts[4] != "aws4_request" ||
		(scopeParts[3] != "s3" && scopeParts[3] != "s3express") {
		return nil, errAuthorizationHeaderMalformed()
	}
	accessKey, date, region, service := scopeParts[0], scopeParts[1], scopeParts[2], scopeParts[3]
	if subtle.ConstantTimeCompare([]byte(accessKey), []byte(a.AccessKey)) != 1 {
		return nil, &APIError{Code: "InvalidAccessKeyId", Message: "The AWS access key ID you provided does not exist.", Status: 403}
	}
	amzDate := q.Get("X-Amz-Date")
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return nil, errAccessDenied("invalid X-Amz-Date")
	}
	expires, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || expires < 1 || expires > 604800 {
		return nil, errAccessDenied("invalid X-Amz-Expires")
	}
	now := time.Now
	if a.Now != nil {
		now = a.Now
	}
	if now().After(t.Add(time.Duration(expires) * time.Second)) {
		return nil, &APIError{Code: "AccessDenied", Message: "Request has expired.", Status: 403}
	}
	if t.After(now().Add(maxSkew)) {
		return nil, &APIError{Code: "RequestTimeTooSkewed", Message: "The request time is too far in the future.", Status: 403}
	}
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	signature := q.Get("X-Amz-Signature")
	if signedHeaders == "" || signature == "" {
		return nil, errAuthorizationHeaderMalformed()
	}
	const payload = "UNSIGNED-PAYLOAD"
	canonical, err := canonicalRequestQuery(r, signedHeaders, payload, canonicalQueryExcept(r.URL.RawQuery, "X-Amz-Signature"))
	if err != nil {
		return nil, err
	}
	scope := date + "/" + region + "/" + service + "/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonical))
	mac := hmac.New(sha256.New, signingKey(a.SecretKey, date, region, service))
	mac.Write([]byte(toSign))
	if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(signature)) != 1 {
		return nil, &APIError{Code: "SignatureDoesNotMatch", Message: "The request signature we calculated does not match the signature you provided.", Status: 403}
	}
	return &AuthInfo{Service: service}, nil
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// payloadHashReader verifies x-amz-content-sha256 without buffering the body.
type payloadHashReader struct {
	r    io.ReadCloser
	h    hash.Hash
	want string
	done bool
}

func (p *payloadHashReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.h.Write(b[:n])
	}
	if err == io.EOF && !p.done {
		p.done = true
		if !strings.EqualFold(hex.EncodeToString(p.h.Sum(nil)), p.want) {
			return n, errPayloadHashMismatch
		}
	}
	return n, err
}

func (p *payloadHashReader) Close() error { return p.r.Close() }

var errPayloadHashMismatch = fmt.Errorf("x-amz-content-sha256 does not match the request body")
