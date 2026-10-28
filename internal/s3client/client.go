// Copyright 2023 Sneller, Inc.
// Copyright 2025 Roman Atachiants
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package s3client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type Config struct {
	Bucket string

	Endpoint string

	Region string

	AccessKey    string
	SecretKey    string
	SessionToken string

	Insecure bool

	PathStyle bool

	Prefix string

	HTTPClient *http.Client
}

type Bucket struct {
	client *client
}

type client struct {
	signer       *signer
	bucket       string
	prefix       string
	scheme       string
	host         string
	pathStyle    bool
	http         *http.Client
	maxSinglePut int64
	minPartSize  int64
}

func New(cfg Config) (*Bucket, error) {
	c, err := newClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Bucket{client: c}, nil
}

func newClient(cfg Config) (*client, error) {
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	if cfg.Bucket == "" {
		return nil, errors.New("s3: bucket is required")
	}
	if !validBucketName(cfg.Bucket) {
		return nil, fmt.Errorf("s3: invalid bucket %q", cfg.Bucket)
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, errors.New("s3: access key and secret key are required")
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = "us-east-1"
	}

	scheme, host, pathStyle, err := resolveEndpoint(cfg, region)
	if err != nil {
		return nil, err
	}
	if !pathStyle && endpointIsIP(host) {
		return nil, fmt.Errorf("s3: virtual-hosted addressing cannot use IP endpoint %q; enable PathStyle", host)
	}
	if !pathStyle {
		host = cfg.Bucket + "." + host
	}

	hc := cfg.HTTPClient
	if hc == nil {
		hc = newHTTPClient(cfg.Insecure)
	}
	return &client{
		signer:       newSigner(region, cfg.AccessKey, cfg.SecretKey, cfg.SessionToken),
		bucket:       cfg.Bucket,
		prefix:       normalizePrefix(cfg.Prefix),
		scheme:       scheme,
		host:         host,
		pathStyle:    pathStyle,
		http:         hc,
		maxSinglePut: defaultMaxSinglePut,
		minPartSize:  defaultMinPartSize,
	}, nil
}

func resolveEndpoint(cfg Config, region string) (scheme, host string, pathStyle bool, err error) {
	if cfg.Endpoint != "" {
		scheme, host, err = parseEndpoint(cfg.Endpoint, cfg.Insecure)
		if err != nil {
			return "", "", false, err
		}
		return scheme, host, cfg.PathStyle, nil
	}
	host = "s3." + region + ".amazonaws.com"
	pathStyle = cfg.PathStyle || strings.Contains(cfg.Bucket, ".")
	if !pathStyle {
		host = cfg.Bucket + "." + host
	}
	return "https", host, pathStyle, nil
}

const (
	defaultMaxSinglePut = 5 << 30

	defaultMinPartSize = 8 << 20
)

func newHTTPClient(insecure bool) *http.Client {
	return &http.Client{Transport: NewTransport(TransportOptions{
		Insecure:              insecure,
		MaxIdleConnsPerHost:   64,
		ResponseHeaderTimeout: 30 * time.Second,
	})}
}

func (b *Bucket) Close() error {
	if b != nil && b.client != nil && b.client.http != nil {
		b.client.http.CloseIdleConnections()
	}
	return nil
}

func (c *client) target(object string) (decoded, escaped string) {
	switch {
	case c.pathStyle:
		decoded = "/" + c.bucket
		if object != "" {
			decoded += "/" + object
		}
	case object != "":
		decoded = "/" + object
	default:
		decoded = "/"
	}
	return decoded, escapePath(decoded)
}

func (c *client) objectKey(key string) (string, error) {
	key = strings.TrimLeft(key, "/")
	if key == "" || key == "." {
		return "", errors.New("s3: object key is required")
	}
	if c.prefix == "" {
		return key, nil
	}
	return c.prefix + "/" + key, nil
}

func (c *client) logicalKey(physical string) string {
	if c.prefix == "" {
		return physical
	}
	return strings.TrimPrefix(physical, c.prefix+"/")
}

func (c *client) listPrefix(prefix string) string {
	prefix = strings.TrimLeft(prefix, "/")
	if c.prefix == "" {
		return prefix
	}
	if prefix == "" {
		return c.prefix + "/"
	}
	return c.prefix + "/" + prefix
}

func normalizePrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), "/")
}

func (c *client) newRequest(ctx context.Context, method, object string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Request, error) {
	if c == nil {
		return nil, errors.New("s3: nil client")
	}
	if ctx == nil {
		return nil, errors.New("s3 request: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	decoded, escaped := c.target(object)
	rawQuery := canonicalQuery(query)
	u := &url.URL{
		Scheme:   c.scheme,
		Host:     c.host,
		Path:     decoded,
		RawPath:  escaped,
		RawQuery: rawQuery,
	}
	var marker []byte
	if body != nil {
		marker = []byte{}
	}
	date, payloadHash, auth := c.signer.signRequest(nil, []byte(method), []byte(escaped), []byte(rawQuery), []byte(c.host), marker, extra...)

	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	for _, h := range extra {
		req.Header.Set(h[0], h[1])
	}
	req.Header.Set("X-Amz-Date", string(date))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if c.signer.token != "" {
		req.Header.Set("X-Amz-Security-Token", c.signer.token)
	}
	req.Header.Set("Authorization", string(auth))
	if body != nil {
		if size < 0 {
			return nil, fmt.Errorf("s3: %s %s: unknown body size", method, object)
		}
		req.ContentLength = size
	}
	return req, nil
}

func (c *client) do(ctx context.Context, method, object string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error) {
	req, err := c.newRequest(ctx, method, object, query, body, size, extra...)
	if err != nil {
		return nil, err
	}
	return c.send(req)
}

func (c *client) send(req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return resp, nil
}

func (c *client) doBytes(ctx context.Context, method, object string, query url.Values, body []byte, extra ...[2]string) (*http.Response, error) {
	var r io.Reader
	size := int64(-1)
	if body != nil {
		r = bytes.NewReader(body)
		size = int64(len(body))
	}
	return c.do(ctx, method, object, query, r, size, extra...)
}

func canonicalQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for key := range q {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, key := range keys {
		values := append([]string(nil), q[key]...)
		sort.Strings(values)
		for _, value := range values {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(awsEscape(key))
			b.WriteByte('=')
			b.WriteString(awsEscape(value))
		}
	}
	return b.String()
}

func awsEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func pickScheme(raw string, insecure bool) string {
	if strings.Contains(raw, "://") {
		if i := strings.Index(raw, "://"); i > 0 {
			return strings.ToLower(raw[:i])
		}
	}
	if insecure {
		return "http"
	}
	return "https"
}

func parseEndpoint(raw string, insecure bool) (scheme, host string, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", "", errors.New("s3: endpoint is required")
	}
	target := raw
	if !strings.Contains(target, "://") {
		target = "//" + target
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", "", fmt.Errorf("s3: invalid endpoint %q: %w", raw, err)
	}
	if u.Scheme != "" && u.Scheme != "http" && u.Scheme != "https" {
		return "", "", fmt.Errorf("s3: endpoint %q must use http or https", raw)
	}
	if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", "", fmt.Errorf("s3: invalid endpoint %q", raw)
	}
	if u.Path != "" && u.Path != "/" {
		return "", "", fmt.Errorf("s3: endpoint %q must not include a path", raw)
	}
	scheme = u.Scheme
	if scheme == "" {
		scheme = pickScheme(raw, insecure)
	}
	return scheme, u.Host, nil
}

func endpointIsIP(host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return net.ParseIP(host) != nil
}

func validBucketName(bucket string) bool {
	if bucket == "" || len(bucket) > 255 {
		return false
	}
	for i := 0; i < len(bucket); i++ {
		c := bucket[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_':
		default:
			return false
		}
	}
	return true
}

func escapePath(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~', c == '/':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&15])
		}
	}
	return b.String()
}

var (
	ErrNotFound = errors.New("s3: object not found")

	ErrPrecondition = errors.New("s3: precondition failed")

	ErrConflict = errors.New("s3: conditional request conflict")

	ErrUnsupported = errors.New("s3: operation unsupported by origin")

	ErrInvalidRange = errors.New("s3: invalid range")
)

type Error struct {
	Op         string
	Key        string
	StatusCode int
	Code       string
	Message    string
	Err        error
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("s3: ")
	b.WriteString(e.Op)
	if e.Key != "" {
		b.WriteString(" ")
		b.WriteString(e.Key)
	}
	if e.StatusCode != 0 {
		fmt.Fprintf(&b, ": HTTP %d", e.StatusCode)
	}
	if e.Code != "" {
		b.WriteString(" ")
		b.WriteString(e.Code)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Err }

func NotFound(err error) bool {
	return errors.Is(err, ErrNotFound) || errors.Is(err, fs.ErrNotExist)
}

func Precondition(err error) bool { return errors.Is(err, ErrPrecondition) }

func Conflict(err error) bool { return errors.Is(err, ErrConflict) }

func Unsupported(err error) bool { return errors.Is(err, ErrUnsupported) }

func InvalidRange(err error) bool { return errors.Is(err, ErrInvalidRange) }

func statusError(op, key string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return bodyError(op, key, resp.StatusCode, body)
}

func bodyError(op, key string, status int, body []byte) error {
	var parsed struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	_ = xml.Unmarshal(body, &parsed)
	msg := strings.TrimSpace(parsed.Message)
	if msg == "" {
		msg = strings.TrimSpace(string(body))
		if len(msg) > 256 {
			msg = msg[:256] + "..."
		}
	}
	return &Error{
		Op: op, Key: key, StatusCode: status, Code: parsed.Code,
		Message: msg, Err: classify(status, parsed.Code),
	}
}

func classify(status int, code string) error {
	switch code {
	case "NoSuchKey", "NoSuchBucket", "NoSuchUpload", "NotFound":
		return ErrNotFound
	case "PreconditionFailed":
		return ErrPrecondition
	case "ConditionalRequestConflict":
		return ErrConflict
	case "InvalidRange":
		return ErrInvalidRange
	case "NotImplemented", "UnsupportedOperation":
		return ErrUnsupported
	}
	switch status {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusPreconditionFailed:
		return ErrPrecondition
	case http.StatusConflict:
		return ErrConflict
	case http.StatusRequestedRangeNotSatisfiable:
		return ErrInvalidRange
	case http.StatusNotImplemented:
		return ErrUnsupported
	default:
		return nil
	}
}

type TransportOptions struct {
	Insecure              bool
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
	IdleConnTimeout       time.Duration
	ResponseHeaderTimeout time.Duration
}

func NewTransport(opts TransportOptions) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	if opts.MaxIdleConns > 0 {
		tr.MaxIdleConns = opts.MaxIdleConns
	}
	if opts.MaxIdleConnsPerHost > 0 {
		tr.MaxIdleConnsPerHost = opts.MaxIdleConnsPerHost
	}
	if opts.IdleConnTimeout > 0 {
		tr.IdleConnTimeout = opts.IdleConnTimeout
	}
	if opts.ResponseHeaderTimeout > 0 {
		tr.ResponseHeaderTimeout = opts.ResponseHeaderTimeout
	}
	if opts.Insecure {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{}
		} else {
			tr.TLSClientConfig = tr.TLSClientConfig.Clone()
		}
		tr.TLSClientConfig.InsecureSkipVerify = true
	}
	return tr
}
