package s3

import (
	"bufio"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func parseMetadata(h http.Header) ([]string, error) {
	type kv struct{ k, v string }
	var pairs []kv
	total := 0
	for name, values := range h {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "x-amz-meta-") {
			continue
		}
		key := strings.TrimPrefix(lower, "x-amz-meta-")
		if key == "" || len(key) > 128 || !validMetaName(key) {
			return nil, errInvalidRequest("invalid user metadata name")
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\x00\r\n") {
				return nil, errInvalidRequest("invalid user metadata value")
			}
			total += len(key) + len(value)
			if total > 2048 || len(pairs) >= 64 {
				return nil, errInvalidRequest("user metadata exceeds the 2 KiB limit")
			}
			pairs = append(pairs, kv{key, value})
		}
	}
	slices.SortFunc(pairs, func(a, b kv) int {
		if a.k != b.k {
			return strings.Compare(a.k, b.k)
		}
		return strings.Compare(a.v, b.v)
	})
	out := make([]string, 0, len(pairs)*2)
	for _, p := range pairs {
		out = append(out, p.k, p.v)
	}
	return out, nil
}

func validMetaName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func metadataHeader(pairs []string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set("x-amz-meta-"+pairs[i], pairs[i+1])
	}
	return h
}

func contentType(v string) (string, error) {
	if v == "" {
		return "application/octet-stream", nil
	}
	if len(v) > 1024 || strings.ContainsAny(v, "\x00\r\n") {
		return "", errInvalidRequest("invalid content type")
	}
	return v, nil
}

func stripETag(v string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(v), "W/"), `"`)
}

func validETag(v string) bool {
	digest, count, hasCount := strings.Cut(stripETag(v), "-")
	if _, err := hex.DecodeString(digest); err != nil || len(digest) != 32 {
		return false
	}
	if !hasCount {
		return true
	}
	n, err := strconv.Atoi(count)
	return err == nil && n >= 1 && n <= 10000
}

func validateWriteHeaders(r *http.Request) error {
	if err := validateSSE(r); err != nil {
		return err
	}
	switch acl := r.Header.Get("x-amz-acl"); acl {
	case "", "private":
	default:
		return errNotImplemented()
	}
	for _, h := range []string{
		"x-amz-grant-read",
		"x-amz-grant-write",
		"x-amz-grant-read-acp",
		"x-amz-grant-write-acp",
		"x-amz-grant-full-control",
		"x-amz-object-lock-mode",
		"x-amz-object-lock-retain-until-date",
		"x-amz-object-lock-legal-hold",
		"x-amz-tagging",
	} {
		if r.Header.Get(h) != "" {
			return errNotImplemented()
		}
	}
	switch class := r.Header.Get("x-amz-storage-class"); class {
	case "", "STANDARD":
	default:
		return errNotImplemented()
	}
	return nil
}

func validateSSE(r *http.Request) error {
	if r.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key-md5") != "" {
		return errNotImplemented()
	}
	for _, h := range []string{
		"x-amz-server-side-encryption-aws-kms-key-id",
		"x-amz-server-side-encryption-context",
		"x-amz-server-side-encryption-bucket-key-enabled",
	} {
		if r.Header.Get(h) != "" {
			return errNotImplemented()
		}
	}
	switch enc := r.Header.Get("x-amz-server-side-encryption"); enc {
	case "", "AES256":
		return nil
	case "aws:kms", "aws:kms:dsse":
		return errNotImplemented()
	default:
		return errInvalidRequest("unsupported server-side encryption")
	}
}

func parseRange(h string) (store.ByteRange, error) {
	if h == "" {
		return store.WholeObjectRange(), nil
	}
	if !strings.HasPrefix(h, "bytes=") {
		return store.ByteRange{}, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return store.ByteRange{}, fmt.Errorf("multiple ranges are not supported")
	}
	start, end, ok := strings.Cut(spec, "-")
	if !ok {
		return store.ByteRange{}, fmt.Errorf("malformed range")
	}
	start, end = strings.TrimSpace(start), strings.TrimSpace(end)
	if start == "" {
		n, err := strconv.ParseInt(end, 10, 64)
		if err != nil || n < 0 {
			return store.ByteRange{}, fmt.Errorf("malformed suffix range")
		}
		if n == 0 {
			return store.ByteRange{}, fmt.Errorf("empty suffix range")
		}
		return store.Range(0, -1, n), nil
	}
	first, err := strconv.ParseInt(start, 10, 64)
	if err != nil || first < 0 {
		return store.ByteRange{}, fmt.Errorf("malformed range start")
	}
	if end == "" {
		return store.Range(first, -1, -1), nil
	}
	last, err := strconv.ParseInt(end, 10, 64)
	if err != nil || last < first {
		return store.ByteRange{}, fmt.Errorf("malformed range end")
	}
	return store.Range(first, last, -1), nil
}

func prefixEnd(prefix string) string {
	r := []rune(prefix)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] < unicode.MaxRune {
			r[i]++
			return string(r[:i+1])
		}
	}
	return string(unicode.MaxRune)
}

func partsCount(m *store.Meta) int {
	if m.Parts != nil {
		return len(m.Parts)
	}
	return 1
}

func partBounds(m *store.Meta, n int) (int64, int64, bool) {
	if n < 1 || n > partsCount(m) {
		return 0, 0, false
	}
	if m.Parts == nil {
		return 0, m.Size - 1, true
	}
	var start int64
	if n > 1 {
		start = m.PartEnds[n-2]
	}
	return start, m.PartEnds[n-1] - 1, true
}

func parsePartNumber(q url.Values) (int, bool, error) {
	if !q.Has("partNumber") {
		return 0, false, nil
	}
	n, err := strconv.Atoi(q.Get("partNumber"))
	if err != nil || n < 1 || n > 10000 {
		return 0, true, errInvalidRequest("invalid partNumber")
	}
	return n, true, nil
}

func splitPath(r *http.Request) (string, string, error) {
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil {
		return "", "", err
	}
	decoded = strings.TrimPrefix(decoded, "/")
	if decoded == "" {
		return "", "", nil
	}
	bucket, key, _ := strings.Cut(decoded, "/")
	return bucket, key, nil
}

func awsEncode(s string, encodeSlash bool) string {
	const hexChars = "0123456789ABCDEF"
	var b strings.Builder
	b.Grow(len(s))
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

func validateBucketName(name string) error {
	if !ns.ValidName(name) {
		return errInvalidBucketName()
	}
	return nil
}

func (s *Server) apiError(err error) error {
	if ae, ok := errors.AsType[*APIError](err); ok {
		return ae
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" && pgErr.ConstraintName == "objects_bucket_fkey" {
		return errNoSuchBucket()
	}
	switch {
	case errors.Is(err, objcache.ErrSpoolBusy):
		return apiErr("SlowDown", "The cache scratch byte budget is busy; retry shortly.", http.StatusServiceUnavailable)
	case errors.Is(err, objcache.ErrReadOnly):
		return errAccessDenied("gateway is read-only")
	case errors.Is(err, objcache.ErrOriginConflict):
		return apiErr("OperationAborted", "A conflicting conditional operation is in progress. Retry the request.", http.StatusConflict)
	case errors.Is(err, objcache.ErrOriginKeyUnsupported):
		return errInvalidRequest("the object key cannot be stored by the configured origin")
	case errors.Is(err, objcache.ErrOriginUnsupported), errors.Is(err, objcache.ErrOriginListingUnsupported):
		return apiErr("NotImplemented", "The configured origin does not support this operation.", http.StatusNotImplemented)
	case errors.Is(err, store.ErrPrecondition):
		return errPreconditionFailed()
	case errors.Is(err, store.ErrNotFound):
		return errNoSuchKey()
	case errors.Is(err, store.ErrNoBucket):
		return errNoSuchBucket()
	case errors.Is(err, store.ErrInvalidBucket), errors.Is(err, ns.ErrInvalidName):
		return errInvalidBucketName()
	case errors.Is(err, store.ErrNoUpload):
		return errNoSuchUpload()
	case errors.Is(err, store.ErrInvalidPart):
		return errInvalidPart()
	case errors.Is(err, store.ErrInvalidRange):
		return errInvalidRange()
	case errors.Is(err, store.ErrMissingChecksum):
		return errInvalidRequest("part lacks the upload's checksum")
	case errors.Is(err, store.ErrTooManyNamespaces):
		return apiErr("TooManyBuckets", "The namespace limit has been reached.", http.StatusBadRequest)
	case errors.Is(err, store.ErrBucketNotEmpty):
		return apiErr("BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict)
	case errors.Is(err, store.ErrQuotaExceeded):
		return apiErr("InsufficientStorage", "The database or namespace is over its storage budget.", http.StatusInsufficientStorage)
	default:
		return errInternal(err, s.log)
	}
}

func bodyError(err error) error {
	if ae, ok := errors.AsType[*APIError](err); ok {
		return ae
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return apiErr("IncompleteBody", "The request body terminated unexpectedly.", http.StatusBadRequest)
	}
	return apiErr("InvalidRequest", "Could not read the request body.", http.StatusBadRequest)
}

func putCondition(r *http.Request) (store.Precondition, error) {
	ifMatch := r.Header.Get("If-Match")
	ifNone := r.Header.Get("If-None-Match")
	switch {
	case ifMatch == "" && ifNone == "":
		return store.Precondition{}, nil
	case ifMatch == "" && strings.TrimSpace(ifNone) == "*":
		return store.Precondition{Mode: store.IfAbsent}, nil
	case ifMatch != "" && ifNone == "" && validETag(ifMatch):
		return store.Precondition{Mode: store.IfMatch, ETag: stripETag(ifMatch)}, nil
	default:
		return store.Precondition{}, errInvalidRequest("unsupported conditional PUT")
	}
}

func headerReadCondition(r *http.Request) store.ReadCondition {
	return store.ReadCondition{
		IfMatch:     r.Header.Get("If-Match"),
		IfNoneMatch: r.Header.Get("If-None-Match"),
	}
}

func copySourceCondition(r *http.Request) store.ReadCondition {
	return store.ReadCondition{
		IfMatch:     r.Header.Get("x-amz-copy-source-if-match"),
		IfNoneMatch: r.Header.Get("x-amz-copy-source-if-none-match"),
	}
}

func readCondition(r *http.Request, etag string) error {
	err := headerReadCondition(r).Check(true, etag)
	if errors.Is(err, store.ErrPrecondition) {
		return errPreconditionFailed()
	}
	return err
}

func setChecksumHeaders(w http.ResponseWriter, checks []store.Checksum) {
	for _, c := range checks {
		if c.Name == "content-md5" {
			continue
		}
		w.Header().Set(c.Name, c.Value)
	}
}

var supportedChecksums = map[string]bool{
	"content-md5":              true,
	"x-amz-checksum-md5":       true,
	"x-amz-checksum-crc32":     true,
	"x-amz-checksum-crc32c":    true,
	"x-amz-checksum-crc64nvme": true,
	"x-amz-checksum-sha1":      true,
	"x-amz-checksum-sha256":    true,
	"x-amz-checksum-sha512":    true,
}

var crc64NVMETables = func() [8][256]uint64 {
	const poly = 0x9a6c9329ac4bc9b5
	var tables [8][256]uint64
	for i := range tables[0] {
		crc := uint64(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		tables[0][i] = crc
	}
	for k := 1; k < len(tables); k++ {
		for i := range tables[k] {
			crc := tables[k-1][i]
			tables[k][i] = tables[0][byte(crc)] ^ (crc >> 8)
		}
	}
	return tables
}()

type ingestInput struct {
	opts store.WriteOptions
	body uploadBody
}

type uploadBody struct {
	reader       io.Reader
	size         int64
	expectedSize int64
	integrity    *integrity
	trailers     http.Header
}

func prepareUploadBody(r *http.Request) (uploadBody, error) {
	body := uploadBody{reader: r.Body, size: r.ContentLength, expectedSize: r.ContentLength, trailers: r.Trailer}
	in, err := newIntegrity(r.Header)
	if err != nil {
		return body, err
	}
	if decoded, ok := r.Body.(*chunkedBody); ok {
		if decoded.decodedLen < 0 {
			return body, errInvalidRequest("invalid x-amz-decoded-content-length")
		}
		body.size, body.expectedSize = decoded.decodedLen, decoded.decodedLen
	} else if v := r.Header.Get("x-amz-decoded-content-length"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			body.expectedSize = n
		}
	}
	body.integrity = in
	if len(in.h) > 0 {
		body.reader = &integrityReader{r: r.Body, in: in}
	}
	return body, nil
}

func (b uploadBody) verify() ([]store.Checksum, error) { return b.integrity.verify(b.trailers) }

func prepareIngest(r *http.Request) (ingestInput, error) {
	var in ingestInput
	if err := validateWriteHeaders(r); err != nil {
		return in, err
	}
	meta, err := parseMetadata(r.Header)
	if err != nil {
		return in, err
	}
	ct, err := contentType(r.Header.Get("Content-Type"))
	if err != nil {
		return in, err
	}
	precondition, err := putCondition(r)
	if err != nil {
		return in, err
	}
	body, err := prepareUploadBody(r)
	if err != nil {
		return in, err
	}
	in.opts = store.WriteOptions{
		Precondition:    precondition,
		UserMetadata:    meta,
		ContentType:     ct,
		ContentEncoding: r.Header.Get("Content-Encoding"),
		ExpectedSize:    max(body.expectedSize, 0),
	}
	in.body = body
	if c := body.integrity.expectedChecksum(); c != nil {
		in.opts.Checksum = c
		in.opts.ChecksumType = "FULL_OBJECT"
	}
	return in, nil
}

var inlineBodyPool = sync.Pool{New: func() any {
	buf := make([]byte, store.MaxInlineBytes)
	return &buf
}}

type inlineBody struct {
	data   []byte
	pooled *[]byte
}

func (b *inlineBody) release() {
	if b.pooled != nil {
		inlineBodyPool.Put(b.pooled)
		b.pooled = nil
		b.data = nil
	}
}

func readInlineBody(contentLength int64, body io.Reader, max int64) (inlineBody, error) {
	if contentLength >= 0 && contentLength <= max {
		pooled := inlineBodyPool.Get().(*[]byte)
		data := (*pooled)[:contentLength]
		if _, err := io.ReadFull(body, data); err != nil {
			inlineBodyPool.Put(pooled)
			return inlineBody{}, err
		}
		if _, err := io.Copy(io.Discard, body); err != nil {
			inlineBodyPool.Put(pooled)
			return inlineBody{}, err
		}
		return inlineBody{data: data, pooled: pooled}, nil
	}
	data, err := io.ReadAll(io.LimitReader(body, max+1))
	return inlineBody{data: data}, err
}

type integrity struct {
	h        map[string]hash.Hash
	expected map[string]string
	trailers []string
}

func newIntegrity(h http.Header) (*integrity, error) {
	in := &integrity{h: map[string]hash.Hash{}, expected: map[string]string{}}
	for name, values := range h {
		lower := strings.ToLower(name)
		if lower == "x-amz-checksum-type" {
			if len(values) != 1 || (values[0] != "FULL_OBJECT" && values[0] != "COMPOSITE") {
				return nil, errInvalidRequest("invalid x-amz-checksum-type")
			}
			continue
		}
		if lower == "content-md5" || (strings.HasPrefix(lower, "x-amz-checksum-") && lower != "x-amz-checksum-algorithm") {
			if !supportedChecksums[lower] {
				return nil, errNotImplemented()
			}
			if len(values) != 1 {
				return nil, errInvalidRequest("duplicate checksum header")
			}
			in.expected[lower] = values[0]
			in.addHasher(lower)
		}
	}
	if declared := h.Get("x-amz-trailer"); declared != "" {
		for name := range strings.SplitSeq(declared, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			if !supportedChecksums[name] || name == "content-md5" {
				return nil, errNotImplemented()
			}
			in.trailers = append(in.trailers, name)
			in.addHasher(name)
		}
	}
	for _, algoHeader := range []string{"x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"} {
		algo := h.Get(algoHeader)
		if algo == "" {
			continue
		}
		name := "x-amz-checksum-" + strings.ToLower(algo)
		if !supportedChecksums[name] {
			return nil, errNotImplemented()
		}
		if _, ok := in.expected[name]; !ok && !slices.Contains(in.trailers, name) {
			return nil, errInvalidRequest("declared checksum algorithm has no checksum")
		}
	}
	return in, nil
}

func (in *integrity) expectedChecksum() *store.Checksum {
	for name, value := range in.expected {
		if name == "content-md5" {
			continue
		}
		return &store.Checksum{Name: name, Value: value}
	}
	return nil
}

func (in *integrity) addHasher(name string) {
	if _, ok := in.h[name]; ok {
		return
	}
	switch name {
	case "content-md5", "x-amz-checksum-md5":
		in.h[name] = md5.New()
	case "x-amz-checksum-crc32":
		in.h[name] = crc32.NewIEEE()
	case "x-amz-checksum-crc32c":
		in.h[name] = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "x-amz-checksum-crc64nvme":
		in.h[name] = newCRC64NVME()
	case "x-amz-checksum-sha1":
		in.h[name] = sha1.New()
	case "x-amz-checksum-sha256":
		in.h[name] = sha256.New()
	case "x-amz-checksum-sha512":
		in.h[name] = sha512.New()
	}
}

type integrityReader struct {
	r  io.ReadCloser
	in *integrity
}

func (r *integrityReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		for _, h := range r.in.h {
			h.Write(p[:n])
		}
	}
	return n, err
}

func (r *integrityReader) Close() error { return r.r.Close() }

func (in *integrity) verify(h http.Header) ([]store.Checksum, error) {
	for _, name := range in.trailers {
		value := h.Get(name)
		if value == "" {
			return nil, errInvalidRequest("missing checksum trailer " + name)
		}
		in.expected[name] = value
	}
	out := make([]store.Checksum, 0, len(in.expected))
	for name, want := range in.expected {
		hasher, ok := in.h[name]
		if !ok {
			return nil, errNotImplemented()
		}
		got := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		if got != want {
			return nil, apiErr("BadDigest", fmt.Sprintf("The %s you specified did not match the calculated checksum.", name), http.StatusBadRequest)
		}
		out = append(out, store.Checksum{Name: name, Value: got})
	}
	return out, nil
}

func algorithmHasher(algorithm string) (hash.Hash, string, bool) {
	switch algorithm {
	case "CRC32":
		return crc32.NewIEEE(), "x-amz-checksum-crc32", true
	case "CRC32C":
		return crc32.New(crc32.MakeTable(crc32.Castagnoli)), "x-amz-checksum-crc32c", true
	case "CRC64NVME":
		return newCRC64NVME(), "x-amz-checksum-crc64nvme", true
	case "SHA1":
		return sha1.New(), "x-amz-checksum-sha1", true
	case "SHA256":
		return sha256.New(), "x-amz-checksum-sha256", true
	default:
		return nil, "", false
	}
}

type crc64NVME struct {
	crc uint64
}

func newCRC64NVME() *crc64NVME { return &crc64NVME{crc: ^uint64(0)} }

func (c *crc64NVME) Write(p []byte) (int, error) {
	n := len(p)
	crc := c.crc
	for len(p) >= 8 {
		crc ^= binary.LittleEndian.Uint64(p)
		t := &crc64NVMETables
		crc = t[7][byte(crc)] ^ t[6][byte(crc>>8)] ^ t[5][byte(crc>>16)] ^ t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^ t[2][byte(crc>>40)] ^ t[1][byte(crc>>48)] ^ t[0][byte(crc>>56)]
		p = p[8:]
	}
	for _, b := range p {
		crc = crc64NVMETables[0][byte(crc)^b] ^ (crc >> 8)
	}
	c.crc = crc
	return n, nil
}

func (c *crc64NVME) Sum64() uint64 { return c.crc ^ ^uint64(0) }

func (c *crc64NVME) Sum(in []byte) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c.Sum64())
	return append(in, b[:]...)
}

func (c *crc64NVME) Reset() { c.crc = ^uint64(0) }

func (c *crc64NVME) Size() int { return 8 }

func (c *crc64NVME) BlockSize() int { return 1 }

var streamingPayloads = map[string]bool{
	"STREAMING-UNSIGNED-PAYLOAD-TRAILER":               true,
	"STREAMING-AWS4-HMAC-SHA256-PAYLOAD":               true,
	"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":       true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD":         true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER": true,
}

const (
	maxChunkHeader  = 128
	maxChunkSize    = 8 << 20
	maxTrailerBytes = 64 << 10
)

func decodedLength(r *http.Request) (int64, error) {
	n, err := strconv.ParseInt(r.Header.Get("x-amz-decoded-content-length"), 10, 64)
	if err != nil || n < 0 {
		return 0, errInvalidRequest("invalid x-amz-decoded-content-length")
	}
	return n, nil
}

func prepareBody(r *http.Request) error {
	if r.Trailer == nil && r.Header.Get("x-amz-trailer") != "" {
		r.Trailer = make(http.Header)
	}
	payload := r.Header.Get("x-amz-content-sha256")
	if !strings.HasPrefix(payload, "STREAMING-") {

		stripAwsChunked(r)
		return nil
	}
	if !streamingPayloads[payload] {
		return errInvalidRequest("unsupported streaming payload type")
	}
	decoded := int64(-1)
	if v := r.Header.Get("x-amz-decoded-content-length"); v != "" {
		n, err := decodedLength(r)
		if err != nil {
			return err
		}
		decoded = n
	}
	if r.Trailer == nil {
		r.Trailer = make(http.Header)
	}
	r.Body = &chunkedBody{
		r:          bufio.NewReaderSize(r.Body, 64<<10),
		trailer:    r.Trailer,
		decodedLen: decoded,
	}
	stripAwsChunked(r)
	return nil
}

func stripAwsChunked(r *http.Request) {
	enc := r.Header.Get("Content-Encoding")
	if !strings.Contains(strings.ToLower(enc), "aws-chunked") {
		return
	}
	var kept []string
	for part := range strings.SplitSeq(enc, ",") {
		part = strings.TrimSpace(part)
		if part != "" && !strings.EqualFold(part, "aws-chunked") {
			kept = append(kept, part)
		}
	}
	if len(kept) == 0 {
		r.Header.Del("Content-Encoding")
	} else {
		r.Header.Set("Content-Encoding", strings.Join(kept, ","))
	}
}

type chunkedBody struct {
	r          *bufio.Reader
	trailer    http.Header
	decodedLen int64
	total      int64
	remaining  int64
	needCRLF   bool
	done       bool
	err        error
}

func (c *chunkedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if c.err != nil {
			return 0, c.err
		}
		if c.remaining > 0 {
			want := len(p)
			if int64(want) > c.remaining {
				want = int(c.remaining)
			}
			n, err := c.r.Read(p[:want])
			if n > 0 {
				c.remaining -= int64(n)
				c.total += int64(n)
				if c.remaining == 0 {
					c.needCRLF = true
				}
				if err != nil {
					c.err = err
				}
				return n, nil
			}
			if err != nil {
				return 0, c.fail(err)
			}
			continue
		}
		if c.needCRLF {
			var crlf [2]byte
			if _, err := io.ReadFull(c.r, crlf[:]); err != nil {
				return 0, c.fail(err)
			}
			if crlf[0] != '\r' || crlf[1] != '\n' {
				return 0, c.fail(errInvalidRequest("invalid aws-chunked delimiter"))
			}
			c.needCRLF = false
			continue
		}
		if c.done {
			if c.decodedLen >= 0 && c.total != c.decodedLen {
				return 0, c.fail(errInvalidRequest("aws-chunked body length does not match x-amz-decoded-content-length"))
			}
			return 0, io.EOF
		}
		line, err := readChunkLine(c.r, maxChunkHeader)
		if err != nil {
			return 0, c.fail(err)
		}
		size, err := parseChunkHeader(line)
		if err != nil {
			return 0, c.fail(err)
		}
		if size == 0 {
			c.done = true
			if err := c.readTrailers(); err != nil {
				return 0, c.fail(err)
			}
			continue
		}
		if c.decodedLen >= 0 && c.total+size > c.decodedLen {
			return 0, c.fail(errInvalidRequest("aws-chunked body exceeds x-amz-decoded-content-length"))
		}
		c.remaining = size
	}
}

func (c *chunkedBody) Close() error { return nil }

func (c *chunkedBody) fail(err error) error {
	if _, ok := errors.AsType[*APIError](err); !ok {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			err = apiErr("IncompleteBody", "The request body terminated unexpectedly.", http.StatusBadRequest)
		} else {
			err = apiErr("InvalidRequest", "Malformed aws-chunked body.", http.StatusBadRequest)
		}
	}
	c.err = err
	return err
}

func readChunkLine(r *bufio.Reader, max int) (string, error) {
	b := make([]byte, 0, min(max, 256))
	for {
		c, err := r.ReadByte()
		if err != nil {
			return "", err
		}
		if c == '\n' {
			if n := len(b); n > 0 && b[n-1] == '\r' {
				b = b[:n-1]
			}
			return string(b), nil
		}
		if len(b) >= max {
			return "", errInvalidRequest("aws-chunked header line too long")
		}
		b = append(b, c)
	}
}

func parseChunkHeader(line string) (int64, error) {
	sizePart, params, _ := strings.Cut(line, ";")
	sizePart = strings.TrimSpace(sizePart)
	if sizePart == "" || len(sizePart) > 16 || !isHex(sizePart) {
		return 0, errInvalidRequest("invalid aws-chunked chunk size")
	}
	if params != "" {
		key, val, ok := strings.Cut(params, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "chunk-signature") ||
			len(strings.TrimSpace(val)) != 64 || !isHex(strings.TrimSpace(val)) {
			return 0, errInvalidRequest("invalid aws-chunked chunk header")
		}
	}
	size, err := strconv.ParseUint(sizePart, 16, 63)
	if err != nil {
		return 0, errInvalidRequest("invalid aws-chunked chunk size")
	}
	if size > maxChunkSize {
		return 0, errInvalidRequest("aws-chunked chunk exceeds 8 MiB")
	}
	return int64(size), nil
}

func (c *chunkedBody) readTrailers() error {
	total := 0
	for {
		line, err := readChunkLine(c.r, 8<<10)
		if err != nil {
			return err
		}
		if line == "" {
			return nil
		}
		total += len(line) + 2
		if total > maxTrailerBytes {
			return errInvalidRequest("aws-chunked trailers exceed 64 KiB")
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok || strings.TrimSpace(k) == "" {
			return errInvalidRequest("invalid aws-chunked trailer")
		}
		if c.trailer != nil {
			c.trailer.Add(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
