package s3

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func metadataHeader(pairs []string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set("x-amz-meta-"+pairs[i], pairs[i+1])
	}
	return h
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
	case errors.Is(err, objcache.ErrOriginConflict):
		return apiErr("OperationAborted", "A conflicting conditional operation is in progress. Retry the request.", http.StatusConflict)
	case errors.Is(err, objcache.ErrOriginUnsupported):
		return apiErr("NotImplemented", "The configured origin does not support this operation.", http.StatusNotImplemented)
	case errors.Is(err, store.ErrPrecondition):
		return errPreconditionFailed()
	case errors.Is(err, store.ErrNotFound):
		return errNoSuchKey()
	case errors.Is(err, ns.ErrInvalidName):
		return errInvalidBucketName()
	case errors.Is(err, store.ErrInvalidRange):
		return errInvalidRange()
	default:
		return errInternal(err, s.log)
	}
}

func headerReadCondition(r *http.Request) store.ReadCondition {
	return store.ReadCondition{
		IfMatch:     r.Header.Get("If-Match"),
		IfNoneMatch: r.Header.Get("If-None-Match"),
	}
}

func readCondition(r *http.Request, etag string) error {
	err := headerReadCondition(r).Check(true, etag)
	if errors.Is(err, store.ErrPrecondition) {
		return errPreconditionFailed()
	}
	return err
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

type crc64NVME struct {
	crc uint64
}

func newCRC64NVME() *crc64NVME { return &crc64NVME{crc: ^uint64(0)} }

func (c *crc64NVME) Write(p []byte) (int, error) {
	n := len(p)
	crc := c.crc
	t := &crc64NVMETables
	for len(p) >= 32 {
		crc ^= binary.LittleEndian.Uint64(p)
		crc = t[7][byte(crc)] ^ t[6][byte(crc>>8)] ^ t[5][byte(crc>>16)] ^ t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^ t[2][byte(crc>>40)] ^ t[1][byte(crc>>48)] ^ t[0][byte(crc>>56)]
		crc ^= binary.LittleEndian.Uint64(p[8:])
		crc = t[7][byte(crc)] ^ t[6][byte(crc>>8)] ^ t[5][byte(crc>>16)] ^ t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^ t[2][byte(crc>>40)] ^ t[1][byte(crc>>48)] ^ t[0][byte(crc>>56)]
		crc ^= binary.LittleEndian.Uint64(p[16:])
		crc = t[7][byte(crc)] ^ t[6][byte(crc>>8)] ^ t[5][byte(crc>>16)] ^ t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^ t[2][byte(crc>>40)] ^ t[1][byte(crc>>48)] ^ t[0][byte(crc>>56)]
		crc ^= binary.LittleEndian.Uint64(p[24:])
		crc = t[7][byte(crc)] ^ t[6][byte(crc>>8)] ^ t[5][byte(crc>>16)] ^ t[4][byte(crc>>24)] ^
			t[3][byte(crc>>32)] ^ t[2][byte(crc>>40)] ^ t[1][byte(crc>>48)] ^ t[0][byte(crc>>56)]
		p = p[32:]
	}
	for len(p) >= 8 {
		crc ^= binary.LittleEndian.Uint64(p)
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
