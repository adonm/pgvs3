package s3

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// AWS SigV4 streaming payload values. pgvs3 does not verify the signatures,
// but it must understand the framing and reject values it cannot parse.
var streamingPayloads = map[string]bool{
	"STREAMING-UNSIGNED-PAYLOAD-TRAILER":               true,
	"STREAMING-AWS4-HMAC-SHA256-PAYLOAD":               true,
	"STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER":       true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD":         true,
	"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER": true,
}

const (
	// maxChunkHeader bounds the "<hex-size>[;chunk-signature=<sig>]" line.
	maxChunkHeader = 128
	// maxChunkSize is a deliberate ceiling, not an AWS limit: AWS SDKs use
	// 64 KiB chunks, and 8 MiB leaves room for unusual clients without letting
	// a declared size become an allocation.
	maxChunkSize = 8 << 20
	// maxTrailerBytes bounds the optional trailer block.
	maxTrailerBytes = 64 << 10
)

// prepareBody decodes aws-chunked framing used by AWS SDKs and the AWS CLI.
// This is transport framing only: the gateway deliberately does not validate
// SigV4 signatures or chunk signatures.
//
// The returned error is an *APIError so handlers can surface a 400 rather than
// an internal error.
func prepareBody(r *http.Request) error {
	payload := r.Header.Get("x-amz-content-sha256")
	enc := r.Header.Get("Content-Encoding")
	encLower := strings.ToLower(enc)
	chunked := strings.HasPrefix(payload, "STREAMING-") || strings.Contains(encLower, "aws-chunked")
	if !chunked {
		return nil
	}
	if strings.HasPrefix(payload, "STREAMING-") && !streamingPayloads[payload] {
		return errInvalidRequest("unsupported streaming payload type")
	}
	decoded := int64(-1)
	if v := r.Header.Get("x-amz-decoded-content-length"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid x-amz-decoded-content-length")
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
	if strings.Contains(encLower, "aws-chunked") {
		var kept []string
		for _, part := range strings.Split(enc, ",") {
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
	return nil
}

// chunkedBody streams an aws-chunked body without allocating the declared
// chunk size. It is a state machine over: header line -> data -> CRLF ->
// header line ... -> zero chunk -> trailers.
type chunkedBody struct {
	r          *bufio.Reader
	trailer    http.Header
	decodedLen int64 // -1 when x-amz-decoded-content-length is absent
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
	var ae *APIError
	if !errors.As(err, &ae) {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			err = apiErr("IncompleteBody", "The request body terminated unexpectedly.", http.StatusBadRequest)
		} else {
			err = apiErr("InvalidRequest", "Malformed aws-chunked body.", http.StatusBadRequest)
		}
	}
	c.err = err
	return err
}

// readChunkLine reads one CRLF-terminated line and enforces max bytes.
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
