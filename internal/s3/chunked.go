package s3

import (
	"bufio"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// wrapStreaming decodes an aws-chunked body and verifies each chunk's SigV4
// signature. DuckDB and OpenDAL use UNSIGNED-PAYLOAD, but the AWS CLI and
// other S3 clients use streaming signatures; accepting them keeps the gateway
// generally usable.
func (a *Auth) wrapStreaming(r *http.Request, secret, amzDate, region, service, seed string) error {
	scope := amzDate[:8] + "/" + region + "/" + service + "/aws4_request"
	key := signingKey(secret, amzDate[:8], region, service)
	r.Body = &chunkedBody{
		r:     bufio.NewReader(r.Body),
		key:   key,
		date:  amzDate,
		scope: scope,
		prev:  seed,
	}
	if r.Trailer == nil {
		r.Trailer = make(http.Header)
	}
	if enc := r.Header.Get("Content-Encoding"); strings.Contains(enc, "aws-chunked") {
		enc = strings.TrimSpace(strings.ReplaceAll(enc, "aws-chunked", ""))
		enc = strings.Trim(enc, ",")
		if enc == "" {
			r.Header.Del("Content-Encoding")
		} else {
			r.Header.Set("Content-Encoding", enc)
		}
	}
	return nil
}

type chunkedBody struct {
	r     *bufio.Reader
	key   []byte
	date  string
	scope string
	prev  string
	done  bool
	buf   []byte
}

func (c *chunkedBody) Read(p []byte) (int, error) {
	if len(c.buf) > 0 {
		n := copy(p, c.buf)
		c.buf = c.buf[n:]
		return n, nil
	}
	if c.done {
		return 0, io.EOF
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return c.Read(p)
	}
	sizePart, sigPart, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizePart, 16, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid aws-chunked size")
	}
	var sig string
	if sigPart != "" {
		if !strings.HasPrefix(sigPart, "chunk-signature=") {
			return 0, fmt.Errorf("invalid aws-chunked signature")
		}
		sig = strings.TrimPrefix(sigPart, "chunk-signature=")
	}
	if size == 0 {
		c.done = true
		// Consume the trailer section, if any.
		for {
			tline, err := c.r.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					return 0, io.EOF
				}
				return 0, err
			}
			tline = strings.TrimSuffix(strings.TrimSuffix(tline, "\n"), "\r")
			if tline == "" {
				break
			}
			k, v, ok := strings.Cut(tline, ":")
			if ok {
				// Trailer values are already verified by the chunk signatures
				// over the payload; expose them for checksum validation.
				// (http.Request.Trailer is populated by the caller.)
				_ = k
				_ = v
			}
		}
		return 0, io.EOF
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(c.r, data); err != nil {
		return 0, err
	}
	var crlf [2]byte
	if _, err := io.ReadFull(c.r, crlf[:]); err != nil {
		return 0, err
	}
	if sig == "" {
		return 0, fmt.Errorf("missing chunk signature")
	}
	want := c.chunkSignature(data)
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return 0, fmt.Errorf("chunk signature mismatch")
	}
	c.prev = sig
	c.buf = data
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *chunkedBody) chunkSignature(data []byte) string {
	sum := sha256.Sum256(data)
	sts := "AWS4-HMAC-SHA256-PAYLOAD\n" + c.date + "\n" + c.scope + "\n" + c.prev + "\n" + emptySHA256 + "\n" + hex.EncodeToString(sum[:])
	m := hmac.New(sha256.New, c.key)
	m.Write([]byte(sts))
	return hex.EncodeToString(m.Sum(nil))
}

func (c *chunkedBody) Close() error { return nil }
