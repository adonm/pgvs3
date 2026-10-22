package s3

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// prepareBody decodes aws-chunked framing used by AWS SDKs and the AWS CLI.
// This is transport framing only: the gateway deliberately does not validate
// SigV4 signatures or chunk signatures.
func prepareBody(r *http.Request) {
	payload := r.Header.Get("x-amz-content-sha256")
	if !strings.HasPrefix(payload, "STREAMING-") &&
		!strings.Contains(strings.ToLower(r.Header.Get("Content-Encoding")), "aws-chunked") {
		return
	}
	if r.Trailer == nil {
		r.Trailer = make(http.Header)
	}
	r.Body = &chunkedBody{r: bufio.NewReader(r.Body), trailer: r.Trailer}
	if enc := r.Header.Get("Content-Encoding"); strings.Contains(strings.ToLower(enc), "aws-chunked") {
		enc = strings.TrimSpace(strings.ReplaceAll(enc, "aws-chunked", ""))
		enc = strings.Trim(enc, ",")
		if enc == "" {
			r.Header.Del("Content-Encoding")
		} else {
			r.Header.Set("Content-Encoding", enc)
		}
	}
}

type chunkedBody struct {
	r       *bufio.Reader
	trailer http.Header
	done    bool
	buf     []byte
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
	sizePart, _, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(sizePart, 16, 64)
	if err != nil || size < 0 {
		return 0, fmt.Errorf("invalid aws-chunked size")
	}
	if size == 0 {
		c.done = true
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
			if ok && c.trailer != nil {
				c.trailer.Add(strings.TrimSpace(k), strings.TrimSpace(v))
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
	c.buf = data
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	return n, nil
}

func (c *chunkedBody) Close() error { return nil }
