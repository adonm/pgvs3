package s3

import (
	"bytes"
	"io"
	"net/http/httptest"
	"testing"
)

func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"bytes=0-0", "bytes=-1", "bytes=0-", "bytes=1-2,3-4", "garbage"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _, _, _, _ = parseRange(s)
	})
}

func FuzzChunkedBody(f *testing.F) {
	f.Add([]byte("5\r\nhello\r\n0\r\n\r\n"))
	f.Add([]byte("0\r\n\r\n"))
	f.Add([]byte("ffffffffff\r\n"))
	f.Add([]byte("\r\n\r\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		req := httptest.NewRequest("PUT", "/bucket/key", bytes.NewReader(data))
		req.Header.Set("Content-Encoding", "aws-chunked")
		if err := prepareBody(req); err != nil {
			return
		}
		_, _ = io.ReadAll(req.Body)
	})
}
