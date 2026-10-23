package s3

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func signedChunk(size, data, sig string) string {
	return size + ";chunk-signature=" + sig + "\r\n" + data + "\r\n"
}

func chunkedRequest(body string) *http.Request {
	req := httptest.NewRequest("PUT", "/bucket/key", strings.NewReader(body))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	return req
}

func readAll(t *testing.T, req *http.Request) ([]byte, error) {
	t.Helper()
	if err := prepareBody(req); err != nil {
		return nil, err
	}
	return io.ReadAll(req.Body)
}

func assertAPIError(t *testing.T, err error) {
	t.Helper()
	var ae *APIError
	if !errors.As(err, &ae) {
		t.Fatalf("got %v, want *APIError", err)
	}
	if ae.Status != http.StatusBadRequest {
		t.Fatalf("got status %d, want 400", ae.Status)
	}
}

func TestChunkedBodySignedWithTrailer(t *testing.T) {
	sig := strings.Repeat("a", 64)
	body := signedChunk("5", "hello", sig) +
		signedChunk("6", " world", sig) +
		"0;chunk-signature=" + sig + "\r\n" +
		"x-amz-checksum-crc32: abc\r\n\r\n"
	req := chunkedRequest(body)
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-decoded-content-length", "11")
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	if err := prepareBody(req); err != nil {
		t.Fatal(err)
	}
	// Read one byte at a time to exercise the data/CRLF state machine.
	var got []byte
	buf := make([]byte, 1)
	for {
		n, err := req.Body.Read(buf)
		got = append(got, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(got) != "hello world" {
		t.Fatalf("got %q", got)
	}
	if v := req.Trailer.Get("x-amz-checksum-crc32"); v != "abc" {
		t.Fatalf("trailer = %q", v)
	}
}

func TestChunkedBodyUnsigned(t *testing.T) {
	body := "5\r\nhello\r\n0\r\n\r\n"
	got, err := readAll(t, chunkedRequest(body))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestChunkedBodyRejectsHugeDeclaredSize(t *testing.T) {
	req := chunkedRequest("ffffffffff\r\n")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyRejectsOversizedChunk(t *testing.T) {
	// 8 MiB + 1, without sending the data.
	req := chunkedRequest("800001\r\n")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyRejectsBlankLines(t *testing.T) {
	req := chunkedRequest("\r\n\r\n\r\n")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyRejectsHeaderLineTooLong(t *testing.T) {
	req := chunkedRequest(strings.Repeat("a", 200) + "\r\n")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyDecodedLengthMismatch(t *testing.T) {
	req := chunkedRequest("5\r\nhello\r\n0\r\n\r\n")
	req.Header.Set("x-amz-decoded-content-length", "3")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyExceedsDecodedLength(t *testing.T) {
	req := chunkedRequest("5\r\nhello\r\n0\r\n\r\n")
	req.Header.Set("x-amz-decoded-content-length", "2")
	_, err := readAll(t, req)
	assertAPIError(t, err)
}

func TestChunkedBodyRejectsUnknownStreamingType(t *testing.T) {
	req := chunkedRequest("0\r\n\r\n")
	req.Header.Set("x-amz-content-sha256", "STREAMING-NOT-A-REAL-TYPE")
	if err := prepareBody(req); err == nil {
		t.Fatal("expected an error")
	} else {
		assertAPIError(t, err)
	}
}

func TestPrepareBodyStripsAwsChunkedEncoding(t *testing.T) {
	req := chunkedRequest("0\r\n\r\n")
	req.Header.Set("Content-Encoding", "aws-chunked, gzip")
	if err := prepareBody(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q", got)
	}
}

func TestAwsChunkedContentEncodingWithoutStreaming(t *testing.T) {
	req := httptest.NewRequest("PUT", "/bucket/key", strings.NewReader("plain-body"))
	req.Header.Set("Content-Encoding", "gzip, aws-chunked")
	if err := prepareBody(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q", got)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil || string(body) != "plain-body" {
		t.Fatalf("body = %q err=%v", body, err)
	}
}
