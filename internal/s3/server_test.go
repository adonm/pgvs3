package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

func TestSpoolAdmissionMapsToRetryableSlowDown(t *testing.T) {
	s := &Server{log: slog.New(slog.DiscardHandler)}
	err := fmt.Errorf("spool: %w", objcache.ErrSpoolBusy)
	ae, ok := s.apiError(err).(*APIError)
	if !ok || ae.Status != http.StatusServiceUnavailable || ae.Code != "SlowDown" {
		t.Fatalf("spool admission mapped to %+v", ae)
	}
	w := httptest.NewRecorder()
	s.fail(w, httptest.NewRequest(http.MethodGet, "/bucket/key", nil), err)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("read-fill admission: status=%d headers=%v", w.Code, w.Header())
	}
}

func TestServerAdmissionBoundsWaitsAndCancels(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(nil, slog.New(slog.DiscardHandler), 1, time.Second, nil)
		defer s.Close()
		release, ok := s.acquire(t.Context())
		if !ok {
			t.Fatal("first acquisition failed")
		}
		acquired := make(chan func(), 1)
		go func() {
			release, _ := s.acquire(t.Context())
			acquired <- release
		}()
		synctest.Wait()
		if len(s.waiters) != 1 || len(acquired) != 0 {
			t.Fatal("contended request did not queue")
		}
		ctx, cancel := context.WithCancel(t.Context())
		canceled := make(chan *httptest.ResponseRecorder, 1)
		body := strings.NewReader("unread body")
		go func() {
			r := httptest.NewRequest(http.MethodPut, "/bucket/key", body).WithContext(ctx)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			canceled <- w
		}()
		synctest.Wait()
		if len(s.waiters) != 2 {
			t.Fatal("second waiter did not reach the queue bound")
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("full queue: status=%d headers=%v", w.Code, w.Header())
		}
		for _, path := range []string{"/healthz", "/metrics"} {
			w := httptest.NewRecorder()
			s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("%s blocked by admission: %d", path, w.Code)
			}
			if path == "/metrics" && !strings.Contains(w.Body.String(), "pgvs3_s3_admission_waiting_requests 2\n") {
				t.Fatal("queue occupancy is missing from metrics")
			}
		}
		cancel()
		synctest.Wait()
		if w := <-canceled; w.Code != http.StatusRequestTimeout || len(s.waiters) != 1 {
			t.Fatalf("cancellation: status=%d waiting=%d", w.Code, len(s.waiters))
		}
		if body.Len() != len("unread body") {
			t.Fatal("queued request body was read before admission")
		}
		release()
		synctest.Wait()
		release = <-acquired
		if release == nil || len(s.waiters) != 0 || len(s.sem) != 1 {
			t.Fatal("slot was not handed to the queued request")
		}
		go func() {
			release, _ := s.acquire(t.Context())
			acquired <- release
		}()
		synctest.Wait()
		time.Sleep(time.Second)
		synctest.Wait()
		if queued := <-acquired; queued != nil || len(s.waiters) != 0 || len(s.sem) != 1 {
			t.Fatal("expired waiter leaked a queue or service slot")
		}
		release()
		if s.queued.Load() != 3 || s.rejected.Load() != 2 {
			t.Fatalf("queued=%d rejected=%d", s.queued.Load(), s.rejected.Load())
		}
	})
	for _, canceled := range []bool{false, true} {
		s := New(nil, slog.New(slog.DiscardHandler), 1, 0, nil)
		ctx, cancel := context.WithCancel(t.Context())
		release, _ := s.acquire(ctx)
		if canceled {
			cancel()
		}
		if _, ok := s.acquire(ctx); ok {
			t.Fatal("disabled queue acquired a busy slot")
		}
		release()
		cancel()
		s.Close()
	}
}

func TestSelectRoute(t *testing.T) {
	tests := []struct {
		name   string
		table  []route
		method string
		target string
		header string
		value  string
		want   string
	}{
		{name: "bucket put", table: bucketRoutes, method: http.MethodPut, target: "/bucket", want: "bucket-put"},
		{name: "bucket delete", table: bucketRoutes, method: http.MethodDelete, target: "/bucket", want: "bucket-delete"},
		{name: "bucket head", table: bucketRoutes, method: http.MethodHead, target: "/bucket", want: "bucket-head"},
		{name: "bucket get default", table: bucketRoutes, method: http.MethodGet, target: "/bucket", want: "bucket-get"},
		{name: "bucket get unknown query", table: bucketRoutes, method: http.MethodGet, target: "/bucket?unknown=1", want: "bucket-get"},
		{name: "bucket location", table: bucketRoutes, method: http.MethodGet, target: "/bucket?location", want: "bucket-location"},
		{name: "bucket versions", table: bucketRoutes, method: http.MethodGet, target: "/bucket?versions", want: "bucket-versions"},
		{name: "bucket uploads", table: bucketRoutes, method: http.MethodGet, target: "/bucket?uploads", want: "bucket-uploads"},
		{name: "bucket location precedes versions", table: bucketRoutes, method: http.MethodGet, target: "/bucket?versions&location", want: "bucket-location"},
		{name: "bucket delete objects", table: bucketRoutes, method: http.MethodPost, target: "/bucket?delete", want: "bucket-delete-objects"},
		{name: "bucket post without delete", table: bucketRoutes, method: http.MethodPost, target: "/bucket", want: ""},
		{name: "bucket patch", table: bucketRoutes, method: http.MethodPatch, target: "/bucket", want: ""},

		{name: "object head", table: objectRoutes, method: http.MethodHead, target: "/bucket/key", want: "object-head"},
		{name: "object get", table: objectRoutes, method: http.MethodGet, target: "/bucket/key", want: "object-get"},
		{name: "object attributes", table: objectRoutes, method: http.MethodGet, target: "/bucket/key?attributes", want: "object-attributes"},
		{name: "object list parts", table: objectRoutes, method: http.MethodGet, target: "/bucket/key?uploadId=u", want: "object-list-parts"},
		{name: "object attributes precede parts", table: objectRoutes, method: http.MethodGet, target: "/bucket/key?uploadId=u&attributes", want: "object-attributes"},
		{name: "object put", table: objectRoutes, method: http.MethodPut, target: "/bucket/key", want: "object-put"},
		{name: "object upload part", table: objectRoutes, method: http.MethodPut, target: "/bucket/key?partNumber=1&uploadId=u", want: "object-upload-part"},
		{
			name: "object upload part copy", table: objectRoutes, method: http.MethodPut,
			target: "/bucket/key?partNumber=1&uploadId=u", header: "x-amz-copy-source", value: "/src/key",
			want: "object-upload-part-copy",
		},
		{
			name: "object copy", table: objectRoutes, method: http.MethodPut,
			target: "/bucket/key", header: "x-amz-copy-source", value: "/src/key", want: "object-copy",
		},
		{
			name: "object copy without upload id falls through to copy", table: objectRoutes, method: http.MethodPut,
			target: "/bucket/key?partNumber=1", header: "x-amz-copy-source", value: "/src/key", want: "object-copy",
		},
		{name: "object create multipart", table: objectRoutes, method: http.MethodPost, target: "/bucket/key?uploads", want: "object-create-multipart"},
		{name: "object complete multipart", table: objectRoutes, method: http.MethodPost, target: "/bucket/key?uploadId=u", want: "object-complete-multipart"},
		{name: "object create precedes complete", table: objectRoutes, method: http.MethodPost, target: "/bucket/key?uploadId=u&uploads", want: "object-create-multipart"},
		{name: "object post without query", table: objectRoutes, method: http.MethodPost, target: "/bucket/key", want: ""},
		{name: "object abort multipart", table: objectRoutes, method: http.MethodDelete, target: "/bucket/key?uploadId=u", want: "object-abort-multipart"},
		{name: "object delete", table: objectRoutes, method: http.MethodDelete, target: "/bucket/key", want: "object-delete"},
		{name: "object patch", table: objectRoutes, method: http.MethodPatch, target: "/bucket/key", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://s3.test"+tt.target, nil)
			if tt.header != "" {
				req.Header.Set(tt.header, tt.value)
			}
			got := selectRoute(tt.table, req, req.URL.Query())
			gotName := ""
			if got != nil {
				gotName = got.name
			}
			if gotName != tt.want {
				t.Fatalf("selectRoute() = %q, want %q", gotName, tt.want)
			}
		})
	}
}

func TestOriginAPIErrorExhaustive(t *testing.T) {
	s := &Server{log: slog.New(slog.DiscardHandler)}
	cases := []struct {
		kind   objcache.OriginErrorKind
		status int
		code   string
	}{
		{objcache.OriginNotFound, http.StatusNotFound, "NoSuchKey"},
		{objcache.OriginPrecondition, http.StatusPreconditionFailed, "PreconditionFailed"},
		{objcache.OriginConflict, http.StatusConflict, "OperationAborted"},
		{objcache.OriginWrongRegion, http.StatusMovedPermanently, "PermanentRedirect"},
		{objcache.OriginOther, http.StatusInternalServerError, "InternalError"},
	}
	for _, tc := range cases {
		ae := s.originAPIError(tc.kind, errors.New("boom"))
		if ae.Status != tc.status || ae.Code != tc.code {
			t.Fatalf("kind %d: got (%d, %s), want (%d, %s)", tc.kind, ae.Status, ae.Code, tc.status, tc.code)
		}
	}
}

func TestReadOnlyDerivedFromTier(t *testing.T) {
	o, err := objcache.NewS3Origin(objcache.S3Config{Endpoint: "http://127.0.0.1:1", Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	tier, err := objcache.New(nil, o, &store.Store{}, objcache.NewRuntime(objcache.RuntimeOptions{}), objcache.TierOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	s := New(nil, slog.New(slog.DiscardHandler), 4, time.Second, tier)
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPost} {
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, httptest.NewRequest(method, "/b/k", nil))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s on read-only tier = %d, want 403", method, rec.Code)
		}
	}
}

func TestCheckReadPreconditions(t *testing.T) {
	mod := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	before := mod.Add(-time.Hour).Format(http.TimeFormat)
	same := mod.Format(http.TimeFormat)
	after := mod.Add(time.Hour).Format(http.TimeFormat)

	cases := []struct {
		name    string
		headers map[string]string
		wantErr error
	}{
		{"if-modified-since past", map[string]string{"If-Modified-Since": before}, nil},
		{"if-modified-since equal", map[string]string{"If-Modified-Since": same}, store.ErrNotModified},
		{"if-modified-since future", map[string]string{"If-Modified-Since": after}, store.ErrNotModified},
		{"if-none-match overrides if-modified-since", map[string]string{"If-None-Match": `"other"`, "If-Modified-Since": after}, nil},
		{"if-unmodified-since past", map[string]string{"If-Unmodified-Since": before}, errPreconditionFailed()},
		{"if-unmodified-since equal", map[string]string{"If-Unmodified-Since": same}, nil},
		{"if-unmodified-since future", map[string]string{"If-Unmodified-Since": after}, nil},
		{"if-match overrides if-unmodified-since", map[string]string{"If-Match": `"etag1"`, "If-Unmodified-Since": before}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/b/k", nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			err := checkReadPreconditions(req, "etag1", mod)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("got %v, want nil", err)
			}
			if tc.wantErr != nil && (err == nil || err.Error() != tc.wantErr.Error()) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestParseRange(t *testing.T) {
	tests := []struct {
		in   string
		want store.ByteRange
		ok   bool
	}{
		{"", store.WholeObjectRange(), true},
		{"bytes=0-4", store.Range(0, 4, -1), true},
		{"bytes=10-", store.Range(10, -1, -1), true},
		{"bytes=-100", store.Range(0, -1, 100), true},
		{"bytes=0-", store.Range(0, -1, -1), true},
		{"bytes=5-4", store.ByteRange{}, false},
		{"bytes=0-1,2-3", store.ByteRange{}, false},
		{"items=0-1", store.ByteRange{}, false},
	}
	for _, tt := range tests {
		got, err := parseRange(tt.in)
		if (err == nil) != tt.ok {
			t.Fatalf("%q: err=%v", tt.in, err)
		}
		if err != nil {
			continue
		}
		if got != tt.want {
			t.Fatalf("%q: %+v, want %+v", tt.in, got, tt.want)
		}
	}
}

func TestSplitPathKeepsDoubleEncodedSlash(t *testing.T) {

	r := httptest.NewRequest("GET", "http://x/bucket/objfs/analytics%252Freport.db/manifest/1", nil)
	bucket, key, err := splitPath(r)
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "bucket" || key != "objfs/analytics%2Freport.db/manifest/1" {
		t.Fatalf("bucket=%q key=%q", bucket, key)
	}
}

func TestPresignedRequestRejected(t *testing.T) {
	s := &Server{
		log:     slog.New(slog.DiscardHandler),
		metrics: &requestMetrics{},
	}
	for _, target := range []string{
		"/bucket/key?X-Amz-Signature=abc",
		"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak",
		"/bucket/key?AWSAccessKeyId=ak&Signature=abc&Expires=1",
	} {
		req := httptest.NewRequest("GET", target, nil)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != 400 {
			t.Fatalf("%s: got %d, want 400", target, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "InvalidRequest") {
			t.Fatalf("%s: body %q does not contain InvalidRequest", target, rec.Body.String())
		}
	}
}

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

func TestMetricsObserveAndWrite(t *testing.T) {
	m := &requestMetrics{}
	m.Observe("GET", 200, 3*time.Millisecond, 10)
	m.Observe("GET", 200, 7*time.Millisecond, 20)
	m.Observe("PUT", 403, 25*time.Millisecond, 0)

	rr := httptest.NewRecorder()
	s := New(nil, nil, 1, time.Second, nil)
	s.metrics = m
	s.writeMetrics(rr, nil)

	if got := rr.Header().Get("Content-Type"); got != "text/plain; version=0.0.4" {
		t.Fatalf("Content-Type = %q", got)
	}
	body := rr.Body.String()
	for _, want := range []string{
		`pgvs3_requests_total{method="GET",code="200"} 2`,
		`pgvs3_requests_total{method="PUT",code="403"} 1`,
		"pgvs3_request_duration_seconds_count 3",
		`pgvs3_request_duration_seconds_bucket{le="0.005"} 1`,
		`pgvs3_request_duration_seconds_bucket{le="0.01"} 2`,
		`pgvs3_request_duration_seconds_bucket{le="+Inf"} 3`,
		"pgvs3_response_bytes_total 30",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsConcurrentObserve(t *testing.T) {
	m := &requestMetrics{}
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			m.Observe("GET", 200, time.Millisecond, 1)
		})
	}
	wg.Wait()
	rr := httptest.NewRecorder()
	s := New(nil, nil, 1, time.Second, nil)
	s.metrics = m
	s.writeMetrics(rr, nil)
	if !strings.Contains(rr.Body.String(), `pgvs3_requests_total{method="GET",code="200"} 100`) {
		t.Fatalf("unexpected request series:\n%s", rr.Body.String())
	}
}

func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"bytes=0-0", "bytes=-1", "bytes=0-", "bytes=1-2,3-4", "garbage"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _ = parseRange(s)
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
