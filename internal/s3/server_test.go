package s3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/telemetry"
	"github.com/adonm/pgvs3/internal/testutil"
)

func TestCRC64NVMEAgainstBitwise(t *testing.T) {
	bitwise := func(data []byte) uint64 {
		crc := ^uint64(0)
		for _, b := range data {
			crc ^= uint64(b)
			for range 8 {
				if crc&1 != 0 {
					crc = (crc >> 1) ^ 0x9a6c9329ac4bc9b5
				} else {
					crc >>= 1
				}
			}
		}
		return ^crc
	}
	for size := range 129 {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i*31 + size)
		}
		hasher := newCRC64NVME()
		if _, err := hasher.Write(data); err != nil {
			t.Fatal(err)
		}
		if got, want := hasher.Sum64(), bitwise(data); got != want {
			t.Fatalf("size %d: got %016x, want %016x", size, got, want)
		}
	}
}

func TestBackendSelectionSharesAllCapabilities(t *testing.T) {
	ctx := context.Background()
	s := New(nil, nil, false, 1, nil)
	capabilities := func() []any {
		return []any{s.backendFor(ctx, "lake"), s.writeBackendFor(ctx, "lake"), s.multipartBackendFor(ctx, "lake")}
	}
	for _, backend := range capabilities() {
		if _, ok := backend.(localBackend); !ok {
			t.Fatalf("native capability selected %T", backend)
		}
	}
	tier, err := objcache.New(nil, []byte(`{"type":"ephemeral"}`), nil, objcache.NewRuntime(objcache.RuntimeOptions{}), objcache.TierOptions{
		Namespace: "origin", TTL: time.Minute, Retention: time.Hour, MaxFills: 1, ReadOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tier.Close()
	s.SetOrigin(tier)
	for _, backend := range capabilities() {
		origin, ok := backend.(originBackend)
		if !ok || origin.Tier != tier {
			t.Fatalf("origin capability selected %T", backend)
		}
	}
}

func TestRangeDoesNotAdvertiseWholeObjectChecksum(t *testing.T) {
	res := &store.ReadResult{Size: 100, Start: 10, End: 19, Ranged: true,
		ChecksumName: "crc32", ChecksumValue: "whole-object", ChecksumType: "FULL_OBJECT"}
	w := httptest.NewRecorder()
	setReadHeaders(w, res, "key", true)
	if got := w.Header().Get("x-amz-checksum-crc32"); got != "" {
		t.Fatalf("partial payload checksum = %q", got)
	}
	if res.ChecksumValue != "whole-object" {
		t.Fatal("range response mutated shared metadata")
	}
	res.Ranged = false
	w = httptest.NewRecorder()
	setReadHeaders(w, res, "key", true)
	if got := w.Header().Get("x-amz-checksum-crc32"); got != "whole-object" {
		t.Fatalf("full payload lost checksum = %q", got)
	}
}

func TestSpoolAdmissionMapsToRetryableSlowDown(t *testing.T) {
	s := &Server{log: testutil.DiscardLogger()}
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

func TestIngestErrorMapsOriginUnsupported(t *testing.T) {
	s := &Server{}
	for _, err := range []error{
		fmt.Errorf("upload: %w", objcache.ErrOriginUnsupported),
		fmt.Errorf("list: %w", objcache.ErrOriginListingUnsupported),
	} {
		got := s.apiError(err)
		ae, ok := got.(*APIError)
		if !ok {
			t.Fatalf("error %v mapped to %T, want *APIError", err, got)
		}
		if ae.Status != http.StatusNotImplemented || ae.Code != "NotImplemented" {
			t.Fatalf("error %v mapped to %+v", err, ae)
		}
	}
}

func TestIngestErrorMapsUnrepresentableKeyToBadRequest(t *testing.T) {
	s := &Server{}
	err := fmt.Errorf("upload: %w", objcache.ErrOriginKeyUnsupported)
	ae, ok := s.apiError(err).(*APIError)
	if !ok || ae.Status != http.StatusBadRequest || ae.Code != "InvalidRequest" {
		t.Fatalf("unrepresentable key mapped to %+v", ae)
	}
}

func TestServerRequestSemaphore(t *testing.T) {
	s := New(nil, testutil.DiscardLogger(), false, 1, nil)
	release, ok := s.acquire()
	if !ok {
		t.Fatal("first acquisition failed")
	}
	if _, ok := s.acquire(); ok {
		t.Fatal("second acquisition succeeded at limit 1")
	}
	release()
	release2, ok := s.acquire()
	if !ok {
		t.Fatal("acquisition after release failed")
	}
	release2()
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

		{name: "origin head", table: originBucketRoutes, method: http.MethodHead, target: "/bucket?location", want: "origin-bucket-head"},
		{name: "origin get default", table: originBucketRoutes, method: http.MethodGet, target: "/bucket", want: "origin-bucket-list"},
		{name: "origin location", table: originBucketRoutes, method: http.MethodGet, target: "/bucket?location", want: "origin-bucket-location"},
		{name: "origin versions", table: originBucketRoutes, method: http.MethodGet, target: "/bucket?versions", want: "origin-bucket-versions"},
		{
			name: "origin copy rejected", table: originBucketRoutes, method: http.MethodPut,
			target: "/bucket", header: "x-amz-copy-source", value: "/src/bucket", want: "origin-bucket-copy",
		},
		{name: "origin put accepted", table: originBucketRoutes, method: http.MethodPut, target: "/bucket", want: "origin-bucket-put"},
		{name: "origin delete refused", table: originBucketRoutes, method: http.MethodDelete, target: "/bucket", want: "origin-bucket-delete"},
		{name: "origin delete objects", table: originBucketRoutes, method: http.MethodPost, target: "/bucket?delete", want: "origin-bucket-delete-objects"},
		{name: "origin post without delete", table: originBucketRoutes, method: http.MethodPost, target: "/bucket", want: ""},
		{name: "origin patch", table: originBucketRoutes, method: http.MethodPatch, target: "/bucket", want: ""},
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

func TestRouteDispatch(t *testing.T) {
	called := ""
	table := []route{
		{name: "specific", method: http.MethodGet, query: "special", run: func(*routeContext) error {
			called = "specific"
			return nil
		}},
		{name: "fallback", method: http.MethodGet, run: func(*routeContext) error {
			called = "fallback"
			return nil
		}},
	}
	tests := []struct {
		name   string
		method string
		target string
		want   string
		run    bool
	}{
		{name: "specific query", method: http.MethodGet, target: "/bucket?special=1", want: "specific", run: true},
		{name: "fallback", method: http.MethodGet, target: "/bucket", want: "fallback", run: true},
		{name: "no match", method: http.MethodPost, target: "/bucket", want: "", run: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = ""
			req := httptest.NewRequest(tt.method, "http://s3.test"+tt.target, nil)
			c := routeContext{r: req, q: req.URL.Query()}
			if got, err := c.dispatch(table); got != tt.run || err != nil {
				t.Fatalf("dispatch() = %v, %v, want %v, nil", got, err, tt.run)
			}
			if called != tt.want {
				t.Fatalf("dispatch() called %q, want %q", called, tt.want)
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

func TestPrefixEndUTF8(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "\U0010FFFF"},
		{"a", "b"},
		{"abc", "abd"},
		{"ab\u00ff", "ab\u0100"},
		{"lake/", "lake0"},
		{"\U0010FFFF", "\U0010FFFF"},
	} {
		if got := prefixEnd(tc.in); got != tc.want {
			t.Fatalf("prefixEnd(%q) = %q, want %q", tc.in, got, tc.want)
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

func TestAWSEncode(t *testing.T) {
	tests := []struct {
		in          string
		encodeSlash bool
		want        string
	}{
		{"abc-._~09AZ", true, "abc-._~09AZ"},
		{"a b", true, "a%20b"},
		{"a/b", true, "a%2Fb"},
		{"a/b", false, "a/b"},
		{"a+b", true, "a%2Bb"},
	}
	for _, tt := range tests {
		if got := awsEncode(tt.in, tt.encodeSlash); got != tt.want {
			t.Errorf("awsEncode(%q, %v) = %q, want %q", tt.in, tt.encodeSlash, got, tt.want)
		}
	}
}

func TestPartBounds(t *testing.T) {
	single := &store.Meta{FileID: 1, Size: 100}
	if s, e, ok := partBounds(single, 1); !ok || s != 0 || e != 99 {
		t.Fatalf("single part = %d,%d,%v", s, e, ok)
	}
	if _, _, ok := partBounds(single, 2); ok {
		t.Fatal("single object has part 2")
	}
	multi := &store.Meta{FileID: 1, Size: 100, Parts: []int64{1, 2}, PartEnds: []int64{40, 100}}
	if s, e, ok := partBounds(multi, 2); !ok || s != 40 || e != 99 {
		t.Fatalf("part 2 = %d,%d,%v", s, e, ok)
	}
	if _, _, ok := partBounds(multi, 3); ok {
		t.Fatal("out of range part accepted")
	}
}

func TestInvalidPartNumberError(t *testing.T) {
	e := errInvalidPartNumber()
	if e.Code != "InvalidPartNumber" || e.Status != 416 {
		t.Fatalf("invalid part number = %+v", e)
	}
}

func headerRequest(h map[string]string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	for k, v := range h {
		req.Header.Set(k, v)
	}
	return req
}

func TestValidateWriteHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"plain", nil, 0},
		{"sse aes256", map[string]string{"x-amz-server-side-encryption": "AES256"}, 0},
		{"sse kms", map[string]string{"x-amz-server-side-encryption": "aws:kms"}, http.StatusNotImplemented},
		{"sse kms key id", map[string]string{"x-amz-server-side-encryption-aws-kms-key-id": "k"}, http.StatusNotImplemented},
		{"sse c", map[string]string{"x-amz-server-side-encryption-customer-algorithm": "AES256"}, http.StatusNotImplemented},
		{"acl private", map[string]string{"x-amz-acl": "private"}, 0},
		{"acl public", map[string]string{"x-amz-acl": "public-read"}, http.StatusNotImplemented},
		{"grant", map[string]string{"x-amz-grant-full-control": "id=x"}, http.StatusNotImplemented},
		{"object lock", map[string]string{"x-amz-object-lock-mode": "COMPLIANCE"}, http.StatusNotImplemented},
		{"legal hold", map[string]string{"x-amz-object-lock-legal-hold": "ON"}, http.StatusNotImplemented},
		{"tagging", map[string]string{"x-amz-tagging": "a=b"}, http.StatusNotImplemented},
		{"storage standard", map[string]string{"x-amz-storage-class": "STANDARD"}, 0},
		{"storage glacier", map[string]string{"x-amz-storage-class": "GLACIER"}, http.StatusNotImplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWriteHeaders(headerRequest(tt.headers))
			if tt.want == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var ae *APIError
			if !errors.As(err, &ae) || ae.Status != tt.want {
				t.Fatalf("got %v, want status %d", err, tt.want)
			}
		})
	}
}

func TestPresignedRequestRejected(t *testing.T) {
	s := &Server{
		log:     testutil.DiscardLogger(),
		metrics: telemetry.NewRequestMetrics(),
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

type fakeReadBackend struct {
	head readHead
	res  *store.ReadResult
	rows []store.Listed
}

func (f fakeReadBackend) Head(context.Context, string, string) (readHead, error) {
	return f.head, nil
}

func (f fakeReadBackend) Open(_ context.Context, _, _ string, rng store.ByteRange) (*store.ReadResult, error) {
	res := *f.res
	res.Ranged = rng.Requested
	return &res, nil
}

func (f fakeReadBackend) List(context.Context, string, string, string, int) ([]store.Listed, error) {
	return f.rows, nil
}

func callHandler(t *testing.T, s *Server, rec *httptest.ResponseRecorder, req *http.Request, fn func() error) {
	t.Helper()
	if err := fn(); err != nil {
		s.fail(rec, req, err)
	}
}

func testReadServer(backend readBackend) *Server {
	return &Server{
		log:          testutil.DiscardLogger(),
		readOverride: backend,
	}
}

func TestReadBackendHeadAndGet(t *testing.T) {
	body := "hello"
	backend := fakeReadBackend{
		head: readHead{Info: store.ObjectInfo{Size: int64(len(body)), ETag: "etag"}},
		res: &store.ReadResult{
			Size: int64(len(body)), ETag: "etag",
			Body:  io.NopCloser(strings.NewReader(body)),
			Start: 0,
			End:   int64(len(body) - 1),
		},
	}
	s := testReadServer(backend)

	head := httptest.NewRequest("HEAD", "/bucket/key", nil)
	headRec := httptest.NewRecorder()
	s.headObject(headRec, head, "bucket", "key")
	if headRec.Code != 200 || headRec.Header().Get("ETag") != `"etag"` || headRec.Header().Get("Content-Length") != "5" {
		t.Fatalf("HEAD status=%d headers=%v", headRec.Code, headRec.Header())
	}

	get := httptest.NewRequest("GET", "/bucket/key", nil)
	getRec := httptest.NewRecorder()
	s.getObject(getRec, get, "bucket", "key")
	if getRec.Code != 200 || getRec.Body.String() != body {
		t.Fatalf("GET status=%d body=%q", getRec.Code, getRec.Body.String())
	}
	if getRec.Header().Get("Content-Length") != "5" {
		t.Fatalf("GET Content-Length = %q", getRec.Header().Get("Content-Length"))
	}
}

func TestReadBackendExplicitFullRange(t *testing.T) {
	body := "hello"
	s := testReadServer(fakeReadBackend{
		head: readHead{Info: store.ObjectInfo{Size: int64(len(body)), ETag: "etag"}},
		res: &store.ReadResult{
			Size: int64(len(body)), ETag: "etag",
			Body:  io.NopCloser(strings.NewReader(body)),
			Start: 0,
			End:   int64(len(body) - 1),
		},
	})
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("Range", "bytes=0-")
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.getObject(rec, req, "bucket", "key") })
	if rec.Code != 206 {
		t.Fatalf("status = %d, want 206", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes 0-4/5" {
		t.Fatalf("Content-Range = %q", got)
	}
}

func TestReadBackendList(t *testing.T) {
	s := testReadServer(fakeReadBackend{
		rows: []store.Listed{{Key: "a", Size: 1, ETag: "ea"}, {Key: "b", Size: 2, ETag: "eb"}},
	})
	req := httptest.NewRequest("GET", "/bucket?list-type=2&prefix=", nil)
	rec := httptest.NewRecorder()
	s.listObjectsV2(rec, req, "bucket", req.URL.Query())
	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "<Key>a</Key>") || !strings.Contains(body, "<Key>b</Key>") {
		t.Fatalf("list body = %s", body)
	}
}

func TestReadBackendConditionalHead(t *testing.T) {
	s := testReadServer(fakeReadBackend{head: readHead{Info: store.ObjectInfo{Size: 5, ETag: "etag"}}})
	req := httptest.NewRequest("HEAD", "/bucket/key", nil)
	req.Header.Set("If-None-Match", `"etag"`)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.headObject(rec, req, "bucket", "key") })
	if rec.Code != 304 {
		t.Fatalf("status = %d, want 304", rec.Code)
	}
}

func TestReadBackendConditionalGet(t *testing.T) {
	body := "hello"
	backend := fakeReadBackend{
		head: readHead{Info: store.ObjectInfo{Size: int64(len(body)), ETag: "etag"}},
		res: &store.ReadResult{
			Size: int64(len(body)), ETag: "etag",
			Body:  io.NopCloser(strings.NewReader(body)),
			Start: 0,
			End:   int64(len(body) - 1),
		},
	}
	s := testReadServer(backend)

	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("If-None-Match", `"etag"`)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.getObject(rec, req, "bucket", "key") })
	if rec.Code != http.StatusNotModified {
		t.Fatalf("If-None-Match status = %d, want 304", rec.Code)
	}

	req = httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("If-Match", `"other"`)
	rec = httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.getObject(rec, req, "bucket", "key") })
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("If-Match status = %d, want 412", rec.Code)
	}
}

func TestReadBackendRangeOnEmptyObject(t *testing.T) {
	s := testReadServer(fakeReadBackend{
		head: readHead{Info: store.ObjectInfo{Size: 0, ETag: "etag"}},
		res: &store.ReadResult{
			Size: 0, ETag: "etag",
			Body: io.NopCloser(strings.NewReader("")),
		},
	})
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Header.Set("Range", "bytes=0-")
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.getObject(rec, req, "bucket", "key") })
	if rec.Code != 416 {
		t.Fatalf("status = %d, want 416", rec.Code)
	}
}

type fakeWriteBackend struct {
	putETag  string
	stored   []byte
	copied   *store.Meta
	copyOpts store.CopyOptions
	deleted  []string
	err      error
}

func (f *fakeWriteBackend) Put(_ context.Context, _, _ string, body io.Reader, size int64, _ store.WriteOptions, validate func() ([]store.Checksum, error)) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	inline, err := readInlineBody(size, body, store.MaxInlineBytes)
	if err != nil {
		return "", err
	}
	defer inline.release()
	if validate != nil {
		if _, err := validate(); err != nil {
			return "", err
		}
	}
	f.stored = bytes.Clone(inline.data)
	return f.putETag, nil
}

func (f *fakeWriteBackend) Copy(_ context.Context, _, _, _, _ string, opts store.CopyOptions) (*store.Meta, error) {
	f.copyOpts = opts
	return f.copied, f.err
}

func (f *fakeWriteBackend) Delete(_ context.Context, _, key string) error {
	f.deleted = append(f.deleted, key)
	return f.err
}

func (f *fakeWriteBackend) DeleteMany(_ context.Context, _ string, keys []string) ([]deleteOutcome, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]deleteOutcome, 0, len(keys))
	for _, key := range keys {
		out = append(out, deleteOutcome{Key: key})
	}
	f.deleted = append(f.deleted, keys...)
	return out, nil
}

func testWriteServer(backend *fakeWriteBackend) *Server {
	return &Server{
		log:           testutil.DiscardLogger(),
		metrics:       telemetry.NewRequestMetrics(),
		writeOverride: backend,
	}
}

func TestWriteBackendPut(t *testing.T) {
	s := testWriteServer(&fakeWriteBackend{putETag: "etag"})
	req := httptest.NewRequest("PUT", "/bucket/key", strings.NewReader("hello"))
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.putObject(rec, req, "bucket", "key") })
	if rec.Code != 200 || rec.Header().Get("ETag") != `"etag"` {
		t.Fatalf("status=%d etag=%q", rec.Code, rec.Header().Get("ETag"))
	}
}

func TestWriteBackendDelete(t *testing.T) {
	backend := &fakeWriteBackend{}
	s := testWriteServer(backend)
	req := httptest.NewRequest("DELETE", "/bucket/key", nil)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.deleteObject(rec, req, "bucket", "key") })
	if rec.Code != 204 || len(backend.deleted) != 1 || backend.deleted[0] != "key" {
		t.Fatalf("status=%d deleted=%v", rec.Code, backend.deleted)
	}
}

func TestWriteBackendDeleteError(t *testing.T) {
	s := testWriteServer(&fakeWriteBackend{err: errors.New("boom")})
	req := httptest.NewRequest("DELETE", "/bucket/key", nil)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.deleteObject(rec, req, "bucket", "key") })
	if rec.Code != 500 {
		t.Fatalf("status=%d, want 500", rec.Code)
	}
}

func TestWriteBackendCopy(t *testing.T) {
	backend := &fakeWriteBackend{copied: &store.Meta{ETag: "copy-etag"}}
	s := testWriteServer(backend)
	req := httptest.NewRequest("PUT", "/bucket/dst", nil)
	req.Header.Set("x-amz-copy-source", "/bucket/src")
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.copyObject(rec, req, "bucket", "dst") })
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "copy-etag") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestWriteBackendCopyConditions(t *testing.T) {
	backend := &fakeWriteBackend{copied: &store.Meta{ETag: "copy-etag"}}
	s := testWriteServer(backend)
	req := httptest.NewRequest("PUT", "/bucket/dst", nil)
	req.Header.Set("x-amz-copy-source", "/bucket/src")
	req.Header.Set("x-amz-copy-source-if-match", `"source-etag"`)
	req.Header.Set("x-amz-copy-source-if-none-match", `"other"`)
	req.Header.Set("If-Match", `"00000000000000000000000000000001"`)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.copyObject(rec, req, "bucket", "dst") })
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := backend.copyOpts.Source, (store.ReadCondition{
		IfMatch: `"source-etag"`, IfNoneMatch: `"other"`,
	}); got != want {
		t.Fatalf("source condition = %+v, want %+v", got, want)
	}
	if got, want := backend.copyOpts.Dest, (store.Precondition{
		Mode: store.IfMatch, ETag: "00000000000000000000000000000001",
	}); got != want {
		t.Fatalf("dest condition = %+v, want %+v", got, want)
	}

	req = httptest.NewRequest("PUT", "/bucket/dst", nil)
	req.Header.Set("x-amz-copy-source", "/bucket/src")
	req.Header.Set("If-None-Match", "*")
	rec = httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.copyObject(rec, req, "bucket", "dst") })
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got, want := backend.copyOpts.Dest, (store.Precondition{Mode: store.IfAbsent}); got != want {
		t.Fatalf("dest condition = %+v, want %+v", got, want)
	}
}

func TestWriteBackendCopyRejectsUnsupportedDestinationCondition(t *testing.T) {
	backend := &fakeWriteBackend{copied: &store.Meta{ETag: "copy-etag"}}
	s := testWriteServer(backend)
	req := httptest.NewRequest("PUT", "/bucket/dst", nil)
	req.Header.Set("x-amz-copy-source", "/bucket/src")
	req.Header.Set("If-None-Match", `"dest-etag"`)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.copyObject(rec, req, "bucket", "dst") })
	if rec.Code != 400 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

type fakeMultipartBackend struct {
	completedOpts store.WriteOptions
	completed     *store.Completed
	parts         []store.UploadPartInfo
	uploads       []store.UploadInfo
	aborted       bool
	startedPart   bool
}

func (f *fakeMultipartBackend) CreateUpload(context.Context, string, string, []string, string, string, *string) (string, error) {
	return "upload-1", nil
}

func (f *fakeMultipartBackend) StartPart(context.Context, string, string, string, int32, int64) (*store.ChunkWriter, error) {
	f.startedPart = true
	return nil, errors.New("unexpected StartPart")
}

func (f *fakeMultipartBackend) ListParts(context.Context, string, string, string, int, int32) ([]store.UploadPartInfo, bool, int32, error) {
	return f.parts, false, 1, nil
}

func (f *fakeMultipartBackend) CompleteUpload(_ context.Context, _, _, _ string, _ []store.ListedPart, opts store.WriteOptions) (*store.Completed, error) {
	f.completedOpts = opts
	return f.completed, nil
}

func (f *fakeMultipartBackend) AbortUpload(context.Context, string, string, string) (bool, error) {
	f.aborted = true
	return true, nil
}

func (f *fakeMultipartBackend) ListUploads(context.Context, string, string, string, string, string, int) ([]store.UploadInfo, bool, error) {
	return f.uploads, false, nil
}

func testMultipartServer(backend *fakeMultipartBackend) *Server {
	return &Server{
		log:               testutil.DiscardLogger(),
		metrics:           telemetry.NewRequestMetrics(),
		multipartOverride: backend,
	}
}

func TestMultipartCompleteCarriesPrecondition(t *testing.T) {
	backend := &fakeMultipartBackend{
		completed: &store.Completed{Bucket: "bucket", Key: "key", ETag: "result"},
	}
	s := testMultipartServer(backend)
	body := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"part"</ETag></Part></CompleteMultipartUpload>`
	req := httptest.NewRequest("POST", "/bucket/key?uploadId=upload-1", strings.NewReader(body))
	req.Header.Set("If-Match", `"`+strings.Repeat("a", 32)+`"`)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.completeMultipartUpload(rec, req, "bucket", "key", req.URL.Query()) })
	if rec.Code != 200 {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if backend.completedOpts.Precondition.Mode != store.IfMatch || backend.completedOpts.Precondition.ETag != strings.Repeat("a", 32) {
		t.Fatalf("precondition = %+v", backend.completedOpts.Precondition)
	}
}

func TestMultipartListAndAbortUseBackend(t *testing.T) {
	backend := &fakeMultipartBackend{
		parts:   []store.UploadPartInfo{{Number: 1, Size: 5, ETag: "etag"}},
		uploads: []store.UploadInfo{{Key: "key", UploadID: "upload-1"}},
	}
	s := testMultipartServer(backend)
	req := httptest.NewRequest("GET", "/bucket/key?uploadId=upload-1", nil)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.listParts(rec, req, "bucket", "key", req.URL.Query()) })
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<PartNumber>1</PartNumber>") {
		t.Fatalf("list parts status=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest("DELETE", "/bucket/key?uploadId=upload-1", nil)
	rec = httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.abortMultipartUpload(rec, req, "bucket", "key", req.URL.Query()) })
	if rec.Code != 204 || !backend.aborted {
		t.Fatalf("abort status=%d aborted=%v", rec.Code, backend.aborted)
	}
}

func TestListMultipartUploadsPageMarkerDoesNotSkipItem(t *testing.T) {
	backend := &fakeMultipartBackend{uploads: []store.UploadInfo{
		{Key: "key-1", UploadID: "upload-1"},
		{Key: "key-2", UploadID: "upload-2"},
	}}
	s := testMultipartServer(backend)
	req := httptest.NewRequest("GET", "/bucket?uploads&max-uploads=1", nil)
	rec := httptest.NewRecorder()
	callHandler(t, s, rec, req, func() error { return s.listMultipartUploads(rec, req, "bucket", req.URL.Query()) })
	if rec.Code != 200 {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var out listMultipartUploadsResult
	if err := xml.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.IsTruncated || len(out.Uploads) != 1 || out.Uploads[0].Key != "key-1" {
		t.Fatalf("page = %+v", out)
	}
	if out.NextKeyMarker != "key-1" || out.NextUploadIDMarker != "upload-1" {
		t.Fatalf("next marker = %q/%q, want key-1/upload-1", out.NextKeyMarker, out.NextUploadIDMarker)
	}
}

func TestParseListRequest(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    listRequest
		wantErr string
	}{
		{name: "defaults", want: listRequest{maxKeys: 1000}},
		{
			name: "filters and page size",
			raw:  "prefix=p%2F&delimiter=%2F&max-keys=7&start-after=p%2Fa",
			want: listRequest{prefix: "p/", delimiter: "/", maxKeys: 7, startAfter: "p/a", after: "p/a"},
		},
		{
			name: "max keys zero",
			raw:  "max-keys=0",
			want: listRequest{maxKeys: 0},
		},
		{
			name: "max keys capped",
			raw:  "max-keys=5000",
			want: listRequest{maxKeys: 1000},
		},
		{name: "negative max keys", raw: "max-keys=-1", wantErr: "invalid max-keys"},
		{name: "non-numeric max keys", raw: "max-keys=ten", wantErr: "invalid max-keys"},
		{
			name: "continuation token wins",
			raw:  "prefix=p&start-after=p%2Fstart&continuation-token=p%2Fnext",
			want: listRequest{
				prefix: "p", maxKeys: 1000, startAfter: "p/start",
				continuationToken: "p/next", after: "p/next",
			},
		},
		{
			name: "empty token falls back to start after",
			raw:  "continuation-token=&start-after=p%2Fstart",
			want: listRequest{maxKeys: 1000, startAfter: "p/start", after: "p/start"},
		},
		{
			name: "encoding type url",
			raw:  "encoding-type=url",
			want: listRequest{maxKeys: 1000, encodeURL: true},
		},
		{name: "unsupported encoding type", raw: "encoding-type=base64", wantErr: "unsupported encoding-type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := url.ParseQuery(tt.raw)
			if err != nil {
				t.Fatalf("parse query: %v", err)
			}
			got, err := parseListRequest(q)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("parseListRequest() error = nil, want %q", tt.wantErr)
				}
				var ae *APIError
				if !errors.As(err, &ae) || ae.Code != "InvalidRequest" || ae.Message != tt.wantErr {
					t.Fatalf("parseListRequest() error = %v, want InvalidRequest %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseListRequest() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseListRequest() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

var testListTime = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

func listed(key string) store.Listed {
	return store.Listed{Key: key, Size: int64(len(key)), ETag: "etag-" + key, CreatedAt: testListTime}
}

func listedRows(rows ...store.Listed) iter.Seq2[store.Listed, error] {
	return func(yield func(store.Listed, error) bool) {
		for _, row := range rows {
			if !yield(row, nil) {
				return
			}
		}
	}
}

func listEntryFor(key string) listEntry {
	return listEntry{key: key, lastModified: testListTime, etag: "etag-" + key, size: int64(len(key))}
}

func TestFoldList(t *testing.T) {
	tests := []struct {
		name string
		req  listRequest
		rows []store.Listed
		want listPage
	}{
		{
			name: "empty",
			req:  listRequest{maxKeys: 1000},
			want: listPage{},
		},
		{
			name: "contents",
			req:  listRequest{maxKeys: 1000},
			rows: []store.Listed{listed("a"), listed("b")},
			want: listPage{contents: []listEntry{listEntryFor("a"), listEntryFor("b")}},
		},
		{
			name: "truncates at max keys",
			req:  listRequest{maxKeys: 2},
			rows: []store.Listed{listed("a"), listed("b"), listed("c")},
			want: listPage{
				contents:  []listEntry{listEntryFor("a"), listEntryFor("b")},
				truncated: true, nextToken: "b",
			},
		},
		{
			name: "max keys zero never reads",
			req:  listRequest{maxKeys: 0},
			rows: []store.Listed{listed("a")},
			want: listPage{},
		},
		{
			name: "delimiter rolls up prefixes",
			req:  listRequest{delimiter: "/", maxKeys: 1000},
			rows: []store.Listed{listed("a/1"), listed("a/2"), listed("b"), listed("c/1")},
			want: listPage{
				contents:       []listEntry{listEntryFor("b")},
				commonPrefixes: []string{"a/", "c/"},
			},
		},
		{
			name: "virtual prefixes count toward max keys",
			req:  listRequest{delimiter: "/", maxKeys: 2},
			rows: []store.Listed{listed("a/1"), listed("b"), listed("c/1")},
			want: listPage{
				contents:       []listEntry{listEntryFor("b")},
				commonPrefixes: []string{"a/"},
				truncated:      true, nextToken: "b",
			},
		},
		{
			name: "continuation resumes inside virtual prefix",
			req:  listRequest{delimiter: "/", after: "a/x", maxKeys: 1000},
			rows: []store.Listed{listed("a/y"), listed("b")},
			want: listPage{contents: []listEntry{listEntryFor("b")}},
		},
		{
			name: "prefix confines roll up",
			req:  listRequest{prefix: "p/", delimiter: "/", maxKeys: 1000},
			rows: []store.Listed{listed("p/a"), listed("p/dir/x"), listed("p/z")},
			want: listPage{
				contents:       []listEntry{listEntryFor("p/a"), listEntryFor("p/z")},
				commonPrefixes: []string{"p/dir/"},
			},
		},
		{
			name: "duplicate virtual prefix does not consume max keys",
			req:  listRequest{delimiter: "/", after: "a/x", maxKeys: 1},
			rows: []store.Listed{listed("a/y"), listed("b"), listed("c")},
			want: listPage{
				contents:  []listEntry{listEntryFor("b")},
				truncated: true, nextToken: "b",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := foldList(tt.req, listedRows(tt.rows...))
			if err != nil {
				t.Fatalf("foldList() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("foldList() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestFoldListPropagatesSourceError(t *testing.T) {
	wantErr := errors.New("listing failed")
	rows := func(yield func(store.Listed, error) bool) {
		if yield(listed("a"), nil) {
			yield(store.Listed{}, wantErr)
		}
	}
	_, err := foldList(listRequest{maxKeys: 10}, rows)
	if !errors.Is(err, wantErr) {
		t.Fatalf("foldList() error = %v, want %v", err, wantErr)
	}
}

func listTestPage(prefix string, n int) []store.Listed {
	out := make([]store.Listed, n)
	for i := range out {
		out[i] = listed(prefix + fmt.Sprintf("%04d", i))
	}
	return out
}

func TestIterListedPages(t *testing.T) {
	tests := []struct {
		name        string
		pages       [][]store.Listed
		errAt       int
		wantKeys    int
		wantCalls   int
		wantCursors []string
		wantErr     bool
	}{
		{
			name:        "empty",
			wantCalls:   1,
			wantCursors: []string{""},
		},
		{
			name:        "short page",
			pages:       [][]store.Listed{{listed("a"), listed("b")}},
			wantKeys:    2,
			wantCalls:   1,
			wantCursors: []string{""},
		},
		{
			name:        "full page then short page",
			pages:       [][]store.Listed{listTestPage("a", listPageSize), listTestPage("b", 2)},
			wantKeys:    listPageSize + 2,
			wantCalls:   2,
			wantCursors: []string{"", "a1023"},
		},
		{
			name:        "error on first page",
			errAt:       0,
			wantCalls:   1,
			wantCursors: []string{""},
			wantErr:     true,
		},
		{
			name:        "error on second page",
			pages:       [][]store.Listed{listTestPage("a", listPageSize)},
			errAt:       1,
			wantKeys:    listPageSize,
			wantCalls:   2,
			wantCursors: []string{"", "a1023"},
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cursors []string
			calls := 0
			source := func(after string, _ int) ([]store.Listed, error) {
				cursors = append(cursors, after)
				idx := calls
				calls++
				if tt.wantErr && idx == tt.errAt {
					return nil, errors.New("origin unavailable")
				}
				if idx >= len(tt.pages) {
					return nil, nil
				}
				return tt.pages[idx], nil
			}
			var got []store.Listed
			var gotErr error
			for row, err := range iterListed(source, "") {
				if err != nil {
					gotErr = err
					break
				}
				got = append(got, row)
			}
			if (gotErr != nil) != tt.wantErr {
				t.Fatalf("iterListed() error = %v, wantErr %v", gotErr, tt.wantErr)
			}
			if len(got) != tt.wantKeys {
				t.Fatalf("iterListed() yielded %d keys, want %d", len(got), tt.wantKeys)
			}
			if calls != tt.wantCalls {
				t.Fatalf("iterListed() fetched %d pages, want %d", calls, tt.wantCalls)
			}
			if !reflect.DeepEqual(cursors, tt.wantCursors) {
				t.Fatalf("iterListed() cursors = %v, want %v", cursors, tt.wantCursors)
			}
		})
	}
}

func TestFoldListStopsFetchingAtLimit(t *testing.T) {
	calls := 0
	source := func(string, int) ([]store.Listed, error) {
		calls++
		return listTestPage("a", listPageSize), nil
	}
	page, err := foldList(listRequest{maxKeys: 1}, iterListed(source, ""))
	if err != nil {
		t.Fatalf("foldList() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("foldList() fetched %d pages, want 1", calls)
	}
	if !page.truncated || page.nextToken != "a0000" || len(page.contents) != 1 {
		t.Fatalf("foldList() page = %+v", page)
	}
}

func TestListView(t *testing.T) {
	epage := listPage{
		contents:       []listEntry{listEntryFor("p a/b&c")},
		commonPrefixes: []string{"p a/dir/"},
		truncated:      true,
		nextToken:      "p a/b&c",
	}
	tests := []struct {
		name   string
		bucket string
		req    listRequest
		page   listPage
		want   listBucketResult
	}{
		{
			name:   "plain",
			bucket: "bucket",
			req: listRequest{
				prefix: "p/", delimiter: "/", maxKeys: 2,
				startAfter: "p/a", continuationToken: "p/a", after: "p/a",
			},
			page: listPage{
				contents:       []listEntry{listEntryFor("p/b")},
				commonPrefixes: []string{"p/dir/"},
				truncated:      true,
				nextToken:      "p/b",
			},
			want: listBucketResult{
				Xmlns: s3Namespace, Name: "bucket", Prefix: "p/", KeyCount: 2, MaxKeys: 2,
				Delimiter: "/", IsTruncated: true,
				Contents:              []listObjectXML{{Key: "p/b", LastModified: testListTime, ETag: "etag-p/b", Size: 3}},
				CommonPrefixes:        []commonPrefixXML{{Prefix: "p/dir/"}},
				NextContinuationToken: "p/b", ContinuationToken: "p/a", StartAfter: "p/a",
			},
		},
		{
			name:   "url encoded",
			bucket: "bucket",
			req: listRequest{
				prefix: "p a/", delimiter: "/", maxKeys: 10, startAfter: "p a/x",
				continuationToken: "p a/x", after: "p a/x", encodeURL: true,
			},
			page: epage,
			want: listBucketResult{
				Xmlns: s3Namespace, Name: "bucket", Prefix: "p%20a%2F", KeyCount: 2, MaxKeys: 10,
				Delimiter: "%2F", IsTruncated: true,
				Contents:              []listObjectXML{{Key: "p%20a%2Fb%26c", LastModified: testListTime, ETag: "etag-p a/b&c", Size: 7}},
				CommonPrefixes:        []commonPrefixXML{{Prefix: "p%20a%2Fdir%2F"}},
				NextContinuationToken: "p a/b&c", ContinuationToken: "p a/x", StartAfter: "p%20a%2Fx",
				EncodingType: "url",
			},
		},
		{
			name:   "untruncated omits next token",
			bucket: "bucket",
			req:    listRequest{maxKeys: 1000},
			page:   listPage{contents: []listEntry{listEntryFor("a")}},
			want: listBucketResult{
				Xmlns: s3Namespace, Name: "bucket", KeyCount: 1, MaxKeys: 1000,
				Contents: []listObjectXML{{Key: "a", LastModified: testListTime, ETag: "etag-a", Size: 1}},
			},
		},
		{
			name:   "zero max keys",
			bucket: "bucket",
			req:    listRequest{maxKeys: 0},
			want:   listBucketResult{Xmlns: s3Namespace, Name: "bucket", MaxKeys: 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.req.listView(tt.bucket, tt.page)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("listView() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWriteListObjectsV2XML(t *testing.T) {
	when := time.Date(2026, 10, 23, 14, 2, 3, 456000000, time.FixedZone("x", -8*3600))
	out := listBucketResult{
		Name: "bucket", Prefix: "pre", KeyCount: 2, MaxKeys: 10,
		Delimiter: "/", IsTruncated: true,
		Contents: []listObjectXML{
			{Key: "pre/a&<b", LastModified: when, ETag: `"abc"`, Size: 7},
			{Key: "pre/c", LastModified: when, ETag: "def", Size: 8},
		},
		CommonPrefixes:        []commonPrefixXML{{Prefix: "pre/dir/"}},
		NextContinuationToken: "next-token",
		ContinuationToken:     "prev-token",
		StartAfter:            "start",
		EncodingType:          "url",
	}
	rec := httptest.NewRecorder()
	writeListObjectsV2XML(rec, 200, out)

	var doc struct {
		XMLName   xml.Name `xml:"ListBucketResult"`
		Xmlns     string   `xml:"xmlns,attr"`
		Name      string   `xml:"Name"`
		Prefix    string   `xml:"Prefix"`
		KeyCount  int      `xml:"KeyCount"`
		MaxKeys   int      `xml:"MaxKeys"`
		Delimiter string   `xml:"Delimiter"`
		Truncated bool     `xml:"IsTruncated"`
		Contents  []struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			StorageClass string `xml:"StorageClass"`
		} `xml:"Contents"`
		CommonPrefixes []struct {
			Prefix string `xml:"Prefix"`
		} `xml:"CommonPrefixes"`
		Next       string `xml:"NextContinuationToken"`
		Prev       string `xml:"ContinuationToken"`
		StartAfter string `xml:"StartAfter"`
		Encoding   string `xml:"EncodingType"`
	}
	if err := xml.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, rec.Body.String())
	}
	if doc.Xmlns != s3Namespace || doc.Name != "bucket" || doc.Prefix != "pre" || doc.KeyCount != 2 || doc.MaxKeys != 10 || doc.Delimiter != "/" || !doc.Truncated {
		t.Fatalf("header mismatch: %+v", doc)
	}
	if len(doc.Contents) != 2 || doc.Contents[0].Key != "pre/a&<b" || doc.Contents[0].ETag != `"abc"` || doc.Contents[0].Size != 7 || doc.Contents[0].StorageClass != "STANDARD" {
		t.Fatalf("contents mismatch: %+v", doc.Contents)
	}
	if doc.Contents[0].LastModified != "2026-10-23T22:02:03.456Z" {
		t.Fatalf("time = %q", doc.Contents[0].LastModified)
	}
	if len(doc.CommonPrefixes) != 1 || doc.CommonPrefixes[0].Prefix != "pre/dir/" {
		t.Fatalf("common prefixes = %+v", doc.CommonPrefixes)
	}
	if doc.Next != "next-token" || doc.Prev != "prev-token" || doc.StartAfter != "start" || doc.Encoding != "url" {
		t.Fatalf("tokens = %+v", doc)
	}
}

func TestPartCopyReader(t *testing.T) {
	body := strings.NewReader("abc")
	src, hasher, err := partCopyReader(body, "")
	if err != nil || hasher != nil || src != body {
		t.Fatalf("no algorithm: src=%v hasher=%v err=%v", src, hasher, err)
	}
	if checks := partCopyChecksums("", nil); checks != nil {
		t.Fatalf("nil hasher produced %+v", checks)
	}

	src, hasher, err = partCopyReader(body, "SHA256")
	if err != nil || hasher == nil {
		t.Fatalf("SHA256 setup: hasher=%v err=%v", hasher, err)
	}
	if _, err := io.Copy(io.Discard, src); err != nil {
		t.Fatal(err)
	}
	const want = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0="
	checks := partCopyChecksums("SHA256", hasher)
	if len(checks) != 1 || checks[0].Name != "x-amz-checksum-sha256" || checks[0].Value != want {
		t.Fatalf("checksum = %+v", checks)
	}

	if _, _, err := partCopyReader(body, "nope"); err == nil {
		t.Fatal("unsupported algorithm accepted")
	} else {
		var ae *APIError
		if !errors.As(err, &ae) || ae.Code != "NotImplemented" || ae.Status != 501 {
			t.Fatalf("unsupported algorithm error = %v", err)
		}
	}
}

func TestReadInlineBody(t *testing.T) {
	payload := bytes.Repeat([]byte("inline-put-"), 512)
	r := &http.Request{ContentLength: int64(len(payload))}
	inline, err := readInlineBody(r.ContentLength, bytes.NewReader(payload), store.MaxInlineBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(inline.data, payload) {
		t.Fatalf("body = %q, want %q", inline.data, payload)
	}
	if inline.pooled == nil {
		t.Fatal("known Content-Length did not use the pooled buffer")
	}
	inline.release()
	if inline.data != nil || inline.pooled != nil {
		t.Fatal("release did not clear the inline body")
	}

	unknown := &http.Request{ContentLength: -1}
	inline, err = readInlineBody(unknown.ContentLength, bytes.NewReader(payload), store.MaxInlineBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(inline.data, payload) || inline.pooled != nil {
		t.Fatalf("unknown-length body = %q pooled=%v", inline.data, inline.pooled != nil)
	}

	short := &http.Request{ContentLength: int64(len(payload) + 1)}
	if _, err := readInlineBody(short.ContentLength, bytes.NewReader(payload), store.MaxInlineBytes); err == nil {
		t.Fatal("short body accepted")
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

func TestChunkedPutSizedByDecodedLength(t *testing.T) {
	sig := strings.Repeat("a", 64)
	body := signedChunk("5", "hello", sig) + signedChunk("6", " world", sig) +
		"0;chunk-signature=" + sig + "\r\nx-amz-checksum-crc32: DUoRhQ==\r\n\r\n"
	req := chunkedRequest(body)
	req.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-decoded-content-length", "11")
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")
	backend := &fakeWriteBackend{putETag: "etag"}
	s := testWriteServer(backend)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT status=%d body=%s", rec.Code, rec.Body.String())
	}
	s.readOverride = fakeReadBackend{res: &store.ReadResult{
		Size: 11, ETag: "etag",
		Body: io.NopCloser(bytes.NewReader(backend.stored)),
		End:  10,
	}}
	get := httptest.NewRecorder()
	s.ServeHTTP(get, httptest.NewRequest("GET", "/bucket/key", nil))
	if get.Code != http.StatusOK || get.Body.String() != "hello world" {
		t.Fatalf("GET status=%d body=%q", get.Code, get.Body.String())
	}
}

func TestChunkedUploadPartNeedsDecodedLength(t *testing.T) {
	backend := &fakeMultipartBackend{}
	s := testMultipartServer(backend)
	req := chunkedRequest("5\r\nhello\r\n0\r\n\r\n")
	req.URL.RawQuery = "partNumber=1&uploadId=upload-1"
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || backend.startedPart {
		t.Fatalf("status=%d startedPart=%v body=%s", rec.Code, backend.startedPart, rec.Body.String())
	}
}

func TestUploadBodyContract(t *testing.T) {
	for _, tc := range []struct {
		name, payload, decoded, checksum string
		streaming, unknown, rawHTTP      bool
		size, expected                   int64
		wantError                        string
	}{
		{name: "plain", payload: "hello", size: 5, expected: 5},
		{name: "unknown length", payload: "hello", unknown: true, size: -1, expected: -1},
		{name: "late HTTP trailer", rawHTTP: true, size: -1, expected: -1,
			payload: "PUT /bucket/key HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nx-amz-trailer: x-amz-checksum-crc32\r\n\r\n5\r\nhello\r\n0\r\nx-amz-checksum-crc32: NhCmhg==\r\n\r\n"},
		{name: "plain multipart hint", payload: "hello", decoded: "7", size: 5, expected: 7},
		{name: "streamed", payload: "5\r\nhello\r\n0\r\n\r\n", streaming: true, decoded: "5", size: 5, expected: 5},
		{name: "empty stream", payload: "0\r\n\r\n", streaming: true, decoded: "0"},
		{name: "missing length", payload: "0\r\n\r\n", streaming: true, wantError: "InvalidRequest"},
		{name: "bad length", payload: "0\r\n\r\n", streaming: true, decoded: "-1", wantError: "InvalidRequest"},
		{name: "short stream", payload: "5\r\nhe", streaming: true, decoded: "5", wantError: "IncompleteBody"},
		{name: "bad checksum", payload: "hello", checksum: "invalid", size: 5, expected: 5, wantError: "BadDigest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "/bucket/key", strings.NewReader(tc.payload))
			if tc.rawHTTP {
				var err error
				r, err = http.ReadRequest(bufio.NewReader(strings.NewReader(tc.payload)))
				testutil.Must(t, err)
			}
			if tc.streaming {
				r.Header.Set("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			}
			if tc.decoded != "" {
				r.Header.Set("x-amz-decoded-content-length", tc.decoded)
			}
			if tc.checksum != "" {
				r.Header.Set("Content-MD5", tc.checksum)
			}
			if tc.unknown {
				r.ContentLength = -1
			}
			err := prepareBody(r)
			r = r.WithContext(r.Context())
			var body uploadBody
			if err == nil {
				body, err = prepareUploadBody(r)
			}
			if err == nil {
				_, err = io.Copy(io.Discard, body.reader)
				if err == nil {
					_, err = body.verify()
				}
			}
			if tc.wantError != "" {
				var ae *APIError
				if !errors.As(err, &ae) || ae.Code != tc.wantError {
					t.Fatalf("error=%v want=%s", err, tc.wantError)
				}
			} else if err != nil || body.size != tc.size || body.expectedSize != tc.expected {
				t.Fatalf("size=%d expected=%d err=%v", body.size, body.expectedSize, err)
			}
		})
	}
}

func TestCRC64NVMEVector(t *testing.T) {
	c := newCRC64NVME()
	_, _ = c.Write([]byte("123456789"))
	if got, want := c.Sum64(), uint64(0xAE8B14860A799888); got != want {
		t.Fatalf("crc64nvme = %#x, want %#x", got, want)
	}
}

func TestMetricsObserveAndWrite(t *testing.T) {
	m := telemetry.NewRequestMetrics()
	m.Observe("GET", 200, 3*time.Millisecond, 10)
	m.Observe("GET", 200, 7*time.Millisecond, 20)
	m.Observe("PUT", 403, 25*time.Millisecond, 0)

	rr := httptest.NewRecorder()
	s := New(nil, nil, false, 1, nil)
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
		"pgvs3_response_bytes_total 30",
		`pgvs3_request_duration_seconds_bucket{le="0.005"} 1`,
		`pgvs3_request_duration_seconds_bucket{le="0.01"} 2`,
		`pgvs3_request_duration_seconds_bucket{le="+Inf"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics body missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsConcurrentObserve(t *testing.T) {
	m := telemetry.NewRequestMetrics()
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			m.Observe("GET", 200, time.Millisecond, 1)
		})
	}
	wg.Wait()
	rr := httptest.NewRecorder()
	s := New(nil, nil, false, 1, nil)
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
