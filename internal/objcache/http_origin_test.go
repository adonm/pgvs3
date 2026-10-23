package objcache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPOriginReadAndRange(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		switch {
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Type", "application/vnd.apache.parquet")
			w.Header().Set("ETag", `"abc"`)
			w.Header().Set("Content-Length", "11")
		case r.Method == http.MethodGet && r.Header.Get("Range") == "bytes=2-5":
			w.Header().Set("Content-Range", "bytes 2-5/11")
			w.Header().Set("Content-Type", "application/vnd.apache.parquet")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, "2345")
		case r.Method == http.MethodGet:
			_, _ = io.WriteString(w, "hello world")
		case r.Method == http.MethodPut:
			_, _ = io.WriteString(w, "ok")
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	b, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{
		"base_url":     srv.URL,
		"strip_bucket": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := b.Attributes(context.Background(), "lake/dir/data.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if a.Size != 11 || a.ETag != "abc" || a.ContentType != "application/vnd.apache.parquet" {
		t.Fatalf("attrs = %#v", a)
	}
	if gotPath != "/dir/data.parquet" {
		t.Fatalf("path = %q", gotPath)
	}
	rc, err := b.GetRange(context.Background(), "lake/dir/data.parquet", 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "2345" {
		t.Fatalf("range = %q", data)
	}
	if err := b.Upload(context.Background(), "lake/x", strings.NewReader("x"), 1); err == nil {
		t.Fatal("read-only HTTP origin accepted PUT")
	}
}

func TestHTTPOriginNotFound(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	b, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{"base_url": srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.Attributes(context.Background(), "bucket/missing")
	if !b.IsObjNotFoundErr(err) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestRangeChunkKey(t *testing.T) {
	if got := rangeChunkKey("lake/data.parquet", 1<<20); got != "lake/data.parquet.__range__.0000000000100000" {
		t.Fatalf("chunk key = %q", got)
	}
}
