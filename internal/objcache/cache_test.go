package objcache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adonm/pgvs3/internal/store"
)

func TestS3OriginAllowlistAndTransparentBucket(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "HEAD /allowed-b/hello.txt":
			w.Header().Set("ETag", `"abc123"`)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
		case "GET /allowed-b/hello.txt":
			w.Header().Set("ETag", `"abc123"`)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("hello"))
		case "PUT /allowed-b/up.txt":
			body, _ := io.ReadAll(r.Body)
			if string(body) != "world" {
				t.Fatalf("PUT body = %q, want world", string(body))
			}
			w.Header().Set("ETag", `"def456"`)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	o, err := NewS3Origin(S3Config{
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		PathStyle: true,
		Buckets:   []string{"allowed-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if !o.BucketAllowed("allowed-b") {
		t.Fatal("allowed-b should be allowed")
	}
	if o.BucketAllowed("other-b") {
		t.Fatal("other-b should not be allowed")
	}

	attrs, err := o.Attributes(context.Background(), "allowed-b", "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	if attrs.ETag != "abc123" || attrs.Size != 5 || attrs.ContentType != "text/plain" {
		t.Fatalf("attrs = %+v", attrs)
	}

	resp, err := o.Proxy(context.Background(), "allowed-b", "up.txt", http.MethodPut, nil, strings.NewReader("world"), 5)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}

	if _, err := o.Attributes(context.Background(), "other-b", "hello.txt"); !errors.Is(err, ErrBucketNotAllowed) {
		t.Fatalf("disallowed bucket err = %v, want ErrBucketNotAllowed", err)
	}

	// Origin 412 maps to store.ErrPrecondition.
	srv412 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer srv412.Close()
	o412, err := NewS3Origin(S3Config{Endpoint: srv412.URL, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer o412.Close()
	if _, err := o412.GetRange(context.Background(), "b", "k", 0, 10, "stale"); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("412 range err = %v, want store.ErrPrecondition", err)
	}
}

func TestConditionalHeadRevalidation(t *testing.T) {
	etag := `"v1"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != "/b/k" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	o, err := NewS3Origin(S3Config{Endpoint: srv.URL, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	_, notMod, err := o.AttributesIfNoneMatch(context.Background(), "b", "k", "v1")
	if err != nil || !notMod {
		t.Fatalf("matching ETag: notMod=%v err=%v, want true nil", notMod, err)
	}

	etag = `"v2"`
	attrs, notMod, err := o.AttributesIfNoneMatch(context.Background(), "b", "k", "v1")
	if err != nil || notMod || attrs.ETag != "v2" {
		t.Fatalf("changed ETag: attrs=%+v notMod=%v err=%v", attrs, notMod, err)
	}
}

func TestSpoolWithBudget(t *testing.T) {
	budget := newSpoolBudget(10<<20, t.TempDir())
	r, sz, clean, err := spoolWithBudget(context.Background(), bytes.NewReader([]byte("abc")), 3, budget)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 3 {
		t.Fatalf("small spool size = %d", sz)
	}
	_ = r
	clean()

	big := bytes.Repeat([]byte("x"), memSpoolMax+1024)
	r2, sz2, clean2, err := spoolWithBudget(context.Background(), bytes.NewReader(big), -1, budget)
	if err != nil {
		t.Fatal(err)
	}
	if sz2 != int64(len(big)) {
		t.Fatalf("big spool size = %d", sz2)
	}
	got, err := io.ReadAll(r2)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("spooled read mismatch: err=%v len=%d", err, len(got))
	}
	clean2()
	if budget.used.Load() != 0 {
		t.Fatalf("budget used after clean = %d, want 0", budget.used.Load())
	}
}
