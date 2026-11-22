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

	rc, gotAttrs, err := o.GetWithAttrs(context.Background(), "allowed-b", "hello.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "hello" || gotAttrs.ETag != "abc123" {
		t.Fatalf("got %q %+v", string(data), gotAttrs)
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
}

func TestSpoolWithBudget(t *testing.T) {
	budget := newSpoolBudget(10<<20, t.TempDir())
	// Small payload stays in memory.
	sp, clean, err := spoolWithBudget(context.Background(), bytes.NewReader([]byte("abc")), 3, budget)
	if err != nil {
		t.Fatal(err)
	}
	if sp.size != 3 || string(sp.data) != "abc" {
		t.Fatalf("small spool = %+v", sp)
	}
	clean()

	// Unknown length payload > memFillMax spills to temp file and releases budget on clean.
	big := bytes.Repeat([]byte("x"), memFillMax+1024)
	sp2, clean2, err := spoolWithBudget(context.Background(), bytes.NewReader(big), -1, budget)
	if err != nil {
		t.Fatal(err)
	}
	if sp2.size != int64(len(big)) || sp2.data != nil {
		t.Fatalf("big spool size=%d dataNil=%v", sp2.size, sp2.data == nil)
	}
	got, err := io.ReadAll(sp2.r)
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("spooled read mismatch: err=%v len=%d", err, len(got))
	}
	clean2()
	if budget.used.Load() != 0 {
		t.Fatalf("budget used after clean = %d, want 0", budget.used.Load())
	}
}

func TestConditionalHeadRevalidation(t *testing.T) {
	var etag = `"v1"`
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

func TestPlanCacheRead(t *testing.T) {
	if p := planCacheRead(cacheResolution{}, 0, false, readView{Size: 0}); p.action != cacheActionZero {
		t.Fatalf("zero size action = %v", p.action)
	}
	if p := planCacheRead(cacheResolution{}, 1<<20, false, readView{Size: 10 << 20, start: 0, end: 100, ranged: true}); p.action != cacheActionRange {
		t.Fatalf("range action = %v", p.action)
	}
	if p := planCacheRead(cacheResolution{}, 1<<20, false, readView{Size: 10 << 20, start: 0, end: 10<<20 - 1}); p.action != cacheActionPassThrough {
		t.Fatalf("large whole action = %v", p.action)
	}
}
