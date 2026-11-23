package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

func TestStatGetRangeAndProxy(t *testing.T) {
	payload := "0123456789abcdef"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bkt/key.txt" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"etag1"`)
		w.Header().Set("Content-Type", "text/plain")
		switch r.Method {
		case http.MethodHead:
			if r.Header.Get("If-None-Match") == `"etag1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if rng := r.Header.Get("Range"); rng != "" {
				var s, e int
				_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &s, &e)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", s, e, len(payload)))
				w.Header().Set("Content-Length", fmt.Sprintf("%d", e-s+1))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, payload[s:e+1])
				return
			}
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, payload)
		case http.MethodPut:
			b, _ := io.ReadAll(r.Body)
			if string(b) != "new" {
				t.Fatalf("PUT body = %q", string(b))
			}
			w.Header().Set("ETag", `"etag2"`)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	b, err := New(Config{
		Bucket:    "bkt",
		Endpoint:  srv.URL,
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	info, err := b.Stat(context.Background(), "key.txt")
	if err != nil || info.ETag != "etag1" || info.Size != int64(len(payload)) {
		t.Fatalf("Stat = %+v err=%v", info, err)
	}
	_, notMod, err := b.StatIfNoneMatch(context.Background(), "key.txt", "etag1")
	if err != nil || !notMod {
		t.Fatalf("StatIfNoneMatch = %v, %v", notMod, err)
	}

	rc, err := b.Get(context.Background(), "key.txt", 4, 6, "etag1")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(got) != "456789" {
		t.Fatalf("range got = %q", string(got))
	}

	resp, err := b.Proxy(context.Background(), http.MethodPut, "key.txt", nil, strings.NewReader("new"), 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Proxy PUT = %d", resp.StatusCode)
	}
}

func TestStatWrongRegionMapsToErrWrongRegion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-amz-bucket-region", "us-west-2")
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer srv.Close()

	b, err := New(Config{Bucket: "bkt", Endpoint: srv.URL, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	_, err = b.Stat(context.Background(), "key.txt")
	if !WrongRegion(err) || !strings.Contains(err.Error(), "us-west-2") {
		t.Fatalf("Stat 301 err = %v, want WrongRegion mentioning us-west-2", err)
	}
}

func TestProxyDoesNotRetryAndGetRetriesOnlyTransient(t *testing.T) {
	var putAttempts, get500Attempts, get503Attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "PUT /bkt/no-retry":
			putAttempts.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		case "GET /bkt/no-retry-500":
			get500Attempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case "GET /bkt/retry-503":
			if get503Attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok")
		}
	}))
	defer srv.Close()

	b, err := New(Config{Bucket: "bkt", Endpoint: srv.URL, Region: "us-east-1", PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	resp, err := b.Proxy(context.Background(), http.MethodPut, "no-retry", nil, strings.NewReader("payload"), 7)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if putAttempts.Load() != 1 {
		t.Fatalf("Proxy PUT attempts = %d, want 1", putAttempts.Load())
	}

	if _, err := b.Get(context.Background(), "no-retry-500", 0, -1, ""); err == nil {
		t.Fatal("expected error on 500")
	}
	if get500Attempts.Load() != 1 {
		t.Fatalf("GET 500 attempts = %d, want 1", get500Attempts.Load())
	}

	rc, err := b.Get(context.Background(), "retry-503", 0, -1, "")
	if err != nil {
		t.Fatalf("GET 503->200 err = %v", err)
	}
	_ = rc.Close()
	if get503Attempts.Load() != 2 {
		t.Fatalf("GET 503 attempts = %d, want 2", get503Attempts.Load())
	}

	if retryableError(errors.New("permanent failure")) {
		t.Fatal("non-transient error should not be retryable")
	}
	if !retryableError(syscall.ECONNRESET) {
		t.Fatal("ECONNRESET should be retryable")
	}
}
