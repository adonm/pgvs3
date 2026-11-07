//go:build integration

package s3

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
)

func TestLocalConditionalGetAndCacheHead(t *testing.T) {
	ctx := originTestCtx
	bucket := fmt.Sprintf("cond-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })
	data := []byte("hello conditional")
	obj := store.ObjectRef{NS: ns.BucketRef(bucket), Key: "obj"}
	_, err := originTestStore.PutInline(ctx, obj, data, store.WriteOptions{ContentType: "text/plain"})
	testutil.Must(t, err)

	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	request := func(method, target string, headers map[string]string) (*http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+target, nil)
		testutil.Must(t, err)
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		resp, err := http.DefaultClient.Do(req)
		testutil.Must(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		testutil.Must(t, err)
		return resp, body
	}

	resp, body := request(http.MethodGet, "/"+bucket+"/obj", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("GET status=%d body=%q", resp.StatusCode, body)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("GET returned no ETag")
	}

	resp, body = request(http.MethodGet, "/"+bucket+"/obj", map[string]string{"If-None-Match": etag})
	if resp.StatusCode != http.StatusNotModified || len(body) != 0 {
		t.Fatalf("If-None-Match status=%d body=%q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("ETag"); got != etag {
		t.Fatalf("304 ETag = %q, want %q", got, etag)
	}

	resp, _ = request(http.MethodGet, "/"+bucket+"/obj", map[string]string{"If-Match": `"other"`})
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match status=%d, want 412", resp.StatusCode)
	}

	resp, body = request(http.MethodGet, "/"+bucket+"/obj", map[string]string{"If-Match": etag})
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, data) {
		t.Fatalf("matching If-Match status=%d body=%q", resp.StatusCode, body)
	}

	name := fmt.Sprintf("cache-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.EnsureNamespace(ctx, ns.Cache, name, nil, nil))
	t.Cleanup(func() { _, _ = originTestStore.DropNamespace(ctx, ns.Cache, name, true) })
	cacheData := []byte("0123456789")
	_, err = originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.CacheRef(name), Key: "k"}, cacheData, store.WriteOptions{})
	testutil.Must(t, err)
	cachePath := CachePrefix + name + "/k"

	resp, _ = request(http.MethodHead, cachePath, nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "10" || resp.Header.Get("ETag") == "" {
		t.Fatalf("cache HEAD status=%d headers=%v", resp.StatusCode, resp.Header)
	}
	cacheETag := resp.Header.Get("ETag")

	resp, _ = request(http.MethodHead, cachePath, map[string]string{"Range": "bytes=2-4"})
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 2-4/10" {
		t.Fatalf("cache ranged HEAD status=%d headers=%v", resp.StatusCode, resp.Header)
	}

	resp, _ = request(http.MethodHead, cachePath, map[string]string{"Range": "bytes=20-"})
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable || resp.Header.Get("Content-Range") != "bytes */10" {
		t.Fatalf("cache invalid HEAD status=%d headers=%v", resp.StatusCode, resp.Header)
	}

	resp, body = request(http.MethodGet, cachePath, map[string]string{"If-None-Match": cacheETag})
	if resp.StatusCode != http.StatusNotModified || len(body) != 0 {
		t.Fatalf("cache 304 status=%d body=%q", resp.StatusCode, body)
	}
	if got := resp.Header.Get("ETag"); got != cacheETag {
		t.Fatalf("cache 304 ETag = %q, want %q", got, cacheETag)
	}
}
