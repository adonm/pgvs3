//go:build integration

package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
)

var (
	originTestStore *store.Store
	originTestCtx   = context.Background()
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunMain(m, func(ctx context.Context, url string) (func(), error) {
		st, err := store.Open(ctx, url, 8, testutil.DiscardLogger())
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		if err := st.Init(ctx); err != nil {
			st.Close()
			return nil, fmt.Errorf("init schema: %w", err)
		}
		originTestStore = st
		return st.Close, nil
	}))
}

func TestOriginWriteValidationAndConditions(t *testing.T) {
	ctx := originTestCtx
	dir := t.TempDir()
	tier, err := objcache.New(testutil.DiscardLogger(),
		fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir),
		originTestStore, fmt.Sprintf("origin-http-%d", os.Getpid()), 0, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tier.Close()
	srv := New(originTestStore, testutil.DiscardLogger(), false, 8)
	srv.SetOrigin(tier)
	httpSrv := httptest.NewServer(srv)
	defer httpSrv.Close()

	put := func(key, body string, headers map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/"+key, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		sum := md5.Sum([]byte(body))
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	read := func(key string) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, httpSrv.URL+"/bkt/"+key, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(b), resp.Header.Clone()
	}

	req, _ := http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/bad", strings.NewReader("new"))
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5.New().Sum(nil)))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad checksum status = %d", resp.StatusCode)
	}
	if status, _, _ := read("bad"); status != http.StatusNotFound {
		t.Fatalf("bad checksum created origin object: status=%d", status)
	}

	if resp := put("obj", "old", map[string]string{
		"Content-Type": "text/plain", "x-amz-meta-owner": "first",
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("initial PUT status = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	req, _ = http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/obj", strings.NewReader("new"))
	req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(md5.New().Sum(nil)))
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("replacement bad checksum status = %d", resp.StatusCode)
	}
	if status, body, _ := read("obj"); status != http.StatusOK || body != "old" {
		t.Fatalf("origin after bad replacement = %d %q", status, body)
	}

	if resp := put("obj", "replacement", map[string]string{"If-None-Match": "*"}); resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-None-Match:* status = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	if status, body, _ := read("obj"); status != http.StatusOK || body != "old" {
		t.Fatalf("origin after failed precondition = %d %q", status, body)
	}

	if resp := put("src", "source", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("source PUT status = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	if resp := put("dest", "destination", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("dest PUT status = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	copyReq, _ := http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/dest", nil)
	copyReq.Header.Set("x-amz-copy-source", "/bkt/src")
	copyReq.Header.Set("If-None-Match", "*")
	resp, err = http.DefaultClient.Do(copyReq)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("copy destination precondition status = %d", resp.StatusCode)
	}
	if status, body, _ := read("dest"); status != http.StatusOK || body != "destination" {
		t.Fatalf("destination after failed copy = %d %q", status, body)
	}
	copyReq, _ = http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/dest", nil)
	copyReq.Header.Set("x-amz-copy-source", "/bkt/src")
	copyReq.Header.Set("x-amz-copy-source-if-match", `"00000000000000000000000000000000"`)
	resp, err = http.DefaultClient.Do(copyReq)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("copy source precondition status = %d", resp.StatusCode)
	}
	srcHead, _ := http.NewRequest(http.MethodHead, httpSrv.URL+"/bkt/src", nil)
	resp, err = http.DefaultClient.Do(srcHead)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	srcETag := resp.Header.Get("ETag")
	copyReq, _ = http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/copied", nil)
	copyReq.Header.Set("x-amz-copy-source", "/bkt/src")
	copyReq.Header.Set("x-amz-copy-source-if-match", srcETag)
	copyReq.Header.Set("If-None-Match", "*")
	resp, err = http.DefaultClient.Do(copyReq)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("successful conditional copy status = %d", resp.StatusCode)
	}
	if status, body, _ := read("copied"); status != http.StatusOK || body != "source" {
		t.Fatalf("copied object = %d %q", status, body)
	}
	t.Run("origin multipart copy stays on origin backend", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/bkt/part-dest?uploads", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var upload initiateMultipartUploadResult
		err = xml.NewDecoder(resp.Body).Decode(&upload)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || upload.UploadID == "" {
			t.Fatalf("create multipart = %d, %+v, %v", resp.StatusCode, upload, err)
		}
		defer tier.AbortUpload(ctx, upload.UploadID, "bkt", "part-dest")
		req, _ = http.NewRequest(http.MethodPut, httpSrv.URL+"/bkt/part-dest?partNumber=1&uploadId="+upload.UploadID, nil)
		req.Header.Set("x-amz-copy-source", "/bkt/src")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "<ETag>") {
			t.Fatalf("origin part copy = %d %s", resp.StatusCode, body)
		}
	})

	if _, err := originTestStore.Cache(tier.Name()).Del(ctx, []string{"bkt/obj"}); err != nil {
		t.Fatal(err)
	}
	head, _ := http.NewRequest(http.MethodHead, httpSrv.URL+"/bkt/obj", nil)
	resp, err = http.DefaultClient.Do(head)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got := resp.Header.Get("x-amz-meta-owner"); got != "first" {
		t.Fatalf("metadata after eviction = %q", got)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
		t.Fatalf("content type after eviction = %q", got)
	}
}

func TestS3MissingBucketAndInvalidRange(t *testing.T) {
	bucket := fmt.Sprintf("errbucket-%d", time.Now().UnixNano())
	local := New(originTestStore, testutil.DiscardLogger(), false, 8)
	httpSrv := httptest.NewServer(local)
	defer httpSrv.Close()
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	do := func(method, path, body string, headers map[string]string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, httpSrv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := do(http.MethodPut, "/"+bucket, "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	if resp := do(http.MethodPut, "/"+bucket+"/ten", "0123456789", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("put = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	sum := crc32.ChecksumIEEE([]byte("0123456789"))
	checksum := base64.StdEncoding.EncodeToString([]byte{byte(sum >> 24), byte(sum >> 16), byte(sum >> 8), byte(sum)})
	if resp := do(http.MethodPut, "/"+bucket+"/encoded", "0123456789", map[string]string{
		"Content-Encoding": "gzip", "Content-Type": "text/plain",
		"x-amz-checksum-crc32": checksum,
	}); resp.StatusCode != http.StatusOK {
		t.Fatalf("put encoded = %d", resp.StatusCode)
	} else {
		resp.Body.Close()
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		resp := do(method, "/"+bucket+"/encoded", "", nil)
		resp.Body.Close()
		if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("%s Content-Encoding = %q", method, got)
		}
		if resp.Header.Get("Last-Modified") == "" {
			t.Fatalf("%s missing Last-Modified", method)
		}
		if got := resp.Header.Get("x-amz-checksum-crc32"); got != checksum {
			t.Fatalf("%s checksum = %q, want %q", method, got, checksum)
		}
	}
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/missing/key"},
		{http.MethodHead, "/missing/key"},
		{http.MethodGet, "/missing"},
		{http.MethodDelete, "/missing/key"},
	} {
		resp := do(tc.method, tc.path, "", nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404", tc.method, tc.path, resp.StatusCode)
		}
	}
	resp := do(http.MethodGet, "/"+bucket+"/ten", "", map[string]string{"Range": "bytes=100-200"})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("invalid range = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes */10" {
		t.Fatalf("invalid range Content-Range = %q", got)
	}

	resp = do(http.MethodGet, "/"+bucket+"/nope", "", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing key = %d", resp.StatusCode)
	}
}
