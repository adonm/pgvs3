//go:build integration

package s3

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
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

func TestZipMembersIntegration(t *testing.T) {
	ctx := originTestCtx
	bucket := fmt.Sprintf("zip-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })

	parquet := bytes.Repeat([]byte{0x50, 0x41, 0x52, 0x31, 0x00, 0x01, 0x02, 0x03}, 2000)
	quant := []byte(strings.Repeat("transcript\tlength\tTPM\n", 500))
	meta := []byte(`{"format":"zstd","rows":500}`)
	archive := buildZip(t, []zipTestFile{
		{name: "data/events.parquet", method: zip.Store, body: parquet},
		{name: "quant.sf", method: zip.Deflate, body: quant},
		{name: "meta/columns.json", method: 93, body: meta},
		{name: "override.txt", method: zip.Store, body: []byte("archive member")},
	})
	if len(archive) > store.MaxInlineBytes {
		t.Fatalf("integration fixture is %d bytes, over the inline limit", len(archive))
	}
	_, err := originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: "sample.zip"}, archive, store.WriteOptions{ContentType: "application/zip"})
	testutil.Must(t, err)
	_, err = originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: "sample.zip/override.txt"}, []byte("real object"), store.WriteOptions{ContentType: "text/plain"})
	testutil.Must(t, err)

	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/"+bucket+"/sample.zip/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != fmt.Sprint(len(parquet)) {
		t.Fatalf("HEAD parquet status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	if got := resp.Header.Get("Content-Type"); got != "application/vnd.apache.parquet" {
		t.Fatalf("HEAD parquet content-type=%q", got)
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=100-499"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[100:500]) {
		t.Fatalf("range parquet status=%d len=%d", resp.StatusCode, len(body))
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("GET quant status=%d len=%d", resp.StatusCode, len(body))
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/meta/columns.json", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, meta) {
		t.Fatalf("GET zstd json status=%d body=%q", resp.StatusCode, body)
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/override.txt", nil)
	if resp.StatusCode != http.StatusOK || string(body) != "real object" {
		t.Fatalf("literal object precedence status=%d body=%q", resp.StatusCode, body)
	}

	out := listZip(t, ts, bucket, "prefix=sample.zip/&delimiter=/")
	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	if strings.Join(prefixes, ",") != "sample.zip/data/,sample.zip/meta/" {
		t.Fatalf("common prefixes = %v", prefixes)
	}
	if len(out.Contents) != 2 || out.Contents[0].Key != "sample.zip/override.txt" || out.Contents[1].Key != "sample.zip/quant.sf" {
		t.Fatalf("root contents = %+v", out.Contents)
	}
}

func TestZipDuckDBParquetAndCSVIntegration(t *testing.T) {
	duckdb, err := exec.LookPath("duckdb")
	if err != nil {
		t.Skip("duckdb CLI is not installed")
	}
	if out, err := exec.Command(duckdb, "-c", "LOAD httpfs;").CombinedOutput(); err != nil {
		t.Skipf("DuckDB httpfs extension unavailable: %v: %s", err, out)
	}
	dir := t.TempDir()
	parquetPath := filepath.Join(dir, "events.parquet")
	create := fmt.Sprintf(`COPY (SELECT i AS id, i * 2 AS value, 'row-' || i AS name FROM range(0, 100) t(i)) TO '%s' (FORMAT PARQUET)`, parquetPath)
	if out, err := exec.Command(duckdb, "-c", create).CombinedOutput(); err != nil {
		t.Fatalf("create parquet fixture: %v: %s", err, out)
	}
	parquet, err := os.ReadFile(parquetPath)
	testutil.Must(t, err)

	csv := []byte("id,value\n1,10\n2,20\n3,30\n")
	archive := buildZip(t, []zipTestFile{
		{name: "data/events.parquet", method: zip.Store, body: parquet},
		{name: "data/events.csv", method: zip.Deflate, body: csv},
	})
	if len(archive) > store.MaxInlineBytes {
		t.Fatalf("integration fixture is %d bytes, over the inline limit", len(archive))
	}
	ctx := originTestCtx
	bucket := fmt.Sprintf("zipduck-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })
	_, err = originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: "batch.zip"}, archive, store.WriteOptions{ContentType: "application/zip"})
	testutil.Must(t, err)

	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	endpoint := strings.TrimPrefix(ts.URL, "http://")
	script := fmt.Sprintf(`
SET s3_endpoint='%s';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_access_key_id='pgvs3';
SET s3_secret_access_key='pgvs3-secret';
SET s3_region='us-east-1';
SELECT count(*), sum(value) FROM read_parquet('s3://%s/batch.zip/data/events.parquet');
SELECT count(*), sum(value) FROM read_csv('s3://%s/batch.zip/data/events.csv', header=true);
`, endpoint, bucket, bucket)
	out, err := exec.Command(duckdb, "-csv", "-c", script).CombinedOutput()
	if err != nil {
		t.Fatalf("duckdb query failed: %v\n%s", err, out)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	foundParquet, foundCSV := false, false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "100,9900" {
			foundParquet = true
		}
		if line == "3,60" {
			foundCSV = true
		}
	}
	if !foundParquet || !foundCSV {
		t.Fatalf("duckdb did not read both members from the zip: %s", out)
	}
}

func TestZipOriginMembersIntegration(t *testing.T) {
	ctx := originTestCtx
	bucket := fmt.Sprintf("ziporigin-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })

	dir := t.TempDir()
	tier, err := objcache.New(testutil.DiscardLogger(),
		fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir),
		originTestStore, objcache.NewRuntime(objcache.RuntimeOptions{}),
		objcache.TierOptions{Namespace: fmt.Sprintf("zip-origin-%d", os.Getpid()), Retention: time.Hour, MaxFills: 4})
	testutil.Must(t, err)
	defer tier.Close()

	parquet := bytes.Repeat([]byte("PAR1-origin-member"), 3000)
	quant := []byte(strings.Repeat("transcript\tTPM\n", 300))
	archive := buildZip(t, []zipTestFile{
		{name: "data/events.parquet", method: zip.Store, body: parquet},
		{name: "quant.sf", method: zip.Deflate, body: quant},
	})
	_, err = tier.UploadWithOptions(ctx, bucket, "sample.zip", bytes.NewReader(archive), objcache.UploadOptions{ExpectedSize: int64(len(archive)), ContentType: "application/zip"})
	testutil.Must(t, err)

	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	srv.SetOrigin(tier)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/"+bucket+"/sample.zip/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != fmt.Sprint(len(parquet)) {
		t.Fatalf("origin HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=3000-5999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[3000:6000]) {
		t.Fatalf("origin range status=%d len=%d", resp.StatusCode, len(body))
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("origin deflate status=%d len=%d", resp.StatusCode, len(body))
	}
	out := listZip(t, ts, bucket, "prefix=sample.zip/&delimiter=/")
	if len(out.Contents) != 1 || out.Contents[0].Key != "sample.zip/quant.sf" {
		t.Fatalf("origin contents = %+v", out.Contents)
	}
	if len(out.CommonPrefixes) != 1 || out.CommonPrefixes[0].Prefix != "sample.zip/data/" {
		t.Fatalf("origin prefixes = %+v", out.CommonPrefixes)
	}
}

func TestTarMembersIntegration(t *testing.T) {
	ctx := originTestCtx
	bucket := fmt.Sprintf("tar-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })

	parquet := bytes.Repeat([]byte("PAR1-tar"), 2000)
	quant := []byte(strings.Repeat("transcript\tTPM\n", 200))
	archive := buildTar(t, []zipTestFile{
		{name: "data/events.parquet", body: parquet},
		{name: "quant.sf", body: quant},
		{name: "meta/columns.json", body: []byte(`{"columns":2}`)},
	})
	if len(archive) > store.MaxInlineBytes {
		t.Fatalf("integration fixture is %d bytes, over the inline limit", len(archive))
	}
	_, err := originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: "bundle.tar"}, archive, store.WriteOptions{ContentType: "application/x-tar"})
	testutil.Must(t, err)

	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/"+bucket+"/bundle.tar/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != fmt.Sprint(len(parquet)) {
		t.Fatalf("HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/bundle.tar/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("GET status=%d len=%d", resp.StatusCode, len(body))
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/bundle.tar/data/events.parquet", map[string]string{"Range": "bytes=100-999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[100:1000]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	out := listZip(t, ts, bucket, "prefix=bundle.tar/&delimiter=/")
	if len(out.CommonPrefixes) != 2 || out.CommonPrefixes[0].Prefix != "bundle.tar/data/" || out.CommonPrefixes[1].Prefix != "bundle.tar/meta/" {
		t.Fatalf("prefixes = %+v", out.CommonPrefixes)
	}
	if len(out.Contents) != 1 || out.Contents[0].Key != "bundle.tar/quant.sf" {
		t.Fatalf("contents = %+v", out.Contents)
	}
}

func TestArchiveOriginZeroMemberRequests(t *testing.T) {
	ctx := originTestCtx
	originBucket := fmt.Sprintf("zipzero-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, originBucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, originBucket, true) })

	parquet := bytes.Repeat([]byte("PAR1-origin-zero-wan"), 2000)
	quant := []byte(strings.Repeat("transcript\tTPM\n", 300))
	archive := buildZip(t, []zipTestFile{
		{name: "data/events.parquet", method: zip.Store, body: parquet},
		{name: "quant.sf", method: zip.Deflate, body: quant},
	})
	_, err := originTestStore.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(originBucket), Key: "sample.zip"}, archive, store.WriteOptions{ContentType: "application/zip"})
	testutil.Must(t, err)

	originSrv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer originSrv.Close()
	var memberRequests atomic.Int64
	originTS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/"+originBucket+"/sample.zip/data/events.parquet") {
			memberRequests.Add(1)
		}
		originSrv.ServeHTTP(w, r)
	}))
	defer originTS.Close()

	tier, err := objcache.New(testutil.DiscardLogger(),
		fmt.Appendf(nil, `{"type":"s3","config":{"bucket":%q,"endpoint":%q,"insecure":true,"bucket_lookup_type":"path","strip_bucket":true}}`, originBucket, originTS.URL),
		originTestStore, objcache.NewRuntime(objcache.RuntimeOptions{}),
		objcache.TierOptions{Namespace: fmt.Sprintf("zip-zero-%d", time.Now().UnixNano()), Retention: time.Hour, MaxFills: 4, RangeChunk: 1 << 20})
	testutil.Must(t, err)
	defer tier.Close()
	if head, err := tier.Head(ctx, "local", "sample.zip"); err != nil {
		t.Fatalf("origin archive HEAD: %v", err)
	} else if head.Size != int64(len(archive)) {
		t.Fatalf("origin archive size = %d, want %d", head.Size, len(archive))
	}
	bucket := fmt.Sprintf("zipzero-local-%d", time.Now().UnixNano())
	testutil.Must(t, originTestStore.CreateBucket(ctx, bucket))
	t.Cleanup(func() { _, _ = originTestStore.DropBucket(ctx, bucket, true) })
	srv := New(originTestStore, testutil.DiscardLogger(), false, 8, time.Second, nil)
	defer srv.Close()
	srv.SetOrigin(tier)
	ts := httptest.NewServer(srv)
	defer ts.Close()
	if idx, _, err := srv.loadZipIndex(ctx, originBackend{Tier: tier}, bucket, "sample.zip"); err != nil {
		t.Fatalf("build origin archive index: %v", err)
	} else if _, ok := idx.member("data/events.parquet"); !ok {
		t.Fatalf("archive index has no member: %+v", idx.entries)
	}
	memberKey := "sample.zip/data/events.parquet"
	if info, ok, err := (originBackend{Tier: tier}).HeadCached(ctx, bucket, memberKey); err != nil || ok {
		t.Fatalf("HeadCached = (%+v, %v, %v), want miss", info, ok, err)
	}
	if _, _, _, ok := srv.resolveZipMember(ctx, originBackend{Tier: tier}, bucket, memberKey); !ok {
		t.Fatal("origin archive member did not resolve")
	}
	if resp, debugBody := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/data/events.parquet", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("debug GET status=%d body=%s", resp.StatusCode, debugBody)
	}
	resp, _ := zipHTTP(t, ts, http.MethodHead, "/"+bucket+"/sample.zip/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != fmt.Sprint(len(parquet)) {
		t.Fatalf("HEAD status=%d length=%q body=%s", resp.StatusCode, resp.Header.Get("Content-Length"), resp.Header.Get("X-Cache"))
	}
	resp, body := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=4000-8999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[4000:9000]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	if n := memberRequests.Load(); n != 0 {
		t.Fatalf("origin saw %d requests for sample.zip/data/events.parquet, want 0", n)
	}
}
