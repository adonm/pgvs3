//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/s3"
	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/jackc/pgx/v5"
)

type fakeObj struct {
	data        []byte
	etag        string
	contentType string
	modTime     time.Time
}

type fakeS3Origin struct {
	mu           sync.Mutex
	objects      map[string]map[string]fakeObj // bucket -> key -> obj
	uploads      map[string]map[int][]byte     // uploadID -> partNo -> data
	nextUpload   int
	counts       map[string]int // "OP:bucket/key" -> count
	outage       bool
	expectAK     string
	expectRegion string
	t            *testing.T
}

func newFakeS3Origin(t *testing.T, expectAK, expectRegion string) *fakeS3Origin {
	return &fakeS3Origin{
		objects:      make(map[string]map[string]fakeObj),
		uploads:      make(map[string]map[int][]byte),
		counts:       make(map[string]int),
		expectAK:     expectAK,
		expectRegion: expectRegion,
		t:            t,
	}
}

func (f *fakeS3Origin) putDirect(bucket, key string, data []byte, contentType string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.objects[bucket] == nil {
		f.objects[bucket] = make(map[string]fakeObj)
	}
	sum := md5.Sum(data)
	f.objects[bucket][key] = fakeObj{
		data:        append([]byte(nil), data...),
		etag:        hex.EncodeToString(sum[:]),
		contentType: contentType,
		modTime:     time.Now().UTC().Truncate(time.Second),
	}
}

func (f *fakeS3Origin) count(op, bucket, key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op+":"+bucket+"/"+key]
}

func (f *fakeS3Origin) setOutage(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outage = v
}

func (f *fakeS3Origin) checkSigV4(r *http.Request) {
	if f.expectAK == "" {
		return
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") ||
		!strings.Contains(auth, "Credential="+f.expectAK+"/") ||
		!strings.Contains(auth, "/"+f.expectRegion+"/s3/aws4_request") ||
		r.Header.Get("X-Amz-Date") == "" {
		f.t.Errorf("origin request %s %s missing/invalid SigV4 header: auth=%q date=%q",
			r.Method, r.URL.Path, auth, r.Header.Get("X-Amz-Date"))
	}
}

func (f *fakeS3Origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.checkSigV4(r)

	f.mu.Lock()
	if f.outage {
		f.mu.Unlock()
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	f.mu.Unlock()

	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, _ := strings.Cut(trimmed, "/")
	q := r.URL.Query()

	f.mu.Lock()
	f.counts[r.Method+":"+bucket+"/"+key]++
	f.mu.Unlock()

	// Bucket operations
	if key == "" {
		if r.Method == http.MethodPost && q.Has("delete") {
			f.handleDeleteObjects(w, r, bucket)
			return
		}
		if r.Method == http.MethodGet && q.Get("list-type") == "2" {
			f.handleListV2(w, bucket, q)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Multipart operations
	if r.Method == http.MethodPost && q.Has("uploads") {
		f.mu.Lock()
		f.nextUpload++
		id := fmt.Sprintf("up-%d", f.nextUpload)
		f.uploads[id] = make(map[int][]byte)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, bucket, key, id)
		return
	}
	if r.Method == http.MethodPut && q.Has("partNumber") && q.Has("uploadId") {
		partNo, _ := strconv.Atoi(q.Get("partNumber"))
		id := q.Get("uploadId")
		data, _ := io.ReadAll(r.Body)
		sum := md5.Sum(data)
		etag := hex.EncodeToString(sum[:])
		f.mu.Lock()
		if f.uploads[id] != nil {
			f.uploads[id][partNo] = data
		}
		f.mu.Unlock()
		w.Header().Set("ETag", `"`+etag+`"`)
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method == http.MethodPost && q.Has("uploadId") {
		id := q.Get("uploadId")
		f.mu.Lock()
		parts := f.uploads[id]
		var nos []int
		for no := range parts {
			nos = append(nos, no)
		}
		sort.Ints(nos)
		var buf bytes.Buffer
		for _, no := range nos {
			buf.Write(parts[no])
		}
		delete(f.uploads, id)
		f.mu.Unlock()
		f.putDirect(bucket, key, buf.Bytes(), "application/octet-stream")
		f.mu.Lock()
		etag := f.objects[bucket][key].etag
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><ETag>"%s"</ETag></CompleteMultipartUploadResult>`, bucket, key, etag)
		return
	}

	switch r.Method {
	case http.MethodHead:
		f.mu.Lock()
		obj, ok := f.objects[bucket][key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if inm := r.Header.Get("If-None-Match"); inm != "" && strings.Trim(inm, `"`) == obj.etag {
			w.Header().Set("ETag", `"`+obj.etag+`"`)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"`+obj.etag+`"`)
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.Header().Set("Content-Type", obj.contentType)
		w.Header().Set("Last-Modified", obj.modTime.Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		f.mu.Lock()
		obj, ok := f.objects[bucket][key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if im := r.Header.Get("If-Match"); im != "" && strings.Trim(im, `"`) != obj.etag {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", `"`+obj.etag+`"`)
		w.Header().Set("Content-Type", obj.contentType)
		w.Header().Set("Last-Modified", obj.modTime.Format(http.TimeFormat))
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int
			_, _ = fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
			if end >= len(obj.data) {
				end = len(obj.data) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(obj.data[start : end+1])
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)
	case http.MethodPut:
		if src := r.Header.Get("x-amz-copy-source"); src != "" {
			src = strings.TrimPrefix(src, "/")
			sb, sk, _ := strings.Cut(src, "/")
			f.mu.Lock()
			sobj, ok := f.objects[sb][sk]
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.putDirect(bucket, key, sobj.data, sobj.contentType)
			f.mu.Lock()
			etag := f.objects[bucket][key].etag
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<CopyObjectResult><ETag>"%s"</ETag></CopyObjectResult>`, etag)
			return
		}
		data, _ := io.ReadAll(r.Body)
		ct := r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}
		f.putDirect(bucket, key, data, ct)
		f.mu.Lock()
		etag := f.objects[bucket][key].etag
		f.mu.Unlock()
		w.Header().Set("ETag", `"`+etag+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		f.mu.Lock()
		if f.objects[bucket] != nil {
			delete(f.objects[bucket], key)
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *fakeS3Origin) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	raw, _ := io.ReadAll(r.Body)
	var req struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	_ = xml.Unmarshal(raw, &req)
	f.mu.Lock()
	var buf bytes.Buffer
	buf.WriteString(`<DeleteResult>`)
	for _, o := range req.Objects {
		if f.objects[bucket] != nil {
			delete(f.objects[bucket], o.Key)
		}
		fmt.Fprintf(&buf, `<Deleted><Key>%s</Key></Deleted>`, o.Key)
	}
	buf.WriteString(`</DeleteResult>`)
	f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func (f *fakeS3Origin) handleListV2(w http.ResponseWriter, bucket string, q url.Values) {
	prefix := q.Get("prefix")
	delim := q.Get("delimiter")
	token := q.Get("continuation-token")
	maxKeys := 1000
	if mk, err := strconv.Atoi(q.Get("max-keys")); err == nil && mk > 0 {
		maxKeys = mk
	}
	f.mu.Lock()
	var keys []string
	for k := range f.objects[bucket] {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	f.mu.Unlock()

	seenPrefixes := map[string]bool{}
	type item struct {
		isPrefix bool
		val      string
		obj      fakeObj
	}
	var all []item
	for _, k := range keys {
		rest := strings.TrimPrefix(k, prefix)
		if delim != "" {
			if idx := strings.Index(rest, delim); idx >= 0 {
				cp := prefix + rest[:idx+len(delim)]
				if !seenPrefixes[cp] {
					seenPrefixes[cp] = true
					all = append(all, item{isPrefix: true, val: cp})
				}
				continue
			}
		}
		f.mu.Lock()
		obj := f.objects[bucket][k]
		f.mu.Unlock()
		all = append(all, item{val: k, obj: obj})
	}
	start := 0
	if token != "" {
		start, _ = strconv.Atoi(token)
	}
	end := min(start+maxKeys, len(all))
	truncated := end < len(all)
	var buf bytes.Buffer
	fmt.Fprintf(&buf, `<ListBucketResult><Name>%s</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys><IsTruncated>%t</IsTruncated>`,
		bucket, prefix, end-start, maxKeys, truncated)
	if truncated {
		fmt.Fprintf(&buf, `<NextContinuationToken>%d</NextContinuationToken>`, end)
	}
	for _, it := range all[start:end] {
		if it.isPrefix {
			fmt.Fprintf(&buf, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, it.val)
		} else {
			fmt.Fprintf(&buf, `<Contents><Key>%s</Key><Size>%d</Size><ETag>"%s"</ETag></Contents>`, it.val, len(it.obj.data), it.obj.etag)
		}
	}
	buf.WriteString(`</ListBucketResult>`)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

func openTestGateway(t *testing.T, dbURL, endpoint string, revalidateAfter time.Duration) (*store.Store, *s3.Server) {
	t.Helper()
	t.Setenv("AWS_ACCESS_KEY_ID", "TESTAK")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "TESTSK")
	ctx := context.Background()
	st, err := store.Open(ctx, dbURL, 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	origin, err := objcache.NewS3Origin(objcache.S3Config{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Creds:     s3client.NewChainProvider(s3client.ChainConfig{Region: "us-east-1", IMDSDisabled: true}),
		PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rt := objcache.NewRuntime(objcache.RuntimeOptions{Fills: 8, SpoolDir: t.TempDir(), SpoolBytes: 64 << 20})
	tier, err := objcache.New(nil, origin, st, rt, objcache.TierOptions{
		TTL:        revalidateAfter,
		RangeChunk: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tier.Close() })
	return st, s3.New(st, nil, false, 32, time.Second, tier)
}

func doReq(t *testing.T, h http.Handler, method, target string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://gw"+target, r)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReadThroughCacheIntegration(t *testing.T) {
	dbURL := os.Getenv("PGVS3_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("PGVS3_TEST_DATABASE_URL not set")
	}

	fake := newFakeS3Origin(t, "TESTAK", "us-east-1")
	originSrv := httptest.NewServer(fake)
	defer originSrv.Close()

	stA, gwA := openTestGateway(t, dbURL, originSrv.URL, 0)
	defer func() {
		_, _ = stA.Evict(context.Background(), 0, 1)
		_ = stA.Delete(context.Background(), "b1", "obj.bin")
	}()
	_, gwB := openTestGateway(t, dbURL, originSrv.URL, 0)

	// 1. Miss -> fill -> hit, ranges (suffix, open-ended), HEAD, 304/412/416.
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1024) // 16 KiB
	fake.putDirect("b1", "obj.bin", payload, "application/octet-stream")

	r1 := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, nil)
	if r1.Code != 200 || !bytes.Equal(r1.Body.Bytes(), payload) {
		t.Fatalf("miss GET = %d len=%d", r1.Code, r1.Body.Len())
	}
	if got := fake.count("GET", "b1", "obj.bin"); got != 1 {
		t.Fatalf("origin GET count = %d, want 1", got)
	}

	r2 := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, nil)
	if r2.Code != 200 || r2.Header().Get("X-Cache") != "HIT" || !bytes.Equal(r2.Body.Bytes(), payload) {
		t.Fatalf("hit GET = %d X-Cache=%q", r2.Code, r2.Header().Get("X-Cache"))
	}
	if got := fake.count("GET", "b1", "obj.bin"); got != 1 {
		t.Fatalf("origin GET count changed on hit: %d", got)
	}

	// Suffix & open-ended ranges
	rsuf := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, map[string]string{"Range": "bytes=-10"})
	if rsuf.Code != 206 || !bytes.Equal(rsuf.Body.Bytes(), payload[len(payload)-10:]) {
		t.Fatalf("suffix range = %d %q", rsuf.Code, rsuf.Body.String())
	}
	ropen := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, map[string]string{"Range": "bytes=16370-"})
	if ropen.Code != 206 || !bytes.Equal(ropen.Body.Bytes(), payload[16370:]) {
		t.Fatalf("open-ended range = %d len=%d", ropen.Code, ropen.Body.Len())
	}
	rbad := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, map[string]string{"Range": "bytes=999999-"})
	if rbad.Code != 416 {
		t.Fatalf("bad range = %d, want 416", rbad.Code)
	}

	// HEAD, 304, 412
	etag := r2.Header().Get("ETag")
	rhead := doReq(t, gwA, http.MethodHead, "/b1/obj.bin", nil, nil)
	if rhead.Code != 200 || rhead.Header().Get("ETag") != etag {
		t.Fatalf("HEAD = %d etag=%q", rhead.Code, rhead.Header().Get("ETag"))
	}
	r304 := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, map[string]string{"If-None-Match": etag})
	if r304.Code != 304 {
		t.Fatalf("304 = %d", r304.Code)
	}
	r412 := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, map[string]string{"If-Match": `"wrong"`})
	if r412.Code != 412 {
		t.Fatalf("412 = %d", r412.Code)
	}

	// 2. Two gateways on one PG: hit on B with zero origin GETs.
	rB := doReq(t, gwB, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rB.Code != 200 || rB.Header().Get("X-Cache") != "HIT" || !bytes.Equal(rB.Body.Bytes(), payload) {
		t.Fatalf("gwB hit = %d X-Cache=%q", rB.Code, rB.Header().Get("X-Cache"))
	}
	if got := fake.count("GET", "b1", "obj.bin"); got != 1 {
		t.Fatalf("gwB hit triggered origin GET: %d", got)
	}

	// 3. PUT / CopyObject / DeleteObject / DeleteObjects / multipart through A invalidate B.
	rPut := doReq(t, gwA, http.MethodPut, "/b1/obj.bin", []byte("updated-via-A"), nil)
	if rPut.Code != 200 {
		t.Fatalf("PUT = %d", rPut.Code)
	}
	rB2 := doReq(t, gwB, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rB2.Code != 200 || rB2.Body.String() != "updated-via-A" {
		t.Fatalf("gwB after PUT = %d %q", rB2.Code, rB2.Body.String())
	}

	rCopy := doReq(t, gwA, http.MethodPut, "/b1/copied.bin", nil, map[string]string{"x-amz-copy-source": "/b1/obj.bin"})
	if rCopy.Code != 200 {
		t.Fatalf("COPY = %d", rCopy.Code)
	}
	rGetCopy := doReq(t, gwB, http.MethodGet, "/b1/copied.bin", nil, nil)
	if rGetCopy.Code != 200 || rGetCopy.Body.String() != "updated-via-A" {
		t.Fatalf("GET copied = %d %q", rGetCopy.Code, rGetCopy.Body.String())
	}

	// Multipart through A
	rInit := doReq(t, gwA, http.MethodPost, "/b1/mp.bin?uploads", nil, nil)
	if rInit.Code != 200 {
		t.Fatalf("CreateMultipart = %d", rInit.Code)
	}
	var initRes struct {
		UploadId string `xml:"UploadId"`
	}
	_ = xml.Unmarshal(rInit.Body.Bytes(), &initRes)
	rPart := doReq(t, gwA, http.MethodPut, "/b1/mp.bin?partNumber=1&uploadId="+initRes.UploadId, []byte("part-one-bytes"), nil)
	if rPart.Code != 200 {
		t.Fatalf("UploadPart = %d", rPart.Code)
	}
	rComp := doReq(t, gwA, http.MethodPost, "/b1/mp.bin?uploadId="+initRes.UploadId, []byte(`<CompleteMultipartUpload/>`), nil)
	if rComp.Code != 200 {
		t.Fatalf("CompleteMultipart = %d", rComp.Code)
	}
	rGetMP := doReq(t, gwB, http.MethodGet, "/b1/mp.bin", nil, nil)
	if rGetMP.Code != 200 || rGetMP.Body.String() != "part-one-bytes" {
		t.Fatalf("GET multipart = %d %q", rGetMP.Code, rGetMP.Body.String())
	}

	// Delete & DeleteObjects
	rDel := doReq(t, gwA, http.MethodDelete, "/b1/copied.bin", nil, nil)
	if rDel.Code != 204 {
		t.Fatalf("DELETE = %d", rDel.Code)
	}
	if r := doReq(t, gwB, http.MethodGet, "/b1/copied.bin", nil, nil); r.Code != 404 {
		t.Fatalf("GET after DELETE = %d, want 404", r.Code)
	}
	rDelMany := doReq(t, gwA, http.MethodPost, "/b1?delete", []byte(`<Delete><Object><Key>mp.bin</Key></Object></Delete>`), nil)
	if rDelMany.Code != 200 {
		t.Fatalf("DeleteObjects = %d", rDelMany.Code)
	}
	if r := doReq(t, gwB, http.MethodGet, "/b1/mp.bin", nil, nil); r.Code != 404 {
		t.Fatalf("GET after DeleteObjects = %d, want 404", r.Code)
	}

	// 4. Immutable default vs TTL revalidation (single HEAD + HEAD persists metadata).
	fake.putDirect("b1", "obj.bin", []byte("mutated-direct"), "text/plain")
	rImm := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rImm.Body.String() != "updated-via-A" {
		t.Fatalf("immutable cache should not see direct origin change, got %q", rImm.Body.String())
	}
	_, gwTTL := openTestGateway(t, dbURL, originSrv.URL, time.Nanosecond)
	headsBefore := fake.count("HEAD", "b1", "obj.bin")
	rTTL := doReq(t, gwTTL, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rTTL.Body.String() != "mutated-direct" {
		t.Fatalf("TTL gateway did not revalidate, got %q", rTTL.Body.String())
	}
	if heads := fake.count("HEAD", "b1", "obj.bin") - headsBefore; heads != 1 {
		t.Fatalf("stale GET issued %d HEADs, want 1", heads)
	}
	// HEAD-only revalidation persists refreshed metadata so subsequent HEAD within TTL is a hit.
	_, gwTTL5s := openTestGateway(t, dbURL, originSrv.URL, 5*time.Second)
	fake.putDirect("b1", "headonly.bin", []byte("v1"), "text/plain")
	if r := doReq(t, gwTTL5s, http.MethodHead, "/b1/headonly.bin", nil, nil); r.Code != 200 {
		t.Fatalf("cold HEAD = %d", r.Code)
	}
	headsBefore2 := fake.count("HEAD", "b1", "headonly.bin")
	if r := doReq(t, gwTTL5s, http.MethodHead, "/b1/headonly.bin", nil, nil); r.Code != 200 || r.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("second HEAD = %d X-Cache=%q", r.Code, r.Header().Get("X-Cache"))
	}
	if fake.count("HEAD", "b1", "headonly.bin") != headsBefore2 {
		t.Fatalf("second HEAD within TTL hit origin")
	}

	// 5. Restart persistence: new gateway instance on same PG still hits with zero origin GETs.
	beforeRestart := fake.count("GET", "b1", "obj.bin")
	_, gwRestarted := openTestGateway(t, dbURL, originSrv.URL, 0)
	rRestart := doReq(t, gwRestarted, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rRestart.Code != 200 || rRestart.Header().Get("X-Cache") != "HIT" || fake.count("GET", "b1", "obj.bin") != beforeRestart {
		t.Fatalf("restart persistence failed: code=%d cache=%q gets=%d->%d",
			rRestart.Code, rRestart.Header().Get("X-Cache"), beforeRestart, fake.count("GET", "b1", "obj.bin"))
	}

	// 6. ListObjectsV2 passthrough with delimiter/continuation.
	fake.putDirect("b1", "dir/a.txt", []byte("a"), "text/plain")
	fake.putDirect("b1", "dir/b.txt", []byte("b"), "text/plain")
	fake.putDirect("b1", "top.txt", []byte("t"), "text/plain")
	rList := doReq(t, gwA, http.MethodGet, "/b1?list-type=2&delimiter=/&max-keys=1", nil, nil)
	if rList.Code != 200 || !strings.Contains(rList.Body.String(), "<NextContinuationToken>") {
		t.Fatalf("ListObjectsV2 = %d %s", rList.Code, rList.Body.String())
	}

	// 7. Origin outage: cached hits still served; misses return 5xx cleanly.
	fake.setOutage(true)
	rOutageHit := doReq(t, gwA, http.MethodGet, "/b1/obj.bin", nil, nil)
	if rOutageHit.Code != 200 || rOutageHit.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("outage cached hit = %d", rOutageHit.Code)
	}
	rOutageMiss := doReq(t, gwA, http.MethodGet, "/b1/uncached.bin", nil, nil)
	if rOutageMiss.Code < 500 || rOutageMiss.Code >= 600 {
		t.Fatalf("outage miss = %d, want 5xx", rOutageMiss.Code)
	}
	fake.setOutage(false)

	// 8. Partial fills, concurrent coalesce, odd-tail multi-chunk, zero-byte object.
	fake.putDirect("b1", "empty.bin", []byte{}, "application/octet-stream")
	rz := doReq(t, gwA, http.MethodGet, "/b1/empty.bin", nil, nil)
	if rz.Code != 200 || rz.Body.Len() != 0 || rz.Header().Get("X-Cache") != "MISS" {
		t.Fatalf("cold zero-byte GET = %d len=%d X-Cache=%q, want 200 len=0 MISS", rz.Code, rz.Body.Len(), rz.Header().Get("X-Cache"))
	}
	rz2 := doReq(t, gwA, http.MethodGet, "/b1/empty.bin", nil, nil)
	if rz2.Code != 200 || rz2.Body.Len() != 0 || rz2.Header().Get("X-Cache") != "HIT" {
		t.Fatalf("warm zero-byte GET = %d len=%d X-Cache=%q, want 200 len=0 HIT", rz2.Code, rz2.Body.Len(), rz2.Header().Get("X-Cache"))
	}
	if rzr := doReq(t, gwA, http.MethodGet, "/b1/empty.bin", nil, map[string]string{"Range": "bytes=0-0"}); rzr.Code != 416 {
		t.Fatalf("zero-byte range = %d, want 416", rzr.Code)
	}

	odd := make([]byte, 5*(64<<10)+137)
	for i := range odd {
		odd[i] = byte(i * 31)
	}
	fake.putDirect("b1", "odd.bin", odd, "application/octet-stream")
	rP0 := doReq(t, gwA, http.MethodGet, "/b1/odd.bin", nil, map[string]string{"Range": "bytes=0-100"})
	if rP0.Code != 206 || !bytes.Equal(rP0.Body.Bytes(), odd[:101]) {
		t.Fatalf("partial chunk 0 = %d", rP0.Code)
	}
	rP4 := doReq(t, gwA, http.MethodGet, "/b1/odd.bin", nil, map[string]string{"Range": fmt.Sprintf("bytes=%d-", len(odd)-50)})
	if rP4.Code != 206 || !bytes.Equal(rP4.Body.Bytes(), odd[len(odd)-50:]) {
		t.Fatalf("partial tail = %d", rP4.Code)
	}
	getsBeforeFull := fake.count("GET", "b1", "odd.bin")
	rFull := doReq(t, gwA, http.MethodGet, "/b1/odd.bin", nil, nil)
	if rFull.Code != 200 || !bytes.Equal(rFull.Body.Bytes(), odd) {
		t.Fatalf("full odd.bin = %d len=%d", rFull.Code, rFull.Body.Len())
	}
	if got := fake.count("GET", "b1", "odd.bin"); got != getsBeforeFull+1 {
		t.Fatalf("expected 1 origin GET for contiguous missing chunks 1..3, got %d->%d", getsBeforeFull, got)
	}
	rFullHit := doReq(t, gwB, http.MethodGet, "/b1/odd.bin", nil, nil)
	if rFullHit.Code != 200 || rFullHit.Header().Get("X-Cache") != "HIT" || fake.count("GET", "b1", "odd.bin") != getsBeforeFull+1 {
		t.Fatalf("odd.bin warm hit failed")
	}

	// Truncate a chunk row in PG; GET must detect the short chunk, refetch from origin, and return full bytes.
	pgConn, err := pgx.Connect(context.Background(), dbURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgConn.Exec(context.Background(), `UPDATE cache_chunks SET data = substr(data, 1, 5) WHERE object_id = (SELECT id FROM cache_objects WHERE bucket='b1' AND key='odd.bin') AND idx = 1`); err != nil {
		t.Fatal(err)
	}
	_ = pgConn.Close(context.Background())
	rRepaired := doReq(t, gwA, http.MethodGet, "/b1/odd.bin", nil, nil)
	if rRepaired.Code != 200 || !bytes.Equal(rRepaired.Body.Bytes(), odd) {
		t.Fatalf("GET after truncated chunk = %d len=%d, want %d", rRepaired.Code, rRepaired.Body.Len(), len(odd))
	}

	coalData := bytes.Repeat([]byte("coalesce"), 1024)
	fake.putDirect("b1", "coal.bin", coalData, "application/octet-stream")
	var cwg sync.WaitGroup
	for range 16 {
		cwg.Go(func() {
			rc := doReq(t, gwA, http.MethodGet, "/b1/coal.bin", nil, nil)
			if rc.Code != 200 || !bytes.Equal(rc.Body.Bytes(), coalData) {
				t.Errorf("concurrent GET = %d len=%d", rc.Code, rc.Body.Len())
			}
		})
	}
	cwg.Wait()
	if got := fake.count("GET", "b1", "coal.bin"); got != 1 {
		t.Fatalf("concurrent coalesce origin GETs = %d, want 1", got)
	}

	// Verify /metrics includes cache counters and latency buckets.
	rm := doReq(t, gwA, http.MethodGet, "/metrics", nil, nil)
	if rm.Code != 200 || !strings.Contains(rm.Body.String(), "pgvs3_cache_hits_total") || !strings.Contains(rm.Body.String(), "pgvs3_request_duration_seconds_bucket") {
		t.Fatalf("/metrics missing expected counters/buckets: %d", rm.Code)
	}
}
