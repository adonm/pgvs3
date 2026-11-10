package s3

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
)

func init() {
	zip.RegisterCompressor(zstd.ZipMethodWinZip, zstd.ZipCompressor())
}

type zipTestFile struct {
	name   string
	method uint16
	body   []byte
}

func buildZip(t *testing.T, files []zipTestFile) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, f := range files {
		hdr := &zip.FileHeader{Name: f.name, Method: f.method, Modified: time.Unix(1_700_000_000, 0)}
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			t.Fatalf("create %s: %v", f.name, err)
		}
		if _, err := w.Write(f.body); err != nil {
			t.Fatalf("write %s: %v", f.name, err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("close zip: %v", err)
	}
	return buf.Bytes()
}

// buildZip64 writes explicit ZIP64 extra fields and data descriptors, which is
// what streaming packers (including s3zip pack) produce.
func buildZip64(t *testing.T, files []zipTestFile) []byte {
	t.Helper()
	type cdEntry struct {
		name          []byte
		method        uint16
		crc           uint32
		compressed    []byte
		uncompressed  int64
		headerOffset  int64
		compressedLen int64
	}
	var out bytes.Buffer
	entries := make([]cdEntry, 0, len(files))
	for _, f := range files {
		var comp bytes.Buffer
		switch f.method {
		case zip.Store:
			comp.Write(f.body)
		case zip.Deflate:
			fw, err := flate.NewWriter(&comp, flate.DefaultCompression)
			if err != nil {
				t.Fatalf("flate %s: %v", f.name, err)
			}
			if _, err := fw.Write(f.body); err != nil {
				t.Fatalf("flate write %s: %v", f.name, err)
			}
			if err := fw.Close(); err != nil {
				t.Fatalf("flate close %s: %v", f.name, err)
			}
		case zstd.ZipMethodWinZip:
			zw, err := zstd.NewWriter(&comp)
			if err != nil {
				t.Fatalf("zstd %s: %v", f.name, err)
			}
			if _, err := zw.Write(f.body); err != nil {
				t.Fatalf("zstd write %s: %v", f.name, err)
			}
			if err := zw.Close(); err != nil {
				t.Fatalf("zstd close %s: %v", f.name, err)
			}
		default:
			t.Fatalf("unsupported method %d", f.method)
		}
		name := []byte(f.name)
		if len(name) > 0xffff {
			t.Fatal("name too long")
		}
		headerOffset := int64(out.Len())
		crc := crc32.ChecksumIEEE(f.body)
		var hdr [30]byte
		binary.LittleEndian.PutUint32(hdr[0:], zipLocalSignature)
		binary.LittleEndian.PutUint16(hdr[4:], 45)
		binary.LittleEndian.PutUint16(hdr[6:], 0x0008) // data descriptor
		binary.LittleEndian.PutUint16(hdr[8:], f.method)
		binary.LittleEndian.PutUint32(hdr[14:], crc)
		binary.LittleEndian.PutUint16(hdr[26:], uint16(len(name)))
		out.Write(hdr[:])
		out.Write(name)
		out.Write(comp.Bytes())
		var desc [24]byte
		binary.LittleEndian.PutUint32(desc[0:], zipDataDescriptor)
		binary.LittleEndian.PutUint32(desc[4:], crc)
		binary.LittleEndian.PutUint64(desc[8:], uint64(comp.Len()))
		binary.LittleEndian.PutUint64(desc[16:], uint64(len(f.body)))
		out.Write(desc[:])
		entries = append(entries, cdEntry{
			name: name, method: f.method, crc: crc,
			compressed: comp.Bytes(), uncompressed: int64(len(f.body)),
			headerOffset: headerOffset, compressedLen: int64(comp.Len()),
		})
	}
	cdStart := int64(out.Len())
	for _, e := range entries {
		var hdr [46]byte
		binary.LittleEndian.PutUint32(hdr[0:], zipCentralSig)
		binary.LittleEndian.PutUint16(hdr[4:], 45)
		binary.LittleEndian.PutUint16(hdr[6:], 45)
		binary.LittleEndian.PutUint16(hdr[8:], 0x0008)
		binary.LittleEndian.PutUint16(hdr[10:], e.method)
		binary.LittleEndian.PutUint32(hdr[16:], e.crc)
		binary.LittleEndian.PutUint32(hdr[20:], 0xffffffff)
		binary.LittleEndian.PutUint32(hdr[24:], 0xffffffff)
		binary.LittleEndian.PutUint16(hdr[28:], uint16(len(e.name)))
		binary.LittleEndian.PutUint16(hdr[30:], 28)
		binary.LittleEndian.PutUint32(hdr[42:], 0xffffffff)
		out.Write(hdr[:])
		out.Write(e.name)
		var extra [28]byte
		binary.LittleEndian.PutUint16(extra[0:], zip64ExtraID)
		binary.LittleEndian.PutUint16(extra[2:], 24)
		binary.LittleEndian.PutUint64(extra[4:], uint64(e.uncompressed))
		binary.LittleEndian.PutUint64(extra[12:], uint64(e.compressedLen))
		binary.LittleEndian.PutUint64(extra[20:], uint64(e.headerOffset))
		out.Write(extra[:])
	}
	cdSize := int64(out.Len()) - cdStart
	zip64EndOffset := int64(out.Len())
	var record [56]byte
	binary.LittleEndian.PutUint32(record[0:], zip64EndSignature)
	binary.LittleEndian.PutUint64(record[4:], 44)
	binary.LittleEndian.PutUint16(record[12:], 45)
	binary.LittleEndian.PutUint16(record[14:], 45)
	binary.LittleEndian.PutUint64(record[24:], uint64(len(entries)))
	binary.LittleEndian.PutUint64(record[32:], uint64(len(entries)))
	binary.LittleEndian.PutUint64(record[40:], uint64(cdSize))
	binary.LittleEndian.PutUint64(record[48:], uint64(cdStart))
	out.Write(record[:])
	var locator [20]byte
	binary.LittleEndian.PutUint32(locator[0:], zip64LocSignature)
	binary.LittleEndian.PutUint64(locator[8:], uint64(zip64EndOffset))
	binary.LittleEndian.PutUint32(locator[16:], 1)
	out.Write(locator[:])
	var eocd [22]byte
	binary.LittleEndian.PutUint32(eocd[0:], zipEndSignature)
	binary.LittleEndian.PutUint16(eocd[8:], 0xffff)
	binary.LittleEndian.PutUint16(eocd[10:], 0xffff)
	binary.LittleEndian.PutUint32(eocd[12:], 0xffffffff)
	binary.LittleEndian.PutUint32(eocd[16:], 0xffffffff)
	out.Write(eocd[:])
	return out.Bytes()
}

type zipTestOpen struct {
	key         string
	first, last int64
}

type zipTestBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
	opens   []zipTestOpen
}

func newZipTestBackend() *zipTestBackend {
	return &zipTestBackend{objects: make(map[string][]byte)}
}

func (b *zipTestBackend) put(key string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[key] = bytes.Clone(data)
}

func (b *zipTestBackend) info(key string, data []byte) store.ObjectInfo {
	return store.ObjectInfo{
		Size:      int64(len(data)),
		ETag:      fmt.Sprintf("etag-%08x", crc32.ChecksumIEEE(data)),
		CreatedAt: time.Unix(1_700_000_000, 0),
	}
}

func (b *zipTestBackend) Head(_ context.Context, _, key string) (readHead, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok {
		return readHead{}, store.ErrNotFound
	}
	return readHead{Info: b.info(key, data)}, nil
}

func (b *zipTestBackend) Open(_ context.Context, _, key string, rng store.ByteRange) (*store.ReadResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[key]
	if !ok {
		return nil, store.ErrNotFound
	}
	size := int64(len(data))
	span, err := rng.Resolve(size)
	if err != nil {
		return nil, err
	}
	b.opens = append(b.opens, zipTestOpen{key: key, first: span.Start, last: span.End})
	info := b.info(key, data)
	body := io.ReadCloser(http.NoBody)
	if span.End >= span.Start && size > 0 {
		body = io.NopCloser(bytes.NewReader(data[span.Start : span.End+1]))
	}
	return &store.ReadResult{ObjectInfo: info, Body: body, Start: span.Start, End: span.End, Ranged: rng.Requested}, nil
}

func (b *zipTestBackend) List(_ context.Context, _, prefix, after string, limit int) ([]store.Listed, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	keys := make([]string, 0, len(b.objects))
	for key := range b.objects {
		if strings.HasPrefix(key, prefix) && key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	out := make([]store.Listed, 0, len(keys))
	for _, key := range keys {
		out = append(out, store.Listed{Key: key, ObjectInfo: b.info(key, b.objects[key])})
	}
	return out, nil
}

func (b *zipTestBackend) resetOps() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.opens = nil
}

func (b *zipTestBackend) opensFor(key string) []zipTestOpen {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []zipTestOpen
	for _, op := range b.opens {
		if op.key == key {
			out = append(out, op)
		}
	}
	return out
}

func newZipTestServer(t *testing.T, backend readBackend) (*Server, *httptest.Server) {
	t.Helper()
	srv := New(nil, testutil.DiscardLogger(), false, 8, time.Second, nil)
	srv.readOverride = backend
	ts := httptest.NewServer(srv)
	t.Cleanup(func() {
		ts.Close()
		srv.Close()
	})
	return srv, ts
}

func zipHTTP(t *testing.T, ts *httptest.Server, method, target string, headers map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+target, nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

func listZip(t *testing.T, ts *httptest.Server, bucket, query string) listBucketResult {
	t.Helper()
	resp, body := zipHTTP(t, ts, http.MethodGet, "/"+bucket+"?list-type=2&"+query, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, body)
	}
	var out listBucketResult
	if err := xml.Unmarshal(body, &out); err != nil {
		t.Fatalf("list XML: %v\n%s", err, body)
	}
	return out
}

func TestSplitZipKey(t *testing.T) {
	tests := []struct {
		key, archive, member string
		ok                   bool
	}{
		{"datasets/sample.zip/inner/quant.sf", "datasets/sample.zip", "inner/quant.sf", true},
		{"datasets/SAMPLE.ZIP/inner/quant.sf", "datasets/SAMPLE.ZIP", "inner/quant.sf", true},
		{"sample.zip/", "sample.zip", "", true},
		{"a.zip/b.zip/c", "a.zip", "b.zip/c", true},
		{"sample.zip", "", "", false},
		{"sample.zipx/a", "", "", false},
		{"sample.zip/../secret", "", "", false},
		{"sample.zip/a/../../secret", "", "", false},
	}
	for _, tt := range tests {
		archive, member, ok := splitZipKey(tt.key)
		if ok != tt.ok || archive != tt.archive || member != tt.member {
			t.Fatalf("splitZipKey(%q) = %q, %q, %v; want %q, %q, %v", tt.key, archive, member, ok, tt.archive, tt.member, tt.ok)
		}
	}
}

func TestZipMemberHeadGetRangeListing(t *testing.T) {
	backend := newZipTestBackend()
	quant := []byte(strings.Repeat("transcript\tlength\teffective_length\tTPM\n", 500))
	parquet := bytes.Repeat([]byte{0x50, 0x41, 0x52, 0x31, 0x00, 0xff}, 50_000)
	meta := []byte(`{"columns":["transcript","TPM"],"format":"zstd"}`)
	index := []byte("<html><body>inner index</body></html>")
	archive := buildZip(t, []zipTestFile{
		{name: "inner/quant.sf", method: zip.Deflate, body: quant},
		{name: "inner/parquet/events.parquet", method: zip.Store, body: parquet},
		{name: "inner/meta/columns.json", method: zstd.ZipMethodWinZip, body: meta},
		{name: "inner/sub/one.txt", method: zip.Deflate, body: []byte("one")},
		{name: "inner/sub/two.txt", method: zip.Deflate, body: []byte("two")},
		{name: "inner/index.html", method: zip.Store, body: index},
	})
	backend.put("sample.zip", archive)
	srv, ts := newZipTestServer(t, backend)

	resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/sample.zip/inner/quant.sf", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(quant)) {
		t.Fatalf("HEAD length=%q want %d", got, len(quant))
	}
	if got := resp.Header.Get("Content-Type"); got != "text/tab-separated-values" {
		t.Fatalf("HEAD content-type=%q", got)
	}
	if got := resp.Header.Get("Last-Modified"); got != time.Unix(1_700_000_000, 0).UTC().Format(http.TimeFormat) {
		t.Fatalf("HEAD last-modified=%q", got)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" || !strings.HasPrefix(etag, `"etag-`) {
		t.Fatalf("HEAD etag=%q", etag)
	}

	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/quant.sf", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, quant) {
		t.Fatalf("GET status=%d len=%d want=%d", resp.StatusCode, len(body), len(quant))
	}
	if got := resp.Header.Get("ETag"); got != etag {
		t.Fatalf("GET etag=%q want %q", got, etag)
	}

	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/quant.sf", map[string]string{"Range": "bytes=10-29"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, quant[10:30]) {
		t.Fatalf("range status=%d len=%d body=%q", resp.StatusCode, len(body), body)
	}
	if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes 10-29/%d", len(quant)) {
		t.Fatalf("Content-Range=%q", got)
	}

	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/quant.sf", map[string]string{"If-None-Match": etag})
	if resp.StatusCode != http.StatusNotModified || len(body) != 0 {
		t.Fatalf("If-None-Match status=%d body=%q", resp.StatusCode, body)
	}

	resp, _ = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/quant.sf", map[string]string{"If-Match": `"other"`})
	if resp.StatusCode != http.StatusPreconditionFailed {
		t.Fatalf("If-Match status=%d", resp.StatusCode)
	}

	resp, _ = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/quant.sf", map[string]string{"Range": "bytes=999999-"})
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes */%d", len(quant)) {
		t.Fatalf("invalid range status=%d headers=%v", resp.StatusCode, resp.Header)
	}

	for name, wantType := range map[string]string{
		"inner/parquet/events.parquet": "application/vnd.apache.parquet",
		"inner/meta/columns.json":      "application/json",
	} {
		resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/sample.zip/"+name, nil)
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != wantType {
			t.Fatalf("HEAD %s status=%d content-type=%q", name, resp.StatusCode, resp.Header.Get("Content-Type"))
		}
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/meta/columns.json", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, meta) {
		t.Fatalf("zstd GET status=%d body=%q", resp.StatusCode, body)
	}
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/meta/columns.json", map[string]string{"Range": "bytes=5-19"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, meta[5:20]) {
		t.Fatalf("zstd range status=%d body=%q", resp.StatusCode, body)
	}

	// A folder path serves index.html from inside the archive.
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/inner/", nil)
	if resp.StatusCode != http.StatusOK || !bytes.Equal(body, index) {
		t.Fatalf("folder index status=%d body=%q", resp.StatusCode, body)
	}

	out := listZip(t, ts, "bucket", "prefix=sample.zip/&delimiter=/")
	if len(out.CommonPrefixes) != 1 || out.CommonPrefixes[0].Prefix != "sample.zip/inner/" {
		t.Fatalf("root common prefixes = %+v", out.CommonPrefixes)
	}

	out = listZip(t, ts, "bucket", "prefix=sample.zip/inner/&delimiter=/")
	if len(out.Contents) != 2 || out.Contents[0].Key != "sample.zip/inner/index.html" || out.Contents[1].Key != "sample.zip/inner/quant.sf" {
		t.Fatalf("inner contents = %+v", out.Contents)
	}
	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, cp.Prefix)
	}
	wantPrefixes := []string{"sample.zip/inner/meta/", "sample.zip/inner/parquet/", "sample.zip/inner/sub/"}
	if strings.Join(prefixes, ",") != strings.Join(wantPrefixes, ",") {
		t.Fatalf("inner common prefixes = %v", prefixes)
	}

	// Pagination walks the same virtual keyspace.
	var all []string
	token := ""
	for {
		query := "prefix=sample.zip/inner/&max-keys=2"
		if token != "" {
			query += "&continuation-token=" + token
		}
		page := listZip(t, ts, "bucket", query)
		for _, obj := range page.Contents {
			all = append(all, obj.Key)
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
		if token == "" {
			t.Fatal("truncated page without continuation token")
		}
	}
	want := []string{
		"sample.zip/inner/index.html", "sample.zip/inner/meta/columns.json",
		"sample.zip/inner/parquet/events.parquet", "sample.zip/inner/quant.sf",
		"sample.zip/inner/sub/one.txt", "sample.zip/inner/sub/two.txt",
	}
	if strings.Join(all, ",") != strings.Join(want, ",") {
		t.Fatalf("paged keys = %v", all)
	}

	page := listZip(t, ts, "bucket", "prefix=sample.zip/inner/&start-after=sample.zip/inner/quant.sf")
	if len(page.Contents) != 2 || page.Contents[0].Key != "sample.zip/inner/sub/one.txt" {
		t.Fatalf("start-after contents = %+v", page.Contents)
	}

	idx, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "sample.zip")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := idx.member("inner/quant.sf"); !ok {
		t.Fatal("index lost deflate member")
	}
}

func TestZipStoreCutThroughCachesDataOffset(t *testing.T) {
	backend := newZipTestBackend()
	data := bytes.Repeat([]byte{0x89, 0x50, 0x41, 0x52, 0x31, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0xff}, 40_000)
	archive := buildZip(t, []zipTestFile{{name: "data/events.parquet", method: zip.Store, body: data}})
	backend.put("sample.zip", archive)

	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	dataOffset, err := zr.File[0].DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := parseZipCentralDirectory(bytes.NewReader(archive), int64(len(archive)))
	if err != nil || len(entries) != 1 {
		t.Fatalf("central entries=%v err=%v", entries, err)
	}
	headerOffset := entries[0].headerOffset

	srv, ts := newZipTestServer(t, backend)
	resp, _ := zipHTTP(t, ts, http.MethodHead, "/bucket/sample.zip/data/events.parquet", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Length"); got != fmt.Sprint(len(data)) {
		t.Fatalf("HEAD length=%q want %d", got, len(data))
	}
	backend.resetOps()
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=1000-1999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[1000:2000]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	ops := backend.opensFor("sample.zip")
	if len(ops) != 1 {
		t.Fatalf("first cut-through used %d archive requests, want 1: %+v", len(ops), ops)
	}
	if ops[0].first != headerOffset {
		t.Fatalf("first cut-through starts at %d, want local header %d", ops[0].first, headerOffset)
	}
	if ops[0].last < dataOffset+1999 {
		t.Fatalf("first cut-through ends at %d, want >= %d", ops[0].last, dataOffset+1999)
	}

	backend.resetOps()
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=1000-1999"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, data[1000:2000]) {
		t.Fatalf("repeat status=%d len=%d", resp.StatusCode, len(body))
	}
	ops = backend.opensFor("sample.zip")
	if len(ops) != 1 || ops[0].first != dataOffset+1000 || ops[0].last != dataOffset+1999 {
		t.Fatalf("cached data offset range = %+v, want [%d,%d]", ops, dataOffset+1000, dataOffset+1999)
	}

	idx, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "sample.zip")
	if err != nil {
		t.Fatal(err)
	}
	member, ok := idx.member("data/events.parquet")
	if !ok || atomic.LoadInt64(&member.dataOffset) != dataOffset {
		t.Fatalf("cached member offset = %v, want %d", member, dataOffset)
	}
}

func TestZipIndexBuildsAndLearnsOffsets(t *testing.T) {
	backend := newZipTestBackend()
	parquet := bytes.Repeat([]byte("PAR1payload"), 5000)
	quant := []byte(strings.Repeat("name\tlength\n", 400))
	meta := []byte(`{"ok":true}`)
	archive := buildZip(t, []zipTestFile{
		{name: "data/events.parquet", method: zip.Store, body: parquet},
		{name: "quant.sf", method: zip.Deflate, body: quant},
		{name: "meta.json", method: zstd.ZipMethodWinZip, body: meta},
	})
	backend.put("sample.zip", archive)

	srv, ts := newZipTestServer(t, backend)
	idx, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "sample.zip")
	if err != nil {
		t.Fatal(err)
	}
	member, ok := idx.member("data/events.parquet")
	if !ok || atomic.LoadInt64(&member.dataOffset) != -1 {
		t.Fatalf("index did not start with an unknown data offset: %+v", member)
	}
	headerOffset := member.headerOffset

	backend.resetOps()
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=100-199"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[100:200]) {
		t.Fatalf("range status=%d len=%d", resp.StatusCode, len(body))
	}
	ops := backend.opensFor("sample.zip")
	if len(ops) != 1 || ops[0].first != headerOffset {
		t.Fatalf("first range read = %+v, want local header %d", ops, headerOffset)
	}
	dataOffset := atomic.LoadInt64(&member.dataOffset)
	if dataOffset < 0 {
		t.Fatalf("data offset not learned: %+v", member)
	}

	backend.resetOps()
	resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/data/events.parquet", map[string]string{"Range": "bytes=500-599"})
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, parquet[500:600]) {
		t.Fatalf("learned range status=%d len=%d", resp.StatusCode, len(body))
	}
	ops = backend.opensFor("sample.zip")
	if len(ops) != 1 || ops[0].first != dataOffset+500 || ops[0].last != dataOffset+599 {
		t.Fatalf("learned cut-through = %+v, want [%d,%d]", ops, dataOffset+500, dataOffset+599)
	}

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"quant.sf", quant},
		{"meta.json", meta},
	} {
		resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, nil)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, tc.body) {
			t.Fatalf("%s GET status=%d body=%q", tc.name, resp.StatusCode, body)
		}
	}
}

func TestZipCompressedMemberCachesDecompressedBytes(t *testing.T) {
	backend := newZipTestBackend()
	deflated := bytes.Repeat([]byte("deflate payload 0123456789\n"), 20_000)
	compressed := bytes.Repeat([]byte("zstd payload 0123456789\n"), 20_000)
	backend.put("sample.zip", buildZip(t, []zipTestFile{
		{name: "data/deflate.txt", method: zip.Deflate, body: deflated},
		{name: "data/zstd.txt", method: zstd.ZipMethodWinZip, body: compressed},
	}))
	srv, ts := newZipTestServer(t, backend)
	if _, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "sample.zip"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"data/deflate.txt", deflated},
		{"data/zstd.txt", compressed},
	} {
		backend.resetOps()
		resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, map[string]string{"Range": "bytes=100-199"})
		if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, tc.body[100:200]) {
			t.Fatalf("%s first range status=%d len=%d", tc.name, resp.StatusCode, len(body))
		}
		if ops := backend.opensFor("sample.zip"); len(ops) == 0 {
			t.Fatalf("%s first range read no archive bytes", tc.name)
		}

		backend.resetOps()
		resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, map[string]string{"Range": "bytes=1000-1099"})
		if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, tc.body[1000:1100]) {
			t.Fatalf("%s cached range status=%d len=%d", tc.name, resp.StatusCode, len(body))
		}
		if ops := backend.opensFor("sample.zip"); len(ops) != 0 {
			t.Fatalf("%s cached range read archive bytes: %+v", tc.name, ops)
		}

		backend.resetOps()
		resp, body = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, nil)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(body, tc.body) {
			t.Fatalf("%s cached whole GET status=%d len=%d", tc.name, resp.StatusCode, len(body))
		}
		if ops := backend.opensFor("sample.zip"); len(ops) != 0 {
			t.Fatalf("%s cached whole GET read archive bytes: %+v", tc.name, ops)
		}
	}
}

func TestZipListingDelimiterJumpsPrefixes(t *testing.T) {
	backend := newZipTestBackend()
	files := make([]zipTestFile, 0, 10_000)
	for dir := range 100 {
		for file := range 100 {
			files = append(files, zipTestFile{
				name:   fmt.Sprintf("d%02d/f%03d.txt", dir, file),
				method: zip.Store,
				body:   []byte("x"),
			})
		}
	}
	backend.put("big.zip", buildZip(t, files))
	srv, ts := newZipTestServer(t, backend)
	idx, _, err := srv.loadZipIndex(context.Background(), backend, "bucket", "big.zip")
	if err != nil {
		t.Fatal(err)
	}

	rows := idx.listing("big.zip", "big.zip/", "/", "", 10)
	if len(rows) != 10 || rows[0].Key != "big.zip/d00/f000.txt" || rows[9].Key != "big.zip/d09/f000.txt" {
		t.Fatalf("first delimiter page = %+v", rows)
	}
	rows = idx.listing("big.zip", "big.zip/", "/", rows[9].Key, 10)
	if len(rows) != 10 || rows[0].Key != "big.zip/d10/f000.txt" {
		t.Fatalf("continuation delimiter page = %+v", rows)
	}

	var prefixes []string
	token := ""
	for {
		query := "prefix=big.zip/&delimiter=/&max-keys=10"
		if token != "" {
			query += "&continuation-token=" + token
		}
		page := listZip(t, ts, "bucket", query)
		if len(page.Contents) != 0 {
			t.Fatalf("delimiter listing returned contents: %+v", page.Contents)
		}
		for _, cp := range page.CommonPrefixes {
			prefixes = append(prefixes, cp.Prefix)
		}
		if !page.IsTruncated {
			break
		}
		token = page.NextContinuationToken
		if token == "" {
			t.Fatal("truncated page without continuation token")
		}
	}
	if len(prefixes) != 100 || prefixes[0] != "big.zip/d00/" || prefixes[99] != "big.zip/d99/" {
		t.Fatalf("delimited prefixes = %d first=%q last=%q", len(prefixes), prefixes[0], prefixes[len(prefixes)-1])
	}
}

func TestZipDuplicateMemberKeepsFirst(t *testing.T) {
	backend := newZipTestBackend()
	archive := buildZip(t, []zipTestFile{
		{name: "same.txt", method: zip.Store, body: []byte("first")},
		{name: "same.txt", method: zip.Store, body: []byte("second")},
	})
	entries, err := parseZipCentralDirectory(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].uncompressedSize != int64(len("first")) {
		t.Fatalf("duplicate handling = %+v", entries)
	}
	backend.put("sample.zip", archive)
	_, ts := newZipTestServer(t, backend)
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/same.txt", nil)
	if resp.StatusCode != http.StatusOK || string(body) != "first" {
		t.Fatalf("duplicate GET status=%d body=%q", resp.StatusCode, body)
	}
}

func TestZipListingAndLiteralObjectPrecedence(t *testing.T) {
	backend := newZipTestBackend()
	files := make([]zipTestFile, 0, 12)
	for i := range 10 {
		files = append(files, zipTestFile{name: fmt.Sprintf("data/f%02d.csv", i), method: zip.Deflate, body: []byte(fmt.Sprintf("id\n%d\n", i))})
	}
	files = append(files, zipTestFile{name: "override.txt", method: zip.Store, body: []byte("archive member")})
	backend.put("sample.zip", buildZip(t, files))
	backend.put("sample.zip/override.txt", []byte("real object"))
	backend.put("sample.zip/extra.txt", []byte("extra real object"))

	_, ts := newZipTestServer(t, backend)
	resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/override.txt", nil)
	if resp.StatusCode != http.StatusOK || string(body) != "real object" {
		t.Fatalf("literal object did not win: status=%d body=%q", resp.StatusCode, body)
	}
	resp, _ = zipHTTP(t, ts, http.MethodHead, "/bucket/sample.zip/override.txt", nil)
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "11" {
		t.Fatalf("literal HEAD status=%d length=%q", resp.StatusCode, resp.Header.Get("Content-Length"))
	}
	out := listZip(t, ts, "bucket", "prefix=sample.zip/")
	seen := map[string]int64{}
	for _, obj := range out.Contents {
		seen[obj.Key] = obj.Size
	}
	if seen["sample.zip/override.txt"] != int64(len("real object")) {
		t.Fatalf("literal override size = %d", seen["sample.zip/override.txt"])
	}
	if seen["sample.zip/extra.txt"] != int64(len("extra real object")) {
		t.Fatalf("literal extra missing: %v", seen)
	}
	if _, dup := seen["sample.zip/data/f00.csv"]; !dup {
		t.Fatalf("virtual members missing from merged listing: %v", seen)
	}
	if len(out.Contents) != 12 {
		t.Fatalf("merged listing has %d entries, want 12: %+v", len(out.Contents), out.Contents)
	}
}

func TestZip64ExplicitExtraAndDataDescriptors(t *testing.T) {
	backend := newZipTestBackend()
	parquet := bytes.Repeat([]byte{0x50, 0x41, 0x52, 0x31, 0x00, 0x01, 0x02}, 20_000)
	quant := []byte(strings.Repeat("tx\t1.5\n", 1000))
	meta := []byte(`{"zip64":true,"method":"zstd"}`)
	archive := buildZip64(t, []zipTestFile{
		{name: "events/part-0.parquet", method: zip.Store, body: parquet},
		{name: "quant.sf", method: zip.Deflate, body: quant},
		{name: "meta/columns.json", method: zstd.ZipMethodWinZip, body: meta},
	})
	zr, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		t.Fatalf("stdlib rejected ZIP64 fixture: %v", err)
	}
	if len(zr.File) != 3 {
		t.Fatalf("stdlib parsed %d entries", len(zr.File))
	}
	entries, err := parseZipCentralDirectory(bytes.NewReader(archive), int64(len(archive)))
	if err != nil || len(entries) != 3 {
		t.Fatalf("central entries=%v err=%v", entries, err)
	}
	for _, entry := range entries {
		if entry.headerOffset < 0 || entry.headerOffset+4 > int64(len(archive)) ||
			binary.LittleEndian.Uint32(archive[entry.headerOffset:]) != zipLocalSignature {
			t.Fatalf("ZIP64 local header not found for %s at %d", entry.name, entry.headerOffset)
		}
	}
	backend.put("sample.zip", archive)
	_, ts := newZipTestServer(t, backend)

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"events/part-0.parquet", parquet},
		{"quant.sf", quant},
		{"meta/columns.json", meta},
	} {
		resp, got := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, nil)
		if resp.StatusCode != http.StatusOK || !bytes.Equal(got, tc.body) {
			t.Fatalf("ZIP64 GET %s status=%d len=%d want=%d", tc.name, resp.StatusCode, len(got), len(tc.body))
		}
		resp, got = zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/"+tc.name, map[string]string{"Range": "bytes=7-20"})
		if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(got, tc.body[7:21]) {
			t.Fatalf("ZIP64 range %s status=%d len=%d", tc.name, resp.StatusCode, len(got))
		}
	}
	out := listZip(t, ts, "bucket", "prefix=sample.zip/&delimiter=/")
	if len(out.CommonPrefixes) != 2 || out.CommonPrefixes[0].Prefix != "sample.zip/events/" || out.CommonPrefixes[1].Prefix != "sample.zip/meta/" {
		t.Fatalf("ZIP64 prefixes = %+v", out.CommonPrefixes)
	}
	if len(out.Contents) != 1 || out.Contents[0].Key != "sample.zip/quant.sf" {
		t.Fatalf("ZIP64 contents = %+v", out.Contents)
	}
}

func TestZipRejectsMalformedAndUnsafeMembers(t *testing.T) {
	backend := newZipTestBackend()
	archive := buildZip(t, []zipTestFile{
		{name: "ok.txt", method: zip.Store, body: []byte("ok")},
		{name: "../escape.txt", method: zip.Store, body: []byte("no")},
	})
	backend.put("sample.zip", archive)
	_, ts := newZipTestServer(t, backend)

	resp, _ := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/..%2Fescape.txt", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unsafe member status=%d", resp.StatusCode)
	}
	out := listZip(t, ts, "bucket", "prefix=sample.zip/")
	if len(out.Contents) != 1 || out.Contents[0].Key != "sample.zip/ok.txt" {
		t.Fatalf("unsafe member listed: %+v", out.Contents)
	}

	backend.put("broken.zip", []byte("not a zip archive"))
	resp, _ = zipHTTP(t, ts, http.MethodGet, "/bucket/broken.zip/inner.txt", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("broken archive status=%d", resp.StatusCode)
	}
	resp, _ = zipHTTP(t, ts, http.MethodGet, "/bucket/missing.zip/inner.txt", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing archive status=%d", resp.StatusCode)
	}
}

func TestZipIndexRefreshesOnETagChange(t *testing.T) {
	backend := newZipTestBackend()
	backend.put("sample.zip", buildZip(t, []zipTestFile{{name: "value.txt", method: zip.Store, body: []byte("old")}}))
	_, ts := newZipTestServer(t, backend)
	if resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/value.txt", nil); resp.StatusCode != http.StatusOK || string(body) != "old" {
		t.Fatalf("initial GET status=%d body=%q", resp.StatusCode, body)
	}
	backend.put("sample.zip", buildZip(t, []zipTestFile{{name: "value.txt", method: zip.Store, body: []byte("new-and-longer")}}))
	if resp, body := zipHTTP(t, ts, http.MethodGet, "/bucket/sample.zip/value.txt", nil); resp.StatusCode != http.StatusOK || string(body) != "new-and-longer" {
		t.Fatalf("refreshed GET status=%d body=%q", resp.StatusCode, body)
	}
}

func TestZipRangeReaderAtPrefetchAndSegment(t *testing.T) {
	backend := newZipTestBackend()
	data := bytes.Repeat([]byte("0123456789abcdef"), 40_000)
	backend.put("blob", data)
	ra, err := newZipRangeReaderAt(context.Background(), backend, "bucket", "blob", int64(len(data)), nil)
	if err != nil {
		t.Fatal(err)
	}
	tail := data[len(data)-zipTailSize:]
	got := make([]byte, 64)
	if n, err := ra.ReadAt(got, int64(len(data))-64); err != nil || n != 64 || !bytes.Equal(got, tail[len(tail)-64:]) {
		t.Fatalf("tail read n=%d err=%v", n, err)
	}
	if ops := backend.opensFor("blob"); len(ops) != 1 || ops[0].first != int64(len(data)-zipTailSize) {
		t.Fatalf("tail prefetch ops = %+v", ops)
	}
	backend.resetOps()
	got = make([]byte, 32)
	if n, err := ra.ReadAt(got, 0); err != nil || n != 32 || !bytes.Equal(got, data[:32]) {
		t.Fatalf("head read n=%d err=%v", n, err)
	}
	ops := backend.opensFor("blob")
	if len(ops) != 1 || ops[0].first != 0 || ops[0].last < zipReadAhead-1 {
		t.Fatalf("segment read ops = %+v", ops)
	}
	if _, err := ra.ReadAt(got, int64(len(data))); !errors.Is(err, io.EOF) {
		t.Fatalf("read past end err=%v", err)
	}
}
