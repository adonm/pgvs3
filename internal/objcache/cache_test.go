package objcache

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

func TestEtagForAttrs(t *testing.T) {
	when := time.Unix(0, 42)
	if got := etagForAttrs(originAttrs{ETag: "etag", Size: 5, LastModified: when}); got != "etag" {
		t.Fatalf("real etag = %q", got)
	}
	if got := etagForAttrs(originAttrs{Size: 5, LastModified: when}); got != "5-42" {
		t.Fatalf("synthetic etag = %q", got)
	}
}

func TestOriginETag(t *testing.T) {
	m := &store.Meta{ETag: "content-md5", OriginETag: "origin-etag"}
	if got := originETag(m); got != "origin-etag" {
		t.Fatalf("origin etag = %q", got)
	}
	m.OriginETag = ""
	if got := originETag(m); got != "content-md5" {
		t.Fatalf("fallback etag = %q", got)
	}
}

func TestRegistryReplacementAndFallback(t *testing.T) {
	r := NewRegistry(nil, nil, true)
	global := Spec{Namespace: "global", Config: `{"type":"inmem"}`}
	bucket := Spec{Namespace: "bucket", Config: `{"type":"inmem"}`}
	wanted := map[string]Spec{"": global, "lake": bucket}
	r.replace(wanted)
	defer r.replace(nil)
	g, b := r.ForBucket(context.Background(), "other"), r.ForBucket(context.Background(), "lake")
	if g == nil || b == nil || g == b || g.Name() != "global" || b.Name() != "bucket" {
		t.Fatalf("unexpected global/bucket resolution: %v %v", g, b)
	}
	r.replace(wanted)
	if r.tiers[""].tier != g || r.tiers["lake"].tier != b {
		t.Fatal("unchanged specs reopened their tiers")
	}
	for key, spec := range wanted {
		spec.Config = `{"type":"unsupported"}`
		wanted[key] = spec
	}
	r.replace(wanted)
	if r.tiers[""].tier != g || r.tiers["lake"].tier != b || g.ctx.Err() != nil || b.ctx.Err() != nil {
		t.Fatal("failed replacements discarded working tiers")
	}
	global.TTL = "2m"
	r.replace(map[string]Spec{"": global})
	if r.tiers[""].tier == g || g.ctx.Err() == nil || b.ctx.Err() == nil {
		t.Fatal("replacement/removal did not close the old tiers")
	}
	if r.ForBucket(context.Background(), "lake") != r.tiers[""].tier {
		t.Fatal("removed bucket did not fall back to the global tier")
	}
	r.SetDefault(g)
	r.replace(nil)
	if r.ForBucket(context.Background(), "lake") != g {
		t.Fatal("missing global tier did not fall back to the default")
	}
}

func TestWriteValidationReleasesLocalLock(t *testing.T) {
	tier := &Tier{}
	want := errors.New("invalid body")
	unlock, err := tier.lockWrite(context.Background(), "key", func() error { return want })
	if unlock != nil || !errors.Is(err, want) {
		t.Fatalf("lockWrite = %v, %v", unlock != nil, err)
	}
	func() {
		defer func() {
			if got := recover(); got != want {
				t.Fatalf("validation panic = %v", got)
			}
		}()
		_, _ = tier.lockWrite(context.Background(), "key", func() error { panic(want) })
	}()
	done := make(chan struct{})
	go func() {
		tier.lockKey("key")()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("validation failure retained the local lock")
	}
}

func TestPlanCacheRead(t *testing.T) {
	const rangeSize = 1 << 20
	large := int64(32 << 20)

	tests := []struct {
		name     string
		res      cacheResolution
		readOnly bool
		start    int64
		end      int64
		size     int64
		ranged   bool
		want     cacheAction
	}{
		{name: "zero", size: 0, want: cacheActionZero},
		{name: "pending write-back is readable", res: cacheResolution{state: store.CachePending, valid: true}, size: 10, end: 9, want: cacheActionCached},
		{name: "read-only miss", res: cacheResolution{state: store.CacheLive}, readOnly: true, size: 10, end: 9, want: cacheActionPassThrough},
		{name: "live hit", res: cacheResolution{state: store.CacheLive, valid: true}, size: 10, end: 9, want: cacheActionCached},
		{name: "small miss", res: cacheResolution{state: store.CacheLive}, size: 10, end: 9, want: cacheActionCached},
		{name: "large range", res: cacheResolution{state: store.CacheLive}, start: 0, end: rangeSize - 1, size: large, ranged: true, want: cacheActionRange},
		{name: "large full", res: cacheResolution{state: store.CacheLive}, size: large, end: large - 1, want: cacheActionCached},
		{name: "large range too wide", res: cacheResolution{state: store.CacheLive}, start: 0, end: maxRangeCacheSpan, size: large, ranged: true, want: cacheActionPassThrough},
		{name: "no range cache", res: cacheResolution{state: store.CacheLive}, start: 0, end: rangeSize - 1, size: large, ranged: true, want: cacheActionPassThrough},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rs := int64(rangeSize)
			if tc.name == "no range cache" {
				rs = 0
			}
			got := planCacheRead(tc.res, rs, tc.readOnly, readView{Size: tc.size, start: tc.start, end: tc.end, ranged: tc.ranged})
			if got.action != tc.want {
				t.Fatalf("action = %v, want %v", got.action, tc.want)
			}
			if got.action != cacheActionZero && tc.size > 0 && got.length != tc.end-tc.start+1 {
				t.Fatalf("length = %d, want %d", got.length, tc.end-tc.start+1)
			}
		})
	}
}

func TestObjectContentLengthAndResolveRange(t *testing.T) {
	zero := Object{Size: 0}
	if got := zero.ContentLength(); got != 0 {
		t.Fatalf("0-byte ContentLength = %d, want 0", got)
	}
	ranged := Object{Size: 100, Start: 10, End: 29, Ranged: true}
	if got := ranged.ContentLength(); got != 20 {
		t.Fatalf("ranged ContentLength = %d, want 20", got)
	}

	span, err := store.Range(0, -1, 15).Resolve(100)
	if err != nil || span.Start != 85 || span.End != 99 || !span.Ranged {
		t.Fatalf("suffix range = (%+v, %v)", span, err)
	}
	span, err = store.WholeObjectRange().Resolve(100)
	if err != nil || span.Start != 0 || span.End != 99 || span.Ranged {
		t.Fatalf("full range = (%+v, %v)", span, err)
	}
}

func TestSpoolStreamMemoryAndTempFile(t *testing.T) {
	small := bytes.Repeat([]byte("a"), 1024)
	sp, clean, err := spoolStream(context.Background(), bytes.NewReader(small), int64(len(small)))
	if err != nil {
		t.Fatal(err)
	}
	defer clean()
	if sp.size != 1024 || len(sp.data) != 1024 {
		t.Fatalf("small spool size=%d data=%d", sp.size, len(sp.data))
	}

	large := bytes.Repeat([]byte("b"), memFillMax+4096)
	sp2, clean2, err := spoolStream(context.Background(), bytes.NewReader(large), int64(len(large)))
	if err != nil {
		t.Fatal(err)
	}
	defer clean2()
	if sp2.size != int64(len(large)) || sp2.data != nil {
		t.Fatalf("large spool size=%d dataPresent=%v", sp2.size, sp2.data != nil)
	}
	got, err := io.ReadAll(sp2.r)
	if err != nil || !bytes.Equal(got, large) {
		t.Fatalf("large spool read len=%d err=%v", len(got), err)
	}
}

func newTestOrigin(t *testing.T, prefix string) *origin {
	t.Helper()
	o, err := newFSBucket(Config{Type: "filesystem", Prefix: prefix, Config: map[string]any{"directory": t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = o.Close() })
	return o
}

func TestListSortsBeforeLimit(t *testing.T) {
	b := newTestOrigin(t, "")
	ctx := context.Background()
	for _, k := range []string{"edge/dir/c", "edge/a", "edge/dir/b"} {
		if _, err := b.Upload(ctx, k, strings.NewReader(k), int64(len(k)), originUpload{}); err != nil {
			t.Fatal(err)
		}
	}
	tier := &Tier{origin: b, namespace: "origin", ttl: time.Minute, retention: time.Hour, fills: make(chan struct{}, 2)}
	rows, err := tier.List(ctx, "edge", "", "", 2)
	if err != nil || len(rows) != 2 || rows[0].Key != "a" || rows[1].Key != "dir/b" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	sub, err := tier.List(ctx, "edge", "dir/b", "", 10)
	if err != nil || len(sub) != 1 || sub[0].Key != "dir/b" {
		t.Fatalf("sub=%+v err=%v", sub, err)
	}
}

func TestTempOriginPrefixIsLogicalAtIterator(t *testing.T) {
	ctx := context.Background()
	b, err := newTempBucket(Config{Prefix: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := b.Upload(ctx, "edge/a", strings.NewReader("alpha"), 5, originUpload{
		ContentType: "text/plain", UserMetadata: []string{"owner", "test"},
	}); err != nil {
		t.Fatal(err)
	}
	attrs, err := b.Attributes(ctx, "edge/a")
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != 5 || attrs.ContentType != "text/plain" ||
		len(attrs.UserMetadata) != 2 || attrs.UserMetadata[1] != "test" {
		t.Fatalf("attrs = %+v", attrs)
	}
	var keys []string
	for obj, err := range b.Iter(ctx, "edge/") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, obj.Key)
	}
	if len(keys) != 1 || keys[0] != "edge/a" {
		t.Fatalf("iter keys = %v", keys)
	}
	if _, err := b.Upload(ctx, "edge/a", strings.NewReader("again"), 5, originUpload{Precondition: store.Precondition{Mode: store.IfAbsent}}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("conditional upload error = %v", err)
	}
}

type countingBucket struct {
	originHandle
	attributes int
}

func (b *countingBucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	b.attributes++
	return b.originHandle.Attributes(ctx, key)
}

func TestTempOriginRemovesDirectoryOnClose(t *testing.T) {
	o, err := newTempBucket(Config{})
	if err != nil {
		t.Fatal(err)
	}
	dir := o.driver.(*localBucket).root.Name()
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temporary origin directory survived Close: %v", err)
	}
}

func TestLocalOverwriteIsAtomic(t *testing.T) {
	ctx := context.Background()
	temp, err := newTempBucket(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer temp.Close()
	versions := []string{strings.Repeat("a", 4096), strings.Repeat("b", 8192)}
	for _, o := range []*origin{newTestOrigin(t, ""), temp} {
		if _, err := o.Upload(ctx, "bkt/k", strings.NewReader(versions[0]), 4096, originUpload{}); err != nil {
			t.Fatal(err)
		}
		rc, attrs, err := o.GetWithAttrs(ctx, "bkt/k")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := o.Upload(ctx, "bkt/k", strings.NewReader(versions[1]), 8192, originUpload{}); err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || string(body) != versions[0] || attrs.Size != 4096 {
			t.Fatalf("open reader saw %d bytes (size %d, err %v) after overwrite", len(body), attrs.Size, err)
		}
		var readers sync.WaitGroup
		failures := make(chan error, 4)
		for range 4 {
			readers.Go(func() {
				for range 200 {
					rc, _, err := o.GetWithAttrs(ctx, "bkt/k")
					if err != nil {
						failures <- err
						return
					}
					body, err := io.ReadAll(rc)
					_ = rc.Close()
					if err != nil || (string(body) != versions[0] && string(body) != versions[1]) {
						failures <- fmt.Errorf("read %d bytes, err %v", len(body), err)
						return
					}
				}
			})
		}
		for i := range 100 {
			if _, err := o.Upload(ctx, "bkt/k", strings.NewReader(versions[i%2]), int64(len(versions[i%2])), originUpload{}); err != nil {
				t.Fatal(err)
			}
		}
		readers.Wait()
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
	}
}

func TestLocalPathRules(t *testing.T) {
	long := strings.Repeat("n", localNameMax)
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{"bkt/a", true},
		{"bkt/a b/c-d.txt", true},
		{"bkt/" + long, true},
		{"bkt/" + long + "n", false},
		{"", false},
		{"bkt//a", false},
		{"bkt/a/", false},
		{"/bkt/a", false},
		{"bkt/./a", false},
		{"bkt/../a", false},
		{"bkt/a\x00", false},
	} {
		if got := validLocalKey(tc.key); got != tc.want {
			t.Fatalf("validLocalKey(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
	if _, err := newTempBucket(Config{Prefix: "a//b"}); err == nil {
		t.Fatal("temporary origin accepted a prefix that cannot be a path")
	}
	if _, err := newFSBucket(Config{Type: "filesystem", Prefix: "../escape", Config: map[string]any{"directory": t.TempDir()}}); err == nil {
		t.Fatal("filesystem origin accepted a prefix that cannot be a path")
	}
}

func TestListDirectUsesIteratorMetadata(t *testing.T) {
	ctx := context.Background()
	mem := newTestOrigin(t, "")
	for _, key := range []string{"edge/a", "edge/b"} {
		if _, err := mem.Upload(ctx, key, strings.NewReader(key), int64(len(key)), originUpload{
			ContentType: "text/plain",
		}); err != nil {
			t.Fatal(err)
		}
	}
	origin := &countingBucket{originHandle: mem}
	tier := &Tier{origin: origin, namespace: "origin", ttl: time.Minute, retention: time.Hour}
	rows, err := tier.List(ctx, "edge", "", "", 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if origin.attributes != 0 {
		t.Fatalf("list issued %d metadata HEADs", origin.attributes)
	}
	if rows[0].Size != int64(len("edge/a")) || rows[0].CreatedAt.IsZero() {
		t.Fatalf("list metadata = %+v", rows[0])
	}
}

func TestSpoolBudgetSharedAndReleased(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	data := bytes.Repeat([]byte("x"), memFillMax+4096)
	for _, expected := range []int64{int64(len(data)), -1} {
		budget := newSpoolBudget(int64(len(data)))
		_, cleanup, err := spoolWithBudget(context.Background(), bytes.NewReader(data), expected, budget)
		if err != nil {
			t.Fatal(err)
		}
		if budget.used.Load() != int64(len(data)) {
			t.Fatalf("reserved %d", budget.used.Load())
		}
		_, _, err = spoolWithBudget(context.Background(), bytes.NewReader(data), expected, budget)
		if !errors.Is(err, ErrSpoolBusy) {
			t.Fatalf("second spool = %v", err)
		}
		cleanup()
		cleanup()
		if budget.used.Load() != 0 {
			t.Fatal("reservation leaked")
		}
		files, err := os.ReadDir(dir)
		if err != nil || len(files) != 0 {
			t.Fatalf("scratch files = %v: %v", files, err)
		}
	}
}

func TestUnknownSpoolBudgetGrowthCannotDeadlock(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	budget := newSpoolBudget(memFillMax + 2)
	data := bytes.Repeat([]byte("x"), memFillMax+4096)
	_, _, err := spoolWithBudget(context.Background(), bytes.NewReader(data), -1, budget)
	if !errors.Is(err, ErrSpoolBusy) || budget.used.Load() != 0 {
		t.Fatalf("growth failure: %v, used=%d", err, budget.used.Load())
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 0 {
		t.Fatal("failed spool left files")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := spoolWithBudget(ctx, bytes.NewReader(data), -1, budget); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err := spoolWithBudget(context.Background(), bytes.NewReader(data), 1, budget); err == nil {
		t.Fatal("accepted incorrect declared size")
	}
}

func TestSleepContextWithSyntheticTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan bool, 1)
		go func() { result <- sleepContext(ctx, time.Hour) }()
		synctest.Wait()
		cancel()
		if <-result {
			t.Fatal("cancelled sleep succeeded")
		}
		start := time.Now()
		if !sleepContext(context.Background(), time.Hour) || time.Since(start) != time.Hour {
			t.Fatal("timer did not use synthetic time")
		}
	})
}

func TestPendingWakeIsLossyHintAndInterruptsIdleWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tier := &Tier{pendingWake: make(chan struct{}, 1)}
		tier.wakePending()
		tier.wakePending()
		if len(tier.pendingWake) != 1 {
			t.Fatal("wake hints did not coalesce")
		}
		<-tier.pendingWake
		start := time.Now()
		result := make(chan bool, 1)
		go func() { result <- waitContext(context.Background(), time.Second, tier.pendingWake) }()
		synctest.Wait()
		tier.wakePending()
		if !<-result || time.Since(start) != 0 {
			t.Fatal("durable publication did not interrupt idle polling")
		}
	})
}

func TestOriginCapabilitySets(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	httpReadOnly, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{
		"base_url": srv.URL,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer httpReadOnly.Close()

	httpWritable, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{
		"base_url": srv.URL, "read_only": false,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer httpWritable.Close()

	files, err := newFSBucket(Config{Type: "filesystem", Config: map[string]any{
		"directory": t.TempDir(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()

	memory, err := newTempBucket(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()

	objects, err := newS3Bucket(Config{Type: "s3", Config: map[string]any{
		"bucket": "test-bucket", "endpoint": srv.URL,
		"access_key": "AKID", "secret_key": "SECRET",
		"bucket_lookup_type": "path",
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()

	readCaps := capRead | capRange | capMetadata | capConditional
	tests := []struct {
		name string
		o    *origin
		want originCapabilities
	}{
		{"http-read-only", httpReadOnly, readCaps},
		{"http-writable", httpWritable, readCaps | capWrite},
		{"filesystem", files, originCapabilitiesAll},
		{"inmem", memory, originCapabilitiesAll},
		{"s3", objects, originCapabilitiesAll},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.o.Capabilities(); got != tc.want {
				t.Fatalf("capabilities = %v, want %v", got, tc.want)
			}
			for _, bit := range []originCapabilities{
				capRead, capWrite, capList, capConditional, capRange, capMetadata,
			} {
				if got, want := tc.o.Supports(bit), tc.want.has(bit); got != want {
					t.Fatalf("Supports(%v) = %v, want %v (caps %v)", bit, got, want, tc.o.Capabilities())
				}
			}
			if got := tc.o.Supports(capRead | capMetadata); !got {
				t.Fatalf("combined capability check = false for %v", tc.o.Capabilities())
			}
		})
	}
}

func TestOriginCapabilityGating(t *testing.T) {
	ctx := context.Background()
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()

	readOnly, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{
		"base_url": srv.URL,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()

	if _, err := readOnly.Upload(ctx, "b/k", strings.NewReader("x"), 1, originUpload{}); !errors.Is(err, ErrOriginUnsupported) {
		t.Fatalf("read-only upload error = %v", err)
	}
	if err := readOnly.Delete(ctx, "b/k"); !errors.Is(err, ErrOriginUnsupported) {
		t.Fatalf("read-only delete error = %v", err)
	}

	iterated := 0
	for _, err := range readOnly.Iter(ctx, "b/") {
		if err != nil {
			t.Fatalf("unsupported listing yielded error: %v", err)
		}
		iterated++
	}
	if iterated != 0 {
		t.Fatalf("unsupported listing yielded %d objects", iterated)
	}
	tier := &Tier{origin: readOnly}
	if _, err := tier.List(ctx, "b", "", "", 10); !errors.Is(err, ErrOriginListingUnsupported) {
		t.Fatalf("Tier.List error = %v", err)
	}

	memory, err := newTempBucket(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	memory.caps &^= capConditional
	if _, err := memory.Upload(ctx, "k", strings.NewReader("x"), 1, originUpload{
		Precondition: store.Precondition{Mode: store.IfAbsent},
	}); !errors.Is(err, ErrOriginUnsupported) {
		t.Fatalf("conditional upload without capability = %v", err)
	}
	if _, err := memory.Upload(ctx, "k", strings.NewReader("x"), 1, originUpload{}); err != nil {
		t.Fatalf("unconditional upload = %v", err)
	}
	memory.caps &^= capRange
	if _, err := memory.GetRange(ctx, "k", 0, 1, ""); !errors.Is(err, ErrOriginUnsupported) {
		t.Fatalf("range read without capability = %v", err)
	}
}

func TestOriginErrorClassification(t *testing.T) {
	status := func(code int) error {
		return &httpStatusError{status: code, msg: http.StatusText(code)}
	}
	tests := []struct {
		name        string
		err         error
		conditional bool
		want        error
	}{
		{"http-precondition", status(http.StatusPreconditionFailed), false, errOriginPrecondition},
		{"http-conflict", status(http.StatusConflict), false, ErrOriginConflict},
		{"http-missing-conditional", status(http.StatusNotFound), true, errOriginPrecondition},
		{"http-missing", status(http.StatusNotFound), false, status(http.StatusNotFound)},
		{"fs-missing-conditional", fs.ErrNotExist, true, errOriginPrecondition},
		{"fs-missing", fs.ErrNotExist, false, fs.ErrNotExist},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mapOriginError(tc.err, tc.conditional)
			if tc.name == "http-missing" {
				var he *httpStatusError
				if !errors.As(got, &he) || he.status != http.StatusNotFound {
					t.Fatalf("error = %v, want HTTP 404", got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("error = %v, want %v", got, tc.want)
			}
		})
	}
	if !isObjectNotFound(status(http.StatusNotFound)) {
		t.Fatal("HTTP 404 was not classified as not found")
	}
	if isObjectNotFound(status(http.StatusForbidden)) {
		t.Fatal("HTTP 403 was classified as not found")
	}
	if isObjectNotFound(errOriginPrecondition) {
		t.Fatal("precondition was classified as not found")
	}
}

func TestOriginPrefixNormalization(t *testing.T) {
	for _, input := range []string{"tenant", "/tenant", "tenant/", "/tenant/"} {
		p := newOriginPrefix(input)
		if got := p.key("edge/a"); got != "tenant/edge/a" {
			t.Fatalf("key for prefix %q = %q", input, got)
		}
		if got := p.logical("tenant/edge/a"); got != "edge/a" {
			t.Fatalf("logical key for prefix %q = %q", input, got)
		}
	}
	if got := newOriginPrefix("").key("edge/a"); got != "edge/a" {
		t.Fatalf("empty prefix key = %q", got)
	}
}

func TestSharedPrefixBehavior(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		open func(t *testing.T) (*origin, string)
	}{
		{
			name: "filesystem",
			open: func(t *testing.T) (*origin, string) {
				dir := t.TempDir()
				o, err := newFSBucket(Config{Type: "filesystem", Prefix: "/tenant/", Config: map[string]any{
					"directory": dir,
				}})
				if err != nil {
					t.Fatal(err)
				}
				return o, dir
			},
		},
		{
			name: "inmem",
			open: func(t *testing.T) (*origin, string) {
				o, err := newTempBucket(Config{Prefix: "/tenant/"})
				if err != nil {
					t.Fatal(err)
				}
				return o, ""
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, dir := tc.open(t)
			defer o.Close()

			if _, err := o.Upload(ctx, "edge/a", strings.NewReader("alpha"), 5, originUpload{
				ContentType: "text/plain", UserMetadata: []string{"owner", "test"},
			}); err != nil {
				t.Fatal(err)
			}
			if dir != "" {
				if _, err := os.Stat(filepath.Join(dir, "tenant", "edge", "a")); err != nil {
					t.Fatalf("physical key was not prefixed: %v", err)
				}
			}
			attrs, err := o.Attributes(ctx, "edge/a")
			if err != nil {
				t.Fatal(err)
			}
			if attrs.Size != 5 || attrs.ContentType != "text/plain" ||
				len(attrs.UserMetadata) != 2 || attrs.UserMetadata[1] != "test" {
				t.Fatalf("attrs = %+v", attrs)
			}
			rc, err := o.GetRange(ctx, "edge/a", 1, 3, "")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(body) != "lph" {
				t.Fatalf("range body = %q", body)
			}

			keys := make([]string, 0, 1)
			for obj, err := range o.Iter(ctx, "edge/") {
				if err != nil {
					t.Fatal(err)
				}
				keys = append(keys, obj.Key)
			}
			if len(keys) != 1 || keys[0] != "edge/a" {
				t.Fatalf("iter keys = %#v, want [edge/a]", keys)
			}

			rootKeys := make([]string, 0, 1)
			for obj, err := range o.Iter(ctx, "") {
				if err != nil {
					t.Fatal(err)
				}
				rootKeys = append(rootKeys, obj.Key)
			}
			if len(rootKeys) != 1 || rootKeys[0] != "edge/a" {
				t.Fatalf("root iter keys = %#v, want [edge/a]", rootKeys)
			}
		})
	}
}

func TestOriginConformance(t *testing.T) {
	adapters := []struct {
		name    string
		factory conformanceFactory
	}{
		{"inmem", conformanceMemFactory},
		{"filesystem", conformanceFSFactory},
		{"http-writable", conformanceHTTPFactory(false)},

		{"http-read-only", conformanceHTTPFactory(true)},
		{"s3", conformanceS3Factory},
	}
	for _, adapter := range adapters {
		t.Run(adapter.name, func(t *testing.T) {
			originConformance(t, adapter.name, adapter.factory)
		})
	}
}

type conformanceFixture struct {
	handle originHandle

	caps originCapabilities

	prefixMapping bool

	conflict bool

	physicalKeys func() []string

	localPaths bool
}

type conformanceFactory func(t *testing.T, prefix string) conformanceFixture

func originConformance(t *testing.T, name string, factory conformanceFactory) {
	t.Helper()
	ctx := context.Background()

	type conformanceCase struct {
		name              string
		requires          originCapabilities
		prefix            string
		requirePrefix     bool
		requireConflict   bool
		requireLocalPaths bool
		run               func(t *testing.T, ctx context.Context, fx conformanceFixture)
	}
	cases := []conformanceCase{
		{
			name: "capabilities-and-gating",
			run:  conformanceCapabilities,
		},
		{
			name:     "upload-get-attributes",
			requires: capRead | capWrite | capRange | capMetadata,
			run:      conformanceUploadGetAttributes,
		},
		{
			name:     "metadata-and-content-type",
			requires: capRead | capWrite | capMetadata,
			run:      conformanceMetadata,
		},
		{
			name:     "delete-and-not-found",
			requires: capRead | capWrite,
			run:      conformanceDeleteNotFound,
		},
		{
			name:     "conditional-uploads",
			requires: capRead | capWrite | capConditional,
			run:      conformanceConditionalUploads,
		},
		{
			name:     "range-reads",
			requires: capRead | capWrite | capRange,
			run:      conformanceRanges,
		},
		{
			name:     "conditional-range-reads",
			requires: capRead | capWrite | capRange | capConditional,
			run:      conformanceConditionalRanges,
		},
		{
			name:     "iteration",
			requires: capRead | capWrite | capList,
			run:      conformanceIteration,
		},
		{
			name:          "prefix-mapping",
			requires:      capRead | capWrite,
			prefix:        "/tenant/acme/",
			requirePrefix: true,
			run:           conformancePrefix,
		},
		{
			name:            "conflict-mapping",
			requires:        capRead | capWrite | capConditional,
			requireConflict: true,
			run:             conformanceConflict,
		},
		{
			name:              "local-path-rules",
			requires:          capRead | capWrite | capList | capRange | capMetadata,
			requireLocalPaths: true,
			run:               conformanceLocalPaths,
		},
		{
			name: "close-is-safe",
			run:  conformanceClose,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := factory(t, tc.prefix)
			if fx.handle == nil {
				t.Fatal("factory returned a nil origin handle")
			}
			t.Cleanup(func() { _ = fx.handle.Close() })
			if tc.requirePrefix && !fx.prefixMapping {
				t.Skipf("%s does not map Config.Prefix to physical keys", name)
			}
			if tc.requireConflict && !fx.conflict {
				t.Skipf("%s test double cannot synthesize a conditional conflict", name)
			}
			if tc.requireLocalPaths && !fx.localPaths {
				t.Skipf("%s does not map keys onto local paths", name)
			}
			if tc.requires != 0 && !fx.handle.Supports(tc.requires) {
				t.Skipf("%s lacks %s (caps %s)", name, tc.requires, fx.handle.Capabilities())
			}
			tc.run(t, ctx, fx)
		})
	}
}

func conformanceCapabilities(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	h := fx.handle

	if got := h.Capabilities(); got != fx.caps {
		t.Fatalf("Capabilities() = %v, want %v", got, fx.caps)
	}
	all := []originCapabilities{capRead, capWrite, capList, capConditional, capRange, capMetadata}
	for _, bit := range all {
		if got, want := h.Supports(bit), fx.caps.has(bit); got != want {
			t.Fatalf("Supports(%v) = %v, want %v (caps %s)", bit, got, want, fx.caps)
		}
	}
	for _, required := range []originCapabilities{
		capRead | capMetadata,
		capRead | capWrite,
		capRead | capWrite | capList | capConditional | capRange | capMetadata,
	} {
		if got, want := h.Supports(required), fx.caps.has(required); got != want {
			t.Fatalf("Supports(%v) = %v, want %v (caps %s)", required, got, want, fx.caps)
		}
	}

	expectUnsupported := func(operation string, err error) {
		t.Helper()
		if !errors.Is(err, ErrOriginUnsupported) {
			t.Fatalf("%s error = %v, want ErrOriginUnsupported", operation, err)
		}
	}
	if !fx.caps.has(capRead) {
		_, err := h.Get(ctx, "cap/read.txt")
		expectUnsupported("Get", err)
	}
	if !fx.caps.has(capWrite) {
		_, err := h.Upload(ctx, "cap/write.txt", strings.NewReader("x"), 1, originUpload{})
		expectUnsupported("Upload", err)
		err = h.Delete(ctx, "cap/write.txt")
		expectUnsupported("Delete", err)
	}
	if !fx.caps.has(capConditional) {
		if fx.caps.has(capWrite) {
			_, err := h.Upload(ctx, "cap/conditional.txt", strings.NewReader("x"), 1, originUpload{
				Precondition: store.Precondition{Mode: store.IfAbsent},
			})
			expectUnsupported("conditional Upload", err)
		}
		if fx.caps.has(capRange) {
			_, err := h.GetRange(ctx, "cap/conditional.txt", 0, 1, "etag")
			expectUnsupported("conditional GetRange", err)
		}
	}
	if !fx.caps.has(capRange) {
		_, err := h.GetRange(ctx, "cap/range.txt", 0, 1, "")
		expectUnsupported("GetRange", err)
	}
	if !fx.caps.has(capMetadata) {
		_, err := h.Attributes(ctx, "cap/meta.txt")
		expectUnsupported("Attributes", err)
	}
	if !fx.caps.has(capList) {

		count := 0
		for _, err := range h.Iter(ctx, "cap/") {
			if err != nil {
				t.Fatalf("unsupported Iter yielded error: %v", err)
			}
			count++
		}
		if count != 0 {
			t.Fatalf("unsupported Iter yielded %d objects", count)
		}
		if _, err := (&Tier{origin: h}).List(ctx, "cap", "", "", 10); !errors.Is(err, ErrOriginListingUnsupported) {
			t.Fatalf("Tier.List error = %v, want ErrOriginListingUnsupported", err)
		}
	}
}

func conformanceUploadGetAttributes(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/data.txt"
	const body = "hello conformance"
	const contentType = "text/plain; charset=utf-8"

	attrs, err := fx.handle.Upload(ctx, key, strings.NewReader(body), int64(len(body)), originUpload{
		ContentType: contentType,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if attrs.Size != int64(len(body)) || attrs.ETag == "" || attrs.ContentType != contentType {
		t.Fatalf("upload attrs = %+v", attrs)
	}
	if got := conformanceGet(t, ctx, fx.handle, key); got != body {
		t.Fatalf("Get body = %q, want %q", got, body)
	}
	head, err := fx.handle.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if head.Size != attrs.Size || head.ETag != attrs.ETag || head.ContentType != attrs.ContentType {
		t.Fatalf("attributes = %+v, upload attrs = %+v", head, attrs)
	}
	if head.LastModified.IsZero() {
		t.Fatal("attributes LastModified is zero")
	}
	if got, want := conformanceGetRange(t, ctx, fx.handle, key, 6, 5, ""), body[6:11]; got != want {
		t.Fatalf("GetRange body = %q, want %q", got, want)
	}
}

func conformanceMetadata(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/meta.parquet"
	const contentType = "application/x-conformance"
	userMeta := []string{"owner", "conformance", "trace", "abc123"}

	attrs, err := fx.handle.Upload(ctx, key, strings.NewReader("meta"), 4, originUpload{
		ContentType: contentType, UserMetadata: userMeta,
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if attrs.ContentType != contentType ||
		conformanceMetaValue(attrs.UserMetadata, "owner") != "conformance" ||
		conformanceMetaValue(attrs.UserMetadata, "trace") != "abc123" {
		t.Fatalf("upload attrs = %+v", attrs)
	}
	head, err := fx.handle.Attributes(ctx, key)
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if head.ContentType != contentType ||
		conformanceMetaValue(head.UserMetadata, "owner") != "conformance" ||
		conformanceMetaValue(head.UserMetadata, "trace") != "abc123" {
		t.Fatalf("attributes = %+v", head)
	}

	const fallbackKey = "edge/fallback.parquet"
	fallback, err := fx.handle.Upload(ctx, fallbackKey, strings.NewReader("fallback"), 8, originUpload{})
	if err != nil {
		t.Fatalf("fallback Upload: %v", err)
	}
	wantType := contentTypeFor(fallbackKey)
	if fallback.ContentType != wantType {
		t.Fatalf("fallback upload ContentType = %q, want %q", fallback.ContentType, wantType)
	}
	fallbackHead, err := fx.handle.Attributes(ctx, fallbackKey)
	if err != nil {
		t.Fatalf("fallback Attributes: %v", err)
	}
	if fallbackHead.ContentType != wantType {
		t.Fatalf("fallback attributes ContentType = %q, want %q", fallbackHead.ContentType, wantType)
	}
}

func conformanceDeleteNotFound(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/delete-me.txt"

	if _, err := fx.handle.Upload(ctx, key, strings.NewReader("delete me"), 9, originUpload{}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if err := fx.handle.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := fx.handle.Get(ctx, key); !fx.handle.IsObjNotFoundErr(err) {
		t.Fatalf("Get after Delete error = %v, want not found", err)
	}
	if fx.handle.Supports(capMetadata) {
		if _, err := fx.handle.Attributes(ctx, key); !fx.handle.IsObjNotFoundErr(err) {
			t.Fatalf("Attributes after Delete error = %v, want not found", err)
		}
	}
	if fx.handle.Supports(capRange) {
		if _, err := fx.handle.GetRange(ctx, key, 0, 1, ""); !fx.handle.IsObjNotFoundErr(err) {
			t.Fatalf("GetRange after Delete error = %v, want not found", err)
		}
	}

	if err := fx.handle.Delete(ctx, key); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if _, err := fx.handle.Get(ctx, "edge/never-existed.txt"); !fx.handle.IsObjNotFoundErr(err) {
		t.Fatalf("Get missing error = %v, want not found", err)
	}

	if fx.handle.IsObjNotFoundErr(errOriginPrecondition) {
		t.Fatal("precondition was classified as not found")
	}
	if fx.handle.IsObjNotFoundErr(ErrOriginConflict) {
		t.Fatal("conflict was classified as not found")
	}
}

func conformanceConditionalUploads(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/conditional.txt"
	h := fx.handle

	if _, err := h.Upload(ctx, key, strings.NewReader("one"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfAbsent},
	}); err != nil {
		t.Fatalf("IfAbsent on missing object: %v", err)
	}
	if _, err := h.Upload(ctx, key, strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfAbsent},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("IfAbsent on existing object error = %v, want precondition", err)
	}
	conformanceRequireBody(t, ctx, h, key, "one")

	attrs, err := h.Upload(ctx, key, strings.NewReader("two"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfPresent},
	})
	if err != nil {
		t.Fatalf("IfPresent on existing object: %v", err)
	}
	if _, err := h.Upload(ctx, "edge/conditional-missing.txt", strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfPresent},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("IfPresent on missing object error = %v, want precondition", err)
	}

	if _, err := h.Upload(ctx, key, strings.NewReader("three"), 5, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: attrs.ETag},
	}); err != nil {
		t.Fatalf("IfMatch with current ETag: %v", err)
	}
	if _, err := h.Upload(ctx, key, strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: "stale-etag"},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("IfMatch with stale ETag error = %v, want precondition", err)
	}
	conformanceRequireBody(t, ctx, h, key, "three")
	if _, err := h.Upload(ctx, "edge/conditional-missing.txt", strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: "stale-etag"},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("IfMatch on missing object error = %v, want precondition", err)
	}
}

func conformanceRanges(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/range.txt"
	const body = "0123456789"

	if _, err := fx.handle.Upload(ctx, key, strings.NewReader(body), int64(len(body)), originUpload{}); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	suffixSpan, err := store.Range(0, -1, 3).Resolve(int64(len(body)))
	if err != nil || !suffixSpan.Ranged {
		t.Fatalf("suffix range was not resolved as ranged: %+v %v", suffixSpan, err)
	}
	suffixStart, suffixEnd := suffixSpan.Start, suffixSpan.End
	ranges := []struct {
		name   string
		offset int64
		length int64
		want   string
	}{
		{"whole-object", 0, int64(len(body)), body},
		{"offset", 3, 4, body[3:7]},
		{"tail", 8, 2, body[8:]},
		{"last-byte", 9, 1, body[9:]},
		{"suffix-derived", suffixStart, suffixEnd - suffixStart + 1, body[len(body)-3:]},
	}
	for _, tc := range ranges {
		t.Run(tc.name, func(t *testing.T) {
			got := conformanceGetRange(t, ctx, fx.handle, key, tc.offset, tc.length, "")
			if got != tc.want {
				t.Fatalf("GetRange(%d,%d) = %q, want %q", tc.offset, tc.length, got, tc.want)
			}
		})
	}
}

func conformanceConditionalRanges(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const key = "edge/range-conditional.txt"
	const body = "abcdef"

	attrs, err := fx.handle.Upload(ctx, key, strings.NewReader(body), int64(len(body)), originUpload{})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if got := conformanceGetRange(t, ctx, fx.handle, key, 1, 3, attrs.ETag); got != "bcd" {
		t.Fatalf("conditional GetRange body = %q, want %q", got, "bcd")
	}
	if _, err := fx.handle.GetRange(ctx, key, 1, 3, "stale-etag"); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("stale conditional GetRange error = %v, want precondition", err)
	}
	if _, err := fx.handle.GetRange(ctx, "edge/range-conditional-missing.txt", 0, 1, "stale-etag"); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("conditional GetRange on missing object error = %v, want precondition", err)
	}
	if _, err := fx.handle.GetRange(ctx, "edge/range-conditional-missing.txt", 0, 1, ""); !fx.handle.IsObjNotFoundErr(err) {
		t.Fatalf("unconditional GetRange on missing object error = %v, want not found", err)
	}
}

func conformanceIteration(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	keys := []string{"edge/dir/b", "edge/dir/a", "edge/other", "outside/z"}
	for _, key := range keys {
		if _, err := fx.handle.Upload(ctx, key, strings.NewReader(key), int64(len(key)), originUpload{
			ContentType: "text/plain",
		}); err != nil {
			t.Fatalf("Upload %s: %v", key, err)
		}
	}

	want := []string{"edge/dir/a", "edge/dir/b", "edge/other"}
	got := conformanceIterKeys(t, ctx, fx.handle, "edge/")
	if !slices.Equal(got, want) {
		t.Fatalf("Iter(edge/) keys = %#v, want %#v", got, want)
	}
	for obj, err := range fx.handle.Iter(ctx, "edge/") {
		if err != nil {
			t.Fatalf("Iter error: %v", err)
		}
		if obj.Attrs.Size == 0 || obj.Attrs.ETag == "" || obj.Attrs.LastModified.IsZero() {
			t.Fatalf("Iter attrs for %q = %+v", obj.Key, obj.Attrs)
		}
	}

	dirWant := []string{"edge/dir/a", "edge/dir/b"}
	dirGot := conformanceIterKeys(t, ctx, fx.handle, "edge/dir/")
	if !slices.Equal(dirGot, dirWant) {
		t.Fatalf("Iter(edge/dir/) keys = %#v, want %#v", dirGot, dirWant)
	}

	if got := conformanceIterKeys(t, ctx, fx.handle, "edge/missing/"); len(got) != 0 {
		t.Fatalf("Iter(edge/missing/) keys = %#v, want none", got)
	}
}

func conformancePrefix(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	const logicalKey = "edge/a"

	if _, err := fx.handle.Upload(ctx, logicalKey, strings.NewReader("alpha"), 5, originUpload{
		ContentType: "text/plain", UserMetadata: []string{"owner", "prefix"},
	}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if got := conformanceGet(t, ctx, fx.handle, logicalKey); got != "alpha" {
		t.Fatalf("Get body = %q, want %q", got, "alpha")
	}
	head, err := fx.handle.Attributes(ctx, logicalKey)
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if head.Size != 5 || head.ContentType != "text/plain" ||
		conformanceMetaValue(head.UserMetadata, "owner") != "prefix" {
		t.Fatalf("attributes = %+v", head)
	}

	if fx.handle.Supports(capList) {
		for _, dir := range []string{"", "edge/"} {
			keys := conformanceIterKeys(t, ctx, fx.handle, dir)
			if !slices.Equal(keys, []string{logicalKey}) {
				t.Fatalf("Iter(%q) keys = %#v, want [%q]", dir, keys, logicalKey)
			}
		}
	}

	if fx.physicalKeys == nil {
		return
	}
	physical := fx.physicalKeys()
	if want := "tenant/acme/edge/a"; !slices.Contains(physical, want) {
		t.Fatalf("physical keys = %#v, want to contain %q", physical, want)
	}
	if slices.Contains(physical, logicalKey) {
		t.Fatalf("physical keys = %#v, unexpectedly contain unprefixed %q", physical, logicalKey)
	}
}

func conformanceConflict(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	for _, tc := range []struct {
		name      string
		key       string
		condition store.Condition
	}{
		{"conditional", "busy/conflict.txt", store.IfAbsent},
		{"unconditional", "busy/conflict-unconditional.txt", store.Unconditional},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := fx.handle.Upload(ctx, tc.key, strings.NewReader("x"), 1, originUpload{
				Precondition: store.Precondition{Mode: tc.condition},
			})
			if !errors.Is(err, ErrOriginConflict) {
				t.Fatalf("Upload error = %v, want ErrOriginConflict", err)
			}
			if errors.Is(err, errOriginPrecondition) {
				t.Fatalf("conflict was classified as a precondition: %v", err)
			}
			if fx.handle.IsObjNotFoundErr(err) {
				t.Fatalf("conflict was classified as not found: %v", err)
			}

			if _, err := fx.handle.Get(ctx, tc.key); !fx.handle.IsObjNotFoundErr(err) {
				t.Fatalf("Get after conflict error = %v, want not found", err)
			}
		})
	}
}

func conformanceLocalPaths(t *testing.T, ctx context.Context, fx conformanceFixture) {
	t.Helper()
	h := fx.handle
	for _, key := range []string{"edge/dir/file", "edge/a/c", "edge/a.txt", "edge/a-b"} {
		if _, err := h.Upload(ctx, key, strings.NewReader(key), int64(len(key)), originUpload{}); err != nil {
			t.Fatalf("Upload %s: %v", key, err)
		}
	}
	want := []string{"edge/a-b", "edge/a.txt", "edge/a/c", "edge/dir/file"}
	if got := conformanceIterKeys(t, ctx, h, "edge/"); !slices.Equal(got, want) {
		t.Fatalf("Iter(edge/) keys = %#v, want %#v", got, want)
	}
	if got := conformanceIterKeys(t, ctx, h, "edge/a-b/"); len(got) != 0 {
		t.Fatalf("Iter below a file yielded %#v", got)
	}

	tooLong := "edge/" + strings.Repeat("n", localNameMax+1)
	for _, key := range []string{"edge//a-b", "edge/a-b/", "/edge/a-b", "edge/./a-b", "edge/../edge/a-b", tooLong} {
		if _, err := h.Upload(ctx, key, strings.NewReader("x"), 1, originUpload{}); !errors.Is(err, ErrOriginKeyUnsupported) {
			t.Fatalf("Upload %q error = %v, want ErrOriginKeyUnsupported", key, err)
		}
		if err := h.Delete(ctx, key); err != nil {
			t.Fatalf("Delete %q: %v", key, err)
		}
		if _, err := h.Get(ctx, key); !h.IsObjNotFoundErr(err) {
			t.Fatalf("Get %q error = %v, want not found", key, err)
		}
	}
	for _, key := range []string{"edge/dir", "edge/a-b/child"} {
		if _, err := h.Upload(ctx, key, strings.NewReader("x"), 1, originUpload{}); !errors.Is(err, ErrOriginKeyUnsupported) {
			t.Fatalf("conflicting Upload %q error = %v, want ErrOriginKeyUnsupported", key, err)
		}
		if _, err := h.Attributes(ctx, key); !h.IsObjNotFoundErr(err) {
			t.Fatalf("Attributes %q error = %v, want not found", key, err)
		}
		if _, err := h.GetRange(ctx, key, 0, 1, ""); !h.IsObjNotFoundErr(err) {
			t.Fatalf("GetRange %q error = %v, want not found", key, err)
		}
	}
	if got := conformanceIterKeys(t, ctx, h, "edge/"); !slices.Equal(got, want) {
		t.Fatalf("rejected keys changed the listing: %#v", got)
	}

	if err := h.Delete(ctx, "edge/dir"); err != nil {
		t.Fatalf("Delete of a directory path: %v", err)
	}
	if got := conformanceGet(t, ctx, h, "edge/dir/file"); got != "edge/dir/file" {
		t.Fatalf("deleting a directory path changed its object: %q", got)
	}
	if rc, err := h.GetRange(ctx, "edge/a-b", int64(len("edge/a-b"))+1, 1, ""); err == nil {
		_ = rc.Close()
		t.Fatal("GetRange beyond the object end succeeded")
	}
}

func conformanceClose(t *testing.T, _ context.Context, fx conformanceFixture) {
	t.Helper()
	if err := fx.handle.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := fx.handle.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func conformanceRequireBody(t *testing.T, ctx context.Context, h originHandle, key, want string) {
	t.Helper()
	if got := conformanceGet(t, ctx, h, key); got != want {
		t.Fatalf("Get %q body = %q, want %q", key, got, want)
	}
}

func conformanceGet(t *testing.T, ctx context.Context, h originHandle, key string) string {
	t.Helper()
	rc, err := h.Get(ctx, key)
	return conformanceRead(t, rc, err)
}

func conformanceGetRange(t *testing.T, ctx context.Context, h originHandle, key string, offset, length int64, ifMatch string) string {
	t.Helper()
	rc, err := h.GetRange(ctx, key, offset, length, ifMatch)
	return conformanceRead(t, rc, err)
}

func conformanceRead(t *testing.T, rc io.ReadCloser, err error) string {
	t.Helper()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if rc == nil {
		t.Fatal("read returned a nil body without an error")
	}
	defer func() {
		if err := rc.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(data)
}

func conformanceIterKeys(t *testing.T, ctx context.Context, h originHandle, dir string) []string {
	t.Helper()
	keys := make([]string, 0, 4)
	for obj, err := range h.Iter(ctx, dir) {
		if err != nil {
			t.Fatalf("Iter(%q) error: %v", dir, err)
		}
		keys = append(keys, obj.Key)
	}
	return keys
}

func conformanceMetaValue(pairs []string, name string) string {
	for i := 0; i+1 < len(pairs); i += 2 {
		if strings.EqualFold(pairs[i], name) {
			return pairs[i+1]
		}
	}
	return ""
}

func conformanceMemFactory(t *testing.T, prefix string) conformanceFixture {
	t.Helper()
	o, err := newOrigin(Config{Type: "inmem", Prefix: prefix})
	if err != nil {
		t.Fatalf("newOrigin(inmem): %v", err)
	}
	return conformanceFixture{
		handle:        o,
		caps:          originCapabilitiesAll,
		prefixMapping: true,
		localPaths:    true,
	}
}

func conformanceFSFactory(t *testing.T, prefix string) conformanceFixture {
	t.Helper()
	dir := t.TempDir()
	o, err := newOrigin(Config{Type: "filesystem", Prefix: prefix, Config: map[string]any{
		"directory": dir,
	}})
	if err != nil {
		t.Fatalf("newOrigin(filesystem): %v", err)
	}
	return conformanceFixture{
		handle:        o,
		caps:          originCapabilitiesAll,
		prefixMapping: true,
		localPaths:    true,
		physicalKeys: func() []string {
			var keys []string
			err := filepath.WalkDir(dir, func(name string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() {
					return nil
				}
				rel, err := filepath.Rel(dir, name)
				if err != nil {
					return err
				}
				keys = append(keys, filepath.ToSlash(rel))
				return nil
			})
			if err != nil {
				t.Fatalf("walk filesystem origin: %v", err)
			}
			sort.Strings(keys)
			return keys
		},
	}
}

func conformanceHTTPFactory(readOnly bool) conformanceFactory {
	return func(t *testing.T, prefix string) conformanceFixture {
		t.Helper()
		backend := newConformanceHTTPBackend()
		server := httptest.NewServer(backend)
		t.Cleanup(server.Close)

		o, err := newOrigin(Config{Type: "http", Prefix: prefix, Config: map[string]any{
			"base_url": server.URL, "read_only": readOnly,
		}})
		if err != nil {
			t.Fatalf("newOrigin(http): %v", err)
		}
		caps := capRead | capRange | capMetadata | capConditional
		if !readOnly {
			caps |= capWrite
		}
		return conformanceFixture{
			handle:        o,
			caps:          caps,
			prefixMapping: true,
			conflict:      !readOnly,
			physicalKeys: func() []string {
				backend.mu.Lock()
				defer backend.mu.Unlock()
				keys := make([]string, 0, len(backend.objects))
				for key := range backend.objects {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				return keys
			},
		}
	}
}

func conformanceS3Factory(t *testing.T, prefix string) conformanceFixture {
	t.Helper()
	fake := newFakeS3()
	server := httptest.NewServer(conformanceS3Handler(fake))
	t.Cleanup(server.Close)

	o, err := newOrigin(Config{Type: "s3", Prefix: prefix, Config: map[string]any{
		"bucket": "conformance-bucket", "endpoint": server.URL, "region": "us-east-1",
		"access_key": "AKID", "secret_key": "SECRET",
		"bucket_lookup_type": "path",
	}})
	if err != nil {
		t.Fatalf("newOrigin(s3): %v", err)
	}
	return conformanceFixture{
		handle:        o,
		caps:          originCapabilitiesAll,
		prefixMapping: true,
		conflict:      true,
		physicalKeys: func() []string {
			fake.mu.Lock()
			defer fake.mu.Unlock()
			keys := make([]string, 0, len(fake.objects))
			for key := range fake.objects {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return keys
		},
	}
}

func conformanceS3Handler(fake *fakeS3) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/busy/") {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `<Error><Code>ConditionalRequestConflict</Code><Message>conformance conflict</Message></Error>`)
			return
		}
		fake.ServeHTTP(w, r)
	})
}

type conformanceHTTPBackend struct {
	mu      sync.Mutex
	objects map[string]conformanceHTTPObject
}

type conformanceHTTPObject struct {
	data        []byte
	etag        string
	contentType string
	userMeta    map[string]string
	modified    time.Time
}

func newConformanceHTTPBackend() *conformanceHTTPBackend {
	return &conformanceHTTPBackend{objects: map[string]conformanceHTTPObject{}}
}

func (b *conformanceHTTPBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	switch r.Method {
	case http.MethodHead, http.MethodGet:
		b.read(w, r, key)
	case http.MethodPut:
		b.write(w, r, key)
	case http.MethodDelete:
		b.mu.Lock()
		delete(b.objects, key)
		b.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *conformanceHTTPBackend) read(w http.ResponseWriter, r *http.Request, key string) {
	b.mu.Lock()
	obj, ok := b.objects[key]
	b.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if ifMatch != "*" && conformanceTrimETag(ifMatch) != conformanceTrimETag(obj.etag) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
	}

	w.Header().Set("ETag", `"`+obj.etag+`"`)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	if obj.contentType != "" {
		w.Header().Set("Content-Type", obj.contentType)
	}
	for name, value := range obj.userMeta {
		w.Header().Set("x-amz-meta-"+name, value)
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(obj.data)
		}
		return
	}
	start, end, ok := conformanceParseByteRange(rangeHeader, int64(len(obj.data)))
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(obj.data)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.data[start : end+1])
	}
}

func (b *conformanceHTTPBackend) write(w http.ResponseWriter, r *http.Request, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(key, "busy/") {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusConflict)
		_, _ = io.WriteString(w, `<Error><Code>ConditionalRequestConflict</Code><Message>conformance conflict</Message></Error>`)
		return
	}

	b.mu.Lock()
	current, exists := b.objects[key]
	if strings.EqualFold(r.Header.Get("If-None-Match"), "*") && exists {
		b.mu.Unlock()
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if !exists {
			b.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		if ifMatch != "*" && conformanceTrimETag(ifMatch) != conformanceTrimETag(current.etag) {
			b.mu.Unlock()
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
	}
	obj := conformanceHTTPObject{
		data:        append([]byte(nil), data...),
		etag:        conformanceETag(data),
		contentType: r.Header.Get("Content-Type"),
		userMeta:    conformanceHTTPUserMeta(r.Header),
		modified:    time.Unix(1_700_000_000, 0).UTC(),
	}
	b.objects[key] = obj
	b.mu.Unlock()

	w.Header().Set("ETag", `"`+obj.etag+`"`)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func conformanceHTTPUserMeta(h http.Header) map[string]string {
	out := map[string]string{}
	for name, values := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			out[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return out
}

func conformanceETag(data []byte) string {
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:])
}

func conformanceTrimETag(v string) string {
	return strings.Trim(strings.TrimSpace(v), `"`)
}

func conformanceParseByteRange(value string, size int64) (start, end int64, ok bool) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	left, right, found := strings.Cut(strings.TrimPrefix(value, "bytes="), "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(left), 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if strings.TrimSpace(right) == "" {
		return start, size - 1, true
	}
	end, err = strconv.ParseInt(strings.TrimSpace(right), 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

func TestHTTPOriginPrefix(t *testing.T) {
	ctx := context.Background()
	backend := newConformanceHTTPBackend()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)

	o, err := newOrigin(Config{Type: "http", Prefix: "/tenant/acme/", Config: map[string]any{
		"base_url": server.URL, "read_only": false,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	const logicalKey = "bkt/a.txt"
	if _, err := o.Upload(ctx, logicalKey, strings.NewReader("prefix"), 6, originUpload{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	_, physicallyPrefixed := backend.objects["tenant/acme/bkt/a.txt"]
	_, unprefixed := backend.objects["bkt/a.txt"]
	_, doublePrefixed := backend.objects["tenant/acme/tenant/acme/bkt/a.txt"]
	backend.mu.Unlock()
	if !physicallyPrefixed || unprefixed || doublePrefixed {
		t.Fatalf("physical keys: prefixed=%v unprefixed=%v doublePrefixed=%v",
			physicallyPrefixed, unprefixed, doublePrefixed)
	}
	if got := conformanceGet(t, ctx, o, logicalKey); got != "prefix" {
		t.Fatalf("Get = %q, want prefix", got)
	}
	attrs, err := o.Attributes(ctx, logicalKey)
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != 6 || attrs.ContentType != "text/plain" {
		t.Fatalf("attributes = %+v", attrs)
	}
	if got := conformanceGetRange(t, ctx, o, logicalKey, 1, 3, ""); got != "ref" {
		t.Fatalf("GetRange = %q, want ref", got)
	}
	if err := o.Delete(ctx, logicalKey); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	_, remains := backend.objects["tenant/acme/bkt/a.txt"]
	backend.mu.Unlock()
	if remains {
		t.Fatal("Delete left the physical object behind")
	}
}

func TestHTTPOriginPrefixWithStripBucket(t *testing.T) {
	ctx := context.Background()
	backend := newConformanceHTTPBackend()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)

	o, err := newOrigin(Config{Type: "http", Prefix: "/tenant/", Config: map[string]any{
		"base_url": server.URL, "read_only": false, "strip_bucket": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if _, err := o.Upload(ctx, "bkt/a.txt", strings.NewReader("prefix"), 6, originUpload{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	_, physical := backend.objects["tenant/a.txt"]
	backend.mu.Unlock()
	if !physical {
		t.Fatalf("physical key did not combine strip_bucket and prefix")
	}
	if got := conformanceGet(t, ctx, o, "bkt/a.txt"); got != "prefix" {
		t.Fatalf("Get = %q, want prefix", got)
	}
	if a, err := o.Attributes(ctx, "bkt/a.txt"); err != nil || a.Size != 6 {
		t.Fatalf("Attributes = %+v err=%v", a, err)
	}
}

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
	rc, err := b.GetRange(context.Background(), "lake/dir/data.parquet", 2, 4, "")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "2345" {
		t.Fatalf("range = %q", data)
	}
	if _, err := b.Upload(context.Background(), "lake/x", strings.NewReader("x"), 1, originUpload{}); err == nil {
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
	if got := rangeChunkKey("lake/data.parquet", 1<<20, "v1"); got != "lake/data.parquet.__range__.0000000000100000.v1" {
		t.Fatalf("chunk key = %q", got)
	}
	if rangeChunkKey("lake/data.parquet", 1<<20, "v1") == rangeChunkKey("lake/data.parquet", 1<<20, "v2") {
		t.Fatal("range chunk keys do not distinguish origin versions")
	}
}

func TestHTTPOriginGetWithAttrs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", "Mon, 26 Oct 2026 02:00:00 GMT")
		w.Header().Set("x-amz-meta-test", "yes")
		_, _ = io.WriteString(w, "hello")
	}))
	defer srv.Close()
	b, err := newHTTPBucket(Config{Type: "http", Config: map[string]any{"base_url": srv.URL, "strip_bucket": true}})
	if err != nil {
		t.Fatal(err)
	}
	rc, attrs, err := b.GetWithAttrs(context.Background(), "bucket/key")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(body) != "hello" || attrs.Size != 5 || attrs.ETag != "v1" ||
		attrs.ContentType != "text/plain" || attrs.LastModified.IsZero() {
		t.Fatalf("body=%q attrs=%+v", body, attrs)
	}
}

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]*fakeS3Object
	heads   int
	lists   int
}

type fakeS3Object struct {
	data        []byte
	etag        string
	contentType string
	meta        map[string]string
	modified    time.Time
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string]*fakeS3Object{}}
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil {
		f.error(w, http.StatusBadRequest, "InvalidRequest", "bad path")
		return
	}
	decoded = strings.TrimPrefix(decoded, "/")
	_, key, _ := strings.Cut(decoded, "/")
	switch r.Method {
	case http.MethodHead:
		f.head(w, key)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			f.list(w, r)
		} else {
			f.get(w, r, key)
		}
	case http.MethodPut:
		f.put(w, r, key)
	case http.MethodDelete:
		f.mu.Lock()
		delete(f.objects, key)
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		f.error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method)
	}
}

func (f *fakeS3) head(w http.ResponseWriter, key string) {
	f.mu.Lock()
	f.heads++
	obj, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		f.error(w, http.StatusNotFound, "NoSuchKey", "missing")
		return
	}
	f.headers(w, obj)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) get(w http.ResponseWriter, r *http.Request, key string) {
	f.mu.Lock()
	obj, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		f.error(w, http.StatusNotFound, "NoSuchKey", "missing")
		return
	}
	if v := r.Header.Get("If-Match"); v != "" {
		if v != "*" && trimQuotes(v) != trimQuotes(obj.etag) {
			f.error(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag changed")
			return
		}
	}
	f.headers(w, obj)
	value := r.Header.Get("Range")
	if value == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)
		return
	}
	start, end, ok := fakeRange(value, int64(len(obj.data)))
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(obj.data)))
		f.error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "bad range")
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(obj.data[start : end+1])
}

func (f *fakeS3) put(w http.ResponseWriter, r *http.Request, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		f.error(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	current, exists := f.objects[key]
	if strings.EqualFold(r.Header.Get("If-None-Match"), "*") && exists {
		f.error(w, http.StatusPreconditionFailed, "PreconditionFailed", "exists")
		return
	}
	if v := r.Header.Get("If-Match"); v != "" {
		if !exists {
			f.error(w, http.StatusNotFound, "NoSuchKey", "missing")
			return
		}
		if v != "*" && trimQuotes(v) != trimQuotes(current.etag) {
			f.error(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag changed")
			return
		}
	}
	obj := &fakeS3Object{
		data: append([]byte(nil), data...), etag: fakeETag(data),
		contentType: r.Header.Get("Content-Type"), meta: fakeMeta(r.Header),
		modified: time.Now().UTC().Truncate(time.Second),
	}
	f.objects[key] = obj
	w.Header().Set("ETag", obj.etag)
	w.WriteHeader(http.StatusOK)
}

func (f *fakeS3) list(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists++
	prefix := r.URL.Query().Get("prefix")
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	type object struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	}
	result := struct {
		XMLName      xml.Name `xml:"ListBucketResult"`
		IsTruncated  bool     `xml:"IsTruncated"`
		EncodingType string   `xml:"EncodingType"`
		Contents     []object `xml:"Contents"`
	}{EncodingType: "url"}
	for _, key := range keys {
		obj := f.objects[key]
		result.Contents = append(result.Contents, object{
			Key: url.PathEscape(key), LastModified: obj.modified.Format(time.RFC3339Nano),
			ETag: obj.etag, Size: int64(len(obj.data)),
		})
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

func (f *fakeS3) headers(w http.ResponseWriter, obj *fakeS3Object) {
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	if obj.contentType != "" {
		w.Header().Set("Content-Type", obj.contentType)
	}
	for key, value := range obj.meta {
		w.Header().Set("x-amz-meta-"+key, value)
	}
}

func (f *fakeS3) error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: message})
}

func fakeETag(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func fakeMeta(h http.Header) map[string]string {
	out := map[string]string{}
	for name, values := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			out[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return out
}

func trimQuotes(v string) string { return strings.Trim(v, `"`) }

func fakeRange(value string, size int64) (start, end int64, ok bool) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	left, right, found := strings.Cut(strings.TrimPrefix(value, "bytes="), "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if right == "" {
		return start, size - 1, true
	}
	end, err = strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, min(end, size-1), true
}

func TestS3OriginMapping(t *testing.T) {
	fake := newFakeS3()
	server := httptest.NewServer(fake)
	defer server.Close()

	origin, err := newS3Bucket(Config{
		Type:   "s3",
		Prefix: "tenant",
		Config: map[string]any{
			"bucket": "test-bucket", "endpoint": server.URL,
			"access_key": "AKID", "secret_key": "SECRET",
			"bucket_lookup_type": "path",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer origin.Close()
	ctx := context.Background()

	attrs, err := origin.Upload(ctx, "bkt/meta.txt", strings.NewReader("alpha"), 5, originUpload{
		ContentType: "text/plain", UserMetadata: []string{"owner", "origin"},
		Precondition: store.Precondition{Mode: store.IfAbsent},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attrs.Size != 5 || attrs.ETag == "" || attrs.ContentType != "text/plain" {
		t.Fatalf("upload attrs = %+v", attrs)
	}
	if len(attrs.UserMetadata) != 2 || attrs.UserMetadata[0] != "owner" || attrs.UserMetadata[1] != "origin" {
		t.Fatalf("upload metadata = %#v", attrs.UserMetadata)
	}

	if _, err := origin.Upload(ctx, "bkt/meta.txt", strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfAbsent},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("If-Absent overwrite = %v", err)
	}
	if _, err := origin.Upload(ctx, "bkt/meta.txt", strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: "00000000000000000000000000000000"},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("stale If-Match = %v", err)
	}
	if _, err := origin.Upload(ctx, "bkt/missing.txt", strings.NewReader("bad"), 3, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: "00000000000000000000000000000000"},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("If-Match on missing object = %v", err)
	}

	head, err := origin.Attributes(ctx, "bkt/meta.txt")
	if err != nil {
		t.Fatal(err)
	}
	if head.ContentType != "text/plain" || len(head.UserMetadata) != 2 || head.UserMetadata[1] != "origin" {
		t.Fatalf("attributes = %+v", head)
	}
	if _, err := origin.Attributes(ctx, "bkt/missing.txt"); !origin.IsObjNotFoundErr(err) {
		t.Fatalf("missing attributes = %v", err)
	}

	full, err := origin.Get(ctx, "bkt/meta.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(full)
	_ = full.Close()
	if string(body) != "alpha" {
		t.Fatalf("full body = %q", body)
	}
	part, err := origin.GetRange(ctx, "bkt/meta.txt", 1, 3, attrs.ETag)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(part)
	_ = part.Close()
	if string(body) != "lph" {
		t.Fatalf("range body = %q", body)
	}
	if _, err := origin.GetRange(ctx, "bkt/meta.txt", 1, 3, "wrong"); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("wrong range If-Match = %v", err)
	}

	for _, key := range []string{"bkt/dir/a", "bkt/dir/b", "other/z"} {
		if _, err := origin.Upload(ctx, key, strings.NewReader(key), int64(len(key)), originUpload{}); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	for obj, err := range origin.Iter(ctx, "bkt/") {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, obj.Key)
		if obj.Attrs.Size == 0 || obj.Attrs.ETag == "" || obj.Attrs.LastModified.IsZero() {
			t.Fatalf("iter attrs = %+v", obj.Attrs)
		}
	}
	want := []string{"bkt/dir/a", "bkt/dir/b", "bkt/meta.txt"}
	if len(keys) != len(want) {
		t.Fatalf("iter keys = %#v", keys)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("iter keys = %#v, want %#v", keys, want)
		}
	}

	fake.mu.Lock()
	before := fake.heads
	fake.mu.Unlock()
	for _, err := range origin.Iter(ctx, "bkt/dir/") {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	fake.mu.Lock()
	after := fake.heads
	fake.mu.Unlock()
	if after != before {
		t.Fatalf("listing issued %d HEADs", after-before)
	}
}
