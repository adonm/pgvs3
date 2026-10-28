//go:build integration

package objcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
)

var (
	testStore *store.Store
	testCtx   = context.Background()
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
		testStore = st
		return st.Close, nil
	}))
}

func dropOnCleanup(t *testing.T, kind ns.Kind, name string) {
	t.Helper()
	t.Cleanup(func() { _, _ = testStore.DropNamespace(testCtx, kind, name, true) })
}

type fixture struct {
	tier *Tier
	dir  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	tier, err := New(testutil.DiscardLogger(), cfg, testStore,
		fmt.Sprintf("origin-%d", time.Now().UnixNano()), 0, 24*time.Hour, 4)
	testutil.Must(t, err)
	dropOnCleanup(t, ns.WriteBack, tier.namespace)
	dropOnCleanup(t, ns.Cache, tier.namespace)
	t.Cleanup(func() { _ = tier.Close() })
	return &fixture{tier: tier, dir: dir}
}

func (f *fixture) readOrigin(t *testing.T, key string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, key))
	testutil.Must(t, err)
	return string(b)
}

func readObject(t *testing.T, obj *Object) string {
	t.Helper()
	defer obj.Body.Close()
	b, err := io.ReadAll(obj.Body)
	testutil.Must(t, err)
	return string(b)
}

var errOriginDown = errors.New("origin down")

type flakyOrigin struct {
	originHandle
	down atomic.Bool
}

func (f *flakyOrigin) Upload(ctx context.Context, key string, r io.Reader, size int64, opts originUpload) (originAttrs, error) {
	if f.down.Load() {
		return originAttrs{}, errOriginDown
	}
	return f.originHandle.Upload(ctx, key, r, size, opts)
}

func readObjectErr(obj *Object) (string, error) {
	defer obj.Body.Close()
	b, err := io.ReadAll(obj.Body)
	return string(b), err
}

func TestTierLifecycle(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "a.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	if got := f.readOrigin(t, "bkt/a.txt"); got != "alpha" {
		t.Fatalf("origin = %q", got)
	}
	head, err := f.tier.Head(ctx, "bkt", "a.txt")
	testutil.Must(t, err)
	if head.Size != 5 || head.ContentType != "text/plain" || head.ETag == "" {
		t.Fatalf("head = %+v", head)
	}
	obj, err := f.tier.Open(ctx, "bkt", "a.txt", store.WholeObjectRange())
	testutil.Must(t, err)
	if got := readObject(t, obj); got != "alpha" {
		t.Fatalf("open = %q", got)
	}
	obj, err = f.tier.Open(ctx, "bkt", "a.txt", store.Range(1, 3, -1))
	testutil.Must(t, err)
	if got := readObject(t, obj); got != "lph" {
		t.Fatalf("range = %q", got)
	}
	testutil.Must(t, os.WriteFile(filepath.Join(f.dir, "bkt", "b.txt"), []byte("beta"), 0o644))
	rows, err := f.tier.List(ctx, "bkt", "", "", 10)
	testutil.Must(t, err)
	if len(rows) != 2 || rows[0].Key != "a.txt" || rows[1].Key != "b.txt" {
		t.Fatalf("list = %+v", rows)
	}
	results := f.tier.DeleteMany(ctx, "bkt", []string{"a.txt", "missing"})
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil {
		t.Fatalf("delete results = %+v", results)
	}
	if _, err := os.Stat(filepath.Join(f.dir, "bkt", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("origin object still exists: %v", err)
	}
	rows, err = f.tier.List(ctx, "bkt", "", "", 10)
	if err != nil || len(rows) != 1 || rows[0].Key != "b.txt" {
		t.Fatalf("list after delete = %+v err=%v", rows, err)
	}
	if _, err := f.tier.Head(ctx, "bkt", "a.txt"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("head missing = %v", err)
	}
	t.Run("etag consistency", func(t *testing.T) {
		ctx := testCtx
		f := newFixture(t)
		if _, err := f.tier.Upload(ctx, "bkt", "a.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
			t.Fatal(err)
		}
		head, err := f.tier.Head(ctx, "bkt", "a.txt")
		testutil.Must(t, err)
		obj, err := f.tier.Open(ctx, "bkt", "a.txt", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "alpha" {
			t.Fatalf("open = %q", got)
		}
		rows, err := f.tier.List(ctx, "bkt", "", "", 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("list = %+v err=%v", rows, err)
		}
		if head.ETag != obj.ETag || head.ETag != rows[0].ETag {
			t.Fatalf("etags differ: head=%q open=%q list=%q", head.ETag, obj.ETag, rows[0].ETag)
		}
	})
}

func TestTierPendingCompletion(t *testing.T) {
	t.Run("multipart completion retries after origin recovers", func(t *testing.T) {
		ctx := testCtx
		f := newFixture(t)
		origin := &flakyOrigin{originHandle: f.tier.origin}
		f.tier.origin = origin
		uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp-fail", nil, "text/plain", "", nil)
		testutil.Must(t, err)
		w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-fail", 1, 0)
		testutil.Must(t, err)
		if _, err := w.Write([]byte("nope")); err != nil {
			t.Fatal(err)
		}
		pub, err := w.Finish(nil)
		testutil.Must(t, err)
		origin.down.Store(true)
		if _, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-fail",
			[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{}); err == nil {
			t.Fatal("expected origin failure")
		}
		head, err := f.tier.Head(ctx, "bkt", "mp-fail")
		if err != nil || head.Size != 4 || !head.Hit {
			t.Fatalf("pending completion head = %+v err=%v", head, err)
		}
		if n := f.pending(t, "mp-fail"); n != 1 {
			t.Fatalf("pending rows after failure = %d, want 1", n)
		}
		origin.down.Store(false)
		testutil.Eventually(t, 15*time.Second, "outbox worker did not clear the pending row", func() bool { return f.pending(t, "mp-fail") == 0 })
		if got := f.readOrigin(t, "bkt/mp-fail"); got != "nope" {
			t.Fatalf("origin after retry = %q", got)
		}
	})
	t.Run("cache body served while origin is down", func(t *testing.T) {
		ctx := testCtx
		f := newFixture(t)
		if _, err := f.tier.Upload(ctx, "bkt", "mp-old", strings.NewReader("old"), nil, "text/plain"); err != nil {
			t.Fatal(err)
		}
		origin := &flakyOrigin{originHandle: f.tier.origin}
		f.tier.origin = origin
		uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp-old", nil, "text/plain", "", nil)
		testutil.Must(t, err)
		w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-old", 1, 0)
		testutil.Must(t, err)
		if _, err := w.Write([]byte("new!")); err != nil {
			t.Fatal(err)
		}
		pub, err := w.Finish(nil)
		testutil.Must(t, err)
		origin.down.Store(true)
		if _, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-old",
			[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{}); err == nil {
			t.Fatal("expected origin failure")
		}
		obj, err := f.tier.Open(ctx, "bkt", "mp-old", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "new!" {
			t.Fatalf("pending read = %q, want completed cache body", got)
		}
		if n := f.pending(t, "mp-old"); n != 1 {
			t.Fatalf("pending rows after failed completion = %d, want 1", n)
		}
		origin.down.Store(false)
		testutil.Eventually(t, 15*time.Second, "outbox worker did not clear the pending row", func() bool { return f.pending(t, "mp-old") == 0 })
		if got := f.readOrigin(t, "bkt/mp-old"); got != "new!" {
			t.Fatalf("origin after retry = %q", got)
		}
	})
}

func (f *fixture) pending(t *testing.T, key string) int {
	t.Helper()
	var n int
	if err := testStore.Pool().QueryRow(testCtx,
		"SELECT count(*) FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2",
		f.tier.namespace, f.tier.key("bkt", key)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

type s3Fixture struct {
	tier   *Tier
	client *s3client.Bucket
}

func newS3Fixture(t *testing.T, prefix string) *s3Fixture {
	t.Helper()
	cfg, err := testutil.S3()
	testutil.Must(t, err)
	bucket := fmt.Sprintf("pgvs3-test-%d", time.Now().UnixNano())
	client, err := s3client.New(s3client.Config{
		Endpoint: cfg.Endpoint, Bucket: bucket, AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey, Insecure: true, PathStyle: true, Prefix: prefix,
	})
	testutil.Must(t, err)
	testutil.Must(t, client.CreateBucket(testCtx))
	t.Cleanup(func() {
		ctx := context.WithoutCancel(testCtx)
		for obj, err := range client.Walk(ctx, "") {
			if err != nil {
				break
			}
			_ = client.Delete(ctx, obj.Key)
		}
		_ = client.DeleteBucket(ctx)
		_ = client.Close()
	})
	originCfg := fmt.Appendf(nil, `{
		"type":"s3",
		"prefix":%q,
		"config":{
			"bucket":%q,
			"endpoint":%q,
			"access_key":%q,
			"secret_key":%q,
			"insecure":true,
			"bucket_lookup_type":"path"
		}
	}`, prefix, bucket, cfg.Endpoint, cfg.AccessKey, cfg.SecretKey)
	tier, err := New(testutil.DiscardLogger(), originCfg, testStore,
		fmt.Sprintf("origin-s3-%d", time.Now().UnixNano()), 0, 24*time.Hour, 4)
	testutil.Must(t, err)
	t.Cleanup(func() { _ = tier.Close() })
	return &s3Fixture{tier: tier, client: client}
}

func (f *s3Fixture) readOrigin(t *testing.T, key string) string {
	t.Helper()
	obj, err := f.client.Get(testCtx, key, 0, -1, "")
	testutil.Must(t, err)
	defer obj.Close()
	b, err := io.ReadAll(obj)
	testutil.Must(t, err)
	return string(b)
}

func TestTierS3PrefixMetadataAndConditions(t *testing.T) {
	ctx := testCtx
	f := newS3Fixture(t, "tenant")
	if _, err := f.tier.UploadWithOptions(ctx, "bkt", "meta.txt", strings.NewReader("alpha"), UploadOptions{
		UserMetadata: []string{"owner", "origin", "x-amz-acl", "public-read"},
		ContentType:  "text/plain", Precondition: store.Precondition{Mode: store.IfAbsent},
	}); err != nil {
		t.Fatal(err)
	}
	attrs, err := f.tier.origin.Attributes(ctx, f.tier.key("bkt", "meta.txt"))
	testutil.Must(t, err)
	metaValue := func(pairs []string, name string) string {
		for i := 0; i+1 < len(pairs); i += 2 {
			if pairs[i] == name {
				return pairs[i+1]
			}
		}
		return ""
	}
	if attrs.ETag == "" || metaValue(attrs.UserMetadata, "owner") != "origin" ||
		metaValue(attrs.UserMetadata, "x-amz-acl") != "public-read" {
		t.Fatalf("origin attrs = %+v", attrs)
	}
	if _, err := f.tier.UploadWithOptions(ctx, "bkt", "meta.txt", strings.NewReader("bad"), UploadOptions{
		Precondition: store.Precondition{Mode: store.IfAbsent},
	}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("If-None-Match:* error = %v", err)
	}
	if _, err := f.tier.UploadWithOptions(ctx, "bkt", "meta.txt", strings.NewReader("bad"), UploadOptions{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: "00000000000000000000000000000000"},
	}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("stale If-Match error = %v", err)
	}
	if got := f.readOrigin(t, "bkt/meta.txt"); got != "alpha" {
		t.Fatalf("origin after failed preconditions = %q", got)
	}
	if _, err := f.tier.Copy(ctx, "bkt", "meta.txt", "bkt", "copy.txt", store.CopyOptions{
		Source: store.ReadCondition{IfMatch: "00000000000000000000000000000000"},
	}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("copy source precondition error = %v", err)
	}
	if _, err := f.tier.Copy(ctx, "bkt", "meta.txt", "bkt", "copy.txt", store.CopyOptions{
		Source: store.ReadCondition{IfNoneMatch: attrs.ETag},
	}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("copy source If-None-Match error = %v", err)
	}
	if _, err := f.tier.Copy(ctx, "bkt", "meta.txt", "bkt", "copy.txt", store.CopyOptions{
		Source: store.ReadCondition{IfMatch: attrs.ETag}, Dest: store.Precondition{Mode: store.IfAbsent},
	}); err != nil {
		t.Fatal(err)
	}
	if got := f.readOrigin(t, "bkt/copy.txt"); got != "alpha" {
		t.Fatalf("conditional copy origin = %q", got)
	}
	if _, err := f.tier.Copy(ctx, "bkt", "meta.txt", "bkt", "copy.txt", store.CopyOptions{
		Dest: store.Precondition{Mode: store.IfAbsent},
	}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("copy destination IfAbsent error = %v", err)
	}
	rc, err := f.tier.origin.GetRange(ctx, f.tier.key("bkt", "meta.txt"), 0, attrs.Size,
		"00000000000000000000000000000000")
	if err == nil {
		_, err = io.ReadAll(rc)
		_ = rc.Close()
	}
	if !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("conditional GET error = %v", err)
	}

	if _, err := f.tier.store.Cache(f.tier.namespace).Del(ctx, []string{f.tier.key("bkt", "meta.txt")}); err != nil {
		t.Fatal(err)
	}
	head, err := f.tier.Head(ctx, "bkt", "meta.txt")
	testutil.Must(t, err)
	if head.ContentType != "text/plain" || metaValue(head.UserMetadata, "owner") != "origin" ||
		metaValue(head.UserMetadata, "x-amz-acl") != "public-read" {
		t.Fatalf("origin metadata after eviction = %+v", head)
	}

	for _, key := range []string{"dir/a", "dir/b", "other"} {
		if _, err := f.tier.Upload(ctx, "bkt", key, strings.NewReader(key), nil, "text/plain"); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := f.tier.List(ctx, "bkt", "dir/", "", 10)
	testutil.Must(t, err)
	if len(rows) != 2 || rows[0].Key != "dir/a" || rows[1].Key != "dir/b" {
		t.Fatalf("prefixed list = %+v", rows)
	}
}

func TestUnsupportedKeysRejectedBeforeCommit(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	f.tier.EnableWriteBack(true)
	if _, err := f.tier.Upload(ctx, "bkt", "source", strings.NewReader("source"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	multipart := func(opts store.WriteOptions) func(*testing.T, *fixture, string) error {
		return func(t *testing.T, f *fixture, key string) error {
			uploadID, err := f.tier.CreateUpload(ctx, "bkt", key, nil, "text/plain", "", nil)
			testutil.Must(t, err)
			defer f.tier.AbortUpload(ctx, uploadID, "bkt", key)
			w, err := f.tier.StartPart(ctx, uploadID, "bkt", key, 1, 0)
			testutil.Must(t, err)
			if _, err := w.Write([]byte("part")); err != nil {
				t.Fatal(err)
			}
			pub, err := w.Finish(nil)
			testutil.Must(t, err)
			_, err = f.tier.CompleteUpload(ctx, uploadID, "bkt", key, []store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, opts)
			return err
		}
	}
	large := bytes.Repeat([]byte("w"), writeBackMin+1)
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, f *fixture, key string) error
	}{
		{"put write-through", func(t *testing.T, f *fixture, key string) error {
			_, err := f.tier.UploadWithOptions(ctx, "bkt", key, strings.NewReader("small"), UploadOptions{})
			return err
		}},
		{"put write-back", func(t *testing.T, f *fixture, key string) error {
			_, err := f.tier.UploadWithOptions(ctx, "bkt", key, bytes.NewReader(large), UploadOptions{})
			return err
		}},
		{"copy", func(t *testing.T, f *fixture, key string) error {
			_, err := f.tier.Copy(ctx, "bkt", "source", "bkt", key, store.CopyOptions{})
			return err
		}},
		{"multipart write-back", multipart(store.WriteOptions{})},
		{"multipart write-through", multipart(store.WriteOptions{Precondition: store.Precondition{Mode: store.IfAbsent}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "dir//" + strings.ReplaceAll(tc.name, " ", "-")
			if err := tc.write(t, f, key); !errors.Is(err, ErrOriginKeyUnsupported) {
				t.Fatalf("error = %v, want ErrOriginKeyUnsupported", err)
			}
			ck := f.tier.key("bkt", key)
			if _, err := testStore.Meta(ctx, f.tier.writeRef(ck)); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("write-back row was committed: %v", err)
			}
			if _, err := testStore.Meta(ctx, f.tier.cacheRef(ck)); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("cache row was committed: %v", err)
			}
			if n := f.pending(t, key); n != 0 {
				t.Fatalf("%d pending origin uploads were queued", n)
			}
			if _, err := f.tier.origin.Attributes(ctx, ck); !f.tier.origin.IsObjNotFoundErr(err) {
				t.Fatalf("origin object exists: %v", err)
			}
		})
	}
}

type blockingOrigin struct {
	originHandle
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (b *blockingOrigin) Upload(ctx context.Context, key string, r io.Reader, size int64, opts originUpload) (originAttrs, error) {
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return originAttrs{}, ctx.Err()
	}
	return b.originHandle.Upload(ctx, key, r, size, opts)
}

type blockingGetOrigin struct {
	originHandle
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingGetOrigin) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	b.calls.Add(1)
	rc, attrs, err := b.originHandle.GetWithAttrs(ctx, key)
	if err != nil {
		return nil, originAttrs{}, err
	}
	b.once.Do(func() { close(b.entered) })
	select {
	case <-b.release:
		return rc, attrs, nil
	case <-ctx.Done():
		_ = rc.Close()
		return nil, originAttrs{}, ctx.Err()
	}
}

func TestFillCoalescing(t *testing.T) {
	ctx := testCtx
	mem := newTestOrigin(t, "")
	if _, err := mem.Upload(ctx, "bkt/shared", strings.NewReader("shared-fill"), 11, originUpload{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-cross-fill-%d", time.Now().UnixNano())
	for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
		testutil.Must(t, testStore.EnsureNamespace(ctx, kind, cache, nil, nil))
	}
	t.Cleanup(func() {
		for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
			_, _ = testStore.DropNamespace(ctx, kind, cache, true)
		}
	})
	blocked := &blockingGetOrigin{
		originHandle: mem,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	newTier := func() *Tier {
		return &Tier{
			origin: blocked, store: testStore, namespace: cache, ttl: 0, retention: time.Hour,
			fills: make(chan struct{}, 2), completes: make(chan struct{}, 2),
			log: testutil.DiscardLogger(), workerID: randomID(),
			cancel: func() {},
		}
	}
	first, second := newTier(), newTier()
	defer first.Close()
	defer second.Close()
	type result struct {
		body string
		err  error
	}
	firstCh := make(chan result, 1)
	go func() {
		obj, err := first.Open(ctx, "bkt", "shared", store.WholeObjectRange())
		if err != nil {
			firstCh <- result{err: err}
			return
		}
		body, err := readObjectErr(obj)
		firstCh <- result{body: body, err: err}
	}()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first tier did not reach origin")
	}
	secondCh := make(chan result, 1)
	go func() {
		obj, err := second.Open(ctx, "bkt", "shared", store.WholeObjectRange())
		if err != nil {
			secondCh <- result{err: err}
			return
		}
		body, err := readObjectErr(obj)
		secondCh <- result{body: body, err: err}
	}()
	time.Sleep(100 * time.Millisecond)
	close(blocked.release)
	for i, ch := range []chan result{firstCh, secondCh} {
		select {
		case res := <-ch:
			if res.err != nil || res.body != "shared-fill" {
				t.Fatalf("tier %d result body=%q err=%v", i, res.body, res.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("tier %d timed out", i)
		}
	}
	if got := blocked.calls.Load(); got != 1 {
		t.Fatalf("origin calls = %d, want 1", got)
	}
	t.Run("cancelled waiter does not cancel owner", func(t *testing.T) {
		f := newFixture(t)
		ck := f.tier.key("bkt", "shared")
		if _, err := f.tier.origin.Upload(testCtx, ck, strings.NewReader("shared"), 6, originUpload{}); err != nil {
			t.Fatal(err)
		}
		blocked := &blockingGetOrigin{originHandle: f.tier.origin, entered: make(chan struct{}), release: make(chan struct{})}
		f.tier.origin = blocked
		owner := make(chan error, 1)
		go func() {
			_, err := f.tier.fill(testCtx, ck, "shared")
			owner <- err
		}()
		select {
		case <-blocked.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("owner did not reach origin")
		}
		ctx, cancel := context.WithCancel(testCtx)
		cancel()
		if _, err := f.tier.fill(ctx, ck, "shared"); !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v", err)
		}
		close(blocked.release)
		select {
		case err := <-owner:
			testutil.Must(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("owner did not finish")
		}
		if got := blocked.calls.Load(); got != 1 {
			t.Fatalf("origin GETs = %d, want 1", got)
		}
	})
}

func TestStaleFillCannotOverwriteNewerCacheVersion(t *testing.T) {
	ctx := testCtx
	mem := newTestOrigin(t, "")
	if _, err := mem.Upload(ctx, "bkt/race", strings.NewReader("old-version"), 11, originUpload{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-fill-%d", time.Now().UnixNano())
	blocked := &blockingGetOrigin{
		originHandle: mem,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	newTier := func(origin originHandle) *Tier {
		return &Tier{
			origin: origin, store: testStore, namespace: cache, ttl: 0, retention: time.Hour,
			fills: make(chan struct{}, 2), completes: make(chan struct{}, 2),
			log: testutil.DiscardLogger(), workerID: randomID(),
			cancel: func() {},
		}
	}
	slow, fast := newTier(blocked), newTier(mem)
	defer slow.Close()
	defer fast.Close()

	type result struct {
		obj *Object
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		obj, err := slow.Open(ctx, "bkt", "race", store.WholeObjectRange())
		resCh <- result{obj: obj, err: err}
	}()
	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("slow fill did not reach the origin")
	}
	const newer = "new-version-longer"
	if _, err := fast.Upload(ctx, "bkt", "race", strings.NewReader(newer), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	res := <-resCh
	if res.err != nil {
		t.Fatal(res.err)
	}
	if got := readObject(t, res.obj); got != newer {
		t.Fatalf("stale fill served %q, want the newer version", got)
	}
	m, err := testStore.MetaHeader(ctx, store.ObjectRef{NS: ns.WriteBackRef(cache), Key: slow.key("bkt", "race")})
	testutil.Must(t, err)
	if m.Size != int64(len(newer)) {
		t.Fatalf("write-back size after stale fill = %d, want %d", m.Size, len(newer))
	}
	if res.obj.ETag != m.ETag {
		t.Fatalf("response ETag %q does not match write-back ETag %q", res.obj.ETag, m.ETag)
	}
}

func TestDeleteDuringFillCannotResurrectCache(t *testing.T) {
	ctx := testCtx
	mem := newTestOrigin(t, "")
	if _, err := mem.Upload(ctx, "bkt/delete-race", strings.NewReader("delete-me"), 9, originUpload{
		ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-delete-%d", time.Now().UnixNano())
	blocked := &blockingGetOrigin{
		originHandle: mem,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	newTier := func(origin originHandle) *Tier {
		return &Tier{
			origin: origin, store: testStore, namespace: cache, ttl: 0, retention: time.Hour,
			fills: make(chan struct{}, 2), completes: make(chan struct{}, 2),
			log: testutil.DiscardLogger(), workerID: randomID(),
			cancel: func() {},
		}
	}
	slow, fast := newTier(blocked), newTier(mem)
	defer slow.Close()
	defer fast.Close()

	resCh := make(chan error, 1)
	go func() {
		_, err := slow.Open(ctx, "bkt", "delete-race", store.WholeObjectRange())
		resCh <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("slow fill did not reach the origin")
	}
	if err := fast.Delete(ctx, "bkt", "delete-race"); err != nil {
		t.Fatalf("delete during fill: %v", err)
	}
	close(blocked.release)
	if err := <-resCh; !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("fill after delete error = %v, want ErrNotFound", err)
	}
	m, err := testStore.MetaHeader(ctx, store.ObjectRef{NS: ns.CacheRef(cache), Key: slow.key("bkt", "delete-race")})
	if err != nil {
		t.Fatalf("tombstone metadata: %v", err)
	}
	if !store.IsDeletedOrigin(m.OriginETag) {
		t.Fatalf("cache row was not replaced by a tombstone: origin=%q", m.OriginETag)
	}
	if _, err := fast.Open(ctx, "bkt", "delete-race", store.WholeObjectRange()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("open after delete = %v, want ErrNotFound", err)
	}
}

func TestStaleMultipartFlushCannotOverwriteNewerWrite(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "race", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	base, err := f.tier.origin.Attributes(ctx, f.tier.key("bkt", "race"))
	testutil.Must(t, err)
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "race", nil, "text/plain", "", nil)
	testutil.Must(t, err)
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "race", 1, 0)
	testutil.Must(t, err)
	if _, err := w.Write([]byte("stale")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	testutil.Must(t, err)
	exp := time.Now().Add(time.Hour)
	if _, err := testStore.CompleteUploadPending(ctx, store.ObjectRef{NS: ns.WriteBackRef(f.tier.namespace), Key: f.tier.key("bkt", "race")}, uploadID, []store.ListedPart{{PartNo: 1, ETag: pub.ETag}},
		store.WriteOptions{ExpiresAt: &exp}, store.PendingCompletion{BaseETag: base.ETag, LeaseOwner: "blocked"}); err != nil {
		t.Fatal(err)
	}

	blocked := &blockingOrigin{
		originHandle: f.tier.origin,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	f.tier.origin = blocked
	pending := store.PendingUpload{
		Key: f.tier.key("bkt", "race"), Token: store.PendingOriginToken(uploadID),
		BaseETag: base.ETag, LeaseOwner: "blocked",
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := f.tier.flushOne(ctx, pending, "blocked")
		errCh <- err
	}()
	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("stale flush did not reach the origin")
	}
	testutil.Must(t, os.WriteFile(filepath.Join(f.dir, "bkt", "race"), []byte("new"), 0o644))
	close(blocked.release)
	if err := <-errCh; !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("stale flush error = %v, want ErrPrecondition", err)
	}
	if got := f.readOrigin(t, "bkt/race"); got != "new" {
		t.Fatalf("origin after stale flush = %q", got)
	}
	if n := f.pending(t, "race"); n != 0 {
		t.Fatalf("pending rows after stale flush = %d", n)
	}
	head, err := f.tier.Head(ctx, "bkt", "race")
	if err != nil || head.Size != 3 {
		t.Fatalf("cache head after race = %+v err=%v", head, err)
	}
}

func TestReadOnlyTier(t *testing.T) {
	ctx := testCtx
	dir := t.TempDir()
	testutil.Must(t, os.MkdirAll(filepath.Join(dir, "bkt"), 0o755))
	testutil.Must(t, os.WriteFile(filepath.Join(dir, "bkt", "cold.txt"), []byte("cold"), 0o644))
	cache := fmt.Sprintf("origin-ro-%d", time.Now().UnixNano())
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	seed, err := New(testutil.DiscardLogger(), cfg, testStore, cache, 0, time.Hour, 2)
	testutil.Must(t, err)
	if _, err := seed.Upload(ctx, "bkt", "warm.txt", strings.NewReader("older"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	seed.Close()
	testutil.Must(t, os.WriteFile(filepath.Join(dir, "bkt", "warm.txt"), []byte("newer"), 0o644))

	tier, err := NewReadOnly(testutil.DiscardLogger(), cfg, testStore, cache, 0, time.Hour, 2)
	testutil.Must(t, err)
	defer tier.Close()
	head, err := tier.Head(ctx, "bkt", "cold.txt")
	if err != nil || head.Size != 4 {
		t.Fatalf("cold head = %+v err=%v", head, err)
	}
	warm, err := tier.Open(ctx, "bkt", "warm.txt", store.WholeObjectRange())
	testutil.Must(t, err)
	if got := readObject(t, warm); got != "newer" {
		t.Fatalf("stale read-only open = %q", got)
	}
	if _, err := testStore.MetaHeader(ctx, store.ObjectRef{NS: ns.WriteBackRef(cache), Key: tier.key("bkt", "warm.txt")}); err != nil {
		t.Fatalf("read-only tier deleted a stale cache row: %v", err)
	}
	if _, err := tier.Upload(ctx, "bkt", "blocked", strings.NewReader("x"), nil, "text/plain"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Upload error = %v", err)
	}
	obj, err := tier.Open(ctx, "bkt", "cold.txt", store.WholeObjectRange())
	testutil.Must(t, err)
	if got := readObject(t, obj); got != "cold" {
		t.Fatalf("cold open = %q", got)
	}
	rows, err := tier.List(ctx, "bkt", "", "", 10)
	if err != nil || len(rows) != 2 || rows[0].Key != "cold.txt" || rows[1].Key != "warm.txt" {
		t.Fatalf("read-only list = %+v err=%v", rows, err)
	}
	if _, err := testStore.MetaHeader(ctx, store.ObjectRef{NS: ns.CacheRef(cache), Key: tier.key("bkt", "cold.txt")}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("read-only tier wrote cache metadata: %v", err)
	}
	t.Run("durable write-back row", func(t *testing.T) {
		ctx := testCtx
		cache := fmt.Sprintf("origin-ro-skip-%d", time.Now().UnixNano())
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.WriteBack, cache, nil, nil))
		dropOnCleanup(t, ns.WriteBack, cache)
		dropOnCleanup(t, ns.Cache, cache)
		if _, err := testStore.PutInline(ctx, store.ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/warm"},
			[]byte("durable"), store.WriteOptions{ContentType: "text/plain", OriginETag: "origin-v1"}); err != nil {
			t.Fatal(err)
		}

		if _, err := testStore.DropNamespace(ctx, ns.Cache, cache, true); err != nil {
			t.Fatal(err)
		}
		origin := newTestOrigin(t, "")
		tier := &Tier{
			origin: origin, store: testStore, namespace: cache, ttl: time.Hour,
			retention: time.Hour, readOnly: true, fills: make(chan struct{}, 2),
			completes: make(chan struct{}, 2), log: testutil.DiscardLogger(),
		}
		defer tier.Close()
		head, err := tier.Head(ctx, "bkt", "warm")
		if err != nil || head.Size != int64(len("durable")) {
			t.Fatalf("read-only head = %+v err=%v", head, err)
		}
		obj, err := tier.Open(ctx, "bkt", "warm", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "durable" {
			t.Fatalf("read-only open = %q", got)
		}
		rows, err := tier.List(ctx, "bkt", "", "", 10)
		if err != nil || len(rows) != 1 || rows[0].Key != "warm" {
			t.Fatalf("read-only list = %+v err=%v", rows, err)
		}
	})
	t.Run("shared cache after origin removal", func(t *testing.T) {
		ctx := testCtx
		dir := t.TempDir()
		testutil.Must(t, os.MkdirAll(filepath.Join(dir, "bucket"), 0o755))
		testutil.Must(t, os.WriteFile(filepath.Join(dir, "bucket", "key"), []byte("shared"), 0o644))
		cache := fmt.Sprintf("origin-shared-%d", time.Now().UnixNano())
		dropOnCleanup(t, ns.WriteBack, cache)
		dropOnCleanup(t, ns.Cache, cache)
		cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
		writer, err := New(testutil.DiscardLogger(), cfg, testStore, cache, time.Hour, time.Hour, 2)
		testutil.Must(t, err)
		defer writer.Close()
		obj, err := writer.Open(ctx, "bucket", "key", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "shared" {
			t.Fatalf("writer open = %q", got)
		}
		testutil.Must(t, os.Remove(filepath.Join(dir, "bucket", "key")))
		reader, err := NewReadOnly(testutil.DiscardLogger(), cfg, testStore, cache, time.Hour, time.Hour, 2)
		testutil.Must(t, err)
		defer reader.Close()
		obj, err = reader.Open(ctx, "bucket", "key", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "shared" {
			t.Fatalf("read-only shared-cache open = %q", got)
		}
	})
}

func TestResponseShape(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "full.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(ctx, "bkt", "full.txt", store.Range(0, -1, -1))
	testutil.Must(t, err)
	if !obj.Ranged {
		t.Fatal("explicit bytes=0- range was not marked as ranged")
	}
	if got := readObject(t, obj); got != "alpha" {
		t.Fatalf("body = %q", got)
	}
	t.Run("zero-byte object", func(t *testing.T) {
		ctx := testCtx
		f := newFixture(t)
		if _, err := f.tier.origin.Upload(ctx, f.tier.key("bkt", "zero"), strings.NewReader(""), 0, originUpload{}); err != nil {
			t.Fatal(err)
		}
		obj, err := f.tier.Open(ctx, "bkt", "zero", store.WholeObjectRange())
		testutil.Must(t, err)
		if got := readObject(t, obj); got != "" || obj.Size != 0 || obj.Ranged {
			t.Fatalf("zero object = %+v body=%q", obj, got)
		}
	})
}

func TestMultipartCompleteOriginPrecondition(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "cond", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	head, err := f.tier.Head(ctx, "bkt", "cond")
	testutil.Must(t, err)
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "cond", nil, "text/plain", "", nil)
	testutil.Must(t, err)
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "cond", 1, 0)
	testutil.Must(t, err)
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	testutil.Must(t, err)
	listed := []store.ListedPart{{PartNo: 1, ETag: pub.ETag}}
	if _, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "cond", listed,
		store.WriteOptions{Precondition: store.Precondition{Mode: store.IfMatch, ETag: "wrong"}}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("wrong If-Match error = %v, want ErrPrecondition", err)
	}
	if _, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "cond", listed,
		store.WriteOptions{Precondition: store.Precondition{Mode: store.IfAbsent}}); !errors.Is(err, store.ErrPrecondition) {
		t.Fatalf("If-None-Match error = %v, want ErrPrecondition", err)
	}
	if _, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "cond", listed,
		store.WriteOptions{Precondition: store.Precondition{Mode: store.IfMatch, ETag: head.ETag}}); err != nil {
		t.Fatalf("matching If-Match: %v", err)
	}
	if got := f.readOrigin(t, "bkt/cond"); got != "new" {
		t.Fatalf("origin after conditional completion = %q", got)
	}
}

type methodCounterOrigin struct {
	originHandle
	gets  atomic.Int32
	heads atomic.Int32
}

func (c *methodCounterOrigin) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	c.gets.Add(1)
	return c.originHandle.GetWithAttrs(ctx, key)
}

func (c *methodCounterOrigin) Attributes(ctx context.Context, key string) (originAttrs, error) {
	c.heads.Add(1)
	return c.originHandle.Attributes(ctx, key)
}

func TestColdWholeReadUsesOneOriginRequest(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.origin.Upload(ctx, f.tier.key("bkt", "cold.txt"), strings.NewReader("cold"), 4, originUpload{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	f.tier.ttl = time.Hour
	counted := &methodCounterOrigin{originHandle: f.tier.origin}
	f.tier.origin = counted

	obj, err := f.tier.Open(ctx, "bkt", "cold.txt", store.WholeObjectRange())
	testutil.Must(t, err)
	if got := readObject(t, obj); got != "cold" {
		t.Fatalf("body = %q", got)
	}
	if got := counted.gets.Load(); got != 1 {
		t.Fatalf("GetWithAttrs calls = %d, want 1", got)
	}
	if got := counted.heads.Load(); got != 0 {
		t.Fatalf("Attributes calls = %d, want 0", got)
	}

	obj, err = f.tier.Open(ctx, "bkt", "cold.txt", store.WholeObjectRange())
	testutil.Must(t, err)
	_ = readObject(t, obj)
	if counted.gets.Load() != 1 || counted.heads.Load() != 0 {
		t.Fatalf("warm read contacted origin: gets=%d heads=%d", counted.gets.Load(), counted.heads.Load())
	}
}

func TestWriteBackAcks(t *testing.T) {
	body := bytes.Repeat([]byte("p"), 9<<20)
	for _, tc := range []struct {
		name  string
		key   string
		write func(ctx context.Context, f *fixture, key string) (int64, error)
		want  []byte
	}{
		{"put", "writeback", func(ctx context.Context, f *fixture, key string) (int64, error) {
			m, err := f.tier.Upload(ctx, "bkt", key, bytes.NewReader(body), nil, "application/octet-stream")
			if err != nil {
				return 0, err
			}
			return m.Size, nil
		}, body},
		{"multipart", "mp-writeback", func(ctx context.Context, f *fixture, key string) (int64, error) {
			uploadID, err := f.tier.CreateUpload(ctx, "bkt", key, nil, "text/plain", "", nil)
			if err != nil {
				return 0, err
			}
			w, err := f.tier.StartPart(ctx, uploadID, "bkt", key, 1, 0)
			if err != nil {
				return 0, err
			}
			if _, err := w.Write([]byte("async")); err != nil {
				return 0, err
			}
			pub, err := w.Finish(nil)
			if err != nil {
				return 0, err
			}
			done, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", key,
				[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{})
			if err != nil {
				return 0, err
			}
			return done.Size, nil
		}, []byte("async")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx
			f := newFixture(t)
			f.tier.EnableWriteBack(true)
			blocked := &blockingOrigin{
				originHandle: f.tier.origin,
				entered:      make(chan struct{}),
				release:      make(chan struct{}),
			}
			f.tier.origin = blocked
			release := func() {
				select {
				case <-blocked.release:
				default:
					close(blocked.release)
				}
			}
			defer release()
			uctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			size, err := tc.write(uctx, f, tc.key)
			if err != nil {
				t.Fatalf("write-back did not acknowledge before the origin: %v", err)
			}
			if size != int64(len(tc.want)) {
				t.Fatalf("write-back size = %d, want %d", size, len(tc.want))
			}
			obj, err := f.tier.Open(ctx, "bkt", tc.key, store.WholeObjectRange())
			testutil.Must(t, err)
			if got := readObject(t, obj); got != string(tc.want) {
				t.Fatalf("cached write-back body length = %d, want %d", len(got), len(tc.want))
			}
			if f.pending(t, tc.key) != 1 {
				t.Fatal("write-back did not enqueue an origin upload")
			}
			select {
			case <-blocked.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("outbox worker did not attempt the origin upload")
			}
			release()
			testutil.Eventually(t, 20*time.Second, "outbox worker did not commit the write-back", func() bool { return f.pending(t, tc.key) == 0 })
			if got := f.readOrigin(t, "bkt/"+tc.key); got != string(tc.want) {
				t.Fatalf("origin after write-back length = %d, want %d", len(got), len(tc.want))
			}
		})
	}
}

func TestListMergesPendingWriteBackObjects(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.origin.Upload(ctx, f.tier.key("bkt", "a"), strings.NewReader("a"), 1, originUpload{}); err != nil {
		t.Fatal(err)
	}
	f.tier.EnableWriteBack(true)
	blocked := &blockingOrigin{
		originHandle: f.tier.origin,
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
	f.tier.origin = blocked
	release := func() {
		select {
		case <-blocked.release:
		default:
			close(blocked.release)
		}
	}
	defer release()

	body := bytes.Repeat([]byte("p"), 9<<20)
	if _, err := f.tier.Upload(ctx, "bkt", "b", bytes.NewReader(body), nil, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	first, err := f.tier.List(ctx, "bkt", "", "", 1)
	if err != nil || len(first) != 1 || first[0].Key != "a" {
		t.Fatalf("first page = %+v err=%v", first, err)
	}
	second, err := f.tier.List(ctx, "bkt", "", "a", 1)
	if err != nil || len(second) != 1 || second[0].Key != "b" || second[0].Size != int64(len(body)) {
		t.Fatalf("second page = %+v err=%v", second, err)
	}
	release()
	testutil.Eventually(t, 20*time.Second, "outbox worker did not commit pending listing object", func() bool { return f.pending(t, "b") == 0 })
}

func TestTierModelAcrossWorkers(t *testing.T) {
	ctx := testCtx
	mem, err := newTempBucket(Config{})
	testutil.Must(t, err)
	cache := fmt.Sprintf("origin-model-%d", time.Now().UnixNano())
	log := testutil.DiscardLogger()
	newTier := func() *Tier {
		return &Tier{
			origin: mem, store: testStore, namespace: cache, ttl: 0, retention: time.Hour,
			fills: make(chan struct{}, 2), completes: make(chan struct{}, 2),
			log: log, workerID: randomID(), cancel: func() {},
		}
	}
	first, second := newTier(), newTier()
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	dropOnCleanup(t, "cache", cache)
	tiers := []*Tier{first, second}

	model := map[string]string{}
	keys := []string{"a", "b", "c", "d", "e"}
	rng := rand.New(rand.NewSource(7))

	check := func(step int) {
		t.Helper()
		for _, tier := range tiers {
			for _, key := range keys {
				obj, err := tier.Open(ctx, "bkt", key, store.WholeObjectRange())
				want, exists := model[key]
				if !exists {
					if !errors.Is(err, store.ErrNotFound) {
						t.Fatalf("step %d open %s: err=%v, want not found", step, key, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("step %d open %s: %v", step, key, err)
				}
				got := readObject(t, obj)
				if got != want {
					t.Fatalf("step %d open %s = %q, want %q", step, key, got, want)
				}
				head, err := tier.Head(ctx, "bkt", key)
				if err != nil {
					t.Fatalf("step %d head %s: %v", step, key, err)
				}
				if head.Size != int64(len(want)) {
					t.Fatalf("step %d head %s size=%d, want %d", step, key, head.Size, len(want))
				}
			}
		}
	}

	for step := 0; step < 120; step++ {
		tier := tiers[rng.Intn(len(tiers))]
		key := keys[rng.Intn(len(keys))]
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5:
			value := fmt.Sprintf("value-%d-%s", step, strings.Repeat("x", rng.Intn(40)))
			if _, err := tier.Upload(ctx, "bkt", key, strings.NewReader(value), nil, "text/plain"); err != nil {
				t.Fatalf("step %d upload %s: %v", step, key, err)
			}
			model[key] = value
		case 6, 7:
			if err := tier.Delete(ctx, "bkt", key); err != nil {
				t.Fatalf("step %d delete %s: %v", step, key, err)
			}
			delete(model, key)
		default:

		}
		if step%8 == 0 {
			check(step)
		}
	}
	check(120)
}

func TestCacheResolutionLifecycle(t *testing.T) {
	f := newFixture(t)
	ctx := testCtx
	for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
		for _, readOnly := range []bool{false, true} {
			for _, state := range []string{"fresh", "revalidate", "stale", "pending", "deleted", "missing-origin"} {
				t.Run(fmt.Sprintf("%s/readonly=%t/%s", kind, readOnly, state), func(t *testing.T) {
					key := fmt.Sprintf("%s-%t-%s", kind, readOnly, state)
					ck := f.tier.key("bkt", key)
					a, err := f.tier.origin.Upload(ctx, ck, strings.NewReader("hello"), 5, originUpload{})
					testutil.Must(t, err)
					version := versionFor(a)
					switch state {
					case "stale":
						version = "old-origin-version"
					case "pending":
						version = store.PendingOriginToken("test")
					case "deleted":
						version = store.DeletedCacheVersion("test").String()
					case "missing-origin":
						testutil.Must(t, f.tier.origin.Delete(ctx, ck))
					}
					ref := store.ObjectRef{NS: ns.Ref{Kind: kind, Name: f.tier.namespace}, Key: ck}
					m, err := testStore.PutInline(ctx, ref, []byte("hello"), store.WriteOptions{OriginETag: version})
					testutil.Must(t, err)
					f.tier.readOnly, f.tier.ttl = readOnly, 0
					if state == "fresh" {
						f.tier.ttl = time.Hour
					}
					res, err := f.tier.resolveCache(ctx, ck, true, false, false)
					if state == "deleted" || state == "missing-origin" {
						if !errors.Is(err, store.ErrNotFound) {
							t.Fatalf("error = %v, want not found", err)
						}
						return
					}
					if err != nil || res.valid != (state != "stale") {
						t.Fatalf("valid=%t err=%v", res.valid, err)
					}
					wantETag := a.ETag
					if state != "stale" && (kind == ns.WriteBack || state == "pending") {
						wantETag = m.ETag
					}
					if res.info.ETag != wantETag {
						t.Fatalf("etag=%q, want %q", res.info.ETag, wantETag)
					}
					_, err = testStore.MetaHeader(ctx, ref)
					if state == "stale" && !readOnly {
						if !errors.Is(err, store.ErrNotFound) {
							t.Fatalf("stale row survived eviction: %v", err)
						}
					} else if err != nil {
						t.Fatalf("read removed a row: %v", err)
					}
				})
			}
		}
	}
}

func TestSpooledPublicationMetadata(t *testing.T) {
	f := newFixture(t)
	f.tier.cancel()
	<-f.tier.done
	data := bytes.Repeat([]byte("encoded"), 16<<10)
	sp, cleanup, err := spoolStream(testCtx, bytes.NewReader(data), int64(len(data)))
	testutil.Must(t, err)
	defer cleanup()
	sum := sha256.Sum256(data)
	checksum := &store.Checksum{Name: "sha256", Value: base64.StdEncoding.EncodeToString(sum[:])}
	modified := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, pending := range []bool{false, true} {
		opts := store.WriteOptions{
			ContentType: "application/parquet", ContentEncoding: "gzip", OriginModified: modified,
			OriginETag: "origin-version", Checksum: checksum, ChecksumType: "FULL_OBJECT",
		}
		if pending {
			opts.OriginETag = store.PendingOriginToken("metadata-test")
			opts.PendingOrigin = &store.PendingOrigin{Token: opts.OriginETag}
		}
		ref := f.tier.writeRef(fmt.Sprintf("bkt/metadata-%t", pending))
		m, err := f.tier.storeSpooled(testCtx, ref, sp, opts)
		testutil.Must(t, err)
		stored, err := testStore.MetaHeader(testCtx, ref)
		testutil.Must(t, err)
		for _, info := range []store.ObjectInfo{m.ObjectInfo, stored.ObjectInfo} {
			if info.ContentEncoding != opts.ContentEncoding || info.ContentType != opts.ContentType ||
				!info.OriginModified.Equal(modified) || info.ChecksumName != checksum.Name ||
				info.ChecksumValue != checksum.Value || info.ChecksumType != opts.ChecksumType {
				t.Fatalf("publication lost metadata: %+v", info)
			}
		}
		if (m.ExpiresAt == nil) != pending || (stored.ExpiresAt == nil) != pending {
			t.Fatalf("pending=%t returned expiry=%v stored expiry=%v", pending, m.ExpiresAt, stored.ExpiresAt)
		}
	}
}

func TestRangeParentMetadataSharedAndReadOnly(t *testing.T) {
	f := newFixture(t)
	f.tier.ttl = time.Hour
	f.tier.EnableRangeCache(64 << 10)
	data := bytes.Repeat([]byte("parquet"), 2<<20)
	ck := f.tier.key("bkt", "ranges")
	if _, err := f.tier.origin.Upload(testCtx, ck, bytes.NewReader(data), int64(len(data)), originUpload{ContentType: "application/parquet", UserMetadata: []string{"owner", "test"}}); err != nil {
		t.Fatal(err)
	}
	counted := &countingBucket{originHandle: f.tier.origin}
	f.tier.origin = counted
	rng := store.Range(100, 199, -1)
	for pass := range 2 {
		obj, err := f.tier.Open(testCtx, "bkt", "ranges", rng)
		testutil.Must(t, err)
		if obj.Hit != (pass > 0) {
			t.Fatalf("range pass %d hit=%t", pass, obj.Hit)
		}
		if got := readObject(t, obj); got != string(data[100:200]) {
			t.Fatal("incorrect range")
		}
	}
	if counted.attributes != 1 {
		t.Fatalf("warm range issued %d HEADs, want 1 total", counted.attributes)
	}
	if _, err := testStore.Meta(testCtx, f.tier.cacheRef(ck)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("metadata became a body: %v", err)
	}

	ro, err := NewReadOnly(nil, []byte(`{"type":"inmem"}`), testStore, f.tier.namespace, time.Hour, time.Hour, 1)
	testutil.Must(t, err)
	defer ro.Close()
	ro.EnableRangeCache(64 << 10)
	obj, err := ro.Open(testCtx, "bkt", "ranges", rng)
	testutil.Must(t, err)
	if got := readObject(t, obj); got != string(data[100:200]) {
		t.Fatal("replica-shaped tier missed shared range")
	}
	if _, err := testStore.Pool().Exec(testCtx, `UPDATE "s3c_`+f.tier.namespace+`".origin_metadata SET validated_at = now()-interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	obj, err = f.tier.Open(testCtx, "bkt", "ranges", rng)
	testutil.Must(t, err)
	_ = obj.Body.Close()
	if counted.attributes != 2 {
		t.Fatalf("expired metadata did not revalidate: %d", counted.attributes)
	}
	testutil.Must(t, f.tier.Delete(testCtx, "bkt", "ranges"))
	if _, err := ro.Open(testCtx, "bkt", "ranges", rng); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("delete exposed metadata/ranges: %v", err)
	}
}

func TestParentHeadPublicationFencedByTombstone(t *testing.T) {
	f := newFixture(t)
	ck := f.tier.key("bkt", "fenced-head")
	info := store.ObjectInfo{Size: 10 << 20, OriginETag: "old", ETag: "old", ContentType: "application/parquet"}
	testutil.Must(t, f.tier.fenceReadCache(testCtx, ck))
	testutil.Must(t, testStore.Cache(f.tier.namespace).RememberOriginMetadata(testCtx, ck, info, time.Now(), time.Hour))
	got, err := testStore.Cache(f.tier.namespace).OriginMetadata(testCtx, ck, time.Hour)
	if err != nil || got != nil {
		t.Fatalf("late HEAD resurrected metadata: %+v %v", got, err)
	}
}

func TestFillRenewal(t *testing.T) {
	f := newFixture(t)
	f.tier.fillTTL = 300 * time.Millisecond
	ck := f.tier.key("bkt", "slow-renewal")
	if _, err := f.tier.origin.Upload(testCtx, ck, strings.NewReader("body"), 4, originUpload{}); err != nil {
		t.Fatal(err)
	}
	blocked := &blockingGetOrigin{originHandle: f.tier.origin, entered: make(chan struct{}), release: make(chan struct{})}
	f.tier.origin = blocked
	result := make(chan error, 1)
	go func() { _, err := f.tier.fill(testCtx, ck, "slow-renewal"); result <- err }()
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("fill not started")
	}

	select {
	case err := <-result:
		t.Fatalf("fill ended early: %v", err)
	case <-time.After(900 * time.Millisecond):
	}
	if got, err := testStore.Cache(f.tier.namespace).AcquireFillLease(testCtx, ck, "competitor", time.Minute); err != nil || got {
		t.Fatalf("renewed fill lost ownership: %t %v", got, err)
	}

	if _, err := testStore.Pool().Exec(testCtx, `UPDATE s3p.cache_fill_leases SET owner='replacement' WHERE namespace=$1 AND key=$2`, f.tier.namespace, ck); err != nil {
		t.Fatal(err)
	}
	close(blocked.release)
	select {
	case err := <-result:
		if !errors.Is(err, store.ErrStaleCache) {
			t.Fatalf("lost owner: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lost owner did not stop")
	}
	if _, err := testStore.Meta(testCtx, f.tier.cacheRef(ck)); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("stale body published: %v", err)
	}
	testStore.Cache(f.tier.namespace).ReleaseFillLease(context.Background(), ck, "replacement")
	t.Run("completion releases the lease", func(t *testing.T) {
		f := newFixture(t)
		ck := f.tier.key("bkt", "renewed")
		if _, err := f.tier.origin.Upload(testCtx, ck, strings.NewReader("body"), 4, originUpload{}); err != nil {
			t.Fatal(err)
		}
		obj, err := f.tier.Open(testCtx, "bkt", "renewed", store.WholeObjectRange())
		testutil.Must(t, err)
		defer obj.Body.Close()
		data, err := io.ReadAll(obj.Body)
		if err != nil || string(data) != "body" {
			t.Fatal(err)
		}
		var n int
		if err := testStore.Pool().QueryRow(testCtx, `SELECT count(*) FROM s3p.cache_fill_leases WHERE namespace=$1`, f.tier.namespace).Scan(&n); err != nil || n != 0 {
			t.Fatalf("lease leaked: %d %v", n, err)
		}
	})
}
