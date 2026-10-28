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
	"log/slog"
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
	url, cleanup, err := testutil.Postgres(testCtx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		os.Exit(1)
	}
	code := 1
	defer func() {
		cleanup()
		os.Exit(code)
	}()
	st, err := store.Open(testCtx, url, 8, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		return
	}
	defer st.Close()
	if err := st.Init(testCtx); err != nil {
		fmt.Fprintln(os.Stderr, "init schema:", err)
		return
	}
	testStore = st
	code = m.Run()
}

type fixture struct {
	tier *Tier
	dir  string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	tier, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore,
		fmt.Sprintf("origin-%d", time.Now().UnixNano()), 0, 24*time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = tier.Close()
		for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
			_, _ = testStore.DropNamespace(testCtx, kind, tier.namespace, true)
		}
	})
	return &fixture{tier: tier, dir: dir}
}

func (f *fixture) readOrigin(t *testing.T, key string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.dir, key))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func readObject(t *testing.T, obj *Object) string {
	t.Helper()
	defer obj.Body.Close()
	b, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTierObjectLifecycle(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "a.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	if got := f.readOrigin(t, "bkt/a.txt"); got != "alpha" {
		t.Fatalf("origin = %q", got)
	}
	head, err := f.tier.Head(ctx, "bkt", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if head.Size != 5 || head.ContentType != "text/plain" || head.ETag == "" {
		t.Fatalf("head = %+v", head)
	}
	obj, err := f.tier.Open(ctx, "bkt", "a.txt", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "alpha" {
		t.Fatalf("open = %q", got)
	}
	obj, err = f.tier.Open(ctx, "bkt", "a.txt", store.Range(1, 3, -1))
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "lph" {
		t.Fatalf("range = %q", got)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "bkt", "b.txt"), []byte("beta"), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, err := f.tier.List(ctx, "bkt", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestTierETagConsistency(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "a.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	head, err := f.tier.Head(ctx, "bkt", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(ctx, "bkt", "a.txt", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestTierMultipartPendingOutbox(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []store.ListedPart
	for i, body := range [][]byte{[]byte("hello "), []byte("world")} {
		w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp", int32(i+1), 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
		pub, err := w.Finish(nil)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, store.ListedPart{PartNo: int32(i + 1), ETag: pub.ETag})
	}
	done, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp", parts, store.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if done.Size != 11 {
		t.Fatalf("size = %d", done.Size)
	}
	if got := f.readOrigin(t, "bkt/mp"); got != "hello world" {
		t.Fatalf("origin = %q", got)
	}
	head, err := f.tier.Head(ctx, "bkt", "mp")
	if err != nil || head.Size != 11 {
		t.Fatalf("head = %+v err=%v", head, err)
	}
	if n := f.pending(t, "mp"); n != 0 {
		t.Fatalf("pending rows after success = %d", n)
	}

	uploadID, err = f.tier.CreateUpload(ctx, "bkt", "mp-fail", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-fail", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("nope")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(f.dir, "bkt"), 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(filepath.Join(f.dir, "bkt"), 0o755) }()
	_, err = f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-fail",
		[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{})
	if err == nil {
		t.Fatal("expected origin failure")
	}
	head, err = f.tier.Head(ctx, "bkt", "mp-fail")
	if err != nil || head.Size != 4 || !head.Hit {
		t.Fatalf("pending completion head = %+v err=%v", head, err)
	}
	if n := f.pending(t, "mp-fail"); n != 1 {
		t.Fatalf("pending rows after failure = %d, want 1", n)
	}

	if err := os.Chmod(filepath.Join(f.dir, "bkt"), 0o755); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for f.pending(t, "mp-fail") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not clear the pending row")
		}
		time.Sleep(100 * time.Millisecond)
	}
	head, err = f.tier.Head(ctx, "bkt", "mp-fail")
	if err != nil || head.Size != 4 {
		t.Fatalf("retried object head = %+v err=%v", head, err)
	}
	if got := f.readOrigin(t, "bkt/mp-fail"); got != "nope" {
		t.Fatalf("origin after retry = %q", got)
	}
}

func TestTierPendingServesNewCacheBody(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "mp-old", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp-old", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-old", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new!")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(f.dir, "bkt"), 0o555); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(f.dir, "bkt", "mp-old"), 0o444); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.Chmod(filepath.Join(f.dir, "bkt", "mp-old"), 0o644)
		_ = os.Chmod(filepath.Join(f.dir, "bkt"), 0o755)
	}()
	_, err = f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-old",
		[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{})
	if err == nil {
		t.Fatal("expected origin failure")
	}
	obj, err := f.tier.Open(ctx, "bkt", "mp-old", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(obj.Body)
	_ = obj.Body.Close()
	if err != nil || string(body) != "new!" {
		t.Fatalf("pending read = %q err=%v, want completed cache body", body, err)
	}
	if n := f.pending(t, "mp-old"); n != 1 {
		t.Fatalf("pending rows after failed completion = %d, want 1", n)
	}

	if err := os.Chmod(filepath.Join(f.dir, "bkt", "mp-old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(f.dir, "bkt"), 0o755); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for f.pending(t, "mp-old") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not clear the pending row")
		}
		time.Sleep(100 * time.Millisecond)
	}
	obj, err = f.tier.Open(ctx, "bkt", "mp-old", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(obj.Body)
	_ = obj.Body.Close()
	if err != nil || string(body) != "new!" {
		t.Fatalf("retried read = %q err=%v", body, err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	bucket := fmt.Sprintf("pgvs3-test-%d", time.Now().UnixNano())
	client, err := s3client.New(s3client.Config{
		Endpoint: cfg.Endpoint, Bucket: bucket, AccessKey: cfg.AccessKey,
		SecretKey: cfg.SecretKey, Insecure: true, PathStyle: true, Prefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CreateBucket(testCtx); err != nil {
		t.Fatal(err)
	}
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
	tier, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), originCfg, testStore,
		fmt.Sprintf("origin-s3-%d", time.Now().UnixNano()), 0, 24*time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tier.Close() })
	return &s3Fixture{tier: tier, client: client}
}

func (f *s3Fixture) readOrigin(t *testing.T, key string) string {
	t.Helper()
	obj, err := f.client.Get(testCtx, key, 0, -1, "")
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Key != "dir/a" || rows[1].Key != "dir/b" {
		t.Fatalf("prefixed list = %+v", rows)
	}
}

func TestTierValidationRunsBeforeOriginMutation(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	boom := errors.New("checksum mismatch")
	if _, err := f.tier.UploadWithOptions(ctx, "bkt", "invalid", strings.NewReader("bytes"), UploadOptions{
		ContentType: "text/plain",
		Validate:    func() error { return boom },
	}); !errors.Is(err, boom) {
		t.Fatalf("validation error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.dir, "bkt", "invalid")); !os.IsNotExist(err) {
		t.Fatalf("origin changed before validation: %v", err)
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

func TestFillLeaseCoalescesAcrossTiers(t *testing.T) {
	ctx := testCtx
	mem := newTestOrigin(t, "")
	if _, err := mem.Upload(ctx, "bkt/shared", strings.NewReader("shared-fill"), 11, originUpload{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-cross-fill-%d", time.Now().UnixNano())
	for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
		if err := testStore.EnsureNamespace(ctx, kind, cache, nil, nil); err != nil {
			t.Fatal(err)
		}
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
			log: slog.New(slog.NewTextHandler(io.Discard, nil)), workerID: randomID(),
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
		firstCh <- result{body: readObject(t, obj)}
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
		secondCh <- result{body: readObject(t, obj)}
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
			log: slog.New(slog.NewTextHandler(io.Discard, nil)), workerID: randomID(),
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
	if err != nil {
		t.Fatal(err)
	}
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
			log: slog.New(slog.NewTextHandler(io.Discard, nil)), workerID: randomID(),
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
	if err != nil {
		t.Fatal(err)
	}
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "race", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "race", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("stale")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
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
	if err := os.WriteFile(filepath.Join(f.dir, "bkt", "race"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
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

func TestOriginConditionFencesStaleBase(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "fenced", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	old, err := f.tier.origin.Attributes(ctx, f.tier.key("bkt", "fenced"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.tier.Upload(ctx, "bkt", "fenced", strings.NewReader("new"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.tier.origin.Upload(ctx, f.tier.key("bkt", "fenced"), strings.NewReader("stale"), 5, originUpload{
		Precondition: store.Precondition{Mode: store.IfMatch, ETag: old.ETag},
	}); !errors.Is(err, errOriginPrecondition) {
		t.Fatalf("stale conditional upload error = %v", err)
	}
	if got := f.readOrigin(t, "bkt/fenced"); got != "new" {
		t.Fatalf("origin after stale flush = %q", got)
	}
}

func TestReadOnlyTierDoesNotWriteCache(t *testing.T) {
	ctx := testCtx
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "bkt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bkt", "cold.txt"), []byte("cold"), 0o644); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-ro-%d", time.Now().UnixNano())
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	seed, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore, cache, 0, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Upload(ctx, "bkt", "warm.txt", strings.NewReader("older"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	seed.Close()
	if err := os.WriteFile(filepath.Join(dir, "bkt", "warm.txt"), []byte("newer"), 0o644); err != nil {
		t.Fatal(err)
	}

	tier, err := NewReadOnly(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore, cache, 0, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer tier.Close()
	head, err := tier.Head(ctx, "bkt", "cold.txt")
	if err != nil || head.Size != 4 {
		t.Fatalf("cold head = %+v err=%v", head, err)
	}
	warm, err := tier.Open(ctx, "bkt", "warm.txt", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestWarmCacheHitSkipsOrigin(t *testing.T) {
	ctx := testCtx
	dir := t.TempDir()
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	tier, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore,
		fmt.Sprintf("origin-%d", time.Now().UnixNano()), time.Minute, time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tier.Close() })

	if _, err := tier.Upload(ctx, "bkt", "warm.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	first, err := tier.Open(ctx, "bkt", "warm.txt", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if !first.Hit || readObject(t, first) != "alpha" {
		t.Fatalf("first open: hit=%v", first.Hit)
	}

	if err := os.Remove(filepath.Join(dir, "bkt", "warm.txt")); err != nil {
		t.Fatal(err)
	}
	second, err := tier.Open(ctx, "bkt", "warm.txt", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if !second.Hit || readObject(t, second) != "alpha" {
		t.Fatalf("warm open after origin delete: hit=%v", second.Hit)
	}
}

func TestExplicitFullRangeIsRanged(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "full.txt", strings.NewReader("alpha"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(ctx, "bkt", "full.txt", store.Range(0, -1, -1))
	if err != nil {
		t.Fatal(err)
	}
	if !obj.Ranged {
		t.Fatal("explicit bytes=0- range was not marked as ranged")
	}
	if got := readObject(t, obj); got != "alpha" {
		t.Fatalf("body = %q", got)
	}
}

func TestMultipartCompleteOriginPrecondition(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.Upload(ctx, "bkt", "cond", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	head, err := f.tier.Head(ctx, "bkt", "cond")
	if err != nil {
		t.Fatal(err)
	}
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "cond", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "cond", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	_ = readObject(t, obj)
	if counted.gets.Load() != 1 || counted.heads.Load() != 0 {
		t.Fatalf("warm read contacted origin: gets=%d heads=%d", counted.gets.Load(), counted.heads.Load())
	}
}

func TestColdWholeZeroObject(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	if _, err := f.tier.origin.Upload(ctx, f.tier.key("bkt", "zero"), strings.NewReader(""), 0, originUpload{}); err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(ctx, "bkt", "zero", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "" || obj.Size != 0 || obj.Ranged {
		t.Fatalf("zero object = %+v body=%q", obj, got)
	}
}

func TestWriteBackAcksBeforeOriginUpload(t *testing.T) {
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

	body := bytes.Repeat([]byte("p"), 9<<20)
	start := time.Now()
	m, err := f.tier.Upload(ctx, "bkt", "writeback", bytes.NewReader(body), nil, "application/octet-stream")
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("write-back upload waited for the origin")
	}
	if m.Size != int64(len(body)) || m.OriginETag == "" {
		t.Fatalf("write-back meta = %+v", m)
	}
	obj, err := f.tier.Open(ctx, "bkt", "writeback", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != string(body) {
		t.Fatalf("cached write-back body length = %d, want %d", len(got), len(body))
	}
	if f.pending(t, "writeback") != 1 {
		t.Fatal("write-back did not enqueue an origin upload")
	}
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("outbox worker did not attempt the origin upload")
	}
	release()
	deadline := time.Now().Add(20 * time.Second)
	for f.pending(t, "writeback") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not commit the write-back")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := f.readOrigin(t, "bkt/writeback"); got != string(body) {
		t.Fatalf("origin after write-back length = %d, want %d", len(got), len(body))
	}
}

func TestMultipartWriteBackAcksBeforeOriginUpload(t *testing.T) {
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

	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp-writeback", nil, "text/plain", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-writeback", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("async")); err != nil {
		t.Fatal(err)
	}
	pub, err := w.Finish(nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	done, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-writeback",
		[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}}, store.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second || done.Size != 5 {
		t.Fatalf("write-back completion waited or returned size %d", done.Size)
	}
	obj, err := f.tier.Open(ctx, "bkt", "mp-writeback", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "async" {
		t.Fatalf("pending multipart body = %q", got)
	}
	select {
	case <-blocked.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("outbox worker did not attempt the multipart origin upload")
	}
	release()
	deadline := time.Now().Add(20 * time.Second)
	for f.pending(t, "mp-writeback") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not commit the multipart write-back")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := f.readOrigin(t, "bkt/mp-writeback"); got != "async" {
		t.Fatalf("origin after multipart write-back = %q", got)
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
	deadline := time.Now().Add(20 * time.Second)
	for f.pending(t, "b") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("outbox worker did not commit pending listing object")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestReadOnlyTierUsesDurableWriteCache(t *testing.T) {
	ctx := testCtx
	cache := fmt.Sprintf("origin-ro-skip-%d", time.Now().UnixNano())
	if err := testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := testStore.EnsureNamespace(ctx, ns.WriteBack, cache, nil, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testStore.DropNamespace(ctx, ns.Cache, cache, true)
		_, _ = testStore.DropNamespace(ctx, ns.WriteBack, cache, true)
	})
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
		completes: make(chan struct{}, 2), log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	defer tier.Close()
	head, err := tier.Head(ctx, "bkt", "warm")
	if err != nil || head.Size != int64(len("durable")) {
		t.Fatalf("read-only head = %+v err=%v", head, err)
	}
	obj, err := tier.Open(ctx, "bkt", "warm", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "durable" {
		t.Fatalf("read-only open = %q", got)
	}
	rows, err := tier.List(ctx, "bkt", "", "", 10)
	if err != nil || len(rows) != 1 || rows[0].Key != "warm" {
		t.Fatalf("read-only list = %+v err=%v", rows, err)
	}
}

func TestSaveSpecValidatesOriginConfig(t *testing.T) {
	err := SaveSpec(testCtx, testStore, "", Spec{Config: `{"type":"bogus","config":{}}`})
	if err == nil {
		t.Fatal("invalid origin type accepted")
	}
}

func TestSharedReadCacheSurvivesOriginRemoval(t *testing.T) {
	ctx := testCtx
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "bucket"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bucket", "key"), []byte("shared"), 0o644); err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-shared-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = testStore.DropNamespace(ctx, ns.Cache, cache, true)
		_, _ = testStore.DropNamespace(ctx, ns.WriteBack, cache, true)
	})
	cfg := fmt.Appendf(nil, `{"type":"filesystem","config":{"directory":%q}}`, dir)
	writer, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore, cache, time.Hour, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	obj, err := writer.Open(ctx, "bucket", "key", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "shared" {
		t.Fatalf("writer open = %q", got)
	}
	if err := os.Remove(filepath.Join(dir, "bucket", "key")); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReadOnly(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore, cache, time.Hour, time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	obj, err = reader.Open(ctx, "bucket", "key", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "shared" {
		t.Fatalf("read-only shared-cache open = %q", got)
	}
}

func TestTierModelAcrossWorkers(t *testing.T) {
	ctx := testCtx
	mem, err := newTempBucket(Config{})
	if err != nil {
		t.Fatal(err)
	}
	cache := fmt.Sprintf("origin-model-%d", time.Now().UnixNano())
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	newTier := func() *Tier {
		return &Tier{
			origin: mem, store: testStore, namespace: cache, ttl: 0, retention: time.Hour,
			fills: make(chan struct{}, 2), completes: make(chan struct{}, 2),
			log: log, workerID: randomID(), cancel: func() {},
		}
	}
	first, second := newTier(), newTier()
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
	t.Cleanup(func() { _, _ = testStore.DropNamespace(context.WithoutCancel(ctx), "cache", cache, true) })
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
					if err != nil {
						t.Fatal(err)
					}
					version := versionFor(a)
					switch state {
					case "stale":
						version = "old-origin-version"
					case "pending":
						version = store.PendingOriginToken("test")
					case "deleted":
						version = store.DeletedCacheVersion("test").String()
					case "missing-origin":
						if err := f.tier.origin.Delete(ctx, ck); err != nil {
							t.Fatal(err)
						}
					}
					ref := store.ObjectRef{NS: ns.Ref{Kind: kind, Name: f.tier.namespace}, Key: ck}
					m, err := testStore.PutInline(ctx, ref, []byte("hello"), store.WriteOptions{OriginETag: version})
					if err != nil {
						t.Fatal(err)
					}
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

func TestCancelledFillWaiterDoesNotCancelOwner(t *testing.T) {
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
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner did not finish")
	}
	if got := blocked.calls.Load(); got != 1 {
		t.Fatalf("origin GETs = %d, want 1", got)
	}
}

func TestSpooledPublicationMetadata(t *testing.T) {
	f := newFixture(t)
	f.tier.cancel()
	<-f.tier.done
	data := bytes.Repeat([]byte("encoded"), 16<<10)
	sp, cleanup, err := spoolStream(testCtx, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatal(err)
		}
		stored, err := testStore.MetaHeader(testCtx, ref)
		if err != nil {
			t.Fatal(err)
		}
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

func TestExternalOriginUpdateAfterWriteFence(t *testing.T) {
	f := newFixture(t)
	if _, err := f.tier.Upload(testCtx, "bkt", "external", strings.NewReader("old"), nil, "text/plain"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.dir, "bkt", "external"), []byte("new-version"), 0600); err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(testCtx, "bkt", "external", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "new-version" {
		t.Fatalf("external update = %q", got)
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
		if err != nil {
			t.Fatal(err)
		}
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
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	ro.EnableRangeCache(64 << 10)
	obj, err := ro.Open(testCtx, "bkt", "ranges", rng)
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != string(data[100:200]) {
		t.Fatal("replica-shaped tier missed shared range")
	}
	if _, err := testStore.Pool().Exec(testCtx, `UPDATE "s3c_`+f.tier.namespace+`".origin_metadata SET validated_at = now()-interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	obj, err = f.tier.Open(testCtx, "bkt", "ranges", rng)
	if err != nil {
		t.Fatal(err)
	}
	_ = obj.Body.Close()
	if counted.attributes != 2 {
		t.Fatalf("expired metadata did not revalidate: %d", counted.attributes)
	}
	if err := f.tier.Delete(testCtx, "bkt", "ranges"); err != nil {
		t.Fatal(err)
	}
	if _, err := ro.Open(testCtx, "bkt", "ranges", rng); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("delete exposed metadata/ranges: %v", err)
	}
}

func TestParentHeadPublicationFencedByTombstone(t *testing.T) {
	f := newFixture(t)
	ck := f.tier.key("bkt", "fenced-head")
	info := store.ObjectInfo{Size: 10 << 20, OriginETag: "old", ETag: "old", ContentType: "application/parquet"}
	if err := f.tier.fenceReadCache(testCtx, ck); err != nil {
		t.Fatal(err)
	}
	if err := testStore.Cache(f.tier.namespace).RememberOriginMetadata(testCtx, ck, info, time.Now(), time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := testStore.Cache(f.tier.namespace).OriginMetadata(testCtx, ck, time.Hour)
	if err != nil || got != nil {
		t.Fatalf("late HEAD resurrected metadata: %+v %v", got, err)
	}
}

func TestFillRenewalAndLostOwner(t *testing.T) {
	f := newFixture(t)
	f.tier.fillTTL = 2 * time.Second
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
	case <-time.After(2500 * time.Millisecond):
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
}

func TestFillRenewalCompletesAndReleases(t *testing.T) {
	f := newFixture(t)
	ck := f.tier.key("bkt", "renewed")
	if _, err := f.tier.origin.Upload(testCtx, ck, strings.NewReader("body"), 4, originUpload{}); err != nil {
		t.Fatal(err)
	}
	obj, err := f.tier.Open(testCtx, "bkt", "renewed", store.WholeObjectRange())
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Body.Close()
	data, err := io.ReadAll(obj.Body)
	if err != nil || string(data) != "body" {
		t.Fatal(err)
	}
	var n int
	if err := testStore.Pool().QueryRow(testCtx, `SELECT count(*) FROM s3p.cache_fill_leases WHERE namespace=$1`, f.tier.namespace).Scan(&n); err != nil || n != 0 {
		t.Fatalf("lease leaked: %d %v", n, err)
	}
}
