//go:build integration

package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/testutil"
)

var (
	testStore *Store
	testCtx   = context.Background()
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunMain(m, func(ctx context.Context, url string) (func(), error) {
		st, err := Open(ctx, url, 8, testutil.DiscardLogger())
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

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func dropOnCleanup(t *testing.T, kind ns.Kind, name string) {
	t.Helper()
	t.Cleanup(func() { _, _ = testStore.DropNamespace(testCtx, kind, name, true) })
}

func TestPendingOutboxLifecycle(t *testing.T) {
	ctx := testCtx
	cache := unique("origin")
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.WriteBack, cache, nil, nil))
	uploadID, err := testStore.CreateUpload(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"}, UploadOptions{ContentType: "text/plain"})
	testutil.Must(t, err)
	var parts []ListedPart
	for i, body := range [][]byte{[]byte("hello "), []byte("world")} {
		w, err := testStore.StartPart(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"}, uploadID, int32(i+1), 0)
		testutil.Must(t, err)
		if _, err := w.Write(body); err != nil {
			t.Fatal(err)
		}
		pub, err := w.Finish(nil)
		testutil.Must(t, err)
		parts = append(parts, ListedPart{PartNo: int32(i + 1), ETag: pub.ETag})
	}
	done, err := testStore.CompleteUploadPending(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"}, uploadID, parts, WriteOptions{}, PendingCompletion{BaseMissing: true, LeaseOwner: "test-owner"})
	testutil.Must(t, err)
	if done.Size != 11 {
		t.Fatalf("size = %d", done.Size)
	}
	m, err := testStore.MetaHeader(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"})
	if err != nil || !IsPendingOrigin(m.OriginETag) {
		t.Fatalf("pending object: m=%+v err=%v", m, err)
	}
	var token string
	if err := testStore.pool.QueryRow(ctx,
		"SELECT token FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2",
		cache, "bkt/mp").Scan(&token); err != nil {
		t.Fatal(err)
	}
	if token != PendingOriginToken(uploadID) {
		t.Fatalf("token = %q, want %q", token, PendingOriginToken(uploadID))
	}

	if owned, err := testStore.Cache(cache).MarkOriginCommitted(ctx, "bkt/mp", "pending:stale", "stale-v1", time.Time{}, nil); err != nil || owned {
		t.Fatalf("stale commit owned=%v err=%v", owned, err)
	}
	m, err = testStore.MetaHeader(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"})
	if err != nil || !IsPendingOrigin(m.OriginETag) {
		t.Fatalf("stale commit changed pending object: m=%+v err=%v", m, err)
	}
	if owned, err := testStore.Cache(cache).MarkOriginCommitted(ctx, "bkt/mp", token, "origin-v1", time.Time{}, nil); err != nil || !owned {
		t.Fatalf("commit owned=%v err=%v", owned, err)
	}
	m, err = testStore.MetaHeader(ctx, ObjectRef{NS: ns.WriteBackRef(cache), Key: "bkt/mp"})
	if err != nil || m.OriginETag != "origin-v1" {
		t.Fatalf("committed object: m=%+v err=%v", m, err)
	}
	var pending int
	if err := testStore.pool.QueryRow(ctx,
		"SELECT count(*) FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2",
		cache, "bkt/mp").Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 0 {
		t.Fatalf("pending rows = %d, want 0", pending)
	}
	t.Run("flush refuses pending uploads", func(t *testing.T) {
		ctx := testCtx
		cache := unique("flush")
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
		testutil.Must(t, testStore.SetNamespaceAttrs(ctx, ns.Cache, cache, map[string]any{"origin": map[string]any{"scope": "global"}}))
		dropOnCleanup(t, ns.WriteBack, cache)
		dropOnCleanup(t, ns.Cache, cache)
		if _, err := testStore.Pool().Exec(ctx, `INSERT INTO s3p.pending_uploads
			(kind, namespace, key, token, size, attempts, next_attempt, base_missing)
			VALUES ('cache', $1, 'bkt/key', 'pending:flush', 1, 0, now(), false)`, cache); err != nil {
			t.Fatal(err)
		}
		if err := testStore.Cache(cache).Flush(ctx); !errors.Is(err, ErrPendingUploads) {
			t.Fatalf("flush with pending error = %v, want ErrPendingUploads", err)
		}
		if _, err := testStore.Pool().Exec(ctx, `DELETE FROM s3p.pending_uploads WHERE namespace=$1`, cache); err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, testStore.Cache(cache).Flush(ctx))
		attrs, ok, err := testStore.NamespaceAttrs(ctx, ns.Cache, cache)
		if err != nil || !ok {
			t.Fatalf("attrs ok=%v err=%v", ok, err)
		}
		if _, ok := attrs["origin"]; !ok {
			t.Fatalf("origin attrs not preserved: %+v", attrs)
		}
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.WriteBack, cache, nil, nil))
	})
}

func TestPendingUploadClaims(t *testing.T) {
	ctx := testCtx
	cache := unique("origin")
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
	_, err := testStore.Pool().Exec(ctx, `INSERT INTO s3p.pending_uploads
		(namespace, key, token, base_etag, base_missing, lease_owner, lease_until)
		VALUES ($1, 'bkt/key', 'pending:one', 'base-v1', false, 'gateway-a', now() + interval '1 minute')`,
		cache)
	testutil.Must(t, err)
	claimed, err := testStore.Cache(cache).ClaimPendingUploads(ctx, "gateway-b", 8, time.Minute)
	testutil.Must(t, err)
	if len(claimed) != 0 {
		t.Fatalf("claimed active lease: %+v", claimed)
	}
	if _, err := testStore.Pool().Exec(ctx,
		`UPDATE s3p.pending_uploads SET lease_until = now() - interval '1 second'
		 WHERE namespace = $1 AND key = 'bkt/key'`, cache); err != nil {
		t.Fatal(err)
	}
	claimed, err = testStore.Cache(cache).ClaimPendingUploads(ctx, "gateway-b", 8, time.Minute)
	testutil.Must(t, err)
	if len(claimed) != 1 || claimed[0].LeaseOwner != "gateway-b" ||
		claimed[0].BaseETag != "base-v1" || claimed[0].BaseMissing {
		t.Fatalf("claimed = %+v", claimed)
	}
	testutil.Must(t, testStore.Cache(cache).FailPendingUpload(ctx, "bkt/key", "pending:one", "gateway-a", 0, errors.New("wrong owner")))
	var attempts int
	if err := testStore.Pool().QueryRow(ctx,
		`SELECT attempts FROM s3p.pending_uploads WHERE namespace = $1 AND key = 'bkt/key'`, cache).
		Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("wrong owner updated attempts = %d", attempts)
	}
	testutil.Must(t, testStore.Cache(cache).FailPendingUpload(ctx, "bkt/key", "pending:one", "gateway-b", 0, errors.New("boom")))
	if err := testStore.Pool().QueryRow(ctx,
		`SELECT attempts FROM s3p.pending_uploads WHERE namespace = $1 AND key = 'bkt/key'`, cache).
		Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts after owner failure = %d", attempts)
	}
	t.Run("distributed across owners", func(t *testing.T) {
		ctx := testCtx
		cache := unique("origin")
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
		_, err := testStore.Pool().Exec(ctx, `INSERT INTO s3p.pending_uploads
			(namespace, key, token, base_etag, base_missing)
			VALUES ($1, 'bkt/a', 'pending:a', 'base-a', false),
			       ($1, 'bkt/b', 'pending:b', 'base-b', false)`, cache)
		testutil.Must(t, err)
		a, err := testStore.Cache(cache).ClaimPendingUploads(ctx, "worker-a", 1, 5*time.Minute)
		testutil.Must(t, err)
		b, err := testStore.Cache(cache).ClaimPendingUploads(ctx, "worker-b", 1, 5*time.Minute)
		testutil.Must(t, err)
		if len(a) != 1 || len(b) != 1 || a[0].Key == b[0].Key {
			t.Fatalf("claims = %+v / %+v, want two distinct keys", a, b)
		}
		if a[0].LeaseOwner != "worker-a" || b[0].LeaseOwner != "worker-b" {
			t.Fatalf("claim owners = %q / %q", a[0].LeaseOwner, b[0].LeaseOwner)
		}
		rest, err := testStore.Cache(cache).ClaimPendingUploads(ctx, "worker-c", 2, 5*time.Minute)
		testutil.Must(t, err)
		if len(rest) != 0 {
			t.Fatalf("duplicate claims = %+v", rest)
		}
	})
	t.Run("dead letters after permanent failure", func(t *testing.T) {
		ctx := testCtx
		cache := unique("dead")
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
		if _, err := testStore.Pool().Exec(ctx, `INSERT INTO s3p.pending_uploads
			(kind, namespace, key, token, attempts, next_attempt, base_missing, lease_owner, lease_until)
			VALUES ('cache', $1, 'bkt/key', 'pending:dead', 0, now(), false, 'owner', now() + interval '1 minute')`,
			cache); err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, testStore.Cache(cache).FailPendingUpload(ctx, "bkt/key", "pending:dead", "owner", 0, errors.New("origin down")))
		var dead bool
		var attempts int
		if err := testStore.Pool().QueryRow(ctx,
			"SELECT dead, attempts FROM s3p.pending_uploads WHERE namespace = $1 AND key = 'bkt/key'", cache).
			Scan(&dead, &attempts); err != nil {
			t.Fatal(err)
		}
		if dead || attempts != 1 {
			t.Fatalf("transient failure dead=%v attempts=%d", dead, attempts)
		}
		if _, err := testStore.Pool().Exec(ctx,
			`UPDATE s3p.pending_uploads SET lease_owner = 'owner', lease_until = now() + interval '1 minute'
			 WHERE namespace = $1 AND key = 'bkt/key'`, cache); err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, testStore.Cache(cache).FailPendingUploadPermanent(ctx, "bkt/key", "pending:dead", "owner", 1, errors.New("missing durable body")))
		if err := testStore.Pool().QueryRow(ctx,
			"SELECT dead, attempts FROM s3p.pending_uploads WHERE namespace = $1 AND key = 'bkt/key'", cache).
			Scan(&dead, &attempts); err != nil {
			t.Fatal(err)
		}
		if !dead {
			t.Fatalf("permanent failure dead=%v attempts=%d", dead, attempts)
		}
		claimed, err := testStore.Cache(cache).ClaimPendingUploads(ctx, "worker", 1, time.Minute)
		if err != nil || len(claimed) != 0 {
			t.Fatalf("dead row claimed: %+v err=%v", claimed, err)
		}
		live, deadCount, err := testStore.PendingUploadCounts(ctx)
		if err != nil || deadCount == 0 {
			t.Fatalf("pending counts live=%d dead=%d err=%v", live, deadCount, err)
		}
	})
}

func TestCachePublishExpectedOriginGuard(t *testing.T) {
	ctx := testCtx
	cache := unique("origin")
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))

	const inlineKey = "bkt/inline"
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: inlineKey}, []byte("v1"),
		WriteOptions{ContentType: "text/plain", OriginETag: "v1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: "bkt/absent"}, []byte("first"),
		WriteOptions{ContentType: "text/plain", OriginETag: "v1", ExpectedOrigin: "never-existed"}); err != nil {
		t.Fatalf("conditional publish without an old row: %v", err)
	}
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: inlineKey}, []byte("stale"),
		WriteOptions{ContentType: "text/plain", OriginETag: "v0", ExpectedOrigin: "v0"}); !errors.Is(err, ErrStaleCache) {
		t.Fatalf("stale inline publish error = %v, want ErrStaleCache", err)
	}
	got, ok, err := testStore.Cache(cache).Get(ctx, inlineKey)
	if err != nil || !ok || string(got) != "v1" {
		t.Fatalf("inline after stale publish = %q ok=%v err=%v", got, ok, err)
	}
	m, err := testStore.MetaHeader(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: inlineKey})
	if err != nil || m.OriginETag != "v1" {
		t.Fatalf("inline origin after stale publish = %+v err=%v", m, err)
	}
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: inlineKey}, []byte("v2"),
		WriteOptions{ContentType: "text/plain", OriginETag: "v2", ExpectedOrigin: "v1"}); err != nil {
		t.Fatalf("matching inline publish: %v", err)
	}
	got, ok, err = testStore.Cache(cache).Get(ctx, inlineKey)
	if err != nil || !ok || string(got) != "v2" {
		t.Fatalf("inline after matching publish = %q ok=%v err=%v", got, ok, err)
	}
	m, err = testStore.MetaHeader(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: inlineKey})
	if err != nil || m.OriginETag != "v2" {
		t.Fatalf("inline origin after matching publish = %+v err=%v", m, err)
	}

	const chunkKey = "bkt/chunked"
	w, err := testStore.StartObject(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: chunkKey},
		WriteOptions{ContentType: "text/plain", OriginETag: "c1"})
	testutil.Must(t, err)
	if _, err := w.Write([]byte("chunk-v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	stale, err := testStore.StartObject(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: chunkKey},
		WriteOptions{ContentType: "text/plain", OriginETag: "c0", ExpectedOrigin: "c0"})
	testutil.Must(t, err)
	if _, err := stale.Write([]byte("stale")); err != nil {
		t.Fatal(err)
	}
	if _, err := stale.Finish(nil); !errors.Is(err, ErrStaleCache) {
		t.Fatalf("stale chunked publish error = %v, want ErrStaleCache", err)
	}
	got, ok, err = testStore.Cache(cache).Get(ctx, chunkKey)
	if err != nil || !ok || string(got) != "chunk-v1" {
		t.Fatalf("chunked after stale publish = %q ok=%v err=%v", got, ok, err)
	}
	m, err = testStore.MetaHeader(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: chunkKey})
	if err != nil || m.OriginETag != "c1" {
		t.Fatalf("chunked origin after stale publish = %+v err=%v", m, err)
	}
	match, err := testStore.StartObject(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: chunkKey},
		WriteOptions{ContentType: "text/plain", OriginETag: "c2", ExpectedOrigin: "c1"})
	testutil.Must(t, err)
	if _, err := match.Write([]byte("chunk-v2")); err != nil {
		t.Fatal(err)
	}
	if _, err := match.Finish(nil); err != nil {
		t.Fatalf("matching chunked publish: %v", err)
	}
	got, ok, err = testStore.Cache(cache).Get(ctx, chunkKey)
	if err != nil || !ok || string(got) != "chunk-v2" {
		t.Fatalf("chunked after matching publish = %q ok=%v err=%v", got, ok, err)
	}
	m, err = testStore.MetaHeader(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: chunkKey})
	if err != nil || m.OriginETag != "c2" {
		t.Fatalf("chunked origin after matching publish = %+v err=%v", m, err)
	}
}

func TestEnsureMaintenanceDetectsSchemaDrift(t *testing.T) {
	ctx := testCtx
	bucket := unique("drift")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	schema, err := nsSQLIdent(ns.Bucket, bucket)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, "DROP INDEX "+schema+".objects_file_id_idx"); err != nil {
		t.Fatal(err)
	}
	if err := testStore.EnsureMaintenance(ctx); err == nil {
		t.Fatal("EnsureMaintenance accepted a missing object index")
	}
	if _, err := testStore.pool.Exec(ctx,
		"CREATE INDEX objects_file_id_idx ON "+schema+".objects (file_id) WHERE file_id IS NOT NULL"); err != nil {
		t.Fatal(err)
	}
	if err := testStore.EnsureMaintenance(ctx); err != nil {
		t.Fatalf("EnsureMaintenance after repair: %v", err)
	}
}

func TestQuotaAdmission(t *testing.T) {
	ctx := testCtx
	bucket := unique("budget")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	testStore.SetMaxDBBytes(1)
	t.Cleanup(func() { testStore.SetMaxDBBytes(0) })
	_, err := testStore.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "k"}, []byte("x"),
		WriteOptions{ContentType: "text/plain"})
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("got %v, want ErrQuotaExceeded", err)
	}
	testStore.SetMaxDBBytes(0)
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "k"}, []byte("x"),
		WriteOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("write after clearing budget: %v", err)
	}
	t.Run("part quota admission", func(t *testing.T) {
		ctx := testCtx
		cache := unique("part-quota")
		quota := int64(1 << 20)
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, &quota, nil))
		dropOnCleanup(t, ns.Cache, cache)
		_, err := testStore.StartPart(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: "bkt/part"}, "missing", 1, 2<<20)
		if !errors.Is(err, ErrQuotaExceeded) {
			t.Fatalf("oversized part error = %v, want ErrQuotaExceeded", err)
		}
	})
}

func TestBucketWritesCannotResurrectDroppedNamespace(t *testing.T) {
	ctx := testCtx
	url := testStore.Pool().Config().ConnString()
	openStore := func(t *testing.T, label string) *Store {
		t.Helper()
		st, err := Open(ctx, url, 8, testutil.DiscardLogger())
		if err != nil {
			t.Fatalf("open %s store: %v", label, err)
		}
		t.Cleanup(st.Close)
		return st
	}
	other := openStore(t, "second")
	warmBucket := func(t *testing.T, bucket string) {
		t.Helper()
		for _, st := range []*Store{testStore, other} {
			ok, err := st.BucketExists(ctx, bucket)
			if err != nil || !ok {
				t.Fatalf("warm bucket cache: exists=%v err=%v", ok, err)
			}
		}
	}
	assertGone := func(t *testing.T, bucket string) {
		t.Helper()
		ok, err := other.BucketExists(ctx, bucket)
		if err != nil || ok {
			t.Fatalf("bucket resurrected: exists=%v err=%v", ok, err)
		}
	}

	t.Run("inline write", func(t *testing.T) {
		bucket := unique("resurrect-put")
		testutil.Must(t, testStore.CreateBucket(ctx, bucket))
		warmBucket(t, bucket)
		if outcome, err := testStore.DropBucket(ctx, bucket, true); err != nil || outcome != BucketDeleted {
			t.Fatalf("drop: outcome=%v err=%v", outcome, err)
		}
		if _, err := other.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "k"}, []byte("x"),
			WriteOptions{ContentType: "text/plain"}); !errors.Is(err, ErrNoBucket) {
			t.Fatalf("write after remote drop: err=%v, want ErrNoBucket", err)
		}
		assertGone(t, bucket)
	})

	t.Run("stream write", func(t *testing.T) {
		bucket := unique("resurrect-stream")
		testutil.Must(t, testStore.CreateBucket(ctx, bucket))
		warmBucket(t, bucket)
		if outcome, err := testStore.DropBucket(ctx, bucket, true); err != nil || outcome != BucketDeleted {
			t.Fatalf("drop: outcome=%v err=%v", outcome, err)
		}
		w, err := other.StartObject(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "k"}, WriteOptions{ContentType: "text/plain"})
		if err == nil {
			if _, werr := w.Write([]byte("x")); werr != nil {
				w.Abort()
				err = werr
			} else {
				_, err = w.Finish(nil)
			}
		}
		if !errors.Is(err, ErrNoBucket) {
			t.Fatalf("stream write after remote drop: err=%v, want ErrNoBucket", err)
		}
		assertGone(t, bucket)
	})

	t.Run("copy", func(t *testing.T) {
		src := unique("resurrect-src")
		dst := unique("resurrect-dst")
		testutil.Must(t, testStore.CreateBucket(ctx, src))
		testutil.Must(t, testStore.CreateBucket(ctx, dst))
		if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.BucketRef(src), Key: "source"}, []byte("x"),
			WriteOptions{ContentType: "text/plain"}); err != nil {
			t.Fatal(err)
		}
		warmBucket(t, dst)
		if outcome, err := testStore.DropBucket(ctx, dst, true); err != nil || outcome != BucketDeleted {
			t.Fatalf("drop: outcome=%v err=%v", outcome, err)
		}
		if _, err := other.Copy(ctx, ObjectRef{NS: ns.BucketRef(src), Key: "source"}, ObjectRef{NS: ns.BucketRef(dst), Key: "copy"}, CopyOptions{}); !errors.Is(err, ErrNoBucket) {
			t.Fatalf("copy after remote drop: err=%v, want ErrNoBucket", err)
		}
		assertGone(t, dst)
	})

	t.Run("cache auto-create", func(t *testing.T) {
		cache := unique("resurrect-cache")
		fresh := openStore(t, "fresh")
		if _, err := fresh.PutInline(ctx, ObjectRef{NS: ns.CacheRef(cache), Key: "k"}, []byte("value"),
			WriteOptions{ContentType: "text/plain"}); err != nil {
			t.Fatalf("implicit cache write: %v", err)
		}
		got, ok, err := other.Cache(cache).Get(ctx, "k")
		if err != nil || !ok || string(got) != "value" {
			t.Fatalf("read cache through second store: got=%q ok=%v err=%v", got, ok, err)
		}
	})
}

func TestSchemaBootstrapAndIdempotentInit(t *testing.T) {
	ctx := testCtx
	var layout int
	testutil.Must(t, testStore.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&layout))
	if layout != LayoutVersion {
		t.Fatalf("layout = %d, want %d", layout, LayoutVersion)
	}
	if err := testStore.Init(ctx); err != nil {
		t.Fatalf("second Init: %v", err)
	}
	if err := testStore.EnsureMaintenance(ctx); err != nil {
		t.Fatalf("EnsureMaintenance: %v", err)
	}
}

func TestCacheKeyAdvisoryLockBlocksSecondOwner(t *testing.T) {
	ctx := testCtx
	release, err := testStore.LockCacheKey(ctx, ns.Cache, "lock-ns", "bkt/key")
	testutil.Must(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := testStore.LockCacheKey(waitCtx, ns.Cache, "lock-ns", "bkt/key"); err == nil {
		t.Fatal("second cache key lock unexpectedly succeeded")
	}
	release()
	release2, err := testStore.LockCacheKey(ctx, ns.Cache, "lock-ns", "bkt/key")
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	release2()
}

func TestWriteBackNamespaceIsLogged(t *testing.T) {
	ctx := testCtx
	cache := unique("wb")
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.WriteBack, cache, nil, nil))
	dropOnCleanup(t, ns.WriteBack, cache)
	dropOnCleanup(t, ns.Cache, cache)
	for kind, want := range map[ns.Kind]string{ns.Cache: "p", ns.WriteBack: "p"} {
		schema, err := kind.Schema(cache)
		testutil.Must(t, err)
		var relpersistence string
		if err := testStore.Pool().QueryRow(ctx, `SELECT relpersistence::text
			FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname=$1 AND c.relname='objects'`, schema).Scan(&relpersistence); err != nil {
			t.Fatal(err)
		}
		if relpersistence != want {
			t.Fatalf("%s.objects relpersistence=%q, want %q", schema, relpersistence, want)
		}
	}
}

func TestConcurrentConditionalCreate(t *testing.T) {
	ctx := testCtx
	bucket := unique("cond")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	start := make(chan struct{})
	type result struct {
		ok  bool
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			<-start
			_, err := testStore.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "race"}, []byte("x"),
				WriteOptions{Precondition: Precondition{Mode: IfAbsent}})
			results <- result{ok: err == nil, err: err}
		}()
	}
	close(start)
	var successes, precondition int
	for range 2 {
		r := <-results
		switch {
		case r.ok:
			successes++
		case errors.Is(r.err, ErrPrecondition):
			precondition++
		default:
			t.Fatalf("conditional create error: %v", r.err)
		}
	}
	if successes != 1 || precondition != 1 {
		t.Fatalf("successes=%d preconditions=%d, want 1/1", successes, precondition)
	}
}

func TestChunkLayoutRequiresReloptionsAndExternalStorage(t *testing.T) {
	ctx := testCtx
	bucket := unique("chunk-layout")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	schema, err := ns.Bucket.Schema(bucket)
	testutil.Must(t, err)
	chunks := quoteIdent(schema) + ".chunks"
	var storage string
	if err := testStore.pool.QueryRow(ctx,
		`SELECT attstorage::text FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'data'`,
		chunks).Scan(&storage); err != nil {
		t.Fatal(err)
	}
	if storage != "e" {
		t.Fatalf("chunks.data storage = %q, want EXTERNAL", storage)
	}
	conn, err := testStore.pool.Acquire(ctx)
	testutil.Must(t, err)
	defer conn.Release()
	if err := checkChunksLayout(ctx, conn, schema); err != nil {
		t.Fatalf("fresh chunk layout rejected: %v", err)
	}
	if _, err := testStore.pool.Exec(ctx, "ALTER TABLE "+chunks+" RESET (toast_tuple_target)"); err != nil {
		t.Fatal(err)
	}
	if err := checkChunksLayout(ctx, conn, schema); err == nil {
		t.Fatal("chunk layout without toast_tuple_target accepted")
	}
}

func TestCheckLayoutRefusesMismatchedVersion(t *testing.T) {
	ctx := testCtx
	if err := testStore.CheckLayout(ctx); err != nil {
		t.Fatalf("current layout rejected: %v", err)
	}
	if _, err := testStore.pool.Exec(ctx, "INSERT INTO s3p.layout (version) VALUES ($1)", LayoutVersion+1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testStore.pool.Exec(testCtx, "DELETE FROM s3p.layout WHERE version = $1", LayoutVersion+1)
	})
	if err := testStore.CheckLayout(ctx); err == nil {
		t.Fatal("layout version mismatch accepted")
	}
}

func TestMaintainHasSingleLeader(t *testing.T) {
	ctx := testCtx
	bucket := unique("leader")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	schema, err := ns.Bucket.Schema(bucket)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, "DROP SCHEMA "+quoteIdent(schema)+" CASCADE"); err != nil {
		t.Fatal(err)
	}
	const maintenanceLock = 0x70677674
	holder, err := testStore.pool.Acquire(ctx)
	testutil.Must(t, err)
	defer func() {
		_, _ = holder.Exec(testCtx, "SELECT pg_advisory_unlock($1)", int64(maintenanceLock))
		holder.Release()
	}()
	if _, err := holder.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(maintenanceLock)); err != nil {
		t.Fatal(err)
	}
	testutil.Must(t, testStore.Maintain(ctx, 0))
	if n := bucketRows(t, bucket); n != 1 {
		t.Fatalf("maintenance ran while another leader held the lock: rows=%d", n)
	}
	if _, err := holder.Exec(ctx, "SELECT pg_advisory_unlock($1)", int64(maintenanceLock)); err != nil {
		t.Fatal(err)
	}
	testutil.Must(t, testStore.Maintain(ctx, 0))
	if n := bucketRows(t, bucket); n != 0 {
		t.Fatalf("maintenance did not prune the vanished schema: rows=%d", n)
	}
}

func bucketRows(t *testing.T, bucket string) int {
	t.Helper()
	var n int
	if err := testStore.pool.QueryRow(testCtx,
		"SELECT count(*) FROM s3p.namespaces WHERE kind = 'bucket' AND name = $1", bucket).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestNamespaceLimit(t *testing.T) {
	ctx := testCtx
	var count int
	testutil.Must(t, testStore.Pool().QueryRow(ctx, `SELECT count(*) FROM s3p.namespaces`).Scan(&count))
	var created []string
	cleanup := func() {
		for _, name := range created {
			_, _ = testStore.DropNamespace(ctx, ns.Cache, name, true)
		}
	}
	defer cleanup()
	for count < SupportedNamespaces {
		name := unique("limit")
		if err := testStore.EnsureNamespace(ctx, ns.Cache, name, nil, nil); err != nil {
			t.Fatalf("fill namespace %d: %v", count, err)
		}
		created = append(created, name)
		count++
	}
	name := unique("limit")
	if err := testStore.EnsureNamespace(ctx, ns.Cache, name, nil, nil); !errors.Is(err, ErrTooManyNamespaces) {
		t.Fatalf("over-limit create error = %v, want ErrTooManyNamespaces", err)
	}
}

func TestMetadataRoundTrip(t *testing.T) {
	ctx := testCtx
	bucket := unique("meta")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	modified := time.Date(2025, 4, 3, 2, 1, 0, 0, time.UTC)
	ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "encoded"}
	if _, err := testStore.PutInline(ctx, ref, []byte("body"), WriteOptions{
		ContentType: "text/plain", ContentEncoding: "gzip", OriginModified: modified,
		Checksum: &Checksum{Name: "x-amz-checksum-crc32", Value: "abc"}, ChecksumType: "FULL_OBJECT",
	}); err != nil {
		t.Fatal(err)
	}
	m, err := testStore.MetaHeader(ctx, ref)
	testutil.Must(t, err)
	if m.ContentEncoding != "gzip" || !m.OriginModified.Equal(modified) || !m.LastModified().Equal(modified) {
		t.Fatalf("metadata encoding=%q origin_modified=%v last=%v", m.ContentEncoding, m.OriginModified, m.LastModified())
	}
	if m.ChecksumName != "x-amz-checksum-crc32" || m.ChecksumValue != "abc" || m.ChecksumType != "FULL_OBJECT" {
		t.Fatalf("checksum metadata = %q/%q/%q", m.ChecksumName, m.ChecksumValue, m.ChecksumType)
	}
	rows, err := testStore.List(ctx, ns.BucketRef(bucket), "", "\U0010FFFF", "", 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("list rows=%+v err=%v", rows, err)
	}
	if !rows[0].CreatedAt.Equal(modified) {
		t.Fatalf("list last-modified = %v, want %v", rows[0].CreatedAt, modified)
	}
	out, err := testStore.Copy(ctx, ref, ObjectRef{NS: ns.BucketRef(bucket), Key: "copied"}, CopyOptions{})
	testutil.Must(t, err)
	if out.ContentEncoding != "gzip" {
		t.Fatalf("copy encoding = %q", out.ContentEncoding)
	}
	t.Run("clone keeps metadata and part checksums", func(t *testing.T) {
		ctx := testCtx
		src, dst := unique("clone-src"), unique("clone-dst")
		testutil.Must(t, testStore.CreateBucket(ctx, src))
		dropOnCleanup(t, ns.Bucket, dst)
		dropOnCleanup(t, ns.Bucket, src)
		modified := time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC)
		srcRef := ObjectRef{NS: ns.BucketRef(src), Key: "obj"}
		if _, err := testStore.PutInline(ctx, srcRef, []byte("body"), WriteOptions{
			ContentType: "text/plain", ContentEncoding: "br", OriginModified: modified,
			Checksum: &Checksum{Name: "x-amz-checksum-crc32", Value: "abc"}, ChecksumType: "FULL_OBJECT",
		}); err != nil {
			t.Fatal(err)
		}
		srcSchema, _ := srcRef.Schema()
		if _, err := testStore.Pool().Exec(ctx,
			`INSERT INTO `+srcSchema+`.object_part_checksums (key, part_no, file_id, algorithm, value)
			 VALUES ('obj', 1, 0, 'CRC32', 'abc')`); err != nil {
			t.Fatal(err)
		}
		testutil.Must(t, testStore.CloneNamespace(ctx, ns.Bucket, src, dst))
		dstRef := ObjectRef{NS: ns.BucketRef(dst), Key: "obj"}
		m, err := testStore.MetaHeader(ctx, dstRef)
		testutil.Must(t, err)
		if m.ContentEncoding != "br" || !m.OriginModified.Equal(modified) || m.ChecksumValue != "abc" {
			t.Fatalf("cloned metadata = %+v", m.ObjectInfo)
		}
		dstSchema, _ := dstRef.Schema()
		var n int
		if err := testStore.Pool().QueryRow(ctx,
			`SELECT count(*) FROM `+dstSchema+`.object_part_checksums WHERE key = 'obj'`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("cloned part checksums = %d err=%v", n, err)
		}
	})
	t.Run("copy keeps stored inline bytes", func(t *testing.T) {
		bucket := unique("copy-inline")
		testutil.Must(t, testStore.CreateBucket(testCtx, bucket))
		dropOnCleanup(t, ns.Bucket, bucket)
		for i, data := range [][]byte{nil, bytes.Repeat([]byte("compressible"), 2048)} {
			src := ObjectRef{NS: ns.BucketRef(bucket), Key: fmt.Sprintf("source-%d", i)}
			dst := ObjectRef{NS: src.NS, Key: fmt.Sprintf("copy-%d", i)}
			expires := time.Now().Add(time.Hour)
			if _, err := testStore.PutInline(testCtx, src, data, WriteOptions{ExpiresAt: &expires, OriginETag: "source-origin"}); err != nil {
				t.Fatal(err)
			}
			copied, err := testStore.Copy(testCtx, src, dst, CopyOptions{})
			testutil.Must(t, err)
			if copied.ExpiresAt != nil || copied.OriginETag != "" {
				t.Fatalf("COPY inherited source lifecycle: %+v", copied)
			}
			got, err := testStore.Get(testCtx, dst, WholeObjectRange())
			if err != nil || !bytes.Equal(got.Bytes, data) {
				t.Fatalf("COPY changed inline bytes: %v", err)
			}
			schema, _ := src.Schema()
			var sourceStored, destStored []byte
			testutil.Must(t, testStore.pool.QueryRow(testCtx, "SELECT inline_data FROM "+schema+".objects WHERE key=$1", src.Key).Scan(&sourceStored))
			testutil.Must(t, testStore.pool.QueryRow(testCtx, "SELECT inline_data FROM "+schema+".objects WHERE key=$1", dst.Key).Scan(&destStored))
			if !bytes.Equal(sourceStored, destStored) || destStored == nil {
				t.Fatal("COPY decoded or discarded stored inline bytes")
			}
		}
	})
}

func TestOpenWithOptionsAppliesSessionPolicy(t *testing.T) {
	ctx := context.Background()
	other, err := OpenWithOptions(ctx, testStore.pool.Config().ConnString(), OpenOptions{
		MaxConns:              2,
		WorkMem:               "8MB",
		StatementTimeout:      2 * time.Second,
		LockTimeout:           time.Second,
		IdleTxTimeout:         30 * time.Second,
		MaxConnLifetime:       time.Minute,
		MaxConnLifetimeJitter: 5 * time.Second,
	}, testutil.DiscardLogger())
	testutil.Must(t, err)
	defer other.Close()

	var workMem, statement, lock, idle string
	if err := other.pool.QueryRow(ctx, `SELECT current_setting('work_mem'),
		current_setting('statement_timeout'), current_setting('lock_timeout'),
		current_setting('idle_in_transaction_session_timeout')`).
		Scan(&workMem, &statement, &lock, &idle); err != nil {
		t.Fatal(err)
	}
	if workMem != "8MB" || statement != "2s" || lock != "1s" || idle != "30s" {
		t.Fatalf("session = %q %q %q %q", workMem, statement, lock, idle)
	}
	var chunkStatement string
	testutil.Must(t, other.chunkPool.QueryRow(ctx, `SELECT current_setting('statement_timeout')`).Scan(&chunkStatement))
	if chunkStatement != "0" {
		t.Fatalf("chunk statement_timeout = %q, want 0", chunkStatement)
	}

	var mainScan, chunkScan string
	testutil.Must(t, other.pool.QueryRow(ctx, `SELECT current_setting('enable_indexscan')`).Scan(&mainScan))
	testutil.Must(t, other.chunkPool.QueryRow(ctx, `SELECT current_setting('enable_indexscan')`).Scan(&chunkScan))
	if mainScan != "on" || chunkScan != "off" {
		t.Fatalf("enable_indexscan main=%s chunk=%s, want on and off", mainScan, chunkScan)
	}
}

func TestGetBodyFromMeta(t *testing.T) {
	ctx := testCtx
	bucket := unique("body-meta")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)

	check := func(ref ObjectRef, m *Meta, want []byte, rng ByteRange) {
		t.Helper()
		sm, body, err := testStore.GetBodyFromMeta(ctx, ref, m, rng)
		testutil.Must(t, err)
		if sm.Size != m.Size {
			t.Fatalf("size = %d, want %d", sm.Size, m.Size)
		}
		got, err := io.ReadAll(body)
		testutil.Must(t, err)
		testutil.Must(t, body.Close())
		if !bytes.Equal(got, want) {
			t.Fatalf("range %+v = %q, want %q", rng, got, want)
		}
	}

	inline := []byte("alpha beta gamma")
	if _, err := testStore.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "inline"}, inline, WriteOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	inlineRef := ObjectRef{NS: ns.BucketRef(bucket), Key: "inline"}
	inlineMeta, err := testStore.Meta(ctx, inlineRef)
	testutil.Must(t, err)
	if inlineMeta.Inline == nil {
		t.Fatal("inline metadata did not load the inline body")
	}
	check(inlineRef, inlineMeta, inline, WholeObjectRange())
	check(inlineRef, inlineMeta, inline[6:10], Range(6, 9, -1))
	check(inlineRef, inlineMeta, inline[len(inline)-5:], Range(0, -1, 5))

	chunked := bytes.Repeat([]byte("0123456789abcdef"), 8192)
	w, err := testStore.StartObject(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: "chunked"}, WriteOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	if _, err := w.Write(chunked); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	if _, err := w.Finish(nil); err != nil {
		t.Fatal(err)
	}
	chunkedRef := ObjectRef{NS: ns.BucketRef(bucket), Key: "chunked"}
	chunkedMeta, err := testStore.Meta(ctx, chunkedRef)
	testutil.Must(t, err)
	check(chunkedRef, chunkedMeta, chunked, WholeObjectRange())
	check(chunkedRef, chunkedMeta, chunked[123:5001], Range(123, 5000, -1))

	expired := *chunkedMeta
	past := time.Now().Add(-time.Second)
	expired.ExpiresAt = &past
	if _, _, err := testStore.GetBodyFromMeta(ctx, chunkedRef, &expired, WholeObjectRange()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired metadata error = %v, want ErrNotFound", err)
	}
}

func TestCopyAndCopyPart(t *testing.T) {
	ctx := testCtx
	srcBucket := unique("copy-src")
	dstBucket := unique("copy-dst")
	testutil.Must(t, testStore.CreateBucket(ctx, srcBucket))
	testutil.Must(t, testStore.CreateBucket(ctx, dstBucket))
	dropOnCleanup(t, ns.Bucket, dstBucket)
	dropOnCleanup(t, ns.Bucket, srcBucket)

	data := bytes.Repeat([]byte("0123456789abcdef"), 8192)
	srcRef := ObjectRef{NS: ns.BucketRef(srcBucket), Key: "source"}
	w, err := testStore.StartObject(ctx, srcRef, WriteOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	if _, err := w.Write(data); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	srcPub, err := w.Finish(nil)
	testutil.Must(t, err)

	dstRef := ObjectRef{NS: ns.BucketRef(dstBucket), Key: "copy"}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := testStore.Get(ctx, dstRef, WholeObjectRange())
	if err != nil || !bytes.Equal(got.Bytes, data) {
		t.Fatalf("copy read len=%d err=%v", len(got.Bytes), err)
	}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{
		Source: ReadCondition{IfMatch: "00000000000000000000000000000000"},
	}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("source If-Match error = %v, want ErrPrecondition", err)
	}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{
		Source: ReadCondition{IfNoneMatch: srcPub.ETag},
	}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("source If-None-Match error = %v, want ErrPrecondition", err)
	}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{
		Dest: Precondition{Mode: IfAbsent},
	}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("destination IfAbsent error = %v, want ErrPrecondition", err)
	}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{
		Dest: Precondition{Mode: IfMatch, ETag: "00000000000000000000000000000000"},
	}); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("destination If-Match error = %v, want ErrPrecondition", err)
	}
	if _, err := testStore.Copy(ctx, srcRef, dstRef, CopyOptions{
		Source: ReadCondition{IfNoneMatch: "00000000000000000000000000000000"},
		Dest:   Precondition{Mode: IfMatch, ETag: srcPub.ETag},
	}); err != nil {
		t.Fatal(err)
	}
	absentRef := ObjectRef{NS: ns.BucketRef(dstBucket), Key: "conditional-absent"}
	if _, err := testStore.Copy(ctx, srcRef, absentRef, CopyOptions{
		Dest: Precondition{Mode: IfAbsent},
	}); err != nil {
		t.Fatal(err)
	}

	uploadRef := ObjectRef{NS: ns.BucketRef(dstBucket), Key: "mp"}
	uploadID, err := testStore.CreateUpload(ctx, uploadRef, UploadOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	pub, ok, err := testStore.CopyPart(ctx, uploadID, uploadRef, dstRef, 1)
	if err != nil || !ok {
		t.Fatalf("copy part ok=%v err=%v", ok, err)
	}
	parts, truncated, _, err := testStore.ListParts(ctx, uploadRef, uploadID, 10, 0)
	if err != nil || truncated || len(parts) != 1 || parts[0].ETag != pub.ETag {
		t.Fatalf("parts=%+v truncated=%v err=%v", parts, truncated, err)
	}
	done, err := testStore.CompleteUpload(ctx, uploadRef, uploadID,
		[]ListedPart{{PartNo: 1, ETag: pub.ETag}}, WriteOptions{})
	if err != nil || done.Size != int64(len(data)) {
		t.Fatalf("complete size=%d err=%v", done.Size, err)
	}
}

func mutationCache(t *testing.T) string {
	t.Helper()
	name := unique("mutation")
	testutil.Must(t, testStore.EnsureNamespace(testCtx, ns.Cache, name, nil, nil))
	dropOnCleanup(t, ns.Cache, name)
	return name
}

func TestCacheSetReturnOldConditions(t *testing.T) {
	name := mutationCache(t)
	set := func(value []byte, cond Condition) ([]byte, bool, bool) {
		t.Helper()
		old, had, wrote, err := testStore.Cache(name).Set(testCtx, "key", value, CacheSetOptions{
			ReturnOld: true, Precondition: Precondition{Mode: cond},
		})
		testutil.Must(t, err)
		return old, had, wrote
	}
	if old, had, wrote := set([]byte("blocked"), IfPresent); old != nil || had || wrote {
		t.Fatalf("XX on missing key = %q %t %t", old, had, wrote)
	}
	if old, had, wrote := set([]byte{}, IfAbsent); old != nil || had || !wrote {
		t.Fatalf("NX on missing key = %q %t %t", old, had, wrote)
	}
	if old, had, wrote := set([]byte("blocked"), IfAbsent); old == nil || len(old) != 0 || !had || wrote {
		t.Fatalf("NX on empty key = %q %t %t", old, had, wrote)
	}
	if old, had, wrote := set([]byte("replaced"), IfPresent); old == nil || len(old) != 0 || !had || !wrote {
		t.Fatalf("XX GET on empty key = %q %t %t", old, had, wrote)
	}
	if old, had, wrote := set(nil, Unconditional); string(old) != "replaced" || !had || !wrote {
		t.Fatalf("SET GET = %q %t %t", old, had, wrote)
	}
	const writers = 8
	previous := make(chan string, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			old, had, wrote, err := testStore.Cache(name).Set(testCtx, "key", fmt.Appendf(nil, "writer-%d", i), CacheSetOptions{ReturnOld: true})
			if err != nil || !had || !wrote {
				t.Errorf("concurrent SET GET had=%t wrote=%t err=%v", had, wrote, err)
			}
			previous <- string(old)
		})
	}
	wg.Wait()
	close(previous)
	seen := map[string]bool{}
	for old := range previous {
		if seen[old] {
			t.Fatalf("two successful replacements returned the same old value %q", old)
		}
		seen[old] = true
	}
}

func TestCacheAtomicMutations(t *testing.T) {
	name := mutationCache(t)
	expires := time.Now().Add(time.Hour).Truncate(time.Microsecond)
	if _, _, _, err := testStore.Cache(name).Set(testCtx, "key", []byte("10"), CacheSetOptions{ExpiresAt: &expires}); err != nil {
		t.Fatal(err)
	}
	if n, err := testStore.Cache(name).Append(testCtx, "key", []byte("0")); err != nil || n != 3 {
		t.Fatalf("append = %d %v", n, err)
	}
	if n, err := testStore.Cache(name).SetRange(testCtx, "key", 0, []byte("2")); err != nil || n != 3 {
		t.Fatalf("setrange = %d %v", n, err)
	}
	if n, err := testStore.Cache(name).IncrBy(testCtx, "key", 1); err != nil || n != 201 {
		t.Fatalf("incr = %d %v", n, err)
	}
	m, err := testStore.MetaHeader(testCtx, ObjectRef{NS: ns.CacheRef(name), Key: "key"})
	if err != nil || m.ExpiresAt == nil || !m.ExpiresAt.Equal(expires) {
		t.Fatalf("mutation changed TTL: %+v %v", m, err)
	}
	for _, off := range []int64{-1, math.MaxInt64} {
		if _, err := testStore.Cache(name).SetRange(testCtx, "key", off, []byte("x")); err == nil {
			t.Fatalf("accepted offset %d", off)
		}
	}
	if _, _, _, err := testStore.Cache(name).Set(testCtx, "empty", []byte{}, CacheSetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Cache(name).IncrBy(testCtx, "empty", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("empty counter = %v", err)
	}
	testutil.Must(t, testStore.CacheTombstone(testCtx, ns.CacheRef(name), "deleted", expires))
	if _, err := testStore.Cache(name).IncrBy(testCtx, "deleted", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("tombstone counter = %v", err)
	}
	const workers, increments = 8, 25
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range increments {
				if _, err := testStore.Cache(name).IncrBy(testCtx, "counter", 1); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	value, ok, err := testStore.Cache(name).Get(testCtx, "counter")
	if err != nil || !ok || string(value) != fmt.Sprint(workers*increments) {
		t.Fatalf("lost increments: %q %t %v", value, ok, err)
	}
	t.Run("concurrent read-modify-write", func(t *testing.T) {
		ctx := testCtx
		cache := unique("atomic")
		testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
		dropOnCleanup(t, ns.Cache, cache)
		if _, _, wrote, err := testStore.Cache(cache).Set(ctx, "k", []byte("a"), CacheSetOptions{}); err != nil || !wrote {
			t.Fatalf("initial set wrote=%v err=%v", wrote, err)
		}
		old, had, wrote, err := testStore.Cache(cache).GetSet(ctx, "k", []byte("b"), CacheSetOptions{})
		if err != nil || !had || !wrote || string(old) != "a" {
			t.Fatalf("getset old=%q had=%v wrote=%v err=%v", old, had, wrote, err)
		}
		const writers = 20
		var wg sync.WaitGroup
		for range writers {
			wg.Go(func() {
				if _, err := testStore.Cache(cache).Append(ctx, "k", []byte("x")); err != nil {
					t.Errorf("append: %v", err)
				}
			})
		}
		wg.Wait()
		if n, err := testStore.Cache(cache).SetRange(ctx, "k", 0, []byte("Z")); err != nil || n != int64(writers+1) {
			t.Fatalf("setrange n=%d err=%v", n, err)
		}
		got, ok, err := testStore.Cache(cache).Get(ctx, "k")
		if err != nil || !ok || string(got) != "Z"+strings.Repeat("x", writers) {
			t.Fatalf("final value len=%d ok=%v err=%v", len(got), ok, err)
		}
	})
}

func TestCacheGetDelMultipartAndRollback(t *testing.T) {
	name := mutationCache(t)
	ref := ObjectRef{NS: ns.CacheRef(name), Key: "multipart"}
	upload, err := testStore.CreateUpload(testCtx, ref, UploadOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	parts := [][]byte{bytes.Repeat([]byte("a"), 5<<20), []byte("tail")}
	listed := make([]ListedPart, len(parts))
	for i, data := range parts {
		writer, err := testStore.StartPart(testCtx, ref, upload, int32(i+1), 0)
		testutil.Must(t, err)
		if _, err := writer.Write(data); err != nil {
			writer.Abort()
			t.Fatal(err)
		}
		pub, err := writer.Finish(nil)
		testutil.Must(t, err)
		listed[i] = ListedPart{PartNo: int32(i + 1), ETag: pub.ETag}
	}
	if _, err := testStore.CompleteUpload(testCtx, ref, upload, listed, WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := testStore.Cache(name).GetDel(testCtx, ref.Key)
	if err != nil || !ok || !bytes.Equal(got, bytes.Join(parts, nil)) {
		t.Fatalf("multipart GETDEL bytes=%d ok=%t err=%v", len(got), ok, err)
	}
	if _, ok, err := testStore.Cache(name).GetDel(testCtx, ref.Key); err != nil || ok {
		t.Fatalf("second GETDEL ok=%t err=%v", ok, err)
	}
	ref.Key = "corrupt"
	if _, _, _, err := testStore.Cache(name).Set(testCtx, ref.Key, bytes.Repeat([]byte("x"), 64<<10), CacheSetOptions{}); err != nil {
		t.Fatal(err)
	}
	m, err := testStore.MetaHeader(testCtx, ref)
	testutil.Must(t, err)
	schema, _ := ref.Schema()
	if _, err := testStore.pool.Exec(testCtx, "DELETE FROM "+schema+".chunks WHERE file_id=$1 AND no=0", m.FileID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := testStore.Cache(name).GetDel(testCtx, ref.Key); err == nil {
		t.Fatal("GETDEL accepted a truncated body")
	}
	if _, err := testStore.MetaHeader(testCtx, ref); err != nil {
		t.Fatalf("failed GETDEL did not roll back deletion: %v", err)
	}
	t.Run("single connection", func(t *testing.T) {
		name := mutationCache(t)
		other, err := Open(testCtx, testStore.pool.Config().ConnString(), 1, testStore.log)
		testutil.Must(t, err)
		defer other.Close()
		data := bytes.Repeat([]byte("x"), 64<<10)
		if _, _, _, err := testStore.Cache(name).Set(testCtx, "key", data, CacheSetOptions{}); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(testCtx, 5*time.Second)
		defer cancel()
		results := make(chan bool, 2)
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				got, found, err := other.Cache(name).GetDel(ctx, "key")
				if err != nil || (found && !bytes.Equal(got, data)) {
					t.Errorf("GETDEL found=%t bytes=%d err=%v", found, len(got), err)
				}
				results <- found
			})
		}
		wg.Wait()
		if first, second := <-results, <-results; first == second {
			t.Fatalf("concurrent GETDEL results = %t, %t", first, second)
		}
		if _, found, err := other.Cache(name+"-absent").GetDel(ctx, "key"); err != nil || found {
			t.Fatalf("GETDEL on missing namespace found=%t err=%v", found, err)
		}
	})
}

func TestCacheExpirationConditions(t *testing.T) {
	name := mutationCache(t)
	for _, condition := range []string{"", "NX", "XX", "GT", "LT"} {
		for _, withTTL := range []bool{false, true} {
			key := fmt.Sprintf("%s-%t", condition, withTTL)
			opts := CacheSetOptions{}
			future := time.Now().Add(time.Hour)
			if withTTL {
				opts.ExpiresAt = &future
			}
			if _, _, _, err := testStore.Cache(name).Set(testCtx, key, []byte("value"), opts); err != nil {
				t.Fatal(err)
			}
			past := time.Now().Add(-time.Minute)
			want := condition == "" || condition == "LT" || (condition == "NX" && !withTTL) || (condition == "XX" && withTTL)
			changed, err := testStore.Cache(name).Expire(testCtx, key, &past, condition)
			if err != nil || changed != want {
				t.Fatalf("condition=%q ttl=%t: changed=%t want=%t err=%v", condition, withTTL, changed, want, err)
			}
			if _, exists, err := testStore.Cache(name).Get(testCtx, key); err != nil || exists == changed {
				t.Fatalf("expired key visibility: exists=%t changed=%t err=%v", exists, changed, err)
			}
		}
	}
}

func TestMetadataDistinguishesMissingKeyFromMissingBucket(t *testing.T) {
	name := unique("metadata-miss")
	testutil.Must(t, testStore.CreateBucket(testCtx, name))
	dropOnCleanup(t, ns.Bucket, name)
	if _, err := testStore.StatNamespace(testCtx, ns.Bucket, name); err != nil {
		t.Fatalf("namespace stats: %v", err)
	}
	if _, err := testStore.Meta(testCtx, ObjectRef{NS: ns.BucketRef(name), Key: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing key: %v", err)
	}
	if _, err := testStore.Meta(testCtx, ObjectRef{NS: ns.BucketRef(unique("absent")), Key: "missing"}); !errors.Is(err, ErrNoBucket) {
		t.Fatalf("missing bucket: %v", err)
	}
	if _, err := testStore.Meta(testCtx, ObjectRef{NS: ns.CacheRef(unique("absent")), Key: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing cache: %v", err)
	}
}

func TestFillPublicationChecksLiveDatabaseOwner(t *testing.T) {
	name := unique("fill-owner")
	ref := ObjectRef{NS: ns.CacheRef(name), Key: "object"}
	testutil.Must(t, testStore.EnsureNamespace(testCtx, ns.Cache, name, nil, nil))
	t.Cleanup(func() {
		_, _ = testStore.DropNamespace(testCtx, ns.Cache, name, true)
		_, _ = testStore.Pool().Exec(testCtx, `DELETE FROM s3p.cache_fill_leases WHERE namespace=$1`, name)
	})
	lease := FillLease{Namespace: name, Key: ref.Key, Owner: "first"}
	if ok, err := testStore.Cache(name).AcquireFillLease(testCtx, ref.Key, lease.Owner, time.Minute); err != nil || !ok {
		t.Fatal(err)
	}
	if _, err := testStore.PutInline(testCtx, ref, []byte("original"), WriteOptions{FillLease: &lease}); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Pool().Exec(testCtx, `UPDATE s3p.cache_fill_leases SET expires_at=now()-interval '1 second' WHERE namespace=$1`, name); err != nil {
		t.Fatal(err)
	}
	if ok, err := testStore.RenewFillLease(testCtx, lease, time.Minute); err != nil || ok {
		t.Fatalf("revived expired owner: %t %v", ok, err)
	}
	if _, err := testStore.PutInline(testCtx, ref, []byte("stale"), WriteOptions{FillLease: &lease}); !errors.Is(err, ErrStaleCache) {
		t.Fatalf("expired inline owner: %v", err)
	}
	if ok, err := testStore.Cache(name).AcquireFillLease(testCtx, ref.Key, "second", time.Minute); err != nil || !ok {
		t.Fatal(err)
	}
	testStore.Cache(name).ReleaseFillLease(testCtx, ref.Key, "first")
	for _, owner := range []string{"first", "second"} {
		attempt := lease
		attempt.Owner = owner
		w, err := testStore.StartObject(testCtx, ref, WriteOptions{FillLease: &attempt})
		testutil.Must(t, err)
		data := bytes.Repeat([]byte(owner), 20<<10)
		if _, err := w.Write(data); err != nil {
			w.Abort()
			t.Fatal(err)
		}
		_, err = w.Finish(nil)
		w.Abort()
		if owner == "first" {
			if !errors.Is(err, ErrStaleCache) {
				t.Fatalf("superseded COPY owner: %v", err)
			}
		} else if err != nil {
			t.Fatalf("live COPY owner: %v", err)
		}
		_, body, err := testStore.GetBody(testCtx, ref, WholeObjectRange(), nil)
		testutil.Must(t, err)
		got, err := io.ReadAll(body)
		_ = body.Close()
		testutil.Must(t, err)
		if owner == "first" && string(got) != "original" {
			t.Fatal("stale COPY changed the object")
		}
		if owner == "second" && !bytes.Equal(got, data) {
			t.Fatal("live COPY lost bytes")
		}
	}
	t.Run("lease acquire and release", func(t *testing.T) {
		ctx := testCtx
		namespace, key := unique("lease"), "bucket/key"
		t.Cleanup(func() {
			_, _ = testStore.Pool().Exec(ctx, `DELETE FROM s3p.cache_fill_leases WHERE namespace=$1`, namespace)
		})
		ok, err := testStore.Cache(namespace).AcquireFillLease(ctx, key, "owner-a", time.Minute)
		if err != nil || !ok {
			t.Fatalf("first acquire ok=%v err=%v", ok, err)
		}
		if ok, err := testStore.Cache(namespace).AcquireFillLease(ctx, key, "owner-b", time.Minute); err != nil || ok {
			t.Fatalf("second acquire ok=%v err=%v", ok, err)
		}
		testStore.Cache(namespace).ReleaseFillLease(ctx, key, "owner-a")
		if ok, err := testStore.Cache(namespace).AcquireFillLease(ctx, key, "owner-b", time.Minute); err != nil || !ok {
			t.Fatalf("post-release acquire ok=%v err=%v", ok, err)
		}
		testStore.Cache(namespace).ReleaseFillLease(ctx, key, "owner-b")
	})
}

func TestRetiringWriteCacheRevokesOldFillsNotNewWrites(t *testing.T) {
	name := unique("retire-origin")
	for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
		testutil.Must(t, testStore.EnsureNamespace(testCtx, kind, name, nil, nil))
	}
	t.Cleanup(func() {
		for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
			_, _ = testStore.DropNamespace(testCtx, kind, name, true)
		}
	})
	write := ObjectRef{NS: ns.WriteBackRef(name), Key: "bkt/key"}
	read := ObjectRef{NS: ns.CacheRef(name), Key: write.Key}
	if _, err := testStore.PutInline(testCtx, write, []byte("same"), WriteOptions{OriginETag: "origin"}); err != nil {
		t.Fatal(err)
	}
	old, err := testStore.Meta(testCtx, write)
	testutil.Must(t, err)
	testutil.Must(t, testStore.CacheTombstone(testCtx, read.NS, read.Key, time.Now().Add(time.Hour)))
	lease := FillLease{Namespace: name, Key: read.Key, Owner: "old-fill"}
	if ok, err := testStore.Cache(name).AcquireFillLease(testCtx, read.Key, lease.Owner, time.Minute); err != nil || !ok {
		t.Fatal(err)
	}

	if _, err := testStore.PutInline(testCtx, write, []byte("same"), WriteOptions{OriginETag: "origin"}); err != nil {
		t.Fatal(err)
	}
	if retired, err := testStore.RetireOriginObject(testCtx, write, old); err != nil || retired {
		t.Fatalf("retired a newer write: %t %v", retired, err)
	}
	current, err := testStore.Meta(testCtx, write)
	testutil.Must(t, err)
	if retired, err := testStore.RetireOriginObject(testCtx, write, current); err != nil || !retired {
		t.Fatalf("retire observed version: %t %v", retired, err)
	}
	if _, err := testStore.Meta(testCtx, read); !errors.Is(err, ErrNotFound) {
		t.Fatalf("write fence remained: %v", err)
	}
	if _, err := testStore.PutInline(testCtx, read, []byte("stale"), WriteOptions{FillLease: &lease}); !errors.Is(err, ErrStaleCache) {
		t.Fatalf("revoked owner resurrected bytes: %v", err)
	}
}

func TestOriginMetadataTTLFlushAndCacheOnlyLookup(t *testing.T) {
	name := unique("parent")
	testutil.Must(t, testStore.EnsureNamespace(testCtx, ns.Cache, name, nil, nil))
	t.Cleanup(func() {
		for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
			_, _ = testStore.DropNamespace(testCtx, kind, name, true)
		}
	})
	info := ObjectInfo{Size: 10 << 20, OriginETag: "version", ContentType: "application/parquet"}
	testutil.Must(t, testStore.Cache(name).RememberOriginMetadata(testCtx, "parent", info, time.Now(), time.Hour))
	for _, table := range []string{"objects", "chunks", "origin_metadata"} {
		var persistence string
		if err := testStore.Pool().QueryRow(testCtx, `SELECT relpersistence::text FROM pg_class WHERE oid=$1::regclass`, `"s3c_`+name+`".`+table).Scan(&persistence); err != nil || persistence != "p" {
			t.Fatalf("%s is not logged: %s %v", table, persistence, err)
		}
	}
	schema, _ := nsSQLIdent(ns.Cache, name)
	if _, err := testStore.Pool().Exec(testCtx, `UPDATE `+schema+`.origin_metadata SET validated_at=now()-interval '5 seconds'`); err != nil {
		t.Fatal(err)
	}
	if m, err := testStore.Cache(name).OriginMetadata(testCtx, "parent", time.Second); err != nil || m != nil {
		t.Fatalf("ignored reader TTL: %+v %v", m, err)
	}
	if m, err := testStore.Cache(name).OriginMetadata(testCtx, "parent", time.Hour); err != nil || m == nil {
		t.Fatalf("lost fresh parent: %+v %v", m, err)
	}
	ref := ObjectRef{NS: ns.CacheRef(name), Key: "body"}
	if _, err := testStore.PutInline(testCtx, ref, []byte("body"), WriteOptions{}); err != nil {
		t.Fatal(err)
	}
	m, selected, err := testStore.Cache(name).Metadata(testCtx, "body", true)
	if err != nil || selected.Kind != ns.Cache || string(m.Inline) != "body" {
		t.Fatalf("cache-only lookup: %+v %v %v", m, selected, err)
	}
	testutil.Must(t, testStore.Cache(name).Flush(testCtx))
	if m, err := testStore.Cache(name).OriginMetadata(testCtx, "parent", time.Hour); err != nil || m != nil {
		t.Fatalf("flush retained parent: %+v %v", m, err)
	}
}

func TestObjectModelAcrossWorkers(t *testing.T) {
	ctx := testCtx
	bucket := unique("model")
	testutil.Must(t, testStore.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)

	other, err := Open(ctx, testStore.Pool().Config().ConnString(), 8,
		testutil.DiscardLogger())
	testutil.Must(t, err)
	t.Cleanup(other.Close)

	stores := []*Store{testStore, other}
	model := map[string][]byte{}
	keys := make([]string, 0, 24)
	for i := range 24 {
		keys = append(keys, fmt.Sprintf("k-%02d", i))
	}
	rng := mathrand.New(mathrand.NewSource(42))

	put := func(st *Store, key string, data []byte) error {
		if len(data) <= MaxInlineBytes {
			_, err := st.PutInline(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key}, data, WriteOptions{ContentType: "application/octet-stream"})
			return err
		}
		w, err := st.StartObject(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key}, WriteOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			w.Abort()
			return err
		}
		_, err = w.Finish(nil)
		return err
	}

	check := func(step int) {
		t.Helper()
		for _, st := range stores {
			for _, key := range keys {
				want, exists := model[key]
				got, err := st.Get(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key}, WholeObjectRange())
				if !exists {
					if err == nil {
						t.Fatalf("step %d store %p key %s: got %d bytes, want not found", step, st, key, len(got.Bytes))
					}
					continue
				}
				if err != nil {
					t.Fatalf("step %d store %p key %s: %v", step, st, key, err)
				}
				if !bytes.Equal(got.Bytes, want) {
					t.Fatalf("step %d store %p key %s: got %d bytes, want %d", step, st, key, len(got.Bytes), len(want))
				}
			}
			rows, err := st.List(ctx, ns.BucketRef(bucket), "k-", "k-\U0010FFFF", "", 1000)
			if err != nil {
				t.Fatalf("step %d list: %v", step, err)
			}
			gotKeys := make([]string, 0, len(rows))
			for _, row := range rows {
				gotKeys = append(gotKeys, row.Key)
			}
			wantKeys := make([]string, 0, len(model))
			for key := range model {
				wantKeys = append(wantKeys, key)
			}
			sort.Strings(wantKeys)
			if !slices.Equal(gotKeys, wantKeys) {
				t.Fatalf("step %d list keys = %v, want %v", step, gotKeys, wantKeys)
			}
		}
	}

	for step := 0; step < 180; step++ {
		st := stores[rng.Intn(len(stores))]
		key := keys[rng.Intn(len(keys))]
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4:
			n := rng.Intn(120 << 10)
			if n <= 4 {
				n = 0
			}
			data := bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, n)
			if err := put(st, key, data); err != nil {
				t.Fatalf("step %d put %s: %v", step, key, err)
			}
			model[key] = data
		case 5, 6:
			if _, err := st.Delete(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key}); err != nil {
				t.Fatalf("step %d delete %s: %v", step, key, err)
			}
			delete(model, key)
		case 7, 8:

			if data, ok := model[key]; ok {
				otherSt := stores[(rng.Intn(len(stores))+1)%len(stores)]
				if err := put(otherSt, key, data); err != nil {
					t.Fatalf("step %d rewrite %s: %v", step, key, err)
				}
			}
		default:

			st, otherSt := stores[0], stores[1]
			_, err1 := st.MetaHeader(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key})
			_, err2 := otherSt.MetaHeader(ctx, ObjectRef{NS: ns.BucketRef(bucket), Key: key})
			_, exists := model[key]
			if (err1 == nil) != exists || (err2 == nil) != exists {
				t.Fatalf("step %d meta key %s: exists=%v err1=%v err2=%v", step, key, exists, err1, err2)
			}
		}
		if step%10 == 0 {
			check(step)
		}
	}
	check(180)
}

func TestMaintenanceExpiry(t *testing.T) {
	ctx := testCtx
	q := unique("ojs-retention")
	t.Cleanup(func() {
		testStore.SetJobRetention(DefaultJobRetention)
		_, _ = testStore.DropNamespace(ctx, ns.Queue, q, true)
	})
	testStore.SetJobRetention(time.Millisecond)

	jobs, err := testStore.EnqueueJobs(ctx, []EnqueueInput{{
		Type: "retention.test", Args: []byte("[]"), Queue: q,
	}})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	fetched, err := testStore.FetchJobs(ctx, []string{q}, 1, "worker-retention", time.Second)
	if err != nil || len(fetched) != 1 {
		t.Fatalf("fetch = %+v, %v", fetched, err)
	}
	if _, err := testStore.AckJob(ctx, fetched[0].ID, fetched[0].LeaseToken, nil); err != nil {
		t.Fatalf("ack: %v", err)
	}
	sc, err := nsSQLIdent(ns.Queue, q)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, "UPDATE "+sc+".jobs SET completed_at = now() - interval '1 hour' WHERE id = $1", jobs[0].ID); err != nil {
		t.Fatal(err)
	}

	removed, err := testStore.queue(q).ExpireMessages(ctx, ExpireBatch)
	if err != nil || removed != 1 {
		t.Fatalf("expire = %d, %v; want 1", removed, err)
	}
	if _, err := testStore.GetJob(ctx, jobs[0].ID); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("GetJob after retention = %v, want ErrJobNotFound", err)
	}
	t.Run("expired lease exhausts attempts", func(t *testing.T) {
		ctx := testCtx
		q := unique("ojs-expired")
		dropOnCleanup(t, ns.Queue, q)

		jobs, err := testStore.EnqueueJobs(ctx, []EnqueueInput{{
			Type: "expired.test", Args: []byte("[]"), Queue: q, MaxAttempts: 1,
		}})
		if err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		fetched, err := testStore.FetchJobs(ctx, []string{q}, 1, "worker-expired", 5*time.Millisecond)
		if err != nil || len(fetched) != 1 {
			t.Fatalf("fetch = %+v, %v", fetched, err)
		}
		sc, err := nsSQLIdent(ns.Queue, q)
		testutil.Must(t, err)
		if _, err := testStore.pool.Exec(ctx, "UPDATE "+sc+".jobs SET lease_until = now() - interval '1 second' WHERE id = $1", jobs[0].ID); err != nil {
			t.Fatal(err)
		}

		discarded, err := testStore.queue(q).DiscardExpiredLeases(ctx, ExpireBatch)
		if err != nil || discarded != 1 {
			t.Fatalf("discard = %d, %v; want 1", discarded, err)
		}
		got, err := testStore.GetJob(ctx, jobs[0].ID)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got.State != JobDiscarded || got.DiscardedAt == nil {
			t.Fatalf("job = %+v, want discarded", got)
		}
	})
}

func TestHeartbeatJobsExtendsLeasesAcrossQueues(t *testing.T) {
	ctx := testCtx
	queues := []string{unique("ojs-hb-a"), unique("ojs-hb-b")}
	for _, q := range queues {
		q := q
		dropOnCleanup(t, ns.Queue, q)
		_, err := testStore.EnqueueJobs(ctx, []EnqueueInput{{
			Type: "heartbeat.test", Args: []byte("[]"), Queue: q,
		}})
		if err != nil {
			t.Fatalf("enqueue %s: %v", q, err)
		}
	}

	jobs, err := testStore.FetchJobs(ctx, queues, 2, "worker-hb", 10*time.Second)
	testutil.Must(t, err)
	if len(jobs) != 2 {
		t.Fatalf("fetched %d jobs, want 2", len(jobs))
	}
	leases := make(map[string]string, len(jobs))
	before := make(map[string]time.Time, len(jobs))
	jobQueues := make(map[string]string, len(jobs))
	for _, j := range jobs {
		if j.State != JobActive || j.LeaseToken == "" || j.LeaseUntil == nil {
			t.Fatalf("job not leased: %+v", j)
		}
		leases[j.ID] = j.LeaseToken
		before[j.ID] = *j.LeaseUntil
		jobQueues[j.ID] = j.Queue
	}
	for _, q := range queues {
		found := false
		for _, got := range jobQueues {
			if got == q {
				found = true
			}
		}
		if !found {
			t.Fatalf("no job leased from queue %s: %+v", q, jobQueues)
		}
	}

	extended, err := testStore.HeartbeatJobs(ctx, "worker-hb", leases, 2*time.Minute)
	testutil.Must(t, err)
	if len(extended) != len(leases) {
		t.Fatalf("extended %d ids, want %d: %+v", len(extended), len(leases), extended)
	}
	returned := make(map[string]bool, len(extended))
	for _, id := range extended {
		returned[id] = true
	}
	for id := range leases {
		if !returned[id] {
			t.Fatalf("heartbeat omitted id %s: %+v", id, extended)
		}
	}

	for id := range leases {
		j, err := testStore.GetJob(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		if j.State != JobActive || j.LeaseUntil == nil || !j.LeaseUntil.After(before[id]) {
			t.Fatalf("queue %s lease not extended: before=%s after=%+v", jobQueues[id], before[id], j.LeaseUntil)
		}
	}
}

func TestMissingQueuesIgnored(t *testing.T) {
	ctx := testCtx
	q := unique("ojs-fetch-missing")
	dropOnCleanup(t, ns.Queue, q)
	if _, err := testStore.EnqueueJobs(ctx, []EnqueueInput{{
		Type: "missing.test", Args: []byte("[]"), Queue: q,
	}}); err != nil {
		t.Fatal(err)
	}
	jobs, err := testStore.FetchJobs(ctx, []string{unique("ojs-no-such-queue"), q}, 1, "worker-missing", time.Second)
	testutil.Must(t, err)
	if len(jobs) != 1 || jobs[0].Queue != q {
		t.Fatalf("jobs = %+v, want one from %s", jobs, q)
	}

	jobs, err = testStore.FetchJobs(ctx, []string{q, unique("ojs-no-such-queue")}, 1, "worker-missing-2", time.Second)
	testutil.Must(t, err)
	if len(jobs) != 0 {
		t.Fatalf("second fetch = %+v, want no second job", jobs)
	}
	t.Run("any queue exists", func(t *testing.T) {
		ctx := testCtx
		q := unique("ojs-any-exists")
		dropOnCleanup(t, ns.Queue, q)
		testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
		if ok, err := testStore.AnyQueueExists(ctx, []string{unique("ojs-missing"), q}); err != nil || !ok {
			t.Fatalf("AnyQueueExists = %v, %v; want true", ok, err)
		}
		if ok, err := testStore.AnyQueueExists(ctx, []string{unique("ojs-missing")}); err != nil || ok {
			t.Fatalf("AnyQueueExists = %v, %v; want false", ok, err)
		}
	})
}

func TestClaimTransitions(t *testing.T) {
	var now time.Time
	testutil.Must(t, testStore.pool.QueryRow(testCtx, "SELECT now()").Scan(&now))
	past := now.Add(-time.Second)
	future := now.Add(time.Hour)
	type claimTransition struct {
		id      string
		state   JobState
		attempt int
	}

	tests := []struct {
		name   string
		job    Job
		want   claimTransition
		wantOK bool
	}{
		{
			name:   "available becomes active",
			job:    Job{ID: "a", State: JobAvailable, Attempt: 0, MaxAttempts: 3},
			want:   claimTransition{id: "a", state: JobActive, attempt: 1},
			wantOK: true,
		},
		{
			name: "scheduled due becomes active",
			job: Job{ID: "b", State: JobScheduled, Attempt: 0, MaxAttempts: 3,
				ScheduledAt: &past},
			want:   claimTransition{id: "b", state: JobActive, attempt: 1},
			wantOK: true,
		},
		{
			name: "retryable due keeps attempt",
			job: Job{ID: "c", State: JobRetryable, Attempt: 2, MaxAttempts: 3,
				ScheduledAt: &past},
			want:   claimTransition{id: "c", state: JobActive, attempt: 2},
			wantOK: true,
		},
		{
			name:   "scheduled without timestamp is due",
			job:    Job{ID: "d", State: JobScheduled, Attempt: 0, MaxAttempts: 3},
			want:   claimTransition{id: "d", state: JobActive, attempt: 1},
			wantOK: true,
		},
		{
			name: "scheduled future is not claimable",
			job: Job{ID: "e", State: JobScheduled, Attempt: 0, MaxAttempts: 3,
				ScheduledAt: &future},
			wantOK: false,
		},
		{
			name: "expired lease requeues and counts attempt",
			job: Job{ID: "f", State: JobActive, Attempt: 1, MaxAttempts: 3,
				LeaseUntil: &past},
			want:   claimTransition{id: "f", state: JobActive, attempt: 2},
			wantOK: true,
		},
		{
			name: "expired lease exhausts attempts",
			job: Job{ID: "g", State: JobActive, Attempt: 3, MaxAttempts: 3,
				LeaseUntil: &past},
			wantOK: false,
		},
		{
			name: "live lease is not claimable",
			job: Job{ID: "h", State: JobActive, Attempt: 1, MaxAttempts: 3,
				LeaseUntil: &future},
			wantOK: false,
		},
		{
			name:   "active without lease is expired",
			job:    Job{ID: "i", State: JobActive, Attempt: 1, MaxAttempts: 3},
			want:   claimTransition{id: "i", state: JobActive, attempt: 2},
			wantOK: true,
		},
		{
			name: "zero max attempts discards expired lease",
			job: Job{ID: "j", State: JobActive, Attempt: 0, MaxAttempts: 0,
				LeaseUntil: &past},
			wantOK: false,
		},
		{
			name:   "terminal state is not claimable",
			job:    Job{ID: "k", State: JobCompleted, Attempt: 1, MaxAttempts: 3},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue := unique("claim")
			id := newJobID()
			dropOnCleanup(t, ns.Queue, queue)
			if _, err := testStore.EnqueueJobs(testCtx, []EnqueueInput{{ID: id, Queue: queue,
				Type: "claim", Args: []byte("[]"), MaxAttempts: tc.job.MaxAttempts}}); err != nil {
				t.Fatal(err)
			}
			sc, _ := nsSQLIdent(ns.Queue, queue)
			if _, err := testStore.pool.Exec(testCtx, `UPDATE `+sc+`.jobs
				SET state=$2, attempt=$3, max_attempts=$4, scheduled_at=$5, lease_until=$6 WHERE id=$1`,
				id, tc.job.State.String(), tc.job.Attempt, tc.job.MaxAttempts, tc.job.ScheduledAt, tc.job.LeaseUntil); err != nil {
				t.Fatal(err)
			}
			jobs, err := testStore.FetchJobs(testCtx, []string{queue}, 1, "worker", time.Minute)
			testutil.Must(t, err)
			ok := len(jobs) != 0
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if ok {
				got := claimTransition{id: jobs[0].ID, state: jobs[0].State, attempt: jobs[0].Attempt}
				want := tc.want
				want.id = id
				if got != want {
					t.Fatalf("claim = %+v, want %+v", got, want)
				}
				if jobs[0].WorkerID != "worker" || jobs[0].LeaseToken == "" || jobs[0].LeaseUntil == nil || !jobs[0].LeaseUntil.After(now) {
					t.Fatalf("claim lost lease fields: %+v", jobs[0])
				}
			}
		})
	}
}

func TestStreamAppend(t *testing.T) {
	ctx := testCtx
	queue := unique("stream-append-rollback")
	dropOnCleanup(t, ns.Queue, queue)
	if _, err := testStore.Queue(queue).Subscribe(ctx, "reader", ptrInt64(0)); err != nil {
		t.Fatal(err)
	}
	_, err := testStore.Queue(queue).AppendMessages(ctx, []StreamMessageInput{
		{ID: "valid", Payload: []byte(`{"ok":true}`)},
		{ID: "invalid", Payload: []byte(`not json`)},
	})
	if err == nil {
		t.Fatal("invalid second message was accepted")
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	testutil.Must(t, err)
	var count int
	testutil.Must(t, testStore.pool.QueryRow(ctx, "SELECT count(*) FROM "+sc+".stream_events").Scan(&count))
	if count != 0 {
		t.Fatalf("failed append committed %d messages", count)
	}
	t.Run("stored in input order", func(t *testing.T) {
		ctx := testCtx
		q := unique("stream-order")
		dropOnCleanup(t, ns.Queue, q)
		testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
		inputs := make([]StreamMessageInput, 6)
		want := make([]string, len(inputs))
		for i := range inputs {
			want[i] = fmt.Sprintf("evt-%d", i)
			inputs[i] = StreamMessageInput{ID: want[i], Payload: []byte(`{}`)}
		}
		if _, err := testStore.Queue(q).AppendMessages(ctx, inputs); err != nil {
			t.Fatal(err)
		}
		sc, err := nsSQLIdent(ns.Queue, q)
		testutil.Must(t, err)
		rows, err := testStore.pool.Query(ctx, "SELECT event_id FROM "+sc+".stream_events ORDER BY seq")
		testutil.Must(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id string
			testutil.Must(t, rows.Scan(&id))
			got = append(got, id)
		}
		testutil.Must(t, rows.Err())
		if !slices.Equal(got, want) {
			t.Fatalf("stored order = %v, want %v", got, want)
		}
	})
}

func TestStreamFanoutAndAck(t *testing.T) {
	ctx := testCtx
	q := unique("stream-fanout")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	for _, consumer := range []string{"a", "b"} {
		if _, err := testStore.Queue(q).Subscribe(ctx, consumer, ptrInt64(0)); err != nil {
			t.Fatalf("subscribe %s: %v", consumer, err)
		}
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{Type: "t", Payload: []byte(`{"n":1}`)},
		{Type: "t", Payload: []byte(`{"n":2}`)},
	}); err != nil {
		t.Fatal(err)
	}

	a, err := testStore.Queue(q).ReceiveBatch(ctx, "a", 10)
	if err != nil || len(a.Messages) != 2 || a.ID == 0 {
		t.Fatalf("a receive = %+v, %v", a, err)
	}
	b, err := testStore.Queue(q).ReceiveBatch(ctx, "b", 10)
	if err != nil || len(b.Messages) != 2 || b.ID == 0 {
		t.Fatalf("b receive = %+v, %v", b, err)
	}
	if a.ID == b.ID {
		t.Fatal("independent consumers shared a batch")
	}

	again, err := testStore.Queue(q).ReceiveBatch(ctx, "a", 1)
	if err != nil || again.ID != a.ID || len(again.Messages) != 2 {
		t.Fatalf("active batch = %+v, %v", again, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	empty, err := testStore.Queue(q).ReceiveBatch(ctx, "a", 10)
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("post-ack receive = %+v, %v", empty, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{Type: "t", Payload: []byte(`{"n":3}`)},
	}); err != nil {
		t.Fatal(err)
	}
	next, err := testStore.Queue(q).ReceiveBatch(ctx, "a", 10)
	if err != nil || len(next.Messages) != 1 {
		t.Fatalf("next receive = %+v, %v", next, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, next.ID); err != nil {
		t.Fatal(err)
	}
	t.Run("receive ceiling", func(t *testing.T) {
		ctx := testCtx
		q := unique("stream-limit")
		dropOnCleanup(t, ns.Queue, q)
		testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
		if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
			t.Fatal(err)
		}
		if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
			{Payload: []byte(`{"n":1}`)}, {Payload: []byte(`{"n":2}`)}, {Payload: []byte(`{"n":3}`)},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 2); !errors.Is(err, ErrStreamBatchTooLarge) {
			t.Fatalf("ceiling err = %v, want ErrStreamBatchTooLarge", err)
		}
		batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
		if err != nil || len(batch.Messages) != 3 {
			t.Fatalf("full receive = %+v, %v", batch, err)
		}
		if _, err := testStore.Queue(q).AckBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStreamTicks(t *testing.T) {
	ctx := testCtx
	q := unique("stream-prune")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	sc, err := nsSQLIdent(ns.Queue, q)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, `INSERT INTO `+sc+`.stream_ticks (max_seq, created_at)
		SELECT 0, now() - interval '1 hour' FROM generate_series(1, 3)`); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).SetConfig(ctx, -1, time.Minute); err != nil {
		t.Fatal(err)
	}
	if n, err := testStore.queue(q).PruneTicks(ctx, 2); err != nil || n != 2 {
		t.Fatalf("first prune = %d, %v; want 2", n, err)
	}
	if n, err := testStore.queue(q).PruneTicks(ctx, ExpireBatch); err != nil || n != 1 {
		t.Fatalf("second prune = %d, %v; want 1", n, err)
	}
	var left int
	if err := testStore.pool.QueryRow(ctx, `SELECT count(*) FROM `+sc+`.stream_ticks`).Scan(&left); err != nil || left != 1 {
		t.Fatalf("ticks left = %d, %v; want the minimum tick only", left, err)
	}
	t.Run("empty receive writes no tick", func(t *testing.T) {
		ctx := testCtx
		q := unique("stream-idle-ticks")
		dropOnCleanup(t, ns.Queue, q)
		testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
		if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
			t.Fatal(err)
		}
		sc, err := nsSQLIdent(ns.Queue, q)
		testutil.Must(t, err)
		countTicks := func() int64 {
			t.Helper()
			var n int64
			testutil.Must(t, testStore.pool.QueryRow(ctx, `SELECT count(*) FROM `+sc+`.stream_ticks`).Scan(&n))
			return n
		}
		if n := countTicks(); n != 1 {
			t.Fatalf("initial ticks = %d, want 1", n)
		}
		for range 3 {
			if batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10); err != nil {
				t.Fatal(err)
			} else if len(batch.Messages) != 0 {
				t.Fatalf("empty receive = %+v", batch)
			}
		}
		if n := countTicks(); n != 1 {
			t.Fatalf("ticks after empty polls = %d, want 1", n)
		}
		if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{{Payload: []byte(`{"n":1}`)}}); err != nil {
			t.Fatal(err)
		}
		batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
		if err != nil || len(batch.Messages) != 1 {
			t.Fatalf("receive = %+v, %v", batch, err)
		}
		if n := countTicks(); n != 2 {
			t.Fatalf("ticks after event = %d, want 2", n)
		}
		if _, err := testStore.Queue(q).AckBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStreamErrorMappings(t *testing.T) {
	ctx := testCtx
	q := unique("stream-errors")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{{ID: "evt-1", Payload: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
	if err != nil || len(batch.Messages) != 1 {
		t.Fatalf("receive = %+v, %v", batch, err)
	}
	ackTwice := func() error {
		if _, err := testStore.Queue(q).AckBatch(ctx, batch.ID); err != nil {
			return err
		}
		_, err := testStore.Queue(q).AckBatch(ctx, batch.ID)
		return err
	}
	tests := []struct {
		name string
		run  func() error
		want error
	}{
		{"unknown nack", func() error {
			_, err := testStore.Queue(q).NackMessage(ctx, batch.ID, "missing", time.Second, "x")
			return err
		}, ErrStreamMessageNotFound},
		{"double ack", ackTwice, ErrStreamBatchNotFound},
		{"missing subscribe tick", func() error {
			_, err := testStore.Queue(q).Subscribe(ctx, "late", ptrInt64(999999))
			return err
		}, ErrStreamTickNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
	t.Run("missing queue batch operations", func(t *testing.T) {
		ctx := testCtx
		q := unique("stream-absent")
		tests := []struct {
			name string
			run  func() error
		}{
			{"get", func() error {
				_, err := testStore.Queue(q).GetBatch(ctx, 1)
				return err
			}},
			{"ack", func() error {
				_, err := testStore.Queue(q).AckBatch(ctx, 1)
				return err
			}},
			{"nack", func() error {
				_, err := testStore.Queue(q).NackMessage(ctx, 1, "evt-1", time.Second, "x")
				return err
			}},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if err := tc.run(); !errors.Is(err, ErrStreamBatchNotFound) {
					t.Fatalf("err = %v, want %v", err, ErrStreamBatchNotFound)
				}
			})
		}
	})
}

func TestStreamSnapshotVisibilityAcrossCommitOrder(t *testing.T) {
	ctx := testCtx
	q := unique("stream-snapshot")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
		t.Fatal(err)
	}
	sc, err := nsSQLIdent(ns.Queue, q)
	testutil.Must(t, err)
	txA, err := testStore.pool.Begin(ctx)
	testutil.Must(t, err)
	defer txA.Rollback(context.WithoutCancel(ctx))
	if _, err := txA.Exec(ctx, `INSERT INTO `+sc+`.stream_events
		(segment, event_id, type, payload) VALUES (0,'slow','t','{"n":1}')`); err != nil {
		t.Fatal(err)
	}
	txB, err := testStore.pool.Begin(ctx)
	testutil.Must(t, err)
	if _, err := txB.Exec(ctx, `INSERT INTO `+sc+`.stream_events
		(segment, event_id, type, payload) VALUES (0,'fast','t','{"n":2}')`); err != nil {
		t.Fatal(err)
	}
	testutil.Must(t, txB.Commit(ctx))

	first, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
	if err != nil || len(first.Messages) != 1 || first.Messages[0].ID != "fast" {
		t.Fatalf("first = %+v, %v", first, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	testutil.Must(t, txA.Commit(ctx))
	second, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
	if err != nil || len(second.Messages) != 1 || second.Messages[0].ID != "slow" {
		t.Fatalf("second = %+v, %v", second, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, second.ID); err != nil {
		t.Fatal(err)
	}
}

func TestStreamCooperativeConsumers(t *testing.T) {
	ctx := testCtx
	q := unique("stream-coop")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).Subscribe(ctx, "group", ptrInt64(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{ID: "a1", Payload: []byte(`{"n":1}`)},
		{ID: "a2", Payload: []byte(`{"n":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	m1, err := testStore.Queue(q).ReceiveCoopBatch(ctx, "group", "m1", 10)
	if err != nil || len(m1.Messages) != 2 {
		t.Fatalf("m1 = %+v, %v", m1, err)
	}
	if again, err := testStore.Queue(q).ReceiveCoopBatch(ctx, "group", "m1", 10); err != nil || again.ID != m1.ID {
		t.Fatalf("cooperative replay = %+v, %v", again, err)
	}
	if _, err := testStore.Queue(q).ReceiveBatch(ctx, "group", 10); !errors.Is(err, ErrStreamBatchActive) {
		t.Fatalf("normal receive bypassed cooperative batch: %v", err)
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{ID: "b1", Payload: []byte(`{"n":3}`)},
		{ID: "b2", Payload: []byte(`{"n":4}`)},
	}); err != nil {
		t.Fatal(err)
	}
	m2, err := testStore.Queue(q).ReceiveCoopBatch(ctx, "group", "m2", 10)
	if err != nil || len(m2.Messages) != 2 {
		t.Fatalf("m2 = %+v, %v", m2, err)
	}
	if m1.Messages[0].ID == m2.Messages[0].ID {
		t.Fatal("cooperative members received the same event")
	}

	if _, err := testStore.Queue(q).AckBatch(ctx, m2.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, m1.ID); err != nil {
		t.Fatal(err)
	}
	if normal, err := testStore.Queue(q).ReceiveBatch(ctx, "group", 10); err != nil || len(normal.Messages) != 0 {
		t.Fatalf("normal receive after cooperative ack = %+v, %v", normal, err)
	}
	next, err := testStore.Queue(q).ReceiveCoopBatch(ctx, "group", "m1", 10)
	if err != nil || len(next.Messages) != 0 {
		t.Fatalf("after coop ack = %+v, %v", next, err)
	}
}

func TestStreamNackRetryDLQAndReplay(t *testing.T) {
	ctx := testCtx
	q := unique("stream-dlq")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).SetConfig(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{ID: "evt-1", Type: "t", Payload: []byte(`{"n":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
	if err != nil || len(batch.Messages) != 1 {
		t.Fatalf("receive = %+v, %v", batch, err)
	}
	state, err := testStore.Queue(q).NackMessage(ctx, batch.ID, "evt-1", time.Millisecond, "retry")
	if err != nil || state != "retryable" {
		t.Fatalf("nack = %q, %v", state, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, batch.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if moved, err := testStore.queue(q).ProcessRetries(ctx, 10); err != nil || moved != 1 {
		t.Fatalf("retry move = %d, %v", moved, err)
	}
	retried, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
	if err != nil || len(retried.Messages) != 1 || retried.Messages[0].RetryCount != 1 {
		t.Fatalf("retried = %+v, %v", retried, err)
	}
	state, err = testStore.Queue(q).NackMessage(ctx, retried.ID, "evt-1", time.Millisecond, "dead")
	if err != nil || state != "dead_letter" {
		t.Fatalf("dead nack = %q, %v", state, err)
	}
	if _, err := testStore.Queue(q).AckBatch(ctx, retried.ID); err != nil {
		t.Fatal(err)
	}
	dead, err := testStore.Queue(q).ListDeadLetters(ctx, 10)
	if err != nil || len(dead) != 1 || dead[0].EventID != "evt-1" {
		t.Fatalf("dead = %+v, %v", dead, err)
	}
	sc, err := nsSQLIdent(ns.Queue, q)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, `INSERT INTO `+sc+`.stream_dead_letters
		(batch_id, event_id, type, payload, headers, retry_count, reason)
		VALUES (0,'evt-2','t','{}','{}',1,'direct')`); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).ReplayDeadLetter(ctx, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	if n, err := testStore.Queue(q).ReplayAllDeadLetters(ctx); err != nil || n != 1 {
		t.Fatalf("replay all = %d, %v", n, err)
	}
	if dead, err = testStore.Queue(q).ListDeadLetters(ctx, 10); err != nil || len(dead) != 0 {
		t.Fatalf("after replay = %+v, %v", dead, err)
	}
}

func TestStreamRotation(t *testing.T) {
	ctx := testCtx
	q := unique("stream-rotate")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).SetConfig(ctx, -1, 2*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{Payload: []byte(`{"n":1}`)}, {Payload: []byte(`{"n":2}`)}, {Payload: []byte(`{"n":3}`)},
	}); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		time.Sleep(5 * time.Millisecond)
		if n, err := testStore.queue(q).Rotate(ctx); err != nil || n != 0 {
			t.Fatalf("rotation %d = %d, %v; want 0", i+1, n, err)
		}
	}
	time.Sleep(5 * time.Millisecond)
	if n, err := testStore.queue(q).Rotate(ctx); err != nil || n != 3 {
		t.Fatalf("truncating rotation = %d, %v; want 3", n, err)
	}
	info, err := testStore.Queue(q).Info(ctx)
	if err != nil || info.Events != 0 {
		t.Fatalf("info = %+v, %v", info, err)
	}

	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{Payload: []byte(`{"n":4}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if info, err = testStore.Queue(q).Info(ctx); err != nil || info.Events != 1 {
		t.Fatalf("post-reuse info = %+v, %v", info, err)
	}
	t.Run("pinned consumer", func(t *testing.T) {
		ctx := testCtx
		q := unique("stream-rotate-pin")
		dropOnCleanup(t, ns.Queue, q)
		testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
		if _, err := testStore.Queue(q).SetConfig(ctx, -1, 2*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		if _, err := testStore.Queue(q).Subscribe(ctx, "worker", ptrInt64(0)); err != nil {
			t.Fatal(err)
		}
		if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
			{Payload: []byte(`{"n":1}`)}, {Payload: []byte(`{"n":2}`)},
		}); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 2; i++ {
			time.Sleep(5 * time.Millisecond)
			if n, err := testStore.queue(q).Rotate(ctx); err != nil || n != 0 {
				t.Fatalf("empty rotation %d = %d, %v; want 0", i+1, n, err)
			}
		}
		time.Sleep(5 * time.Millisecond)
		if n, err := testStore.queue(q).Rotate(ctx); err != nil || n != 0 {
			t.Fatalf("pinned rotation = %d, %v; want 0", n, err)
		}
		info, err := testStore.Queue(q).Info(ctx)
		if err != nil || info.Events != 2 {
			t.Fatalf("pinned info = %+v, %v; want 2 events", info, err)
		}

		batch, err := testStore.Queue(q).ReceiveBatch(ctx, "worker", 10)
		if err != nil || len(batch.Messages) != 2 {
			t.Fatalf("receive = %+v, %v", batch, err)
		}
		if _, err := testStore.Queue(q).AckBatch(ctx, batch.ID); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		if n, err := testStore.queue(q).Rotate(ctx); err != nil || n != 2 {
			t.Fatalf("released rotation = %d, %v; want 2", n, err)
		}
	})
}

func TestStreamStatAndSizeIncludeSegments(t *testing.T) {
	ctx := testCtx
	q := unique("stream-stat")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	if _, err := testStore.Queue(q).AppendMessages(ctx, []StreamMessageInput{
		{Payload: []byte(`{"n":1}`)}, {Payload: []byte(`{"n":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	st, err := testStore.StatNamespace(ctx, ns.Queue, q)
	testutil.Must(t, err)
	if st.Events != 2 || st.PhysicalBytes <= 0 {
		t.Fatalf("stats = %+v; want 2 events and nonzero physical bytes", st)
	}
	if size := testStore.namespaceSize(ctx, ns.Queue, q); size <= 0 {
		t.Fatalf("namespace size = %d", size)
	}
	conn, err := testStore.pool.Acquire(ctx)
	testutil.Must(t, err)
	defer conn.Release()
	if err := testStore.checkNamespaceTables(ctx, conn); err != nil {
		t.Fatalf("queue relation check: %v", err)
	}
}

func ptrInt64(v int64) *int64 { return &v }

func TestDiagnoseNamesMissingStreamFunction(t *testing.T) {
	ctx := testCtx
	q := unique("stream-diagnose")
	dropOnCleanup(t, ns.Queue, q)
	testutil.Must(t, testStore.queue(q).Ensure(ctx, nil))
	sc, err := ns.Queue.Schema(q)
	testutil.Must(t, err)
	if _, err := testStore.pool.Exec(ctx, "DROP FUNCTION "+quoteIdent(sc)+".stream_ack(int8)"); err != nil {
		t.Fatal(err)
	}
	d, err := testStore.Diagnose(ctx)
	testutil.Must(t, err)
	if want := sc + ".stream_ack"; !slices.Contains(d.MissingQueueFuncs, want) {
		t.Fatalf("missing queue functions = %v, want %s", d.MissingQueueFuncs, want)
	}
}

func BenchmarkCacheCommands(b *testing.B) {
	for _, size := range []int{256, 64 << 10, 10 << 20} {
		data := make([]byte, size)
		if _, err := rand.Read(data); err != nil {
			b.Fatal(err)
		}
		for _, op := range []string{"GET", "SET", "GETSET", "APPEND", "SETRANGE", "GETDEL"} {
			b.Run(fmt.Sprintf("%d/%s", size, op), func(b *testing.B) {
				name := unique("bench-cache")
				b.Cleanup(func() { _, _ = testStore.DropNamespace(testCtx, ns.Cache, name, true) })
				seed := func() {
					if _, _, _, err := testStore.Cache(name).Set(testCtx, "key", data, CacheSetOptions{}); err != nil {
						b.Fatal(err)
					}
				}
				seed()
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for range b.N {
					if op == "APPEND" || op == "GETDEL" {
						b.StopTimer()
						seed()
						b.StartTimer()
					}
					var err error
					switch op {
					case "GET":
						_, _, err = testStore.Cache(name).Get(testCtx, "key")
					case "SET":
						_, _, _, err = testStore.Cache(name).Set(testCtx, "key", data, CacheSetOptions{})
					case "GETSET":
						_, _, _, err = testStore.Cache(name).GetSet(testCtx, "key", data, CacheSetOptions{})
					case "APPEND":
						_, err = testStore.Cache(name).Append(testCtx, "key", []byte("x"))
					case "SETRANGE":
						_, err = testStore.Cache(name).SetRange(testCtx, "key", 0, []byte("x"))
					case "GETDEL":
						_, _, err = testStore.Cache(name).GetDel(testCtx, "key")
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
	b.Run("increment", func(b *testing.B) {
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("parallel=%t", parallel), func(b *testing.B) {
				name := unique("bench-incr")
				b.Cleanup(func() { _, _ = testStore.DropNamespace(testCtx, ns.Cache, name, true) })
				if _, _, _, err := testStore.Cache(name).Set(testCtx, "counter", []byte("0"), CacheSetOptions{}); err != nil {
					b.Fatal(err)
				}
				incr := func() {
					if _, err := testStore.Cache(name).IncrBy(testCtx, "counter", 1); err != nil {
						b.Error(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				if parallel {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							incr()
						}
					})
				} else {
					for range b.N {
						incr()
					}
				}
			})
		}
	})
}
