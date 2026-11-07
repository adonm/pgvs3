//go:build integration

package store

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/testutil"
)

func cacheNamespace(t *testing.T, prefix string, budget int64) NamespaceInfo {
	t.Helper()
	ctx := context.Background()
	cache := unique(prefix)
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil))
	dropOnCleanup(t, ns.Cache, cache)
	if budget > 0 {
		testutil.Must(t, testStore.SetNamespaceAttrs(ctx, ns.Cache, cache, map[string]any{"max_bytes": budget}))
	}
	infos, err := testStore.Namespaces(ctx, ns.Cache)
	testutil.Must(t, err)
	for _, info := range infos {
		if info.Name == cache {
			return info
		}
	}
	t.Fatalf("cache namespace %q not found", cache)
	return NamespaceInfo{}
}

func putCacheObject(t *testing.T, cache, key string, size int, expires time.Time) {
	t.Helper()
	body := bytes.Repeat([]byte{key[0]}, size)
	_, _, _, err := testStore.Cache(cache).Set(context.Background(), key, body, CacheSetOptions{ExpiresAt: &expires})
	testutil.Must(t, err)
}

func TestCacheCapacityEviction(t *testing.T) {
	ctx := context.Background()
	info := cacheNamespace(t, "evict", 40000)
	if info.maxBytes != 40000 {
		t.Fatalf("decoded max_bytes = %d, want 40000", info.maxBytes)
	}
	now := time.Now()
	putCacheObject(t, info.Name, "a", 10000, now.Add(time.Hour))
	putCacheObject(t, info.Name, "b", 20000, now.Add(2*time.Hour))
	putCacheObject(t, info.Name, "c", 30000, now.Add(3*time.Hour))

	conn, err := testStore.Pool().Acquire(ctx)
	testutil.Must(t, err)
	defer conn.Release()
	evicted, err := testStore.expireNamespace(ctx, conn, quoteIdent(info.Schema), info, 1)
	testutil.Must(t, err)
	if evicted != 2 {
		t.Fatalf("evicted = %d, want 2", evicted)
	}
	for key, want := range map[string]bool{"a": false, "b": false, "c": true} {
		_, ok, err := testStore.Cache(info.Name).Get(ctx, key)
		testutil.Must(t, err)
		if ok != want {
			t.Fatalf("object %q present = %t, want %t", key, ok, want)
		}
	}
	var garbage int
	testutil.Must(t, testStore.Pool().QueryRow(ctx,
		"SELECT count(*) FROM "+quoteIdent(info.Schema)+".garbage").Scan(&garbage))
	if garbage != 2 {
		t.Fatalf("garbage files = %d, want 2", garbage)
	}
}

func TestWriteBackCapacityEvictsOnlyCommittedCopies(t *testing.T) {
	ctx := context.Background()
	name := unique("evict-wb")
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.Cache, name, nil, nil))
	dropOnCleanup(t, ns.Cache, name)
	testutil.Must(t, testStore.EnsureNamespace(ctx, ns.WriteBack, name, nil, map[string]any{"max_bytes": 40000}))
	dropOnCleanup(t, ns.WriteBack, name)
	infos, err := testStore.Namespaces(ctx, ns.WriteBack)
	testutil.Must(t, err)
	var info NamespaceInfo
	for _, candidate := range infos {
		if candidate.Name == name {
			info = candidate
		}
	}
	if info.Name == "" || info.maxBytes != 40000 {
		t.Fatalf("write-back namespace = %+v max_bytes=%d", info, info.maxBytes)
	}
	now := time.Now()
	put := func(key string, size int, opts WriteOptions) {
		t.Helper()
		body := bytes.Repeat([]byte{key[0]}, size)
		_, err := testStore.PutInline(ctx, ObjectRef{NS: ns.WriteBackRef(name), Key: key}, body, opts)
		testutil.Must(t, err)
	}
	for i, size := range []int{10000, 20000, 30000} {
		expires := now.Add(time.Duration(i+1) * time.Hour)
		key := string(rune('a' + i))
		put(key, size, WriteOptions{ExpiresAt: &expires, Publication: NormalPublication("origin-" + key)})
	}
	for _, key := range []string{"pending-one", "pending-two"} {
		w, err := testStore.StartObject(ctx, ObjectRef{NS: ns.WriteBackRef(name), Key: key}, WriteOptions{
			Publication: WriteBackPublication(PendingOrigin{Token: PendingOriginToken(key)}),
		})
		testutil.Must(t, err)
		if _, err := w.Write(bytes.Repeat([]byte{key[0]}, 50000)); err != nil {
			w.Abort()
			t.Fatal(err)
		}
		if _, err := w.Finish(nil); err != nil {
			t.Fatal(err)
		}
	}

	conn, err := testStore.Pool().Acquire(ctx)
	testutil.Must(t, err)
	defer conn.Release()
	evicted, err := testStore.expireNamespace(ctx, conn, quoteIdent(info.Schema), info, 1)
	testutil.Must(t, err)
	if evicted != 2 {
		t.Fatalf("evicted = %d, want 2 committed copies", evicted)
	}
	for key, want := range map[string]bool{
		"a": false, "b": false, "c": true, "pending-one": true, "pending-two": true,
	} {
		_, err := testStore.MetaHeader(ctx, ObjectRef{NS: ns.WriteBackRef(name), Key: key})
		if present := err == nil; present != want {
			t.Fatalf("object %q present = %t (err=%v), want %t", key, present, err, want)
		}
	}
	var used int64
	testutil.Must(t, testStore.Pool().QueryRow(ctx,
		"SELECT COALESCE(SUM(size),0)::int8 FROM "+quoteIdent(info.Schema)+".objects WHERE expires_at IS NOT NULL").Scan(&used))
	if used > info.maxBytes {
		t.Fatalf("committed bytes = %d, budget = %d", used, info.maxBytes)
	}
	var pending int
	testutil.Must(t, testStore.Pool().QueryRow(ctx,
		"SELECT count(*) FROM "+quoteIdent(info.Schema)+".objects WHERE expires_at IS NULL").Scan(&pending))
	if pending != 2 {
		t.Fatalf("pending rows = %d, want 2", pending)
	}
}

func TestCacheCapacityEvictionProgrammaticBudget(t *testing.T) {
	ctx := context.Background()
	info := cacheNamespace(t, "evict-mem", 0)
	if info.maxBytes != 0 {
		t.Fatalf("persisted max_bytes = %d, want 0", info.maxBytes)
	}
	testStore.SetCacheMaxBytes(info.Name, 25000)
	t.Cleanup(func() { testStore.SetCacheMaxBytes(info.Name, 0) })
	now := time.Now()
	putCacheObject(t, info.Name, "a", 10000, now.Add(time.Hour))
	putCacheObject(t, info.Name, "b", 20000, now.Add(2*time.Hour))

	conn, err := testStore.Pool().Acquire(ctx)
	testutil.Must(t, err)
	defer conn.Release()
	evicted, err := testStore.expireNamespace(ctx, conn, quoteIdent(info.Schema), info, 1)
	testutil.Must(t, err)
	if evicted != 1 {
		t.Fatalf("evicted = %d, want 1", evicted)
	}
	if _, ok, err := testStore.Cache(info.Name).Get(ctx, "a"); err != nil || ok {
		t.Fatalf("oldest object present = %t err=%v", ok, err)
	}
	if _, ok, err := testStore.Cache(info.Name).Get(ctx, "b"); err != nil || !ok {
		t.Fatalf("newest object present = %t err=%v", ok, err)
	}
}
