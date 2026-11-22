//go:build integration

package store_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

func openIntegrationStore(t *testing.T) *store.Store {
	t.Helper()
	dbURL := os.Getenv("PGVS3_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("PGVS3_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, dbURL, 16, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Init(ctx); err != nil {
		t.Fatal(err)
	}
	// Clean state for isolated store test.
	_, _ = st.Evict(ctx, 0, 1)
	_ = st.DeleteMany(ctx, "st-test", []string{"a", "b", "c", "zero", "multi"})
	return st
}

func TestStoreEvictionLRUAndBudget(t *testing.T) {
	st := openIntegrationStore(t)
	ctx := context.Background()

	// Insert a (1000B), b (1000B), c (1000B) with staggered last_access.
	for _, k := range []string{"a", "b", "c"} {
		id, err := st.UpsertObject(ctx, store.ObjectMeta{
			Bucket: "st-test", Key: k, ETag: "e-" + k, Size: 1000, LastModified: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := st.PutChunk(ctx, id, 0, bytes.Repeat([]byte(k), 1000)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(15 * time.Millisecond)
	}

	// Touch 'a' so 'b' becomes the least-recently-used.
	st.TouchAsync("st-test", "a")
	// Wait briefly for async touch worker to commit.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ma, _ := st.GetObject(ctx, "st-test", "a")
		mc, _ := st.GetObject(ctx, "st-test", "c")
		if ma != nil && mc != nil && ma.LastAccess.After(mc.LastAccess) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Budget 2100B should evict 'b' (oldest) and keep 'a' and 'c'.
	evicted, err := st.Evict(ctx, 0, 2100)
	if err != nil {
		t.Fatal(err)
	}
	if evicted == 0 {
		t.Fatal("expected eviction")
	}
	if _, err := st.GetObject(ctx, "st-test", "b"); err != store.ErrNotFound {
		t.Fatalf("expected b to be evicted, got err=%v", err)
	}
	if _, err := st.GetObject(ctx, "st-test", "a"); err != nil {
		t.Fatalf("expected a to survive LRU eviction, got err=%v", err)
	}
	if _, err := st.GetObject(ctx, "st-test", "c"); err != nil {
		t.Fatalf("expected c to survive LRU eviction, got err=%v", err)
	}
}

func TestStoreCascadeDeleteETagReplaceAndOddTail(t *testing.T) {
	st := openIntegrationStore(t)
	ctx := context.Background()

	// Zero-byte object
	if _, err := st.UpsertObject(ctx, store.ObjectMeta{
		Bucket: "st-test", Key: "zero", ETag: "d41d8cd98f00b204e9800998ecf8427e", Size: 0, LastModified: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	mz, chunk0, err := st.GetSingleChunk(ctx, "st-test", "zero", 0)
	if err != nil || mz.Size != 0 || chunk0 != nil {
		t.Fatalf("zero object: m=%+v chunk=%v err=%v", mz, chunk0, err)
	}

	// Multi-chunk object with odd tail (3 * 64KiB + 137B)
	const cs = 64 << 10
	const tail = 137
	total := int64(3*cs + tail)
	id1, err := st.UpsertObject(ctx, store.ObjectMeta{
		Bucket: "st-test", Key: "multi", ETag: "v1", Size: total, LastModified: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := int32(0); i < 3; i++ {
		if err := st.PutChunk(ctx, id1, i, bytes.Repeat([]byte{byte('a' + i)}, cs)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.PutChunk(ctx, id1, 3, bytes.Repeat([]byte{'z'}, tail)); err != nil {
		t.Fatal(err)
	}

	sr, err := st.OpenChunkStream(ctx, id1, 1, 3, 100, int64(2*cs-100+tail))
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(sr)
	_ = sr.Close()
	if err != nil || int64(len(got)) != int64(2*cs-100+tail) {
		t.Fatalf("stream odd tail len=%d err=%v", len(got), err)
	}

	// ETag change replaces old chunks
	id2, err := st.UpsertObject(ctx, store.ObjectMeta{
		Bucket: "st-test", Key: "multi", ETag: "v2", Size: 10, LastModified: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	chunks, err := st.GetChunks(ctx, id2, 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected old chunks cleared on ETag change, got %d", len(chunks))
	}

	// Delete cascades chunks
	if err := st.PutChunk(ctx, id2, 0, []byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete(ctx, "st-test", "multi"); err != nil {
		t.Fatal(err)
	}
	chunksAfter, err := st.GetChunks(ctx, id2, 0, 0)
	if err != nil || len(chunksAfter) != 0 {
		t.Fatalf("expected cascade delete of chunks, got %d err=%v", len(chunksAfter), err)
	}
}
