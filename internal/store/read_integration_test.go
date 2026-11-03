//go:build integration

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/testutil"
)

func openReadStore(t *testing.T, mutate func(*OpenOptions)) *Store {
	t.Helper()
	opts := defaultOpenOptions(16)
	opts.IdleTxTimeout = 300 * time.Millisecond
	if mutate != nil {
		mutate(&opts)
	}
	st, err := OpenWithOptions(testCtx, os.Getenv("PGVS3_TEST_DATABASE_URL"), opts, testutil.DiscardLogger())
	testutil.Must(t, err)
	t.Cleanup(st.Close)
	return st
}

func putChunked(t *testing.T, st *Store, ref ObjectRef, data []byte) {
	t.Helper()
	w, err := st.StartObject(testCtx, ref, WriteOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	if _, err := w.Write(data); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	_, err = w.Finish(nil)
	testutil.Must(t, err)
}

func randomBytes(n int, seed int64) []byte {
	b := make([]byte, n)
	mathrand.New(mathrand.NewSource(seed)).Read(b)
	return b
}

func chunkRowsOf(t *testing.T, ref ObjectRef, fileID int64) int {
	t.Helper()
	schema, err := ref.Schema()
	testutil.Must(t, err)
	var n int
	testutil.Must(t, testStore.pool.QueryRow(testCtx, "SELECT count(*) FROM "+schema+".chunks WHERE file_id = $1", fileID).Scan(&n))
	return n
}

// A multi-piece read must survive a slow client longer than the
// idle-in-transaction timeout and an overwrite whose old file the reaper
// tries to delete mid-stream, in both guard modes.
func TestMultiPieceReadSurvivesSlowClientAndReap(t *testing.T) {
	for _, mode := range []string{"pinned", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx := testCtx
			st := openReadStore(t, nil)
			st.forceSnapshotReads = mode == "snapshot"
			bucket := unique("read-guard")
			testutil.Must(t, st.CreateBucket(ctx, bucket))
			dropOnCleanup(t, ns.Bucket, bucket)
			ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "big"}
			original := randomBytes(24<<20, 1)
			putChunked(t, st, ref, original)
			old, err := st.Meta(ctx, ref)
			testutil.Must(t, err)

			_, body, err := st.GetBody(ctx, ref, WholeObjectRange(), nil)
			testutil.Must(t, err)
			defer body.Close()
			head := make([]byte, 512<<10)
			if _, err := io.ReadFull(body, head); err != nil {
				t.Fatal(err)
			}
			putChunked(t, st, ref, randomBytes(24<<20, 2))
			skips := st.reapPinnedSkips.Load()
			testutil.Must(t, st.Maintain(ctx, 0))
			if mode == "pinned" {
				if st.reapPinnedSkips.Load() == skips {
					t.Fatal("reaper did not skip the pinned file")
				}
				if rows := chunkRowsOf(t, ref, old.FileID); rows == 0 {
					t.Fatal("reaper deleted a pinned file")
				}
			}
			time.Sleep(time.Second)
			rest, err := io.ReadAll(body)
			testutil.Must(t, err)
			if got := append(head, rest...); !bytes.Equal(got, original) {
				t.Fatalf("read %d bytes, mismatch against the original %d", len(got), len(original))
			}
			testutil.Must(t, body.Close())
			if mode == "pinned" && st.pinnedReads.Load() == 0 {
				t.Fatal("read was not pinned")
			}
			if mode == "snapshot" && st.snapshotReads.Load() == 0 {
				t.Fatal("read did not use a snapshot")
			}
			testutil.Must(t, st.Maintain(ctx, 0))
			if rows := chunkRowsOf(t, ref, old.FileID); rows != 0 {
				t.Fatalf("old file still has %d rows after the read released it", rows)
			}
		})
	}
}

// The exported snapshot must stay importable while a worker waits for a
// connection, and an importing worker must survive idling between pieces.
func TestReadGuardSnapshotOutlivesIdleTimeout(t *testing.T) {
	ctx := testCtx
	st := openReadStore(t, nil)
	st.forceSnapshotReads = true
	bucket := unique("read-guard-idle")
	testutil.Must(t, st.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "obj"}
	putChunked(t, st, ref, randomBytes(2<<20, 6))
	m, err := st.Meta(ctx, ref)
	testutil.Must(t, err)
	pieces, err := planPieces(m, 0, m.Size-1, 2)
	testutil.Must(t, err)
	guard, err := st.openReadGuard(ctx, pieces, 2)
	testutil.Must(t, err)
	defer guard.close()
	schema, err := ref.Schema()
	testutil.Must(t, err)
	first, doneFirst := guard.lead()
	defer doneFirst()
	time.Sleep(time.Second)
	second, doneSecond, err := guard.join(ctx)
	testutil.Must(t, err)
	defer doneSecond()
	time.Sleep(time.Second)
	for i, q := range []querier{first, second} {
		var got []byte
		err := streamRows(ctx, q, schema, pieces[i], 0, m.Size-1, nil, func(b []byte) bool {
			got = append(got, b...)
			return true
		})
		testutil.Must(t, err)
		if len(got) == 0 {
			t.Fatalf("worker %d read nothing", i)
		}
	}
}

// A multi-piece read that gets only its guard connection must still finish:
// the guard session does the read's work instead of waiting on the pool for
// a worker connection that other reads hold.
func TestMultiPieceReadProgressesOnGuardConnection(t *testing.T) {
	for _, mode := range []string{"pinned", "snapshot"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testCtx, 30*time.Second)
			defer cancel()
			st := openReadStore(t, func(o *OpenOptions) { o.MaxConns = 6 })
			st.forceSnapshotReads = mode == "snapshot"
			bucket := unique("read-guard-pool")
			testutil.Must(t, st.CreateBucket(ctx, bucket))
			dropOnCleanup(t, ns.Bucket, bucket)
			ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "big"}
			data := randomBytes(16<<20, 7)
			putChunked(t, st, ref, data)
			for range 5 {
				held, err := st.chunkPool.Acquire(ctx)
				testutil.Must(t, err)
				defer held.Release()
			}
			got, err := st.Get(ctx, ref, WholeObjectRange())
			testutil.Must(t, err)
			if !bytes.Equal(got.Bytes, data) {
				t.Fatalf("read %d bytes, mismatch against %d", len(got.Bytes), len(data))
			}
		})
	}
}

// A chunked write that holds the only main-pool connection must still be able
// to allocate a file ID block.
func TestChunkedWriteOnSingleConnectionPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(testCtx, 30*time.Second)
	defer cancel()
	st := openReadStore(t, func(o *OpenOptions) { o.MaxConns = 1 })
	bucket := unique("write-one-conn")
	testutil.Must(t, st.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "obj"}
	data := randomBytes(1<<20, 8)
	w, err := st.StartObject(ctx, ref, WriteOptions{ContentType: "application/octet-stream"})
	testutil.Must(t, err)
	if _, err := w.Write(data); err != nil {
		w.Abort()
		t.Fatal(err)
	}
	_, err = w.Finish(nil)
	testutil.Must(t, err)
	got, err := st.Get(ctx, ref, WholeObjectRange())
	testutil.Must(t, err)
	if !bytes.Equal(got.Bytes, data) {
		t.Fatal("read back different bytes")
	}
}

func TestCacheReadManyAssemblesUnorderedRows(t *testing.T) {
	for _, cacheBytes := range []int64{0, 64 << 20} {
		t.Run(fmt.Sprintf("read-cache-%d", cacheBytes), func(t *testing.T) {
			ctx := testCtx
			st := openReadStore(t, func(o *OpenOptions) { o.ReadCacheBytes = cacheBytes })
			name := unique("read-many")
			ref := ns.CacheRef(name)
			dropOnCleanup(t, ns.Cache, name)
			want := map[string][]byte{
				"inline": []byte("small"),
				"empty":  {},
				"one":    randomBytes(3*RowBytes+17, 9),
				"two":    randomBytes(40*RowBytes, 10),
				"three":  randomBytes(130*RowBytes+5, 11),
			}
			for key, data := range want {
				obj := ObjectRef{NS: ref, Key: key}
				if key == "inline" {
					_, err := st.PutInline(ctx, obj, data, WriteOptions{})
					testutil.Must(t, err)
					continue
				}
				putChunked(t, st, obj, data)
			}
			keys := []string{"two", "missing", "inline", "one", "three", "empty", "two"}
			for pass := range 2 {
				got, err := st.Cache(name).ReadMany(ctx, keys)
				testutil.Must(t, err)
				for i, key := range keys {
					if key == "missing" {
						if got[i] != nil {
							t.Fatalf("missing key returned %d bytes", len(got[i]))
						}
						continue
					}
					if got[i] == nil || !bytes.Equal(got[i], want[key]) {
						t.Fatalf("pass %d, %s (position %d): got %d bytes, want %d", pass, key, i, len(got[i]), len(want[key]))
					}
				}
			}
			if cacheBytes > 0 && st.blocks.hits.Load() == 0 {
				t.Fatal("second pass was not served from the read cache")
			}
		})
	}
}

func TestReadCacheServesRepeatedReads(t *testing.T) {
	ctx := testCtx
	st := openReadStore(t, func(o *OpenOptions) { o.ReadCacheBytes = 128 << 20 })
	bucket := unique("read-cache")
	testutil.Must(t, st.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "obj"}
	data := randomBytes(10<<20+12345, 3)
	putChunked(t, st, ref, data)

	read := func(rng ByteRange, want []byte) {
		t.Helper()
		got, err := st.Get(ctx, ref, rng)
		testutil.Must(t, err)
		if !bytes.Equal(got.Bytes, want) {
			t.Fatalf("range %+v: %d bytes, mismatch", rng, len(got.Bytes))
		}
	}
	read(WholeObjectRange(), data)
	if st.blocks.fills.Load() == 0 {
		t.Fatal("first read filled no blocks")
	}
	acquires := st.chunkPool.Stat().AcquireCount()
	read(WholeObjectRange(), data)
	read(Range(3<<20+7, 5<<20, -1), data[3<<20+7:5<<20+1])
	read(Range(0, 100, -1), data[:101])
	read(Range(-1, -1, 4000), data[len(data)-4000:])
	if got := st.chunkPool.Stat().AcquireCount(); got != acquires {
		t.Fatalf("cached reads acquired %d chunk connections", got-acquires)
	}
	if st.blocks.hits.Load() == 0 {
		t.Fatal("no cache hits recorded")
	}

	changed := randomBytes(len(data), 4)
	putChunked(t, st, ref, changed)
	read(WholeObjectRange(), changed)
	read(Range(1<<20, 2<<20, -1), changed[1<<20:2<<20+1])

	var metrics bytes.Buffer
	st.WriteMetrics(&metrics)
	for _, name := range []string{"pgvs3_read_cache_hits_total", "pgvs3_read_pinned_total", "pgvs3_reap_pinned_skips_total"} {
		if !bytes.Contains(metrics.Bytes(), []byte(name+" ")) {
			t.Fatalf("metrics missing %s", name)
		}
	}
}

// Pieces split across cached and uncached blocks must still come out in order.
func TestReadCachePartialHitKeepsOrder(t *testing.T) {
	ctx := testCtx
	st := openReadStore(t, func(o *OpenOptions) { o.ReadCacheBytes = 128 << 20 })
	bucket := unique("read-cache-partial")
	testutil.Must(t, st.CreateBucket(ctx, bucket))
	dropOnCleanup(t, ns.Bucket, bucket)
	ref := ObjectRef{NS: ns.BucketRef(bucket), Key: "obj"}
	data := randomBytes(32<<20, 5)
	putChunked(t, st, ref, data)
	for _, rng := range [][2]int64{{9 << 20, 12 << 20}, {20 << 20, 21 << 20}, {0, int64(len(data)) - 1}, {5 << 20, 25 << 20}} {
		got, err := st.Get(ctx, ref, Range(rng[0], rng[1], -1))
		testutil.Must(t, err)
		if !bytes.Equal(got.Bytes, data[rng[0]:rng[1]+1]) {
			t.Fatalf("range %v mismatch", rng)
		}
	}
	ctx2, cancel := context.WithCancel(ctx)
	_, body, err := st.GetBody(ctx2, ref, WholeObjectRange(), nil)
	testutil.Must(t, err)
	buf := make([]byte, 1<<20)
	_, err = io.ReadFull(body, buf)
	testutil.Must(t, err)
	cancel()
	testutil.Must(t, body.Close())
}
