//go:build integration

package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
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
	st, err := Open(testCtx, url, 8, slog.New(slog.NewTextHandler(io.Discard, nil)))
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

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestFIFOOrderingAndDedup(t *testing.T) {
	ctx := testCtx
	q := unique("fifo")
	if err := testStore.EnsureQueue(ctx, q, map[string]any{"FifoQueue": "true"}); err != nil {
		t.Fatal(err)
	}
	sent, err := testStore.SendMessages(ctx, q, []OutMessage{
		{Body: []byte("a1"), GroupID: "g", DedupID: "d1"},
		{Body: []byte("duplicate"), GroupID: "g", DedupID: "d1"},
		{Body: []byte("a2"), GroupID: "g", DedupID: "d2"},
		{Body: []byte("b1"), GroupID: "h", DedupID: "d3"},
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if sent[0].ID != sent[1].ID {
		t.Fatalf("dedup returned different IDs: %s vs %s", sent[0].ID, sent[1].ID)
	}
	if sent[0].SequenceNum == "" {
		t.Fatal("missing sequence number")
	}
	msgs, err := testStore.ReceiveMessages(ctx, q, 10, 30, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want one per group (2): %+v", len(msgs), msgs)
	}
	bodies := map[string]bool{}
	for _, m := range msgs {
		bodies[string(m.Body)] = true
		if m.GroupID == "" || m.SequenceNum == "" {
			t.Fatalf("missing FIFO metadata: %+v", m)
		}
	}
	if !bodies["a1"] || !bodies["b1"] || bodies["a2"] || bodies["duplicate"] {
		t.Fatalf("unexpected first batch: %+v", bodies)
	}
	var a1Receipt string
	for _, m := range msgs {
		if string(m.Body) == "a1" {
			a1Receipt = m.Receipt
		}
	}
	if _, err := testStore.DeleteMessage(ctx, q, a1Receipt); err != nil {
		t.Fatal(err)
	}
	next, err := testStore.ReceiveMessages(ctx, q, 10, 30, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(next) != 1 || string(next[0].Body) != "a2" {
		t.Fatalf("after deleting a1 got %+v, want a2", next)
	}
	if err := testStore.PurgeQueue(ctx, q); err != nil {
		t.Fatal(err)
	}
}

func TestFIFORedrivePreservesGroup(t *testing.T) {
	ctx := testCtx
	src := unique("src")
	dst := unique("dlq")
	for _, q := range []string{src, dst} {
		if err := testStore.EnsureQueue(ctx, q, map[string]any{"FifoQueue": "true"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := testStore.SendMessages(ctx, src, []OutMessage{
		{Body: []byte("doomed"), GroupID: "g", DedupID: "r1"},
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	msgs, err := testStore.ReceiveMessages(ctx, src, 1, 0, true, true)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("receive: %v %+v", err, msgs)
	}
	n, err := testStore.Redrive(ctx, src, dst, []string{msgs[0].Receipt}, time.Now().Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("redrive: n=%d err=%v", n, err)
	}
	dead, err := testStore.ReceiveMessages(ctx, dst, 1, 30, true, true)
	if err != nil || len(dead) != 1 {
		t.Fatalf("receive DLQ: %v %+v", err, dead)
	}
	if string(dead[0].Body) != "doomed" || dead[0].GroupID != "g" || dead[0].DedupID != "r1" {
		t.Fatalf("redrive lost FIFO metadata: %+v", dead[0])
	}
}

func TestOriginListingIndex(t *testing.T) {
	ctx := testCtx
	cache := unique("origin")
	if err := testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil); err != nil {
		t.Fatal(err)
	}
	prefix := "bkt-" + cache + "/"
	prefixEnd := prefix + "\U0010FFFF"
	now := time.Now().UTC().Truncate(time.Millisecond)
	entries := []OriginListEntry{
		{Key: prefix + "a", Size: 1, ETag: "ea", ContentType: "text/plain", LastModified: now},
		{Key: prefix + "b", Size: 2, ETag: "eb", ContentType: "text/plain", LastModified: now},
	}
	if err := testStore.ReplaceOriginListing(ctx, cache, prefix, prefixEnd, entries); err != nil {
		t.Fatal(err)
	}
	rows, fresh, err := testStore.ListOrigin(ctx, cache, prefix, prefixEnd, "", 10, time.Hour)
	if err != nil || !fresh || len(rows) != 2 {
		t.Fatalf("list: fresh=%v rows=%+v err=%v", fresh, rows, err)
	}
	if rows[0].Key != prefix+"a" || rows[1].Key != prefix+"b" {
		t.Fatalf("order: %+v", rows)
	}
	rows, fresh, err = testStore.ListOrigin(ctx, cache, prefix, prefixEnd, prefix+"a", 1, time.Hour)
	if err != nil || !fresh || len(rows) != 1 || rows[0].Key != prefix+"b" {
		t.Fatalf("pagination: fresh=%v rows=%+v err=%v", fresh, rows, err)
	}
	if err := testStore.UpsertOriginEntry(ctx, cache, OriginListEntry{
		Key: prefix + "c", Size: 3, ETag: "ec", ContentType: "text/plain", LastModified: now,
	}); err != nil {
		t.Fatal(err)
	}
	rows, _, err = testStore.ListOrigin(ctx, cache, prefix, prefixEnd, "", 10, time.Hour)
	if err != nil || len(rows) != 3 || rows[2].Key != prefix+"c" {
		t.Fatalf("upsert: rows=%+v err=%v", rows, err)
	}
	if err := testStore.DeleteOriginEntries(ctx, cache, []string{prefix + "a"}); err != nil {
		t.Fatal(err)
	}
	rows, _, err = testStore.ListOrigin(ctx, cache, prefix, prefixEnd, "", 10, time.Hour)
	if err != nil || len(rows) != 2 || rows[0].Key != prefix+"b" {
		t.Fatalf("delete: rows=%+v err=%v", rows, err)
	}
	if _, fresh, err := testStore.ListOrigin(ctx, cache, prefix, prefixEnd, "", 10, 0); err != nil || fresh {
		t.Fatalf("ttl=0 should force refresh: fresh=%v err=%v", fresh, err)
	}
}

func TestCompleteUploadPendingOutbox(t *testing.T) {
	ctx := testCtx
	cache := unique("origin")
	if err := testStore.EnsureNamespace(ctx, ns.Cache, cache, nil, nil); err != nil {
		t.Fatal(err)
	}
	uploadID, err := testStore.CreateUploadKind(ctx, ns.Cache, cache, "bkt/mp", nil, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []ListedPart
	for i, body := range [][]byte{[]byte("hello "), []byte("world")} {
		w, err := testStore.StartPartKind(ctx, ns.Cache, uploadID, cache, "bkt/mp", int32(i+1))
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
		parts = append(parts, ListedPart{PartNo: int32(i + 1), ETag: pub.ETag})
	}
	done, err := testStore.CompleteUploadKindPending(ctx, ns.Cache, uploadID, cache, "bkt/mp", parts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if done.Size != 11 {
		t.Fatalf("size = %d", done.Size)
	}
	m, err := testStore.MetaHeaderKind(ctx, ns.Cache, cache, "bkt/mp")
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
	// A stale flush must not publish over a newer pending token.
	if err := testStore.MarkOriginCommitted(ctx, cache, "bkt/mp", "pending:stale", "stale-v1"); err != nil {
		t.Fatal(err)
	}
	m, err = testStore.MetaHeaderKind(ctx, ns.Cache, cache, "bkt/mp")
	if err != nil || !IsPendingOrigin(m.OriginETag) {
		t.Fatalf("stale commit changed pending object: m=%+v err=%v", m, err)
	}
	if err := testStore.MarkOriginCommitted(ctx, cache, "bkt/mp", token, "origin-v1"); err != nil {
		t.Fatal(err)
	}
	m, err = testStore.MetaHeaderKind(ctx, ns.Cache, cache, "bkt/mp")
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
}

func TestDatabaseBudgetGuard(t *testing.T) {
	ctx := testCtx
	bucket := unique("budget")
	testStore.SetMaxDBBytes(1)
	_, err := testStore.PutInlineKind(ctx, ns.Bucket, bucket, "k", []byte("x"),
		Unconditional, "", nil, "text/plain", nil, false, "")
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("got %v, want ErrQuotaExceeded", err)
	}
	testStore.SetMaxDBBytes(0)
	if _, err := testStore.PutInlineKind(ctx, ns.Bucket, bucket, "k", []byte("x"),
		Unconditional, "", nil, "text/plain", nil, false, ""); err != nil {
		t.Fatalf("write after clearing budget: %v", err)
	}
}

func TestCacheBasics(t *testing.T) {
	ctx := testCtx
	cache := unique("cache")
	if _, _, _, err := testStore.CacheSet(ctx, cache, "k", []byte("value"), CacheSetOptions{}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := testStore.CacheGet(ctx, cache, "k")
	if err != nil || !ok || string(got) != "value" {
		t.Fatalf("get: %q %v %v", got, ok, err)
	}
	n, err := testStore.CacheIncrBy(ctx, cache, "counter", 2)
	if err != nil || n != 2 {
		t.Fatalf("incr: %d %v", n, err)
	}
	n, err = testStore.CacheIncrBy(ctx, cache, "counter", -1)
	if err != nil || n != 1 {
		t.Fatalf("decr: %d %v", n, err)
	}
}

func TestGooseMigrationsAndIdempotentInit(t *testing.T) {
	ctx := testCtx
	var gooseVersion int64
	if err := testStore.pool.QueryRow(ctx,
		"SELECT max(version_id) FROM s3p.goose_db_version").Scan(&gooseVersion); err != nil {
		t.Fatal(err)
	}
	if gooseVersion != 4 {
		t.Fatalf("goose version = %d, want 4", gooseVersion)
	}
	var layout int
	if err := testStore.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&layout); err != nil {
		t.Fatal(err)
	}
	if layout != LayoutVersion {
		t.Fatalf("layout = %d, want %d", layout, LayoutVersion)
	}
	if err := testStore.Init(ctx); err != nil {
		t.Fatalf("second Init: %v", err)
	}
	n, err := testStore.Migrate(ctx)
	if err != nil || n != 0 {
		t.Fatalf("Migrate on current layout: n=%d err=%v", n, err)
	}
}

func TestInitAdoptsExistingLayoutWithoutGooseHistory(t *testing.T) {
	ctx := testCtx
	if _, err := testStore.pool.Exec(ctx, "DROP TABLE IF EXISTS s3p.goose_db_version"); err != nil {
		t.Fatal(err)
	}
	if err := testStore.Init(ctx); err != nil {
		t.Fatalf("Init without goose history: %v", err)
	}
	var version int64
	if err := testStore.pool.QueryRow(ctx,
		"SELECT max(version_id) FROM s3p.goose_db_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 4 {
		t.Fatalf("goose version after adoption = %d, want 4", version)
	}
}
