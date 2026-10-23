//go:build integration

package objcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	cfg := []byte(fmt.Sprintf("type: filesystem\nconfig:\n  directory: %s\n", dir))
	tier, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg, testStore,
		fmt.Sprintf("origin-%d", time.Now().UnixNano()), 0, 24*time.Hour, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tier.Close() })
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
	obj, err := f.tier.Open(ctx, "bkt", "a.txt", 0, -1, -1)
	if err != nil {
		t.Fatal(err)
	}
	if got := readObject(t, obj); got != "alpha" {
		t.Fatalf("open = %q", got)
	}
	obj, err = f.tier.Open(ctx, "bkt", "a.txt", 1, 3, -1)
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
	obj, err := f.tier.Open(ctx, "bkt", "a.txt", 0, -1, -1)
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

func TestTierMultipartOriginFirst(t *testing.T) {
	ctx := testCtx
	f := newFixture(t)
	uploadID, err := f.tier.CreateUpload(ctx, "bkt", "mp", nil, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	var parts []store.ListedPart
	for i, body := range [][]byte{[]byte("hello "), []byte("world")} {
		w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp", int32(i+1))
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
	done, err := f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp", parts)
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

	// A failed origin upload must leave the cache unpublished.
	uploadID, err = f.tier.CreateUpload(ctx, "bkt", "mp-fail", nil, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := f.tier.StartPart(ctx, uploadID, "bkt", "mp-fail", 1)
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
	_, err = f.tier.CompleteUpload(ctx, uploadID, "bkt", "mp-fail",
		[]store.ListedPart{{PartNo: 1, ETag: pub.ETag}})
	_ = os.Chmod(filepath.Join(f.dir, "bkt"), 0o755)
	if err == nil {
		t.Fatal("expected origin failure")
	}
	if _, err := f.tier.Head(ctx, "bkt", "mp-fail"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed completion published an object: %v", err)
	}
}
