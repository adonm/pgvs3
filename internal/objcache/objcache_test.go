package objcache

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestObjectContentLengthAndResolveRange(t *testing.T) {
	zero := Object{Size: 0}
	if got := zero.ContentLength(); got != 0 {
		t.Fatalf("0-byte ContentLength = %d, want 0", got)
	}
	ranged := Object{Size: 100, Start: 10, End: 29, Ranged: true}
	if got := ranged.ContentLength(); got != 20 {
		t.Fatalf("ranged ContentLength = %d, want 20", got)
	}

	start, end, isRanged := resolveRange(100, 0, -1, 15)
	if start != 85 || end != 99 || !isRanged {
		t.Fatalf("suffix range = (%d,%d,%v)", start, end, isRanged)
	}
	start, end, isRanged = resolveRange(100, 0, -1, -1)
	if start != 0 || end != 99 || isRanged {
		t.Fatalf("full range = (%d,%d,%v)", start, end, isRanged)
	}
}

func TestSpoolStreamMemoryAndTempFile(t *testing.T) {
	small := bytes.Repeat([]byte("a"), 1024)
	sp, clean, err := spoolStream(bytes.NewReader(small))
	if err != nil {
		t.Fatal(err)
	}
	defer clean()
	if sp.size != 1024 || len(sp.data) != 1024 {
		t.Fatalf("small spool size=%d data=%d", sp.size, len(sp.data))
	}

	large := bytes.Repeat([]byte("b"), memFillMax+4096)
	sp2, clean2, err := spoolStream(bytes.NewReader(large))
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

func TestListSortsBeforeLimit(t *testing.T) {
	b := newMemBucket()
	ctx := context.Background()
	for _, k := range []string{"edge/dir/c", "edge/a", "edge/dir/b"} {
		if err := b.Upload(ctx, k, strings.NewReader(k), int64(len(k))); err != nil {
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
