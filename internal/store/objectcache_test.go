package store

import (
	"testing"
	"time"
)

func TestPlanPiecesWholeRead(t *testing.T) {
	m := &Meta{FileID: 1, Size: 64 << 20}
	pieces, err := planPieces(m, 0, m.Size-1, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) != 1 {
		t.Fatalf("64 MiB span used %d pieces, want 1", len(pieces))
	}
	big := &Meta{FileID: 2, Size: 2 * 64 << 20}
	pieces, err = planPieces(big, 0, big.Size-1, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) < 2 {
		t.Fatalf("128 MiB span used %d pieces, want >1", len(pieces))
	}
}

func TestObjectCache(t *testing.T) {
	c := newObjectCache(1<<20, 50*time.Millisecond, false)
	defer c.close()
	if _, ok := c.get("b", "k"); ok {
		t.Fatal("empty cache hit")
	}
	c.put("b", "k", objectEntry{data: []byte("value"), etag: "e", fileID: 1})
	e, ok := c.get("b", "k")
	if !ok || string(e.data) != "value" || e.fileID != 1 {
		t.Fatalf("get = %+v %v", e, ok)
	}
	c.invalidate("b", "k")
	if _, ok := c.get("b", "k"); ok {
		t.Fatal("invalidate did not remove")
	}
	c.put("b", "k", objectEntry{data: []byte("x")})
	time.Sleep(70 * time.Millisecond)
	if _, ok := c.get("b", "k"); ok {
		t.Fatal("expired entry served")
	}
}
