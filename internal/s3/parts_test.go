package s3

import (
	"testing"

	"github.com/adonm/pgvs3/internal/store"
)

func TestPartBounds(t *testing.T) {
	single := &store.Meta{FileID: 1, Size: 100}
	if s, e, ok := partBounds(single, 1); !ok || s != 0 || e != 99 {
		t.Fatalf("single part = %d,%d,%v", s, e, ok)
	}
	if _, _, ok := partBounds(single, 2); ok {
		t.Fatal("single object has part 2")
	}
	multi := &store.Meta{FileID: 1, Size: 100, Parts: []int64{1, 2}, PartEnds: []int64{40, 100}}
	if s, e, ok := partBounds(multi, 2); !ok || s != 40 || e != 99 {
		t.Fatalf("part 2 = %d,%d,%v", s, e, ok)
	}
	if _, _, ok := partBounds(multi, 3); ok {
		t.Fatal("out of range part accepted")
	}
}
