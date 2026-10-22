package peercache

import (
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

func TestMetaWireRoundTrip(t *testing.T) {
	want := store.Meta{
		Size: 10, FileID: 9, ETag: "deadbeef", ContentType: "application/parquet",
		CreatedAt: time.Unix(1700000000, 123), SHA256: []byte{1, 2, 3},
		UserMetadata: []string{"a", "b", "c", "d"},
		Parts:        []int64{9, 10}, PartEnds: []int64{4, 10},
	}
	b, err := encodeMeta(&want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeMeta(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Size != want.Size || got.FileID != want.FileID || got.ETag != want.ETag ||
		got.ContentType != want.ContentType || !got.CreatedAt.Equal(want.CreatedAt) ||
		len(got.Parts) != 2 || got.PartEnds[1] != 10 || len(got.UserMetadata) != 4 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, err := decodeMeta(b[:len(b)-1]); err == nil {
		t.Fatal("truncated metadata accepted")
	}
}
