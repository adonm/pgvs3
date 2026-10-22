package store

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestCompressibleBlocksShrink(t *testing.T) {
	src := bytes.Repeat([]byte("abcdefgh"), 1015)
	enc := compressBlock(nil, src)
	if len(enc) >= len(src)/4 {
		t.Fatalf("expected >4x compression, got %d -> %d", len(src), len(enc))
	}
	dec, err := decompressBlock(nil, enc, len(src))
	if err != nil || !bytes.Equal(dec, src) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestIncompressibleBlocksStayRaw(t *testing.T) {
	src := make([]byte, 8120)
	if _, err := rand.Read(src); err != nil {
		t.Fatal(err)
	}
	enc := compressBlock(nil, src)
	if len(enc) != len(src) {
		t.Fatalf("random block should stay raw, got %d -> %d", len(src), len(enc))
	}
}
