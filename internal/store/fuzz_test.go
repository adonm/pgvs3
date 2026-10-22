package store

import (
	"bytes"
	"testing"
)

func FuzzRowFramer(f *testing.F) {
	f.Add([]byte("hello"), int64(1))
	f.Add(bytes.Repeat([]byte("x"), RowBytes+1), int64(42))
	f.Fuzz(func(t *testing.T, data []byte, id int64) {
		if len(data) > 4*RowBytes || id < 0 {
			return
		}
		fr := newRowFramer(id)
		if _, err := fr.push(data); err != nil {
			return
		}
		fr.finish()
	})
}

func FuzzBlockCodec(f *testing.F) {
	f.Add([]byte("hello world hello world hello world"))
	f.Add(bytes.Repeat([]byte("abcd1234"), 1024))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxInlineBytes {
			return
		}
		enc := compressBlock(nil, data)
		if len(enc) > len(data) {
			t.Fatalf("compressed block grew: %d > %d", len(enc), len(data))
		}
		dec, err := decompressBlock(nil, enc, len(data))
		if err != nil {
			t.Fatalf("decompress failed: %v", err)
		}
		if !bytes.Equal(dec, data) {
			t.Fatalf("round-trip mismatch (len=%d, enc=%d)", len(data), len(enc))
		}
	})
}
