package store

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"
)

func TestRowFramerLayout(t *testing.T) {
	if len(copyHeader) != 19 {
		t.Fatalf("COPY header is %d bytes, want 19", len(copyHeader))
	}
	f := newRowFramer(42)
	if _, err := f.push([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := f.finish()
	defer putFramerBuf(b)
	out := b.Bytes()
	if string(out[:11]) != "PGCOPY\n\xff\r\n\x00" {
		t.Fatalf("bad signature")
	}
	row := out[19 : len(out)-2]
	if got := binary.BigEndian.Uint16(row[0:2]); got != 3 {
		t.Fatalf("field count = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[2:6]); got != 8 {
		t.Fatalf("file_id length = %d", got)
	}
	if got := binary.BigEndian.Uint64(row[6:14]); got != 42 {
		t.Fatalf("file_id = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[14:18]); got != 4 {
		t.Fatalf("no length = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[18:22]); got != 0 {
		t.Fatalf("no = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[22:26]); got != 5 {
		t.Fatalf("data length = %d", got)
	}
	if string(row[26:]) != "hello" {
		t.Fatalf("data = %q", row[26:])
	}
}

func TestRowFramerSplitsAtRowBoundary(t *testing.T) {
	// Incompressible payload: stays exact raw size (zero header overhead).
	raw := make([]byte, RowBytes+RowBytes/2)
	_, _ = rand.Read(raw)
	f := newRowFramer(1)
	if _, err := f.push(raw); err != nil {
		t.Fatal(err)
	}
	b := f.finish()
	defer putFramerBuf(b)
	out := b.Bytes()
	if want := len(copyHeader) + RowBytes + RowBytes/2 + 2*26 + 2; len(out) != want {
		t.Fatalf("incompressible frame length %d, want %d", len(out), want)
	}

	// Compressible payload: 12,180 zero bytes compress into two tiny rows and
	// round-trip through decodeRow.
	zeros := make([]byte, RowBytes+RowBytes/2)
	fc := newRowFramer(2)
	if _, err := fc.push(zeros); err != nil {
		t.Fatal(err)
	}
	bc := fc.finish()
	defer putFramerBuf(bc)
	if len(bc.Bytes()) >= 200 {
		t.Fatalf("expected compressed frame < 200 bytes, got %d", len(bc.Bytes()))
	}
	r0Len := int(binary.BigEndian.Uint32(bc.Bytes()[19+22 : 19+26]))
	r0 := bc.Bytes()[19+26 : 19+26+r0Len]
	dec0, err := decodeRow(nil, int64(len(zeros)), 0, r0)
	if err != nil || !bytes.Equal(dec0, zeros[:RowBytes]) {
		t.Fatalf("row 0 decode failed: %v", err)
	}
}
