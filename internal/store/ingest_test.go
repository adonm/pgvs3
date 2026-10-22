package store

import (
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
	out := f.finish()
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
	f := newRowFramer(1)
	// One and a half rows: the first row is framed, the rest waits for finish.
	if _, err := f.push(make([]byte, RowBytes+RowBytes/2)); err != nil {
		t.Fatal(err)
	}
	out := f.finish()
	// 1.5 rows become two rows (8120 + 4060 bytes), each with 26 bytes of
	// framing, plus the COPY header and trailer.
	if want := len(copyHeader) + RowBytes + RowBytes/2 + 2*26 + 2; len(out) != want {
		t.Fatalf("frame length %d, want %d", len(out), want)
	}
}
