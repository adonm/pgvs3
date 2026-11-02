package store

import (
	"testing"
)

func pieceRows(p piece) (first, last int32) { return p.lo, p.hi }

func checkCoverage(t *testing.T, m *Meta, start, end int64, pieces []piece) {
	t.Helper()
	var base int64
	var next int32
	for i, p := range pieces {
		lo, hi := pieceRows(p)
		if hi < lo {
			t.Fatalf("piece %d has lo %d > hi %d", i, lo, hi)
		}
		if p.base != base {
			t.Fatalf("piece %d base %d, want %d", i, p.base, base)
		}
		if i > 0 && lo != next {
			t.Fatalf("piece %d starts at %d, want contiguous %d", i, lo, next)
		}
		next = hi + 1
	}
	segments, err := m.Segments()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(pieces), len(segments); got < want {
		t.Fatalf("no pieces for segments %v", segments)
	}
	last := pieces[len(pieces)-1]
	segment := segments[len(segments)-1]
	firstByte := last.base + int64(last.lo)*RowBytes
	lastByte := last.base + int64(last.hi)*RowBytes + RowBytes - 1
	if firstByte > end || lastByte < min(end, segment.Base+segment.Length-1) {
		t.Fatalf("pieces cover [%d,%d], want [%d,%d]", firstByte, lastByte, start, end)
	}
}

func singleFile(size int64) *Meta { return &Meta{Size: size, FileID: 7} }

func TestPlanPiecesAdaptsToSpanAndReaders(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		name      string
		size      int64
		start     int64
		end       int64
		readers   int
		want      int
		wantFirst int32
		wantLast  int32
	}{
		{"one query for 512 KiB", 512 << 10, 0, 512<<10 - 1, 8, 1, 0, 64},
		{"10 MiB over eight readers", 10 * mib, 0, 10*mib - 1, 8, 8, 0, 1291},
		{"10 MiB over three readers", 10 * mib, 0, 10*mib - 1, 3, 3, 0, 1291},
		{"10 MiB with one reader stays one query", 10 * mib, 0, 10*mib - 1, 1, 1, 0, 1291},
		{"64 MiB over eight readers", 64 * mib, 0, 64*mib - 1, 8, 8, 0, 8264},
		{"64 MiB over one reader stays one query", 64 * mib, 0, 64*mib - 1, 1, 1, 0, 8264},
		{"1 GiB uses 8 MiB pieces", 1 << 30, 0, 1<<30 - 1, 8, 128, 0, 132234},
		{"8 MiB middle range over eight readers", 1 << 30, 100 * mib, 108*mib - 1, 8, 8, 12913, 13946},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pieces, err := planPieces(singleFile(tc.size), tc.start, tc.end, tc.readers)
			if err != nil {
				t.Fatal(err)
			}
			if len(pieces) != tc.want {
				t.Fatalf("got %d pieces, want %d: %+v", len(pieces), tc.want, pieces)
			}
			if pieces[0].lo != tc.wantFirst || pieces[len(pieces)-1].hi != tc.wantLast {
				t.Fatalf("coverage %d..%d, want %d..%d", pieces[0].lo, pieces[len(pieces)-1].hi, tc.wantFirst, tc.wantLast)
			}
			checkCoverage(t, singleFile(tc.size), tc.start, tc.end, pieces)
		})
	}
}

func TestPlanPiecesMultipartKeepsOrder(t *testing.T) {
	m := &Meta{Size: 24 << 20, FileID: 1, Parts: []int64{1, 2, 3}, PartEnds: []int64{8 << 20, 16 << 20, 24 << 20}}
	pieces, err := planPieces(m, 0, m.Size-1, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) != 6 {
		t.Fatalf("got %d pieces, want 2 per part", len(pieces))
	}
	// Three equal parts; pieces must follow part order and stay within them.
	for i := 1; i < len(pieces); i++ {
		if pieces[i].base < pieces[i-1].base {
			t.Fatalf("pieces out of order: %+v", pieces)
		}
	}
	for i := 0; i < len(pieces); i += 2 {
		part := i / 2
		wantBase := int64(part) * (8 << 20)
		if pieces[i].fileID != m.Parts[part] || pieces[i].base != wantBase || pieces[i+1].base != wantBase ||
			pieces[i].lo != 0 || pieces[i+1].hi != 1033 {
			t.Fatalf("part %d pieces %+v", part, pieces[i:i+2])
		}
	}
}

func TestPlanPiecesRejectsBrokenManifest(t *testing.T) {
	m := &Meta{Size: 2 << 20, FileID: 1, Parts: []int64{1}, PartEnds: []int64{1 << 20}}
	if _, err := planPieces(m, 0, m.Size-1, 8); err == nil {
		t.Fatal("short multipart manifest accepted")
	}
}

func TestReadWorkersLeavesRoomForSnapshotsAndConcurrentReads(t *testing.T) {
	for _, tc := range []struct{ max, inflight, want int }{
		{64, 1, 8}, {64, 8, 3}, {64, 16, 1}, {64, 32, 1},
		{32, 1, 8}, {32, 8, 1}, {8, 1, 3}, {8, 4, 1}, {2, 1, 1},
	} {
		if got := readWorkersFor(tc.max, tc.inflight); got != tc.want {
			t.Fatalf("pool %d inflight %d: got %d want %d", tc.max, tc.inflight, got, tc.want)
		}
	}
}

func TestPlanPiecesRejectsRangeOutsideObject(t *testing.T) {
	if _, err := planPieces(singleFile(1<<20), 2<<20, 3<<20, 8); err == nil {
		t.Fatal("range outside object accepted")
	}
}
