package store

import (
	"bytes"
	"testing"
)

const blockBytes = blockRows * RowBytes

func block(fill byte) []byte { return bytes.Repeat([]byte{fill}, blockBytes) }

func TestBlockCacheStaysWithinBudget(t *testing.T) {
	c := newBlockCache(blockShards * 4 * blockBytes)
	for i := range int32(1000) {
		c.put(blockKey{table: 1, file: 1, block: i}, block(byte(i)))
	}
	used, blocks := c.resident()
	if used > blockShards*4*blockBytes || blocks > blockShards*4 {
		t.Fatalf("resident %d bytes in %d blocks exceeds budget", used, blocks)
	}
	if c.evictions.Load() == 0 {
		t.Fatal("no evictions recorded")
	}
}

func TestBlockCacheResistsOneScan(t *testing.T) {
	c := newBlockCache(blockShards * 10 * blockBytes)
	hot := make([]blockKey, 0, 40)
	for i := range int32(40) {
		k := blockKey{table: 1, file: 1, block: i}
		hot = append(hot, k)
		c.put(k, block(1))
		c.get(k)
	}
	for i := range int32(2000) {
		c.put(blockKey{table: 1, file: 2, block: i}, block(2))
	}
	kept := 0
	for _, k := range hot {
		if c.contains(k) {
			kept++
		}
	}
	if kept < len(hot)*3/4 {
		t.Fatalf("a single scan evicted %d of %d hot blocks", len(hot)-kept, len(hot))
	}
}

func TestBlockCacheDisabled(t *testing.T) {
	var c *blockCache
	if newBlockCache(0) != nil {
		t.Fatal("zero budget built a cache")
	}
	if _, ok := c.pieceParts(1, piece{fileID: 1, segLen: 10, hi: 0}, 0, 9); ok {
		t.Fatal("nil cache reported a hit")
	}
	if c.fill(1, piece{}) != nil {
		t.Fatal("nil cache built a fill")
	}
	c.fill(1, piece{}).add(0, []byte("x"))
	var b bytes.Buffer
	c.writeMetrics(&b)
	if !bytes.Contains(b.Bytes(), []byte("pgvs3_read_cache_bytes 0")) {
		t.Fatalf("metrics: %s", b.String())
	}
}

func TestBlockFillAndPiecePartsRoundTrip(t *testing.T) {
	segLen := int64(3*blockBytes + 5000)
	payload := make([]byte, segLen)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	c := newBlockCache(64 << 20)
	p := piece{fileID: 9, base: 1000, segLen: segLen, lo: 0, hi: segmentLastRow(segLen)}
	f := c.fill(5, p)
	for no := p.lo; no <= p.hi; no++ {
		lo := int64(no) * RowBytes
		f.add(no, payload[lo:min(lo+RowBytes, segLen)])
	}
	if _, blocks := c.resident(); blocks != 4 {
		t.Fatalf("filled %d blocks, want 4", blocks)
	}
	for _, span := range [][2]int64{{0, segLen - 1}, {1, 2}, {blockBytes - 3, blockBytes + 3}, {segLen - 10, segLen - 1}} {
		parts, ok := c.pieceParts(5, p, p.base+span[0], p.base+span[1])
		if !ok {
			t.Fatalf("span %v missed", span)
		}
		if got := bytes.Join(parts, nil); !bytes.Equal(got, payload[span[0]:span[1]+1]) {
			t.Fatalf("span %v returned %d wrong bytes", span, len(got))
		}
	}
	if _, ok := c.pieceParts(6, p, p.base, p.base+10); ok {
		t.Fatal("other table hit")
	}
}

func TestBlockFillSkipsIncompleteBlocks(t *testing.T) {
	c := newBlockCache(64 << 20)
	segLen := int64(4 * blockBytes)
	p := piece{fileID: 1, segLen: segLen, lo: 5, hi: blockRows*2 + 3}
	f := c.fill(1, p)
	for no := p.lo; no <= p.hi; no++ {
		f.add(no, make([]byte, RowBytes))
	}
	if !c.contains(blockKey{table: 1, file: 1, block: 1}) {
		t.Fatal("complete block 1 not cached")
	}
	if c.contains(blockKey{table: 1, file: 1, block: 0}) || c.contains(blockKey{table: 1, file: 1, block: 2}) {
		t.Fatal("partial edge block cached")
	}
}

func TestAlignPiecesSnapsToBlocks(t *testing.T) {
	m := &Meta{Size: 24 << 20, FileID: 1, Parts: []int64{1, 2, 3}, PartEnds: []int64{8 << 20, 16 << 20, 24 << 20}}
	pieces, err := planPieces(m, 3<<20+17, 20<<20, 8)
	if err != nil {
		t.Fatal(err)
	}
	aligned := alignPieces(pieces)
	for i, p := range aligned {
		if p.lo%blockRows != 0 {
			t.Fatalf("piece %d starts mid-block: %+v", i, p)
		}
		if p.hi != segmentLastRow(p.segLen) && (p.hi+1)%blockRows != 0 {
			t.Fatalf("piece %d ends mid-block: %+v", i, p)
		}
		if i > 0 && aligned[i-1].fileID == p.fileID && aligned[i-1].hi+1 != p.lo {
			t.Fatalf("pieces %d and %d are not contiguous", i-1, i)
		}
	}
	for i, p := range pieces {
		var covered bool
		for _, a := range aligned {
			covered = covered || (a.fileID == p.fileID && a.lo <= p.lo && a.hi >= p.hi) ||
				(a.fileID == p.fileID && a.lo <= p.hi && a.hi >= p.lo)
		}
		if !covered {
			t.Fatalf("planned piece %d %+v lost by alignment", i, p)
		}
	}
	if first, last := aligned[0], aligned[len(aligned)-1]; first.lo > pieces[0].lo || last.hi < pieces[len(pieces)-1].hi {
		t.Fatalf("alignment shrank the span: %+v .. %+v", first, last)
	}
}

func TestPinKeysDeduplicate(t *testing.T) {
	keys := pinKeys([]piece{{fileID: 7}, {fileID: 7}, {fileID: 1 << 40}, {fileID: 3}})
	if len(keys) != 3 {
		t.Fatalf("keys %v", keys)
	}
	for _, k := range keys {
		if k < 0 {
			t.Fatalf("negative key %d", k)
		}
	}
}
