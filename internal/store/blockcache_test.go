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

func TestChunkDescInvalidation(t *testing.T) {
	c := newBlockCache(blockShards * blockBytes)
	c.setChunkDesc("ns", "key", chunkDesc{size: 10})
	c.setChunkDesc("ns", "other", chunkDesc{size: 10})
	c.setChunkDesc("ns2", "key", chunkDesc{size: 10})
	if d, ok := c.lookupChunkDesc("ns", "key"); !ok || d.size != 10 {
		t.Fatalf("descriptor = %+v %t", d, ok)
	}
	c.invalidateChunkKey("ns", "key")
	if _, ok := c.lookupChunkDesc("ns", "key"); ok {
		t.Fatal("invalidateChunkKey left the descriptor")
	}
	c.invalidateChunkNS("ns")
	if _, ok := c.lookupChunkDesc("ns", "other"); ok {
		t.Fatal("invalidateChunkNS left another key")
	}
	if _, ok := c.lookupChunkDesc("ns2", "key"); !ok {
		t.Fatal("invalidateChunkNS removed another namespace")
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

func TestBlockFillDisabledWhenBlocksCannotFit(t *testing.T) {
	c := newBlockCache(blockShards)
	p := piece{fileID: 1, segLen: blockBytes, lo: 0, hi: segmentLastRow(blockBytes)}
	if f := c.fill(1, p); f != nil {
		t.Fatal("cache smaller than one block still enabled fills")
	}
}

func TestBlockFillReportsAssembledBlocks(t *testing.T) {
	segLen := int64(blockBytes)
	payload := make([]byte, segLen)
	for i := range payload {
		payload[i] = byte(i * 3)
	}
	c := newBlockCache(64 << 20)
	p := piece{fileID: 4, segLen: segLen, lo: 0, hi: segmentLastRow(segLen)}
	f := c.fill(11, p)
	for no := p.lo; no < p.hi; no++ {
		lo := int64(no) * RowBytes
		block, inBlock := f.add(no, payload[lo:lo+RowBytes])
		if !inBlock || block != nil {
			t.Fatalf("row %d: block=%v inBlock=%v, want an unfinished filled block", no, block != nil, inBlock)
		}
	}
	last := p.hi
	block, inBlock := f.add(last, payload[int64(last)*RowBytes:])
	if !inBlock || !bytes.Equal(block, payload) {
		t.Fatalf("last row returned block=%d bytes, inBlock=%v; want the assembled block", len(block), inBlock)
	}

	f = c.fill(11, p)
	for no := p.lo; no <= p.hi; no++ {
		if _, inBlock := f.add(no, payload[int64(no)*RowBytes:int64(no+1)*RowBytes]); inBlock {
			t.Fatalf("row %d refilled an already cached block", no)
		}
	}
}

func TestLookupPiecesCountsCachedBlocksOnce(t *testing.T) {
	const table = 12
	c := newBlockCache(blockShards * 8 * blockBytes)
	s := &Store{blocks: c}
	segLen := int64(2 * blockBytes)
	payload := make([]byte, segLen)
	for i := range payload {
		payload[i] = byte(i * 5)
	}
	p0 := piece{fileID: 1, segLen: segLen, lo: 0, hi: segmentLastRow(segLen)}
	p1 := piece{fileID: 2, segLen: segLen, lo: 0, hi: segmentLastRow(segLen)}
	fill := func(p piece, through int32) {
		f := c.fill(table, p)
		for no := p.lo; no <= through; no++ {
			lo := int64(no) * RowBytes
			f.add(no, payload[lo:lo+RowBytes])
		}
	}
	fill(p0, p0.hi)

	cached, need := s.lookupPieces(table, []piece{p0, p1}, 0, 2*segLen-1)
	if len(cached) != 2 || cached[0] == nil || cached[1] != nil {
		t.Fatalf("cached = %v, want piece 0 only", cached)
	}
	if len(need) != 1 || need[0] != 1 {
		t.Fatalf("need = %v, want [1]", need)
	}
	if got := bytes.Join(cached[0], nil); !bytes.Equal(got, payload) {
		t.Fatalf("cached piece bytes mismatch: %d", len(got))
	}
	if got := c.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 (one per cached block)", got)
	}
	if got := c.hitBytes.Load(); got != 2*blockBytes {
		t.Fatalf("hitBytes = %d, want %d", got, 2*blockBytes)
	}

	fill(p1, blockRows-1)
	cached, need = s.lookupPieces(table, []piece{p0, p1}, 0, 2*segLen-1)
	if cached[1] != nil {
		t.Fatal("partially cached piece reported as cached")
	}
	if len(need) != 1 || need[0] != 1 {
		t.Fatalf("need = %v, want [1]", need)
	}
	if got := c.hits.Load(); got != 4 {
		t.Fatalf("hits = %d, want 4 (single pass over each lookup)", got)
	}
	if got := c.hitBytes.Load(); got != 4*blockBytes {
		t.Fatalf("hitBytes = %d, want %d", got, 4*blockBytes)
	}
}

func TestCacheFileLargerThanShard(t *testing.T) {
	// Ten blocks per file exceed one shard's eight-block budget.
	c := newBlockCache(blockShards * 8 * blockBytes)
	s := &Store{blocks: c}
	size := int64(9*blockBytes + 321)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*7 + 1)
	}
	want := bytes.Clone(data)
	s.cacheFile(3, 77, data)
	for i := range data {
		data[i] = 0
	}
	if c.contains(blockKey{table: 3, file: 77, block: wholeFile}) {
		t.Fatal("large file stored as one whole-file entry")
	}
	if !c.contains(blockKey{table: 3, file: 77, block: 0}) ||
		!c.contains(blockKey{table: 3, file: 77, block: int32(size / (blockRows * RowBytes))}) {
		t.Fatal("large file was not split into block entries")
	}
	got, ok := s.cachedFile(3, 77, size)
	if !ok {
		t.Fatal("large split file missed")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("large split file returned %d wrong bytes", len(got))
	}

	small := bytes.Repeat([]byte("s"), 2*blockBytes)
	s.cacheFile(3, 78, small)
	if !c.contains(blockKey{table: 3, file: 78, block: wholeFile}) {
		t.Fatal("small file did not use a whole-file entry")
	}
	if got, ok := s.cachedFile(3, 78, int64(len(small))); !ok || !bytes.Equal(got, small) {
		t.Fatalf("small whole-file round trip failed: ok=%v len=%d", ok, len(got))
	}
	if _, ok := s.cachedFile(3, 79, 0); ok {
		t.Fatal("zero-size file reported cached")
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
