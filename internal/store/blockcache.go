package store

import (
	"container/list"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// blockRows is the gateway read-cache unit: 32 chunk rows (253.75 KiB).
const blockRows = 32

const blockShards = 16

// blockKey names immutable bytes. File IDs are never reused within one objects
// table, and the table OID changes when a namespace is dropped or restored, so
// a key can never map to different content.
type blockKey struct {
	table uint32
	file  int64
	block int32
}

// blockCache is a per-gateway, byte-bounded cache of decoded chunk blocks. It
// holds no authority: metadata still comes from PostgreSQL on every request,
// and only bytes of files that metadata currently references are served.
// Each shard runs S3-FIFO so a single large scan cannot flush hot blocks.
type blockCache struct {
	shards [blockShards]blockShard

	hits, hitBytes, fills, evictions atomic.Int64
}

type blockShard struct {
	mu      sync.Mutex
	max     int64
	small   fifo
	main    fifo
	items   map[blockKey]*list.Element
	ghost   map[blockKey]struct{}
	ghostQ  []blockKey
	ghostAt int
}

type fifo struct {
	l     list.List
	bytes int64
}

type blockEntry struct {
	key  blockKey
	data []byte
	freq uint8
	main bool
}

func newBlockCache(maxBytes int64) *blockCache {
	if maxBytes <= 0 {
		return nil
	}
	c := &blockCache{}
	per := max(maxBytes/blockShards, 1)
	ghost := max(int(per/(blockRows*RowBytes)), 8)
	for i := range c.shards {
		c.shards[i] = blockShard{
			max:    per,
			items:  make(map[blockKey]*list.Element),
			ghost:  make(map[blockKey]struct{}, ghost),
			ghostQ: make([]blockKey, ghost),
		}
	}
	return c
}

func (c *blockCache) shard(k blockKey) *blockShard {
	h := uint64(k.table)*0x9e3779b97f4a7c15 ^ uint64(k.file)*0xbf58476d1ce4e5b9 ^ uint64(k.block)*0x94d049bb133111eb
	return &c.shards[(h>>59)%blockShards]
}

func (c *blockCache) get(k blockKey) ([]byte, bool) {
	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	el, ok := sh.items[k]
	if !ok {
		return nil, false
	}
	e := el.Value.(*blockEntry)
	e.freq = min(e.freq+1, 3)
	return e.data, true
}

func (c *blockCache) contains(k blockKey) bool {
	sh := c.shard(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	_, ok := sh.items[k]
	return ok
}

// put stores an immutable block; callers must not modify data afterwards.
func (c *blockCache) put(k blockKey, data []byte) {
	sh := c.shard(k)
	size := int64(len(data))
	if size == 0 || size > sh.max {
		return
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if _, ok := sh.items[k]; ok {
		return
	}
	e := &blockEntry{key: k, data: data}
	if _, ok := sh.ghost[k]; ok {
		delete(sh.ghost, k)
		e.main = true
		sh.items[k] = sh.main.l.PushFront(e)
		sh.main.bytes += size
	} else {
		sh.items[k] = sh.small.l.PushFront(e)
		sh.small.bytes += size
	}
	c.fills.Add(1)
	for sh.small.bytes+sh.main.bytes > sh.max {
		c.evictOne(sh)
	}
}

func (c *blockCache) evictOne(sh *blockShard) {
	if sh.small.bytes > sh.max/10 || sh.main.l.Len() == 0 {
		el := sh.small.l.Back()
		e := el.Value.(*blockEntry)
		sh.small.l.Remove(el)
		sh.small.bytes -= int64(len(e.data))
		if e.freq > 0 {
			e.freq, e.main = 0, true
			sh.items[e.key] = sh.main.l.PushFront(e)
			sh.main.bytes += int64(len(e.data))
			return
		}
		delete(sh.items, e.key)
		sh.remember(e.key)
		c.evictions.Add(1)
		return
	}
	el := sh.main.l.Back()
	e := el.Value.(*blockEntry)
	sh.main.l.Remove(el)
	if e.freq > 0 {
		e.freq--
		sh.items[e.key] = sh.main.l.PushFront(e)
		return
	}
	sh.main.bytes -= int64(len(e.data))
	delete(sh.items, e.key)
	c.evictions.Add(1)
}

func (sh *blockShard) remember(k blockKey) {
	old := sh.ghostQ[sh.ghostAt]
	delete(sh.ghost, old)
	sh.ghostQ[sh.ghostAt] = k
	sh.ghostAt = (sh.ghostAt + 1) % len(sh.ghostQ)
	sh.ghost[k] = struct{}{}
}

func (c *blockCache) resident() (bytes int64, blocks int) {
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		bytes += sh.small.bytes + sh.main.bytes
		blocks += len(sh.items)
		sh.mu.Unlock()
	}
	return bytes, blocks
}

func (c *blockCache) writeMetrics(w io.Writer) {
	var bytes, hits, hitBytes, fills, evictions int64
	var blocks int
	if c != nil {
		bytes, blocks = c.resident()
		hits, hitBytes, fills, evictions = c.hits.Load(), c.hitBytes.Load(), c.fills.Load(), c.evictions.Load()
	}
	fmt.Fprintf(w, "pgvs3_read_cache_bytes %d\n", bytes)
	fmt.Fprintf(w, "pgvs3_read_cache_blocks %d\n", blocks)
	fmt.Fprintf(w, "pgvs3_read_cache_hits_total %d\n", hits)
	fmt.Fprintf(w, "pgvs3_read_cache_hit_bytes_total %d\n", hitBytes)
	fmt.Fprintf(w, "pgvs3_read_cache_fills_total %d\n", fills)
	fmt.Fprintf(w, "pgvs3_read_cache_evictions_total %d\n", evictions)
}

func segmentLastRow(segLen int64) int32 {
	return int32((segLen - 1) / RowBytes)
}

func blockLastRow(block int32, segLen int64) int32 {
	return min(block*blockRows+blockRows-1, segmentLastRow(segLen))
}

// covered reports whether every block of p is cached, without touching
// recency or hit counters.
func (c *blockCache) covered(table uint32, p piece) bool {
	if c == nil || table == 0 {
		return false
	}
	for b := p.lo / blockRows; b <= p.hi/blockRows; b++ {
		if !c.contains(blockKey{table: table, file: p.fileID, block: b}) {
			return false
		}
	}
	return true
}

// pieceParts returns the cached bytes of p trimmed to [start,end], or false if
// any block of the piece is missing.
func (c *blockCache) pieceParts(table uint32, p piece, start, end int64) ([][]byte, bool) {
	if c == nil || table == 0 {
		return nil, false
	}
	first, last := p.lo/blockRows, p.hi/blockRows
	blocks := make([][]byte, 0, last-first+1)
	for b := first; b <= last; b++ {
		data, ok := c.get(blockKey{table: table, file: p.fileID, block: b})
		if !ok {
			return nil, false
		}
		blocks = append(blocks, data)
	}
	lo := max(start-p.base, int64(p.lo)*RowBytes)
	hi := min(end-p.base, int64(p.hi)*RowBytes+RowBytes-1, p.segLen-1)
	parts := make([][]byte, 0, len(blocks))
	var served int64
	for i, data := range blocks {
		off := int64(first+int32(i)) * blockRows * RowBytes
		a, z := max(lo-off, 0), min(hi-off+1, int64(len(data)))
		if a < z {
			parts = append(parts, data[a:z:z])
			served += z - a
		}
	}
	c.hits.Add(int64(len(blocks)))
	c.hitBytes.Add(served)
	return parts, true
}

// blockFill assembles complete blocks from rows streamed in order and stores
// those not already cached. Rows outside complete blocks are ignored.
type blockFill struct {
	cache  *blockCache
	table  uint32
	file   int64
	segLen int64
	block  int32
	buf    []byte
}

func (c *blockCache) fill(table uint32, p piece) *blockFill {
	if c == nil || table == 0 {
		return nil
	}
	return &blockFill{cache: c, table: table, file: p.fileID, segLen: p.segLen, block: -1}
}

func (f *blockFill) add(no int32, data []byte) {
	if f == nil {
		return
	}
	b := no / blockRows
	if no%blockRows == 0 {
		f.buf, f.block = nil, -1
		k := blockKey{table: f.table, file: f.file, block: b}
		if !f.cache.contains(k) {
			rows := int64(blockLastRow(b, f.segLen)-no) + 1
			f.buf = make([]byte, 0, int(min(rows*RowBytes, f.segLen-int64(no)*RowBytes)))
			f.block = b
		}
	}
	if f.block != b || f.buf == nil {
		return
	}
	f.buf = append(f.buf, data...)
	if no == blockLastRow(b, f.segLen) {
		f.cache.put(blockKey{table: f.table, file: f.file, block: b}, f.buf)
		f.buf, f.block = nil, -1
	}
}

// alignPieces widens each segment's first and last piece to whole blocks and
// snaps internal boundaries to block edges so every block read is cacheable.
func alignPieces(pieces []piece) []piece {
	out := make([]piece, 0, len(pieces))
	for i := 0; i < len(pieces); {
		j := i
		for j+1 < len(pieces) && pieces[j+1].fileID == pieces[i].fileID && pieces[j+1].base == pieces[i].base {
			j++
		}
		seg := pieces[i : j+1]
		lastRow := segmentLastRow(seg[0].segLen)
		lo := seg[0].lo / blockRows * blockRows
		for k, p := range seg {
			hi := p.hi
			if k == len(seg)-1 {
				hi = min((hi/blockRows+1)*blockRows-1, lastRow)
			} else {
				hi = (seg[k+1].lo+blockRows/2)/blockRows*blockRows - 1
			}
			if hi >= lo {
				p.lo, p.hi = lo, hi
				out = append(out, p)
				lo = hi + 1
			}
		}
		i = j + 1
	}
	return out
}
