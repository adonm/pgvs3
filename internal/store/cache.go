package store

import (
	"context"

	"github.com/maypok86/otter"
)

// MetaCache is the metadata cache in front of s3p.objects. Reads are the hot
// path: a hit must cost a local memory access, not a PostgreSQL round trip.
type MetaCache interface {
	Get(ctx context.Context, bucket, key string) (Meta, bool, error)
	Put(ctx context.Context, bucket, key string, m Meta)
	Invalidate(ctx context.Context, bucket, key string)
	Close()
}

// LocalMetaCache is the single-process Otter (W-TinyLFU) cache. Otter reads
// are lock-free in the common case, which matters at thousands of GETs/s.
type LocalMetaCache struct {
	c otter.Cache[string, Meta]
}

func NewLocalMetaCache(bytes int64) *LocalMetaCache {
	if bytes < 1<<20 {
		bytes = 1 << 20
	}
	if bytes > 1<<40 {
		bytes = 1 << 40
	}
	b, err := otter.NewBuilder[string, Meta](int(bytes))
	if err != nil {
		panic(err)
	}
	c, err := b.Cost(func(key string, m Meta) uint32 {
		n := 64 + len(key) + m.SizeEstimate()
		if n > int(^uint32(0)) {
			return ^uint32(0)
		}
		return uint32(n)
	}).Build()
	if err != nil {
		panic(err)
	}
	return &LocalMetaCache{c: c}
}

func (c *LocalMetaCache) Get(_ context.Context, bucket, key string) (Meta, bool, error) {
	m, ok := c.c.Get(cacheKey(bucket, key))
	return m, ok, nil
}

func (c *LocalMetaCache) Put(_ context.Context, bucket, key string, m Meta) {
	c.c.Set(cacheKey(bucket, key), m)
}

func (c *LocalMetaCache) Invalidate(_ context.Context, bucket, key string) {
	c.c.Delete(cacheKey(bucket, key))
}

func (c *LocalMetaCache) Close() { c.c.Close() }

func cacheKey(bucket, key string) string { return bucket + "\x00" + key }

// SizeEstimate approximates the heap cost of one metadata entry.
func (m *Meta) SizeEstimate() int {
	n := 128 + len(m.ETag) + len(m.SHA256) + len(m.ContentType) + 16*len(m.Parts) + 8*len(m.PartEnds)
	for _, s := range m.UserMetadata {
		n += len(s)
	}
	return n
}

// SetCache installs the process-wide metadata cache. Call before serving.
func (s *Store) SetCache(c MetaCache) {
	if c == nil {
		return
	}
	if s.cache != nil {
		s.cache.Close()
	}
	s.cache = c
}

// CloseCache releases the metadata cache (and, in peer mode, the peer server).
func (s *Store) CloseCache() {
	if s.cache != nil {
		s.cache.Close()
	}
}
