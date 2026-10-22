package store

import (
	"context"
	"time"

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
	c   otter.Cache[string, localEntry]
	ttl time.Duration
}

type localEntry struct {
	m       Meta
	expires time.Time
}

func NewLocalMetaCache(bytes int64) *LocalMetaCache {
	if bytes < 1<<20 {
		bytes = 1 << 20
	}
	if bytes > 1<<40 {
		bytes = 1 << 40
	}
	b, err := otter.NewBuilder[string, localEntry](int(bytes))
	if err != nil {
		panic(err)
	}
	c, err := b.Cost(func(key string, e localEntry) uint32 {
		n := 64 + len(key) + e.m.SizeEstimate()
		if n > int(^uint32(0)) {
			return ^uint32(0)
		}
		return uint32(n)
	}).Build()
	if err != nil {
		panic(err)
	}
	return &LocalMetaCache{c: c, ttl: 5 * time.Second}
}

func (c *LocalMetaCache) Get(_ context.Context, bucket, key string) (Meta, bool, error) {
	e, ok := c.c.Get(cacheKey(bucket, key))
	if !ok {
		return Meta{}, false, nil
	}
	if time.Now().After(e.expires) {
		c.c.Delete(cacheKey(bucket, key))
		return Meta{}, false, nil
	}
	return e.m, true, nil
}

func (c *LocalMetaCache) Put(_ context.Context, bucket, key string, m Meta) {
	c.c.Set(cacheKey(bucket, key), localEntry{m: m, expires: time.Now().Add(c.ttl)})
}

func (c *LocalMetaCache) Invalidate(_ context.Context, bucket, key string) {
	c.c.Delete(cacheKey(bucket, key))
}

func (c *LocalMetaCache) Close() { c.c.Close() }

func cacheKey(bucket, key string) string { return bucket + "\x00" + key }

// SizeEstimate approximates the heap cost of one metadata entry.
func (m *Meta) SizeEstimate() int {
	n := 128 + len(m.ETag) + len(m.SHA256) + len(m.ContentType) + len(m.Inline) + 16*len(m.Parts) + 8*len(m.PartEnds)
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

// invalidatePeers removes the key from peer caches without dropping the local
// metadata entry. Used after a local publish when the local cache is current.
func (s *Store) invalidatePeers(ctx context.Context, bucket, key string) {
	if p, ok := s.cache.(interface {
		InvalidatePeers(context.Context, string, string)
	}); ok {
		p.InvalidatePeers(ctx, bucket, key)
	}
}
