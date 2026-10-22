package store

import (
	"sync/atomic"
	"time"

	"github.com/maypok86/otter"
)

// smallObjectMax is the largest whole object kept in the byte cache. Metadata
// and footers are the repeated small reads in a lake; large data files are
// already cached by DuckDB.
const smallObjectMax = 256 << 10

type objectEntry struct {
	data    []byte
	etag    string
	fileID  int64
	expires time.Time
}

// objectCache is a per-gateway byte cache. Every hit is revalidated against
// s3p.objects before the bytes are served, so a write through another gateway
// cannot be masked by a stale entry.
type objectCache struct {
	c          otter.Cache[string, objectEntry]
	ttl        time.Duration
	maxObject  int64
	revalidate bool
	hits       atomic.Int64
	misses     atomic.Int64
}

func newObjectCache(bytes int64, ttl time.Duration, revalidate bool) *objectCache {
	if bytes <= 0 {
		return nil
	}
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	if bytes > 1<<40 {
		bytes = 1 << 40
	}
	builder, err := otter.NewBuilder[string, objectEntry](int(bytes))
	if err != nil {
		return nil
	}
	c, err := builder.Cost(func(key string, e objectEntry) uint32 {
		n := 64 + len(key) + len(e.data)
		if n > int(^uint32(0)) {
			return ^uint32(0)
		}
		return uint32(n)
	}).Build()
	if err != nil {
		return nil
	}
	return &objectCache{c: c, ttl: ttl, maxObject: smallObjectMax, revalidate: revalidate}
}

func (c *objectCache) get(bucket, key string) (objectEntry, bool) {
	if c == nil {
		return objectEntry{}, false
	}
	e, ok := c.c.Get(cacheKey(bucket, key))
	if !ok {
		return objectEntry{}, false
	}
	if time.Now().After(e.expires) {
		c.c.Delete(cacheKey(bucket, key))
		return objectEntry{}, false
	}
	c.hits.Add(1)
	return e, true
}

func (c *objectCache) put(bucket, key string, e objectEntry) {
	if c == nil || int64(len(e.data)) > c.maxObject {
		return
	}
	e.expires = time.Now().Add(c.ttl)
	c.c.Set(cacheKey(bucket, key), e)
}

func (c *objectCache) invalidate(bucket, key string) {
	if c == nil {
		return
	}
	c.c.Delete(cacheKey(bucket, key))
}

func (c *objectCache) close() {
	if c != nil {
		c.c.Close()
	}
}
