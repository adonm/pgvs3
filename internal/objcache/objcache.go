// Package objcache turns pgvs3 into a read-through object cache in front of
// an S3-compatible origin (filesystem/in-memory are local/test providers).
package objcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"mime"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// Tier is a caching layer over an origin bucket.
type Tier struct {
	origin    originBucket
	store     *store.Store
	namespace string
	ttl       time.Duration
	retention time.Duration
	fills     chan struct{}
	completes chan struct{}
	group     singleflight.Group
	log       *slog.Logger
	keyLocks  [64]sync.Mutex
	rangeSize int64

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

// memFillMax is the largest origin body buffered in memory before writing to
// PostgreSQL. Larger bodies spool to a temp file so a database connection is
// never held while waiting on the origin network.
const memFillMax = 8 << 20

// maxListingEntries bounds one origin listing refresh. Beyond this the direct
// walk is used and the index is not built, keeping memory bounded.
const maxListingEntries = 1_000_000

// fullCacheMax is the largest object downloaded whole to satisfy a range
// request. Above it, ranges are passed through to the origin so a footer read
// never pulls a multi-GB file.
const fullCacheMax = 8 << 20

// maxRangeCacheSpan bounds one range-cache request. Larger spans pass through
// rather than occupying memory with many aligned chunks.
const maxRangeCacheSpan = 8 << 20

// errPendingOrigin means a multipart completion is still waiting for its
// origin upload. It must not be overwritten by a read-through fill.
var errPendingOrigin = errors.New("cache object is pending origin upload")

// New builds a Tier. The s3 provider is the production path; filesystem and
// inmem are available for local development and tests.
func New(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration, maxFills int) (*Tier, error) {
	c, err := parseConfig(config)
	if err != nil {
		return nil, fmt.Errorf("parse origin config: %w", err)
	}
	b, err := newOrigin(c)
	if err != nil {
		return nil, err
	}
	if namespace == "" {
		namespace = "origin"
	}
	if !ns.ValidName(namespace) {
		_ = b.Close()
		return nil, fmt.Errorf("invalid cache namespace %q", namespace)
	}
	if ttl < 0 {
		ttl = time.Minute
	}
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	if maxFills <= 0 {
		maxFills = 8
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Tier{
		origin:    b,
		store:     st,
		namespace: namespace,
		ttl:       ttl,
		retention: retention,
		fills:     make(chan struct{}, maxFills),
		completes: make(chan struct{}, maxFills),
		log:       log,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go t.runPending(ctx)
	return t, nil
}

func (t *Tier) Name() string { return t.namespace }

// EnableRangeCache caches aligned ranges of large objects instead of passing
// them through to the origin. chunk is the alignment; 0 disables range caching.
func (t *Tier) EnableRangeCache(chunk int64) {
	if chunk < 0 {
		chunk = 0
	}
	if chunk > 0 && chunk < 64<<10 {
		chunk = 64 << 10
	}
	if chunk > 8<<20 {
		chunk = 8 << 20
	}
	t.rangeSize = chunk
}

func (t *Tier) Close() error {
	t.closeOnce.Do(func() {
		t.cancel()
		<-t.done
	})
	return t.origin.Close()
}
func (t *Tier) key(bucket, key string) string { return bucket + "/" + key }

// lockKey serializes writes and pending flushes for a key. A fixed stripe set
// avoids an unbounded per-key lock map; collisions only reduce concurrency.
func (t *Tier) lockKey(ck string) func() {
	h := fnv.New32a()
	_, _ = h.Write([]byte(ck))
	mu := &t.keyLocks[h.Sum32()%uint32(len(t.keyLocks))]
	mu.Lock()
	return mu.Unlock
}

// Object is one origin response, served from the cache or passed through.
type Object struct {
	Size         int64
	ETag         string
	ContentType  string
	UserMetadata []string
	LastModified time.Time
	Body         io.ReadCloser
	Start, End   int64
	Ranged       bool
	Hit          bool
}

// ContentLength returns the number of response payload bytes.
func (o *Object) ContentLength() int64 {
	if o.Size == 0 {
		return 0
	}
	return o.End - o.Start + 1
}

// Head returns metadata for bucket/key without downloading the origin body on a
// cache miss.
func (t *Tier) Head(ctx context.Context, bucket, key string) (*Object, error) {
	ck := t.key(bucket, key)
	m, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	pending := err == nil && store.IsPendingOrigin(m.OriginETag)
	if err == nil && !pending && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl {
		return &Object{
			Size: m.Size, ETag: originETag(m), ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: m.CreatedAt,
			End: max(m.Size-1, 0), Hit: true,
		}, nil
	}
	a, aerr := t.attrs(ctx, ck, key)
	if aerr != nil {
		if t.origin.IsObjNotFoundErr(aerr) {
			return nil, store.ErrNotFound
		}
		return nil, aerr
	}
	version := versionFor(a)
	if err == nil && !pending && m.Size == a.Size && m.OriginETag == version {
		t.touch(ctx, ck)
		return &Object{
			Size: m.Size, ETag: originETag(m), ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: time.Now(),
			End: max(m.Size-1, 0), Hit: true,
		}, nil
	}
	ct := a.ContentType
	if ct == "" {
		ct = contentTypeFor(key)
	}
	return &Object{
		Size: a.Size, ETag: etagForAttrs(a), ContentType: ct,
		LastModified: a.LastModified, End: max(a.Size-1, 0),
	}, nil
}

// Open serves [first,last] (or the whole object) with a cache hit, a whole
// object download, or a pass-through origin range. Warm cache hits within
// --cache-ttl do not contact the origin.
func (t *Tier) Open(ctx context.Context, bucket, key string, first, last, suffix int64) (*Object, error) {
	ck := t.key(bucket, key)
	m, merr := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck)
	if merr != nil && !errors.Is(merr, store.ErrNotFound) {
		return nil, merr
	}
	pending := merr == nil && store.IsPendingOrigin(m.OriginETag)
	valid := merr == nil && !pending && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl

	var (
		size    int64
		version string
		lastMod time.Time
		ct      string
		a       originAttrs
	)
	if valid {
		size = m.Size
		lastMod = m.CreatedAt
		ct = m.ContentType
		version = originETag(m)
	} else {
		var err error
		a, err = t.attrs(ctx, ck, key)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		size = a.Size
		lastMod = a.LastModified
		version = versionFor(a)
		ct = a.ContentType
		if merr == nil && !pending {
			if m.Size == size && m.OriginETag == version {
				t.touch(ctx, ck)
				valid = true
				ct = m.ContentType
			} else {
				// Origin moved on: never serve stale bytes.
				_, _ = t.store.CacheDel(ctx, t.namespace, []string{ck})
				m, merr = nil, store.ErrNotFound
			}
		}
	}
	if ct == "" {
		ct = contentTypeFor(key)
	}

	start, end, ranged := resolveRange(size, first, last, suffix)
	if size > 0 && (start > end || start >= size) {
		return nil, store.ErrInvalidRange
	}
	if size == 0 {
		if pending {
			return &Object{
				Size: 0, ETag: etagForAttrs(a), ContentType: ct,
				LastModified: lastMod, Body: io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
		if !valid {
			var err error
			m, err = t.fill(ctx, ck, key, 0, version, ct)
			if errors.Is(err, errPendingOrigin) {
				return &Object{
					Size: 0, ETag: etagForAttrs(a), ContentType: ct,
					LastModified: lastMod, Body: io.NopCloser(bytes.NewReader(nil)),
				}, nil
			}
			if err != nil {
				return nil, err
			}
		}
		return &Object{
			Size: 0, ETag: originETag(m), ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: m.CreatedAt,
			Body: io.NopCloser(bytes.NewReader(nil)), Hit: valid,
		}, nil
	}

	length := end - start + 1
	if pending {
		return t.passThrough(ctx, ck, start, length, size, end, ranged, a, ct, lastMod)
	}
	if t.rangeSize > 0 && !valid && size > fullCacheMax && length < size && length <= maxRangeCacheSpan {
		return t.openRange(ctx, ck, key, start, end, size, version, ct, lastMod, ranged)
	}
	if valid || length == size || size <= fullCacheMax {
		if !valid {
			if _, err := t.fill(ctx, ck, key, size, version, ct); errors.Is(err, errPendingOrigin) {
				return t.passThrough(ctx, ck, start, length, size, end, ranged, a, ct, lastMod)
			} else if err != nil {
				return nil, err
			}
		}
		sm, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, start, end, -1, nil)
		if err != nil {
			return nil, err
		}
		if store.IsPendingOrigin(sm.OriginETag) {
			_ = body.Close()
			return t.openPending(ctx, ck, key, first, last, suffix)
		}
		return &Object{
			Size: sm.Size, ETag: originETagSlice(sm), ContentType: sm.ContentType,
			UserMetadata: sm.UserMetadata, LastModified: sm.CreatedAt,
			Body: body, Start: sm.Start, End: sm.End,
			Ranged: ranged, Hit: valid,
		}, nil
	}

	return t.passThrough(ctx, ck, start, length, size, end, ranged, a, ct, lastMod)
}

// openPending serves an object from the origin after a concurrent multipart
// completion replaced a committed cache object with a pending one. The caller
// must not serve the pending cache bytes.
func (t *Tier) openPending(ctx context.Context, ck, key string, first, last, suffix int64) (*Object, error) {
	a, err := t.attrs(ctx, ck, key)
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	ct := a.ContentType
	if ct == "" {
		ct = contentTypeFor(key)
	}
	size := a.Size
	start, end, ranged := resolveRange(size, first, last, suffix)
	if size > 0 && (start > end || start >= size) {
		return nil, store.ErrInvalidRange
	}
	if size == 0 {
		return &Object{
			Size: 0, ETag: etagForAttrs(a), ContentType: ct,
			LastModified: a.LastModified, Body: io.NopCloser(bytes.NewReader(nil)),
		}, nil
	}
	return t.passThrough(ctx, ck, start, end-start+1, size, end, ranged, a, ct, a.LastModified)
}

func (t *Tier) passThrough(ctx context.Context, ck string, start, length, size, end int64, ranged bool, a originAttrs, ct string, lastMod time.Time) (*Object, error) {
	rc, err := t.origin.GetRange(ctx, ck, start, length)
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	return &Object{
		Size: size, ETag: etagForAttrs(a), ContentType: ct,
		LastModified: lastMod, Body: rc, Start: start, End: end, Ranged: ranged,
	}, nil
}

// fill downloads ck from the origin under the fill semaphore, stores it in the
// cache namespace with version, and returns the stored metadata. Concurrent
// fills for the same ck coalesce via singleflight so a thundering herd on a
// cold key pulls from the origin only once.
func rangeChunkKey(ck string, offset int64) string {
	return fmt.Sprintf("%s.__range__.%016x", ck, offset)
}

// openRange serves a bounded range from aligned cached chunks. It is used for
// large objects where downloading the whole object would be wasteful but the
// caller still reads small, repeatable ranges (Parquet footers, row groups,
// PMTiles).
func (t *Tier) openRange(ctx context.Context, ck, key string, start, end, size int64, version, ct string, lastMod time.Time, ranged bool) (*Object, error) {
	length := end - start + 1
	body := make([]byte, 0, length)
	first := start - start%t.rangeSize
	for off := first; off <= end; off += t.rangeSize {
		want := t.rangeSize
		if off+want > size {
			want = size - off
		}
		chunk, err := t.rangeChunk(ctx, ck, key, off, want, version, ct)
		if err != nil {
			return nil, err
		}
		from := int64(0)
		if off == first && start > off {
			from = start - off
		}
		to := int64(len(chunk))
		if off == first+((end-first)/t.rangeSize)*t.rangeSize && end < off+to-1 {
			to = end - off + 1
		}
		if from < to {
			body = append(body, chunk[from:to]...)
		}
	}
	if int64(len(body)) != length {
		return nil, fmt.Errorf("range cache assembled %d bytes, wanted %d", len(body), length)
	}
	return &Object{
		Size: size, ETag: version, ContentType: ct, LastModified: lastMod,
		Body: io.NopCloser(bytes.NewReader(body)), Start: start, End: end,
		Ranged: ranged,
	}, nil
}

func (t *Tier) rangeChunk(ctx context.Context, ck, key string, off, length int64, version, ct string) ([]byte, error) {
	chunkKey := rangeChunkKey(ck, off)
	if m, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, chunkKey); err == nil && m.OriginETag == version && m.Size == length {
		t.touch(ctx, chunkKey)
		_, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, chunkKey, 0, -1, -1, nil)
		if err == nil {
			b, rerr := io.ReadAll(body)
			_ = body.Close()
			if rerr == nil && int64(len(b)) == length {
				return b, nil
			}
		}
	}

	ch := t.group.DoChan(chunkKey+"\x00"+version, func() (any, error) {
		unlock := t.lockKey(chunkKey)
		defer unlock()
		if m, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, chunkKey); err == nil && m.OriginETag == version && m.Size == length {
			return nil, nil
		}
		fillCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		select {
		case t.fills <- struct{}{}:
			defer func() { <-t.fills }()
		case <-fillCtx.Done():
			return nil, fillCtx.Err()
		}
		rc, err := t.origin.GetRange(fillCtx, ck, off, length)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := spoolStream(io.LimitReader(rc, length))
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		defer cleanup()
		if sp.size != length {
			return nil, fmt.Errorf("origin range %s at %d returned %d bytes, wanted %d", key, off, sp.size, length)
		}
		_, err = t.storeSpooled(fillCtx, chunkKey, nil, ct, version, sp)
		return nil, err
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	_, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, chunkKey, 0, -1, -1, nil)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != length {
		return nil, fmt.Errorf("cached range chunk %s has %d bytes, wanted %d", chunkKey, len(b), length)
	}
	return b, nil
}

func (t *Tier) fill(ctx context.Context, ck, key string, _ int64, version, ct string) (*store.Meta, error) {
	ch := t.group.DoChan(ck, func() (any, error) {
		unlock := t.lockKey(ck)
		defer unlock()
		if cur, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck); err == nil && store.IsPendingOrigin(cur.OriginETag) {
			return nil, errPendingOrigin
		}
		fillCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
		defer cancel()
		select {
		case t.fills <- struct{}{}:
			defer func() { <-t.fills }()
		case <-fillCtx.Done():
			return nil, fillCtx.Err()
		}
		rc, err := t.origin.Get(fillCtx, ck)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := spoolStream(rc)
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		defer cleanup()
		return t.storeSpooled(fillCtx, ck, nil, ct, version, sp)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*store.Meta), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type spooled struct {
	r    io.ReadSeeker
	data []byte
	size int64
}

// spoolStream buffers r into memory (<= memFillMax) or a temporary file so no
// PostgreSQL connection is held during network I/O.
func spoolStream(r io.Reader) (spooled, func(), error) {
	head, err := io.ReadAll(io.LimitReader(r, memFillMax+1))
	if err != nil {
		return spooled{}, nil, err
	}
	if len(head) <= memFillMax {
		return spooled{
			r:    bytes.NewReader(head),
			data: head,
			size: int64(len(head)),
		}, func() {}, nil
	}
	tmp, err := os.CreateTemp("", "pgvs3-fill-*")
	if err != nil {
		return spooled{}, nil, err
	}
	name := tmp.Name()
	clean := func() { _ = tmp.Close(); _ = os.Remove(name) }
	if _, err := tmp.Write(head); err != nil {
		clean()
		return spooled{}, nil, err
	}
	n, err := io.Copy(tmp, r)
	if err != nil {
		clean()
		return spooled{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		clean()
		return spooled{}, nil, err
	}
	return spooled{r: tmp, size: int64(len(head)) + n}, clean, nil
}

func (t *Tier) storeSpooled(ctx context.Context, ck string, userMeta []string, ct, version string, sp spooled) (*store.Meta, error) {
	exp := time.Now().Add(t.retention)
	if sp.data != nil && int64(len(sp.data)) <= int64(ns.Cache.InlineMax()) {
		return t.store.PutInlineKind(ctx, ns.Cache, t.namespace, ck, sp.data,
			store.Unconditional, "", userMeta, ct, &exp, false, version)
	}
	if _, err := sp.r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	w, err := t.store.StartObjectKind(ctx, ns.Cache, t.namespace, ck,
		store.Unconditional, "", userMeta, ct, &exp, false, version)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(w, sp.r); err != nil {
		w.Abort()
		return nil, err
	}
	pub, err := w.Finish(nil)
	if err != nil {
		return nil, err
	}
	return &store.Meta{
		FileID: pub.FileID, Size: pub.Size, SHA256: pub.SHA256, ETag: pub.ETag,
		UserMetadata: userMeta, ContentType: ct, CreatedAt: time.Now(),
		ExpiresAt: &exp, OriginETag: version,
	}, nil
}

// Upload spools the body, writes it through to the origin, and populates the
// PostgreSQL cache so other cache consumers immediately read a cache hit
// without re-pulling from the origin.
func (t *Tier) Upload(ctx context.Context, bucket, key string, body io.Reader, userMeta []string, contentType string) (*store.Meta, error) {
	ck := t.key(bucket, key)
	sp, cleanup, err := spoolStream(body)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	unlock := t.lockKey(ck)
	defer unlock()

	if err := t.origin.Upload(ctx, ck, sp.r, sp.size); err != nil {
		return nil, err
	}
	// The origin now has this write; an older pending multipart completion for
	// the same key must not be flushed over it.
	_ = t.store.ClearPendingUpload(context.WithoutCancel(ctx), t.namespace, ck)
	var version string
	var attrs originAttrs
	if a, err := t.attrs(ctx, ck, key); err == nil {
		attrs = a
		version = versionFor(a)
	}
	m, err := t.storeSpooled(ctx, ck, userMeta, contentType, version, sp)
	if err != nil {
		// Drop a superseded pending cache object if the replacement cache write
		// failed; the origin already has the new bytes.
		_, _ = t.store.CacheDel(context.WithoutCancel(ctx), t.namespace, []string{ck})
		return nil, err
	}
	if !attrs.LastModified.IsZero() {
		m.ETag = etagForAttrs(attrs)
		_ = t.store.UpsertOriginEntry(ctx, t.namespace, store.OriginListEntry{
			Key: ck, Size: sp.size, ETag: etagForAttrs(attrs),
			ContentType: contentType, LastModified: attrs.LastModified,
		})
	}
	return m, nil
}

// Copy copies srcBucket/srcKey to dstBucket/dstKey with write-through caching:
// the source is read from the cache (or origin if cold) and the destination is
// written to both the origin and the PostgreSQL cache.
func (t *Tier) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, userMeta []string, contentType string, replaceMeta bool) (*store.Meta, error) {
	srcObj, err := t.Open(ctx, srcBucket, srcKey, 0, -1, -1)
	if err != nil {
		return nil, err
	}
	defer srcObj.Body.Close()
	meta, ct := srcObj.UserMetadata, srcObj.ContentType
	if replaceMeta {
		meta, ct = userMeta, contentType
	}
	return t.Upload(ctx, dstBucket, dstKey, srcObj.Body, meta, ct)
}

// CreateUpload starts a write-through multipart upload staged in the cache namespace.
func (t *Tier) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType string, checksumAlg *string) (string, error) {
	return t.store.CreateUploadKind(ctx, ns.Cache, t.namespace, t.key(bucket, key), userMeta, contentType, checksumAlg)
}

// StartPart opens a multipart part writer in the cache namespace.
func (t *Tier) StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32) (*store.ChunkWriter, error) {
	return t.store.StartPartKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key), partNo)
}

// CompleteUpload publishes the multipart object in the cache as pending,
// records an outbox row, and flushes it to the origin. Readers treat the
// pending object as a miss, so a failed or crashed upload cannot be served.
func (t *Tier) CompleteUpload(ctx context.Context, uploadID, bucket, key string, listed []store.ListedPart) (*store.Completed, error) {
	ck := t.key(bucket, key)
	exp := time.Now().Add(t.retention)
	select {
	case t.completes <- struct{}{}:
		defer func() { <-t.completes }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	unlock := t.lockKey(ck)
	defer unlock()
	done, err := t.store.CompleteUploadKindPending(ctx, ns.Cache, uploadID, t.namespace, ck, listed, &exp)
	if err != nil {
		return nil, err
	}
	_ = t.store.DeleteOriginEntries(context.WithoutCancel(ctx), t.namespace, []string{ck})
	if err := t.flushOneLocked(ctx, ck, store.PendingOriginToken(uploadID)); err != nil {
		return nil, err
	}
	if a, err := t.attrs(ctx, ck, key); err == nil {
		done.ETag = etagForAttrs(a)
	}
	done.Bucket = bucket
	done.Key = key
	return done, nil
}

// flushOne coalesces concurrent attempts for the same key.
func (t *Tier) flushOne(ctx context.Context, ck, token string) error {
	unlock := t.lockKey(ck)
	defer unlock()
	_, err, _ := t.group.Do("pending:"+ck, func() (any, error) {
		return nil, t.flushOneLocked(ctx, ck, token)
	})
	return err
}

// flushOneLocked spools the pending cache object, uploads it to the origin,
// then marks the cache object committed. The PostgreSQL connection is released
// before the origin upload starts.
func (t *Tier) flushOneLocked(ctx context.Context, ck, token string) error {
	sm, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, 0, -1, -1, nil)
	if errors.Is(err, store.ErrNotFound) {
		_ = t.store.ClearPendingUpload(context.WithoutCancel(ctx), t.namespace, ck)
		return nil
	}
	if err != nil {
		return err
	}
	if sm.OriginETag != token {
		_ = t.store.ClearPendingUploadToken(context.WithoutCancel(ctx), t.namespace, ck, token)
		return nil
	}
	sp, cleanup, err := spoolStream(body)
	_ = body.Close()
	if err != nil {
		_ = t.store.FailPendingUpload(context.WithoutCancel(ctx), t.namespace, ck, token, err)
		return err
	}
	defer cleanup()
	if err := t.origin.Upload(ctx, ck, sp.r, sp.size); err != nil {
		_ = t.store.FailPendingUpload(context.WithoutCancel(ctx), t.namespace, ck, token, err)
		return err
	}
	version := sm.ETag
	var attrs originAttrs
	if a, err := t.attrs(ctx, ck, ck); err == nil {
		attrs = a
		version = versionFor(a)
	}
	if err := t.store.MarkOriginCommitted(ctx, t.namespace, ck, token, version); err != nil {
		_ = t.store.FailPendingUpload(context.WithoutCancel(ctx), t.namespace, ck, token, err)
		return err
	}
	if !attrs.LastModified.IsZero() {
		_ = t.store.UpsertOriginEntry(ctx, t.namespace, store.OriginListEntry{
			Key: ck, Size: sm.Size, ETag: etagForAttrs(attrs),
			ContentType: sm.ContentType, LastModified: attrs.LastModified,
		})
	}
	return nil
}

func (t *Tier) runPending(ctx context.Context) {
	defer close(t.done)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		pending, err := t.store.PendingUploads(ctx, t.namespace, 8)
		if err != nil {
			t.log.Warn("list pending origin uploads", "err", err)
			continue
		}
		for _, p := range pending {
			flushCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := t.flushOne(flushCtx, p.Key, p.Token)
			cancel()
			if err != nil {
				t.log.Warn("pending origin upload failed", "key", p.Key, "attempt", p.Attempts+1, "err", err)
			}
		}
	}
}

// AbortUpload discards an in-progress multipart upload in the cache namespace.
func (t *Tier) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	return t.store.AbortUploadKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key))
}

// ListParts returns one page of an in-progress multipart upload's parts in the cache namespace.
func (t *Tier) ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]store.UploadPartInfo, bool, int32, error) {
	return t.store.ListPartsKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key), max, marker)
}

// Delete removes the object from the origin and the cache.
func (t *Tier) Delete(ctx context.Context, bucket, key string) error {
	ck := t.key(bucket, key)
	unlock := t.lockKey(ck)
	defer unlock()
	err := t.origin.Delete(ctx, ck)
	if err != nil && !t.origin.IsObjNotFoundErr(err) {
		return err
	}
	cleanupCtx := context.WithoutCancel(ctx)
	_, _ = t.store.CacheDel(cleanupCtx, t.namespace, []string{ck})
	_ = t.store.DeleteOriginEntries(cleanupCtx, t.namespace, []string{ck})
	_ = t.store.ClearPendingUpload(cleanupCtx, t.namespace, ck)
	return nil
}

// DeleteResult is the outcome of one origin delete. A nil Err means the key
// was deleted or did not exist (both are success in S3 DeleteObjects).
type DeleteResult struct {
	Key string
	Err error
}

// DeleteMany removes each key from the origin and evicts successful keys from
// the cache. It continues past per-key failures so the S3 layer can return
// S3-shaped per-key results.
func (t *Tier) DeleteMany(ctx context.Context, bucket string, keys []string) []DeleteResult {
	results := make([]DeleteResult, 0, len(keys))
	for _, k := range keys {
		ck := t.key(bucket, k)
		unlock := t.lockKey(ck)
		err := t.origin.Delete(ctx, ck)
		if err != nil && !t.origin.IsObjNotFoundErr(err) {
			unlock()
			results = append(results, DeleteResult{Key: k, Err: err})
			continue
		}
		cleanupCtx := context.WithoutCancel(ctx)
		_, _ = t.store.CacheDel(cleanupCtx, t.namespace, []string{ck})
		_ = t.store.DeleteOriginEntries(cleanupCtx, t.namespace, []string{ck})
		_ = t.store.ClearPendingUpload(cleanupCtx, t.namespace, ck)
		unlock()
		results = append(results, DeleteResult{Key: k})
	}
	return results
}

// List returns up to limit origin keys under prefix in lexical order. It
// serves from the PostgreSQL listing index when that prefix is fresh, and
// rebuilds the index from the origin on a cold or stale prefix. If the rebuild
// fails (for example an origin outage) it falls back to a direct walk.
func (t *Tier) List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	if limit <= 0 {
		return nil, nil
	}
	pfx := bucket + "/" + prefix
	prefixEnd := pfx + "\U0010FFFF"
	if t.store != nil {
		rows, fresh, err := t.store.ListOrigin(ctx, t.namespace, pfx, prefixEnd, bucket+"/"+after, limit, t.ttl)
		if err == nil && fresh {
			return listedFromEntries(rows, bucket), nil
		}
		if err := t.refreshListing(ctx, bucket, prefix); err != nil {
			t.log.Warn("origin listing refresh failed; falling back to direct walk", "bucket", bucket, "prefix", prefix, "err", err)
			return t.listDirect(ctx, bucket, prefix, after, limit)
		}
		rows, fresh, err = t.store.ListOrigin(ctx, t.namespace, pfx, prefixEnd, bucket+"/"+after, limit, t.ttl)
		if err == nil && fresh {
			return listedFromEntries(rows, bucket), nil
		}
	}
	return t.listDirect(ctx, bucket, prefix, after, limit)
}

func (t *Tier) refreshListing(ctx context.Context, bucket, prefix string) error {
	pfx := bucket + "/" + prefix
	_, err, _ := t.group.Do("listing:"+pfx, func() (any, error) {
		listCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		if err := t.store.EnsureNamespace(listCtx, ns.Cache, t.namespace, nil, nil); err != nil {
			return nil, err
		}
		dir := bucket + "/"
		if i := strings.LastIndexByte(pfx, '/'); i >= 0 {
			dir = pfx[:i+1]
		}
		entries := make([]store.OriginListEntry, 0, 1024)
		err := t.origin.Iter(listCtx, dir, func(name string) error {
			if strings.HasSuffix(name, "/") || !strings.HasPrefix(name, pfx) {
				return nil
			}
			if len(entries) >= maxListingEntries {
				return fmt.Errorf("origin listing exceeds %d keys", maxListingEntries)
			}
			a, err := t.attrs(listCtx, name, strings.TrimPrefix(name, bucket+"/"))
			if err != nil {
				return err
			}
			entries = append(entries, store.OriginListEntry{
				Key: name, Size: a.Size, ETag: etagForAttrs(a),
				ContentType: a.ContentType, LastModified: a.LastModified,
			})
			return nil
		}, true)
		if err != nil {
			return nil, err
		}
		return nil, t.store.ReplaceOriginListing(listCtx, t.namespace, pfx, pfx+"\U0010FFFF", entries)
	})
	return err
}

// listDirect is the pre-index fallback: one origin walk plus metadata lookups.
func (t *Tier) listDirect(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	pfx := bucket + "/" + prefix
	dir := bucket + "/"
	if i := strings.LastIndexByte(pfx, '/'); i >= 0 {
		dir = pfx[:i+1]
	}
	var rels []string
	err := t.origin.Iter(ctx, dir, func(name string) error {
		if strings.HasSuffix(name, "/") || !strings.HasPrefix(name, pfx) {
			return nil
		}
		rel := strings.TrimPrefix(name, bucket+"/")
		if rel > after {
			rels = append(rels, rel)
		}
		return nil
	}, true)
	if err != nil {
		return nil, err
	}
	sort.Strings(rels)
	if len(rels) > limit {
		rels = rels[:limit]
	}
	cached := map[string]store.Listed{}
	if t.store != nil && t.ttl > 0 && len(rels) > 0 {
		rows, err := t.store.ListKind(ctx, ns.Cache, t.namespace, pfx, pfx+"\U0010FFFF", bucket+"/"+after, len(rels)*2)
		if err == nil {
			for _, r := range rows {
				if time.Since(r.CreatedAt) < t.ttl && !store.IsPendingOrigin(r.OriginETag) {
					cached[strings.TrimPrefix(r.Key, bucket+"/")] = r
				}
			}
		}
	}
	out := make([]store.Listed, 0, len(rels))
	for _, rel := range rels {
		if c, ok := cached[rel]; ok {
			c.Key = rel
			out = append(out, c)
			continue
		}
		a, err := t.attrs(ctx, bucket+"/"+rel, rel)
		if err != nil {
			continue
		}
		out = append(out, store.Listed{
			Key: rel, Size: a.Size, ETag: etagForAttrs(a), CreatedAt: a.LastModified,
		})
	}
	return out, nil
}

func listedFromEntries(entries []store.OriginListEntry, bucket string) []store.Listed {
	out := make([]store.Listed, 0, len(entries))
	for _, e := range entries {
		out = append(out, store.Listed{
			Key: strings.TrimPrefix(e.Key, bucket+"/"), Size: e.Size,
			ETag: e.ETag, CreatedAt: e.LastModified,
		})
	}
	return out
}

// resolveRange maps S3 range headers onto [start,end].
func resolveRange(size, first, last, suffix int64) (int64, int64, bool) {
	if suffix >= 0 {
		return max(size-suffix, 0), size - 1, true
	}
	start := max(first, 0)
	end := size - 1
	if last >= 0 {
		end = min(last, size-1)
	}
	return start, end, first > 0 || last >= 0
}

func versionFor(a originAttrs) string {
	if a.ETag != "" {
		return a.ETag
	}
	return fmt.Sprintf("%d-%d", a.Size, a.LastModified.UnixNano())
}

func etagForAttrs(a originAttrs) string {
	if a.ETag != "" {
		return a.ETag
	}
	return versionFor(a)
}

func originETag(m *store.Meta) string {
	if m.OriginETag != "" {
		return m.OriginETag
	}
	return m.ETag
}

func originETagSlice(m *store.SliceMeta) string {
	if m.OriginETag != "" {
		return m.OriginETag
	}
	return m.ETag
}

// attrs returns exact origin metadata from the S3-compatible provider.
func (t *Tier) attrs(ctx context.Context, ck, key string) (originAttrs, error) {
	return t.origin.Attributes(ctx, ck)
}

func (t *Tier) touch(ctx context.Context, ck string) {
	sc, err := ns.Cache.Schema(t.namespace)
	if err != nil {
		return
	}
	_, _ = t.store.Pool().Exec(ctx,
		`UPDATE "`+sc+`".objects SET created_at = now() WHERE key = $1`, ck)
}

func contentTypeFor(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}
