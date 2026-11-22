package objcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/syncx"
)

type Tier struct {
	origin    originHandle
	store     *store.Store
	runtime   *Runtime
	namespace string
	ttl       time.Duration
	retention time.Duration
	fills     chan struct{}
	group     syncx.Group
	log       *slog.Logger
	readOnly  bool
	workerID  string
	rangeSize int64
	maxBytes  int64
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	fillTTL   time.Duration

	hits              atomic.Uint64
	misses            atomic.Uint64
	hitBytes          atomic.Uint64
	missBytes         atomic.Uint64
	fillCount         atomic.Uint64
	fillDurationNanos atomic.Uint64
	originErrors      atomic.Uint64
}

const (
	memFillMax        = 8 << 20
	fullCacheMax      = 8 << 20
	maxRangeChunk     = 8 << 20
	maxRangeCacheSpan = 64 << 20
	fillLeaseTTL      = 2 * time.Minute
	fillLeaseWait     = 45 * time.Second
)

var (
	errFillBusy  = errors.New("origin fill owned by another worker")
	ErrReadOnly  = errors.New("cache tier is read-only")
	ErrSpoolBusy = errors.New("cache spool byte budget exhausted")
)

type RuntimeOptions struct {
	Fills      int
	SpoolDir   string
	SpoolBytes int64
}

type Runtime struct {
	fills chan struct{}
	spool *spoolBudget
}

func NewRuntime(opts RuntimeOptions) *Runtime {
	if opts.Fills <= 0 {
		opts.Fills = 64
	}
	if opts.SpoolBytes <= 0 {
		opts.SpoolBytes = 8 << 30
	}
	return &Runtime{
		fills: make(chan struct{}, opts.Fills),
		spool: newSpoolBudget(opts.SpoolBytes, opts.SpoolDir),
	}
}

type TierOptions struct {
	Namespace  string
	TTL        time.Duration
	Retention  time.Duration
	MaxFills   int
	RangeChunk int64
	MaxBytes   int64
	ReadOnly   bool
}

func acquireProcess(sem chan struct{}, ctx context.Context) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func New(log *slog.Logger, origin originHandle, st *store.Store, runtime *Runtime, opts TierOptions) (*Tier, error) {
	if origin == nil {
		return nil, errors.New("origin is required")
	}
	if runtime == nil {
		return nil, errors.New("cache runtime is required")
	}
	if !opts.ReadOnly && st == nil {
		return nil, errors.New("writable cache tier requires a store")
	}
	if log == nil {
		log = slog.Default()
	}
	if opts.Namespace == "" {
		opts.Namespace = "origin"
	}
	if !ns.ValidName(opts.Namespace) {
		return nil, fmt.Errorf("invalid cache namespace %q", opts.Namespace)
	}
	if opts.TTL < 0 {
		opts.TTL = 0
	}
	if opts.Retention <= 0 {
		opts.Retention = 24 * time.Hour
	}
	if opts.MaxFills <= 0 {
		opts.MaxFills = 8
	}
	if opts.RangeChunk > 0 {
		opts.RangeChunk = min(max(opts.RangeChunk, 64<<10), maxRangeChunk)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &Tier{
		origin:    origin,
		store:     st,
		runtime:   runtime,
		namespace: opts.Namespace,
		ttl:       opts.TTL,
		retention: opts.Retention,
		fills:     make(chan struct{}, opts.MaxFills),
		log:       log,
		readOnly:  opts.ReadOnly,
		rangeSize: max(opts.RangeChunk, 0),
		maxBytes:  max(opts.MaxBytes, 0),
		workerID:  randomID(),
		ctx:       ctx,
		cancel:    cancel,
	}
	if st != nil {
		st.SetCacheMaxBytes(opts.Namespace, t.maxBytes)
		if !opts.ReadOnly {
			if err := st.EnsureNamespace(ctx, ns.Cache, opts.Namespace, nil, nil); err != nil {
				cancel()
				return nil, fmt.Errorf("ensure cache namespace: %w", err)
			}
		}
	}
	return t, nil
}

func randomID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func (t *Tier) Name() string { return t.namespace }

func (t *Tier) BucketAllowed(bucket string) bool { return t.origin.BucketAllowed(bucket) }

func (t *Tier) WriteMetrics(w io.Writer) {
	fmt.Fprintf(w, "pgvs3_cache_hits_total %d\npgvs3_cache_misses_total %d\npgvs3_cache_hit_bytes_total %d\npgvs3_cache_miss_bytes_total %d\npgvs3_cache_fill_duration_seconds_sum %.6f\npgvs3_cache_fill_duration_seconds_count %d\npgvs3_origin_errors_total %d\n",
		t.hits.Load(), t.misses.Load(), t.hitBytes.Load(), t.missBytes.Load(),
		float64(t.fillDurationNanos.Load())/float64(time.Second), t.fillCount.Load(), t.originErrors.Load())
	if t.runtime != nil {
		t.runtime.WriteMetrics(w)
	}
}

func (t *Tier) Close() error {
	t.closeOnce.Do(func() {
		if t.cancel != nil {
			t.cancel()
		}
		if t.origin != nil {
			_ = t.origin.Close()
		}
	})
	return nil
}

func (t *Tier) baseContext() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return context.Background()
}

func (t *Tier) key(bucket, key string) string { return bucket + "/" + key }

func (t *Tier) cacheRef(ck string) store.ObjectRef {
	return store.ObjectRef{NS: ns.CacheRef(t.namespace), Key: ck}
}

type Object struct {
	store.ObjectInfo
	Body   io.ReadCloser
	Start  int64
	End    int64
	Ranged bool
	Hit    bool
}

func (o *Object) ContentLength() int64 {
	if o == nil {
		return 0
	}
	if o.Ranged {
		return o.End - o.Start + 1
	}
	return o.Size
}

func (o *Object) Close() error {
	if o == nil || o.Body == nil {
		return nil
	}
	return o.Body.Close()
}

type readView struct {
	store.ObjectInfo
	start  int64
	end    int64
	ranged bool
	hit    bool
}

func newObject(v readView, body io.ReadCloser) *Object {
	info := v.ObjectInfo
	return &Object{
		ObjectInfo: info,
		Body:       body,
		Start:      v.start,
		End:        v.end,
		Ranged:     v.ranged,
		Hit:        v.hit,
	}
}

type cacheAction uint8

const (
	cacheActionRead cacheAction = iota
	cacheActionZero
	cacheActionPassThrough
	cacheActionRange
)

type cachePlan struct {
	action cacheAction
}

func planCacheRead(res cacheResolution, rangeSize int64, readOnly bool, v readView) cachePlan {
	if v.Size == 0 {
		return cachePlan{action: cacheActionZero}
	}
	if res.meta != nil && (res.meta.Size == 0 || res.meta.Inline != nil || res.meta.FileID > 0) {
		return cachePlan{action: cacheActionRead}
	}
	if rangeSize > 0 && v.Size > rangeSize {
		span := v.end - v.start + 1
		if v.ranged && (span <= maxRangeCacheSpan || v.Size > fullCacheMax) {
			return cachePlan{action: cacheActionRange}
		}
		if v.Size > fullCacheMax {
			return cachePlan{action: cacheActionPassThrough}
		}
	}
	if v.Size > fullCacheMax && v.ranged {
		return cachePlan{action: cacheActionPassThrough}
	}
	if readOnly && res.meta == nil {
		return cachePlan{action: cacheActionPassThrough}
	}
	return cachePlan{action: cacheActionRead}
}

type cacheResolution struct {
	meta       *store.Meta
	ref        store.ObjectRef
	info       store.ObjectInfo
	attrs      originAttrs
	valid      bool
	attrsValid bool
}

func (t *Tier) isFresh(createdAt time.Time) bool {
	return t.ttl == 0 || time.Since(createdAt) < t.ttl
}

func (t *Tier) resolveCache(ctx context.Context, bucket, key, ck string, deleteStale, withInline, skipAttributesOnMiss bool) (cacheResolution, error) {
	res := cacheResolution{ref: t.cacheRef(ck)}
	var m *store.Meta
	var info *store.ObjectInfo
	var err error
	if !skipAttributesOnMiss && t.rangeSize > 0 {
		var cref ns.Ref
		m, cref, info, err = t.store.Cache(t.namespace).MetadataOrOrigin(ctx, ck, withInline, t.ttl)
		res.ref = store.ObjectRef{NS: cref, Key: ck}
	} else if withInline {
		m, err = t.store.Meta(ctx, res.ref)
	} else {
		m, err = t.store.MetaHeader(ctx, res.ref)
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return res, err
	}
	if m != nil {
		res.meta = m
		if t.isFresh(m.CreatedAt) {
			res.info = cachedInfo(m)
			res.valid = true
			return res, nil
		}
	} else if info != nil {
		res.info = *info
		res.attrs = originAttrs{
			Size: info.Size, ETag: info.OriginETag,
			LastModified: info.LastModified(), ContentType: info.ContentType,
			ContentEncoding: info.ContentEncoding, UserMetadata: info.UserMetadata,
		}
		res.attrsValid = true
		return res, nil
	} else if skipAttributesOnMiss {
		return res, nil
	}

	type headResult struct {
		attrs       originAttrs
		notModified bool
	}
	ifNoneMatch := ""
	if m != nil {
		ifNoneMatch = m.OriginETag
	}
	validated := time.Now()
	ch := t.group.DoChan("head:"+ck+":"+ifNoneMatch, func() (any, error) {
		headCtx, cancel := context.WithTimeout(t.baseContext(), 30*time.Second)
		defer cancel()
		attrs, notMod, err := t.origin.AttributesIfNoneMatch(headCtx, bucket, key, ifNoneMatch)
		if err != nil {
			return nil, err
		}
		return headResult{attrs: attrs, notModified: notMod}, nil
	})
	var hr headResult
	var aerr error
	select {
	case r := <-ch:
		if r.Err != nil {
			aerr = r.Err
		} else {
			hr = r.Val.(headResult)
		}
	case <-ctx.Done():
		return res, ctx.Err()
	}
	if aerr != nil {
		if isObjectNotFound(aerr) {
			if m != nil && !t.readOnly && deleteStale {
				_, _ = t.store.DeleteExpected(context.WithoutCancel(ctx), res.ref, m.OriginETag)
			}
			return res, store.ErrNotFound
		}
		return res, aerr
	}
	if hr.notModified && m != nil {
		if !t.readOnly {
			_ = t.store.Cache(t.namespace).MarkRevalidated(t.baseContext(), ck, m.OriginETag, t.retention)
		}
		res.info = cachedInfo(m)
		res.valid = true
		return res, nil
	}
	a := hr.attrs
	res.attrs, res.info = a, a.objectInfo()
	if m == nil && t.rangeSize > 0 && a.Size > t.rangeSize && !t.readOnly {
		ttl := t.ttl
		if ttl == 0 {
			ttl = t.retention
		}
		if err := t.store.Cache(t.namespace).RememberOriginMetadata(ctx, ck, res.info, validated, min(ttl, t.retention)); err != nil {
			return res, err
		}
	}
	if m != nil {
		if m.Size == res.info.Size && m.OriginETag == res.info.OriginETag {
			if !t.readOnly {
				_ = t.store.Cache(t.namespace).MarkRevalidated(t.baseContext(), ck, m.OriginETag, t.retention)
			}
			res.info = cachedInfo(m)
			res.valid = true
		} else if !t.readOnly && deleteStale {
			_, _ = t.store.DeleteExpected(context.WithoutCancel(ctx), res.ref, m.OriginETag)
		}
	}
	return res, nil
}

func cachedInfo(m *store.Meta) store.ObjectInfo {
	info := m.ObjectInfo
	info.ETag = responseETag(m)
	info.OriginETag = originETag(m)
	return info
}

func responseETag(m *store.Meta) string {
	if v := m.OriginETag; v != "" {
		return v
	}
	return m.ETag
}

func originETag(m *store.Meta) string {
	if m.OriginETag != "" {
		return m.OriginETag
	}
	return m.ETag
}

func (t *Tier) RangeChunkSize() int64 { return t.rangeSize }

func (t *Tier) Head(ctx context.Context, bucket, key string) (*Object, error) {
	ck := t.key(bucket, key)
	res, err := t.resolveCache(ctx, bucket, key, ck, false, false, false)
	if err != nil {
		return nil, err
	}
	info := res.info
	if info.ContentType == "" {
		info.ContentType = contentTypeFor(key)
	}
	return newObject(readView{ObjectInfo: info, end: max(info.Size-1, 0), hit: res.valid || res.attrsValid}, nil), nil
}

func (t *Tier) Open(ctx context.Context, bucket, key string, rng store.ByteRange) (*Object, error) {
	obj, err := t.open(ctx, bucket, key, rng)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidRange) && !errors.Is(err, context.Canceled) {
			t.originErrors.Add(1)
		}
		return nil, err
	}
	if obj.Hit {
		t.hits.Add(1)
		t.hitBytes.Add(uint64(obj.ContentLength()))
	} else {
		t.misses.Add(1)
		t.missBytes.Add(uint64(obj.ContentLength()))
	}
	return obj, nil
}

func (t *Tier) open(ctx context.Context, bucket, key string, rng store.ByteRange) (*Object, error) {
	ck := t.key(bucket, key)
	skipAttributes := !rng.Requested
	res, err := t.resolveCache(ctx, bucket, key, ck, true, true, skipAttributes)
	if err != nil {
		return nil, err
	}
	if skipAttributes && res.meta == nil {
		if t.readOnly {
			return t.passThroughWhole(ctx, bucket, key, rng)
		}
		m, fillErr := t.fill(ctx, bucket, key, ck)
		if errors.Is(fillErr, errFillBusy) {
			return t.passThroughWhole(ctx, bucket, key, rng)
		}
		if errors.Is(fillErr, store.ErrStaleCache) {
			return t.passThroughWhole(ctx, bucket, key, rng)
		}
		if fillErr != nil {
			return nil, fillErr
		}
		span, spanErr := rng.Resolve(m.Size)
		if spanErr != nil {
			return nil, spanErr
		}
		return t.openCached(ctx, t.cacheRef(ck), m, readView{start: span.Start, end: span.End, ranged: span.Ranged})
	}
	size, version, lastMod, ct := res.info.Size, res.info.OriginETag, res.info.CreatedAt, res.info.ContentType
	if ct == "" {
		ct = contentTypeFor(key)
	}
	span, err := rng.Resolve(size)
	if err != nil {
		return nil, err
	}
	view := readView{Size: size, ContentType: ct, CreatedAt: lastMod, start: span.Start, end: span.End, ranged: span.Ranged}
	plan := planCacheRead(res, t.rangeSize, t.readOnly, view)
	switch plan.action {
	case cacheActionZero:
		return t.openZero(ctx, bucket, key, ck, res, rng)
	case cacheActionPassThrough:
		return t.passThrough(ctx, bucket, key, view, res.attrs)
	case cacheActionRange:
		view.ETag, view.ContentEncoding, view.UserMetadata = version, res.info.ContentEncoding, res.attrs.UserMetadata
		obj, err := t.openRange(ctx, bucket, key, ck, view)
		if errors.Is(err, store.ErrStaleCache) || errors.Is(err, errFillBusy) {
			return t.passThrough(ctx, bucket, key, view, res.attrs)
		}
		return obj, err
	default:
		readMeta, readRef := res.meta, res.ref
		if !res.valid && t.readOnly {
			return t.passThrough(ctx, bucket, key, view, res.attrs)
		}
		if !res.valid {
			fm, err := t.fill(ctx, bucket, key, ck)
			if errors.Is(err, errFillBusy) || errors.Is(err, store.ErrStaleCache) {
				return t.passThrough(ctx, bucket, key, view, res.attrs)
			}
			if err != nil {
				return nil, err
			}
			if fm.Size != size {
				return t.passThrough(ctx, bucket, key, view, res.attrs)
			}
			readMeta, readRef = fm, t.cacheRef(ck)
		}
		return t.openCached(ctx, readRef, readMeta, view)
	}
}

func (t *Tier) openZero(ctx context.Context, bucket, key, ck string, res cacheResolution, rng store.ByteRange) (*Object, error) {
	if _, err := rng.Resolve(0); err != nil {
		return nil, err
	}
	if res.meta == nil && !t.readOnly {
		if _, err := t.fill(ctx, bucket, key, ck); err != nil && !errors.Is(err, errFillBusy) {
			return nil, err
		}
	} else if res.meta != nil && !res.valid {
		t.touch(ck)
	}
	info := res.info
	info.ETag = res.info.OriginETag
	if res.meta != nil {
		info = cachedInfo(res.meta)
	}
	return newObject(readView{ObjectInfo: info, ranged: rng.Requested, hit: res.valid}, io.NopCloser(bytes.NewReader(nil))), nil
}

func (t *Tier) openCached(ctx context.Context, ref store.ObjectRef, m *store.Meta, v readView) (*Object, error) {
	rng := store.WholeObjectRange()
	if v.ranged {
		rng = store.Range(v.start, v.end, -1)
	}
	sm, body, err := t.store.GetBodyFromMeta(ctx, ref, m, rng)
	if err != nil {
		return nil, err
	}
	info := sm.ObjectInfo
	info.ETag, info.OriginETag = responseETag(m), originETag(m)
	t.touch(ref.Key)
	return newObject(readView{ObjectInfo: info, start: sm.Start, end: sm.End, ranged: v.ranged, hit: true}, body), nil
}

func (t *Tier) passThroughWhole(ctx context.Context, bucket, key string, rng store.ByteRange) (*Object, error) {
	rc, attrs, err := t.origin.GetWithAttrs(ctx, bucket, key)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	span, err := rng.Resolve(attrs.Size)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	if span.Ranged {
		_ = rc.Close()
		return t.passThrough(ctx, bucket, key, readView{
			ObjectInfo: attrs.objectInfo(),
			start:      span.Start, end: span.End, ranged: true,
		}, attrs)
	}
	info := attrs.objectInfo()
	info.ETag = attrs.ETag
	return newObject(readView{ObjectInfo: info, end: max(attrs.Size-1, 0)}, rc), nil
}

func (t *Tier) passThrough(ctx context.Context, bucket, key string, v readView, a originAttrs) (*Object, error) {
	rc, err := t.origin.GetRange(ctx, bucket, key, v.start, v.end-v.start+1, a.ETag)
	if err != nil {
		if isObjectNotFound(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	v.ETag, v.ContentEncoding, v.UserMetadata = a.ETag, a.ContentEncoding, a.UserMetadata
	return newObject(v, rc), nil
}

func rangeChunkKey(ck string, offset int64, version string) string {
	return fmt.Sprintf("%s#r#%016x#%s", ck, offset, version)
}

func (t *Tier) rangeChunkLen(v readView, off int64) int64 { return min(t.rangeSize, v.Size-off) }

func (t *Tier) openRange(ctx context.Context, bucket, key, ck string, v readView) (*Object, error) {
	start, end := v.start, v.end
	var offsets []int64
	for off := start - start%t.rangeSize; off <= end; off += t.rangeSize {
		offsets = append(offsets, off)
	}
	keys := make([]string, len(offsets))
	for i, off := range offsets {
		keys[i] = rangeChunkKey(ck, off, v.ETag)
	}
	retention := t.retention
	if t.readOnly {
		retention = 0
	}
	chunks, err := t.store.Cache(t.namespace).ReadManyWithTouch(ctx, keys, retention)
	if err != nil {
		return nil, err
	}
	present := func(i int) bool { return chunks[i] != nil && int64(len(chunks[i])) == t.rangeChunkLen(v, offsets[i]) }
	hit := true
	for i := 0; i < len(offsets); {
		if present(i) {
			i++
			continue
		}
		if t.readOnly {
			return nil, errFillBusy
		}
		j := i
		for j+1 < len(offsets) && !present(j+1) {
			next := j + 1
			if (offsets[next]-offsets[i])+t.rangeChunkLen(v, offsets[next]) > maxRangeChunk {
				break
			}
			j = next
		}
		filled, err := t.fillRangeRun(ctx, bucket, key, ck, v, offsets[i], offsets[j])
		if err != nil {
			return nil, err
		}
		copy(chunks[i:j+1], filled)
		hit = false
		i = j + 1
	}
	parts := make([][]byte, len(offsets))
	var assembled int64
	for i, off := range offsets {
		part := chunks[i][max(start-off, 0):min(end-off+1, int64(len(chunks[i])))]
		parts[i] = part
		assembled += int64(len(part))
	}
	if assembled != end-start+1 {
		return nil, fmt.Errorf("range cache assembled %d bytes, wanted %d", assembled, end-start+1)
	}
	v.hit = hit
	return newObject(v, store.NewPartsBody(parts)), nil
}

func (t *Tier) fillRangeRun(ctx context.Context, bucket, key, ck string, v readView, first, last int64) ([][]byte, error) {
	start := time.Now()
	defer func() {
		t.fillCount.Add(1)
		t.fillDurationNanos.Add(uint64(time.Since(start)))
	}()
	version := v.ETag
	length := last + t.rangeChunkLen(v, last) - first
	ch := t.group.DoChan(fmt.Sprintf("range:%s:%x:%x:%s", ck, first, length, version), func() (any, error) {
		fillCtx, cancel := context.WithTimeout(t.baseContext(), 2*time.Minute)
		defer cancel()
		if !acquireProcess(t.runtime.fills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-t.runtime.fills }()
		if !acquireProcess(t.fills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-t.fills }()
		rc, err := t.origin.GetRange(fillCtx, bucket, key, first, length, version)
		if err != nil {
			if errors.Is(err, errOriginPrecondition) {
				_ = t.store.Cache(t.namespace).ForgetOriginMetadata(fillCtx, ck, version)
				return nil, store.ErrStaleCache
			}
			if isObjectNotFound(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		buf := make([]byte, length)
		_, err = io.ReadFull(rc, buf)
		_ = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("origin range %s at %d: %w", key, first, err)
		}
		var chunks [][]byte
		g, gctx := syncx.WithContext(fillCtx)
		g.SetLimit(4)
		for off := first; off <= last; off += t.rangeSize {
			chunk := buf[off-first : off-first+t.rangeChunkLen(v, off)]
			chunks = append(chunks, chunk)
			chunkKey := rangeChunkKey(ck, off, version)
			g.Go(func() error {
				_, err := t.storeSpooled(gctx, t.cacheRef(chunkKey), spooled{r: bytes.NewReader(chunk), data: chunk, size: int64(len(chunk))},
					store.WriteOptions{ContentType: v.ContentType, OriginModified: v.CreatedAt,
						Publication: store.FillPublication(version, version, nil)}, nil)
				return err
			})
		}
		if err := g.Wait(); err != nil {
			t.log.Debug("range chunk store failed", "key", key, "offset", first, "err", err)
		}
		return chunks, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([][]byte), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *Tier) currentMeta(ctx context.Context, ck string) (*store.Meta, error) {
	m, err := t.store.Meta(ctx, t.cacheRef(ck))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	return m, err
}

func (t *Tier) touch(ck string) {
	if t.readOnly {
		return
	}
	exp := time.Now().Add(t.retention)
	_, _ = t.store.Cache(t.namespace).Expire(t.baseContext(), ck, &exp)
}

func (t *Tier) coalesceFill(ctx context.Context, ck string, timeout time.Duration, fetch func(context.Context) (*store.Meta, error)) (*store.Meta, error) {
	ch := t.group.DoChan("fill:"+ck, func() (any, error) {
		fillCtx, cancel := context.WithTimeout(t.baseContext(), timeout)
		defer cancel()
		if !acquireProcess(t.runtime.fills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-t.runtime.fills }()
		if !acquireProcess(t.fills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-t.fills }()
		lease, existing, err := t.acquireFill(fillCtx, ck, fillLeaseWait)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			return existing, nil
		}
		if lease == nil {
			return nil, errFillBusy
		}
		defer func() { t.store.Cache(t.namespace).ReleaseFillLease(context.Background(), ck, t.workerID) }()
		leaseCtx, stopRenew := t.renewFill(fillCtx, *lease)
		defer stopRenew()
		return fetch(leaseCtx)
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

func (t *Tier) acquireFill(ctx context.Context, ck string, wait time.Duration) (*store.FillLease, *store.Meta, error) {
	deadline := time.Now().Add(wait)
	delay := 15 * time.Millisecond
	for {
		acquired, err := t.store.Cache(t.namespace).AcquireFillLease(ctx, ck, t.workerID, t.leaseTTL())
		if err != nil {
			return nil, nil, err
		}
		if acquired {
			return &store.FillLease{Namespace: t.namespace, Key: ck, Owner: t.workerID}, nil, nil
		}
		cur, err := t.currentMeta(ctx, ck)
		if err != nil {
			return nil, nil, err
		}
		if cur != nil && t.isFresh(cur.CreatedAt) {
			return nil, cur, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, nil, nil
		}
		select {
		case <-time.After(min(delay, remaining)):
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
		delay = min(delay*2, 200*time.Millisecond)
	}
}

func (t *Tier) leaseTTL() time.Duration {
	if t.fillTTL > 0 {
		return t.fillTTL
	}
	return fillLeaseTTL
}

func (t *Tier) renewFill(ctx context.Context, lease store.FillLease) (context.Context, func()) {
	leaseCtx, cancel := context.WithCancelCause(ctx)
	interval := max(t.leaseTTL()/3, 20*time.Millisecond)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				ok, err := t.store.RenewFillLease(leaseCtx, lease, t.leaseTTL())
				if err != nil && leaseCtx.Err() == nil {
					cancel(err)
					return
				}
				if err == nil && !ok {
					cancel(errFillBusy)
					return
				}
			}
		}
	}()
	return leaseCtx, func() {
		cancel(nil)
		<-done
	}
}

func (t *Tier) fill(ctx context.Context, bucket, key, ck string) (*store.Meta, error) {
	start := time.Now()
	defer func() {
		t.fillCount.Add(1)
		t.fillDurationNanos.Add(uint64(time.Since(start)))
	}()
	return t.coalesceFill(ctx, ck, 10*time.Minute, func(fillCtx context.Context) (*store.Meta, error) {
		rc, attrs, err := t.origin.GetWithAttrs(fillCtx, bucket, key)
		if err != nil {
			if isObjectNotFound(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := t.runtime.spoolStream(fillCtx, rc, attrs.Size)
		_ = rc.Close()
		if err != nil {
			if isObjectNotFound(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		defer cleanup()
		if sp.size != attrs.Size {
			return nil, fmt.Errorf("origin GET %s returned %d bytes, wanted %d", key, sp.size, attrs.Size)
		}
		opts := attrs.writeOptions()
		opts.Publication = store.FillPublication(attrs.ETag, attrs.ETag, nil)
		m, err := t.storeSpooled(fillCtx, t.cacheRef(ck), sp, opts, nil)
		if errors.Is(err, store.ErrStaleCache) {
			return t.currentFillResult(fillCtx, ck)
		}
		return m, err
	})
}

func (t *Tier) currentFillResult(ctx context.Context, ck string) (*store.Meta, error) {
	cur, err := t.currentMeta(ctx, ck)
	if err != nil || cur != nil {
		return cur, err
	}
	return nil, store.ErrStaleCache
}

// Proxy forwards a request straight to origin without caching.
func (t *Tier) Proxy(ctx context.Context, bucket, key, method string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error) {
	return t.origin.Proxy(ctx, bucket, key, method, query, body, size, extra...)
}

// SpoolStream spools an unknown-length request body into memory (<=8 MiB) or a temp file.
func (t *Tier) SpoolStream(ctx context.Context, body io.Reader, expected int64) (io.ReadSeeker, int64, func(), error) {
	sp, clean, err := t.runtime.spoolStream(ctx, body, expected)
	if err != nil {
		return nil, 0, nil, err
	}
	return sp.r, sp.size, clean, nil
}

// Invalidate removes a cached object and its origin metadata from PostgreSQL.
func (t *Tier) Invalidate(ctx context.Context, bucket, key string) error {
	return t.InvalidateMany(ctx, bucket, []string{key})
}

// InvalidateMany removes multiple cached objects and their origin metadata from PostgreSQL.
func (t *Tier) InvalidateMany(ctx context.Context, bucket string, keys []string) error {
	if t.readOnly || len(keys) == 0 {
		return nil
	}
	cks := make([]string, len(keys))
	for i, k := range keys {
		cks[i] = t.key(bucket, k)
	}
	c := t.store.Cache(t.namespace)
	if _, err := c.Del(ctx, cks); err != nil {
		return err
	}
	for _, ck := range cks {
		_ = c.ForgetOriginMetadata(ctx, ck, "")
	}
	return nil
}

type spooled struct {
	r    io.ReadSeeker
	data []byte
	size int64
}

type spoolBudget struct {
	sem      *syncx.Weighted
	limit    int64
	dir      string
	used     atomic.Int64
	peak     atomic.Int64
	rejected atomic.Uint64
}

func newSpoolBudget(limit int64, dir string) *spoolBudget {
	return &spoolBudget{sem: syncx.NewWeighted(limit), limit: limit, dir: dir}
}

func (r *Runtime) WriteMetrics(w io.Writer) {
	fmt.Fprintf(w, "pgvs3_spool_bytes %d\npgvs3_spool_peak_bytes %d\npgvs3_spool_limit_bytes %d\npgvs3_spool_rejections_total %d\n",
		r.spool.used.Load(), r.spool.peak.Load(), r.spool.limit, r.spool.rejected.Load())
}

func (r *Runtime) spoolStream(ctx context.Context, body io.Reader, expected int64) (spooled, func(), error) {
	return spoolWithBudget(ctx, body, expected, r.spool)
}

func spoolWithBudget(ctx context.Context, r io.Reader, expected int64, budget *spoolBudget) (spooled, func(), error) {
	if err := ctx.Err(); err != nil {
		return spooled{}, nil, err
	}
	var bounded *io.LimitedReader
	if expected >= 0 {
		bounded = &io.LimitedReader{R: r, N: expected + 1}
		r = bounded
	}
	head, err := io.ReadAll(io.LimitReader(r, memFillMax+1))
	if err != nil {
		return spooled{}, nil, err
	}
	if len(head) <= memFillMax {
		if expected >= 0 && int64(len(head)) != expected {
			return spooled{}, nil, fmt.Errorf("spooled %d bytes, expected %d", len(head), expected)
		}
		return spooled{r: bytes.NewReader(head), data: head, size: int64(len(head))}, func() {}, nil
	}
	var reserved int64
	reserve := func(want int64) error {
		if want <= reserved {
			return nil
		}
		if !budget.sem.TryAcquire(want - reserved) {
			budget.rejected.Add(1)
			return ErrSpoolBusy
		}
		used := budget.used.Add(want - reserved)
		for peak := budget.peak.Load(); used > peak; peak = budget.peak.Load() {
			if budget.peak.CompareAndSwap(peak, used) {
				break
			}
		}
		reserved = want
		return nil
	}
	if err := reserve(max(expected, int64(len(head)))); err != nil {
		return spooled{}, nil, err
	}
	tmp, err := os.CreateTemp(budget.dir, "pgvs3-fill-*")
	if err != nil {
		budget.used.Add(-reserved)
		budget.sem.Release(reserved)
		return spooled{}, nil, err
	}
	var once sync.Once
	clean := func() {
		once.Do(func() {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			budget.used.Add(-reserved)
			budget.sem.Release(reserved)
		})
	}
	if _, err := tmp.Write(head); err != nil {
		clean()
		return spooled{}, nil, err
	}
	var n int64
	if bounded != nil {
		bounded.N = expected - int64(len(head))
		n, err = io.Copy(tmp, bounded)
		if err == nil {
			bounded.N = 1
			var probe [1]byte
			extra, readErr := bounded.Read(probe[:])
			if extra != 0 {
				err = errors.New("spooled body exceeds declared size")
			} else if readErr != nil && readErr != io.EOF {
				err = readErr
			}
		}
	} else {
		w := &spoolWriter{ctx: ctx, file: tmp, size: int64(len(head)), reserve: reserve}
		n, err = io.Copy(w, r)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && expected >= 0 && int64(len(head))+n != expected {
		err = fmt.Errorf("spooled %d bytes, expected %d", int64(len(head))+n, expected)
	}
	if err == nil {
		_, err = tmp.Seek(0, io.SeekStart)
	}
	if err != nil {
		clean()
		return spooled{}, nil, err
	}
	return spooled{r: tmp, size: int64(len(head)) + n}, clean, nil
}

type spoolWriter struct {
	ctx     context.Context
	file    *os.File
	size    int64
	reserve func(int64) error
}

func (w *spoolWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	want := w.size + int64(len(p))
	if err := w.reserve(want); err != nil {
		return 0, err
	}
	n, err := w.file.Write(p)
	w.size += int64(n)
	return n, err
}

func (t *Tier) storeSpooled(ctx context.Context, ref store.ObjectRef, sp spooled, opts store.WriteOptions, locked *store.LockedObject) (*store.Meta, error) {
	if locked != nil && locked.Ref() != ref {
		return nil, store.ErrInvalidPublication
	}
	exp := time.Now().Add(t.retention)
	opts.ExpiresAt = &exp
	opts.ExpectedSize = sp.size
	if sp.data != nil && int64(len(sp.data)) <= int64(ref.NS.Kind.InlineMax()) {
		if locked != nil {
			return locked.PutInline(ctx, sp.data, opts)
		}
		return t.store.PutInline(ctx, ref, sp.data, opts)
	}
	if _, err := sp.r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var w *store.ChunkWriter
	var err error
	if locked != nil {
		w, err = locked.StartObject(ctx, opts)
	} else {
		w, err = t.store.StartObject(ctx, ref, opts)
	}
	if err != nil {
		return nil, err
	}
	defer w.Abort()
	if _, err := io.Copy(w, sp.r); err != nil {
		return nil, err
	}
	pub, err := w.Finish(nil)
	if err != nil {
		return nil, err
	}
	m := &store.Meta{
		Size: pub.Size, ETag: pub.ETag, UserMetadata: opts.UserMetadata,
		ContentType: opts.ContentType, ContentEncoding: opts.ContentEncoding,
		CreatedAt: time.Now(), OriginModified: opts.OriginModified, OriginETag: opts.Publication.OriginVersion(),
		FileID: pub.FileID, SHA256: pub.SHA256, ExpiresAt: opts.ExpiresAt,
	}
	if opts.Checksum != nil {
		m.ChecksumName, m.ChecksumValue, m.ChecksumType = opts.Checksum.Name, opts.Checksum.Value, opts.ChecksumType
	}
	return m, nil
}

func contentTypeFor(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}
