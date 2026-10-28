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
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sync/singleflight"
)

type Tier struct {
	origin         originHandle
	store          *store.Store
	namespace      string
	ttl            time.Duration
	retention      time.Duration
	fills          chan struct{}
	completes      chan struct{}
	group          singleflight.Group
	log            *slog.Logger
	readOnly       bool
	writeBack      bool
	workerID       string
	keyLocks       [64]sync.Mutex
	rangeSize      int64
	pendingWorkers int
	pendingWake    chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	done           chan struct{}
	workerStarted  bool
	closeOnce      sync.Once
	fillTTL        time.Duration
}

const (
	memFillMax         = 8 << 20
	fullCacheMax       = 8 << 20
	maxRangeCacheSpan  = 8 << 20
	writeBackMin       = 8 << 20
	originTombstoneTTL = 30 * time.Minute
	fillLeaseTTL       = 2 * time.Minute
	fillLeaseWait      = 45 * time.Second
)

var (
	processFills     = make(chan struct{}, 64)
	processPending   = make(chan struct{}, 64)
	spoolDirectory   string
	writeBackEnabled bool

	errPendingOrigin = errors.New("cache object is pending origin upload")
	errFillBusy      = errors.New("origin fill owned by another worker")
	ErrReadOnly      = errors.New("cache tier is read-only")
)

func ConfigureLimits(fills, pending int) {
	if fills <= 0 {
		fills = 64
	}
	if pending <= 0 {
		pending = fills
	}
	processFills = make(chan struct{}, fills)
	processPending = make(chan struct{}, pending)
}

func ConfigureSpoolDir(dir string) { spoolDirectory = dir }

func ConfigureWriteBack(enabled bool) { writeBackEnabled = enabled }

func acquireProcess(sem chan struct{}, ctx context.Context) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func New(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration, maxFills int) (*Tier, error) {
	return newTier(log, config, st, namespace, ttl, retention, maxFills, false)
}

func NewReadOnly(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration, maxFills int) (*Tier, error) {
	return newTier(log, config, st, namespace, ttl, retention, maxFills, true)
}

func newTier(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration, maxFills int, readOnly bool) (*Tier, error) {
	if log == nil {
		log = slog.Default()
	}
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
		origin: b, store: st, namespace: namespace, ttl: ttl, retention: retention,
		fills: make(chan struct{}, maxFills), completes: make(chan struct{}, maxFills),
		log: log, readOnly: readOnly, writeBack: writeBackEnabled,
		workerID: randomID(), pendingWorkers: min(maxFills, 8),
		pendingWake: make(chan struct{}, 1),
		ctx:         ctx, cancel: cancel, done: make(chan struct{}),
	}
	if !readOnly {

		for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
			if err := st.EnsureNamespace(ctx, kind, namespace, nil, nil); err != nil {
				cancel()
				_ = b.Close()
				return nil, fmt.Errorf("ensure %s namespace: %w", kind, err)
			}
		}
		t.workerStarted = true
		go t.runPending(ctx)
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

func (t *Tier) Capabilities() string { return t.origin.Capabilities().String() }

func (t *Tier) EnableRangeCache(chunk int64) {
	if chunk > 0 {
		chunk = min(max(chunk, 64<<10), 8<<20)
	}
	t.rangeSize = max(chunk, 0)
}

func (t *Tier) EnableWriteBack(enabled bool) { t.writeBack = enabled }

func (t *Tier) wakePending() {
	select {
	case t.pendingWake <- struct{}{}:
	default:
	}
}

func (t *Tier) Close() error {
	t.closeOnce.Do(func() {
		if t.cancel != nil {
			t.cancel()
		}
		if t.workerStarted {
			<-t.done
		}
	})
	return t.origin.Close()
}

func (t *Tier) baseContext() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return context.Background()
}

func (t *Tier) key(bucket, key string) string { return bucket + "/" + key }

func (t *Tier) durableRetention() time.Duration { return max(t.retention, originTombstoneTTL) }

func (t *Tier) cacheRef(ck string) store.ObjectRef {
	return store.ObjectRef{NS: ns.CacheRef(t.namespace), Key: ck}
}

func (t *Tier) writeRef(ck string) store.ObjectRef {
	return store.ObjectRef{NS: ns.WriteBackRef(t.namespace), Key: ck}
}

func (t *Tier) lockKey(ck string) func() {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	for i := 0; i < len(ck); i++ {
		h = (h ^ uint32(ck[i])) * prime
	}
	mu := &t.keyLocks[h%uint32(len(t.keyLocks))]
	mu.Lock()
	return mu.Unlock
}

type cacheAction uint8

const (
	cacheActionZero cacheAction = iota
	cacheActionPassThrough
	cacheActionRange
	cacheActionCached
)

type cachePlan struct {
	action cacheAction
	length int64
}

func planCacheRead(res cacheResolution, rangeSize int64, readOnly bool, v readView) cachePlan {
	size := v.Size
	var plan cachePlan
	if size == 0 {
		plan.action = cacheActionZero
		return plan
	}
	plan.length = v.end - v.start + 1
	switch {
	case readOnly && !res.valid && !res.attrsValid:
		plan.action = cacheActionPassThrough
	case rangeSize > 0 && !res.valid && size > fullCacheMax &&
		plan.length < size && plan.length <= maxRangeCacheSpan:
		plan.action = cacheActionRange
	case res.valid || plan.length == size || size <= fullCacheMax:
		plan.action = cacheActionCached
	default:
		plan.action = cacheActionPassThrough
	}
	return plan
}

type Object = store.ReadResult

type readView struct {
	store.ObjectInfo
	start, end int64
	ranged     bool
	hit        bool
}

func newObject(v readView, body io.ReadCloser) *Object {
	return &Object{ObjectInfo: v.ObjectInfo, Body: body, Start: v.start, End: v.end, Ranged: v.ranged, Hit: v.hit}
}

type cacheResolution struct {
	meta       *store.Meta
	ref        store.ObjectRef
	state      store.CacheState
	attrs      originAttrs
	info       store.ObjectInfo
	valid      bool
	attrsValid bool
}

func (t *Tier) resolveCache(ctx context.Context, ck string, deleteStale, withInline, skipAttributesOnMiss bool) (cacheResolution, error) {
	res := cacheResolution{ref: t.cacheRef(ck), state: store.CacheLive}
	m, ref, err := t.lookupCached(ctx, ck, withInline)
	if err != nil {
		return res, err
	}
	if m != nil {
		res.meta, res.ref = m, ref
		res.state = store.ParseCacheVersion(m.OriginETag).State
		if res.state == store.CacheDeleted {
			return res, store.ErrNotFound
		}
		if res.state == store.CachePending || (t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl) {
			res.info = cachedInfo(res.ref, m)
			res.valid = true
			return res, nil
		}
	} else if skipAttributesOnMiss {
		return res, nil
	}

	if m == nil && t.rangeSize > 0 && t.ttl > 0 {
		info, err := t.store.Cache(t.namespace).OriginMetadata(ctx, ck, t.ttl)
		if err != nil {
			return res, err
		}
		if info != nil {
			res.info = *info
			res.attrs = originAttrs{Size: info.Size, ETag: info.OriginETag,
				LastModified: info.LastModified(), ContentType: info.ContentType,
				ContentEncoding: info.ContentEncoding, UserMetadata: info.UserMetadata}
			res.attrsValid = true
			return res, nil
		}
	}
	validated := time.Now()
	a, aerr := t.origin.Attributes(ctx, ck)
	if aerr != nil {
		if t.origin.IsObjNotFoundErr(aerr) {
			if m != nil && res.ref.NS.Kind == ns.WriteBack && !t.readOnly {
				_, _ = t.store.RetireOriginObject(ctx, res.ref, m)
			}
			return res, store.ErrNotFound
		}
		return res, aerr
	}
	res.attrs, res.info = a, a.objectInfo()
	if m == nil && t.rangeSize > 0 && t.ttl > 0 && a.Size > fullCacheMax && !t.readOnly {
		if err := t.store.Cache(t.namespace).RememberOriginMetadata(ctx, ck, res.info, validated, min(t.ttl, t.retention)); err != nil {
			return res, err
		}
	}
	if m != nil {
		if m.Size == res.info.Size && m.OriginETag == res.info.OriginETag {
			t.touch(res.ref.NS.Kind, ck)
			res.info = cachedInfo(res.ref, m)
			res.valid = true
		} else {
			if !t.readOnly && (deleteStale || res.ref.NS.Kind == ns.WriteBack) {
				if res.ref.NS.Kind == ns.WriteBack {
					if _, err := t.store.RetireOriginObject(ctx, res.ref, m); err != nil {
						return res, err
					}
				} else {
					_, _ = t.store.DeleteExpected(context.WithoutCancel(ctx), res.ref, m.OriginETag)
				}
			}
			if res.ref.NS.Kind == ns.WriteBack {
				res.meta = nil
			}
		}
	}
	return res, nil
}

func cachedInfo(ref store.ObjectRef, m *store.Meta) store.ObjectInfo {
	info := m.ObjectInfo
	info.ETag, info.OriginETag = responseETag(m), originETag(m)
	if ref.NS.Kind == ns.WriteBack {
		info.ETag = m.ETag
	}
	return info
}

func (t *Tier) Head(ctx context.Context, bucket, key string) (*Object, error) {
	ck := t.key(bucket, key)
	res, err := t.resolveCache(ctx, ck, false, false, false)
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
	ck := t.key(bucket, key)

	skipAttributes := !rng.Requested
	res, err := t.resolveCache(ctx, ck, true, true, skipAttributes)
	if err != nil {
		return nil, err
	}
	if skipAttributes && res.meta == nil {
		if t.readOnly {
			return t.passThroughWhole(ctx, ck, key, rng)
		}
		m, fillErr := t.fill(ctx, ck, key)
		if errors.Is(fillErr, errPendingOrigin) || errors.Is(fillErr, errFillBusy) {
			return t.openPending(ctx, ck, key, rng)
		}
		if errors.Is(fillErr, store.ErrStaleCache) {
			return t.openPending(ctx, ck, key, rng)
		}
		if fillErr != nil {
			return nil, fillErr
		}
		readRef := t.cacheRef(ck)
		if wm, werr := t.store.Meta(ctx, t.writeRef(ck)); werr == nil {
			if store.ParseCacheVersion(wm.OriginETag).IsDeleted() {
				return nil, store.ErrNotFound
			}
			m, readRef = wm, t.writeRef(ck)
		}
		span, spanErr := rng.Resolve(m.Size)
		if spanErr != nil {
			return nil, spanErr
		}
		return t.openCached(ctx, readRef, m, readView{start: span.Start, end: span.End, ranged: span.Ranged})
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
		return t.openZero(ctx, ck, key, res, rng)
	case cacheActionPassThrough:
		return t.passThrough(ctx, ck, view, res.attrs)
	case cacheActionRange:
		view.ETag, view.ContentEncoding, view.UserMetadata = version, res.info.ContentEncoding, res.attrs.UserMetadata
		obj, err := t.openRange(ctx, ck, key, view)
		if errors.Is(err, store.ErrStaleCache) || errors.Is(err, errFillBusy) {
			return t.openPending(ctx, ck, key, rng)
		}
		return obj, err
	default:
		readMeta, readRef := res.meta, res.ref
		if !res.valid && t.readOnly {
			return t.passThrough(ctx, ck, view, res.attrs)
		}
		if !res.valid {
			fm, err := t.fill(ctx, ck, key)
			if errors.Is(err, errPendingOrigin) || errors.Is(err, errFillBusy) {
				return t.passThrough(ctx, ck, view, res.attrs)
			} else if errors.Is(err, store.ErrStaleCache) {
				return t.openPending(ctx, ck, key, rng)
			} else if err != nil {
				return nil, err
			}
			if fm.Size != size {
				return t.openPending(ctx, ck, key, rng)
			}
			readMeta, readRef = fm, t.cacheRef(ck)
			if wm, werr := t.store.Meta(ctx, t.writeRef(ck)); werr == nil {
				if store.ParseCacheVersion(wm.OriginETag).IsDeleted() {
					return nil, store.ErrNotFound
				}
				readMeta, readRef = wm, t.writeRef(ck)
			}
		}
		view.hit = res.valid
		return t.openCached(ctx, readRef, readMeta, view)
	}
}

func (t *Tier) openZero(ctx context.Context, ck, key string, res cacheResolution, rng store.ByteRange) (*Object, error) {
	info := res.info
	if info.ContentType == "" {
		info.ContentType = contentTypeFor(key)
	}
	originZero := func() *Object {
		info.Size = 0
		return newObject(readView{ObjectInfo: info, ranged: rng.Requested}, io.NopCloser(bytes.NewReader(nil)))
	}
	if t.readOnly && !res.valid {
		return originZero(), nil
	}
	if !res.valid {
		m, err := t.fill(ctx, ck, key)
		if errors.Is(err, errPendingOrigin) || errors.Is(err, errFillBusy) {
			return originZero(), nil
		}
		if errors.Is(err, store.ErrStaleCache) {
			return t.openPending(ctx, ck, key, rng)
		}
		if err != nil {
			return nil, err
		}
		if m.Size != 0 {

			return t.openPending(ctx, ck, key, rng)
		}
		info = m.ObjectInfo
		info.ETag, info.OriginETag = responseETag(m), originETag(m)
	}
	return newObject(readView{ObjectInfo: info, hit: res.valid}, io.NopCloser(bytes.NewReader(nil))), nil
}

func (t *Tier) openCached(ctx context.Context, ref store.ObjectRef, m *store.Meta, v readView) (*Object, error) {
	start, end := v.start, v.end
	sm, body, err := t.store.GetBodyFromMeta(ctx, ref, m, store.Range(start, end, -1))
	if err != nil {
		return nil, err
	}
	info := store.ObjectInfo{
		Size: sm.Size, ETag: responseETagSliceFor(ref, sm), ContentType: sm.ContentType,
		ContentEncoding: sm.ContentEncoding, ChecksumName: sm.ChecksumName,
		ChecksumValue: sm.ChecksumValue, ChecksumType: sm.ChecksumType,
		UserMetadata: sm.UserMetadata, CreatedAt: sm.CreatedAt, OriginModified: sm.OriginModified,
	}
	return newObject(readView{ObjectInfo: info, start: sm.Start, end: sm.End, ranged: v.ranged, hit: v.hit}, body), nil
}

func (t *Tier) openPending(ctx context.Context, ck, key string, rng store.ByteRange) (*Object, error) {
	a, err := t.origin.Attributes(ctx, ck)
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
	span, err := rng.Resolve(size)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		info := store.ObjectInfo{
			Size: 0, ETag: etagForAttrs(a), ContentType: ct,
			ContentEncoding: a.ContentEncoding,
			UserMetadata:    a.UserMetadata, CreatedAt: a.LastModified,
		}
		return newObject(readView{ObjectInfo: info, ranged: rng.Requested}, io.NopCloser(bytes.NewReader(nil))), nil
	}
	view := readView{Size: size, ContentType: ct, CreatedAt: a.LastModified, start: span.Start, end: span.End, ranged: span.Ranged}
	return t.passThrough(ctx, ck, view, a)
}

func (t *Tier) passThroughWhole(ctx context.Context, ck, key string, rng store.ByteRange) (*Object, error) {
	rc, attrs, err := t.origin.GetWithAttrs(ctx, ck)
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	ct := attrs.ContentType
	if ct == "" {
		ct = contentTypeFor(key)
	}
	span, err := rng.Resolve(attrs.Size)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	info := store.ObjectInfo{
		Size: attrs.Size, ETag: etagForAttrs(attrs), ContentType: ct,
		ContentEncoding: attrs.ContentEncoding,
		UserMetadata:    attrs.UserMetadata, CreatedAt: attrs.LastModified,
	}
	return newObject(readView{ObjectInfo: info, start: span.Start, end: span.End, ranged: span.Ranged}, rc), nil
}

func (t *Tier) passThrough(ctx context.Context, ck string, v readView, a originAttrs) (*Object, error) {
	rc, err := t.origin.GetRange(ctx, ck, v.start, v.end-v.start+1, "")
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	v.ETag, v.ContentEncoding, v.UserMetadata = etagForAttrs(a), a.ContentEncoding, a.UserMetadata
	return newObject(v, rc), nil
}

func rangeChunkKey(ck string, offset int64, version string) string {
	return fmt.Sprintf("%s.__range__.%016x.%s", ck, offset, version)
}

func (t *Tier) openRange(ctx context.Context, ck, key string, v readView) (*Object, error) {
	start, end := v.start, v.end
	length := end - start + 1
	body := make([]byte, 0, length)
	hit := true
	first := start - start%t.rangeSize
	last := first + ((end-first)/t.rangeSize)*t.rangeSize
	for off := first; off <= end; off += t.rangeSize {
		chunk, cached, err := t.rangeChunk(ctx, ck, key, v, off)
		if err != nil {
			return nil, err
		}
		hit = hit && cached
		from := int64(0)
		if off == first && start > off {
			from = start - off
		}
		to := int64(len(chunk))
		if off == last && end < off+to-1 {
			to = end - off + 1
		}
		if from < to {
			body = append(body, chunk[from:to]...)
		}
	}
	if int64(len(body)) != length {
		return nil, fmt.Errorf("range cache assembled %d bytes, wanted %d", len(body), length)
	}
	v.hit = hit
	return newObject(v, io.NopCloser(bytes.NewReader(body))), nil
}

func (t *Tier) rangeChunk(ctx context.Context, ck, key string, v readView, off int64) ([]byte, bool, error) {
	length := t.rangeSize
	if off+length > v.Size {
		length = v.Size - off
	}
	version, ct, lastModified := v.ETag, v.ContentType, v.CreatedAt
	chunkKey := rangeChunkKey(ck, off, version)
	if m, err := t.store.MetaHeader(ctx, t.cacheRef(chunkKey)); err == nil && m.OriginETag == version && m.Size == length {
		_, body, err := t.store.GetBody(ctx, t.cacheRef(chunkKey), store.WholeObjectRange(), nil)
		if err == nil {
			b, rerr := io.ReadAll(body)
			_ = body.Close()
			if rerr == nil && int64(len(b)) == length {
				return b, true, nil
			}
		}
	}
	if t.readOnly {
		return nil, false, errFillBusy
	}

	m, err := t.coalesceFill(ctx, chunkKey, 2*time.Minute, func(fillCtx context.Context) (*store.Meta, error) {
		rc, err := t.origin.GetRange(fillCtx, ck, off, length, version)
		if err != nil {
			if errors.Is(err, errOriginPrecondition) {
				_ = t.store.Cache(t.namespace).ForgetOriginMetadata(fillCtx, ck, version)
				return nil, store.ErrStaleCache
			}
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := spoolStream(fillCtx, io.LimitReader(rc, length), length)
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		defer cleanup()
		if sp.size != length {
			return nil, fmt.Errorf("origin range %s at %d returned %d bytes, wanted %d", key, off, sp.size, length)
		}
		return t.storeSpooled(fillCtx, t.cacheRef(chunkKey), sp, store.WriteOptions{
			ContentType: ct, OriginModified: lastModified,
			OriginETag: version, ExpectedOrigin: version,
		})
	})
	if err != nil {
		return nil, false, err
	}
	if m.OriginETag != version || m.Size != length {
		return nil, false, errFillBusy
	}
	_, body, err := t.store.GetBody(ctx, t.cacheRef(chunkKey), store.WholeObjectRange(), nil)
	if err != nil {
		return nil, false, err
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, false, err
	}
	if int64(len(b)) != length {
		return nil, false, fmt.Errorf("cached range chunk %s has %d bytes, wanted %d", chunkKey, len(b), length)
	}
	return b, false, nil
}

func (t *Tier) lookupCached(ctx context.Context, ck string, withInline bool) (*store.Meta, store.ObjectRef, error) {
	m, ref, err := t.store.Cache(t.namespace).Metadata(ctx, ck, withInline)
	if errors.Is(err, store.ErrNotFound) {
		return nil, store.ObjectRef{}, nil
	}
	return m, store.ObjectRef{NS: ref, Key: ck}, err
}

func (t *Tier) currentMeta(ctx context.Context, ck string) (*store.Meta, error) {
	m, _, err := t.lookupCached(ctx, ck, true)
	if m != nil && store.ParseCacheVersion(m.OriginETag).IsDeleted() {
		m = nil
	}
	return m, err
}

func (t *Tier) List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	if limit <= 0 {
		return nil, nil
	}
	originRows, originErr := t.listDirect(ctx, bucket, prefix, after, limit)
	writeRows, writeErr := t.listWriteBack(ctx, bucket, prefix, after, limit)
	if writeErr != nil {
		return nil, writeErr
	}
	if originErr != nil {
		if !errors.Is(originErr, ErrOriginListingUnsupported) || len(writeRows) == 0 {
			return nil, originErr
		}
	}
	return mergeListed(originRows, writeRows, limit), nil
}

func (t *Tier) listWriteBack(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	if t.store == nil {
		return nil, nil
	}
	pfx := bucket + "/" + prefix
	afterKey := bucket + "/" + after
	rows, err := t.store.List(ctx, ns.WriteBackRef(t.namespace), pfx, pfx+"\U0010FFFF", afterKey, limit)
	if err != nil {
		return nil, err
	}
	out := make([]store.Listed, 0, len(rows))
	for _, row := range rows {
		if store.IsDeletedOrigin(row.OriginETag) {
			continue
		}
		row.Key = strings.TrimPrefix(row.Key, bucket+"/")
		if row.Key == "" || !strings.HasPrefix(row.Key, prefix) {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

func mergeListed(originRows, pendingRows []store.Listed, limit int) []store.Listed {
	out := make([]store.Listed, 0, min(limit, len(originRows)+len(pendingRows)))
	i, j := 0, 0
	for len(out) < limit && (i < len(originRows) || j < len(pendingRows)) {
		switch {
		case i >= len(originRows):
			out = append(out, pendingRows[j])
			j++
		case j >= len(pendingRows):
			out = append(out, originRows[i])
			i++
		case originRows[i].Key < pendingRows[j].Key:
			out = append(out, originRows[i])
			i++
		case originRows[i].Key > pendingRows[j].Key:
			out = append(out, pendingRows[j])
			j++
		default:

			out = append(out, pendingRows[j])
			i++
			j++
		}
	}
	return out
}

func (t *Tier) listDirect(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	if limit <= 0 {
		return nil, nil
	}
	if !t.origin.Supports(capList) {
		return nil, ErrOriginListingUnsupported
	}
	pfx := bucket + "/" + prefix
	dir := bucket + "/"
	if i := strings.LastIndexByte(pfx, '/'); i >= 0 {
		dir = pfx[:i+1]
	}
	out := make([]store.Listed, 0, limit)
	for obj, err := range t.origin.Iter(ctx, dir) {
		if err != nil {
			return nil, err
		}
		name := obj.Key
		if strings.HasSuffix(name, "/") || !strings.HasPrefix(name, pfx) {
			continue
		}
		rel := strings.TrimPrefix(name, bucket+"/")
		if rel <= after {
			continue
		}
		out = append(out, store.Listed{
			Key: rel, Size: obj.Attrs.Size, ETag: etagForAttrs(obj.Attrs), CreatedAt: obj.Attrs.LastModified,
		})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func orValue(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func versionFor(a originAttrs) string {
	return orValue(a.ETag, fmt.Sprintf("%d-%d", a.Size, a.LastModified.UnixNano()))
}

func etagForAttrs(a originAttrs) string { return versionFor(a) }

func originETag(m *store.Meta) string { return orValue(m.OriginETag, m.ETag) }

func originETagSlice(m *store.SliceMeta) string { return orValue(m.OriginETag, m.ETag) }

func responseETag(m *store.Meta) string {
	if store.ParseCacheVersion(m.OriginETag).IsPending() {
		return m.ETag
	}
	return originETag(m)
}

func responseETagSlice(m *store.SliceMeta) string {
	if store.ParseCacheVersion(m.OriginETag).IsPending() {
		return m.ETag
	}
	return originETagSlice(m)
}

func responseETagSliceFor(ref store.ObjectRef, m *store.SliceMeta) string {
	if ref.NS.Kind == ns.WriteBack {
		return m.ETag
	}
	return responseETagSlice(m)
}

func (t *Tier) touch(kind ns.Kind, ck string) {
	if t.readOnly {
		return
	}
	retention := t.retention
	if kind == ns.WriteBack {
		retention = t.durableRetention()
	}
	_ = t.store.TouchNamespaceObject(context.Background(), kind, t.namespace, ck, retention)
}

func contentTypeFor(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

type fillLeaseContextKey struct{}

func (t *Tier) coalesceFill(ctx context.Context, ck string, timeout time.Duration, fetch func(context.Context) (*store.Meta, error)) (*store.Meta, error) {
	ch := t.group.DoChan("fill:"+ck, func() (any, error) {
		unlock := t.lockKey(ck)
		defer unlock()
		fillCtx, cancel := context.WithTimeout(t.baseContext(), timeout)
		defer cancel()
		if !acquireProcess(processFills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-processFills }()
		if !acquireProcess(t.fills, fillCtx) {
			return nil, fillCtx.Err()
		}
		defer func() { <-t.fills }()
		wait := min(timeout-time.Second, max(fillLeaseWait, timeout/2))
		lease, existing, err := t.acquireFill(fillCtx, ck, wait)
		if err != nil || existing != nil {
			return existing, err
		}
		leaseCtx, stop := t.renewFill(fillCtx, *lease)
		defer stop()
		m, err := fetch(leaseCtx)
		if errors.Is(context.Cause(leaseCtx), store.ErrStaleCache) {
			return nil, store.ErrStaleCache
		}
		return m, err
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
	probe := func() (*store.Meta, error) {
		m, _, err := t.lookupCached(ctx, ck, true)
		if err == nil && m != nil {
			switch store.ParseCacheVersion(m.OriginETag).State {
			case store.CacheDeleted:
				return nil, store.ErrNotFound
			case store.CachePending:
				return nil, errPendingOrigin
			}
		}
		return m, err
	}
	for {
		if m, err := probe(); err != nil || m != nil {
			return nil, m, err
		}
		owner := t.workerID + "-" + randomID()
		acquired, err := t.store.Cache(t.namespace).AcquireFillLease(ctx, ck, owner, t.leaseTTL())
		if err != nil {
			return nil, nil, err
		}
		if acquired {

			if m, err := probe(); err != nil || m != nil {
				t.store.Cache(t.namespace).ReleaseFillLease(context.WithoutCancel(ctx), ck, owner)
				return nil, m, err
			}
			return &store.FillLease{Namespace: t.namespace, Key: ck, Owner: owner}, nil, nil
		}
		pause := min(100*time.Millisecond, time.Until(deadline))
		if pause <= 0 {
			return nil, nil, errFillBusy
		}
		if !sleepContext(ctx, pause) {
			return nil, nil, ctx.Err()
		}
	}
}

func (t *Tier) leaseTTL() time.Duration {
	if t.fillTTL > 0 {
		return t.fillTTL
	}
	return fillLeaseTTL
}

func (t *Tier) renewFill(ctx context.Context, lease store.FillLease) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, fillLeaseContextKey{}, &lease)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(t.leaseTTL() / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stop := context.WithTimeout(ctx, t.leaseTTL()/3)
				owned, err := t.store.RenewFillLease(renewCtx, lease, t.leaseTTL())
				stop()
				if err != nil || !owned {
					cancel(store.ErrStaleCache)
					return
				}
			}
		}
	}()
	return ctx, func() {
		cancel(nil)
		<-done
		releaseCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		t.store.Cache(lease.Namespace).ReleaseFillLease(releaseCtx, lease.Key, lease.Owner)
	}
}

func (t *Tier) fill(ctx context.Context, ck, key string) (*store.Meta, error) {
	return t.coalesceFill(ctx, ck, 10*time.Minute, func(fillCtx context.Context) (*store.Meta, error) {
		rc, attrs, err := t.origin.GetWithAttrs(fillCtx, ck)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := spoolStream(fillCtx, rc, attrs.Size)
		_ = rc.Close()
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		defer cleanup()
		if sp.size != attrs.Size {
			return nil, fmt.Errorf("origin GET %s returned %d bytes, wanted %d", key, sp.size, attrs.Size)
		}
		opts := attrs.writeOptions()
		opts.ExpectedOrigin = opts.OriginETag
		m, err := t.storeSpooled(fillCtx, t.cacheRef(ck), sp, opts)
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

type spooled struct {
	r    io.ReadSeeker
	data []byte
	size int64
}

var ErrSpoolBusy = errors.New("cache spool byte budget exhausted")

type spoolBudget struct {
	sem      *semaphore.Weighted
	limit    int64
	used     atomic.Int64
	rejected atomic.Uint64
}

func newSpoolBudget(limit int64) *spoolBudget {
	return &spoolBudget{sem: semaphore.NewWeighted(limit), limit: limit}
}

var processSpool = newSpoolBudget(8 << 30)

func ConfigureSpoolBytes(limit int64) { processSpool = newSpoolBudget(limit) }

func WriteSpoolMetrics(w io.Writer) {
	fmt.Fprintf(w, "pgvs3_spool_bytes %d\npgvs3_spool_limit_bytes %d\npgvs3_spool_rejections_total %d\n",
		processSpool.used.Load(), processSpool.limit, processSpool.rejected.Load())
}

func spoolStream(ctx context.Context, r io.Reader, expected int64) (spooled, func(), error) {
	return spoolWithBudget(ctx, r, expected, processSpool)
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
		budget.used.Add(want - reserved)
		reserved = want
		return nil
	}
	if err := reserve(max(expected, int64(len(head)))); err != nil {
		return spooled{}, nil, err
	}
	tmp, err := os.CreateTemp(spoolDirectory, "pgvs3-fill-*")
	if err != nil {
		budget.sem.Release(reserved)
		budget.used.Add(-reserved)
		return spooled{}, nil, err
	}
	var once sync.Once
	clean := func() {
		once.Do(func() {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			budget.sem.Release(reserved)
			budget.used.Add(-reserved)
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

func (t *Tier) storeSpooled(ctx context.Context, ref store.ObjectRef, sp spooled, opts store.WriteOptions) (*store.Meta, error) {
	if lease, ok := ctx.Value(fillLeaseContextKey{}).(*store.FillLease); ok {
		opts.FillLease = lease
	}
	retention := t.retention
	if ref.NS.Kind == ns.WriteBack {
		retention = t.durableRetention()
		opts.SkipLock = true
	}
	exp := time.Now().Add(retention)
	opts.ExpiresAt = &exp
	if opts.PendingOrigin != nil {

		opts.ExpiresAt = nil
	}
	opts.ExpectedSize = sp.size
	if sp.data != nil && int64(len(sp.data)) <= int64(ref.NS.Kind.InlineMax()) {
		if opts.PendingOrigin != nil {
			return nil, errors.New("pending origin publication requires the chunked write path")
		}
		return t.store.PutInline(ctx, ref, sp.data, opts)
	}
	if _, err := sp.r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	w, err := t.store.StartObject(ctx, ref, opts)
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
		CreatedAt: time.Now(), OriginModified: opts.OriginModified, OriginETag: opts.OriginETag,
		FileID: pub.FileID, SHA256: pub.SHA256, ExpiresAt: opts.ExpiresAt,
	}
	if opts.Checksum != nil {
		m.ChecksumName, m.ChecksumValue, m.ChecksumType = opts.Checksum.Name, opts.Checksum.Value, opts.ChecksumType
	}
	return m, nil
}

func (t *Tier) requireWrite(operation string) error {
	if t.readOnly {
		return ErrReadOnly
	}
	if !t.origin.Supports(capWrite) {
		return fmt.Errorf("origin %s: %w", operation, ErrOriginUnsupported)
	}
	return nil
}

func (t *Tier) lockWrite(ctx context.Context, ck string, validate func() error) (func(), error) {
	unlock := t.lockKey(ck)
	var unlockDB func()
	defer func() {
		if unlockDB == nil {
			unlock()
		}
	}()
	if validate != nil {
		if err := validate(); err != nil {
			return nil, err
		}
	}
	unlockDB, err := t.store.LockCacheKey(ctx, ns.WriteBack, t.namespace, ck)
	if err != nil {
		return nil, err
	}
	return func() {
		defer unlock()
		unlockDB()
	}, nil
}

func (t *Tier) resolvePrecondition(ctx context.Context, ck string, p store.Precondition) (store.Precondition, error) {
	if p.Mode == store.Unconditional {
		return p, nil
	}
	cur, err := t.currentMeta(ctx, ck)
	if err != nil || cur == nil {
		return p, err
	}
	if p.Mode == store.IfAbsent {
		return p, store.ErrPrecondition
	}
	if p.Mode == store.IfMatch && cur.ETag == p.ETag {
		if store.ParseCacheVersion(cur.OriginETag).IsPending() || cur.OriginETag == "" {
			return p, ErrOriginConflict
		}
		p.ETag = cur.OriginETag
	}
	return p, nil
}

type UploadOptions struct {
	ExpectedSize    int64
	UserMetadata    []string
	ContentType     string
	ContentEncoding string
	Checksum        *store.Checksum
	ChecksumType    string
	Precondition    store.Precondition
	Validate        func() error
}

func (o UploadOptions) writeOptions() store.WriteOptions {
	return store.WriteOptions{
		UserMetadata: o.UserMetadata, ContentType: o.ContentType, ContentEncoding: o.ContentEncoding,
		Checksum: o.Checksum, ChecksumType: o.ChecksumType,
	}
}

func (t *Tier) Upload(ctx context.Context, bucket, key string, body io.Reader, userMeta []string, contentType string) (*store.Meta, error) {
	return t.UploadWithOptions(ctx, bucket, key, body, UploadOptions{UserMetadata: userMeta, ContentType: contentType})
}

func (t *Tier) UploadWithOptions(ctx context.Context, bucket, key string, body io.Reader, opts UploadOptions) (*store.Meta, error) {
	if err := t.requireWrite("upload"); err != nil {
		return nil, err
	}
	ck := t.key(bucket, key)
	if err := t.origin.checkKey(ck); err != nil {
		return nil, err
	}
	expected := opts.ExpectedSize
	if expected <= 0 {
		expected = -1
	}
	sp, cleanup, err := spoolStream(ctx, body, expected)
	if err != nil {
		switch {
		case errors.Is(err, errOriginPrecondition):
			return nil, store.ErrPrecondition
		case t.origin.IsObjNotFoundErr(err):
			return nil, store.ErrNotFound
		default:
			return nil, err
		}
	}
	defer cleanup()
	unlock, err := t.lockWrite(ctx, ck, opts.Validate)
	if err != nil {
		return nil, err
	}
	defer unlock()
	resolved, err := t.resolvePrecondition(ctx, ck, opts.Precondition)
	if err != nil {
		return nil, err
	}
	opts.Precondition = resolved
	writeOpts := opts.writeOptions()
	if t.writeBack && sp.size > writeBackMin && opts.Precondition.Mode == store.Unconditional {
		token := store.PendingOriginToken("write-" + randomID())
		writeOpts.OriginETag, writeOpts.PendingOrigin = token, &store.PendingOrigin{Token: token}
		m, err := t.storeSpooled(ctx, t.writeRef(ck), sp, writeOpts)
		if err == nil {
			_ = t.fenceReadCache(ctx, ck)
			t.wakePending()
		}
		return m, err
	}
	attrs, err := t.origin.Upload(ctx, ck, sp.r, sp.size, originUpload{
		UserMetadata: opts.UserMetadata, ContentType: opts.ContentType,
		ContentEncoding: opts.ContentEncoding, Checksum: opts.Checksum, Precondition: opts.Precondition,
	})
	if errors.Is(err, errOriginPrecondition) {
		return nil, store.ErrPrecondition
	}
	if err != nil {
		return nil, err
	}
	writeOpts.OriginETag, writeOpts.OriginModified = versionFor(attrs), attrs.LastModified
	m, cacheErr := t.storeSpooled(ctx, t.writeRef(ck), sp, writeOpts)

	_ = t.fenceReadCache(ctx, ck)
	_ = t.store.Cache(t.namespace).ClearPendingUploads(context.WithoutCancel(ctx), ck)
	if cacheErr != nil {
		t.log.Warn("populating durable origin write cache", "bucket", bucket, "key", key, "err", cacheErr)
		return nil, fmt.Errorf("populate durable origin write cache: %w", cacheErr)
	}
	return m, nil
}

func (t *Tier) fenceReadCache(ctx context.Context, ck string) error {
	return t.store.CacheTombstone(context.WithoutCancel(ctx), ns.CacheRef(t.namespace), ck,
		time.Now().Add(originTombstoneTTL))
}

func (t *Tier) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, opts store.CopyOptions) (*store.Meta, error) {
	if err := t.requireWrite("copy"); err != nil {
		return nil, err
	}
	if err := t.origin.checkKey(t.key(dstBucket, dstKey)); err != nil {
		return nil, err
	}
	srcObj, err := t.Open(ctx, srcBucket, srcKey, store.WholeObjectRange())
	if err != nil {
		return nil, err
	}
	defer srcObj.Body.Close()
	if !opts.Source.Allows(true, srcObj.ETag) {
		return nil, store.ErrPrecondition
	}
	meta, ct, encoding := srcObj.UserMetadata, srcObj.ContentType, srcObj.ContentEncoding
	if opts.ReplaceMetadata {
		meta, ct, encoding = opts.UserMetadata, opts.ContentType, opts.ContentEncoding
	}
	return t.UploadWithOptions(ctx, dstBucket, dstKey, srcObj.Body, UploadOptions{
		ExpectedSize: srcObj.Size,
		UserMetadata: meta, ContentType: ct, ContentEncoding: encoding, Precondition: opts.Dest,
	})
}

func (t *Tier) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType, contentEncoding string, checksumAlg *string) (string, error) {
	if err := t.requireWrite("multipart upload"); err != nil {
		return "", err
	}
	return t.store.CreateUpload(ctx, t.writeRef(t.key(bucket, key)), store.UploadOptions{UserMetadata: userMeta, ContentType: contentType, ContentEncoding: contentEncoding, ChecksumAlgorithm: checksumAlg})
}

func (t *Tier) StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32, expectedSize int64) (*store.ChunkWriter, error) {
	if err := t.requireWrite("multipart upload"); err != nil {
		return nil, err
	}
	return t.store.StartPart(ctx, t.writeRef(t.key(bucket, key)), uploadID, partNo, expectedSize)
}

func (t *Tier) CompleteUpload(ctx context.Context, uploadID, bucket, key string, listed []store.ListedPart, opts store.WriteOptions) (*store.Completed, error) {
	if err := t.requireWrite("multipart upload"); err != nil {
		return nil, err
	}
	ck := t.key(bucket, key)
	if err := t.origin.checkKey(ck); err != nil {
		return nil, err
	}
	exp := time.Now().Add(t.durableRetention())
	unlock, err := t.lockWrite(ctx, ck, nil)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if t.writeBack && opts.Precondition.Mode == store.Unconditional {
		opts.ExpiresAt, opts.SkipLock = nil, true
		done, err := t.store.CompleteUploadPending(ctx, t.writeRef(ck), uploadID, listed, opts, store.PendingCompletion{})
		if err != nil {
			return nil, err
		}
		_ = t.fenceReadCache(ctx, ck)
		t.wakePending()
		done.Bucket, done.Key = bucket, key
		return done, nil
	}
	if !acquireProcess(processPending, ctx) {
		return nil, ctx.Err()
	}
	defer func() { <-processPending }()
	if !acquireProcess(t.completes, ctx) {
		return nil, ctx.Err()
	}
	defer func() { <-t.completes }()
	base, baseErr := t.origin.Attributes(ctx, ck)
	baseMissing := errors.Is(baseErr, store.ErrNotFound) || (baseErr != nil && t.origin.IsObjNotFoundErr(baseErr))
	if baseErr != nil && !baseMissing {
		return nil, baseErr
	}
	baseETag := ""
	if !baseMissing {
		baseETag = versionFor(base)
	}
	resolved, err := t.resolvePrecondition(ctx, ck, opts.Precondition)
	if err != nil {
		return nil, err
	}
	if !resolved.Allows(!baseMissing, baseETag) {
		return nil, store.ErrPrecondition
	}

	opts.Precondition = store.Precondition{}
	opts.ExpiresAt, opts.SkipLock = &exp, true
	done, err := t.store.CompleteUploadPending(ctx, t.writeRef(ck), uploadID, listed, opts, store.PendingCompletion{BaseETag: baseETag, BaseMissing: baseMissing, LeaseOwner: t.workerID})
	if err != nil {
		return nil, err
	}
	_ = t.fenceReadCache(ctx, ck)
	pending := store.PendingUpload{
		Key: ck, Token: store.PendingOriginToken(uploadID),
		BaseETag: baseETag, BaseMissing: baseMissing, LeaseOwner: t.workerID,
	}
	localETag, err := t.flushOneNoLock(ctx, pending, t.workerID)
	if err != nil {
		if errors.Is(err, store.ErrPrecondition) {
			_, _ = t.store.Delete(context.WithoutCancel(ctx), t.cacheRef(ck))
		}
		return nil, err
	}
	if localETag == "" {
		return nil, store.ErrPrecondition
	}
	done.ETag, done.Bucket, done.Key = localETag, bucket, key
	return done, nil
}

func (t *Tier) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	if t.readOnly {
		return false, ErrReadOnly
	}
	return t.store.AbortUpload(ctx, t.writeRef(t.key(bucket, key)), uploadID)
}

func (t *Tier) ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]store.UploadPartInfo, bool, int32, error) {
	return t.store.ListParts(ctx, t.writeRef(t.key(bucket, key)), uploadID, max, marker)
}

func (t *Tier) ListUploads(ctx context.Context, bucket, prefix, bound, keyMarker, uploadMarker string, max int) ([]store.UploadInfo, bool, error) {
	rows, truncated, err := t.store.ListUploads(ctx, store.ObjectRef{NS: ns.WriteBackRef(t.namespace)},
		t.key(bucket, prefix), t.key(bucket, bound), t.key(bucket, keyMarker), uploadMarker, max)
	if err != nil {
		return nil, false, err
	}
	for i := range rows {
		rows[i].Key = strings.TrimPrefix(rows[i].Key, bucket+"/")
	}
	return rows, truncated, nil
}

func (t *Tier) Delete(ctx context.Context, bucket, key string) error {
	if err := t.requireWrite("delete"); err != nil {
		return err
	}
	ck := t.key(bucket, key)
	unlock, err := t.lockWrite(ctx, ck, nil)
	if err != nil {
		return err
	}
	defer unlock()
	if err := t.origin.Delete(ctx, ck); err != nil && !t.origin.IsObjNotFoundErr(err) {
		return err
	}
	cleanupCtx := context.WithoutCancel(ctx)
	if err := t.store.CacheTombstone(cleanupCtx, ns.WriteBackRef(t.namespace), ck, time.Now().Add(originTombstoneTTL)); err != nil {
		return err
	}
	if err := t.fenceReadCache(ctx, ck); err != nil {
		return err
	}
	_ = t.store.Cache(t.namespace).ClearPendingUploads(cleanupCtx, ck)
	return nil
}

type DeleteResult struct {
	Key string
	Err error
}

func (t *Tier) DeleteMany(ctx context.Context, bucket string, keys []string) []DeleteResult {
	results := make([]DeleteResult, len(keys))
	for i, key := range keys {
		results[i].Key = key
	}
	workers := min(8, len(keys))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				results[i].Err = t.Delete(ctx, bucket, keys[i])
			}
		})
	}
	for i := range keys {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results
}

func (t *Tier) flushOne(ctx context.Context, p store.PendingUpload, owner string) (string, error) {
	unlock, err := t.lockWrite(ctx, p.Key, nil)
	if err != nil {
		return "", err
	}
	defer unlock()
	return t.flushOneNoLock(ctx, p, owner)
}

func (t *Tier) flushOneNoLock(ctx context.Context, p store.PendingUpload, owner string) (string, error) {
	sm, body, err := t.store.GetBody(ctx, t.writeRef(p.Key), store.WholeObjectRange(), nil)
	if errors.Is(err, store.ErrNotFound) {
		err = fmt.Errorf("durable pending body missing for %s: %w", p.Key, err)
		_ = t.store.Cache(t.namespace).FailPendingUploadPermanent(context.WithoutCancel(ctx), p.Key, p.Token, owner, p.Attempts, err)
		return "", err
	}
	if err != nil {
		return "", err
	}
	if sm.OriginETag != p.Token {
		_ = body.Close()
		_ = t.store.Cache(t.namespace).ClearPendingUploadToken(context.WithoutCancel(ctx), p.Key, p.Token)
		return "", nil
	}
	sp, cleanup, err := spoolStream(ctx, body, sm.Size)
	_ = body.Close()
	if err != nil {
		_ = t.store.Cache(t.namespace).FailPendingUpload(context.WithoutCancel(ctx), p.Key, p.Token, owner, p.Attempts, err)
		return "", err
	}
	defer cleanup()
	condition := store.IfMatch
	switch {
	case p.BaseMissing:
		condition = store.IfAbsent
	case p.BaseETag == "":

		condition = store.Unconditional
	}
	attrs, err := t.origin.Upload(ctx, p.Key, sp.r, sp.size, originUpload{
		UserMetadata: sm.UserMetadata, ContentType: sm.ContentType,
		ContentEncoding: sm.ContentEncoding,
		Precondition:    store.Precondition{Mode: condition, ETag: p.BaseETag},
	})
	if errors.Is(err, errOriginPrecondition) {
		_ = t.store.Cache(t.namespace).AbandonPendingUpload(context.WithoutCancel(ctx), p.Key, p.Token)

		_, _ = t.store.Delete(context.WithoutCancel(ctx), t.cacheRef(p.Key))
		return "", store.ErrPrecondition
	}
	if err != nil {
		cache := t.store.Cache(t.namespace)
		fail := cache.FailPendingUpload
		if errors.Is(err, ErrOriginUnsupported) {
			fail = cache.FailPendingUploadPermanent
		}
		_ = fail(context.WithoutCancel(ctx), p.Key, p.Token, owner, p.Attempts, err)
		return "", err
	}
	expiresAt := time.Now().Add(t.durableRetention())
	owned, err := t.store.Cache(t.namespace).MarkOriginCommitted(ctx, p.Key, p.Token, versionFor(attrs), attrs.LastModified, &expiresAt)
	if err != nil {
		_ = t.store.Cache(t.namespace).FailPendingUpload(context.WithoutCancel(ctx), p.Key, p.Token, owner, p.Attempts, err)
		return "", err
	}
	if !owned {
		return "", nil
	}
	return sm.ETag, nil
}

func (t *Tier) runPending(ctx context.Context) {
	defer close(t.done)
	var wg sync.WaitGroup
	for i := range max(t.pendingWorkers, 1) {
		owner := fmt.Sprintf("%s-%d", t.workerID, i)
		wg.Go(func() { t.runPendingWorker(ctx, owner) })
	}
	wg.Wait()
}

func (t *Tier) runPendingWorker(ctx context.Context, owner string) {
	for ctx.Err() == nil {
		if !acquireProcess(processPending, ctx) {
			return
		}
		pending, err := t.store.Cache(t.namespace).ClaimPendingUploads(ctx, owner, 1, 5*time.Minute)
		if err != nil || len(pending) == 0 {
			<-processPending
			if err != nil && ctx.Err() == nil {
				t.log.Warn("claiming pending origin uploads", "owner", owner, "err", err)
			}
			if !waitContext(ctx, time.Second, t.pendingWake) {
				return
			}
			continue
		}
		p := pending[0]
		flushCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		_, err = t.flushOne(flushCtx, p, p.LeaseOwner)
		cancel()
		<-processPending
		if err != nil && ctx.Err() == nil {
			t.log.Warn("pending origin upload failed", "key", p.Key, "attempt", p.Attempts+1, "err", err)
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	return waitContext(ctx, d, nil)
}

func waitContext(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	case <-wake:
		return true
	}
}
