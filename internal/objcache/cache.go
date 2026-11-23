package objcache

import (
	"bytes"
	"context"
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

	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/syncx"
)

const (
	defaultChunkSize = 1 << 20 // 1 MiB
	maxChunkSize     = 8 << 20 // 8 MiB
	maxRangeFillSpan = 8 << 20 // 8 MiB per origin range fill
	memSpoolMax      = 8 << 20
	touchCooldown    = 30 * time.Second
)

var ErrSpoolBusy = errors.New("cache spool byte budget exhausted")

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

func (r *Runtime) WriteMetrics(w io.Writer) {
	fmt.Fprintf(w, "pgvs3_spool_bytes %d\npgvs3_spool_peak_bytes %d\npgvs3_spool_limit_bytes %d\npgvs3_spool_rejections_total %d\n",
		r.spool.used.Load(), r.spool.peak.Load(), r.spool.limit, r.spool.rejected.Load())
}

type TierOptions struct {
	TTL        time.Duration
	RangeChunk int64
	ReadOnly   bool
}

type Tier struct {
	origin    originHandle
	store     *store.Store
	runtime   *Runtime
	ttl       time.Duration
	chunkSize int64
	readOnly  bool
	group     syncx.Group
	log       *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once

	hits              atomic.Uint64
	misses            atomic.Uint64
	hitBytes          atomic.Uint64
	missBytes         atomic.Uint64
	fillCount         atomic.Uint64
	fillDurationNanos atomic.Uint64
	originErrors      atomic.Uint64
}

func New(log *slog.Logger, origin originHandle, st *store.Store, runtime *Runtime, opts TierOptions) (*Tier, error) {
	if origin == nil {
		return nil, errors.New("origin is required")
	}
	if runtime == nil {
		return nil, errors.New("cache runtime is required")
	}
	if st == nil {
		return nil, errors.New("store is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if opts.TTL < 0 {
		opts.TTL = 0
	}
	cs := opts.RangeChunk
	if cs <= 0 {
		cs = defaultChunkSize
	}
	cs = min(max(cs, 64<<10), maxChunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	return &Tier{
		origin:    origin,
		store:     st,
		runtime:   runtime,
		ttl:       opts.TTL,
		chunkSize: cs,
		readOnly:  opts.ReadOnly,
		log:       log,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (t *Tier) BucketAllowed(bucket string) bool { return t.origin.BucketAllowed(bucket) }
func (t *Tier) ReadOnly() bool                   { return t != nil && t.readOnly }

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

func (t *Tier) WriteMetrics(w io.Writer) {
	fmt.Fprintf(w, "pgvs3_cache_hits_total %d\npgvs3_cache_misses_total %d\npgvs3_cache_hit_bytes_total %d\npgvs3_cache_miss_bytes_total %d\npgvs3_cache_fill_duration_seconds_sum %.6f\npgvs3_cache_fill_duration_seconds_count %d\npgvs3_origin_errors_total %d\n",
		t.hits.Load(), t.misses.Load(), t.hitBytes.Load(), t.missBytes.Load(),
		float64(t.fillDurationNanos.Load())/float64(time.Second), t.fillCount.Load(), t.originErrors.Load())
	t.runtime.WriteMetrics(w)
}

func (t *Tier) isFresh(filledAt time.Time) bool {
	return t.ttl == 0 || time.Since(filledAt) < t.ttl
}

func (t *Tier) maybeTouch(m *store.ObjectMeta) {
	if t.readOnly || m == nil {
		return
	}
	if time.Since(m.LastAccess) > touchCooldown {
		t.store.TouchAsync(m.Bucket, m.Key)
	}
}

// resolveMeta validates m against t.ttl and origin if needed, persisting
// refreshed or updated metadata when writable. It issues at most one origin HEAD.
func (t *Tier) resolveMeta(ctx context.Context, bucket, key string, m *store.ObjectMeta) (meta *store.ObjectMeta, hit bool, changed bool, err error) {
	if m != nil && t.isFresh(m.FilledAt) {
		t.maybeTouch(m)
		return m, true, false, nil
	}

	type revalRes struct {
		meta    *store.ObjectMeta
		hit     bool
		changed bool
	}
	ch := t.group.DoChan("reval:"+bucket+"/"+key, func() (any, error) {
		rctx, cancel := context.WithTimeout(t.ctx, 30*time.Second)
		defer cancel()

		cur := m
		if cur != nil && !t.readOnly {
			if fresh, gerr := t.store.GetObject(rctx, bucket, key); gerr == nil && t.isFresh(fresh.FilledAt) {
				return revalRes{meta: fresh, hit: fresh.ETag == cur.ETag, changed: fresh.ETag != cur.ETag}, nil
			}
		}

		ifNoneMatch := ""
		if cur != nil {
			ifNoneMatch = cur.ETag
		}
		attrs, notMod, herr := t.origin.AttributesIfNoneMatch(rctx, bucket, key, ifNoneMatch)
		if herr != nil {
			if isObjectNotFound(herr) {
				if cur != nil && !t.readOnly {
					_ = t.store.Delete(context.WithoutCancel(rctx), bucket, key)
				}
				return nil, store.ErrNotFound
			}
			return nil, herr
		}
		if cur != nil && (notMod || (attrs.ETag == cur.ETag && attrs.Size == cur.Size)) {
			if !t.readOnly {
				_ = t.store.MarkRevalidated(rctx, bucket, key, cur.ETag)
			}
			return revalRes{meta: cur, hit: true, changed: false}, nil
		}
		if attrs.ContentType == "" {
			attrs.ContentType = contentTypeFor(key)
		}
		if !t.readOnly {
			id, uerr := t.store.UpsertObject(rctx, attrs)
			if uerr != nil {
				return nil, uerr
			}
			attrs.ID = id
		}
		return revalRes{meta: &attrs, hit: false, changed: true}, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, false, false, res.Err
		}
		rr := res.Val.(revalRes)
		return rr.meta, rr.hit, rr.changed, nil
	case <-ctx.Done():
		return nil, false, false, ctx.Err()
	}
}

func (t *Tier) Head(ctx context.Context, bucket, key string) (*store.ReadResult, error) {
	m, err := t.store.GetObject(ctx, bucket, key)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	meta, hit, _, err := t.resolveMeta(ctx, bucket, key, m)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, context.Canceled) {
			t.originErrors.Add(1)
		}
		return nil, err
	}
	return &store.ReadResult{ObjectMeta: *meta, End: max(meta.Size-1, 0), Hit: hit}, nil
}

func (t *Tier) Open(ctx context.Context, bucket, key string, rng store.ByteRange) (*store.ReadResult, error) {
	res, err := t.open(ctx, bucket, key, rng)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) && !errors.Is(err, store.ErrInvalidRange) && !errors.Is(err, context.Canceled) {
			t.originErrors.Add(1)
		}
		return nil, err
	}
	if res.Hit {
		t.hits.Add(1)
		t.hitBytes.Add(uint64(res.ContentLength()))
	} else {
		t.misses.Add(1)
		t.missBytes.Add(uint64(res.ContentLength()))
	}
	return res, nil
}

func (t *Tier) open(ctx context.Context, bucket, key string, rng store.ByteRange) (*store.ReadResult, error) {
	// Fast path: 1-RTT lookup for metadata + first needed chunk when start index is known.
	fastIdx := int32(0)
	if rng.Requested && rng.Suffix < 0 && rng.First >= 0 {
		fastIdx = int32(rng.First / t.chunkSize)
	}
	m, fastChunk, err := t.store.GetSingleChunk(ctx, bucket, key, fastIdx)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}

	m, metaHit, changed, err := t.resolveMeta(ctx, bucket, key, m)
	if err != nil {
		return nil, err
	}
	if changed {
		fastChunk = nil
	}
	if t.readOnly && changed {
		return t.passThroughMeta(ctx, m, rng)
	}

	span, err := rng.Resolve(m.Size)
	if err != nil {
		return nil, err
	}
	if m.Size == 0 {
		t.maybeTouch(m)
		return &store.ReadResult{
			ObjectMeta: *m,
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Start:      span.Start, End: span.End, Ranged: span.Ranged, Hit: metaHit,
		}, nil
	}

	loIdx := int32(span.Start / t.chunkSize)
	hiIdx := int32(span.End / t.chunkSize)

	// 1-RTT single-chunk hit!
	if loIdx == hiIdx && loIdx == fastIdx && fastChunk != nil {
		wantLen := t.chunkLen(m.Size, loIdx)
		if int64(len(fastChunk)) == wantLen {
			t.maybeTouch(m)
			off := span.Start % t.chunkSize
			end := off + (span.End - span.Start + 1)
			return &store.ReadResult{
				ObjectMeta: *m,
				Body:       io.NopCloser(bytes.NewReader(fastChunk[off:end])),
				Start:      span.Start, End: span.End, Ranged: span.Ranged, Hit: true,
			}, nil
		}
	}

	// Multi-chunk or suffix fast-path stream from PostgreSQL if all chunks are already cached.
	firstSkip := span.Start % t.chunkSize
	totalLen := span.End - span.Start + 1
	if m.ID > 0 && (loIdx != hiIdx || fastChunk != nil || rng.Suffix >= 0) {
		if sr, serr := t.store.OpenChunkStream(ctx, m.ID, loIdx, hiIdx, firstSkip, totalLen, t.chunkSize, m.Size); serr == nil {
			t.maybeTouch(m)
			return &store.ReadResult{
				ObjectMeta: *m,
				Body:       sr,
				Start:      span.Start, End: span.End, Ranged: span.Ranged, Hit: true,
			}, nil
		} else if !errors.Is(serr, store.ErrNotFound) {
			return nil, serr
		}
	}

	// Fill missing chunks from origin.
	chunks, err := t.store.GetChunks(ctx, m.ID, loIdx, hiIdx)
	if err != nil {
		return nil, err
	}
	hit := true
	for idx := loIdx; idx <= hiIdx; {
		if c := chunks[idx]; c != nil && int64(len(c)) == t.chunkLen(m.Size, idx) {
			idx++
			continue
		}
		if t.readOnly {
			return t.passThroughMeta(ctx, m, rng)
		}
		hit = false
		runEnd := idx
		for runEnd+1 <= hiIdx {
			next := runEnd + 1
			if c := chunks[next]; c != nil && int64(len(c)) == t.chunkLen(m.Size, next) {
				break
			}
			if (int64(next-idx)+1)*t.chunkSize > maxRangeFillSpan {
				break
			}
			runEnd = next
		}
		filled, ferr := t.fillChunkRun(ctx, m, idx, runEnd)
		if ferr != nil {
			return nil, ferr
		}
		for k, data := range filled {
			chunks[idx+int32(k)] = data
		}
		idx = runEnd + 1
	}

	// Assemble response body from the chunks.
	var readers []io.Reader
	for idx := loIdx; idx <= hiIdx; idx++ {
		c := chunks[idx]
		cs := int64(0)
		if idx == loIdx {
			cs = span.Start % t.chunkSize
		}
		ce := int64(len(c))
		if idx == hiIdx {
			ce = (span.End % t.chunkSize) + 1
		}
		readers = append(readers, bytes.NewReader(c[cs:ce]))
	}
	t.maybeTouch(m)
	return &store.ReadResult{
		ObjectMeta: *m,
		Body:       io.NopCloser(io.MultiReader(readers...)),
		Start:      span.Start, End: span.End, Ranged: span.Ranged, Hit: hit,
	}, nil
}

func (t *Tier) chunkLen(size int64, idx int32) int64 {
	off := int64(idx) * t.chunkSize
	return min(t.chunkSize, size-off)
}

func (t *Tier) fillChunkRun(ctx context.Context, m *store.ObjectMeta, firstIdx, lastIdx int32) ([][]byte, error) {
	startTime := time.Now()
	defer func() {
		t.fillCount.Add(1)
		t.fillDurationNanos.Add(uint64(time.Since(startTime)))
	}()

	firstOff := int64(firstIdx) * t.chunkSize
	lastLen := t.chunkLen(m.Size, lastIdx)
	totalLen := (int64(lastIdx)*t.chunkSize + lastLen) - firstOff
	sfKey := fmt.Sprintf("fill:%s/%s:%d:%d:%s", m.Bucket, m.Key, firstIdx, lastIdx, m.ETag)

	ch := t.group.DoChan(sfKey, func() (any, error) {
		fctx, cancel := context.WithTimeout(t.ctx, 2*time.Minute)
		defer cancel()
		if existing, err := t.store.GetChunks(fctx, m.ID, firstIdx, lastIdx); err == nil {
			allPresent := true
			out := make([][]byte, 0, lastIdx-firstIdx+1)
			for idx := firstIdx; idx <= lastIdx; idx++ {
				c := existing[idx]
				if c == nil || int64(len(c)) != t.chunkLen(m.Size, idx) {
					allPresent = false
					break
				}
				out = append(out, c)
			}
			if allPresent {
				return out, nil
			}
		}
		if !acquireSem(t.runtime.fills, fctx) {
			return nil, fctx.Err()
		}
		defer func() { <-t.runtime.fills }()

		rc, err := t.origin.GetRange(fctx, m.Bucket, m.Key, firstOff, totalLen, m.ETag)
		if err != nil {
			if isObjectNotFound(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		buf := make([]byte, totalLen)
		_, err = io.ReadFull(rc, buf)
		_ = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("origin range %s/%s at %d: %w", m.Bucket, m.Key, firstOff, err)
		}

		var out [][]byte
		for idx := firstIdx; idx <= lastIdx; idx++ {
			relOff := int64(idx-firstIdx) * t.chunkSize
			cLen := t.chunkLen(m.Size, idx)
			chunk := buf[relOff : relOff+cLen]
			out = append(out, chunk)
			if err := t.store.PutChunk(fctx, m.ID, idx, chunk); err != nil {
				t.log.Debug("store chunk failed", "bucket", m.Bucket, "key", m.Key, "idx", idx, "err", err)
			}
		}
		return out, nil
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

func (t *Tier) passThroughMeta(ctx context.Context, m *store.ObjectMeta, rng store.ByteRange) (*store.ReadResult, error) {
	span, err := rng.Resolve(m.Size)
	if err != nil {
		return nil, err
	}
	var rc io.ReadCloser
	if m.Size == 0 {
		rc = io.NopCloser(bytes.NewReader(nil))
	} else {
		rc, err = t.origin.GetRange(ctx, m.Bucket, m.Key, span.Start, span.End-span.Start+1, m.ETag)
		if err != nil {
			return nil, err
		}
	}
	return &store.ReadResult{
		ObjectMeta: *m,
		Body:       rc,
		Start:      span.Start, End: span.End, Ranged: span.Ranged,
	}, nil
}

func (t *Tier) Proxy(ctx context.Context, bucket, key, method string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error) {
	return t.origin.Proxy(ctx, bucket, key, method, query, body, size, extra...)
}

func (t *Tier) Invalidate(ctx context.Context, bucket, key string) error {
	if t.readOnly {
		return nil
	}
	return t.store.Delete(ctx, bucket, key)
}

func (t *Tier) InvalidateMany(ctx context.Context, bucket string, keys []string) error {
	if t.readOnly || len(keys) == 0 {
		return nil
	}
	return t.store.DeleteMany(ctx, bucket, keys)
}

func (t *Tier) SpoolStream(ctx context.Context, body io.Reader, expected int64) (io.ReadSeeker, int64, func(), error) {
	return spoolWithBudget(ctx, body, expected, t.runtime.spool)
}

func acquireSem(sem chan struct{}, ctx context.Context) bool {
	select {
	case sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
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

func spoolWithBudget(ctx context.Context, r io.Reader, expected int64, budget *spoolBudget) (io.ReadSeeker, int64, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, nil, err
	}
	head, err := io.ReadAll(io.LimitReader(r, memSpoolMax+1))
	if err != nil {
		return nil, 0, nil, err
	}
	if len(head) <= memSpoolMax {
		return bytes.NewReader(head), int64(len(head)), func() {}, nil
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
		return nil, 0, nil, err
	}
	tmp, err := os.CreateTemp(budget.dir, "pgvs3-spool-*")
	if err != nil {
		budget.used.Add(-reserved)
		budget.sem.Release(reserved)
		return nil, 0, nil, err
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
		return nil, 0, nil, err
	}
	w := &spoolWriter{ctx: ctx, file: tmp, size: int64(len(head)), reserve: reserve}
	n, err := io.Copy(w, r)
	if err == nil {
		_, err = tmp.Seek(0, io.SeekStart)
	}
	if err != nil {
		clean()
		return nil, 0, nil, err
	}
	return tmp, int64(len(head)) + n, clean, nil
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

func contentTypeFor(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}
