package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"
)

type ObjectRef struct {
	NS  ns.Ref
	Key string
}

func NewObjectRef(kind ns.Kind, namespace, key string) (ObjectRef, error) {
	ref := ObjectRef{NS: ns.Ref{Kind: kind, Name: namespace}, Key: key}
	if err := ref.Validate(); err != nil {
		return ObjectRef{}, err
	}
	return ref, nil
}

func (r ObjectRef) Validate() error {
	if _, err := r.NS.Schema(); err != nil {
		return err
	}
	if maxKey := r.NS.Kind.MaxKeyLen(); len(r.Key) == 0 || len(r.Key) > maxKey {
		return fmt.Errorf("%w: object key must be 1..%d bytes", ErrInvalidBucket, maxKey)
	}
	return nil
}

func (r ObjectRef) Schema() (string, error) {
	return nsSQLIdent(r.NS.Kind, r.NS.Name)
}

func (r ObjectRef) String() string {
	return string(r.NS.Kind) + ":" + r.NS.Name + "/" + r.Key
}

type ObjectInfo struct {
	Size            int64
	ETag            string
	OriginETag      string
	ContentType     string
	ContentEncoding string
	ChecksumName    string
	ChecksumValue   string
	ChecksumType    string
	UserMetadata    []string
	CreatedAt       time.Time
	OriginModified  time.Time
}

func (i ObjectInfo) LastModified() time.Time {
	if !i.OriginModified.IsZero() {
		return i.OriginModified
	}
	return i.CreatedAt
}

type ReadResult struct {
	ObjectInfo
	Body   io.ReadCloser
	Start  int64
	End    int64
	Ranged bool
	Hit    bool
}

func (r *ReadResult) ContentLength() int64 {
	if r.Size == 0 {
		return 0
	}
	return r.End - r.Start + 1
}

type Meta struct {
	ObjectInfo
	SHA256    []byte
	ExpiresAt *time.Time
	FileID    int64

	Parts    []int64
	PartEnds []int64

	Inline []byte

	// table is the objects relation OID, which keys the gateway block cache.
	table uint32
}

type Segment struct {
	FileID int64
	Base   int64
	Length int64
}

func (m *Meta) Segments() ([]Segment, error) {
	if m.Parts == nil && m.PartEnds == nil {
		return []Segment{{FileID: m.FileID, Base: 0, Length: m.Size}}, nil
	}
	if m.Parts == nil || m.PartEnds == nil {
		return nil, fmt.Errorf("incomplete multipart manifest")
	}
	if len(m.Parts) == 0 || len(m.Parts) != len(m.PartEnds) ||
		m.Parts[0] != m.FileID || m.PartEnds[len(m.PartEnds)-1] != m.Size {
		return nil, fmt.Errorf("invalid multipart manifest")
	}
	out := make([]Segment, 0, len(m.Parts))
	var prev int64
	for i, id := range m.Parts {
		end := m.PartEnds[i]
		if end < prev {
			return nil, fmt.Errorf("invalid multipart offsets")
		}
		out = append(out, Segment{FileID: id, Base: prev, Length: end - prev})
		prev = end
	}
	return out, nil
}

func (m *Meta) Live(now time.Time) bool {
	return m.ExpiresAt == nil || m.ExpiresAt.After(now)
}

type ByteRange struct {
	First, Last, Suffix int64
	Requested           bool
}

func WholeObjectRange() ByteRange {
	return ByteRange{First: 0, Last: -1, Suffix: -1}
}

func Range(first, last, suffix int64) ByteRange {
	return ByteRange{First: first, Last: last, Suffix: suffix, Requested: true}
}

type Span struct {
	Start, End int64
	Ranged     bool
}

func (r ByteRange) Resolve(size int64) (Span, error) {
	if !r.Requested {
		return Span{Start: 0, End: size - 1}, nil
	}
	if size <= 0 {
		return Span{Start: 0, End: -1, Ranged: true}, nil
	}
	if r.Suffix >= 0 {
		if r.Suffix == 0 {
			return Span{}, ErrInvalidRange
		}
		return Span{Start: max(size-r.Suffix, 0), End: size - 1, Ranged: true}, nil
	}
	start := max(r.First, 0)
	if start >= size {
		return Span{}, ErrInvalidRange
	}
	end := size - 1
	if r.Last >= 0 {
		if r.Last < start {
			return Span{}, ErrInvalidRange
		}
		end = min(r.Last, size-1)
	}
	return Span{Start: start, End: end, Ranged: true}, nil
}

type Precondition struct {
	Mode Condition
	ETag string
}

func (p Precondition) SQL() (string, any) {
	switch p.Mode {
	case IfAbsent:
		return "if_absent", nil
	case IfPresent:
		return "if_present", nil
	case IfMatch:

		return "if_match", p.ETag
	default:
		return "none", nil
	}
}

func (o WriteOptions) s3MetaJSON() []byte {
	meta := map[string]string{}
	if o.ContentEncoding != "" {
		meta["content_encoding"] = o.ContentEncoding
	}
	if !o.OriginModified.IsZero() {
		meta["origin_modified"] = o.OriginModified.UTC().Format(time.RFC3339Nano)
	}
	if o.Checksum != nil && o.Checksum.Name != "" && o.Checksum.Value != "" {
		meta["checksum_name"] = o.Checksum.Name
		meta["checksum_value"] = o.Checksum.Value
		meta["checksum_type"] = o.ChecksumType
	}
	if lease := o.Publication.lease; lease != nil {
		meta["fill_namespace"] = lease.Namespace
		meta["fill_key"] = lease.Key
		meta["fill_owner"] = lease.Owner
	}
	if len(meta) == 0 {
		return []byte("{}")
	}
	b, _ := json.Marshal(meta)
	return b
}

func (p Precondition) Allows(exists bool, etag string) bool {
	switch p.Mode {
	case IfAbsent:
		return !exists
	case IfPresent:
		return exists
	case IfMatch:
		return exists && p.ETag != "" && etagMatches(p.ETag, etag)
	default:
		return true
	}
}

type PendingOrigin struct {
	Token       string
	BaseETag    string
	BaseMissing bool
}

type FillLease struct {
	Namespace string
	Key       string
	Owner     string
}

type publicationKind uint8

const (
	publicationNormal publicationKind = iota
	publicationFill
	publicationPending
	publicationTombstone
)

type Publication struct {
	kind     publicationKind
	origin   string
	expected string
	lease    *FillLease
	pending  PendingOrigin
}

func NormalPublication(origin string) Publication { return Publication{origin: origin} }

func FillPublication(origin, expected string, lease *FillLease) Publication {
	p := Publication{kind: publicationFill, origin: origin, expected: expected}
	if lease != nil {
		copied := *lease
		p.lease = &copied
	}
	return p
}

func WriteBackPublication(pending PendingOrigin) Publication {
	return Publication{kind: publicationPending, origin: pending.Token, pending: pending}
}

func (p Publication) OriginVersion() string { return p.origin }

func (p Publication) IsPending() bool { return p.kind == publicationPending }

func (s *Store) prepareWrite(ref ObjectRef, o WriteOptions, inline bool) (WriteOptions, error) {
	if err := o.validateLock(ref); err != nil {
		return o, err
	}
	if o.lock != nil && o.lock.store != s {
		return o, ErrInvalidPublication
	}
	p := o.Publication
	switch p.kind {
	case publicationNormal:
		if !ParseCacheVersion(p.origin).IsLive() {
			return o, ErrInvalidPublication
		}
	case publicationFill:
		if ref.NS.Kind != ns.Cache || !ParseCacheVersion(p.origin).IsLive() || !ParseCacheVersion(p.expected).IsLive() ||
			(p.lease != nil && (p.lease.Namespace != ref.NS.Name || p.lease.Key == "" || p.lease.Owner == "")) {
			return o, ErrInvalidPublication
		}
	case publicationPending:
		v := ParseCacheVersion(p.pending.Token)
		if inline || (ref.NS.Kind != ns.Cache && ref.NS.Kind != ns.WriteBack) || !v.IsPending() || v.Token == "" ||
			(p.pending.BaseMissing && p.pending.BaseETag != "") || o.Precondition.Mode != Unconditional {
			return o, ErrInvalidPublication
		}
		o.ExpiresAt, o.KeepTTL = nil, false
	case publicationTombstone:
		if (ref.NS.Kind != ns.Cache && ref.NS.Kind != ns.WriteBack) || !ParseCacheVersion(p.origin).IsDeleted() || o.ExpiresAt == nil {
			return o, ErrInvalidPublication
		}
	default:
		return o, ErrInvalidPublication
	}
	return o, nil
}

func (o WriteOptions) validateLock(ref ObjectRef) error {
	if o.lock != nil && (o.lock.store == nil || o.lock.release == nil || o.lock.ref != ref || o.lock.released.Load()) {
		return ErrInvalidPublication
	}
	return nil
}

func (o WriteOptions) skipLock() bool { return o.lock != nil }

type WriteOptions struct {
	Precondition    Precondition
	UserMetadata    []string
	ContentType     string
	ContentEncoding string

	OriginModified time.Time
	Checksum       *Checksum
	ChecksumType   string
	ExpiresAt      *time.Time
	KeepTTL        bool
	Publication    Publication
	lock           *LockedObject

	ExpectedSize int64
}

type metadataBody uint8

const (
	metadataHeader metadataBody = iota
	metadataStored
	metadataDecoded
)

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func metadataColumns(body metadataBody) string {
	inline := "NULL::bytea, false"
	if body != metadataHeader {
		inline = "inline_data, inline_data IS NOT NULL"
	}
	return `COALESCE(file_id, 0), size, sha256, created_at, parts, part_ends,
		etag, user_metadata, content_type, content_encoding, origin_modified,
		COALESCE(checksum_name, ''), COALESCE(checksum_value, ''), COALESCE(checksum_type, ''),
		` + inline + `, expires_at, COALESCE(origin_etag, ''), tableoid`
}

func scanMetadata(row pgx.Row, body metadataBody) (*Meta, error) {
	var m Meta
	var modified pgtype.Timestamptz
	var hasInline bool
	err := row.Scan(&m.FileID, &m.Size, &m.SHA256, &m.CreatedAt, &m.Parts, &m.PartEnds,
		&m.ETag, &m.UserMetadata, &m.ContentType, &m.ContentEncoding, &modified,
		&m.ChecksumName, &m.ChecksumValue, &m.ChecksumType,
		&m.Inline, &hasInline, &m.ExpiresAt, &m.OriginETag, &m.table)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if modified.Valid {
		m.OriginModified = modified.Time
	}
	if hasInline {
		if body == metadataDecoded {
			m.Inline, err = decompressBlock(nil, m.Inline, int(m.Size))
		} else if m.Inline == nil {
			m.Inline = []byte{}
		}
	}
	return &m, err
}

func queryMetadata(ctx context.Context, q rowQuerier, schema, key string, body metadataBody) (*Meta, error) {
	return scanMetadata(q.QueryRow(ctx, `SELECT `+metadataColumns(body)+` FROM `+schema+`.objects
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`, key), body)
}

func (s *Store) Meta(ctx context.Context, ref ObjectRef) (*Meta, error) {
	return s.metaRef(ctx, ref, metadataDecoded)
}

func (s *Store) MetaHeader(ctx context.Context, ref ObjectRef) (*Meta, error) {
	return s.metaRef(ctx, ref, metadataHeader)
}

func (s *Store) metaRef(ctx context.Context, ref ObjectRef, body metadataBody) (*Meta, error) {
	return withSchema(ctx, s, ref.NS, ErrNoBucket, ErrNotFound, func(schema string) (*Meta, error) {
		return queryMetadata(ctx, s.pool, schema, ref.Key, body)
	})
}

func (c Cache) Metadata(ctx context.Context, key string, withInline bool) (*Meta, ns.Ref, error) {
	refs := []ns.Ref{ns.WriteBackRef(c.name), ns.CacheRef(c.name)}
	write, err := nsSQLIdent(ns.WriteBack, c.name)
	if err != nil {
		return nil, ns.Ref{}, err
	}
	read, _ := nsSQLIdent(ns.Cache, c.name)
	body := metadataHeader
	if withInline {
		body = metadataDecoded
	}
	cols := metadataColumns(body)
	var kind string
	var priority int
	row := &cacheMetadataRow{Row: c.s.pool.QueryRow(ctx, `SELECT * FROM (
		SELECT `+cols+`, 'writeback'::text, 0 AS source_priority FROM `+write+`.objects
		 WHERE key=$1 AND (expires_at IS NULL OR expires_at > now())
		UNION ALL
		SELECT `+cols+`, 'cache'::text, 1 FROM `+read+`.objects
		 WHERE key=$1 AND (expires_at IS NULL OR expires_at > now())
		) AS candidates ORDER BY source_priority LIMIT 1`, key), kind: &kind, priority: &priority}
	m, err := scanMetadata(row, body)
	if missingNamespace(ctx, c.s.pool, write, row.err) || missingNamespace(ctx, c.s.pool, read, row.err) {

		for _, ref := range refs {
			m, err := c.s.metaRef(ctx, ObjectRef{NS: ref, Key: key}, body)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNoBucket) {
				continue
			}
			return m, ref, err
		}
		return nil, ns.Ref{}, ErrNotFound
	}
	return m, ns.Ref{Kind: ns.Kind(kind), Name: c.name}, err
}

type cacheMetadataRow struct {
	pgx.Row
	kind     *string
	priority *int
	err      error
}

func (r *cacheMetadataRow) Scan(dest ...any) error {
	r.err = r.Row.Scan(append(dest, r.kind, r.priority)...)
	return r.err
}

var (
	ErrNotFound     = errors.New("object not found")
	ErrInvalidRange = errors.New("invalid range")
	ErrNotModified  = errors.New("not modified")
)

type ReadCondition struct {
	IfMatch     string
	IfNoneMatch string
}

func (c ReadCondition) Check(exists bool, etag string) error {
	if c.IfMatch != "" && (!exists || !etagMatches(c.IfMatch, etag)) {
		return ErrPrecondition
	}
	if c.IfNoneMatch != "" && exists && etagMatches(c.IfNoneMatch, etag) {
		return ErrNotModified
	}
	return nil
}

func (c ReadCondition) Allows(exists bool, etag string) bool {
	return c.Check(exists, etag) == nil
}

type SliceMeta struct {
	ObjectInfo
	FileID   int64
	Parts    []int64
	PartEnds []int64
	Start    int64
	End      int64
}

func (m *SliceMeta) Len() int64 {
	if m.End < m.Start {
		return 0
	}
	return m.End - m.Start + 1
}

func (s *Store) metaForRead(ctx context.Context, ref ObjectRef, cond *ReadCondition) (*Meta, error) {
	m, err := s.Meta(ctx, ref)
	if err != nil {
		return nil, err
	}
	if cond == nil {
		return m, nil
	}
	if err := cond.Check(true, m.ETag); err != nil {
		return nil, err
	}
	return m, nil
}

func etagMatches(header, etag string) bool {
	want := strings.Trim(strings.TrimPrefix(strings.TrimSpace(etag), "W/"), `"`)
	for token := range strings.SplitSeq(header, ",") {
		t := strings.TrimSpace(token)
		if t == "*" {
			return true
		}
		t = strings.Trim(strings.TrimPrefix(t, "W/"), `"`)
		if t != "" && t == want {
			return true
		}
	}
	return false
}

type piece struct {
	fileID int64
	base   int64
	segLen int64
	lo, hi int32
}

const (
	readPieceBytes    = 8 << 20
	minReadPieceBytes = 1 << 20
	oneQueryReadBytes = 1 << 20
	maxParallelPieces = 8
)

// planPieces splits a byte span into ordered 8,120-byte-row pieces. Small spans
// use one query; spans up to 64 MiB split across the available readers; larger
// spans use 8 MiB pieces and an ordered sliding window.
func planPieces(m *Meta, start, end int64, parallel int) ([]piece, error) {
	segments, err := m.Segments()
	if err != nil {
		return nil, err
	}
	span := end - start + 1
	parallel = max(parallel, 1)
	var overlapping []Segment
	for _, seg := range segments {
		if seg.Length > 0 && seg.Base+seg.Length-1 >= start && seg.Base <= end {
			overlapping = append(overlapping, seg)
		}
	}
	if len(overlapping) == 0 {
		return nil, fmt.Errorf("range [%d,%d] outside object size %d", start, end, m.Size)
	}
	share := max(parallel/len(overlapping), 1)
	var out []piece
	for _, seg := range overlapping {
		lo := int32((max(start, seg.Base) - seg.Base) / RowBytes)
		hi := int32((min(end, seg.Base+seg.Length-1) - seg.Base) / RowBytes)
		rows := int64(hi-lo) + 1
		count := int64(1)
		switch {
		case span <= oneQueryReadBytes || parallel <= 1:
			// One query.
		case span <= int64(maxParallelPieces)*readPieceBytes:
			if len(overlapping) == 1 {
				target := max((span+int64(parallel)-1)/int64(parallel), minReadPieceBytes)
				count = min(rows, (span+target-1)/target)
			} else {
				count = min(rows, int64(share))
			}
		default:
			count = min(rows, max(rows*RowBytes/readPieceBytes, 1))
		}
		step := int32((rows + count - 1) / count)
		for r := lo; r <= hi; r += step {
			out = append(out, piece{fileID: seg.FileID, base: seg.Base, segLen: seg.Length, lo: r, hi: min(r+step-1, hi)})
		}
	}
	return out, nil
}

type result struct {
	b []byte
	// shared marks cache-owned bytes that must not return to chunkBufPool.
	shared bool
}

type Body struct {
	cur    []byte
	chunk  []byte
	shared bool
	ch     <-chan result
	errc   <-chan error
	parts  [][]byte
	cancel context.CancelFunc
	err    error
	once   sync.Once
	static bool

	reader *chunkRowReader
	conn   *pgxpool.Conn
}

// NewPartsBody returns a streaming body over caller-owned parts. The body
// borrows the bytes without copying; callers must not modify them while the
// body is in use. Parts are never returned to the chunk buffer pool.
func NewPartsBody(parts [][]byte) *Body {
	return &Body{parts: parts}
}

func (b *Body) Read(p []byte) (int, error) {
	if b.static {
		if len(b.cur) == 0 {
			return 0, io.EOF
		}
		n := copy(p, b.cur)
		b.cur = b.cur[n:]
		return n, nil
	}
	for len(b.cur) == 0 {
		chunk, err := b.nextStreamChunk()
		if err != nil {
			return 0, err
		}
		b.cur = chunk
	}
	n := copy(p, b.cur)
	b.cur = b.cur[n:]
	return n, nil
}

func (b *Body) WriteTo(w io.Writer) (int64, error) {
	var n int64
	write := func(p []byte) error {
		if len(p) == 0 {
			return nil
		}
		m, err := w.Write(p)
		n += int64(m)
		if err == nil && m != len(p) {
			err = io.ErrShortWrite
		}
		return err
	}
	if err := write(b.cur); err != nil {
		b.releaseChunk()
		return n, err
	}
	b.releaseChunk()
	if b.static {
		return n, nil
	}
	for {
		chunk, err := b.nextStreamChunk()
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if err := write(chunk); err != nil {
			b.releaseChunk()
			return n, err
		}
		b.releaseChunk()
	}
}

func (b *Body) nextStreamChunk() ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	b.releaseChunk()
	if b.reader != nil {
		chunk, err := b.nextChunk()
		if err != nil {
			b.err = err
			return nil, err
		}
		b.chunk = chunk
		return chunk, nil
	}
	if b.ch == nil {
		if len(b.parts) == 0 {
			b.err = io.EOF
			return nil, io.EOF
		}
		b.chunk, b.shared, b.parts = b.parts[0], true, b.parts[1:]
		return b.chunk, nil
	}
	r, ok := <-b.ch
	if !ok {
		b.err = io.EOF
		if err := <-b.errc; err != nil {
			b.err = err
		}
		return nil, b.err
	}
	b.chunk, b.shared = r.b, r.shared
	return r.b, nil
}

func (b *Body) releaseChunk() {
	if b.chunk != nil {
		if !b.shared {
			putChunkBuf(b.chunk)
		}
		b.chunk = nil
		b.shared = false
		b.cur = nil
	}
}

func (b *Body) Close() error {
	b.once.Do(func() {
		b.releaseChunk()
		if b.reader != nil {
			b.reader.close()
		}
		if b.conn != nil {
			b.conn.Release()
		}
		if b.cancel != nil {
			b.cancel()
		}
	})
	return nil
}

var decompPool = sync.Pool{New: func() any {
	buf := make([]byte, RowBytes)
	return &buf
}}

const chunkBufCap = chunkSize + RowBytes

var chunkBufPool = sync.Pool{New: func() any {
	buf := make([]byte, 0, chunkBufCap)
	return &buf
}}

func getChunkBuf() []byte {
	buf := chunkBufPool.Get().(*[]byte)
	return (*buf)[:0]
}

func putChunkBuf(buf []byte) {
	if cap(buf) < chunkBufCap {
		return
	}
	chunkBufPool.Put(&buf)
}

func decodeRow(dst []byte, segLen int64, no int32, stored []byte) ([]byte, error) {
	want := int(min(int64(RowBytes), segLen-int64(no)*RowBytes))
	if want < 0 {
		return nil, fmt.Errorf("row %d beyond segment length %d", no, segLen)
	}
	return decompressBlock(dst, stored, want)
}

func (b *Body) nextChunk() ([]byte, error) {
	r := b.reader
	capHint := int64(chunkSize + RowBytes)
	if span := r.spanEnd - r.spanStart + 1; span < capHint {
		capHint = span + RowBytes
	}
	var out []byte
	if capHint >= chunkSize {
		out = getChunkBuf()
	} else {
		out = make([]byte, 0, int(capHint))
	}
	for {
		no, data, err := r.next()
		if err != nil {
			if err == io.EOF && len(out) > 0 {
				return out, nil
			}
			putChunkBuf(out)
			return nil, err
		}
		r.appendRow(&out, no, data)
		if len(out) >= chunkSize {
			return out, nil
		}
	}
}

func (s *Store) GetBody(ctx context.Context, ref ObjectRef, rng ByteRange, cond *ReadCondition) (*SliceMeta, *Body, error) {
	var last error
	for range 2 {
		m, err := s.metaForRead(ctx, ref, cond)
		if err != nil {
			return nil, nil, err
		}
		sm, body, err := s.bodyFromMeta(ctx, ref, m, rng)
		if err == nil {
			return sm, body, nil
		}
		if errors.Is(err, ErrInvalidRange) {
			return nil, nil, err
		}
		last = err
	}
	return nil, nil, last
}

func (s *Store) GetBodyFromMeta(ctx context.Context, ref ObjectRef, m *Meta, rng ByteRange) (*SliceMeta, *Body, error) {
	if !m.Live(time.Now()) {
		return nil, nil, ErrNotFound
	}
	return s.bodyFromMeta(ctx, ref, m, rng)
}

func (s *Store) bodyFromMeta(ctx context.Context, ref ObjectRef, m *Meta, rng ByteRange) (*SliceMeta, *Body, error) {
	span, err := rng.Resolve(m.Size)
	if err != nil {
		return nil, nil, err
	}
	mstart, mend := span.Start, span.End
	sm := &SliceMeta{
		ObjectInfo: m.ObjectInfo,
		FileID:     m.FileID, Parts: m.Parts, PartEnds: m.PartEnds,
		Start: mstart, End: mend,
	}
	if m.Size == 0 || sm.Len() == 0 {
		return sm, bytesBody(nil), nil
	}
	if m.Inline != nil {
		return sm, bytesBody(m.Inline[sm.Start : sm.End+1]), nil
	}
	streamCtx, cancel := context.WithCancel(ctx)
	endRead := s.beginRead()
	release := sync.OnceFunc(func() {
		cancel()
		endRead()
	})
	body, err := s.startStream(streamCtx, release, ref, m, mstart, mend)
	if err != nil {
		release()
		return nil, nil, err
	}
	return sm, body, nil
}

func bytesBody(data []byte) *Body {
	return &Body{cur: data, static: true}
}

func (s *Store) startSingleStream(ctx context.Context, cancel context.CancelFunc, schema string, table uint32, p piece, start, end int64) (*Body, error) {
	conn, err := s.chunkPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, getRangeSQL(schema), p.fileID, p.lo, p.hi)
	if err != nil {
		conn.Release()
		return nil, err
	}
	body := &Body{
		reader: newChunkRowReader(rows, p, start, end, s.blocks.fill(table, p)),
		conn:   conn, cancel: cancel,
	}
	chunk, err := body.nextChunk()
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	body.chunk, body.cur = chunk, chunk
	return body, nil
}

func (s *Store) startStream(ctx context.Context, cancel context.CancelFunc, ref ObjectRef, m *Meta, start, end int64) (*Body, error) {
	schema, err := ref.Schema()
	if err != nil {
		return nil, err
	}
	pieces, err := planPieces(m, start, end, s.readWorkers())
	if err != nil {
		return nil, err
	}
	var table uint32
	if s.blocks != nil {
		table = m.table
	}
	if table != 0 {
		pieces = alignPieces(pieces)
		if parts, ok := s.cachedSpan(table, pieces, start, end); ok {
			return &Body{parts: parts, cancel: cancel}, nil
		}
	}
	if len(pieces) == 1 {
		return s.startSingleStream(ctx, cancel, schema, table, pieces[0], start, end)
	}
	ch := make(chan result, 32)
	errc := make(chan error, 1)
	go func() {
		errc <- s.streamWindow(ctx, ch, schema, table, pieces, start, end)
		close(ch)
	}()
	first, ok := <-ch
	if !ok {
		if err := <-errc; err != nil {
			return nil, err
		}
		return nil, io.ErrUnexpectedEOF
	}
	return &Body{ch: ch, errc: errc, cancel: cancel, chunk: first.b, cur: first.b, shared: first.shared}, nil
}

// cachedSpan serves a whole span from the block cache, or reports a miss.
func (s *Store) cachedSpan(table uint32, pieces []piece, start, end int64) ([][]byte, bool) {
	for _, p := range pieces {
		if !s.blocks.covered(table, p) {
			return nil, false
		}
	}
	var out [][]byte
	for _, p := range pieces {
		parts, ok := s.blocks.pieceParts(table, p, start, end)
		if !ok {
			return nil, false
		}
		out = append(out, parts...)
	}
	return out, true
}

// readWorkers reserves one guard connection per in-flight read and shares the
// rest of the chunk pool. The cap keeps one GET from starving concurrent reads.
func (s *Store) readWorkers() int {
	return readWorkersFor(int(s.chunkPool.Stat().MaxConns()), int(s.inflightReads.Load()))
}

// readWorkersFor reserves half the chunk pool for writes, maintenance and the
// next read; one slot per in-flight read is its guard connection.
func readWorkersFor(maxConns, inflight int) int {
	inflight = max(inflight, 1)
	budget := max(maxConns/2, 1)
	available := max(budget-inflight, 0)
	return min(max(available/inflight, 1), maxParallelPieces)
}

func (s *Store) beginRead() func() {
	s.inflightReads.Add(1)
	return func() { s.inflightReads.Add(-1) }
}

// streamWindow reads pieces through an ordered window. Workers pull pieces in
// order from a shared counter and stream each into its own channel; the
// consumer drains the channels in piece order, so output stays contiguous and
// memory is bounded by the workers' channel buffers. Pieces already in the
// block cache are emitted by the consumer without a query. A readGuard keeps
// every streamed file from being reaped until the read ends.
func (s *Store) streamWindow(ctx context.Context, out chan<- result, schema string, table uint32, pieces []piece, start, end int64) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cached := make([][][]byte, len(pieces))
	var need []int
	for i, p := range pieces {
		if parts, ok := s.blocks.pieceParts(table, p, start, end); ok {
			cached[i] = parts
		} else {
			need = append(need, i)
		}
	}
	chans := make([]chan result, len(pieces))
	for _, i := range need {
		chans[i] = make(chan result, 4)
	}
	g, gctx := errgroup.WithContext(runCtx)
	if workers := min(s.readWorkers(), len(need)); workers > 0 {
		guarded := make([]piece, len(need))
		for j, i := range need {
			guarded[j] = pieces[i]
		}
		guard, err := s.openReadGuard(runCtx, guarded, workers)
		if err != nil {
			return err
		}
		defer guard.close()
		// Helpers wait on the pool only until every piece is claimed; the
		// lead works on the guard's session, so the read always progresses.
		claimCtx, allClaimed := context.WithCancel(gctx)
		defer allClaimed()
		var next atomic.Int64
		work := func(q querier) error {
			for {
				if err := gctx.Err(); err != nil {
					return err
				}
				j := int(next.Add(1) - 1)
				if j >= len(need) {
					allClaimed()
					return nil
				}
				i := need[j]
				ch := chans[i]
				send := func(b []byte) bool {
					select {
					case ch <- result{b: b}:
						return true
					case <-gctx.Done():
						return false
					}
				}
				if err := streamRows(gctx, q, schema, pieces[i], start, end, s.blocks.fill(table, pieces[i]), send); err != nil {
					return err
				}
				close(ch)
			}
		}
		g.Go(func() error {
			q, done := guard.lead()
			defer done()
			return work(q)
		})
		for range workers - 1 {
			g.Go(func() error {
				q, done, err := guard.join(claimCtx)
				if err != nil {
					if gctx.Err() == nil && claimCtx.Err() != nil {
						return nil
					}
					return err
				}
				defer done()
				return work(q)
			})
		}
	}

	fail := func(err error) error {
		cancel()
		if werr := g.Wait(); werr != nil && !errors.Is(werr, context.Canceled) {
			return werr
		}
		return err
	}
	emit := func(r result) error {
		select {
		case out <- r:
			return nil
		case <-runCtx.Done():
			return runCtx.Err()
		}
	}
	for i := range pieces {
		if parts := cached[i]; parts != nil {
			for _, b := range parts {
				if err := emit(result{b: b, shared: true}); err != nil {
					return fail(err)
				}
			}
			continue
		}
		for open := true; open; {
			select {
			case r, ok := <-chans[i]:
				if !ok {
					open = false
					continue
				}
				if err := emit(r); err != nil {
					return fail(err)
				}
			case <-gctx.Done():
				return fail(context.Cause(gctx))
			}
		}
	}
	return g.Wait()
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func getRangeSQL(schema string) string {
	return `SELECT c.no, c.data FROM ` + schema + `.chunks c
WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3`
}

func streamRows(ctx context.Context, q querier, schema string, p piece, start, end int64, fill *blockFill, send func([]byte) bool) error {
	rows, err := q.Query(ctx, getRangeSQL(schema), p.fileID, p.lo, p.hi)
	if err != nil {
		return err
	}
	r := newChunkRowReader(rows, p, start, end, fill)
	defer r.close()
	capHint := int64(chunkSize + RowBytes)
	if span := end - start + 1; span < capHint {
		capHint = span + RowBytes
	}
	var out []byte
	if capHint >= chunkSize {
		out = getChunkBuf()
	} else {
		out = make([]byte, 0, int(capHint))
	}
	defer func() { putChunkBuf(out) }()
	for {
		no, data, err := r.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		r.appendRow(&out, no, data)
		if len(out) >= chunkSize {
			if !send(out) {
				return ctx.Err()
			}
			out = nil
			if capHint >= chunkSize {
				out = getChunkBuf()
			} else {
				out = make([]byte, 0, int(capHint))
			}
		}
	}
	if len(out) > 0 {
		if !send(out) {
			return ctx.Err()
		}
		out = nil
	}
	return nil
}

func appendRow(out *[]byte, base int64, no int32, data []byte, start, end int64) {
	rowStart := base + int64(no)*RowBytes
	lo := max(start, rowStart) - rowStart
	hi := min(end, rowStart+int64(len(data))-1) - rowStart
	if hi >= lo {
		*out = append(*out, data[lo:hi+1]...)
	}
}

func (s *Store) Get(ctx context.Context, ref ObjectRef, rng ByteRange) (*Slice, error) {
	sm, body, err := s.GetBody(ctx, ref, rng, nil)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &Slice{ObjectInfo: sm.ObjectInfo, Start: sm.Start, End: sm.End, Bytes: b}, nil
}

type Slice struct {
	ObjectInfo
	Start int64
	End   int64
	Bytes []byte
}

func (s *Store) Delete(ctx context.Context, ref ObjectRef) (bool, error) {
	n, err := s.DeleteMany(ctx, ref.NS, []string{ref.Key})
	return n > 0, err
}

func (s *Store) DeleteMany(ctx context.Context, ref ns.Ref, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	return withSchema(ctx, s, ref, ErrNoBucket, nil, func(sc string) (int64, error) {
		schema := strings.Trim(sc, `"`)
		return withTx(ctx, s, func(tx pgx.Tx) (int64, error) {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || E'\x1f' || k, 0))
				FROM unnest($2::text[]) AS k`, schema, sorted); err != nil {
				return 0, err
			}
			var deleted int64
			err := tx.QueryRow(ctx, `WITH doomed AS (
				DELETE FROM `+sc+`.objects WHERE key = ANY($1::text[])
				RETURNING key, file_id, parts, (expires_at IS NULL OR expires_at > now()) AS live
			), dead AS (
				SELECT file_id FROM doomed WHERE file_id IS NOT NULL
				UNION ALL
				SELECT unnest(parts) FROM doomed WHERE parts IS NOT NULL
			), queued AS (
				INSERT INTO `+sc+`.garbage (file_id)
				SELECT DISTINCT d.file_id FROM dead d
				WHERE d.file_id IS NOT NULL
				  AND NOT EXISTS (SELECT 1 FROM `+sc+`.objects o
				    WHERE o.file_id IS NOT NULL AND o.file_id = d.file_id
				      AND NOT (o.key = ANY($1::text[])))
				  AND NOT EXISTS (SELECT 1 FROM `+sc+`.objects o
				    WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[d.file_id]
				      AND NOT (o.key = ANY($1::text[])))
				  AND NOT EXISTS (SELECT 1 FROM `+sc+`.upload_parts p WHERE p.file_id = d.file_id)
				ON CONFLICT DO NOTHING
			)
			SELECT count(*) FILTER (WHERE live) FROM doomed`, keys).Scan(&deleted)
			return deleted, err
		})
	})
}

type Listed struct {
	Key string
	ObjectInfo
}

func (s *Store) List(ctx context.Context, ref ns.Ref, prefix, prefixEnd, after string, limit int) ([]Listed, error) {
	return withSchema(ctx, s, ref, ErrNoBucket, nil, func(sc string) ([]Listed, error) {
		query := `SELECT key, size, etag, COALESCE(origin_etag, ''), COALESCE(origin_modified, created_at)
			FROM ` + sc + `.objects
			WHERE key COLLATE "C" >= $1 AND key COLLATE "C" < $2
			  AND key COLLATE "C" > $3
			  AND (expires_at IS NULL OR expires_at > now())
			  AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')
			ORDER BY key COLLATE "C" LIMIT $4`
		rows, err := s.pool.Query(ctx, query, prefix, prefixEnd, after, limit)
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Listed, error) {
			var l Listed
			err := row.Scan(&l.Key, &l.Size, &l.ETag, &l.OriginETag, &l.CreatedAt)
			return l, err
		})
	})
}

func (s *Store) PendingUploadCounts(ctx context.Context) (live, dead int64, err error) {
	err = s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE NOT dead),
		count(*) FILTER (WHERE dead) FROM s3p.pending_uploads`).Scan(&live, &dead)
	return live, dead, err
}

func (s *Store) PendingUploadStats(ctx context.Context) (live, dead, bytes int64, oldest *time.Time, err error) {
	var ts pgtype.Timestamptz
	err = s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE NOT dead),
		count(*) FILTER (WHERE dead),
		COALESCE(sum(size) FILTER (WHERE NOT dead), 0),
		min(created_at) FILTER (WHERE NOT dead)
		FROM s3p.pending_uploads`).Scan(&live, &dead, &bytes, &ts)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	if ts.Valid {
		t := ts.Time
		oldest = &t
	}
	return live, dead, bytes, oldest, nil
}

type BucketInfo struct {
	Name    string
	Created time.Time
}

func (s *Store) Buckets(ctx context.Context) ([]BucketInfo, error) {
	infos, err := s.Namespaces(ctx, ns.Bucket)
	if err != nil {
		return nil, err
	}
	out := make([]BucketInfo, len(infos))
	for i, n := range infos {
		out[i] = BucketInfo{Name: n.Name, Created: n.CreatedAt}
	}
	return out, nil
}

type chunkRowReader struct {
	rows      pgx.Rows
	piece     piece
	spanStart int64
	spanEnd   int64
	nextNo    int32
	early     map[int32][]byte
	decomp    []byte
	decompPtr *[]byte
	fill      *blockFill
}

func newChunkRowReader(rows pgx.Rows, p piece, start, end int64, fill *blockFill) *chunkRowReader {
	scratch := decompPool.Get().(*[]byte)
	return &chunkRowReader{
		rows: rows, piece: p, spanStart: start, spanEnd: end,
		nextNo: p.lo, decomp: *scratch, decompPtr: scratch, fill: fill,
	}
}

func (r *chunkRowReader) close() {
	if r.rows != nil {
		r.rows.Close()
		r.rows = nil
	}
	if r.decompPtr != nil {
		decompPool.Put(r.decompPtr)
		r.decompPtr = nil
		r.decomp = nil
	}
}

func (r *chunkRowReader) next() (int32, []byte, error) {
	if data, ok := r.early[r.nextNo]; ok {
		no := r.nextNo
		row, err := decodeRow(r.decomp[:0], r.piece.segLen, no, data)
		if err != nil {
			return 0, nil, err
		}
		delete(r.early, no)
		r.nextNo++
		r.fill.add(no, row)
		return no, row, nil
	}
	for r.rows.Next() {
		raw := r.rows.RawValues()
		if len(raw) != 2 || len(raw[0]) != 4 || raw[1] == nil {
			return 0, nil, fmt.Errorf("file %d rows %d..=%d: unexpected row shape", r.piece.fileID, r.piece.lo, r.piece.hi)
		}
		no := int32(uint32(raw[0][0])<<24 | uint32(raw[0][1])<<16 | uint32(raw[0][2])<<8 | uint32(raw[0][3]))
		if no < r.nextNo {
			continue
		}
		if no > r.nextNo {
			if r.early == nil {
				r.early = make(map[int32][]byte)
			}
			r.early[no] = append([]byte(nil), raw[1]...)
			continue
		}
		row, err := decodeRow(r.decomp[:0], r.piece.segLen, no, raw[1])
		if err != nil {
			return 0, nil, err
		}
		r.nextNo++
		r.fill.add(no, row)
		return no, row, nil
	}
	if err := r.rows.Err(); err != nil {
		return 0, nil, err
	}
	if r.nextNo <= r.piece.hi {
		return 0, nil, fmt.Errorf("file %d rows %d..=%d: row %d missing", r.piece.fileID, r.piece.lo, r.piece.hi, r.nextNo)
	}
	return 0, nil, io.EOF
}

func (r *chunkRowReader) appendRow(out *[]byte, no int32, data []byte) {
	appendRow(out, r.piece.base, no, data, r.spanStart, r.spanEnd)
}

const (
	zstdWindow             = 64 << 10
	zstdEncoderConcurrency = 4
	zstdDecoderConcurrency = 8
)

var (
	zstdEncoder = mustZstdEncoder()
	zstdDecoder = mustZstdDecoder()
)

func mustZstdEncoder() *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(zstdEncoderConcurrency),
		zstd.WithWindowSize(zstdWindow),
	)
	if err != nil {
		panic("initializing zstd encoder: " + err.Error())
	}
	return enc
}

func mustZstdDecoder() *zstd.Decoder {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(zstdDecoderConcurrency),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(zstdWindow),
		zstd.WithDecoderMaxMemory(1<<20),
	)
	if err != nil {
		panic("initializing zstd decoder: " + err.Error())
	}
	return dec
}

func compressBlock(dst, src []byte) []byte {
	n := len(src)
	if n < 16 || n > MaxInlineBytes {
		return src
	}
	if likelyIncompressible(src) {
		return src
	}
	if cap(dst) < n {
		dst = make([]byte, 0, n)
	} else {
		dst = dst[:0]
	}
	dst = zstdEncoder.EncodeAll(src, dst)
	if len(dst) >= n {
		return src
	}
	return dst
}

func likelyIncompressible(src []byte) bool {
	const sample = 512
	n := min(len(src), sample)
	var seen [256]bool
	distinct := 0
	for _, b := range src[:n] {
		if !seen[b] {
			seen[b] = true
			distinct++
		}
	}
	return distinct > 200
}

func decompressBlock(dst, src []byte, wantLen int) ([]byte, error) {
	if wantLen == 0 {
		return []byte{}, nil
	}
	if len(src) == wantLen {
		return src, nil
	}
	if len(src) > wantLen {
		return nil, fmt.Errorf("corrupt block: stored %d > logical %d", len(src), wantLen)
	}
	if cap(dst) < wantLen {
		dst = make([]byte, 0, wantLen)
	} else {
		dst = dst[:0]
	}
	out, err := zstdDecoder.DecodeAll(src, dst)
	if err != nil {
		return nil, fmt.Errorf("corrupt zstd block: %w", err)
	}
	if len(out) != wantLen {
		return nil, fmt.Errorf("corrupt block: decoded %d bytes, want %d", len(out), wantLen)
	}
	return out, nil
}
