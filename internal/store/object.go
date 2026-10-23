package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adonm/pgvs3/internal/ns"
)

var (
	ErrNotFound     = errors.New("object not found")
	ErrInvalidRange = errors.New("invalid range")
	ErrNotModified  = errors.New("not modified")
)

// ReadCondition mirrors the conditional read headers used by S3 clients.
type ReadCondition struct {
	IfMatch     string
	IfNoneMatch string
}

// SliceMeta is everything but the body of a served range.
type SliceMeta struct {
	Size         int64
	ETag         string
	OriginETag   string
	UserMetadata []string
	ContentType  string
	CreatedAt    time.Time
	FileID       int64
	Parts        []int64
	PartEnds     []int64
	Start        int64
	End          int64
}

func (m *SliceMeta) Len() int64 {
	if m.End < m.Start {
		return 0
	}
	return m.End - m.Start + 1
}

// Meta reads object metadata (and inline bytes for objects stored inline) from
// the bucket's objects table in PostgreSQL shared_buffers.
func (s *Store) Meta(ctx context.Context, bucket, key string) (*Meta, error) {
	return s.MetaKind(ctx, ns.Bucket, bucket, key)
}

// MetaKind reads metadata from any namespace kind.
func (s *Store) MetaKind(ctx context.Context, kind ns.Kind, name, key string) (*Meta, error) {
	return s.metaKind(ctx, kind, name, key, true)
}

// MetaHeader reads object metadata without transferring inline_data.
func (s *Store) MetaHeader(ctx context.Context, bucket, key string) (*Meta, error) {
	return s.MetaHeaderKind(ctx, ns.Bucket, bucket, key)
}

// MetaHeaderKind reads metadata from any namespace kind without inline bytes.
func (s *Store) MetaHeaderKind(ctx context.Context, kind ns.Kind, name, key string) (*Meta, error) {
	return s.metaKind(ctx, kind, name, key, false)
}

func (s *Store) metaKind(ctx context.Context, kind ns.Kind, name, key string, withInline bool) (*Meta, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return nil, err
	}
	var (
		m         Meta
		epoch     float64
		parts     []int64
		ends      []int64
		fileID    pgtype.Int8
		inline    []byte
		hasInline bool
		expires   pgtype.Timestamptz
		origin    pgtype.Text
	)
	inlineExpr := "NULL::bytea, false"
	if withInline {
		inlineExpr = "inline_data, inline_data IS NOT NULL"
	}
	err = s.pool.QueryRow(ctx, `SELECT file_id, size, sha256, EXTRACT(EPOCH FROM created_at)::float8,
		parts, part_ends, etag, user_metadata, content_type, `+inlineExpr+`, expires_at, origin_etag
		FROM `+sc+`.objects WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`, key).
		Scan(&fileID, &m.Size, &m.SHA256, &epoch, &parts, &ends, &m.ETag, &m.UserMetadata,
			&m.ContentType, &inline, &hasInline, &expires, &origin)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	m.CreatedAt = time.Unix(0, int64(epoch*1e9))
	m.Parts, m.PartEnds = parts, ends
	if fileID.Valid {
		m.FileID = fileID.Int64
	}
	if expires.Valid {
		t := expires.Time
		m.ExpiresAt = &t
	}
	if origin.Valid {
		m.OriginETag = origin.String
	}
	if hasInline {
		raw, err := decompressBlock(nil, inline, int(m.Size))
		if err != nil {
			return nil, err
		}
		m.Inline = raw
	}
	return &m, nil
}

func (s *Store) metaForRead(ctx context.Context, kind ns.Kind, name, key string, cond *ReadCondition) (*Meta, error) {
	m, err := s.MetaKind(ctx, kind, name, key)
	if err != nil {
		return nil, err
	}
	if cond == nil {
		return m, nil
	}
	if cond.IfMatch != "" && !ETagMatches(cond.IfMatch, m.ETag) {
		return nil, ErrPrecondition
	}
	if cond.IfNoneMatch != "" && ETagMatches(cond.IfNoneMatch, m.ETag) {
		return nil, ErrNotModified
	}
	return m, nil
}

// ETagMatches accepts quoted and unquoted strong ETags; a comma-separated list
// and "*" are also valid per RFC 9110.
func ETagMatches(header, etag string) bool {
	want := strings.Trim(strings.TrimPrefix(strings.TrimSpace(etag), "W/"), `"`)
	for _, token := range strings.Split(header, ",") {
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

func effRange(size, first, last, suffix int64) (int64, int64) {
	if suffix >= 0 {
		return max(size-suffix, 0), size - 1
	}
	end := size - 1
	if last >= 0 {
		end = min(last, size-1)
	}
	return max(first, 0), end
}

type piece struct {
	fileID int64
	base   int64
	segLen int64
	lo, hi int32
}

func planPieces(m *Meta, start, end int64, wholeMax int64) ([]piece, error) {
	segments, err := m.Segments()
	if err != nil {
		return nil, err
	}
	// One query for a single-file object up to 64 MiB: PostgreSQL streams the
	// rows, and avoiding the snapshot/piece machinery wins at low concurrency.
	if m.Parts == nil && end-start+1 <= wholeMax {
		seg := segments[0]
		lo := int32((max(start, seg.Base) - seg.Base) / RowBytes)
		hi := int32((min(end, seg.Base+seg.Length-1) - seg.Base) / RowBytes)
		return []piece{{fileID: seg.FileID, base: seg.Base, segLen: seg.Length, lo: lo, hi: hi}}, nil
	}
	var out []piece
	for _, seg := range segments {
		if seg.Length <= 0 || seg.Base+seg.Length-1 < start || seg.Base > end {
			continue
		}
		lo := int32((max(start, seg.Base) - seg.Base) / RowBytes)
		hi := int32((min(end, seg.Base+seg.Length-1) - seg.Base) / RowBytes)
		for r := lo; r <= hi; r += partRows {
			out = append(out, piece{fileID: seg.FileID, base: seg.Base, segLen: seg.Length, lo: r, hi: min(r+partRows-1, hi)})
		}
	}
	return out, nil
}

type result struct {
	b   []byte
	err error
}

// Body is a cut-through response body: the first chunk was fetched before the
// response started, the rest arrive in row order from PostgreSQL.
type Body struct {
	cur    []byte
	ch     <-chan result
	cancel context.CancelFunc
	err    error
	once   sync.Once

	// Direct mode: one piece streamed synchronously from pgx rows, with no
	// goroutine/channel hop on the common single-file read path.
	rows      pgx.Rows
	conn      *pgxpool.Conn
	piece     piece
	spanStart int64
	spanEnd   int64
	next      int32
	early     map[int32][]byte
}

func (b *Body) Read(p []byte) (int, error) {
	if b.rows != nil {
		for len(b.cur) == 0 {
			if b.err != nil {
				return 0, b.err
			}
			chunk, err := b.nextChunk()
			if err != nil {
				b.err = err
				return 0, err
			}
			b.cur = chunk
		}
		n := copy(p, b.cur)
		b.cur = b.cur[n:]
		return n, nil
	}
	for len(b.cur) == 0 {
		if b.err != nil {
			return 0, b.err
		}
		r, ok := <-b.ch
		if !ok {
			b.err = io.EOF
			return 0, io.EOF
		}
		if r.err != nil {
			b.err = r.err
			return 0, r.err
		}
		b.cur = r.b
	}
	n := copy(p, b.cur)
	b.cur = b.cur[n:]
	return n, nil
}

// WriteTo writes each PostgreSQL chunk straight to the response, avoiding the
// extra copy io.Copy would make through Read.
func (b *Body) WriteTo(w io.Writer) (int64, error) {
	var n int64
	write := func(p []byte) error {
		if len(p) == 0 {
			return nil
		}
		m, err := w.Write(p)
		n += int64(m)
		return err
	}
	if err := write(b.cur); err != nil {
		return n, err
	}
	b.cur = nil
	if b.rows != nil {
		for {
			chunk, err := b.nextChunk()
			if err == io.EOF {
				return n, nil
			}
			if err != nil {
				return n, err
			}
			if err := write(chunk); err != nil {
				return n, err
			}
		}
	}
	for {
		if b.err != nil {
			if b.err == io.EOF {
				return n, nil
			}
			return n, b.err
		}
		r, ok := <-b.ch
		if !ok {
			b.err = io.EOF
			return n, nil
		}
		if r.err != nil {
			b.err = r.err
			return n, r.err
		}
		if err := write(r.b); err != nil {
			return n, err
		}
	}
}

func (b *Body) Close() error {
	b.once.Do(func() {
		if b.rows != nil {
			b.rows.Close()
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

// decodeRow decompresses one stored chunk row into its logical length.
func decodeRow(dst []byte, segLen int64, no int32, stored []byte) ([]byte, error) {
	want := int(min(int64(RowBytes), segLen-int64(no)*RowBytes))
	if want < 0 {
		return nil, fmt.Errorf("row %d beyond segment length %d", no, segLen)
	}
	return decompressBlock(dst, stored, want)
}

// nextChunk pulls rows until it has a response-sized chunk. It preserves the
// same gap/early-row handling as streamRows while running on the caller's
// goroutine.
func (b *Body) nextChunk() ([]byte, error) {
	capHint := int64(chunkSize + RowBytes)
	if span := b.spanEnd - b.spanStart + 1; span < capHint {
		capHint = span + RowBytes
	}
	out := make([]byte, 0, int(capHint))
	maybe := decompPool.Get().(*[]byte)
	defer decompPool.Put(maybe)
	decomp := *maybe
	for b.rows.Next() {
		raw := b.rows.RawValues()
		if len(raw) != 2 || raw[0] == nil || raw[1] == nil {
			return nil, fmt.Errorf("file %d rows %d..=%d: unexpected row shape", b.piece.fileID, b.piece.lo, b.piece.hi)
		}
		no := int32(uint32(raw[0][0])<<24 | uint32(raw[0][1])<<16 | uint32(raw[0][2])<<8 | uint32(raw[0][3]))
		if no < b.next {
			continue
		}
		if no != b.next {
			if b.early == nil {
				b.early = make(map[int32][]byte)
			}
			b.early[no] = append([]byte(nil), raw[1]...)
			continue
		}
		dec, err := decodeRow(decomp[:0], b.piece.segLen, no, raw[1])
		if err != nil {
			return nil, err
		}
		appendRow(&out, b.piece.base, no, dec, b.spanStart, b.spanEnd)
		b.next++
		for {
			data, ok := b.early[b.next]
			if !ok {
				break
			}
			dec, err := decodeRow(decomp[:0], b.piece.segLen, b.next, data)
			if err != nil {
				return nil, err
			}
			appendRow(&out, b.piece.base, b.next, dec, b.spanStart, b.spanEnd)
			delete(b.early, b.next)
			b.next++
		}
		if len(out) >= chunkSize {
			return out, nil
		}
	}
	if err := b.rows.Err(); err != nil {
		return nil, err
	}
	if b.next <= b.piece.hi {
		return nil, fmt.Errorf("file %d rows %d..=%d: row %d missing", b.piece.fileID, b.piece.lo, b.piece.hi, b.next)
	}
	if len(out) > 0 {
		return out, nil
	}
	return nil, io.EOF
}

// GetBody serves [start,end] of an object. first/last/suffix follow S3 range
// rules; suffix >= 0 means the last suffix bytes. Conditional failures return
// ErrPrecondition and ErrNotModified before any bytes are sent.
func (s *Store) GetBody(ctx context.Context, bucket, key string, first, last, suffix int64, cond *ReadCondition) (*SliceMeta, *Body, error) {
	return s.GetBodyKind(ctx, ns.Bucket, bucket, key, first, last, suffix, cond)
}

// GetBodyKind serves a range from any namespace kind.
func (s *Store) GetBodyKind(ctx context.Context, kind ns.Kind, name, key string, first, last, suffix int64, cond *ReadCondition) (*SliceMeta, *Body, error) {
	for attempt := 0; attempt < 2; attempt++ {
		m, err := s.metaForRead(ctx, kind, name, key, cond)
		if err != nil {
			return nil, nil, err
		}
		mstart, mend := effRange(m.Size, first, last, suffix)
		if m.Size > 0 && (mstart > mend || mstart >= m.Size) {
			return nil, nil, ErrInvalidRange
		}
		sm := &SliceMeta{
			Size: m.Size, ETag: m.ETag, OriginETag: m.OriginETag, UserMetadata: m.UserMetadata,
			ContentType: m.ContentType, CreatedAt: m.CreatedAt,
			FileID: m.FileID, Parts: m.Parts, PartEnds: m.PartEnds,
			Start: mstart, End: mend,
		}
		if m.Size == 0 || sm.Len() == 0 {
			return sm, bytesBody(nil), nil
		}
		if m.Inline != nil {
			return sm, bytesBody(m.Inline[sm.Start : sm.End+1]), nil
		}
		streamCtx, cancel := context.WithCancel(ctx)
		body, err := s.startStream(streamCtx, cancel, kind, name, key, m, mstart, mend)
		if err != nil {
			cancel()
			if attempt == 0 {
				continue
			}
			return nil, nil, err
		}
		return sm, body, nil
	}
	return nil, nil, ErrNotFound
}

func bytesBody(data []byte) *Body {
	ch := make(chan result)
	close(ch)
	return &Body{cur: data, ch: ch}
}

func (s *Store) startSingleStream(ctx context.Context, cancel context.CancelFunc, schema string, p piece, start, end int64) (*Body, error) {
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
		rows: rows, conn: conn, cancel: cancel, piece: p,
		spanStart: start, spanEnd: end, next: p.lo,
	}
	chunk, err := body.nextChunk()
	if err != nil {
		_ = body.Close()
		return nil, err
	}
	body.cur = chunk
	return body, nil
}

func (s *Store) startStream(ctx context.Context, cancel context.CancelFunc, kind ns.Kind, name, key string, m *Meta, start, end int64) (*Body, error) {
	schema, err := nsSQLIdent(kind, name)
	if err != nil {
		return nil, err
	}
	pieces, err := planPieces(m, start, end, wholeReadMax)
	if err != nil {
		return nil, err
	}
	// Synchronous rows avoid a scheduler hop for ordinary file reads, but the
	// cut-through goroutine overlaps PostgreSQL fetch with socket writes and
	// wins on long single-piece spans.
	if len(pieces) == 1 && end-start+1 <= 8<<20 {
		body, err := s.startSingleStream(ctx, cancel, schema, pieces[0], start, end)
		if err != nil {
			return nil, err
		}
		return body, nil
	}
	ch := make(chan result, 32)
	body := &Body{ch: ch, cancel: cancel}
	go func() {
		defer close(ch)
		s.streamSpan(ctx, ch, schema, key, m.FileID, pieces, start, end)
	}()
	first, ok := <-ch
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}
	if first.err != nil {
		return nil, first.err
	}
	body.cur = first.b
	return body, nil
}

func (s *Store) streamSpan(ctx context.Context, ch chan<- result, schema, key string, rootID int64, pieces []piece, start, end int64) {
	send := func(b []byte) bool {
		select {
		case ch <- result{b: b}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if len(pieces) > 1 && len(pieces) <= maxParallelPieces {
		if err := s.streamParallel(ctx, ch, schema, key, rootID, pieces, start, end); err != nil {
			select {
			case ch <- result{err: err}:
			case <-ctx.Done():
			}
		}
		return
	}
	if len(pieces) > 1 {
		tx, err := s.chunkPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			select {
			case ch <- result{err: err}:
			case <-ctx.Done():
			}
			return
		}
		defer tx.Rollback(context.WithoutCancel(ctx))
		for _, p := range pieces {
			if err := streamRows(ctx, tx, schema, p, start, end, send); err != nil {
				select {
				case ch <- result{err: err}:
				case <-ctx.Done():
				}
				return
			}
		}
		return
	}
	for _, p := range pieces {
		if err := s.streamPiece(ctx, ch, schema, key, rootID, p, start, end, send); err != nil {
			select {
			case ch <- result{err: err}:
			case <-ctx.Done():
			}
			return
		}
	}
}

// maxParallelPieces bounds the number of connections one GET may borrow. An
// 8 MiB piece is one query; DuckLake's 64 MiB files become eight parallel
// range reads under one exported snapshot.
const maxParallelPieces = 8

// streamParallel fetches each piece on its own connection under a single
// exported repeatable-read snapshot. Piece channels are drained in order, so
// the response stays byte-ordered while PostgreSQL reads the pieces in
// parallel.
func (s *Store) streamParallel(ctx context.Context, out chan<- result, schema, _ string, _ int64, pieces []piece, start, end int64) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	exportConn, err := s.chunkPool.Acquire(runCtx)
	if err != nil {
		return err
	}
	defer exportConn.Release()
	exportTx, err := exportConn.BeginTx(runCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer exportTx.Rollback(context.WithoutCancel(runCtx))
	var snapshot string
	if err := exportTx.QueryRow(runCtx, "SELECT pg_export_snapshot()").Scan(&snapshot); err != nil {
		return err
	}
	chans := make([]chan result, len(pieces))
	for i := range chans {
		chans[i] = make(chan result, 4)
	}
	var wg sync.WaitGroup
	for i, p := range pieces {
		wg.Add(1)
		go func(p piece, ch chan result) {
			defer wg.Done()
			defer close(ch)
			conn, err := s.chunkPool.Acquire(runCtx)
			if err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
				return
			}
			defer conn.Release()
			tx, err := conn.BeginTx(runCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
			if err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
				return
			}
			defer tx.Rollback(context.WithoutCancel(runCtx))
			if _, err := tx.Exec(runCtx, "SET TRANSACTION SNAPSHOT '"+snapshot+"'"); err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
				return
			}
			send := func(b []byte) bool {
				select {
				case ch <- result{b: b}:
					return true
				case <-runCtx.Done():
					return false
				}
			}
			if err := streamRows(runCtx, tx, schema, p, start, end, send); err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
				return
			}
			_ = tx.Commit(runCtx)
		}(p, chans[i])
	}
	for _, ch := range chans {
		for r := range ch {
			if r.err != nil {
				cancel()
				wg.Wait()
				return r.err
			}
			select {
			case out <- r:
			case <-runCtx.Done():
				cancel()
				wg.Wait()
				return runCtx.Err()
			}
		}
	}
	wg.Wait()
	return nil
}

func (s *Store) streamPiece(ctx context.Context, _ chan<- result, schema string, _ string, _ int64, p piece, start, end int64, send func([]byte) bool) error {
	return streamRows(ctx, s.chunkPool, schema, p, start, end, send)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func getRangeSQL(schema string) string {
	return `SELECT c.no, c.data FROM ` + schema + `.chunks c
WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3`
}

func streamRows(ctx context.Context, q querier, schema string, p piece, start, end int64, send func([]byte) bool) error {
	rows, err := q.Query(ctx, getRangeSQL(schema), p.fileID, p.lo, p.hi)
	if err != nil {
		return err
	}
	defer rows.Close()
	capHint := int64(chunkSize + RowBytes)
	if span := end - start + 1; span < capHint {
		capHint = span + RowBytes
	}
	out := make([]byte, 0, int(capHint))
	var early map[int32][]byte
	maybe := decompPool.Get().(*[]byte)
	defer decompPool.Put(maybe)
	decomp := *maybe
	next := p.lo
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2 || raw[0] == nil || raw[1] == nil {
			return fmt.Errorf("file %d rows %d..=%d: unexpected row shape", p.fileID, p.lo, p.hi)
		}
		no := int32(uint32(raw[0][0])<<24 | uint32(raw[0][1])<<16 | uint32(raw[0][2])<<8 | uint32(raw[0][3]))
		if no != next {
			if early == nil {
				early = make(map[int32][]byte)
			}
			early[no] = append([]byte(nil), raw[1]...)
			continue
		}
		dec, err := decodeRow(decomp[:0], p.segLen, no, raw[1])
		if err != nil {
			return err
		}
		appendRow(&out, p.base, no, dec, start, end)
		next++
		for {
			data, ok := early[next]
			if !ok {
				break
			}
			dec, err := decodeRow(decomp[:0], p.segLen, next, data)
			if err != nil {
				return err
			}
			appendRow(&out, p.base, next, dec, start, end)
			delete(early, next)
			next++
		}
		if len(out) >= chunkSize {
			if !send(out) {
				return ctx.Err()
			}
			out = make([]byte, 0, int(capHint))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if next <= p.hi {
		return fmt.Errorf("file %d rows %d..=%d: row %d missing", p.fileID, p.lo, p.hi, next)
	}
	if len(out) > 0 && !send(out) {
		return ctx.Err()
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

// Get is the buffered variant used by tests and small control paths.
func (s *Store) Get(ctx context.Context, bucket, key string, first, last, suffix int64) (*Slice, error) {
	return s.GetKind(ctx, ns.Bucket, bucket, key, first, last, suffix)
}

// GetKind is the buffered variant for any namespace kind.
func (s *Store) GetKind(ctx context.Context, kind ns.Kind, name, key string, first, last, suffix int64) (*Slice, error) {
	sm, body, err := s.GetBodyKind(ctx, kind, name, key, first, last, suffix, nil)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &Slice{Size: sm.Size, ETag: sm.ETag, CreatedAt: sm.CreatedAt, Start: sm.Start, End: sm.End, Bytes: b}, nil
}

type Slice struct {
	Size      int64
	ETag      string
	CreatedAt time.Time
	Start     int64
	End       int64
	Bytes     []byte
}

// Delete removes one object and queues its storage for reaping.
func (s *Store) Delete(ctx context.Context, bucket, key string) (bool, error) {
	n, err := s.DeleteManyKind(ctx, ns.Bucket, bucket, []string{key})
	return n > 0, err
}

// DeleteMany removes up to 1000 keys in one transaction, queueing every file
// it unpublishes. Locks are taken in sorted order so concurrent batches cannot
// deadlock.
func (s *Store) DeleteMany(ctx context.Context, bucket string, keys []string) error {
	_, err := s.DeleteManyKind(ctx, ns.Bucket, bucket, keys)
	return err
}

// DeleteManyKind removes keys from a bucket or cache namespace and returns the
// number of live keys that were deleted.
func (s *Store) DeleteManyKind(ctx context.Context, kind ns.Kind, name string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return 0, err
	}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	return withTx(ctx, s, func(tx pgx.Tx) (int64, error) {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
			FROM unnest($2::text[]) AS k`, name, sorted); err != nil {
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
		if isMissingBucketSchema(err) {
			return 0, nil
		}
		return deleted, err
	})
}

type Listed struct {
	Key        string
	Size       int64
	ETag       string
	OriginETag string
	CreatedAt  time.Time
}

// List returns key-ordered objects in [prefix, prefixEnd) with key > after.
func (s *Store) List(ctx context.Context, bucket, prefix, prefixEnd, after string, limit int) ([]Listed, error) {
	return s.ListKind(ctx, ns.Bucket, bucket, prefix, prefixEnd, after, limit)
}

// ListKind returns key-ordered objects in a bucket or cache namespace.
func (s *Store) ListKind(ctx context.Context, kind ns.Kind, name, prefix, prefixEnd, after string, limit int) ([]Listed, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return nil, err
	}
	query := `SELECT key, size, etag, COALESCE(origin_etag, ''), EXTRACT(EPOCH FROM created_at)::float8
		FROM ` + sc + `.objects
		WHERE key COLLATE "C" >= $1 AND key COLLATE "C" < $2
		  AND key COLLATE "C" > $3
		  AND (expires_at IS NULL OR expires_at > now())
		ORDER BY key COLLATE "C" LIMIT $4`
	rows, err := s.pool.Query(ctx, query, prefix, prefixEnd, after, limit)
	if isMissingBucketSchema(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Listed, error) {
		var l Listed
		var epoch float64
		err := row.Scan(&l.Key, &l.Size, &l.ETag, &l.OriginETag, &epoch)
		l.CreatedAt = time.Unix(0, int64(epoch*1e9))
		return l, err
	})
	if isMissingBucketSchema(err) {
		return nil, nil
	}
	return out, err
}

// Buckets lists S3 bucket namespaces in key order.
func (s *Store) Buckets(ctx context.Context) ([]struct {
	Name    string
	Created time.Time
}, error) {
	infos, err := s.Namespaces(ctx, ns.Bucket)
	if err != nil {
		return nil, err
	}
	out := make([]struct {
		Name    string
		Created time.Time
	}, 0, len(infos))
	for _, n := range infos {
		out = append(out, struct {
			Name    string
			Created time.Time
		}{n.Name, n.CreatedAt})
	}
	return out, nil
}
