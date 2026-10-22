package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaFS embed.FS

// LayoutVersion is the storage layout this binary reads and writes. There is
// no in-place migration: a different layout requires a fresh database.
const LayoutVersion = 9

const (
	smallMax     = 8 << 20
	wholeReadMax = 64 << 20
	partRows     = smallMax/RowBytes + 2
	chunkSize    = 256 << 10
	reapRows     = 65536
)

// Store is the PostgreSQL-backed object store. Metadata and inline objects
// live in s3p.objects and are served directly from PostgreSQL shared_buffers.
type Store struct {
	pool      *pgxpool.Pool
	chunkPool *pgxpool.Pool
	log       *slog.Logger

	fileIDMu    sync.Mutex
	fileIDBlock []int64
	fileIDPos   int

	reapedRows      atomic.Int64
	reapedFiles     atomic.Int64
	maintenanceRuns atomic.Int64
}

// Open connects and warms the pool. TLS is configured via standard sslmode
// parameters in the PostgreSQL URL.
func Open(ctx context.Context, url string, maxConns int32, log *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if maxConns <= 0 {
		maxConns = 64
	}
	minConns := min(maxConns, 8)
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.ConnConfig.RuntimeParams["application_name"] == "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = "pgvs3"
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = minConns
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnLifetime = 0 // keep the hot working set for the most-recently-used pool
	sessionSQL := func(indexScan bool) string {
		scan := "off"
		if indexScan {
			scan = "on"
		}
		return `SET enable_indexscan = ` + scan + `;
SET work_mem = '64MB';
SET effective_io_concurrency = 32;
SET tcp_keepalives_idle = 30;
SET tcp_keepalives_interval = 10;
SET tcp_keepalives_count = 3;
SET idle_in_transaction_session_timeout = '5min';
SET synchronous_commit = on;`
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Index scans keep s3p.objects / s3p.buckets point lookups and ordered
		// listings at ~6 us in PostgreSQL shared_buffers.
		_, err := conn.Exec(ctx, sessionSQL(true))
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	// Chunk range reads use a separate pool with enable_indexscan = off so
	// PostgreSQL 18 bitmap heap scans drive the read-ahead stream.
	chunkCfg := cfg.Copy()
	chunkCfg.MaxConns = min(maxConns, 64)
	chunkCfg.MinConns = min(minConns, 4)
	chunkCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, sessionSQL(false))
		return err
	}
	chunkPool, err := pgxpool.NewWithConfig(ctx, chunkCfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	if err := chunkPool.Ping(ctx); err != nil {
		pool.Close()
		chunkPool.Close()
		return nil, err
	}
	// Open the warm set up front so a burst does not pay TCP, TLS and SCRAM.
	var wg sync.WaitGroup
	errs := make(chan error, minConns)
	for i := int32(0); i < minConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := pool.Acquire(ctx)
			if err != nil {
				errs <- err
				return
			}
			c.Release()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		pool.Close()
		chunkPool.Close()
		return nil, err
	}
	return &Store{pool: pool, chunkPool: chunkPool, log: log}, nil
}

// nextFileID hands out sequence values in blocks. The sequence itself is
// CACHE 64; batching here removes a nextval round trip from most writes.
func (s *Store) nextFileID(ctx context.Context) (int64, error) {
	s.fileIDMu.Lock()
	defer s.fileIDMu.Unlock()
	if s.fileIDPos < len(s.fileIDBlock) {
		id := s.fileIDBlock[s.fileIDPos]
		s.fileIDPos++
		return id, nil
	}
	rows, err := s.pool.Query(ctx, "SELECT nextval('s3p.file_ids') FROM generate_series(1,64)")
	if err != nil {
		return 0, err
	}
	block := make([]int64, 0, 64)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		block = append(block, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(block) == 0 {
		return 0, fmt.Errorf("no file ids allocated")
	}
	s.fileIDBlock = block
	s.fileIDPos = 0
	id := s.fileIDBlock[0]
	s.fileIDPos = 1
	return id, nil
}

func (s *Store) Close() {
	if s.chunkPool != nil {
		s.chunkPool.Close()
	}
	s.pool.Close()
}
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Init installs the layout once, under a transaction-scoped advisory lock,
// and refuses any other layout version rather than misreading it.
func (s *Store) Init(ctx context.Context) error {
	const lock int64 = 0x70677673 // "pgvs"
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lock); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lock)

	var versionNum int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&versionNum); err != nil {
		return err
	}
	if versionNum < 180000 {
		return fmt.Errorf("pgvs3 requires PostgreSQL 18 or later (found %d)", versionNum)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))

	var chunks, marked bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass('s3p.chunks') IS NOT NULL").Scan(&chunks); err != nil {
		return err
	}
	if err := tx.QueryRow(ctx, "SELECT to_regclass('s3p.layout') IS NOT NULL").Scan(&marked); err != nil {
		return err
	}
	if chunks {
		if !marked {
			return fmt.Errorf("s3p.layout is missing: refusing to guess the storage layout")
		}
		var found pgtype.Int4
		if err := tx.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&found); err != nil {
			return err
		}
		if !found.Valid || found.Int32 != LayoutVersion {
			return fmt.Errorf("s3p holds storage layout %v; this pgvs3 reads v%d (migrate explicitly, or use a fresh database)", found.Int32, LayoutVersion)
		}
		// Re-apply the idempotent table/function definitions so a database
		// created by an earlier v9 build gets new SQL helpers such as
		// s3p.commit_part. Tables and indexes only use IF NOT EXISTS.
		schema, err := schemaFS.ReadFile("schema.sql")
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(schema)); err != nil {
			return err
		}
		// Metadata-only CopyObject shares file IDs between object
		// rows. Older v8 databases have a UNIQUE constraint that prevents it;
		// dropping it is safe because the reaper checks every reference.
		if _, err := tx.Exec(ctx, "ALTER TABLE s3p.objects DROP CONSTRAINT IF EXISTS objects_file_id_key"); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if marked {
		return fmt.Errorf("s3p.layout exists without chunks: refusing a partial layout")
	}
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, string(schema)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO s3p.layout (version) VALUES ($1)", LayoutVersion); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EnsureMaintenance verifies that the layout has the inline-storage,
// autovacuum and GC relations this gateway relies on. Maintenance itself runs
// in-process (see Maintain), so pg_cron is not required.
// CheckLayout verifies the installed layout without running DDL. Used by
// read-only gateways connected to a primary or a streaming standby.
func (s *Store) CheckLayout(ctx context.Context) error {
	var version pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&version); err != nil {
		return err
	}
	if !version.Valid || version.Int32 != LayoutVersion {
		return fmt.Errorf("storage layout is %v, want v%d", version.Int32, LayoutVersion)
	}
	return nil
}

func (s *Store) EnsureMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var kind string
	var opts []string
	if err := conn.QueryRow(ctx, `SELECT relkind::text, reloptions FROM pg_class WHERE oid = 's3p.chunks'::regclass`).
		Scan(&kind, &opts); err != nil {
		return err
	}
	want := []string{
		"toast_tuple_target=8160",
		"autovacuum_vacuum_scale_factor=0.01",
		"autovacuum_analyze_scale_factor=0.02",
		"autovacuum_vacuum_threshold=1000",
	}
	hasAll := func(have []string) bool {
		for _, w := range want {
			found := false
			for _, h := range have {
				if h == w {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	if kind == "p" {
		// Storage parameters are not allowed on a partitioned parent; every
		// leaf must carry the inline-page and autovacuum settings.
		var total, configured int
		if err := conn.QueryRow(ctx, `SELECT count(*),
			count(*) FILTER (WHERE c.reloptions @> ARRAY['toast_tuple_target=8160',
				'autovacuum_vacuum_scale_factor=0.01',
				'autovacuum_analyze_scale_factor=0.02',
				'autovacuum_vacuum_threshold=1000'])
			FROM pg_partition_tree('s3p.chunks') t
			JOIN pg_class c ON c.oid = t.relid
			WHERE t.isleaf`).Scan(&total, &configured); err != nil {
			return err
		}
		if total == 0 || total != configured {
			return fmt.Errorf("s3p.chunks partitions lack the expected inline storage and autovacuum settings")
		}
	} else if kind != "r" || !hasAll(opts) {
		return fmt.Errorf("s3p.chunks lacks the expected inline storage and autovacuum settings")
	}
	for _, relation := range []string{"s3p.garbage", "s3p.objects_parts_idx", "s3p.upload_parts"} {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required layout relation %s is missing", relation)
		}
	}
	return nil
}

// Meta reads object metadata (and inline bytes for objects <= 64 KiB) from
// s3p.objects in PostgreSQL shared_buffers.
func (s *Store) Meta(ctx context.Context, bucket, key string) (*Meta, error) {
	var m Meta
	var epoch float64
	var parts, ends []int64
	var fileID pgtype.Int8
	var inline []byte
	var hasInline bool
	err := s.pool.QueryRow(ctx, `SELECT file_id, size, sha256, EXTRACT(EPOCH FROM created_at)::float8,
		parts, part_ends, etag, user_metadata, content_type, inline_data, inline_data IS NOT NULL
		FROM s3p.objects WHERE bucket = $1 AND key = $2`, bucket, key).
		Scan(&fileID, &m.Size, &m.SHA256, &epoch, &parts, &ends, &m.ETag, &m.UserMetadata, &m.ContentType, &inline, &hasInline)
	if errors.Is(err, pgx.ErrNoRows) {
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
	if hasInline {
		if inline == nil {
			inline = []byte{}
		}
		m.Inline = inline
	}
	return &m, nil
}

// MetaHeader reads object metadata without transferring inline_data.
func (s *Store) MetaHeader(ctx context.Context, bucket, key string) (*Meta, error) {
	var m Meta
	var epoch float64
	var parts, ends []int64
	var fileID pgtype.Int8
	err := s.pool.QueryRow(ctx, `SELECT file_id, size, sha256, EXTRACT(EPOCH FROM created_at)::float8,
		parts, part_ends, etag, user_metadata, content_type
		FROM s3p.objects WHERE bucket = $1 AND key = $2`, bucket, key).
		Scan(&fileID, &m.Size, &m.SHA256, &epoch, &parts, &ends, &m.ETag, &m.UserMetadata,
			&m.ContentType)
	if errors.Is(err, pgx.ErrNoRows) {
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
	return &m, nil
}

var (
	ErrNotFound     = errors.New("object not found")
	ErrInvalidRange = errors.New("invalid range")
	ErrNotModified  = errors.New("not modified")
)

// ReadCondition mirrors the conditional read headers used by DuckDB 2.0
// (If-Match) and SlateDB (If-None-Match).
type ReadCondition struct {
	IfMatch     string
	IfNoneMatch string
}

// SliceMeta is everything but the body of a served range.
type SliceMeta struct {
	Size         int64
	ETag         string
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

// nextChunk pulls rows until it has a response-sized chunk. It preserves the
// same gap/early-row handling as streamRows while running on the caller's
// goroutine.
func (b *Body) nextChunk() ([]byte, error) {
	capHint := int64(chunkSize + RowBytes)
	if span := b.spanEnd - b.spanStart + 1; span < capHint {
		capHint = span + RowBytes
	}
	out := make([]byte, 0, int(capHint))
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
		appendRow(&out, b.piece.base, no, raw[1], b.spanStart, b.spanEnd)
		b.next++
		for {
			data, ok := b.early[b.next]
			if !ok {
				break
			}
			appendRow(&out, b.piece.base, b.next, data, b.spanStart, b.spanEnd)
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
	for attempt := 0; attempt < 2; attempt++ {
		m, err := s.metaForRead(ctx, bucket, key, cond)
		if err != nil {
			return nil, nil, err
		}
		mstart, mend := effRange(m.Size, first, last, suffix)
		if m.Size > 0 && (mstart > mend || mstart >= m.Size) {
			return nil, nil, ErrInvalidRange
		}
		sm := &SliceMeta{
			Size: m.Size, ETag: m.ETag, UserMetadata: m.UserMetadata,
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
		body, err := s.startStream(streamCtx, cancel, bucket, key, m, mstart, mend)
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

func (s *Store) metaForRead(ctx context.Context, bucket, key string, cond *ReadCondition) (*Meta, error) {
	m, err := s.Meta(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if cond == nil {
		return m, nil
	}
	if cond.IfMatch != "" && !etagMatches(cond.IfMatch, m.ETag) {
		return nil, ErrPrecondition
	}
	if cond.IfNoneMatch != "" && etagMatches(cond.IfNoneMatch, m.ETag) {
		return nil, ErrNotModified
	}
	return m, nil
}

// etagMatches accepts quoted and unquoted strong ETags; a comma-separated list
// and "*" are also valid per RFC 9110.
func etagMatches(header, etag string) bool {
	for _, want := range strings.Split(header, ",") {
		want = strings.TrimSpace(want)
		if want == "*" {
			return true
		}
		want = strings.TrimPrefix(want, "W/")
		want = strings.Trim(want, `"`)
		if want == etag {
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
		return []piece{{fileID: seg.FileID, base: seg.Base, lo: lo, hi: hi}}, nil
	}
	var out []piece
	for _, seg := range segments {
		if seg.Length <= 0 || seg.Base+seg.Length-1 < start || seg.Base > end {
			continue
		}
		lo := int32((max(start, seg.Base) - seg.Base) / RowBytes)
		hi := int32((min(end, seg.Base+seg.Length-1) - seg.Base) / RowBytes)
		for r := lo; r <= hi; r += partRows {
			out = append(out, piece{fileID: seg.FileID, base: seg.Base, lo: r, hi: min(r+partRows-1, hi)})
		}
	}
	return out, nil
}

func (s *Store) startSingleStream(ctx context.Context, cancel context.CancelFunc, p piece, start, end int64) (*Body, error) {
	conn, err := s.chunkPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, getRangeSQL, p.fileID, p.lo, p.hi)
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

func (s *Store) startStream(ctx context.Context, cancel context.CancelFunc, bucket, key string, m *Meta, start, end int64) (*Body, error) {
	pieces, err := planPieces(m, start, end, wholeReadMax)
	if err != nil {
		return nil, err
	}
	// Synchronous rows avoid a scheduler hop for ordinary file reads, but the
	// cut-through goroutine overlaps PostgreSQL fetch with socket writes and
	// wins on long single-piece spans.
	if len(pieces) == 1 && end-start+1 <= 8<<20 {
		body, err := s.startSingleStream(ctx, cancel, pieces[0], start, end)
		if err != nil {
			return nil, err
		}
		return body, nil
	}
	ch := make(chan result, 32)
	body := &Body{ch: ch, cancel: cancel}
	go func() {
		defer close(ch)
		s.streamSpan(ctx, ch, bucket, key, m.FileID, pieces, start, end)
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

func (s *Store) streamSpan(ctx context.Context, ch chan<- result, bucket, key string, rootID int64, pieces []piece, start, end int64) {
	send := func(b []byte) bool {
		select {
		case ch <- result{b: b}:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if len(pieces) > 1 && len(pieces) <= maxParallelPieces {
		if err := s.streamParallel(ctx, ch, bucket, key, rootID, pieces, start, end); err != nil {
			select {
			case ch <- result{err: err}:
			case <-ctx.Done():
			}
		}
		return
	}
	if len(pieces) > 1 {
		if err := s.streamSnapshot(ctx, ch, bucket, key, rootID, pieces, start, end, send); err != nil {
			select {
			case ch <- result{err: err}:
			case <-ctx.Done():
			}
		}
		return
	}
	for _, p := range pieces {
		if err := s.streamPiece(ctx, ch, bucket, key, rootID, p, start, end, send); err != nil {
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
func (s *Store) streamParallel(ctx context.Context, out chan<- result, bucket, key string, rootID int64, pieces []piece, start, end int64) error {
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
		go func(index int, p piece, ch chan result) {
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
			if err := streamRows(runCtx, tx, p, start, end, send); err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
				return
			}
			if err := tx.Commit(runCtx); err != nil {
				select {
				case ch <- result{err: err}:
				case <-runCtx.Done():
				}
			}
		}(i, p, chans[i])
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

func (s *Store) streamSnapshot(ctx context.Context, ch chan<- result, bucket, key string, rootID int64, pieces []piece, start, end int64, send func([]byte) bool) error {
	tx, err := s.chunkPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	for _, p := range pieces {
		if err := streamRows(ctx, tx, p, start, end, send); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) streamPiece(ctx context.Context, _ chan<- result, _ string, _ string, _ int64, p piece, start, end int64, send func([]byte) bool) error {
	return streamRows(ctx, s.chunkPool, p, start, end, send)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const getRangeSQL = `SELECT c.no, c.data FROM s3p.chunks c
WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3`

func streamRows(ctx context.Context, q querier, p piece, start, end int64, send func([]byte) bool) error {
	rows, err := q.Query(ctx, getRangeSQL, p.fileID, p.lo, p.hi)
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
		appendRow(&out, p.base, no, raw[1], start, end)
		next++
		for {
			data, ok := early[next]
			if !ok {
				break
			}
			appendRow(&out, p.base, next, data, start, end)
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
	sm, body, err := s.GetBody(ctx, bucket, key, first, last, suffix, nil)
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

// Put overwrites or creates an object through the same streaming path as PUT.
func (s *Store) Put(ctx context.Context, bucket, key string, data []byte) error {
	w, err := s.StartObject(ctx, bucket, key, Unconditional, "", nil, "application/octet-stream")
	if err != nil {
		return err
	}
	defer w.Abort()
	for len(data) > 0 {
		n := min(len(data), 4<<20)
		if _, err := w.Write(data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	_, err = w.Finish(nil)
	return err
}

// Delete removes one object and queues its storage for reaping.
func (s *Store) Delete(ctx context.Context, bucket, key string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := lockObjectKey(ctx, tx, bucket, key); err != nil {
		return false, err
	}
	var fileID pgtype.Int8
	var parts []int64
	err = tx.QueryRow(ctx, "DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id, parts", bucket, key).Scan(&fileID, &parts)
	found := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if found && fileID.Valid {
		if err := queueFiles(ctx, tx, fileID.Int64, parts); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return found, nil
}

// DeleteMany removes up to 1000 keys in one transaction, queueing every file
// it unpublishes. It is the batch path behind DeleteObjects.
func (s *Store) DeleteMany(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	// Serialise with publish_object/Copy per key without 1,000 round trips.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
		FROM unnest($2::text[]) AS k`, bucket, keys); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `WITH doomed AS (
		DELETE FROM s3p.objects WHERE bucket = $1 AND key = ANY($2::text[])
		RETURNING file_id, parts
	), dead AS (
		SELECT file_id FROM doomed UNION ALL SELECT unnest(parts) FROM doomed
	)
	INSERT INTO s3p.garbage (file_id)
	SELECT DISTINCT d.file_id FROM dead d
	WHERE d.file_id IS NOT NULL
	  AND NOT EXISTS (SELECT 1 FROM s3p.objects o
	    WHERE (o.file_id = d.file_id OR d.file_id = ANY(o.parts))
	      AND NOT (o.bucket = $1 AND o.key = ANY($2::text[])))
	  AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = d.file_id)
	ON CONFLICT DO NOTHING`, bucket, keys)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	_, err := s.pool.Exec(ctx, "INSERT INTO s3p.buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", name)
	return err
}

func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx, "SELECT 1 FROM s3p.buckets WHERE name = $1", name).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

type BucketDeletion int

const (
	BucketDeleted BucketDeletion = iota
	BucketNotEmpty
	BucketNotFound
)

func (s *Store) DeleteBucket(ctx context.Context, name string) (BucketDeletion, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var one int
	err = tx.QueryRow(ctx, "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR UPDATE", name).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return BucketNotFound, nil
	}
	if err != nil {
		return 0, err
	}
	var occupied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM s3p.objects WHERE bucket = $1)
		OR EXISTS (SELECT 1 FROM s3p.uploads WHERE bucket = $1)`, name).Scan(&occupied); err != nil {
		return 0, err
	}
	if occupied {
		return BucketNotEmpty, nil
	}
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.buckets WHERE name = $1", name); err != nil {
		return 0, err
	}
	return BucketDeleted, tx.Commit(ctx)
}

func (s *Store) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType string, checksumAlgorithm *string) (string, error) {
	if _, err := s.pool.Exec(ctx, "INSERT INTO s3p.buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", bucket); err != nil {
		return "", err
	}
	var id string
	err := s.pool.QueryRow(ctx, `INSERT INTO s3p.uploads (upload_id, bucket, key, checksum_algorithm, user_metadata, content_type)
		VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5)
		RETURNING upload_id`, bucket, key, checksumAlgorithm, userMeta, contentType).Scan(&id)
	return id, err
}

type ListedPart struct {
	PartNo   int32
	ETag     string
	Checksum *Checksum
}

type Completed struct {
	Bucket string
	Key    string
	ETag   string
	Size   int64
}

// CompleteUpload validates the listed parts and publishes the ordered part
// list without moving any data.
func (s *Store) CompleteUpload(ctx context.Context, uploadID, requestBucket, requestKey string, listed []ListedPart) (*Completed, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var bucket, key, contentType string
	var algorithm pgtype.Text
	var userMeta []string
	err = tx.QueryRow(ctx, `SELECT bucket, key, checksum_algorithm, user_metadata, content_type
		FROM s3p.uploads WHERE upload_id = $1 FOR UPDATE`, uploadID).
		Scan(&bucket, &key, &algorithm, &userMeta, &contentType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoUpload
	}
	if err != nil {
		return nil, err
	}
	if bucket != requestBucket || key != requestKey {
		return nil, ErrNoUpload
	}
	if len(listed) == 0 {
		return nil, ErrInvalidPart
	}
	for i, p := range listed {
		if p.PartNo < 1 || p.PartNo > 10000 || (i > 0 && listed[i-1].PartNo >= p.PartNo) {
			return nil, ErrInvalidPart
		}
	}
	rows, err := tx.Query(ctx, `SELECT part_no, file_id, size, sha256, md5, checksum
		FROM s3p.upload_parts WHERE upload_id = $1 ORDER BY part_no`, uploadID)
	if err != nil {
		return nil, err
	}
	type stored struct {
		no       int32
		fileID   int64
		size     int64
		sha, m5  []byte
		checksum pgtype.Text
	}
	var parts []stored
	for rows.Next() {
		var p stored
		if err := rows.Scan(&p.no, &p.fileID, &p.size, &p.sha, &p.m5, &p.checksum); err != nil {
			rows.Close()
			return nil, err
		}
		parts = append(parts, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	want := 0
	var ids, ends []int64
	var size int64
	var unused []int64
	type selectedCheck struct {
		no       int32
		fileID   int64
		checksum string
	}
	var selectedChecks []selectedCheck
	shaAll := sha256.New()
	md5All := md5.New()
	for _, p := range parts {
		if want >= len(listed) || listed[want].PartNo != p.no {
			unused = append(unused, p.fileID)
			continue
		}
		lp := listed[want]
		checksumOK := false
		switch {
		case !algorithm.Valid && lp.Checksum == nil:
			checksumOK = true
		case algorithm.Valid && lp.Checksum != nil:
			checksumOK = algorithm.String == lp.Checksum.Name && p.checksum.Valid && p.checksum.String == lp.Checksum.Value
		}
		if lp.ETag != hex.EncodeToString(p.m5) || !checksumOK {
			return nil, ErrInvalidPart
		}
		shaAll.Write(p.sha)
		md5All.Write(p.m5)
		size += p.size
		ids = append(ids, p.fileID)
		ends = append(ends, size)
		if algorithm.Valid && p.checksum.Valid {
			selectedChecks = append(selectedChecks, selectedCheck{no: p.no, fileID: p.fileID, checksum: p.checksum.String})
		}
		want++
	}
	if want != len(listed) {
		return nil, ErrInvalidPart
	}
	etag := s3ETag(md5All.Sum(nil), len(ids))
	if err := swapObjectParts(ctx, tx, bucket, key, nil, ids[0], size, shaAll.Sum(nil), etag, userMeta, contentType, ids, ends); err != nil {
		return nil, err
	}
	for _, c := range selectedChecks {
		if _, err := tx.Exec(ctx, `INSERT INTO s3p.object_part_checksums
			(bucket, key, part_no, file_id, algorithm, value) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (bucket, key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			bucket, key, c.no, c.fileID, algorithm.String, c.checksum); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.uploads WHERE upload_id = $1", uploadID); err != nil {
		return nil, err
	}
	if err := queueFileIDs(ctx, tx, unused); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Completed{Bucket: bucket, Key: key, ETag: etag, Size: size}, nil
}

func (s *Store) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var one int
	err = tx.QueryRow(ctx, "SELECT 1 FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR UPDATE", uploadID, bucket, key).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ids []int64
	rows, err := tx.Query(ctx, "SELECT file_id FROM s3p.upload_parts WHERE upload_id = $1", uploadID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.uploads WHERE upload_id = $1", uploadID); err != nil {
		return false, err
	}
	if err := queueFileIDs(ctx, tx, ids); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

type Listed struct {
	Key       string
	Size      int64
	ETag      string
	CreatedAt time.Time
}

// List returns key-ordered objects in [prefix, prefixEnd) with key > after.
func (s *Store) List(ctx context.Context, bucket, prefix, prefixEnd, after string, limit int) ([]Listed, error) {
	query := `SELECT key, size, etag, EXTRACT(EPOCH FROM created_at)::float8
		FROM s3p.objects
		WHERE bucket = $1 AND key COLLATE "C" >= $2 AND key COLLATE "C" < $3
		  AND key COLLATE "C" > $4
		ORDER BY key COLLATE "C" LIMIT $5`
	rows, err := s.pool.Query(ctx, query, bucket, prefix, prefixEnd, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Listed
	for rows.Next() {
		var l Listed
		var epoch float64
		if err := rows.Scan(&l.Key, &l.Size, &l.ETag, &epoch); err != nil {
			return nil, err
		}
		l.CreatedAt = time.Unix(0, int64(epoch*1e9))
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) Buckets(ctx context.Context) ([]struct {
	Name    string
	Created time.Time
}, error) {
	rows, err := s.pool.Query(ctx, "SELECT name, EXTRACT(EPOCH FROM created_at)::float8 FROM s3p.buckets ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct {
		Name    string
		Created time.Time
	}
	for rows.Next() {
		var n string
		var epoch float64
		if err := rows.Scan(&n, &epoch); err != nil {
			return nil, err
		}
		out = append(out, struct {
			Name    string
			Created time.Time
		}{n, time.Unix(0, int64(epoch*1e9))})
	}
	return out, rows.Err()
}

type Stats struct {
	Objects       int64          `json:"objects"`
	Chunks        int64          `json:"chunks"`
	LogicalBytes  int64          `json:"logical_bytes"`
	PhysicalBytes int64          `json:"physical_bytes"`
	DatabaseBytes int64          `json:"database_bytes"`
	GarbageFiles  int64          `json:"garbage_files"`
	OldestGarbage *time.Duration `json:"oldest_garbage,omitempty"`
}

func (s *Store) Stat(ctx context.Context) (Stats, error) {
	var st Stats
	var oldest pgtype.Float8
	err := s.pool.QueryRow(ctx, `SELECT (SELECT COALESCE(sum(size),0)::int8 FROM s3p.objects),
		pg_total_relation_size('s3p.objects') + pg_total_relation_size('s3p.chunks'),
		(SELECT count(*) FROM s3p.objects),
		(SELECT count(*) FROM s3p.chunks),
		pg_database_size(current_database()),
		(SELECT count(*) FROM s3p.garbage),
		(SELECT EXTRACT(EPOCH FROM now() - min(queued_at))::float8 FROM s3p.garbage)`).
		Scan(&st.LogicalBytes, &st.PhysicalBytes, &st.Objects, &st.Chunks, &st.DatabaseBytes, &st.GarbageFiles, &oldest)
	if err != nil {
		return st, err
	}
	if oldest.Valid {
		d := time.Duration(oldest.Float64 * float64(time.Second))
		st.OldestGarbage = &d
	}
	return st, nil
}

// Maintain expires abandoned multipart uploads and reaps garbage older than
// grace, at most reapRows chunk rows per run. A transaction-scoped try-lock
// serialises reapers across gateways.
func (s *Store) Maintain(ctx context.Context, grace time.Duration) error {
	s.maintenanceRuns.Add(1)
	const lock int64 = 0x70677674 // "pgvt"
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lock).Scan(&got); err != nil || !got {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lock)

	var expired int
	if err := conn.QueryRow(ctx, "SELECT s3p.expire_uploads(interval '24 hours', 64, NULL)").Scan(&expired); err != nil {
		return err
	}
	if expired > 0 {
		s.log.Info("expired multipart uploads", "count", expired)
	}
	remaining := reapRows
	for remaining > 0 {
		n, err := s.reapOne(ctx, conn.Conn(), grace, remaining)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		s.reapedRows.Add(int64(n))
		s.reapedFiles.Add(1)
		remaining -= n
	}
	return nil
}
func (s *Store) reapOne(ctx context.Context, conn *pgx.Conn, grace time.Duration, budget int) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var fileID int64
	err = tx.QueryRow(ctx, `SELECT file_id FROM s3p.garbage
		WHERE queued_at <= now() - make_interval(secs => $1)
		ORDER BY queued_at, file_id LIMIT 1 FOR UPDATE SKIP LOCKED`, grace.Seconds()).Scan(&fileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var referenced bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM s3p.objects WHERE file_id = $1 OR $1 = ANY(parts))
		OR EXISTS (SELECT 1 FROM s3p.upload_parts WHERE file_id = $1)`, fileID).Scan(&referenced); err != nil {
		return 0, err
	}
	if referenced {
		// A shared CopyObject/UploadPartCopy file can be queued by an older
		// gateway. Drop the false-positive garbage row instead of wedging the
		// reaper behind it forever.
		if _, err := tx.Exec(ctx, "DELETE FROM s3p.garbage WHERE file_id = $1", fileID); err != nil {
			return 0, err
		}
		return 1, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM s3p.chunks WHERE ctid IN
		(SELECT ctid FROM s3p.chunks WHERE file_id = $1 LIMIT $2)`, fileID, budget)
	if err != nil {
		return 0, err
	}
	n := int(tag.RowsAffected())
	if n < budget {
		var left bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM s3p.chunks WHERE file_id = $1)", fileID).Scan(&left); err != nil {
			return 0, err
		}
		if !left {
			if _, err := tx.Exec(ctx, "DELETE FROM s3p.garbage WHERE file_id = $1", fileID); err != nil {
				return 0, err
			}
			// A zero-byte file has no rows but still frees a garbage entry;
			// report progress so the reaper moves on to the next file.
			n = 1
		}
	}
	return n, tx.Commit(ctx)
}

func s3ETag(md5sum []byte, parts int) string {
	h := hex.EncodeToString(md5sum)
	if parts > 0 {
		return fmt.Sprintf("%s-%d", h, parts)
	}
	return h
}

// Ready checks that the database answers and the installed layout matches.
func (s *Store) Ready(ctx context.Context) error {
	var one int
	if err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	var version pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&version); err != nil {
		return err
	}
	if !version.Valid || version.Int32 != LayoutVersion {
		return fmt.Errorf("storage layout is %v, want v%d", version.Int32, LayoutVersion)
	}
	return nil
}

// WriteMetrics writes pool and maintenance counters in Prometheus text.
func (s *Store) WriteMetrics(w io.Writer) {
	st := s.pool.Stat()
	fmt.Fprintf(w, "pgvs3_pg_pool_total_conns %d\n", st.TotalConns())
	fmt.Fprintf(w, "pgvs3_pg_pool_acquired_conns %d\n", st.AcquiredConns())
	fmt.Fprintf(w, "pgvs3_pg_pool_idle_conns %d\n", st.IdleConns())
	fmt.Fprintf(w, "pgvs3_pg_pool_max_conns %d\n", st.MaxConns())
	if s.chunkPool != nil {
		cs := s.chunkPool.Stat()
		fmt.Fprintf(w, "pgvs3_chunk_pool_total_conns %d\n", cs.TotalConns())
		fmt.Fprintf(w, "pgvs3_chunk_pool_acquired_conns %d\n", cs.AcquiredConns())
		fmt.Fprintf(w, "pgvs3_chunk_pool_idle_conns %d\n", cs.IdleConns())
	}
	fmt.Fprintf(w, "pgvs3_reaped_rows_total %d\n", s.reapedRows.Load())
	fmt.Fprintf(w, "pgvs3_reaped_files_total %d\n", s.reapedFiles.Load())
	fmt.Fprintf(w, "pgvs3_maintenance_runs_total %d\n", s.maintenanceRuns.Load())
}

func (s *Store) ReapedRows() int64  { return s.reapedRows.Load() }
func (s *Store) ReapedFiles() int64 { return s.reapedFiles.Load() }
