package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/x509"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaFS embed.FS

// LayoutVersion is the storage layout this binary reads and writes. It is
// deliberately unchanged from the Rust implementation so both can serve the
// same database during the rewrite.
const LayoutVersion = 8

const (
	smallMax  = 8 << 20
	partRows  = smallMax/RowBytes + 2
	chunkSize = 256 << 10
	reapRows  = 65536
)

// Options are the PostgreSQL connection settings.
type Options struct {
	MinConns       int32
	MaxConns       int32
	CAFile         string
	AllowPlaintext bool
}

// Store is the PostgreSQL-backed object store.
type Store struct {
	pool  *pgxpool.Pool
	cache MetaCache
	log   *slog.Logger
}

// Open connects and warms the pool. Remote servers must use TLS; loopback
// servers may use plaintext for local development.
func Open(ctx context.Context, url string, opts Options, log *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	if opts.MinConns <= 0 {
		opts.MinConns = 64
	}
	if opts.MaxConns <= 0 {
		opts.MaxConns = 64
	}
	if opts.MinConns > opts.MaxConns {
		opts.MinConns = opts.MaxConns
	}
	host := cfg.ConnConfig.Host
	local := host == "localhost" || strings.HasPrefix(host, "/") || isLoopback(host)
	mode := sslMode(url)
	if !local && !opts.AllowPlaintext {
		if mode != "require" && mode != "verify-ca" && mode != "verify-full" {
			return nil, fmt.Errorf("remote PostgreSQL requires sslmode=require (or PGVS3_DB_ALLOW_PLAINTEXT=true for an isolated rig)")
		}
	}
	if cfg.ConnConfig.TLSConfig != nil {
		// pgx treats sslmode=require as encrypted-but-unverified. Match the Rust
		// gateway: verify the chain and the hostname against the system CAs.
		cfg.ConnConfig.TLSConfig.InsecureSkipVerify = false
		if cfg.ConnConfig.TLSConfig.ServerName == "" {
			cfg.ConnConfig.TLSConfig.ServerName = host
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if opts.CAFile != "" {
			pem, err := os.ReadFile(opts.CAFile)
			if err != nil {
				return nil, fmt.Errorf("reading PostgreSQL CA file: %w", err)
			}
			if !roots.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("PostgreSQL CA file contains no certificates")
			}
		}
		cfg.ConnConfig.TLSConfig.RootCAs = roots
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	if cfg.ConnConfig.RuntimeParams["application_name"] == "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = "pgvs3"
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	}
	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = opts.MinConns
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnLifetime = 0 // keep the hot working set, like the Rust pool
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Bitmap scans use PostgreSQL 18 read-ahead for cold chunk ranges.
		// Ordered metadata pages need index scans; listings override that
		// per transaction (see List).
		_, err := conn.Exec(ctx, `SET enable_indexscan = off;
SET work_mem = '64MB';
SET effective_io_concurrency = 32;
SET tcp_keepalives_idle = 30;
SET tcp_keepalives_interval = 10;
SET tcp_keepalives_count = 3;
SET idle_in_transaction_session_timeout = '5min';
SET synchronous_commit = on;`)
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
	// Open the warm set up front so a burst does not pay TCP, TLS and SCRAM.
	var wg sync.WaitGroup
	errs := make(chan error, opts.MinConns)
	for i := int32(0); i < opts.MinConns; i++ {
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
		return nil, err
	}
	return &Store{pool: pool, cache: NewLocalMetaCache(64 << 20), log: log}, nil
}

func (s *Store) Close()              { s.pool.Close() }
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func isLoopback(host string) bool {
	// net.ParseIP is in net; avoid importing it here by checking the common
	// textual forms and delegating to net in one place.
	if host == "::1" || host == "[::1]" {
		return true
	}
	parts := strings.Split(host, ".")
	if len(parts) != 4 || parts[0] != "127" {
		return false
	}
	for _, p := range parts {
		if p == "" || len(p) > 3 {
			return false
		}
	}
	return true
}

func sslMode(raw string) string {
	i := strings.IndexByte(raw, '?')
	if i < 0 {
		return "prefer"
	}
	for _, kv := range strings.Split(raw[i+1:], "&") {
		k, v, _ := strings.Cut(kv, "=")
		if k == "sslmode" {
			return v
		}
	}
	return "prefer"
}

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
func (s *Store) EnsureMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var configured bool
	err = conn.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_class WHERE oid = 's3p.chunks'::regclass AND relkind = 'r'
		AND reloptions @> ARRAY['toast_tuple_target=8160',
			'autovacuum_vacuum_scale_factor=0.01',
			'autovacuum_analyze_scale_factor=0.02',
			'autovacuum_vacuum_threshold=1000'])`).Scan(&configured)
	if err != nil {
		return err
	}
	if !configured {
		return fmt.Errorf("s3p.chunks lacks the expected inline storage and autovacuum settings")
	}
	for _, relation := range []string{"s3p.garbage", "s3p.objects_parts_idx", "s3p.upload_parts_file_id_key"} {
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

// Meta returns cached metadata, or reads it from PostgreSQL. HEAD uses fresh
// metadata so it never reports a stale size or ETag.
func (s *Store) Meta(ctx context.Context, bucket, key string, fresh bool) (*Meta, error) {
	if !fresh {
		m, ok, err := s.cache.Get(ctx, bucket, key)
		if err != nil {
			return nil, err
		}
		if ok {
			return &m, nil
		}
	}
	m, err := s.metaFresh(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// FreshMeta bypasses the cache and reads s3p.objects directly. The peer-cache
// getter uses it on the key's owner; retries after a stale entry use it too.
func (s *Store) FreshMeta(ctx context.Context, bucket, key string) (*Meta, error) {
	return s.metaFresh(ctx, bucket, key)
}

func (s *Store) metaFresh(ctx context.Context, bucket, key string) (*Meta, error) {
	var m Meta
	var epoch float64
	var parts, ends []int64
	err := s.pool.QueryRow(ctx, `SELECT file_id, size, sha256, EXTRACT(EPOCH FROM created_at)::float8,
		parts, part_ends, etag, user_metadata, content_type
		FROM s3p.objects WHERE bucket = $1 AND key = $2`, bucket, key).
		Scan(&m.FileID, &m.Size, &m.SHA256, &epoch, &parts, &ends, &m.ETag, &m.UserMetadata, &m.ContentType)
	if errors.Is(err, pgx.ErrNoRows) {
		s.cache.Invalidate(ctx, bucket, key)
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	m.CreatedAt = time.Unix(0, int64(epoch*1e9))
	m.Parts, m.PartEnds = parts, ends
	s.cache.Put(ctx, bucket, key, m)
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
}

func (b *Body) Read(p []byte) (int, error) {
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
		if b.cancel != nil {
			b.cancel()
		}
	})
	return nil
}

// GetBody serves [start,end] of an object. first/last/suffix follow S3 range
// rules; suffix >= 0 means the last suffix bytes. Conditional failures return
// ErrPrecondition and ErrNotModified before any bytes are sent.
func (s *Store) GetBody(ctx context.Context, bucket, key string, first, last, suffix int64, cond *ReadCondition) (*SliceMeta, *Body, error) {
	for attempt := 0; attempt < 2; attempt++ {
		m, err := s.metaForRead(ctx, bucket, key, cond, attempt == 1)
		if err != nil {
			return nil, nil, err
		}
		mstart, mend := effRange(m.Size, first, last, suffix)
		if m.Size > 0 && (mstart > mend || mstart >= m.Size) {
			if attempt == 0 {
				s.cache.Invalidate(ctx, bucket, key)
				continue
			}
			return nil, nil, ErrInvalidRange
		}
		sm := &SliceMeta{
			Size: m.Size, ETag: m.ETag, UserMetadata: m.UserMetadata,
			ContentType: m.ContentType, CreatedAt: m.CreatedAt,
			FileID: m.FileID, Parts: m.Parts, PartEnds: m.PartEnds,
			Start: mstart, End: mend,
		}
		if m.Size == 0 {
			if attempt == 0 {
				// A stale zero-size entry could hide a replacement made through
				// another gateway; re-read once before serving an empty body.
				s.cache.Invalidate(ctx, bucket, key)
				continue
			}
			return sm, &Body{}, nil
		}
		if sm.Len() == 0 {
			return sm, &Body{}, nil
		}
		streamCtx, cancel := context.WithCancel(ctx)
		body, err := s.startStream(streamCtx, cancel, bucket, key, m, mstart, mend)
		if err != nil {
			cancel()
			if attempt == 0 {
				s.cache.Invalidate(ctx, bucket, key)
				continue
			}
			return nil, nil, err
		}
		return sm, body, nil
	}
	return nil, nil, ErrNotFound
}

func (s *Store) metaForRead(ctx context.Context, bucket, key string, cond *ReadCondition, fresh bool) (*Meta, error) {
	if cond != nil && cond.IfNoneMatch != "" {
		fresh = true
	}
	var m *Meta
	var err error
	if !fresh {
		m, err = s.Meta(ctx, bucket, key, false)
	} else {
		m, err = s.Meta(ctx, bucket, key, true)
	}
	if err != nil {
		return nil, err
	}
	if cond == nil {
		return m, nil
	}
	if cond.IfMatch != "" {
		if !etagMatches(cond.IfMatch, m.ETag) {
			if !fresh {
				return s.metaForRead(ctx, bucket, key, cond, true)
			}
			return nil, ErrPrecondition
		}
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

func planPieces(m *Meta, start, end int64) ([]piece, error) {
	segments, err := m.Segments()
	if err != nil {
		return nil, err
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

func (s *Store) startStream(ctx context.Context, cancel context.CancelFunc, bucket, key string, m *Meta, start, end int64) (*Body, error) {
	pieces, err := planPieces(m, start, end)
	if err != nil {
		return nil, err
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

func (s *Store) streamSnapshot(ctx context.Context, ch chan<- result, bucket, key string, rootID int64, pieces []piece, start, end int64, send func([]byte) bool) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	for _, p := range pieces {
		if err := streamRows(ctx, tx, ch, bucket, key, rootID, p, start, end, send); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) streamPiece(ctx context.Context, ch chan<- result, bucket, key string, rootID int64, p piece, start, end int64, send func([]byte) bool) error {
	return streamRows(ctx, s.pool, ch, bucket, key, rootID, p, start, end, send)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const getRangeSQL = `SELECT c.no, c.data FROM s3p.chunks c
WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3
AND EXISTS (SELECT 1 FROM s3p.objects o WHERE o.bucket = $4 AND o.key = $5 AND o.file_id = $6)`

func streamRows(ctx context.Context, q querier, ch chan<- result, bucket, key string, rootID int64, p piece, start, end int64, send func([]byte) bool) error {
	rows, err := q.Query(ctx, getRangeSQL, p.fileID, p.lo, p.hi, bucket, key, rootID)
	if err != nil {
		return err
	}
	defer rows.Close()
	out := make([]byte, 0, chunkSize+RowBytes)
	early := make(map[int32][]byte)
	next := p.lo
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2 || raw[0] == nil || raw[1] == nil {
			return fmt.Errorf("file %d rows %d..=%d: unexpected row shape", p.fileID, p.lo, p.hi)
		}
		no := int32(uint32(raw[0][0])<<24 | uint32(raw[0][1])<<16 | uint32(raw[0][2])<<8 | uint32(raw[0][3]))
		if no != next {
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
			out = make([]byte, 0, chunkSize+RowBytes)
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
	var fileID int64
	var parts []int64
	err = tx.QueryRow(ctx, "DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id, parts", bucket, key).Scan(&fileID, &parts)
	found := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, err
	}
	if found {
		if err := queueFiles(ctx, tx, fileID, parts); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	s.cache.Invalidate(ctx, bucket, key)
	return found, nil
}

// DeleteMany removes up to 1000 keys in one transaction, queueing every file
// it unpublishes. It is the batch path behind DeleteObjects.
func (s *Store) DeleteMany(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `WITH doomed AS (
		DELETE FROM s3p.objects WHERE bucket = $1 AND key = ANY($2::text[])
		RETURNING file_id, parts
	), dead AS (
		SELECT file_id FROM doomed UNION ALL SELECT unnest(parts) FROM doomed
	)
	INSERT INTO s3p.garbage (file_id) SELECT DISTINCT file_id FROM dead WHERE file_id IS NOT NULL
	ON CONFLICT DO NOTHING`, bucket, keys)
	if err != nil {
		return err
	}
	for _, k := range keys {
		s.cache.Invalidate(ctx, bucket, k)
	}
	return nil
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
	var id string
	err := s.pool.QueryRow(ctx, `INSERT INTO s3p.uploads (upload_id, bucket, key, checksum_algorithm, user_metadata, content_type)
		SELECT gen_random_uuid()::text, name, $2, $3, $4, $5 FROM s3p.buckets WHERE name = $1
		RETURNING upload_id`, bucket, key, checksumAlgorithm, userMeta, contentType).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNoBucket
	}
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
		want++
	}
	if want != len(listed) {
		return nil, ErrInvalidPart
	}
	etag := s3ETag(md5All.Sum(nil), len(ids))
	if err := swapObjectParts(ctx, tx, bucket, key, nil, ids[0], size, shaAll.Sum(nil), etag, userMeta, contentType, ids, ends); err != nil {
		return nil, err
	}
	if len(unused) > 0 {
		if _, err := tx.Exec(ctx, "INSERT INTO s3p.garbage (file_id) SELECT unnest($1::int8[]) ON CONFLICT DO NOTHING", unused); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.uploads WHERE upload_id = $1", uploadID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	s.cache.Put(ctx, bucket, key, Meta{
		Size: size, SHA256: shaAll.Sum(nil), ETag: etag, UserMetadata: userMeta,
		ContentType: contentType, CreatedAt: time.Now(), FileID: ids[0], Parts: ids, PartEnds: ends,
	})
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
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, "INSERT INTO s3p.garbage (file_id) SELECT unnest($1::int8[]) ON CONFLICT DO NOTHING", ids); err != nil {
			return false, err
		}
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
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	// Chunk ranges favor bitmap scans, but ordered metadata pages need the
	// primary-key index. Scope the override to one listing.
	if _, err := tx.Exec(ctx, "SET LOCAL enable_indexscan = on"); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT key, size, etag, EXTRACT(EPOCH FROM created_at)::float8
		FROM s3p.objects
		WHERE bucket = $1 AND key COLLATE "C" >= $2 AND key COLLATE "C" < $3
		  AND key COLLATE "C" > $4
		ORDER BY key COLLATE "C" LIMIT $5`, bucket, prefix, prefixEnd, after, limit)
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
	return out, tx.Commit(ctx)
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
	Objects       int64
	Chunks        int64
	LogicalBytes  int64
	PhysicalBytes int64
	DatabaseBytes int64
	GarbageFiles  int64
	OldestGarbage *time.Duration
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
		return 0, fmt.Errorf("garbage file %d is still referenced", fileID)
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
