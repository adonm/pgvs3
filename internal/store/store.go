package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const LayoutVersion = 1

//go:embed schema.sql
var schemaSQL string

type OpenOptions struct {
	MaxConns              int32
	WorkMem               string
	StatementTimeout      time.Duration
	LockTimeout           time.Duration
	IdleTxTimeout         time.Duration
	MaxConnLifetime       time.Duration
	MaxConnLifetimeJitter time.Duration
}

type Store struct {
	pool           *pgxpool.Pool
	log            *slog.Logger
	evictedObjects atomic.Int64
	expiredObjects atomic.Int64
	touchCh        chan [2]string
	touchStop      chan struct{}
	touchCancel    context.CancelFunc
	closeOnce      sync.Once
	touchDone      chan struct{}
}

func afterConnectSQL(opts OpenOptions) string {
	workMem := opts.WorkMem
	if workMem == "" {
		workMem = "16MB"
	}
	return fmt.Sprintf("SET work_mem = '%s'; SET statement_timeout = %d; SET lock_timeout = %d; SET idle_in_transaction_session_timeout = %d;",
		strings.ReplaceAll(workMem, "'", ""),
		opts.StatementTimeout.Milliseconds(),
		opts.LockTimeout.Milliseconds(),
		opts.IdleTxTimeout.Milliseconds(),
	)
}

func Open(ctx context.Context, url string, maxConns int32, log *slog.Logger) (*Store, error) {
	return OpenWithOptions(ctx, url, OpenOptions{MaxConns: maxConns}, log)
}

func OpenWithOptions(ctx context.Context, url string, opts OpenOptions, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	if opts.MaxConns <= 0 {
		opts.MaxConns = 64
	}
	if opts.WorkMem == "" {
		opts.WorkMem = "16MB"
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = opts.MaxConns
	cfg.MinConns = min(max(opts.MaxConns/4, 2), 16)
	cfg.MaxConnLifetime = opts.MaxConnLifetime
	cfg.MaxConnLifetimeJitter = opts.MaxConnLifetimeJitter
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, afterConnectSQL(opts))
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
	touchCtx, touchCancel := context.WithCancel(context.Background())
	s := &Store{
		pool:        pool,
		log:         log,
		touchCh:     make(chan [2]string, 1024),
		touchStop:   make(chan struct{}),
		touchCancel: touchCancel,
		touchDone:   make(chan struct{}),
	}
	go s.runTouchWorkerWith(touchCtx, func(ctx context.Context, bucket, key string) error {
		_, err := s.pool.Exec(ctx, `UPDATE cache_objects SET last_access = now() WHERE bucket = $1 AND key = $2`, bucket, key)
		return err
	})
	return s, nil
}

func (s *Store) Close() {
	s.closeOnce.Do(func() {
		if s.touchStop != nil {
			close(s.touchStop)
		}
		if s.touchCancel != nil {
			s.touchCancel()
		}
		if s.touchDone != nil {
			<-s.touchDone
		}
		if s.pool != nil {
			s.pool.Close()
		}
	})
}

func (s *Store) runTouchWorkerWith(baseCtx context.Context, fn func(context.Context, string, string) error) {
	defer close(s.touchDone)
	for {
		select {
		case <-s.touchStop:
			return
		case bk := <-s.touchCh:
			ctx, reqCancel := context.WithTimeout(baseCtx, 2*time.Second)
			err := fn(ctx, bk[0], bk[1])
			reqCancel()
			if err != nil && !errors.Is(err, context.Canceled) && s.log != nil {
				s.log.Warn("touch last_access failed", "bucket", bk[0], "key", bk[1], "err", err)
			}
		}
	}
}

func (s *Store) TouchAsync(bucket, key string) {
	if s.touchCh == nil || s.touchStop == nil {
		return
	}
	select {
	case <-s.touchStop:
		return
	default:
	}
	select {
	case s.touchCh <- [2]string{bucket, key}:
	case <-s.touchStop:
	default:
	}
}

func (s *Store) Init(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(70677673)`); err != nil {
			return err
		}
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass('cache_layout') IS NOT NULL`).Scan(&exists); err != nil {
			return err
		}
		if exists {
			var ver int
			err := tx.QueryRow(ctx, `SELECT version FROM cache_layout LIMIT 1`).Scan(&ver)
			if err != nil {
				return fmt.Errorf("read cache_layout: %w", err)
			}
			if ver != LayoutVersion {
				return fmt.Errorf("cache_layout version = %d, want %d (drop and re-init)", ver, LayoutVersion)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, schemaSQL); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
		_, err := tx.Exec(ctx, `INSERT INTO cache_layout (version) VALUES ($1)`, LayoutVersion)
		return err
	})
}

func (s *Store) CheckLayout(ctx context.Context) error {
	var ver int
	err := s.pool.QueryRow(ctx, `SELECT version FROM cache_layout LIMIT 1`).Scan(&ver)
	if err != nil {
		return fmt.Errorf("check cache_layout: %w", err)
	}
	if ver != LayoutVersion {
		return fmt.Errorf("cache_layout version = %d, want %d (drop and re-init)", ver, LayoutVersion)
	}
	return nil
}

func (s *Store) Ready(ctx context.Context) error {
	var one int
	return s.pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
}

type Stats struct {
	Objects       int64
	LogicalBytes  int64
	DatabaseBytes int64
}

func (s *Store) Stat(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(size), 0), pg_database_size(current_database()) FROM cache_objects`).
		Scan(&st.Objects, &st.LogicalBytes, &st.DatabaseBytes)
	return st, err
}

func (s *Store) GetObject(ctx context.Context, bucket, key string) (*ObjectMeta, error) {
	var m ObjectMeta
	err := s.pool.QueryRow(ctx, `SELECT id, bucket, key, etag, size, content_type, content_encoding, last_modified, user_meta, filled_at, last_access
		FROM cache_objects WHERE bucket = $1 AND key = $2`, bucket, key).
		Scan(&m.ID, &m.Bucket, &m.Key, &m.ETag, &m.Size, &m.ContentType, &m.ContentEncoding, &m.LastModified, &m.UserMeta, &m.FilledAt, &m.LastAccess)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// GetSingleChunk fetches object metadata and one chunk row in a single round trip.
func (s *Store) GetSingleChunk(ctx context.Context, bucket, key string, idx int32) (*ObjectMeta, []byte, error) {
	var m ObjectMeta
	var data []byte
	err := s.pool.QueryRow(ctx, `SELECT o.id, o.bucket, o.key, o.etag, o.size, o.content_type, o.content_encoding, o.last_modified, o.user_meta, o.filled_at, o.last_access, c.data
		FROM cache_objects o
		LEFT JOIN cache_chunks c ON c.object_id = o.id AND c.idx = $3
		WHERE o.bucket = $1 AND o.key = $2`, bucket, key, idx).
		Scan(&m.ID, &m.Bucket, &m.Key, &m.ETag, &m.Size, &m.ContentType, &m.ContentEncoding, &m.LastModified, &m.UserMeta, &m.FilledAt, &m.LastAccess, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	return &m, data, nil
}

func (s *Store) GetChunks(ctx context.Context, objectID int64, lo, hi int32) (map[int32][]byte, error) {
	rows, err := s.pool.Query(ctx, `SELECT idx, data FROM cache_chunks WHERE object_id = $1 AND idx BETWEEN $2 AND $3 ORDER BY idx`, objectID, lo, hi)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int32][]byte, max(int(hi-lo+1), 1))
	for rows.Next() {
		var idx int32
		var data []byte
		if err := rows.Scan(&idx, &data); err != nil {
			return nil, err
		}
		out[idx] = data
	}
	return out, rows.Err()
}

// OpenChunkStream returns a ReadCloser streaming chunk rows [lo..hi] clipped to [firstOff..lastOffInLast].
func (s *Store) OpenChunkStream(ctx context.Context, objectID int64, lo, hi int32, firstSkip, totalLen int64) (io.ReadCloser, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, `SELECT idx, data, count(*) OVER () FROM cache_chunks WHERE object_id = $1 AND idx BETWEEN $2 AND $3 ORDER BY idx`, objectID, lo, hi)
	if err != nil {
		conn.Release()
		return nil, err
	}
	sr := &chunkStreamReader{
		conn: conn, rows: rows,
		expectIdx: lo, wantRows: int64(hi - lo + 1),
		firstSkip: firstSkip, remaining: totalLen,
	}
	// Eagerly fetch first chunk so missing chunks are detected before 200 OK headers are sent.
	if err := sr.fill(); err != nil {
		_ = sr.Close()
		return nil, err
	}
	return sr, nil
}

type chunkStreamReader struct {
	conn      *pgxpool.Conn
	rows      pgx.Rows
	expectIdx int32
	wantRows  int64
	checked   bool
	firstSkip int64
	remaining int64
	buf       []byte
	closed    bool
}

func (r *chunkStreamReader) fill() error {
	for len(r.buf) == 0 && r.remaining > 0 {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return err
			}
			return ErrNotFound
		}
		var idx int32
		var data []byte
		var totalRows int64
		if err := r.rows.Scan(&idx, &data, &totalRows); err != nil {
			return err
		}
		if !r.checked {
			r.checked = true
			if totalRows != r.wantRows {
				return ErrNotFound
			}
		}
		if idx != r.expectIdx {
			return ErrNotFound
		}
		r.expectIdx++
		if r.firstSkip > 0 {
			if r.firstSkip >= int64(len(data)) {
				return ErrNotFound
			}
			data = data[r.firstSkip:]
			r.firstSkip = 0
		}
		if int64(len(data)) > r.remaining {
			data = data[:r.remaining]
		}
		r.buf = data
	}
	return nil
}

func (r *chunkStreamReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	if len(r.buf) == 0 {
		if err := r.fill(); err != nil {
			return 0, err
		}
		if len(r.buf) == 0 {
			return 0, io.EOF
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	r.remaining -= int64(n)
	return n, nil
}

func (r *chunkStreamReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.rows != nil {
		r.rows.Close()
	}
	if r.conn != nil {
		r.conn.Release()
	}
	return nil
}

func (s *Store) UpsertObject(ctx context.Context, m ObjectMeta) (int64, error) {
	if m.ContentType == "" {
		m.ContentType = "application/octet-stream"
	}
	if m.UserMeta == nil {
		m.UserMeta = []string{}
	}
	var id int64
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var oldID int64
		var oldETag string
		err := tx.QueryRow(ctx, `SELECT id, etag FROM cache_objects WHERE bucket = $1 AND key = $2 FOR UPDATE`, m.Bucket, m.Key).Scan(&oldID, &oldETag)
		if err == nil {
			if oldETag != m.ETag {
				if _, err := tx.Exec(ctx, `DELETE FROM cache_chunks WHERE object_id = $1`, oldID); err != nil {
					return err
				}
			}
			err = tx.QueryRow(ctx, `UPDATE cache_objects
				SET etag = $2, size = $3, content_type = $4, content_encoding = $5,
				    last_modified = $6, user_meta = $7, filled_at = now(), last_access = now()
				WHERE id = $1 RETURNING id`,
				oldID, m.ETag, m.Size, m.ContentType, m.ContentEncoding, m.LastModified, m.UserMeta).Scan(&id)
			return err
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO cache_objects (bucket, key, etag, size, content_type, content_encoding, last_modified, user_meta)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (bucket, key) DO UPDATE
			  SET etag = EXCLUDED.etag, size = EXCLUDED.size, content_type = EXCLUDED.content_type,
			      content_encoding = EXCLUDED.content_encoding, last_modified = EXCLUDED.last_modified,
			      user_meta = EXCLUDED.user_meta, filled_at = now(), last_access = now()
			RETURNING id`,
			m.Bucket, m.Key, m.ETag, m.Size, m.ContentType, m.ContentEncoding, m.LastModified, m.UserMeta).Scan(&id)
	})
	return id, err
}

func (s *Store) PutChunk(ctx context.Context, objectID int64, idx int32, data []byte) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO cache_chunks (object_id, idx, data) VALUES ($1, $2, $3)
		ON CONFLICT (object_id, idx) DO NOTHING`, objectID, idx, data)
	return err
}

func (s *Store) MarkRevalidated(ctx context.Context, bucket, key, etag string) error {
	_, err := s.pool.Exec(ctx, `UPDATE cache_objects SET filled_at = now(), last_access = now()
		WHERE bucket = $1 AND key = $2 AND etag = $3`, bucket, key, etag)
	return err
}

func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM cache_objects WHERE bucket = $1 AND key = $2`, bucket, key)
	return err
}

func (s *Store) DeleteMany(ctx context.Context, bucket string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM cache_objects WHERE bucket = $1 AND key = ANY($2)`, bucket, keys)
	return err
}

func (s *Store) Evict(ctx context.Context, retention time.Duration, maxBytes int64) (int64, error) {
	var totalEvicted int64
	if retention > 0 {
		for {
			tag, err := s.pool.Exec(ctx, `DELETE FROM cache_objects WHERE id IN (
				SELECT id FROM cache_objects
				WHERE last_access < now() - make_interval(secs => $1)
				ORDER BY last_access, id LIMIT 1000
			)`, retention.Seconds())
			if err != nil {
				return totalEvicted, err
			}
			n := tag.RowsAffected()
			s.expiredObjects.Add(n)
			totalEvicted += n
			if n < 1000 {
				break
			}
		}
	}
	if maxBytes > 0 {
		for {
			var cur int64
			if err := s.pool.QueryRow(ctx, `SELECT COALESCE(sum(size), 0) FROM cache_objects`).Scan(&cur); err != nil {
				return totalEvicted, err
			}
			if cur <= maxBytes {
				break
			}
			excess := cur - maxBytes
			tag, err := s.pool.Exec(ctx, `WITH ranked AS (
				SELECT id, size, sum(size) OVER (ORDER BY last_access, id) AS cum
				FROM cache_objects
			)
			DELETE FROM cache_objects WHERE id IN (
				SELECT id FROM ranked WHERE cum - size < $1 LIMIT 128
			)`, excess)
			if err != nil {
				return totalEvicted, err
			}
			n := tag.RowsAffected()
			if n == 0 {
				break
			}
			s.evictedObjects.Add(n)
			totalEvicted += n
		}
	}
	return totalEvicted, nil
}

func (s *Store) CacheEvictedObjects() int64 { return s.evictedObjects.Load() }
func (s *Store) ExpiredObjects() int64      { return s.expiredObjects.Load() }

func (s *Store) WriteMetrics(w io.Writer) {
	if s == nil || s.pool == nil {
		return
	}
	st := s.pool.Stat()
	fmt.Fprintf(w, "pgvs3_pool_total_conns %d\npgvs3_pool_acquired_conns %d\npgvs3_pool_idle_conns %d\npgvs3_pool_max_conns %d\n",
		st.TotalConns(), st.AcquiredConns(), st.IdleConns(), st.MaxConns())
	fmt.Fprintf(w, "pgvs3_expired_objects_total %d\npgvs3_cache_evicted_objects_total %d\n",
		s.expiredObjects.Load(), s.evictedObjects.Load())
}
