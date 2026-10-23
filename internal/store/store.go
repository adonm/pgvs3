package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LayoutVersion is the on-disk compatibility marker this binary reads and
// writes. Incremental schema changes live in internal/store/migrations.
const LayoutVersion = 11

const (
	smallMax     = 8 << 20
	wholeReadMax = 64 << 20
	partRows     = smallMax/RowBytes + 2
	chunkSize    = 256 << 10
	// ReapRows bounds one garbage-reaping pass by chunk rows.
	ReapRows = 65536
	// ExpireBatch bounds one TTL-expiry statement; callers chain passes.
	ExpireBatch = 4096
)

// Store is the PostgreSQL-backed namespace engine. Every namespace (S3
// bucket, cache database, SQS queue) owns a PostgreSQL schema created by
// s3p.ensure_namespace.
type Store struct {
	pool      *pgxpool.Pool
	chunkPool *pgxpool.Pool
	log       *slog.Logger

	maxDBBytes int64
	dbSize     atomic.Int64
	dbSizeAt   atomic.Int64

	knownNS sync.Map // "kind\x00name" -> struct{}
	quota   sync.Map // "kind\x00name" -> quotaState gauge

	fileIDMu    sync.Mutex
	fileIDBlock []int64
	fileIDPos   int

	reapedRows      atomic.Int64
	reapedFiles     atomic.Int64
	expiredObjects  atomic.Int64
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
		// Index scans keep objects/buckets point lookups and ordered listings
		// at ~6 us in PostgreSQL shared_buffers.
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

// SetMaxDBBytes installs a write-admission cap on pg_database_size. Zero
// disables the guard. The size is sampled at most once every five seconds.
func (s *Store) SetMaxDBBytes(n int64) { s.maxDBBytes = n }

// Init applies pending goose migrations under a session advisory lock and
// refuses a layout version this binary cannot read.
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
	var marked, legacy bool
	if err := conn.QueryRow(ctx,
		"SELECT to_regclass('s3p.layout') IS NOT NULL, to_regclass('s3p.buckets') IS NOT NULL").
		Scan(&marked, &legacy); err != nil {
		return err
	}
	if legacy && !marked {
		return fmt.Errorf("s3p.layout is missing: refusing to guess the storage layout")
	}
	if marked {
		var found pgtype.Int4
		if err := conn.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&found); err != nil {
			return err
		}
		if !found.Valid || found.Int32 != LayoutVersion {
			return fmt.Errorf("s3p holds storage layout %v; this pgvs3 reads v%d (run `pgvs3 migrate` for v10, or use a fresh database)", found.Int32, LayoutVersion)
		}
	}
	if err := s.gooseUp(ctx); err != nil {
		return err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var found pgtype.Int4
	if err := tx.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&found); err != nil {
		return err
	}
	if !found.Valid || found.Int32 != LayoutVersion {
		return fmt.Errorf("s3p holds storage layout %v; this pgvs3 reads v%d", found.Int32, LayoutVersion)
	}
	if _, err := tx.Exec(ctx, "SELECT s3p.reconcile_namespaces()"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT s3p.upgrade_queue_schemas()"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CheckLayout verifies the installed layout without running DDL. Used by
// read-only gateways connected to a primary or a streaming standby.
func (s *Store) CheckLayout(ctx context.Context) error {
	var version pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&version); err != nil {
		return err
	}
	if !version.Valid || version.Int32 != LayoutVersion {
		return fmt.Errorf("storage layout is %v, want v%d (run `pgvs3 migrate` for v10)", version.Int32, LayoutVersion)
	}
	return nil
}

// EnsureMaintenance verifies the catalog and per-namespace table settings.
func (s *Store) EnsureMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, rel := range []string{"s3p.layout", "s3p.namespaces", "s3p.file_ids", "s3p.origin_listing", "s3p.origin_listing_state", "s3p.goose_db_version"} {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required layout relation %s is missing", rel)
		}
	}
	for _, fn := range []string{
		"s3p.ensure_namespace(text,text,bigint,jsonb)",
		"s3p.drop_namespace(text,text,boolean)",
		"s3p.clone_namespace(text,text,text)",
		"s3p.adopt_namespace(text,text)",
		"s3p.adopt_namespaces(text)",
		"s3p.upgrade_queue_schemas()",
	} {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regprocedure($1) IS NOT NULL", fn).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required layout function %s is missing", fn)
		}
	}
	return s.checkNamespaceTables(ctx, conn)
}

// Ready checks that the database answers and the installed layout matches.
func (s *Store) Ready(ctx context.Context) error {
	var one int
	if err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	return s.CheckLayout(ctx)
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
	fmt.Fprintf(w, "pgvs3_expired_objects_total %d\n", s.expiredObjects.Load())
	fmt.Fprintf(w, "pgvs3_maintenance_runs_total %d\n", s.maintenanceRuns.Load())
}

func (s *Store) ReapedRows() int64      { return s.reapedRows.Load() }
func (s *Store) ReapedFiles() int64     { return s.reapedFiles.Load() }
func (s *Store) ExpiredObjects() int64  { return s.expiredObjects.Load() }
func (s *Store) MaintenanceRuns() int64 { return s.maintenanceRuns.Load() }

// isMissingBucketSchema reports whether err is PostgreSQL saying the
// namespace schema or one of its relations/functions does not exist.
func isMissingBucketSchema(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "3F000", "42P01", "42883":
		return true
	}
	return false
}
