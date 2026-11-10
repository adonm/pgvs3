package store

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const LayoutVersion = 25

const (
	chunkSize = 256 << 10
	// chunkReadBuffer sits under pgx's 8 KiB frame reader on chunk connections
	// so one socket read carries ~32 rows instead of one.
	chunkReadBuffer     = 256 << 10
	reapCandidates      = 32
	ReapRows            = 65536
	ExpireBatch         = 4096
	DefaultJobRetention = 24 * time.Hour
)

type Store struct {
	pool      *pgxpool.Pool
	chunkPool *pgxpool.Pool
	log       *slog.Logger

	maxDBBytes   int64
	dbSize       atomic.Int64
	dbSizeAt     atomic.Int64
	jobRetention time.Duration

	knownNS sync.Map
	quota   sync.Map

	// cacheMaxBytes maps a cache namespace to the byte budget registered by
	// objcache tiers that were constructed programmatically rather than via a
	// persisted origin spec.
	cacheMaxBytes sync.Map

	fileIDMu    sync.Mutex
	fileIDBlock []int64
	fileIDPos   int

	// inflightReads bounds per-GET chunk connections when many reads overlap.
	inflightReads atomic.Int64
	// writeGate bounds open ChunkWriter transactions so metadata operations
	// always keep part of the main pool free.
	writeGate chan struct{}

	blocks             *blockCache
	forceSnapshotReads bool
	pinnedReads        atomic.Int64
	snapshotReads      atomic.Int64
	reapPinnedSkips    atomic.Int64

	reapedRows          atomic.Int64
	reapedFiles         atomic.Int64
	expiredObjects      atomic.Int64
	cacheEvictedObjects atomic.Int64
	maintenanceRuns     atomic.Int64
	layoutChecked       atomic.Int64
}

type OpenOptions struct {
	MaxConns              int32
	WorkMem               string
	StatementTimeout      time.Duration
	LockTimeout           time.Duration
	IdleTxTimeout         time.Duration
	MaxConnLifetime       time.Duration
	MaxConnLifetimeJitter time.Duration
	MinIdleConns          int32
	Tracer                pgx.QueryTracer
	// ReadCacheBytes bounds the gateway-local cache of immutable chunk blocks
	// (0 disables it).
	ReadCacheBytes int64
}

func defaultOpenOptions(maxConns int32) OpenOptions {
	if maxConns <= 0 {
		maxConns = 64
	}
	return OpenOptions{
		MaxConns:              maxConns,
		WorkMem:               "16MB",
		StatementTimeout:      5 * time.Minute,
		LockTimeout:           5 * time.Second,
		IdleTxTimeout:         time.Minute,
		MaxConnLifetime:       time.Hour,
		MaxConnLifetimeJitter: 5 * time.Minute,
		MinIdleConns:          min(maxConns, 8),
	}
}

func Open(ctx context.Context, url string, maxConns int32, log *slog.Logger) (*Store, error) {
	return OpenWithOptions(ctx, url, defaultOpenOptions(maxConns), log)
}

func OpenWithOptions(ctx context.Context, url string, opts OpenOptions, log *slog.Logger) (*Store, error) {
	opts, err := normalizeOpenOptions(opts)
	if err != nil {
		return nil, err
	}
	cfg, err := newPoolConfig(url, opts)
	if err != nil {
		return nil, err
	}
	pool, err := openPool(ctx, cfg)
	if err != nil {
		return nil, err
	}
	chunkPool, err := openPool(ctx, newChunkPoolConfig(cfg, opts))
	if err != nil {
		pool.Close()
		return nil, err
	}
	if err := warmPool(ctx, pool, cfg.MinConns); err != nil {
		pool.Close()
		chunkPool.Close()
		return nil, err
	}
	return &Store{
		pool: pool, chunkPool: chunkPool, log: log,
		jobRetention: DefaultJobRetention,
		blocks:       newBlockCache(opts.ReadCacheBytes),
		writeGate:    make(chan struct{}, max(int(cfg.MaxConns)/2, 1)),
	}, nil
}

func (s *Store) acquireWriter(ctx context.Context) error {
	if s.writeGate == nil {
		return nil
	}
	select {
	case s.writeGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) releaseWriter() {
	if s.writeGate == nil {
		return
	}
	<-s.writeGate
}

func normalizeOpenOptions(opts OpenOptions) (OpenOptions, error) {
	if opts.MaxConns <= 0 {
		opts.MaxConns = 64
	}
	if opts.WorkMem == "" {
		opts.WorkMem = "16MB"
	}
	if !validWorkMem(opts.WorkMem) {
		return opts, fmt.Errorf("invalid work_mem %q", opts.WorkMem)
	}
	if opts.MinIdleConns <= 0 {
		opts.MinIdleConns = min(opts.MaxConns, 8)
	}
	return opts, nil
}

func newPoolConfig(url string, opts OpenOptions) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
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
	cfg.MinConns = min(opts.MaxConns, 8)
	cfg.MinIdleConns = min(opts.MinIdleConns, opts.MaxConns)
	cfg.MaxConnIdleTime = 10 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	cfg.MaxConnLifetime = opts.MaxConnLifetime
	cfg.MaxConnLifetimeJitter = opts.MaxConnLifetimeJitter
	if opts.Tracer != nil {
		cfg.ConnConfig.Tracer = opts.Tracer
	}
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, sessionSQL(opts, true, opts.StatementTimeout))
		return err
	}
	return cfg, nil
}

func newChunkPoolConfig(cfg *pgxpool.Config, opts OpenOptions) *pgxpool.Config {
	chunkCfg := cfg.Copy()
	chunkCfg.MaxConns = min(opts.MaxConns, 64)
	chunkCfg.MinConns = min(opts.MaxConns, 4)
	chunkCfg.MinIdleConns = min(opts.MinIdleConns, chunkCfg.MaxConns)
	chunkCfg.ConnConfig.BuildFrontend = func(r io.Reader, w io.Writer) *pgproto3.Frontend {
		return pgproto3.NewFrontend(bufio.NewReaderSize(r, chunkReadBuffer), w)
	}
	chunkCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, sessionSQL(opts, false, 0)); err != nil {
			return err
		}
		var recovery bool
		if err := conn.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovery); err != nil {
			return err
		}
		conn.PgConn().CustomData()[recoveryKey] = recovery
		return nil
	}
	return chunkCfg
}

func sessionSQL(opts OpenOptions, indexScan bool, statementTimeout time.Duration) string {
	scan := "off"
	if indexScan {
		scan = "on"
	}
	return "SET enable_indexscan = " + scan + ";" +
		"\nSET work_mem = '" + opts.WorkMem + "';" +
		"\nSET effective_io_concurrency = 32;" +
		"\nSET statement_timeout = '" + durationMillis(statementTimeout) + "';" +
		"\nSET lock_timeout = '" + durationMillis(opts.LockTimeout) + "';" +
		"\nSET idle_in_transaction_session_timeout = '" + durationMillis(opts.IdleTxTimeout) + "';" +
		"\nSET tcp_keepalives_idle = 30;" +
		"\nSET tcp_keepalives_interval = 10;" +
		"\nSET tcp_keepalives_count = 3;" +
		"\nSET synchronous_commit = on;"
}

func warmPool(ctx context.Context, pool *pgxpool.Pool, conns int32) error {
	var wg sync.WaitGroup
	errs := make(chan error, conns)
	for range conns {
		wg.Go(func() {
			c, err := pool.Acquire(ctx)
			if err != nil {
				errs <- err
				return
			}
			c.Release()
		})
	}
	wg.Wait()
	close(errs)
	return <-errs
}

func openPool(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func durationMillis(d time.Duration) string {
	if d <= 0 {
		return "0"
	}
	return strconv.FormatInt(int64(max(d/time.Millisecond, 1)), 10) + "ms"
}

func validWorkMem(v string) bool {

	if len(v) < 3 || len(v) > 16 || v[0] == '0' {
		return false
	}
	for i, r := range v {
		if r >= '0' && r <= '9' {
			continue
		}
		suffix := strings.ToUpper(v[i:])
		if suffix == "KB" || suffix == "MB" || suffix == "GB" || suffix == "TB" {
			return i > 0
		}
		return false
	}
	return false
}

func (s *Store) SetJobRetention(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.jobRetention = d
}

// nextFileID refills its block through the caller's own session: a writer
// that waits on the pool while holding a connection can exhaust it.
func (s *Store) nextFileID(ctx context.Context, q querier) (int64, error) {
	s.fileIDMu.Lock()
	defer s.fileIDMu.Unlock()
	if s.fileIDPos < len(s.fileIDBlock) {
		id := s.fileIDBlock[s.fileIDPos]
		s.fileIDPos++
		return id, nil
	}
	rows, err := q.Query(ctx, "SELECT nextval('s3p.file_ids') FROM generate_series(1,64)")
	if err != nil {
		return 0, err
	}
	block, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, err
	}
	if len(block) == 0 {
		return 0, errors.New("no file ids allocated")
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

func (s *Store) SetMaxDBBytes(n int64) { s.maxDBBytes = n }

func (s *Store) Init(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", schemaLockKey); err != nil {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", schemaLockKey)

	var versionNum int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&versionNum); err != nil {
		return err
	}
	if versionNum < 180000 {
		return fmt.Errorf("pgvs3 requires PostgreSQL 18 or later (found %d)", versionNum)
	}

	var initialized bool
	if err := conn.QueryRow(ctx, "SELECT to_regnamespace('s3p') IS NOT NULL").Scan(&initialized); err != nil {
		return err
	}
	if !initialized {
		if err := applySchema(ctx, conn); err != nil {
			return err
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := checkLayout(ctx, tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT s3p.reconcile_namespaces()"); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "SELECT s3p.upgrade_queue_schemas()"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CheckLayout(ctx context.Context) error {
	return checkLayout(ctx, s.pool)
}

func checkLayout(ctx context.Context, db rowQuerier) error {
	var version pgtype.Int4
	if err := db.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&version); err != nil {
		return fmt.Errorf("read storage layout (use a fresh database if the s3p schema is partial): %w", err)
	}
	if !version.Valid || version.Int32 != LayoutVersion {
		return fmt.Errorf("storage layout is %v, want v%d (use a fresh database)", version.Int32, LayoutVersion)
	}
	return nil
}

func (s *Store) Ready(ctx context.Context) error {
	var one int
	if err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}

	last := s.layoutChecked.Load()
	if last != 0 && time.Since(time.Unix(0, last)) < 30*time.Second {
		return nil
	}
	if err := s.CheckLayout(ctx); err != nil {
		return err
	}
	s.layoutChecked.Store(time.Now().UnixNano())
	return nil
}

func (s *Store) WriteMetrics(w io.Writer) {
	writePoolMetrics(w, "pgvs3_pg_pool", s.pool.Stat())
	if s.chunkPool != nil {
		writePoolMetrics(w, "pgvs3_chunk_pool", s.chunkPool.Stat())
	}
	fmt.Fprintf(w, "pgvs3_reaped_rows_total %d\n", s.reapedRows.Load())
	fmt.Fprintf(w, "pgvs3_reaped_files_total %d\n", s.reapedFiles.Load())
	fmt.Fprintf(w, "pgvs3_expired_objects_total %d\n", s.expiredObjects.Load())
	fmt.Fprintf(w, "pgvs3_cache_evicted_objects_total %d\n", s.cacheEvictedObjects.Load())
	fmt.Fprintf(w, "pgvs3_maintenance_runs_total %d\n", s.maintenanceRuns.Load())
	fmt.Fprintf(w, "pgvs3_read_pinned_total %d\n", s.pinnedReads.Load())
	fmt.Fprintf(w, "pgvs3_read_snapshot_total %d\n", s.snapshotReads.Load())
	fmt.Fprintf(w, "pgvs3_reap_pinned_skips_total %d\n", s.reapPinnedSkips.Load())
	s.blocks.writeMetrics(w)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if live, dead, pendingBytes, oldest, err := s.PendingUploadStats(ctx); err == nil {
		fmt.Fprintf(w, "pgvs3_pending_uploads_live %d\n", live)
		fmt.Fprintf(w, "pgvs3_pending_uploads_dead %d\n", dead)
		fmt.Fprintf(w, "pgvs3_pending_uploads_bytes %d\n", pendingBytes)
		age := 0.0
		if oldest != nil {
			age = time.Since(*oldest).Seconds()
		}
		fmt.Fprintf(w, "pgvs3_pending_uploads_oldest_seconds %.3f\n", age)
	}
	cancel()
}

func writePoolMetrics(w io.Writer, prefix string, st *pgxpool.Stat) {
	fmt.Fprintf(w, "%s_total_conns %d\n", prefix, st.TotalConns())
	fmt.Fprintf(w, "%s_acquired_conns %d\n", prefix, st.AcquiredConns())
	fmt.Fprintf(w, "%s_idle_conns %d\n", prefix, st.IdleConns())
	fmt.Fprintf(w, "%s_constructing_conns %d\n", prefix, st.ConstructingConns())
	fmt.Fprintf(w, "%s_max_conns %d\n", prefix, st.MaxConns())
	fmt.Fprintf(w, "%s_acquire_count_total %d\n", prefix, st.AcquireCount())
	fmt.Fprintf(w, "%s_acquire_duration_seconds_total %f\n", prefix, st.AcquireDuration().Seconds())
	fmt.Fprintf(w, "%s_empty_acquire_count_total %d\n", prefix, st.EmptyAcquireCount())
	fmt.Fprintf(w, "%s_empty_acquire_wait_seconds_total %f\n", prefix, st.EmptyAcquireWaitTime().Seconds())
	fmt.Fprintf(w, "%s_canceled_acquire_count_total %d\n", prefix, st.CanceledAcquireCount())
	fmt.Fprintf(w, "%s_new_conns_count_total %d\n", prefix, st.NewConnsCount())
	fmt.Fprintf(w, "%s_max_lifetime_destroy_count_total %d\n", prefix, st.MaxLifetimeDestroyCount())
	fmt.Fprintf(w, "%s_max_idle_destroy_count_total %d\n", prefix, st.MaxIdleDestroyCount())
}

func (s *Store) ReapedRows() int64 { return s.reapedRows.Load() }

func (s *Store) ReapedFiles() int64 { return s.reapedFiles.Load() }

func (s *Store) ExpiredObjects() int64 { return s.expiredObjects.Load() }

// CacheEvictedObjects counts objects removed to stay within a cache budget.
func (s *Store) CacheEvictedObjects() int64 { return s.cacheEvictedObjects.Load() }

// SetCacheMaxBytes registers a byte budget for cache namespace name.
// Maintenance evicts the oldest cached objects once the namespace exceeds it.
// A non-positive budget disables capacity eviction.
func (s *Store) SetCacheMaxBytes(name string, maxBytes int64) {
	if maxBytes <= 0 {
		s.cacheMaxBytes.Delete(name)
		return
	}
	s.cacheMaxBytes.Store(name, maxBytes)
}

func (s *Store) MaintenanceRuns() int64 { return s.maintenanceRuns.Load() }

func isSchemaLookupError(err error) bool {
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

type namespaceQueryError struct {
	schema string
	err    error
}

func (e *namespaceQueryError) Error() string { return e.err.Error() }

func (e *namespaceQueryError) Unwrap() error { return e.err }

func inNamespace(schema string, err error) error {
	if !isSchemaLookupError(err) {
		return err
	}
	return &namespaceQueryError{schema: schema, err: err}
}

func missingNamespace(ctx context.Context, db rowQuerier, schema string, err error) bool {
	if !isSchemaLookupError(err) {
		return false
	}
	if scoped, ok := errors.AsType[*namespaceQueryError](err); ok {
		schema = scoped.schema
	}
	if schema == "" {
		return false
	}
	var absent bool
	return db.QueryRow(ctx, `SELECT to_regnamespace($1) IS NULL`, schema).Scan(&absent) == nil && absent
}

func namespaceResultError(ctx context.Context, db rowQuerier, schema string, err, absent error) error {
	if missingNamespace(ctx, db, schema, err) {
		return absent
	}
	return err
}

//go:embed schema.sql
var schemaSQL string

const schemaLockKey = int64(0x70677673)

var layoutManifest = struct {
	relations []string
	functions []functionContract
}{
	relations: []string{
		"s3p.layout", "s3p.namespaces", "s3p.file_ids", "s3p.job_index",
		"s3p.pending_uploads", "s3p.cache_fill_leases",
	},
	functions: []functionContract{
		{signature: "namespace_schema(text,text)", result: "text"},
		{signature: "ensure_namespace(text,text,bigint,jsonb)", result: "text"},
		{signature: "ensure_object_schema(text,text)", result: "void"},
		{signature: "ensure_queue_schema(text)", result: "void"},
		{signature: "tune_queue_schema(text)", result: "void"},
		{signature: "drop_namespace(text,text,boolean)", result: "text"},
		{signature: "clone_namespace(text,text,text)", result: "text"},
		{signature: "adopt_namespace(text,text)", result: "text"},
		{signature: "adopt_namespaces(text)", result: "integer"},
		{signature: "reconcile_namespaces()", result: "integer"},
		{signature: "upgrade_queue_schemas()", result: "integer"},
	},
}

func applySchema(ctx context.Context, conn *pgxpool.Conn) error {
	if _, err := conn.Conn().PgConn().Exec(ctx, schemaSQL).ReadAll(); err != nil {
		return fmt.Errorf("apply schema baseline: %w", err)
	}
	return nil
}

const SupportedNamespaces = 100

var ErrQuotaExceeded = errors.New("namespace or database storage budget exceeded")

var ErrNamespaceLayout = errors.New("incomplete namespace layout")

func NewNamespaceRef(kind ns.Kind, name string) (ns.Ref, error) {
	if !kind.Valid() {
		return ns.Ref{}, ns.ErrUnknownKind
	}
	if !ns.ValidName(name) {
		return ns.Ref{}, ErrInvalidBucket
	}
	return ns.Ref{Kind: kind, Name: name}, nil
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func nsSQLIdent(kind ns.Kind, name string) (string, error) {
	sc, err := kind.Schema(name)
	if err != nil {
		return "", err
	}
	return quoteIdent(sc), nil
}

func (s *Store) known(ref ns.Ref) bool {
	_, ok := s.knownNS.Load(ref.Key())
	return ok
}

func (s *Store) markKnown(ref ns.Ref) {
	s.knownNS.Store(ref.Key(), struct{}{})
}

func (s *Store) forget(ref ns.Ref) {
	s.knownNS.Delete(ref.Key())
	s.quota.Delete(ref.Key())
}

func withTx[T any](ctx context.Context, s *Store, fn func(pgx.Tx) (T, error)) (T, error) {
	return withTxOptions(ctx, s, pgx.TxOptions{}, fn)
}

func withTxOptions[T any](ctx context.Context, s *Store, opts pgx.TxOptions, fn func(pgx.Tx) (T, error)) (T, error) {
	var zero T
	tx, err := s.pool.BeginTx(ctx, opts)
	if err != nil {
		return zero, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	v, err := fn(tx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, err
	}
	return v, nil
}

func writableNamespaceSchema(ctx context.Context, s *Store, kind ns.Kind, name string) (string, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return "", err
	}
	ref := ns.Ref{Kind: kind, Name: name}
	if s.known(ref) {
		return sc, nil
	}
	present, err := s.NamespaceExists(ctx, kind, name)
	if err != nil {
		return "", err
	}
	if !present {
		if kind == ns.Bucket {
			return "", ErrNoBucket
		}
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return "", err
		}
	}
	return sc, nil
}

func withAutoCreate[T any](ctx context.Context, s *Store, kind ns.Kind, name string, fn func(schema string) (T, error)) (T, error) {
	var zero T
	sc, err := writableNamespaceSchema(ctx, s, kind, name)
	if err != nil {
		return zero, err
	}
	ref := ns.Ref{Kind: kind, Name: name}
	v, err := fn(sc)
	if err == nil {
		s.markKnown(ref)
		return v, nil
	}
	if !missingNamespace(ctx, s.pool, sc, err) {
		return v, err
	}
	if kind == ns.Bucket {
		s.forget(ref)
		return zero, ErrNoBucket
	}
	if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
		return zero, err
	}
	v, err = fn(sc)
	if err == nil {
		s.markKnown(ref)
	}
	return v, err
}

func withSchema[T any](ctx context.Context, s *Store, ref ns.Ref, missingBucket, missingOther error, fn func(schema string) (T, error)) (T, error) {
	var zero T
	sc, err := nsSQLIdent(ref.Kind, ref.Name)
	if err != nil {
		return zero, err
	}
	v, err := fn(sc)
	if !missingNamespace(ctx, s.pool, sc, err) {
		return v, err
	}
	if ref.Kind == ns.Bucket {
		return zero, missingBucket
	}
	return zero, missingOther
}

func withSchema2[A, B any](ctx context.Context, s *Store, ref ns.Ref, missingBucket, missingOther error, fn func(schema string) (A, B, error)) (A, B, error) {
	type result struct {
		a A
		b B
	}
	v, err := withSchema(ctx, s, ref, missingBucket, missingOther, func(sc string) (result, error) {
		a, b, err := fn(sc)
		return result{a: a, b: b}, err
	})
	return v.a, v.b, err
}

func withSchema3[A, B, C any](ctx context.Context, s *Store, ref ns.Ref, missingBucket, missingOther error, fn func(schema string) (A, B, C, error)) (A, B, C, error) {
	type result struct {
		a A
		b B
		c C
	}
	v, err := withSchema(ctx, s, ref, missingBucket, missingOther, func(sc string) (result, error) {
		a, b, c, err := fn(sc)
		return result{a: a, b: b, c: c}, err
	})
	return v.a, v.b, v.c, err
}

func (s *Store) EnsureNamespace(ctx context.Context, kind ns.Kind, name string, quota *int64, attrs map[string]any) error {
	ref, err := NewNamespaceRef(kind, name)
	if err != nil {
		return err
	}
	var attrJSON []byte
	if len(attrs) > 0 {
		var err error
		attrJSON, err = json.Marshal(attrs)
		if err != nil {
			return err
		}
	}
	var sc string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.ensure_namespace($1,$2,$3,$4)",
		string(kind), name, quota, attrJSON).Scan(&sc); err != nil {
		if isInvalidNameErr(err) {
			return ErrInvalidBucket
		}
		if isNamespaceLimitErr(err) {
			return ErrTooManyNamespaces
		}
		return err
	}
	if kind == ns.Queue {
		if _, err := s.pool.Exec(ctx, "SELECT s3p.tune_queue_schema($1)", sc); err != nil {
			return err
		}
	}
	s.knownNS.Store(ref.Key(), struct{}{})
	return nil
}

func isInvalidNameErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22023"
}

var ErrTooManyNamespaces = errors.New("namespace limit reached")

func isNamespaceLimitErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "54000"
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	return s.EnsureNamespace(ctx, ns.Bucket, name, nil, nil)
}

func (q Queue) Ensure(ctx context.Context, attrs map[string]any) error {
	return q.s.EnsureNamespace(ctx, ns.Queue, q.name, nil, attrs)
}

func (s *Store) NamespaceExists(ctx context.Context, kind ns.Kind, name string) (bool, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return false, err
	}
	table := "objects"
	if kind == ns.Queue {
		table = "jobs"
	}
	var present bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL",
		sc+"."+table).Scan(&present); err != nil {
		return false, err
	}
	if !present {
		var hasSchema bool
		if err := s.pool.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", sc).Scan(&hasSchema); err != nil {
			return false, err
		}
		if hasSchema {
			return false, fmt.Errorf("%w: namespace %s is missing required relation %s", ErrNamespaceLayout, sc, table)
		}
	}
	if present && !s.known(ns.Ref{Kind: kind, Name: name}) {
		_, _ = s.Adopt(ctx, kind, name)
	}
	return present, nil
}

func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	return s.NamespaceExists(ctx, ns.Bucket, name)
}

type BucketDeletion int

const (
	BucketDeleted BucketDeletion = iota
	BucketNotEmpty
	BucketNotFound
)

func (s *Store) DropNamespace(ctx context.Context, kind ns.Kind, name string, force bool) (BucketDeletion, error) {
	ref, err := NewNamespaceRef(kind, name)
	if err != nil {
		return BucketNotFound, err
	}
	if kind == ns.Cache || kind == ns.WriteBack {
		var pending int64
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM s3p.pending_uploads WHERE namespace = $1`, name).Scan(&pending); err != nil {
			return 0, err
		}
		if pending > 0 {
			return 0, ErrPendingUploads
		}
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.drop_namespace($1,$2,$3)", string(kind), name, force).Scan(&outcome); err != nil {
		return 0, err
	}
	switch outcome {
	case "deleted":
		s.forget(ref)
		if kind == ns.Cache || kind == ns.WriteBack {
			s.invalidateChunkNS(name)
		}
		return BucketDeleted, nil
	case "not_empty":
		return BucketNotEmpty, nil
	default:
		return BucketNotFound, nil
	}
}

func (s *Store) DropBucket(ctx context.Context, name string, force bool) (BucketDeletion, error) {
	return s.DropNamespace(ctx, ns.Bucket, name, force)
}

func (s *Store) CloneNamespace(ctx context.Context, kind ns.Kind, src, dst string) error {
	dstRef, err := NewNamespaceRef(kind, dst)
	if err != nil {
		return err
	}
	if _, err := NewNamespaceRef(kind, src); err != nil {
		return err
	}
	var outcome string
	outcome, err = withTxOptions(ctx, s, pgx.TxOptions{IsoLevel: pgx.RepeatableRead}, func(tx pgx.Tx) (string, error) {
		var result string
		err := tx.QueryRow(ctx, "SELECT s3p.clone_namespace($1,$2,$3)", string(kind), src, dst).Scan(&result)
		return result, err
	})
	if err != nil {
		return err
	}
	switch outcome {
	case "ok":
		s.knownNS.Store(dstRef.Key(), struct{}{})
		return nil
	case "not_found":
		return ErrNoBucket
	case "not_empty":
		return ErrBucketNotEmpty
	default:
		return fmt.Errorf("unexpected clone outcome %q", outcome)
	}
}

func (s *Store) CloneBucket(ctx context.Context, src, dst string) error {
	return s.CloneNamespace(ctx, ns.Bucket, src, dst)
}

type NamespaceInfo struct {
	Kind      ns.Kind        `json:"kind"`
	Name      string         `json:"name"`
	Schema    string         `json:"schema"`
	Quota     *int64         `json:"quota_bytes,omitempty"`
	Attrs     map[string]any `json:"attrs,omitempty"`
	CreatedAt time.Time      `json:"created_at"`

	maxBytes int64
}

const namespaceCols = "kind, name, schema_name, quota_bytes, attrs, created_at"

func scanNamespace(row pgx.CollectableRow) (NamespaceInfo, error) {
	var n NamespaceInfo
	var kind string
	var attrs []byte
	if err := row.Scan(&kind, &n.Name, &n.Schema, &n.Quota, &attrs, &n.CreatedAt); err != nil {
		return n, err
	}
	n.Kind = ns.Kind(kind)
	if len(attrs) > 0 {
		_ = json.Unmarshal(attrs, &n.Attrs)
	}
	if v, ok := n.Attrs["max_bytes"].(float64); ok && v > 0 {
		n.maxBytes = int64(v)
	}
	return n, nil
}

func (s *Store) Namespaces(ctx context.Context, kind ns.Kind) ([]NamespaceInfo, error) {
	q := "SELECT " + namespaceCols + " FROM s3p.namespaces"
	var args []any
	if kind != "" {
		q += " WHERE kind = $1"
		args = append(args, string(kind))
	}
	q += " ORDER BY kind, name"
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanNamespace)
}

func (s *Store) Adopt(ctx context.Context, kind ns.Kind, name string) (string, error) {
	ref, err := NewNamespaceRef(kind, name)
	if err != nil {
		return "", err
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.adopt_namespace($1,$2)", string(kind), name).Scan(&outcome); err != nil {
		return "", err
	}
	if outcome == "adopted" || outcome == "exists" {
		s.markKnown(ref)
	} else if outcome == "limit" {
		return outcome, ErrTooManyNamespaces
	}
	return outcome, nil
}

func (s *Store) AdoptAll(ctx context.Context, kind ns.Kind) (int, error) {
	var arg any
	if kind != "" {
		if !kind.Valid() {
			return 0, ns.ErrUnknownKind
		}
		arg = string(kind)
	}
	var n int
	if err := s.pool.QueryRow(ctx, "SELECT s3p.adopt_namespaces($1)", arg).Scan(&n); err != nil {
		return 0, err
	}
	s.knownNS.Clear()
	infos, err := s.Namespaces(ctx, "")
	if err == nil {
		for _, n := range infos {
			s.markKnown(ns.Ref{Kind: n.Kind, Name: n.Name})
		}
	}
	return n, nil
}

func (s *Store) NamespaceAttrs(ctx context.Context, kind ns.Kind, name string) (map[string]any, bool, error) {
	if _, err := NewNamespaceRef(kind, name); err != nil {
		return nil, false, err
	}
	var raw []byte
	err := s.pool.QueryRow(ctx,
		"SELECT attrs FROM s3p.namespaces WHERE kind = $1 AND name = $2",
		string(kind), name).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return out, true, nil
}

func (s *Store) SetNamespaceAttrs(ctx context.Context, kind ns.Kind, name string, attrs map[string]any) error {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return err
	}
	b, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		"UPDATE s3p.namespaces SET attrs = attrs || $3::jsonb WHERE kind = $1 AND name = $2",
		string(kind), name, b); err != nil {
		return err
	}
	_, _ = s.pool.Exec(ctx, "UPDATE "+sc+".meta SET attrs = attrs || $1::jsonb", b)
	return nil
}

func (s *Store) NamespacesWithAttr(ctx context.Context, attr string) ([]NamespaceInfo, error) {
	all, err := s.Namespaces(ctx, "")
	if err != nil {
		return nil, err
	}
	out := make([]NamespaceInfo, 0)
	for _, n := range all {
		if _, ok := n.Attrs[attr]; ok {
			out = append(out, n)
		}
	}
	return out, nil
}

func (s *Store) RemoveNamespaceAttr(ctx context.Context, kind ns.Kind, name, attr string) error {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx,
		"UPDATE s3p.namespaces SET attrs = attrs - $3 WHERE kind = $1 AND name = $2",
		string(kind), name, attr); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, "UPDATE "+sc+".meta SET attrs = attrs - $1", attr)
	return err
}

func (s *Store) TouchNamespaceObject(ctx context.Context, kind ns.Kind, name, key string, retention time.Duration) error {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return err
	}
	if retention > 0 {
		_, err = s.pool.Exec(ctx,
			"UPDATE "+sc+".objects SET created_at = now(), expires_at = now() + make_interval(secs => $2) WHERE key = $1",
			key, retention.Seconds())
	} else {
		_, err = s.pool.Exec(ctx, "UPDATE "+sc+".objects SET created_at = now() WHERE key = $1", key)
	}
	return err
}

type quotaState struct {
	checked time.Time
	size    int64
	quota   int64
}

func (s *Store) checkQuota(ctx context.Context, kind ns.Kind, name string, add int64) bool {
	return s.checkQuotaOn(ctx, s.pool, kind, name, add)
}

// checkQuotaOn queries through db, so a caller inside a transaction passes it
// instead of waiting on the pool while holding a connection.
func (s *Store) checkQuotaOn(ctx context.Context, db rowQuerier, kind ns.Kind, name string, add int64) bool {
	if s.overDBBudget(ctx, db, add) {
		return true
	}
	key := ns.Ref{Kind: kind, Name: name}.Key()
	var st quotaState
	if v, ok := s.quota.Load(key); ok {
		st = v.(quotaState)
	}
	if st.checked.IsZero() || time.Since(st.checked) > time.Second {
		var q pgtype.Int8
		err := db.QueryRow(ctx,
			"SELECT quota_bytes FROM s3p.namespaces WHERE kind = $1 AND name = $2",
			string(kind), name).Scan(&q)
		if errors.Is(err, pgx.ErrNoRows) {
			return false
		}
		if err != nil || !q.Valid {
			s.quota.Store(key, quotaState{checked: time.Now()})
			return false
		}
		st = quotaState{checked: time.Now(), quota: q.Int64, size: namespaceSize(ctx, db, kind, name)}
		s.quota.Store(key, st)
	}
	if st.quota <= 0 {
		return false
	}
	return st.size+add > st.quota
}

var (
	queueSizeRelations = []string{"jobs", "stream_events", "stream_events_0", "stream_events_1",
		"stream_events_2", "stream_ticks", "stream_subscriptions", "stream_batches",
		"stream_batch_events", "stream_retry_events", "stream_dead_letters"}
	objectSizeRelations = []string{"objects", "chunks", "uploads", "upload_parts",
		"object_part_checksums", "garbage", "origin_metadata"}
)

func namespaceSizeQuery(kind ns.Kind, quotedSchema string) (string, []any) {
	relations := objectSizeRelations
	if kind == ns.Queue {
		relations = queueSizeRelations
	}
	args := make([]any, len(relations))
	var query strings.Builder
	query.WriteString("SELECT ")
	for i, relation := range relations {
		if i > 0 {
			query.WriteString(" + ")
		}
		query.WriteString("pg_total_relation_size($")
		query.WriteString(strconv.Itoa(i + 1))
		query.WriteString("::regclass)")
		args[i] = quotedSchema + "." + relation
	}
	return query.String(), args
}

func namespaceSize(ctx context.Context, db rowQuerier, kind ns.Kind, name string) int64 {
	sc, err := kind.Schema(name)
	if err != nil {
		return 0
	}
	query, args := namespaceSizeQuery(kind, quoteIdent(sc))
	var size int64
	if err := db.QueryRow(ctx, query, args...).Scan(&size); err != nil {
		return 0
	}
	return size
}

func (s *Store) setQuotaState(kind ns.Kind, name string, size, quota int64) {
	s.quota.Store(ns.Ref{Kind: kind, Name: name}.Key(), quotaState{checked: time.Now(), size: size, quota: quota})
}

func (s *Store) overDBBudget(ctx context.Context, db rowQuerier, add int64) bool {
	if s.maxDBBytes <= 0 {
		return false
	}
	now := time.Now().UnixNano()
	size := s.dbSize.Load()
	if size == 0 || now-s.dbSizeAt.Load() > int64(5*time.Second) {
		var n int64
		if err := db.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&n); err != nil {
			return false
		}
		size = n
		s.dbSize.Store(n)
		s.dbSizeAt.Store(now)
	}
	return size+add > s.maxDBBytes
}

type relationRequirement struct {
	relation  string
	cacheSkip bool
}

var queueRelations = []relationRequirement{
	{relation: "jobs_claim"}, {relation: "jobs_active"}, {relation: "jobs_state"},
	{relation: "jobs_expiry"}, {relation: "jobs_terminal_age"}, {relation: "stream_config"},
	{relation: "stream_ticks"}, {relation: "stream_ticks_created"},
	{relation: "stream_segments"}, {relation: "stream_event_seq"},
	{relation: "stream_events"}, {relation: "stream_events_0"},
	{relation: "stream_events_1"}, {relation: "stream_events_2"},
	{relation: "stream_events_txid"}, {relation: "stream_events_seq"},
	{relation: "stream_subscriptions"}, {relation: "stream_batches"},
	{relation: "stream_batches_active"}, {relation: "stream_batch_events"},
	{relation: "stream_retry_events"}, {relation: "stream_dead_letters"},
	{relation: "meta"},
}

var bucketRelations = []relationRequirement{
	{relation: "objects"}, {relation: "origin_metadata"},
	{relation: "origin_metadata_expiry"}, {relation: "objects_file_id_idx"},
	{relation: "objects_parts_idx"},
	{relation: "upload_parts", cacheSkip: true},
	{relation: "upload_parts_file_id_idx", cacheSkip: true},
	{relation: "garbage"},
}

func (s *Store) checkNamespaceTables(ctx context.Context, conn *pgxpool.Conn) error {
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return err
	}
	for _, n := range infos {
		if err := checkNamespaceLayout(ctx, conn, n); err != nil {
			return err
		}
	}
	return nil
}

func checkNamespaceLayout(ctx context.Context, conn *pgxpool.Conn, n NamespaceInfo) error {
	q := quoteIdent(n.Schema)
	if n.Kind == ns.Queue {
		present, err := relationExists(ctx, conn, q+".jobs")
		if err != nil || !present {
			return err
		}
		return checkRelations(ctx, conn, q, queueRelations, false)
	}
	present, err := relationExists(ctx, conn, q+".objects")
	if err != nil || !present {
		return err
	}
	if err := checkChunksLayout(ctx, conn, n.Schema); err != nil {
		return err
	}
	return checkRelations(ctx, conn, q, bucketRelations, n.Kind == ns.Cache)
}

func relationExists(ctx context.Context, db rowQuerier, relation string) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists)
	return exists, err
}

func checkRelations(ctx context.Context, conn *pgxpool.Conn, schema string, requirements []relationRequirement, cache bool) error {
	for _, req := range requirements {
		if cache && req.cacheSkip {
			continue
		}
		rel := schema + "." + req.relation
		exists, err := relationExists(ctx, conn, rel)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required relation %s is missing", rel)
		}
	}
	return nil
}

func chunksConfigured(ctx context.Context, db rowQuerier, schema string) (bool, string, error) {
	var relkind string
	var configured bool
	err := db.QueryRow(ctx, `SELECT c.relkind::text,
		c.reloptions @> ARRAY['toast_tuple_target=8160',
			'autovacuum_vacuum_scale_factor=0.01',
			'autovacuum_analyze_scale_factor=0.02',
			'autovacuum_vacuum_threshold=1000']
		FROM pg_class c
		JOIN pg_namespace p ON p.oid = c.relnamespace
		WHERE p.nspname = $1 AND c.relname = 'chunks'`, schema).Scan(&relkind, &configured)
	return configured && relkind == "r", relkind, err
}

func checkChunksLayout(ctx context.Context, conn *pgxpool.Conn, schema string) error {
	ok, _, err := chunksConfigured(ctx, conn, schema)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s.chunks lacks the expected inline storage settings", schema)
	}
	return nil
}
