package store

import (
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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const LayoutVersion = 24

const (
	smallMax            = 8 << 20
	wholeReadMax        = 64 << 20
	partRows            = smallMax/RowBytes + 2
	chunkSize           = 256 << 10
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

	fileIDMu    sync.Mutex
	fileIDBlock []int64
	fileIDPos   int

	reapedRows      atomic.Int64
	reapedFiles     atomic.Int64
	expiredObjects  atomic.Int64
	maintenanceRuns atomic.Int64
	layoutChecked   atomic.Int64
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
	if opts.MaxConns <= 0 {
		opts.MaxConns = 64
	}
	if opts.WorkMem == "" {
		opts.WorkMem = "16MB"
	}
	if !validWorkMem(opts.WorkMem) {
		return nil, fmt.Errorf("invalid work_mem %q", opts.WorkMem)
	}
	if opts.MinIdleConns <= 0 {
		opts.MinIdleConns = min(opts.MaxConns, 8)
	}
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
	sessionSQL := func(indexScan bool, statementTimeout time.Duration) string {
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
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {

		_, err := conn.Exec(ctx, sessionSQL(true, opts.StatementTimeout))
		return err
	}

	pool, err := openPool(ctx, cfg)
	if err != nil {
		return nil, err
	}

	chunkCfg := cfg.Copy()
	chunkCfg.MaxConns = min(opts.MaxConns, 64)
	chunkCfg.MinConns = min(opts.MaxConns, 4)
	chunkCfg.MinIdleConns = min(opts.MinIdleConns, chunkCfg.MaxConns)
	chunkCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, sessionSQL(false, 0))
		return err
	}
	chunkPool, err := openPool(ctx, chunkCfg)
	if err != nil {
		pool.Close()
		return nil, err
	}

	var wg sync.WaitGroup
	errs := make(chan error, cfg.MinConns)
	for range cfg.MinConns {
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
	for err := range errs {
		pool.Close()
		chunkPool.Close()
		return nil, err
	}
	return &Store{
		pool: pool, chunkPool: chunkPool, log: log,
		jobRetention: DefaultJobRetention,
	}, nil
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

func (s *Store) EnsureMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, rel := range layoutManifest.relations {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required layout relation %s is missing", rel)
		}
	}
	for _, fn := range layoutManifest.functions {
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
	fmt.Fprintf(w, "pgvs3_maintenance_runs_total %d\n", s.maintenanceRuns.Load())
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

func (s *Store) MaintenanceRuns() int64 { return s.maintenanceRuns.Load() }

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

//go:embed schema.sql
var schemaSQL string

const schemaLockKey = int64(0x70677673)

var layoutManifest = struct {
	relations []string
	functions []string
}{
	relations: []string{
		"s3p.layout", "s3p.namespaces", "s3p.file_ids", "s3p.job_index",
		"s3p.pending_uploads", "s3p.cache_fill_leases",
	},
	functions: []string{
		"s3p.namespace_schema(text,text)",
		"s3p.ensure_namespace(text,text,bigint,jsonb)",
		"s3p.ensure_object_schema(text,text)",
		"s3p.ensure_queue_schema(text)",
		"s3p.tune_queue_schema(text)",
		"s3p.drop_namespace(text,text,boolean)",
		"s3p.clone_namespace(text,text,text)",
		"s3p.adopt_namespace(text,text)",
		"s3p.adopt_namespaces(text)",
		"s3p.reconcile_namespaces()",
		"s3p.upgrade_queue_schemas()",
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
	if kind != ns.Bucket {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return "", err
		}
		return sc, nil
	}
	present, err := s.NamespaceExists(ctx, kind, name)
	if err != nil {
		return "", err
	}
	if !present {
		return "", ErrNoBucket
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
	if !isMissingBucketSchema(err) {
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
	if !isMissingBucketSchema(err) {
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

func (s *Store) EnsureQueue(ctx context.Context, name string, attrs map[string]any) error {
	return s.EnsureNamespace(ctx, ns.Queue, name, nil, attrs)
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
	if s.overDBBudget(ctx, add) {
		return true
	}
	key := ns.Ref{Kind: kind, Name: name}.Key()
	var st quotaState
	if v, ok := s.quota.Load(key); ok {
		st = v.(quotaState)
	}
	if st.checked.IsZero() || time.Since(st.checked) > time.Second {
		var q pgtype.Int8
		err := s.pool.QueryRow(ctx,
			"SELECT quota_bytes FROM s3p.namespaces WHERE kind = $1 AND name = $2",
			string(kind), name).Scan(&q)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return false
		}
		if err != nil || !q.Valid {
			s.quota.Store(key, quotaState{checked: time.Now()})
			return false
		}
		st = quotaState{checked: time.Now(), quota: q.Int64, size: s.namespaceSize(ctx, kind, name)}
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

func (s *Store) namespaceSize(ctx context.Context, kind ns.Kind, name string) int64 {
	sc, err := kind.Schema(name)
	if err != nil {
		return 0
	}
	query, args := namespaceSizeQuery(kind, quoteIdent(sc))
	var size int64
	if err := s.pool.QueryRow(ctx, query, args...).Scan(&size); err != nil {
		return 0
	}
	return size
}

func (s *Store) setQuotaState(kind ns.Kind, name string, size, quota int64) {
	s.quota.Store(ns.Ref{Kind: kind, Name: name}.Key(), quotaState{checked: time.Now(), size: size, quota: quota})
}

func (s *Store) overDBBudget(ctx context.Context, add int64) bool {
	if s.maxDBBytes <= 0 {
		return false
	}
	now := time.Now().UnixNano()
	size := s.dbSize.Load()
	if size == 0 || now-s.dbSizeAt.Load() > int64(5*time.Second) {
		var n int64
		if err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&n); err != nil {
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

func relationExists(ctx context.Context, conn *pgxpool.Conn, relation string) (bool, error) {
	var exists bool
	err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists)
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

type Stats struct {
	Buckets       int64          `json:"buckets"`
	Namespaces    int64          `json:"namespaces"`
	Objects       int64          `json:"objects"`
	Chunks        int64          `json:"chunks"`
	Events        int64          `json:"events"`
	LogicalBytes  int64          `json:"logical_bytes"`
	PhysicalBytes int64          `json:"physical_bytes"`
	DatabaseBytes int64          `json:"database_bytes"`
	GarbageFiles  int64          `json:"garbage_files"`
	OldestGarbage *time.Duration `json:"oldest_garbage,omitempty"`
}

func (s *Store) Stat(ctx context.Context) (Stats, error) { return s.StatNamespace(ctx, "", "") }

func (s *Store) StatNamespace(ctx context.Context, kind ns.Kind, name string) (Stats, error) {
	var st Stats
	rows, err := s.pool.Query(ctx, `SELECT kind, name, schema_name
		FROM s3p.namespaces
		WHERE ($1 = '' OR kind = $1) AND ($2 = '' OR name = $2)
		ORDER BY kind, name`, string(kind), name)
	if err != nil {
		return st, err
	}
	infos, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (NamespaceInfo, error) {
		var n NamespaceInfo
		var k string
		err := row.Scan(&k, &n.Name, &n.Schema)
		n.Kind = ns.Kind(k)
		return n, err
	})
	if err != nil {
		return st, err
	}
	var maxOldest float64
	var hasOldest bool
	for _, n := range infos {
		st.Namespaces++
		q := quoteIdent(n.Schema)
		var logical, physical, objs, chunks, events, garb int64
		var oldest pgtype.Float8
		var err error
		if n.Kind == ns.Queue {
			err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(pg_column_size(args) + coalesce(pg_column_size(meta),0) + octet_length(type)),0)::int8 FROM `+q+`.jobs)
				+ (SELECT COALESCE(sum(pg_column_size(payload) + pg_column_size(headers)),0)::int8 FROM `+q+`.stream_events),
				pg_total_relation_size('`+q+`.jobs')
				+ pg_total_relation_size('`+q+`.stream_events')
				+ pg_total_relation_size('`+q+`.stream_events_0')
				+ pg_total_relation_size('`+q+`.stream_events_1')
				+ pg_total_relation_size('`+q+`.stream_events_2')
				+ pg_total_relation_size('`+q+`.stream_ticks')
				+ pg_total_relation_size('`+q+`.stream_subscriptions')
				+ pg_total_relation_size('`+q+`.stream_batches')
				+ pg_total_relation_size('`+q+`.stream_batch_events')
				+ pg_total_relation_size('`+q+`.stream_retry_events')
				+ pg_total_relation_size('`+q+`.stream_dead_letters'),
				(SELECT count(*) FROM `+q+`.jobs),
				0,
				(SELECT count(*) FROM `+q+`.stream_events),
				0, NULL`).
				Scan(&logical, &physical, &objs, &chunks, &events, &garb, &oldest)
		} else {
			err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(size),0)::int8 FROM `+q+`.objects),
				pg_total_relation_size('`+q+`.objects') + pg_total_relation_size('`+q+`.chunks') + pg_total_relation_size('`+q+`.origin_metadata'),
				(SELECT count(*) FROM `+q+`.objects),
				(SELECT COALESCE(sum(
					CASE
						WHEN part_ends IS NOT NULL THEN (
							SELECT sum((part_ends[i] - CASE WHEN i = 1 THEN 0 ELSE part_ends[i-1] END + 8119) / 8120)
							FROM generate_subscripts(part_ends, 1) AS i
						)
						ELSE (size + 8119) / 8120
					END
				),0)::int8 FROM `+q+`.objects WHERE file_id IS NOT NULL),
				0,
				(SELECT count(*) FROM `+q+`.garbage),
				(SELECT EXTRACT(EPOCH FROM now() - min(queued_at))::float8 FROM `+q+`.garbage)`).
				Scan(&logical, &physical, &objs, &chunks, &events, &garb, &oldest)
		}
		if isMissingBucketSchema(err) {
			continue
		}
		if err != nil {
			return st, err
		}
		st.LogicalBytes += logical
		st.PhysicalBytes += physical
		st.Objects += objs
		st.Chunks += chunks
		st.Events += events
		st.GarbageFiles += garb
		if n.Kind == ns.Bucket {
			st.Buckets++
		}
		if oldest.Valid && (!hasOldest || oldest.Float64 > maxOldest) {
			maxOldest = oldest.Float64
			hasOldest = true
		}
	}
	if err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&st.DatabaseBytes); err != nil {
		return st, err
	}
	if hasOldest {
		d := time.Duration(maxOldest * float64(time.Second))
		st.OldestGarbage = &d
	}
	return st, nil
}

const expirePassRounds = 128

type maintenancePass struct {
	name   string
	budget int
	run    func(ctx context.Context, budget int) error
}

func runMaintenancePasses(ctx context.Context, passes []maintenancePass) error {
	for _, pass := range passes {
		if err := pass.run(ctx, pass.budget); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Maintain(ctx context.Context, grace time.Duration) error {
	s.maintenanceRuns.Add(1)
	const lock int64 = 0x70677674
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

	if _, err := conn.Exec(ctx, "SELECT s3p.reconcile_namespaces()"); err != nil {
		return err
	}
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return err
	}
	if _, err := s.CleanupFillLeases(ctx); err != nil {
		return err
	}
	if err := runMaintenancePasses(ctx, s.maintenancePasses(conn, infos, grace)); err != nil {
		return err
	}
	s.refreshQuotas(ctx, infos)
	return nil
}

func (s *Store) maintenancePasses(conn *pgxpool.Conn, infos []NamespaceInfo, grace time.Duration) []maintenancePass {
	return []maintenancePass{
		{name: "expire", budget: expirePassRounds, run: func(ctx context.Context, budget int) error {
			return s.expirePass(ctx, conn, infos, budget)
		}},
		{name: "reap", budget: ReapRows, run: func(ctx context.Context, budget int) error {
			return s.reapPass(ctx, conn, infos, grace, budget)
		}},
	}
}

func (s *Store) expirePass(ctx context.Context, conn *pgxpool.Conn, infos []NamespaceInfo, rounds int) error {
	var expiredUploads int
	for _, n := range infos {
		if n.Kind == ns.Queue {
			removed, err := s.expireQueue(ctx, n.Name, rounds)
			if err != nil {
				return err
			}
			s.expiredObjects.Add(removed)
			continue
		}
		q := quoteIdent(n.Schema)
		var uploads int
		if err := conn.QueryRow(ctx, "SELECT "+q+".expire_uploads(interval '24 hours', 64)").Scan(&uploads); err != nil {
			if isMissingBucketSchema(err) {
				continue
			}
			return err
		}
		expiredUploads += uploads
		if err := s.expireNamespace(ctx, conn, q, rounds); err != nil {
			return err
		}
	}
	if expiredUploads > 0 {
		s.log.Info("expired multipart uploads", "count", expiredUploads)
	}
	return nil
}

func (s *Store) expireQueue(ctx context.Context, queue string, rounds int) (int64, error) {
	var total int64
	for range rounds {
		removed, err := s.ExpireQueueMessages(ctx, queue, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += removed
		if removed < ExpireBatch {
			break
		}
	}
	for range rounds {
		discarded, err := s.DiscardExpiredLeases(ctx, queue, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += discarded
		if discarded < ExpireBatch {
			break
		}
	}
	for range rounds {
		moved, err := s.ProcessStreamRetries(ctx, queue, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += moved
		if moved < ExpireBatch {
			break
		}
	}
	if rotated, err := s.RotateStream(ctx, queue); err != nil {
		return total, err
	} else {
		total += rotated
	}
	if pruned, err := s.PruneStreamTicks(ctx, queue, ExpireBatch); err != nil {
		return total, err
	} else {
		total += pruned
	}
	return total, nil
}

func (s *Store) expireNamespace(ctx context.Context, conn *pgxpool.Conn, schema string, rounds int) error {

	for range rounds {
		var v int
		if err := conn.QueryRow(ctx, "SELECT "+schema+".expire_objects($1)", ExpireBatch).Scan(&v); err != nil {
			if isMissingBucketSchema(err) {
				return nil
			}
			return err
		}
		s.expiredObjects.Add(int64(v))
		if v < ExpireBatch {
			return nil
		}
	}
	return nil
}

func (s *Store) reapPass(ctx context.Context, conn *pgxpool.Conn, infos []NamespaceInfo, grace time.Duration, budget int) error {
	remaining := budget
	for _, n := range infos {
		if n.Kind == ns.Queue {
			continue
		}
		var err error
		remaining, err = s.reapNamespace(ctx, conn.Conn(), quoteIdent(n.Schema), grace, remaining)
		if err != nil {
			return err
		}
		if remaining <= 0 {
			break
		}
	}
	return nil
}

func (s *Store) reapNamespace(ctx context.Context, conn *pgx.Conn, schema string, grace time.Duration, remaining int) (int, error) {
	for remaining > 0 {
		got, done, err := s.reapOne(ctx, conn, schema, grace, remaining)
		if isMissingBucketSchema(err) {
			return remaining, nil
		}
		if err != nil {
			return remaining, err
		}
		if got == 0 && !done {
			return remaining, nil
		}
		s.reapedRows.Add(int64(got))
		if done {
			s.reapedFiles.Add(1)
		}
		remaining -= max(got, 1)
	}
	return remaining, nil
}

func (s *Store) ExpireObjects(ctx context.Context, kind ns.Kind, name string, max int) (int, error) {
	q, err := nsSQLIdent(kind, name)
	if err != nil {
		return 0, err
	}
	var n int
	err = s.pool.QueryRow(ctx, "SELECT "+q+".expire_objects($1)", max).Scan(&n)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return n, err
}

func (s *Store) refreshQuotas(ctx context.Context, infos []NamespaceInfo) {
	for _, n := range infos {
		if n.Quota == nil {
			continue
		}
		query, args := namespaceSizeQuery(n.Kind, quoteIdent(n.Schema))
		var size int64
		if err := s.pool.QueryRow(ctx, query, args...).Scan(&size); err != nil {
			continue
		}
		s.setQuotaState(n.Kind, n.Name, size, *n.Quota)
	}
}

func (s *Store) reapOne(ctx context.Context, conn *pgx.Conn, schema string, grace time.Duration, budget int) (int, bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var fileID int64
	err = tx.QueryRow(ctx, `SELECT file_id FROM `+schema+`.garbage
		WHERE queued_at <= now() - make_interval(secs => $1)
		ORDER BY queued_at, file_id LIMIT 1 FOR UPDATE SKIP LOCKED`, grace.Seconds()).Scan(&fileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var referenced bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.objects WHERE file_id IS NOT NULL AND file_id = $1)
		OR EXISTS (SELECT 1 FROM `+schema+`.objects WHERE parts IS NOT NULL AND parts @> ARRAY[$1::int8])
		OR EXISTS (SELECT 1 FROM `+schema+`.upload_parts WHERE file_id = $1)`, fileID).Scan(&referenced); err != nil {
		return 0, false, err
	}
	if referenced {

		if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
			return 0, false, err
		}
		return 0, true, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM `+schema+`.chunks WHERE ctid IN
		(SELECT ctid FROM `+schema+`.chunks WHERE file_id = $1 LIMIT $2)`, fileID, budget)
	if err != nil {
		return 0, false, err
	}
	n := int(tag.RowsAffected())
	done := false
	if n < budget {
		var left bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+schema+".chunks WHERE file_id = $1)", fileID).Scan(&left); err != nil {
			return 0, false, err
		}
		if !left {
			if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
				return 0, false, err
			}
			done = true
		}
	}
	return n, done, tx.Commit(ctx)
}

type Diagnosis struct {
	ServerVersion     int    `json:"server_version_num"`
	AuroraVersion     string `json:"aurora_version,omitempty"`
	Layout            int    `json:"layout"`
	Namespaces        int64  `json:"namespaces"`
	OverScale         bool   `json:"over_supported_scale"`
	Buckets           int64  `json:"buckets"`
	Queues            int64  `json:"queues"`
	Caches            int64  `json:"caches"`
	ChunksRelkind     string `json:"chunks_relkind"`
	ChunksConfigured  bool   `json:"chunks_configured"`
	Objects           int64  `json:"objects"`
	GarbageFiles      int64  `json:"garbage_files"`
	GarbageReferenced int64  `json:"garbage_referenced"`
	DatabaseBytes     int64  `json:"database_bytes"`
	OrphanSchemas     int64  `json:"orphan_schemas"`
	MissingSchemas    int64  `json:"missing_schemas"`
}

func (s *Store) Diagnose(ctx context.Context) (Diagnosis, error) {
	var d Diagnosis
	var versionText string
	if err := s.pool.QueryRow(ctx, "SHOW server_version_num").Scan(&versionText); err != nil {
		return d, err
	}
	d.ServerVersion, _ = strconv.Atoi(versionText)
	var aurora pgtype.Text
	if err := s.pool.QueryRow(ctx, `SELECT CASE
		WHEN to_regprocedure('aurora_version()') IS NULL THEN ''
		ELSE aurora_version()::text END`).Scan(&aurora); err == nil && aurora.Valid {
		d.AuroraVersion = aurora.String
	}
	var layout pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&layout); err != nil {
		return d, err
	}
	if layout.Valid {
		d.Layout = int(layout.Int32)
	}
	d.ChunksRelkind = "r"
	d.ChunksConfigured = true
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return d, err
	}
	d.Namespaces = int64(len(infos))
	d.OverScale = d.Namespaces > SupportedNamespaces
	for _, n := range infos {
		switch n.Kind {
		case ns.Bucket:
			d.Buckets++
		case ns.Queue:
			d.Queues++
		case ns.Cache, ns.WriteBack:
			d.Caches++
		}
		q := quoteIdent(n.Schema)
		var present bool
		if err := s.pool.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", n.Schema).Scan(&present); err != nil {
			return d, err
		}
		if !present {
			continue
		}
		if n.Kind == ns.Queue {
			var hasIndex bool
			if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", q+".jobs_claim").Scan(&hasIndex); err != nil || !hasIndex {
				d.ChunksConfigured = false
			}
			var jobs int64
			if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM "+q+".jobs").Scan(&jobs); err == nil {
				d.Objects += jobs
			}
			continue
		}
		configured, relkind, err := chunksConfigured(ctx, s.pool, n.Schema)
		if err != nil || !configured {
			d.ChunksConfigured = false
		}
		if relkind != "" {
			d.ChunksRelkind = relkind
		}
		var objs, garb int64
		if err := s.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM "+q+".objects), (SELECT count(*) FROM "+q+".garbage)").
			Scan(&objs, &garb); err == nil {
			d.Objects += objs
			d.GarbageFiles += garb
		}

		uploadCheck := ""
		if n.Kind == ns.Bucket {
			uploadCheck = " OR EXISTS (SELECT 1 FROM " + q + ".upload_parts p WHERE p.file_id = g.file_id)"
		}
		var referenced int64
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+q+`.garbage g
			WHERE EXISTS (SELECT 1 FROM `+q+`.objects o WHERE o.file_id = g.file_id)
			   OR EXISTS (SELECT 1 FROM `+q+`.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[g.file_id])`+
			uploadCheck).Scan(&referenced); err == nil {
			d.GarbageReferenced += referenced
		}
	}
	if err := s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM pg_namespace p
		  WHERE p.nspname ~ '^s3[bcqw]_[a-z0-9._-]{1,58}$'
		    AND NOT EXISTS (SELECT 1 FROM s3p.namespaces n WHERE n.schema_name = p.nspname)),
		(SELECT count(*) FROM s3p.namespaces n
		  WHERE to_regnamespace(format('%I', n.schema_name)) IS NULL)`).
		Scan(&d.OrphanSchemas, &d.MissingSchemas); err != nil {
		return d, err
	}
	if err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&d.DatabaseBytes); err != nil {
		return d, err
	}
	return d, nil
}
