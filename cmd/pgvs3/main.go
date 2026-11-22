package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adonm/pgvs3/internal/gateway"
	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/telemetry"
)

var version = "dev"

func main() { os.Exit(run(os.Args[1:])) }

type command struct {
	name    string
	summary string
	run     func([]string) int
}

var commandTable = []command{
	{name: "serve", summary: "Run the S3 cache gateway (default when no command is given).", run: serve},
	{name: "health", summary: "Exit 0 when a gateway's readiness URL returns 200.", run: health},
	{name: "reap", summary: "Run one bounded maintenance pass (or all passes) without a gateway.", run: reap},
	{name: "stat", summary: "Print namespace, object, chunk and storage-overhead counts.", run: stat},
	{name: "version", summary: "Print the binary version.", run: func([]string) int { fmt.Println(version); return 0 }},
}

// describeFlags, when set, receives each command's flags instead of running it.
// The reference test uses it to generate docs/reference.md from the real flag sets.
var describeFlags func(command string, fs *flag.FlagSet)

var errDescribed = errors.New("flags described")

// reportedFlagError marks a flag.Parse error whose output the flag package
// already wrote, so callers do not print it twice.
type reportedFlagError struct{ err error }

func (e *reportedFlagError) Error() string { return e.err.Error() }
func (e *reportedFlagError) Unwrap() error { return e.err }

func flagExitCode(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	return 2
}

var commandByName = indexCommands(commandTable)

func indexCommands(table []command) map[string]command {
	index := make(map[string]command, len(table))
	for _, cmd := range table {
		index[cmd.name] = cmd
	}
	return index
}

func run(args []string) int {
	cmd, rest, ok := selectCommand(args)
	if !ok {
		return serve(args)
	}
	return cmd.run(rest)
}

func selectCommand(args []string) (command, []string, bool) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			return command{}, args, false
		}
		if strings.HasPrefix(a, "-") {
			if !strings.Contains(a, "=") && flagTakesValue(strings.TrimLeft(a, "-")) && i+1 < len(args) {
				i++
			}
			continue
		}
		cmd, ok := commandByName[a]
		if !ok {
			return command{}, args, false
		}
		out := make([]string, 0, len(args)-1)
		out = append(out, args[:i]...)
		out = append(out, args[i+1:]...)
		return cmd, out, true
	}
	return command{}, args, false
}

func extractSubcommand(args []string) (string, []string) {
	cmd, rest, ok := selectCommand(args)
	if !ok {
		return "", args
	}
	return cmd.name, rest
}

var booleanFlags = map[string]bool{
	"read-only":         true,
	"origin-path-style": true,
	"origin-insecure":   true,
	"force":             true,
	"all":               true,
	"json":              true,
	"h":                 true,
	"help":              true,
}

func flagTakesValue(name string) bool {
	return !booleanFlags[name]
}

type serveConfig struct {
	url                     string
	addr                    string
	poolMax                 int
	spoolDir                string
	spoolMaxBytes           int64
	dbWorkMem               string
	dbStatementTimeout      time.Duration
	dbLockTimeout           time.Duration
	dbIdleTxTimeout         time.Duration
	dbMaxConnLifetime       time.Duration
	dbMaxConnLifetimeJitter time.Duration
	maxConns                int
	httpReadTimeout         time.Duration
	httpWriteTimeout        time.Duration
	maxDBBytes              int64
	reapGrace               time.Duration
	readOnly                bool
	logLevel                string
	originEndpoint          string
	originRegion            string
	originPathStyle         bool
	originInsecure          bool
	buckets                 string
	revalidateAfter         time.Duration
	cacheRetention          time.Duration
	cacheNamespace          string
	fillConcurrency         int
	rangeCacheChunk         int64
	cacheMaxBytes           int64
	s3Concurrency           int
	s3QueueWait             time.Duration
}

func parseServeConfig(args []string) (serveConfig, error) {
	var cfg serveConfig
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.StringVar(&cfg.url, "url", "postgres://localhost/pgvs3", "PostgreSQL URL: the writer endpoint, or a standby with --read-only (use sslmode for database TLS)")
	fs.StringVar(&cfg.addr, "addr", "0.0.0.0:8014", "listen address for S3/HTTP, /healthz, /readyz and /metrics")
	fs.IntVar(&cfg.poolMax, "pool-max", 64, "main-pool PostgreSQL connections; the chunk pool adds up to min(pool-max, 64)")
	fs.StringVar(&cfg.spoolDir, "spool-dir", "", "scratch directory for cache fills over 8 MiB; use local disk, not a small tmpfs (empty uses the system temp dir)")
	fs.Int64Var(&cfg.spoolMaxBytes, "spool-max-bytes", 8<<30, "process-wide spool byte budget for fills; busy admission returns 503")
	fs.StringVar(&cfg.dbWorkMem, "db-work-mem", "16MB", "session work_mem per sort/hash operation (not a per-connection ceiling)")
	fs.DurationVar(&cfg.dbStatementTimeout, "db-statement-timeout", 5*time.Minute, "PostgreSQL statement timeout (0 disables)")
	fs.DurationVar(&cfg.dbLockTimeout, "db-lock-timeout", 5*time.Second, "PostgreSQL lock timeout (0 disables)")
	fs.DurationVar(&cfg.dbIdleTxTimeout, "db-idle-tx-timeout", time.Minute, "PostgreSQL idle-in-transaction timeout (0 disables)")
	fs.DurationVar(&cfg.dbMaxConnLifetime, "db-max-conn-lifetime", time.Hour, "PostgreSQL connection lifetime (0 disables)")
	fs.DurationVar(&cfg.dbMaxConnLifetimeJitter, "db-max-conn-lifetime-jitter", 5*time.Minute, "Randomized PostgreSQL lifetime jitter")
	fs.IntVar(&cfg.maxConns, "max-conns", 4096, "concurrent client connections before shedding load")
	fs.DurationVar(&cfg.httpReadTimeout, "http-read-timeout", 30*time.Minute, "HTTP read deadline including request bodies (0 disables)")
	fs.DurationVar(&cfg.httpWriteTimeout, "http-write-timeout", 30*time.Minute, "HTTP response write deadline (0 disables)")
	fs.Int64Var(&cfg.maxDBBytes, "max-db-bytes", 0, "reject writes when pg_database_size exceeds this (0 = unlimited)")
	fs.DurationVar(&cfg.reapGrace, "reap-grace", 10*time.Minute, "how long overwritten or deleted files stay readable before garbage collection")
	fs.BoolVar(&cfg.readOnly, "read-only", false, "reject writes and skip DDL, reaping, and fills (for standbys)")
	fs.StringVar(&cfg.logLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&cfg.originEndpoint, "origin-endpoint", "", "S3 origin endpoint URL (empty uses AWS S3 for --origin-region)")
	fs.StringVar(&cfg.originRegion, "origin-region", "us-east-1", "S3 origin region")
	fs.BoolVar(&cfg.originPathStyle, "origin-path-style", false, "use path-style S3 origin addressing")
	fs.BoolVar(&cfg.originInsecure, "origin-insecure", false, "skip TLS certificate verification for origin")
	fs.StringVar(&cfg.buckets, "buckets", "", "optional comma-separated bucket allowlist (empty allows all valid bucket names)")
	fs.DurationVar(&cfg.revalidateAfter, "revalidate-after", 0, "origin revalidation interval (0 = immutable, never revalidate hits)")
	fs.DurationVar(&cfg.cacheRetention, "cache-retention", 24*time.Hour, "evict cached objects after this idle period")
	fs.StringVar(&cfg.cacheNamespace, "cache-namespace", "origin", "cache namespace for origin objects")
	fs.IntVar(&cfg.fillConcurrency, "fill-concurrency", 8, "process-wide concurrent origin fills")
	fs.Int64Var(&cfg.rangeCacheChunk, "range-cache-chunk", 0, "cache aligned ranges of large objects at this byte size (0 disables)")
	fs.Int64Var(&cfg.cacheMaxBytes, "cache-max-bytes", 0, "evict oldest cached objects when cache namespace exceeds this byte budget (0 disables)")
	fs.IntVar(&cfg.s3Concurrency, "s3-concurrency", 32, "active S3 requests; excess waits in a queue of twice this, then gets 503 SlowDown")
	fs.DurationVar(&cfg.s3QueueWait, "s3-queue-wait", time.Second, "maximum S3 admission wait (0 rejects excess immediately)")
	if describeFlags != nil {
		describeFlags("serve", fs)
		return cfg, errDescribed
	}
	if err := applyFlagEnv(fs, "PGVS3_"); err != nil {
		return serveConfig{}, err
	}
	if err := fs.Parse(args); err != nil {
		return serveConfig{}, &reportedFlagError{err: err}
	}
	if fs.NArg() > 0 {
		return serveConfig{}, fmt.Errorf("unexpected serve argument %q", fs.Arg(0))
	}
	if cfg.spoolMaxBytes <= 0 || cfg.poolMax <= 0 || cfg.maxConns <= 0 || cfg.fillConcurrency <= 0 ||
		cfg.s3Concurrency <= 0 {
		return serveConfig{}, fmt.Errorf("pool/connection/concurrency limits must be positive")
	}
	if cfg.cacheMaxBytes < 0 {
		return serveConfig{}, fmt.Errorf("--cache-max-bytes must be non-negative")
	}
	if cfg.revalidateAfter < 0 {
		return serveConfig{}, fmt.Errorf("--revalidate-after must be non-negative")
	}
	if cfg.dbStatementTimeout < 0 || cfg.dbLockTimeout < 0 || cfg.dbIdleTxTimeout < 0 ||
		cfg.dbMaxConnLifetime < 0 || cfg.dbMaxConnLifetimeJitter < 0 ||
		cfg.httpReadTimeout < 0 || cfg.httpWriteTimeout < 0 || cfg.s3QueueWait < 0 {
		return serveConfig{}, fmt.Errorf("timeouts must be non-negative")
	}
	return cfg, nil
}

func serve(args []string) int {
	cfg, err := parseServeConfig(args)
	if err != nil {
		if errors.Is(err, errDescribed) {
			return 0
		}
		if _, ok := errors.AsType[*reportedFlagError](err); ok {
			return flagExitCode(err)
		}
		fmt.Fprintf(os.Stderr, "pgvs3 serve: %v\n", err)
		return 2
	}
	log := newLogger(cfg.logLevel)
	shutdownTracing, err := setupTracing(log)
	if err != nil {
		log.Error("initializing tracing", "err", err)
		return 1
	}
	defer shutdownTracing()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.OpenWithOptions(ctx, cfg.url, storeOpenOptions(cfg), log)
	if err != nil {
		log.Error("connecting to PostgreSQL", "err", err)
		return 1
	}
	defer st.Close()
	st.SetMaxDBBytes(cfg.maxDBBytes)

	stopMaintenance, err := prepareStore(ctx, st, cfg, log)
	defer stopMaintenance()
	if err != nil {
		return 1
	}
	runtime, err := cacheRuntime(cfg)
	if err != nil {
		log.Error("configuring origin cache", "err", err)
		return 1
	}
	tier, err := openOrigin(st, runtime, cfg, log)
	if err != nil {
		return 1
	}
	g := newGateway(ctx, st, cfg, tier, log)
	defer g.Close()
	return runGateway(ctx, g, cfg, log)
}

func setupTracing(log *slog.Logger) (func(), error) {
	shutdown, err := telemetry.Setup(context.Background(), version, log)
	if err != nil {
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}, nil
}

func storeOpenOptions(cfg serveConfig) store.OpenOptions {
	return store.OpenOptions{
		MaxConns:              int32(cfg.poolMax),
		WorkMem:               cfg.dbWorkMem,
		StatementTimeout:      cfg.dbStatementTimeout,
		LockTimeout:           cfg.dbLockTimeout,
		IdleTxTimeout:         cfg.dbIdleTxTimeout,
		MaxConnLifetime:       cfg.dbMaxConnLifetime,
		MaxConnLifetimeJitter: cfg.dbMaxConnLifetimeJitter,
		Tracer:                telemetry.PGXTracer{},
	}
}

func prepareStore(ctx context.Context, st *store.Store, cfg serveConfig, log *slog.Logger) (func(), error) {
	noop := func() {}
	if cfg.readOnly {
		if err := st.CheckLayout(ctx); err != nil {
			log.Error("checking storage layout", "err", err)
			return noop, err
		}
	} else if err := st.Init(ctx); err != nil {
		log.Error("initializing schema", "err", err)
		return noop, err
	}
	if err := st.EnsureMaintenance(ctx); err != nil {
		log.Error("verifying maintenance schema", "err", err)
		return noop, err
	}
	if cfg.readOnly {
		return noop, nil
	}
	return startMaintenance(ctx, st, cfg.reapGrace, log), nil
}

func splitCSV(raw string) []string {
	var out []string
	for item := range strings.SplitSeq(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func startMaintenance(ctx context.Context, st *store.Store, reapGrace time.Duration, log *slog.Logger) func() {
	maintainCtx, cancelMaintain := context.WithCancel(ctx)
	maintainDone := make(chan struct{})
	go func() {
		defer close(maintainDone)
		maintain(maintainCtx, st, reapGrace, time.Minute, log)
	}()
	return func() {
		cancelMaintain()
		select {
		case <-maintainDone:
		case <-time.After(10 * time.Second):
			log.Warn("maintenance did not stop within shutdown timeout")
		}
	}
}

func cacheRuntime(cfg serveConfig) (*objcache.Runtime, error) {
	if cfg.spoolDir != "" {
		info, err := os.Stat(cfg.spoolDir)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("invalid spool directory %q: %v", cfg.spoolDir, err)
		}
	}
	return objcache.NewRuntime(objcache.RuntimeOptions{
		Fills: cfg.fillConcurrency, SpoolDir: cfg.spoolDir, SpoolBytes: cfg.spoolMaxBytes,
	}), nil
}

func openOrigin(st *store.Store, runtime *objcache.Runtime, cfg serveConfig, log *slog.Logger) (*objcache.Tier, error) {
	region := cfg.originRegion
	if region == "" {
		region = firstEnv("AWS_REGION", "AWS_DEFAULT_REGION")
	}
	origin, err := objcache.NewS3Origin(objcache.S3Config{
		Endpoint:     cfg.originEndpoint,
		Region:       region,
		AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		Insecure:     cfg.originInsecure,
		PathStyle:    cfg.originPathStyle,
		Buckets:      splitCSV(cfg.buckets),
	})
	if err != nil {
		log.Error("configuring s3 origin", "err", err)
		return nil, err
	}
	tier, err := objcache.New(log, origin, st, runtime, objcache.TierOptions{
		Namespace:  cfg.cacheNamespace,
		TTL:        cfg.revalidateAfter,
		Retention:  cfg.cacheRetention,
		MaxFills:   cfg.fillConcurrency,
		RangeChunk: cfg.rangeCacheChunk,
		MaxBytes:   cfg.cacheMaxBytes,
		ReadOnly:   cfg.readOnly,
	})
	if err != nil {
		_ = origin.Close()
		log.Error("opening origin cache", "err", err)
		return nil, err
	}
	log.Info("origin cache enabled", "namespace", tier.Name(), "endpoint", cfg.originEndpoint, "region", region)
	return tier, nil
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

func newGateway(ctx context.Context, st *store.Store, cfg serveConfig, tier *objcache.Tier, log *slog.Logger) *gateway.Gateway {
	g := gateway.New(st, log, cfg.readOnly, tier, gateway.Limits{
		S3: cfg.s3Concurrency, S3QueueWait: cfg.s3QueueWait,
	})
	go func() {
		<-ctx.Done()
		g.SetDraining(true)
	}()
	return g
}

func serveHandler(g *gateway.Gateway, cfg serveConfig) http.Handler {
	return telemetry.Middleware(g)
}

func runGateway(ctx context.Context, g *gateway.Gateway, cfg serveConfig, log *slog.Logger) int {
	srv := gateway.NewHTTPServer(cfg.addr, serveHandler(g, cfg), log)
	srv.ReadTimeout = cfg.httpReadTimeout
	srv.WriteTimeout = cfg.httpWriteTimeout
	log.Info("pgvs3 starting", "version", version, "addr", cfg.addr)
	if err := gateway.Run(ctx, srv, g, 5*time.Second, cfg.maxConns, log); err != nil {
		log.Error("serving", "err", err)
		return 1
	}
	return 0
}

func fail(err error) int {
	fmt.Fprintln(os.Stderr, err)
	return 1
}

func withStore(name string, args []string, timeout time.Duration, checkLayout bool, setup func(*flag.FlagSet), fn func(context.Context, *store.Store, []string) int) int {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	if setup != nil {
		setup(fs)
	}
	if describeFlags != nil {
		describeFlags(name, fs)
		return 0
	}
	if err := applyFlagEnv(fs, "PGVS3_"); err != nil {
		return fail(err)
	}
	if err := fs.Parse(args); err != nil {
		return flagExitCode(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	st, err := store.Open(ctx, *url, 4, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		return fail(err)
	}
	defer st.Close()
	if checkLayout {
		if err := st.CheckLayout(ctx); err != nil {
			return fail(err)
		}
	}
	return fn(ctx, st, fs.Args())
}

func stat(args []string) int {
	var kind, name *string
	var asJSON *bool
	return withStore("stat", args, 30*time.Second, false, func(fs *flag.FlagSet) {
		kind = fs.String("kind", "", "filter by namespace kind (bucket, cache, writeback or queue)")
		name = fs.String("name", "", "namespace name")
		asJSON = fs.Bool("json", false, "print JSON")
	}, func(ctx context.Context, st *store.Store, _ []string) int {
		s, err := st.StatNamespace(ctx, ns.Kind(*kind), *name)
		if err != nil {
			return fail(err)
		}
		overhead := 0.0
		if s.LogicalBytes > 0 {
			overhead = (float64(s.PhysicalBytes)/float64(s.LogicalBytes) - 1) * 100
		}
		oldest := "none"
		if s.OldestGarbage != nil {
			oldest = s.OldestGarbage.Round(time.Second).String()
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(s)
			return 0
		}
		fmt.Printf("namespaces=%d buckets=%d objects=%d chunks=%d events=%d logical=%d MiB physical=%d MiB db=%d MiB overhead=%.2f%% garbage_files=%d oldest_garbage=%s",
			s.Namespaces, s.Buckets, s.Objects, s.Chunks, s.Events, s.LogicalBytes>>20,
			s.PhysicalBytes>>20, s.DatabaseBytes>>20, overhead, s.GarbageFiles, oldest)
		if s.OriginMetadata > 0 {
			fmt.Printf(" origin_metadata=%d", s.OriginMetadata)
		}
		if s.MaxBytes > 0 {
			fmt.Printf(" max_bytes=%d", s.MaxBytes)
		}
		fmt.Println()
		return 0
	})
}

func health(args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	u := fs.String("url", env("PGVS3_HEALTH_URL", "http://127.0.0.1:8014/readyz"), "readiness URL")
	if describeFlags != nil {
		describeFlags("health", fs)
		return 0
	}
	if err := fs.Parse(args); err != nil {
		return flagExitCode(err)
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(*u)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func reap(args []string) int {
	var grace *time.Duration
	var all *bool
	var retention *time.Duration
	return withStore("reap", args, 30*time.Minute, true, func(fs *flag.FlagSet) {
		grace = fs.Duration("grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "only reap files deleted at least this long ago")
		all = fs.Bool("all", false, "repeat the bounded pass until no garbage remains")
		retention = fs.Duration("job-retention", envDuration("PGVS3_JOB_RETENTION", store.DefaultJobRetention), "keep terminal OJS jobs this long (0 = forever)")
	}, func(ctx context.Context, st *store.Store, _ []string) int {
		st.SetJobRetention(*retention)
		for i := 0; ; i++ {
			beforeRows, beforeFiles := st.ReapedRows(), st.ReapedFiles()
			if err := st.Maintain(ctx, *grace); err != nil {
				return fail(err)
			}
			if !*all || (st.ReapedRows() == beforeRows && st.ReapedFiles() == beforeFiles) || i >= 1000 {
				break
			}
		}
		after, err := st.Stat(ctx)
		if err != nil {
			return fail(err)
		}
		fmt.Printf("garbage_files=%d reaped_rows=%d reaped_files=%d expired_objects=%d db=%d MiB\n",
			after.GarbageFiles, st.ReapedRows(), st.ReapedFiles(), st.ExpiredObjects(), after.DatabaseBytes>>20)
		return 0
	})
}

func maintain(ctx context.Context, st *store.Store, grace, interval time.Duration, log *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			if err := st.Maintain(runCtx, grace); err != nil && ctx.Err() == nil {
				log.Warn("maintenance failed", "err", err)
			}
			cancel()
		}
	}
}

var logLevels = map[string]slog.Level{
	"debug":   slog.LevelDebug,
	"warn":    slog.LevelWarn,
	"warning": slog.LevelWarn,
	"error":   slog.LevelError,
}

func newLogger(level string) *slog.Logger {
	l, ok := logLevels[strings.ToLower(level)]
	if !ok {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func applyFlagEnv(fs *flag.FlagSet, prefix string) error {
	var first error
	fs.VisitAll(func(f *flag.Flag) {
		if first != nil {
			return
		}
		key := prefix + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
		value, exists := os.LookupEnv(key)
		if !exists {
			return
		}
		if boolean, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && boolean.IsBoolFlag() {
			switch strings.ToLower(value) {
			case "yes":
				value = "true"
			case "no", "":
				value = "false"
			}
		} else if value == "" {
			return
		}

		if getter, ok := f.Value.(flag.Getter); ok {
			switch getter.Get().(type) {
			case int, int64:
				number, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					first = fmt.Errorf("%s: %w", key, err)
					return
				}
				value = strconv.FormatInt(number, 10)
			}
		}
		if err := f.Value.Set(value); err != nil {
			first = fmt.Errorf("%s: %w", key, err)
			return
		}
		f.DefValue = f.Value.String()
	})
	return first
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return def
}
