package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
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
	"github.com/adonm/pgvs3/internal/work"
)

var version = "dev"

func main() { os.Exit(run(os.Args[1:])) }

type command struct {
	name string
	run  func([]string) int
}

var commandTable = []command{
	{name: "serve", run: serve},
	{name: "health", run: health},
	{name: "reap", run: reap},
	{name: "doctor", run: doctor},
	{name: "stat", run: stat},
	{name: "clone", run: cloneNS},
	{name: "drop", run: dropNS},
	{name: "adopt", run: adoptNS},
	{name: "origin", run: originCmd},
	{name: "pending", run: pendingCmd},
	{name: "version", run: func([]string) int { fmt.Println(version); return 0 }},
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
	"read-only":        true,
	"cache-write-back": true,
	"force":            true,
	"all":              true,
	"json":             true,
	"h":                true,
	"help":             true,
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
	cacheWriteBack          bool
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
	pprofAddr               string
	originConfig            string
	cacheTTL                time.Duration
	cacheRetention          time.Duration
	cacheNamespace          string
	fillConcurrency         int
	rangeCacheChunk         int64
	s3Concurrency           int
	s3QueueWait             time.Duration
	queueConcurrency        int
	respConcurrency         int
	jobRetention            time.Duration
	simulateLatency         time.Duration
	simulateRPS             int
	simulateBandwidth       int64
}

func parseServeConfig(args []string) (serveConfig, error) {
	var cfg serveConfig
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.StringVar(&cfg.url, "url", "postgres://localhost/pgvs3", "PostgreSQL URL")
	fs.StringVar(&cfg.addr, "addr", "0.0.0.0:8014", "S3/OJS/HTTP/RESP listen address")
	fs.IntVar(&cfg.poolMax, "pool-max", 64, "PostgreSQL connections per gateway")
	fs.StringVar(&cfg.spoolDir, "spool-dir", "", "scratch directory for large cache fills (empty uses the system temp dir)")
	fs.Int64Var(&cfg.spoolMaxBytes, "spool-max-bytes", 8<<30, "process-wide disk spool payload byte budget")
	fs.BoolVar(&cfg.cacheWriteBack, "cache-write-back", true, "acknowledge large unconditional cache writes before the origin upload completes")
	fs.StringVar(&cfg.dbWorkMem, "db-work-mem", "16MB", "PostgreSQL work_mem per connection")
	fs.DurationVar(&cfg.dbStatementTimeout, "db-statement-timeout", 5*time.Minute, "PostgreSQL statement timeout (0 disables)")
	fs.DurationVar(&cfg.dbLockTimeout, "db-lock-timeout", 5*time.Second, "PostgreSQL lock timeout (0 disables)")
	fs.DurationVar(&cfg.dbIdleTxTimeout, "db-idle-tx-timeout", time.Minute, "PostgreSQL idle-in-transaction timeout (0 disables)")
	fs.DurationVar(&cfg.dbMaxConnLifetime, "db-max-conn-lifetime", time.Hour, "PostgreSQL connection lifetime (0 disables)")
	fs.DurationVar(&cfg.dbMaxConnLifetimeJitter, "db-max-conn-lifetime-jitter", 5*time.Minute, "Randomized PostgreSQL lifetime jitter")
	fs.IntVar(&cfg.maxConns, "max-conns", 4096, "concurrent client connections")
	fs.DurationVar(&cfg.httpReadTimeout, "http-read-timeout", 30*time.Minute, "HTTP read deadline including request bodies (0 disables)")
	fs.DurationVar(&cfg.httpWriteTimeout, "http-write-timeout", 30*time.Minute, "HTTP response write deadline (0 disables)")
	fs.Int64Var(&cfg.maxDBBytes, "max-db-bytes", 0, "reject writes when pg_database_size exceeds this (0 = unlimited)")
	fs.DurationVar(&cfg.reapGrace, "reap-grace", 10*time.Minute, "how long deleted files stay readable")
	fs.BoolVar(&cfg.readOnly, "read-only", false, "reject mutating requests")
	fs.StringVar(&cfg.logLevel, "log-level", "info", "debug, info, warn or error")
	fs.StringVar(&cfg.pprofAddr, "pprof-addr", "", "optional loopback address for net/http/pprof, e.g. 127.0.0.1:6060")
	fs.StringVar(&cfg.originConfig, "origin-config", "", "origin JSON config: serve S3 as a read-through cache")
	fs.DurationVar(&cfg.cacheTTL, "cache-ttl", time.Minute, "origin revalidation interval")
	fs.DurationVar(&cfg.cacheRetention, "cache-retention", 24*time.Hour, "evict cached objects after this idle period")
	fs.StringVar(&cfg.cacheNamespace, "cache-namespace", "origin", "cache namespace for origin objects")
	fs.IntVar(&cfg.fillConcurrency, "fill-concurrency", 8, "concurrent origin cache fills")
	fs.Int64Var(&cfg.rangeCacheChunk, "range-cache-chunk", 0, "cache aligned ranges of large objects at this byte size (0 disables)")
	fs.IntVar(&cfg.s3Concurrency, "s3-concurrency", 32, "concurrent S3 requests")
	fs.DurationVar(&cfg.s3QueueWait, "s3-queue-wait", time.Second, "maximum S3 admission wait (queue holds twice s3-concurrency; 0 rejects immediately)")
	fs.IntVar(&cfg.queueConcurrency, "queue-concurrency", 16, "concurrent OJS queue requests")
	fs.IntVar(&cfg.respConcurrency, "resp-concurrency", 32, "concurrent RESP batches")
	fs.DurationVar(&cfg.jobRetention, "job-retention", store.DefaultJobRetention, "keep completed/cancelled/discarded OJS jobs this long (0 = forever)")
	fs.DurationVar(&cfg.simulateLatency, "simulate-latency", 0, "benchmark-only one-way HTTP latency added to S3 requests")
	fs.IntVar(&cfg.simulateRPS, "simulate-rps", 0, "benchmark-only maximum S3 requests per second (0 disables)")
	fs.Int64Var(&cfg.simulateBandwidth, "simulate-bandwidth", 0, "benchmark-only per-request bandwidth in bytes/sec (0 disables)")
	if err := applyFlagEnv(fs); err != nil {
		return serveConfig{}, err
	}
	if err := fs.Parse(args); err != nil {
		return serveConfig{}, err
	}
	if cfg.spoolMaxBytes <= 0 || cfg.poolMax <= 0 || cfg.maxConns <= 0 || cfg.fillConcurrency <= 0 ||
		cfg.s3Concurrency <= 0 || cfg.queueConcurrency <= 0 || cfg.respConcurrency <= 0 {
		return serveConfig{}, fmt.Errorf("pool/connection/concurrency limits must be positive")
	}
	if cfg.dbStatementTimeout < 0 || cfg.dbLockTimeout < 0 || cfg.dbIdleTxTimeout < 0 ||
		cfg.dbMaxConnLifetime < 0 || cfg.dbMaxConnLifetimeJitter < 0 ||
		cfg.httpReadTimeout < 0 || cfg.httpWriteTimeout < 0 || cfg.s3QueueWait < 0 || cfg.simulateLatency < 0 || cfg.simulateRPS < 0 || cfg.simulateBandwidth < 0 {
		return serveConfig{}, fmt.Errorf("timeouts must be non-negative")
	}
	return cfg, nil
}

func serve(args []string) int {
	cfg, err := parseServeConfig(args)
	if err != nil {
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
	st.SetJobRetention(cfg.jobRetention)

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
	origin, err := openOrigin(st, runtime, cfg, log)
	if err != nil {
		return 1
	}
	origins := objcache.NewRegistry(st, log, runtime, objcache.TierOptions{ReadOnly: cfg.readOnly, WriteBack: cfg.cacheWriteBack})
	origins.SetDefault(origin)
	origins.Start(ctx)
	g := newGateway(ctx, st, cfg, origins, log)
	defer g.Close()
	startPprof(cfg.pprofAddr, log)
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
		Fills: cfg.fillConcurrency, Pending: min(cfg.fillConcurrency, 8),
		SpoolDir: cfg.spoolDir, SpoolBytes: cfg.spoolMaxBytes,
	}), nil
}

func openOrigin(st *store.Store, runtime *objcache.Runtime, cfg serveConfig, log *slog.Logger) (*objcache.Tier, error) {
	if cfg.originConfig == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(cfg.originConfig)
	if err != nil {
		log.Error("reading origin config", "err", err)
		return nil, err
	}
	origin, err := objcache.New(log, raw, st, runtime, objcache.TierOptions{
		Namespace: cfg.cacheNamespace, TTL: cfg.cacheTTL, Retention: cfg.cacheRetention,
		MaxFills: cfg.fillConcurrency, RangeChunk: cfg.rangeCacheChunk,
		ReadOnly: cfg.readOnly, WriteBack: cfg.cacheWriteBack,
	})
	if err != nil {
		log.Error("opening origin", "err", err)
		return nil, err
	}
	log.Info("origin cache enabled", "namespace", origin.Name(), "capabilities", origin.Capabilities())
	return origin, nil
}

func newGateway(ctx context.Context, st *store.Store, cfg serveConfig, origins *objcache.Registry, log *slog.Logger) *gateway.Gateway {
	g := gateway.New(st, log, cfg.readOnly, origins, gateway.Limits{
		S3: cfg.s3Concurrency, S3QueueWait: cfg.s3QueueWait, OJS: cfg.queueConcurrency, RESP: cfg.respConcurrency,
	})
	g.SetVersion(version)
	go func() {
		<-ctx.Done()
		g.SetDraining(true)
	}()
	return g
}

func startPprof(addr string, log *slog.Logger) {
	if addr == "" {
		return
	}
	go func() {
		log.Info("pprof listening", "addr", addr)
		if err := http.ListenAndServe(addr, http.DefaultServeMux); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("pprof server", "err", err)
		}
	}()
}

func serveHandler(g *gateway.Gateway, cfg serveConfig) http.Handler {
	handler := telemetry.Middleware(g)
	sim := gateway.Simulation{Latency: cfg.simulateLatency, RPS: cfg.simulateRPS, Bandwidth: cfg.simulateBandwidth}
	if sim.Enabled() {
		handler = gateway.Constrain(handler, sim)
	}
	return handler
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
	if err := fs.Parse(args); err != nil {
		return 2
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
		kind = fs.String("kind", "", "bucket, cache or queue (default: all)")
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
		fmt.Printf("namespaces=%d buckets=%d objects=%d chunks=%d events=%d logical=%d MiB physical=%d MiB db=%d MiB overhead=%.2f%% garbage_files=%d oldest_garbage=%s\n",
			s.Namespaces, s.Buckets, s.Objects, s.Chunks, s.Events, s.LogicalBytes>>20,
			s.PhysicalBytes>>20, s.DatabaseBytes>>20, overhead, s.GarbageFiles, oldest)
		return 0
	})
}

func cloneNS(args []string) int {
	var kind *string
	return withStore("clone", args, 30*time.Minute, true, func(fs *flag.FlagSet) {
		kind = fs.String("kind", "bucket", "bucket or cache")
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		if len(rest) != 2 {
			fmt.Fprintln(os.Stderr, "usage: pgvs3 clone [--kind bucket|cache] <src> <dst>")
			return 2
		}
		if err := st.CloneNamespace(ctx, ns.Kind(*kind), rest[0], rest[1]); err != nil {
			return fail(err)
		}
		fmt.Printf("cloned=%s->%s kind=%s\n", rest[0], rest[1], *kind)
		return 0
	})
}

func dropNS(args []string) int {
	var kind *string
	var force *bool
	return withStore("drop", args, 5*time.Minute, true, func(fs *flag.FlagSet) {
		kind = fs.String("kind", "bucket", "bucket, cache or queue")
		force = fs.Bool("force", false, "drop non-empty namespaces and reclaim their schema immediately")
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		if len(rest) != 1 {
			fmt.Fprintln(os.Stderr, "usage: pgvs3 drop [--kind bucket|cache|queue] [--force] <name>")
			return 2
		}
		res, err := st.DropNamespace(ctx, ns.Kind(*kind), rest[0], *force)
		if err != nil {
			return fail(err)
		}
		switch res {
		case store.BucketDeleted:
			if *kind == string(ns.Cache) {
				_, _ = st.DropNamespace(ctx, ns.WriteBack, rest[0], true)
			}
			fmt.Printf("dropped=%s kind=%s\n", rest[0], *kind)
			return 0
		case store.BucketNotEmpty:
			fmt.Fprintf(os.Stderr, "namespace %s is not empty (pass --force)\n", rest[0])
			return 1
		default:
			fmt.Fprintf(os.Stderr, "namespace %s does not exist\n", rest[0])
			return 1
		}
	})
}

func pendingCmd(args []string) int {
	var namespace *string
	return withStore("pending", args, 30*time.Second, true, func(fs *flag.FlagSet) {
		namespace = fs.String("namespace", "", "cache namespace (default: all)")
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		if len(rest) == 0 {
			fmt.Fprintln(os.Stderr, "usage: pgvs3 pending [--namespace N] list|retry")
			return 2
		}
		switch rest[0] {
		case "list":
			rows, err := st.ListPendingUploads(ctx, *namespace)
			if err != nil {
				return fail(err)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			if err := enc.Encode(rows); err != nil {
				return fail(err)
			}
			return 0
		case "retry":
			n, err := st.RetryDeadPendingUploads(ctx, *namespace)
			if err != nil {
				return fail(err)
			}
			fmt.Printf("retried=%d namespace=%q\n", n, *namespace)
			return 0
		default:
			fmt.Fprintln(os.Stderr, "unknown pending action (list or retry)")
			return 2
		}
	})
}

func adoptNS(args []string) int {
	var kind *string
	var all *bool
	return withStore("adopt", args, 30*time.Minute, true, func(fs *flag.FlagSet) {
		kind = fs.String("kind", "", "bucket, cache or queue (default: all)")
		all = fs.Bool("all", false, "adopt every matching schema")
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		if *all || len(rest) == 0 {
			n, err := st.AdoptAll(ctx, ns.Kind(*kind))
			if err != nil {
				return fail(err)
			}
			fmt.Printf("adopted=%d kind=%s\n", n, orAll(*kind))
			return 0
		}
		for _, name := range rest {
			outcome, err := st.Adopt(ctx, ns.Kind(*kind), name)
			if err != nil {
				return fail(err)
			}
			fmt.Printf("%s=%s kind=%s\n", name, outcome, orAll(*kind))
		}
		return 0
	})
}

func orAll(kind string) string {
	if kind == "" {
		return "all"
	}
	return kind
}

type originConfig struct {
	bucket          string
	cacheNamespace  string
	ttl             time.Duration
	retention       time.Duration
	fillConcurrency int
	rangeCacheChunk int64
}

func registerOriginFlags(fs *flag.FlagSet, cfg *originConfig) {
	fs.StringVar(&cfg.bucket, "bucket", "", "restrict origin passthrough to one bucket (default: global)")
	fs.StringVar(&cfg.cacheNamespace, "cache-namespace", "origin", "cache namespace in PostgreSQL")
	fs.DurationVar(&cfg.ttl, "cache-ttl", time.Minute, "origin revalidation interval")
	fs.DurationVar(&cfg.retention, "cache-retention", 24*time.Hour, "idle eviction interval")
	fs.IntVar(&cfg.fillConcurrency, "fill-concurrency", 8, "concurrent origin cache fills")
	fs.Int64Var(&cfg.rangeCacheChunk, "range-cache-chunk", 0, "cache aligned ranges of large objects at this byte size (0 disables)")
}

func (c originConfig) scope() string {
	if c.bucket == "" {
		return "global"
	}
	return c.bucket
}

func (c originConfig) spec(raw []byte) objcache.Spec {
	return objcache.Spec{
		Namespace:  c.cacheNamespace,
		TTL:        c.ttl.String(),
		Retention:  c.retention.String(),
		MaxFills:   c.fillConcurrency,
		RangeChunk: c.rangeCacheChunk,
		Config:     string(raw),
	}
}

type originAction func(context.Context, *store.Store, []string, originConfig) int

var originActions = map[string]originAction{
	"set":   originSet,
	"clear": originClear,
	"list":  originList,
}

func originCmd(args []string) int {
	var cfg originConfig
	return withStore("origin", args, 30*time.Second, true, func(fs *flag.FlagSet) {
		registerOriginFlags(fs, &cfg)
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		action := "list"
		if len(rest) > 0 {
			action = rest[0]
			rest = rest[1:]
		}
		run, ok := originActions[action]
		if !ok {
			fmt.Fprintf(os.Stderr, "unknown origin action %q (set, clear or list)\n", action)
			return 2
		}
		return run(ctx, st, rest, cfg)
	})
}

func originSet(ctx context.Context, st *store.Store, rest []string, cfg originConfig) int {
	if len(rest) != 1 {
		fmt.Fprintln(os.Stderr, "usage: pgvs3 origin set [--bucket B] [--cache-namespace NS] [--range-cache-chunk N] <origin.json>")
		return 2
	}
	raw, err := os.ReadFile(rest[0])
	if err != nil {
		return fail(err)
	}
	if err := objcache.SaveSpec(ctx, st, cfg.bucket, cfg.spec(raw)); err != nil {
		return fail(err)
	}
	fmt.Printf("origin_set=%s cache=%s\n", cfg.scope(), cfg.cacheNamespace)
	return 0
}

func originClear(ctx context.Context, st *store.Store, _ []string, cfg originConfig) int {
	if err := objcache.ClearSpec(ctx, st, cfg.bucket, cfg.cacheNamespace); err != nil {
		return fail(err)
	}
	fmt.Printf("origin_cleared=%s\n", cfg.scope())
	return 0
}

func originList(ctx context.Context, st *store.Store, _ []string, _ originConfig) int {
	specs, err := objcache.ListSpecs(ctx, st)
	if err != nil {
		return fail(err)
	}
	for _, sp := range specs {
		scope := sp.Bucket
		if scope == "" {
			scope = "global"
		}
		fmt.Printf("origin=%s cache=%s ttl=%s retention=%s range_chunk=%d\n", scope, sp.Namespace, sp.TTL, sp.Retention, sp.RangeChunk)
	}
	return 0
}

func health(args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	u := fs.String("url", env("PGVS3_HEALTH_URL", "http://127.0.0.1:8014/readyz"), "readiness URL")
	if err := fs.Parse(args); err != nil {
		return 2
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

func doctor(args []string) int {
	return withStore("doctor", args, 30*time.Second, false, nil, func(ctx context.Context, st *store.Store, _ []string) int {
		d, err := st.Diagnose(ctx)
		if err != nil {
			return fail(err)
		}
		fmt.Printf("server_version_num=%d aurora_version=%q layout=%d namespaces=%d over_supported_scale=%v buckets=%d queues=%d caches=%d configured=%v objects=%d garbage=%d garbage_referenced=%d orphan_schemas=%d missing_schemas=%d db=%d MiB\n",
			d.ServerVersion, d.AuroraVersion, d.Layout, d.Namespaces, d.OverScale, d.Buckets, d.Queues, d.Caches,
			d.ChunksConfigured, d.Objects, d.GarbageFiles, d.GarbageReferenced,
			d.OrphanSchemas, d.MissingSchemas, d.DatabaseBytes>>20)
		for _, issue := range d.FunctionIssues {
			fmt.Fprintf(os.Stderr, "function contract: %s\n", issue)
		}
		if d.ServerVersion < 180000 || d.Layout != store.LayoutVersion || !d.ChunksConfigured ||
			d.OrphanSchemas > 0 || d.MissingSchemas > 0 || d.GarbageReferenced > 0 || d.OverScale ||
			len(d.FunctionIssues) > 0 {
			return 1
		}
		if err := st.EnsureMaintenance(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "schema drift: %v\n", err)
			return 1
		}
		return 0
	})
}

func maintain(ctx context.Context, st *store.Store, grace, interval time.Duration, log *slog.Logger) {
	work.Periodic(ctx, work.PeriodicOptions{
		Name:     "maintenance",
		Interval: interval,
		Timeout:  2 * time.Minute,
		Drain:    true,
		Log:      log,
	}, func(ctx context.Context) (bool, error) {
		beforeRows, beforeFiles := st.ReapedRows(), st.ReapedFiles()
		if err := st.Maintain(ctx, grace); err != nil {
			return false, err
		}
		return st.ReapedRows()-beforeRows >= store.ReapRows ||
			st.ReapedFiles()-beforeFiles >= store.ReapRows, nil
	})
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

func applyFlagEnv(fs *flag.FlagSet) error {
	var first error
	fs.VisitAll(func(f *flag.Flag) {
		if first != nil {
			return
		}
		key := "PGVS3_" + strings.ToUpper(strings.ReplaceAll(f.Name, "-", "_"))
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
