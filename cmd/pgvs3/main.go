package main

import (
	"context"
	"encoding/json"
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
)

var version = "dev"

func main() { os.Exit(run()) }

func run() int {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "version" {
		fmt.Println(version)
		return 0
	}
	for _, cmd := range []struct {
		name string
		fn   func([]string) int
	}{
		{"health", health}, {"reap", reap}, {"doctor", doctor}, {"stat", stat},
		{"clone", cloneNS}, {"drop", dropNS}, {"adopt", adoptNS}, {"migrate", migrateNS}, {"version", nil},
	} {
		if i := indexArg(args, cmd.name); i >= 0 && cmd.fn != nil {
			return cmd.fn(removeArg(args, i))
		}
	}
	if i := indexArg(args, "serve"); i >= 0 {
		args = removeArg(args, i)
	}
	return serve(args)
}

func indexArg(args []string, want string) int {
	for i, a := range args {
		if a == want {
			return i
		}
	}
	return -1
}

func removeArg(args []string, i int) []string {
	out := make([]string, 0, len(args)-1)
	out = append(out, args[:i]...)
	return append(out, args[i+1:]...)
}

func serve(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var (
		url       = fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
		addr      = fs.String("addr", env("PGVS3_ADDR", "0.0.0.0:8014"), "S3/SQS/HTTP/RESP listen address")
		poolMax   = fs.Int("pool-max", envInt("PGVS3_POOL_MAX", 64), "PostgreSQL connections per gateway")
		maxConns  = fs.Int("max-conns", envInt("PGVS3_MAX_CONNS", 4096), "concurrent client connections")
		reapGrace = fs.Duration("reap-grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "how long deleted files stay readable")
		readOnly  = fs.Bool("read-only", envBool("PGVS3_READ_ONLY"), "reject mutating requests")
		logLevel  = fs.String("log-level", env("PGVS3_LOG_LEVEL", "info"), "debug, info, warn or error")
		originCfg = fs.String("origin-config", env("PGVS3_ORIGIN_CONFIG", ""), "objstore YAML file: serve S3 as a read-through cache")
		cacheTTL  = fs.Duration("cache-ttl", envDuration("PGVS3_CACHE_TTL", time.Minute), "origin revalidation interval")
		cacheKeep = fs.Duration("cache-retention", envDuration("PGVS3_CACHE_RETENTION", 24*time.Hour), "evict cached objects after this idle period")
		cacheNS   = fs.String("cache-namespace", env("PGVS3_CACHE_NAMESPACE", "origin"), "cache namespace for origin objects")
		fillMax   = fs.Int("fill-concurrency", envInt("PGVS3_FILL_CONCURRENCY", 8), "concurrent origin cache fills")
		sqsMax    = fs.Int("queue-concurrency", envInt("PGVS3_QUEUE_CONCURRENCY", 16), "concurrent SQS requests")
		respMax   = fs.Int("resp-concurrency", envInt("PGVS3_RESP_CONCURRENCY", 32), "concurrent RESP batches")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := newLogger(*logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, *url, int32(*poolMax), log)
	if err != nil {
		log.Error("connecting to PostgreSQL", "err", err)
		return 1
	}
	defer st.Close()
	if *readOnly {
		if err := st.CheckLayout(ctx); err != nil {
			log.Error("checking storage layout", "err", err)
			return 1
		}
	} else {
		if err := st.Init(ctx); err != nil {
			log.Error("initialising storage layout", "err", err)
			return 1
		}
		if err := st.EnsureMaintenance(ctx); err != nil {
			log.Error("checking storage layout", "err", err)
			return 1
		}
		go maintain(ctx, st, *reapGrace, min(*reapGrace, time.Minute), log)
	}

	var origin *objcache.Tier
	if *originCfg != "" {
		cfg, err := os.ReadFile(*originCfg)
		if err != nil {
			log.Error("reading origin config", "err", err)
			return 1
		}
		origin, err = objcache.New(log, cfg, st, *cacheNS, *cacheTTL, *cacheKeep, *fillMax)
		if err != nil {
			log.Error("building origin cache", "err", err)
			return 1
		}
		defer origin.Close()
		log.Info("origin cache enabled", "namespace", *cacheNS, "ttl", *cacheTTL)
	}

	g := gateway.New(st, log, *readOnly, origin, gateway.Limits{SQS: *sqsMax, RESP: *respMax})
	g.SetVersion(version)
	go func() {
		<-ctx.Done()
		g.SetDraining(true)
	}()
	srv := gateway.NewHTTPServer(*addr, g, log)
	log.Info("pgvs3 starting", "version", version, "addr", *addr)
	if err := gateway.Run(ctx, srv, g, 5*time.Second, *maxConns, log); err != nil {
		log.Error("serving", "err", err)
		return 1
	}
	return 0
}

func stat(args []string) int {
	fs := flag.NewFlagSet("stat", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	kind := fs.String("kind", "", "bucket, cache or queue (default: all)")
	name := fs.String("name", "", "namespace name")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	s, err := st.StatNamespace(ctx, ns.Kind(*kind), *name, "", 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
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
	fmt.Printf("namespaces=%d buckets=%d objects=%d chunks=%d logical=%d MiB physical=%d MiB db=%d MiB overhead=%.2f%% garbage_files=%d oldest_garbage=%s\n",
		s.Namespaces, s.Buckets, s.Objects, s.Chunks, s.LogicalBytes>>20,
		s.PhysicalBytes>>20, s.DatabaseBytes>>20, overhead, s.GarbageFiles, oldest)
	return 0
}

func cloneNS(args []string) int {
	fs := flag.NewFlagSet("clone", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	kind := fs.String("kind", "bucket", "bucket or cache")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintln(os.Stderr, "usage: pgvs3 clone [--kind bucket|cache] <src> <dst>")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	if err := st.CheckLayout(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := st.CloneNamespace(ctx, ns.Kind(*kind), rest[0], rest[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("cloned=%s->%s kind=%s\n", rest[0], rest[1], *kind)
	return 0
}

func dropNS(args []string) int {
	fs := flag.NewFlagSet("drop", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	kind := fs.String("kind", "bucket", "bucket, cache or queue")
	force := fs.Bool("force", false, "drop non-empty namespaces and reclaim their schema immediately")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fmt.Fprintln(os.Stderr, "usage: pgvs3 drop [--kind bucket|cache|queue] [--force] <name>")
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	if err := st.CheckLayout(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	res, err := st.DropNamespace(ctx, ns.Kind(*kind), rest[0], *force)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	switch res {
	case store.BucketDeleted:
		fmt.Printf("dropped=%s kind=%s\n", rest[0], *kind)
		return 0
	case store.BucketNotEmpty:
		fmt.Fprintf(os.Stderr, "namespace %s is not empty (pass --force)\n", rest[0])
		return 1
	default:
		fmt.Fprintf(os.Stderr, "namespace %s does not exist\n", rest[0])
		return 1
	}
}

func migrateNS(args []string) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	n, err := st.Migrate(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("migrated_buckets=%d layout=%d\n", n, store.LayoutVersion)
	return 0
}

func adoptNS(args []string) int {
	fs := flag.NewFlagSet("adopt", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	kind := fs.String("kind", "", "bucket, cache or queue (default: all)")
	all := fs.Bool("all", false, "adopt every matching schema")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	if err := st.CheckLayout(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if *all || len(fs.Args()) == 0 {
		n, err := st.AdoptAll(ctx, ns.Kind(*kind))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("adopted=%d kind=%s\n", n, orAll(*kind))
		return 0
	}
	for _, name := range fs.Args() {
		outcome, err := st.Adopt(ctx, ns.Kind(*kind), name)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("%s=%s kind=%s\n", name, outcome, orAll(*kind))
	}
	return 0
}

func orAll(kind string) string {
	if kind == "" {
		return "all"
	}
	return kind
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
	fs := flag.NewFlagSet("reap", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	grace := fs.Duration("grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "only reap files deleted at least this long ago")
	all := fs.Bool("all", false, "repeat the bounded pass until no garbage remains")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	if err := st.CheckLayout(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	for i := 0; ; i++ {
		beforeRows, beforeFiles := st.ReapedRows(), st.ReapedFiles()
		if err := st.Maintain(ctx, *grace); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		if !*all || (st.ReapedRows() == beforeRows && st.ReapedFiles() == beforeFiles) || i >= 1000 {
			break
		}
	}
	after, err := st.Stat(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("garbage_files=%d reaped_rows=%d reaped_files=%d expired_objects=%d db=%d MiB\n",
		after.GarbageFiles, st.ReapedRows(), st.ReapedFiles(), st.ExpiredObjects(), after.DatabaseBytes>>20)
	return 0
}

func doctor(args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st := mustStore(ctx, *url)
	defer st.Close()
	d, err := st.Diagnose(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("server_version_num=%d layout=%d namespaces=%d buckets=%d queues=%d caches=%d configured=%v objects=%d garbage=%d orphan_schemas=%d missing_schemas=%d db=%d MiB\n",
		d.ServerVersion, d.Layout, d.Namespaces, d.Buckets, d.Queues, d.Caches,
		d.ChunksConfigured, d.Objects, d.GarbageFiles, d.OrphanSchemas, d.MissingSchemas, d.DatabaseBytes>>20)
	if d.ServerVersion < 180000 || d.Layout != store.LayoutVersion || !d.ChunksConfigured {
		return 1
	}
	return 0
}

func mustStore(ctx context.Context, url string) *store.Store {
	st, err := store.Open(ctx, url, 4, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	return st
}

func maintain(ctx context.Context, st *store.Store, grace, interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		interval = time.Minute
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for ctx.Err() == nil {
				beforeRows, beforeFiles := st.ReapedRows(), st.ReapedFiles()
				runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				err := st.Maintain(runCtx, grace)
				cancel()
				if err != nil {
					if ctx.Err() == nil {
						log.Error("maintenance", "err", err)
					}
					break
				}
				if st.ReapedRows()-beforeRows < store.ReapRows && st.ReapedFiles()-beforeFiles < store.ReapRows {
					break
				}
			}
		}
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn", "warning":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes":
		return true
	}
	return false
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
