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

func main() { os.Exit(run(os.Args[1:])) }

var subcommands = map[string]func([]string) int{
	"serve":   serve,
	"health":  health,
	"reap":    reap,
	"doctor":  doctor,
	"stat":    stat,
	"clone":   cloneNS,
	"drop":    dropNS,
	"adopt":   adoptNS,
	"origin":  originCmd,
	"migrate": migrateNS,
	"version": func([]string) int { fmt.Println(version); return 0 },
}

func run(args []string) int {
	cmd, rest := extractSubcommand(args)
	if fn, ok := subcommands[cmd]; ok {
		return fn(rest)
	}
	return serve(args)
}

// extractSubcommand locates the subcommand at the first non-flag position so
// positional arguments after a subcommand (for example `pgvs3 drop stat`) are
// never mistaken for another subcommand, while leading global flags like
// `pgvs3 --url ... serve` continue to work.
func extractSubcommand(args []string) (string, []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			return "", args
		}
		if strings.HasPrefix(a, "-") {
			if !strings.Contains(a, "=") && flagTakesValue(strings.TrimLeft(a, "-")) && i+1 < len(args) {
				i++
			}
			continue
		}
		if _, ok := subcommands[a]; ok {
			out := make([]string, 0, len(args)-1)
			out = append(out, args[:i]...)
			out = append(out, args[i+1:]...)
			return a, out
		}
		return "", args
	}
	return "", args
}

func flagTakesValue(name string) bool {
	switch name {
	case "read-only", "force", "all", "json", "h", "help":
		return false
	default:
		return true
	}
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
			log.Error("initializing schema", "err", err)
			return 1
		}
		if err := st.EnsureMaintenance(ctx); err != nil {
			log.Error("verifying maintenance schema", "err", err)
			return 1
		}
		go maintain(ctx, st, *reapGrace, time.Minute, log)
	}

	var origin *objcache.Tier
	if *originCfg != "" {
		raw, err := os.ReadFile(*originCfg)
		if err != nil {
			log.Error("reading origin config", "err", err)
			return 1
		}
		origin, err = objcache.New(log, raw, st, *cacheNS, *cacheTTL, *cacheKeep, *fillMax)
		if err != nil {
			log.Error("opening origin", "err", err)
			return 1
		}
		defer origin.Close()
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
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer st.Close()
	if checkLayout {
		if err := st.CheckLayout(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
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
			fmt.Fprintln(os.Stderr, err)
			return 1
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
	})
}

func migrateNS(args []string) int {
	return withStore("migrate", args, 6*time.Hour, false, nil, func(ctx context.Context, st *store.Store, _ []string) int {
		n, err := st.Migrate(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		fmt.Printf("migrated_buckets=%d layout=%d\n", n, store.LayoutVersion)
		return 0
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
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			fmt.Printf("adopted=%d kind=%s\n", n, orAll(*kind))
			return 0
		}
		for _, name := range rest {
			outcome, err := st.Adopt(ctx, ns.Kind(*kind), name)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
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

func originCmd(args []string) int {
	var (
		bucket    *string
		cacheNS   *string
		ttl       *time.Duration
		retention *time.Duration
		fills     *int
	)
	return withStore("origin", args, 30*time.Second, true, func(fs *flag.FlagSet) {
		bucket = fs.String("bucket", "", "restrict origin passthrough to one bucket (default: global)")
		cacheNS = fs.String("cache-namespace", "origin", "cache namespace in PostgreSQL")
		ttl = fs.Duration("cache-ttl", time.Minute, "origin revalidation interval")
		retention = fs.Duration("cache-retention", 24*time.Hour, "idle eviction interval")
		fills = fs.Int("fill-concurrency", 8, "concurrent origin cache fills")
	}, func(ctx context.Context, st *store.Store, rest []string) int {
		action := "list"
		if len(rest) > 0 {
			action = rest[0]
			rest = rest[1:]
		}
		switch action {
		case "set":
			if len(rest) != 1 {
				fmt.Fprintln(os.Stderr, "usage: pgvs3 origin set [--bucket B] [--cache-namespace NS] <origin.yaml>")
				return 2
			}
			raw, err := os.ReadFile(rest[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			sp := objcache.Spec{
				Namespace:  *cacheNS,
				TTL:        ttl.String(),
				Retention:  retention.String(),
				MaxFills:   *fills,
				ConfigYAML: string(raw),
			}
			if err := objcache.SaveSpec(ctx, st, *bucket, sp); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			scope := *bucket
			if scope == "" {
				scope = "global"
			}
			fmt.Printf("origin_set=%s cache=%s\n", scope, *cacheNS)
			return 0
		case "clear":
			if err := objcache.ClearSpec(ctx, st, *bucket, *cacheNS); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			scope := *bucket
			if scope == "" {
				scope = "global"
			}
			fmt.Printf("origin_cleared=%s\n", scope)
			return 0
		case "list":
			specs, err := objcache.ListSpecs(ctx, st)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			for _, sp := range specs {
				scope := sp.Bucket
				if scope == "" {
					scope = "global"
				}
				fmt.Printf("origin=%s cache=%s ttl=%s retention=%s\n", scope, sp.Namespace, sp.TTL, sp.Retention)
			}
			return 0
		default:
			fmt.Fprintf(os.Stderr, "unknown origin action %q (set, clear or list)\n", action)
			return 2
		}
	})
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
	return withStore("reap", args, 30*time.Minute, true, func(fs *flag.FlagSet) {
		grace = fs.Duration("grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "only reap files deleted at least this long ago")
		all = fs.Bool("all", false, "repeat the bounded pass until no garbage remains")
	}, func(ctx context.Context, st *store.Store, _ []string) int {
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
	})
}

func doctor(args []string) int {
	return withStore("doctor", args, 30*time.Second, false, nil, func(ctx context.Context, st *store.Store, _ []string) int {
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
	})
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
