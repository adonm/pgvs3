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

	"github.com/adonm/pgvs3/internal/peercache"
	"github.com/adonm/pgvs3/internal/s3"
	"github.com/adonm/pgvs3/internal/store"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "version" {
		fmt.Println(version)
		return 0
	}
	// Accept both `pgvs3 serve --url ...` and the historical
	// `pgvs3 --url ... serve --addr ...`: strip the verb wherever it appears,
	// then let flag.Parse see only flags.
	if i := indexArg(args, "health"); i >= 0 {
		return health(removeArg(args, i))
	}
	if i := indexArg(args, "reap"); i >= 0 {
		return reap(removeArg(args, i))
	}
	if i := indexArg(args, "doctor"); i >= 0 {
		return doctor(removeArg(args, i))
	}
	if i := indexArg(args, "stat"); i >= 0 {
		return stat(removeArg(args, i))
	}
	if i := indexArg(args, "serve"); i >= 0 {
		args = removeArg(args, i)
	}
	return serve(args)
}

func flagSet(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
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
		url           = fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
		addr          = fs.String("addr", env("PGVS3_ADDR", "127.0.0.1:8014"), "S3 listen address")
		accessKey     = fs.String("access-key", env("PGVS3_ACCESS_KEY", ""), "SigV4 access key")
		secretKey     = fs.String("secret-key", env("PGVS3_SECRET_KEY", ""), "SigV4 secret key")
		tlsCert       = fs.String("tls-cert", env("PGVS3_TLS_CERT", ""), "PEM certificate")
		tlsKey        = fs.String("tls-key", env("PGVS3_TLS_KEY", ""), "PEM private key")
		allowHTTP     = fs.Bool("allow-http", envBool("PGVS3_ALLOW_HTTP"), "allow plaintext HTTP outside loopback")
		region        = fs.String("region", env("PGVS3_REGION", "us-east-1"), "S3 region")
		poolMin       = fs.Int("pool-min", envInt("PGVS3_POOL_MIN", 64), "PostgreSQL connections kept warm")
		poolMax       = fs.Int("pool-max", envInt("PGVS3_POOL_MAX", 64), "PostgreSQL connections per gateway")
		reapGrace     = fs.Duration("reap-grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "how long deleted files stay readable")
		maintainEvery = fs.Duration("maintenance-interval", envDuration("PGVS3_MAINTENANCE_INTERVAL", time.Minute), "maintenance interval")
		cacheMain     = fs.Int64("cache-main-bytes", envInt64("PGVS3_CACHE_MAIN_BYTES", 64<<20), "owned metadata cache bytes")
		cacheHot      = fs.Int64("cache-hot-bytes", envInt64("PGVS3_CACHE_HOT_BYTES", 64<<20), "mirrored peer metadata cache bytes")
		peers         = fs.String("peers", env("PGVS3_PEERS", ""), "comma-separated peer addresses for the cross-AZ cache")
		peerAddr      = fs.String("peer-addr", env("PGVS3_PEER_ADDR", "0.0.0.0:8015"), "cross-AZ cache listen address")
		peerAdvert    = fs.String("peer-advertise", env("PGVS3_PEER_ADVERTISE", ""), "this gateway's address as its peers see it")
		peerSecret    = fs.String("peer-secret", env("PGVS3_PEER_SECRET", ""), "shared secret for peer cache requests")
		logLevel      = fs.String("log-level", env("PGVS3_LOG_LEVEL", "info"), "debug, info, warn or error")
		vhost         = fs.String("vhost-domain", env("PGVS3_VHOST_DOMAIN", ""), "virtual-host suffix, e.g. s3.internal")
		objectCache   = fs.Int64("object-cache-bytes", envInt64("PGVS3_OBJECT_CACHE_BYTES", 64<<20), "small-object byte cache size (0 disables)")
		objectTTL     = fs.Duration("object-cache-ttl", envDuration("PGVS3_OBJECT_CACHE_TTL", 5*time.Second), "small-object cache TTL")
		objectCheck   = fs.Bool("object-cache-revalidate", envBoolDefault("PGVS3_OBJECT_CACHE_REVALIDATE", false), "revalidate each small-object cache hit against PostgreSQL")
		wholeRead     = fs.Int64("whole-read-max", envInt64("PGVS3_WHOLE_READ_MAX", 64<<20), "largest single-file span read in one query")
		writeCommit   = fs.String("write-commit", env("PGVS3_WRITE_COMMIT", "on"), "synchronous_commit for unconditional writes: on, local or off")
		verifyReads   = fs.Bool("verify-reads", envBoolDefault("PGVS3_VERIFY_READS", true), "verify the current object file_id on every chunk range query")
		inlineMax     = fs.Int64("inline-max", envInt64("PGVS3_INLINE_MAX", 65536), "largest object stored inline (0 disables, max 65536)")
		buckets       = fs.String("buckets", env("PGVS3_BUCKETS", ""), "comma-separated buckets to create at startup")
		readOnly      = fs.Bool("read-only", envBool("PGVS3_READ_ONLY"), "reject mutating S3 requests")
		noAuth        = fs.Bool("no-auth", envBool("PGVS3_NO_AUTH"), "accept unsigned S3 requests (trust the network)")
		webAddr       = fs.String("web-addr", env("PGVS3_WEB_ADDR", ""), "plain HTTP web/static-site listen address")
		webIndex      = fs.String("web-index", env("PGVS3_WEB_INDEX", "index.html"), "index object for directory requests")
		webError      = fs.String("web-error", env("PGVS3_WEB_ERROR", ""), "optional 404 object key")
		drain         = fs.Duration("drain", envDuration("PGVS3_DRAIN", 5*time.Second), "readiness drain period before shutdown")
		adminAddr     = fs.String("admin-addr", env("PGVS3_ADMIN_ADDR", ""), "separate health/metrics listen address")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	peersList := splitList(*peers)
	// A single gateway invalidates its own cache on write. With peers, another
	// gateway can overwrite a key at any time, so revalidate cache hits.
	if os.Getenv("PGVS3_OBJECT_CACHE_REVALIDATE") == "" && !flagSet(fs, "object-cache-revalidate") && len(peersList) > 0 {
		*objectCheck = true
	}
	log := newLogger(*logLevel)
	if !*noAuth && (*accessKey == "" || *secretKey == "") {
		log.Error("PGVS3_ACCESS_KEY and PGVS3_SECRET_KEY are required unless --no-auth is set")
		return 2
	}
	if *noAuth {
		log.Warn("authentication disabled; any client that can reach this listener can read and write objects")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, *url, store.Options{
		MinConns: int32(*poolMin), MaxConns: int32(*poolMax),
		CAFile: os.Getenv("PGVS3_DB_CA_FILE"), AllowPlaintext: envBool("PGVS3_DB_ALLOW_PLAINTEXT"),
		ObjectCacheBytes: *objectCache, ObjectCacheTTL: *objectTTL, ObjectCacheRevalidate: *objectCheck, WholeReadMax: *wholeRead, RevalidateReads: len(peersList) > 0, WriteCommit: *writeCommit, SkipReadVerify: !*verifyReads, InlineMax: *inlineMax,
	}, log)
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
		if err := st.EnsureBuckets(ctx, splitList(*buckets)); err != nil {
			log.Error("creating startup buckets", "err", err)
			return 1
		}
	}
	cache, err := peercache.New(ctx, st, peercache.Options{
		MainBytes: *cacheMain, HotBytes: *cacheHot, TTL: 10 * time.Second,
		Peers: peersList, Self: *peerAdvert, ListenAddr: *peerAddr, Secret: *peerSecret,
	}, log)
	if err != nil {
		log.Error("starting metadata cache", "err", err)
		return 1
	}
	st.SetCache(cache)
	defer st.CloseCache()
	if *poolMax > 1 {
		go st.StartNotify(ctx)
	}

	if !*readOnly {
		go maintain(ctx, st, *reapGrace, *maintainEvery, log)
	}

	auth := &s3.Auth{AccessKey: *accessKey, SecretKey: *secretKey, Disabled: *noAuth}
	handler := s3.New(st, auth, log, *region, *vhost, *readOnly)
	if *adminAddr != "" {
		handler.SetAdminOnly(true)
	}
	go func() {
		<-ctx.Done()
		handler.SetDraining(true)
	}()
	srv, err := s3.Listen(s3.ServeConfig{
		Addr: *addr, TLSCert: *tlsCert, TLSKey: *tlsKey, AllowHTTP: *allowHTTP,
	}, handler, log)
	if err != nil {
		log.Error("configuring listener", "err", err)
		return 1
	}
	log.Info("pgvs3 starting", "version", version, "addr", *addr, "cache_peers", len(peersList), "object_cache_revalidate", *objectCheck)
	if *webAddr != "" {
		web := &http.Server{Addr: *webAddr, Handler: handler.WebHandler(*webIndex, *webError), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := s3.Run(ctx, web, "", "", 0, log); err != nil {
				log.Error("web listener", "err", err)
			}
		}()
	}
	if *adminAddr != "" {
		admin := &http.Server{Addr: *adminAddr, Handler: handler.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := s3.Run(ctx, admin, "", "", 0, log); err != nil {
				log.Error("admin listener", "err", err)
			}
		}()
	}
	if err := s3.Run(ctx, srv, *tlsCert, *tlsKey, *drain, log); err != nil {
		log.Error("serving", "err", err)
		return 1
	}
	return 0
}

func stat(args []string) int {
	fs := flag.NewFlagSet("stat", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := store.Open(ctx, *url, store.Options{
		MinConns: 1, MaxConns: 2, AllowPlaintext: envBool("PGVS3_DB_ALLOW_PLAINTEXT"),
	}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer st.Close()
	s, err := st.Stat(ctx)
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
	fmt.Printf("objects=%d chunks=%d logical=%d MiB physical=%d MiB db=%d MiB overhead=%.2f%% garbage_files=%d oldest_garbage=%s\n",
		s.Objects, s.Chunks, s.LogicalBytes>>20, s.PhysicalBytes>>20, s.DatabaseBytes>>20, overhead, s.GarbageFiles, oldest)
	return 0
}

func health(args []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_HEALTH_URL", "http://127.0.0.1:8014/readyz"), "readiness URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(*url)
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	st, err := store.Open(ctx, *url, store.Options{
		MinConns: 1, MaxConns: 2, AllowPlaintext: envBool("PGVS3_DB_ALLOW_PLAINTEXT"),
	}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer st.Close()
	if err := st.CheckLayout(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	before, _ := st.Stat(ctx)
	for i := 0; ; i++ {
		if err := st.Maintain(ctx, *grace); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		after, _ := st.Stat(ctx)
		if !*all || after.GarbageFiles == 0 || after.GarbageFiles >= before.GarbageFiles || i >= 100 {
			break
		}
		before = after
	}
	after, err := st.Stat(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("garbage_files=%d reaped_rows=%d reaped_files=%d db=%d MiB\n",
		after.GarbageFiles, st.ReapedRows(), st.ReapedFiles(), after.DatabaseBytes>>20)
	return 0
}

func doctor(args []string) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	url := fs.String("url", env("PGVS3_URL", "postgres://localhost/pgvs3"), "PostgreSQL URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := store.Open(ctx, *url, store.Options{
		MinConns: 1, MaxConns: 2, AllowPlaintext: envBool("PGVS3_DB_ALLOW_PLAINTEXT"),
	}, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer st.Close()
	d, err := st.Diagnose(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Printf("server_version_num=%d layout=%d chunks=%s configured=%v objects=%d garbage=%d db=%d MiB\n",
		d.ServerVersion, d.Layout, d.ChunksRelkind, d.ChunksConfigured, d.Objects, d.GarbageFiles, d.DatabaseBytes>>20)
	if d.ServerVersion < 180000 || d.Layout != store.LayoutVersion || !d.ChunksConfigured {
		return 1
	}
	return 0
}

func maintain(ctx context.Context, st *store.Store, grace, every time.Duration, log *slog.Logger) {
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runCtx, cancel := context.WithTimeout(ctx, 55*time.Second)
			if err := st.Maintain(runCtx, grace); err != nil {
				log.Error("maintenance", "err", err)
			}
			cancel()
		}
	}
}

func newLogger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBoolDefault(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func envBool(key string) bool {
	v, _ := strconv.ParseBool(os.Getenv(key))
	return v
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v, err := strconv.ParseInt(os.Getenv(key), 10, 64); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
