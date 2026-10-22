package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
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
	if i := indexArg(args, "stat"); i >= 0 {
		return stat(removeArg(args, i))
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
		accountID     = fs.String("account-id", env("PGVS3_ACCOUNT_ID", "000000000000"), "account id in bucket ARNs")
		sessionKey    = fs.String("session-key", env("PGVS3_SESSION_KEY", ""), "HMAC key for S3 Express session tokens")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := newLogger(*logLevel)
	if *accessKey == "" || *secretKey == "" {
		log.Error("PGVS3_ACCESS_KEY and PGVS3_SECRET_KEY are required")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, *url, store.Options{
		MinConns: int32(*poolMin), MaxConns: int32(*poolMax),
		CAFile: os.Getenv("PGVS3_DB_CA_FILE"), AllowPlaintext: envBool("PGVS3_DB_ALLOW_PLAINTEXT"),
	}, log)
	if err != nil {
		log.Error("connecting to PostgreSQL", "err", err)
		return 1
	}
	defer st.Close()
	if err := st.Init(ctx); err != nil {
		log.Error("initialising storage layout", "err", err)
		return 1
	}
	if err := st.EnsureMaintenance(ctx); err != nil {
		log.Error("checking storage layout", "err", err)
		return 1
	}
	cache, err := peercache.New(ctx, st, peercache.Options{
		MainBytes: *cacheMain, HotBytes: *cacheHot, TTL: 10 * time.Second,
		Peers: splitPeers(*peers), Self: *peerAdvert, ListenAddr: *peerAddr, Secret: *peerSecret,
	}, log)
	if err != nil {
		log.Error("starting metadata cache", "err", err)
		return 1
	}
	st.SetCache(cache)
	defer st.CloseCache()

	go maintain(ctx, st, *reapGrace, *maintainEvery, log)

	auth := &s3.Auth{AccessKey: *accessKey, SecretKey: *secretKey}
	if *sessionKey != "" {
		auth.Sessions = s3.NewSessionIssuer([]byte(*sessionKey), 5*time.Minute)
	}
	handler := s3.New(st, auth, log, *region, *vhost, *accountID)
	srv, err := s3.Listen(s3.ServeConfig{
		Addr: *addr, TLSCert: *tlsCert, TLSKey: *tlsKey, AllowHTTP: *allowHTTP,
	}, handler, log)
	if err != nil {
		log.Error("configuring listener", "err", err)
		return 1
	}
	log.Info("pgvs3 starting", "version", version, "addr", *addr, "cache_peers", len(splitPeers(*peers)))
	if err := s3.Run(ctx, srv, *tlsCert, *tlsKey, log); err != nil {
		log.Error("serving", "err", err)
		return 1
	}
	return 0
}

func stat(args []string) int {
	fs := flag.NewFlagSet("stat", flag.ContinueOnError)
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
	fmt.Printf("objects=%d chunks=%d logical=%d MiB physical=%d MiB db=%d MiB overhead=%.2f%% garbage_files=%d oldest_garbage=%s\n",
		s.Objects, s.Chunks, s.LogicalBytes>>20, s.PhysicalBytes>>20, s.DatabaseBytes>>20, overhead, s.GarbageFiles, oldest)
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

func splitPeers(s string) []string {
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
