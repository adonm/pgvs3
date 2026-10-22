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
		addr      = fs.String("addr", env("PGVS3_ADDR", "0.0.0.0:8014"), "S3 and HTTP listen address")
		poolMax   = fs.Int("pool-max", envInt("PGVS3_POOL_MAX", 64), "PostgreSQL connections per gateway")
		reapGrace = fs.Duration("reap-grace", envDuration("PGVS3_REAP_GRACE", 10*time.Minute), "how long deleted files stay readable")
		readOnly  = fs.Bool("read-only", envBool("PGVS3_READ_ONLY"), "reject mutating S3 requests")
		logLevel  = fs.String("log-level", env("PGVS3_LOG_LEVEL", "info"), "debug, info, warn or error")
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

	handler := s3.New(st, log, *readOnly)
	go func() {
		<-ctx.Done()
		handler.SetDraining(true)
	}()
	srv := s3.NewHTTPServer(*addr, handler, log)
	log.Info("pgvs3 starting", "version", version, "addr", *addr)
	if err := s3.Run(ctx, srv, 5*time.Second, log); err != nil {
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
	st, err := store.Open(ctx, *url, 2, slog.New(slog.NewTextHandler(os.Stderr, nil)))
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
	st, err := store.Open(ctx, *url, 2, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
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
	st, err := store.Open(ctx, *url, 2, slog.New(slog.NewTextHandler(os.Stderr, nil)))
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
