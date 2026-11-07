package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

func TestCommandTable(t *testing.T) {
	want := []string{"serve", "health", "reap", "doctor", "stat", "clone", "drop", "adopt", "origin", "pending", "version"}
	if len(commandTable) != len(want) {
		t.Fatalf("command table has %d entries, want %d", len(commandTable), len(want))
	}
	for _, name := range want {
		cmd, ok := commandByName[name]
		if !ok || cmd.name != name || cmd.run == nil {
			t.Fatalf("command %q = %+v ok=%v", name, cmd, ok)
		}
	}
}

func TestSelectCommand(t *testing.T) {
	cases := []struct {
		in      []string
		wantCmd string
		wantOK  bool
	}{
		{in: []string{"version"}, wantCmd: "version", wantOK: true},
		{in: []string{"--addr=127.0.0.1:8014", "serve"}, wantCmd: "serve", wantOK: true},
		{in: []string{"--url", "postgres://localhost/pgvs3", "stat"}, wantCmd: "stat", wantOK: true},
		{in: []string{"--read-only", "serve", "--addr", "127.0.0.1:8014"}, wantCmd: "serve", wantOK: true},
		{in: []string{"serve", "--url", "postgres://localhost/pgvs3"}, wantCmd: "serve", wantOK: true},
		{in: []string{"unknown", "serve"}, wantOK: false},
		{in: []string{"--", "serve"}, wantOK: false},
	}
	for _, tc := range cases {
		cmd, _, ok := selectCommand(tc.in)
		if ok != tc.wantOK || cmd.name != tc.wantCmd {
			t.Fatalf("selectCommand(%v) = (%q, %v), want (%q, %v)", tc.in, cmd.name, ok, tc.wantCmd, tc.wantOK)
		}
	}
}

func clearServeEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"PGVS3_URL", "PGVS3_ADDR", "PGVS3_POOL_MAX", "PGVS3_MAX_CONNS",
		"PGVS3_MAX_DB_BYTES", "PGVS3_REAP_GRACE", "PGVS3_READ_ONLY",
		"PGVS3_LOG_LEVEL", "PGVS3_PPROF_ADDR", "PGVS3_ORIGIN_CONFIG",
		"PGVS3_CACHE_TTL", "PGVS3_CACHE_RETENTION", "PGVS3_CACHE_NAMESPACE",
		"PGVS3_FILL_CONCURRENCY", "PGVS3_RANGE_CACHE_CHUNK",
		"PGVS3_CACHE_MAX_BYTES",
		"PGVS3_S3_ENDPOINT", "PGVS3_S3_BUCKET", "PGVS3_S3_REGION",
		"PGVS3_S3_ACCESS_KEY", "PGVS3_S3_SECRET_KEY",
		"PGVS3_S3_PATH_STYLE", "PGVS3_S3_INSECURE",
		"PGVS3_QUEUE_CONCURRENCY", "PGVS3_RESP_CONCURRENCY",
		"PGVS3_JOB_RETENTION",
		"PGVS3_SPOOL_MAX_BYTES",
		"PGVS3_S3_QUEUE_WAIT",
	} {
		t.Setenv(key, "")
	}
}

func runCaptured(t *testing.T, args ...string) (int, string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stderr-")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	old := os.Stderr
	os.Stderr = f
	defer func() { os.Stderr = old }()
	code := run(args)
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return code, string(out)
}

func TestParseServeConfigDefaults(t *testing.T) {
	clearServeEnv(t)
	cfg, err := parseServeConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	want := serveConfig{
		url:                     "postgres://localhost/pgvs3",
		addr:                    "0.0.0.0:8014",
		poolMax:                 64,
		spoolMaxBytes:           8 << 30,
		dbWorkMem:               "16MB",
		dbStatementTimeout:      5 * time.Minute,
		dbLockTimeout:           5 * time.Second,
		dbIdleTxTimeout:         time.Minute,
		dbMaxConnLifetime:       time.Hour,
		dbMaxConnLifetimeJitter: 5 * time.Minute,
		maxConns:                4096,
		httpReadTimeout:         30 * time.Minute,
		httpWriteTimeout:        30 * time.Minute,
		reapGrace:               10 * time.Minute,
		logLevel:                "info",
		cacheTTL:                time.Minute,
		cacheRetention:          24 * time.Hour,
		cacheNamespace:          "origin",
		cacheWriteBack:          true,
		fillConcurrency:         8,
		s3Concurrency:           32,
		s3QueueWait:             time.Second,
		queueConcurrency:        16,
		respConcurrency:         32,
		jobRetention:            store.DefaultJobRetention,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestParseServeConfigFlags(t *testing.T) {
	clearServeEnv(t)
	cfg, err := parseServeConfig([]string{
		"--url", "postgres://db/pgvs3", "--addr", "127.0.0.1:9000",
		"--pool-max", "12", "--max-conns", "99", "--max-db-bytes", "1024",
		"--http-read-timeout", "10m", "--http-write-timeout", "20m",
		"--db-work-mem", "32MB", "--db-statement-timeout", "2m",
		"--db-lock-timeout", "2s", "--db-idle-tx-timeout", "30s",
		"--db-max-conn-lifetime", "30m", "--db-max-conn-lifetime-jitter", "2m",
		"--reap-grace", "2m", "--read-only", "--log-level", "warn",
		"--pprof-addr", "127.0.0.1:6060", "--origin-config", "/tmp/origin.json",
		"--cache-ttl", "30s", "--cache-retention", "1h", "--cache-namespace", "cache",
		"--fill-concurrency", "3", "--range-cache-chunk", "4096",
		"--cache-max-bytes", "1048576",
		"--s3-concurrency", "7", "--s3-queue-wait", "75ms",
		"--queue-concurrency", "5", "--resp-concurrency", "6",
		"--job-retention", "2h",
		"--simulate-latency", "25ms", "--simulate-rps", "17", "--simulate-bandwidth", "52428800",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := serveConfig{
		url:                     "postgres://db/pgvs3",
		addr:                    "127.0.0.1:9000",
		poolMax:                 12,
		spoolMaxBytes:           8 << 30,
		dbWorkMem:               "32MB",
		dbStatementTimeout:      2 * time.Minute,
		dbLockTimeout:           2 * time.Second,
		dbIdleTxTimeout:         30 * time.Second,
		dbMaxConnLifetime:       30 * time.Minute,
		dbMaxConnLifetimeJitter: 2 * time.Minute,
		maxConns:                99,
		httpReadTimeout:         10 * time.Minute,
		httpWriteTimeout:        20 * time.Minute,
		maxDBBytes:              1024,
		reapGrace:               2 * time.Minute,
		readOnly:                true,
		logLevel:                "warn",
		pprofAddr:               "127.0.0.1:6060",
		originConfig:            "/tmp/origin.json",
		cacheTTL:                30 * time.Second,
		cacheRetention:          time.Hour,
		cacheNamespace:          "cache",
		cacheWriteBack:          true,
		fillConcurrency:         3,
		rangeCacheChunk:         4096,
		cacheMaxBytes:           1048576,
		s3Concurrency:           7,
		s3QueueWait:             75 * time.Millisecond,
		queueConcurrency:        5,
		respConcurrency:         6,
		jobRetention:            2 * time.Hour,
		simulateLatency:         25 * time.Millisecond,
		simulateRPS:             17,
		simulateBandwidth:       52428800,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestOriginConfig(t *testing.T) {
	cfg := originConfig{
		cacheNamespace:  "cache",
		ttl:             time.Minute,
		retention:       24 * time.Hour,
		fillConcurrency: 8,
		rangeCacheChunk: 4096,
		cacheMaxBytes:   1 << 30,
	}
	if got := cfg.scope(); got != "global" {
		t.Fatalf("scope = %q", got)
	}
	cfg.bucket = "bucket"
	if got := cfg.scope(); got != "bucket" {
		t.Fatalf("scope = %q", got)
	}
	sp := cfg.spec([]byte(`{"endpoint":"origin"}`))
	if sp.Namespace != "cache" || sp.TTL != "1m0s" || sp.Retention != "24h0m0s" ||
		sp.MaxFills != 8 || sp.RangeChunk != 4096 || sp.MaxBytes != 1<<30 ||
		sp.Config != `{"endpoint":"origin"}` {
		t.Fatalf("spec = %+v", sp)
	}
	for _, action := range []string{"set", "clear", "list", "sql"} {
		if originActions[action] == nil {
			t.Fatalf("origin action %q is missing", action)
		}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	fn()
	os.Stdout = old
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestOriginSQL(t *testing.T) {
	t.Setenv("PGVS3_ENDPOINT", "http://gateway.example:9000")
	t.Setenv("PGVS3_URL", "postgres://alice:s%27cret@db.example:5544/lake_catalog?sslmode=disable")
	code := -1
	out := captureStdout(t, func() { code = originSQL(context.Background(), nil, nil, originConfig{bucket: "edge"}) })
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{
		"ENDPOINT 'gateway.example:9000'",
		"HOST 'db.example'",
		"PORT 5544",
		"DATABASE 'lake_catalog'",
		"USER 'alice'",
		"PASSWORD 's''cret'",
		"DATA_PATH 's3://edge/ducklake/'",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestOriginSQLDefaults(t *testing.T) {
	t.Setenv("PGVS3_ENDPOINT", "")
	t.Setenv("PGVS3_URL", "")
	host, port, user, password, database := ducklakePostgres()
	if host != "127.0.0.1" || port != 5432 || user != "postgres" || password != "postgres" || database != "ducklake_catalog" {
		t.Fatalf("defaults = %s %d %s %s %s", host, port, user, password, database)
	}
	code := -1
	out := captureStdout(t, func() { code = originSQL(context.Background(), nil, nil, originConfig{}) })
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{"ENDPOINT '127.0.0.1:8014'", "DATA_PATH 's3://lake/ducklake/'"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}

func TestOriginS3ShorthandFlags(t *testing.T) {
	clearServeEnv(t)
	var cfg originConfig
	fs := flag.NewFlagSet("origin", flag.ContinueOnError)
	registerOriginFlags(fs, &cfg)
	if err := fs.Parse([]string{
		"--s3-endpoint", "s3.example.com", "--s3-bucket", "warehouse",
		"--s3-region", "eu-west-1", "--s3-access-key", "key",
		"--s3-secret-key", "secret", "--s3-path-style", "--s3-insecure",
		"--cache-max-bytes", "2048",
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.buildS3JSON()
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Type   string `json:"type"`
		Config struct {
			Bucket           string `json:"bucket"`
			Endpoint         string `json:"endpoint"`
			Region           string `json:"region"`
			AccessKey        string `json:"access_key"`
			SecretKey        string `json:"secret_key"`
			Insecure         bool   `json:"insecure"`
			BucketLookupType string `json:"bucket_lookup_type"`
			StripBucket      bool   `json:"strip_bucket"`
		} `json:"config"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Type != "s3" || got.Config.Bucket != "warehouse" || got.Config.Endpoint != "s3.example.com" ||
		got.Config.Region != "eu-west-1" || got.Config.AccessKey != "key" || got.Config.SecretKey != "secret" ||
		!got.Config.Insecure || got.Config.BucketLookupType != "path" || got.Config.StripBucket {
		t.Fatalf("s3 config = %+v (%s)", got, raw)
	}
	if cfg.spec(raw).MaxBytes != 2048 {
		t.Fatalf("max bytes = %d", cfg.spec(raw).MaxBytes)
	}

	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "ap-south-1")
	t.Setenv("AWS_ACCESS_KEY_ID", "env-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
	cfg = originConfig{bucket: "edge", s3Endpoint: "minio:9000"}
	raw, err = cfg.buildS3JSON()
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Config.Bucket != "edge" || !got.Config.StripBucket || got.Config.BucketLookupType != "auto" ||
		got.Config.Region != "ap-south-1" || got.Config.AccessKey != "env-key" || got.Config.SecretKey != "env-secret" {
		t.Fatalf("environment fallback = %+v (%s)", got, raw)
	}

	var envCfg originConfig
	fs = flag.NewFlagSet("origin", flag.ContinueOnError)
	registerOriginFlags(fs, &envCfg)
	t.Setenv("PGVS3_S3_ENDPOINT", "env-host:9000")
	t.Setenv("PGVS3_S3_BUCKET", "env-bucket")
	t.Setenv("PGVS3_S3_PATH_STYLE", "yes")
	if err := applyFlagEnv(fs, "PGVS3_"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if envCfg.s3Endpoint != "env-host:9000" || envCfg.s3Bucket != "env-bucket" || !envCfg.s3PathStyle {
		t.Fatalf("environment flags = %+v", envCfg)
	}
}

func TestExtractSubcommand(t *testing.T) {
	cases := []struct {
		in       []string
		wantCmd  string
		wantRest []string
	}{
		{
			in:       []string{"drop", "--force", "stat"},
			wantCmd:  "drop",
			wantRest: []string{"--force", "stat"},
		},
		{
			in:       []string{"clone", "stat", "doctor"},
			wantCmd:  "clone",
			wantRest: []string{"stat", "doctor"},
		},
		{
			in:       []string{"--url", "postgres://localhost/pgvs3", "serve", "--addr", "127.0.0.1:8014"},
			wantCmd:  "serve",
			wantRest: []string{"--url", "postgres://localhost/pgvs3", "--addr", "127.0.0.1:8014"},
		},
		{
			in:       []string{"--addr", "127.0.0.1:8014"},
			wantCmd:  "",
			wantRest: []string{"--addr", "127.0.0.1:8014"},
		},
	}
	for _, tc := range cases {
		cmd, rest := extractSubcommand(tc.in)
		if cmd != tc.wantCmd || !reflect.DeepEqual(rest, tc.wantRest) {
			t.Fatalf("extractSubcommand(%v) = (%q, %v), want (%q, %v)", tc.in, cmd, rest, tc.wantCmd, tc.wantRest)
		}
	}
}

func TestParseServeConfigRejectsInvalidLimits(t *testing.T) {
	for _, args := range [][]string{
		{"--pool-max", "0"},
		{"--spool-max-bytes", "0"},
		{"--max-conns", "-1"},
		{"--s3-concurrency", "0"},
		{"--s3-queue-wait", "-1s"},
		{"--http-read-timeout", "-1s"},
		{"--cache-max-bytes", "-1"},
	} {
		if _, err := parseServeConfig(args); err == nil {
			t.Fatalf("parseServeConfig(%v) succeeded, want error", args)
		}
	}
}

func TestServePrintsConfigErrors(t *testing.T) {
	cases := []struct {
		name     string
		envKey   string
		envValue string
		args     []string
		want     string
	}{
		{
			name: "malformed environment", envKey: "PGVS3_POOL_MAX", envValue: "not-a-number",
			args: []string{"serve"}, want: "PGVS3_POOL_MAX",
		},
		{
			name: "invalid validation", args: []string{"serve", "--pool-max", "0"},
			want: "limits must be positive",
		},
		{
			name: "unknown flag is not repeated", args: []string{"serve", "--not-a-flag"},
			want: "flag provided but not defined",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearServeEnv(t)
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envValue)
			}
			code, stderr := runCaptured(t, tc.args...)
			if code != 2 {
				t.Fatalf("exit = %d, want 2; stderr=%q", code, stderr)
			}
			if strings.Count(stderr, tc.want) != 1 {
				t.Fatalf("stderr = %q, want exactly one %q", stderr, tc.want)
			}
		})
	}
}

func TestHelpSucceeds(t *testing.T) {
	for _, args := range [][]string{
		{"--help"},
		{"serve", "--help"},
		{"stat", "--help"},
		{"health", "--help"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			clearServeEnv(t)
			code, stderr := runCaptured(t, args...)
			if code != 0 {
				t.Fatalf("run(%v) = %d, want 0; stderr=%q", args, code, stderr)
			}
			if !strings.Contains(stderr, "Usage of ") {
				t.Fatalf("run(%v) did not print usage; stderr=%q", args, stderr)
			}
		})
	}
}

func TestRunRejectsUnexpectedServeArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"typo before flags", []string{"nonsense", "--url", "postgres://localhost/pgvs3"}, `unexpected serve argument "nonsense"`},
		{"typo after flags", []string{"--url", "postgres://localhost/pgvs3", "nonsense"}, `unexpected serve argument "nonsense"`},
		{"extra serve arg", []string{"serve", "extra"}, `unexpected serve argument "extra"`},
		{"arg after terminator", []string{"--", "serve"}, `unexpected serve argument "serve"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearServeEnv(t)
			code, stderr := runCaptured(t, tc.args...)
			if code != 2 {
				t.Fatalf("run(%v) = %d, want 2; stderr=%q", tc.args, code, stderr)
			}
			if !strings.Contains(stderr, tc.want) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr, tc.want)
			}
		})
	}
}

func TestServeEnvironmentUsesFlagParsers(t *testing.T) {
	clearServeEnv(t)
	t.Setenv("PGVS3_POOL_MAX", "0012")
	t.Setenv("PGVS3_DB_LOCK_TIMEOUT", "1250ms")
	t.Setenv("PGVS3_READ_ONLY", "yes")
	t.Setenv("PGVS3_CACHE_WRITE_BACK", "no")
	t.Setenv("PGVS3_SIMULATE_RPS", "17")
	cfg, err := parseServeConfig([]string{"--pool-max=9", "--read-only=false"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.poolMax != 9 || cfg.readOnly || cfg.cacheWriteBack || cfg.dbLockTimeout != 1250*time.Millisecond || cfg.simulateRPS != 17 {
		t.Fatalf("environment/flag precedence: %+v", cfg)
	}
	if fromEnv, err := parseServeConfig(nil); err != nil || fromEnv.poolMax != 12 {
		t.Fatalf("zero-padded decimal environment: %+v %v", fromEnv, err)
	}
	for _, key := range []string{"PGVS3_SIMULATE_RPS", "PGVS3_DB_LOCK_TIMEOUT", "PGVS3_CACHE_WRITE_BACK"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "invalid")
			if _, err := parseServeConfig(nil); err == nil {
				t.Fatalf("accepted invalid %s", key)
			}
		})
	}
	t.Setenv("PGVS3_CACHE_WRITE_BACK", "")
	cfg, err = parseServeConfig(nil)
	if err != nil || cfg.cacheWriteBack {
		t.Fatalf("empty boolean environment: %+v %v", cfg, err)
	}
}
