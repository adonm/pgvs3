package main

import (
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func clearServeEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AWS_REGION",
		"PGVS3_URL", "PGVS3_ADDR", "PGVS3_POOL_MAX", "PGVS3_SPOOL_DIR",
		"PGVS3_SPOOL_MAX_BYTES", "PGVS3_MAX_CONNS", "PGVS3_READ_ONLY",
		"PGVS3_LOG_LEVEL", "PGVS3_ORIGIN_ENDPOINT", "PGVS3_ORIGIN_REGION",
		"PGVS3_ORIGIN_PATH_STYLE", "PGVS3_ORIGIN_INSECURE", "PGVS3_BUCKETS",
		"PGVS3_REVALIDATE_AFTER", "PGVS3_CACHE_RETENTION",
		"PGVS3_FILL_CONCURRENCY", "PGVS3_RANGE_CACHE_CHUNK",
		"PGVS3_CACHE_MAX_BYTES", "PGVS3_S3_CONCURRENCY", "PGVS3_S3_QUEUE_WAIT",
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
		logLevel:                "info",
		originRegion:            "us-east-1",
		revalidateAfter:         0,
		cacheRetention:          24 * time.Hour,
		fillConcurrency:         8,
		s3Concurrency:           32,
		s3QueueWait:             time.Second,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}
}

func TestParseServeConfigFlags(t *testing.T) {
	clearServeEnv(t)
	cfg, err := parseServeConfig([]string{
		"--url", "postgres://db/pgvs3", "--addr", "127.0.0.1:9000",
		"--pool-max", "12", "--max-conns", "99",
		"--http-read-timeout", "10m", "--http-write-timeout", "20m",
		"--db-work-mem", "32MB", "--db-statement-timeout", "2m",
		"--db-lock-timeout", "2s", "--db-idle-tx-timeout", "30s",
		"--db-max-conn-lifetime", "30m", "--db-max-conn-lifetime-jitter", "2m",
		"--read-only", "--log-level", "warn",
		"--origin-endpoint", "http://127.0.0.1:9000", "--origin-region", "us-west-2",
		"--origin-path-style", "--origin-insecure", "--buckets", "lake,edge",
		"--revalidate-after", "30s", "--cache-retention", "1h",
		"--fill-concurrency", "3", "--range-cache-chunk", "4096",
		"--cache-max-bytes", "1048576",
		"--s3-concurrency", "7", "--s3-queue-wait", "75ms",
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
		readOnly:                true,
		logLevel:                "warn",
		originEndpoint:          "http://127.0.0.1:9000",
		originRegion:            "us-west-2",
		originPathStyle:         true,
		originInsecure:          true,
		buckets:                 "lake,edge",
		revalidateAfter:         30 * time.Second,
		cacheRetention:          time.Hour,
		fillConcurrency:         3,
		rangeCacheChunk:         4096,
		cacheMaxBytes:           1048576,
		s3Concurrency:           7,
		s3QueueWait:             75 * time.Millisecond,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("config = %+v, want %+v", cfg, want)
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
		{"--revalidate-after", "-1s"},
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
			if !strings.Contains(stderr, "Usage of ") && !strings.Contains(stderr, "usage: pgvs3 ") {
				t.Fatalf("run(%v) did not print usage; stderr=%q", args, stderr)
			}
		})
	}
}

func TestRunRejectsUnknownCommandAndExtraServeArgs(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"unknown command", []string{"nonsense", "--url", "postgres://localhost/pgvs3"}, `unknown command "nonsense"`},
		{"flag before command", []string{"--url", "postgres://localhost/pgvs3", "serve"}, `unknown command "--url"`},
		{"extra serve arg", []string{"serve", "extra"}, `unexpected serve argument "extra"`},
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
	t.Setenv("PGVS3_ORIGIN_PATH_STYLE", "no")
	t.Setenv("PGVS3_S3_CONCURRENCY", "17")
	cfg, err := parseServeConfig([]string{"--pool-max=9", "--read-only=false"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.poolMax != 9 || cfg.readOnly || cfg.originPathStyle || cfg.dbLockTimeout != 1250*time.Millisecond || cfg.s3Concurrency != 17 {
		t.Fatalf("environment/flag precedence: %+v", cfg)
	}
	if fromEnv, err := parseServeConfig(nil); err != nil || fromEnv.poolMax != 12 {
		t.Fatalf("zero-padded decimal environment: %+v %v", fromEnv, err)
	}
	for _, key := range []string{"PGVS3_S3_CONCURRENCY", "PGVS3_DB_LOCK_TIMEOUT", "PGVS3_ORIGIN_PATH_STYLE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "invalid")
			if _, err := parseServeConfig(nil); err == nil {
				t.Fatalf("accepted invalid %s", key)
			}
		})
	}
	t.Setenv("AWS_REGION", "eu-central-1")
	if cfgRegion, err := parseServeConfig(nil); err != nil || cfgRegion.originRegion != "eu-central-1" {
		t.Fatalf("AWS_REGION default = %q err=%v", cfgRegion.originRegion, err)
	}
	t.Setenv("PGVS3_ORIGIN_REGION", "ap-southeast-2")
	if cfgRegion, err := parseServeConfig(nil); err != nil || cfgRegion.originRegion != "ap-southeast-2" {
		t.Fatalf("PGVS3_ORIGIN_REGION override = %q err=%v", cfgRegion.originRegion, err)
	}
	t.Setenv("AWS_REGION", "")
	t.Setenv("PGVS3_ORIGIN_REGION", "")

	t.Setenv("PGVS3_ORIGIN_PATH_STYLE", "")
	cfg, err = parseServeConfig(nil)
	if err != nil || cfg.originPathStyle {
		t.Fatalf("empty boolean environment: %+v %v", cfg, err)
	}
}
