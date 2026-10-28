package main

import (
	"reflect"
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
		"PGVS3_QUEUE_CONCURRENCY", "PGVS3_RESP_CONCURRENCY",
		"PGVS3_JOB_RETENTION",
		"PGVS3_SPOOL_MAX_BYTES",
	} {
		t.Setenv(key, "")
	}
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
		"--s3-concurrency", "7",
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
		s3Concurrency:           7,
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
		sp.MaxFills != 8 || sp.RangeChunk != 4096 || sp.Config != `{"endpoint":"origin"}` {
		t.Fatalf("spec = %+v", sp)
	}
	for _, action := range []string{"set", "clear", "list"} {
		if originActions[action] == nil {
			t.Fatalf("origin action %q is missing", action)
		}
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
		{"--http-read-timeout", "-1s"},
	} {
		if _, err := parseServeConfig(args); err == nil {
			t.Fatalf("parseServeConfig(%v) succeeded, want error", args)
		}
	}
}

func TestParseServeConfigRejectsMalformedEnv(t *testing.T) {
	clearServeEnv(t)
	t.Setenv("PGVS3_POOL_MAX", "not-a-number")
	if _, err := parseServeConfig(nil); err == nil {
		t.Fatal("parseServeConfig accepted malformed PGVS3_POOL_MAX")
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
