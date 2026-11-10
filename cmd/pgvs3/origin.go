package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

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

type originConfig struct {
	bucket          string
	cacheNamespace  string
	ttl             time.Duration
	retention       time.Duration
	fillConcurrency int
	rangeCacheChunk int64
	cacheMaxBytes   int64
	s3Endpoint      string
	s3Bucket        string
	s3Region        string
	s3AccessKey     string
	s3SecretKey     string
	s3PathStyle     bool
	s3Insecure      bool
}

func registerOriginFlags(fs *flag.FlagSet, cfg *originConfig) {
	fs.StringVar(&cfg.bucket, "bucket", "", "restrict origin passthrough to one bucket (default: global)")
	fs.StringVar(&cfg.cacheNamespace, "cache-namespace", "origin", "cache namespace in PostgreSQL")
	fs.DurationVar(&cfg.ttl, "cache-ttl", time.Minute, "origin revalidation interval (0 always revalidates)")
	fs.DurationVar(&cfg.retention, "cache-retention", 24*time.Hour, "idle eviction interval")
	fs.IntVar(&cfg.fillConcurrency, "fill-concurrency", 8, "process-wide concurrent origin fills and pending-upload workers")
	fs.Int64Var(&cfg.rangeCacheChunk, "range-cache-chunk", 0, "cache aligned ranges of large objects at this byte size (0 disables)")
	fs.Int64Var(&cfg.cacheMaxBytes, "cache-max-bytes", 0, "evict oldest cached objects when cache namespace exceeds this byte budget (0 disables)")
	fs.StringVar(&cfg.s3Endpoint, "s3-endpoint", "", "S3 origin endpoint URL or host when not passing an origin.json file")
	fs.StringVar(&cfg.s3Bucket, "s3-bucket", "", "S3 origin bucket name (defaults to --bucket)")
	fs.StringVar(&cfg.s3Region, "s3-region", "", "S3 origin region (defaults to AWS_REGION or us-east-1)")
	fs.StringVar(&cfg.s3AccessKey, "s3-access-key", "", "S3 origin access key ID (defaults to AWS_ACCESS_KEY_ID)")
	fs.StringVar(&cfg.s3SecretKey, "s3-secret-key", "", "S3 origin secret access key (defaults to AWS_SECRET_ACCESS_KEY)")
	fs.BoolVar(&cfg.s3PathStyle, "s3-path-style", false, "use path-style S3 origin addressing")
	fs.BoolVar(&cfg.s3Insecure, "s3-insecure", false, "allow plain HTTP S3 origin endpoint")
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
		MaxBytes:   c.cacheMaxBytes,
		Config:     string(raw),
	}
}

func (c originConfig) buildS3JSON() ([]byte, error) {
	bucket := c.s3Bucket
	if bucket == "" {
		bucket = c.bucket
	}
	region := c.s3Region
	if region == "" {
		region = env("AWS_REGION", env("AWS_DEFAULT_REGION", "us-east-1"))
	}
	lookup := "auto"
	if c.s3PathStyle {
		lookup = "path"
	}
	config := struct {
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
	}{Type: "s3"}
	accessKey := c.s3AccessKey
	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	secretKey := c.s3SecretKey
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	config.Config.Bucket = bucket
	config.Config.Endpoint = c.s3Endpoint
	config.Config.Region = region
	config.Config.AccessKey = accessKey
	config.Config.SecretKey = secretKey
	config.Config.Insecure = c.s3Insecure
	config.Config.BucketLookupType = lookup
	config.Config.StripBucket = c.bucket != ""
	return json.Marshal(config)
}

type originAction func(context.Context, *store.Store, []string, originConfig) int

var originActions = map[string]originAction{
	"set":   originSet,
	"clear": originClear,
	"list":  originList,
	"sql":   originSQL,
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
			fmt.Fprintf(os.Stderr, "unknown origin action %q (set, clear, list or sql)\n", action)
			return 2
		}
		return run(ctx, st, rest, cfg)
	})
}

func originSet(ctx context.Context, st *store.Store, rest []string, cfg originConfig) int {
	var raw []byte
	var err error
	switch {
	case len(rest) == 0 && cfg.s3Endpoint != "":
		raw, err = cfg.buildS3JSON()
		if err != nil {
			return fail(err)
		}
	case len(rest) == 1:
		raw, err = os.ReadFile(rest[0])
		if err != nil {
			return fail(err)
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: pgvs3 origin set [--bucket B] [--cache-namespace NS] [--range-cache-chunk N] [--cache-max-bytes N] <origin.json>")
		fmt.Fprintln(os.Stderr, "   or: pgvs3 origin set --s3-endpoint HOST [--s3-bucket B] [--bucket B] [...]")
		return 2
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

func originList(ctx context.Context, st *store.Store, _ []string, cfg originConfig) int {
	specs, err := objcache.ListSpecs(ctx, st)
	if err != nil {
		return fail(err)
	}
	for _, sp := range specs {
		scope := sp.Scope()
		if cfg.bucket != "" && scope != cfg.bucket {
			continue
		}
		line := fmt.Sprintf("origin=%s cache=%s ttl=%s retention=%s range_chunk=%d",
			scope, sp.Namespace, sp.TTL, sp.Retention, sp.RangeChunk)
		if sp.MaxBytes > 0 {
			line += fmt.Sprintf(" max_bytes=%d", sp.MaxBytes)
		}
		fmt.Println(line)
	}
	return 0
}

// originSQL prints a DuckDB/DuckLake attach snippet for the configured bucket.
func originSQL(_ context.Context, _ *store.Store, _ []string, cfg originConfig) int {
	bucket := cfg.bucket
	if bucket == "" {
		bucket = "lake"
	}
	endpoint := env("PGVS3_ENDPOINT", "127.0.0.1:8014")
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		endpoint = u.Host
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "https://"), "http://")
	host, port, user, password, database := ducklakePostgres()
	fmt.Println("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")
	fmt.Printf("CREATE TEMP SECRET pgvs3_s3 (TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1',\n"+
		"    ENDPOINT '%s', URL_STYLE 'path', USE_SSL false);\n", sqlLiteral(endpoint))
	fmt.Printf("CREATE TEMP SECRET pgvs3_pg (TYPE POSTGRES, HOST '%s', PORT %d,\n"+
		"    DATABASE '%s', USER '%s', PASSWORD '%s');\n",
		sqlLiteral(host), port, sqlLiteral(database), sqlLiteral(user), sqlLiteral(password))
	fmt.Printf("ATTACH 'ducklake:pgvs3_pg' AS lake (DATA_PATH 's3://%s/ducklake/');\n", sqlLiteral(bucket))
	return 0
}

func ducklakePostgres() (host string, port int, user, password, database string) {
	host, port, user, password, database = "127.0.0.1", 5432, "postgres", "postgres", "ducklake_catalog"
	raw := os.Getenv("PGVS3_URL")
	if raw == "" {
		return host, port, user, password, database
	}
	u, err := url.Parse(raw)
	if err != nil {
		return host, port, user, password, database
	}
	if value := u.Hostname(); value != "" {
		host = value
	}
	if value := u.Port(); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			port = parsed
		}
	}
	if u.User != nil {
		if value := u.User.Username(); value != "" {
			user = value
		}
		if value, ok := u.User.Password(); ok {
			password = value
		}
	}
	if value := strings.TrimPrefix(u.Path, "/"); value != "" {
		database = value
	}
	return host, port, user, password, database
}

func sqlLiteral(value string) string { return strings.ReplaceAll(value, "'", "''") }
