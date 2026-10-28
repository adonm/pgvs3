//go:build integration

package testutil

import (
	"context"
	"fmt"
	"os"
)

func Postgres(ctx context.Context) (string, func(), error) {
	url := os.Getenv("PGVS3_TEST_DATABASE_URL")
	if url == "" {
		return "", nil, fmt.Errorf("PGVS3_TEST_DATABASE_URL is not set (run `just integration` or provide a test PostgreSQL URL)")
	}
	return url, func() {}, nil
}

type S3Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

func S3() (S3Config, error) {
	cfg := S3Config{
		Endpoint:  os.Getenv("PGVS3_TEST_S3_ENDPOINT"),
		AccessKey: os.Getenv("PGVS3_TEST_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("PGVS3_TEST_S3_SECRET_KEY"),
	}
	if cfg.Endpoint == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		return S3Config{}, fmt.Errorf("PGVS3_TEST_S3_ENDPOINT, PGVS3_TEST_S3_ACCESS_KEY and PGVS3_TEST_S3_SECRET_KEY must be set (run `just integration` or provide an external S3-compatible origin)")
	}
	return cfg, nil
}
