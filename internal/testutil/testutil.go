package testutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

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

func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func Eventually(t testing.TB, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		select {
		case <-deadline.C:
			t.Fatal(msg)
		case <-tick.C:
		}
	}
}

func RunMain(m *testing.M, setup func(ctx context.Context, url string) (func(), error)) int {
	url := os.Getenv("PGVS3_TEST_DATABASE_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "PGVS3_TEST_DATABASE_URL is not set (run `just integration` or provide a test PostgreSQL URL)")
		return 1
	}
	cleanup, err := setup(context.Background(), url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer cleanup()
	return m.Run()
}
