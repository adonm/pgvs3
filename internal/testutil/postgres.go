//go:build integration

// Package testutil provides the external PostgreSQL connection used by
// integration tests. It is compiled only with the integration build tag.
package testutil

import (
	"context"
	"fmt"
	"os"
)

// Postgres returns the configured integration database. Tests never manage a
// container from Go; local runs use `just integration`, CI uses a service.
func Postgres(ctx context.Context) (string, func(), error) {
	url := os.Getenv("PGVS3_TEST_DATABASE_URL")
	if url == "" {
		return "", nil, fmt.Errorf("PGVS3_TEST_DATABASE_URL is not set (run `just integration` or provide a test PostgreSQL URL)")
	}
	return url, func() {}, nil
}
