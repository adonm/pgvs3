//go:build integration

// Package testutil provides the PostgreSQL container used by integration
// tests. It is compiled only with the integration build tag.
package testutil

import (
	"context"

	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Postgres starts PostgreSQL 18 and returns its connection string plus a
// cleanup function. The caller owns the returned Store.
func Postgres(ctx context.Context) (string, func(), error) {
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("pgvs3_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = ctr.Terminate(context.Background()) }
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		cleanup()
		return "", nil, err
	}
	return url, cleanup, nil
}
