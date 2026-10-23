package store

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// gooseUp applies every pending schema migration. The advisory lock in Init
// and Migrate serializes this across gateways; goose itself is not locked.
func (s *Store) gooseUp(ctx context.Context) error {
	db := stdlib.OpenDBFromPool(s.pool)
	defer db.Close()
	goose.SetBaseFS(migrationsFS)
	goose.SetTableName("s3p.goose_db_version")
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS s3p"); err != nil {
		return err
	}
	return goose.UpContext(ctx, db, "migrations")
}
