//go:build integration

package store

import "github.com/jackc/pgx/v5/pgxpool"

func (s *Store) Pool() *pgxpool.Pool { return s.pool }
