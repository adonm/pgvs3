package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type Diagnosis struct {
	ServerVersion    int    `json:"server_version_num"`
	Layout           int    `json:"layout"`
	Buckets          int64  `json:"buckets"`
	ChunksRelkind    string `json:"chunks_relkind"`
	ChunksConfigured bool   `json:"chunks_configured"`
	Objects          int64  `json:"objects"`
	GarbageFiles     int64  `json:"garbage_files"`
	DatabaseBytes    int64  `json:"database_bytes"`
}

func (s *Store) Diagnose(ctx context.Context) (Diagnosis, error) {
	var d Diagnosis
	var versionText string
	if err := s.pool.QueryRow(ctx, "SHOW server_version_num").Scan(&versionText); err != nil {
		return d, err
	}
	fmt.Sscan(versionText, &d.ServerVersion)
	var layout pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&layout); err != nil {
		return d, err
	}
	if layout.Valid {
		d.Layout = int(layout.Int32)
	}
	var hasEnsure bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regprocedure('s3p.ensure_bucket(text)') IS NOT NULL").Scan(&hasEnsure); err != nil {
		return d, err
	}
	d.ChunksRelkind = "r"
	d.ChunksConfigured = hasEnsure
	schemas, err := s.bucketSchemas(ctx, "")
	if err != nil {
		return d, err
	}
	d.Buckets = int64(len(schemas))
	for _, sc := range schemas {
		q := quoteIdent(sc)
		var kind string
		var ok bool
		err := s.pool.QueryRow(ctx, `SELECT c.relkind::text,
			c.reloptions @> ARRAY['toast_tuple_target=8160',
				'autovacuum_vacuum_scale_factor=0.01',
				'autovacuum_analyze_scale_factor=0.02',
				'autovacuum_vacuum_threshold=1000']
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = 'chunks'`, sc).Scan(&kind, &ok)
		if err != nil || kind != "r" || !ok {
			d.ChunksConfigured = false
		}
		var objs, garb int64
		if err := s.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM "+q+".objects), (SELECT count(*) FROM "+q+".garbage)").
			Scan(&objs, &garb); err == nil {
			d.Objects += objs
			d.GarbageFiles += garb
		}
	}
	if err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&d.DatabaseBytes); err != nil {
		return d, err
	}
	return d, nil
}
