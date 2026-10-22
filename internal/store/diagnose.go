package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type Diagnosis struct {
	ServerVersion    int    `json:"server_version_num"`
	Layout           int    `json:"layout"`
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
	if err := s.pool.QueryRow(ctx, "SELECT relkind::text FROM pg_class WHERE oid = 's3p.chunks'::regclass").Scan(&d.ChunksRelkind); err != nil {
		return d, err
	}
	want := `reloptions @> ARRAY['toast_tuple_target=8160',
		'autovacuum_vacuum_scale_factor=0.01',
		'autovacuum_analyze_scale_factor=0.02',
		'autovacuum_vacuum_threshold=1000']`
	query := `SELECT EXISTS (SELECT 1 FROM pg_class WHERE oid = 's3p.chunks'::regclass AND ` + want + `)`
	if d.ChunksRelkind == "p" {
		query = `SELECT count(*) > 0 AND count(*) = count(*) FILTER (WHERE c.reloptions @> ARRAY['toast_tuple_target=8160',
			'autovacuum_vacuum_scale_factor=0.01',
			'autovacuum_analyze_scale_factor=0.02',
			'autovacuum_vacuum_threshold=1000'])
			FROM pg_partition_tree('s3p.chunks') t JOIN pg_class c ON c.oid = t.relid WHERE t.isleaf`
	}
	if err := s.pool.QueryRow(ctx, query).Scan(&d.ChunksConfigured); err != nil {
		return d, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM s3p.objects),
		(SELECT count(*) FROM s3p.garbage), pg_database_size(current_database())`).
		Scan(&d.Objects, &d.GarbageFiles, &d.DatabaseBytes); err != nil {
		return d, err
	}
	return d, nil
}
