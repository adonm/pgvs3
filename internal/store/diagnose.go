package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
)

// Diagnosis is the output of `pgvs3 doctor`.
type Diagnosis struct {
	ServerVersion    int    `json:"server_version_num"`
	Layout           int    `json:"layout"`
	Namespaces       int64  `json:"namespaces"`
	Buckets          int64  `json:"buckets"`
	Queues           int64  `json:"queues"`
	Caches           int64  `json:"caches"`
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
	d.ChunksRelkind = "r"
	d.ChunksConfigured = true
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return d, err
	}
	d.Namespaces = int64(len(infos))
	for _, n := range infos {
		switch n.Kind {
		case ns.Bucket:
			d.Buckets++
		case ns.Queue:
			d.Queues++
		case ns.Cache:
			d.Caches++
		}
		q := quoteIdent(n.Schema)
		if n.Kind == ns.Queue {
			var msgs int64
			if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM "+q+".messages").Scan(&msgs); err == nil {
				d.Objects += msgs
			}
			continue
		}
		var kind string
		var ok bool
		err := s.pool.QueryRow(ctx, `SELECT c.relkind::text,
			c.reloptions @> ARRAY['toast_tuple_target=8160',
				'autovacuum_vacuum_scale_factor=0.01',
				'autovacuum_analyze_scale_factor=0.02',
				'autovacuum_vacuum_threshold=1000']
			FROM pg_class c
			JOIN pg_namespace p ON p.oid = c.relnamespace
			WHERE p.nspname = $1 AND c.relname = 'chunks'`, n.Schema).Scan(&kind, &ok)
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
