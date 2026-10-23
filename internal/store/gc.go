package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adonm/pgvs3/internal/ns"
)

// Maintain reconciles the namespace catalog, expires TTL'd keys and multipart
// uploads, reaps garbage and refreshes quota gauges. One gateway per cluster
// holds the advisory lock; the rest skip the tick.
func (s *Store) Maintain(ctx context.Context, grace time.Duration) error {
	s.maintenanceRuns.Add(1)
	const lock int64 = 0x70677674 // "pgvt"
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lock).Scan(&got); err != nil || !got {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lock)

	if _, err := conn.Exec(ctx, "SELECT s3p.reconcile_namespaces()"); err != nil {
		return err
	}
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return err
	}

	var expiredUploads int
	for _, n := range infos {
		q := quoteIdent(n.Schema)
		if n.Kind == ns.Queue {
			if removed, err := s.ExpireQueueMessages(ctx, n.Name, ExpireBatch); err == nil {
				s.expiredObjects.Add(removed)
			}
			continue
		}
		if n.Kind == ns.Bucket {
			var v int
			if err := conn.QueryRow(ctx, "SELECT "+q+".expire_uploads(interval '24 hours', 64)").Scan(&v); err != nil {
				if isMissingBucketSchema(err) {
					continue
				}
				return err
			}
			expiredUploads += v
		}
		// Expiry is chained until a pass comes back short, so a burst of TTL'd
		// keys drains in one tick instead of 4 keys/s.
		for pass := 0; pass < 128; pass++ {
			var v int
			if err := conn.QueryRow(ctx, "SELECT "+q+".expire_objects($1)", ExpireBatch).Scan(&v); err != nil {
				if isMissingBucketSchema(err) {
					break
				}
				return err
			}
			s.expiredObjects.Add(int64(v))
			if v < ExpireBatch {
				break
			}
		}
	}
	if expiredUploads > 0 {
		s.log.Info("expired multipart uploads", "count", expiredUploads)
	}

	remaining := ReapRows
	for _, n := range infos {
		if n.Kind == ns.Queue {
			continue
		}
		q := quoteIdent(n.Schema)
		for remaining > 0 {
			got, done, err := s.reapOne(ctx, conn.Conn(), q, grace, remaining)
			if isMissingBucketSchema(err) {
				break
			}
			if err != nil {
				return err
			}
			if got == 0 && !done {
				break
			}
			s.reapedRows.Add(int64(got))
			if done {
				s.reapedFiles.Add(1)
			}
			remaining -= max(got, 1)
		}
		if remaining <= 0 {
			break
		}
	}
	s.refreshQuotas(ctx, infos)
	return nil
}

// ExpireObjects removes expired keys from one namespace, up to max per call.
func (s *Store) ExpireObjects(ctx context.Context, kind ns.Kind, name string, max int) (int, error) {
	q, err := nsSQLIdent(kind, name)
	if err != nil {
		return 0, err
	}
	var n int
	err = s.pool.QueryRow(ctx, "SELECT "+q+".expire_objects($1)", max).Scan(&n)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return n, err
}

// refreshQuotas recomputes each quota'd namespace's physical size and flips
// the write gate. Runs on the maintenance tick so writes stay one query.
func (s *Store) refreshQuotas(ctx context.Context, infos []NamespaceInfo) {
	for _, n := range infos {
		if n.Quota == nil {
			continue
		}
		q := quoteIdent(n.Schema)
		table := "objects"
		if n.Kind == ns.Queue {
			table = "messages"
		}
		var size int64
		if err := s.pool.QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)", q+"."+table).Scan(&size); err != nil {
			continue
		}
		s.setQuotaState(n.Kind, n.Name, size, *n.Quota)
	}
}

func (s *Store) reapOne(ctx context.Context, conn *pgx.Conn, schema string, grace time.Duration, budget int) (int, bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var fileID int64
	err = tx.QueryRow(ctx, `SELECT file_id FROM `+schema+`.garbage
		WHERE queued_at <= now() - make_interval(secs => $1)
		ORDER BY queued_at, file_id LIMIT 1 FOR UPDATE SKIP LOCKED`, grace.Seconds()).Scan(&fileID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var referenced bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.objects WHERE file_id IS NOT NULL AND file_id = $1)
		OR EXISTS (SELECT 1 FROM `+schema+`.objects WHERE parts IS NOT NULL AND parts @> ARRAY[$1::int8])
		OR EXISTS (SELECT 1 FROM `+schema+`.upload_parts WHERE file_id = $1)`, fileID).Scan(&referenced); err != nil {
		return 0, false, err
	}
	if referenced {
		// A shared CopyObject/UploadPartCopy file can be queued by an older
		// gateway. Drop the false-positive garbage row instead of wedging the
		// reaper behind it forever.
		if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
			return 0, false, err
		}
		return 0, true, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM `+schema+`.chunks WHERE ctid IN
		(SELECT ctid FROM `+schema+`.chunks WHERE file_id = $1 LIMIT $2)`, fileID, budget)
	if err != nil {
		return 0, false, err
	}
	n := int(tag.RowsAffected())
	done := false
	if n < budget {
		var left bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+schema+".chunks WHERE file_id = $1)", fileID).Scan(&left); err != nil {
			return 0, false, err
		}
		if !left {
			if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
				return 0, false, err
			}
			done = true
		}
	}
	return n, done, tx.Commit(ctx)
}
