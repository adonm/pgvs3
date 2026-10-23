package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// OriginListEntry is one cached origin object in the listing index.
type OriginListEntry struct {
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
}

// ListOrigin returns one page of the cache namespace's origin listing index.
// fresh is false when the index for prefix has never been built or is older
// than ttl; the caller must refresh it from the origin before trusting rows.
func (s *Store) ListOrigin(ctx context.Context, namespace, prefix, prefixEnd, after string, limit int, ttl time.Duration) ([]OriginListEntry, bool, error) {
	var refreshed time.Time
	var complete bool
	err := s.pool.QueryRow(ctx,
		`SELECT refreshed_at, complete FROM s3p.origin_listing_state
		 WHERE namespace = $1 AND prefix = $2`, namespace, prefix).Scan(&refreshed, &complete)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !complete || ttl <= 0 || time.Since(refreshed) > ttl {
		return nil, false, nil
	}
	rows, err := s.pool.Query(ctx,
		`SELECT key, size, etag, content_type, last_modified
		 FROM s3p.origin_listing
		 WHERE namespace = $1 AND key >= $2 AND key < $3 AND key > $4
		 ORDER BY key COLLATE "C" LIMIT $5`,
		namespace, prefix, prefixEnd, after, limit)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := make([]OriginListEntry, 0, limit)
	for rows.Next() {
		var e OriginListEntry
		if err := rows.Scan(&e.Key, &e.Size, &e.ETag, &e.ContentType, &e.LastModified); err != nil {
			return nil, false, err
		}
		out = append(out, e)
	}
	return out, true, rows.Err()
}

// ReplaceOriginListing atomically replaces every index row under prefix with
// entries and marks the prefix complete. It is called after a full origin walk.
func (s *Store) ReplaceOriginListing(ctx context.Context, namespace, prefix, prefixEnd string, entries []OriginListEntry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx,
		`DELETE FROM s3p.origin_listing
		 WHERE namespace = $1 AND key >= $2 AND key < $3`, namespace, prefix, prefixEnd); err != nil {
		return err
	}
	if len(entries) > 0 {
		if _, err := tx.CopyFrom(ctx,
			pgx.Identifier{"s3p", "origin_listing"},
			[]string{"namespace", "key", "size", "etag", "content_type", "last_modified"},
			pgx.CopyFromSlice(len(entries), func(i int) ([]any, error) {
				e := entries[i]
				return []any{namespace, e.Key, e.Size, e.ETag, e.ContentType, e.LastModified}, nil
			})); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO s3p.origin_listing_state (namespace, prefix, refreshed_at, complete)
		 VALUES ($1, $2, now(), true)
		 ON CONFLICT (namespace, prefix) DO UPDATE
		   SET refreshed_at = now(), complete = true`, namespace, prefix); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// UpsertOriginEntry keeps a write-through object visible in the listing index
// when that prefix has already been built. It is harmless before the first
// refresh: ListOrigin ignores rows until the prefix is marked complete.
func (s *Store) UpsertOriginEntry(ctx context.Context, namespace string, e OriginListEntry) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO s3p.origin_listing
		   (namespace, key, size, etag, content_type, last_modified, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, now())
		 ON CONFLICT (namespace, key) DO UPDATE
		   SET size = EXCLUDED.size, etag = EXCLUDED.etag,
		       content_type = EXCLUDED.content_type,
		       last_modified = EXCLUDED.last_modified, updated_at = now()`,
		namespace, e.Key, e.Size, e.ETag, e.ContentType, e.LastModified)
	return err
}

// DeleteOriginEntries removes keys from the listing index after a successful
// origin delete.
func (s *Store) DeleteOriginEntries(ctx context.Context, namespace string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.origin_listing WHERE namespace = $1 AND key = ANY($2::text[])`,
		namespace, keys)
	return err
}
