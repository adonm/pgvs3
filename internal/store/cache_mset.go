package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"

	"github.com/adonm/pgvs3/internal/ns"
)

// CacheMSet writes multiple inline values in one atomic statement and one WAL
// commit. Values above the inline ceiling fall back to CacheSet.
func (s *Store) CacheMSet(ctx context.Context, name string, keys []string, vals [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	inlineMax := int64(ns.Cache.InlineMax())
	for _, v := range vals {
		if int64(len(v)) > inlineMax {
			for i := range keys {
				if _, _, _, err := s.CacheSet(ctx, name, keys[i], vals[i], CacheSetOptions{}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	if s.overQuota(ns.Cache, name) {
		return ErrQuotaExceeded
	}
	if !s.known(ns.Cache, name) {
		if err := s.EnsureNamespace(ctx, ns.Cache, name, nil, nil); err != nil {
			return err
		}
	}
	// Deduplicate keys within the batch (last value wins); ON CONFLICT DO
	// UPDATE cannot touch the same row twice in one statement.
	seen := make(map[string]int, len(keys))
	var uKeys []string
	var uStored [][]byte
	var uSizes []int64
	var uSHAs [][]byte
	var uETags []string
	for i, k := range keys {
		v := vals[i]
		sumMD5 := md5.Sum(v)
		sumSHA := sha256.Sum256(v)
		etag := hex.EncodeToString(sumMD5[:])
		stored := compressBlock(nil, v)
		if idx, ok := seen[k]; ok {
			uStored[idx] = stored
			uSizes[idx] = int64(len(v))
			uSHAs[idx] = sumSHA[:]
			uETags[idx] = etag
			continue
		}
		seen[k] = len(uKeys)
		uKeys = append(uKeys, k)
		uStored = append(uStored, stored)
		uSizes = append(uSizes, int64(len(v)))
		uSHAs = append(uSHAs, sumSHA[:])
		uETags = append(uETags, etag)
	}
	err := s.cacheMSet(ctx, name, uKeys, uSizes, uSHAs, uETags, uStored)
	if isMissingBucketSchema(err) {
		if e := s.EnsureNamespace(ctx, ns.Cache, name, nil, nil); e != nil {
			return e
		}
		err = s.cacheMSet(ctx, name, uKeys, uSizes, uSHAs, uETags, uStored)
	}
	return err
}

func (s *Store) cacheMSet(ctx context.Context, name string, keys []string, sizes []int64,
	shas [][]byte, etags []string, stored [][]byte) error {
	sc, err := nsSQLIdent(ns.Cache, name)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	// Lock in sorted order so concurrent batches cannot deadlock.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
		FROM (SELECT DISTINCT unnest($2::text[]) AS k ORDER BY 1) s`, name, keys); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `WITH old_objs AS (
		SELECT key, file_id, parts FROM `+sc+`.objects
		WHERE key = ANY($1::text[]) AND file_id IS NOT NULL
	), cleared AS (
		DELETE FROM `+sc+`.object_part_checksums WHERE key = ANY($1::text[])
	), upserted AS (
		INSERT INTO `+sc+`.objects (key, file_id, size, sha256, etag, expires_at, parts, part_ends, inline_data)
		SELECT k, NULL, sz, sha, et, NULL, NULL, NULL, dat
		FROM unnest($1::text[], $2::int8[], $3::bytea[], $4::text[], $5::bytea[])
		  AS t(k, sz, sha, et, dat)
		ON CONFLICT (key) DO UPDATE SET
		  file_id = NULL, size = EXCLUDED.size, sha256 = EXCLUDED.sha256,
		  etag = EXCLUDED.etag, user_metadata = '{}'::text[],
		  content_type = 'application/octet-stream', expires_at = NULL,
		  origin_etag = NULL, parts = NULL, part_ends = NULL,
		  inline_data = EXCLUDED.inline_data, created_at = now()
		RETURNING key
	), dead AS (
		SELECT file_id FROM old_objs WHERE file_id IS NOT NULL
		UNION ALL
		SELECT unnest(parts) FROM old_objs WHERE parts IS NOT NULL
	)
	INSERT INTO `+sc+`.garbage (file_id)
	SELECT DISTINCT d.file_id FROM dead d
	WHERE d.file_id IS NOT NULL
	  AND NOT EXISTS (SELECT 1 FROM `+sc+`.objects o
	    WHERE o.file_id IS NOT NULL AND o.file_id = d.file_id
	      AND NOT (o.key = ANY($1::text[])))
	  AND NOT EXISTS (SELECT 1 FROM `+sc+`.objects o
	    WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[d.file_id]
	      AND NOT (o.key = ANY($1::text[])))
	  AND NOT EXISTS (SELECT 1 FROM `+sc+`.upload_parts p WHERE p.file_id = d.file_id)
	ON CONFLICT DO NOTHING`, keys, sizes, shas, etags, stored); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
