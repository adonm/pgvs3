package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrNotInteger = errors.New("value is not an integer or out of range")

// CacheSetOptions configures a RESP SET command on the unified object store.
type CacheSetOptions struct {
	Condition Condition
	ExpiresAt *time.Time
	KeepTTL   bool
	ReturnOld bool
}

// CacheGet returns the uncompressed bytes of an object (inline or chunked).
func (s *Store) CacheGet(ctx context.Context, bucket, key string) ([]byte, bool, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, false, err
	}
	var size int64
	var inline []byte
	var hasInline bool
	err = s.pool.QueryRow(ctx, `SELECT size, inline_data, inline_data IS NOT NULL
		FROM `+sc+`.objects WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`, key).
		Scan(&size, &inline, &hasInline)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if hasInline {
		raw, err := decompressBlock(nil, inline, int(size))
		if err != nil {
			return nil, false, err
		}
		return raw, true, nil
	}
	sl, err := s.Get(ctx, bucket, key, 0, -1, -1)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return sl.Bytes, true, nil
}

// CacheSet writes a value (compressed inline <= 64 KiB, or compressed 8,120 B
// chunk blocks > 64 KiB) to the bucket schema.
func (s *Store) CacheSet(ctx context.Context, bucket, key string, val []byte, opts CacheSetOptions) ([]byte, bool, bool, error) {
	var oldVal []byte
	var hadOld bool
	if opts.ReturnOld {
		v, ok, err := s.CacheGet(ctx, bucket, key)
		if err != nil {
			return nil, false, false, err
		}
		oldVal, hadOld = v, ok
	}
	if int64(len(val)) <= MaxInlineBytes {
		_, err := s.PutInlineOpts(ctx, bucket, key, val, opts.Condition, "", nil, "application/octet-stream", opts.ExpiresAt, opts.KeepTTL)
		if errors.Is(err, ErrPrecondition) {
			return oldVal, hadOld, false, nil
		}
		if err != nil {
			return nil, false, false, err
		}
		return oldVal, hadOld, true, nil
	}
	w, err := s.startWriter(ctx, writerTarget{
		kind:      0,
		bucket:    bucket,
		key:       key,
		condition: opts.Condition,
		expiresAt: opts.ExpiresAt,
		keepTTL:   opts.KeepTTL,
		content:   "application/octet-stream",
	})
	if err != nil {
		return nil, false, false, err
	}
	defer w.Abort()
	for rest := val; len(rest) > 0; {
		n := min(len(rest), 4<<20)
		if _, err := w.Write(rest[:n]); err != nil {
			return nil, false, false, err
		}
		rest = rest[n:]
	}
	_, err = w.Finish(nil)
	if errors.Is(err, ErrPrecondition) {
		return oldVal, hadOld, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}
	return oldVal, hadOld, true, nil
}

// CacheMGet fetches multiple keys in one SQL query for inline objects and
// streams any chunked (> 64 KiB) objects from their chunks table.
func (s *Store) CacheMGet(ctx context.Context, bucket string, keys []string) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT k.ord, o.size, o.inline_data, o.inline_data IS NOT NULL
		FROM unnest($1::text[]) WITH ORDINALITY AS k(key, ord)
		JOIN `+sc+`.objects o ON o.key = k.key
		WHERE o.expires_at IS NULL OR o.expires_at > now()`, keys)
	if isMissingBucketSchema(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	var chunkedIdx []int
	for rows.Next() {
		var ord int64
		var size int64
		var inline []byte
		var hasInline bool
		if err := rows.Scan(&ord, &size, &inline, &hasInline); err != nil {
			rows.Close()
			return nil, err
		}
		idx := int(ord - 1)
		if idx < 0 || idx >= len(out) {
			continue
		}
		if hasInline {
			raw, err := decompressBlock(nil, inline, int(size))
			if err != nil {
				rows.Close()
				return nil, err
			}
			out[idx] = raw
		} else {
			chunkedIdx = append(chunkedIdx, idx)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, idx := range chunkedIdx {
		v, ok, err := s.CacheGet(ctx, bucket, keys[idx])
		if err != nil {
			return nil, err
		}
		if ok {
			out[idx] = v
		}
	}
	return out, nil
}

// CacheMSet writes multiple inline (<= 64 KiB) key-value pairs in one atomic
// SQL round trip and one WAL commit, falling back to CacheSet for > 64 KiB
// chunked values.
func (s *Store) CacheMSet(ctx context.Context, bucket string, keys []string, vals [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	for _, v := range vals {
		if int64(len(v)) > MaxInlineBytes {
			for i := range keys {
				if _, _, _, err := s.CacheSet(ctx, bucket, keys[i], vals[i], CacheSetOptions{}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return err
	}
	if _, ok := s.knownBuckets.Load(bucket); !ok {
		if err := s.CreateBucket(ctx, bucket); err != nil {
			return err
		}
	}
	// Deduplicate keys within the batch (last value wins) so ON CONFLICT DO
	// UPDATE does not touch the same row twice in one statement.
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

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
		FROM (SELECT unnest($2::text[]) AS k ORDER BY 1) s`, bucket, uKeys); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `WITH old_objs AS (
		SELECT key, file_id, parts FROM `+sc+`.objects
		WHERE key = ANY($1::text[]) AND file_id IS NOT NULL
		FOR UPDATE
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
		  parts = NULL, part_ends = NULL, inline_data = EXCLUDED.inline_data,
		  created_at = now()
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
	ON CONFLICT DO NOTHING`, uKeys, uSizes, uSHAs, uETags, uStored)
	if isMissingBucketSchema(err) {
		_ = tx.Rollback(ctx)
		if err := s.CreateBucket(ctx, bucket); err != nil {
			return err
		}
		return s.CacheMSet(ctx, bucket, keys, vals)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// CacheDel removes keys and returns the number of live keys that were deleted.
func (s *Store) CacheDel(ctx context.Context, bucket string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
		FROM unnest($2::text[]) AS k`, bucket, keys); err != nil {
		return 0, err
	}
	var deleted int64
	err = tx.QueryRow(ctx, `WITH doomed AS (
		DELETE FROM `+sc+`.objects WHERE key = ANY($1::text[])
		RETURNING key, file_id, parts, (expires_at IS NULL OR expires_at > now()) AS live
	), dead AS (
		SELECT file_id FROM doomed WHERE file_id IS NOT NULL
		UNION ALL
		SELECT unnest(parts) FROM doomed WHERE parts IS NOT NULL
	), queued AS (
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
		ON CONFLICT DO NOTHING
	)
	SELECT count(*) FILTER (WHERE live) FROM doomed`, keys).Scan(&deleted)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return deleted, nil
}

// CacheExists returns the count of requested keys that exist (with duplicates
// counted per Redis EXISTS semantics).
func (s *Store) CacheExists(ctx context.Context, bucket string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return 0, err
	}
	var count int64
	err = s.pool.QueryRow(ctx, `SELECT count(*)
		FROM unnest($1::text[]) AS k(key)
		JOIN `+sc+`.objects o ON o.key = k.key
		WHERE o.expires_at IS NULL OR o.expires_at > now()`, keys).Scan(&count)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return count, err
}

// CacheExpire sets or clears (`expiresAt == nil` for PERSIST) the expiration on
// a live key.
func (s *Store) CacheExpire(ctx context.Context, bucket, key string, expiresAt *time.Time) (bool, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return false, err
	}
	if expiresAt == nil {
		tag, err := s.pool.Exec(ctx, `UPDATE `+sc+`.objects SET expires_at = NULL
			WHERE key = $1 AND expires_at IS NOT NULL AND expires_at > now()`, key)
		if isMissingBucketSchema(err) {
			return false, nil
		}
		return tag.RowsAffected() > 0, err
	}
	if !expiresAt.After(time.Now()) {
		n, err := s.CacheDel(ctx, bucket, []string{key})
		return n > 0, err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE `+sc+`.objects SET expires_at = $2
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`, key, *expiresAt)
	if isMissingBucketSchema(err) {
		return false, nil
	}
	return tag.RowsAffected() > 0, err
}

// CacheTTL returns the remaining TTL of a key.
func (s *Store) CacheTTL(ctx context.Context, bucket, key string) (time.Duration, bool, bool, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return 0, false, false, err
	}
	var remSec pgtype.Float8
	err = s.pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (expires_at - now()))::float8
		FROM `+sc+`.objects
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`, key).Scan(&remSec)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return 0, false, false, nil
	}
	if err != nil {
		return 0, false, false, err
	}
	if !remSec.Valid {
		return 0, false, true, nil
	}
	d := time.Duration(remSec.Float64 * float64(time.Second))
	if d < 0 {
		return 0, false, false, nil
	}
	return d, true, true, nil
}

// CacheIncrBy atomically increments a signed 64-bit decimal integer stored at
// key (initializing to 0 if missing or expired) and preserves any active TTL.
func (s *Store) CacheIncrBy(ctx context.Context, bucket, key string, delta int64) (int64, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return 0, err
	}
	if _, ok := s.knownBuckets.Load(bucket); !ok {
		if err := s.CreateBucket(ctx, bucket); err != nil {
			return 0, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := lockObjectKey(ctx, tx, bucket, key); err != nil {
		return 0, err
	}
	var (
		size      int64
		inline    []byte
		hasInline bool
		expiresAt pgtype.Timestamptz
		live      bool
	)
	err = tx.QueryRow(ctx, `SELECT size, inline_data, inline_data IS NOT NULL, expires_at,
		(expires_at IS NULL OR expires_at > now())
		FROM `+sc+`.objects WHERE key = $1 FOR UPDATE`, key).
		Scan(&size, &inline, &hasInline, &expiresAt, &live)
	if isMissingBucketSchema(err) {
		_ = tx.Rollback(ctx)
		if err := s.CreateBucket(ctx, bucket); err != nil {
			return 0, err
		}
		return s.CacheIncrBy(ctx, bucket, key, delta)
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	var cur int64
	var keepExp *time.Time
	if err == nil && live {
		if !hasInline {
			return 0, ErrNotInteger
		}
		raw, err := decompressBlock(nil, inline, int(size))
		if err != nil {
			return 0, err
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		cur = parsed
		if (delta > 0 && cur > (1<<63-1)-delta) || (delta < 0 && cur < (-1<<63)-delta) {
			return 0, ErrNotInteger
		}
		if expiresAt.Valid {
			t := expiresAt.Time
			keepExp = &t
		}
	}
	next := cur + delta
	payload := []byte(strconv.FormatInt(next, 10))
	sumMD5 := md5.Sum(payload)
	sumSHA := sha256.Sum256(payload)
	etag := hex.EncodeToString(sumMD5[:])
	stored := compressBlock(nil, payload)
	var outcome string
	if err := tx.QueryRow(ctx,
		"SELECT "+sc+".put_inline($1,$2,$3,$4,$5,$6,$7,'none',NULL,'on',$8,false)",
		key, stored, int64(len(payload)), sumSHA[:], etag, []string{}, "text/plain; charset=utf-8", keepExp).Scan(&outcome); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return next, nil
}

// CacheDBSize returns the number of non-expired objects in a bucket.
func (s *Store) CacheDBSize(ctx context.Context, bucket string) (int64, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return 0, err
	}
	var n int64
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM `+sc+`.objects WHERE expires_at IS NULL OR expires_at > now()`).Scan(&n)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return n, err
}

// CacheFlush drops and recreates a bucket schema immediately.
func (s *Store) CacheFlush(ctx context.Context, bucket string) error {
	if _, err := s.DropBucket(ctx, bucket, true); err != nil {
		return err
	}
	return s.CreateBucket(ctx, bucket)
}
