package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
)

var ErrNotInteger = errors.New("value is not an integer or out of range")

// CacheSetOptions configures a RESP SET command on the unified object store.
type CacheSetOptions struct {
	Condition Condition
	ExpiresAt *time.Time
	KeepTTL   bool
	ReturnOld bool
}

// CacheGet returns the value of one cache key.
func (s *Store) CacheGet(ctx context.Context, name, key string) ([]byte, bool, error) {
	sc, err := nsSQLIdent(ns.Cache, name)
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
	if !hasInline {
		// A cache value larger than one page spilled into chunks.
		sl, err := s.GetKind(ctx, ns.Cache, name, key, 0, -1, -1)
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		return sl.Bytes, true, nil
	}
	raw, err := decompressBlock(nil, inline, int(size))
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// CacheSet writes a value (one compressed page when it fits, chunked blocks
// above that) into a cache namespace.
func (s *Store) CacheSet(ctx context.Context, name, key string, val []byte, opts CacheSetOptions) ([]byte, bool, bool, error) {
	var oldVal []byte
	var hadOld bool
	if opts.ReturnOld {
		v, ok, err := s.CacheGet(ctx, name, key)
		if err != nil {
			return nil, false, false, err
		}
		oldVal, hadOld = v, ok
	}
	if int64(len(val)) <= int64(ns.Cache.InlineMax()) {
		_, err := s.PutInlineKind(ctx, ns.Cache, name, key, val, opts.Condition, "", nil,
			"application/octet-stream", opts.ExpiresAt, opts.KeepTTL, "")
		if errors.Is(err, ErrPrecondition) {
			return oldVal, hadOld, false, nil
		}
		if err != nil {
			return nil, false, false, err
		}
		return oldVal, hadOld, true, nil
	}
	w, err := s.StartObjectKind(ctx, ns.Cache, name, key, opts.Condition, "", nil,
		"application/octet-stream", opts.ExpiresAt, opts.KeepTTL, "")
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

// CacheMGet fetches multiple keys in one SQL query for inline values and
// streams any chunked values.
func (s *Store) CacheMGet(ctx context.Context, name string, keys []string) ([][]byte, error) {
	out := make([][]byte, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	sc, err := nsSQLIdent(ns.Cache, name)
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
		v, ok, err := s.CacheGet(ctx, name, keys[idx])
		if err != nil {
			return nil, err
		}
		if ok {
			out[idx] = v
		}
	}
	return out, nil
}

// CacheDel removes keys and returns the number of live keys that were deleted.
// Keys are sorted before locking so concurrent batches cannot deadlock.
func (s *Store) CacheDel(ctx context.Context, name string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sc, err := nsSQLIdent(ns.Cache, name)
	if err != nil {
		return 0, err
	}
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended(k,1))
		FROM unnest($2::text[]) AS k`, name, sorted); err != nil {
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

// CacheExists counts requested keys that exist (duplicates counted per Redis).
func (s *Store) CacheExists(ctx context.Context, name string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	sc, err := nsSQLIdent(ns.Cache, name)
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

// CacheExpire sets or clears (`expiresAt == nil`) a key's expiration.
func (s *Store) CacheExpire(ctx context.Context, name, key string, expiresAt *time.Time) (bool, error) {
	sc, err := nsSQLIdent(ns.Cache, name)
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
		n, err := s.CacheDel(ctx, name, []string{key})
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
func (s *Store) CacheTTL(ctx context.Context, name, key string) (time.Duration, bool, bool, error) {
	sc, err := nsSQLIdent(ns.Cache, name)
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

// CacheIncrBy atomically increments a decimal integer stored at key and
// preserves any active TTL.
func (s *Store) CacheIncrBy(ctx context.Context, name, key string, delta int64) (int64, error) {
	sc, err := nsSQLIdent(ns.Cache, name)
	if err != nil {
		return 0, err
	}
	if !s.known(ns.Cache, name) {
		if err := s.EnsureNamespace(ctx, ns.Cache, name, nil, nil); err != nil {
			return 0, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0) # hashtextextended($2,1))`, name, key); err != nil {
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
		if err := s.EnsureNamespace(ctx, ns.Cache, name, nil, nil); err != nil {
			return 0, err
		}
		return s.CacheIncrBy(ctx, name, key, delta)
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
		"SELECT "+sc+".put_inline(p_key=>$1,p_data=>$2,p_size=>$3,p_sha256=>$4,p_etag=>$5,p_meta=>$6,p_ct=>$7,p_expires_at=>$8)",
		key, stored, int64(len(payload)), sumSHA[:], etag, []string{},
		"text/plain; charset=utf-8", keepExp).Scan(&outcome); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return next, nil
}

// CacheDBSize returns the number of live keys in a cache namespace.
func (s *Store) CacheDBSize(ctx context.Context, name string) (int64, error) {
	sc, err := nsSQLIdent(ns.Cache, name)
	if err != nil {
		return 0, err
	}
	var n int64
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM `+sc+`.objects
		WHERE expires_at IS NULL OR expires_at > now()`).Scan(&n)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return n, err
}

// CacheFlush drops and recreates a cache namespace. It can never touch an S3
// bucket or SQS queue because the kind is part of the schema name.
func (s *Store) CacheFlush(ctx context.Context, name string) error {
	if _, err := s.DropNamespace(ctx, ns.Cache, name, true); err != nil {
		return err
	}
	return s.EnsureNamespace(ctx, ns.Cache, name, nil, nil)
}
