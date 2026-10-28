package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/work"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

var ErrNotInteger = errors.New("value is not an integer or out of range")

const (
	cacheContentType        = "application/octet-stream"
	cacheCounterContentType = "text/plain; charset=utf-8"
)

func IsDeletedOrigin(version string) bool {
	return ParseCacheVersion(version).IsDeleted()
}

func (s *Store) CacheTombstone(ctx context.Context, ref ns.Ref, key string, expiresAt time.Time) error {
	_, err := s.PutInline(ctx, ObjectRef{NS: ref, Key: key}, []byte{}, WriteOptions{
		ContentType: "application/octet-stream",
		ExpiresAt:   &expiresAt,
		OriginETag:  DeletedCacheVersion(NewDeletedCacheToken()).String(),
		SkipLock:    ref.Kind == ns.WriteBack,
	})
	return err
}

const (
	pendingOriginPrefix = "pending:"
	deletedOriginPrefix = "deleted:"
)

type CacheState uint8

const (
	CacheLive CacheState = iota
	CachePending
	CacheDeleted
)

type CacheVersion struct {
	State  CacheState
	Origin string
	Token  string
}

func ParseCacheVersion(raw string) CacheVersion {
	if token, ok := strings.CutPrefix(raw, pendingOriginPrefix); ok {
		return CacheVersion{State: CachePending, Token: token}
	}
	if token, ok := strings.CutPrefix(raw, deletedOriginPrefix); ok {
		return CacheVersion{State: CacheDeleted, Token: token}
	}
	return CacheVersion{State: CacheLive, Origin: raw}
}

func LiveCacheVersion(origin string) CacheVersion {
	return CacheVersion{State: CacheLive, Origin: origin}
}

func PendingCacheVersion(token string) CacheVersion {
	return CacheVersion{State: CachePending, Token: token}
}

func DeletedCacheVersion(token string) CacheVersion {
	return CacheVersion{State: CacheDeleted, Token: token}
}

func (v CacheVersion) String() string {
	switch v.State {
	case CachePending:
		return pendingOriginPrefix + v.Token
	case CacheDeleted:
		return deletedOriginPrefix + v.Token
	default:
		return v.Origin
	}
}

func (v CacheVersion) IsLive() bool { return v.State == CacheLive }

func (v CacheVersion) IsPending() bool { return v.State == CachePending }

func (v CacheVersion) IsDeleted() bool { return v.State == CacheDeleted }

func NewDeletedCacheToken() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}

func (s *Store) CacheFlushAll(ctx context.Context) (int64, error) {
	infos, err := s.Namespaces(ctx, ns.Cache)
	if err != nil {
		return 0, err
	}
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}
	if len(names) > 0 {
		var pending int64
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM s3p.pending_uploads WHERE namespace = ANY($1::text[])`, names).Scan(&pending); err != nil {
			return 0, err
		}
		if pending > 0 {
			return 0, ErrPendingUploads
		}
	}
	var n int64
	for _, info := range infos {
		if err := s.CacheFlush(ctx, info.Name); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

type CacheSetOptions struct {
	Precondition Precondition
	ExpiresAt    *time.Time
	KeepTTL      bool
	ReturnOld    bool
}

func (s *Store) CacheGet(ctx context.Context, name, key string) ([]byte, bool, error) {
	return withSchema2(ctx, s, ns.CacheRef(name), nil, nil, func(sc string) ([]byte, bool, error) {
		var size int64
		var inline []byte
		var hasInline bool
		err := s.pool.QueryRow(ctx, `SELECT size, inline_data, inline_data IS NOT NULL
			FROM `+sc+`.objects WHERE key = $1
			  AND (expires_at IS NULL OR expires_at > now())
			  AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')`, key).
			Scan(&size, &inline, &hasInline)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if !hasInline {
			sl, err := s.Get(ctx, ObjectRef{NS: ns.CacheRef(name), Key: key}, WholeObjectRange())
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
	})
}

func (s *Store) writeCacheValue(ctx context.Context, ref ObjectRef, val []byte, opts WriteOptions) (bool, error) {
	if opts.ContentType == "" {
		opts.ContentType = cacheContentType
	}
	if int64(len(val)) <= int64(ref.NS.Kind.InlineMax()) {
		_, err := s.PutInline(ctx, ref, val, opts)
		if errors.Is(err, ErrPrecondition) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
	opts.ExpectedSize = int64(len(val))
	w, err := s.StartObject(ctx, ref, opts)
	if err != nil {
		return false, err
	}
	defer w.Abort()
	for rest := val; len(rest) > 0; {
		n := min(len(rest), 4<<20)
		if _, err := w.Write(rest[:n]); err != nil {
			return false, err
		}
		rest = rest[n:]
	}
	_, err = w.Finish(nil)
	if errors.Is(err, ErrPrecondition) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) CacheSet(ctx context.Context, name, key string, val []byte, opts CacheSetOptions) ([]byte, bool, bool, error) {
	if opts.ReturnOld {
		return s.CacheGetSet(ctx, name, key, val, opts)
	}
	wrote, err := s.writeCacheValue(ctx, ObjectRef{NS: ns.CacheRef(name), Key: key}, val, WriteOptions{
		Precondition: opts.Precondition,
		ExpiresAt:    opts.ExpiresAt,
		KeepTTL:      opts.KeepTTL,
	})
	if err != nil {
		return nil, false, false, err
	}
	return nil, false, wrote, nil
}

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
		WHERE (o.expires_at IS NULL OR o.expires_at > now())
		  AND (o.origin_etag IS NULL OR o.origin_etag NOT LIKE 'deleted:%')`, keys)
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

func (s *Store) CacheDel(ctx context.Context, name string, keys []string) (int64, error) {
	return s.DeleteMany(ctx, ns.CacheRef(name), keys)
}

func (s *Store) CacheDelExpected(ctx context.Context, name, key, originETag string) (int64, error) {
	return s.DeleteExpected(ctx, ObjectRef{NS: ns.CacheRef(name), Key: key}, originETag)
}

func (s *Store) DeleteExpected(ctx context.Context, ref ObjectRef, originETag string) (int64, error) {
	return withSchema(ctx, s, ref.NS, ErrNoBucket, nil, func(sc string) (int64, error) {
		deleted, err := withTx(ctx, s, func(tx pgx.Tx) (int64, error) {
			schema := strings.Trim(sc, `"`)
			if err := lockObjectKey(ctx, tx, schema, ref.Key); err != nil {
				return 0, err
			}
			var fileID pgtype.Int8
			var parts []int64
			err := tx.QueryRow(ctx, `DELETE FROM `+sc+`.objects
				WHERE key = $1 AND origin_etag = $2
				RETURNING file_id, parts`, ref.Key, originETag).Scan(&fileID, &parts)
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, nil
			}
			if err != nil {
				return 0, err
			}
			if fileID.Valid {
				if err := queueFiles(ctx, tx, sc, fileID.Int64, parts); err != nil {
					return 0, err
				}
			}
			return 1, nil
		})
		return deleted, err
	})
}

func (s *Store) CacheGetDel(ctx context.Context, name, key string) ([]byte, bool, error) {
	schema, err := ns.Cache.Schema(name)
	if err != nil {
		return nil, false, err
	}
	sc := quoteIdent(schema)
	type value struct {
		bytes []byte
		live  bool
	}
	result, err := withTx(ctx, s, func(tx pgx.Tx) (value, error) {
		if err := lockObjectKey(ctx, tx, schema, key); err != nil {
			return value{}, err
		}
		m, err := scanMetadata(tx.QueryRow(ctx, `DELETE FROM `+sc+`.objects WHERE key = $1
			AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')
			RETURNING `+metadataColumns(metadataDecoded), key), metadataDecoded)
		if err != nil {
			return value{}, err
		}
		result := value{live: m.Live(time.Now())}
		if result.live {

			_, body, err := s.bodyFromMeta(ctx, ObjectRef{NS: ns.CacheRef(name), Key: key}, m, WholeObjectRange())
			if err != nil {
				return value{}, err
			}
			defer body.Close()
			result.bytes, err = io.ReadAll(body)
			if err != nil {
				return value{}, err
			}
		}
		if m.FileID != 0 {
			if err := queueFiles(ctx, tx, sc, m.FileID, m.Parts); err != nil {
				return value{}, err
			}
		}
		return result, nil
	})
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNoBucket) {
		return nil, false, nil
	}
	return result.bytes, result.live, err
}

func (s *Store) CacheExists(ctx context.Context, name string, keys []string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return withSchema(ctx, s, ns.CacheRef(name), nil, nil, func(sc string) (int64, error) {
		var count int64
		err := s.pool.QueryRow(ctx, `SELECT count(*)
			FROM unnest($1::text[]) AS k(key)
			JOIN `+sc+`.objects o ON o.key = k.key
			WHERE (o.expires_at IS NULL OR o.expires_at > now())
			  AND (o.origin_etag IS NULL OR o.origin_etag NOT LIKE 'deleted:%')`, keys).Scan(&count)
		return count, err
	})
}

func (s *Store) CacheExpire(ctx context.Context, name, key string, expiresAt *time.Time, opts ...string) (bool, error) {
	return withSchema(ctx, s, ns.CacheRef(name), nil, nil, func(sc string) (bool, error) {
		var condSQL strings.Builder
		if expiresAt == nil {
			condSQL.WriteString(" AND expires_at IS NOT NULL")
		} else {
			for _, opt := range opts {
				switch strings.ToUpper(opt) {
				case "NX":
					condSQL.WriteString(" AND expires_at IS NULL")
				case "XX":
					condSQL.WriteString(" AND expires_at IS NOT NULL")
				case "GT":
					condSQL.WriteString(" AND expires_at IS NOT NULL AND $2 > expires_at")
				case "LT":
					condSQL.WriteString(" AND (expires_at IS NULL OR $2 < expires_at)")
				}
			}
		}
		tag, err := s.pool.Exec(ctx, `UPDATE `+sc+`.objects SET expires_at = $2
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())
			  AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')`+condSQL.String(), key, expiresAt)
		return tag.RowsAffected() > 0, err
	})
}

func (s *Store) CacheTTL(ctx context.Context, name, key string) (time.Duration, bool, bool, error) {
	return withSchema3(ctx, s, ns.CacheRef(name), nil, nil, func(sc string) (time.Duration, bool, bool, error) {
		var remSec pgtype.Float8
		err := s.pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (expires_at - now()))::float8
			FROM `+sc+`.objects
			WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())
			  AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')`, key).Scan(&remSec)
		if errors.Is(err, pgx.ErrNoRows) {
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
	})
}

func cacheAdd(current []byte, delta int64) (int64, error) {
	cur, err := strconv.ParseInt(strings.TrimSpace(string(current)), 10, 64)
	if err != nil {
		return 0, ErrNotInteger
	}
	if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
		return 0, ErrNotInteger
	}
	return cur + delta, nil
}

func (s *Store) CacheDBSize(ctx context.Context, name string) (int64, error) {
	return withSchema(ctx, s, ns.CacheRef(name), nil, nil, func(sc string) (int64, error) {
		var n int64
		err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+sc+`.objects
			WHERE (expires_at IS NULL OR expires_at > now())
			  AND (origin_etag IS NULL OR origin_etag NOT LIKE 'deleted:%')`).Scan(&n)
		return n, err
	})
}

var ErrPendingUploads = errors.New("cache namespace has pending origin uploads")

func (s *Store) CacheFlush(ctx context.Context, name string) error {
	var allTables []string
	for _, kind := range []ns.Kind{ns.Cache, ns.WriteBack} {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return err
		}
		schema, err := nsSQLIdent(kind, name)
		if err != nil {
			return err
		}
		for _, table := range []string{"objects", "origin_metadata", "chunks", "uploads", "upload_parts", "object_part_checksums", "garbage"} {
			allTables = append(allTables, schema+"."+table)
		}
	}
	_, err := withTx(ctx, s, func(tx pgx.Tx) (struct{}, error) {
		if _, err := tx.Exec(ctx, "LOCK TABLE "+strings.Join(allTables, ", ")+" IN ACCESS EXCLUSIVE MODE"); err != nil {
			return struct{}{}, err
		}
		var pending int64
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM s3p.pending_uploads WHERE namespace = $1`, name).Scan(&pending); err != nil {
			return struct{}{}, err
		}
		if pending > 0 {
			return struct{}{}, ErrPendingUploads
		}
		if _, err := tx.Exec(ctx, "TRUNCATE "+strings.Join(allTables, ", ")); err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

const cacheMutationRetries = 32

var ErrCacheContention = errors.New("cache mutation contention")

type cacheMutation struct {
	WriteOptions
	inlineOnly bool
	retries    int
	apply      func([]byte) ([]byte, error)
}

type cacheMutationResult struct {
	previous []byte
	existed  bool
	wrote    bool
	size     int64
}

func (s *Store) mutateCache(ctx context.Context, name, key string, change cacheMutation) (cacheMutationResult, error) {
	ref := ObjectRef{NS: ns.CacheRef(name), Key: key}
	for attempt := 0; change.retries == 0 || attempt < change.retries; attempt++ {
		value, m, err := s.cacheValueWithMeta(ctx, ref, change.inlineOnly)
		if err != nil {
			return cacheMutationResult{}, err
		}
		result := cacheMutationResult{previous: value, existed: m != nil}
		opts := change.WriteOptions
		opts.Precondition = Precondition{Mode: IfAbsent}
		if m != nil {
			opts.Precondition = Precondition{Mode: IfMatch, ETag: m.ETag}
		}
		if !change.Precondition.Allows(result.existed, opts.Precondition.ETag) {
			return result, nil
		}
		next, err := change.apply(value)
		if err != nil {
			return cacheMutationResult{}, err
		}
		result.wrote, err = s.writeCacheValue(ctx, ref, next, opts)
		if err != nil {
			return cacheMutationResult{}, err
		}
		if result.wrote {
			result.size = int64(len(next))
			return result, nil
		}
	}
	return cacheMutationResult{}, ErrCacheContention
}

func (s *Store) cacheValueWithMeta(ctx context.Context, ref ObjectRef, inlineOnly bool) ([]byte, *Meta, error) {
	m, err := s.Meta(ctx, ref)
	if errors.Is(err, ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if !m.Live(time.Now()) {
		return nil, nil, nil
	}
	if m.Inline != nil {
		return m.Inline, m, nil
	}
	if inlineOnly {
		return nil, nil, ErrNotInteger
	}
	_, body, err := s.GetBodyFromMeta(ctx, ref, m, WholeObjectRange())
	if errors.Is(err, ErrNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	defer body.Close()
	value, err := io.ReadAll(body)
	return value, m, err
}

func (s *Store) CacheGetSet(ctx context.Context, name, key string, val []byte, opts CacheSetOptions) ([]byte, bool, bool, error) {
	result, err := s.mutateCache(ctx, name, key, cacheMutation{
		Precondition: opts.Precondition, ExpiresAt: opts.ExpiresAt, KeepTTL: opts.KeepTTL,
		retries: cacheMutationRetries,
		apply:   func([]byte) ([]byte, error) { return val, nil },
	})
	return result.previous, result.existed, result.wrote, err
}

func (s *Store) CacheAppend(ctx context.Context, name, key string, suffix []byte) (int64, error) {
	result, err := s.mutateCache(ctx, name, key, cacheMutation{
		KeepTTL: true, retries: cacheMutationRetries,
		apply: func(current []byte) ([]byte, error) { return append(current, suffix...), nil },
	})
	return result.size, err
}

func (s *Store) CacheSetRange(ctx context.Context, name, key string, off int64, val []byte) (int64, error) {
	if off < 0 || off > int64(math.MaxInt)-int64(len(val)) {
		return 0, errors.New("offset is out of range")
	}
	result, err := s.mutateCache(ctx, name, key, cacheMutation{
		KeepTTL: true, retries: cacheMutationRetries,
		apply: func(current []byte) ([]byte, error) {
			if need := off + int64(len(val)); int64(len(current)) < need {
				current = append(current, make([]byte, need-int64(len(current)))...)
			}
			copy(current[off:], val)
			return current, nil
		},
	})
	return result.size, err
}

func (s *Store) CacheIncrBy(ctx context.Context, name, key string, delta int64) (int64, error) {
	var next int64
	_, err := s.mutateCache(ctx, name, key, cacheMutation{
		KeepTTL: true, ContentType: cacheCounterContentType, inlineOnly: true,
		apply: func(current []byte) ([]byte, error) {
			next = delta
			var err error
			if current != nil {
				next, err = cacheAdd(current, delta)
			}
			return []byte(strconv.FormatInt(next, 10)), err
		},
	})
	if err != nil {
		return 0, err
	}
	return next, err
}

func (s *Store) CacheMSet(ctx context.Context, name string, keys []string, vals [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	inlineMax := int64(ns.Cache.InlineMax())
	for _, v := range vals {
		if int64(len(v)) > inlineMax {
			ref := ObjectRef{NS: ns.CacheRef(name)}
			for i := range keys {
				ref.Key = keys[i]
				if _, err := s.writeCacheValue(ctx, ref, vals[i], WriteOptions{}); err != nil {
					return err
				}
			}
			return nil
		}
	}
	var add int64
	for _, v := range vals {
		add += int64(len(v))
	}
	if s.checkQuota(ctx, ns.Cache, name, add) {
		return ErrQuotaExceeded
	}
	batch := encodeCacheBatch(keys, vals)
	_, err := withAutoCreate(ctx, s, ns.Cache, name, func(sc string) (struct{}, error) {
		return struct{}{}, s.cacheMSet(ctx, sc, batch)
	})
	return err
}

type cacheStoredValue struct {
	size   int64
	sha    []byte
	etag   string
	stored []byte
}

func encodeCacheValue(v []byte) cacheStoredValue {
	sumMD5 := md5.Sum(v)
	sumSHA := sha256.Sum256(v)
	return cacheStoredValue{
		size:   int64(len(v)),
		sha:    sumSHA[:],
		etag:   hex.EncodeToString(sumMD5[:]),
		stored: compressBlock(nil, v),
	}
}

type cacheBatch struct {
	keys   []string
	sizes  []int64
	shas   [][]byte
	etags  []string
	stored [][]byte
}

func encodeCacheBatch(keys []string, vals [][]byte) cacheBatch {
	batch := cacheBatch{
		keys:   make([]string, 0, len(keys)),
		sizes:  make([]int64, 0, len(keys)),
		shas:   make([][]byte, 0, len(keys)),
		etags:  make([]string, 0, len(keys)),
		stored: make([][]byte, 0, len(keys)),
	}
	seen := make(map[string]int, len(keys))
	for i, k := range keys {
		v := encodeCacheValue(vals[i])
		if idx, ok := seen[k]; ok {
			batch.sizes[idx] = v.size
			batch.shas[idx] = v.sha
			batch.etags[idx] = v.etag
			batch.stored[idx] = v.stored
			continue
		}
		seen[k] = len(batch.keys)
		batch.keys = append(batch.keys, k)
		batch.sizes = append(batch.sizes, v.size)
		batch.shas = append(batch.shas, v.sha)
		batch.etags = append(batch.etags, v.etag)
		batch.stored = append(batch.stored, v.stored)
	}
	return batch
}

func (s *Store) cacheMSet(ctx context.Context, sc string, batch cacheBatch) error {
	schema := strings.Trim(sc, `"`)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || E'\x1f' || k, 0))
		FROM (SELECT DISTINCT unnest($2::text[]) AS k ORDER BY 1) s`, schema, batch.keys); err != nil {
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
		  content_type = 'application/octet-stream', content_encoding = '',
		  origin_modified = NULL, checksum_name = NULL, checksum_value = NULL,
		  checksum_type = NULL, expires_at = NULL,
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
	ON CONFLICT DO NOTHING`, batch.keys, batch.sizes, batch.shas, batch.etags, batch.stored); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func IsPendingOrigin(version string) bool {
	return ParseCacheVersion(version).IsPending()
}

func PendingOriginToken(uploadID string) string {
	return PendingCacheVersion(uploadID).String()
}

func (s *Store) LockCacheKey(ctx context.Context, kind ns.Kind, namespace, key string) (func(), error) {
	schema, err := kind.Schema(namespace)
	if err != nil {
		return nil, err
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	lockKey := schema + "\x1f" + key
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", lockKey); err != nil {
		conn.Release()
		return nil, err
	}
	release := func() {
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, unlockErr := conn.Exec(uctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", lockKey)
		cancel()
		if unlockErr != nil {

			_ = conn.Conn().Close(context.Background())
		}
		conn.Release()
	}
	return release, nil
}

type PendingUploadInfo struct {
	Namespace   string     `json:"namespace"`
	Key         string     `json:"key"`
	Token       string     `json:"token"`
	Size        int64      `json:"size"`
	Attempts    int        `json:"attempts"`
	NextAttempt time.Time  `json:"next_attempt"`
	LastError   string     `json:"last_error,omitempty"`
	BaseETag    string     `json:"base_etag,omitempty"`
	BaseMissing bool       `json:"base_missing"`
	LeaseOwner  string     `json:"lease_owner,omitempty"`
	LeaseUntil  *time.Time `json:"lease_until,omitempty"`
	Dead        bool       `json:"dead"`
	CreatedAt   time.Time  `json:"created_at"`
}

func (s *Store) ListPendingUploads(ctx context.Context, namespace string) ([]PendingUploadInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT namespace, key, token, size, attempts,
		next_attempt, coalesce(last_error, ''), coalesce(base_etag, ''), base_missing,
		coalesce(lease_owner, ''), lease_until, dead, created_at
		FROM s3p.pending_uploads
		WHERE ($1 = '' OR namespace = $1)
		ORDER BY dead DESC, next_attempt, namespace, key`, namespace)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (PendingUploadInfo, error) {
		var p PendingUploadInfo
		var lease pgtype.Timestamptz
		err := row.Scan(&p.Namespace, &p.Key, &p.Token, &p.Size, &p.Attempts,
			&p.NextAttempt, &p.LastError, &p.BaseETag, &p.BaseMissing,
			&p.LeaseOwner, &lease, &p.Dead, &p.CreatedAt)
		if lease.Valid {
			t := lease.Time
			p.LeaseUntil = &t
		}
		return p, err
	})
}

type PendingUpload struct {
	Key         string
	Token       string
	Size        int64
	Attempts    int
	LastError   string
	BaseETag    string
	BaseMissing bool
	LeaseOwner  string
}

func (s *Store) ClaimPendingUploads(ctx context.Context, namespace, owner string, limit int, lease time.Duration) ([]PendingUpload, error) {
	rows, err := s.pool.Query(ctx, `WITH due AS (
			SELECT namespace, key
			FROM s3p.pending_uploads
			WHERE namespace = $1 AND next_attempt <= now() AND NOT dead
			  AND (lease_until IS NULL OR lease_until <= now())
			ORDER BY next_attempt
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE s3p.pending_uploads p
		SET lease_owner = $3, lease_until = now() + make_interval(secs => $4)
		FROM due
		WHERE p.namespace = due.namespace AND p.key = due.key
		RETURNING p.key, p.token, p.size, p.attempts, coalesce(p.last_error, ''),
		          coalesce(p.base_etag, ''), p.base_missing, coalesce(p.lease_owner, '')`,
		namespace, limit, owner, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (PendingUpload, error) {
		var p PendingUpload
		err := row.Scan(&p.Key, &p.Token, &p.Size, &p.Attempts, &p.LastError,
			&p.BaseETag, &p.BaseMissing, &p.LeaseOwner)
		return p, err
	})
}

func (s *Store) MarkOriginCommitted(ctx context.Context, namespace, key, token, version string, originModified time.Time, expiresAt *time.Time) (bool, error) {
	sc, err := ns.WriteBack.Schema(namespace)
	if err != nil {
		return false, err
	}
	var expiry, modified any
	if expiresAt != nil {
		expiry = *expiresAt
	}
	if !originModified.IsZero() {
		modified = originModified
	}
	return withTx(ctx, s, func(tx pgx.Tx) (bool, error) {
		tag, err := tx.Exec(ctx,
			`UPDATE `+quoteIdent(sc)+`.objects SET origin_etag = $3, created_at = now(),
			   origin_modified = COALESCE($5, origin_modified), expires_at = $4
			 WHERE key = $1 AND origin_etag = $2`, key, token, version, expiry, modified)
		if err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM s3p.pending_uploads
			 WHERE namespace = $1 AND key = $2 AND token = $3`, namespace, key, token); err != nil {
			return false, err
		}
		return tag.RowsAffected() == 1, nil
	})
}

func (s *Store) AbandonPendingUpload(ctx context.Context, namespace, key, token string) error {
	sc, err := ns.WriteBack.Schema(namespace)
	if err != nil {
		return err
	}
	_, err = withTx(ctx, s, func(tx pgx.Tx) (struct{}, error) {
		var fileID pgtype.Int8
		var parts []int64
		err := tx.QueryRow(ctx,
			`DELETE FROM `+quoteIdent(sc)+`.objects
			 WHERE key = $1 AND origin_etag = $2
			 RETURNING file_id, parts`,
			key, token).Scan(&fileID, &parts)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return struct{}{}, err
		}
		if err == nil && fileID.Valid {
			if err := queueFiles(ctx, tx, quoteIdent(sc), fileID.Int64, parts); err != nil {
				return struct{}{}, err
			}
		}
		_, err = tx.Exec(ctx,
			`DELETE FROM s3p.pending_uploads
			 WHERE namespace = $1 AND key = $2 AND token = $3`, namespace, key, token)
		return struct{}{}, err
	})
	return err
}

func (s *Store) FailPendingUpload(ctx context.Context, namespace, key, token, owner string, attempt int, cause error) error {
	return s.failPending(ctx, namespace, key, token, owner, attempt, cause, false)
}

func (s *Store) FailPendingUploadPermanent(ctx context.Context, namespace, key, token, owner string, attempt int, cause error) error {
	return s.failPending(ctx, namespace, key, token, owner, attempt, cause, true)
}

func (s *Store) failPending(ctx context.Context, namespace, key, token, owner string, attempt int, cause error, permanent bool) error {
	delay := work.Backoff{
		Initial: 2 * time.Second,
		Factor:  2,
		Max:     300 * time.Second,
	}.Delay(attempt + 1)
	_, err := s.pool.Exec(ctx, `UPDATE s3p.pending_uploads
		SET attempts = attempts + 1,
		    dead = $7,
		    last_error = $4,
		    lease_owner = NULL,
		    lease_until = NULL,
		    next_attempt = now() + make_interval(secs => $6)
		WHERE namespace = $1 AND key = $2 AND token = $3 AND lease_owner = $5`,
		namespace, key, token, errorText(cause), owner, delay.Seconds(), permanent)
	return err
}

func (s *Store) RetryDeadPendingUploads(ctx context.Context, namespace string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE s3p.pending_uploads
		SET dead = false, attempts = 0, last_error = NULL,
		    lease_owner = NULL, lease_until = NULL, next_attempt = now()
		WHERE dead AND ($1 = '' OR namespace = $1)`, namespace)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) ClearPendingUploads(ctx context.Context, namespace, key string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2`, namespace, key)
	return err
}

func (s *Store) ClearPendingUploadBefore(ctx context.Context, namespace, key string, before time.Time) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.pending_uploads
		 WHERE namespace = $1 AND key = $2 AND created_at < $3`, namespace, key, before)
	return err
}

func (s *Store) ClearPendingUploadToken(ctx context.Context, namespace, key, token string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2 AND token = $3`,
		namespace, key, token)
	return err
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	if len(err.Error()) > 1024 {
		return err.Error()[:1024]
	}
	return err.Error()
}

var ErrFillLeaseBusy = errors.New("origin fill already in progress")

func (s *Store) AcquireFillLease(ctx context.Context, namespace, key, owner string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO s3p.cache_fill_leases (namespace, key, owner, expires_at)
		VALUES ($1, $2, $3, now() + make_interval(secs => $4))
		ON CONFLICT (namespace, key) DO UPDATE
		  SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
		  WHERE s3p.cache_fill_leases.expires_at <= now()`,
		namespace, key, owner, ttl.Seconds())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) ReleaseFillLease(ctx context.Context, namespace, key, owner string) {
	_, _ = s.pool.Exec(ctx,
		`DELETE FROM s3p.cache_fill_leases WHERE namespace = $1 AND key = $2 AND owner = $3`,
		namespace, key, owner)
}

func (s *Store) RenewFillLease(ctx context.Context, lease FillLease, ttl time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE s3p.cache_fill_leases
		SET expires_at = clock_timestamp() + make_interval(secs => $4)
		WHERE namespace = $1 AND key = $2 AND owner = $3 AND expires_at > clock_timestamp()`,
		lease.Namespace, lease.Key, lease.Owner, ttl.Seconds())
	return tag.RowsAffected() == 1, err
}

func (s *Store) CleanupFillLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM s3p.cache_fill_leases WHERE expires_at <= now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) OriginMetadata(ctx context.Context, namespace, key string, maxAge time.Duration) (*ObjectInfo, error) {
	return withSchema(ctx, s, ns.CacheRef(namespace), nil, nil, func(sc string) (*ObjectInfo, error) {
		var raw []byte
		err := s.pool.QueryRow(ctx, `SELECT info FROM `+sc+`.origin_metadata
			WHERE key = $1 AND expires_at > now()
			AND validated_at > now() - make_interval(secs => $2)`, key, maxAge.Seconds()).Scan(&raw)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		var info ObjectInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return nil, err
		}
		return &info, nil
	})
}

func (s *Store) RememberOriginMetadata(ctx context.Context, namespace, key string, info ObjectInfo, validated time.Time, ttl time.Duration) error {
	ref := ObjectRef{NS: ns.CacheRef(namespace), Key: key}
	sc, err := ref.Schema()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(info)
	if err != nil {
		return err
	}
	schema, _ := ref.NS.Schema()
	_, err = withTx(ctx, s, func(tx pgx.Tx) (struct{}, error) {
		if err := lockObjectKey(ctx, tx, schema, key); err != nil {
			return struct{}{}, err
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+sc+`.origin_metadata (key, info, validated_at, expires_at)
			SELECT $1, $2, clock_timestamp()-make_interval(secs => $3),
			  clock_timestamp()+make_interval(secs => $4-$3) WHERE $4 > $3 AND NOT EXISTS (
			  SELECT 1 FROM `+sc+`.objects WHERE key = $1 AND (expires_at IS NULL OR expires_at > now()))
			ON CONFLICT (key) DO UPDATE SET info = EXCLUDED.info,
			validated_at = EXCLUDED.validated_at, expires_at = EXCLUDED.expires_at`, key, raw, time.Since(validated).Seconds(), ttl.Seconds())
		return struct{}{}, err
	})
	return err
}

func (s *Store) ForgetOriginMetadata(ctx context.Context, namespace, key, version string) error {
	sc, err := nsSQLIdent(ns.Cache, namespace)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM `+sc+`.origin_metadata WHERE key = $1 AND info->>'OriginETag' = $2`, key, version)
	return err
}

func (s *Store) RetireOriginObject(ctx context.Context, ref ObjectRef, observed *Meta) (bool, error) {
	if ref.NS.Kind != ns.WriteBack || observed == nil || !ParseCacheVersion(observed.OriginETag).IsLive() {
		return false, ErrPrecondition
	}
	write, err := ref.Schema()
	if err != nil {
		return false, err
	}
	if err := s.EnsureNamespace(ctx, ns.Cache, ref.NS.Name, nil, nil); err != nil {
		return false, err
	}
	read, _ := nsSQLIdent(ns.Cache, ref.NS.Name)
	writeSchema, _ := ref.NS.Schema()
	readSchema, _ := ns.Cache.Schema(ref.NS.Name)
	return withTx(ctx, s, func(tx pgx.Tx) (bool, error) {

		if err := lockObjectKey(ctx, tx, writeSchema, ref.Key); err != nil {
			return false, err
		}
		var file pgtype.Int8
		var parts []int64
		err := tx.QueryRow(ctx, `DELETE FROM `+write+`.objects WHERE key=$1
			AND COALESCE(origin_etag,'')=$2 AND etag=$3 AND created_at=$4
			AND COALESCE(file_id,0)=$5 RETURNING file_id, parts`,
			ref.Key, observed.OriginETag, observed.ETag, observed.CreatedAt, observed.FileID).Scan(&file, &parts)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if file.Valid {
			if err := queueFiles(ctx, tx, write, file.Int64, parts); err != nil {
				return false, err
			}
		}
		if err := lockObjectKey(ctx, tx, readSchema, ref.Key); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM s3p.cache_fill_leases WHERE namespace=$1 AND key=$2`, ref.NS.Name, ref.Key); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM `+read+`.origin_metadata WHERE key=$1`, ref.Key); err != nil {
			return false, err
		}
		err = tx.QueryRow(ctx, `DELETE FROM `+read+`.objects WHERE key=$1 RETURNING file_id, parts`, ref.Key).Scan(&file, &parts)
		if errors.Is(err, pgx.ErrNoRows) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if file.Valid {
			if err := queueFiles(ctx, tx, read, file.Int64, parts); err != nil {
				return false, err
			}
		}
		return true, nil
	})
}
