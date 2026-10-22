package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CopyOptions controls a CopyObject. Same-bucket copies are metadata-only
// (bytes are shared; GC keeps a file while any object references it).
// Cross-bucket copies duplicate chunk rows inside PostgreSQL without streaming
// bytes through the gateway.
type CopyOptions struct {
	ReplaceMetadata bool
	UserMetadata    []string
	ContentType     string
	SourceIfMatch   string
	SourceIfNone    string
	DestIfMatch     string
	DestIfNone      string
}

// Copy creates or replaces dst with src's bytes.
func (s *Store) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, opts CopyOptions) (*Meta, error) {
	srcSchema, err := bucketSQLIdent(srcBucket)
	if err != nil {
		return nil, err
	}
	dstSchema, err := bucketSQLIdent(dstBucket)
	if err != nil {
		return nil, err
	}
	if _, ok := s.knownBuckets.Load(dstBucket); !ok {
		if err := s.CreateBucket(ctx, dstBucket); err != nil {
			return nil, err
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := lockTwo(ctx, tx, srcBucket, srcKey, dstBucket, dstKey); err != nil {
		return nil, err
	}
	var src Meta
	var srcEpoch float64
	var srcFile pgtype.Int8
	var srcInline []byte
	var srcHasInline bool
	err = tx.QueryRow(ctx, `SELECT file_id, size, sha256, etag, user_metadata, content_type,
		parts, part_ends, EXTRACT(EPOCH FROM created_at)::float8, inline_data, inline_data IS NOT NULL
		FROM `+srcSchema+`.objects WHERE key = $1 FOR UPDATE`, srcKey).
		Scan(&srcFile, &src.Size, &src.SHA256, &src.ETag, &src.UserMetadata, &src.ContentType,
			&src.Parts, &src.PartEnds, &srcEpoch, &srcInline, &srcHasInline)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if srcFile.Valid {
		src.FileID = srcFile.Int64
	}
	if srcHasInline {
		if srcInline == nil {
			srcInline = []byte{}
		}
		src.Inline = srcInline
	}
	if !condOK(opts.SourceIfMatch, opts.SourceIfNone, true, src.ETag) {
		return nil, ErrPrecondition
	}
	var oldFile pgtype.Int8
	var oldParts []int64
	var destETag string
	err = tx.QueryRow(ctx, `SELECT file_id, parts, etag FROM `+dstSchema+`.objects WHERE key = $1 FOR UPDATE`,
		dstKey).Scan(&oldFile, &oldParts, &destETag)
	if isMissingBucketSchema(err) {
		_ = tx.Rollback(ctx)
		if err := s.CreateBucket(ctx, dstBucket); err != nil {
			return nil, err
		}
		return s.Copy(ctx, srcBucket, srcKey, dstBucket, dstKey, opts)
	}
	destExists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if !condOK(opts.DestIfMatch, opts.DestIfNone, destExists, destETag) {
		return nil, ErrPrecondition
	}
	meta, ct := src.UserMetadata, src.ContentType
	if opts.ReplaceMetadata {
		meta, ct = opts.UserMetadata, opts.ContentType
	}
	dstFileID := src.FileID
	dstParts := src.Parts
	idMap := map[int64]int64{}
	if srcBucket != dstBucket && src.Inline == nil && srcFile.Valid {
		if len(src.Parts) == 0 {
			newID, err := s.nextFileID(ctx)
			if err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx,
				"INSERT INTO "+dstSchema+".chunks (file_id, no, data) SELECT $1, no, data FROM "+srcSchema+".chunks WHERE file_id = $2",
				newID, src.FileID); err != nil {
				return nil, err
			}
			dstFileID = newID
		} else {
			dstParts = make([]int64, len(src.Parts))
			for i, oldPartID := range src.Parts {
				newID, ok := idMap[oldPartID]
				if !ok {
					newID, err = s.nextFileID(ctx)
					if err != nil {
						return nil, err
					}
					if _, err := tx.Exec(ctx,
						"INSERT INTO "+dstSchema+".chunks (file_id, no, data) SELECT $1, no, data FROM "+srcSchema+".chunks WHERE file_id = $2",
						newID, oldPartID); err != nil {
						return nil, err
					}
					idMap[oldPartID] = newID
				}
				dstParts[i] = newID
			}
			dstFileID = dstParts[0]
		}
	}
	var dstFileVal any
	if srcFile.Valid {
		dstFileVal = dstFileID
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+dstSchema+".object_part_checksums WHERE key = $1", dstKey); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+dstSchema+`.objects
		(key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends, inline_data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		  sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
		  content_type = EXCLUDED.content_type, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends,
		  inline_data = EXCLUDED.inline_data, created_at = now()`,
		dstKey, dstFileVal, src.Size, src.SHA256, src.ETag, meta, ct, dstParts, src.PartEnds, src.Inline)
	if err != nil {
		return nil, err
	}
	if srcBucket == dstBucket && srcKey != dstKey {
		if _, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
			(key, part_no, file_id, algorithm, value)
			SELECT $2, part_no, file_id, algorithm, value
			FROM `+srcSchema+`.object_part_checksums WHERE key = $1
			ON CONFLICT (key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			srcKey, dstKey); err != nil {
			return nil, err
		}
	} else if srcBucket != dstBucket && len(dstParts) > 0 {
		rows, err := tx.Query(ctx,
			"SELECT part_no, file_id, algorithm, value FROM "+srcSchema+".object_part_checksums WHERE key = $1",
			srcKey)
		if err != nil {
			return nil, err
		}
		type checkRow struct {
			no       int32
			fileID   int64
			alg, val string
		}
		var checks []checkRow
		for rows.Next() {
			var c checkRow
			if err := rows.Scan(&c.no, &c.fileID, &c.alg, &c.val); err != nil {
				rows.Close()
				return nil, err
			}
			if mapped, ok := idMap[c.fileID]; ok {
				c.fileID = mapped
			}
			checks = append(checks, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		for _, c := range checks {
			if _, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
				(key, part_no, file_id, algorithm, value) VALUES ($1,$2,$3,$4,$5)`,
				dstKey, c.no, c.fileID, c.alg, c.val); err != nil {
				return nil, err
			}
		}
	}
	if destExists && oldFile.Valid {
		if err := queueFiles(ctx, tx, dstSchema, oldFile.Int64, oldParts); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	out := src
	out.FileID = dstFileID
	out.Parts = dstParts
	out.UserMetadata, out.ContentType = meta, ct
	out.CreatedAt = time.Now()
	return &out, nil
}

// lockTwo takes the two key locks in a deterministic order so concurrent
// renames/copies in opposite directions cannot deadlock.
func lockTwo(ctx context.Context, tx pgx.Tx, b1, k1, b2, k2 string) error {
	if b1+"\x00"+k1 > b2+"\x00"+k2 {
		b1, b2, k1, k2 = b2, b1, k2, k1
	}
	if err := lockObjectKey(ctx, tx, b1, k1); err != nil {
		return err
	}
	if b1 == b2 && k1 == k2 {
		return nil
	}
	return lockObjectKey(ctx, tx, b2, k2)
}

// condOK evaluates the destination/source conditional headers. ETag matching
// is intentionally opaque; only equality and "*" matter.
func condOK(ifMatch, ifNone string, exists bool, etag string) bool {
	if ifMatch != "" {
		if !exists || !etagListMatch(ifMatch, etag) {
			return false
		}
	}
	if ifNone != "" {
		if strings.TrimSpace(ifNone) == "*" && exists {
			return false
		}
		if exists && etagListMatch(ifNone, etag) {
			return false
		}
	}
	return true
}

func etagListMatch(header, etag string) bool {
	for _, want := range strings.Split(header, ",") {
		want = strings.Trim(strings.TrimPrefix(strings.TrimSpace(want), "W/"), `"`)
		if want == etag {
			return true
		}
	}
	return false
}
