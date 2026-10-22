package store

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CopyOptions controls a metadata-only CopyObject. The bytes are shared; the
// GC keeps a file while any object still references it.
type CopyOptions struct {
	ReplaceMetadata bool
	UserMetadata    []string
	ContentType     string
	SourceIfMatch   string
	SourceIfNone    string
	DestIfMatch     string
	DestIfNone      string
}

// Copy creates or replaces dst with src's bytes. No chunk rows move.
func (s *Store) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, opts CopyOptions) (*Meta, error) {
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
		FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE`, srcBucket, srcKey).
		Scan(&srcFile, &src.Size, &src.SHA256, &src.ETag, &src.UserMetadata, &src.ContentType,
			&src.Parts, &src.PartEnds, &srcEpoch, &srcInline, &srcHasInline)
	if errors.Is(err, pgx.ErrNoRows) {
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
	err = tx.QueryRow(ctx, `SELECT file_id, parts, etag FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE`,
		dstBucket, dstKey).Scan(&oldFile, &oldParts, &destETag)
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
	var srcFileVal any
	if srcFile.Valid {
		srcFileVal = srcFile.Int64
	}
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.object_part_checksums WHERE bucket = $1 AND key = $2", dstBucket, dstKey); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO s3p.objects
		(bucket, key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends, inline_data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		  sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
		  content_type = EXCLUDED.content_type, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends,
		  inline_data = EXCLUDED.inline_data, created_at = now()`,
		dstBucket, dstKey, srcFileVal, src.Size, src.SHA256, src.ETag, meta, ct, src.Parts, src.PartEnds, src.Inline)
	if err != nil {
		return nil, err
	}
	if srcBucket != dstBucket || srcKey != dstKey {
		if _, err := tx.Exec(ctx, `INSERT INTO s3p.object_part_checksums
			(bucket, key, part_no, file_id, algorithm, value)
			SELECT $3, $4, part_no, file_id, algorithm, value
			FROM s3p.object_part_checksums WHERE bucket = $1 AND key = $2
			ON CONFLICT (bucket, key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			srcBucket, srcKey, dstBucket, dstKey); err != nil {
			return nil, err
		}
	}
	if destExists && oldFile.Valid {
		if err := queueFiles(ctx, tx, oldFile.Int64, oldParts); err != nil {
			return nil, err
		}
	}
	if err := notifyTx(ctx, tx, dstBucket, dstKey); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	out := src
	out.UserMetadata, out.ContentType = meta, ct
	out.CreatedAt = time.Now()
	s.cache.Put(ctx, dstBucket, dstKey, out)
	s.invalidatePeers(ctx, dstBucket, dstKey)
	s.objects.invalidate(dstBucket, dstKey)
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
