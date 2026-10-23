package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
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
	if !s.known(ns.Bucket, dstBucket) {
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
	src, srcFile, err := readCopySource(ctx, tx, srcSchema, srcBucket, srcKey)
	if err != nil {
		return nil, err
	}
	if !condOK(opts.SourceIfMatch, opts.SourceIfNone, true, src.ETag) {
		return nil, ErrPrecondition
	}
	if s.checkQuota(ctx, ns.Bucket, dstBucket, src.Size) {
		return nil, ErrQuotaExceeded
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
	dstFileID, dstParts, idMap, err := s.prepareDestFiles(ctx, tx, srcSchema, dstSchema, srcBucket == dstBucket, src, srcFile)
	if err != nil {
		return nil, err
	}
	var dstFileVal any
	if srcFile.Valid {
		dstFileVal = dstFileID
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+dstSchema+".object_part_checksums WHERE key = $1", dstKey); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.objects
		(key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends, inline_data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		  sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
		  content_type = EXCLUDED.content_type, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends,
		  inline_data = EXCLUDED.inline_data, created_at = now()`,
		dstKey, dstFileVal, src.Size, src.SHA256, src.ETag, meta, ct, dstParts, src.PartEnds, src.Inline); err != nil {
		return nil, err
	}
	if err := copyPartChecksums(ctx, tx, srcSchema, srcKey, dstSchema, dstKey, srcBucket == dstBucket, len(dstParts) > 0, idMap); err != nil {
		return nil, err
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

func readCopySource(ctx context.Context, tx pgx.Tx, srcSchema, srcBucket, srcKey string) (Meta, pgtype.Int8, error) {
	var (
		src          Meta
		srcEpoch     float64
		srcFile      pgtype.Int8
		srcInline    []byte
		srcHasInline bool
	)
	err := tx.QueryRow(ctx, `SELECT file_id, size, sha256, etag, user_metadata, content_type,
		parts, part_ends, EXTRACT(EPOCH FROM created_at)::float8, inline_data, inline_data IS NOT NULL
		FROM `+srcSchema+`.objects
		WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())`,
		srcKey).
		Scan(&srcFile, &src.Size, &src.SHA256, &src.ETag, &src.UserMetadata, &src.ContentType,
			&src.Parts, &src.PartEnds, &srcEpoch, &srcInline, &srcHasInline)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return Meta{}, pgtype.Int8{}, ErrNotFound
	}
	if err != nil {
		return Meta{}, pgtype.Int8{}, err
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
	return src, srcFile, nil
}

func (s *Store) prepareDestFiles(ctx context.Context, tx pgx.Tx, srcSchema, dstSchema string, sameBucket bool, src Meta, srcFile pgtype.Int8) (int64, []int64, map[int64]int64, error) {
	if sameBucket || src.Inline != nil || !srcFile.Valid {
		return src.FileID, src.Parts, nil, nil
	}
	idMap := map[int64]int64{}
	dupOne := func(oldID int64) (int64, error) {
		if newID, ok := idMap[oldID]; ok {
			return newID, nil
		}
		newID, err := s.nextFileID(ctx)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO "+dstSchema+".chunks (file_id, no, data) SELECT $1, no, data FROM "+srcSchema+".chunks WHERE file_id = $2",
			newID, oldID); err != nil {
			return 0, err
		}
		idMap[oldID] = newID
		return newID, nil
	}
	if len(src.Parts) == 0 {
		newID, err := dupOne(src.FileID)
		return newID, nil, idMap, err
	}
	dstParts := make([]int64, len(src.Parts))
	for i, oldPartID := range src.Parts {
		newID, err := dupOne(oldPartID)
		if err != nil {
			return 0, nil, nil, err
		}
		dstParts[i] = newID
	}
	return dstParts[0], dstParts, idMap, nil
}

func copyPartChecksums(ctx context.Context, tx pgx.Tx, srcSchema, srcKey, dstSchema, dstKey string, sameBucket, hasParts bool, idMap map[int64]int64) error {
	if sameBucket {
		if srcKey == dstKey {
			return nil
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
			(key, part_no, file_id, algorithm, value)
			SELECT $2, part_no, file_id, algorithm, value
			FROM `+srcSchema+`.object_part_checksums WHERE key = $1
			ON CONFLICT (key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			srcKey, dstKey)
		return err
	}
	if !hasParts {
		return nil
	}
	oldIDs := make([]int64, 0, len(idMap))
	newIDs := make([]int64, 0, len(idMap))
	for oldID, newID := range idMap {
		oldIDs = append(oldIDs, oldID)
		newIDs = append(newIDs, newID)
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
		(key, part_no, file_id, algorithm, value)
		SELECT $2, c.part_no, COALESCE(m.new_id, c.file_id), c.algorithm, c.value
		FROM `+srcSchema+`.object_part_checksums c
		LEFT JOIN unnest($3::int8[], $4::int8[]) AS m(old_id, new_id) ON m.old_id = c.file_id
		WHERE c.key = $1`,
		srcKey, dstKey, oldIDs, newIDs)
	return err
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
		if !exists || !ETagMatches(ifMatch, etag) {
			return false
		}
	}
	if ifNone != "" {
		if exists && ETagMatches(ifNone, etag) {
			return false
		}
	}
	return true
}
