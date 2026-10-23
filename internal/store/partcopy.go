package store

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CopyPart records a multipart part that references an existing single-file
// object in the same bucket, without moving bytes. It returns ok=false when
// the source is in a different bucket, inline, multipart, a range, or uses a
// checksum we cannot reproduce.
func (s *Store) CopyPart(ctx context.Context, uploadID, bucket, key string, partNo int32, srcBucket, srcKey string) (*Published, bool, error) {
	if srcBucket != bucket {
		return nil, false, nil
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, false, err
	}
	src, err := s.Meta(ctx, srcBucket, srcKey)
	if err != nil {
		return nil, false, err
	}
	if src.Inline != nil || src.Parts != nil || src.FileID == 0 || src.Size <= 0 {
		return nil, false, nil
	}
	md5sum, ok := etagMD5(src.ETag)
	if !ok {
		return nil, false, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var algorithm pgtype.Text
	err = tx.QueryRow(ctx, `SELECT checksum_algorithm FROM `+sc+`.uploads
		WHERE upload_id = $1 AND key = $2 FOR KEY SHARE`,
		uploadID, key).Scan(&algorithm)
	if err == pgx.ErrNoRows || isMissingBucketSchema(err) {
		return nil, false, ErrNoUpload
	}
	if err != nil {
		return nil, false, err
	}
	var chk *Checksum
	if algorithm.Valid {
		if algorithm.String != "SHA256" {
			return nil, false, nil
		}
		chk = &Checksum{Name: "SHA256", Value: base64.StdEncoding.EncodeToString(src.SHA256)}
	}
	if err := commitPart(ctx, tx, sc, uploadID, partNo, src.FileID, src.Size, src.SHA256, md5sum, chk); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return &Published{
		FileID: src.FileID, Size: src.Size, ETag: hex.EncodeToString(md5sum),
		SHA256: src.SHA256, MD5: md5sum, Checksum: chk,
	}, true, nil
}

func etagMD5(etag string) ([]byte, bool) {
	if i := strings.IndexByte(etag, '-'); i >= 0 {
		etag = etag[:i]
	}
	b, err := hex.DecodeString(etag)
	if err != nil || len(b) != 16 {
		return nil, false
	}
	return b, true
}
