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
// object, without moving bytes. It returns ok=false when the source is inline,
// multipart, a range, or uses a checksum we cannot reproduce.
func (s *Store) CopyPart(ctx context.Context, uploadID, bucket, key string, partNo int32, srcBucket, srcKey string) (*Published, bool, error) {
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
	err = tx.QueryRow(ctx, `SELECT checksum_algorithm FROM s3p.uploads
		WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR KEY SHARE`,
		uploadID, bucket, key).Scan(&algorithm)
	if err == pgx.ErrNoRows {
		return nil, false, ErrNoUpload
	}
	if err != nil {
		return nil, false, err
	}
	var checksum *string
	if algorithm.Valid {
		if algorithm.String != "SHA256" {
			return nil, false, nil
		}
		v := base64.StdEncoding.EncodeToString(src.SHA256)
		checksum = &v
	}
	var old pgtype.Int8
	err = tx.QueryRow(ctx, `DELETE FROM s3p.upload_parts WHERE upload_id = $1 AND part_no = $2
		RETURNING file_id`, uploadID, partNo).Scan(&old)
	if err != nil && err != pgx.ErrNoRows {
		return nil, false, err
	}
	if old.Valid {
		if err := queueFiles(ctx, tx, old.Int64, nil); err != nil {
			return nil, false, err
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO s3p.upload_parts
		(upload_id, part_no, file_id, size, sha256, md5, checksum)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		uploadID, partNo, src.FileID, src.Size, src.SHA256, md5sum, checksum); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return &Published{
		FileID: src.FileID, Size: src.Size, ETag: hex.EncodeToString(md5sum),
		SHA256: src.SHA256, MD5: md5sum,
		Checksum: func() *Checksum {
			if checksum == nil {
				return nil
			}
			return &Checksum{Name: "SHA256", Value: *checksum}
		}(),
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
