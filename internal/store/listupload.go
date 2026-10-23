package store

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
)

type PartInfo struct {
	Number   int32
	Size     int64
	FileID   int64
	Checksum *Checksum
}

// Attributes returns metadata plus per-part sizes for GetObjectAttributes.
func (s *Store) Attributes(ctx context.Context, bucket, key string) (*Meta, []PartInfo, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, nil, err
	}
	m, err := s.Meta(ctx, bucket, key)
	if err != nil {
		return nil, nil, err
	}
	var parts []PartInfo
	if m.Parts != nil {
		type partCheck struct {
			fileID int64
			c      *Checksum
		}
		checks := map[int32]partCheck{}
		rows, err := s.pool.Query(ctx, `SELECT part_no, file_id, algorithm, value
			FROM `+sc+`.object_part_checksums WHERE key = $1`, key)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			var no int32
			var fileID int64
			var alg, value string
			if err := rows.Scan(&no, &fileID, &alg, &value); err != nil {
				rows.Close()
				return nil, nil, err
			}
			checks[no] = partCheck{fileID: fileID, c: &Checksum{Name: alg, Value: value}}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, nil, err
		}
		var prev int64
		for i, end := range m.PartEnds {
			no := int32(i + 1)
			p := PartInfo{Number: no, Size: end - prev}
			if i < len(m.Parts) {
				p.FileID = m.Parts[i]
			}
			if c, ok := checks[no]; ok && c.fileID == p.FileID {
				// Ignore a stale row whose file was replaced by this object.
				p.Checksum = c.c
			}
			parts = append(parts, p)
			prev = end
		}
	}
	return m, parts, nil
}

type UploadPartInfo struct {
	Number       int32
	Size         int64
	ETag         string
	Checksum     *Checksum
	LastModified time.Time
}

// ListParts returns one page of an in-progress multipart upload's parts.
func (s *Store) ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]UploadPartInfo, bool, int32, error) {
	return s.ListPartsKind(ctx, ns.Bucket, uploadID, bucket, key, max, marker)
}

// ListPartsKind returns one page of an in-progress multipart upload's parts in
// a bucket or cache namespace.
func (s *Store) ListPartsKind(ctx context.Context, kind ns.Kind, uploadID, name, key string, max int, marker int32) ([]UploadPartInfo, bool, int32, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return nil, false, 0, err
	}
	var algorithm pgtype.Text
	var epoch float64
	err = s.pool.QueryRow(ctx, `SELECT checksum_algorithm, EXTRACT(EPOCH FROM created_at)::float8
		FROM `+sc+`.uploads WHERE upload_id = $1 AND key = $2`, uploadID, key).
		Scan(&algorithm, &epoch)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return nil, false, 0, ErrNoUpload
	}
	if err != nil {
		return nil, false, 0, err
	}
	rows, err := s.pool.Query(ctx, `SELECT part_no, size, md5, checksum FROM `+sc+`.upload_parts
		WHERE upload_id = $1 AND part_no > $2 ORDER BY part_no LIMIT $3`, uploadID, marker, max+1)
	if err != nil {
		return nil, false, 0, err
	}
	defer rows.Close()
	out := make([]UploadPartInfo, 0, max+1)
	for rows.Next() {
		var no int32
		var size int64
		var md5sum []byte
		var checksum pgtype.Text
		if err := rows.Scan(&no, &size, &md5sum, &checksum); err != nil {
			return nil, false, 0, err
		}
		p := UploadPartInfo{Number: no, Size: size, ETag: hex.EncodeToString(md5sum), LastModified: time.Unix(0, int64(epoch*1e9))}
		if algorithm.Valid && checksum.Valid {
			p.Checksum = &Checksum{Name: algorithm.String, Value: checksum.String}
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, false, 0, err
	}
	truncated := len(out) > max
	if truncated {
		out = out[:max]
	}
	next := marker
	if len(out) > 0 {
		next = out[len(out)-1].Number
	}
	return out, truncated, next, nil
}

type UploadInfo struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

// ListUploads returns one page of in-progress uploads in key/upload order.
func (s *Store) ListUploads(ctx context.Context, bucket, prefix, bound, keyMarker, uploadMarker string, max int) ([]UploadInfo, bool, error) {
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, false, err
	}
	rows, err := s.pool.Query(ctx, `SELECT key, upload_id, EXTRACT(EPOCH FROM created_at)::float8
		FROM `+sc+`.uploads
		WHERE key >= $1 AND key < $2
		  AND (key > $3 OR (key = $3 AND upload_id > $4))
		ORDER BY key, upload_id LIMIT $5`, prefix, bound, keyMarker, uploadMarker, max+1)
	if isMissingBucketSchema(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := make([]UploadInfo, 0, max+1)
	for rows.Next() {
		var u UploadInfo
		var epoch float64
		if err := rows.Scan(&u.Key, &u.UploadID, &epoch); err != nil {
			return nil, false, err
		}
		u.Initiated = time.Unix(0, int64(epoch*1e9))
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	truncated := len(out) > max
	if truncated {
		out = out[:max]
	}
	return out, truncated, nil
}
