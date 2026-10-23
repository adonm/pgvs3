package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
)

// CreateUpload starts a multipart upload and returns its id.
func (s *Store) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType string, checksumAlgorithm *string) (string, error) {
	return s.CreateUploadKind(ctx, ns.Bucket, bucket, key, userMeta, contentType, checksumAlgorithm)
}

// CreateUploadKind starts a multipart upload in a bucket or cache namespace.
func (s *Store) CreateUploadKind(ctx context.Context, kind ns.Kind, name, key string, userMeta []string, contentType string, checksumAlgorithm *string) (string, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return "", err
	}
	if !s.known(kind, name) {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return "", err
		}
	}
	query := `INSERT INTO ` + sc + `.uploads (upload_id, key, checksum_algorithm, user_metadata, content_type)
		VALUES (gen_random_uuid()::text, $1, $2, $3, $4)
		RETURNING upload_id`
	var id string
	err = s.pool.QueryRow(ctx, query, key, checksumAlgorithm, userMeta, contentType).Scan(&id)
	if isMissingBucketSchema(err) {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return "", err
		}
		err = s.pool.QueryRow(ctx, query, key, checksumAlgorithm, userMeta, contentType).Scan(&id)
	}
	return id, err
}

// ListedPart is one part named in CompleteMultipartUpload.
type ListedPart struct {
	PartNo   int32
	ETag     string
	Checksum *Checksum
}

// Completed describes a published multipart object.
type Completed struct {
	Bucket string
	Key    string
	ETag   string
	Size   int64
}

// CompleteUpload validates the listed parts and publishes the ordered part
// list without moving any data.
func (s *Store) CompleteUpload(ctx context.Context, uploadID, requestBucket, requestKey string, listed []ListedPart) (*Completed, error) {
	return s.CompleteUploadKind(ctx, ns.Bucket, uploadID, requestBucket, requestKey, listed, nil)
}

// CompleteUploadKind validates the listed parts and publishes the multipart
// object in a bucket or cache namespace.
func (s *Store) CompleteUploadKind(ctx context.Context, kind ns.Kind, uploadID, requestBucket, requestKey string, listed []ListedPart, expiresAt *time.Time) (*Completed, error) {
	sc, err := nsSQLIdent(kind, requestBucket)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var key, contentType string
	var algorithm pgtype.Text
	var userMeta []string
	err = tx.QueryRow(ctx, `SELECT key, checksum_algorithm, user_metadata, content_type
		FROM `+sc+`.uploads WHERE upload_id = $1 FOR UPDATE`, uploadID).
		Scan(&key, &algorithm, &userMeta, &contentType)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return nil, ErrNoUpload
	}
	if err != nil {
		return nil, err
	}
	if key != requestKey {
		return nil, ErrNoUpload
	}
	if len(listed) == 0 {
		return nil, ErrInvalidPart
	}
	for i, p := range listed {
		if p.PartNo < 1 || p.PartNo > 10000 || (i > 0 && listed[i-1].PartNo >= p.PartNo) {
			return nil, ErrInvalidPart
		}
	}
	rows, err := tx.Query(ctx, `SELECT part_no, file_id, size, sha256, md5, checksum
		FROM `+sc+`.upload_parts WHERE upload_id = $1 ORDER BY part_no`, uploadID)
	if err != nil {
		return nil, err
	}
	type stored struct {
		no       int32
		fileID   int64
		size     int64
		sha, m5  []byte
		checksum pgtype.Text
	}
	var parts []stored
	for rows.Next() {
		var p stored
		if err := rows.Scan(&p.no, &p.fileID, &p.size, &p.sha, &p.m5, &p.checksum); err != nil {
			rows.Close()
			return nil, err
		}
		parts = append(parts, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	want := 0
	var ids, ends []int64
	var size int64
	var unused []int64
	type selectedCheck struct {
		no       int32
		fileID   int64
		checksum string
	}
	var selectedChecks []selectedCheck
	shaAll := sha256.New()
	md5All := md5.New()
	for _, p := range parts {
		if want >= len(listed) || listed[want].PartNo != p.no {
			unused = append(unused, p.fileID)
			continue
		}
		lp := listed[want]
		checksumOK := false
		switch {
		case !algorithm.Valid && lp.Checksum == nil:
			checksumOK = true
		case algorithm.Valid && lp.Checksum != nil:
			checksumOK = algorithm.String == lp.Checksum.Name && p.checksum.Valid && p.checksum.String == lp.Checksum.Value
		}
		if lp.ETag != hex.EncodeToString(p.m5) || !checksumOK {
			return nil, ErrInvalidPart
		}
		shaAll.Write(p.sha)
		md5All.Write(p.m5)
		size += p.size
		ids = append(ids, p.fileID)
		ends = append(ends, size)
		if algorithm.Valid && p.checksum.Valid {
			selectedChecks = append(selectedChecks, selectedCheck{no: p.no, fileID: p.fileID, checksum: p.checksum.String})
		}
		want++
	}
	if want != len(listed) {
		return nil, ErrInvalidPart
	}
	etag := s3ETag(md5All.Sum(nil), len(ids))
	var target *writerTarget
	if expiresAt != nil {
		target = &writerTarget{expiresAt: expiresAt}
	}
	if err := swapObjectParts(ctx, tx, sc, requestBucket, key, target, ids[0], size, shaAll.Sum(nil), etag, userMeta, contentType, ids, ends); err != nil {
		return nil, err
	}
	if len(selectedChecks) > 0 {
		nos := make([]int32, len(selectedChecks))
		fids := make([]int64, len(selectedChecks))
		vals := make([]string, len(selectedChecks))
		for i, c := range selectedChecks {
			nos[i], fids[i], vals[i] = c.no, c.fileID, c.checksum
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+sc+`.object_part_checksums
			(key, part_no, file_id, algorithm, value)
			SELECT $1, no, fid, $4, val
			FROM unnest($2::int4[], $3::int8[], $5::text[]) AS t(no, fid, val)
			ON CONFLICT (key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			key, nos, fids, algorithm.String, vals); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+sc+".uploads WHERE upload_id = $1", uploadID); err != nil {
		return nil, err
	}
	if err := queueFileIDs(ctx, tx, sc, unused); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Completed{Bucket: requestBucket, Key: key, ETag: etag, Size: size}, nil
}

// AbortUpload discards an in-progress multipart upload.
func (s *Store) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	return s.AbortUploadKind(ctx, ns.Bucket, uploadID, bucket, key)
}

// AbortUploadKind discards an in-progress multipart upload in a bucket or
// cache namespace.
func (s *Store) AbortUploadKind(ctx context.Context, kind ns.Kind, uploadID, name, key string) (bool, error) {
	sc, err := nsSQLIdent(kind, name)
	if err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var one int
	err = tx.QueryRow(ctx, "SELECT 1 FROM "+sc+".uploads WHERE upload_id = $1 AND key = $2 FOR UPDATE", uploadID, key).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var ids []int64
	rows, err := tx.Query(ctx, "SELECT file_id FROM "+sc+".upload_parts WHERE upload_id = $1", uploadID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if _, err := tx.Exec(ctx, "DELETE FROM "+sc+".uploads WHERE upload_id = $1", uploadID); err != nil {
		return false, err
	}
	if err := queueFileIDs(ctx, tx, sc, ids); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func s3ETag(md5sum []byte, parts int) string {
	h := hex.EncodeToString(md5sum)
	if parts > 0 {
		return h + "-" + itoa(parts)
	}
	return h
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

var _ = time.Now
