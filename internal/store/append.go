package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrInvalidOffset = errors.New("invalid write offset")
	ErrTooManyParts  = errors.New("too many parts")
)

// FinishAppend attaches appended bytes to an object, but only if offset equals
// the object's current size. Existing chunks are never rewritten.
func (s *Store) FinishAppend(ctx context.Context, bucket, key string, offset, fileID, size int64, sha []byte, cond Condition, ifMatch string) (*Meta, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := lockObjectKey(ctx, tx, bucket, key); err != nil {
		return nil, err
	}
	var m Meta
	var epoch float64
	err = tx.QueryRow(ctx, `SELECT file_id, size, sha256, etag, user_metadata, content_type,
		parts, part_ends, EXTRACT(EPOCH FROM created_at)::float8
		FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE`, bucket, key).
		Scan(&m.FileID, &m.Size, &m.SHA256, &m.ETag, &m.UserMetadata, &m.ContentType,
			&m.Parts, &m.PartEnds, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		if cond == IfMatch {
			return nil, ErrPrecondition
		}
		if offset != 0 {
			return nil, ErrInvalidOffset
		}
		etag := randomETag()
		shaAll := sha256.Sum256(sha)
		if _, err := tx.Exec(ctx, `INSERT INTO s3p.objects
			(bucket, key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends)
			VALUES ($1,$2,$3,$4,$5,$6,'{}','application/octet-stream',NULL,NULL)`,
			bucket, key, fileID, size, shaAll[:], etag); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		out := Meta{FileID: fileID, Size: size, SHA256: shaAll[:], ETag: etag,
			ContentType: "application/octet-stream", CreatedAt: time.Now()}
		s.cache.Put(ctx, bucket, key, out)
		s.objects.invalidate(bucket, key)
		return &out, nil
	}
	if err != nil {
		return nil, err
	}
	switch cond {
	case IfAbsent:
		return nil, ErrPrecondition
	case IfMatch:
		if m.ETag != ifMatch {
			return nil, ErrPrecondition
		}
	}
	if offset != m.Size {
		return nil, ErrInvalidOffset
	}
	parts := m.Parts
	ends := m.PartEnds
	if parts == nil {
		parts = []int64{m.FileID}
		ends = []int64{m.Size}
	}
	if len(parts) >= 10000 {
		return nil, ErrTooManyParts
	}
	if size < 0 || m.Size > (1<<62)-size {
		return nil, fmt.Errorf("object exceeds the maximum size")
	}
	parts = append(append([]int64(nil), parts...), fileID)
	newSize := m.Size + size
	ends = append(append([]int64(nil), ends...), newSize)
	shaAll := sha256.New()
	shaAll.Write(m.SHA256)
	shaAll.Write(sha)
	etag := randomETag()
	_, err = tx.Exec(ctx, `UPDATE s3p.objects SET file_id = $3, size = $4, sha256 = $5,
		etag = $6, parts = $7, part_ends = $8, created_at = now()
		WHERE bucket = $1 AND key = $2`,
		bucket, key, parts[0], newSize, shaAll.Sum(nil), etag, parts, ends)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	out := m
	out.FileID, out.Size, out.SHA256, out.ETag = parts[0], newSize, shaAll.Sum(nil), etag
	out.Parts, out.PartEnds, out.CreatedAt = parts, ends, time.Now()
	s.cache.Put(ctx, bucket, key, out)
	s.objects.invalidate(bucket, key)
	return &out, nil
}

// DiscardFile queues an unreferenced append file for reaping after a failed
// offset/condition check.
func (s *Store) DiscardFile(ctx context.Context, fileID int64) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if queueFiles(ctx, tx, fileID, nil) == nil {
		_ = tx.Commit(ctx)
	}
}

func randomETag() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString(sha256.New().Sum(nil))[:32]
	}
	return hex.EncodeToString(b[:])
}
