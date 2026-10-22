package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const MaxInlineBytes = 65536

// PutInline publishes an object up to 64 KiB in one SQL round trip. The body
// is already verified by the caller; conditions and old-file garbage collection
// happen inside the bucket schema's put_inline function.
func (s *Store) PutInline(ctx context.Context, bucket, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string) (*Meta, error) {
	return s.PutInlineOpts(ctx, bucket, key, data, cond, ifMatch, userMeta, contentType, nil, false)
}

func (s *Store) PutInlineOpts(ctx context.Context, bucket, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string, expiresAt *time.Time, keepTTL bool) (*Meta, error) {
	if int64(len(data)) > MaxInlineBytes {
		return nil, ErrInlineTooLarge
	}
	sc, err := bucketSQLIdent(bucket)
	if err != nil {
		return nil, err
	}
	sumMD5 := md5.Sum(data)
	sumSHA := sha256.Sum256(data)
	etag := hex.EncodeToString(sumMD5[:])
	stored := compressBlock(nil, data)
	condText := "none"
	switch cond {
	case IfAbsent:
		condText = "if_absent"
	case IfPresent:
		condText = "if_present"
	case IfMatch:
		condText = "if_match"
	}
	var match any
	if ifMatch != "" {
		match = ifMatch
	}
	if userMeta == nil {
		userMeta = []string{}
	}
	query := "SELECT " + sc + ".put_inline($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)"
	var result string
	err = s.pool.QueryRow(ctx, query,
		key, stored, int64(len(data)), sumSHA[:], etag, userMeta, contentType, condText, match, "on", expiresAt, keepTTL).Scan(&result)
	if isMissingBucketSchema(err) {
		if err := s.CreateBucket(ctx, bucket); err != nil {
			return nil, err
		}
		err = s.pool.QueryRow(ctx, query,
			key, stored, int64(len(data)), sumSHA[:], etag, userMeta, contentType, condText, match, "on", expiresAt, keepTTL).Scan(&result)
	}
	if err != nil {
		return nil, err
	}
	switch result {
	case "ok":
	case "nobucket":
		return nil, ErrNoBucket
	case "precondition":
		return nil, ErrPrecondition
	default:
		return nil, ErrPrecondition
	}
	s.knownBuckets.Store(bucket, struct{}{})
	m := Meta{
		Size: int64(len(data)), SHA256: sumSHA[:], ETag: etag,
		UserMetadata: userMeta, ContentType: contentType, CreatedAt: time.Now(),
		Inline: data,
	}
	return &m, nil
}

var ErrInlineTooLarge = errInlineTooLarge{}

type errInlineTooLarge struct{}

func (errInlineTooLarge) Error() string { return "inline object exceeds 64 KiB" }
