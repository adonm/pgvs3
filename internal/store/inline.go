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
// happen inside s3p.put_inline.
func (s *Store) PutInline(ctx context.Context, bucket, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string) (*Meta, error) {
	if int64(len(data)) > s.maxInline {
		return nil, ErrInlineTooLarge
	}
	sumMD5 := md5.Sum(data)
	sumSHA := sha256.Sum256(data)
	etag := hex.EncodeToString(sumMD5[:])
	condText := "none"
	switch cond {
	case IfAbsent:
		condText = "if_absent"
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
	var result string
	err := s.pool.QueryRow(ctx,
		"SELECT s3p.put_inline($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
		bucket, key, data, sumSHA[:], etag, userMeta, contentType, condText, match, s.writeCommit).Scan(&result)
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
