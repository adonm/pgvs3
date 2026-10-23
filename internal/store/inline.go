package store

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
)

// MaxInlineBytes is the bucket-kind inline ceiling. Cache and queue kinds cap
// inline values at one 8 KB page; use Kind.InlineMax for those.
const MaxInlineBytes = 65536

// PutInline publishes a bucket object up to 64 KiB in one SQL round trip.
func (s *Store) PutInline(ctx context.Context, bucket, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string) (*Meta, error) {
	return s.PutInlineOpts(ctx, bucket, key, data, cond, ifMatch, userMeta, contentType, nil, false)
}

// PutInlineOpts is PutInline with TTL control.
func (s *Store) PutInlineOpts(ctx context.Context, bucket, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string, expiresAt *time.Time, keepTTL bool) (*Meta, error) {
	return s.PutInlineKind(ctx, ns.Bucket, bucket, key, data, cond, ifMatch, userMeta, contentType, expiresAt, keepTTL, "")
}

// PutInlineKind publishes an object up to the kind's inline ceiling in one SQL
// round trip. The body is already verified by the caller; conditions and
// old-file garbage collection happen inside the namespace's put_inline.
func (s *Store) PutInlineKind(ctx context.Context, kind ns.Kind, name, key string, data []byte, cond Condition, ifMatch string, userMeta []string, contentType string, expiresAt *time.Time, keepTTL bool, originETag string) (*Meta, error) {
	if int64(len(data)) > int64(kind.InlineMax()) {
		return nil, ErrInlineTooLarge
	}
	if s.checkQuota(ctx, kind, name, int64(len(data))) {
		return nil, ErrQuotaExceeded
	}
	sc, err := nsSQLIdent(kind, name)
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
	if !s.known(kind, name) {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return nil, err
		}
	}
	query := "SELECT " + sc + ".put_inline($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)"
	var result string
	err = s.pool.QueryRow(ctx, query,
		key, stored, int64(len(data)), sumSHA[:], etag, userMeta, contentType,
		condText, match, "on", expiresAt, keepTTL, originETag).Scan(&result)
	if isMissingBucketSchema(err) {
		if err := s.EnsureNamespace(ctx, kind, name, nil, nil); err != nil {
			return nil, err
		}
		err = s.pool.QueryRow(ctx, query,
			key, stored, int64(len(data)), sumSHA[:], etag, userMeta, contentType,
			condText, match, "on", expiresAt, keepTTL, originETag).Scan(&result)
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
	s.markKnown(kind, name)
	m := Meta{
		Size: int64(len(data)), SHA256: sumSHA[:], ETag: etag,
		UserMetadata: userMeta, ContentType: contentType, CreatedAt: time.Now(),
		ExpiresAt: expiresAt, OriginETag: originETag, Inline: data,
	}
	return &m, nil
}

var ErrInlineTooLarge = errInlineTooLarge{}

type errInlineTooLarge struct{}

func (errInlineTooLarge) Error() string { return "inline object exceeds the namespace limit" }
