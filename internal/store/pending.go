package store

import (
	"context"
	"strings"

	"github.com/adonm/pgvs3/internal/ns"
)

const pendingPrefix = "pending:"

// IsPendingOrigin reports whether a cache object is waiting for its origin
// upload to finish. Readers must treat such an object as a cache miss.
func IsPendingOrigin(version string) bool {
	return strings.HasPrefix(version, pendingPrefix)
}

func pendingVersion(uploadID string) string {
	return pendingPrefix + uploadID
}

// PendingOriginToken is the origin_etag sentinel for a pending upload.
func PendingOriginToken(uploadID string) string {
	return pendingVersion(uploadID)
}

// PendingUpload is one cache object whose origin upload still needs a retry.
type PendingUpload struct {
	Key       string
	Token     string
	Attempts  int
	LastError string
}

// PendingUploads returns due outbox rows for one cache namespace.
func (s *Store) PendingUploads(ctx context.Context, namespace string, limit int) ([]PendingUpload, error) {
	rows, err := s.pool.Query(ctx, `SELECT key, token, attempts, coalesce(last_error, '')
		FROM s3p.pending_uploads
		WHERE namespace = $1 AND next_attempt <= now()
		ORDER BY next_attempt
		LIMIT $2`, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PendingUpload, 0, limit)
	for rows.Next() {
		var p PendingUpload
		if err := rows.Scan(&p.Key, &p.Token, &p.Attempts, &p.LastError); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MarkOriginCommitted publishes the origin version on the cache object and
// removes its outbox row. The token guard prevents an old flush from
// overwriting a newer PUT or a newer pending completion for the same key.
func (s *Store) MarkOriginCommitted(ctx context.Context, namespace, key, token, version string) error {
	sc, err := ns.Cache.Schema(namespace)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx,
		`UPDATE `+quoteIdent(sc)+`.objects SET origin_etag = $3, created_at = now()
		 WHERE key = $1 AND origin_etag = $2`, key, token, version); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM s3p.pending_uploads
		 WHERE namespace = $1 AND key = $2 AND token = $3`, namespace, key, token); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailPendingUpload records a retry with bounded exponential backoff.
func (s *Store) FailPendingUpload(ctx context.Context, namespace, key, token string, cause error) error {
	_, err := s.pool.Exec(ctx, `UPDATE s3p.pending_uploads
		SET attempts = attempts + 1,
		    last_error = $4,
		    next_attempt = now() + make_interval(secs => LEAST(300, (1 << LEAST(attempts + 1, 8))))
		WHERE namespace = $1 AND key = $2 AND token = $3`, namespace, key, token, errorText(cause))
	return err
}

// ClearPendingUpload removes an outbox row without changing the object. Used
// when the cache object was deleted or overwritten.
func (s *Store) ClearPendingUpload(ctx context.Context, namespace, key string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2`, namespace, key)
	return err
}

// ClearPendingUploadToken removes a specific outbox row without changing the
// object. Used when a flush discovers the object was overwritten by a newer
// write carrying a different token.
func (s *Store) ClearPendingUploadToken(ctx context.Context, namespace, key, token string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM s3p.pending_uploads WHERE namespace = $1 AND key = $2 AND token = $3`,
		namespace, key, token)
	return err
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	if len(err.Error()) > 1024 {
		return err.Error()[:1024]
	}
	return err.Error()
}
