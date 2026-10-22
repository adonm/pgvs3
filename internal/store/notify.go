package store

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// notifyTx publishes a cache invalidation as part of the write transaction, so
// it is delivered only if the write commits.
func notifyTx(ctx context.Context, tx pgx.Tx, bucket, key string) error {
	_, err := tx.Exec(ctx, "SELECT pg_notify('s3p_invalidate', $1)", bucket+"\x1f"+key)
	return err
}

// StartNotify keeps one connection on LISTEN and drops local metadata/byte
// cache entries as writes commit anywhere in the cluster.
func (s *Store) StartNotify(ctx context.Context) {
	for ctx.Err() == nil {
		conn, err := s.pool.Acquire(ctx)
		if err != nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if _, err := conn.Exec(ctx, "LISTEN s3p_invalidate"); err != nil {
			conn.Release()
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for {
			n, err := conn.Conn().WaitForNotification(ctx)
			if err != nil {
				break
			}
			if strings.HasPrefix(n.Payload, "b:") {
				s.dropBucket(strings.TrimPrefix(n.Payload, "b:"))
				continue
			}
			bucket, key, ok := strings.Cut(n.Payload, "\x1f")
			if !ok {
				continue
			}
			s.cache.Invalidate(ctx, bucket, key)
			s.objects.invalidate(bucket, key)
		}
		conn.Release()
		if ctx.Err() == nil {
			time.Sleep(200 * time.Millisecond)
		}
	}
}
