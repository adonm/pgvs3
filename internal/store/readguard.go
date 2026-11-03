package store

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Read pins are shared advisory locks in their own two-key space. A reader
// pins every file it streams; the reaper takes the exclusive lock before
// deleting a file's chunks and skips pinned files. Key collisions only make
// the reaper skip a file until a later pass.
const (
	pinClass       int32 = 0x70677670
	maxPinnedFiles       = 64
	recoveryKey          = "pgvs3.in_recovery"
)

func pinKey(fileID int64) int32 { return int32((fileID ^ (fileID >> 31)) & 0x7fffffff) }

func pinKeys(pieces []piece) []int32 {
	keys := make([]int32, 0, len(pieces))
	for _, p := range pieces {
		keys = append(keys, pinKey(p.fileID))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// readGuard keeps every file of a multi-piece read from being reaped until the
// read finishes. On a writable primary it pins the files. A standby cannot pin
// against the primary's reaper, so it (and reads of many files, or a file the
// reaper is deleting right now) falls back to one exported REPEATABLE READ
// snapshot that each worker imports once. The exporting transaction ends as
// soon as every worker has imported it.
type readGuard struct {
	s       *Store
	conn    *pgxpool.Conn
	keys    []int32
	snap    string
	export  pgx.Tx
	pending atomic.Int32
	once    sync.Once
}

func (s *Store) openReadGuard(ctx context.Context, pieces []piece, workers int) (*readGuard, error) {
	conn, err := s.chunkPool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	g := &readGuard{s: s, conn: conn}
	g.pending.Store(int32(workers))
	keys := pinKeys(pieces)
	if !s.forceSnapshotReads && !inRecovery(conn) && len(keys) <= maxPinnedFiles {
		var got []int32
		err := conn.QueryRow(ctx, `SELECT COALESCE(array_agg(k), '{}') FROM unnest($2::int4[]) AS k
			WHERE pg_try_advisory_lock_shared($1, k)`, pinClass, keys).Scan(&got)
		if err != nil {
			conn.Release()
			return nil, err
		}
		g.keys = got
		if len(got) == len(keys) {
			s.pinnedReads.Add(1)
			return g, nil
		}
		g.unpin()
		if g.conn == nil {
			if g.conn, err = s.chunkPool.Acquire(ctx); err != nil {
				return nil, err
			}
		}
	}
	if err := g.exportSnapshot(ctx); err != nil {
		g.close()
		return nil, err
	}
	s.snapshotReads.Add(1)
	return g, nil
}

func (g *readGuard) exportSnapshot(ctx context.Context) error {
	tx, err := g.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	g.export = tx
	if _, err := tx.Exec(ctx, "SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
		return err
	}
	return tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&g.snap)
}

// lead gives the first worker the guard's own session, so a read never holds
// its guard while waiting on the pool for a worker connection. It counts as
// that worker's join.
func (g *readGuard) lead() (querier, func()) {
	if g.export != nil {
		return g.export, g.joined
	}
	return g.conn, g.joined
}

// join gives a further worker its query session on a fresh chunk connection.
// Every worker but the lead must call join exactly once, even if it is about
// to give up.
func (g *readGuard) join(ctx context.Context) (querier, func(), error) {
	defer g.joined()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	conn, err := g.s.chunkPool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	if g.snap == "" {
		return conn, conn.Release, nil
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		conn.Release()
		return nil, nil, err
	}
	done := func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		conn.Release()
	}
	// A worker can wait on a slow client between pieces; the snapshot must
	// survive that, so the idle-in-transaction timeout is lifted locally.
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+g.snap+"'; SET LOCAL idle_in_transaction_session_timeout = 0"); err != nil {
		done()
		return nil, nil, err
	}
	return tx, done, nil
}

func (g *readGuard) joined() {
	if g.pending.Add(-1) == 0 && g.export != nil {
		g.endExport()
	}
}

func (g *readGuard) endExport() {
	g.once.Do(func() {
		if g.export != nil {
			_ = g.export.Rollback(context.Background())
		}
		if g.conn != nil {
			g.conn.Release()
			g.conn = nil
		}
	})
}

func (g *readGuard) unpin() {
	if len(g.keys) == 0 || g.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var released bool
	err := g.conn.QueryRow(ctx, `SELECT COALESCE(bool_and(pg_advisory_unlock_shared($1, k)), true)
		FROM unnest($2::int4[]) AS k`, pinClass, g.keys).Scan(&released)
	g.keys = nil
	if err != nil || !released {
		// Never return a session that might still hold a pin to the pool.
		_ = g.conn.Conn().Close(ctx)
		g.conn.Release()
		g.conn = nil
	}
}

func (g *readGuard) close() {
	if g.export != nil {
		g.endExport()
		return
	}
	g.unpin()
	if g.conn != nil {
		g.conn.Release()
		g.conn = nil
	}
}

func inRecovery(conn *pgxpool.Conn) bool {
	v, _ := conn.Conn().PgConn().CustomData()[recoveryKey].(bool)
	return v
}

// claimReapable locks the first candidate file that no reader has pinned.
func claimReapable(ctx context.Context, tx pgx.Tx, candidates []int64) (int64, bool, int, error) {
	skipped := 0
	for _, id := range candidates {
		var ok bool
		if err := tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock($1, $2)", pinClass, pinKey(id)).Scan(&ok); err != nil {
			return 0, false, skipped, err
		}
		if ok {
			return id, true, skipped, nil
		}
		skipped++
	}
	return 0, false, skipped, nil
}
