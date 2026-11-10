package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (s *Store) EnsureMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, rel := range layoutManifest.relations {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("required layout relation %s is missing", rel)
		}
	}
	if err := s.checkNamespaceTables(ctx, conn); err != nil {
		return err
	}
	issues, err := functionContractIssues(ctx, conn)
	if err != nil {
		return err
	}
	if len(issues) > 0 {
		return fmt.Errorf("%w: %s", ErrNamespaceLayout, issues[0])
	}
	return nil
}

type Stats struct {
	Buckets        int64          `json:"buckets"`
	Namespaces     int64          `json:"namespaces"`
	Objects        int64          `json:"objects"`
	Chunks         int64          `json:"chunks"`
	Events         int64          `json:"events"`
	LogicalBytes   int64          `json:"logical_bytes"`
	PhysicalBytes  int64          `json:"physical_bytes"`
	DatabaseBytes  int64          `json:"database_bytes"`
	GarbageFiles   int64          `json:"garbage_files"`
	MaxBytes       int64          `json:"max_bytes,omitempty"`
	OriginMetadata int64          `json:"origin_metadata,omitempty"`
	OldestGarbage  *time.Duration `json:"oldest_garbage,omitempty"`
}

func (s *Store) Stat(ctx context.Context) (Stats, error) { return s.StatNamespace(ctx, "", "") }

func (s *Store) StatNamespace(ctx context.Context, kind ns.Kind, name string) (Stats, error) {
	var st Stats
	infos, err := s.listNamespaceStats(ctx, kind, name)
	if err != nil {
		return st, err
	}
	var maxOldest float64
	var hasOldest bool
	for _, n := range infos {
		st.Namespaces++
		u, err := s.queryNamespaceUsage(ctx, n)
		if missingNamespace(ctx, s.pool, quoteIdent(n.Schema), err) {
			continue
		}
		if err != nil {
			return st, err
		}
		st.LogicalBytes += u.logical
		st.PhysicalBytes += u.physical
		st.Objects += u.objects
		st.Chunks += u.chunks
		st.Events += u.events
		st.GarbageFiles += u.garbage
		if n.Kind == ns.Bucket {
			st.Buckets++
		}
		if n.Kind == ns.Cache || n.Kind == ns.WriteBack {
			budget := n.maxBytes
			if budget == 0 {
				if v, ok := s.cacheMaxBytes.Load(n.Name); ok {
					budget, _ = v.(int64)
				}
			}
			st.MaxBytes += budget
		}
		if n.Kind == ns.Cache {
			var meta int64
			err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+quoteIdent(n.Schema)+`.origin_metadata`).Scan(&meta)
			if missingNamespace(ctx, s.pool, quoteIdent(n.Schema), err) {
				continue
			}
			if err != nil {
				return st, err
			}
			st.OriginMetadata += meta
		}
		if u.oldest.Valid && (!hasOldest || u.oldest.Float64 > maxOldest) {
			maxOldest = u.oldest.Float64
			hasOldest = true
		}
	}
	size, err := s.databaseSize(ctx)
	if err != nil {
		return st, err
	}
	st.DatabaseBytes = size
	if hasOldest {
		d := time.Duration(maxOldest * float64(time.Second))
		st.OldestGarbage = &d
	}
	return st, nil
}

func (s *Store) listNamespaceStats(ctx context.Context, kind ns.Kind, name string) ([]NamespaceInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT kind, name, schema_name
		FROM s3p.namespaces
		WHERE ($1 = '' OR kind = $1) AND ($2 = '' OR name = $2)
		ORDER BY kind, name`, string(kind), name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (NamespaceInfo, error) {
		var n NamespaceInfo
		var k string
		err := row.Scan(&k, &n.Name, &n.Schema)
		n.Kind = ns.Kind(k)
		return n, err
	})
}

type namespaceUsage struct {
	logical, physical, objects, chunks, events, garbage int64
	oldest                                              pgtype.Float8
}

func (s *Store) queryNamespaceUsage(ctx context.Context, n NamespaceInfo) (namespaceUsage, error) {
	q := quoteIdent(n.Schema)
	var u namespaceUsage
	var err error
	if n.Kind == ns.Queue {
		err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(pg_column_size(args) + coalesce(pg_column_size(meta),0) + octet_length(type)),0)::int8 FROM `+q+`.jobs)
				+ (SELECT COALESCE(sum(pg_column_size(payload) + pg_column_size(headers)),0)::int8 FROM `+q+`.stream_events),
				pg_total_relation_size('`+q+`.jobs')
				+ pg_total_relation_size('`+q+`.stream_events')
				+ pg_total_relation_size('`+q+`.stream_events_0')
				+ pg_total_relation_size('`+q+`.stream_events_1')
				+ pg_total_relation_size('`+q+`.stream_events_2')
				+ pg_total_relation_size('`+q+`.stream_ticks')
				+ pg_total_relation_size('`+q+`.stream_subscriptions')
				+ pg_total_relation_size('`+q+`.stream_batches')
				+ pg_total_relation_size('`+q+`.stream_batch_events')
				+ pg_total_relation_size('`+q+`.stream_retry_events')
				+ pg_total_relation_size('`+q+`.stream_dead_letters'),
				(SELECT count(*) FROM `+q+`.jobs),
				0,
				(SELECT count(*) FROM `+q+`.stream_events),
				0, NULL`).
			Scan(&u.logical, &u.physical, &u.objects, &u.chunks, &u.events, &u.garbage, &u.oldest)
	} else {
		err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(size),0)::int8 FROM `+q+`.objects),
				pg_total_relation_size('`+q+`.objects') + pg_total_relation_size('`+q+`.chunks') + pg_total_relation_size('`+q+`.origin_metadata'),
				(SELECT count(*) FROM `+q+`.objects),
				(SELECT COALESCE(sum(
					CASE
						WHEN part_ends IS NOT NULL THEN (
							SELECT sum((part_ends[i] - CASE WHEN i = 1 THEN 0 ELSE part_ends[i-1] END + 8119) / 8120)
							FROM generate_subscripts(part_ends, 1) AS i
						)
						ELSE (size + 8119) / 8120
					END
				),0)::int8 FROM `+q+`.objects WHERE file_id IS NOT NULL),
				0,
				(SELECT count(*) FROM `+q+`.garbage),
				(SELECT EXTRACT(EPOCH FROM now() - min(queued_at))::float8 FROM `+q+`.garbage)`).
			Scan(&u.logical, &u.physical, &u.objects, &u.chunks, &u.events, &u.garbage, &u.oldest)
	}
	return u, err
}

const expirePassRounds = 128

type maintenancePass struct {
	name   string
	budget int
	run    func(ctx context.Context, budget int) error
}

func runMaintenancePasses(ctx context.Context, passes []maintenancePass) error {
	for _, pass := range passes {
		if err := pass.run(ctx, pass.budget); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Maintain(ctx context.Context, grace time.Duration) error {
	s.maintenanceRuns.Add(1)
	const lock int64 = 0x70677674
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	var got bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", lock).Scan(&got); err != nil || !got {
		return err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", lock)

	if _, err := conn.Exec(ctx, "SELECT s3p.reconcile_namespaces()"); err != nil {
		return err
	}
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return err
	}
	if _, err := s.CleanupFillLeases(ctx); err != nil {
		return err
	}
	if err := runMaintenancePasses(ctx, s.maintenancePasses(conn, infos, grace)); err != nil {
		return err
	}
	s.refreshQuotas(ctx, infos)
	return nil
}

func (s *Store) maintenancePasses(conn *pgxpool.Conn, infos []NamespaceInfo, grace time.Duration) []maintenancePass {
	return []maintenancePass{
		{name: "expire", budget: expirePassRounds, run: func(ctx context.Context, budget int) error {
			return s.expirePass(ctx, conn, infos, budget)
		}},
		{name: "reap", budget: ReapRows, run: func(ctx context.Context, budget int) error {
			return s.reapPass(ctx, conn, infos, grace, budget)
		}},
	}
}

func (s *Store) expirePass(ctx context.Context, conn *pgxpool.Conn, infos []NamespaceInfo, rounds int) error {
	var expiredUploads int
	for _, n := range infos {
		if n.Kind == ns.Queue {
			removed, err := s.queue(n.Name).expire(ctx, rounds)
			if err != nil {
				return err
			}
			s.expiredObjects.Add(removed)
			continue
		}
		q := quoteIdent(n.Schema)
		var uploads int
		if err := conn.QueryRow(ctx, "SELECT "+q+".expire_uploads(interval '24 hours', 64)").Scan(&uploads); err != nil {
			if missingNamespace(ctx, conn, q, err) {
				continue
			}
			return err
		}
		expiredUploads += uploads
		if _, err := s.expireNamespace(ctx, conn, q, n, rounds); err != nil {
			return err
		}
	}
	if expiredUploads > 0 {
		s.log.Info("expired multipart uploads", "count", expiredUploads)
	}
	return nil
}

func (q Queue) expire(ctx context.Context, rounds int) (int64, error) {
	var total int64
	for range rounds {
		removed, err := q.ExpireMessages(ctx, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += removed
		if removed < ExpireBatch {
			break
		}
	}
	for range rounds {
		discarded, err := q.DiscardExpiredLeases(ctx, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += discarded
		if discarded < ExpireBatch {
			break
		}
	}
	for range rounds {
		moved, err := q.ProcessRetries(ctx, ExpireBatch)
		if err != nil {
			return total, err
		}
		total += moved
		if moved < ExpireBatch {
			break
		}
	}
	if rotated, err := q.Rotate(ctx); err != nil {
		return total, err
	} else {
		total += rotated
	}
	if pruned, err := q.PruneTicks(ctx, ExpireBatch); err != nil {
		return total, err
	} else {
		total += pruned
	}
	return total, nil
}

func (s *Store) expireNamespace(ctx context.Context, conn *pgxpool.Conn, schema string, n NamespaceInfo, rounds int) (int, error) {
	total := 0
	for range rounds {
		var v int
		if err := conn.QueryRow(ctx, "SELECT "+schema+".expire_objects($1)", ExpireBatch).Scan(&v); err != nil {
			if missingNamespace(ctx, conn, schema, err) {
				return total, nil
			}
			return total, err
		}
		s.expiredObjects.Add(int64(v))
		total += v
		if v < ExpireBatch {
			break
		}
	}
	if n.Kind != ns.Cache && n.Kind != ns.WriteBack {
		return total, nil
	}
	maxBytes := n.maxBytes
	if maxBytes == 0 {
		if v, ok := s.cacheMaxBytes.Load(n.Name); ok {
			maxBytes, _ = v.(int64)
		}
	}
	if maxBytes <= 0 {
		return total, nil
	}
	for range rounds {
		evicted, err := s.evictCacheOverBudget(ctx, conn, schema, n.Name, maxBytes, ExpireBatch)
		if err != nil {
			return total, err
		}
		s.expiredObjects.Add(int64(evicted))
		s.cacheEvictedObjects.Add(int64(evicted))
		total += evicted
		if evicted < ExpireBatch {
			break
		}
	}
	return total, nil
}

// evictCacheOverBudget deletes the oldest expiring cache objects until the
// namespace is within maxBytes. It locks candidates with SKIP LOCKED so
// concurrent maintenance or readers are not blocked, and queues their files
// for the reaper in the same transaction.
func (s *Store) evictCacheOverBudget(ctx context.Context, conn *pgxpool.Conn, schema, namespace string, maxBytes int64, limit int) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var used int64
	if err := tx.QueryRow(ctx,
		"SELECT COALESCE(SUM(size),0)::int8 FROM "+schema+".objects WHERE expires_at IS NOT NULL").Scan(&used); err != nil {
		if missingNamespace(ctx, tx, schema, err) {
			return 0, nil
		}
		return 0, err
	}
	if used <= maxBytes {
		return 0, nil
	}
	excess := used - maxBytes
	rows, err := tx.Query(ctx, `WITH locked AS (
			SELECT key, size, file_id, parts, expires_at, created_at
			FROM `+schema+`.objects
			WHERE expires_at IS NOT NULL
			ORDER BY expires_at ASC, created_at ASC, key ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), ranked AS (
			SELECT key, file_id, parts,
			       SUM(size) OVER (ORDER BY expires_at ASC, created_at ASC, key ASC) - size AS prior_bytes
			FROM locked
		), doomed AS (
			DELETE FROM `+schema+`.objects o
			USING ranked r
			WHERE o.key = r.key AND r.prior_bytes < $2
			RETURNING o.file_id, o.parts
		)
		SELECT file_id, parts FROM doomed`, limit, excess)
	if err != nil {
		if missingNamespace(ctx, tx, schema, err) {
			return 0, nil
		}
		return 0, err
	}
	evicted := 0
	var files []int64
	for rows.Next() {
		var fileID *int64
		var parts []int64
		if err := rows.Scan(&fileID, &parts); err != nil {
			rows.Close()
			return 0, err
		}
		evicted++
		if fileID != nil {
			files = append(files, *fileID)
		}
		files = append(files, parts...)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(files) > 0 {
		if _, err := tx.Exec(ctx, "SELECT "+schema+".queue_garbage($1)", files); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	if evicted > 0 {
		s.invalidateChunkNS(namespace)
	}
	return evicted, nil
}

func (s *Store) reapPass(ctx context.Context, conn *pgxpool.Conn, infos []NamespaceInfo, grace time.Duration, budget int) error {
	remaining := budget
	for _, n := range infos {
		if n.Kind == ns.Queue {
			continue
		}
		var err error
		remaining, err = s.reapNamespace(ctx, conn.Conn(), quoteIdent(n.Schema), grace, remaining)
		if err != nil {
			return err
		}
		if remaining <= 0 {
			break
		}
	}
	return nil
}

func (s *Store) reapNamespace(ctx context.Context, conn *pgx.Conn, schema string, grace time.Duration, remaining int) (int, error) {
	for remaining > 0 {
		got, done, err := s.reapOne(ctx, conn, schema, grace, remaining)
		if missingNamespace(ctx, conn, schema, err) {
			return remaining, nil
		}
		if err != nil {
			return remaining, err
		}
		if got == 0 && !done {
			return remaining, nil
		}
		s.reapedRows.Add(int64(got))
		if done {
			s.reapedFiles.Add(1)
		}
		remaining -= max(got, 1)
	}
	return remaining, nil
}

func (s *Store) ExpireObjects(ctx context.Context, kind ns.Kind, name string, max int) (int, error) {
	q, err := nsSQLIdent(kind, name)
	if err != nil {
		return 0, err
	}
	var n int
	err = s.pool.QueryRow(ctx, "SELECT "+q+".expire_objects($1)", max).Scan(&n)
	if missingNamespace(ctx, s.pool, q, err) {
		return 0, nil
	}
	return n, err
}

func (s *Store) refreshQuotas(ctx context.Context, infos []NamespaceInfo) {
	for _, n := range infos {
		if n.Quota == nil {
			continue
		}
		query, args := namespaceSizeQuery(n.Kind, quoteIdent(n.Schema))
		var size int64
		if err := s.pool.QueryRow(ctx, query, args...).Scan(&size); err != nil {
			continue
		}
		s.setQuotaState(n.Kind, n.Name, size, *n.Quota)
	}
}

func (s *Store) reapOne(ctx context.Context, conn *pgx.Conn, schema string, grace time.Duration, budget int) (int, bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	rows, err := tx.Query(ctx, `SELECT file_id FROM `+schema+`.garbage
		WHERE queued_at <= now() - make_interval(secs => $1)
		ORDER BY queued_at, file_id LIMIT $2 FOR UPDATE SKIP LOCKED`, grace.Seconds(), reapCandidates)
	if err != nil {
		return 0, false, err
	}
	candidates, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, false, err
	}
	fileID, ok, skipped, err := claimReapable(ctx, tx, candidates)
	s.reapPinnedSkips.Add(int64(skipped))
	if err != nil || !ok {
		return 0, false, err
	}
	var referenced bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+schema+`.objects WHERE file_id IS NOT NULL AND file_id = $1)
		OR EXISTS (SELECT 1 FROM `+schema+`.objects WHERE parts IS NOT NULL AND parts @> ARRAY[$1::int8])
		OR EXISTS (SELECT 1 FROM `+schema+`.upload_parts WHERE file_id = $1)`, fileID).Scan(&referenced); err != nil {
		return 0, false, err
	}
	if referenced {

		if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
			return 0, false, err
		}
		return 0, true, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM `+schema+`.chunks WHERE ctid IN
		(SELECT ctid FROM `+schema+`.chunks WHERE file_id = $1 LIMIT $2)`, fileID, budget)
	if err != nil {
		return 0, false, err
	}
	n := int(tag.RowsAffected())
	done := false
	if n < budget {
		var left bool
		if err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+schema+".chunks WHERE file_id = $1)", fileID).Scan(&left); err != nil {
			return 0, false, err
		}
		if !left {
			if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".garbage WHERE file_id = $1", fileID); err != nil {
				return 0, false, err
			}
			done = true
		}
	}
	return n, done, tx.Commit(ctx)
}

type Diagnosis struct {
	ServerVersion     int      `json:"server_version_num"`
	AuroraVersion     string   `json:"aurora_version,omitempty"`
	Layout            int      `json:"layout"`
	Namespaces        int64    `json:"namespaces"`
	OverScale         bool     `json:"over_supported_scale"`
	Buckets           int64    `json:"buckets"`
	Queues            int64    `json:"queues"`
	Caches            int64    `json:"caches"`
	ChunksRelkind     string   `json:"chunks_relkind"`
	ChunksConfigured  bool     `json:"chunks_configured"`
	Objects           int64    `json:"objects"`
	GarbageFiles      int64    `json:"garbage_files"`
	GarbageReferenced int64    `json:"garbage_referenced"`
	DatabaseBytes     int64    `json:"database_bytes"`
	OrphanSchemas     int64    `json:"orphan_schemas"`
	MissingSchemas    int64    `json:"missing_schemas"`
	MissingQueueFuncs []string `json:"missing_queue_functions,omitempty"`
	FunctionIssues    []string `json:"function_contract_issues,omitempty"`
}

// ServerFacts are the live settings and backlog that `pgvs3 doctor` judges.
type ServerFacts struct {
	Settings          map[string]string `json:"settings"`
	MaxWALBytes       int64             `json:"max_wal_bytes"`
	InRecovery        bool              `json:"in_recovery"`
	ClientBackends    int64             `json:"client_backends"`
	PendingUploads    int64             `json:"pending_uploads"`
	PendingBytes      int64             `json:"pending_bytes"`
	DeadUploads       int64             `json:"dead_uploads"`
	OldestPendingSecs float64           `json:"oldest_pending_seconds"`
	Standbys          int64             `json:"standbys"`
	MaxReplayLagBytes int64             `json:"max_replay_lag_bytes"`
}

var doctorSettings = []string{
	"server_version", "max_connections", "superuser_reserved_connections", "reserved_connections",
	"shared_buffers", "max_wal_size", "fsync", "full_page_writes", "synchronous_commit",
	"synchronous_standby_names", "io_method", "wal_level",
}

func (s *Store) ServerFacts(ctx context.Context) (ServerFacts, error) {
	f := ServerFacts{Settings: map[string]string{}}
	rows, err := s.pool.Query(ctx, "SELECT name, current_setting(name) FROM pg_settings WHERE name = ANY($1)", doctorSettings)
	if err != nil {
		return f, err
	}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			rows.Close()
			return f, err
		}
		f.Settings[name] = value
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return f, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT pg_size_bytes(current_setting('max_wal_size')), pg_is_in_recovery(),
		(SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend')`).
		Scan(&f.MaxWALBytes, &f.InRecovery, &f.ClientBackends); err != nil {
		return f, err
	}
	var hasPending bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass('s3p.pending_uploads') IS NOT NULL").Scan(&hasPending); err != nil {
		return f, err
	}
	if hasPending {
		if err := s.pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(size), 0), count(*) FILTER (WHERE dead),
			COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)::float8 FROM s3p.pending_uploads`).
			Scan(&f.PendingUploads, &f.PendingBytes, &f.DeadUploads, &f.OldestPendingSecs); err != nil {
			return f, err
		}
	}
	if !f.InRecovery {
		if err := s.pool.QueryRow(ctx, `SELECT count(*),
			COALESCE(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), 0)::int8 FROM pg_stat_replication`).
			Scan(&f.Standbys, &f.MaxReplayLagBytes); err != nil {
			return f, err
		}
	}
	return f, nil
}

func (s *Store) Diagnose(ctx context.Context) (Diagnosis, error) {
	var d Diagnosis
	if err := s.diagnoseServer(ctx, &d); err != nil {
		return d, err
	}
	d.ChunksRelkind = "r"
	d.ChunksConfigured = true
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return d, err
	}
	d.Namespaces = int64(len(infos))
	d.OverScale = d.Namespaces > SupportedNamespaces
	for _, n := range infos {
		if err := s.diagnoseNamespace(ctx, n, &d); err != nil {
			return d, err
		}
	}
	if err := s.diagnoseSchemas(ctx, &d); err != nil {
		return d, err
	}
	if err := s.diagnoseFunctionContracts(ctx, &d); err != nil {
		return d, err
	}
	size, err := s.databaseSize(ctx)
	if err != nil {
		return d, err
	}
	d.DatabaseBytes = size
	return d, nil
}

func (s *Store) diagnoseServer(ctx context.Context, d *Diagnosis) error {
	var versionText string
	if err := s.pool.QueryRow(ctx, "SHOW server_version_num").Scan(&versionText); err != nil {
		return err
	}
	d.ServerVersion, _ = strconv.Atoi(versionText)
	var aurora pgtype.Text
	if err := s.pool.QueryRow(ctx, `SELECT CASE
		WHEN to_regprocedure('aurora_version()') IS NULL THEN ''
		ELSE aurora_version()::text END`).Scan(&aurora); err == nil && aurora.Valid {
		d.AuroraVersion = aurora.String
	}
	var layout pgtype.Int4
	if err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&layout); err != nil {
		return err
	}
	if layout.Valid {
		d.Layout = int(layout.Int32)
	}
	return nil
}

func (s *Store) diagnoseNamespace(ctx context.Context, n NamespaceInfo, d *Diagnosis) error {
	switch n.Kind {
	case ns.Bucket:
		d.Buckets++
	case ns.Queue:
		d.Queues++
	case ns.Cache, ns.WriteBack:
		d.Caches++
	}
	q := quoteIdent(n.Schema)
	var present bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regnamespace($1) IS NOT NULL", n.Schema).Scan(&present); err != nil {
		return err
	}
	if !present {
		return nil
	}
	if n.Kind == ns.Queue {
		hasIndex, err := relationExists(ctx, s.pool, q+".jobs_claim")
		if err != nil || !hasIndex {
			d.ChunksConfigured = false
		}
		var jobs int64
		if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM "+q+".jobs").Scan(&jobs); err == nil {
			d.Objects += jobs
		}
		return nil
	}
	configured, relkind, err := chunksConfigured(ctx, s.pool, n.Schema)
	if err != nil || !configured {
		d.ChunksConfigured = false
	}
	if relkind != "" {
		d.ChunksRelkind = relkind
	}
	var objs, garb int64
	if err := s.pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM "+q+".objects), (SELECT count(*) FROM "+q+".garbage)").
		Scan(&objs, &garb); err == nil {
		d.Objects += objs
		d.GarbageFiles += garb
	}
	var referenced int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+q+`.garbage g
			WHERE EXISTS (SELECT 1 FROM `+q+`.objects o WHERE o.file_id = g.file_id)
			   OR EXISTS (SELECT 1 FROM `+q+`.objects o WHERE o.parts IS NOT NULL AND o.parts @> ARRAY[g.file_id])`+
		garbageUploadCheck(q, n.Kind)).Scan(&referenced); err == nil {
		d.GarbageReferenced += referenced
	}
	return nil
}

func garbageUploadCheck(q string, kind ns.Kind) string {
	if kind != ns.Bucket {
		return ""
	}
	return " OR EXISTS (SELECT 1 FROM " + q + ".upload_parts p WHERE p.file_id = g.file_id)"
}

func (s *Store) diagnoseSchemas(ctx context.Context, d *Diagnosis) error {
	return s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM pg_namespace p
		  WHERE p.nspname ~ '^s3[bcqw]_[a-z0-9._-]{1,58}$'
		    AND NOT EXISTS (SELECT 1 FROM s3p.namespaces n WHERE n.schema_name = p.nspname)),
		(SELECT count(*) FROM s3p.namespaces n
		  WHERE to_regnamespace(format('%I', n.schema_name)) IS NULL)`).
		Scan(&d.OrphanSchemas, &d.MissingSchemas)
}

type functionContract struct {
	signature string
	result    string
	set       bool
	columns   string
}

const streamBatchColumns = "batch_id bigint,consumer text,member text,start_tick bigint,end_tick bigint,batch_created_at timestamp with time zone,seq bigint,event_id text,type text,payload jsonb,headers jsonb,retry_count integer,created_at timestamp with time zone"

var objectFunctions = []functionContract{
	{signature: "queue_garbage(bigint[])", result: "void"},
	{signature: "publish_value(text,bigint,bytea,bigint,bytea,text,text[],text,text,text,timestamp with time zone,boolean,text,text,boolean,jsonb)", result: "text"},
	{signature: "publish_object(text,bigint,bigint,bytea,text,text[],text,text,text,timestamp with time zone,boolean,text,text,boolean,jsonb)", result: "text"},
	{signature: "put_inline(text,bytea,bigint,bytea,text,text[],text,text,text,text,timestamp with time zone,boolean,text,text,boolean,jsonb)", result: "text"},
	{signature: "expire_objects(integer)", result: "integer"},
	{signature: "commit_part(text,integer,bigint,bigint,bytea,bytea,text)", result: "void"},
	{signature: "expire_uploads(interval,integer)", result: "integer"},
}

var streamFunctions = []functionContract{
	{signature: "stream_new_tick()", result: "bigint"},
	{signature: "stream_latest_tick()", result: "bigint"},
	{signature: "stream_append(text[],text[],text[],text[])", result: "@.stream_events", set: true,
		columns: "seq bigint,segment smallint,event_id text,txid xid8,type text,payload jsonb,headers jsonb,retry_count integer,created_at timestamp with time zone"},
	{signature: "stream_subscribe(text,bigint)", result: "@.stream_subscriptions",
		columns: "consumer text,last_tick bigint,next_tick bigint,created_at timestamp with time zone,updated_at timestamp with time zone"},
	{signature: "stream_batch(bigint)", result: "record", set: true, columns: streamBatchColumns},
	{signature: "stream_receive(text,text,integer)", result: "record", set: true, columns: streamBatchColumns},
	{signature: "stream_ack(bigint)", result: "bigint"},
	{signature: "stream_nack(bigint,text,double precision,text)", result: "text"},
	{signature: "stream_process_retries(integer)", result: "bigint"},
	{signature: "stream_prune_ticks(integer)", result: "bigint"},
	{signature: "stream_rotate()", result: "bigint"},
	{signature: "stream_replay_dead_letter(bigint)", result: "text"},
	{signature: "stream_replay_dead_letters()", result: "bigint"},
}

type functionIssue struct {
	schema, scope, signature, reason string
}

func (i functionIssue) String() string { return i.schema + "." + i.signature + ": " + i.reason }

type rowsQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func functionContractIssues(ctx context.Context, db rowsQuerier) ([]functionIssue, error) {
	var scopes, signatures, results, columns []string
	var sets []bool
	for _, group := range []struct {
		scope string
		items []functionContract
	}{{"global", layoutManifest.functions}, {"object", objectFunctions}, {"queue", streamFunctions}} {
		for _, f := range group.items {
			scopes = append(scopes, group.scope)
			signatures = append(signatures, f.signature)
			results = append(results, f.result)
			sets = append(sets, f.set)
			columns = append(columns, f.columns)
		}
	}
	rows, err := db.Query(ctx, `WITH targets AS (
		SELECT 'global'::text AS scope, 's3p'::text AS schema_name
		UNION ALL SELECT CASE WHEN kind='queue' THEN 'queue' ELSE 'object' END, schema_name FROM s3p.namespaces
	), contracts AS (
		SELECT * FROM unnest($1::text[],$2::text[],$3::text[],$4::boolean[],$5::text[])
		AS f(scope,signature,result,is_set,columns)
	), checked AS (
		SELECT t.schema_name, f.scope, f.signature,
		CASE WHEN p.oid IS NULL THEN 'missing'
		WHEN p.prorettype IS DISTINCT FROM to_regtype(replace(f.result,'@',quote_ident(t.schema_name)))
		  OR p.proretset IS DISTINCT FROM f.is_set THEN 'return type differs from ' || f.result
		WHEN f.columns IS DISTINCT FROM COALESCE(
		  CASE WHEN p.prorettype='record'::regtype THEN
		    (SELECT string_agg(p.proargnames[i] || ' ' || format_type(p.proallargtypes[i],NULL),',' ORDER BY i)
		     FROM generate_subscripts(p.proallargtypes,1) AS i WHERE p.proargmodes[i] IN ('o','t'))
		  ELSE (SELECT string_agg(a.attname || ' ' || format_type(a.atttypid,a.atttypmod),',' ORDER BY a.attnum)
		    FROM pg_type ty JOIN pg_attribute a ON a.attrelid=ty.typrelid
		    WHERE ty.oid=p.prorettype AND a.attnum>0 AND NOT a.attisdropped) END,'')
		THEN 'return columns differ from ' || f.columns END AS reason
		FROM targets t JOIN contracts f USING (scope)
		LEFT JOIN pg_proc p ON p.oid=to_regprocedure(format('%I.%s',t.schema_name,f.signature))
	) SELECT schema_name,scope,signature,reason FROM checked WHERE reason IS NOT NULL ORDER BY 1,3`,
		scopes, signatures, results, sets, columns)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (functionIssue, error) {
		var issue functionIssue
		err := row.Scan(&issue.schema, &issue.scope, &issue.signature, &issue.reason)
		return issue, err
	})
}

func (s *Store) diagnoseFunctionContracts(ctx context.Context, d *Diagnosis) error {
	issues, err := functionContractIssues(ctx, s.pool)
	for _, issue := range issues {
		d.FunctionIssues = append(d.FunctionIssues, issue.String())
		if issue.scope == "queue" && issue.reason == "missing" {
			name, _, _ := strings.Cut(issue.signature, "(")
			d.MissingQueueFuncs = append(d.MissingQueueFuncs, issue.schema+"."+name)
		}
	}
	return err
}

func (s *Store) databaseSize(ctx context.Context) (int64, error) {
	var size int64
	err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&size)
	return size, err
}
