package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adonm/pgvs3/internal/ns"
)

// ErrQuotaExceeded is returned when a namespace is over its configured quota.
var ErrQuotaExceeded = errors.New("namespace quota exceeded")

// NewNamespaceRef validates a kind/name pair.
func NewNamespaceRef(kind ns.Kind, name string) (ns.Kind, string, error) {
	if !kind.Valid() {
		return "", "", ns.ErrUnknownKind
	}
	if !ns.ValidName(name) {
		return "", "", ErrInvalidBucket
	}
	return kind, name, nil
}

// BucketSchema returns the PostgreSQL schema for an S3 bucket name.
func BucketSchema(bucket string) (string, error) { return ns.Bucket.Schema(bucket) }

// NamespaceSchema returns the schema for any namespace kind.
func NamespaceSchema(kind ns.Kind, name string) (string, error) { return kind.Schema(name) }

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func nsSQLIdent(kind ns.Kind, name string) (string, error) {
	sc, err := kind.Schema(name)
	if err != nil {
		return "", err
	}
	return quoteIdent(sc), nil
}

func bucketSQLIdent(bucket string) (string, error) { return nsSQLIdent(ns.Bucket, bucket) }

func nsKey(kind ns.Kind, name string) string { return string(kind) + "\x00" + name }

func (s *Store) known(kind ns.Kind, name string) bool {
	_, ok := s.knownNS.Load(nsKey(kind, name))
	return ok
}

func (s *Store) markKnown(kind ns.Kind, name string) {
	s.knownNS.Store(nsKey(kind, name), struct{}{})
}

func (s *Store) forget(kind ns.Kind, name string) {
	s.knownNS.Delete(nsKey(kind, name))
	s.quota.Delete(nsKey(kind, name))
}

// EnsureNamespace creates (idempotently) a namespace and its schema.
func (s *Store) EnsureNamespace(ctx context.Context, kind ns.Kind, name string, quota *int64, attrs map[string]any) error {
	if _, _, err := NewNamespaceRef(kind, name); err != nil {
		return err
	}
	var attrJSON []byte
	if len(attrs) > 0 {
		var err error
		attrJSON, err = json.Marshal(attrs)
		if err != nil {
			return err
		}
	}
	var sc string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.ensure_namespace($1,$2,$3,$4)",
		string(kind), name, quota, attrJSON).Scan(&sc); err != nil {
		if isInvalidNameErr(err) {
			return ErrInvalidBucket
		}
		return err
	}
	s.knownNS.Store(nsKey(kind, name), struct{}{})
	return nil
}

func isInvalidNameErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22023"
}

// CreateBucket ensures an S3 bucket namespace.
func (s *Store) CreateBucket(ctx context.Context, name string) error {
	return s.EnsureNamespace(ctx, ns.Bucket, name, nil, nil)
}

// EnsureCache ensures a volatile cache namespace.
func (s *Store) EnsureCache(ctx context.Context, name string) error {
	return s.EnsureNamespace(ctx, ns.Cache, name, nil, nil)
}

// EnsureQueue ensures a queue namespace with the given attributes.
func (s *Store) EnsureQueue(ctx context.Context, name string, attrs map[string]any) error {
	return s.EnsureNamespace(ctx, ns.Queue, name, nil, attrs)
}

// NamespaceExists reports whether the namespace is registered.
func (s *Store) NamespaceExists(ctx context.Context, kind ns.Kind, name string) (bool, error) {
	if _, _, err := NewNamespaceRef(kind, name); err != nil {
		return false, err
	}
	var one int
	err := s.pool.QueryRow(ctx, "SELECT 1 FROM s3p.namespaces WHERE kind = $1 AND name = $2", string(kind), name).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) BucketExists(ctx context.Context, name string) (bool, error) {
	return s.NamespaceExists(ctx, ns.Bucket, name)
}

type BucketDeletion int

const (
	BucketDeleted BucketDeletion = iota
	BucketNotEmpty
	BucketNotFound
)

// DropNamespace drops a namespace schema (`DROP SCHEMA CASCADE`). When force
// is false, non-empty namespaces are refused.
func (s *Store) DropNamespace(ctx context.Context, kind ns.Kind, name string, force bool) (BucketDeletion, error) {
	if _, _, err := NewNamespaceRef(kind, name); err != nil {
		return BucketNotFound, err
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.drop_namespace($1,$2,$3)", string(kind), name, force).Scan(&outcome); err != nil {
		return 0, err
	}
	switch outcome {
	case "deleted":
		s.forget(kind, name)
		return BucketDeleted, nil
	case "not_empty":
		return BucketNotEmpty, nil
	default:
		return BucketNotFound, nil
	}
}

func (s *Store) DeleteBucket(ctx context.Context, name string) (BucketDeletion, error) {
	return s.DropNamespace(ctx, ns.Bucket, name, false)
}

func (s *Store) DropBucket(ctx context.Context, name string, force bool) (BucketDeletion, error) {
	return s.DropNamespace(ctx, ns.Bucket, name, force)
}

// CloneNamespace copies a bucket or cache namespace inside PostgreSQL.
func (s *Store) CloneNamespace(ctx context.Context, kind ns.Kind, src, dst string) error {
	if _, _, err := NewNamespaceRef(kind, src); err != nil {
		return err
	}
	if _, _, err := NewNamespaceRef(kind, dst); err != nil {
		return err
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.clone_namespace($1,$2,$3)", string(kind), src, dst).Scan(&outcome); err != nil {
		return err
	}
	switch outcome {
	case "ok":
		s.knownNS.Store(nsKey(kind, dst), struct{}{})
		return nil
	case "not_found":
		return ErrNoBucket
	case "not_empty":
		return ErrBucketNotEmpty
	default:
		return fmt.Errorf("unexpected clone outcome %q", outcome)
	}
}

func (s *Store) CloneBucket(ctx context.Context, src, dst string) error {
	return s.CloneNamespace(ctx, ns.Bucket, src, dst)
}

// NamespaceInfo is one row of the catalog.
type NamespaceInfo struct {
	Kind      ns.Kind        `json:"kind"`
	Name      string         `json:"name"`
	Schema    string         `json:"schema"`
	Quota     *int64         `json:"quota_bytes,omitempty"`
	Attrs     map[string]any `json:"attrs,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

func (s *Store) Namespaces(ctx context.Context, kind ns.Kind) ([]NamespaceInfo, error) {
	q := "SELECT kind, name, schema_name, quota_bytes, attrs, EXTRACT(EPOCH FROM created_at)::float8 FROM s3p.namespaces"
	var args []any
	if kind != "" {
		q += " WHERE kind = $1"
		args = append(args, string(kind))
	}
	q += " ORDER BY kind, name"
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NamespaceInfo
	for rows.Next() {
		var kindS, name, schema string
		var quota pgtype.Int8
		var attrs []byte
		var epoch float64
		if err := rows.Scan(&kindS, &name, &schema, &quota, &attrs, &epoch); err != nil {
			return nil, err
		}
		n := NamespaceInfo{Kind: ns.Kind(kindS), Name: name, Schema: schema, CreatedAt: time.Unix(0, int64(epoch*1e9))}
		if quota.Valid {
			v := quota.Int64
			n.Quota = &v
		}
		if len(attrs) > 0 {
			_ = json.Unmarshal(attrs, &n.Attrs)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// namespaceSchemas returns the schemas of one kind (or all kinds).
func (s *Store) namespaceSchemas(ctx context.Context, kind ns.Kind) ([]string, error) {
	infos, err := s.Namespaces(ctx, kind)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(infos))
	for _, n := range infos {
		out = append(out, n.Schema)
	}
	return out, nil
}

// Migrate upgrades a layout v10 database in place. It returns the number of
// buckets migrated and is a no-op on an already-current database.
func (s *Store) Migrate(ctx context.Context) (int, error) {
	var v pgtype.Int4
	err := s.pool.QueryRow(ctx, "SELECT max(version) FROM s3p.layout").Scan(&v)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("no s3p.layout row: not a pgvs3 database")
	}
	if err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, fmt.Errorf("s3p.layout is empty")
	}
	switch v.Int32 {
	case LayoutVersion:
		return 0, nil
	case 10:
	default:
		return 0, fmt.Errorf("cannot migrate layout v%d to v%d", v.Int32, LayoutVersion)
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", int64(0x70677673)); err != nil {
		return 0, err
	}
	defer conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", int64(0x70677673))
	schema, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return 0, err
	}
	if _, err := conn.Exec(ctx, string(schema)); err != nil {
		return 0, err
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT s3p.migrate_v10()").Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Adopt registers one existing namespace schema. Returns "adopted", "exists"
// or "missing".
func (s *Store) Adopt(ctx context.Context, kind ns.Kind, name string) (string, error) {
	if _, _, err := NewNamespaceRef(kind, name); err != nil {
		return "", err
	}
	var outcome string
	if err := s.pool.QueryRow(ctx, "SELECT s3p.adopt_namespace($1,$2)", string(kind), name).Scan(&outcome); err != nil {
		return "", err
	}
	if outcome == "adopted" || outcome == "exists" {
		s.markKnown(kind, name)
	}
	return outcome, nil
}

// AdoptAll adopts every matching schema (all kinds when kind is empty) and
// prunes catalog rows whose schema has vanished.
func (s *Store) AdoptAll(ctx context.Context, kind ns.Kind) (int, error) {
	var arg any
	if kind != "" {
		if !kind.Valid() {
			return 0, ns.ErrUnknownKind
		}
		arg = string(kind)
	}
	var n int
	if err := s.pool.QueryRow(ctx, "SELECT s3p.adopt_namespaces($1)", arg).Scan(&n); err != nil {
		return 0, err
	}
	s.knownNS.Range(func(k, _ any) bool {
		s.knownNS.Delete(k)
		return true
	})
	infos, err := s.Namespaces(ctx, "")
	if err == nil {
		for _, n := range infos {
			s.markKnown(n.Kind, n.Name)
		}
	}
	return n, nil
}

// SetNamespaceAttrs merges attrs into a namespace.
func (s *Store) SetNamespaceAttrs(ctx context.Context, kind ns.Kind, name string, attrs map[string]any) error {
	b, err := json.Marshal(attrs)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx,
		"UPDATE s3p.namespaces SET attrs = attrs || $3::jsonb WHERE kind = $1 AND name = $2",
		string(kind), name, b)
	return err
}

type quotaState struct {
	checked time.Time
	size    int64
	quota   int64
}

// checkQuota enforces a namespace's quota against a gauge refreshed at most
// once per second, adding the size of the incoming write. This makes the cap
// near-hard (bounded by ~1s of writes) instead of once-per-maintenance-tick.
func (s *Store) checkQuota(ctx context.Context, kind ns.Kind, name string, add int64) bool {
	key := nsKey(kind, name)
	var st quotaState
	if v, ok := s.quota.Load(key); ok {
		st = v.(quotaState)
	}
	if st.checked.IsZero() || time.Since(st.checked) > time.Second {
		var q pgtype.Int8
		err := s.pool.QueryRow(ctx,
			"SELECT quota_bytes FROM s3p.namespaces WHERE kind = $1 AND name = $2",
			string(kind), name).Scan(&q)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return false
		}
		if err != nil || !q.Valid {
			s.quota.Store(key, quotaState{checked: time.Now()})
			return false
		}
		st = quotaState{checked: time.Now(), quota: q.Int64, size: s.namespaceSize(ctx, kind, name)}
		s.quota.Store(key, st)
	}
	if st.quota <= 0 {
		return false
	}
	return st.size+add > st.quota
}

func (s *Store) namespaceSize(ctx context.Context, kind ns.Kind, name string) int64 {
	sc, err := kind.Schema(name)
	if err != nil {
		return 0
	}
	table := "objects"
	if kind == ns.Queue {
		table = "messages"
	}
	var size int64
	err = s.pool.QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)",
		quoteIdent(sc)+"."+table).Scan(&size)
	if err != nil {
		return 0
	}
	return size
}

func (s *Store) setQuotaState(kind ns.Kind, name string, size, quota int64) {
	s.quota.Store(nsKey(kind, name), quotaState{checked: time.Now(), size: size, quota: quota})
}

// checkNamespaceTables validates the per-namespace table settings the gateway
// relies on: one 8,160-byte chunk tuple per page and the required indexes.
func (s *Store) checkNamespaceTables(ctx context.Context, conn *pgxpool.Conn) error {
	infos, err := s.Namespaces(ctx, "")
	if err != nil {
		return err
	}
	for _, n := range infos {
		q := quoteIdent(n.Schema)
		if n.Kind == ns.Queue {
			var present bool
			if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", q+".messages").Scan(&present); err != nil {
				return err
			}
			if !present {
				continue
			}
			for _, rel := range []string{q + ".messages_ready", q + ".messages_receipt"} {
				var exists bool
				if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&exists); err != nil {
					return err
				}
				if !exists {
					return fmt.Errorf("required queue relation %s is missing", rel)
				}
			}
			continue
		}
		var present bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", q+".objects").Scan(&present); err != nil {
			return err
		}
		if !present {
			continue
		}
		var kind string
		var ok bool
		if err := conn.QueryRow(ctx, `SELECT c.relkind::text,
			c.reloptions @> ARRAY['toast_tuple_target=8160',
				'autovacuum_vacuum_scale_factor=0.01',
				'autovacuum_analyze_scale_factor=0.02',
				'autovacuum_vacuum_threshold=1000']
			FROM pg_class c
			JOIN pg_namespace p ON p.oid = c.relnamespace
			WHERE p.nspname = $1 AND c.relname = 'chunks'`, n.Schema).Scan(&kind, &ok); err != nil {
			return err
		}
		if kind != "r" || !ok {
			return fmt.Errorf("%s.chunks lacks the expected inline storage settings", n.Schema)
		}
		for _, rel := range []string{
			q + ".objects", q + ".objects_file_id_idx", q + ".objects_parts_idx",
			q + ".upload_parts", q + ".upload_parts_file_id_idx", q + ".garbage",
		} {
			if n.Kind == ns.Cache && (strings.HasSuffix(rel, "upload_parts") || strings.HasSuffix(rel, "upload_parts_file_id_idx")) {
				continue
			}
			var exists bool
			if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", rel).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("required relation %s is missing", rel)
			}
		}
	}
	return nil
}

type Stats struct {
	Buckets       int64          `json:"buckets"`
	Namespaces    int64          `json:"namespaces"`
	Objects       int64          `json:"objects"`
	Chunks        int64          `json:"chunks"`
	LogicalBytes  int64          `json:"logical_bytes"`
	PhysicalBytes int64          `json:"physical_bytes"`
	DatabaseBytes int64          `json:"database_bytes"`
	GarbageFiles  int64          `json:"garbage_files"`
	OldestGarbage *time.Duration `json:"oldest_garbage,omitempty"`
}

// Stat aggregates every namespace.
func (s *Store) Stat(ctx context.Context) (Stats, error) { return s.StatNamespace(ctx, "", "", "", 0) }

// StatBucket aggregates one bucket, or all namespaces when bucket is empty.
func (s *Store) StatBucket(ctx context.Context, bucket string) (Stats, error) {
	if bucket == "" {
		return s.StatNamespace(ctx, "", "", "", 0)
	}
	return s.StatNamespace(ctx, ns.Bucket, bucket, "", 0)
}

// StatNamespace reports usage for one namespace, one kind, or everything.
func (s *Store) StatNamespace(ctx context.Context, kind ns.Kind, name, prefix string, limit int) (Stats, error) {
	var st Stats
	infos, err := s.Namespaces(ctx, kind)
	if err != nil {
		return st, err
	}
	var maxOldest float64
	var hasOldest bool
	for _, n := range infos {
		if name != "" && n.Name != name {
			continue
		}
		st.Namespaces++
		q := quoteIdent(n.Schema)
		var logical, physical, objs, chunks, garb int64
		var oldest pgtype.Float8
		var err error
		if n.Kind == ns.Queue {
			err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(body_size),0)::int8 FROM `+q+`.messages),
				pg_total_relation_size('`+q+`.messages'),
				(SELECT count(*) FROM `+q+`.messages), 0, 0, NULL`).
				Scan(&logical, &physical, &objs, &chunks, &garb, &oldest)
		} else {
			err = s.pool.QueryRow(ctx, `SELECT
				(SELECT COALESCE(sum(size),0)::int8 FROM `+q+`.objects),
				pg_total_relation_size('`+q+`.objects') + pg_total_relation_size('`+q+`.chunks'),
				(SELECT count(*) FROM `+q+`.objects),
				(SELECT count(*) FROM `+q+`.chunks),
				(SELECT count(*) FROM `+q+`.garbage),
				(SELECT EXTRACT(EPOCH FROM now() - min(queued_at))::float8 FROM `+q+`.garbage)`).
				Scan(&logical, &physical, &objs, &chunks, &garb, &oldest)
		}
		if isMissingBucketSchema(err) {
			continue
		}
		if err != nil {
			return st, err
		}
		st.LogicalBytes += logical
		st.PhysicalBytes += physical
		st.Objects += objs
		st.Chunks += chunks
		st.GarbageFiles += garb
		if n.Kind == ns.Bucket {
			st.Buckets++
		}
		if oldest.Valid && (!hasOldest || oldest.Float64 > maxOldest) {
			maxOldest = oldest.Float64
			hasOldest = true
		}
	}
	if err := s.pool.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&st.DatabaseBytes); err != nil {
		return st, err
	}
	if hasOldest {
		d := time.Duration(maxOldest * float64(time.Second))
		st.OldestGarbage = &d
	}
	return st, nil
}
