package objcache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// Spec is an origin cache configuration persisted inside PostgreSQL in
// `s3p.namespaces.attrs["origin"]` (and mirrored in `<schema>.meta.attrs`).
type Spec struct {
	Scope      string `json:"scope"` // "global" or "bucket"
	Bucket     string `json:"bucket,omitempty"`
	Namespace  string `json:"namespace"`
	TTL        string `json:"ttl,omitempty"`
	Retention  string `json:"retention,omitempty"`
	MaxFills   int    `json:"max_fills,omitempty"`
	ConfigYAML string `json:"config_yaml"`
}

// NewFromSpec builds a Tier from a database-stored Spec.
func NewFromSpec(log *slog.Logger, st *store.Store, spec Spec) (*Tier, error) {
	ttl := time.Minute
	if spec.TTL != "" {
		d, err := time.ParseDuration(spec.TTL)
		if err != nil {
			return nil, fmt.Errorf("invalid origin ttl %q: %w", spec.TTL, err)
		}
		ttl = d
	}
	ret := 24 * time.Hour
	if spec.Retention != "" {
		d, err := time.ParseDuration(spec.Retention)
		if err != nil {
			return nil, fmt.Errorf("invalid origin retention %q: %w", spec.Retention, err)
		}
		ret = d
	}
	return New(log, []byte(spec.ConfigYAML), st, spec.Namespace, ttl, ret, spec.MaxFills)
}

// SaveSpec validates and persists an origin configuration in PostgreSQL so any
// gateway connected to the database automatically serves the cache tier without
// external config files. When bucket is empty, the origin applies globally
// across all buckets; otherwise it applies only to the named bucket.
func SaveSpec(ctx context.Context, st *store.Store, bucket string, spec Spec) error {
	if _, err := parseConfig([]byte(spec.ConfigYAML)); err != nil {
		return fmt.Errorf("parse origin config: %w", err)
	}
	if spec.Namespace == "" {
		if bucket != "" {
			spec.Namespace = "origin-" + bucket
		} else {
			spec.Namespace = "origin"
		}
	}
	if !ns.ValidName(spec.Namespace) {
		return fmt.Errorf("invalid cache namespace %q", spec.Namespace)
	}
	if err := st.EnsureNamespace(ctx, ns.Cache, spec.Namespace, nil, nil); err != nil {
		return err
	}
	targetKind := ns.Cache
	targetName := spec.Namespace
	if bucket != "" {
		if !ns.ValidName(bucket) {
			return fmt.Errorf("invalid bucket name %q", bucket)
		}
		spec.Scope = "bucket"
		spec.Bucket = bucket
		targetKind = ns.Bucket
		targetName = bucket
		if err := st.EnsureNamespace(ctx, ns.Bucket, bucket, nil, nil); err != nil {
			return err
		}
	} else {
		spec.Scope = "global"
	}
	return st.SetNamespaceAttrs(ctx, targetKind, targetName, map[string]any{"origin": spec})
}

// ClearSpec removes a persisted origin configuration from PostgreSQL.
func ClearSpec(ctx context.Context, st *store.Store, bucket, cacheNS string) error {
	kind := ns.Cache
	name := cacheNS
	if name == "" {
		name = "origin"
	}
	if bucket != "" {
		kind = ns.Bucket
		name = bucket
	}
	sc, err := kind.Schema(name)
	if err != nil {
		return err
	}
	if _, err := st.Pool().Exec(ctx,
		"UPDATE s3p.namespaces SET attrs = attrs - 'origin' WHERE kind = $1 AND name = $2",
		string(kind), name); err != nil {
		return err
	}
	_, _ = st.Pool().Exec(ctx, fmt.Sprintf("UPDATE %q.meta SET attrs = attrs - 'origin'", sc))
	return nil
}

// ListSpecs returns every origin Spec currently stored in PostgreSQL.
func ListSpecs(ctx context.Context, st *store.Store) ([]Spec, error) {
	rows, err := st.Pool().Query(ctx, `SELECT kind, name, attrs->'origin'
		FROM s3p.namespaces WHERE attrs ? 'origin' ORDER BY kind, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Spec
	for rows.Next() {
		var kind, name string
		var raw []byte
		if err := rows.Scan(&kind, &name, &raw); err != nil {
			return nil, err
		}
		var sp Spec
		if json.Unmarshal(raw, &sp) != nil || sp.ConfigYAML == "" {
			continue
		}
		if kind == string(ns.Bucket) && sp.Bucket == "" {
			sp.Bucket = name
			sp.Scope = "bucket"
		}
		out = append(out, sp)
	}
	return out, rows.Err()
}

// Registry resolves global and per-bucket origin Tiers from PostgreSQL, caching
// open Tiers in memory and refreshing the catalog at most once per second.
type Registry struct {
	store       *store.Store
	log         *slog.Logger
	mu          sync.Mutex
	defaultTier *Tier
	checked     time.Time
	global      *Tier
	globalKey   string
	buckets     map[string]*Tier
	bucketKeys  map[string]string
}

func NewRegistry(st *store.Store, log *slog.Logger) *Registry {
	return &Registry{
		store:      st,
		log:        log,
		buckets:    map[string]*Tier{},
		bucketKeys: map[string]string{},
	}
}

func (r *Registry) SetDefault(t *Tier) {
	r.mu.Lock()
	r.defaultTier = t
	r.mu.Unlock()
}

func (r *Registry) ForBucket(ctx context.Context, bucket string) *Tier {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.store != nil && (r.checked.IsZero() || time.Since(r.checked) > time.Second) {
		r.refreshLocked(ctx)
	}
	if bucket != "" {
		if t, ok := r.buckets[bucket]; ok {
			return t
		}
	}
	if r.global != nil {
		return r.global
	}
	return r.defaultTier
}

func (r *Registry) refreshLocked(ctx context.Context) {
	r.checked = time.Now()
	specs, err := ListSpecs(ctx, r.store)
	if err != nil {
		return
	}
	var wantGlobal *Spec
	wantBuckets := map[string]Spec{}
	for i := range specs {
		sp := specs[i]
		if sp.Scope == "bucket" || sp.Bucket != "" {
			wantBuckets[sp.Bucket] = sp
		} else if wantGlobal == nil {
			wantGlobal = &sp
		}
	}
	if wantGlobal == nil {
		if r.global != nil {
			_ = r.global.Close()
			r.global = nil
			r.globalKey = ""
		}
	} else {
		raw, _ := json.Marshal(wantGlobal)
		key := string(raw)
		if key != r.globalKey {
			if t, err := NewFromSpec(r.log, r.store, *wantGlobal); err == nil {
				if r.global != nil {
					_ = r.global.Close()
				}
				r.global = t
				r.globalKey = key
			}
		}
	}
	for b, old := range r.buckets {
		if _, ok := wantBuckets[b]; !ok {
			_ = old.Close()
			delete(r.buckets, b)
			delete(r.bucketKeys, b)
		}
	}
	for b, sp := range wantBuckets {
		raw, _ := json.Marshal(sp)
		key := string(raw)
		if r.bucketKeys[b] == key {
			continue
		}
		if t, err := NewFromSpec(r.log, r.store, sp); err == nil {
			if old, ok := r.buckets[b]; ok {
				_ = old.Close()
			}
			r.buckets[b] = t
			r.bucketKeys[b] = key
		}
	}
}
