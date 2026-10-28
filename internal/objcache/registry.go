package objcache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

type Spec struct {
	Scope      string `json:"scope"`
	Bucket     string `json:"bucket,omitempty"`
	Namespace  string `json:"namespace"`
	TTL        string `json:"ttl,omitempty"`
	Retention  string `json:"retention,omitempty"`
	MaxFills   int    `json:"max_fills,omitempty"`
	RangeChunk int64  `json:"range_chunk,omitempty"`
	Config     string `json:"config,omitempty"`
}

func NewFromSpec(log *slog.Logger, st *store.Store, spec Spec, readOnly bool) (*Tier, error) {
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
	t, err := newTier(log, []byte(spec.Config), st, spec.Namespace, ttl, ret, spec.MaxFills, readOnly)
	if err != nil {
		return nil, err
	}
	t.EnableRangeCache(spec.RangeChunk)
	return t, nil
}

func ValidateConfig(config []byte) error {
	c, err := parseConfig(config)
	if err != nil {
		return err
	}
	o, err := newOrigin(c)
	if err != nil {
		return err
	}
	return o.Close()
}

func SaveSpec(ctx context.Context, st *store.Store, bucket string, spec Spec) error {
	if err := ValidateConfig([]byte(spec.Config)); err != nil {
		return fmt.Errorf("invalid origin config: %w", err)
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
	if bucket == "" {
		specs, err := ListSpecs(ctx, st)
		if err != nil {
			return err
		}
		for _, existing := range specs {
			if (existing.Scope == "global" || existing.Bucket == "") && existing.Namespace != spec.Namespace {
				return fmt.Errorf("a global origin already exists in cache namespace %q", existing.Namespace)
			}
		}
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
	return st.RemoveNamespaceAttr(ctx, kind, name, "origin")
}

func ListSpecs(ctx context.Context, st *store.Store) ([]Spec, error) {
	namespaces, err := st.NamespacesWithAttr(ctx, "origin")
	if err != nil {
		return nil, err
	}
	out := make([]Spec, 0, len(namespaces))
	for _, n := range namespaces {
		raw, err := json.Marshal(n.Attrs["origin"])
		if err != nil {
			continue
		}
		var sp Spec
		if json.Unmarshal(raw, &sp) != nil || sp.Config == "" {
			continue
		}
		if n.Kind == ns.Bucket && sp.Bucket == "" {
			sp.Bucket = n.Name
			sp.Scope = "bucket"
		}
		out = append(out, sp)
	}
	return out, nil
}

type Registry struct {
	store       *store.Store
	log         *slog.Logger
	mu          sync.RWMutex
	refreshMu   sync.Mutex
	defaultTier *Tier
	readOnly    bool
	checked     atomic.Int64
	tiers       map[string]registeredTier
}

type registeredTier struct {
	spec Spec
	tier *Tier
}

func NewRegistry(st *store.Store, log *slog.Logger, readOnly bool) *Registry {
	return &Registry{
		store:    st,
		log:      log,
		readOnly: readOnly,
		tiers:    map[string]registeredTier{},
	}
}

func (r *Registry) SetDefault(t *Tier) {
	r.mu.Lock()
	r.defaultTier = t
	r.mu.Unlock()
}

func (r *Registry) ForBucket(ctx context.Context, bucket string) *Tier {
	if r == nil {
		return nil
	}
	r.refreshIfStale(ctx)
	r.mu.RLock()
	defer r.mu.RUnlock()
	if bucket != "" {
		if entry, ok := r.tiers[bucket]; ok {
			return entry.tier
		}
	}
	if entry, ok := r.tiers[""]; ok {
		return entry.tier
	}
	return r.defaultTier
}

func (r *Registry) refreshIfStale(ctx context.Context) {
	if r.store == nil {
		return
	}
	if last := r.checked.Load(); last != 0 && time.Since(time.Unix(0, last)) < time.Second {
		return
	}
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	if last := r.checked.Load(); last != 0 && time.Since(time.Unix(0, last)) < time.Second {
		return
	}
	r.checked.Store(time.Now().UnixNano())
	specs, err := ListSpecs(ctx, r.store)
	if err != nil {
		return
	}
	wanted := make(map[string]Spec, len(specs))
	for _, sp := range specs {
		if sp.Scope == "bucket" || sp.Bucket != "" {
			wanted[sp.Bucket] = sp
		} else if _, exists := wanted[""]; !exists {
			wanted[""] = sp
		}
	}
	r.replace(wanted)
}

func (r *Registry) replace(wanted map[string]Spec) {
	r.mu.RLock()
	current := make(map[string]registeredTier, len(r.tiers))
	maps.Copy(current, r.tiers)
	r.mu.RUnlock()

	var retired []*Tier
	for bucket, entry := range current {
		if _, ok := wanted[bucket]; !ok {
			retired = append(retired, entry.tier)
		}
	}
	opened := make(map[string]registeredTier, len(wanted))
	for bucket, spec := range wanted {
		if entry, ok := current[bucket]; ok && entry.spec == spec {
			continue
		}
		tier, err := NewFromSpec(r.log, r.store, spec, r.readOnly)
		if err != nil {
			if r.log != nil {
				r.log.Error("opening persisted origin", "bucket", bucket, "cache_namespace", spec.Namespace, "err", err)
			}
			continue
		}
		opened[bucket] = registeredTier{spec: spec, tier: tier}
		if entry, ok := current[bucket]; ok {
			retired = append(retired, entry.tier)
		}
	}

	r.mu.Lock()
	for bucket := range r.tiers {
		if _, ok := wanted[bucket]; !ok {
			delete(r.tiers, bucket)
		}
	}
	maps.Copy(r.tiers, opened)
	r.mu.Unlock()

	for _, tier := range retired {
		_ = tier.Close()
	}
}
