package objcache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

func NewFromSpec(log *slog.Logger, st *store.Store, runtime *Runtime, spec Spec, opts TierOptions) (*Tier, error) {
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
	opts.Namespace, opts.TTL, opts.Retention = spec.Namespace, ttl, ret
	opts.MaxFills, opts.RangeChunk = spec.MaxFills, spec.RangeChunk
	return New(log, []byte(spec.Config), st, runtime, opts)
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
	defaultTier *registeredTier
	runtime     *Runtime
	options     TierOptions
	checked     atomic.Int64
	tiers       map[string]*registeredTier
	closed      bool
}

type registeredTier struct {
	spec    Spec
	tier    *Tier
	users   int
	retired bool
}

func NewRegistry(st *store.Store, log *slog.Logger, runtime *Runtime, opts TierOptions) *Registry {
	return &Registry{
		store:   st,
		log:     log,
		runtime: runtime,
		options: opts,
		tiers:   map[string]*registeredTier{},
	}
}

func (r *Registry) WriteMetrics(w io.Writer) { r.runtime.WriteMetrics(w) }

func (r *Registry) SetDefault(t *Tier) {
	r.mu.Lock()
	old := r.defaultTier
	if old != nil && old.tier == t {
		r.mu.Unlock()
		return
	}
	if r.closed {
		r.mu.Unlock()
		if t != nil {
			_ = t.Close()
		}
		return
	}
	r.defaultTier = nil
	if t != nil {
		r.defaultTier = &registeredTier{tier: t}
	}
	r.mu.Unlock()
	r.retire(old)
}

func (r *Registry) Acquire(ctx context.Context, bucket string) (*Tier, func()) {
	if r == nil {
		return nil, nil
	}
	r.refreshIfStale(ctx)
	r.mu.Lock()
	entry := r.tiers[bucket]
	if entry == nil {
		entry = r.tiers[""]
	}
	if entry == nil {
		entry = r.defaultTier
	}
	if entry == nil || r.closed {
		r.mu.Unlock()
		return nil, nil
	}
	entry.users++
	r.mu.Unlock()
	var once sync.Once
	return entry.tier, func() {
		once.Do(func() {
			r.mu.Lock()
			entry.users--
			closeTier := entry.retired && entry.users == 0
			r.mu.Unlock()
			if closeTier {
				_ = entry.tier.Close()
			}
		})
	}
}

func (r *Registry) retire(entry *registeredTier) {
	if entry == nil {
		return
	}
	r.mu.Lock()
	entry.retired = true
	closeTier := entry.users == 0
	r.mu.Unlock()
	if closeTier {
		_ = entry.tier.Close()
	}
}

func (r *Registry) Close() {
	r.mu.Lock()
	r.closed = true
	retired := []*registeredTier{r.defaultTier}
	for _, entry := range r.tiers {
		retired = append(retired, entry)
	}
	r.defaultTier, r.tiers = nil, nil
	r.mu.Unlock()
	for _, entry := range retired {
		r.retire(entry)
	}
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
	if r.closed {
		r.mu.RUnlock()
		return
	}
	current := make(map[string]*registeredTier, len(r.tiers))
	maps.Copy(current, r.tiers)
	r.mu.RUnlock()

	var retired []*registeredTier
	for bucket, entry := range current {
		if _, ok := wanted[bucket]; !ok {
			retired = append(retired, entry)
		}
	}
	opened := make(map[string]*registeredTier, len(wanted))
	for bucket, spec := range wanted {
		if entry, ok := current[bucket]; ok && entry.spec == spec {
			continue
		}
		tier, err := NewFromSpec(r.log, r.store, r.runtime, spec, r.options)
		if err != nil {
			if r.log != nil {
				r.log.Error("opening persisted origin", "bucket", bucket, "cache_namespace", spec.Namespace, "err", err)
			}
			continue
		}
		opened[bucket] = &registeredTier{spec: spec, tier: tier}
		if entry, ok := current[bucket]; ok {
			retired = append(retired, entry)
		}
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		for _, entry := range opened {
			_ = entry.tier.Close()
		}
		return
	}
	for bucket := range r.tiers {
		if _, ok := wanted[bucket]; !ok {
			delete(r.tiers, bucket)
		}
	}
	maps.Copy(r.tiers, opened)
	r.mu.Unlock()

	for _, entry := range retired {
		r.retire(entry)
	}
}
