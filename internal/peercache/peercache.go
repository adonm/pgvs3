package peercache

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/store"
	"github.com/groupcache/groupcache-go/v3"
	"github.com/groupcache/groupcache-go/v3/contrib"
	"github.com/groupcache/groupcache-go/v3/transport"
	"github.com/groupcache/groupcache-go/v3/transport/peer"
)

// Options configures the 3-AZ metadata cache.
type Options struct {
	MainBytes  int64
	HotBytes   int64
	TTL        time.Duration
	Peers      []string
	Self       string
	ListenAddr string
	Secret     string
}

// New returns a local Otter cache when no peers are configured, or a
// groupcache cluster whose owner loads metadata from PostgreSQL once
// cluster-wide.
func New(ctx context.Context, st *store.Store, opts Options, log *slog.Logger) (store.MetaCache, error) {
	if len(opts.Peers) == 0 {
		return store.NewLocalMetaCache(opts.MainBytes + opts.HotBytes), nil
	}
	if opts.TTL <= 0 {
		opts.TTL = 10 * time.Second
	}
	if opts.ListenAddr == "" {
		opts.ListenAddr = "0.0.0.0:8015"
	}
	peers, self, err := normalizePeers(opts.Peers, opts.Self)
	if err != nil {
		return nil, err
	}
	if err := validateListen(opts.ListenAddr); err != nil {
		return nil, err
	}
	mainBytes, hotBytes := opts.MainBytes, opts.HotBytes
	if mainBytes < 1<<20 {
		mainBytes = 1 << 20
	}
	if hotBytes < 1<<20 {
		hotBytes = 1 << 20
	}

	client := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &secretTransport{
			base:   http.DefaultTransport,
			secret: opts.Secret,
		},
	}
	tr := transport.NewHttpTransport(transport.HttpTransportOptions{
		Client: client,
		Logger: log,
	})
	// groupcache hardcodes hot = total/8. The factory is called once for main
	// and once for hot, in that order, so size each Otter cache explicitly.
	var calls atomic.Int32
	factory := func(int64) (groupcache.Cache, error) {
		if calls.Add(1)%2 == 1 {
			return contrib.NewOtterCache(mainBytes)
		}
		return contrib.NewOtterCache(hotBytes)
	}
	inst := groupcache.New(groupcache.Options{
		Transport:    tr,
		CacheFactory: factory,
		Logger:       log,
	})
	getter := groupcache.GetterFunc(func(ctx context.Context, key string, dest transport.Sink) error {
		bucket, objectKey, ok := strings.Cut(key, "\x00")
		if !ok {
			return fmt.Errorf("malformed metadata cache key")
		}
		m, err := st.FreshMeta(ctx, bucket, objectKey)
		if errors.Is(err, store.ErrNotFound) {
			return &transport.ErrNotFound{Msg: "object not found"}
		}
		if err != nil {
			return err
		}
		b, err := encodeMeta(m)
		if err != nil {
			return err
		}
		return dest.SetBytes(b, time.Now().Add(opts.TTL))
	})
	group, err := inst.NewGroup("pgvs3-meta", mainBytes+hotBytes, getter)
	if err != nil {
		return nil, err
	}
	infos := make([]peer.Info, 0, len(peers))
	for _, p := range peers {
		infos = append(infos, peer.Info{Address: p, IsSelf: p == self})
	}
	if err := inst.SetPeers(ctx, infos); err != nil {
		return nil, err
	}

	c := &groupMetaCache{
		store: st, group: group, log: log, ttl: opts.TTL,
		queue: make(chan string, 4096),
	}
	for i := 0; i < 4; i++ {
		c.wg.Add(1)
		go c.invalidateWorker()
	}
	handler := http.Handler(tr)
	if opts.Secret != "" {
		handler = secretHandler{next: handler, secret: opts.Secret}
	}
	c.server = &http.Server{
		Addr:              opts.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		log.Info("peer cache listening", "addr", opts.ListenAddr, "peers", len(peers))
		if err := c.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("peer cache server", "err", err)
		}
	}()
	return c, nil
}

type groupMetaCache struct {
	store  *store.Store
	group  groupcache.Group
	log    *slog.Logger
	ttl    time.Duration
	queue  chan string
	wg     sync.WaitGroup
	once   sync.Once
	server *http.Server
}

func (c *groupMetaCache) Get(ctx context.Context, bucket, key string) (store.Meta, bool, error) {
	var b []byte
	err := c.group.Get(ctx, cacheKey(bucket, key), transport.AllocatingByteSliceSink(&b))
	if err != nil {
		if errors.Is(err, &transport.ErrNotFound{}) {
			return store.Meta{}, false, store.ErrNotFound
		}
		// A peer problem must not take the gateway down: fall back to the
		// authoritative database on the next Meta call.
		c.log.Debug("peer cache get failed", "bucket", bucket, "key", key, "err", err)
		return store.Meta{}, false, nil
	}
	m, err := decodeMeta(b)
	if err != nil {
		return store.Meta{}, false, nil
	}
	return m, true, nil
}

// Put is a no-op: a write only invalidates; the owner fills the value on the
// next read. Broadcasting every PUT would put cross-AZ traffic on the write
// path and pollute peers' main caches with keys they do not own.
func (c *groupMetaCache) Put(context.Context, string, string, store.Meta) {}

// Invalidate removes the key locally and from peers. It is queued so a write
// never waits for a cross-AZ round trip; metadata verifies the object's file
// ID on read, so a stale entry can only cause one retry, never stale bytes.
func (c *groupMetaCache) Invalidate(_ context.Context, bucket, key string) {
	k := cacheKey(bucket, key)
	select {
	case c.queue <- k:
	default:
		// The TTL bounds staleness if the queue is saturated.
	}
}

func (c *groupMetaCache) invalidateWorker() {
	defer c.wg.Done()
	for k := range c.queue {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		if err := c.group.Remove(ctx, k); err != nil {
			c.log.Debug("peer cache invalidation failed", "key", k, "err", err)
		}
		cancel()
	}
}

func (c *groupMetaCache) Close() {
	c.once.Do(func() {
		close(c.queue)
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = c.server.Shutdown(shutdown)
		cancel()
		c.wg.Wait()
	})
}

func cacheKey(bucket, key string) string { return bucket + "\x00" + key }

func normalizePeers(in []string, self string) ([]string, string, error) {
	var out []string
	seen := map[string]bool{}
	for _, p := range in {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "http://")
		p = strings.TrimPrefix(p, "https://")
		p = strings.TrimSuffix(p, "/")
		if p == "" || seen[p] {
			continue
		}
		if _, _, err := net.SplitHostPort(p); err != nil {
			return nil, "", fmt.Errorf("peer %q must be host:port: %w", p, err)
		}
		seen[p] = true
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, "", fmt.Errorf("no peer addresses")
	}
	self = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(self), "http://"), "https://")
	if self == "" {
		return nil, "", fmt.Errorf("PGVS3_PEER_ADVERTISE is required when peers are configured")
	}
	if !seen[self] {
		return nil, "", fmt.Errorf("PGVS3_PEER_ADVERTISE %q must appear in PGVS3_PEERS", self)
	}
	return out, self, nil
}

func validateListen(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid peer listen address: %w", err)
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			// A specific private interface is fine; only warn about a public
			// address via the caller's network policy.
			return nil
		}
	}
	return nil
}

type secretTransport struct {
	base   http.RoundTripper
	secret string
}

func (t *secretTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.secret == "" {
		return t.base.RoundTrip(r)
	}
	clone := r.Clone(r.Context())
	clone.Header.Set("x-pgvs3-peer-token", t.secret)
	return t.base.RoundTrip(clone)
}

type secretHandler struct {
	next   http.Handler
	secret string
}

func (h secretHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	got := r.Header.Get("x-pgvs3-peer-token")
	if subtle.ConstantTimeCompare([]byte(got), []byte(h.secret)) != 1 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	h.next.ServeHTTP(w, r)
}
