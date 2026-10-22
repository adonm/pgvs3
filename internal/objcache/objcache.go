// Package objcache turns pgvs3 into a read-through object cache in front of
// any bucket supported by thanos-io/objstore.
package objcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/s3"
	"gopkg.in/yaml.v2"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// Tier is a caching layer over an origin bucket.
type Tier struct {
	origin    objstore.Bucket
	store     *store.Store
	namespace string
	ttl       time.Duration
	retention time.Duration
	log       *slog.Logger
}

// Config is the subset of the Thanos objstore config pgvs3 ships with.
type Config struct {
	Type   string      `yaml:"type"`
	Config interface{} `yaml:"config"`
	Prefix string      `yaml:"prefix"`
}

// New builds a Tier. config accepts `type: s3|filesystem|inmem` plus the
// provider's `config:` block, matching Thanos objstore YAML.
func New(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration) (*Tier, error) {
	var c Config
	if err := yaml.UnmarshalStrict(config, &c); err != nil {
		return nil, fmt.Errorf("parse origin config: %w", err)
	}
	confYAML, err := yaml.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	var b objstore.Bucket
	switch strings.ToLower(c.Type) {
	case "s3":
		b, err = s3.NewBucket(logAdapter{log}, confYAML, "pgvs3", nil)
	case "filesystem":
		b, err = filesystem.NewBucketFromConfig(confYAML)
	case "inmem":
		b = objstore.NewInMemBucket()
	default:
		return nil, fmt.Errorf("unsupported origin type %q (s3, filesystem or inmem)", c.Type)
	}
	if err != nil {
		return nil, err
	}
	if c.Prefix != "" {
		b = objstore.NewPrefixedBucket(b, c.Prefix)
	}
	if namespace == "" {
		namespace = "origin"
	}
	if !ns.ValidName(namespace) {
		return nil, fmt.Errorf("invalid cache namespace %q", namespace)
	}
	if ttl < 0 {
		ttl = time.Minute
	}
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	return &Tier{origin: b, store: st, namespace: namespace, ttl: ttl, retention: retention, log: log}, nil
}

func (t *Tier) Name() string                  { return t.namespace }
func (t *Tier) Origin() objstore.Bucket       { return t.origin }
func (t *Tier) Close() error                  { return t.origin.Close() }
func (t *Tier) key(bucket, key string) string { return bucket + "/" + key }

// Ensure returns fresh metadata for bucket/key, downloading from the origin
// when the cached copy is missing, stale or a different version.
func (t *Tier) Ensure(ctx context.Context, bucket, key string) (*store.Meta, error) {
	ck := t.key(bucket, key)
	m, err := t.store.MetaKind(ctx, ns.Cache, t.namespace, ck)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if err == nil && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl {
		return m, nil
	}

	attrs, aerr := t.origin.Attributes(ctx, ck)
	if aerr != nil {
		if t.origin.IsObjNotFoundErr(aerr) {
			return nil, store.ErrNotFound
		}
		return nil, aerr
	}
	version := fmt.Sprintf("%d-%d", attrs.Size, attrs.LastModified.UnixNano())
	if err == nil && m.Size == attrs.Size && m.OriginETag == version {
		t.touch(ctx, ck)
		m.CreatedAt = time.Now()
		return m, nil
	}

	rc, gerr := t.origin.Get(ctx, ck)
	if gerr != nil {
		if t.origin.IsObjNotFoundErr(gerr) {
			return nil, store.ErrNotFound
		}
		return nil, gerr
	}
	defer rc.Close()

	exp := time.Now().Add(t.retention)
	ct := contentTypeFor(key)
	w, werr := t.store.StartObjectKind(ctx, ns.Cache, t.namespace, ck,
		store.Unconditional, "", nil, ct, &exp, false, version)
	if werr != nil {
		return nil, werr
	}
	if _, err := io.Copy(w, rc); err != nil {
		w.Abort()
		return nil, err
	}
	if _, err := w.Finish(nil); err != nil {
		return nil, err
	}
	return t.store.MetaKind(ctx, ns.Cache, t.namespace, ck)
}

// OpenRange serves [first,last] from the cached copy, downloading on a miss.
func (t *Tier) OpenRange(ctx context.Context, bucket, key string, first, last, suffix int64) (*store.SliceMeta, *store.Body, error) {
	if _, err := t.Ensure(ctx, bucket, key); err != nil {
		return nil, nil, err
	}
	return t.store.GetBodyKind(ctx, ns.Cache, t.namespace, t.key(bucket, key), first, last, suffix, nil)
}

// Upload writes through to the origin and refreshes the cached copy.
func (t *Tier) Upload(ctx context.Context, bucket, key string, body io.Reader, contentType string) error {
	ck := t.key(bucket, key)
	pr, pw := io.Pipe()
	uploadErr := make(chan error, 1)
	go func() { uploadErr <- t.origin.Upload(ctx, ck, pr) }()

	exp := time.Now().Add(t.retention)
	w, err := t.store.StartObjectKind(ctx, ns.Cache, t.namespace, ck,
		store.Unconditional, "", nil, contentType, &exp, false, "")
	if err != nil {
		_ = pw.CloseWithError(err)
		<-uploadErr
		return err
	}
	_, copyErr := io.Copy(io.MultiWriter(pw, w), body)
	_ = pw.CloseWithError(copyErr)
	upErr := <-uploadErr
	if copyErr != nil {
		w.Abort()
		return copyErr
	}
	if upErr != nil {
		w.Abort()
		return upErr
	}
	if _, err := w.Finish(nil); err != nil {
		return err
	}
	// Record the uploaded version so the next read revalidates cheaply.
	if attrs, err := t.origin.Attributes(ctx, ck); err == nil {
		version := fmt.Sprintf("%d-%d", attrs.Size, attrs.LastModified.UnixNano())
		if sc, err := ns.Cache.Schema(t.namespace); err == nil {
			_, _ = t.store.Pool().Exec(ctx,
				`UPDATE "`+sc+`".objects SET origin_etag = $2 WHERE key = $1`, ck, version)
		}
	}
	return nil
}

// Delete removes the object from the origin and the cache.
func (t *Tier) Delete(ctx context.Context, bucket, key string) error {
	ck := t.key(bucket, key)
	err := t.origin.Delete(ctx, ck)
	if err != nil && !t.origin.IsObjNotFoundErr(err) {
		return err
	}
	_, _ = t.store.CacheDel(ctx, t.namespace, []string{ck})
	return nil
}

// Exists reports whether the object exists at the origin.
func (t *Tier) Exists(ctx context.Context, bucket, key string) (bool, error) {
	return t.origin.Exists(ctx, t.key(bucket, key))
}

// List returns up to limit origin keys under prefix in lexical order.
func (t *Tier) List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	var out []store.Listed
	err := t.origin.Iter(ctx, bucket+"/"+prefix, func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		rel := strings.TrimPrefix(name, bucket+"/")
		if rel <= after {
			return nil
		}
		attrs, err := t.origin.Attributes(ctx, name)
		if err != nil {
			return nil
		}
		out = append(out, store.Listed{Key: rel, Size: attrs.Size, CreatedAt: attrs.LastModified})
		if len(out) >= limit {
			return errStopIter
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopIter) {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

var errStopIter = errors.New("stop iteration")

func (t *Tier) touch(ctx context.Context, ck string) {
	sc, err := ns.Cache.Schema(t.namespace)
	if err != nil {
		return
	}
	_, _ = t.store.Pool().Exec(ctx,
		`UPDATE "`+sc+`".objects SET created_at = now() WHERE key = $1`, ck)
}

func contentTypeFor(key string) string {
	if ct := mime.TypeByExtension(path.Ext(key)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

// logAdapter bridges *slog.Logger to go-kit's log.Logger (what objstore wants).
type logAdapter struct{ l *slog.Logger }

func (a logAdapter) Log(keyvals ...interface{}) error {
	a.l.Debug("objstore", keyvals...)
	return nil
}

var _ log.Logger = logAdapter{}
