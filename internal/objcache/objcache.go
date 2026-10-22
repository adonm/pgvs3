// Package objcache turns pgvs3 into a read-through object cache in front of
// any bucket supported by thanos-io/objstore.
package objcache

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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

// fullCacheMax is the largest object downloaded whole to satisfy a range
// request. Above it, ranges are passed through to the origin so a footer read
// never pulls a multi-GB file.
const fullCacheMax = 8 << 20

// Object is one origin response, served from the cache or passed through.
type Object struct {
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
	Body         io.ReadCloser
	Start, End   int64
	Ranged       bool
	Hit          bool
}

// Open serves [first,last] (or the whole object) with a cache hit, a whole
// object download, or a pass-through origin range.
func (t *Tier) Open(ctx context.Context, bucket, key string, first, last, suffix int64) (*Object, error) {
	ck := t.key(bucket, key)
	attrs, err := t.origin.Attributes(ctx, ck)
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	size := attrs.Size
	start, end, ranged := resolveRange(size, first, last, suffix)
	if size > 0 && (start > end || start >= size) {
		return nil, store.ErrInvalidRange
	}
	if size == 0 {
		if _, err := t.Ensure(ctx, bucket, key); err != nil {
			return nil, err
		}
		sm, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, 0, -1, -1, nil)
		if err != nil {
			return nil, err
		}
		return &Object{Size: 0, ETag: sm.ETag, ContentType: sm.ContentType,
			LastModified: sm.CreatedAt, Body: body}, nil
	}
	length := end - start + 1
	version := fmt.Sprintf("%d-%d", size, attrs.LastModified.UnixNano())

	m, merr := t.store.MetaKind(ctx, ns.Cache, t.namespace, ck)
	if merr == nil && m.OriginETag != "" && m.OriginETag != version {
		// Origin moved on: never serve stale bytes.
		_, _ = t.store.CacheDel(ctx, t.namespace, []string{ck})
		m, merr = nil, store.ErrNotFound
	}
	fresh := merr == nil && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl
	if fresh || length == size || size <= fullCacheMax {
		if !fresh {
			m, err = t.Ensure(ctx, bucket, key)
			if err != nil {
				return nil, err
			}
		}
		sm, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, start, end, -1, nil)
		if err != nil {
			return nil, err
		}
		return &Object{
			Size: sm.Size, ETag: sm.ETag, ContentType: sm.ContentType,
			LastModified: sm.CreatedAt, Body: body, Start: sm.Start, End: sm.End,
			Ranged: ranged, Hit: true,
		}, nil
	}

	rc, err := t.origin.GetRange(ctx, ck, start, length)
	if err != nil {
		if t.origin.IsObjNotFoundErr(err) {
			return nil, store.ErrNotFound
		}
		return nil, err
	}
	return &Object{
		Size: size, ETag: etagFor(version), ContentType: contentTypeFor(key),
		LastModified: attrs.LastModified, Body: rc, Start: start, End: end, Ranged: ranged,
	}, nil
}

// resolveRange maps S3 range headers onto [start,end].
func resolveRange(size, first, last, suffix int64) (int64, int64, bool) {
	if suffix >= 0 {
		return max(size-suffix, 0), size - 1, true
	}
	start := max(first, 0)
	end := size - 1
	if last >= 0 {
		end = min(last, size-1)
	}
	return start, end, first > 0 || last >= 0
}

func etagFor(version string) string {
	sum := md5.Sum([]byte(version))
	return hex.EncodeToString(sum[:])
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
