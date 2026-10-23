// Package objcache turns pgvs3 into a read-through object cache in front of
// any bucket supported by thanos-io/objstore.
package objcache

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/objstore/providers/s3"
	"golang.org/x/sync/singleflight"
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
	fills     chan struct{}
	group     singleflight.Group
	log       *slog.Logger
}

// memFillMax is the largest origin body buffered in memory before writing to
// PostgreSQL. Larger bodies spool to a temp file so a database connection is
// never held while waiting on the origin network.
const memFillMax = 8 << 20

// fullCacheMax is the largest object downloaded whole to satisfy a range
// request. Above it, ranges are passed through to the origin so a footer read
// never pulls a multi-GB file.
const fullCacheMax = 8 << 20

// Config is the subset of the Thanos objstore config pgvs3 ships with.
type Config struct {
	Type   string      `yaml:"type"`
	Config interface{} `yaml:"config"`
	Prefix string      `yaml:"prefix"`
}

// New builds a Tier. config accepts `type: s3|filesystem|inmem` plus the
// provider's `config:` block, matching Thanos objstore YAML.
func New(log *slog.Logger, config []byte, st *store.Store, namespace string, ttl, retention time.Duration, maxFills int) (*Tier, error) {
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
	if maxFills <= 0 {
		maxFills = 8
	}
	return &Tier{
		origin:    b,
		store:     st,
		namespace: namespace,
		ttl:       ttl,
		retention: retention,
		fills:     make(chan struct{}, maxFills),
		log:       log,
	}, nil
}

func (t *Tier) Name() string                  { return t.namespace }
func (t *Tier) Origin() objstore.Bucket       { return t.origin }
func (t *Tier) Close() error                  { return t.origin.Close() }
func (t *Tier) key(bucket, key string) string { return bucket + "/" + key }

// Object is one origin response, served from the cache or passed through.
type Object struct {
	Size         int64
	ETag         string
	ContentType  string
	UserMetadata []string
	LastModified time.Time
	Body         io.ReadCloser
	Start, End   int64
	Ranged       bool
	Hit          bool
}

// ContentLength returns the number of response payload bytes.
func (o *Object) ContentLength() int64 {
	if o.Size == 0 {
		return 0
	}
	return o.End - o.Start + 1
}

// Ensure returns fresh metadata for bucket/key, downloading from the origin
// when the cached copy is missing, stale or a different version.
func (t *Tier) Ensure(ctx context.Context, bucket, key string) (*store.Meta, error) {
	ck := t.key(bucket, key)
	m, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck)
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
	version := versionFor(attrs)
	if err == nil && m.Size == attrs.Size && m.OriginETag == version {
		t.touch(ctx, ck)
		m.CreatedAt = time.Now()
		return m, nil
	}
	return t.fill(ctx, ck, key, attrs.Size, version)
}

// Head returns metadata for bucket/key without downloading the origin body on a
// cache miss.
func (t *Tier) Head(ctx context.Context, bucket, key string) (*Object, error) {
	ck := t.key(bucket, key)
	m, err := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if err == nil && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl {
		return &Object{
			Size: m.Size, ETag: m.ETag, ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: m.CreatedAt,
			End: max(m.Size-1, 0), Hit: true,
		}, nil
	}
	attrs, aerr := t.origin.Attributes(ctx, ck)
	if aerr != nil {
		if t.origin.IsObjNotFoundErr(aerr) {
			return nil, store.ErrNotFound
		}
		return nil, aerr
	}
	version := versionFor(attrs)
	if err == nil && m.Size == attrs.Size && m.OriginETag == version {
		t.touch(ctx, ck)
		return &Object{
			Size: m.Size, ETag: m.ETag, ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: time.Now(),
			End: max(m.Size-1, 0), Hit: true,
		}, nil
	}
	return &Object{
		Size: attrs.Size, ETag: etagFor(version), ContentType: contentTypeFor(key),
		LastModified: attrs.LastModified, End: max(attrs.Size-1, 0),
	}, nil
}

// Open serves [first,last] (or the whole object) with a cache hit, a whole
// object download, or a pass-through origin range. Warm cache hits within
// --cache-ttl do not contact the origin.
func (t *Tier) Open(ctx context.Context, bucket, key string, first, last, suffix int64) (*Object, error) {
	ck := t.key(bucket, key)
	m, merr := t.store.MetaHeaderKind(ctx, ns.Cache, t.namespace, ck)
	if merr != nil && !errors.Is(merr, store.ErrNotFound) {
		return nil, merr
	}
	valid := merr == nil && t.ttl > 0 && time.Since(m.CreatedAt) < t.ttl

	var (
		size    int64
		version string
		lastMod time.Time
	)
	if valid {
		size = m.Size
		lastMod = m.CreatedAt
	} else {
		attrs, err := t.origin.Attributes(ctx, ck)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		size = attrs.Size
		lastMod = attrs.LastModified
		version = versionFor(attrs)
		if merr == nil {
			if m.Size == size && m.OriginETag == version {
				t.touch(ctx, ck)
				valid = true
			} else {
				// Origin moved on: never serve stale bytes.
				_, _ = t.store.CacheDel(ctx, t.namespace, []string{ck})
				m, merr = nil, store.ErrNotFound
			}
		}
	}

	start, end, ranged := resolveRange(size, first, last, suffix)
	if size > 0 && (start > end || start >= size) {
		return nil, store.ErrInvalidRange
	}
	if size == 0 {
		if !valid {
			var err error
			m, err = t.fill(ctx, ck, key, 0, version)
			if err != nil {
				return nil, err
			}
		}
		return &Object{
			Size: 0, ETag: m.ETag, ContentType: m.ContentType,
			UserMetadata: m.UserMetadata, LastModified: m.CreatedAt,
			Body: io.NopCloser(bytes.NewReader(nil)), Hit: valid,
		}, nil
	}

	length := end - start + 1
	if valid || length == size || size <= fullCacheMax {
		if !valid {
			if _, err := t.fill(ctx, ck, key, size, version); err != nil {
				return nil, err
			}
		}
		sm, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, start, end, -1, nil)
		if err != nil {
			return nil, err
		}
		return &Object{
			Size: sm.Size, ETag: sm.ETag, ContentType: sm.ContentType,
			UserMetadata: sm.UserMetadata, LastModified: sm.CreatedAt,
			Body: body, Start: sm.Start, End: sm.End,
			Ranged: ranged, Hit: valid,
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
		LastModified: lastMod, Body: rc, Start: start, End: end, Ranged: ranged,
	}, nil
}

// fill downloads ck from the origin under the fill semaphore, stores it in the
// cache namespace with version, and returns the stored metadata. Concurrent
// fills for the same ck coalesce via singleflight so a thundering herd on a
// cold key pulls from the origin only once.
func (t *Tier) fill(ctx context.Context, ck, key string, _ int64, version string) (*store.Meta, error) {
	ch := t.group.DoChan(ck, func() (any, error) {
		fillCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
		defer cancel()
		select {
		case t.fills <- struct{}{}:
			defer func() { <-t.fills }()
		case <-fillCtx.Done():
			return nil, fillCtx.Err()
		}
		rc, err := t.origin.Get(fillCtx, ck)
		if err != nil {
			if t.origin.IsObjNotFoundErr(err) {
				return nil, store.ErrNotFound
			}
			return nil, err
		}
		sp, cleanup, err := spoolStream(rc)
		_ = rc.Close()
		if err != nil {
			return nil, err
		}
		defer cleanup()
		return t.storeSpooled(fillCtx, ck, nil, contentTypeFor(key), version, sp)
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*store.Meta), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type spooled struct {
	r    io.ReadSeeker
	data []byte
	size int64
}

// spoolStream buffers r into memory (<= memFillMax) or a temporary file so no
// PostgreSQL connection is held during network I/O.
func spoolStream(r io.Reader) (spooled, func(), error) {
	head, err := io.ReadAll(io.LimitReader(r, memFillMax+1))
	if err != nil {
		return spooled{}, nil, err
	}
	if len(head) <= memFillMax {
		return spooled{
			r:    bytes.NewReader(head),
			data: head,
			size: int64(len(head)),
		}, func() {}, nil
	}
	tmp, err := os.CreateTemp("", "pgvs3-fill-*")
	if err != nil {
		return spooled{}, nil, err
	}
	name := tmp.Name()
	clean := func() { _ = tmp.Close(); _ = os.Remove(name) }
	if _, err := tmp.Write(head); err != nil {
		clean()
		return spooled{}, nil, err
	}
	n, err := io.Copy(tmp, r)
	if err != nil {
		clean()
		return spooled{}, nil, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		clean()
		return spooled{}, nil, err
	}
	return spooled{r: tmp, size: int64(len(head)) + n}, clean, nil
}

func (t *Tier) storeSpooled(ctx context.Context, ck string, userMeta []string, ct, version string, sp spooled) (*store.Meta, error) {
	exp := time.Now().Add(t.retention)
	if sp.data != nil && int64(len(sp.data)) <= int64(ns.Cache.InlineMax()) {
		return t.store.PutInlineKind(ctx, ns.Cache, t.namespace, ck, sp.data,
			store.Unconditional, "", userMeta, ct, &exp, false, version)
	}
	if _, err := sp.r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	w, err := t.store.StartObjectKind(ctx, ns.Cache, t.namespace, ck,
		store.Unconditional, "", userMeta, ct, &exp, false, version)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(w, sp.r); err != nil {
		w.Abort()
		return nil, err
	}
	pub, err := w.Finish(nil)
	if err != nil {
		return nil, err
	}
	return &store.Meta{
		FileID: pub.FileID, Size: pub.Size, SHA256: pub.SHA256, ETag: pub.ETag,
		UserMetadata: userMeta, ContentType: ct, CreatedAt: time.Now(),
		ExpiresAt: &exp, OriginETag: version,
	}, nil
}

// Upload spools the body, writes it through to the origin, and populates the
// PostgreSQL cache so other cache consumers immediately read a cache hit
// without re-pulling from the origin.
func (t *Tier) Upload(ctx context.Context, bucket, key string, body io.Reader, userMeta []string, contentType string) (*store.Meta, error) {
	ck := t.key(bucket, key)
	sp, cleanup, err := spoolStream(body)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	if err := t.origin.Upload(ctx, ck, sp.r); err != nil {
		return nil, err
	}
	var version string
	if attrs, err := t.origin.Attributes(ctx, ck); err == nil {
		version = versionFor(attrs)
	}
	return t.storeSpooled(ctx, ck, userMeta, contentType, version, sp)
}

// Copy copies srcBucket/srcKey to dstBucket/dstKey with write-through caching:
// the source is read from the cache (or origin if cold) and the destination is
// written to both the origin and the PostgreSQL cache.
func (t *Tier) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, userMeta []string, contentType string, replaceMeta bool) (*store.Meta, error) {
	srcObj, err := t.Open(ctx, srcBucket, srcKey, 0, -1, -1)
	if err != nil {
		return nil, err
	}
	defer srcObj.Body.Close()
	meta, ct := srcObj.UserMetadata, srcObj.ContentType
	if replaceMeta {
		meta, ct = userMeta, contentType
	}
	return t.Upload(ctx, dstBucket, dstKey, srcObj.Body, meta, ct)
}

// CreateUpload starts a write-through multipart upload staged in the cache namespace.
func (t *Tier) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType string, checksumAlg *string) (string, error) {
	return t.store.CreateUploadKind(ctx, ns.Cache, t.namespace, t.key(bucket, key), userMeta, contentType, checksumAlg)
}

// StartPart opens a multipart part writer in the cache namespace.
func (t *Tier) StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32) (*store.ChunkWriter, error) {
	return t.store.StartPartKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key), partNo)
}

// CompleteUpload publishes the multipart object in the PostgreSQL cache and
// writes it through to the origin so both hold the completed object immediately.
func (t *Tier) CompleteUpload(ctx context.Context, uploadID, bucket, key string, listed []store.ListedPart) (*store.Completed, error) {
	ck := t.key(bucket, key)
	exp := time.Now().Add(t.retention)
	done, err := t.store.CompleteUploadKind(ctx, ns.Cache, uploadID, t.namespace, ck, listed, &exp)
	if err != nil {
		return nil, err
	}
	_, body, err := t.store.GetBodyKind(ctx, ns.Cache, t.namespace, ck, 0, -1, -1, nil)
	if err != nil {
		return nil, err
	}
	upErr := t.origin.Upload(ctx, ck, body)
	_ = body.Close()
	if upErr != nil {
		_, _ = t.store.CacheDel(ctx, t.namespace, []string{ck})
		return nil, upErr
	}
	if attrs, err := t.origin.Attributes(ctx, ck); err == nil {
		t.setOriginETag(ctx, ck, versionFor(attrs))
	}
	done.Bucket = bucket
	done.Key = key
	return done, nil
}

// AbortUpload discards an in-progress multipart upload in the cache namespace.
func (t *Tier) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	return t.store.AbortUploadKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key))
}

// ListParts returns one page of an in-progress multipart upload's parts in the cache namespace.
func (t *Tier) ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]store.UploadPartInfo, bool, int32, error) {
	return t.store.ListPartsKind(ctx, ns.Cache, uploadID, t.namespace, t.key(bucket, key), max, marker)
}

func (t *Tier) setOriginETag(ctx context.Context, ck, version string) {
	sc, err := ns.Cache.Schema(t.namespace)
	if err != nil {
		return
	}
	_, _ = t.store.Pool().Exec(ctx,
		`UPDATE "`+sc+`".objects SET origin_etag = $2, created_at = now() WHERE key = $1`, ck, version)
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

// DeleteMany removes each key from the origin and evicts them from the cache in
// a single batch.
func (t *Tier) DeleteMany(ctx context.Context, bucket string, keys []string) error {
	cks := make([]string, 0, len(keys))
	for _, k := range keys {
		ck := t.key(bucket, k)
		if err := t.origin.Delete(ctx, ck); err != nil && !t.origin.IsObjNotFoundErr(err) {
			return err
		}
		cks = append(cks, ck)
	}
	_, _ = t.store.CacheDel(ctx, t.namespace, cks)
	return nil
}

// Exists reports whether the object exists at the origin.
func (t *Tier) Exists(ctx context.Context, bucket, key string) (bool, error) {
	return t.origin.Exists(ctx, t.key(bucket, key))
}

// List returns up to limit origin keys under prefix in lexical order, using
// cached metadata when fresh so warm keys avoid per-key origin HEAD calls.
func (t *Tier) List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	if limit <= 0 {
		return nil, nil
	}
	pfx := bucket + "/" + prefix
	var rels []string
	err := t.origin.Iter(ctx, pfx, func(name string) error {
		if strings.HasSuffix(name, "/") {
			return nil
		}
		rel := strings.TrimPrefix(name, bucket+"/")
		if rel > after {
			rels = append(rels, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(rels)
	if len(rels) > limit {
		rels = rels[:limit]
	}
	cached := map[string]store.Listed{}
	if t.store != nil && t.ttl > 0 && len(rels) > 0 {
		rows, err := t.store.ListKind(ctx, ns.Cache, t.namespace, pfx, pfx+"\U0010FFFF", bucket+"/"+after, len(rels)*2)
		if err == nil {
			for _, r := range rows {
				if time.Since(r.CreatedAt) < t.ttl {
					cached[strings.TrimPrefix(r.Key, bucket+"/")] = r
				}
			}
		}
	}
	out := make([]store.Listed, 0, len(rels))
	for _, rel := range rels {
		if c, ok := cached[rel]; ok {
			c.Key = rel
			out = append(out, c)
			continue
		}
		attrs, err := t.origin.Attributes(ctx, bucket+"/"+rel)
		if err != nil {
			continue
		}
		out = append(out, store.Listed{
			Key: rel, Size: attrs.Size, ETag: etagFor(versionFor(attrs)), CreatedAt: attrs.LastModified,
		})
	}
	return out, nil
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

func versionFor(attrs objstore.ObjectAttributes) string {
	return fmt.Sprintf("%d-%d", attrs.Size, attrs.LastModified.UnixNano())
}

func etagFor(version string) string {
	sum := md5.Sum([]byte(version))
	return hex.EncodeToString(sum[:])
}

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
