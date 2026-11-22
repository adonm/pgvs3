package objcache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/adonm/pgvs3/internal/store"
)

var (
	ErrOriginConflict     = errors.New("origin conditional request conflict")
	ErrOriginUnsupported  = errors.New("origin operation unsupported")
	ErrBucketNotAllowed   = errors.New("bucket is not in --buckets allowlist")
	errOriginPrecondition = errors.New("origin precondition failed")
)

type S3Config struct {
	Endpoint     string
	Region       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Creds        s3client.CredentialsProvider
	Insecure     bool
	PathStyle    bool
	Buckets      []string
	HTTPClient   *http.Client
}

type originAttrs struct {
	Key             string
	Size            int64
	ETag            string
	LastModified    time.Time
	ContentType     string
	ContentEncoding string
	UserMetadata    []string
}

func (a originAttrs) objectInfo() store.ObjectInfo {
	return store.ObjectInfo{
		Size: a.Size, OriginETag: a.ETag, OriginModified: a.LastModified, CreatedAt: a.LastModified,
		ContentType: a.ContentType, ContentEncoding: a.ContentEncoding, UserMetadata: a.UserMetadata,
	}
}

func (a originAttrs) writeOptions() store.WriteOptions {
	return store.WriteOptions{
		ContentType: a.ContentType, ContentEncoding: a.ContentEncoding,
		UserMetadata: a.UserMetadata, OriginModified: a.LastModified,
	}
}

type originHandle interface {
	BucketAllowed(bucket string) bool
	Attributes(ctx context.Context, bucket, key string) (originAttrs, error)
	AttributesIfNoneMatch(ctx context.Context, bucket, key, ifNoneMatch string) (originAttrs, bool, error)
	GetWithAttrs(ctx context.Context, bucket, key string) (io.ReadCloser, originAttrs, error)
	GetRange(ctx context.Context, bucket, key string, offset, length int64, ifMatch string) (io.ReadCloser, error)
	Proxy(ctx context.Context, bucket, key, method string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error)
	Close() error
}

type S3Origin struct {
	cfg     S3Config
	allowed map[string]struct{}
	buckets sync.Map // string -> *s3client.Bucket
	mu      sync.Mutex
}

func NewS3Origin(cfg S3Config) (*S3Origin, error) {
	if (cfg.AccessKey == "") != (cfg.SecretKey == "") {
		return nil, errors.New("s3 origin: access key and secret key must both be set or both be empty")
	}
	if cfg.AccessKey == "" && cfg.SessionToken != "" {
		return nil, errors.New("s3 origin: session token requires access key and secret key")
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Transport: s3client.NewTransport(s3client.TransportOptions{
				Insecure:              cfg.Insecure,
				MaxIdleConnsPerHost:   64,
				ResponseHeaderTimeout: 30 * time.Second,
			}),
		}
	}
	var allowed map[string]struct{}
	if len(cfg.Buckets) > 0 {
		allowed = make(map[string]struct{}, len(cfg.Buckets))
		for _, b := range cfg.Buckets {
			b = strings.TrimSpace(b)
			if b != "" {
				allowed[b] = struct{}{}
			}
		}
	}
	o := &S3Origin{cfg: cfg, allowed: allowed}
	// Validate endpoint/region upfront using a synthetic bucket name.
	if _, err := o.newBucketClient("validate-config"); err != nil {
		return nil, err
	}
	return o, nil
}

func (o *S3Origin) BucketAllowed(bucket string) bool {
	if !ns.ValidName(bucket) {
		return false
	}
	if len(o.allowed) == 0 {
		return true
	}
	_, ok := o.allowed[bucket]
	return ok
}

func (o *S3Origin) newBucketClient(bucket string) (*s3client.Bucket, error) {
	return s3client.New(s3client.Config{
		Bucket:       bucket,
		Endpoint:     o.cfg.Endpoint,
		Region:       o.cfg.Region,
		AccessKey:    o.cfg.AccessKey,
		SecretKey:    o.cfg.SecretKey,
		SessionToken: o.cfg.SessionToken,
		Creds:        o.cfg.Creds,
		Insecure:     o.cfg.Insecure,
		PathStyle:    o.cfg.PathStyle,
		HTTPClient:   o.cfg.HTTPClient,
	})
}

func (o *S3Origin) bucket(name string) (*s3client.Bucket, error) {
	if !o.BucketAllowed(name) {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotAllowed, name)
	}
	if v, ok := o.buckets.Load(name); ok {
		return v.(*s3client.Bucket), nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if v, ok := o.buckets.Load(name); ok {
		return v.(*s3client.Bucket), nil
	}
	b, err := o.newBucketClient(name)
	if err != nil {
		return nil, err
	}
	o.buckets.Store(name, b)
	return b, nil
}

func (o *S3Origin) Attributes(ctx context.Context, bucket, key string) (originAttrs, error) {
	attrs, _, err := o.AttributesIfNoneMatch(ctx, bucket, key, "")
	return attrs, err
}

func (o *S3Origin) AttributesIfNoneMatch(ctx context.Context, bucket, key, ifNoneMatch string) (originAttrs, bool, error) {
	b, err := o.bucket(bucket)
	if err != nil {
		return originAttrs{}, false, err
	}
	meta, notModified, err := b.StatIfNoneMatch(ctx, key, ifNoneMatch)
	if err != nil {
		return originAttrs{}, false, mapOriginError(err)
	}
	if notModified {
		return originAttrs{}, true, nil
	}
	return attrsFromS3(meta, key), false, nil
}

func (o *S3Origin) GetWithAttrs(ctx context.Context, bucket, key string) (io.ReadCloser, originAttrs, error) {
	b, err := o.bucket(bucket)
	if err != nil {
		return nil, originAttrs{}, err
	}
	rc, info, err := b.GetWithInfo(ctx, key, 0, -1, "")
	if err != nil {
		return nil, originAttrs{}, mapOriginError(err)
	}
	return rc, attrsFromS3(info, key), nil
}

func (o *S3Origin) GetRange(ctx context.Context, bucket, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	b, err := o.bucket(bucket)
	if err != nil {
		return nil, err
	}
	rc, err := b.Get(ctx, key, offset, length, ifMatch)
	if err != nil {
		return nil, mapOriginError(err)
	}
	return rc, nil
}

func (o *S3Origin) Proxy(ctx context.Context, bucket, key, method string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error) {
	b, err := o.bucket(bucket)
	if err != nil {
		return nil, err
	}
	return b.Proxy(ctx, method, key, query, body, size, extra...)
}

func (o *S3Origin) Close() error {
	if o.cfg.HTTPClient != nil {
		o.cfg.HTTPClient.CloseIdleConnections()
	}
	return nil
}

func attrsFromS3(meta s3client.ObjectInfo, key string) originAttrs {
	return originAttrs{
		Key:             key,
		Size:            meta.Size,
		ETag:            strings.Trim(meta.ETag, `"`),
		LastModified:    meta.LastModified.UTC(),
		ContentType:     meta.ContentType,
		ContentEncoding: meta.ContentEncoding,
		UserMetadata:    meta.UserMetadata,
	}
}

func mapOriginError(err error) error {
	switch {
	case errors.Is(err, ErrOriginConflict), s3client.Conflict(err):
		return ErrOriginConflict
	case errors.Is(err, errOriginPrecondition), s3client.Precondition(err):
		return errOriginPrecondition
	default:
		return err
	}
}

func isObjectNotFound(err error) bool {
	return s3client.NotFound(err)
}
