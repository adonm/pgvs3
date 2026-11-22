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

	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/adonm/pgvs3/internal/store"
)

var (
	ErrOriginConflict   = errors.New("origin conditional request conflict")
	ErrBucketNotAllowed = errors.New("bucket is not in --buckets allowlist")
)

type S3Config struct {
	Endpoint   string
	Region     string
	Creds      s3client.CredentialsProvider
	Insecure   bool
	PathStyle  bool
	Buckets    []string
	HTTPClient *http.Client
}

type originHandle interface {
	BucketAllowed(bucket string) bool
	Attributes(ctx context.Context, bucket, key string) (store.ObjectMeta, error)
	AttributesIfNoneMatch(ctx context.Context, bucket, key, ifNoneMatch string) (store.ObjectMeta, bool, error)
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
	if !ValidBucketName(bucket) {
		return false
	}
	if len(o.allowed) == 0 {
		return true
	}
	_, ok := o.allowed[bucket]
	return ok
}

func ValidBucketName(name string) bool {
	if len(name) < 1 || len(name) > 58 || strings.Contains(name, "..") {
		return false
	}
	for i := range len(name) {
		c := name[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (i > 0 && (c == '.' || c == '_' || c == '-'))
		if !ok {
			return false
		}
	}
	return true
}

func (o *S3Origin) newBucketClient(bucket string) (*s3client.Bucket, error) {
	return s3client.New(s3client.Config{
		Bucket:     bucket,
		Endpoint:   o.cfg.Endpoint,
		Region:     o.cfg.Region,
		Creds:      o.cfg.Creds,
		Insecure:   o.cfg.Insecure,
		PathStyle:  o.cfg.PathStyle,
		HTTPClient: o.cfg.HTTPClient,
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

func (o *S3Origin) Attributes(ctx context.Context, bucket, key string) (store.ObjectMeta, error) {
	attrs, _, err := o.AttributesIfNoneMatch(ctx, bucket, key, "")
	return attrs, err
}

func (o *S3Origin) AttributesIfNoneMatch(ctx context.Context, bucket, key, ifNoneMatch string) (store.ObjectMeta, bool, error) {
	b, err := o.bucket(bucket)
	if err != nil {
		return store.ObjectMeta{}, false, err
	}
	meta, notModified, err := b.StatIfNoneMatch(ctx, key, ifNoneMatch)
	if err != nil {
		return store.ObjectMeta{}, false, mapOriginError(err)
	}
	if notModified {
		return store.ObjectMeta{}, true, nil
	}
	return attrsFromS3(meta, bucket, key), false, nil
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
	o.cfg.HTTPClient.CloseIdleConnections()
	return nil
}

func attrsFromS3(meta s3client.ObjectInfo, bucket, key string) store.ObjectMeta {
	return store.ObjectMeta{
		Bucket:          bucket,
		Key:             key,
		Size:            meta.Size,
		ETag:            strings.Trim(meta.ETag, `"`),
		LastModified:    meta.LastModified.UTC(),
		ContentType:     meta.ContentType,
		ContentEncoding: meta.ContentEncoding,
		UserMeta:        meta.UserMetadata,
	}
}

func mapOriginError(err error) error {
	switch {
	case s3client.Conflict(err):
		return ErrOriginConflict
	case s3client.Precondition(err):
		return store.ErrPrecondition
	default:
		return err
	}
}

func isObjectNotFound(err error) bool {
	return s3client.NotFound(err)
}
