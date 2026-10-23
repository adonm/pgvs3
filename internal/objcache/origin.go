package objcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.yaml.in/yaml/v3"
)

// Config is the origin configuration. The S3 provider is the production
// path; filesystem and in-memory providers are local/test conveniences.
type Config struct {
	Type   string         `yaml:"type"`
	Prefix string         `yaml:"prefix"`
	Config map[string]any `yaml:"config"`
}

func parseConfig(data []byte) (Config, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return Config{}, err
	}
	return c, nil
}

type originBucket interface {
	Upload(ctx context.Context, key string, r io.Reader, size int64) error
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)
	Attributes(ctx context.Context, key string) (originAttrs, error)
	Iter(ctx context.Context, dir string, fn func(string) error, recursive bool) error
	IsObjNotFoundErr(error) bool
	Close() error
}

type originAttrs struct {
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

func newOrigin(c Config) (originBucket, error) {
	switch strings.ToLower(c.Type) {
	case "s3":
		return newS3Bucket(c)
	case "filesystem":
		return newFSBucket(c)
	case "inmem":
		return newMemBucket(), nil
	default:
		return nil, fmt.Errorf("unsupported origin type %q (s3, filesystem or inmem)", c.Type)
	}
}

type s3Config struct {
	Bucket           string `yaml:"bucket"`
	Endpoint         string `yaml:"endpoint"`
	Region           string `yaml:"region"`
	AccessKey        string `yaml:"access_key"`
	SecretKey        string `yaml:"secret_key"`
	SessionToken     string `yaml:"session_token"`
	Insecure         bool   `yaml:"insecure"`
	BucketLookupType string `yaml:"bucket_lookup_type"`
}

type s3Bucket struct {
	client *minio.Client
	bucket string
	prefix string
}

func newS3Bucket(c Config) (*s3Bucket, error) {
	raw, err := yaml.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	var sc s3Config
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		return nil, err
	}
	if sc.Bucket == "" || sc.Endpoint == "" {
		return nil, fmt.Errorf("s3 origin requires bucket and endpoint")
	}
	endpoint := strings.TrimPrefix(strings.TrimPrefix(sc.Endpoint, "https://"), "http://")
	secure := !sc.Insecure && !strings.HasPrefix(sc.Endpoint, "http://")
	lookup := minio.BucketLookupAuto
	switch strings.ToLower(sc.BucketLookupType) {
	case "path", "path-style":
		lookup = minio.BucketLookupPath
	case "virtual-hosted", "virtual-hosted-style":
		lookup = minio.BucketLookupDNS
	}
	var creds *credentials.Credentials
	switch {
	case sc.AccessKey != "":
		creds = credentials.NewStaticV4(sc.AccessKey, sc.SecretKey, sc.SessionToken)
	case os.Getenv("AWS_ACCESS_KEY_ID") != "":
		creds = credentials.NewEnvAWS()
	default:
		creds = credentials.NewIAM("")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        creds,
		Secure:       secure,
		Region:       sc.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &s3Bucket{client: client, bucket: sc.Bucket, prefix: strings.Trim(c.Prefix, "/")}, nil
}

func (b *s3Bucket) key(key string) string {
	if b.prefix == "" {
		return key
	}
	return b.prefix + "/" + key
}

func (b *s3Bucket) Upload(ctx context.Context, key string, r io.Reader, size int64) error {
	_, err := b.client.PutObject(ctx, b.bucket, b.key(key), r, size, minio.PutObjectOptions{})
	return err
}

func (b *s3Bucket) Delete(ctx context.Context, key string) error {
	return b.client.RemoveObject(ctx, b.bucket, b.key(key), minio.RemoveObjectOptions{})
}

func (b *s3Bucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	return b.client.GetObject(ctx, b.bucket, b.key(key), minio.GetObjectOptions{})
}

func (b *s3Bucket) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if length > 0 {
		opts.SetRange(offset, offset+length-1)
	}
	return b.client.GetObject(ctx, b.bucket, b.key(key), opts)
}

func (b *s3Bucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	info, err := b.client.StatObject(ctx, b.bucket, b.key(key), minio.StatObjectOptions{})
	if err != nil {
		return originAttrs{}, err
	}
	return originAttrs{
		Size: info.Size, ETag: strings.Trim(info.ETag, `"`),
		LastModified: info.LastModified, ContentType: info.ContentType,
	}, nil
}

func (b *s3Bucket) Iter(ctx context.Context, dir string, fn func(string) error, recursive bool) error {
	for info := range b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{
		Prefix: b.key(dir), Recursive: recursive,
	}) {
		if info.Err != nil {
			return info.Err
		}
		if strings.HasSuffix(info.Key, "/") {
			continue
		}
		if err := fn(info.Key); err != nil {
			return err
		}
	}
	return nil
}

func (b *s3Bucket) IsObjNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	resp := minio.ToErrorResponse(err)
	return resp.Code == "NoSuchKey" || resp.Code == "NoSuchBucket" || errors.Is(err, fs.ErrNotExist)
}

func (b *s3Bucket) Close() error { return nil }

type fsConfig struct {
	Directory string `yaml:"directory"`
}

type fsBucket struct {
	root   *os.Root
	prefix string
}

func newFSBucket(c Config) (*fsBucket, error) {
	raw, err := yaml.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	var fc fsConfig
	if err := yaml.Unmarshal(raw, &fc); err != nil {
		return nil, err
	}
	if fc.Directory == "" {
		return nil, fmt.Errorf("filesystem origin requires directory")
	}
	if err := os.MkdirAll(fc.Directory, 0o755); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(fc.Directory)
	if err != nil {
		return nil, err
	}
	return &fsBucket{root: root, prefix: strings.Trim(c.Prefix, "/")}, nil
}

func (b *fsBucket) key(key string) string {
	if b.prefix == "" {
		return key
	}
	return b.prefix + "/" + key
}

func (b *fsBucket) Upload(ctx context.Context, key string, r io.Reader, _ int64) error {
	name := b.key(key)
	if err := b.root.MkdirAll(path.Dir(name), 0o755); err != nil {
		return err
	}
	f, err := b.root.Create(name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		_ = b.root.Remove(name)
		return err
	}
	return f.Close()
}

func (b *fsBucket) Delete(_ context.Context, key string) error {
	err := b.root.Remove(b.key(key))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (b *fsBucket) Get(_ context.Context, key string) (io.ReadCloser, error) {
	return b.root.Open(b.key(key))
}

func (b *fsBucket) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	f, err := b.root.Open(b.key(key))
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &rangeReadCloser{r: io.LimitReader(f, length), c: f}, nil
}

type rangeReadCloser struct {
	r io.Reader
	c io.Closer
}

func (r *rangeReadCloser) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *rangeReadCloser) Close() error               { return r.c.Close() }

func (b *fsBucket) Attributes(_ context.Context, key string) (originAttrs, error) {
	fi, err := b.root.Stat(b.key(key))
	if err != nil {
		return originAttrs{}, err
	}
	return originAttrs{
		Size: fi.Size(), ETag: fmt.Sprintf("%d-%d", fi.Size(), fi.ModTime().UnixNano()),
		LastModified: fi.ModTime(), ContentType: contentTypeFor(key),
	}, nil
}

func (b *fsBucket) Iter(_ context.Context, dir string, fn func(string) error, recursive bool) error {
	dir = strings.TrimSuffix(b.key(dir), "/")
	if dir == "" {
		dir = "."
	}
	err := fs.WalkDir(b.root.FS(), dir, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			if name != dir && !recursive {
				return fs.SkipDir
			}
			return nil
		}
		return fn(name)
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

func (b *fsBucket) IsObjNotFoundErr(err error) bool { return errors.Is(err, fs.ErrNotExist) }
func (b *fsBucket) Close() error                    { return b.root.Close() }

type memObject struct {
	data []byte
	mod  time.Time
}

type memBucket struct {
	mu   sync.Mutex
	data map[string]memObject
}

func newMemBucket() *memBucket { return &memBucket{data: make(map[string]memObject)} }

func (b *memBucket) Upload(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.data[key] = memObject{data: data, mod: time.Now()}
	b.mu.Unlock()
	return nil
}

func (b *memBucket) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.data, key)
	b.mu.Unlock()
	return nil
}

func (b *memBucket) Get(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.Lock()
	o, ok := b.data[key]
	b.mu.Unlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(o.data)), nil
}

func (b *memBucket) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	b.mu.Lock()
	o, ok := b.data[key]
	b.mu.Unlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	if offset < 0 || offset > int64(len(o.data)) {
		return nil, fs.ErrInvalid
	}
	end := min(offset+length, int64(len(o.data)))
	return io.NopCloser(bytes.NewReader(o.data[offset:end])), nil
}

func (b *memBucket) Attributes(_ context.Context, key string) (originAttrs, error) {
	b.mu.Lock()
	o, ok := b.data[key]
	b.mu.Unlock()
	if !ok {
		return originAttrs{}, fs.ErrNotExist
	}
	return originAttrs{
		Size: int64(len(o.data)), ETag: fmt.Sprintf("%d-%d", len(o.data), o.mod.UnixNano()),
		LastModified: o.mod, ContentType: contentTypeFor(key),
	}, nil
}

func (b *memBucket) Iter(_ context.Context, dir string, fn func(string) error, recursive bool) error {
	b.mu.Lock()
	keys := make([]string, 0, len(b.data))
	for key := range b.data {
		keys = append(keys, key)
	}
	b.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasPrefix(key, dir) {
			continue
		}
		if !recursive && strings.Contains(strings.TrimPrefix(key, dir), "/") {
			continue
		}
		if err := fn(key); err != nil {
			return err
		}
	}
	return nil
}

func (b *memBucket) IsObjNotFoundErr(err error) bool { return errors.Is(err, fs.ErrNotExist) }
func (b *memBucket) Close() error                    { return nil }
