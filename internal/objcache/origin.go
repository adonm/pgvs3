package objcache

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
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
	case "http", "https":
		return newHTTPBucket(c)
	case "filesystem":
		return newFSBucket(c)
	case "inmem":
		return newMemBucket(), nil
	default:
		return nil, fmt.Errorf("unsupported origin type %q (s3, http, filesystem or inmem)", c.Type)
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

type httpConfig struct {
	BaseURL     string            `yaml:"base_url"`
	Headers     map[string]string `yaml:"headers"`
	StripBucket bool              `yaml:"strip_bucket"`
	ReadOnly    *bool             `yaml:"read_only"`
	Insecure    bool              `yaml:"insecure"`
}

type httpStatusError struct {
	status int
	msg    string
}

func (e *httpStatusError) Error() string { return e.msg }

type httpBucket struct {
	base        *url.URL
	headers     map[string]string
	stripBucket bool
	readOnly    bool
	client      *http.Client
}

func newHTTPBucket(c Config) (*httpBucket, error) {
	raw, err := yaml.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	var hc httpConfig
	if err := yaml.Unmarshal(raw, &hc); err != nil {
		return nil, err
	}
	if hc.BaseURL == "" {
		return nil, fmt.Errorf("http origin requires base_url")
	}
	u, err := url.Parse(hc.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("http origin base_url must be an absolute http(s) URL")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if hc.Insecure {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{}
		}
		tr.TLSClientConfig.InsecureSkipVerify = true
	}
	readOnly := true
	if hc.ReadOnly != nil {
		readOnly = *hc.ReadOnly
	}
	return &httpBucket{
		base: u, headers: hc.Headers, stripBucket: hc.StripBucket,
		readOnly: readOnly, client: &http.Client{Transport: tr, Timeout: 5 * time.Minute},
	}, nil
}

func (b *httpBucket) urlFor(key string) (*url.URL, error) {
	key = strings.TrimPrefix(key, "/")
	if b.stripBucket {
		if i := strings.IndexByte(key, '/'); i >= 0 {
			key = key[i+1:]
		} else {
			key = ""
		}
	}
	u := *b.base
	parts := strings.Split(key, "/")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		clean = append(clean, p)
	}
	if len(clean) == 0 {
		return &u, nil
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.Join(clean, "/")
	return &u, nil
}

func (b *httpBucket) do(ctx context.Context, method, key string, rangeHeader string) (*http.Response, error) {
	u, err := b.urlFor(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return nil, err
	}
	for k, v := range b.headers {
		req.Header.Set(k, v)
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		defer resp.Body.Close()
		return nil, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin %s %s: %s", method, u, resp.Status)}
	}
	return resp, nil
}

func (b *httpBucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	attrs, err := b.head(ctx, key)
	if err == nil {
		return attrs, nil
	}
	if he, ok := err.(*httpStatusError); ok && he.status == http.StatusNotFound {
		return originAttrs{}, err
	}
	// HEAD is commonly rejected by object stores and static CDNs. A one-byte
	// GET still returns the total size via Content-Range.
	resp, gerr := b.do(ctx, http.MethodGet, key, "bytes=0-0")
	if gerr != nil {
		return originAttrs{}, gerr
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return originAttrs{}, fmt.Errorf("origin GET %s: %s", key, resp.Status)
	}
	a := originAttrs{ETag: strings.Trim(resp.Header.Get("ETag"), `"`), ContentType: resp.Header.Get("Content-Type")}
	if total := contentRangeTotal(resp.Header.Get("Content-Range")); total > 0 {
		a.Size = total
	} else if resp.ContentLength > 0 {
		a.Size = resp.ContentLength
	}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		a.LastModified = t
	}
	return a, nil
}

func (b *httpBucket) head(ctx context.Context, key string) (originAttrs, error) {
	resp, err := b.do(ctx, http.MethodHead, key, "")
	if err != nil {
		return originAttrs{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return originAttrs{}, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin HEAD: %s", resp.Status)}
	}
	if resp.ContentLength < 0 {
		return originAttrs{}, fmt.Errorf("origin HEAD returned no Content-Length")
	}
	a := originAttrs{Size: resp.ContentLength, ETag: strings.Trim(resp.Header.Get("ETag"), `"`), ContentType: resp.Header.Get("Content-Type")}
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		a.LastModified = t
	}
	return a, nil
}

func (b *httpBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := b.do(ctx, http.MethodGet, key, "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin GET: %s", resp.Status)}
	}
	return resp.Body, nil
}

func (b *httpBucket) GetRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return b.Get(ctx, key)
	}
	resp, err := b.do(ctx, http.MethodGet, key, fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusPartialContent {
		return resp.Body, nil
	}
	if resp.StatusCode == http.StatusOK {
		// Some servers ignore Range. Only accept it when the whole object fits
		// in the requested span; otherwise a cache chunk would silently become
		// a multi-gigabyte fill or the wrong bytes for a non-zero offset.
		if offset > 0 || (resp.ContentLength >= 0 && resp.ContentLength > length) {
			resp.Body.Close()
			return nil, fmt.Errorf("origin ignored Range for %s (offset %d, size %d, wanted %d)", key, offset, resp.ContentLength, length)
		}
		return resp.Body, nil
	}
	resp.Body.Close()
	return nil, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin range GET: %s", resp.Status)}
}

func (b *httpBucket) Upload(ctx context.Context, key string, r io.Reader, size int64) error {
	if b.readOnly {
		return fmt.Errorf("http origin is read-only")
	}
	u, err := b.urlFor(key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), r)
	if err != nil {
		return err
	}
	req.ContentLength = size
	for k, v := range b.headers {
		req.Header.Set(k, v)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin PUT: %s", resp.Status)}
	}
	return nil
}

func (b *httpBucket) Delete(ctx context.Context, key string) error {
	if b.readOnly {
		return fmt.Errorf("http origin is read-only")
	}
	u, err := b.urlFor(key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	for k, v := range b.headers {
		req.Header.Set(k, v)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin DELETE: %s", resp.Status)}
	}
	return nil
}

func (b *httpBucket) Iter(_ context.Context, _ string, _ func(string) error, _ bool) error {
	return fmt.Errorf("http origin does not support listing; use an S3 origin for ListObjectsV2")
}

func (b *httpBucket) IsObjNotFoundErr(err error) bool {
	var he *httpStatusError
	return errors.As(err, &he) && he.status == http.StatusNotFound
}

func (b *httpBucket) Close() error { return nil }

func contentRangeTotal(v string) int64 {
	if v == "" {
		return 0
	}
	i := strings.LastIndexByte(v, '/')
	if i < 0 || i+1 >= len(v) {
		return 0
	}
	n, err := strconv.ParseInt(v[i+1:], 10, 64)
	if err != nil {
		return 0
	}
	return n
}

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
