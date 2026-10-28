package objcache

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/telemetry"
)

type Config struct {
	Type   string         `json:"type"`
	Prefix string         `json:"prefix,omitempty"`
	Config map[string]any `json:"config,omitempty"`
}

func parseConfig(data []byte) (Config, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		return Config{}, err
	}
	if dec.More() {
		return Config{}, errors.New("origin config contains trailing JSON values")
	}
	return c, nil
}

func decodeConfig[T any](c Config, dst *T) error {
	raw, err := json.Marshal(c.Config)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

var errOriginPrecondition = errors.New("origin precondition failed")

var ErrOriginConflict = errors.New("origin conditional request conflict")

var ErrOriginUnsupported = errors.New("origin operation unsupported")

var ErrOriginListingUnsupported = errors.New("origin does not support listing")

var ErrOriginKeyUnsupported = fmt.Errorf("%w: key cannot be stored", ErrOriginUnsupported)

type originUpload struct {
	UserMetadata    []string
	ContentType     string
	ContentEncoding string
	Checksum        *store.Checksum
	Precondition    store.Precondition
}

func (in originUpload) conditional() bool {
	switch in.Precondition.Mode {
	case store.IfPresent:
		return true
	case store.IfMatch:
		return in.Precondition.ETag != ""
	default:
		return false
	}
}

type originCapabilities uint16

const (
	capRead originCapabilities = 1 << iota
	capWrite
	capList
	capConditional
	capRange
	capMetadata
)

const originCapabilitiesAll = capRead | capWrite | capList | capConditional | capRange | capMetadata

func (c originCapabilities) has(required originCapabilities) bool { return c&required == required }

func (c originCapabilities) String() string {
	if c == 0 {
		return "none"
	}
	names := make([]string, 0, 6)
	for _, feature := range []struct {
		cap  originCapabilities
		name string
	}{
		{capRead, "read"},
		{capWrite, "write"},
		{capList, "list"},
		{capConditional, "conditional"},
		{capRange, "range"},
		{capMetadata, "metadata"},
	} {
		if c.has(feature.cap) {
			names = append(names, feature.name)
		}
	}
	return strings.Join(names, "|")
}

type originAttrs struct {
	Size            int64
	ETag            string
	LastModified    time.Time
	ContentType     string
	ContentEncoding string
	UserMetadata    []string
}

func (a originAttrs) objectInfo() store.ObjectInfo {
	return store.ObjectInfo{
		Size: a.Size, ETag: versionFor(a), OriginETag: versionFor(a),
		ContentType: a.ContentType, ContentEncoding: a.ContentEncoding,
		UserMetadata: a.UserMetadata, CreatedAt: a.LastModified,
	}
}

func (a originAttrs) writeOptions() store.WriteOptions {
	return store.WriteOptions{
		UserMetadata: a.UserMetadata, ContentType: a.ContentType, ContentEncoding: a.ContentEncoding,
		OriginModified: a.LastModified, OriginETag: versionFor(a),
	}
}

type originObject struct {
	Key   string
	Attrs originAttrs
}

type originPrefix string

func newOriginPrefix(prefix string) originPrefix {
	return originPrefix(strings.Trim(prefix, "/"))
}

func (p originPrefix) key(key string) string {
	if p == "" {
		return key
	}
	return string(p) + "/" + key
}

func (p originPrefix) logical(key string) string {
	if p == "" {
		return key
	}
	return strings.TrimPrefix(key, string(p)+"/")
}

type originDriver interface {
	Upload(context.Context, string, io.Reader, int64, originUpload) (originAttrs, error)
	Delete(context.Context, string) error
	GetWithAttrs(context.Context, string) (io.ReadCloser, originAttrs, error)
	GetRange(context.Context, string, int64, int64, string) (io.ReadCloser, error)
	Attributes(context.Context, string) (originAttrs, error)
	Iter(context.Context, string) iter.Seq2[originObject, error]
	Close() error
}

type originHandle interface {
	originDriver
	Get(context.Context, string) (io.ReadCloser, error)
	Capabilities() originCapabilities
	Supports(originCapabilities) bool
	IsObjNotFoundErr(error) bool
	checkKey(string) error
}

type keyChecker interface {
	checkKey(string) error
}

type origin struct {
	kind   string
	caps   originCapabilities
	prefix originPrefix
	keyFn  func(string) string
	driver originDriver
}

func (o *origin) physicalKey(key string) string {
	if o.keyFn != nil {
		key = o.keyFn(key)
	}
	return o.prefix.key(key)
}

func (o *origin) Capabilities() originCapabilities {
	if o == nil {
		return 0
	}
	return o.caps
}

func (o *origin) Supports(required originCapabilities) bool {
	return o != nil && o.caps.has(required)
}

func (o *origin) unsupported(operation string) error {
	return fmt.Errorf("%s origin does not support %s: %w", o.kind, operation, ErrOriginUnsupported)
}

func (o *origin) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	if !o.Supports(capWrite) {
		return originAttrs{}, o.unsupported("writes")
	}
	if in.Precondition.Mode != store.Unconditional && !o.Supports(capConditional) {
		return originAttrs{}, o.unsupported("conditional writes")
	}
	attrs, err := o.driver.Upload(ctx, o.physicalKey(key), r, size, in)
	if err != nil {
		return originAttrs{}, mapOriginError(err, in.conditional())
	}
	return attrs, nil
}

func (o *origin) Delete(ctx context.Context, key string) error {
	if !o.Supports(capWrite) {
		return o.unsupported("deletes")
	}
	return o.driver.Delete(ctx, o.physicalKey(key))
}

func (o *origin) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, _, err := o.GetWithAttrs(ctx, key)
	return rc, err
}

func (o *origin) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	if !o.Supports(capRead) {
		return nil, originAttrs{}, o.unsupported("reads")
	}
	rc, attrs, err := o.driver.GetWithAttrs(ctx, o.physicalKey(key))
	if err != nil {
		return nil, originAttrs{}, mapOriginError(err, false)
	}
	return rc, attrs, nil
}

func (o *origin) GetRange(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	if !o.Supports(capRange) {
		return nil, o.unsupported("ranged reads")
	}
	if ifMatch != "" && !o.Supports(capConditional) {
		return nil, o.unsupported("conditional reads")
	}
	rc, err := o.driver.GetRange(ctx, o.physicalKey(key), offset, length, ifMatch)
	if err != nil {
		return nil, mapOriginError(err, ifMatch != "")
	}
	return rc, nil
}

func (o *origin) Attributes(ctx context.Context, key string) (originAttrs, error) {
	if !o.Supports(capMetadata) {
		return originAttrs{}, o.unsupported("metadata")
	}
	return o.driver.Attributes(ctx, o.physicalKey(key))
}

func (o *origin) Iter(ctx context.Context, dir string) iter.Seq2[originObject, error] {
	if !o.Supports(capList) {
		return func(func(originObject, error) bool) {}
	}
	return func(yield func(originObject, error) bool) {
		for obj, err := range o.driver.Iter(ctx, o.physicalKey(dir)) {
			if err != nil {
				if !yield(originObject{}, err) {
					return
				}
				continue
			}
			obj.Key = o.prefix.logical(obj.Key)
			if !yield(obj, nil) {
				return
			}
		}
	}
}

func (o *origin) IsObjNotFoundErr(err error) bool { return isObjectNotFound(err) }

func (o *origin) checkKey(key string) error {
	c, ok := o.driver.(keyChecker)
	if !ok {
		return nil
	}
	return c.checkKey(o.physicalKey(key))
}

func (o *origin) Close() error {
	return o.driver.Close()
}

func newOrigin(c Config) (*origin, error) {
	switch strings.ToLower(c.Type) {
	case "s3":
		return newS3Bucket(c)
	case "http", "https":
		return newHTTPBucket(c)
	case "filesystem":
		return newFSBucket(c)
	case "ephemeral":
		return newTempBucket(c)
	default:
		return nil, fmt.Errorf("unsupported origin type %q (s3, http, filesystem or ephemeral)", c.Type)
	}
}

type s3Config struct {
	Bucket           string `json:"bucket"`
	Endpoint         string `json:"endpoint"`
	Region           string `json:"region"`
	AccessKey        string `json:"access_key"`
	SecretKey        string `json:"secret_key"`
	SessionToken     string `json:"session_token"`
	Insecure         bool   `json:"insecure"`
	BucketLookupType string `json:"bucket_lookup_type"`
}

type s3Bucket struct {
	client *s3client.Bucket
}

func newS3Bucket(c Config) (*origin, error) {
	var sc s3Config
	if err := decodeConfig(c, &sc); err != nil {
		return nil, err
	}
	pathStyle := false
	switch strings.ToLower(sc.BucketLookupType) {
	case "path", "path-style":
		pathStyle = true
	case "", "auto":
	default:
		return nil, fmt.Errorf("s3 origin: unsupported bucket_lookup_type %q", sc.BucketLookupType)
	}

	prefix := newOriginPrefix(c.Prefix)
	client, err := s3client.New(s3client.Config{
		Endpoint: sc.Endpoint, Region: sc.Region, Bucket: sc.Bucket,
		AccessKey: sc.AccessKey, SecretKey: sc.SecretKey, SessionToken: sc.SessionToken,
		Insecure: sc.Insecure, PathStyle: pathStyle,
	})
	if err != nil {
		return nil, err
	}
	b := &s3Bucket{client: client}
	return &origin{
		kind: "s3", caps: originCapabilitiesAll, prefix: prefix,
		driver: b,
	}, nil
}

func (b *s3Bucket) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	ifMatch, ifNone := "", ""
	switch in.Precondition.Mode {
	case store.IfAbsent:
		ifNone = "*"
	case store.IfPresent:
		ifMatch = "*"
	case store.IfMatch:
		ifMatch = in.Precondition.ETag
	}
	meta, err := b.client.WriteReader(ctx, key, r, size, s3client.PutOptions{
		UserMetadata: in.UserMetadata, ContentType: in.ContentType, ContentEncoding: in.ContentEncoding,
		IfMatch: ifMatch, IfNoneMatch: ifNone,
	})
	if err != nil {
		return originAttrs{}, err
	}
	return attrsFromS3(meta, key), nil
}

func (b *s3Bucket) Delete(ctx context.Context, key string) error {
	return b.client.Delete(ctx, key)
}

func (b *s3Bucket) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	rc, info, err := b.client.GetWithInfo(ctx, key, 0, -1, "")
	if err != nil {
		return nil, originAttrs{}, err
	}
	return rc, attrsFromS3(info, key), nil
}

func (b *s3Bucket) GetRange(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	return b.client.Get(ctx, key, offset, length, ifMatch)
}

func (b *s3Bucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	meta, err := b.client.Stat(ctx, key)
	if err != nil {
		return originAttrs{}, err
	}
	return attrsFromS3(meta, key), nil
}

func (b *s3Bucket) Iter(ctx context.Context, dir string) iter.Seq2[originObject, error] {

	return func(yield func(originObject, error) bool) {
		for obj, err := range b.client.Walk(ctx, dir) {
			if err != nil {
				yield(originObject{}, err)
				return
			}
			if !yield(originObject{Key: obj.Key, Attrs: attrsFromS3(obj, obj.Key)}, nil) {
				return
			}
		}
	}
}

func attrsFromS3(meta s3client.ObjectInfo, key string) originAttrs {
	return originAttrsFor(key, meta.Size, meta.ETag, meta.LastModified, meta.ContentType, meta.ContentEncoding, meta.UserMetadata)
}

func (b *s3Bucket) Close() error { return b.client.Close() }

func originAttrsFor(key string, size int64, etag string, modified time.Time, contentType, contentEncoding string, userMeta []string) originAttrs {
	if contentType == "" {
		contentType = contentTypeFor(key)
	}
	return originAttrs{
		Size: size, ETag: etag, LastModified: modified,
		ContentType: contentType, ContentEncoding: contentEncoding,
		UserMetadata: clonePairs(userMeta),
	}
}

type originErrorClass uint8

const (
	originErrorOther originErrorClass = iota
	originErrorNotFound
	originErrorPrecondition
	originErrorConflict
)

func classifyOriginError(err error) originErrorClass {
	if err == nil {
		return originErrorOther
	}
	var status *httpStatusError
	switch {
	case errors.Is(err, ErrOriginConflict), s3client.Conflict(err):
		return originErrorConflict
	case errors.Is(err, errOriginPrecondition), s3client.Precondition(err):
		return originErrorPrecondition
	case errors.As(err, &status) && status.status == http.StatusConflict:
		return originErrorConflict
	case errors.As(err, &status) && status.status == http.StatusPreconditionFailed:
		return originErrorPrecondition
	case errors.As(err, &status) && status.status == http.StatusNotFound:
		return originErrorNotFound
	case s3client.NotFound(err), errors.Is(err, fs.ErrNotExist):
		return originErrorNotFound
	default:
		return originErrorOther
	}
}

func mapOriginError(err error, conditional bool) error {
	switch classifyOriginError(err) {
	case originErrorConflict:
		return ErrOriginConflict
	case originErrorPrecondition:
		return errOriginPrecondition
	case originErrorNotFound:
		if conditional {
			return errOriginPrecondition
		}
	}
	return err
}

func isObjectNotFound(err error) bool {
	return classifyOriginError(err) == originErrorNotFound
}

type httpConfig struct {
	BaseURL     string            `json:"base_url"`
	Headers     map[string]string `json:"headers"`
	StripBucket bool              `json:"strip_bucket"`
	ReadOnly    *bool             `json:"read_only"`
	Insecure    bool              `json:"insecure"`
}

type httpStatusError struct {
	status int
	msg    string
}

func (e *httpStatusError) Error() string { return e.msg }

type httpBucket struct {
	base    *url.URL
	headers map[string]string
	client  *http.Client
}

func newHTTPBucket(c Config) (*origin, error) {
	var hc httpConfig
	if err := decodeConfig(c, &hc); err != nil {
		return nil, err
	}
	if hc.BaseURL == "" {
		return nil, fmt.Errorf("http origin requires base_url")
	}
	u, err := url.Parse(hc.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("http origin base_url must be an absolute http(s) URL")
	}
	tr := s3client.NewTransport(s3client.TransportOptions{
		Insecure:              hc.Insecure,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	})
	readOnly := true
	if hc.ReadOnly != nil {
		readOnly = *hc.ReadOnly
	}
	b := &httpBucket{
		base: u, headers: hc.Headers,
		client: &http.Client{Transport: tr, Timeout: 10 * time.Minute},
	}
	caps := capRead | capRange | capMetadata | capConditional
	if !readOnly {
		caps |= capWrite
	}
	keyFn := func(key string) string { return key }
	if hc.StripBucket {
		keyFn = func(key string) string {
			key = strings.TrimPrefix(key, "/")
			if i := strings.IndexByte(key, '/'); i >= 0 {
				return key[i+1:]
			}
			return ""
		}
	}
	return &origin{
		kind: "http", caps: caps, prefix: newOriginPrefix(c.Prefix), keyFn: keyFn,
		driver: b,
	}, nil
}

func (b *httpBucket) urlFor(key string) (*url.URL, error) {
	key = strings.TrimPrefix(key, "/")
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

func (b *httpBucket) request(ctx context.Context, method, key string, body io.Reader) (*http.Request, error) {
	u, err := b.urlFor(key)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	telemetry.Inject(ctx, req.Header)
	for k, v := range b.headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (b *httpBucket) do(ctx context.Context, method, key, rangeHeader, ifMatch string) (*http.Response, error) {
	req, err := b.request(ctx, method, key, nil)
	if err != nil {
		return nil, err
	}
	u := req.URL
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", quotedOriginETag(ifMatch))
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusPreconditionFailed:
		resp.Body.Close()
		return nil, errOriginPrecondition
	case http.StatusConflict:
		resp.Body.Close()
		return nil, ErrOriginConflict
	case http.StatusNotFound, http.StatusForbidden:
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

	resp, gerr := b.do(ctx, http.MethodGet, key, "bytes=0-0", "")
	if gerr != nil {
		return originAttrs{}, gerr
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return originAttrs{}, fmt.Errorf("origin GET %s: %s", key, resp.Status)
	}
	a := originAttrsFor(key, 0, strings.Trim(resp.Header.Get("ETag"), `"`), time.Time{},
		resp.Header.Get("Content-Type"), resp.Header.Get("Content-Encoding"), httpUserMetadata(resp.Header))
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
	resp, err := b.do(ctx, http.MethodHead, key, "", "")
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
	a := originAttrsFor(key, resp.ContentLength, strings.Trim(resp.Header.Get("ETag"), `"`), time.Time{},
		resp.Header.Get("Content-Type"), resp.Header.Get("Content-Encoding"), httpUserMetadata(resp.Header))
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		a.LastModified = t
	}
	return a, nil
}

func (b *httpBucket) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := b.do(ctx, http.MethodGet, key, "", "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin GET: %s", resp.Status)}
	}
	return resp.Body, nil
}

func (b *httpBucket) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	resp, err := b.do(ctx, http.MethodGet, key, "", "")
	if err != nil {
		return nil, originAttrs{}, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, originAttrs{}, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin GET: %s", resp.Status)}
	}
	if resp.ContentLength < 0 {

		resp.Body.Close()
		a, aerr := b.Attributes(ctx, key)
		if aerr != nil {
			return nil, originAttrs{}, aerr
		}
		rc, gerr := b.Get(ctx, key)
		if gerr != nil {
			return nil, originAttrs{}, gerr
		}
		return rc, a, nil
	}
	a := originAttrsFor(key, resp.ContentLength, strings.Trim(resp.Header.Get("ETag"), `"`), time.Time{}, resp.Header.Get("Content-Type"), resp.Header.Get("Content-Encoding"), httpUserMetadata(resp.Header))
	if t, err := http.ParseTime(resp.Header.Get("Last-Modified")); err == nil {
		a.LastModified = t
	}
	return resp.Body, a, nil
}

func (b *httpBucket) GetRange(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	if length <= 0 && ifMatch == "" {
		return b.Get(ctx, key)
	}
	resp, err := b.do(ctx, http.MethodGet, key,
		fmt.Sprintf("bytes=%d-%d", offset, offset+max(length, 1)-1), ifMatch)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusPartialContent {
		return resp.Body, nil
	}
	if resp.StatusCode == http.StatusOK {

		if offset > 0 || (resp.ContentLength >= 0 && resp.ContentLength > length) {
			resp.Body.Close()
			return nil, fmt.Errorf("origin ignored Range for %s (offset %d, size %d, wanted %d)", key, offset, resp.ContentLength, length)
		}
		return resp.Body, nil
	}
	resp.Body.Close()
	return nil, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin range GET: %s", resp.Status)}
}

func (b *httpBucket) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	req, err := b.request(ctx, http.MethodPut, key, r)
	if err != nil {
		return originAttrs{}, err
	}
	req.ContentLength = size
	if in.ContentType != "" {
		req.Header.Set("Content-Type", in.ContentType)
	}
	if in.ContentEncoding != "" {
		req.Header.Set("Content-Encoding", in.ContentEncoding)
	}
	for i := 0; i+1 < len(in.UserMetadata); i += 2 {
		req.Header.Set("x-amz-meta-"+in.UserMetadata[i], in.UserMetadata[i+1])
	}
	switch in.Precondition.Mode {
	case store.IfAbsent:
		req.Header.Set("If-None-Match", "*")
	case store.IfPresent:
		req.Header.Set("If-Match", "*")
	case store.IfMatch:
		req.Header.Set("If-Match", quotedOriginETag(in.Precondition.ETag))
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return originAttrs{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return originAttrs{}, &httpStatusError{status: resp.StatusCode, msg: fmt.Sprintf("origin PUT: %s", resp.Status)}
	}
	modified, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	if modified.IsZero() {
		modified = time.Now().UTC()
	}
	return originAttrsFor(key, size, strings.Trim(resp.Header.Get("ETag"), `"`), modified,
		in.ContentType, resp.Header.Get("Content-Encoding"), in.UserMetadata), nil
}

func (b *httpBucket) Delete(ctx context.Context, key string) error {
	req, err := b.request(ctx, http.MethodDelete, key, nil)
	if err != nil {
		return err
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

func (b *httpBucket) Close() error { return nil }

func (b *httpBucket) Iter(context.Context, string) iter.Seq2[originObject, error] {
	return func(func(originObject, error) bool) {}
}

func httpUserMetadata(h http.Header) []string {
	out := map[string]string{}
	for key, values := range h {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			out[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return metadataPairs(out)
}

func metadataPairs(values map[string]string) []string {
	if len(values) == 0 {
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		out = append(out, strings.ToLower(key), values[key])
	}
	return out
}

func clonePairs(pairs []string) []string {
	if len(pairs) == 0 {
		return nil
	}
	return append([]string(nil), pairs...)
}

func quotedOriginETag(etag string) string {
	if etag == "*" {
		return "*"
	}
	return `"` + strings.Trim(etag, `"`) + `"`
}

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
	Directory string `json:"directory"`
}

type localMeta struct {
	contentType     string
	contentEncoding string
	userMeta        []string
}

const (
	localNameMax = 255
	localTempDir = ".pgvs3-tmp"
)

type localBucket struct {
	root      *os.Root
	mu        sync.Mutex
	meta      map[string]localMeta
	temporary bool
}

func newFSBucket(c Config) (*origin, error) {
	var fc fsConfig
	if err := decodeConfig(c, &fc); err != nil {
		return nil, err
	}
	if fc.Directory == "" {
		return nil, fmt.Errorf("filesystem origin requires directory")
	}
	if err := os.MkdirAll(fc.Directory, 0o755); err != nil {
		return nil, err
	}
	return openLocalBucket("filesystem", c.Prefix, fc.Directory, false)
}

func newTempBucket(c Config) (*origin, error) {
	dir, err := os.MkdirTemp("", "pgvs3-ephemeral-")
	if err != nil {
		return nil, err
	}
	o, err := openLocalBucket("ephemeral", c.Prefix, dir, true)
	if err != nil {
		_ = os.RemoveAll(dir)
	}
	return o, err
}

func openLocalBucket(kind, prefix, dir string, temporary bool) (*origin, error) {
	p := newOriginPrefix(prefix)
	if p != "" && !validLocalKey(string(p)) {
		return nil, fmt.Errorf("%s origin prefix %q cannot be stored as a path", kind, prefix)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	if err := root.MkdirAll(localTempDir, 0o755); err != nil {
		_ = root.Close()
		return nil, err
	}
	b := &localBucket{root: root, meta: map[string]localMeta{}, temporary: temporary}
	return &origin{kind: kind, caps: originCapabilitiesAll, prefix: p, driver: b}, nil
}

func (b *localBucket) Upload(_ context.Context, key string, r io.Reader, _ int64, in originUpload) (originAttrs, error) {
	if !validLocalKey(key) {
		return originAttrs{}, fmt.Errorf("%q: %w", key, ErrOriginKeyUnsupported)
	}
	tmp, err := b.writeTemp(r)
	if err != nil {
		return originAttrs{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.publishLocked(tmp, key, in.Precondition); err != nil {
		_ = b.root.Remove(tmp)
		return originAttrs{}, err
	}
	b.meta[key] = localMeta{contentType: in.ContentType, contentEncoding: in.ContentEncoding, userMeta: clonePairs(in.UserMetadata)}
	attrs, _, err := b.attrsLocked(key)
	return attrs, err
}

func (b *localBucket) writeTemp(r io.Reader) (string, error) {
	name := localTempDir + "/" + rand.Text()
	f, err := b.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return "", err
	}
	_, err = io.Copy(f, r)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = b.root.Remove(name)
		return "", err
	}
	return name, nil
}

func (b *localBucket) publishLocked(tmp, key string, p store.Precondition) error {
	current, exists, err := b.attrsLocked(key)
	if err != nil {
		return err
	}
	if !p.Allows(exists, current.ETag) {
		return errOriginPrecondition
	}
	if err := b.root.MkdirAll(path.Dir(key), 0o755); err != nil {
		return localWriteError(key, err)
	}
	if err := b.root.Rename(tmp, key); err != nil {
		return localWriteError(key, err)
	}
	return nil
}

func (b *localBucket) Delete(_ context.Context, key string) error {
	if !validLocalKey(key) {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.meta, key)
	err := b.root.Remove(key)
	if err == nil || isMissingPath(err) {
		return nil
	}
	if fi, statErr := b.root.Stat(key); statErr == nil && fi.IsDir() {
		return nil
	}
	return err
}

func (b *localBucket) GetWithAttrs(_ context.Context, key string) (io.ReadCloser, originAttrs, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	attrs, ok, err := b.attrsLocked(key)
	if err != nil {
		return nil, originAttrs{}, err
	}
	if !ok {
		return nil, originAttrs{}, fs.ErrNotExist
	}
	f, err := b.root.Open(key)
	if err != nil {
		return nil, originAttrs{}, err
	}
	return f, attrs, nil
}

func (b *localBucket) GetRange(_ context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	cond := store.ReadCondition{IfMatch: ifMatch}
	b.mu.Lock()
	defer b.mu.Unlock()
	attrs, exists, err := b.attrsLocked(key)
	if err != nil {
		return nil, err
	}
	if !cond.Allows(exists, attrs.ETag) {
		return nil, errOriginPrecondition
	}
	if !exists {
		return nil, fs.ErrNotExist
	}
	if offset < 0 || offset > attrs.Size {
		return nil, fs.ErrInvalid
	}
	f, err := b.root.Open(key)
	if err != nil {
		return nil, err
	}
	return &rangeReadCloser{r: io.NewSectionReader(f, offset, length), c: f}, nil
}

func (b *localBucket) Attributes(_ context.Context, key string) (originAttrs, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	attrs, ok, err := b.attrsLocked(key)
	if err != nil {
		return originAttrs{}, err
	}
	if !ok {
		return originAttrs{}, fs.ErrNotExist
	}
	return attrs, nil
}

func (b *localBucket) attrsLocked(key string) (originAttrs, bool, error) {
	if !validLocalKey(key) {
		return originAttrs{}, false, nil
	}
	fi, err := b.root.Stat(key)
	if err != nil {
		if isMissingPath(err) {
			return originAttrs{}, false, nil
		}
		return originAttrs{}, false, err
	}
	if fi.IsDir() {
		return originAttrs{}, false, nil
	}
	m := b.meta[key]
	size := fi.Size()
	return originAttrsFor(key, size, fmt.Sprintf("%d-%d", size, fi.ModTime().UnixNano()),
		fi.ModTime(), m.contentType, m.contentEncoding, m.userMeta), true, nil
}

func (b *localBucket) Iter(_ context.Context, dir string) iter.Seq2[originObject, error] {
	return func(yield func(originObject, error) bool) {
		name := strings.TrimSuffix(dir, "/")
		if name == "" {
			name = "."
		} else if !validLocalKey(name) {
			return
		}
		b.walk(name, yield)
	}
}

func (b *localBucket) walk(dir string, yield func(originObject, error) bool) bool {
	entries, err := b.readDir(dir)
	if err != nil {
		if isMissingPath(err) {
			return true
		}
		yield(originObject{}, err)
		return false
	}
	slices.SortFunc(entries, compareListing)
	for _, entry := range entries {
		name := entry.Name()
		if dir != "." {
			name = dir + "/" + name
		}
		if entry.IsDir() {
			if !b.walk(name, yield) {
				return false
			}
			continue
		}
		b.mu.Lock()
		attrs, ok, err := b.attrsLocked(name)
		b.mu.Unlock()
		if err != nil {
			yield(originObject{}, err)
			return false
		}
		if ok && !yield(originObject{Key: name, Attrs: attrs}, nil) {
			return false
		}
	}
	return true
}

func (b *localBucket) readDir(dir string) ([]fs.DirEntry, error) {
	f, err := b.root.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return f.ReadDir(-1)
}

func (b *localBucket) Close() error {
	name := b.root.Name()
	err := b.root.Close()
	if b.temporary {
		err = errors.Join(err, os.RemoveAll(name))
	}
	return err
}

func (b *localBucket) checkKey(key string) error {
	if !validLocalKey(key) {
		return fmt.Errorf("%q: %w", key, ErrOriginKeyUnsupported)
	}
	if parent, err := b.root.Stat(path.Dir(key)); (err == nil && !parent.IsDir()) || errors.Is(err, syscall.ENOTDIR) {
		return fmt.Errorf("%q conflicts with an existing object: %w", key, ErrOriginKeyUnsupported)
	}
	if fi, err := b.root.Stat(key); err == nil && fi.IsDir() {
		return fmt.Errorf("%q conflicts with an existing prefix: %w", key, ErrOriginKeyUnsupported)
	}
	return nil
}

func compareListing(a, b fs.DirEntry) int {
	return strings.Compare(listingKey(a), listingKey(b))
}

func listingKey(e fs.DirEntry) string {
	if e.IsDir() {
		return e.Name() + "/"
	}
	return e.Name()
}

func validLocalKey(key string) bool {
	if strings.IndexByte(key, 0) >= 0 || key == localTempDir || strings.HasPrefix(key, localTempDir+"/") {
		return false
	}
	for part := range strings.SplitSeq(key, "/") {
		if part == "" || part == "." || part == ".." || len(part) > localNameMax {
			return false
		}
	}
	return true
}

func isMissingPath(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

func localWriteError(key string, err error) error {
	if errors.Is(err, syscall.EEXIST) || errors.Is(err, syscall.ENOTDIR) || errors.Is(err, syscall.EISDIR) {
		return fmt.Errorf("%q conflicts with an existing object or prefix: %w", key, ErrOriginKeyUnsupported)
	}
	return err
}

type rangeReadCloser struct {
	r io.Reader
	c io.Closer
}

func (r *rangeReadCloser) Read(p []byte) (int, error) { return r.r.Read(p) }

func (r *rangeReadCloser) Close() error { return r.c.Close() }
