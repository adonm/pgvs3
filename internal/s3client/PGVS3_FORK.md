# internal/s3client provenance

This package is pgvs3's S3 origin client. It began as a fork of
[`github.com/kelindar/s3`](https://github.com/kelindar/s3) v0.4.0
(commit `65303631681c39f9614ec2c265cf0d3045d8198f`, module checksum
`h1:PYHcGLECRzYkBY5bL6SKgdE1iHo9A47BqsY+hGfkNvE=`) and is now maintained as
pgvs3 core code. It is not a drop-in replacement for upstream.

The Apache License 2.0 text is preserved in `LICENSE`. Files derived from
upstream retain their copyright headers. The SigV4 implementation in
`signer.go` is adapted from upstream `aws/v4.go`; the object API, transport,
multipart writer and tests were derived from the same fork.

## Supported surface

```go
type Config struct {
    Bucket, Endpoint, Region            string
    AccessKey, SecretKey, SessionToken  string
    Insecure, PathStyle                 bool
    Prefix                              string
    HTTPClient                          *http.Client
}

func New(Config) (*Bucket, error)

type ObjectInfo struct {
    Key             string
    Size            int64
    ETag            string
    LastModified    time.Time
    ContentType     string
    ContentEncoding string
    UserMetadata    []string
}

type PutOptions struct {
    UserMetadata    []string
    ContentType     string
    ContentEncoding string
    IfMatch         string
    IfNoneMatch     string
}

func (*Bucket) Stat(ctx, key) (ObjectInfo, error)
func (*Bucket) Walk(ctx, prefix) iter.Seq2[ObjectInfo, error]
func (*Bucket) WalkAfter(ctx, prefix, startAfter string) iter.Seq2[ObjectInfo, error]
func (*Bucket) Get(ctx, key, offset, length int64, ifMatch string) (io.ReadCloser, error)
func (*Bucket) GetWithInfo(ctx, key, offset, length int64, ifMatch string) (io.ReadCloser, ObjectInfo, error)
func (*Bucket) WriteReader(ctx, key string, r io.Reader, size int64, opts PutOptions) (ObjectInfo, error)
func (*Bucket) Delete(ctx, key string) error
func (*Bucket) DeleteObjects(ctx, keys []string) ([]error, error)
func (*Bucket) CreateBucket(ctx) error
func (*Bucket) DeleteBucket(ctx) error
func (*Bucket) Close() error

func NewSigner(region, accessKey, secret, token string) *Signer
func (*Signer) Sign(req *http.Request, body []byte) error

type Error struct { Op, Key string; StatusCode int; Code, Message string; Err error }
var ErrNotFound, ErrPrecondition, ErrConflict, ErrUnsupported, ErrInvalidRange error
func NotFound, Precondition, Conflict, Unsupported, InvalidRange(error) bool
```

## Deliberate behavior

- Anonymous (unsigned) access is supported for public S3 buckets when both
  `AccessKey` and `SecretKey` are empty; requests then omit SigV4 headers. A
  session token without keys is rejected. Credentials are still required for
  writes and for buckets that reject unsigned reads.
- Exact ranged reads reject an origin that ignores `Range` and returns a
  different window.
- Conditional writes apply conditions to final-object completion for
  multipart uploads.
- `WriteReader` streams and never retries a part. When the calculated part size
  is at most 16 MiB, it reads parts into memory and uploads four at a time;
  larger parts use sequential streaming to keep memory bounded.
- The HTTP transport disables transparent compression.
- `Walk` validates continuation-token progress. `WalkAfter` sends `start-after`
  on the first list page, then uses the continuation token, so deep pages do not
  rescan the prefix client-side.
- `DeleteObjects` batches up to 1,000 keys into one `POST /?delete` request and
  returns a per-key error. `NoSuchKey` counts as success, matching `Delete`.
- Replayable requests (no body or a seekable body) retry up to three attempts on
  429/5xx responses and transport errors with short exponential backoff.
  Non-replayable uploads are left to the caller's retry loop.
- User metadata is sorted, bare name/value pairs.

The package is used by `internal/objcache` for the production S3 origin and by
`test/loadgen` for SigV4 requests. The pgvs3 gateway itself remains a
trusted-network, unsigned S3 implementation.
