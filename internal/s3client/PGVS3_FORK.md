# internal/s3client provenance

This package is pgvs3's S3 origin client. It began as a fork of
[`github.com/kelindar/s3`](https://github.com/kelindar/s3) v0.4.0
(commit `65303631681c39f9614ec2c265cf0d3045d8198f`, module checksum
`h1:PYHcGLECRzYkBY5bL6SKgdE1iHo9A47BqsY+hGfkNvE=`) and is now trimmed to
the surface pgvs3's read-through cache and passthrough proxy use.

The Apache License 2.0 text is preserved in `LICENSE`. `client.go`, `signer.go`, and their tests retain their upstream copyright headers (`object.go` was rewritten for pgvs3 and carries no upstream header). The SigV4 implementation in `signer.go` is adapted from upstream `aws/v4.go`.

## Supported surface

```go
type Config struct {
    Bucket, Endpoint, Region           string
    AccessKey, SecretKey, SessionToken string
    Creds                              CredentialsProvider
    Insecure, PathStyle                bool
    Prefix                             string
    HTTPClient                         *http.Client
}

func New(Config) (*Bucket, error)
func NewChainProvider(ChainConfig) *ChainProvider
func NewTransport(TransportOptions) *http.Transport

type ObjectInfo struct {
    Key             string
    Size            int64
    ETag            string
    LastModified    time.Time
    ContentType     string
    ContentEncoding string
    UserMetadata    []string
}

func (*Bucket) Stat(ctx context.Context, key string) (ObjectInfo, error)
func (*Bucket) StatIfNoneMatch(ctx context.Context, key, ifNoneMatch string) (ObjectInfo, bool, error)
func (*Bucket) Get(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error)
func (*Bucket) GetWithInfo(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, ObjectInfo, error)
func (*Bucket) Proxy(ctx context.Context, method, key string, query url.Values, body io.Reader, size int64, extra ...[2]string) (*http.Response, error)
func (*Bucket) Close() error

type Error struct { Op, Key string; StatusCode int; Code, Message string; Err error }
var ErrNotFound, ErrPrecondition, ErrConflict, ErrWrongRegion, ErrIMDS error
func NotFound(error) bool
func Precondition(error) bool
func Conflict(error) bool
func WrongRegion(error) bool
```
