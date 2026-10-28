package s3clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/s3client"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
)

type benchmarkOrigin interface {
	Attributes(context.Context, string) (originAttrs, error)
	GetWithAttrs(context.Context, string) (io.ReadCloser, originAttrs, error)
	Upload(context.Context, string, io.Reader, int64, originUpload) (originAttrs, error)
	Close() error
}

type minioBenchmarkOrigin struct {
	core minio.Core
	tr   *http.Transport
}

func newMinioBenchmarkOrigin(c Config) (benchmarkOrigin, error) {
	var sc s3Config
	if err := decodeConfig(c, &sc); err != nil {
		return nil, err
	}
	u, err := url.Parse(sc.Endpoint)
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	tr.MaxIdleConnsPerHost = 64
	core, err := minio.NewCore(u.Host, &minio.Options{Secure: u.Scheme == "https", Transport: tr,
		Creds: miniocreds.NewStaticV4(sc.AccessKey, sc.SecretKey, sc.SessionToken), Region: "us-east-1", BucketLookup: minio.BucketLookupPath, MaxRetries: 1})
	if err != nil {
		return nil, err
	}
	return &minioBenchmarkOrigin{core: *core, tr: tr}, nil
}

func (m *minioBenchmarkOrigin) Attributes(ctx context.Context, key string) (originAttrs, error) {
	a, err := m.core.StatObject(ctx, "benchmark", key, minio.StatObjectOptions{})
	return originAttrsFor(key, a.Size, a.ETag, a.LastModified, a.ContentType, a.ContentEncoding, nil), err
}

func (m *minioBenchmarkOrigin) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	r, a, _, err := m.core.GetObject(ctx, "benchmark", key, minio.GetObjectOptions{})
	return r, originAttrsFor(key, a.Size, a.ETag, a.LastModified, a.ContentType, a.ContentEncoding, nil), err
}

func (m *minioBenchmarkOrigin) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	a, err := m.core.PutObject(ctx, "benchmark", key, r, size, "", "", minio.PutObjectOptions{DisableContentSha256: true})
	return originAttrs{Size: a.Size, ETag: a.ETag}, err
}

func (m *minioBenchmarkOrigin) Close() error { m.tr.CloseIdleConnections(); return nil }

func BenchmarkS3OriginClients(b *testing.B) {
	for _, size := range []int{4096, 10 << 20, 64 << 20, 256 << 20, 1 << 30} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload := make([]byte, size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
				w.Header().Set("Last-Modified", "Wed, 28 Oct 2026 00:00:00 GMT")
				switch r.Method {
				case http.MethodPut:
					_, _ = io.Copy(io.Discard, r.Body)
				case http.MethodHead:
					w.Header().Set("Content-Length", fmt.Sprint(size))
				default:
					w.Header().Set("Content-Length", fmt.Sprint(size))
					_, _ = w.Write(payload)
				}
			}))
			defer server.Close()
			for _, client := range []struct {
				name string
				new  func(Config) (benchmarkOrigin, error)
			}{{"fork", func(c Config) (benchmarkOrigin, error) { return newS3Bucket(c) }},
				{"aws", func(c Config) (benchmarkOrigin, error) { return newSDKBucket(c) }},
				{"minio", newMinioBenchmarkOrigin}} {
				b.Run(client.name, func(b *testing.B) {
					o, err := client.new(Config{Type: "s3", Config: map[string]any{
						"bucket": "benchmark", "endpoint": server.URL, "access_key": "test", "secret_key": "test", "bucket_lookup_type": "path",
					}})
					if err != nil {
						b.Fatal(err)
					}
					defer o.Close()
					ctx := context.Background()
					for _, op := range []string{"head", "get", "put"} {
						b.Run(op, func(b *testing.B) {
							b.ReportAllocs()
							if op != "head" {
								b.SetBytes(int64(size))
							}
							for b.Loop() {
								switch op {
								case "head":
									_, err = o.Attributes(ctx, "object")
								case "put":
									_, err = o.Upload(ctx, "object", bytes.NewReader(payload), int64(size), originUpload{})
								case "get":
									var body io.ReadCloser
									body, _, err = o.GetWithAttrs(ctx, "object")
									if err == nil {
										_, err = io.Copy(io.Discard, body)
										_ = body.Close()
									}
								}
								if err != nil {
									b.Fatal(err)
								}
							}
						})
					}
				})
			}
		})
	}
}

type sdkBucket struct {
	client    *awss3.Client
	bucket    string
	transport *http.Transport
}

func newSDKBucket(c Config) (benchmarkOrigin, error) {
	var sc s3Config
	if err := decodeConfig(c, &sc); err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	tr.MaxIdleConnsPerHost = 64
	tr.ResponseHeaderTimeout = 30 * time.Second
	b := &sdkBucket{bucket: sc.Bucket, transport: tr}
	b.client = awss3.New(awss3.Options{
		Region: "us-east-1", BaseEndpoint: aws.String(sc.Endpoint), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider(sc.AccessKey, sc.SecretKey, sc.SessionToken),
		HTTPClient:  &http.Client{Transport: tr, Timeout: 10 * time.Minute},

		RetryMaxAttempts:             1,
		RequestChecksumCalculation:   aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation:   aws.ResponseChecksumValidationWhenRequired,
		ContinueHeaderThresholdBytes: -1,
		APIOptions:                   []func(*middleware.Stack) error{v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware},
	})
	return b, nil
}

func (b *sdkBucket) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	out, err := b.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &b.bucket, Key: &key, Body: r, ContentLength: &size,
		ContentType: aws.String(in.ContentType), ContentEncoding: aws.String(in.ContentEncoding),
	})
	if err != nil {
		return originAttrs{}, err
	}
	return originAttrsFor(key, size, aws.ToString(out.ETag), time.Now().UTC(), in.ContentType, in.ContentEncoding, nil), nil
}

func (b *sdkBucket) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	out, err := b.client.GetObject(ctx, &awss3.GetObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		return nil, originAttrs{}, err
	}
	return out.Body, originAttrsFor(key, aws.ToInt64(out.ContentLength), aws.ToString(out.ETag), aws.ToTime(out.LastModified),
		aws.ToString(out.ContentType), aws.ToString(out.ContentEncoding), nil), nil
}

func (b *sdkBucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	out, err := b.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &b.bucket, Key: &key})
	if err != nil {
		return originAttrs{}, err
	}
	return originAttrsFor(key, aws.ToInt64(out.ContentLength), aws.ToString(out.ETag), aws.ToTime(out.LastModified),
		aws.ToString(out.ContentType), aws.ToString(out.ContentEncoding), nil), nil
}

func (b *sdkBucket) Close() error { b.transport.CloseIdleConnections(); return nil }

type Config struct {
	Type   string
	Config map[string]any
}

type s3Config struct {
	Bucket       string `json:"bucket"`
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"access_key"`
	SecretKey    string `json:"secret_key"`
	SessionToken string `json:"session_token"`
}

type originAttrs struct {
	Size                         int64
	ETag                         string
	LastModified                 time.Time
	ContentType, ContentEncoding string
}

type originUpload struct{ ContentType, ContentEncoding string }

func decodeConfig(c Config, dst *s3Config) error {
	raw, err := json.Marshal(c.Config)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dst)
}

func originAttrsFor(_ string, size int64, etag string, modified time.Time, ct, encoding string, _ []string) originAttrs {
	return originAttrs{size, etag, modified, ct, encoding}
}

type forkBucket struct{ client *s3client.Bucket }

func newS3Bucket(c Config) (benchmarkOrigin, error) {
	var sc s3Config
	if err := decodeConfig(c, &sc); err != nil {
		return nil, err
	}
	client, err := s3client.New(s3client.Config{Bucket: sc.Bucket, Endpoint: sc.Endpoint,
		AccessKey: sc.AccessKey, SecretKey: sc.SecretKey, SessionToken: sc.SessionToken, PathStyle: true})
	if err != nil {
		return nil, err
	}
	return &forkBucket{client}, nil
}

func (b *forkBucket) Attributes(ctx context.Context, key string) (originAttrs, error) {
	a, err := b.client.Stat(ctx, key)
	return originAttrsFor(key, a.Size, a.ETag, a.LastModified, a.ContentType, a.ContentEncoding, nil), err
}

func (b *forkBucket) GetWithAttrs(ctx context.Context, key string) (io.ReadCloser, originAttrs, error) {
	r, a, err := b.client.GetWithInfo(ctx, key, 0, -1, "")
	return r, originAttrsFor(key, a.Size, a.ETag, a.LastModified, a.ContentType, a.ContentEncoding, nil), err
}

func (b *forkBucket) Upload(ctx context.Context, key string, r io.Reader, size int64, in originUpload) (originAttrs, error) {
	a, err := b.client.WriteReader(ctx, key, r, size, s3client.PutOptions{})
	return originAttrsFor(key, a.Size, a.ETag, a.LastModified, a.ContentType, a.ContentEncoding, nil), err
}

func (b *forkBucket) Close() error { return b.client.Close() }
