package objcache

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v2"
)

// s3Meta supplies the origin metadata that thanos objstore's
// ObjectAttributes does not expose (Content-Type) and the exact ETag. It is
// optional: filesystem/inmem origins and S3 origins without static
// credentials fall back to objstore plus extension-based content types.
type s3Meta struct {
	client *minio.Client
	bucket string
	prefix string
}

type s3MetaConfig struct {
	Bucket           string `yaml:"bucket"`
	Endpoint         string `yaml:"endpoint"`
	Region           string `yaml:"region"`
	AccessKey        string `yaml:"access_key"`
	SecretKey        string `yaml:"secret_key"`
	SessionToken     string `yaml:"session_token"`
	Insecure         bool   `yaml:"insecure"`
	BucketLookupType string `yaml:"bucket_lookup_type"`
}

func newS3Meta(c Config) (*s3Meta, error) {
	b, err := yaml.Marshal(c.Config)
	if err != nil {
		return nil, err
	}
	var sc s3MetaConfig
	if err := yaml.Unmarshal(b, &sc); err != nil {
		return nil, err
	}
	if sc.Bucket == "" || sc.Endpoint == "" || sc.AccessKey == "" {
		return nil, fmt.Errorf("s3 metadata requires bucket, endpoint and access_key")
	}
	endpoint := strings.TrimPrefix(strings.TrimPrefix(sc.Endpoint, "https://"), "http://")
	lookup := minio.BucketLookupAuto
	switch strings.ToLower(sc.BucketLookupType) {
	case "path", "path-style":
		lookup = minio.BucketLookupPath
	case "virtual-hosted", "virtual-hosted-style":
		lookup = minio.BucketLookupDNS
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(sc.AccessKey, sc.SecretKey, sc.SessionToken),
		Secure:       !sc.Insecure,
		Region:       sc.Region,
		BucketLookup: lookup,
	})
	if err != nil {
		return nil, err
	}
	return &s3Meta{
		client: client,
		bucket: sc.Bucket,
		prefix: strings.TrimSuffix(c.Prefix, "/"),
	}, nil
}

type originAttrs struct {
	Size         int64
	ETag         string
	LastModified time.Time
	ContentType  string
}

func (m *s3Meta) attrs(ctx context.Context, key string) (originAttrs, error) {
	if m.prefix != "" {
		key = m.prefix + "/" + key
	}
	info, err := m.client.StatObject(ctx, m.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return originAttrs{}, err
	}
	return originAttrs{
		Size:         info.Size,
		ETag:         strings.Trim(info.ETag, `"`),
		LastModified: info.LastModified,
		ContentType:  info.ContentType,
	}, nil
}
