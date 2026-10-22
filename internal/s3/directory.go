package s3

import (
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const directorySuffix = "--x-s3"

// isDirectoryBucket recognizes the S3 Express One Zone naming convention:
// base-name--zone-id--x-s3.
func isDirectoryBucket(name string) bool {
	if !strings.HasSuffix(name, directorySuffix) {
		return false
	}
	base := strings.TrimSuffix(name, directorySuffix)
	return strings.LastIndex(base, "--") > 0
}

func bucketZone(name string) string {
	base := strings.TrimSuffix(name, directorySuffix)
	if i := strings.LastIndex(base, "--"); i >= 0 {
		return base[i+2:]
	}
	return ""
}

func (s *Server) bucketARN(bucket string) string {
	return fmt.Sprintf("arn:aws:s3express:%s:%s:bucket/%s", s.region, s.accountID, bucket)
}

func (s *Server) setDirectoryObjectHeaders(w http.ResponseWriter, bucket string) {
	if !isDirectoryBucket(bucket) {
		return
	}
	w.Header().Set("x-amz-storage-class", "EXPRESS_ONEZONE")
	w.Header().Set("x-amz-server-side-encryption", "AES256")
}

func (s *Server) setDirectoryHeaders(w http.ResponseWriter, bucket string) {
	if !isDirectoryBucket(bucket) {
		return
	}
	w.Header().Set("x-amz-bucket-location-type", "AvailabilityZone")
	w.Header().Set("x-amz-bucket-region", s.region)
	w.Header().Set("x-amz-bucket-arn", s.bucketARN(bucket))
	w.Header().Set("x-amz-server-side-encryption", "AES256")
}

// splitPath accepts both path-style and the virtual-hosted zonal style used by
// S3 Express and AWS SDKs.
func splitPath(r *http.Request, vhostDomain string) (string, string, error) {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if i := strings.IndexByte(host, '.'); i > 0 {
		first := host[:i]
		vhost := strings.HasSuffix(first, directorySuffix)
		if !vhost && vhostDomain != "" {
			vhost = host == vhostDomain || strings.HasSuffix(host, "."+vhostDomain)
		}
		if vhost {
			key, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/"))
			if err != nil {
				return "", "", err
			}
			return first, key, nil
		}
	}
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil {
		return "", "", err
	}
	decoded = strings.TrimPrefix(decoded, "/")
	if decoded == "" {
		return "", "", nil
	}
	bucket, key, _ := strings.Cut(decoded, "/")
	return bucket, key, nil
}

type createBucketConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration"`
	Location struct {
		Type string `xml:"Type"`
		Name string `xml:"Name"`
	} `xml:"Location"`
	Bucket struct {
		Type           string `xml:"Type"`
		DataRedundancy string `xml:"DataRedundancy"`
	} `xml:"Bucket"`
}

func (s *Server) validateCreateBucket(r *http.Request, bucket string) error {
	if r.ContentLength == 0 {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		return errInvalidRequest("could not read CreateBucketConfiguration")
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	var cfg createBucketConfiguration
	if err := xml.Unmarshal(body, &cfg); err != nil {
		return errInvalidRequest("malformed CreateBucketConfiguration")
	}
	if cfg.Bucket.Type != "Directory" {
		return nil
	}
	if !isDirectoryBucket(bucket) {
		return errInvalidRequest("directory bucket names must end in --<zone-id>--x-s3")
	}
	zone := bucketZone(bucket)
	if cfg.Location.Name != "" && cfg.Location.Name != zone {
		return errInvalidRequest("bucket name zone and Location.Name differ")
	}
	if cfg.Bucket.DataRedundancy != "" && cfg.Bucket.DataRedundancy != "SingleAvailabilityZone" {
		return errInvalidRequest("directory buckets require SingleAvailabilityZone")
	}
	return nil
}

// validateSSE accepts the encryption modes directory buckets support. The
// gateway stores bytes in PostgreSQL, so AES256 is bookkeeping; SSE-C and
// DSSE-KMS are rejected exactly as Express does.
func validateSSE(r *http.Request) error {
	if r.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key") != "" ||
		r.Header.Get("x-amz-server-side-encryption-customer-key-md5") != "" {
		return errNotImplemented()
	}
	switch enc := r.Header.Get("x-amz-server-side-encryption"); enc {
	case "", "AES256", "aws:kms":
		return nil
	case "aws:kms:dsse":
		return errNotImplemented()
	default:
		return errInvalidRequest("unsupported server-side encryption")
	}
}
