package s3

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

type sessionCredentials struct {
	AccessKeyID     string `xml:"AccessKeyId"`
	Expiration      string `xml:"Expiration"`
	SecretAccessKey string `xml:"SecretAccessKey"`
	SessionToken    string `xml:"SessionToken"`
}

type createSessionResult struct {
	XMLName     xml.Name           `xml:"CreateSessionResult"`
	Xmlns       string             `xml:"xmlns,attr"`
	Credentials sessionCredentials `xml:"Credentials"`
}

type listDirectoryBucketsResult struct {
	XMLName               xml.Name    `xml:"ListDirectoryBucketsResult"`
	Xmlns                 string      `xml:"xmlns,attr"`
	Buckets               []bucketXML `xml:"Buckets>Bucket"`
	ContinuationToken     string      `xml:"ContinuationToken,omitempty"`
	MaxDirectoryBuckets   int         `xml:"MaxDirectoryBuckets"`
	IsTruncated           bool        `xml:"IsTruncated"`
	NextContinuationToken string      `xml:"NextContinuationToken,omitempty"`
}

func (s *Server) createSession(w http.ResponseWriter, r *http.Request, bucket string) {
	ok, err := s.store.BucketExists(r.Context(), bucket)
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	if !ok {
		writeError(w, r, errNoSuchBucket(), s.log)
		return
	}
	mode := r.Header.Get("x-amz-create-session-mode")
	sess := s.auth.Sessions.Create(bucket, mode)
	w.Header().Set("x-amz-server-side-encryption", "AES256")
	writeXML(w, http.StatusOK, createSessionResult{
		Xmlns: s3Namespace,
		Credentials: sessionCredentials{
			AccessKeyID: sess.AccessKey, SecretAccessKey: sess.SecretKey,
			SessionToken: sess.Token, Expiration: sess.Expiry.UTC().Format(time.RFC3339),
		},
	})
}

func (s *Server) listDirectoryBuckets(w http.ResponseWriter, r *http.Request, q map[string][]string) {
	max := 1000
	if v := first(q, "max-directory-buckets"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid max-directory-buckets"), s.log)
			return
		}
		max = min(n, 1000)
	}
	after := first(q, "continuation-token")
	buckets, err := s.store.Buckets(r.Context())
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	out := listDirectoryBucketsResult{
		Xmlns: s3Namespace, MaxDirectoryBuckets: max, ContinuationToken: after,
	}
	for _, b := range buckets {
		if !isDirectoryBucket(b.Name) || b.Name <= after {
			continue
		}
		if len(out.Buckets) >= max {
			out.IsTruncated = true
			out.NextContinuationToken = out.Buckets[len(out.Buckets)-1].Name
			break
		}
		out.Buckets = append(out.Buckets, bucketXML{Name: b.Name, CreationDate: isoTime(b.Created)})
	}
	writeXML(w, http.StatusOK, out)
}

func first(q map[string][]string, key string) string {
	if v, ok := q[key]; ok && len(v) > 0 {
		return v[0]
	}
	return ""
}

var _ = store.BucketDeleted
