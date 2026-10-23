package s3

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/adonm/pgvs3/internal/store"
)

// listObjectVersions is a read-only compatibility shim. pgvs3 does not
// implement versioning, so every current object is reported as the single
// "null" version. Clients such as s3t use this only to empty a bucket before
// deleting it; a real versioning API remains out of scope.
func (s *Server) listObjectVersions(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	maxKeys, prefix, keyMarker, apiErr := versionParams(q)
	if apiErr != nil {
		writeError(w, r, apiErr, s.log)
		return
	}
	exists, err := s.store.BucketExists(r.Context(), bucket)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	if !exists {
		writeError(w, r, errNoSuchBucket(), s.log)
		return
	}
	rows, err := s.store.List(r.Context(), bucket, prefix, prefixEnd(prefix), keyMarker, maxKeys+1)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	s.writeVersionList(w, bucket, q, maxKeys, prefix, keyMarker, rows)
}

// listOriginObjectVersions applies the same null-version shim to an origin
// bucket so cache-tier deployments can use the same cleanup tools.
func (s *Server) listOriginObjectVersions(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	origin := s.originFor(r.Context(), bucket)
	if origin == nil {
		s.listObjectVersions(w, r, bucket, q)
		return
	}
	maxKeys, prefix, keyMarker, apiErr := versionParams(q)
	if apiErr != nil {
		writeError(w, r, apiErr, s.log)
		return
	}
	rows, err := origin.List(r.Context(), bucket, prefix, keyMarker, maxKeys+1)
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	s.writeVersionList(w, bucket, q, maxKeys, prefix, keyMarker, rows)
}

func versionParams(q url.Values) (maxKeys int, prefix, keyMarker string, err *APIError) {
	if q.Get("delimiter") != "" {
		return 0, "", "", errNotImplemented()
	}
	maxKeys = 1000
	if v := q.Get("max-keys"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 0 {
			return 0, "", "", errInvalidRequest("invalid max-keys")
		}
		maxKeys = min(n, 1000)
	}
	return maxKeys, q.Get("prefix"), q.Get("key-marker"), nil
}

func (s *Server) writeVersionList(w http.ResponseWriter, bucket string, q url.Values,
	maxKeys int, prefix, keyMarker string, rows []store.Listed) {
	truncated := len(rows) > maxKeys
	var nextKey string
	if truncated {
		nextKey = rows[0].Key
		rows = rows[:maxKeys]
		if len(rows) > 0 {
			nextKey = rows[len(rows)-1].Key
		}
	}
	out := listVersionsResult{
		Xmlns:           s3Namespace,
		Name:            bucket,
		Prefix:          prefix,
		KeyMarker:       keyMarker,
		VersionIdMarker: q.Get("version-id-marker"),
		MaxKeys:         maxKeys,
		IsTruncated:     truncated,
	}
	for _, row := range rows {
		out.Versions = append(out.Versions, versionXML{
			Key:          row.Key,
			VersionId:    "null",
			IsLatest:     true,
			LastModified: isoTime(row.CreatedAt),
			ETag:         quoteETag(row.ETag),
			Size:         row.Size,
			StorageClass: "STANDARD",
			Owner:        ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		})
	}
	if truncated {
		out.NextKeyMarker = nextKey
		out.NextVersionIdMarker = "null"
	}
	if q.Get("encoding-type") == "url" {
		out.EncodingType = "url"
		out.Prefix = awsEncode(out.Prefix, true)
		out.KeyMarker = awsEncode(out.KeyMarker, true)
		out.NextKeyMarker = awsEncode(out.NextKeyMarker, true)
		for i := range out.Versions {
			out.Versions[i].Key = awsEncode(out.Versions[i].Key, true)
		}
	}
	writeXML(w, http.StatusOK, out)
}
