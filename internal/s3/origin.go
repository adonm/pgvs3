package s3

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

// SetOrigin enables read-through and write-through caching over an origin object store.
func (s *Server) SetOrigin(t *objcache.Tier) { s.origin = t }

// serveOrigin handles S3 requests in cache-tier mode.
func (s *Server) serveOrigin(w http.ResponseWriter, r *http.Request, bucket, key string) {
	q := r.URL.Query()
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			writeXML(w, http.StatusOK, listAllMyBucketsResult{
				Xmlns: s3Namespace,
				Owner: ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
			})
			return
		}
		s.methodNotAllowed(w, r)
	case key == "":
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if q.Has("location") {
				s.getBucketLocation(w, r, bucket)
				return
			}
			s.listOrigin(w, r, bucket, q)
		case http.MethodDelete:
			writeError(w, r, errNotImplemented(), s.log)
		case http.MethodPut:
			if r.Header.Get("x-amz-copy-source") != "" {
				writeError(w, r, errNotImplemented(), s.log)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodPost:
			if q.Has("delete") {
				s.deleteObjects(w, r, bucket)
				return
			}
			s.methodNotAllowed(w, r)
		default:
			s.methodNotAllowed(w, r)
		}
	default:
		s.originObject(w, r, bucket, key)
	}
}

func (s *Server) originObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if s.readOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, r, errAccessDenied("gateway is read-only"), s.log)
		return
	}
	q := r.URL.Query()
	switch r.Method {
	case http.MethodHead:
		obj, err := s.origin.Head(r.Context(), bucket, key)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, r, errNoSuchKey(), s.log)
				return
			}
			writeError(w, r, errInternal(err, s.log), s.log)
			return
		}
		writeOriginHeaders(w, obj)
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		if q.Has("uploadId") {
			s.listParts(w, r, bucket, key, q)
			return
		}
		first, last, suffix, _, err := parseRange(r.Header.Get("Range"))
		if err != nil {
			writeError(w, r, errInvalidRange(), s.log)
			return
		}
		obj, err := s.origin.Open(r.Context(), bucket, key, first, last, suffix)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, r, errNoSuchKey(), s.log)
				return
			}
			if errors.Is(err, store.ErrInvalidRange) {
				writeError(w, r, errInvalidRange(), s.log)
				return
			}
			writeError(w, r, errInternal(err, s.log), s.log)
			return
		}
		defer obj.Body.Close()
		writeOriginHeaders(w, obj)
		if obj.Ranged {
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(obj.Start, 10)+"-"+
				strconv.FormatInt(obj.End, 10)+"/"+strconv.FormatInt(obj.Size, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		buf := copyBuf()
		defer putCopyBuf(buf)
		_, _ = io.CopyBuffer(w, obj.Body, buf)

	case http.MethodPut:
		if q.Has("partNumber") && q.Has("uploadId") {
			s.uploadPart(w, r, bucket, key, q)
			return
		}
		if r.Header.Get("x-amz-copy-source") != "" {
			s.copyOriginObject(w, r, bucket, key)
			return
		}
		meta, err := parseMetadata(r.Header)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		ct, err := contentType(r.Header.Get("Content-Type"))
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		in, err := newIntegrity(r.Header)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		body := io.Reader(r.Body)
		if len(in.h) > 0 {
			body = &integrityReader{r: r.Body, in: in}
		}
		m, err := s.origin.Upload(r.Context(), bucket, key, body, meta, ct)
		if err != nil {
			writeError(w, r, s.ingestError(err), s.log)
			return
		}
		checks, err := in.verify(r.Trailer)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		if m.ETag != "" {
			w.Header().Set("ETag", quoteETag(m.ETag))
		}
		for _, c := range checks {
			w.Header().Set(c.Name, c.Value)
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodPost:
		if q.Has("uploads") {
			s.createMultipartUpload(w, r, bucket, key)
			return
		}
		if q.Has("uploadId") {
			s.completeMultipartUpload(w, r, bucket, key, q)
			return
		}
		s.methodNotAllowed(w, r)

	case http.MethodDelete:
		if q.Has("uploadId") {
			s.abortMultipartUpload(w, r, bucket, key, q)
			return
		}
		if err := s.origin.Delete(r.Context(), bucket, key); err != nil {
			writeError(w, r, errInternal(err, s.log), s.log)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		s.methodNotAllowed(w, r)
	}
}

func (s *Server) copyOriginObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"), bucket)
	if err != nil {
		writeError(w, r, errInvalidRequest("invalid x-amz-copy-source"), s.log)
		return
	}
	replace := strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE")
	var meta []string
	var ct string
	if replace {
		meta, err = parseMetadata(r.Header)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		ct, err = contentType(r.Header.Get("Content-Type"))
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
	}
	out, err := s.origin.Copy(r.Context(), srcBucket, srcKey, bucket, key, meta, ct, replace)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNoSuchKey(), s.log)
			return
		}
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns: s3Namespace, ETag: quoteETag(out.ETag), LastModified: isoTime(out.CreatedAt),
	})
}

func writeOriginHeaders(w http.ResponseWriter, obj *objcache.Object) {
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", quoteETag(obj.ETag))
	if obj.Hit {
		w.Header().Set("X-Cache", "HIT")
	} else {
		w.Header().Set("X-Cache", "MISS")
	}
	if obj.ContentType != "" {
		w.Header().Set("Content-Type", obj.ContentType)
	}
	for i := 0; i+1 < len(obj.UserMetadata); i += 2 {
		w.Header().Set("x-amz-meta-"+obj.UserMetadata[i], obj.UserMetadata[i+1])
	}
	if !obj.LastModified.IsZero() {
		w.Header().Set("Last-Modified", obj.LastModified.UTC().Format(http.TimeFormat))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(obj.ContentLength(), 10))
}

func (s *Server) listOrigin(w http.ResponseWriter, r *http.Request, bucket string, q map[string][]string) {
	prefix := first(q["prefix"])
	maxKeys := 1000
	if v := first(q["max-keys"]); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxKeys = min(n, 1000)
		}
	}
	after := first(q["continuation-token"])
	if after == "" {
		after = first(q["start-after"])
	}
	rows, err := s.origin.List(r.Context(), bucket, prefix, after, maxKeys+1)
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	truncated := len(rows) > maxKeys
	if truncated {
		rows = rows[:maxKeys]
	}
	out := listBucketResult{
		Xmlns: s3Namespace, Name: bucket, Prefix: prefix, MaxKeys: maxKeys,
		IsTruncated: truncated,
	}
	for _, row := range rows {
		out.Contents = append(out.Contents, objectXML{
			Key: row.Key, LastModified: isoTime(row.CreatedAt),
			ETag: quoteETag(row.ETag), Size: row.Size, StorageClass: "STANDARD",
		})
	}
	out.KeyCount = len(out.Contents)
	if truncated && len(rows) > 0 {
		out.NextContinuationToken = rows[len(rows)-1].Key
	}
	writeXML(w, http.StatusOK, out)
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}
