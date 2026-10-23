package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
				s.deleteOriginObjects(w, r, bucket)
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
			s.uploadOriginPart(w, r, bucket, key, q)
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
			s.createOriginMultipartUpload(w, r, bucket, key)
			return
		}
		if q.Has("uploadId") {
			s.completeOriginMultipartUpload(w, r, bucket, key, q)
			return
		}
		s.methodNotAllowed(w, r)

	case http.MethodDelete:
		if q.Has("uploadId") {
			ok, err := s.origin.AbortUpload(r.Context(), q.Get("uploadId"), bucket, key)
			if err != nil {
				writeError(w, r, errInternal(err, s.log), s.log)
				return
			}
			if !ok {
				writeError(w, r, errNoSuchUpload(), s.log)
				return
			}
			w.WriteHeader(http.StatusNoContent)
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

func (s *Server) createOriginMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	algorithm := r.Header.Get("x-amz-checksum-algorithm")
	if algorithm != "" && !checksumAlgorithms[algorithm] {
		writeError(w, r, errNotImplemented(), s.log)
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
	var alg *string
	if algorithm != "" {
		alg = &algorithm
	}
	id, err := s.origin.CreateUpload(r.Context(), bucket, key, meta, ct, alg)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: id,
	})
}

func (s *Server) uploadOriginPart(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	partNo, err := strconv.ParseInt(q.Get("partNumber"), 10, 32)
	if err != nil || partNo < 1 || partNo > 10000 {
		writeError(w, r, errInvalidPart(), s.log)
		return
	}
	in, err := newIntegrity(r.Header)
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	writer, err := s.origin.StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo))
	if err != nil {
		if errors.Is(err, store.ErrNoUpload) {
			writeError(w, r, errNoSuchUpload(), s.log)
			return
		}
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	body := io.ReadCloser(r.Body)
	if len(in.h) > 0 {
		body = &integrityReader{r: body, in: in}
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, body, buf); err != nil {
		writer.Abort()
		writeError(w, r, bodyError(err), s.log)
		return
	}
	checks, err := in.verify(r.Trailer)
	if err != nil {
		writer.Abort()
		writeError(w, r, err, s.log)
		return
	}
	pub, err := writer.Finish(checks)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	w.Header().Set("ETag", quoteETag(pub.ETag))
	if pub.Checksum != nil {
		w.Header().Set(pub.Checksum.Name, pub.Checksum.Value)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) completeOriginMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeError(w, r, bodyError(err), s.log)
		return
	}
	var req completeMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, errInvalidRequest("malformed CompleteMultipartUpload XML"), s.log)
		return
	}
	listed := make([]store.ListedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		if p.PartNumber < 1 || p.PartNumber > 10000 || p.ETag == "" {
			writeError(w, r, errInvalidPart(), s.log)
			return
		}
		var checks []store.Checksum
		for name, value := range map[string]string{
			"CRC32": p.ChecksumCRC32, "CRC32C": p.ChecksumCRC32C, "CRC64NVME": p.ChecksumCRC64NVME,
			"SHA1": p.ChecksumSHA1, "SHA256": p.ChecksumSHA256,
		} {
			if value != "" {
				checks = append(checks, store.Checksum{Name: name, Value: value})
			}
		}
		if len(checks) > 1 {
			writeError(w, r, errInvalidRequest("a part may carry only one checksum"), s.log)
			return
		}
		var cs *store.Checksum
		if len(checks) == 1 {
			cs = &checks[0]
		}
		listed = append(listed, store.ListedPart{PartNo: int32(p.PartNumber), ETag: stripETag(p.ETag), Checksum: cs})
	}
	done, err := s.origin.CompleteUpload(r.Context(), q.Get("uploadId"), bucket, key, listed)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNoUpload):
			writeError(w, r, errNoSuchUpload(), s.log)
		case errors.Is(err, store.ErrInvalidPart):
			writeError(w, r, errInvalidPart(), s.log)
		default:
			writeError(w, r, s.ingestError(err), s.log)
		}
		return
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns: s3Namespace, Location: fmt.Sprintf("%s://%s/%s/%s", scheme, r.Host, done.Bucket, done.Key),
		Bucket: done.Bucket, Key: done.Key, ETag: quoteETag(done.ETag),
	})
}

func (s *Server) deleteOriginObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeError(w, r, bodyError(err), s.log)
		return
	}
	if want := r.Header.Get("Content-MD5"); want != "" {
		sum := md5.Sum(body)
		if base64.StdEncoding.EncodeToString(sum[:]) != want {
			writeError(w, r, apiErr("BadDigest", "The Content-MD5 you specified did not match the request body.", http.StatusBadRequest), s.log)
			return
		}
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, errInvalidRequest("malformed Delete XML"), s.log)
		return
	}
	if len(req.Objects) == 0 || len(req.Objects) > 1000 {
		writeError(w, r, errInvalidRequest("Delete must contain between 1 and 1000 objects"), s.log)
		return
	}
	out := deleteResult{Xmlns: s3Namespace}
	for _, o := range req.Objects {
		if o.Key == "" || len(o.Key) > 1024 {
			writeError(w, r, errInvalidRequest("invalid object key"), s.log)
			return
		}
		if err := s.origin.Delete(r.Context(), bucket, o.Key); err != nil {
			writeError(w, r, errInternal(err, s.log), s.log)
			return
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedXML{Key: o.Key})
		}
	}
	writeXML(w, http.StatusOK, out)
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
