package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

// SetOrigin sets the default read-through/write-through origin tier. Per-bucket
// and global origins stored in PostgreSQL (`s3p.namespaces.attrs["origin"]`)
// are also resolved dynamically by originFor.
func (s *Server) SetOrigin(t *objcache.Tier) { s.origins.SetDefault(t) }

func (s *Server) originFor(ctx context.Context, bucket string) *objcache.Tier {
	return s.origins.ForBucket(ctx, bucket)
}

func (s *Server) serveOriginBucket(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	switch r.Method {
	case http.MethodHead:
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if q.Has("location") {
			s.getBucketLocation(w, r, bucket)
			return
		}
		if q.Has("versions") {
			s.listOriginObjectVersions(w, r, bucket, q)
			return
		}
		s.listObjectsV2(w, r, bucket, q)
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
}

func (s *Server) headOriginObject(w http.ResponseWriter, r *http.Request, origin *objcache.Tier, bucket, key string) {
	obj, err := origin.Head(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNoSuchKey(), s.log)
			return
		}
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	if err := readCondition(r, obj.ETag); err != nil {
		if errors.Is(err, errNotModifiedSentinel) {
			w.Header().Set("ETag", quoteETag(obj.ETag))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeError(w, r, err, s.log)
		return
	}
	writeOriginHeaders(w, obj)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getOriginObject(w http.ResponseWriter, r *http.Request, origin *objcache.Tier, bucket, key string) {
	first, last, suffix, _, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		writeError(w, r, errInvalidRange(), s.log)
		return
	}
	obj, err := origin.Open(r.Context(), bucket, key, first, last, suffix)
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
	if obj.Size == 0 && obj.Ranged {
		writeError(w, r, errInvalidRange(), s.log)
		return
	}
	if err := readCondition(r, obj.ETag); err != nil {
		if errors.Is(err, errNotModifiedSentinel) {
			w.Header().Set("ETag", quoteETag(obj.ETag))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeError(w, r, err, s.log)
		return
	}
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
}

func (s *Server) putOriginObject(w http.ResponseWriter, r *http.Request, origin *objcache.Tier, bucket, key string) {
	if err := validateWriteHeaders(r); err != nil {
		writeError(w, r, err, s.log)
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
	m, err := origin.Upload(r.Context(), bucket, key, body, meta, ct)
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
