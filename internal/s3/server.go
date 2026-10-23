package s3

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgconn"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

const ioChunk = 256 << 10

var copyBufPool = sync.Pool{New: func() any {
	b := make([]byte, ioChunk)
	return &b
}}

func copyBuf() []byte     { return *(copyBufPool.Get().(*[]byte)) }
func putCopyBuf(b []byte) { copyBufPool.Put(&b) }

// Server is the S3 HTTP front end. It is stateless over PostgreSQL, so any
// gateway instance can serve any request of any multipart upload.
type Server struct {
	store       *store.Store
	log         *slog.Logger
	metrics     *metrics
	readOnly    bool
	draining    atomic.Bool
	origin      *objcache.Tier
	metricsHook func(io.Writer)
}

// SetMetricsHook appends another protocol's counters to /metrics.
func (s *Server) SetMetricsHook(f func(io.Writer)) { s.metricsHook = f }

func New(st *store.Store, log *slog.Logger, readOnly bool) *Server {
	return &Server{
		store: st, log: log, metrics: newMetrics(), readOnly: readOnly,
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &recWriter{ResponseWriter: w}
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	if r.URL.Path == "/readyz" {
		if s.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		if err := s.store.Ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
		return
	}
	if r.URL.Path == "/metrics" {
		s.metrics.write(w, s.store)
		if s.metricsHook != nil {
			s.metricsHook(w)
		}
		return
	}
	defer func() { s.metrics.observe(r.Method, rw.status, time.Since(start), rw.n) }()
	w.Header().Set("x-amz-request-id", requestIDHeader())
	s.log.Debug("request", "method", r.Method, "host", r.Host, "path", r.URL.Path, "query", r.URL.RawQuery)
	s.serve(rw, r)
}

func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	prepareBody(r)
	if strings.HasPrefix(r.URL.EscapedPath(), CachePrefix) {
		s.serveCacheHTTP(w, r)
		return
	}
	bucket, key, err := splitPath(r)
	if err != nil {
		writeError(w, r, errInvalidRequest("invalid object path"), s.log)
		return
	}
	if s.readOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, r, errAccessDenied("gateway is read-only"), s.log)
		return
	}
	if s.origin != nil {
		s.serveOrigin(w, r, bucket, key)
		return
	}
	q := r.URL.Query()
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			s.listBuckets(w, r)
			return
		}
		s.methodNotAllowed(w, r)
	case key == "":
		s.bucketRequest(w, r, bucket, q)
	default:
		s.objectRequest(w, r, bucket, key, q)
	}
}

func (s *Server) bucketRequest(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	switch r.Method {
	case http.MethodPut:
		s.createBucket(w, r, bucket)
	case http.MethodDelete:
		s.deleteBucket(w, r, bucket)
	case http.MethodHead:
		s.headBucket(w, r, bucket)
	case http.MethodGet:
		switch {
		case q.Has("location"):
			s.getBucketLocation(w, r, bucket)
		case q.Has("uploads"):
			s.listMultipartUploads(w, r, bucket, q)
		case r.URL.RawQuery == "" && r.Header.Get("Authorization") == "" && r.Header.Get("x-amz-date") == "":
			if _, err := s.store.MetaHeader(r.Context(), bucket, "index.html"); err == nil {
				s.getObject(w, r, bucket, "index.html")
				return
			}
			s.listObjectsV2(w, r, bucket, q)
		default:
			s.listObjectsV2(w, r, bucket, q)
		}
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

func (s *Server) objectRequest(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	if len(key) > 1024 {
		writeError(w, r, errInvalidRequest("object key exceeds 1024 bytes"), s.log)
		return
	}
	if q.Has("renameObject") || r.Header.Get("x-amz-rename-source") != "" ||
		r.Header.Get("x-amz-write-offset-bytes") != "" {
		writeError(w, r, errNotImplemented(), s.log)
		return
	}
	switch r.Method {
	case http.MethodHead:
		s.headObject(w, r, bucket, key)
	case http.MethodGet:
		if q.Has("attributes") {
			s.getObjectAttributes(w, r, bucket, key)
			return
		}
		if q.Has("uploadId") {
			s.listParts(w, r, bucket, key, q)
			return
		}
		s.getObject(w, r, bucket, key)
	case http.MethodPut:
		if q.Has("partNumber") && q.Has("uploadId") {
			if r.Header.Get("x-amz-copy-source") != "" {
				s.uploadPartCopy(w, r, bucket, key, q)
				return
			}
			s.uploadPart(w, r, bucket, key, q)
			return
		}
		if r.Header.Get("x-amz-copy-source") != "" {
			s.copyObject(w, r, bucket, key)
			return
		}
		s.putObject(w, r, bucket, key)
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
		s.deleteObject(w, r, bucket, key)
	default:
		s.methodNotAllowed(w, r)
	}
}

func (s *Server) methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, apiErr("MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed), s.log)
}

func (s *Server) headBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	ok, err := s.store.BucketExists(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, store.ErrInvalidBucket) {
			writeError(w, r, errInvalidBucketName(), s.log)
			return
		}
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	if !ok {
		writeError(w, r, errNoSuchBucket(), s.log)
		return
	}
	w.Header().Set("x-amz-bucket-region", "us-east-1")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	src := strings.TrimPrefix(strings.TrimSpace(r.Header.Get("x-amz-copy-source")), "/")
	if src == "" {
		src = strings.TrimPrefix(strings.TrimSpace(r.URL.Query().Get("clone")), "/")
	}
	if src != "" {
		if err := s.store.CloneBucket(r.Context(), src, bucket); err != nil {
			switch {
			case errors.Is(err, store.ErrInvalidBucket):
				writeError(w, r, errInvalidBucketName(), s.log)
			case errors.Is(err, store.ErrNoBucket):
				writeError(w, r, errNoSuchBucket(), s.log)
			case errors.Is(err, store.ErrBucketNotEmpty):
				writeError(w, r, apiErr("BucketNotEmpty", "The destination bucket is not empty.", http.StatusConflict), s.log)
			default:
				writeError(w, r, errInternal(err, s.log), s.log)
			}
			return
		}
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
		return
	}
	s.log.Debug("create bucket", "bucket", bucket, "content_length", r.ContentLength)
	if err := s.store.CreateBucket(r.Context(), bucket); err != nil {
		s.log.Debug("create bucket failed", "bucket", bucket, "err", err)
		if errors.Is(err, store.ErrInvalidBucket) {
			writeError(w, r, errInvalidBucketName(), s.log)
			return
		}
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	s.log.Debug("created bucket", "bucket", bucket)
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	force := q.Get("force") == "true" || q.Get("force") == "1" ||
		strings.EqualFold(r.Header.Get("x-amz-force-delete"), "true")
	res, err := s.store.DropBucket(r.Context(), bucket, force)
	if err != nil {
		if errors.Is(err, store.ErrInvalidBucket) {
			writeError(w, r, errInvalidBucketName(), s.log)
			return
		}
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	switch res {
	case store.BucketDeleted:
		w.WriteHeader(http.StatusNoContent)
	case store.BucketNotEmpty:
		writeError(w, r, apiErr("BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict), s.log)
	default:
		writeError(w, r, errNoSuchBucket(), s.log)
	}
}

func (s *Server) listBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.store.Buckets(r.Context())
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	out := listAllMyBucketsResult{
		Xmlns: s3Namespace,
		Owner: ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
	}
	for _, b := range buckets {
		out.Buckets = append(out.Buckets, bucketXML{Name: b.Name, CreationDate: isoTime(b.Created)})
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) getBucketLocation(w http.ResponseWriter, _ *http.Request, _ string) {
	writeXML(w, http.StatusOK, locationConstraint{Xmlns: s3Namespace, Value: "us-east-1"})
}

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	m, err := s.store.MetaHeader(r.Context(), bucket, key)
	if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
		key += "index.html"
		m, err = s.store.MetaHeader(r.Context(), bucket, key)
	}
	if err != nil {
		s.metaError(w, r, bucket, err)
		return
	}
	part, hasPart, err := parsePartNumber(r.URL.Query())
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	length := m.Size
	if hasPart {
		start, end, ok := partBounds(m, part)
		if !ok {
			writeError(w, r, errInvalidRange(), s.log)
			return
		}
		length = end - start + 1
		if length < 0 {
			length = 0
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, m.Size))
	}
	setPartsCount(w, m)
	if err := readCondition(r, m.ETag); err != nil {
		if errors.Is(err, errNotModifiedSentinel) {
			w.Header().Set("ETag", quoteETag(m.ETag))
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeError(w, r, err, s.log)
		return
	}
	setObjectHeaders(w, m, key)
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	part, hasPart, err := parsePartNumber(r.URL.Query())
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	first, last, suffix, ranged, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		writeError(w, r, errInvalidRange(), s.log)
		return
	}
	var partCount int
	if hasPart {
		if ranged {
			writeError(w, r, errInvalidRequest("partNumber and Range are mutually exclusive"), s.log)
			return
		}
		m, err := s.store.MetaHeader(r.Context(), bucket, key)
		if err != nil {
			s.metaError(w, r, bucket, err)
			return
		}
		start, end, ok := partBounds(m, part)
		if !ok {
			writeError(w, r, errInvalidRange(), s.log)
			return
		}
		first, last, suffix, ranged = start, end, -1, true
		partCount = partsCount(m)
	}
	cond := &store.ReadCondition{IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match")}
	sm, body, err := s.store.GetBody(r.Context(), bucket, key, first, last, suffix, cond)
	if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
		key += "index.html"
		sm, body, err = s.store.GetBody(r.Context(), bucket, key, first, last, suffix, cond)
	}
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			s.metaError(w, r, bucket, err)
		case errors.Is(err, store.ErrPrecondition):
			writeError(w, r, errPreconditionFailed(), s.log)
		case errors.Is(err, store.ErrNotModified):
			w.Header().Set("ETag", quoteETag(metaETagFromCondition(cond, r)))
			w.WriteHeader(http.StatusNotModified)
		case errors.Is(err, store.ErrInvalidRange):
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(smSize(sm), 10))
			writeError(w, r, errInvalidRange(), s.log)
		default:
			writeError(w, r, errInternal(err, s.log), s.log)
		}
		return
	}
	defer body.Close()
	if ranged && sm.Len() == 0 && sm.Size > 0 {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(sm.Size, 10))
		writeError(w, r, errInvalidRange(), s.log)
		return
	}
	setObjectHeaders(w, &store.Meta{
		Size: sm.Size, ETag: sm.ETag, UserMetadata: sm.UserMetadata,
		ContentType: sm.ContentType, CreatedAt: sm.CreatedAt,
		FileID: sm.FileID, Parts: sm.Parts, PartEnds: sm.PartEnds,
	}, key)
	if partCount > 0 {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(partCount))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(sm.Len(), 10))
	if ranged {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", sm.Start, sm.End, sm.Size))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(w, body, buf); err != nil {
		s.log.Debug("GET body interrupted", "bucket", bucket, "key", key, "err", err)
	}
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Header.Get("Content-Encoding") != "" {
		writeError(w, r, errNotImplemented(), s.log)
		return
	}
	if err := validateSSE(r); err != nil {
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
	cond, ifMatch, err := putCondition(r)
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	in, err := newIntegrity(r.Header)
	if err != nil {
		writeError(w, r, err, s.log)
		return
	}
	if maxInline := int64(store.MaxInlineBytes); r.ContentLength >= 0 && r.ContentLength <= maxInline {
		body := io.ReadCloser(r.Body)
		if len(in.h) > 0 {
			body = &integrityReader{r: body, in: in}
		}
		data, err := io.ReadAll(io.LimitReader(body, maxInline+1))
		if err != nil {
			writeError(w, r, bodyError(err), s.log)
			return
		}
		if int64(len(data)) > maxInline {
			writeError(w, r, errInvalidRequest("inline object exceeds 64 KiB"), s.log)
			return
		}
		checks, err := in.verify(r.Trailer)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		obj, err := s.store.PutInline(r.Context(), bucket, key, data, cond, ifMatch, meta, ct)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrNoBucket):
				writeError(w, r, errNoSuchBucket(), s.log)
			case errors.Is(err, store.ErrPrecondition):
				writeError(w, r, errPreconditionFailed(), s.log)
			default:
				writeError(w, r, s.ingestError(err), s.log)
			}
			return
		}
		w.Header().Set("ETag", quoteETag(obj.ETag))
		setChecksumHeaders(w, checks)
		w.WriteHeader(http.StatusOK)
		return
	}
	writer, err := s.store.StartObject(r.Context(), bucket, key, cond, ifMatch, meta, ct)
	if err != nil {
		s.metaError(w, r, bucket, err)
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
	setChecksumHeaders(w, checks)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.URL.Query().Has("versionId") || r.Header.Get("If-Match") != "" ||
		r.Header.Get("x-amz-if-match-size") != "" || r.Header.Get("x-amz-if-match-last-modified-time") != "" {
		writeError(w, r, errNotImplemented(), s.log)
		return
	}
	if _, err := s.store.Delete(r.Context(), bucket, key); err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
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
	keys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		if o.Key == "" || len(o.Key) > 1024 {
			writeError(w, r, errInvalidRequest("invalid object key"), s.log)
			return
		}
		keys = append(keys, o.Key)
	}
	if s.origin != nil {
		err = s.origin.DeleteMany(r.Context(), bucket, keys)
	} else {
		err = s.store.DeleteMany(r.Context(), bucket, keys)
	}
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	out := deleteResult{Xmlns: s3Namespace}
	if !req.Quiet {
		for _, k := range keys {
			out.Deleted = append(out.Deleted, deletedXML{Key: k})
		}
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if r.Header.Get("Content-Encoding") != "" {
		writeError(w, r, errNotImplemented(), s.log)
		return
	}
	if err := validateSSE(r); err != nil {
		writeError(w, r, err, s.log)
		return
	}
	algorithm := r.Header.Get("x-amz-checksum-algorithm")
	if algorithm != "" && !checksumAlgorithms[algorithm] {
		writeError(w, r, errNotImplemented(), s.log)
		return
	}
	if typ := r.Header.Get("x-amz-checksum-type"); typ != "" && (algorithm == "" || typ != "COMPOSITE") {
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
	var id string
	if s.origin != nil {
		id, err = s.origin.CreateUpload(r.Context(), bucket, key, meta, ct, alg)
	} else {
		id, err = s.store.CreateUpload(r.Context(), bucket, key, meta, ct, alg)
	}
	if err != nil {
		if errors.Is(err, store.ErrNoBucket) {
			writeError(w, r, errNoSuchBucket(), s.log)
			return
		}
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: id,
	})
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
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
	var writer *store.ChunkWriter
	if s.origin != nil {
		writer, err = s.origin.StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo))
	} else {
		writer, err = s.store.StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo))
	}
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

func (s *Server) completeMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
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
	var done *store.Completed
	if s.origin != nil {
		done, err = s.origin.CompleteUpload(r.Context(), q.Get("uploadId"), bucket, key, listed)
	} else {
		done, err = s.store.CompleteUpload(r.Context(), q.Get("uploadId"), bucket, key, listed)
	}
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

func (s *Server) abortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	var (
		ok  bool
		err error
	)
	if s.origin != nil {
		ok, err = s.origin.AbortUpload(r.Context(), q.Get("uploadId"), bucket, key)
	} else {
		ok, err = s.store.AbortUpload(r.Context(), q.Get("uploadId"), bucket, key)
	}
	if err != nil {
		writeError(w, r, errInternal(err, s.log), s.log)
		return
	}
	if !ok {
		writeError(w, r, errNoSuchUpload(), s.log)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) {
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, r, errInvalidRequest("invalid max-keys"), s.log)
			return
		}
		maxKeys = min(n, 1000)
	}
	startAfter := q.Get("start-after")
	after := q.Get("continuation-token")
	if after == "" {
		after = startAfter
	}
	encode := q.Get("encoding-type") == "url"
	if q.Get("encoding-type") != "" && !encode {
		writeError(w, r, errInvalidRequest("unsupported encoding-type"), s.log)
		return
	}
	bound := prefixEnd(prefix)
	var contents []objectXML
	var common []commonPrefixXML
	seen := map[string]bool{}
	if delimiter != "" {
		if rest := strings.TrimPrefix(after, prefix); rest != after {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				seen[prefix+rest[:idx+len(delimiter)]] = true
			}
		}
	}
	truncated := false
	if maxKeys > 0 {
	outer:
		for {
			rows, err := s.store.List(r.Context(), bucket, prefix, bound, after, 1024)
			if err != nil {
				writeError(w, r, errInternal(err, s.log), s.log)
				return
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				rest := strings.TrimPrefix(row.Key, prefix)
				if delimiter != "" {
					if idx := strings.Index(rest, delimiter); idx >= 0 {
						cp := prefix + rest[:idx+len(delimiter)]
						if !seen[cp] {
							if len(contents)+len(common) >= maxKeys {
								truncated = true
								break outer
							}
							seen[cp] = true
							display := cp
							if encode {
								display = awsEncode(cp, true)
							}
							common = append(common, commonPrefixXML{Prefix: display})
						}
						after = row.Key
						continue
					}
				}
				if len(contents)+len(common) >= maxKeys {
					truncated = true
					break outer
				}
				display := row.Key
				if encode {
					display = awsEncode(row.Key, true)
				}
				storageClass := "STANDARD"
				contents = append(contents, objectXML{
					Key: display, LastModified: isoTime(row.CreatedAt),
					ETag: quoteETag(row.ETag), Size: row.Size, StorageClass: storageClass,
				})
				after = row.Key
			}
			if len(rows) < 1024 {
				break
			}
		}
	}
	displayPrefix, displayDelim, displayStart := prefix, delimiter, startAfter
	if encode {
		displayPrefix, displayDelim, displayStart = awsEncode(prefix, true), awsEncode(delimiter, true), awsEncode(startAfter, true)
	}
	out := listBucketResult{
		Xmlns: s3Namespace, Name: bucket, Prefix: displayPrefix, KeyCount: len(contents) + len(common),
		MaxKeys: maxKeys, IsTruncated: truncated, Contents: contents, CommonPrefixes: common,
		ContinuationToken: q.Get("continuation-token"), StartAfter: displayStart,
	}
	if delimiter != "" {
		out.Delimiter = displayDelim
	}
	if encode {
		out.EncodingType = "url"
	}
	if truncated {
		out.NextContinuationToken = after
	}
	writeXML(w, http.StatusOK, out)
}

func (s *Server) metaError(w http.ResponseWriter, r *http.Request, _ string, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, r, errNoSuchKey(), s.log)
	case errors.Is(err, store.ErrInvalidBucket):
		writeError(w, r, errInvalidBucketName(), s.log)
	default:
		writeError(w, r, errInternal(err, s.log), s.log)
	}
}

func (s *Server) ingestError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" && pgErr.ConstraintName == "objects_bucket_fkey" {
		return errNoSuchBucket()
	}
	switch {
	case errors.Is(err, store.ErrPrecondition):
		return errPreconditionFailed()
	case errors.Is(err, store.ErrNoBucket):
		return errNoSuchBucket()
	case errors.Is(err, store.ErrInvalidBucket):
		return errInvalidBucketName()
	case errors.Is(err, store.ErrNoUpload):
		return errNoSuchUpload()
	case errors.Is(err, store.ErrMissingChecksum):
		return errInvalidRequest("part lacks the upload's checksum")
	default:
		return errInternal(err, s.log)
	}
}

func bodyError(err error) error {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return apiErr("IncompleteBody", "The request body terminated unexpectedly.", http.StatusBadRequest)
	}
	return apiErr("InvalidRequest", "Could not read the request body.", http.StatusBadRequest)
}

func putCondition(r *http.Request) (store.Condition, string, error) {
	ifMatch := r.Header.Get("If-Match")
	ifNone := r.Header.Get("If-None-Match")
	switch {
	case ifMatch == "" && ifNone == "":
		return store.Unconditional, "", nil
	case ifMatch == "" && strings.TrimSpace(ifNone) == "*":
		return store.IfAbsent, "", nil
	case ifMatch != "" && ifNone == "" && validETag(ifMatch):
		return store.IfMatch, stripETag(ifMatch), nil
	default:
		return 0, "", errInvalidRequest("unsupported conditional PUT")
	}
}

func readCondition(r *http.Request, etag string) error {
	if v := r.Header.Get("If-Match"); v != "" && !etagListMatches(v, etag) {
		return errPreconditionFailed()
	}
	if v := r.Header.Get("If-None-Match"); v != "" && etagListMatches(v, etag) {
		return errNotModifiedSentinel
	}
	return nil
}

var errNotModifiedSentinel = errors.New("not modified")

func etagListMatches(header, etag string) bool {
	for _, want := range strings.Split(header, ",") {
		want = strings.TrimSpace(want)
		if want == "*" {
			return true
		}
		want = strings.Trim(strings.TrimPrefix(want, "W/"), `"`)
		if want == etag {
			return true
		}
	}
	return false
}

func setObjectHeaders(w http.ResponseWriter, m *store.Meta, key string) {
	ct := m.ContentType
	if ct == "" || ct == "application/octet-stream" || ct == "application/x-www-form-urlencoded" {
		if inferred := mime.TypeByExtension(path.Ext(key)); inferred != "" {
			ct = inferred
		}
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", quoteETag(m.ETag))
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Last-Modified", m.CreatedAt.UTC().Format(http.TimeFormat))
	for k, vs := range metadataHeader(m.UserMetadata) {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
}

func setChecksumHeaders(w http.ResponseWriter, checks []store.Checksum) {
	for _, c := range checks {
		if c.Name == "content-md5" {
			continue
		}
		w.Header().Set(c.Name, c.Value)
	}
}

func metaETagFromCondition(cond *store.ReadCondition, r *http.Request) string {
	if cond != nil {
		return stripETag(cond.IfNoneMatch)
	}
	return ""
}

func smSize(sm *store.SliceMeta) int64 {
	if sm == nil {
		return 0
	}
	return sm.Size
}
