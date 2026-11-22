package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

const ioChunk = 256 << 10

var copyBufPool = sync.Pool{New: func() any {
	b := make([]byte, ioChunk)
	return &b
}}

func copyBuf() []byte { return *(copyBufPool.Get().(*[]byte)) }

func putCopyBuf(b []byte) { copyBufPool.Put(&b) }

type Server struct {
	store     *store.Store
	log       *slog.Logger
	metrics   *requestMetrics
	readOnly  bool
	draining  atomic.Bool
	tier      *objcache.Tier
	sem       chan struct{}
	waiters   chan struct{}
	queueWait time.Duration
	queued    atomic.Uint64
	rejected  atomic.Uint64
	waitNanos atomic.Uint64
}

func New(st *store.Store, log *slog.Logger, readOnly bool, maxConc int, queueWait time.Duration, tier *objcache.Tier) *Server {
	if maxConc <= 0 {
		maxConc = 32
	}
	if log == nil {
		log = slog.Default()
	}
	queueSize := 0
	if queueWait > 0 {
		queueSize = 2 * maxConc
	}
	return &Server{
		store: st, log: log, metrics: &requestMetrics{}, readOnly: readOnly,
		tier:    tier,
		sem:     make(chan struct{}, maxConc),
		waiters: make(chan struct{}, queueSize), queueWait: queueWait,
	}
}

func (s *Server) acquire(ctx context.Context) (func(), bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	if s.sem == nil {
		return func() {}, true
	}
	select {
	case s.sem <- struct{}{}:
		return func() { <-s.sem }, true
	default:
	}
	if s.queueWait <= 0 {
		s.rejected.Add(1)
		return nil, false
	}
	select {
	case s.waiters <- struct{}{}:
	default:
		s.rejected.Add(1)
		return nil, false
	}
	s.queued.Add(1)
	start := time.Now()
	defer func() {
		<-s.waiters
		s.waitNanos.Add(uint64(time.Since(start)))
	}()
	timer := time.NewTimer(s.queueWait)
	defer timer.Stop()
	select {
	case s.sem <- struct{}{}:
		if ctx.Err() != nil {
			<-s.sem
			return nil, false
		}
		return func() { <-s.sem }, true
	case <-ctx.Done():
		return nil, false
	case <-timer.C:
		s.rejected.Add(1)
		return nil, false
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
			if s.log != nil {
				s.log.Error("s3 readiness check failed", "err", err)
			}
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
		return
	}
	if r.URL.Path == "/metrics" {
		s.writeMetrics(w, s.store)
		return
	}
	defer func() { s.metrics.Observe(r.Method, rw.status, time.Since(start), rw.n) }()
	ensureRequestID(w)
	release, ok := s.acquire(r.Context())
	if !ok {
		if r.Context().Err() != nil {
			writeError(rw, r, apiErr("RequestTimeout", "The request ended before S3 admission.", http.StatusRequestTimeout), s.log)
			return
		}
		w.Header().Set("Retry-After", "1")
		writeError(rw, r, apiErr("SlowDown", "The S3 admission queue is full or its wait budget expired; retry shortly.", http.StatusServiceUnavailable), s.log)
		return
	}
	defer release()
	if isPresigned(r.URL.Query()) {
		writeError(rw, r, apiErr("InvalidRequest", "presigned URLs are not supported: pgvs3 does not authenticate requests and must run behind network-level access control", http.StatusBadRequest), s.log)
		return
	}
	s.log.Debug("request", "method", r.Method, "host", r.Host, "path", r.URL.Path, "query", r.URL.RawQuery)
	if err := s.serve(rw, r); err != nil {
		s.fail(rw, r, err)
	}
}

func isPresigned(q url.Values) bool {
	var sigv4Alg, sigv4Cred, sigv4Sig, legKey, legSig bool
	for k := range q {
		switch {
		case strings.EqualFold(k, "X-Amz-Algorithm"):
			sigv4Alg = true
		case strings.EqualFold(k, "X-Amz-Credential"):
			sigv4Cred = true
		case strings.EqualFold(k, "X-Amz-Signature"):
			sigv4Sig = true
		case strings.EqualFold(k, "AWSAccessKeyId"):
			legKey = true
		case strings.EqualFold(k, "Signature"):
			legSig = true
		}
	}
	return sigv4Sig || sigv4Alg || sigv4Cred || (legKey && legSig)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	err = s.apiError(err)
	var ae *APIError
	if errors.As(err, &ae) && ae.Code == "SlowDown" {
		w.Header().Set("Retry-After", "1")
	}
	writeError(w, r, err, s.log)
}

func (s *Server) SetDraining(v bool) { s.draining.Store(v) }

func (s *Server) Close() {
	if s.tier != nil {
		_ = s.tier.Close()
	}
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) error {
	if err := prepareBody(r); err != nil {
		return err
	}
	bucket, key, err := splitPath(r)
	if err != nil {
		return errInvalidRequest("invalid object path")
	}
	if bucket != "" {
		if err := validateBucketName(bucket); err != nil {
			return err
		}
		if s.tier != nil && !s.tier.BucketAllowed(bucket) {
			return errNoSuchBucket()
		}
	}
	if s.readOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		return errAccessDenied("gateway is read-only")
	}
	q := r.URL.Query()
	switch {
	case bucket == "":
		if r.Method == http.MethodGet {
			return s.listBuckets(w, r)
		}
		return errMethodNotAllowed()
	case key == "":
		return s.bucketRequest(w, r, bucket, q)
	default:
		return s.objectRequest(w, r, bucket, key, q)
	}
}

func (s *Server) bucketRequest(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	c := routeContext{s: s, w: w, r: r, bucket: bucket, q: q}
	found, err := c.dispatch(bucketRoutes)
	if !found {
		return errMethodNotAllowed()
	}
	return err
}

func (s *Server) objectRequest(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	if len(key) > 1024 {
		return errInvalidRequest("object key exceeds 1024 bytes")
	}
	if q.Has("renameObject") || r.Header.Get("x-amz-rename-source") != "" ||
		r.Header.Get("x-amz-write-offset-bytes") != "" {
		return errNotImplemented()
	}
	c := routeContext{s: s, w: w, r: r, bucket: bucket, key: key, q: q}
	rt := selectRoute(objectRoutes, r, q)
	if rt == nil {
		return errMethodNotAllowed()
	}
	return rt.run(&c)
}

func errMethodNotAllowed() *APIError {
	return apiErr("MethodNotAllowed", "The specified method is not allowed against this resource.", http.StatusMethodNotAllowed)
}

func (s *Server) listBuckets(w http.ResponseWriter, _ *http.Request) error {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>pgvs3</ID><DisplayName>pgvs3</DisplayName></Owner><Buckets/></ListAllMyBucketsResult>`)
	return nil
}

type routeContext struct {
	s      *Server
	w      http.ResponseWriter
	r      *http.Request
	bucket string
	key    string
	q      url.Values
}

type route struct {
	name   string
	method string
	query  string
	query2 string
	header string
	run    func(*routeContext) error
}

func (rt *route) matches(r *http.Request, q url.Values) bool {
	if rt.method != "" && rt.method != r.Method {
		return false
	}
	if rt.query != "" && !q.Has(rt.query) {
		return false
	}
	if rt.query2 != "" && !q.Has(rt.query2) {
		return false
	}
	if rt.header != "" && r.Header.Get(rt.header) == "" {
		return false
	}
	return true
}

func selectRoute(table []route, r *http.Request, q url.Values) *route {
	for i := range table {
		if table[i].matches(r, q) {
			return &table[i]
		}
	}
	return nil
}

func (c *routeContext) dispatch(table []route) (bool, error) {
	rt := selectRoute(table, c.r, c.q)
	if rt == nil {
		return false, nil
	}
	return true, rt.run(c)
}

var bucketRoutes = []route{
	{name: "bucket-put", method: http.MethodPut, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-delete", method: http.MethodDelete, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-head", method: http.MethodHead, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-location", method: http.MethodGet, query: "location", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-versions", method: http.MethodGet, query: "versions", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-uploads", method: http.MethodGet, query: "uploads", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
	{name: "bucket-delete-objects", method: http.MethodPost, query: "delete", run: func(c *routeContext) error {
		return c.s.deleteObjects(c.w, c.r, c.bucket)
	}},
	{name: "bucket-get", method: http.MethodGet, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, "", false)
	}},
}

var objectRoutes = []route{
	{name: "object-head", method: http.MethodHead, run: func(c *routeContext) error {
		return c.s.headObject(c.w, c.r, c.bucket, c.key)
	}},
	{name: "object-attributes", method: http.MethodGet, query: "attributes", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
	}},
	{name: "object-list-parts", method: http.MethodGet, query: "uploadId", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
	}},
	{name: "object-get", method: http.MethodGet, run: func(c *routeContext) error {
		return c.s.getObject(c.w, c.r, c.bucket, c.key)
	}},
	{name: "object-upload-part-copy", method: http.MethodPut, query: "partNumber", query2: "uploadId",
		header: "x-amz-copy-source", run: func(c *routeContext) error {
			return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
		}},
	{name: "object-upload-part", method: http.MethodPut, query: "partNumber", query2: "uploadId", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
	}},
	{name: "object-copy", method: http.MethodPut, header: "x-amz-copy-source", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, true)
	}},
	{name: "object-put", method: http.MethodPut, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, true)
	}},
	{name: "object-create-multipart", method: http.MethodPost, query: "uploads", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
	}},
	{name: "object-complete-multipart", method: http.MethodPost, query: "uploadId", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, true)
	}},
	{name: "object-abort-multipart", method: http.MethodDelete, query: "uploadId", run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, false)
	}},
	{name: "object-delete", method: http.MethodDelete, run: func(c *routeContext) error {
		return c.s.proxyToOrigin(c.w, c.r, c.bucket, c.key, true)
	}},
}

type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func apiErr(code, message string, status int) *APIError {
	return &APIError{Code: code, Message: message, Status: status}
}

func errAccessDenied(msg string) *APIError { return apiErr("AccessDenied", msg, http.StatusForbidden) }
func errNoSuchKey() *APIError {
	return apiErr("NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
}
func errNoSuchBucket() *APIError {
	return apiErr("NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
}
func errInvalidBucketName() *APIError {
	return apiErr("InvalidBucketName", "The specified bucket is not valid.", http.StatusBadRequest)
}
func errPreconditionFailed() *APIError {
	return apiErr("PreconditionFailed", "At least one of the preconditions you specified did not hold.", http.StatusPreconditionFailed)
}
func errInvalidRange() *APIError {
	return apiErr("InvalidRange", "The requested range is not satisfiable.", http.StatusRequestedRangeNotSatisfiable)
}
func errInvalidRequest(msg string) *APIError {
	return apiErr("InvalidRequest", msg, http.StatusBadRequest)
}

func errNotImplemented() *APIError {
	return apiErr("NotImplemented", "A header or operation you provided implies functionality that is not implemented.", http.StatusNotImplemented)
}

func errInternal(err error, log *slog.Logger) *APIError {
	if log != nil {
		log.Error("internal error", "err", err)
	}
	return apiErr("InternalError", "We encountered an internal error. Please try again.", http.StatusInternalServerError)
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func writeError(w http.ResponseWriter, r *http.Request, err error, log *slog.Logger) {
	var ae *APIError
	if !errors.As(err, &ae) {
		ae = errInternal(err, log)
	}
	id := ensureRequestID(w)
	if r.Method == http.MethodHead {
		w.WriteHeader(ae.Status)
		return
	}
	body, _ := xml.Marshal(errorResponse{
		Code: ae.Code, Message: ae.Message, Resource: r.URL.Path, RequestID: id,
	})
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(ae.Status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

var reqSeq atomic.Uint64

func requestID() string {
	return fmt.Sprintf("%016x", reqSeq.Add(1))
}

func ensureRequestID(w http.ResponseWriter) string {
	id := w.Header().Get("x-amz-request-id")
	if id == "" {
		id = requestID()
		w.Header().Set("x-amz-request-id", id)
	}
	return id
}

func (s *Server) writeMetrics(w http.ResponseWriter, st *store.Store) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.metrics.Write(w, "pgvs3")
	fmt.Fprintf(w, "pgvs3_s3_admission_active_requests %d\npgvs3_s3_admission_waiting_requests %d\npgvs3_s3_admission_queue_limit %d\npgvs3_s3_admission_queued_total %d\npgvs3_s3_admission_rejected_total %d\npgvs3_s3_admission_wait_seconds_total %f\n",
		len(s.sem), len(s.waiters), cap(s.waiters), s.queued.Load(), s.rejected.Load(), float64(s.waitNanos.Load())/float64(time.Second))
	if st != nil {
		st.WriteMetrics(w)
	}
	if s.tier != nil {
		s.tier.WriteMetrics(w)
	}
}

type recWriter struct {
	http.ResponseWriter
	status int
	n      int64
}

func (w *recWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *recWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *recWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
