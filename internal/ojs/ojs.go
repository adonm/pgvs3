package ojs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"uuid"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/telemetry"
)

const (
	maxBody        = 1 << 20
	maxFetchQueues = 64
	longPoll       = 2 * time.Second
	pollPeriod     = 100 * time.Millisecond
)

type jobStore interface {
	EnqueueJobs(context.Context, []store.EnqueueInput) ([]store.Job, error)
	FetchJobs(context.Context, []string, int, string, time.Duration) ([]store.Job, error)
	GetJob(context.Context, string) (store.Job, error)
	CancelJob(context.Context, string) (store.Job, error)
	AckJob(context.Context, string, string, []byte) (store.Job, error)
	NackJob(context.Context, string, string, []byte, bool) (store.Job, error)
	HeartbeatJobs(context.Context, string, map[string]string, time.Duration) ([]string, error)
	QueueStats(context.Context, string) (store.QueueStats, error)
	AnyQueueExists(context.Context, []string) (bool, error)
}

type streamStore interface {
	AppendStreamMessages(context.Context, string, []store.StreamMessageInput) ([]store.StreamMessage, error)
	SubscribeStream(context.Context, string, string, *int64) (store.StreamSubscription, error)
	UnsubscribeStream(context.Context, string, string) (bool, error)
	ReceiveStreamBatch(context.Context, string, string, int) (*store.StreamBatch, error)
	ReceiveStreamCoopBatch(context.Context, string, string, string, int) (*store.StreamBatch, error)
	GetStreamBatch(context.Context, string, int64) (*store.StreamBatch, error)
	AckStreamBatch(context.Context, string, int64) (int64, error)
	NackStreamMessage(context.Context, string, int64, string, time.Duration, string) (string, error)
	StreamInfo(context.Context, string) (store.StreamInfo, error)
	StreamConfig(context.Context, string) (store.StreamConfig, error)
	SetStreamConfig(context.Context, string, int, time.Duration) (store.StreamConfig, error)
	ListStreamConsumers(context.Context, string) ([]store.StreamConsumerInfo, error)
	ListDeadLetters(context.Context, string, int) ([]store.DeadLetter, error)
	ReplayDeadLetter(context.Context, string, int64) (string, error)
	ReplayAllDeadLetters(context.Context, string) (int64, error)
	PurgeDeadLetters(context.Context, string, time.Duration) (int64, error)
}

type Store interface {
	jobStore
	streamStore
	ListQueues(context.Context, int, int) ([]store.NamespaceInfo, int64, error)
	NamespaceExists(context.Context, ns.Kind, string) (bool, error)
	Ping(context.Context) error
}

type Server struct {
	st       Store
	log      *slog.Logger
	readOnly bool
	version  string
	started  time.Time
	sem      chan struct{}
	longPoll time.Duration
	metrics  *telemetry.RequestMetrics
}

func New(st Store, log *slog.Logger, readOnly bool, maxConc int, version string) *Server {
	if maxConc <= 0 {
		maxConc = 16
	}
	if version == "" {
		version = "dev"
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{st: st, log: log, readOnly: readOnly, version: version,
		started: time.Now(), sem: make(chan struct{}, maxConc), longPoll: longPoll, metrics: telemetry.NewRequestMetrics()}
}

func (s *Server) Metrics(w io.Writer) { s.metrics.Write(w, "pgvs3_ojs") }

func (s *Server) SetVersion(v string) {
	if v != "" {
		s.version = v
	}
}

func (s *Server) acquire(ctx context.Context) (func(), bool) {
	select {
	case s.sem <- struct{}{}:
		return func() { <-s.sem }, true
	case <-ctx.Done():
		return nil, false
	}
}

type endpoint func(*Server, http.ResponseWriter, *http.Request) (any, error)

func writable(run endpoint) endpoint {
	return func(s *Server, w http.ResponseWriter, r *http.Request) (any, error) {
		if e := s.requireWritable(); e != nil {
			return nil, e
		}
		return run(s, w, r)
	}
}

func jsonEndpoint[T any](run func(*Server, *http.Request, T) (any, error)) endpoint {
	return writable(func(s *Server, w http.ResponseWriter, r *http.Request) (any, error) {
		var req T
		if e := s.decode(w, r, &req); e != nil {
			return nil, e
		}
		return run(s, r, req)
	})
}

type ojsRoute struct {
	name, method, path, prefix, suffix string
	run                                endpoint
}

func (rt ojsRoute) matches(r *http.Request) bool {
	return (rt.method == "" || rt.method == r.Method) &&
		(rt.path == "" || rt.path == r.URL.Path) &&
		(rt.prefix == "" || strings.HasPrefix(r.URL.Path, rt.prefix)) &&
		(rt.suffix == "" || strings.HasSuffix(r.URL.Path, rt.suffix))
}

func selectOjsRoute(table []ojsRoute, r *http.Request) *ojsRoute {
	for i := range table {
		if table[i].matches(r) {
			return &table[i]
		}
	}
	return nil
}

func dispatchOjsRoute(table []ojsRoute, s *Server, w http.ResponseWriter, r *http.Request) bool {
	rt := selectOjsRoute(table, r)
	if rt == nil {
		return false
	}
	body, err := rt.run(s, w, r)
	s.respond(w, body, err)
	return true
}

var fetchRoutes = []ojsRoute{
	{name: "worker-fetch", method: http.MethodPost, path: "/ojs/v1/workers/fetch", run: writable((*Server).handleFetch)},
	{name: "stream-receive", method: http.MethodPost, prefix: "/ojs/v1/queues/", suffix: "/receive", run: (*Server).handleStreamReceiveRoute},
}

var ojsRoutes = []ojsRoute{
	{name: "manifest", method: http.MethodGet, path: "/ojs/manifest", run: (*Server).handleManifest},
	{name: "health", method: http.MethodGet, path: "/ojs/v1/health", run: (*Server).handleHealth},
	{name: "job-enqueue", method: http.MethodPost, path: "/ojs/v1/jobs", run: jsonEndpoint((*Server).handleEnqueue)},
	{name: "job-batch", method: http.MethodPost, path: "/ojs/v1/jobs/batch", run: jsonEndpoint((*Server).handleBatch)},
	{name: "worker-ack", method: http.MethodPost, path: "/ojs/v1/workers/ack", run: jsonEndpoint((*Server).handleAck)},
	{name: "worker-nack", method: http.MethodPost, path: "/ojs/v1/workers/nack", run: jsonEndpoint((*Server).handleNack)},
	{name: "worker-heartbeat", method: http.MethodPost, path: "/ojs/v1/workers/heartbeat", run: jsonEndpoint((*Server).handleHeartbeat)},
	{name: "queues", method: http.MethodGet, path: "/ojs/v1/queues", run: (*Server).handleQueues},
	{name: "job", prefix: "/ojs/v1/jobs/", run: (*Server).handleJob},
	{name: "queue-stats", method: http.MethodGet, prefix: "/ojs/v1/queues/", suffix: "/stats", run: (*Server).handleQueueStats},
	{name: "stream-ack", method: http.MethodPost, path: "/ojs/v1/streams/ack", run: jsonEndpoint((*Server).handleStreamAck)},
	{name: "stream-nack", method: http.MethodPost, path: "/ojs/v1/streams/nack", run: jsonEndpoint((*Server).handleStreamNack)},
	{name: "stream", prefix: "/ojs/v1/queues/", run: (*Server).handleStream},
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	rw := &statusWriter{ResponseWriter: w}
	w = rw
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(15 * time.Second))
	_ = controller.SetWriteDeadline(time.Now().Add(30 * time.Second))
	defer func() { s.metrics.Observe(r.Method, rw.status, time.Since(started), rw.n) }()
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("OJS-Version", specVersion)
	w.Header().Set("X-Request-Id", "req_"+uuid.NewV7().String())
	if v := r.Header.Get("OJS-Version"); v != "" && !strings.HasPrefix(v, "1") {
		s.writeErr(w, &apiError{status: 422, code: "unsupported", message: "only OJS 1.x is supported"})
		return
	}
	if r.Method == http.MethodPost && !acceptsJSON(r.Header.Get("Content-Type")) {
		s.writeErr(w, badRequest("Content-Type must be application/openjobspec+json or application/json"))
		return
	}
	if dispatchOjsRoute(fetchRoutes, s, w, r) {
		return
	}
	release, ok := s.acquire(r.Context())
	if !ok {
		s.writeErr(w, badRequest("request canceled"))
		return
	}
	defer release()
	if !dispatchOjsRoute(ojsRoutes, s, w, r) {
		s.writeErr(w, notFound("unknown OJS endpoint"))
	}
}

func acceptsJSON(contentType string) bool {
	if contentType == "" {
		return true
	}
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && (mt == mediaType || mt == "application/json")
}

func (s *Server) decode(w http.ResponseWriter, r *http.Request, dst any) *apiError {
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return &apiError{status: 413, code: "payload_too_large", message: "request body exceeds 1 MiB"}
		}
		return badRequest("could not read request body")
	}
	if len(body) == 0 {
		return badRequest("request body is required")
	}
	if err := json.Unmarshal(body, dst); err != nil {
		return badRequest("malformed JSON: " + err.Error())
	}
	return nil
}

type reply struct {
	status   int
	body     any
	location string
}

func (s *Server) respond(w http.ResponseWriter, body any, err error) {
	if errors.Is(err, errClientGone) {
		return
	}
	if err != nil {
		s.writeErr(w, storeError(err))
		return
	}
	result, ok := body.(reply)
	if !ok {
		result = reply{status: http.StatusOK, body: body}
	}
	if result.location != "" {
		w.Header().Set("Location", result.location)
	}
	s.writeJSON(w, result.status, result.body)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

func (s *Server) writeErr(w http.ResponseWriter, e *apiError) {
	if e.status >= 500 && s.log != nil {
		s.log.Error("ojs request failed", "code", e.code, "request_id", w.Header().Get("X-Request-Id"), "message", e.message, "err", e.cause)
	}
	if e.allow != "" {
		w.Header().Set("Allow", e.allow)
	}
	s.writeJSON(w, e.status, errorEnvelope{Error: errorBody{
		Code: e.code, Message: e.message, Retryable: e.code == "stale_lease" || e.code == "conflict",
		RequestID: w.Header().Get("X-Request-Id"), Details: e.details,
	}})
}

func (s *Server) requireWritable() *apiError {
	if s.readOnly {
		return readOnly()
	}
	return nil
}

func methodError(allow string) *apiError {
	return &apiError{status: http.StatusMethodNotAllowed, code: "method_not_allowed", message: "method not allowed", allow: allow}
}

func asAPIError(err error) *apiError {
	if e, ok := errors.AsType[*apiError](err); ok {
		return e
	}
	return badRequest(err.Error())
}

func storeError(err error) *apiError {
	if e, ok := errors.AsType[*apiError](err); ok {
		return e
	}
	switch {
	case errors.Is(err, store.ErrJobNotFound), errors.Is(err, store.ErrQueueNotFound),
		errors.Is(err, store.ErrStreamBatchNotFound), errors.Is(err, store.ErrStreamMessageNotFound),
		errors.Is(err, store.ErrStreamConsumerNotFound), errors.Is(err, store.ErrStreamTickNotFound):
		return notFound(err.Error())
	case errors.Is(err, store.ErrStreamBatchTooLarge):
		return &apiError{status: http.StatusRequestEntityTooLarge, code: "batch_too_large", message: err.Error()}
	case errors.Is(err, store.ErrStreamBatchActive), errors.Is(err, store.ErrJobConflict), errors.Is(err, store.ErrDuplicateJob):
		return conflict(err.Error())
	case errors.Is(err, store.ErrInvalidStreamTick), errors.Is(err, ns.ErrInvalidName), errors.Is(err, store.ErrInvalidBucket):
		return badRequest(err.Error())
	case errors.Is(err, store.ErrLeaseStale):
		return staleLease(err.Error())
	case errors.Is(err, store.ErrQuotaExceeded), errors.Is(err, store.ErrTooManyNamespaces):
		return &apiError{status: 507, code: "insufficient_storage", message: err.Error()}
	default:
		return internalErr(err)
	}
}

var errClientGone = errors.New("long-poll client disconnected")

func poll[T any](ctx context.Context, wait time.Duration, attempt func() (T, error), ready func(T) bool) (T, error) {
	deadline := time.Now().Add(wait)
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		value, err := attempt()
		if err != nil || ready(value) || wait <= 0 {
			return value, err
		}
		pause := min(pollPeriod, time.Until(deadline))
		if pause <= 0 {
			return value, nil
		}
		timer.Reset(pause)
		select {
		case <-ctx.Done():
			return value, errClientGone
		case <-timer.C:
		}
	}
}

func (s *Server) handleHealth(_ http.ResponseWriter, r *http.Request) (any, error) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	started := time.Now()
	err := s.st.Ping(ctx)
	backend := map[string]any{"type": "postgres", "status": "connected"}
	if ms := time.Since(started).Milliseconds(); ms != 0 {
		backend["latency_ms"] = ms
	}
	body := map[string]any{"status": "ok", "version": specVersion,
		"uptime_seconds": int64(time.Since(s.started).Seconds()), "backend": backend}
	if err != nil {
		if s.log != nil {
			s.log.Error("ojs health check failed", "err", err)
		}
		body["status"], backend["status"], backend["error"] = "degraded", "disconnected", "database unavailable"
		return reply{status: http.StatusServiceUnavailable, body: body}, nil
	}
	return body, nil
}

var manifestCapabilities = map[string]bool{
	"batch_enqueue": true, "cron_jobs": false, "dead_letter": false, "delayed_jobs": true,
	"job_ttl": false, "priority_queues": true, "rate_limiting": false, "schema_validation": false,
	"unique_jobs": false, "workflows": false, "pause_resume": false,
	"streams": true, "stream_fanout": true, "stream_dead_letter": true,
}

func (s *Server) handleManifest(_ http.ResponseWriter, r *http.Request) (any, error) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return map[string]any{
		"ojs_version": specVersion, "specversion": specVersion,
		"implementation":    map[string]string{"name": "pgvs3", "version": s.version, "language": "go", "homepage": "https://github.com/adonm/pgvs3"},
		"conformance_level": 0, "protocols": []string{"http"}, "backend": "postgres",
		"capabilities": manifestCapabilities,
		"extensions":   []string{"lease_token", "streams"},
		"endpoints":    map[string]string{"base_url": scheme + "://" + r.Host + "/ojs/v1", "manifest": "/ojs/manifest", "health": "/ojs/v1/health"},
	}, nil
}

const (
	mediaType   = "application/openjobspec+json"
	specVersion = "1.0"
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Args        json.RawMessage `json:"args"`
	Meta        json.RawMessage `json:"meta,omitempty"`
	Queue       string          `json:"queue"`
	State       string          `json:"state"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	Priority    int             `json:"priority"`
	TimeoutMS   *int            `json:"timeout_ms,omitempty"`

	CreatedAt   string `json:"created_at"`
	ScheduledAt string `json:"scheduled_at,omitempty"`
	EnqueuedAt  string `json:"enqueued_at,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	CancelledAt string `json:"cancelled_at,omitempty"`
	DiscardedAt string `json:"discarded_at,omitempty"`
	ExpiresAt   string `json:"expires_at,omitempty"`

	WorkerID       string `json:"worker_id,omitempty"`
	LeaseToken     string `json:"lease_token,omitempty"`
	LeaseExpiresAt string `json:"lease_expires_at,omitempty"`
	RetryDelayMS   *int   `json:"retry_delay_ms,omitempty"`

	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
	Tags   []string        `json:"tags,omitempty"`

	PreviousState string `json:"previous_state,omitempty"`
}

func timeString(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

func timePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return timeString(*t)
}

func wireState(state store.JobState) string {
	if state == store.JobStateUnknown {
		return ""
	}
	return state.String()
}

func wireJob(j store.Job) Job {
	w := Job{
		ID: j.ID, Type: j.Type, Args: rawOrEmpty(j.Args), Meta: rawOrNil(j.Meta),
		Queue: j.Queue, State: wireState(j.State), Attempt: j.Attempt,
		MaxAttempts: j.MaxAttempts, Priority: j.Priority, TimeoutMS: j.TimeoutMS,
		CreatedAt: timeString(j.CreatedAt), ScheduledAt: timePtr(j.ScheduledAt),
		EnqueuedAt: timeString(j.EnqueuedAt), StartedAt: timePtr(j.StartedAt),
		CompletedAt: timePtr(j.CompletedAt), CancelledAt: timePtr(j.CancelledAt),
		DiscardedAt: timePtr(j.DiscardedAt), ExpiresAt: timePtr(j.ExpiresAt),
		WorkerID: j.WorkerID, LeaseToken: j.LeaseToken,
		LeaseExpiresAt: timePtr(j.LeaseUntil), RetryDelayMS: j.RetryDelayMS,
		Result: rawOrNil(j.Result), Error: rawOrNil(j.Error), Tags: j.Tags,
		PreviousState: wireState(j.PreviousState),
	}
	return w
}

func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func rawOrEmpty(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("[]")
	}
	return json.RawMessage(b)
}

type retryOptions struct {
	MaxAttempts        *int    `json:"max_attempts"`
	InitialInterval    string  `json:"initial_interval"`
	InitialIntervalMS  int     `json:"initial_interval_ms"`
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	MaxIntervalMS      int     `json:"max_interval_ms"`
}

type enqueueOverrides struct {
	Queue       string        `json:"queue"`
	Priority    *int          `json:"priority"`
	TimeoutMS   *int          `json:"timeout_ms"`
	ScheduledAt string        `json:"scheduled_at"`
	DelayUntil  string        `json:"delay_until"`
	RunAt       string        `json:"run_at"`
	ExpiresAt   string        `json:"expires_at"`
	Tags        []string      `json:"tags"`
	Retry       *retryOptions `json:"retry"`
	RetryPolicy *retryOptions `json:"retry_policy"`
}

type enqueueRequest struct {
	ID          string           `json:"id"`
	Type        string           `json:"type"`
	Args        json.RawMessage  `json:"args"`
	Meta        json.RawMessage  `json:"meta"`
	Queue       string           `json:"queue"`
	Priority    *int             `json:"priority"`
	MaxAttempts *int             `json:"max_attempts"`
	TimeoutMS   *int             `json:"timeout_ms"`
	ScheduledAt string           `json:"scheduled_at"`
	RunAt       string           `json:"run_at"`
	ExpiresAt   string           `json:"expires_at"`
	Tags        []string         `json:"tags"`
	Retry       *retryOptions    `json:"retry"`
	Options     enqueueOverrides `json:"options"`
}

type enqueueOptions struct {
	Queue        string
	Priority     int
	MaxAttempts  int
	TimeoutMS    *int
	ScheduledAt  time.Time
	ExpiresAt    *time.Time
	RetryInitial int
	RetryMax     int
	RetryCoeff   float64
	Tags         []string
}

func (r enqueueRequest) retry() *retryOptions {
	if r.Options.Retry != nil {
		return r.Options.Retry
	}
	if r.Options.RetryPolicy != nil {
		return r.Options.RetryPolicy
	}
	return r.Retry
}

func (r *retryOptions) attemptLimit() *int {
	if r == nil {
		return nil
	}
	return r.MaxAttempts
}

func (r enqueueRequest) input(now time.Time) (store.EnqueueInput, error) {
	id, err := r.identity()
	if err != nil {
		return store.EnqueueInput{}, err
	}
	opts, err := r.options(now)
	if err != nil {
		return store.EnqueueInput{}, err
	}
	return opts.build(r, id), nil
}

func (r enqueueRequest) identity() (string, error) {
	if !validJobType(r.Type) {
		return "", badRequest("invalid or missing job 'type'")
	}
	if len(r.Args) == 0 || !json.Valid(r.Args) || firstByte(r.Args) != '[' {
		return "", badRequest("job 'args' must be a JSON array")
	}
	if len(r.Meta) > 0 && (!json.Valid(r.Meta) || firstByte(r.Meta) != '{') {
		return "", badRequest("job 'meta' must be a JSON object")
	}
	if r.ID == "" {
		return "", nil
	}
	u, err := uuid.Parse(r.ID)
	if err != nil {
		return "", badRequest("job 'id' must be a UUID")
	}
	return u.String(), nil
}

func (r enqueueRequest) options(now time.Time) (enqueueOptions, error) {
	opts := enqueueOptions{
		Queue:        firstNonEmpty(r.Queue, r.Options.Queue, "default"),
		Priority:     intOption(0, r.Options.Priority, r.Priority),
		MaxAttempts:  intOption(3, r.Options.Retry.attemptLimit(), r.Options.RetryPolicy.attemptLimit(), r.Retry.attemptLimit(), r.MaxAttempts),
		TimeoutMS:    intPtrOption(r.Options.TimeoutMS, r.TimeoutMS),
		RetryInitial: 1000,
		RetryMax:     60000,
		RetryCoeff:   2,
		Tags:         r.Tags,
	}
	if !ns.ValidName(opts.Queue) {
		return enqueueOptions{}, badRequest("invalid queue name (lowercase a-z, 0-9, '.', '_' or '-', max 58 bytes)")
	}
	if opts.Priority < -100 || opts.Priority > 100 {
		return enqueueOptions{}, badRequest("priority must be between -100 and 100")
	}
	if opts.MaxAttempts < 0 || opts.MaxAttempts > 1000 {
		return enqueueOptions{}, badRequest("max_attempts must be between 0 and 1000")
	}
	if opts.TimeoutMS != nil && *opts.TimeoutMS < 0 {
		return enqueueOptions{}, badRequest("timeout_ms must be >= 0")
	}
	scheduled := firstNonEmpty(r.Options.ScheduledAt, r.Options.DelayUntil, r.Options.RunAt, r.ScheduledAt, r.RunAt)
	scheduledAt, err := parseWhen(scheduled, now)
	if err != nil {
		return enqueueOptions{}, badRequest("invalid scheduled_at/run_at: " + err.Error())
	}
	opts.ScheduledAt = scheduledAt
	expiresRaw := firstNonEmpty(r.Options.ExpiresAt, r.ExpiresAt)
	expiresAt, err := parseWhen(expiresRaw, now)
	if err != nil {
		return enqueueOptions{}, badRequest("invalid expires_at: " + err.Error())
	}
	if !expiresAt.IsZero() {
		opts.ExpiresAt = &expiresAt
	}
	opts.applyRetry(r.retry())
	if len(opts.Tags) == 0 {
		opts.Tags = r.Options.Tags
	}
	return opts, nil
}

func (o *enqueueOptions) applyRetry(retry *retryOptions) {
	if retry == nil {
		return
	}
	if retry.InitialIntervalMS > 0 {
		o.RetryInitial = retry.InitialIntervalMS
	} else if retry.InitialInterval != "" {
		if d, err := parseISODuration(retry.InitialInterval); err == nil {
			o.RetryInitial = int(d / time.Millisecond)
		}
	}
	if retry.MaxIntervalMS > 0 {
		o.RetryMax = retry.MaxIntervalMS
	}
	if retry.BackoffCoefficient > 0 {
		o.RetryCoeff = retry.BackoffCoefficient
	}
}

func (o enqueueOptions) build(r enqueueRequest, id string) store.EnqueueInput {
	return store.EnqueueInput{
		ID: id, Type: r.Type, Args: r.Args, Meta: rawOrNil(r.Meta), Queue: o.Queue,
		Priority: o.Priority, MaxAttempts: o.MaxAttempts, ScheduledAt: o.ScheduledAt,
		TimeoutMS: o.TimeoutMS, RetryInitial: o.RetryInitial, RetryMax: o.RetryMax,
		RetryCoeff: o.RetryCoeff, Tags: o.Tags, ExpiresAt: o.ExpiresAt,
	}
}

func intOption(def int, values ...*int) int {
	for _, v := range values {
		if v != nil {
			return *v
		}
	}
	return def
}

func intPtrOption(values ...*int) *int {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

func firstByte(b []byte) byte {
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return 0
	}
	return b[0]
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func validJobType(t string) bool {
	if t == "" || len(t) > 255 {
		return false
	}
	atStart := true
	for i := 0; i < len(t); i++ {
		c := t[i]
		if atStart {
			if c < 'a' || c > 'z' {
				return false
			}
			atStart = false
			continue
		}
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_', c == '-':
		case c == '.':
			atStart = true
		default:
			return false
		}
	}
	return !atStart
}

var isoDuration = regexp.MustCompile(`^P(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

func parseISODuration(s string) (time.Duration, error) {
	m := isoDuration.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("not an ISO 8601 duration: %q", s)
	}
	var total float64
	for i, unit := range []time.Duration{time.Hour, time.Minute, time.Second} {
		if m[i+1] == "" {
			continue
		}
		v, err := strconv.ParseFloat(m[i+1], 64)
		if err != nil {
			return 0, err
		}
		total += v * float64(unit)
	}
	return time.Duration(total), nil
}

func parseWhen(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if strings.HasPrefix(s, "+") {
		raw := s[1:]
		if d, err := parseISODuration(raw); err == nil {
			return now.UTC().Add(d), nil
		}
		if d, err := time.ParseDuration(raw); err == nil {
			return now.UTC().Add(d), nil
		}
		return time.Time{}, fmt.Errorf("invalid relative duration %q", s)
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

type errorBody struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details,omitempty"`
	RequestID string         `json:"request_id,omitempty"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type apiError struct {
	status  int
	code    string
	message string
	details map[string]any
	cause   error
	allow   string
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func badRequest(msg string) *apiError {
	return &apiError{status: 400, code: "invalid_request", message: msg}
}

func conflict(msg string) *apiError {
	return &apiError{status: 409, code: "conflict", message: msg}
}

func staleLease(msg string) *apiError {
	return &apiError{status: 409, code: "stale_lease", message: msg}
}

func notFound(msg string) *apiError {
	return &apiError{status: 404, code: "not_found", message: msg}
}

func readOnly() *apiError {
	return &apiError{status: 403, code: "read_only", message: "gateway is read-only; OJS enqueue, fetch, ack, nack, heartbeat and cancel require a writable primary"}
}

func internalErr(err error) *apiError {
	return &apiError{
		status: http.StatusInternalServerError, code: "internal_error",
		message: "internal server error", cause: err,
	}
}

type jobResponse struct {
	Job Job `json:"job"`
}

type batchRequest struct {
	Jobs []enqueueRequest `json:"jobs"`
}

func (s *Server) handleEnqueue(r *http.Request, req enqueueRequest) (any, error) {
	return s.enqueue(r, []enqueueRequest{req}, false)
}

func (s *Server) handleBatch(r *http.Request, req batchRequest) (any, error) {
	return s.enqueue(r, req.Jobs, true)
}

func (s *Server) enqueue(r *http.Request, requests []enqueueRequest, batch bool) (any, error) {
	if len(requests) == 0 {
		return nil, badRequest("'jobs' must contain at least one job")
	}
	if len(requests) > 100 {
		return nil, badRequest("batch supports at most 100 jobs")
	}
	now := time.Now().UTC()
	inputs := make([]store.EnqueueInput, len(requests))
	for i, req := range requests {
		in, err := req.input(now)
		if err != nil {
			e := asAPIError(err)
			if batch {
				e.message = "job at index " + strconv.Itoa(i) + ": " + e.message
				e.details = map[string]any{"index": i}
			}
			return nil, e
		}
		inputs[i] = in
	}
	jobs, err := s.st.EnqueueJobs(r.Context(), inputs)
	if err != nil {
		return nil, err
	}
	if !batch {
		return reply{status: http.StatusCreated, body: jobResponse{Job: wireJob(jobs[0])}, location: "/ojs/v1/jobs/" + jobs[0].ID}, nil
	}
	wire := make([]Job, len(jobs))
	for i, j := range jobs {
		wire[i] = wireJob(j)
	}
	return reply{status: http.StatusCreated, body: map[string]any{"jobs": wire, "count": len(wire)}}, nil
}

func (s *Server) handleJob(_ http.ResponseWriter, r *http.Request) (any, error) {
	id := strings.TrimPrefix(r.URL.Path, "/ojs/v1/jobs/")
	if id == "" || strings.Contains(id, "/") {
		return nil, notFound("job not found")
	}
	u, err := uuid.Parse(id)
	if err != nil {
		return nil, badRequest("job id must be a UUID")
	}
	var job store.Job
	switch r.Method {
	case http.MethodGet:
		job, err = s.st.GetJob(r.Context(), u.String())
	case http.MethodDelete:
		if e := s.requireWritable(); e != nil {
			return nil, e
		}
		job, err = s.st.CancelJob(r.Context(), u.String())
	default:
		return nil, methodError("GET, DELETE")
	}
	return jobResponse{Job: wireJob(job)}, err
}

type fetchRequest struct {
	Queues              []string `json:"queues"`
	Count               int      `json:"count"`
	WorkerID            string   `json:"worker_id"`
	VisibilityTimeoutMS *int     `json:"visibility_timeout_ms"`
}

type fetchResponse struct {
	Jobs []Job `json:"jobs"`
	Job  *Job  `json:"job,omitempty"`
}

func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) (any, error) {
	req, visibility, e := s.parseFetchRequest(w, r)
	if e != nil {
		return nil, e
	}

	release, ok := s.acquire(r.Context())
	if !ok {
		return nil, badRequest("request canceled")
	}
	exists, err := s.st.AnyQueueExists(r.Context(), req.Queues)
	release()
	if err != nil {
		return nil, internalErr(err)
	}
	wait := s.longPoll
	if !exists {
		wait = 0
	}
	jobs, err := poll(r.Context(), wait, func() ([]store.Job, error) {
		return s.fetchOnce(r.Context(), req, visibility)
	}, func(jobs []store.Job) bool { return len(jobs) > 0 })
	return fetchResponseFor(jobs), err
}

func visibilityTimeout(ms *int) (time.Duration, *apiError) {
	if ms == nil {
		return 30 * time.Second, nil
	}
	if *ms < 0 {
		return 0, badRequest("visibility_timeout_ms must be >= 0")
	}
	return time.Duration(*ms) * time.Millisecond, nil
}

func (s *Server) parseFetchRequest(w http.ResponseWriter, r *http.Request) (fetchRequest, time.Duration, *apiError) {
	var req fetchRequest
	if e := s.decode(w, r, &req); e != nil {
		return req, 0, e
	}
	if len(req.Queues) == 0 {
		return req, 0, badRequest("'queues' must contain at least one queue")
	}
	if len(req.Queues) > maxFetchQueues {
		return req, 0, badRequest("'queues' may contain at most 64 queues")
	}
	for _, q := range req.Queues {
		if !ns.ValidName(q) {
			return req, 0, badRequest("invalid queue name " + strconv.Quote(q))
		}
	}
	req.Count = min(max(req.Count, 1), 100)
	visibility, e := visibilityTimeout(req.VisibilityTimeoutMS)
	return req, visibility, e
}

func (s *Server) fetchOnce(ctx context.Context, req fetchRequest, visibility time.Duration) ([]store.Job, error) {
	release, ok := s.acquire(ctx)
	if !ok {
		return nil, badRequest("request canceled")
	}
	defer release()
	return s.st.FetchJobs(ctx, req.Queues, req.Count, req.WorkerID, visibility)
}

func fetchResponseFor(jobs []store.Job) fetchResponse {
	wire := make([]Job, len(jobs))
	for i, j := range jobs {
		wire[i] = wireJob(j)
	}
	response := fetchResponse{Jobs: wire}
	if len(wire) > 0 {
		response.Job = &wire[0]
	}
	return response
}

type jobLease struct {
	JobID      string `json:"job_id"`
	LeaseToken string `json:"lease_token"`
}

func (req jobLease) validate() (string, *apiError) {
	id, e := parseJobID(req.JobID)
	if e != nil {
		return "", e
	}
	if req.LeaseToken == "" {
		return "", leaseRequired()
	}
	return id, nil
}

func leaseRequired() *apiError {
	e := badRequest("'lease_token' is required (returned by /ojs/v1/workers/fetch)")
	e.details = map[string]any{"extension": "lease_token"}
	return e
}

type ackRequest struct {
	jobLease
	Result json.RawMessage `json:"result"`
}

type ackResponse struct {
	Acknowledged bool   `json:"acknowledged"`
	JobID        string `json:"job_id"`
	State        string `json:"state"`
	CompletedAt  string `json:"completed_at"`
}

func (s *Server) handleAck(r *http.Request, req ackRequest) (any, error) {
	id, e := req.validate()
	if e != nil {
		return nil, e
	}
	if len(req.Result) > 0 && !json.Valid(req.Result) {
		return nil, badRequest("'result' must be valid JSON")
	}
	j, err := s.st.AckJob(r.Context(), id, req.LeaseToken, req.Result)
	return ackResponse{Acknowledged: true, JobID: j.ID, State: wireState(j.State), CompletedAt: timePtr(j.CompletedAt)}, err
}

type nackRequest struct {
	jobLease
	Error json.RawMessage `json:"error"`
}

type nackResponse struct {
	JobID         string `json:"job_id"`
	State         string `json:"state"`
	Attempt       int    `json:"attempt"`
	MaxAttempts   int    `json:"max_attempts"`
	NextAttemptAt string `json:"next_attempt_at,omitempty"`
	DiscardedAt   string `json:"discarded_at,omitempty"`
}

func (s *Server) handleNack(r *http.Request, req nackRequest) (any, error) {
	id, e := req.validate()
	if e != nil {
		return nil, e
	}
	if len(req.Error) == 0 || !json.Valid(req.Error) || firstByte(req.Error) != '{' {
		return nil, badRequest("'error' must be a JSON object")
	}
	var detail struct {
		Retryable *bool `json:"retryable"`
	}
	_ = json.Unmarshal(req.Error, &detail)
	retryable := detail.Retryable == nil || *detail.Retryable
	j, err := s.st.NackJob(r.Context(), id, req.LeaseToken, req.Error, retryable)
	response := nackResponse{JobID: j.ID, State: wireState(j.State), Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		NextAttemptAt: timePtr(j.ScheduledAt), DiscardedAt: timePtr(j.DiscardedAt)}
	if j.State == store.JobDiscarded {
		response.NextAttemptAt = ""
	}
	return response, err
}

type heartbeatRequest struct {
	WorkerID            string            `json:"worker_id"`
	JobID               string            `json:"job_id"`
	ActiveJobs          []string          `json:"active_jobs"`
	LeaseToken          string            `json:"lease_token"`
	LeaseTokens         map[string]string `json:"lease_tokens"`
	VisibilityTimeoutMS *int              `json:"visibility_timeout_ms"`
}

type heartbeatResponse struct {
	State        string   `json:"state"`
	Directive    string   `json:"directive"`
	JobsExtended []string `json:"jobs_extended"`
	ServerTime   string   `json:"server_time"`
}

func (s *Server) handleHeartbeat(r *http.Request, req heartbeatRequest) (any, error) {
	if req.WorkerID == "" {
		return nil, badRequest("'worker_id' is required")
	}
	visibility, e := visibilityTimeout(req.VisibilityTimeoutMS)
	if e != nil {
		return nil, e
	}
	leases := make(map[string]string)
	ids := req.ActiveJobs
	if req.JobID != "" {
		ids = append([]string{req.JobID}, ids...)
	}
	for _, raw := range ids {
		id, e := parseJobID(raw)
		if e != nil {
			return nil, e
		}
		token := req.LeaseTokens[id]
		if token == "" {
			token = req.LeaseToken
		}
		if token == "" {
			return nil, leaseRequired()
		}
		leases[id] = token
	}
	extended, err := s.st.HeartbeatJobs(r.Context(), req.WorkerID, leases, visibility)
	return heartbeatResponse{State: "running", Directive: "", JobsExtended: extended, ServerTime: timeString(time.Now())}, err
}

func parseJobID(raw string) (string, *apiError) {
	u, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", badRequest("'job_id' must be a UUID")
	}
	return u.String(), nil
}

type queueInfo struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type pagination struct {
	Total   int64 `json:"total"`
	Limit   int   `json:"limit"`
	Offset  int   `json:"offset"`
	HasMore bool  `json:"has_more"`
}

func (s *Server) handleQueues(_ http.ResponseWriter, r *http.Request) (any, error) {
	limit, offset, e := pageParams(r)
	if e != nil {
		return nil, e
	}
	infos, total, err := s.st.ListQueues(r.Context(), limit, offset)
	if err != nil {
		return nil, internalErr(err)
	}
	queues := make([]queueInfo, len(infos))
	for i, n := range infos {
		queues[i] = queueInfo{Name: n.Name, Status: "active", CreatedAt: timeString(n.CreatedAt)}
	}
	return map[string]any{"queues": queues, "pagination": pagination{
		Total: total, Limit: limit, Offset: offset, HasMore: int64(offset+len(queues)) < total}}, nil
}

type queueStatsBody struct {
	Name      string `json:"name"`
	Status    string `json:"status"`
	Paused    bool   `json:"paused"`
	Available int64  `json:"available"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retryable int64  `json:"retryable"`
	Completed int64  `json:"completed"`
	Cancelled int64  `json:"cancelled"`
	Discarded int64  `json:"discarded"`
	Total     int64  `json:"total"`
	Events    int64  `json:"events"`
	Consumers int64  `json:"consumers"`
	Dead      int64  `json:"dead_letters"`
}

func (s *Server) handleQueueStats(_ http.ResponseWriter, r *http.Request) (any, error) {
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/ojs/v1/queues/"), "/stats")
	if !ns.ValidName(name) {
		return nil, badRequest("invalid queue name")
	}
	st, err := s.st.QueueStats(r.Context(), name)
	if err != nil {
		return nil, err
	}
	body := queueStatsBody{Name: name, Status: "active", Available: st.Available, Active: st.Active,
		Scheduled: st.Scheduled, Retryable: st.Retryable, Completed: st.Completed,
		Cancelled: st.Cancelled, Discarded: st.Discarded, Total: st.Total}
	if info, err := s.st.StreamInfo(r.Context(), name); err == nil {
		body.Events, body.Consumers, body.Dead = info.Events, info.Consumers, info.DeadLetters
	}
	return map[string]any{"queue": body, "status": "active", "stats": body, "computed_at": timeString(time.Now())}, nil
}

func pageParams(r *http.Request) (int, int, *apiError) {
	limit, offset := 50, 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return 0, 0, badRequest("limit must be between 1 and 1000")
		}
		limit = n
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return 0, 0, badRequest("offset must be >= 0")
		}
		offset = n
	}
	return limit, offset, nil
}

type statusWriter struct {
	http.ResponseWriter
	status int
	n      int64
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.n += int64(n)
	return n, err
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
