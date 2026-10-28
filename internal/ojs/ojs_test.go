package ojs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

type fakeStore struct {
	enqueue     func(context.Context, []store.EnqueueInput) ([]store.Job, error)
	fetch       func(context.Context, []string, int, string, time.Duration) ([]store.Job, error)
	get         func(context.Context, string) (store.Job, error)
	cancel      func(context.Context, string) (store.Job, error)
	ack         func(context.Context, string, string, []byte) (store.Job, error)
	nack        func(context.Context, string, string, []byte, bool) (store.Job, error)
	heartbeat   func(context.Context, string, map[string]string, time.Duration) ([]string, error)
	queues      func(context.Context, int, int) ([]store.NamespaceInfo, int64, error)
	stats       func(context.Context, string) (store.QueueStats, error)
	exists      func(context.Context, ns.Kind, string) (bool, error)
	receive     func(context.Context, string, string, int) (*store.StreamBatch, error)
	receiveCoop func(context.Context, string, string, string, int) (*store.StreamBatch, error)
	ping        func(context.Context) error
}

func (f *fakeStore) EnqueueJobs(ctx context.Context, in []store.EnqueueInput) ([]store.Job, error) {
	if f.enqueue != nil {
		return f.enqueue(ctx, in)
	}
	return nil, errors.New("unexpected enqueue")
}

func (f *fakeStore) FetchJobs(ctx context.Context, q []string, n int, w string, v time.Duration) ([]store.Job, error) {
	if f.fetch != nil {
		return f.fetch(ctx, q, n, w, v)
	}
	return nil, nil
}

func (f *fakeStore) GetJob(ctx context.Context, id string) (store.Job, error) {
	if f.get != nil {
		return f.get(ctx, id)
	}
	return store.Job{}, store.ErrJobNotFound
}

func (f *fakeStore) CancelJob(ctx context.Context, id string) (store.Job, error) {
	if f.cancel != nil {
		return f.cancel(ctx, id)
	}
	return store.Job{}, store.ErrJobNotFound
}

func (f *fakeStore) AckJob(ctx context.Context, id, token string, result []byte) (store.Job, error) {
	if f.ack != nil {
		return f.ack(ctx, id, token, result)
	}
	return store.Job{}, store.ErrJobNotFound
}

func (f *fakeStore) NackJob(ctx context.Context, id, token string, e []byte, retry bool) (store.Job, error) {
	if f.nack != nil {
		return f.nack(ctx, id, token, e, retry)
	}
	return store.Job{}, store.ErrJobNotFound
}

func (f *fakeStore) HeartbeatJobs(ctx context.Context, w string, m map[string]string, v time.Duration) ([]string, error) {
	if f.heartbeat != nil {
		return f.heartbeat(ctx, w, m, v)
	}
	return []string{}, nil
}

func (f *fakeStore) ListQueues(ctx context.Context, limit, offset int) ([]store.NamespaceInfo, int64, error) {
	if f.queues != nil {
		return f.queues(ctx, limit, offset)
	}
	return nil, 0, nil
}

func (f *fakeStore) QueueStats(ctx context.Context, name string) (store.QueueStats, error) {
	if f.stats != nil {
		return f.stats(ctx, name)
	}
	return store.QueueStats{}, store.ErrQueueNotFound
}

func (f *fakeStore) NamespaceExists(ctx context.Context, k ns.Kind, name string) (bool, error) {
	if f.exists != nil {
		return f.exists(ctx, k, name)
	}
	return true, nil
}

func (f *fakeStore) AnyQueueExists(ctx context.Context, queues []string) (bool, error) {
	if f.exists == nil {
		return len(queues) > 0, nil
	}
	for _, q := range queues {
		ok, err := f.exists(ctx, ns.Queue, q)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}

func (f *fakeStore) Ping(ctx context.Context) error {
	if f.ping != nil {
		return f.ping(ctx)
	}
	return nil
}

func (f *fakeStore) AppendStreamMessages(context.Context, string, []store.StreamMessageInput) ([]store.StreamMessage, error) {
	return nil, errors.New("unexpected stream append")
}

func (f *fakeStore) SubscribeStream(context.Context, string, string, *int64) (store.StreamSubscription, error) {
	return store.StreamSubscription{}, store.ErrQueueNotFound
}

func (f *fakeStore) UnsubscribeStream(context.Context, string, string) (bool, error) {
	return false, nil
}

func (f *fakeStore) ReceiveStreamBatch(ctx context.Context, queue, consumer string, max int) (*store.StreamBatch, error) {
	if f.receive != nil {
		return f.receive(ctx, queue, consumer, max)
	}
	return nil, store.ErrQueueNotFound
}

func (f *fakeStore) ReceiveStreamCoopBatch(ctx context.Context, queue, consumer, member string, max int) (*store.StreamBatch, error) {
	if f.receiveCoop != nil {
		return f.receiveCoop(ctx, queue, consumer, member, max)
	}
	return nil, store.ErrQueueNotFound
}

func (f *fakeStore) GetStreamBatch(context.Context, string, int64) (*store.StreamBatch, error) {
	return nil, store.ErrStreamBatchNotFound
}

func (f *fakeStore) AckStreamBatch(context.Context, string, int64) (int64, error) {
	return 0, store.ErrStreamBatchNotFound
}

func (f *fakeStore) NackStreamMessage(context.Context, string, int64, string, time.Duration, string) (string, error) {
	return "", store.ErrStreamBatchNotFound
}

func (f *fakeStore) StreamInfo(context.Context, string) (store.StreamInfo, error) {
	return store.StreamInfo{}, store.ErrQueueNotFound
}

func (f *fakeStore) StreamConfig(context.Context, string) (store.StreamConfig, error) {
	return store.StreamConfig{}, store.ErrQueueNotFound
}

func (f *fakeStore) SetStreamConfig(context.Context, string, int, time.Duration) (store.StreamConfig, error) {
	return store.StreamConfig{}, store.ErrQueueNotFound
}

func (f *fakeStore) ListStreamConsumers(context.Context, string) ([]store.StreamConsumerInfo, error) {
	return nil, store.ErrQueueNotFound
}

func (f *fakeStore) ListDeadLetters(context.Context, string, int) ([]store.DeadLetter, error) {
	return nil, store.ErrQueueNotFound
}

func (f *fakeStore) ReplayDeadLetter(context.Context, string, int64) (string, error) {
	return "", store.ErrStreamMessageNotFound
}

func (f *fakeStore) ReplayAllDeadLetters(context.Context, string) (int64, error) {
	return 0, nil
}

func (f *fakeStore) PurgeDeadLetters(context.Context, string, time.Duration) (int64, error) {
	return 0, nil
}

func testServer(t *testing.T, st Store, readOnly bool) *Server {
	t.Helper()
	return New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), readOnly, 4, "test")
}

func post(t *testing.T, s *Server, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", mediaType)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func do(t *testing.T, s *Server, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", mediaType)
	}
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	return rr
}

func decodeMap(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode %q: %v", rr.Body.String(), err)
	}
	return m
}

func sampleJob() store.Job {
	created := time.Date(2026, 2, 12, 10, 30, 0, 0, time.UTC)
	started := created.Add(time.Second)
	return store.Job{
		ID: "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", Type: "email.send",
		Args: []byte(`["a@example.com",{"locale":"en"}]`), Meta: []byte(`{"trace_id":"t"}`),
		Queue: "email", State: store.JobActive, Attempt: 1, MaxAttempts: 3,
		Priority: 5, CreatedAt: created, ScheduledAt: &created,
		EnqueuedAt: created, StartedAt: &started,
		WorkerID: "worker-1", LeaseToken: "019414d4-aaaa-7000-c000-111111111111",
		Tags: []string{"onboarding"},
	}
}

func TestManifestAndHeaders(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	rr := do(t, s, http.MethodGet, "/ojs/manifest", "")
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}
	if got := rr.Header().Get("Content-Type"); got != mediaType {
		t.Fatalf("content-type = %q", got)
	}
	if got := rr.Header().Get("OJS-Version"); got != "1.0" {
		t.Fatalf("OJS-Version = %q", got)
	}
	if !strings.HasPrefix(rr.Header().Get("X-Request-Id"), "req_") {
		t.Fatalf("X-Request-Id = %q", rr.Header().Get("X-Request-Id"))
	}
	m := decodeMap(t, rr)
	if m["ojs_version"] != "1.0" || m["specversion"] != "1.0" {
		t.Fatalf("manifest version: %+v", m)
	}
	impl := m["implementation"].(map[string]any)
	if impl["name"] != "pgvs3" || impl["language"] != "go" {
		t.Fatalf("implementation: %+v", impl)
	}
	caps := m["capabilities"].(map[string]any)
	if caps["batch_enqueue"] != true || caps["priority_queues"] != true || caps["workflows"] != false {
		t.Fatalf("capabilities: %+v", caps)
	}
}

func TestEnqueueJSONShape(t *testing.T) {
	var captured store.EnqueueInput
	st := &fakeStore{enqueue: func(_ context.Context, in []store.EnqueueInput) ([]store.Job, error) {
		captured = in[0]
		j := sampleJob()
		j.ID = "019414d4-0000-7000-a000-000000000001"
		j.State = store.JobAvailable
		j.Attempt = 0
		j.StartedAt = nil
		j.WorkerID, j.LeaseToken = "", ""
		j.Args = in[0].Args
		j.Priority = in[0].Priority
		j.MaxAttempts = in[0].MaxAttempts
		return []store.Job{j}, nil
	}}
	s := testServer(t, st, false)
	rr := post(t, s, "/ojs/v1/jobs", `{
		"type":"email.send",
		"args":["a@example.com",{"locale":"en"}],
		"meta":{"trace_id":"t"},
		"options":{"queue":"email","priority":5,"retry":{"max_attempts":4},
		           "scheduled_at":"2030-01-02T03:04:05Z","tags":["onboarding"]}
	}`)
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if got := rr.Header().Get("Location"); got != "/ojs/v1/jobs/019414d4-0000-7000-a000-000000000001" {
		t.Fatalf("Location = %q", got)
	}
	if captured.Queue != "email" || captured.Priority != 5 || captured.MaxAttempts != 4 {
		t.Fatalf("input = %+v", captured)
	}
	if captured.ScheduledAt.IsZero() || !captured.ScheduledAt.After(time.Now()) {
		t.Fatalf("scheduled_at = %v", captured.ScheduledAt)
	}
	m := decodeMap(t, rr)
	job := m["job"].(map[string]any)
	for _, key := range []string{"id", "type", "args", "queue", "state", "attempt", "max_attempts", "priority", "created_at", "scheduled_at"} {
		if _, ok := job[key]; !ok {
			t.Fatalf("job missing %q: %+v", key, job)
		}
	}
	if job["state"] != store.JobAvailable.String() {
		t.Fatalf("state = %v", job["state"])
	}
}

func TestBatchAtomicValidation(t *testing.T) {
	called := false
	st := &fakeStore{enqueue: func(_ context.Context, in []store.EnqueueInput) ([]store.Job, error) {
		called = true
		return nil, nil
	}}
	s := testServer(t, st, false)
	rr := post(t, s, "/ojs/v1/jobs/batch", `{"jobs":[
		{"type":"email.send","args":[]},
		{"type":"email.send","args":{"not":"an array"}}
	]}`)
	if rr.Code != 400 || called {
		t.Fatalf("status=%d called=%v body=%s", rr.Code, called, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "index") {
		t.Fatalf("batch error should identify index: %s", rr.Body)
	}
}

func TestFetchShapeAndLongPoll(t *testing.T) {
	calls := 0
	st := &fakeStore{fetch: func(_ context.Context, _ []string, _ int, _ string, _ time.Duration) ([]store.Job, error) {
		calls++
		if calls == 1 {
			return nil, nil
		}
		j := sampleJob()
		return []store.Job{j}, nil
	}}
	s := testServer(t, st, false)
	rr := post(t, s, "/ojs/v1/workers/fetch", `{"queues":["email"],"count":5,"worker_id":"w1","visibility_timeout_ms":1500}`)
	if rr.Code != 200 {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if calls < 2 {
		t.Fatalf("expected a long-poll retry, calls=%d", calls)
	}
	m := decodeMap(t, rr)
	jobs := m["jobs"].([]any)
	if len(jobs) != 1 {
		t.Fatalf("jobs = %v", jobs)
	}
	job := jobs[0].(map[string]any)
	if job["state"] != store.JobActive.String() || job["attempt"] != float64(1) {
		t.Fatalf("job = %+v", job)
	}
	if job["lease_token"] != "019414d4-aaaa-7000-c000-111111111111" {
		t.Fatalf("lease token missing: %+v", job)
	}
}

func TestAckNackHeartbeatShapes(t *testing.T) {
	id := "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6"
	completed := time.Now().UTC()
	next := time.Now().UTC().Add(time.Second)
	var ackToken string
	st := &fakeStore{
		ack: func(_ context.Context, gotID, token string, _ []byte) (store.Job, error) {
			ackToken = token
			j := sampleJob()
			j.ID, j.State, j.CompletedAt = gotID, store.JobCompleted, &completed
			return j, nil
		},
		nack: func(_ context.Context, gotID, _ string, _ []byte, retry bool) (store.Job, error) {
			j := sampleJob()
			j.ID, j.State, j.Attempt = gotID, store.JobRetryable, 2
			j.ScheduledAt = &next
			if !retry {
				j.State, j.DiscardedAt = store.JobDiscarded, &completed
			}
			return j, nil
		},
		heartbeat: func(_ context.Context, w string, m map[string]string, _ time.Duration) ([]string, error) {
			if w != "w1" || m[id] != "tok" {
				return nil, store.ErrLeaseStale
			}
			return []string{id}, nil
		},
	}
	s := testServer(t, st, false)

	rr := post(t, s, "/ojs/v1/workers/ack", `{"job_id":"`+id+`","lease_token":"tok","result":{"ok":true}}`)
	if rr.Code != 200 || ackToken != "tok" {
		t.Fatalf("ack status=%d token=%q body=%s", rr.Code, ackToken, rr.Body)
	}
	ack := decodeMap(t, rr)
	if ack["acknowledged"] != true || ack["state"] != store.JobCompleted.String() {
		t.Fatalf("ack = %+v", ack)
	}

	rr = post(t, s, "/ojs/v1/workers/nack", `{"job_id":"`+id+`","lease_token":"tok","error":{"retryable":true,"message":"boom"}}`)
	if rr.Code != 200 {
		t.Fatalf("nack status=%d body=%s", rr.Code, rr.Body)
	}
	nack := decodeMap(t, rr)
	if nack["state"] != store.JobRetryable.String() || nack["attempt"] != float64(2) || nack["next_attempt_at"] == "" {
		t.Fatalf("nack = %+v", nack)
	}

	rr = post(t, s, "/ojs/v1/workers/nack", `{"job_id":"`+id+`","lease_token":"tok","error":{"retryable":false,"message":"fatal"}}`)
	if rr.Code != 200 {
		t.Fatalf("nack discard status=%d body=%s", rr.Code, rr.Body)
	}
	nack = decodeMap(t, rr)
	if nack["state"] != store.JobDiscarded.String() || nack["discarded_at"] == "" {
		t.Fatalf("discard = %+v", nack)
	}

	rr = post(t, s, "/ojs/v1/workers/heartbeat", `{"worker_id":"w1","active_jobs":["`+id+`"],"lease_tokens":{"`+id+`":"tok"},"visibility_timeout_ms":1000}`)
	if rr.Code != 200 {
		t.Fatalf("heartbeat status=%d body=%s", rr.Code, rr.Body)
	}
	hb := decodeMap(t, rr)
	if hb["state"] != "running" {
		t.Fatalf("heartbeat = %+v", hb)
	}
	rr = post(t, s, "/ojs/v1/workers/heartbeat", `{"worker_id":"w1","active_jobs":["`+id+`"],"lease_tokens":{"`+id+`":"stale"}}`)
	if rr.Code != 409 {
		t.Fatalf("stale heartbeat status=%d body=%s", rr.Code, rr.Body)
	}
}

func TestCancelResponsePreviousState(t *testing.T) {
	id := "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6"
	st := &fakeStore{cancel: func(_ context.Context, gotID string) (store.Job, error) {
		j := sampleJob()
		j.ID = gotID
		j.State = store.JobCancelled
		j.PreviousState = store.JobActive
		return j, nil
	}}
	s := testServer(t, st, false)
	rr := do(t, s, http.MethodDelete, "/ojs/v1/jobs/"+id, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body)
	}
	job := decodeMap(t, rr)["job"].(map[string]any)
	if job["state"] != store.JobCancelled.String() || job["previous_state"] != store.JobActive.String() {
		t.Fatalf("cancel job = %+v", job)
	}
}

func TestReadOnlyMetadata(t *testing.T) {
	id := "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6"
	st := &fakeStore{
		get: func(_ context.Context, _ string) (store.Job, error) { return sampleJob(), nil },
		queues: func(_ context.Context, _, _ int) ([]store.NamespaceInfo, int64, error) {
			return []store.NamespaceInfo{{Kind: ns.Queue, Name: "email", CreatedAt: time.Now()}}, 1, nil
		},
		stats: func(_ context.Context, name string) (store.QueueStats, error) {
			return store.QueueStats{Available: 2, Active: 1, Completed: 3, Total: 6}, nil
		},
	}
	s := testServer(t, st, true)
	for _, path := range []string{"/ojs/v1/jobs/" + id, "/ojs/v1/queues", "/ojs/v1/queues/email/stats"} {
		rr := do(t, s, http.MethodGet, path, "")
		if rr.Code != 200 {
			t.Fatalf("GET %s status=%d body=%s", path, rr.Code, rr.Body)
		}
	}
	rr := post(t, s, "/ojs/v1/jobs", `{"type":"x","args":[]}`)
	if rr.Code != 403 || !strings.Contains(rr.Body.String(), "read_only") {
		t.Fatalf("enqueue read-only status=%d body=%s", rr.Code, rr.Body)
	}
	rr = post(t, s, "/ojs/v1/workers/fetch", `{"queues":["email"]}`)
	if rr.Code != 403 {
		t.Fatalf("fetch read-only status=%d", rr.Code)
	}
	rr = do(t, s, http.MethodDelete, "/ojs/v1/jobs/"+id, "")
	if rr.Code != 403 {
		t.Fatalf("cancel read-only status=%d", rr.Code)
	}
}

func TestValidationAndErrors(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	cases := []struct {
		body string
		want int
	}{
		{`{"type":"x"}`, 400},
		{`{"type":"X","args":[]}`, 400},
		{`{"type":"x","args":{}}`, 400},
		{`{"type":"x","args":[],"options":{"queue":"Bad Name"}}`, 400},
		{`{"type":"x","args":[],"priority":101}`, 400},
		{`{"type":"x","args":[],"options":{"scheduled_at":"nope"}}`, 400},
	}
	for _, c := range cases {
		rr := post(t, s, "/ojs/v1/jobs", c.body)
		if rr.Code != c.want {
			t.Fatalf("body %s: status=%d want=%d", c.body, rr.Code, c.want)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/jobs", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if rr.Code != 400 {
		t.Fatalf("content-type status = %d", rr.Code)
	}
	rr = post(t, s, "/ojs/v1/workers/ack", `{"job_id":"019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6"}`)
	if rr.Code != 400 || !strings.Contains(rr.Body.String(), "lease_token") {
		t.Fatalf("missing lease status=%d body=%s", rr.Code, rr.Body)
	}
}

func TestQueueStatsShape(t *testing.T) {
	st := &fakeStore{stats: func(_ context.Context, name string) (store.QueueStats, error) {
		if name != "email" {
			return store.QueueStats{}, store.ErrQueueNotFound
		}
		return store.QueueStats{Available: 2, Active: 1, Scheduled: 3, Retryable: 4, Completed: 5, Cancelled: 6, Discarded: 7, Total: 28}, nil
	}}
	s := testServer(t, st, false)
	rr := do(t, s, http.MethodGet, "/ojs/v1/queues/email/stats", "")
	if rr.Code != 200 {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body)
	}
	m := decodeMap(t, rr)
	stats := m["stats"].(map[string]any)
	if stats["available"] != float64(2) || stats["discarded"] != float64(7) {
		t.Fatalf("stats = %+v", stats)
	}
	queue := m["queue"].(map[string]any)
	if queue["name"] != "email" || queue["total"] != float64(28) {
		t.Fatalf("queue = %+v", queue)
	}
	rr = do(t, s, http.MethodGet, "/ojs/v1/queues/missing/stats", "")
	if rr.Code != 404 {
		t.Fatalf("missing queue status=%d", rr.Code)
	}
}

func TestMethodAndUnknownRoutes(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	rr := do(t, s, http.MethodGet, "/ojs/v1/jobs", "")
	if rr.Code != 405 && rr.Code != 404 {
		t.Fatalf("GET jobs status=%d", rr.Code)
	}
	rr = do(t, s, http.MethodGet, "/ojs/v1/nope", "")
	if rr.Code != 404 {
		t.Fatalf("unknown status=%d", rr.Code)
	}
}

func TestHealthDoesNotExposeBackendError(t *testing.T) {
	secret := errors.New("password=super-secret host=private-db")
	s := testServer(t, &fakeStore{ping: func(context.Context) error { return secret }}, false)
	rr := do(t, s, http.MethodGet, "/ojs/v1/health", "")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if strings.Contains(rr.Body.String(), "super-secret") || strings.Contains(rr.Body.String(), "private-db") {
		t.Fatalf("health response leaked backend error: %s", rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "database unavailable") {
		t.Fatalf("health response missing generic error: %s", rr.Body)
	}
}

func TestInternalErrorDoesNotExposeCause(t *testing.T) {
	s := testServer(t, &fakeStore{enqueue: func(context.Context, []store.EnqueueInput) ([]store.Job, error) {
		return nil, errors.New(`SQL failed: relation "secret_table" does not exist`)
	}}, false)
	rr := post(t, s, "/ojs/v1/jobs", `{"type":"x","args":[]}`)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if strings.Contains(rr.Body.String(), "secret_table") || strings.Contains(rr.Body.String(), "SQL failed") {
		t.Fatalf("internal error leaked cause: %s", rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "internal server error") {
		t.Fatalf("internal error missing generic message: %s", rr.Body)
	}
}

func TestFetchRejectsTooManyQueues(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	queues := make([]string, maxFetchQueues+1)
	for i := range queues {
		queues[i] = "q" + strconv.Itoa(i)
	}
	body, _ := json.Marshal(fetchRequest{Queues: queues})
	rr := post(t, s, "/ojs/v1/workers/fetch", string(body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
}

func TestMetricsExposeOJSRequests(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	if rr := do(t, s, http.MethodGet, "/ojs/v1/health", ""); rr.Code != http.StatusOK {
		t.Fatalf("health status = %d", rr.Code)
	}
	var b strings.Builder
	s.Metrics(&b)
	for _, want := range []string{
		`pgvs3_ojs_requests_total{method="GET",code="200"} 1`,
		"pgvs3_ojs_request_duration_seconds_count 1",
	} {
		if !strings.Contains(b.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, b.String())
		}
	}
}

func TestStreamMethodNotAllowed(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	cases := []struct {
		method string
		path   string
		allow  string
	}{
		{http.MethodGet, "/ojs/v1/queues/q/messages", "POST"},
		{http.MethodPost, "/ojs/v1/queues/q/stats", "GET"},
		{http.MethodPost, "/ojs/v1/queues/q/info", "GET"},
		{http.MethodPatch, "/ojs/v1/queues/q/dlq", "GET, DELETE"},
	}
	for _, tc := range cases {
		rr := do(t, s, tc.method, tc.path, "")
		if rr.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s status = %d body=%s", tc.method, tc.path, rr.Code, rr.Body)
		}
		if got := rr.Header().Get("Allow"); got != tc.allow {
			t.Fatalf("%s %s Allow = %q, want %q", tc.method, tc.path, got, tc.allow)
		}
	}
}

func TestStreamReceiveLongPollReturnsBatch(t *testing.T) {
	calls := 0
	st := &fakeStore{receive: func(context.Context, string, string, int) (*store.StreamBatch, error) {
		calls++
		if calls == 1 {
			return &store.StreamBatch{Consumer: "worker", StartTick: 1, EndTick: 1}, nil
		}
		return &store.StreamBatch{ID: 7, Consumer: "worker", StartTick: 1, EndTick: 2,
			Messages: []store.StreamMessage{{ID: "m1", Type: "t", Payload: []byte(`{}`)}}}, nil
	}}
	s := testServer(t, st, false)
	rr := post(t, s, "/ojs/v1/queues/q/receive", `{"consumer":"worker","max":10,"wait_ms":300}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if calls < 2 {
		t.Fatalf("calls = %d, want at least 2", calls)
	}
	m := decodeMap(t, rr)
	if m["batch_id"] != float64(7) || m["count"] != float64(1) {
		t.Fatalf("response = %+v", m)
	}
}

func TestWireStateConversion(t *testing.T) {
	states := []store.JobState{
		store.JobScheduled, store.JobAvailable, store.JobPending,
		store.JobActive, store.JobCompleted, store.JobRetryable,
		store.JobCancelled, store.JobDiscarded,
	}
	for _, state := range states {
		t.Run(state.String(), func(t *testing.T) {
			if got := wireState(state); got != state.String() {
				t.Fatalf("wireState(%d) = %q, want %q", state, got, state.String())
			}
			job := wireJob(store.Job{State: state, PreviousState: state})
			if job.State != state.String() {
				t.Fatalf("wireJob state = %q, want %q", job.State, state.String())
			}
			if job.PreviousState != state.String() {
				t.Fatalf("wireJob previous state = %q, want %q", job.PreviousState, state.String())
			}
		})
	}
	if got := wireState(store.JobStateUnknown); got != "" {
		t.Fatalf("wireState(unknown) = %q, want empty", got)
	}
	job := wireJob(store.Job{State: store.JobAvailable})
	if job.PreviousState != "" {
		t.Fatalf("unset previous state = %q", job.PreviousState)
	}
}

func TestWireJobLeaseExtensionAndPreviousState(t *testing.T) {
	leaseUntil := time.Date(2026, 10, 24, 12, 0, 1, 0, time.UTC)
	job := wireJob(store.Job{
		ID: "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", Type: "email.send",
		Queue: "email", State: store.JobCancelled, PreviousState: store.JobActive,
		Args: []byte(`[]`), LeaseToken: "token", LeaseUntil: &leaseUntil,
	})
	raw, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if body["state"] != "cancelled" || body["previous_state"] != "active" {
		t.Fatalf("state fields = %+v", body)
	}
	if body["lease_token"] != "token" || body["lease_expires_at"] == "" {
		t.Fatalf("lease extension = %+v", body)
	}
}

func TestEnqueueRequestInput(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	req := enqueueRequest{
		ID:   "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6",
		Type: "email.send",
		Args: json.RawMessage(`["a@example.com"]`),
		Meta: json.RawMessage(`{"trace_id":"t"}`),
		Priority: func() *int {
			v := 1
			return &v
		}(),
		MaxAttempts: func() *int {
			v := 9
			return &v
		}(),
		ScheduledAt: "2026-10-24T13:00:00Z",
		ExpiresAt:   "2026-10-25T13:00:00Z",
		Tags:        []string{"top"},
		Retry: &retryOptions{
			MaxAttempts:        new(4),
			InitialIntervalMS:  250,
			BackoffCoefficient: 3,
			MaxIntervalMS:      5000,
		},
		Options: enqueueOverrides{
			Queue: "email",
			Priority: func() *int {
				v := 7
				return &v
			}(),
			TimeoutMS: func() *int {
				v := 1500
				return &v
			}(),
			Tags: []string{"options"},
		},
	}
	in, err := req.input(now)
	if err != nil {
		t.Fatalf("input() error = %v", err)
	}
	if in.ID != req.ID || in.Type != req.Type || in.Queue != "email" {
		t.Fatalf("identity fields = %+v", in)
	}
	if in.Priority != 7 || in.MaxAttempts != 4 || in.TimeoutMS == nil || *in.TimeoutMS != 1500 {
		t.Fatalf("options = %+v", in)
	}
	wantScheduled := time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC)
	if !in.ScheduledAt.Equal(wantScheduled) {
		t.Fatalf("scheduled_at = %v, want %v", in.ScheduledAt, wantScheduled)
	}
	wantExpires := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC)
	if in.ExpiresAt == nil || !in.ExpiresAt.Equal(wantExpires) {
		t.Fatalf("expires_at = %v, want %v", in.ExpiresAt, wantExpires)
	}
	if in.RetryInitial != 250 || in.RetryMax != 5000 || in.RetryCoeff != 3 {
		t.Fatalf("retry policy = %+v", in)
	}
	if len(in.Tags) != 1 || in.Tags[0] != "top" {
		t.Fatalf("tags = %+v", in.Tags)
	}
}

func TestEnqueueRequestInputRelativeSchedule(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	req := enqueueRequest{
		Type: "email.send", Args: json.RawMessage(`[]`),
		RunAt: "+PT1M30S",
	}
	in, err := req.input(now)
	if err != nil {
		t.Fatalf("input() error = %v", err)
	}
	if want := now.Add(90 * time.Second); !in.ScheduledAt.Equal(want) {
		t.Fatalf("scheduled_at = %v, want %v", in.ScheduledAt, want)
	}
}

func TestEnqueueRequestInputValidation(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		req  enqueueRequest
	}{
		{name: "missing type", req: enqueueRequest{Args: json.RawMessage(`[]`)}},
		{name: "invalid type", req: enqueueRequest{Type: "Email.Send", Args: json.RawMessage(`[]`)}},
		{name: "missing args", req: enqueueRequest{Type: "email.send"}},
		{name: "args not array", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`{}`)}},
		{name: "args invalid json", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[`)}},
		{name: "meta not object", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Meta: json.RawMessage(`[]`)}},
		{name: "invalid id", req: enqueueRequest{ID: "nope", Type: "email.send", Args: json.RawMessage(`[]`)}},
		{name: "invalid queue", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Queue: "Bad Name"}},
		{name: "invalid options queue", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{Queue: "Bad Name"}}},
		{name: "priority too high", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Priority: new(101)}},
		{name: "priority too low", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Priority: new(-101)}},
		{name: "options priority too high", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{Priority: new(101)}}},
		{name: "negative attempts", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), MaxAttempts: new(-1)}},
		{name: "too many attempts", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), MaxAttempts: new(1001)}},
		{name: "options retry attempts too low", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{Retry: &retryOptions{MaxAttempts: new(-1)}}}},
		{name: "options retry policy attempts too high", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{RetryPolicy: &retryOptions{MaxAttempts: new(1001)}}}},
		{name: "negative timeout", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), TimeoutMS: new(-1)}},
		{name: "options negative timeout", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{TimeoutMS: new(-1)}}},
		{name: "invalid schedule", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), ScheduledAt: "not-a-time"}},
		{name: "invalid delay_until", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{DelayUntil: "not-a-time"}}},
		{name: "invalid options run_at", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{RunAt: "not-a-time"}}},
		{name: "invalid expires", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), ExpiresAt: "not-a-time"}},
		{name: "invalid options expires", req: enqueueRequest{Type: "email.send", Args: json.RawMessage(`[]`), Options: enqueueOverrides{ExpiresAt: "not-a-time"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.req.input(now)
			if err == nil {
				t.Fatal("input() succeeded, want validation error")
			}
			var apiErr *apiError
			if !errors.As(err, &apiErr) || apiErr.status != 400 {
				t.Fatalf("error = %v, want HTTP 400 apiError", err)
			}
		})
	}
}

func TestEnqueueRequestInputDefaults(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	in, err := (enqueueRequest{
		Type: "email.send", Args: json.RawMessage(`[]`),
	}).input(now)
	if err != nil {
		t.Fatalf("input() error = %v", err)
	}
	if in.Queue != "default" || in.Priority != 0 || in.MaxAttempts != 3 {
		t.Fatalf("defaults = %+v", in)
	}

	in, err = (enqueueRequest{
		Type: "email.send", Args: json.RawMessage(`[]`), Queue: "top-level",
		Options: enqueueOverrides{Queue: "options"},
	}).input(now)
	if err != nil {
		t.Fatalf("input() queue precedence error = %v", err)
	}
	if in.Queue != "top-level" {
		t.Fatalf("top-level queue should win: %+v", in)
	}
	if in.RetryInitial != 1000 || in.RetryMax != 60000 || in.RetryCoeff != 2 {
		t.Fatalf("retry defaults = %+v", in)
	}
	if !in.ScheduledAt.IsZero() || in.ExpiresAt != nil {
		t.Fatalf("schedule defaults = %+v", in)
	}
}

func TestEnqueueRequestAliasPrecedence(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	req := enqueueRequest{
		Type: "email.send", Args: json.RawMessage(`[]`),
		Queue: "top-level", Priority: new(1), MaxAttempts: new(2),
		TimeoutMS: new(10), ScheduledAt: "2026-10-24T13:00:00Z",
		RunAt: "+PT2H", ExpiresAt: "2026-10-25T13:00:00Z",
		Tags: []string{"top"},
		Retry: &retryOptions{
			MaxAttempts: new(3), InitialIntervalMS: 100,
			MaxIntervalMS: 1000, BackoffCoefficient: 1.5,
		},
		Options: enqueueOverrides{
			Queue: "options", Priority: new(7), TimeoutMS: new(1500),
			ScheduledAt: "2026-10-24T15:00:00Z",
			DelayUntil:  "2026-10-24T16:00:00Z",
			RunAt:       "2026-10-24T17:00:00Z",
			ExpiresAt:   "2026-10-26T13:00:00Z",
			Tags:        []string{"options"},
			Retry: &retryOptions{
				MaxAttempts: new(4), InitialIntervalMS: 250,
				MaxIntervalMS: 5000, BackoffCoefficient: 3,
			},
			RetryPolicy: &retryOptions{MaxAttempts: new(5)},
		},
	}
	in, err := req.input(now)
	if err != nil {
		t.Fatalf("input() error = %v", err)
	}
	if in.Queue != "top-level" {
		t.Fatalf("queue = %q, want top-level", in.Queue)
	}
	if in.Priority != 7 || in.MaxAttempts != 4 {
		t.Fatalf("options did not win: %+v", in)
	}
	if in.TimeoutMS == nil || *in.TimeoutMS != 1500 {
		t.Fatalf("timeout = %v, want 1500", in.TimeoutMS)
	}
	if want := time.Date(2026, 10, 24, 15, 0, 0, 0, time.UTC); !in.ScheduledAt.Equal(want) {
		t.Fatalf("scheduled_at = %v, want %v", in.ScheduledAt, want)
	}
	if want := time.Date(2026, 10, 26, 13, 0, 0, 0, time.UTC); in.ExpiresAt == nil || !in.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", in.ExpiresAt, want)
	}
	if len(in.Tags) != 1 || in.Tags[0] != "top" {
		t.Fatalf("tags = %+v, want top-level tags", in.Tags)
	}
	if in.RetryInitial != 250 || in.RetryMax != 5000 || in.RetryCoeff != 3 {
		t.Fatalf("retry policy = %+v", in)
	}
}

func TestEnqueueRequestScheduleAliasOrder(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		options     enqueueOverrides
		scheduledAt string
		runAt       string
		want        time.Time
	}{
		{
			name: "options scheduled_at first",
			options: enqueueOverrides{
				ScheduledAt: "2026-10-24T13:00:00Z",
				DelayUntil:  "2026-10-24T14:00:00Z",
				RunAt:       "2026-10-24T15:00:00Z",
			},
			scheduledAt: "2026-10-24T16:00:00Z", runAt: "2026-10-24T17:00:00Z",
			want: time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC),
		},
		{
			name: "options delay_until before run_at",
			options: enqueueOverrides{
				DelayUntil: "2026-10-24T14:00:00Z",
				RunAt:      "2026-10-24T15:00:00Z",
			},
			scheduledAt: "2026-10-24T16:00:00Z", runAt: "2026-10-24T17:00:00Z",
			want: time.Date(2026, 10, 24, 14, 0, 0, 0, time.UTC),
		},
		{
			name:        "options run_at before top-level",
			options:     enqueueOverrides{RunAt: "2026-10-24T15:00:00Z"},
			scheduledAt: "2026-10-24T16:00:00Z", runAt: "2026-10-24T17:00:00Z",
			want: time.Date(2026, 10, 24, 15, 0, 0, 0, time.UTC),
		},
		{
			name:        "top-level scheduled_at before run_at",
			scheduledAt: "2026-10-24T16:00:00Z", runAt: "2026-10-24T17:00:00Z",
			want: time.Date(2026, 10, 24, 16, 0, 0, 0, time.UTC),
		},
		{
			name:  "top-level run_at last",
			runAt: "2026-10-24T17:00:00Z",
			want:  time.Date(2026, 10, 24, 17, 0, 0, 0, time.UTC),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in, err := (enqueueRequest{
				Type: "email.send", Args: json.RawMessage(`[]`),
				Options: tc.options, ScheduledAt: tc.scheduledAt, RunAt: tc.runAt,
			}).input(now)
			if err != nil {
				t.Fatalf("input() error = %v", err)
			}
			if !in.ScheduledAt.Equal(tc.want) {
				t.Fatalf("scheduled_at = %v, want %v", in.ScheduledAt, tc.want)
			}
		})
	}
}

func TestEnqueueRequestMaxAttemptsPrecedence(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		options     enqueueOverrides
		retry       *retryOptions
		maxAttempts *int
		want        int
	}{
		{
			name: "options retry",
			options: enqueueOverrides{
				Retry:       &retryOptions{MaxAttempts: new(4)},
				RetryPolicy: &retryOptions{MaxAttempts: new(5)},
			},
			retry: &retryOptions{MaxAttempts: new(6)}, maxAttempts: new(7), want: 4,
		},
		{
			name: "options retry policy",
			options: enqueueOverrides{
				Retry:       &retryOptions{},
				RetryPolicy: &retryOptions{MaxAttempts: new(5)},
			},
			retry: &retryOptions{MaxAttempts: new(6)}, maxAttempts: new(7), want: 5,
		},
		{
			name:    "top-level retry",
			options: enqueueOverrides{Retry: &retryOptions{}},
			retry:   &retryOptions{MaxAttempts: new(6)}, maxAttempts: new(7), want: 6,
		},
		{
			name:        "top-level max_attempts",
			options:     enqueueOverrides{Retry: &retryOptions{}},
			maxAttempts: new(7), want: 7,
		},
		{name: "default", want: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in, err := (enqueueRequest{
				Type: "email.send", Args: json.RawMessage(`[]`),
				Options: tc.options, Retry: tc.retry, MaxAttempts: tc.maxAttempts,
			}).input(now)
			if err != nil {
				t.Fatalf("input() error = %v", err)
			}
			if in.MaxAttempts != tc.want {
				t.Fatalf("max_attempts = %d, want %d", in.MaxAttempts, tc.want)
			}
		})
	}
}

func TestEnqueueRequestTopLevelFallbacks(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	in, err := (enqueueRequest{
		Type: "email.send", Args: json.RawMessage(`[]`),
		Priority: new(-5), MaxAttempts: new(8), TimeoutMS: new(80),
		ScheduledAt: "2026-10-24T13:00:00Z", ExpiresAt: "2026-10-25T13:00:00Z",
		Tags: []string{"top"},
		Retry: &retryOptions{
			InitialInterval: "PT0.5S", MaxIntervalMS: 4000, BackoffCoefficient: 4,
		},
	}).input(now)
	if err != nil {
		t.Fatalf("input() error = %v", err)
	}
	if in.Queue != "default" || in.Priority != -5 || in.MaxAttempts != 8 {
		t.Fatalf("top-level options = %+v", in)
	}
	if in.TimeoutMS == nil || *in.TimeoutMS != 80 {
		t.Fatalf("timeout = %v, want 80", in.TimeoutMS)
	}
	if want := time.Date(2026, 10, 24, 13, 0, 0, 0, time.UTC); !in.ScheduledAt.Equal(want) {
		t.Fatalf("scheduled_at = %v, want %v", in.ScheduledAt, want)
	}
	if want := time.Date(2026, 10, 25, 13, 0, 0, 0, time.UTC); in.ExpiresAt == nil || !in.ExpiresAt.Equal(want) {
		t.Fatalf("expires_at = %v, want %v", in.ExpiresAt, want)
	}
	if in.RetryInitial != 500 || in.RetryMax != 4000 || in.RetryCoeff != 4 {
		t.Fatalf("retry policy = %+v", in)
	}
}

func TestOJSRouteSelection(t *testing.T) {
	cases := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{"manifest", http.MethodGet, "/ojs/manifest", "manifest"},
		{"health", http.MethodGet, "/ojs/v1/health", "health"},
		{"enqueue", http.MethodPost, "/ojs/v1/jobs", "job-enqueue"},
		{"batch-before-job-prefix", http.MethodPost, "/ojs/v1/jobs/batch", "job-batch"},
		{"ack", http.MethodPost, "/ojs/v1/workers/ack", "worker-ack"},
		{"nack", http.MethodPost, "/ojs/v1/workers/nack", "worker-nack"},
		{"heartbeat", http.MethodPost, "/ojs/v1/workers/heartbeat", "worker-heartbeat"},
		{"queues", http.MethodGet, "/ojs/v1/queues", "queues"},
		{"job-get", http.MethodGet, "/ojs/v1/jobs/019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", "job"},
		{"job-delete", http.MethodDelete, "/ojs/v1/jobs/019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", "job"},
		{"job-any-method", http.MethodPost, "/ojs/v1/jobs/019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", "job"},
		{"queue-stats", http.MethodGet, "/ojs/v1/queues/email/stats", "queue-stats"},
		{"queue-stats-wrong-method-falls-to-stream", http.MethodPost, "/ojs/v1/queues/email/stats", "stream"},
		{"queue-without-stream-action-falls-to-stream", http.MethodGet, "/ojs/v1/queues/email", "stream"},
		{"stream-send", http.MethodPost, "/ojs/v1/queues/email/messages", "stream"},
		{"stream-receive", http.MethodPost, "/ojs/v1/queues/email/receive", "stream"},
		{"stream-ack", http.MethodPost, "/ojs/v1/streams/ack", "stream-ack"},
		{"stream-nack", http.MethodPost, "/ojs/v1/streams/nack", "stream-nack"},
		{"job-exact-get", http.MethodGet, "/ojs/v1/jobs", ""},
		{"manifest-wrong-method", http.MethodPost, "/ojs/manifest", ""},
		{"unknown", http.MethodGet, "/ojs/v1/nope", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			got := ""
			if rt := selectOjsRoute(ojsRoutes, r); rt != nil {
				got = rt.name
			}
			if got != tc.want {
				t.Fatalf("%s %s selected %q, want %q", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestFetchRouteIsUnmeteredOnlyForPost(t *testing.T) {
	post := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", nil)
	if rt := selectOjsRoute(fetchRoutes, post); rt == nil || rt.name != "worker-fetch" {
		t.Fatalf("POST fetch selected %+v", rt)
	}
	if rt := selectOjsRoute(ojsRoutes, post); rt != nil {
		t.Fatalf("fetch also selected from metered table: %+v", rt)
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		r := httptest.NewRequest(method, "/ojs/v1/workers/fetch", nil)
		if rt := selectOjsRoute(fetchRoutes, r); rt != nil {
			t.Fatalf("%s fetch selected %+v", method, rt)
		}
	}
}

func TestStreamMethodsAndReadOnlyPrecedence(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	for _, tc := range []struct{ method, path, allow string }{
		{"GET", "messages", "POST"}, {"GET", "messages/batch", "POST"},
		{"DELETE", "consumers", "GET, POST"}, {"POST", "consumers/alice", "DELETE"},
		{"POST", "batches/1", "GET"}, {"DELETE", "info", "GET"},
		{"DELETE", "config", "GET, POST"}, {"POST", "dlq", "GET, DELETE"},
		{"GET", "dlq/replay", "POST"}, {"GET", "dlq/1/replay", "POST"},
		{"POST", "stats", "GET"},
	} {
		rr := do(t, s, tc.method, "/ojs/v1/queues/email/"+tc.path, "")
		if rr.Code != http.StatusMethodNotAllowed || rr.Header().Get("Allow") != tc.allow {
			t.Fatalf("%s %s: status=%d allow=%q body=%s", tc.method, tc.path, rr.Code, rr.Header().Get("Allow"), rr.Body)
		}
	}
	s.readOnly = true
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/ojs/v1/jobs", 403}, {"/ojs/v1/workers/ack", 403},
		{"/ojs/v1/queues/email/messages", 403}, {"/ojs/v1/queues/email/receive", 403},
		{"/ojs/v1/queues/INVALID/messages", 400}, {"/ojs/v1/queues/INVALID/receive", 404},
	} {
		rr := post(t, s, tc.path, "{")
		if rr.Code != tc.status {
			t.Fatalf("read-only %s: status=%d body=%s", tc.path, rr.Code, rr.Body)
		}
	}
}

func TestParseFetchRequest(t *testing.T) {
	s := testServer(t, &fakeStore{}, false)
	valid := []struct {
		name       string
		body       string
		count      int
		visibility time.Duration
	}{
		{"defaults", `{"queues":["email"]}`, 1, 30 * time.Second},
		{"count-zero", `{"queues":["email"],"count":0}`, 1, 30 * time.Second},
		{"count-negative", `{"queues":["email"],"count":-5}`, 1, 30 * time.Second},
		{"count-clamped", `{"queues":["email"],"count":500}`, 100, 30 * time.Second},
		{"visibility", `{"queues":["email"],"visibility_timeout_ms":1500}`, 1, 1500 * time.Millisecond},
		{"visibility-zero", `{"queues":["email"],"visibility_timeout_ms":0}`, 1, 0},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", mediaType)
			got, visibility, e := s.parseFetchRequest(httptest.NewRecorder(), req)
			if e != nil {
				t.Fatalf("parse: %+v", e)
			}
			if got.Count != tc.count || visibility != tc.visibility {
				t.Fatalf("count=%d visibility=%s, want %d/%s", got.Count, visibility, tc.count, tc.visibility)
			}
			if len(got.Queues) != 1 || got.Queues[0] != "email" {
				t.Fatalf("queues = %v", got.Queues)
			}
		})
	}

	invalid := []struct {
		name string
		body string
	}{
		{"empty-body", ``},
		{"malformed", `{"queues":`},
		{"no-queues", `{"queues":[]}`},
		{"invalid-queue", `{"queues":["Bad Name"]}`},
		{"negative-visibility", `{"queues":["email"],"visibility_timeout_ms":-1}`},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", mediaType)
			if _, _, e := s.parseFetchRequest(httptest.NewRecorder(), req); e == nil || e.status != http.StatusBadRequest {
				t.Fatalf("error = %+v, want 400", e)
			}
		})
	}
}

func TestFetchUnknownQueueReturnsImmediately(t *testing.T) {
	calls := 0
	st := &fakeStore{
		exists: func(context.Context, ns.Kind, string) (bool, error) { return false, nil },
		fetch: func(context.Context, []string, int, string, time.Duration) ([]store.Job, error) {
			calls++
			return nil, nil
		},
	}
	s := testServer(t, st, false)
	s.longPoll = time.Hour

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch",
		strings.NewReader(`{"queues":["missing"]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", mediaType)
	rr := httptest.NewRecorder()
	started := time.Now()
	s.ServeHTTP(rr, req)
	if elapsed := time.Since(started); elapsed > 200*time.Millisecond {
		t.Fatalf("unknown queue fetch took %s", elapsed)
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if calls != 1 {
		t.Fatalf("fetch calls = %d, want 1", calls)
	}
	m := decodeMap(t, rr)
	jobs, ok := m["jobs"].([]any)
	if !ok || len(jobs) != 0 {
		t.Fatalf("jobs = %#v", m["jobs"])
	}
	if _, ok := m["job"]; ok {
		t.Fatalf("empty response must omit job: %s", rr.Body)
	}
}

func TestFetchLongPollDeadlineReturnsEmptyShape(t *testing.T) {
	calls := 0
	st := &fakeStore{fetch: func(context.Context, []string, int, string, time.Duration) ([]store.Job, error) {
		calls++
		return nil, nil
	}}
	s := testServer(t, st, false)
	s.longPoll = 40 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/ojs/v1/workers/fetch",
		strings.NewReader(`{"queues":["email"]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", mediaType)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rr.Code, rr.Body)
	}
	if calls < 1 {
		t.Fatalf("fetch was not attempted")
	}
	m := decodeMap(t, rr)
	jobs, ok := m["jobs"].([]any)
	if !ok || len(jobs) != 0 {
		t.Fatalf("jobs = %#v", m["jobs"])
	}
	if _, ok := m["job"]; ok {
		t.Fatalf("empty response must omit job: %s", rr.Body)
	}
}

func BenchmarkHTTPBinding(b *testing.B) {
	job := sampleJob()
	st := &fakeStore{
		enqueue: func(context.Context, []store.EnqueueInput) ([]store.Job, error) { return []store.Job{job}, nil },
		fetch: func(context.Context, []string, int, string, time.Duration) ([]store.Job, error) {
			return []store.Job{job}, nil
		},
		ack:  func(context.Context, string, string, []byte) (store.Job, error) { return job, nil },
		nack: func(context.Context, string, string, []byte, bool) (store.Job, error) { return job, nil },
		receive: func(context.Context, string, string, int) (*store.StreamBatch, error) {
			return &store.StreamBatch{ID: 1, Consumer: "consumer"}, nil
		},
	}
	for _, tc := range []struct{ name, path, body string }{
		{"enqueue", "/ojs/v1/jobs", `{"type":"bench","args":["value"]}`},
		{"fetch", "/ojs/v1/workers/fetch", `{"queues":["bench"],"worker_id":"w"}`},
		{"ack", "/ojs/v1/workers/ack", `{"job_id":"` + job.ID + `","lease_token":"token"}`},
		{"nack", "/ojs/v1/workers/nack", `{"job_id":"` + job.ID + `","lease_token":"token","error":{"retryable":true}}`},
		{"stream-receive", "/ojs/v1/queues/bench/receive", `{"consumer":"consumer"}`},
		{"invalid", "/ojs/v1/jobs", `{"type":"INVALID","args":[]}`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s := New(st, slog.New(slog.NewTextHandler(io.Discard, nil)), false, 4, "bench")
			b.ReportAllocs()
			for b.Loop() {
				r := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
				r.Header.Set("Content-Type", mediaType)
				w := httptest.NewRecorder()
				s.ServeHTTP(w, r)
				if w.Code >= 500 {
					b.Fatalf("HTTP %d: %s", w.Code, w.Body)
				}
			}
		})
	}
}

func TestPollUsesFakeTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		value, err := poll(context.Background(), time.Second, func() (int, error) {
			calls++
			return calls, nil
		}, func(v int) bool { return v >= 3 })
		if err != nil || value != 3 || calls != 3 {
			t.Fatalf("poll = %d, %v, calls=%d", value, err, calls)
		}
	})
}

type queueView struct {
	f    *fakeStore
	name string
}

func (v queueView) Stats(ctx context.Context) (store.QueueStats, error) {
	return v.f.QueueStats(ctx, v.name)
}

func (v queueView) Info(ctx context.Context) (store.StreamInfo, error) {
	return v.f.StreamInfo(ctx, v.name)
}

func (v queueView) AppendMessages(ctx context.Context, msgs []store.StreamMessageInput) ([]store.StreamMessage, error) {
	return v.f.AppendStreamMessages(ctx, v.name, msgs)
}

func (v queueView) ReceiveBatch(ctx context.Context, consumer string, max int) (*store.StreamBatch, error) {
	return v.f.ReceiveStreamBatch(ctx, v.name, consumer, max)
}

func (v queueView) ReceiveCoopBatch(ctx context.Context, consumer, member string, max int) (*store.StreamBatch, error) {
	return v.f.ReceiveStreamCoopBatch(ctx, v.name, consumer, member, max)
}

func (v queueView) GetBatch(ctx context.Context, id int64) (*store.StreamBatch, error) {
	return v.f.GetStreamBatch(ctx, v.name, id)
}

func (v queueView) AckBatch(ctx context.Context, id int64) (int64, error) {
	return v.f.AckStreamBatch(ctx, v.name, id)
}

func (v queueView) NackMessage(ctx context.Context, batch int64, msg string, delay time.Duration, reason string) (string, error) {
	return v.f.NackStreamMessage(ctx, v.name, batch, msg, delay, reason)
}

func (v queueView) ListConsumers(ctx context.Context) ([]store.StreamConsumerInfo, error) {
	return v.f.ListStreamConsumers(ctx, v.name)
}

func (v queueView) Subscribe(ctx context.Context, consumer string, from *int64) (store.StreamSubscription, error) {
	return v.f.SubscribeStream(ctx, v.name, consumer, from)
}

func (v queueView) Unsubscribe(ctx context.Context, consumer string) (bool, error) {
	return v.f.UnsubscribeStream(ctx, v.name, consumer)
}

func (v queueView) Config(ctx context.Context) (store.StreamConfig, error) {
	return v.f.StreamConfig(ctx, v.name)
}

func (v queueView) SetConfig(ctx context.Context, maxRetries int, retention time.Duration) (store.StreamConfig, error) {
	return v.f.SetStreamConfig(ctx, v.name, maxRetries, retention)
}

func (v queueView) ListDeadLetters(ctx context.Context, limit int) ([]store.DeadLetter, error) {
	return v.f.ListDeadLetters(ctx, v.name, limit)
}

func (v queueView) PurgeDeadLetters(ctx context.Context, olderThan time.Duration) (int64, error) {
	return v.f.PurgeDeadLetters(ctx, v.name, olderThan)
}

func (v queueView) ReplayAllDeadLetters(ctx context.Context) (int64, error) {
	return v.f.ReplayAllDeadLetters(ctx, v.name)
}

func (v queueView) ReplayDeadLetter(ctx context.Context, id int64) (string, error) {
	return v.f.ReplayDeadLetter(ctx, v.name, id)
}

func (f *fakeStore) Queue(name string) store.QueueHandle { return queueView{f: f, name: name} }
