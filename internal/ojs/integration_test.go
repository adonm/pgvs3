//go:build integration

package ojs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
)

var (
	intStore *store.Store
	intCtx   = context.Background()
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunMain(m, func(ctx context.Context, url string) (func(), error) {
		st, err := store.Open(ctx, url, 8, testutil.DiscardLogger())
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		if err := st.Init(ctx); err != nil {
			st.Close()
			return nil, fmt.Errorf("init schema: %w", err)
		}
		intStore = st
		return st.Close, nil
	}))
}

type apiReply struct {
	status int
	body   map[string]any
	header http.Header
}

func newIntServer(t *testing.T, readOnly bool) (string, *Server) {
	t.Helper()
	srv := New(intStore, testutil.DiscardLogger(), readOnly, 8, "test")
	srv.longPoll = 0
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts.URL, srv
}

func uniqQueue(t *testing.T) string {
	t.Helper()
	q := fmt.Sprintf("ojs-%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = intStore.DropNamespace(intCtx, ns.Queue, q, true) })
	return q
}

func call(t *testing.T, base, method, path, body string) apiReply {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, base+path, rdr)
	testutil.Must(t, err)
	if body != "" {
		req.Header.Set("Content-Type", mediaType)
	}
	resp, err := http.DefaultClient.Do(req)
	testutil.Must(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	reply := apiReply{status: resp.StatusCode, header: resp.Header.Clone()}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &reply.body); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return reply
}

func expectStatus(t *testing.T, r apiReply, want int) {
	t.Helper()
	if r.status != want {
		t.Fatalf("status = %d, want %d: %+v", r.status, want, r.body)
	}
}

func enqueue(t *testing.T, base, queue, typ, args, extra string) map[string]any {
	t.Helper()
	opts := `"queue":"` + queue + `"`
	if extra != "" {
		opts += "," + extra
	}
	body := `{"type":"` + typ + `","args":` + args + `,"options":{` + opts + `}}`
	r := call(t, base, http.MethodPost, "/ojs/v1/jobs", body)
	expectStatus(t, r, http.StatusCreated)
	return r.body["job"].(map[string]any)
}

func fetch(t *testing.T, base, queue string, count int, visibilityMS int) []map[string]any {
	t.Helper()
	body := fmt.Sprintf(`{"queues":["%s"],"count":%d,"worker_id":"worker-1","visibility_timeout_ms":%d}`,
		queue, count, visibilityMS)
	r := call(t, base, http.MethodPost, "/ojs/v1/workers/fetch", body)
	expectStatus(t, r, http.StatusOK)
	raw := r.body["jobs"].([]any)
	out := make([]map[string]any, len(raw))
	for i, j := range raw {
		out[i] = j.(map[string]any)
	}
	return out
}

func TestIntegrationLeaseAckAndStale(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	created := enqueue(t, base, q, "email.send", `["a@example.com",{"locale":"en"}]`, `"priority":3`)
	if created["state"] != store.JobAvailable.String() || created["attempt"] != float64(0) {
		t.Fatalf("created = %+v", created)
	}
	jobs := fetch(t, base, q, 1, 100)
	if len(jobs) != 1 {
		t.Fatalf("fetched %d jobs", len(jobs))
	}
	job := jobs[0]
	id := job["id"].(string)
	token := job["lease_token"].(string)
	if token == "" || job["state"] != store.JobActive.String() || job["attempt"] != float64(1) {
		t.Fatalf("fetched job = %+v", job)
	}
	if job["started_at"] == nil || job["worker_id"] != "worker-1" {
		t.Fatalf("lease fields = %+v", job)
	}

	r := call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+id+`","lease_token":"wrong","result":{"ok":true}}`)
	expectStatus(t, r, http.StatusConflict)
	if r.body["error"].(map[string]any)["code"] != "stale_lease" {
		t.Fatalf("wrong token error = %+v", r.body)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+id+`","lease_token":"`+token+`","result":{"ok":true}}`)
	expectStatus(t, r, http.StatusOK)
	if r.body["state"] != store.JobCompleted.String() || r.body["acknowledged"] != true {
		t.Fatalf("ack = %+v", r.body)
	}
	r = call(t, base, http.MethodGet, "/ojs/v1/jobs/"+id, "")
	expectStatus(t, r, http.StatusOK)
	got := r.body["job"].(map[string]any)
	if got["state"] != store.JobCompleted.String() || got["completed_at"] == "" || got["result"] == nil {
		t.Fatalf("completed job = %+v", got)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+id+`","lease_token":"`+token+`"}`)
	expectStatus(t, r, http.StatusConflict)

	enqueue(t, base, q, "email.send", `[]`, "")
	jobs = fetch(t, base, q, 1, 10)
	oldID, oldToken := jobs[0]["id"].(string), jobs[0]["lease_token"].(string)
	testutil.Eventually(t, 5*time.Second, "expired lease was not redelivered", func() bool {
		jobs = fetch(t, base, q, 1, 1000)
		return len(jobs) == 1
	})
	if jobs[0]["id"] != oldID {
		t.Fatalf("refetch = %+v", jobs)
	}
	newToken := jobs[0]["lease_token"].(string)
	if newToken == oldToken {
		t.Fatal("lease token was reused")
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+oldID+`","lease_token":"`+oldToken+`"}`)
	expectStatus(t, r, http.StatusConflict)
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+oldID+`","lease_token":"`+newToken+`"}`)
	expectStatus(t, r, http.StatusOK)
}

func TestIntegrationLeaseExclusivity(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	for range 2 {
		enqueue(t, base, q, "email.send", `[]`, "")
	}
	var wg sync.WaitGroup
	results := make([][]store.Job, 2)
	errs := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = intStore.FetchJobs(intCtx, []string{q}, 1, "worker-1", time.Second)
		}(i)
	}
	wg.Wait()
	seen := map[string]bool{}
	for i, jobs := range results {
		if errs[i] != nil {
			t.Fatalf("fetch %d: %v", i, errs[i])
		}
		if len(jobs) != 1 {
			t.Fatalf("fetch %d returned %d jobs", i, len(jobs))
		}
		if seen[jobs[0].ID] {
			t.Fatalf("job %s leased twice", jobs[0].ID)
		}
		seen[jobs[0].ID] = true
	}
}

func TestIntegrationPriorityAndScheduled(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	enqueue(t, base, q, "low", `[]`, `"priority":0`)
	enqueue(t, base, q, "high", `[]`, `"priority":10`)
	jobs := fetch(t, base, q, 1, 1000)
	if len(jobs) != 1 || jobs[0]["type"] != "high" {
		t.Fatalf("priority fetch = %+v", jobs)
	}
	id := jobs[0]["id"].(string)
	r := call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+id+`","lease_token":"`+jobs[0]["lease_token"].(string)+`"}`)
	expectStatus(t, r, http.StatusOK)

	drained := fetch(t, base, q, 1, 1000)
	if len(drained) != 1 || drained[0]["type"] != "low" {
		t.Fatalf("remaining low-priority fetch = %+v", drained)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+drained[0]["id"].(string)+`","lease_token":"`+drained[0]["lease_token"].(string)+`"}`)
	expectStatus(t, r, http.StatusOK)

	scheduledAt := time.Now().UTC().Add(time.Second).Format(time.RFC3339Nano)
	body := `{"type":"later","args":[],"queue":"` + q + `","scheduled_at":"` + scheduledAt + `"}`
	r = call(t, base, http.MethodPost, "/ojs/v1/jobs", body)
	expectStatus(t, r, http.StatusCreated)
	scheduled := r.body["job"].(map[string]any)
	if scheduled["state"] != store.JobScheduled.String() {
		t.Fatalf("scheduled state = %+v", scheduled)
	}
	if got := fetch(t, base, q, 1, 1000); len(got) != 0 {
		t.Fatalf("scheduled job fetched before scheduled_at: %+v", got)
	}
	testutil.Eventually(t, 5*time.Second, "scheduled job was not fetched when due", func() bool {
		jobs = fetch(t, base, q, 1, 1000)
		return len(jobs) != 0
	})
	if len(jobs) != 1 || jobs[0]["type"] != "later" {
		t.Fatalf("due scheduled fetch = %+v", jobs)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/ack",
		`{"job_id":"`+jobs[0]["id"].(string)+`","lease_token":"`+jobs[0]["lease_token"].(string)+`"}`)
	expectStatus(t, r, http.StatusOK)
}

func TestIntegrationNackRetryDiscard(t *testing.T) {
	base, srv := newIntServer(t, false)
	srv.longPoll = 300 * time.Millisecond
	q := uniqQueue(t)
	created := enqueue(t, base, q, "flaky", `[]`,
		`"max_attempts":2,"retry":{"max_attempts":2,"initial_interval_ms":10,"backoff_coefficient":2}`)
	id := created["id"].(string)
	jobs := fetch(t, base, q, 1, 1000)
	if len(jobs) != 1 || jobs[0]["attempt"] != float64(1) {
		t.Fatalf("first fetch = %+v", jobs)
	}
	r := call(t, base, http.MethodPost, "/ojs/v1/workers/nack",
		`{"job_id":"`+id+`","lease_token":"`+jobs[0]["lease_token"].(string)+`","error":{"retryable":true,"message":"boom"}}`)
	expectStatus(t, r, http.StatusOK)
	if r.body["state"] != store.JobRetryable.String() || r.body["attempt"] != float64(2) {
		t.Fatalf("retry nack = %+v", r.body)
	}
	if r.body["next_attempt_at"] == "" {
		t.Fatalf("missing next_attempt_at: %+v", r.body)
	}
	jobs = fetch(t, base, q, 1, 1000)
	if len(jobs) != 1 || jobs[0]["attempt"] != float64(2) {
		t.Fatalf("retry fetch = %+v", jobs)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/nack",
		`{"job_id":"`+id+`","lease_token":"`+jobs[0]["lease_token"].(string)+`","error":{"retryable":true,"message":"again"}}`)
	expectStatus(t, r, http.StatusOK)
	if r.body["state"] != store.JobDiscarded.String() || r.body["discarded_at"] == "" {
		t.Fatalf("discard nack = %+v", r.body)
	}
	r = call(t, base, http.MethodGet, "/ojs/v1/jobs/"+id, "")
	expectStatus(t, r, http.StatusOK)
	if r.body["job"].(map[string]any)["state"] != store.JobDiscarded.String() {
		t.Fatalf("discarded job = %+v", r.body)
	}

	created = enqueue(t, base, q, "fatal", `[]`, `"max_attempts":5`)
	jobs = fetch(t, base, q, 1, 1000)
	r = call(t, base, http.MethodPost, "/ojs/v1/workers/nack",
		`{"job_id":"`+created["id"].(string)+`","lease_token":"`+jobs[0]["lease_token"].(string)+`","error":{"retryable":false,"message":"fatal"}}`)
	expectStatus(t, r, http.StatusOK)
	if r.body["state"] != store.JobDiscarded.String() {
		t.Fatalf("non-retryable nack = %+v", r.body)
	}
}

func TestIntegrationStreamSendReceiveAck(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)

	r := call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/consumers",
		`{"consumer":"stream-worker","from_tick":0}`)
	expectStatus(t, r, http.StatusCreated)

	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/messages",
		`{"type":"order.created","payload":{"id":42},"headers":{"trace":"t"}}`)
	expectStatus(t, r, http.StatusCreated)
	sent := r.body["message"].(map[string]any)
	if sent["id"] == "" || sent["payload"].(map[string]any)["id"] != float64(42) {
		t.Fatalf("sent = %+v", sent)
	}

	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/receive",
		`{"consumer":"stream-worker","max":10}`)
	expectStatus(t, r, http.StatusOK)
	batchID := int64(r.body["batch_id"].(float64))
	messages := r.body["messages"].([]any)
	if batchID == 0 || len(messages) != 1 {
		t.Fatalf("receive = %+v", r.body)
	}
	if messages[0].(map[string]any)["type"] != "order.created" {
		t.Fatalf("message = %+v", messages[0])
	}

	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/consumers",
		`{"consumer":"audit","from_tick":0}`)
	expectStatus(t, r, http.StatusCreated)
	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/receive",
		`{"consumer":"audit","max":10}`)
	expectStatus(t, r, http.StatusOK)
	if len(r.body["messages"].([]any)) != 1 {
		t.Fatalf("audit fanout = %+v", r.body)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/streams/ack",
		fmt.Sprintf(`{"queue":%q,"batch_id":%d}`, q, batchID))
	expectStatus(t, r, http.StatusOK)
	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/receive",
		`{"consumer":"stream-worker","max":10}`)
	expectStatus(t, r, http.StatusOK)
	if len(r.body["messages"].([]any)) != 0 {
		t.Fatalf("post-ack receive = %+v", r.body)
	}

	r = call(t, base, http.MethodGet, "/ojs/v1/queues/"+q+"/info", "")
	expectStatus(t, r, http.StatusOK)
	info := r.body["stream"].(map[string]any)
	if info["events"] != float64(1) || info["consumers"] != float64(2) {
		t.Fatalf("stream info = %+v", info)
	}
	r = call(t, base, http.MethodGet, "/ojs/v1/queues/"+q+"/consumers", "")
	expectStatus(t, r, http.StatusOK)
	if len(r.body["consumers"].([]any)) != 2 {
		t.Fatalf("consumers = %+v", r.body)
	}
	t.Run("job and stream share a queue", func(t *testing.T) {
		base, _ := newIntServer(t, false)
		q := uniqQueue(t)
		created := enqueue(t, base, q, "email.send", `[]`, "")
		if created["state"] != store.JobAvailable.String() {
			t.Fatalf("job = %+v", created)
		}
		r := call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/consumers",
			`{"consumer":"stream-worker","from_tick":0}`)
		expectStatus(t, r, http.StatusCreated)
		r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/messages",
			`{"type":"event","payload":{"n":1}}`)
		expectStatus(t, r, http.StatusCreated)

		jobs := fetch(t, base, q, 1, 1000)
		if len(jobs) != 1 {
			t.Fatalf("jobs = %+v", jobs)
		}
		r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/receive",
			`{"consumer":"stream-worker","max":10}`)
		expectStatus(t, r, http.StatusOK)
		if len(r.body["messages"].([]any)) != 1 {
			t.Fatalf("stream messages = %+v", r.body)
		}
	})
}

func TestIntegrationStreamConfig(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	r := call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/consumers",
		`{"consumer":"config-worker","from_tick":0}`)
	expectStatus(t, r, http.StatusCreated)
	r = call(t, base, http.MethodPost, "/ojs/v1/queues/"+q+"/config",
		`{"max_retries":2,"retention":"2h"}`)
	expectStatus(t, r, http.StatusOK)
	cfg := r.body["stream"].(map[string]any)
	if cfg["max_retries"] != float64(2) || cfg["retention"] != "2h0m0s" {
		t.Fatalf("config = %+v", cfg)
	}

}

func TestIntegrationCancelAndReadOnly(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	created := enqueue(t, base, q, "cancel.me", `[]`, "")
	id := created["id"].(string)
	r := call(t, base, http.MethodDelete, "/ojs/v1/jobs/"+id, "")
	expectStatus(t, r, http.StatusOK)
	cancelled := r.body["job"].(map[string]any)
	if cancelled["state"] != store.JobCancelled.String() {
		t.Fatalf("cancel = %+v", r.body)
	}
	if cancelled["previous_state"] != store.JobAvailable.String() {
		t.Fatalf("cancel previous_state = %+v", cancelled)
	}
	r = call(t, base, http.MethodDelete, "/ojs/v1/jobs/"+id, "")
	expectStatus(t, r, http.StatusConflict)

	r = call(t, base, http.MethodGet, "/ojs/v1/queues/"+q+"/stats", "")
	expectStatus(t, r, http.StatusOK)
	if r.body["stats"].(map[string]any)["cancelled"] != float64(1) {
		t.Fatalf("stats = %+v", r.body)
	}
}

func TestIntegrationBatchListAndStats(t *testing.T) {
	base, _ := newIntServer(t, false)
	q := uniqQueue(t)
	body := `{"jobs":[{"type":"a.one","args":[1],"options":{"queue":"` + q + `"}},
		{"type":"a.two","args":[2],"options":{"queue":"` + q + `"}}]}`
	r := call(t, base, http.MethodPost, "/ojs/v1/jobs/batch", body)
	expectStatus(t, r, http.StatusCreated)
	if r.body["count"] != float64(2) {
		t.Fatalf("batch = %+v", r.body)
	}
	r = call(t, base, http.MethodPost, "/ojs/v1/jobs/batch",
		`{"jobs":[{"type":"a.three","args":[],"options":{"queue":"`+q+`"}},{"type":"a.four","args":{}}]}`)
	expectStatus(t, r, http.StatusBadRequest)
	r = call(t, base, http.MethodGet, "/ojs/v1/queues/"+q+"/stats", "")
	expectStatus(t, r, http.StatusOK)
	stats := r.body["stats"].(map[string]any)
	if stats["available"] != float64(2) || stats["total"] != float64(2) {
		t.Fatalf("atomic batch stats = %+v", stats)
	}
	r = call(t, base, http.MethodGet, "/ojs/v1/queues", "")
	expectStatus(t, r, http.StatusOK)
	found := false
	for _, raw := range r.body["queues"].([]any) {
		if raw.(map[string]any)["name"] == q {
			found = true
		}
	}
	if !found {
		t.Fatalf("queue %s missing from list: %+v", q, r.body)
	}

}
