package s3

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

type requestKey struct {
	method string
	code   int
}

type metrics struct {
	requests sync.Map // requestKey -> *atomic.Uint64
	duration atomic.Uint64
	outBytes atomic.Uint64
	buckets  [11]atomic.Uint64 // non-cumulative; last is +Inf
}

var latencyBounds = [...]float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

func newMetrics() *metrics { return &metrics{} }

func (m *metrics) observe(method string, code int, d time.Duration, out int64) {
	key := fmt.Sprintf("%s\x00%d", method, code)
	v, ok := m.requests.Load(key)
	if !ok {
		v, _ = m.requests.LoadOrStore(key, new(atomic.Uint64))
	}
	v.(*atomic.Uint64).Add(1)
	m.duration.Add(uint64(d))
	ms := float64(d) / float64(time.Millisecond)
	idx := len(latencyBounds)
	for i, bound := range latencyBounds {
		if ms <= bound {
			idx = i
			break
		}
	}
	m.buckets[idx].Add(1)
	if out > 0 {
		m.outBytes.Add(uint64(out))
	}
}

func (m *metrics) write(w http.ResponseWriter, st *store.Store) {
	keys := make([]requestKey, 0, 64)
	counts := make(map[requestKey]uint64, 64)
	m.requests.Range(func(k, v any) bool {
		rk := k.(requestKey)
		keys = append(keys, rk)
		counts[rk] = v.(*atomic.Uint64).Load()
		return true
	})
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].method != keys[j].method {
			return keys[i].method < keys[j].method
		}
		return keys[i].code < keys[j].code
	})
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, k := range keys {
		fmt.Fprintf(w, "pgvs3_requests_total{method=%q,code=%q} %d\n", k.method, strconv.Itoa(k.code), counts[k])
	}
	fmt.Fprintf(w, "pgvs3_request_duration_seconds_sum %f\n", float64(m.duration.Load())/float64(time.Second))
	fmt.Fprintf(w, "pgvs3_request_duration_seconds_count %d\n", total(counts))
	var cumulative uint64
	for i, bound := range latencyBounds {
		cumulative += m.buckets[i].Load()
		fmt.Fprintf(w, "pgvs3_request_duration_seconds_bucket{le=%q} %d\n", fmt.Sprintf("%g", bound/1000), cumulative)
	}
	cumulative += m.buckets[len(latencyBounds)].Load()
	fmt.Fprintf(w, "pgvs3_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", cumulative)
	fmt.Fprintf(w, "pgvs3_response_bytes_total %d\n", m.outBytes.Load())
	if st != nil {
		st.WriteMetrics(w)
	}
}

func total(m map[requestKey]uint64) uint64 {
	var n uint64
	for _, v := range m {
		n += v
	}
	return n
}

// recWriter captures the status and bytes for metrics while preserving Flush
// for cut-through GET responses.
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
