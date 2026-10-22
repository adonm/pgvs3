package s3

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type metrics struct {
	requests sync.Map // key -> *atomic.Uint64
	duration atomic.Uint64
	outBytes atomic.Uint64
}

func newMetrics() *metrics { return &metrics{} }

func (m *metrics) observe(method string, code int, d time.Duration, out int64) {
	key := fmt.Sprintf("%s\x00%d", method, code)
	v, ok := m.requests.Load(key)
	if !ok {
		v, _ = m.requests.LoadOrStore(key, new(atomic.Uint64))
	}
	v.(*atomic.Uint64).Add(1)
	m.duration.Add(uint64(d))
	if out > 0 {
		m.outBytes.Add(uint64(out))
	}
}

func (m *metrics) write(w http.ResponseWriter) {
	keys := make([]string, 0, 64)
	counts := make(map[string]uint64, 64)
	m.requests.Range(func(k, v any) bool {
		keys = append(keys, k.(string))
		counts[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})
	sort.Strings(keys)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, k := range keys {
		method, code, _ := cut(k, '\x00')
		fmt.Fprintf(w, "pgvs3_requests_total{method=%q,code=%q} %d\n", method, code, counts[k])
	}
	fmt.Fprintf(w, "pgvs3_request_duration_seconds_sum %f\n", float64(m.duration.Load())/float64(time.Second))
	fmt.Fprintf(w, "pgvs3_request_duration_seconds_count %d\n", total(counts))
	fmt.Fprintf(w, "pgvs3_response_bytes_total %d\n", m.outBytes.Load())
}

func total(m map[string]uint64) uint64 {
	var n uint64
	for _, v := range m {
		n += v
	}
	return n
}

func cut(s string, sep byte) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
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
