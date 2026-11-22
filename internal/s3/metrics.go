package s3

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type requestMetrics struct {
	requests sync.Map
	duration atomic.Uint64
	count    atomic.Uint64
	outBytes atomic.Uint64
}

type metricKey struct {
	method string
	code   int
}

func (m *requestMetrics) Observe(method string, code int, d time.Duration, out int64) {
	k := metricKey{method, code}
	v, ok := m.requests.Load(k)
	if !ok {
		v, _ = m.requests.LoadOrStore(k, new(atomic.Uint64))
	}
	v.(*atomic.Uint64).Add(1)
	m.count.Add(1)
	m.duration.Add(uint64(d))
	if out > 0 {
		m.outBytes.Add(uint64(out))
	}
}

func (m *requestMetrics) Write(w io.Writer, prefix string) {
	m.requests.Range(func(k, v any) bool {
		rk := k.(metricKey)
		fmt.Fprintf(w, "%s_requests_total{method=%q,code=\"%d\"} %d\n", prefix, rk.method, rk.code, v.(*atomic.Uint64).Load())
		return true
	})
	fmt.Fprintf(w, "%s_request_duration_seconds_sum %.6f\n%s_request_duration_seconds_count %d\n%s_response_bytes_total %d\n",
		prefix, float64(m.duration.Load())/float64(time.Second), prefix, m.count.Load(), prefix, m.outBytes.Load())
}
