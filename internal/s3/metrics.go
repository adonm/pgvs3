package s3

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var latencyBoundsMs = [...]float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000}

type metricKey struct {
	method string
	code   int
}

type requestMetrics struct {
	requests sync.Map
	duration atomic.Uint64
	count    atomic.Uint64
	outBytes atomic.Uint64
	buckets  [len(latencyBoundsMs) + 1]atomic.Uint64
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
	ms := float64(d) / float64(time.Millisecond)
	idx := len(latencyBoundsMs)
	for i, bound := range latencyBoundsMs {
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

func (m *requestMetrics) Write(w io.Writer, prefix string) {
	keys := make([]metricKey, 0, 16)
	counts := make(map[metricKey]uint64, 16)
	m.requests.Range(func(k, v any) bool {
		rk := k.(metricKey)
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
	for _, k := range keys {
		fmt.Fprintf(w, "%s_requests_total{method=%q,code=%q} %d\n",
			prefix, k.method, strconv.Itoa(k.code), counts[k])
	}
	fmt.Fprintf(w, "%s_request_duration_seconds_sum %.6f\n%s_request_duration_seconds_count %d\n",
		prefix, float64(m.duration.Load())/float64(time.Second), prefix, m.count.Load())
	var cumulative uint64
	for i, bound := range latencyBoundsMs {
		cumulative += m.buckets[i].Load()
		fmt.Fprintf(w, "%s_request_duration_seconds_bucket{le=%q} %d\n",
			prefix, fmt.Sprintf("%g", bound/1000), cumulative)
	}
	cumulative += m.buckets[len(latencyBoundsMs)].Load()
	fmt.Fprintf(w, "%s_request_duration_seconds_bucket{le=\"+Inf\"} %d\n%s_response_bytes_total %d\n",
		prefix, cumulative, prefix, m.outBytes.Load())
}
