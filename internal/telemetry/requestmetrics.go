package telemetry

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var latencyBounds = [...]float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}

type requestKey struct {
	method string
	code   int
}

type RequestMetrics struct {
	requests sync.Map
	duration atomic.Uint64
	outBytes atomic.Uint64
	buckets  [len(latencyBounds) + 1]atomic.Uint64
}

func NewRequestMetrics() *RequestMetrics { return &RequestMetrics{} }

func (m *RequestMetrics) Observe(method string, code int, d time.Duration, out int64) {
	key := requestKey{method: method, code: code}
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

func (m *RequestMetrics) Write(w io.Writer, prefix string) {
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
	for _, key := range keys {
		fmt.Fprintf(w, "%s_requests_total{method=%q,code=%q} %d\n",
			prefix, key.method, strconv.Itoa(key.code), counts[key])
	}
	fmt.Fprintf(w, "%s_request_duration_seconds_sum %f\n", prefix,
		float64(m.duration.Load())/float64(time.Second))
	var total uint64
	for _, count := range counts {
		total += count
	}
	fmt.Fprintf(w, "%s_request_duration_seconds_count %d\n", prefix, total)
	var cumulative uint64
	for i, bound := range latencyBounds {
		cumulative += m.buckets[i].Load()
		fmt.Fprintf(w, "%s_request_duration_seconds_bucket{le=%q} %d\n",
			prefix, fmt.Sprintf("%g", bound/1000), cumulative)
	}
	cumulative += m.buckets[len(latencyBounds)].Load()
	fmt.Fprintf(w, "%s_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", prefix, cumulative)
	fmt.Fprintf(w, "%s_response_bytes_total %d\n", prefix, m.outBytes.Load())
}

type CommandMetrics struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
}

func NewCommandMetrics() *CommandMetrics {
	return &CommandMetrics{counters: map[string]*atomic.Int64{}}
}

func (m *CommandMetrics) Observe(command string, status int) {
	key := command + "|" + strconv.Itoa(status)
	m.mu.Lock()
	c, ok := m.counters[key]
	if !ok {
		c = &atomic.Int64{}
		m.counters[key] = c
	}
	m.mu.Unlock()
	c.Add(1)
}

func (m *CommandMetrics) Write(w io.Writer, prefix string) {
	m.mu.Lock()
	keys := make([]string, 0, len(m.counters))
	for k := range m.counters {
		keys = append(keys, k)
	}
	values := make([]int64, len(keys))
	for i, k := range keys {
		values[i] = m.counters[k].Load()
	}
	m.mu.Unlock()
	sort.Strings(keys)
	for i, k := range keys {
		command, status, _ := strings.Cut(k, "|")
		fmt.Fprintf(w, "%s_requests_total{command=%q,status=%q} %d\n", prefix, command, status, values[i])
	}
}
