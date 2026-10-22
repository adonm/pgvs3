package resp

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

type metrics struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
}

func newMetrics() *metrics {
	return &metrics{counters: map[string]*atomic.Int64{}}
}

func (m *metrics) observe(command string, status int, _ time.Duration, _ int64) {
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

func (m *metrics) write(w io.Writer) {
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
		fmt.Fprintf(w, "pgvs3_resp_requests_total{command=%q,status=%q} %d\n", command, status, values[i])
	}
}
