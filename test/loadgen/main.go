package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/s3client"
)

type result struct {
	Op       string  `json:"op"`
	Workers  int     `json:"workers"`
	Pipeline int     `json:"pipeline,omitempty"`
	Size     int64   `json:"size"`
	Count    int     `json:"count"`
	Requests int64   `json:"requests"`
	Seconds  float64 `json:"seconds"`
	RPS      float64 `json:"requests_per_s"`
	MiBps    float64 `json:"mib_per_s"`
	P50      float64 `json:"p50_ms"`
	P90      float64 `json:"p90_ms"`
	P99      float64 `json:"p99_ms"`
	Max      float64 `json:"max_ms"`
	Errors   int64   `json:"errors"`
	FirstErr string  `json:"first_error,omitempty"`
}

type config struct {
	endpoint, bucket, access, secret, op, queue, prefix string
	size, rangeSize, rangeOffset, requests              int64
	workers, pipeline, count                            int
	duration                                            time.Duration
	httpTimeout                                         time.Duration
	warmup, prefill, unsigned                           bool
	uniqueReads                                         bool
	overwrite                                           bool
	payload                                             []byte
}

type plan struct {
	op                string
	workers, pipeline int
	size              int64
	count             int
	requests          int64
	duration          time.Duration
	keepGoing         bool
}

type worker struct {
	step    func() (ops, xfer int64, err error)
	release func()
}

type factory func(id int) (worker, error)

type ojsJob struct {
	ID         string `json:"id"`
	LeaseToken string `json:"lease_token"`
	Args       []any  `json:"args"`
}

type ojsReply struct {
	Jobs []ojsJob `json:"jobs"`
}

type streamReply struct {
	BatchID  int64             `json:"batch_id"`
	Messages []json.RawMessage `json:"messages"`
}

func main() {
	var c config
	var payloadFile string
	flag.StringVar(&c.endpoint, "endpoint", "http://127.0.0.1:18014", "gateway endpoint")
	flag.StringVar(&c.bucket, "bucket", "bench", "bucket")
	flag.StringVar(&c.access, "access", "testkey", "access key")
	flag.StringVar(&c.secret, "secret", "testsecret", "secret key")
	flag.StringVar(&c.op, "op", "get", "put, get, head, range, list, resp-set, resp-get, ojs-enqueue, ojs-fetch, ojs-cycle, ojs-stream-send, ojs-stream-receive or ojs-stream-cycle")
	flag.Int64Var(&c.size, "size", 4096, "object size for put/get")
	flag.Int64Var(&c.rangeSize, "range-size", 8192, "range length for -op range")
	flag.IntVar(&c.workers, "workers", 16, "concurrent workers")
	flag.IntVar(&c.pipeline, "pipeline", 1, "RESP pipeline depth for resp-set/resp-get")
	flag.DurationVar(&c.duration, "duration", 5*time.Second, "measurement duration")
	flag.DurationVar(&c.httpTimeout, "timeout", time.Minute, "S3 HTTP request timeout")
	flag.IntVar(&c.count, "count", 2000, "objects to prefill/read")
	flag.BoolVar(&c.warmup, "warmup", false, "read every key once before measuring")
	flag.BoolVar(&c.uniqueReads, "unique-reads", false, "share a read sequence across workers for distinct cold keys")
	flag.BoolVar(&c.overwrite, "overwrite", false, "reuse one prefilled object key per PUT worker")
	flag.BoolVar(&c.prefill, "prefill", true, "create objects before get/range/head/list benchmarks")
	flag.Int64Var(&c.requests, "requests", 0, "fixed measured request count (0 uses duration)")
	flag.BoolVar(&c.unsigned, "unsigned", false, "omit signing for trusted-network gateway comparisons")
	flag.StringVar(&payloadFile, "payload-file", "", "use file bytes for PUT/prefill instead of random data")
	flag.Int64Var(&c.rangeOffset, "range-offset", 0, "starting byte for -op range")
	flag.StringVar(&c.prefix, "prefix", "", "key prefix (a unique one is generated if empty)")
	flag.StringVar(&c.queue, "queue", "benchq", "OJS queue name for ojs-enqueue/ojs-fetch")
	flag.Parse()
	if c.requests < 0 || c.count <= 0 || c.workers <= 0 || c.size < 0 || c.rangeOffset < 0 || c.rangeSize <= 0 || c.httpTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "request count, object count, workers, size and range must be valid")
		os.Exit(2)
	}
	if payloadFile != "" {
		var err error
		c.payload, err = os.ReadFile(payloadFile)
		must(err)
		c.size = int64(len(c.payload))
	}
	if c.prefix == "" {
		c.prefix = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	c.pipeline = max(c.pipeline, 1)
	switch {
	case strings.HasPrefix(c.op, "ojs-stream-"):
		runStream(c)
	case strings.HasPrefix(c.op, "ojs-"):
		runOJS(c)
	case strings.HasPrefix(c.op, "resp-"):
		runRESP(c)
	default:
		runS3(c)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (c config) body() []byte {
	if c.payload != nil {
		return c.payload
	}
	b := make([]byte, c.size)
	_, _ = rand.Read(b)
	return b
}

func newClient(timeout time.Duration, workers int, noCompression bool) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{
		MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096, MaxConnsPerHost: workers,
		DisableCompression: noCompression, IdleConnTimeout: 90 * time.Second,
	}}
}

func iterate(step func(id, seq int) (int64, int64, error)) factory {
	return func(id int) (worker, error) {
		seq := 0
		return worker{step: func() (int64, int64, error) {
			seq++
			return step(id, seq)
		}}, nil
	}
}

func measure(p plan, newWorker factory) {
	var ops, xfer, errs, issued atomic.Int64
	var firstErr atomic.Value
	fail := func(err error) {
		errs.Add(1)
		firstErr.CompareAndSwap(nil, err.Error())
	}
	start := time.Now()
	deadline := start.Add(p.duration)
	more := func() bool {
		if p.requests > 0 {
			return issued.Add(1) <= p.requests
		}
		return time.Now().Before(deadline)
	}
	samples := make([][]time.Duration, p.workers)
	var wg sync.WaitGroup
	for id := range p.workers {
		wg.Go(func() {
			w, err := newWorker(id)
			if err != nil {
				fail(err)
				return
			}
			if w.release != nil {
				defer w.release()
			}
			local := make([]time.Duration, 0, 4096)
			defer func() { samples[id] = local }()
			for more() {
				t0 := time.Now()
				n, b, err := w.step()
				ops.Add(n)
				xfer.Add(b)
				if err != nil {
					fail(err)
					if !p.keepGoing {
						return
					}
					continue
				}
				local = append(local, time.Since(t0)/time.Duration(max(p.pipeline, 1)))
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	lat := slices.Concat(samples...)
	slices.Sort(lat)
	pick := func(q float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[int(q*float64(len(lat)-1))].Microseconds()) / 1000
	}
	out := result{
		Op: p.op, Workers: p.workers, Pipeline: p.pipeline, Size: p.size, Count: p.count,
		Requests: ops.Load(), Seconds: elapsed, RPS: float64(ops.Load()) / elapsed,
		MiBps: float64(xfer.Load()) / elapsed / (1 << 20),
		P50:   pick(.50), P90: pick(.90), P99: pick(.99), Max: pick(1),
		Errors: errs.Load(),
	}
	if v := firstErr.Load(); v != nil {
		out.FirstErr = v.(string)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

type s3run struct {
	config
	base   string
	client *http.Client
	sig    *s3client.Signer
}

func runS3(c config) {
	s := &s3run{config: c, base: strings.TrimRight(c.endpoint, "/"), client: newClient(c.httpTimeout, c.workers, true)}
	if !c.unsigned {
		s.sig = s3client.NewSigner("us-east-1", c.access, c.secret, "")
	}
	status, _, err := s.send(http.MethodPut, s.base+"/"+c.bucket, nil, "")
	must(err)
	if status >= 300 && status != http.StatusConflict {
		must(fmt.Errorf("HTTP %d", status))
	}
	if !slices.Contains([]string{"put", "get", "head", "range", "list"}, c.op) {
		fmt.Fprintln(os.Stderr, "unknown -op")
		os.Exit(2)
	}
	reads := c.op == "get" || c.op == "head" || c.op == "range"
	rng := ""
	if c.op == "range" {
		rng = fmt.Sprintf("bytes=%d-%d", c.rangeOffset, c.rangeOffset+c.rangeSize-1)
	}
	if c.prefill && reads {
		s.prefill(c.body())
	}
	if c.prefill && c.op == "list" {
		s.prefill([]byte("x"))
	}
	if c.warmup && reads {
		for i := range c.count {
			_, _, err := s.send(http.MethodGet, s.key(i), nil, rng)
			must(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	var newWorker factory
	switch c.op {
	case "put":
		newWorker = s.put(c.body())
	case "list":
		newWorker = s.list()
	case "head":
		newWorker = s.objects(http.MethodHead, "", http.StatusOK)
	case "range":
		newWorker = s.objects(http.MethodGet, rng, http.StatusPartialContent)
	default:
		newWorker = s.objects(http.MethodGet, "", http.StatusOK)
	}
	measure(plan{op: c.op, workers: c.workers, size: c.size, count: c.count, requests: c.requests, duration: c.duration, keepGoing: true}, newWorker)
}

func (s *s3run) key(i int) string {
	return fmt.Sprintf("%s/%s/%s/obj-%06d", s.base, s.bucket, s.prefix, i)
}

func (s *s3run) send(method, u string, body []byte, rng string) (int, int64, error) {
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return 0, 0, err
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	if s.sig != nil {
		if err := s.sig.Sign(req, body); err != nil {
			return 0, 0, err
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	got, err := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode, got, err
}

func (s *s3run) call(method, u string, body []byte, rng string, want int) (int64, int64, error) {
	status, got, err := s.send(method, u, body, rng)
	if err == nil && status != want {
		err = fmt.Errorf("HTTP %d", status)
	}
	if err != nil {
		return 0, 0, err
	}
	return 1, got, nil
}

func (s *s3run) prefill(payload []byte) {
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for i := range s.count {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			status, _, err := s.send(http.MethodPut, s.key(i), append([]byte(nil), payload...), "")
			if err != nil {
				fmt.Fprintln(os.Stderr, "prefill:", err)
				os.Exit(1)
			}
			if status != http.StatusOK {
				fmt.Fprintln(os.Stderr, "prefill HTTP", status)
				os.Exit(1)
			}
		})
	}
	wg.Wait()
}

func (s *s3run) objects(method, rng string, want int) factory {
	var next atomic.Int64
	return iterate(func(_, seq int) (int64, int64, error) {
		if s.uniqueReads {
			seq = int(next.Add(1) - 1)
		}
		return s.call(method, s.key(seq%s.count), nil, rng, want)
	})
}

func (s *s3run) put(body []byte) factory {
	return iterate(func(id, seq int) (int64, int64, error) {
		key := fmt.Sprintf("%s/%s/%s/w%d-%d", s.base, s.bucket, s.prefix, id, seq)
		if s.overwrite {
			key = s.key(id % s.count)
		}
		ops, _, err := s.call(http.MethodPut, key, body, "", http.StatusOK)
		return ops, ops * s.size, err
	})
}

func (s *s3run) list() factory {
	u := fmt.Sprintf("%s/%s?list-type=2&prefix=%s&max-keys=1000", s.base, s.bucket, url.QueryEscape(s.prefix))
	return iterate(func(int, int) (int64, int64, error) {
		return s.call(http.MethodGet, u, nil, "", http.StatusOK)
	})
}

func runRESP(c config) {
	u, err := url.Parse(c.endpoint)
	must(err)
	payload := bytes.Repeat([]byte("pgvs3-compressed-cache-block-"), int(c.size/29)+1)[:c.size]
	if c.op == "resp-get" {
		prefillRESP(u.Host, c, payload)
	}
	measure(plan{op: c.op, workers: c.workers, pipeline: c.pipeline, size: c.size, count: c.count, duration: c.duration}, respWorker(u.Host, c, payload))
}

func respWorker(addr string, c config, payload []byte) factory {
	set := c.op == "resp-set"
	return func(id int) (worker, error) {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return worker{}, err
		}
		br := bufio.NewReaderSize(conn, 64<<10)
		bw := bufio.NewWriterSize(conn, 64<<10)
		_, _ = respBatch(bw, br, 1, func(int) { respWrite(bw, []byte("SELECT"), []byte(c.bucket)) })
		var seq int64
		return worker{release: func() { _ = conn.Close() }, step: func() (int64, int64, error) {
			n, err := respBatch(bw, br, c.pipeline, func(int) {
				seq++
				if set {
					respWrite(bw, []byte("SET"), []byte(fmt.Sprintf("%s/w%d-%d", c.prefix, id, seq)), payload)
				} else {
					respWrite(bw, []byte("GET"), []byte(fmt.Sprintf("%s/obj-%06d", c.prefix, int(seq)%c.count)))
				}
			})
			if err != nil {
				return 0, 0, err
			}
			if set {
				n = int64(c.pipeline) * c.size
			}
			return int64(c.pipeline), n, nil
		}}, nil
	}
}

func prefillRESP(addr string, c config, payload []byte) {
	conn, err := net.Dial("tcp", addr)
	must(err)
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 64<<10)
	bw := bufio.NewWriterSize(conn, 64<<10)
	_, err = respBatch(bw, br, 1, func(int) { respWrite(bw, []byte("SELECT"), []byte(c.bucket)) })
	must(err)
	for i := 0; i < c.count; i += 32 {
		_, err := respBatch(bw, br, min(32, c.count-i), func(j int) {
			respWrite(bw, []byte("SET"), []byte(fmt.Sprintf("%s/obj-%06d", c.prefix, i+j)), payload)
		})
		must(err)
	}
}

func respBatch(bw *bufio.Writer, br *bufio.Reader, n int, write func(i int)) (int64, error) {
	for i := range n {
		write(i)
	}
	if err := bw.Flush(); err != nil {
		return 0, err
	}
	var total int64
	for range n {
		m, err := respRead(br)
		if err != nil {
			return 0, err
		}
		total += m
	}
	return total, nil
}

func respWrite(bw *bufio.Writer, args ...[]byte) {
	fmt.Fprintf(bw, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(bw, "$%d\r\n", len(a))
		_, _ = bw.Write(a)
		_, _ = bw.WriteString("\r\n")
	}
}

func respRead(br *bufio.Reader) (int64, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case '+':
		return 0, nil
	case '-':
		return 0, errors.New(strings.TrimSpace(line[1:]))
	case '$':
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || n < 0 {
			return 0, err
		}
		_, err = br.Discard(n + 2)
		return int64(n), err
	default:
		return 0, fmt.Errorf("unexpected RESP reply %q", line)
	}
}

type ojsRun struct {
	config
	client *http.Client
	data   string
}

func runOJS(c config) {
	o := &ojsRun{config: c, data: strings.Repeat("q", int(c.size)), client: newClient(30*time.Second, c.workers, false)}
	newWorker := o.fetch(c.op == "ojs-cycle")
	if c.op == "ojs-enqueue" {
		newWorker = o.enqueue()
	}
	measure(plan{op: c.op, workers: c.workers, pipeline: c.pipeline, size: c.size, duration: c.duration}, newWorker)
}

func (o *ojsRun) call(path string, payload, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, o.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/openjobspec+json")
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, data)
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

func (o *ojsRun) enqueue() factory {
	return iterate(func(int, int) (int64, int64, error) {
		var ops, xfer int64
		for range o.pipeline {
			if err := o.call("/ojs/v1/jobs", map[string]any{"type": "bench", "args": []any{o.data}, "options": map[string]any{"queue": o.queue}}, nil); err != nil {
				return ops, xfer, err
			}
			ops++
			xfer += o.size
		}
		return ops, xfer, nil
	})
}

func (o *ojsRun) fetch(cycle bool) factory {
	count := 10
	if cycle {
		count = 1
	}
	return iterate(func(id, _ int) (int64, int64, error) {
		if cycle {
			if err := o.call("/ojs/v1/jobs", map[string]any{"type": "bench", "args": []any{o.data}, "queue": o.queue}, nil); err != nil {
				return 0, 0, err
			}
		}
		var reply ojsReply
		if err := o.call("/ojs/v1/workers/fetch", map[string]any{"queues": []string{o.queue}, "count": count, "worker_id": fmt.Sprintf("loadgen-%d", id), "visibility_timeout_ms": 30000}, &reply); err != nil {
			return 0, 0, err
		}
		var ops, xfer int64
		for _, job := range reply.Jobs {
			if err := o.call("/ojs/v1/workers/ack", map[string]any{"job_id": job.ID, "lease_token": job.LeaseToken}, nil); err != nil {
				return ops, xfer, err
			}
			ops++
			for _, arg := range job.Args {
				if s, ok := arg.(string); ok {
					xfer += int64(len(s))
				}
			}
		}
		return ops, xfer, nil
	})
}

func runStream(c config) {
	o := &ojsRun{config: c, data: strings.Repeat("s", int(c.size)), client: &http.Client{Timeout: 10 * time.Second}}
	if c.op == "ojs-stream-receive" || c.op == "ojs-stream-cycle" {
		if err := o.call("/ojs/v1/queues/"+c.queue+"/consumers", map[string]any{"consumer": "loadgen", "from_tick": 0}, nil); err != nil {
			fmt.Fprintln(os.Stderr, "subscribe:", err)
		}
	}
	newWorker := o.streamReceive(c.op == "ojs-stream-cycle")
	if c.op == "ojs-stream-send" {
		newWorker = o.streamSend()
	}
	measure(plan{op: c.op, workers: c.workers, size: c.size, duration: c.duration}, newWorker)
}

func (o *ojsRun) produce() error {
	return o.call("/ojs/v1/queues/"+o.queue+"/messages", map[string]any{"type": "bench", "payload": map[string]any{"data": o.data}}, nil)
}

func (o *ojsRun) streamSend() factory {
	return iterate(func(int, int) (int64, int64, error) {
		if err := o.produce(); err != nil {
			return 0, 0, err
		}
		return 1, o.size, nil
	})
}

func (o *ojsRun) streamReceive(cycle bool) factory {
	return iterate(func(id, _ int) (int64, int64, error) {
		request := map[string]any{"consumer": "loadgen", "max": 100000, "wait_ms": 100}
		if cycle {
			request["wait_ms"] = 0
			if o.workers > 1 {
				request["member"] = fmt.Sprintf("member-%d", id)
			}
			if err := o.produce(); err != nil {
				return 0, 0, err
			}
		}
		var reply streamReply
		if err := o.call("/ojs/v1/queues/"+o.queue+"/receive", request, &reply); err != nil {
			return 0, 0, err
		}
		if reply.BatchID > 0 {
			if err := o.call("/ojs/v1/streams/ack", map[string]any{"queue": o.queue, "batch_id": reply.BatchID}, nil); err != nil {
				return 0, 0, err
			}
		}
		n := int64(len(reply.Messages))
		return n, n * o.size, nil
	})
}
