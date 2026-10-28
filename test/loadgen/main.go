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

type signer struct {
	client   *s3client.Signer
	unsigned bool
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:18014", "gateway endpoint")
	bucket := flag.String("bucket", "bench", "bucket")
	access := flag.String("access", "testkey", "access key")
	secret := flag.String("secret", "testsecret", "secret key")
	op := flag.String("op", "get", "put, get, head, range, list, resp-set, resp-get, ojs-enqueue, ojs-fetch, ojs-cycle, ojs-stream-send, ojs-stream-receive or ojs-stream-cycle")
	size := flag.Int64("size", 4096, "object size for put/get")
	rangeSize := flag.Int64("range-size", 8192, "range length for -op range")
	workers := flag.Int("workers", 16, "concurrent workers")
	pipeline := flag.Int("pipeline", 1, "RESP pipeline depth for resp-set/resp-get")
	duration := flag.Duration("duration", 5*time.Second, "measurement duration")
	count := flag.Int("count", 2000, "objects to prefill/read")
	warmup := flag.Bool("warmup", false, "read every key once before measuring")
	prefillObjects := flag.Bool("prefill", true, "create objects before get/range/head/list benchmarks")
	requests := flag.Int64("requests", 0, "fixed measured request count (0 uses duration)")
	unsigned := flag.Bool("unsigned", false, "omit signing for trusted-network gateway comparisons")
	payloadFile := flag.String("payload-file", "", "use file bytes for PUT/prefill instead of random data")
	rangeOffset := flag.Int64("range-offset", 0, "starting byte for -op range")
	prefix := flag.String("prefix", "", "key prefix (a unique one is generated if empty)")
	queue := flag.String("queue", "benchq", "OJS queue name for ojs-enqueue/ojs-fetch")
	flag.Parse()
	if *requests < 0 || *count <= 0 || *workers <= 0 || *size < 0 || *rangeOffset < 0 || *rangeSize <= 0 {
		fmt.Fprintln(os.Stderr, "request count, object count, workers, size and range must be valid")
		os.Exit(2)
	}
	var filePayload []byte
	if *payloadFile != "" {
		var err error
		filePayload, err = os.ReadFile(*payloadFile)
		must(err)
		*size = int64(len(filePayload))
	}
	if *prefix == "" {
		*prefix = fmt.Sprintf("bench-%d", time.Now().UnixNano())
	}
	if strings.HasPrefix(*op, "ojs-stream-") {
		runStream(*endpoint, *queue, *op, *size, *workers, *duration)
		return
	}
	if strings.HasPrefix(*op, "ojs-") {
		runOJS(*endpoint, *queue, *op, *size, *workers, max(*pipeline, 1), *duration)
		return
	}
	if strings.HasPrefix(*op, "resp-") {
		runRESP(*endpoint, *bucket, *op, *size, *workers, max(*pipeline, 1), *duration, *count, *prefix)
		return
	}
	c := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4096,
			MaxIdleConnsPerHost: 4096,
			MaxConnsPerHost:     *workers,
			DisableCompression:  true,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	s := &signer{unsigned: *unsigned}
	if !*unsigned {
		s.client = s3client.NewSigner("us-east-1", *access, *secret, "")
	}
	base := strings.TrimRight(*endpoint, "/")

	if err := s.do(c, base+"/"+*bucket, http.MethodPut, nil); err != nil && !strings.Contains(err.Error(), "HTTP 409") {
		must(err)
	}

	switch *op {
	case "put":
	case "get", "range", "head":
		if *prefillObjects {
			payload := filePayload
			if payload == nil {
				payload = make([]byte, *size)
				_, _ = rand.Read(payload)
			}
			prefill(c, s, base, *bucket, *prefix, *count, payload, 16)
		}
	case "list":
		if *prefillObjects {
			payload := []byte("x")
			prefill(c, s, base, *bucket, *prefix, *count, payload, 16)
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown -op")
		os.Exit(2)
	}
	if *warmup && (*op == "get" || *op == "range" || *op == "head") {
		for i := 0; i < *count; i++ {
			u := fmt.Sprintf("%s/%s/%s/obj-%06d", base, *bucket, *prefix, i)
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			if *op == "range" {
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", *rangeOffset, *rangeOffset+*rangeSize-1))
			}
			if err := s.sign(req, nil); err != nil {
				must(err)
			}
			resp, err := c.Do(req)
			if err != nil {
				must(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}
	time.Sleep(300 * time.Millisecond)

	var putBody []byte
	if *op == "put" {
		if filePayload != nil {
			putBody = filePayload
		} else {
			putBody = make([]byte, *size)
			_, _ = rand.Read(putBody)
		}
	}

	var (
		reqs, errs, bytesRead int64
		firstErr              atomic.Value
		start                 = time.Now()
		deadline              = start.Add(*duration)
		wg                    sync.WaitGroup
		latMu                 sync.Mutex
		latencies             []time.Duration
		issued                atomic.Int64
	)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			local := make([]time.Duration, 0, 4096)
			buf := make([]byte, 32<<10)
			var seq int64
			for *requests > 0 || time.Now().Before(deadline) {
				if *requests > 0 && issued.Add(1) > *requests {
					break
				}
				seq++
				var (
					method = http.MethodGet
					urlStr = fmt.Sprintf("%s/%s/%s/obj-%06d", base, *bucket, *prefix, int(seq)%*count)
					header http.Header
					body   []byte
					want   = http.StatusOK
				)
				switch *op {
				case "head":
					method = http.MethodHead
				case "put":
					method = http.MethodPut
					urlStr = fmt.Sprintf("%s/%s/%s/w%d-%d", base, *bucket, *prefix, id, seq)
					body = putBody
					want = http.StatusOK
				case "range":
					header = http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", *rangeOffset, *rangeOffset+*rangeSize-1)}}
					want = http.StatusPartialContent
				case "list":
					urlStr = fmt.Sprintf("%s/%s?list-type=2&prefix=%s&max-keys=1000", base, *bucket, url.QueryEscape(*prefix))
				}
				t0 := time.Now()
				req, err := http.NewRequest(method, urlStr, bytes.NewReader(body))
				if err != nil {
					atomic.AddInt64(&errs, 1)
					continue
				}
				if header != nil {
					req.Header = header
				}
				if err := s.sign(req, body); err != nil {
					atomic.AddInt64(&errs, 1)
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}
				resp, err := c.Do(req)
				if err != nil {
					atomic.AddInt64(&errs, 1)
					firstErr.CompareAndSwap(nil, err.Error())
					continue
				}

				got, readErr := io.CopyBuffer(io.Discard, resp.Body, buf)
				_ = resp.Body.Close()
				if readErr != nil {
					atomic.AddInt64(&errs, 1)
					firstErr.CompareAndSwap(nil, readErr.Error())
					continue
				}
				if resp.StatusCode != want {
					atomic.AddInt64(&errs, 1)
					firstErr.CompareAndSwap(nil, fmt.Sprintf("HTTP %d", resp.StatusCode))
					continue
				}
				local = append(local, time.Since(t0))
				atomic.AddInt64(&reqs, 1)
				if *op == "put" {
					atomic.AddInt64(&bytesRead, *size)
				} else if *op != "head" {
					atomic.AddInt64(&bytesRead, got)
				}
			}
			latMu.Lock()
			latencies = append(latencies, local...)
			latMu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	slices.Sort(latencies)
	pick := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		i := int(q * float64(len(latencies)-1))
		return float64(latencies[i].Microseconds()) / 1000
	}
	out := result{
		Op: *op, Workers: *workers, Size: *size, Count: *count, Requests: reqs,
		Seconds: elapsed, RPS: float64(reqs) / elapsed,
		MiBps: float64(bytesRead) / elapsed / (1 << 20),
		P50:   pick(.50), P90: pick(.90), P99: pick(.99), Max: pick(1),
		Errors: errs,
	}
	if v := firstErr.Load(); v != nil {
		out.FirstErr = v.(string)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func prefill(c *http.Client, s *signer, base, bucket, prefix string, count int, payload []byte, workers int) {
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			u := fmt.Sprintf("%s/%s/%s/obj-%06d", base, bucket, prefix, i)
			body := append([]byte(nil), payload...)
			req, _ := http.NewRequest(http.MethodPut, u, bytes.NewReader(body))
			if err := s.sign(req, body); err != nil {
				fmt.Fprintln(os.Stderr, "prefill:", err)
				os.Exit(1)
			}
			resp, err := c.Do(req)
			if err != nil {
				fmt.Fprintln(os.Stderr, "prefill:", err)
				os.Exit(1)
			}
			_, _ = resp.Body.Read(make([]byte, 0))
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				fmt.Fprintln(os.Stderr, "prefill HTTP", resp.StatusCode)
				os.Exit(1)
			}
		}(i)
	}
	wg.Wait()
}

func (s *signer) do(c *http.Client, u string, method string, body []byte) error {
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if err := s.sign(req, body); err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	_, _ = resp.Body.Read(make([]byte, 0))
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return nil
}

func (s *signer) sign(req *http.Request, body []byte) error {
	if s.unsigned {
		return nil
	}
	return s.client.Sign(req, body)
}

func runRESP(endpoint, bucket, op string, size int64, workers, pipeline int, duration time.Duration, count int, prefix string) {
	u, err := url.Parse(endpoint)
	must(err)
	addr := u.Host
	payload := bytes.Repeat([]byte("pgvs3-compressed-cache-block-"), int(size/29)+1)[:size]

	writeCmd := func(bw *bufio.Writer, args ...[]byte) {
		fmt.Fprintf(bw, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(bw, "$%d\r\n", len(a))
			_, _ = bw.Write(a)
			_, _ = bw.WriteString("\r\n")
		}
	}
	readReply := func(br *bufio.Reader) (int64, error) {
		line, err := br.ReadString('\n')
		if err != nil {
			return 0, err
		}
		if len(line) == 0 {
			return 0, io.ErrUnexpectedEOF
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
			if _, err := io.CopyN(io.Discard, br, int64(n)+2); err != nil {
				return 0, err
			}
			return int64(n), nil
		default:
			return 0, fmt.Errorf("unexpected RESP reply %q", line)
		}
	}

	if op == "resp-get" {
		conn, err := net.Dial("tcp", addr)
		must(err)
		br := bufio.NewReaderSize(conn, 64<<10)
		bw := bufio.NewWriterSize(conn, 64<<10)
		writeCmd(bw, []byte("SELECT"), []byte(bucket))
		must(bw.Flush())
		_, err = readReply(br)
		must(err)
		for i := 0; i < count; i += 32 {
			end := min(i+32, count)
			for j := i; j < end; j++ {
				k := fmt.Sprintf("%s/obj-%06d", prefix, j)
				writeCmd(bw, []byte("SET"), []byte(k), payload)
			}
			must(bw.Flush())
			for j := i; j < end; j++ {
				_, err := readReply(br)
				must(err)
			}
		}
		_ = conn.Close()
	}

	var (
		reqs, errs, bytesXfer int64
		firstErr              atomic.Value
		start                 = time.Now()
		deadline              = start.Add(duration)
		wg                    sync.WaitGroup
		latMu                 sync.Mutex
		latencies             []time.Duration
	)
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				atomic.AddInt64(&errs, 1)
				firstErr.CompareAndSwap(nil, err.Error())
				return
			}
			defer conn.Close()
			br := bufio.NewReaderSize(conn, 64<<10)
			bw := bufio.NewWriterSize(conn, 64<<10)
			writeCmd(bw, []byte("SELECT"), []byte(bucket))
			_ = bw.Flush()
			_, _ = readReply(br)

			local := make([]time.Duration, 0, 4096)
			var seq int64
			for time.Now().Before(deadline) {
				t0 := time.Now()
				for range pipeline {
					seq++
					if op == "resp-set" {
						k := fmt.Sprintf("%s/w%d-%d", prefix, id, seq)
						writeCmd(bw, []byte("SET"), []byte(k), payload)
					} else {
						k := fmt.Sprintf("%s/obj-%06d", prefix, int(seq)%count)
						writeCmd(bw, []byte("GET"), []byte(k))
					}
				}
				if err := bw.Flush(); err != nil {
					atomic.AddInt64(&errs, 1)
					firstErr.CompareAndSwap(nil, err.Error())
					return
				}
				var batchBytes int64
				failed := false
				for range pipeline {
					n, err := readReply(br)
					if err != nil {
						atomic.AddInt64(&errs, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						failed = true
						break
					}
					if op == "resp-set" {
						batchBytes += size
					} else {
						batchBytes += n
					}
				}
				if failed {
					return
				}
				perOp := time.Since(t0) / time.Duration(pipeline)
				local = append(local, perOp)
				atomic.AddInt64(&reqs, int64(pipeline))
				atomic.AddInt64(&bytesXfer, batchBytes)
			}
			latMu.Lock()
			latencies = append(latencies, local...)
			latMu.Unlock()
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	slices.Sort(latencies)
	pick := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		i := int(q * float64(len(latencies)-1))
		return float64(latencies[i].Microseconds()) / 1000
	}
	out := result{
		Op: op, Workers: workers, Pipeline: pipeline, Size: size, Count: count,
		Requests: reqs, Seconds: elapsed, RPS: float64(reqs) / elapsed,
		MiBps: float64(bytesXfer) / elapsed / (1 << 20),
		P50:   pick(.50), P90: pick(.90), P99: pick(.99), Max: pick(1),
		Errors: errs,
	}
	if v := firstErr.Load(); v != nil {
		out.FirstErr = v.(string)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

type ojsJob struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	LeaseToken string `json:"lease_token"`
	Args       []any  `json:"args"`
}

type ojsReply struct {
	Jobs []ojsJob `json:"jobs"`
}

func ojsCall(c *http.Client, endpoint, path string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/openjobspec+json")
	resp, err := c.Do(req)
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

func runOJS(endpoint, queue, op string, size int64, workers, pipeline int, duration time.Duration) {
	c := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4096,
			MaxIdleConnsPerHost: 4096,
			MaxConnsPerHost:     workers,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	payload := bytes.Repeat([]byte("q"), int(size))

	var (
		msgs, errs, bytesXfer int64
		firstErr              atomic.Value
		start                 = time.Now()
		deadline              = start.Add(duration)
		wg                    sync.WaitGroup
		latMu                 sync.Mutex
		latencies             []time.Duration
	)
	for w := range workers {
		wg.Go(func() {
			local := make([]time.Duration, 0, 4096)
			workerID := fmt.Sprintf("loadgen-%d", w)
			for time.Now().Before(deadline) {
				t0 := time.Now()
				if op == "ojs-enqueue" {
					for range pipeline {
						if err := ojsCall(c, endpoint, "/ojs/v1/jobs", map[string]any{
							"type": "bench", "args": []any{string(payload)},
							"options": map[string]any{"queue": queue},
						}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
						atomic.AddInt64(&msgs, 1)
						atomic.AddInt64(&bytesXfer, size)
					}
				} else {
					count := 10
					if op == "ojs-cycle" {
						count = 1
						if err := ojsCall(c, endpoint, "/ojs/v1/jobs", map[string]any{
							"type": "bench", "args": []any{string(payload)}, "queue": queue,
						}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
					}
					var reply ojsReply
					if err := ojsCall(c, endpoint, "/ojs/v1/workers/fetch", map[string]any{
						"queues": []string{queue}, "count": count, "worker_id": workerID,
						"visibility_timeout_ms": 30000,
					}, &reply); err != nil {
						atomic.AddInt64(&errs, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
					for _, job := range reply.Jobs {
						if err := ojsCall(c, endpoint, "/ojs/v1/workers/ack", map[string]any{
							"job_id": job.ID, "lease_token": job.LeaseToken,
						}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
						atomic.AddInt64(&msgs, 1)
						for _, arg := range job.Args {
							if s, ok := arg.(string); ok {
								atomic.AddInt64(&bytesXfer, int64(len(s)))
							}
						}
					}
				}
				local = append(local, time.Since(t0)/time.Duration(max(pipeline, 1)))
			}
			latMu.Lock()
			latencies = append(latencies, local...)
			latMu.Unlock()
		})
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	slices.Sort(latencies)
	pick := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		i := int(q * float64(len(latencies)-1))
		return float64(latencies[i].Microseconds()) / 1000
	}
	out := result{
		Op: op, Workers: workers, Pipeline: pipeline, Size: size,
		Requests: msgs, Seconds: elapsed, RPS: float64(msgs) / elapsed,
		MiBps: float64(bytesXfer) / elapsed / (1 << 20),
		P50:   pick(.50), P90: pick(.90), P99: pick(.99), Max: pick(1),
		Errors: errs,
	}
	if v := firstErr.Load(); v != nil {
		out.FirstErr = v.(string)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

type streamReply struct {
	BatchID  int64             `json:"batch_id"`
	Messages []json.RawMessage `json:"messages"`
}

func runStream(endpoint, queue, op string, size int64, workers int, duration time.Duration) {
	c := &http.Client{Timeout: 10 * time.Second}
	payload := bytes.Repeat([]byte("s"), int(size))
	if op == "ojs-stream-receive" || op == "ojs-stream-cycle" {
		if err := ojsCall(c, endpoint, "/ojs/v1/queues/"+queue+"/consumers",
			map[string]any{"consumer": "loadgen", "from_tick": 0}, nil); err != nil {
			fmt.Fprintln(os.Stderr, "subscribe:", err)
		}
	}
	var (
		msgs, errs, bytesXfer int64
		firstErr              atomic.Value
		start                 = time.Now()
		deadline              = start.Add(duration)
		wg                    sync.WaitGroup
		latMu                 sync.Mutex
		latencies             []time.Duration
	)
	for worker := range workers {
		wg.Go(func() {
			local := make([]time.Duration, 0, 4096)
			for time.Now().Before(deadline) {
				t0 := time.Now()
				if op == "ojs-stream-send" {
					if err := ojsCall(c, endpoint, "/ojs/v1/queues/"+queue+"/messages",
						map[string]any{"type": "bench", "payload": map[string]any{"data": string(payload)}}, nil); err != nil {
						atomic.AddInt64(&errs, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
					atomic.AddInt64(&msgs, 1)
					atomic.AddInt64(&bytesXfer, size)
				} else {
					request := map[string]any{"consumer": "loadgen", "max": 100000, "wait_ms": 100}
					if op == "ojs-stream-cycle" {
						request["wait_ms"] = 0
						if workers > 1 {
							request["member"] = fmt.Sprintf("member-%d", worker)
						}
						if err := ojsCall(c, endpoint, "/ojs/v1/queues/"+queue+"/messages",
							map[string]any{"type": "bench", "payload": map[string]any{"data": string(payload)}}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
					}
					var reply streamReply
					if err := ojsCall(c, endpoint, "/ojs/v1/queues/"+queue+"/receive",
						request, &reply); err != nil {
						atomic.AddInt64(&errs, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
					if reply.BatchID > 0 {
						if err := ojsCall(c, endpoint, "/ojs/v1/streams/ack",
							map[string]any{"queue": queue, "batch_id": reply.BatchID}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
					}
					atomic.AddInt64(&msgs, int64(len(reply.Messages)))
					for range reply.Messages {
						atomic.AddInt64(&bytesXfer, size)
					}
				}
				local = append(local, time.Since(t0))
			}
			latMu.Lock()
			latencies = append(latencies, local...)
			latMu.Unlock()
		})
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	slices.Sort(latencies)
	pick := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return float64(latencies[int(q*float64(len(latencies)-1))].Microseconds()) / 1000
	}
	out := result{Op: op, Workers: workers, Size: size, Requests: msgs, Seconds: elapsed,
		RPS: float64(msgs) / elapsed, MiBps: float64(bytesXfer) / elapsed / (1 << 20),
		P50: pick(.50), P90: pick(.90), P99: pick(.99), Max: pick(1), Errors: errs}
	if v := firstErr.Load(); v != nil {
		out.FirstErr = v.(string)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}
