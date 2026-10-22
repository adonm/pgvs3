// Command loadgen is a small SigV4 S3 load generator used by `just load`.
// It is a benchmark tool, not part of the gateway image.
package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	Op       string  `json:"op"`
	Workers  int     `json:"workers"`
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
	access, secret, region string
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
	op := flag.String("op", "get", "put, get, range or list")
	size := flag.Int64("size", 4096, "object size for put/get")
	rangeSize := flag.Int64("range-size", 8192, "range length for -op range")
	workers := flag.Int("workers", 16, "concurrent workers")
	duration := flag.Duration("duration", 5*time.Second, "measurement duration")
	count := flag.Int("count", 2000, "objects to prefill/read")
	warmup := flag.Bool("warmup", false, "read every key once before measuring")
	prefix := flag.String("prefix", "", "key prefix (a unique one is generated if empty)")
	flag.Parse()
	if *prefix == "" {
		*prefix = fmt.Sprintf("bench-%d", time.Now().UnixNano())
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
	s := &signer{access: *access, secret: *secret, region: "us-east-1"}
	base := strings.TrimRight(*endpoint, "/")
	must(s.do(c, base+"/"+*bucket, http.MethodPut, nil))

	switch *op {
	case "put":
	case "get", "range", "head":
		payload := make([]byte, *size)
		_, _ = rand.Read(payload)
		prefill(c, s, base, *bucket, *prefix, *count, payload, 16)
	case "list":
		payload := []byte("x")
		prefill(c, s, base, *bucket, *prefix, *count, payload, 16)
	default:
		fmt.Fprintln(os.Stderr, "unknown -op")
		os.Exit(2)
	}
	if *warmup && (*op == "get" || *op == "range" || *op == "head") {
		for i := 0; i < *count; i++ {
			u := fmt.Sprintf("%s/%s/%s/obj-%06d", base, *bucket, *prefix, i)
			req, _ := http.NewRequest(http.MethodGet, u, nil)
			if *op == "range" {
				req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", *rangeSize-1))
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

	var (
		reqs, errs, bytesRead int64
		firstErr              atomic.Value
		start                 = time.Now()
		deadline              = start.Add(*duration)
		wg                    sync.WaitGroup
		latMu                 sync.Mutex
		latencies             []time.Duration
	)
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			local := make([]time.Duration, 0, 4096)
			var seq int64
			for time.Now().Before(deadline) {
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
					body = make([]byte, *size)
					_, _ = rand.Read(body)
					want = http.StatusOK
				case "range":
					header = http.Header{"Range": []string{fmt.Sprintf("bytes=0-%d", *rangeSize-1)}}
					want = http.StatusPartialContent
				case "list":
					urlStr = fmt.Sprintf("%s/%s?list-type=2&prefix=%s&max-keys=1000", base, *bucket, url.QueryEscape(*prefix))
				}
				t0 := time.Now()
				req, err := http.NewRequest(method, urlStr, bytes.NewReader(body))
				if err != nil {
					errs++
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
				// Drain so the connection is reusable.
				buf := make([]byte, 32<<10)
				var got int64
				for {
					m, e := resp.Body.Read(buf)
					got += int64(m)
					if e != nil {
						break
					}
				}
				_ = resp.Body.Close()
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
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
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
	for i := 0; i < count; i++ {
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
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	payloadHash := "UNSIGNED-PAYLOAD"
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	headers := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	_ = body
	uri := req.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	canonical := strings.Join([]string{
		req.Method, uri, canonicalQuery(req.URL.RawQuery), headers,
		"host;x-amz-content-sha256;x-amz-date", payloadHash,
	}, "\n")
	scope := date + "/" + s.region + "/s3/aws4_request"
	toSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hexSHA256([]byte(canonical))
	key := signingKey(s.secret, date, s.region, "s3")
	m := hmac.New(sha256.New, key)
	m.Write([]byte(toSign))
	sig := hex.EncodeToString(m.Sum(nil))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.access+"/"+scope+
		", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="+sig)
	return nil
}

func signingKey(secret, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func hexSHA256(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func canonicalQuery(raw string) string {
	if raw == "" {
		return ""
	}
	type pair struct{ k, v string }
	var pairs []pair
	for _, part := range strings.Split(raw, "&") {
		if part == "" {
			continue
		}
		k, v, _ := strings.Cut(part, "=")
		dk, _ := url.QueryUnescape(k)
		dv, _ := url.QueryUnescape(v)
		pairs = append(pairs, pair{awsEncode(dk), awsEncode(dv)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	var b strings.Builder
	for i, p := range pairs {
		if i > 0 {
			b.WriteByte('&')
		}
		b.WriteString(p.k)
		b.WriteByte('=')
		b.WriteString(p.v)
	}
	return b.String()
}

func awsEncode(s string) string {
	const h = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '.', c == '_', c == '~':
			b.WriteByte(c)
		default:
			b.WriteByte('%')
			b.WriteByte(h[c>>4])
			b.WriteByte(h[c&15])
		}
	}
	return b.String()
}
