package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type sqsReply struct {
	Messages []struct {
		MessageId     string `json:"MessageId"`
		ReceiptHandle string `json:"ReceiptHandle"`
		Body          string `json:"Body"`
	} `json:"Messages"`
}

func sqsCall(c *http.Client, endpoint, target string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+"/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+target)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("sqs %s: HTTP %d: %s", target, resp.StatusCode, data)
	}
	if out != nil {
		return json.Unmarshal(data, out)
	}
	return nil
}

// runSQS benchmarks SQS send or receive+delete against one queue.
func runSQS(endpoint, queue, op string, size int64, workers, pipeline int, duration time.Duration) {
	c := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        4096,
			MaxIdleConnsPerHost: 4096,
			MaxConnsPerHost:     workers,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	queueURL := endpoint + "/" + queue
	if err := sqsCall(c, endpoint, "CreateQueue", map[string]any{"QueueName": queue}, nil); err != nil {
		fmt.Fprintln(os.Stderr, "create queue:", err)
		return
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
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, 4096)
			for time.Now().Before(deadline) {
				t0 := time.Now()
				if op == "sqs-send" {
					for p := 0; p < pipeline; p++ {
						if err := sqsCall(c, endpoint, "SendMessage", map[string]any{
							"QueueUrl": queueURL, "MessageBody": string(payload),
						}, nil); err != nil {
							atomic.AddInt64(&errs, 1)
							firstErr.CompareAndSwap(nil, err.Error())
							return
						}
						atomic.AddInt64(&msgs, 1)
						atomic.AddInt64(&bytesXfer, size)
					}
				} else {
					var reply sqsReply
					if err := sqsCall(c, endpoint, "ReceiveMessage", map[string]any{
						"QueueUrl": queueURL, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0,
					}, &reply); err != nil {
						atomic.AddInt64(&errs, 1)
						firstErr.CompareAndSwap(nil, err.Error())
						return
					}
					for _, m := range reply.Messages {
						_ = sqsCall(c, endpoint, "DeleteMessage", map[string]any{
							"QueueUrl": queueURL, "ReceiptHandle": m.ReceiptHandle,
						}, nil)
						atomic.AddInt64(&msgs, 1)
						atomic.AddInt64(&bytesXfer, int64(len(m.Body)))
					}
				}
				local = append(local, time.Since(t0)/time.Duration(max(pipeline, 1)))
			}
			latMu.Lock()
			latencies = append(latencies, local...)
			latMu.Unlock()
		}()
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
