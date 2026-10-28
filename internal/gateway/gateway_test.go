package gateway

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMuxListenerConnectionCapLivesUntilClose(t *testing.T) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := newMuxListener(base, 1)
	t.Cleanup(func() { _ = m.Close() })

	first := dialMux(t, m)
	writeMuxProbe(t, first)
	conn, err := m.Accept()
	if err != nil {
		t.Fatalf("accept first: %v", err)
	}
	defer conn.Close()

	second := dialMux(t, m)
	writeMuxProbe(t, second)
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	_, readErr := second.Read(make([]byte, 1))
	_ = second.Close()
	if readErr == nil {
		t.Fatal("second connection stayed open while the only slot was live")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close first: %v", err)
	}

	third := dialMux(t, m)
	defer third.Close()
	writeMuxProbe(t, third)
	accepted := make(chan net.Conn, 1)
	errs := make(chan error, 1)
	go func() {
		c, err := m.Accept()
		if err != nil {
			errs <- err
			return
		}
		accepted <- c
	}()
	select {
	case c := <-accepted:
		_ = c.Close()
	case err := <-errs:
		t.Fatalf("accept after release: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("connection slot was not released")
	}
}

func dialMux(t *testing.T, m *muxListener) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", m.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeMuxProbe(t *testing.T, c net.Conn) {
	t.Helper()
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
}

func TestConstrainAddsRequestAndResponseDelay(t *testing.T) {
	h := Constrain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}), Simulation{Latency: 5 * time.Millisecond})
	start := time.Now()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/bench/key", nil))
	if elapsed := time.Since(start); elapsed < 8*time.Millisecond {
		t.Fatalf("elapsed = %v, want both request and response delay", elapsed)
	}
	if rr.Code != http.StatusOK || rr.Body.String() != "ok" {
		t.Fatalf("response = %d %q", rr.Code, rr.Body.String())
	}
}

func TestConstrainRateLimitSpacesRequests(t *testing.T) {
	h := Constrain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), Simulation{RPS: 100})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bench/a", nil))
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bench/b", nil))
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("second request waited only %v", elapsed)
	}
}

func TestConstrainBypassesHealthAndMetrics(t *testing.T) {
	h := Constrain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), Simulation{Latency: 50 * time.Millisecond, RPS: 1})
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		start := time.Now()
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
		if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
			t.Fatalf("%s took %v", path, elapsed)
		}
	}
}

func TestConstrainStopsWhenContextCanceled(t *testing.T) {
	h := Constrain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), Simulation{Latency: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bench/key", nil).WithContext(ctx))
	if elapsed := time.Since(start); elapsed > 20*time.Millisecond {
		t.Fatalf("canceled request took %v", elapsed)
	}
}

func TestConstrainShapesResponseBandwidth(t *testing.T) {
	const payload = 64 << 10
	h := Constrain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(bytes.Repeat([]byte("x"), payload))
	}), Simulation{Bandwidth: 1 << 20})
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bench/key", nil))
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("response bandwidth delay = %v", elapsed)
	}
}

func TestConstrainShapesRequestBodyBandwidth(t *testing.T) {
	h := Constrain(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}), Simulation{Bandwidth: 1 << 20})
	req := httptest.NewRequest(http.MethodPut, "/bench/key", bytes.NewReader(bytes.Repeat([]byte("x"), 64<<10)))
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), req)
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("request bandwidth delay = %v", elapsed)
	}
}
