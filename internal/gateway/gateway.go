package gateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/ojs"
	"github.com/adonm/pgvs3/internal/resp"
	"github.com/adonm/pgvs3/internal/s3"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/tidwall/redcon"
)

type Gateway struct {
	s3   *s3.Server
	ojs  *ojs.Server
	resp *resp.Server
}

type Limits struct {
	S3   int
	OJS  int
	RESP int
}

func New(st *store.Store, log *slog.Logger, readOnly bool, origins *objcache.Registry, lim Limits) *Gateway {
	g := &Gateway{
		s3:   s3.New(st, log, readOnly, lim.S3, origins),
		ojs:  ojs.New(st, log, readOnly, lim.OJS, "dev"),
		resp: resp.New(st, readOnly, lim.RESP),
	}
	g.s3.SetMetricsHook(func(w io.Writer) {
		g.resp.Metrics(w)
		g.ojs.Metrics(w)
	})
	return g
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ojs" || strings.HasPrefix(r.URL.Path, "/ojs/") {
		g.ojs.ServeHTTP(w, r)
		return
	}
	g.s3.ServeHTTP(w, r)
}

func (g *Gateway) SetDraining(v bool) { g.s3.SetDraining(v) }

func (g *Gateway) Close() { g.s3.Close() }

func (g *Gateway) SetVersion(v string) {
	g.resp.SetVersion(v)
	g.ojs.SetVersion(v)
}

func NewHTTPServer(addr string, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:                addr,
		Handler:             h,
		ReadHeaderTimeout:   10 * time.Second,
		ReadTimeout:         30 * time.Minute,
		WriteTimeout:        30 * time.Minute,
		IdleTimeout:         2 * time.Minute,
		MaxHeaderBytes:      1 << 20,
		MaxHeaderValueCount: 100,
		ErrorLog:            slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

func Run(ctx context.Context, srv *http.Server, g *Gateway, drain time.Duration, maxConns int, log *slog.Logger) error {
	raw, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	srv.BaseContext = func(net.Listener) context.Context { return baseCtx }
	ln := newMuxListener(raw, maxConns)
	g.resp.SetContext(baseCtx)
	rs := redcon.NewServer("", g.resp.ServeRESP, nil, nil)
	rs.SetIdleClose(2 * time.Minute)
	respLn := &respListener{m: ln}
	go func() {
		if err := rs.Serve(respLn); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warn("resp serve", "err", err)
		}
	}()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Info("serving", "addr", raw.Addr().String(), "protocols", "http,s3,ojs,resp2")
	select {
	case <-ctx.Done():
		if drain > 0 {
			time.Sleep(drain)
		}
		_ = rs.Close()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := srv.Shutdown(shutdown)
		cancel()
		if err != nil {
			baseCancel()
			_ = srv.Close()
		}
		return err
	case err := <-errCh:
		baseCancel()
		_ = rs.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type muxListener struct {
	net.Listener
	httpCh     chan net.Conn
	respCh     chan net.Conn
	sem        chan struct{}
	closed     chan struct{}
	httpClosed chan struct{}
	respClosed chan struct{}
	once       sync.Once
	httpOnce   sync.Once
	respOnce   sync.Once
}

func newMuxListener(base net.Listener, maxConns int) *muxListener {
	if maxConns <= 0 {
		maxConns = 4096
	}
	m := &muxListener{
		Listener:   base,
		httpCh:     make(chan net.Conn, 256),
		respCh:     make(chan net.Conn, 256),
		sem:        make(chan struct{}, maxConns),
		closed:     make(chan struct{}),
		httpClosed: make(chan struct{}),
		respClosed: make(chan struct{}),
	}
	go m.acceptLoop()
	return m
}

func (m *muxListener) acceptLoop() {
	for {
		c, err := m.Listener.Accept()
		if err != nil {
			_ = m.Close()
			return
		}
		select {
		case m.sem <- struct{}{}:
		default:

			_ = c.Close()
			continue
		}
		go func(conn net.Conn) {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			br := bufio.NewReaderSize(conn, 4096)
			head, err := br.Peek(5)
			_ = conn.SetReadDeadline(time.Time{})
			pc := &peekConn{Conn: conn, r: br, release: func() { <-m.sem }}
			if err == nil && isRESP(head) {
				select {
				case m.respCh <- pc:
				case <-m.respClosed:
					_ = pc.Close()
				}
				return
			}
			select {
			case m.httpCh <- pc:
			case <-m.httpClosed:
				_ = pc.Close()
			case <-m.closed:
				_ = pc.Close()
			}
		}(c)
	}
}

var (
	httpPrefixes = [][]byte{
		[]byte("GET "), []byte("PUT "), []byte("POST "), []byte("HEAD "),
		[]byte("DELET"), []byte("OPTIO"), []byte("PATCH"), []byte("CONNE"), []byte("TRACE"),
	}
	respPrefixes = [][]byte{
		[]byte("PING"), []byte("ECHO"), []byte("QUIT"), []byte("AUTH"),
		[]byte("INFO"), []byte("TIME"), []byte("SCAN"), []byte("KEYS"),
	}
)

func isRESP(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	switch b[0] {
	case '*', '+', '-', '$', ':':
		return true
	case 0x16:
		return false
	}
	for _, prefix := range httpPrefixes {
		if bytes.HasPrefix(b, prefix) {
			return false
		}
	}
	for _, prefix := range respPrefixes {
		if bytes.HasPrefix(b, prefix) {
			return true
		}
	}
	return false
}

func (m *muxListener) Accept() (net.Conn, error) {
	select {
	case c := <-m.httpCh:
		return c, nil
	case <-m.httpClosed:
		return nil, net.ErrClosed
	case <-m.closed:
		return nil, net.ErrClosed
	}
}

func (m *muxListener) AcceptRESP() (net.Conn, error) {
	select {
	case c := <-m.respCh:
		return c, nil
	case <-m.respClosed:
		return nil, net.ErrClosed
	case <-m.closed:
		return nil, net.ErrClosed
	}
}

func (m *muxListener) closeHTTP() {
	m.httpOnce.Do(func() {
		close(m.httpClosed)
		for {
			select {
			case c := <-m.httpCh:
				_ = c.Close()
			default:
				return
			}
		}
	})
}

func (m *muxListener) closeRESP() {
	m.respOnce.Do(func() {
		close(m.respClosed)
		for {
			select {
			case c := <-m.respCh:
				_ = c.Close()
			default:
				return
			}
		}
	})
}

func (m *muxListener) Close() error {
	var err error
	m.once.Do(func() {
		close(m.closed)
		m.closeHTTP()
		m.closeRESP()
		err = m.Listener.Close()
	})
	return err
}

type respListener struct{ m *muxListener }

func (l *respListener) Accept() (net.Conn, error) { return l.m.AcceptRESP() }

func (l *respListener) Close() error { l.m.closeRESP(); return nil }

func (l *respListener) Addr() net.Addr { return l.m.Listener.Addr() }

type peekConn struct {
	net.Conn
	r           *bufio.Reader
	release     func()
	releaseOnce sync.Once
}

func (p *peekConn) Read(b []byte) (int, error) { return p.r.Read(b) }

func (p *peekConn) Close() error {
	err := p.Conn.Close()
	if p.release != nil {
		p.releaseOnce.Do(p.release)
	}
	return err
}

var _ io.Reader = (*peekConn)(nil)

type Simulation struct {
	Latency   time.Duration
	RPS       int
	Bandwidth int64
}

func (s Simulation) Enabled() bool {
	return s.Latency > 0 || s.RPS > 0 || s.Bandwidth > 0
}

func Constrain(next http.Handler, sim Simulation) http.Handler {
	var limiter *rpsLimiter
	if sim.RPS > 0 {
		limiter = &rpsLimiter{interval: time.Second / time.Duration(sim.RPS)}
	}
	if !sim.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz", "/readyz", "/metrics":
			next.ServeHTTP(w, r)
			return
		}
		if limiter != nil {
			if err := limiter.wait(r.Context()); err != nil {
				return
			}
		}
		if sim.Latency > 0 {
			if err := sleepContext(r.Context(), sim.Latency); err != nil {
				return
			}
		}
		if sim.Bandwidth > 0 && r.Body != nil {
			r.Body = &bandwidthReader{ReadCloser: r.Body, rate: sim.Bandwidth, ctx: r.Context()}
		}
		var out http.ResponseWriter = &delayedWriter{ResponseWriter: w, delay: sim.Latency, ctx: r.Context()}
		if sim.Bandwidth > 0 {
			out = &bandwidthWriter{ResponseWriter: out, rate: sim.Bandwidth, ctx: r.Context()}
		}
		next.ServeHTTP(out, r)
	})
}

type rpsLimiter struct {
	mu       sync.Mutex
	next     time.Time
	interval time.Duration
}

func (l *rpsLimiter) wait(ctx context.Context) error {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	wait := l.next.Sub(now)
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	return sleepContext(ctx, wait)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type delayedWriter struct {
	http.ResponseWriter
	delay time.Duration
	ctx   context.Context
	once  sync.Once
}

func (w *delayedWriter) wait() {
	w.once.Do(func() {
		if w.delay > 0 {
			_ = sleepContext(w.ctx, w.delay)
		}
	})
}

func (w *delayedWriter) WriteHeader(code int) {
	w.wait()
	w.ResponseWriter.WriteHeader(code)
}

func (w *delayedWriter) Write(p []byte) (int, error) {
	w.wait()
	return w.ResponseWriter.Write(p)
}

func (w *delayedWriter) Flush() {
	w.wait()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *delayedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type bandwidthReader struct {
	io.ReadCloser
	rate int64
	ctx  context.Context
}

func (r *bandwidthReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 {
		if serr := sleepContext(r.ctx, bytesDuration(n, r.rate)); serr != nil {
			return n, serr
		}
	}
	return n, err
}

type bandwidthWriter struct {
	http.ResponseWriter
	rate int64
	ctx  context.Context
}

func (w *bandwidthWriter) Write(p []byte) (int, error) {
	if err := sleepContext(w.ctx, bytesDuration(len(p), w.rate)); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(p)
}

func (w *bandwidthWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *bandwidthWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func bytesDuration(n int, rate int64) time.Duration {
	if n <= 0 || rate <= 0 {
		return 0
	}
	return time.Duration(float64(n) / float64(rate) * float64(time.Second))
}
