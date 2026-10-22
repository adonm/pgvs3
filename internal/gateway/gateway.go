// Package gateway wires the S3, SQS and RESP protocols onto one listener.
package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/resp"
	"github.com/adonm/pgvs3/internal/s3"
	"github.com/adonm/pgvs3/internal/sqs"
	"github.com/adonm/pgvs3/internal/store"
)

// Gateway dispatches HTTP requests to S3 or SQS and muxes RESP by first byte.
type Gateway struct {
	s3   *s3.Server
	sqs  *sqs.Server
	resp *resp.Server
}

func New(st *store.Store, log *slog.Logger, readOnly bool, origin *objcache.Tier) *Gateway {
	g := &Gateway{
		s3:   s3.New(st, log, readOnly),
		sqs:  sqs.New(st, log, readOnly),
		resp: resp.New(st, log, readOnly),
	}
	if origin != nil {
		g.s3.SetOrigin(origin)
	}
	g.s3.SetMetricsHook(g.resp.Metrics)
	return g
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if sqs.IsSQS(r) {
		g.sqs.ServeHTTP(w, r)
		return
	}
	g.s3.ServeHTTP(w, r)
}

func (g *Gateway) SetDraining(v bool) { g.s3.SetDraining(v) }

// NewHTTPServer returns the shared HTTP server.
func NewHTTPServer(addr string, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

// Run serves HTTP, S3, SQS and RESP on one port until ctx is cancelled.
func Run(ctx context.Context, srv *http.Server, g *Gateway, drain time.Duration, maxConns int, log *slog.Logger) error {
	raw, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	ln := newMuxListener(ctx, raw, g.resp, maxConns, log)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Info("serving", "addr", raw.Addr().String(), "protocols", "http,s3,sqs,resp")
	select {
	case <-ctx.Done():
		if drain > 0 {
			time.Sleep(drain)
		}
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type muxListener struct {
	net.Listener
	httpCh chan net.Conn
	ctx    context.Context
	resp   *resp.Server
	sem    chan struct{}
	log    *slog.Logger
	closed chan struct{}
	once   sync.Once
}

func newMuxListener(ctx context.Context, base net.Listener, r *resp.Server, maxConns int, log *slog.Logger) *muxListener {
	if maxConns <= 0 {
		maxConns = 4096
	}
	m := &muxListener{
		Listener: base,
		httpCh:   make(chan net.Conn, 256),
		ctx:      ctx,
		resp:     r,
		sem:      make(chan struct{}, maxConns),
		log:      log,
		closed:   make(chan struct{}),
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
			// Connection cap hit: shed load rather than grow goroutines.
			_ = c.Close()
			continue
		}
		go func(conn net.Conn) {
			defer func() { <-m.sem }()
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			br := bufio.NewReaderSize(conn, 4096)
			head, err := br.Peek(5)
			_ = conn.SetReadDeadline(time.Time{})
			pc := &peekConn{Conn: conn, r: br}
			if err == nil && isRESP(head) {
				m.resp.ServeConn(m.ctx, pc)
				return
			}
			select {
			case m.httpCh <- pc:
			case <-m.closed:
				_ = pc.Close()
			}
		}(c)
	}
}

// isRESP classifies the first bytes. RESP arrays start with '*'; inline
// commands start with an uppercase verb that is not an HTTP method. Anything
// else (including a TLS ClientHello) goes to the HTTP server so failures are
// reported as HTTP rather than protocol garbage.
func isRESP(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	switch b[0] {
	case '*', '+', '-', '$', ':', '_':
		return true
	case 0x16: // TLS handshake
		return false
	}
	for _, m := range []string{"GET ", "PUT ", "POST ", "HEAD ", "DELET", "OPTIO", "PATCH", "CONNE", "TRACE"} {
		if len(b) >= len(m) && string(b[:len(m)]) == m {
			return false
		}
	}
	if len(b) >= 4 {
		switch string(b[:4]) {
		case "PING", "ECHO", "QUIT", "AUTH", "INFO", "TIME", "SCAN", "KEYS":
			return true
		}
	}
	return false
}

func (m *muxListener) Accept() (net.Conn, error) {
	select {
	case c := <-m.httpCh:
		return c, nil
	case <-m.closed:
		return nil, net.ErrClosed
	}
}

func (m *muxListener) Close() error {
	var err error
	m.once.Do(func() {
		close(m.closed)
		err = m.Listener.Close()
	})
	return err
}

// peekConn serves the buffered prefix before the raw connection.
type peekConn struct {
	net.Conn
	r *bufio.Reader
}

func (p *peekConn) Read(b []byte) (int, error) { return p.r.Read(b) }

var _ io.Reader = (*peekConn)(nil)
