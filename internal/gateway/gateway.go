package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/s3"
	"github.com/adonm/pgvs3/internal/store"
)

type Gateway struct {
	s3 *s3.Server
}

type Limits struct {
	S3          int
	S3QueueWait time.Duration
}

func New(st *store.Store, log *slog.Logger, readOnly bool, tier *objcache.Tier, lim Limits) *Gateway {
	return &Gateway{
		s3: s3.New(st, log, readOnly, lim.S3, lim.S3QueueWait, tier),
	}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.s3.ServeHTTP(w, r)
}

func (g *Gateway) SetDraining(v bool) { g.s3.SetDraining(v) }

func (g *Gateway) Close() { g.s3.Close() }

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
	ln := newLimitListener(raw, maxConns)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	log.Info("serving", "addr", raw.Addr().String(), "protocols", "http,s3")
	select {
	case <-ctx.Done():
		if drain > 0 {
			time.Sleep(drain)
		}
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
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

type limitListener struct {
	net.Listener
	sem chan struct{}
}

func newLimitListener(base net.Listener, maxConns int) *limitListener {
	if maxConns <= 0 {
		maxConns = 4096
	}
	return &limitListener{
		Listener: base,
		sem:      make(chan struct{}, maxConns),
	}
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.sem <- struct{}{}:
			return &limitConn{Conn: c, release: func() { <-l.sem }}, nil
		default:
			_ = c.Close()
		}
	}
}

type limitConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *limitConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}
