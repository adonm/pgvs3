package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

func newHTTPServer(addr string, h http.Handler, readTimeout, writeTimeout time.Duration, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:                addr,
		Handler:             h,
		ReadHeaderTimeout:   10 * time.Second,
		ReadTimeout:         readTimeout,
		WriteTimeout:        writeTimeout,
		IdleTimeout:         2 * time.Minute,
		MaxHeaderBytes:      1 << 20,
		MaxHeaderValueCount: 100,
		ErrorLog:            slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

func runHTTP(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration, maxConns int, log *slog.Logger) error {
	raw, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	return runHTTPListener(ctx, raw, srv, shutdownTimeout, maxConns, log)
}

func runHTTPListener(ctx context.Context, raw net.Listener, srv *http.Server, shutdownTimeout time.Duration, maxConns int, log *slog.Logger) error {
	baseCtx, baseCancel := context.WithCancel(ctx)
	defer baseCancel()
	srv.BaseContext = func(net.Listener) context.Context { return baseCtx }
	ln := newLimitListener(raw, maxConns)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	if log != nil {
		log.Info("serving", "addr", raw.Addr().String(), "protocols", "http,s3")
	}
	select {
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
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
