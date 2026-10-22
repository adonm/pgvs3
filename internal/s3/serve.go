package s3

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// NewHTTPServer returns a configured HTTP server for S3 or web traffic.
func NewHTTPServer(addr string, handler http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

// Run starts the server and shuts it down when ctx is cancelled.
func Run(ctx context.Context, srv *http.Server, drain time.Duration, log *slog.Logger) error {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	log.Info("serving", "addr", ln.Addr().String())
	select {
	case <-ctx.Done():
		if drain > 0 {
			time.Sleep(drain)
		}
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdown)
	case err := <-errCh:
		if err == http.ErrServerClosed {
			return nil
		}
		return err
	}
}
