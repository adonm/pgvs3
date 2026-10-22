package s3

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// ServeConfig is the S3 listener configuration.
type ServeConfig struct {
	Addr      string
	TLSCert   string
	TLSKey    string
	AllowHTTP bool
}

// Listen validates the listener and returns an HTTP server. A non-loopback
// plaintext listener is refused unless AllowHTTP is explicit.
func Listen(cfg ServeConfig, handler http.Handler, log *slog.Logger) (*http.Server, error) {
	host, _, err := net.SplitHostPort(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen address: %w", err)
	}
	ip := net.ParseIP(host)
	loopback := host == "localhost" || (ip != nil && ip.IsLoopback()) || host == ""
	if cfg.TLSCert == "" && cfg.TLSKey == "" {
		if !loopback && !cfg.AllowHTTP {
			return nil, fmt.Errorf("HTTPS certificate/key required outside loopback (or explicitly set PGVS3_ALLOW_HTTP=true for an isolated rig)")
		}
	} else if cfg.TLSCert == "" || cfg.TLSKey == "" {
		return nil, fmt.Errorf("both PGVS3_TLS_CERT and PGVS3_TLS_KEY must be set")
	}
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
	return srv, nil
}

// Run starts the server and shuts it down when ctx is cancelled.
func Run(ctx context.Context, srv *http.Server, cert, key string, drain time.Duration, log *slog.Logger) error {
	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		if cert != "" {
			errCh <- srv.ServeTLS(ln, cert, key)
		} else {
			errCh <- srv.Serve(ln)
		}
	}()
	scheme := "http"
	if cert != "" {
		scheme = "https"
	}
	log.Info("serving", "scheme", scheme, "addr", ln.Addr().String())
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
