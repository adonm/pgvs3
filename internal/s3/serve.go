package s3

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"
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

// Run starts the server (multiplexing S3/HTTP and RESP on the same port) and
// shuts it down when ctx is cancelled.
func Run(ctx context.Context, srv *http.Server, drain time.Duration, log *slog.Logger) error {
	rawLn, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	ln := rawLn
	if s3Srv, ok := srv.Handler.(*Server); ok {
		ln = newMuxListener(ctx, rawLn, s3Srv)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()
	log.Info("serving", "addr", rawLn.Addr().String())
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

type muxListener struct {
	net.Listener
	httpCh chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newMuxListener(ctx context.Context, base net.Listener, s *Server) *muxListener {
	m := &muxListener{
		Listener: base,
		httpCh:   make(chan net.Conn, 256),
		closed:   make(chan struct{}),
	}
	go m.acceptLoop(ctx, s)
	return m
}

func (m *muxListener) acceptLoop(ctx context.Context, s *Server) {
	for {
		c, err := m.Listener.Accept()
		if err != nil {
			m.Close()
			return
		}
		go func(conn net.Conn) {
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			var buf [5]byte
			n, err := conn.Read(buf[:])
			_ = conn.SetReadDeadline(time.Time{})
			if n == 0 {
				_ = conn.Close()
				return
			}
			pc := &peekConn{Conn: conn, buf: buf[:n], err: err}
			if isHTTPPrefix(buf[:n]) {
				select {
				case m.httpCh <- pc:
				case <-m.closed:
					_ = pc.Close()
				}
				return
			}
			s.serveRESP(ctx, pc)
		}(c)
	}
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

func isHTTPPrefix(b []byte) bool {
	if len(b) < 5 || b[0] == '*' {
		return false
	}
	switch string(b[:5]) {
	case "GET /", "GET h", "PUT /", "PUT h", "HEAD ", "POST ", "DELET", "OPTIO", "PATCH":
		return true
	}
	return false
}

type peekConn struct {
	net.Conn
	buf []byte
	err error
}

func (p *peekConn) Read(b []byte) (int, error) {
	if len(p.buf) > 0 {
		n := copy(b, p.buf)
		p.buf = p.buf[n:]
		if len(p.buf) == 0 && p.err != nil {
			err := p.err
			p.err = nil
			return n, err
		}
		return n, nil
	}
	if p.err != nil {
		err := p.err
		p.err = nil
		return 0, err
	}
	return p.Conn.Read(b)
}
