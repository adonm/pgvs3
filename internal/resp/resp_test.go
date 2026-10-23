package resp

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/tidwall/redcon"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

type stubConn struct {
	ctx      any
	closed   bool
	pipeline []redcon.Command
	out      []string
	errs     []string
}

func (c *stubConn) RemoteAddr() string             { return "127.0.0.1:0" }
func (c *stubConn) Close() error                   { c.closed = true; return nil }
func (c *stubConn) WriteError(msg string)          { c.errs = append(c.errs, msg) }
func (c *stubConn) WriteString(str string)         { c.out = append(c.out, "+"+str) }
func (c *stubConn) WriteBulk(b []byte)             { c.out = append(c.out, "$"+string(b)) }
func (c *stubConn) WriteBulkString(s string)       { c.out = append(c.out, "$"+s) }
func (c *stubConn) WriteInt(n int)                 { c.WriteInt64(int64(n)) }
func (c *stubConn) WriteInt64(n int64)             { c.out = append(c.out, ":") }
func (c *stubConn) WriteUint64(n uint64)           { c.out = append(c.out, ":") }
func (c *stubConn) WriteArray(n int)               { c.out = append(c.out, "*") }
func (c *stubConn) WriteNull()                     { c.out = append(c.out, "$-1") }
func (c *stubConn) WriteRaw(b []byte)              { c.out = append(c.out, string(b)) }
func (c *stubConn) WriteAny(v any)                 {}
func (c *stubConn) Context() any                   { return c.ctx }
func (c *stubConn) SetContext(v any)               { c.ctx = v }
func (c *stubConn) SetReadBuffer(int)              {}
func (c *stubConn) Detach() redcon.DetachedConn    { return nil }
func (c *stubConn) ReadPipeline() []redcon.Command { p := c.pipeline; c.pipeline = nil; return p }
func (c *stubConn) PeekPipeline() []redcon.Command { return c.pipeline }
func (c *stubConn) NetConn() net.Conn              { return nil }
func (c *stubConn) WriteBulkFrom(int64, io.Reader) {}

func TestResolveAndWriteTarget(t *testing.T) {
	kind, b, k := resolveTarget("cache", "plain-key")
	if kind != ns.Cache || b != "cache" || k != "plain-key" {
		t.Fatalf("got (%v, %q, %q)", kind, b, k)
	}
	kind, b, k = resolveTarget("cache", "s3://lake/dir/obj")
	if kind != ns.Bucket || b != "lake" || k != "dir/obj" {
		t.Fatalf("got (%v, %q, %q)", kind, b, k)
	}
	// s3:// writes must be rejected even if the bucket name matches currentCache.
	if _, _, err := writeTarget("cache", []byte("s3://cache/key")); err == nil {
		t.Fatal("expected s3:// write to be rejected")
	}
	if _, _, err := writeTarget("cache", []byte("s3://lake/key")); err == nil {
		t.Fatal("expected cross-namespace write to be rejected")
	}
	b, k, err := writeTarget("cache", []byte("local-key"))
	if err != nil || b != "cache" || k != "local-key" {
		t.Fatalf("got (%q, %q, %v)", b, k, err)
	}
}

func TestParseSetOptions(t *testing.T) {
	opts, err := parseRESPSetOptions([][]byte{[]byte("nx"), []byte("ex"), []byte("10"), []byte("get")})
	if err != nil || opts.Condition != store.IfAbsent || !opts.ReturnOld || opts.ExpiresAt == nil {
		t.Fatalf("unexpected opts: %+v err=%v", opts, err)
	}
	if _, err := parseRESPSetOptions([][]byte{[]byte("nx"), []byte("xx")}); err == nil {
		t.Fatal("expected NX+XX conflict error")
	}
	if _, err := parseRESPSetOptions([][]byte{[]byte("ex"), []byte("0")}); err == nil {
		t.Fatal("expected non-positive EX error")
	}
}

func TestCommandDispatchValidation(t *testing.T) {
	s := New(nil, true, 4)
	s.SetVersion("0.4.0")
	ctx := context.Background()

	run := func(name string, args ...string) (*stubConn, *respSession, bool) {
		c := &stubConn{}
		sess := &respSession{bucket: DefaultCache}
		bargs := make([][]byte, len(args))
		for i, a := range args {
			bargs[i] = []byte(a)
		}
		quit := s.execRESPCommand(ctx, c, sess, respCmd{name: name, args: bargs})
		return c, sess, quit
	}

	if c, sess, _ := run("NOPE"); sess.status != 400 || len(c.errs) != 1 || !strings.Contains(c.errs[0], "unknown command") {
		t.Fatalf("unknown command: status=%d errs=%v", sess.status, c.errs)
	}
	if c, sess, _ := run("GET"); sess.status != 400 || len(c.errs) != 1 || !strings.Contains(c.errs[0], "wrong number of arguments") {
		t.Fatalf("arity error: status=%d errs=%v", sess.status, c.errs)
	}
	if c, sess, _ := run("SET", "k", "v"); sess.status != 403 || len(c.errs) != 1 || !strings.HasPrefix(c.errs[0], "READONLY ") {
		t.Fatalf("read-only error: status=%d errs=%v", sess.status, c.errs)
	}
	if c, _, _ := run("HELLO", "3"); len(c.errs) != 1 || !strings.HasPrefix(c.errs[0], "NOPROTO ") {
		t.Fatalf("HELLO 3: errs=%v", c.errs)
	}
	if c, _, _ := run("HELLO", "2"); len(c.errs) != 0 || len(c.out) == 0 {
		t.Fatalf("HELLO 2: out=%v errs=%v", c.out, c.errs)
	}
	if c, _, _ := run("MULTI"); len(c.errs) != 1 || !strings.Contains(c.errs[0], "MULTI/EXEC/WATCH") {
		t.Fatalf("MULTI: errs=%v", c.errs)
	}
	if _, sess, _ := run("SELECT", "2"); sess.bucket != "cache-2" {
		t.Fatalf("SELECT 2: bucket=%q", sess.bucket)
	}
	if _, _, quit := run("QUIT"); !quit {
		t.Fatal("expected QUIT to request close")
	}
}
