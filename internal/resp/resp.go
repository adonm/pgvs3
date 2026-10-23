package resp

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/tidwall/redcon"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// DefaultCache is the cache namespace used when a client has not run SELECT.
const DefaultCache = "cache"

var (
	errCrossNamespace = errors.New("cross-namespace write denied")
	errNotInt         = errors.New("value is not an integer or out of range")
)

// Server terminates RESP2 on a connection.
type Server struct {
	store    *store.Store
	readOnly bool
	metrics  *metrics
	sem      chan struct{}
	ctx      context.Context
	version  string
}

// New returns a RESP server over the shared store.
func New(st *store.Store, readOnly bool, maxConc int) *Server {
	if maxConc <= 0 {
		maxConc = 32
	}
	return &Server{
		store:    st,
		readOnly: readOnly,
		metrics:  newMetrics(),
		sem:      make(chan struct{}, maxConc),
		ctx:      context.Background(),
		version:  "dev",
	}
}

// Metrics writes this server's Prometheus counters.
func (s *Server) Metrics(w io.Writer) { s.metrics.write(w) }

// SetContext supplies the cancellation context for store work.
func (s *Server) SetContext(ctx context.Context) { s.ctx = ctx }

// SetVersion sets the version reported by HELLO and INFO.
func (s *Server) SetVersion(v string) { s.version = v }

type respCmd struct {
	name string
	args [][]byte
}

type respSession struct {
	bucket string
	status int
	quit   bool
}

func fromRedcon(cmd redcon.Command) respCmd {
	return respCmd{name: strings.ToUpper(string(cmd.Args[0])), args: cmd.Args[1:]}
}

// ServeRESP implements redcon's command handler. Any remaining commands in the
// current pipeline are consumed here so consecutive GETs and SETs can still be
// coalesced into CacheMGet/CacheMSet.
func (s *Server) ServeRESP(conn redcon.Conn, first redcon.Command) {
	sess, _ := conn.Context().(*respSession)
	if sess == nil {
		sess = &respSession{bucket: DefaultCache}
		conn.SetContext(sess)
	}
	batch := make([]respCmd, 0, 1+len(conn.PeekPipeline()))
	batch = append(batch, fromRedcon(first))
	for _, cmd := range conn.ReadPipeline() {
		batch = append(batch, fromRedcon(cmd))
	}
	quit := false
	for len(batch) > 0 && !quit {
		n := min(len(batch), 128)
		quit = s.execRESPBatch(s.ctx, conn, sess, batch[:n])
		batch = batch[n:]
	}
	if quit {
		_ = conn.Close()
	}
}

func (s *Server) execRESPBatch(ctx context.Context, conn redcon.Conn, sess *respSession, batch []respCmd) bool {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		conn.WriteError("ERR server shutting down")
		return true
	}
	for i := 0; i < len(batch); i++ {
		if n := s.tryCoalesceGet(ctx, conn, sess.bucket, batch[i:]); n > 1 {
			i += n - 1
			continue
		}
		if n := s.tryCoalesceSet(ctx, conn, sess.bucket, batch[i:]); n > 1 {
			i += n - 1
			continue
		}
		if s.execRESPCommand(ctx, conn, sess, batch[i]) {
			return true
		}
	}
	return false
}

func (s *Server) tryCoalesceGet(ctx context.Context, conn redcon.Conn, currentCache string, batch []respCmd) int {
	if len(batch) < 2 || batch[0].name != "GET" || len(batch[0].args) != 1 {
		return 0
	}
	k0Kind, b0, k0 := resolveTarget(currentCache, string(batch[0].args[0]))
	if k0Kind != ns.Cache {
		return 0
	}
	j := 1
	for j < len(batch) && batch[j].name == "GET" && len(batch[j].args) == 1 {
		kjKind, bj, _ := resolveTarget(currentCache, string(batch[j].args[0]))
		if kjKind != ns.Cache || bj != b0 {
			break
		}
		j++
	}
	if j < 2 {
		return 0
	}
	keys := make([]string, j)
	keys[0] = k0
	for m := 1; m < j; m++ {
		_, _, keys[m] = resolveTarget(currentCache, string(batch[m].args[0]))
	}
	vals, err := s.store.CacheMGet(ctx, b0, keys)
	if err != nil {
		for range keys {
			conn.WriteError("ERR " + err.Error())
		}
	} else {
		for _, v := range vals {
			writeBulk(conn, v)
		}
	}
	return j
}

func (s *Server) tryCoalesceSet(ctx context.Context, conn redcon.Conn, currentCache string, batch []respCmd) int {
	if s.readOnly || len(batch) < 2 {
		return 0
	}
	isBatchableSet := func(c respCmd) (string, bool) {
		if c.name != "SET" || len(c.args) != 2 || len(c.args[1]) > store.MaxInlineBytes {
			return "", false
		}
		_, k, err := writeTarget(currentCache, c.args[0])
		return k, err == nil
	}
	k0, ok := isBatchableSet(batch[0])
	if !ok {
		return 0
	}
	j := 1
	for j < len(batch) {
		if _, ok := isBatchableSet(batch[j]); !ok {
			break
		}
		j++
	}
	if j < 2 {
		return 0
	}
	keys := make([]string, j)
	vals := make([][]byte, j)
	keys[0], vals[0] = k0, batch[0].args[1]
	for m := 1; m < j; m++ {
		keys[m], _ = isBatchableSet(batch[m])
		vals[m] = batch[m].args[1]
	}
	if err := s.store.CacheMSet(ctx, currentCache, keys, vals); err != nil {
		for range keys {
			conn.WriteError("ERR " + err.Error())
		}
	} else {
		for range keys {
			conn.WriteString("OK")
		}
	}
	return j
}

// resolveTarget parses an optional "s3://<bucket>/<key>" prefix. Without that
// prefix, the key addresses the session's active cache namespace.
func resolveTarget(currentCache, rawKey string) (ns.Kind, string, string) {
	if strings.HasPrefix(rawKey, "s3://") {
		rest := strings.TrimPrefix(rawKey, "s3://")
		if b, k, ok := strings.Cut(rest, "/"); ok && b != "" && k != "" {
			return ns.Bucket, b, k
		}
	}
	return ns.Cache, currentCache, rawKey
}

// writeTarget resolves a key for a mutating command, rejecting cross-namespace
// and s3:// writes.
func writeTarget(currentCache string, rawKey []byte) (string, string, error) {
	raw := string(rawKey)
	if strings.HasPrefix(raw, "s3://") {
		return "", "", errCrossNamespace
	}
	kind, b, k := resolveTarget(currentCache, raw)
	if kind != ns.Cache || b != currentCache {
		return "", "", errCrossNamespace
	}
	return b, k, nil
}

type cmdHandler func(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error

type cmdSpec struct {
	minArgs int
	maxArgs int // -1 for variadic
	write   bool
	fn      cmdHandler
}

var commands = map[string]cmdSpec{
	"MULTI":     {0, -1, false, cmdTxUnsupported},
	"EXEC":      {0, -1, false, cmdTxUnsupported},
	"DISCARD":   {0, -1, false, cmdTxUnsupported},
	"WATCH":     {0, -1, false, cmdTxUnsupported},
	"UNWATCH":   {0, -1, false, cmdTxUnsupported},
	"PING":      {0, -1, false, cmdPing},
	"ECHO":      {1, 1, false, cmdEcho},
	"AUTH":      {0, -1, false, cmdOK},
	"CLIENT":    {0, -1, false, cmdOK},
	"QUIT":      {0, -1, false, cmdQuit},
	"RESET":     {0, -1, false, cmdReset},
	"HELLO":     {0, -1, false, cmdHello},
	"COMMAND":   {0, -1, false, cmdCommand},
	"CONFIG":    {0, -1, false, cmdConfig},
	"SELECT":    {1, 1, false, cmdSelect},
	"INFO":      {0, -1, false, cmdInfo},
	"DBSIZE":    {0, -1, false, cmdDBSize},
	"FLUSHDB":   {0, -1, true, cmdFlush},
	"FLUSHALL":  {0, -1, true, cmdFlush},
	"TYPE":      {1, 1, false, cmdType},
	"STRLEN":    {1, 1, false, cmdStrLen},
	"GET":       {1, 1, false, cmdGet},
	"MGET":      {1, -1, false, cmdMGet},
	"GETRANGE":  {3, 3, false, cmdGetRange},
	"SUBSTR":    {3, 3, false, cmdGetRange},
	"SET":       {2, -1, true, cmdSet},
	"SETNX":     {2, 2, true, cmdSetNX},
	"SETEX":     {3, 3, true, cmdSetEX},
	"PSETEX":    {3, 3, true, cmdSetEX},
	"GETSET":    {2, 2, true, cmdGetSet},
	"GETDEL":    {1, 1, true, cmdGetDel},
	"MSET":      {2, -1, true, cmdMSet},
	"APPEND":    {2, 2, true, cmdAppend},
	"SETRANGE":  {3, 3, true, cmdSetRange},
	"INCR":      {1, 1, true, cmdIncr},
	"DECR":      {1, 1, true, cmdIncr},
	"INCRBY":    {2, 2, true, cmdIncr},
	"DECRBY":    {2, 2, true, cmdIncr},
	"DEL":       {1, -1, true, cmdDel},
	"UNLINK":    {1, -1, true, cmdDel},
	"EXISTS":    {1, -1, false, cmdExists},
	"EXPIRE":    {2, -1, true, cmdExpire},
	"PEXPIRE":   {2, -1, true, cmdExpire},
	"EXPIREAT":  {2, -1, true, cmdExpire},
	"PEXPIREAT": {2, -1, true, cmdExpire},
	"PERSIST":   {1, 1, true, cmdPersist},
	"TTL":       {1, 1, false, cmdTTL},
	"PTTL":      {1, 1, false, cmdTTL},
	"KEYS":      {1, 1, false, cmdKeys},
	"SCAN":      {1, -1, false, cmdScan},
}

func (s *Server) execRESPCommand(ctx context.Context, conn redcon.Conn, sess *respSession, cmd respCmd) bool {
	sess.status = 200
	defer func() {
		s.metrics.observe("RESP_"+cmd.name, sess.status)
	}()

	spec, ok := commands[cmd.name]
	if !ok {
		sess.status = 400
		conn.WriteError("ERR unknown command '" + strings.ToLower(cmd.name) + "'")
		return false
	}
	if len(cmd.args) < spec.minArgs || (spec.maxArgs >= 0 && len(cmd.args) > spec.maxArgs) {
		sess.status = 400
		conn.WriteError("ERR wrong number of arguments for '" + strings.ToLower(cmd.name) + "' command")
		return false
	}
	if spec.write && s.readOnly {
		sess.status = 403
		conn.WriteError("READONLY You can't write against a read only replica.")
		return false
	}
	if err := spec.fn(s, ctx, conn, sess, cmd.name, cmd.args); err != nil {
		if sess.status == 200 {
			sess.status = 400
		}
		msg := err.Error()
		if !strings.HasPrefix(msg, "ERR ") && !strings.HasPrefix(msg, "NOPROTO ") && !strings.HasPrefix(msg, "READONLY ") {
			msg = "ERR " + msg
		}
		conn.WriteError(msg)
	}
	return sess.quit
}

// writeBulk distinguishes a nil value (RESP null) from an empty value.
func writeBulk(conn redcon.Conn, b []byte) {
	if b == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(b)
}

func writeOK(conn redcon.Conn, err error) error {
	if err == nil {
		conn.WriteString("OK")
	}
	return err
}

func writeInt(conn redcon.Conn, n int64, err error) error {
	if err == nil {
		conn.WriteInt64(n)
	}
	return err
}

func writeBoolInt(conn redcon.Conn, ok bool, err error) error {
	if err != nil {
		return err
	}
	if ok {
		conn.WriteInt64(1)
	} else {
		conn.WriteInt64(0)
	}
	return nil
}
