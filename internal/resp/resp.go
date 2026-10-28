package resp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/telemetry"
	"github.com/tidwall/redcon"
)

const DefaultCache = "cache"

var (
	errCrossNamespace = errors.New("cross-namespace write denied")
	errNotInt         = errors.New("value is not an integer or out of range")
)

type Server struct {
	store    *store.Store
	readOnly bool
	metrics  *telemetry.CommandMetrics
	sem      chan struct{}
	ctx      context.Context
	version  string
}

func New(st *store.Store, readOnly bool, maxConc int) *Server {
	if maxConc <= 0 {
		maxConc = 32
	}
	return &Server{
		store:    st,
		readOnly: readOnly,
		metrics:  telemetry.NewCommandMetrics(),
		sem:      make(chan struct{}, maxConc),
		ctx:      context.Background(),
		version:  "dev",
	}
}

func (s *Server) Metrics(w io.Writer) { s.metrics.Write(w, "pgvs3_resp") }

func (s *Server) SetContext(ctx context.Context) { s.ctx = ctx }

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
	vals, err := s.store.Cache(b0).MGet(ctx, keys)
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
	if err := s.store.Cache(currentCache).MSet(ctx, keys, vals); err != nil {
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

func targetRef(kind ns.Kind, namespace, key string) store.ObjectRef {
	return store.ObjectRef{NS: ns.Ref{Kind: kind, Name: namespace}, Key: key}
}

func resolveTarget(currentCache, rawKey string) (ns.Kind, string, string) {
	if after, ok := strings.CutPrefix(rawKey, "s3://"); ok {
		rest := after
		if b, k, ok := strings.Cut(rest, "/"); ok && b != "" && k != "" {
			return ns.Bucket, b, k
		}
	}
	return ns.Cache, currentCache, rawKey
}

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
	maxArgs int
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
		s.metrics.Observe("RESP_"+cmd.name, sess.status)
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

func cmdTxUnsupported(_ *Server, _ context.Context, _ redcon.Conn, _ *respSession, _ string, _ [][]byte) error {
	return errors.New("MULTI/EXEC/WATCH are not supported; use a pipeline (redis-py: transaction=False)")
}

func cmdPing(_ *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, args [][]byte) error {
	if len(args) == 0 {
		conn.WriteString("PONG")
	} else {
		writeBulk(conn, args[0])
	}
	return nil
}

func cmdEcho(_ *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, args [][]byte) error {
	writeBulk(conn, args[0])
	return nil
}

func cmdOK(_ *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, _ [][]byte) error {
	conn.WriteString("OK")
	return nil
}

func cmdQuit(_ *Server, _ context.Context, conn redcon.Conn, sess *respSession, _ string, _ [][]byte) error {
	sess.quit = true
	conn.WriteString("OK")
	return nil
}

func cmdReset(_ *Server, _ context.Context, conn redcon.Conn, sess *respSession, _ string, _ [][]byte) error {
	sess.bucket = DefaultCache
	conn.WriteString("RESET")
	return nil
}

func cmdHello(s *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, args [][]byte) error {
	if len(args) >= 1 {
		ver, err := strconv.Atoi(string(args[0]))
		if err != nil {
			return errors.New("protocol version is not an integer or out of range")
		}
		if ver != 2 {
			return errors.New("NOPROTO sorry this protocol version is not supported")
		}
	}
	conn.WriteArray(14)
	conn.WriteBulkString("server")
	conn.WriteBulkString("pgvs3")
	conn.WriteBulkString("version")
	conn.WriteBulkString(s.version)
	conn.WriteBulkString("proto")
	conn.WriteInt64(2)
	conn.WriteBulkString("id")
	conn.WriteInt64(1)
	conn.WriteBulkString("mode")
	conn.WriteBulkString("standalone")
	conn.WriteBulkString("role")
	conn.WriteBulkString("master")
	conn.WriteBulkString("modules")
	conn.WriteArray(0)
	return nil
}

func cmdCommand(_ *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, _ [][]byte) error {
	conn.WriteArray(0)
	return nil
}

func cmdConfig(_ *Server, _ context.Context, conn redcon.Conn, _ *respSession, _ string, args [][]byte) error {
	if len(args) >= 2 && strings.EqualFold(string(args[0]), "GET") {
		switch strings.ToLower(string(args[1])) {
		case "save":
			conn.WriteArray(2)
			conn.WriteBulkString("save")
			conn.WriteBulkString("")
		case "appendonly":
			conn.WriteArray(2)
			conn.WriteBulkString("appendonly")
			conn.WriteBulkString("no")
		default:
			conn.WriteArray(0)
		}
		return nil
	}
	conn.WriteString("OK")
	return nil
}

func cmdSelect(_ *Server, _ context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	target := string(args[0])
	if n, err := strconv.Atoi(target); err == nil {
		if n < 0 || n > 15 {
			return errors.New("DB index is out of range")
		}
		if n == 0 {
			target = DefaultCache
		} else {
			target = fmt.Sprintf("%s-%d", DefaultCache, n)
		}
	}
	if !ns.ValidName(target) {
		return errors.New("invalid namespace name")
	}
	sess.bucket = target
	conn.WriteString("OK")
	return nil
}

func cmdInfo(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, _ [][]byte) error {
	st, _ := s.store.StatNamespace(ctx, ns.Cache, sess.bucket)
	info := fmt.Sprintf("# Server\r\nredis_version:6.2.0\r\npgvs3_version:%s\r\nredis_mode:standalone\r\n# Keyspace\r\ndb0:keys=%d,logical_bytes=%d,physical_bytes=%d\r\n",
		s.version, st.Objects, st.LogicalBytes, st.PhysicalBytes)
	conn.WriteBulkString(info)
	return nil
}

func cmdDBSize(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, _ [][]byte) error {
	n, err := s.store.Cache(sess.bucket).DBSize(ctx)
	return writeInt(conn, n, err)
}

func cmdFlush(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, _ [][]byte) error {
	if strings.EqualFold(name, "FLUSHALL") {
		_, err := s.store.CacheFlushAll(ctx)
		return writeOK(conn, err)
	}
	return writeOK(conn, s.store.Cache(sess.bucket).Flush(ctx))
}

func cmdGet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	val, err := s.readValue(ctx, sess.bucket, args[0])
	if err != nil {
		sess.status = 500
		return err
	}
	if val == nil {
		sess.status = 404
	}
	writeBulk(conn, val)
	return nil
}

func (s *Server) readValue(ctx context.Context, current string, key []byte) ([]byte, error) {
	kind, name, k := resolveTarget(current, string(key))
	if kind == ns.Cache {
		value, _, err := s.store.Cache(name).Get(ctx, k)
		return value, err
	}
	value, err := s.store.Get(ctx, targetRef(kind, name, k), store.WholeObjectRange())
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return value.Bytes, nil
}

func cmdMGet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	cacheKeys := make([]string, len(args))
	allCache := true
	for i, a := range args {
		kind, _, key := resolveTarget(sess.bucket, string(a))
		if kind != ns.Cache {
			allCache = false
			break
		}
		cacheKeys[i] = key
	}
	if allCache {
		vals, err := s.store.Cache(sess.bucket).MGet(ctx, cacheKeys)
		if err != nil {
			sess.status = 500
			return err
		}
		conn.WriteArray(len(vals))
		for _, v := range vals {
			writeBulk(conn, v)
		}
		return nil
	}
	conn.WriteArray(len(args))
	for _, a := range args {
		val, err := s.readValue(ctx, sess.bucket, a)
		if err != nil {
			sess.status = 500
			return err
		}
		writeBulk(conn, val)
	}
	return nil
}

func cmdSet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	opts, err := parseRESPSetOptions(args[2:])
	if err != nil {
		return err
	}
	oldVal, _, wrote, err := s.store.Cache(b).Set(ctx, k, args[1], opts)
	if err != nil {
		sess.status = 500
		return err
	}
	if opts.ReturnOld {
		writeBulk(conn, oldVal)
		return nil
	}
	if !wrote {
		conn.WriteNull()
		return nil
	}
	conn.WriteString("OK")
	return nil
}

func cmdSetNX(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	_, _, wrote, err := s.store.Cache(b).Set(ctx, k, args[1], store.CacheSetOptions{Precondition: store.Precondition{Mode: store.IfAbsent}})
	return writeBoolInt(conn, wrote, err)
}

func cmdSetEX(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("invalid expire time in '%s' command", strings.ToLower(name))
	}
	exp := respExpiration(name, n, time.Now())
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	_, _, _, err = s.store.Cache(b).Set(ctx, k, args[2], store.CacheSetOptions{ExpiresAt: &exp})
	return writeOK(conn, err)
}

func cmdGetSet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	oldVal, _, _, err := s.store.Cache(b).GetSet(ctx, k, args[1], store.CacheSetOptions{ReturnOld: true})
	if err != nil {
		return err
	}
	writeBulk(conn, oldVal)
	return nil
}

func cmdGetDel(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	val, _, err := s.store.Cache(b).GetDel(ctx, k)
	if err != nil {
		return err
	}
	writeBulk(conn, val)
	return nil
}

func cmdMSet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	if len(args)%2 != 0 {
		return errors.New("wrong number of arguments for 'mset' command")
	}
	n := len(args) / 2
	keys := make([]string, n)
	vals := make([][]byte, n)
	for i := range n {
		_, k, err := writeTarget(sess.bucket, args[2*i])
		if err != nil {
			return err
		}
		keys[i], vals[i] = k, args[2*i+1]
	}
	return writeOK(conn, s.store.Cache(sess.bucket).MSet(ctx, keys, vals))
}

func cmdAppend(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	n, err := s.store.Cache(b).Append(ctx, k, args[1])
	return writeInt(conn, n, err)
}

func cmdSetRange(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	off, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || off < 0 {
		return errors.New("offset is out of range")
	}
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	n, err := s.store.Cache(b).SetRange(ctx, k, off, args[2])
	return writeInt(conn, n, err)
}

func cmdGetRange(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	startIdx, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	endIdx, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil {
		return errNotInt
	}
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	m, err := s.store.MetaHeader(ctx, targetRef(kind, b, k))
	if errors.Is(err, store.ErrNotFound) || (err == nil && m.Size == 0) {
		writeBulk(conn, []byte{})
		return nil
	}
	if err != nil {
		return err
	}
	if startIdx < 0 {
		startIdx = max(m.Size+startIdx, 0)
	}
	if endIdx < 0 {
		endIdx = m.Size + endIdx
	}
	if startIdx > endIdx || startIdx >= m.Size || endIdx < 0 {
		writeBulk(conn, []byte{})
		return nil
	}
	sl, err := s.store.Get(ctx, targetRef(kind, b, k), store.Range(startIdx, min(endIdx, m.Size-1), -1))
	if err != nil {
		return err
	}
	writeBulk(conn, sl.Bytes)
	return nil
}

func cmdStrLen(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	m, err := s.store.MetaHeader(ctx, targetRef(kind, b, k))
	if errors.Is(err, store.ErrNotFound) {
		conn.WriteInt64(0)
		return nil
	}
	if err != nil {
		return err
	}
	conn.WriteInt64(m.Size)
	return nil
}

func cmdIncr(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	var delta int64 = 1
	switch name {
	case "DECR":
		delta = -1
	case "INCRBY", "DECRBY":
		v, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return errNotInt
		}
		if name == "DECRBY" {
			delta = -v
		} else {
			delta = v
		}
	}
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	next, err := s.store.Cache(b).IncrBy(ctx, k, delta)
	if errors.Is(err, store.ErrNotInteger) {
		return errNotInt
	}
	return writeInt(conn, next, err)
}

func cmdType(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	_, err := s.store.MetaHeader(ctx, targetRef(kind, b, k))
	if errors.Is(err, store.ErrNotFound) {
		conn.WriteString("none")
		return nil
	}
	if err != nil {
		return err
	}
	conn.WriteString("string")
	return nil
}

func cmdDel(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	keys := make([]string, len(args))
	for i, a := range args {
		_, k, err := writeTarget(sess.bucket, a)
		if err != nil {
			return err
		}
		keys[i] = k
	}
	n, err := s.store.Cache(sess.bucket).Del(ctx, keys)
	return writeInt(conn, n, err)
}

func cmdExists(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	var n int64
	cacheKeys := make([]string, 0, len(args))
	for _, a := range args {
		kind, b, k := resolveTarget(sess.bucket, string(a))
		if kind == ns.Bucket {
			if _, err := s.store.MetaHeader(ctx, targetRef(kind, b, k)); err == nil {
				n++
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			continue
		}
		cacheKeys = append(cacheKeys, k)
	}
	if len(cacheKeys) > 0 {
		count, err := s.store.Cache(sess.bucket).Exists(ctx, cacheKeys)
		if err != nil {
			return err
		}
		n += count
	}
	return writeInt(conn, n, nil)
}

func cmdExpire(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return errNotInt
	}
	exp := respExpiration(name, n, time.Now())
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	opts := make([]string, len(args)-2)
	for i, opt := range args[2:] {
		opts[i] = string(opt)
	}
	ok, err := s.store.Cache(b).Expire(ctx, k, &exp, opts...)
	return writeBoolInt(conn, ok, err)
}

func cmdPersist(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	ok, err := s.store.Cache(b).Expire(ctx, k, nil)
	return writeBoolInt(conn, ok, err)
}

func cmdTTL(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	if kind == ns.Bucket {
		if _, err := s.store.MetaHeader(ctx, targetRef(kind, b, k)); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				conn.WriteInt64(-2)
				return nil
			}
			return err
		}
		conn.WriteInt64(-1)
		return nil
	}
	d, hasTTL, exists, err := s.store.Cache(b).TTL(ctx, k)
	if err != nil {
		return err
	}
	if !exists {
		conn.WriteInt64(-2)
		return nil
	}
	if !hasTTL {
		conn.WriteInt64(-1)
		return nil
	}
	if name == "PTTL" {
		conn.WriteInt64(max(d.Milliseconds(), 1))
	} else {
		conn.WriteInt64(max(int64(d.Seconds()+0.5), 1))
	}
	return nil
}

func cmdKeys(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	_, keys, err := s.scanCache(ctx, sess.bucket, string(args[0]), "", 0)
	if err != nil {
		return err
	}
	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
	return nil
}

func cmdScan(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	cursor := string(args[0])
	count := 10
	match := "*"
	for i := 1; i+1 < len(args); i += 2 {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			match = string(args[i+1])
		case "COUNT":
			if n, err := strconv.Atoi(string(args[i+1])); err == nil && n > 0 {
				count = n
			}
		case "TYPE":
			if !strings.EqualFold(string(args[i+1]), "string") {
				conn.WriteArray(2)
				conn.WriteBulkString("0")
				conn.WriteArray(0)
				return nil
			}
		}
	}
	after := ""
	if cursor != "0" {
		after = cursor
	}
	next, keys, err := s.scanCache(ctx, sess.bucket, match, after, count)
	if err != nil {
		return err
	}
	conn.WriteArray(2)
	conn.WriteBulkString(next)
	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
	return nil
}

func parseRESPSetOptions(args [][]byte) (store.CacheSetOptions, error) {
	var opts store.CacheSetOptions
	for i := 0; i < len(args); i++ {
		flag := strings.ToUpper(string(args[i]))
		switch flag {
		case "NX":
			if opts.Precondition.Mode == store.IfPresent {
				return opts, errors.New("syntax error")
			}
			opts.Precondition.Mode = store.IfAbsent
		case "XX":
			if opts.Precondition.Mode == store.IfAbsent {
				return opts, errors.New("syntax error")
			}
			opts.Precondition.Mode = store.IfPresent
		case "GET":
			opts.ReturnOld = true
		case "KEEPTTL":
			opts.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) {
				return opts, errors.New("syntax error")
			}
			i++
			n, err := strconv.ParseInt(string(args[i]), 10, 64)
			if err != nil || n <= 0 {
				return opts, errors.New("invalid expire time in 'set' command")
			}
			t := respExpiration(flag, n, time.Now())
			opts.ExpiresAt = &t
		default:
			return opts, errors.New("syntax error")
		}
	}
	return opts, nil
}

func respExpiration(name string, n int64, now time.Time) time.Time {
	switch name {
	case "EX", "SETEX", "EXPIRE":
		return now.Add(time.Duration(n) * time.Second)
	case "PX", "PSETEX", "PEXPIRE":
		return now.Add(time.Duration(n) * time.Millisecond)
	case "EXAT", "EXPIREAT":
		return time.Unix(n, 0)
	case "PXAT", "PEXPIREAT":
		return time.UnixMilli(n)
	default:
		return time.Time{}
	}
}

func literalPrefix(pat string) string {
	for i := 0; i < len(pat); i++ {
		switch pat[i] {
		case '*', '?', '[', '\\':
			return pat[:i]
		}
	}
	return pat
}

func prefixUpper(prefix string) string { return prefix + "\U0010FFFF" }

func (s *Server) scanCache(ctx context.Context, name, pattern, after string, count int) (string, []string, error) {
	if count <= 0 {
		count = int(^uint(0) >> 1)
	}
	prefix := literalPrefix(pattern)
	page := 1024
	if count < page {
		page = max(count*4, 64)
	}
	keys := make([]string, 0, min(count, page))
	for len(keys) < count {
		rows, err := s.store.List(ctx, ns.CacheRef(name), prefix, prefixUpper(prefix), after, page)
		if err != nil {
			return "0", nil, err
		}
		if len(rows) == 0 {
			return "0", keys, nil
		}
		for _, row := range rows {
			after = row.Key
			if pattern == "*" {
				keys = append(keys, row.Key)
			} else if ok, _ := path.Match(pattern, row.Key); ok {
				keys = append(keys, row.Key)
			}
			if len(keys) >= count {
				return after, keys, nil
			}
		}
		if len(rows) < page {
			return "0", keys, nil
		}
	}
	return after, keys, nil
}
