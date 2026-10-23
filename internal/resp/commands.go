package resp

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// ---- connection & server commands -------------------------------------------

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
			return errors.New("Protocol version is not an integer or out of range")
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
	n, err := s.store.CacheDBSize(ctx, sess.bucket)
	return writeInt(conn, n, err)
}

func cmdFlush(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, _ [][]byte) error {
	return writeOK(conn, s.store.CacheFlush(ctx, sess.bucket))
}

// ---- string commands --------------------------------------------------------

func cmdGet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	var (
		val []byte
		ok  bool
		err error
	)
	if kind == ns.Bucket {
		var sl *store.Slice
		sl, err = s.store.GetKind(ctx, ns.Bucket, b, k, 0, -1, -1)
		if sl != nil {
			val, ok = sl.Bytes, true
		}
	} else {
		val, ok, err = s.store.CacheGet(ctx, b, k)
	}
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		sess.status = 500
		return err
	}
	if !ok {
		sess.status = 404
		conn.WriteNull()
		return nil
	}
	writeBulk(conn, val)
	return nil
}

func cmdMGet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	keys := make([]string, len(args))
	for i, a := range args {
		_, _, keys[i] = resolveTarget(sess.bucket, string(a))
	}
	vals, err := s.store.CacheMGet(ctx, sess.bucket, keys)
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

func cmdSet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	opts, err := parseRESPSetOptions(args[2:])
	if err != nil {
		return err
	}
	oldVal, hadOld, wrote, err := s.store.CacheSet(ctx, b, k, args[1], opts)
	if err != nil {
		sess.status = 500
		return err
	}
	if opts.ReturnOld {
		if !hadOld {
			conn.WriteNull()
		} else {
			writeBulk(conn, oldVal)
		}
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
	_, _, wrote, err := s.store.CacheSet(ctx, b, k, args[1], store.CacheSetOptions{Condition: store.IfAbsent})
	return writeBoolInt(conn, wrote, err)
}

func cmdSetEX(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || n <= 0 {
		return fmt.Errorf("invalid expire time in '%s' command", strings.ToLower(name))
	}
	unit := time.Second
	if name == "PSETEX" {
		unit = time.Millisecond
	}
	exp := time.Now().Add(time.Duration(n) * unit)
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	_, _, _, err = s.store.CacheSet(ctx, b, k, args[2], store.CacheSetOptions{ExpiresAt: &exp})
	return writeOK(conn, err)
}

func cmdGetSet(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	oldVal, hadOld, _, err := s.store.CacheSet(ctx, b, k, args[1], store.CacheSetOptions{ReturnOld: true})
	if err != nil {
		return err
	}
	if !hadOld {
		conn.WriteNull()
	} else {
		writeBulk(conn, oldVal)
	}
	return nil
}

func cmdGetDel(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	val, ok, err := s.store.CacheGetDel(ctx, b, k)
	if err != nil {
		return err
	}
	if !ok {
		conn.WriteNull()
		return nil
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
	for i := 0; i < n; i++ {
		_, k, err := writeTarget(sess.bucket, args[2*i])
		if err != nil {
			return err
		}
		keys[i], vals[i] = k, args[2*i+1]
	}
	return writeOK(conn, s.store.CacheMSet(ctx, sess.bucket, keys, vals))
}

func cmdAppend(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	cur, _, err := s.store.CacheGet(ctx, b, k)
	if err != nil {
		return err
	}
	next := append(append([]byte(nil), cur...), args[1]...)
	_, _, _, err = s.store.CacheSet(ctx, b, k, next, store.CacheSetOptions{KeepTTL: true})
	return writeInt(conn, int64(len(next)), err)
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
	cur, _, err := s.store.CacheGet(ctx, b, k)
	if err != nil {
		return err
	}
	if need := off + int64(len(args[2])); int64(len(cur)) < need {
		cur = append(cur, make([]byte, need-int64(len(cur)))...)
	}
	copy(cur[off:], args[2])
	_, _, _, err = s.store.CacheSet(ctx, b, k, cur, store.CacheSetOptions{KeepTTL: true})
	return writeInt(conn, int64(len(cur)), err)
}

func cmdGetRange(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	startIdx, err1 := strconv.ParseInt(string(args[1]), 10, 64)
	endIdx, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil {
		return errNotInt
	}
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	m, err := s.store.MetaHeaderKind(ctx, kind, b, k)
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
	sl, err := s.store.GetKind(ctx, kind, b, k, startIdx, min(endIdx, m.Size-1), -1)
	if err != nil {
		return err
	}
	writeBulk(conn, sl.Bytes)
	return nil
}

func cmdStrLen(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	m, err := s.store.MetaHeaderKind(ctx, kind, b, k)
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
	next, err := s.store.CacheIncrBy(ctx, b, k, delta)
	if errors.Is(err, store.ErrNotInteger) {
		return errNotInt
	}
	return writeInt(conn, next, err)
}

// ---- key, TTL & scan commands -----------------------------------------------

func cmdType(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	kind, b, k := resolveTarget(sess.bucket, string(args[0]))
	_, err := s.store.MetaHeaderKind(ctx, kind, b, k)
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
	n, err := s.store.CacheDel(ctx, sess.bucket, keys)
	return writeInt(conn, n, err)
}

func cmdExists(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	keys := make([]string, len(args))
	for i, a := range args {
		_, _, keys[i] = resolveTarget(sess.bucket, string(a))
	}
	n, err := s.store.CacheExists(ctx, sess.bucket, keys)
	return writeInt(conn, n, err)
}

func cmdExpire(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	n, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return errNotInt
	}
	var exp time.Time
	switch name {
	case "EXPIRE":
		exp = time.Now().Add(time.Duration(n) * time.Second)
	case "PEXPIRE":
		exp = time.Now().Add(time.Duration(n) * time.Millisecond)
	case "EXPIREAT":
		exp = time.Unix(n, 0)
	case "PEXPIREAT":
		exp = time.UnixMilli(n)
	}
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	opts := make([]string, len(args)-2)
	for i, opt := range args[2:] {
		opts[i] = string(opt)
	}
	ok, err := s.store.CacheExpire(ctx, b, k, &exp, opts...)
	return writeBoolInt(conn, ok, err)
}

func cmdPersist(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, _ string, args [][]byte) error {
	b, k, err := writeTarget(sess.bucket, args[0])
	if err != nil {
		return err
	}
	ok, err := s.store.CacheExpire(ctx, b, k, nil)
	return writeBoolInt(conn, ok, err)
}

func cmdTTL(s *Server, ctx context.Context, conn redcon.Conn, sess *respSession, name string, args [][]byte) error {
	_, b, k := resolveTarget(sess.bucket, string(args[0]))
	d, hasTTL, exists, err := s.store.CacheTTL(ctx, b, k)
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
	pat := string(args[0])
	prefix := literalPrefix(pat)
	var matched []string
	after := ""
	for {
		rows, err := s.store.ListKind(ctx, ns.Cache, sess.bucket, prefix, prefixUpper(prefix), after, 1024)
		if err != nil {
			return err
		}
		for _, r := range rows {
			after = r.Key
			if pat == "*" {
				matched = append(matched, r.Key)
			} else if ok, _ := path.Match(pat, r.Key); ok {
				matched = append(matched, r.Key)
			}
		}
		if len(rows) < 1024 {
			break
		}
	}
	conn.WriteArray(len(matched))
	for _, k := range matched {
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
	prefix := literalPrefix(match)
	after := ""
	if cursor != "0" {
		after = cursor
	}
	limit := max(count*4, 64)
	var keys []string
	next := "0"
	for len(keys) < count {
		rows, err := s.store.ListKind(ctx, ns.Cache, sess.bucket, prefix, prefixUpper(prefix), after, limit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			next = "0"
			break
		}
		for _, r := range rows {
			after = r.Key
			if match == "*" {
				keys = append(keys, r.Key)
			} else if ok, _ := path.Match(match, r.Key); ok {
				keys = append(keys, r.Key)
			}
			if len(keys) >= count {
				next = after
				break
			}
		}
		if len(keys) >= count {
			break
		}
		if len(rows) < limit {
			next = "0"
			break
		}
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
			if opts.Condition == store.IfPresent {
				return opts, errors.New("syntax error")
			}
			opts.Condition = store.IfAbsent
		case "XX":
			if opts.Condition == store.IfAbsent {
				return opts, errors.New("syntax error")
			}
			opts.Condition = store.IfPresent
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
			var t time.Time
			switch flag {
			case "EX":
				t = time.Now().Add(time.Duration(n) * time.Second)
			case "PX":
				t = time.Now().Add(time.Duration(n) * time.Millisecond)
			case "EXAT":
				t = time.Unix(n, 0)
			case "PXAT":
				t = time.UnixMilli(n)
			}
			opts.ExpiresAt = &t
		default:
			return opts, errors.New("syntax error")
		}
	}
	return opts, nil
}

// literalPrefix returns the leading literal bytes of a Redis glob pattern.
func literalPrefix(pat string) string {
	for i := 0; i < len(pat); i++ {
		switch pat[i] {
		case '*', '?', '[', '\\':
			return pat[:i]
		}
	}
	return pat
}

// prefixUpper returns a strict upper bound for keys starting with prefix.
func prefixUpper(prefix string) string { return prefix + "\U0010FFFF" }
