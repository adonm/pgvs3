package resp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// DefaultCache is the cache namespace used when a client has not run SELECT.
const DefaultCache = "cache"

// Server terminates RESP2/RESP3 on a connection.
type Server struct {
	store    *store.Store
	log      *slog.Logger
	readOnly bool
	metrics  *metrics
	sem      chan struct{}
}

// New returns a RESP server over the shared store.
func New(st *store.Store, log *slog.Logger, readOnly bool, maxConc int) *Server {
	if maxConc <= 0 {
		maxConc = 32
	}
	return &Server{store: st, log: log, readOnly: readOnly, metrics: newMetrics(),
		sem: make(chan struct{}, maxConc)}
}

// Metrics writes this server's Prometheus counters.
func (s *Server) Metrics(w io.Writer) { s.metrics.write(w) }

func (s *Server) ServeConn(ctx context.Context, conn net.Conn) { s.serveRESP(ctx, conn) }

type respCmd struct {
	name string
	args [][]byte
}

type respSession struct {
	bucket string
	proto  int
}

func (s *Server) serveRESP(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReaderSize(conn, 64<<10)
	bw := bufio.NewWriterSize(conn, 64<<10)
	sess := &respSession{bucket: DefaultCache, proto: 2}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		first, err := readRESPCommand(br)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				writeRESPError(bw, "ERR "+err.Error())
				_ = bw.Flush()
			}
			return
		}
		batch := []respCmd{first}
		for br.Buffered() > 0 && len(batch) < 128 {
			next, err := readRESPCommand(br)
			if err != nil {
				break
			}
			batch = append(batch, next)
		}

		quit := s.execRESPBatch(ctx, bw, sess, batch)
		if err := bw.Flush(); err != nil || quit {
			return
		}
	}
}

func (s *Server) execRESPBatch(ctx context.Context, bw *bufio.Writer, sess *respSession, batch []respCmd) bool {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		writeRESPError(bw, "ERR server shutting down")
		return true
	}
	for i := 0; i < len(batch); i++ {
		// Coalesce consecutive pipelined GETs on the same bucket into a single
		// CacheMGet query.
		if batch[i].name == "GET" && len(batch[i].args) == 1 {
			b0, k0 := resolveRESPTarget(sess.bucket, string(batch[i].args[0]))
			j := i + 1
			for j < len(batch) && batch[j].name == "GET" && len(batch[j].args) == 1 {
				bj, _ := resolveRESPTarget(sess.bucket, string(batch[j].args[0]))
				if bj != b0 {
					break
				}
				j++
			}
			if j-i > 1 {
				keys := make([]string, j-i)
				keys[0] = k0
				for m := i + 1; m < j; m++ {
					_, keys[m-i] = resolveRESPTarget(sess.bucket, string(batch[m].args[0]))
				}
				vals, err := s.store.CacheMGet(ctx, b0, keys)
				if err != nil {
					for range keys {
						writeRESPError(bw, "ERR "+err.Error())
					}
				} else {
					for _, v := range vals {
						writeRESPBulk(bw, sess.proto, v)
					}
				}
				i = j - 1
				continue
			}
		}
		// Coalesce consecutive unconditional SETs on the same bucket into a
		// single CacheMSet transaction.
		if !s.readOnly && batch[i].name == "SET" && len(batch[i].args) == 2 && len(batch[i].args[1]) <= store.MaxInlineBytes {
			b0, k0 := resolveRESPTarget(sess.bucket, string(batch[i].args[0]))
			j := i + 1
			for j < len(batch) && batch[j].name == "SET" && len(batch[j].args) == 2 && len(batch[j].args[1]) <= store.MaxInlineBytes {
				bj, _ := resolveRESPTarget(sess.bucket, string(batch[j].args[0]))
				if bj != b0 {
					break
				}
				j++
			}
			if j-i > 1 {
				keys := make([]string, j-i)
				vals := make([][]byte, j-i)
				keys[0], vals[0] = k0, batch[i].args[1]
				for m := i + 1; m < j; m++ {
					_, keys[m-i] = resolveRESPTarget(sess.bucket, string(batch[m].args[0]))
					vals[m-i] = batch[m].args[1]
				}
				if err := s.store.CacheMSet(ctx, b0, keys, vals); err != nil {
					for range keys {
						writeRESPError(bw, "ERR "+err.Error())
					}
				} else {
					for range keys {
						writeRESPSimple(bw, "OK")
					}
				}
				i = j - 1
				continue
			}
		}
		if s.execRESPCommand(ctx, bw, sess, batch[i]) {
			return true
		}
	}
	return false
}

func resolveRESPTarget(currentBucket, rawKey string) (string, string) {
	if strings.HasPrefix(rawKey, "s3://") {
		rest := strings.TrimPrefix(rawKey, "s3://")
		if b, k, ok := strings.Cut(rest, "/"); ok && b != "" && k != "" {
			return b, k
		}
	}
	return currentBucket, rawKey
}

func (s *Server) execRESPCommand(ctx context.Context, bw *bufio.Writer, sess *respSession, cmd respCmd) bool {
	start := time.Now()
	status := 200
	defer func() {
		s.metrics.observe("RESP_"+cmd.name, status, time.Since(start), 0)
	}()

	rejectRO := func() bool {
		if s.readOnly {
			status = 403
			writeRESPError(bw, "READONLY You can't write against a read only replica.")
			return true
		}
		return false
	}

	args := cmd.args
	switch cmd.name {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		// Atomic transaction blocks are deliberately unsupported: pretending
		// otherwise would silently lose updates. Pipelines (MULTI-free) work.
		writeRESPError(bw, "ERR MULTI/EXEC/WATCH are not supported; use a pipeline (redis-py: transaction=False)")
	case "TYPE":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'type' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		_, err := s.store.MetaHeaderKind(ctx, ns.Cache, b, k)
		if errors.Is(err, store.ErrNotFound) {
			writeRESPSimple(bw, "none")
		} else if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
		} else {
			writeRESPSimple(bw, "string")
		}
	case "SCAN":
		s.respScan(ctx, bw, sess, args)
	case "APPEND":
		if rejectRO() {
			return false
		}
		if len(args) != 2 {
			writeRESPError(bw, "ERR wrong number of arguments for 'append' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		cur, _, err := s.store.CacheGet(ctx, b, k)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		next := append(append([]byte(nil), cur...), args[1]...)
		if _, _, _, err := s.store.CacheSet(ctx, b, k, next, store.CacheSetOptions{KeepTTL: true}); err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, int64(len(next)))
	case "SETRANGE":
		if rejectRO() {
			return false
		}
		if len(args) != 3 {
			writeRESPError(bw, "ERR wrong number of arguments for 'setrange' command")
			return false
		}
		off, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || off < 0 {
			writeRESPError(bw, "ERR offset is out of range")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		cur, _, err := s.store.CacheGet(ctx, b, k)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if int64(len(cur)) < off {
			cur = append(cur, make([]byte, off-int64(len(cur)))...)
		}
		need := off + int64(len(args[2]))
		if int64(len(cur)) < need {
			cur = append(cur, make([]byte, need-int64(len(cur)))...)
		}
		copy(cur[off:], args[2])
		if _, _, _, err := s.store.CacheSet(ctx, b, k, cur, store.CacheSetOptions{KeepTTL: true}); err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, int64(len(cur)))
	case "PING":
		if len(args) == 0 {
			writeRESPSimple(bw, "PONG")
		} else {
			writeRESPBulk(bw, sess.proto, args[0])
		}
	case "ECHO":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'echo' command")
			return false
		}
		writeRESPBulk(bw, sess.proto, args[0])
	case "AUTH":
		writeRESPSimple(bw, "OK")
	case "QUIT":
		writeRESPSimple(bw, "OK")
		return true
	case "RESET":
		sess.bucket = DefaultCache
		sess.proto = 2
		writeRESPSimple(bw, "RESET")
	case "HELLO":
		if len(args) >= 1 {
			ver, err := strconv.Atoi(string(args[0]))
			if err != nil {
				writeRESPError(bw, "ERR Protocol version is not an integer or out of range")
				return false
			}
			if ver < 2 || ver > 3 {
				writeRESPError(bw, "NOPROTO sorry this protocol version is not supported")
				return false
			}
			sess.proto = ver
		}
		if sess.proto == 3 {
			_, _ = bw.WriteString("%7\r\n")
		} else {
			writeRESPArrayHeader(bw, 14)
		}
		writeRESPBulkString(bw, "server")
		writeRESPBulkString(bw, "pgvs3")
		writeRESPBulkString(bw, "version")
		writeRESPBulkString(bw, "0.2.0")
		writeRESPBulkString(bw, "proto")
		writeRESPInt(bw, int64(sess.proto))
		writeRESPBulkString(bw, "id")
		writeRESPInt(bw, 1)
		writeRESPBulkString(bw, "mode")
		writeRESPBulkString(bw, "standalone")
		writeRESPBulkString(bw, "role")
		writeRESPBulkString(bw, "master")
		writeRESPBulkString(bw, "modules")
		writeRESPArrayHeader(bw, 0)
	case "COMMAND":
		writeRESPArrayHeader(bw, 0)
	case "CLIENT":
		writeRESPSimple(bw, "OK")
	case "CONFIG":
		if len(args) >= 2 && strings.EqualFold(string(args[0]), "GET") {
			param := strings.ToLower(string(args[1]))
			switch param {
			case "save":
				writeRESPArrayHeader(bw, 2)
				writeRESPBulkString(bw, "save")
				writeRESPBulkString(bw, "")
			case "appendonly":
				writeRESPArrayHeader(bw, 2)
				writeRESPBulkString(bw, "appendonly")
				writeRESPBulkString(bw, "no")
			default:
				writeRESPArrayHeader(bw, 0)
			}
			return false
		}
		writeRESPSimple(bw, "OK")
	case "SELECT":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'select' command")
			return false
		}
		sel := string(args[0])
		target := sel
		if n, err := strconv.Atoi(sel); err == nil {
			if n < 0 || n > 15 {
				writeRESPError(bw, "ERR DB index is out of range")
				return false
			}
			if n == 0 {
				target = DefaultCache
			} else {
				target = fmt.Sprintf("%s-%d", DefaultCache, n)
			}
		}
		if !ns.ValidName(target) {
			writeRESPError(bw, "ERR invalid namespace name")
			return false
		}
		sess.bucket = target
		writeRESPSimple(bw, "OK")
	case "GET":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'get' command")
			return false
		}
		raw := string(args[0])
		b, k := resolveRESPTarget(sess.bucket, raw)
		var val []byte
		var ok bool
		var err error
		if strings.HasPrefix(raw, "s3://") {
			// Explicit cross-namespace read of a durable bucket.
			var sl *store.Slice
			sl, err = s.store.GetKind(ctx, ns.Bucket, b, k, 0, -1, -1)
			if sl != nil {
				val, ok = sl.Bytes, true
			}
		} else {
			val, ok, err = s.store.CacheGet(ctx, b, k)
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			status = 500
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if !ok {
			status = 404
			writeRESPNull(bw, sess.proto)
			return false
		}
		writeRESPBulk(bw, sess.proto, val)
	case "MGET":
		if len(args) < 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'mget' command")
			return false
		}
		keys := make([]string, len(args))
		for i, a := range args {
			_, keys[i] = resolveRESPTarget(sess.bucket, string(a))
		}
		vals, err := s.store.CacheMGet(ctx, sess.bucket, keys)
		if err != nil {
			status = 500
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPArrayHeader(bw, len(vals))
		for _, v := range vals {
			writeRESPBulk(bw, sess.proto, v)
		}
	case "SET":
		if rejectRO() {
			return false
		}
		if len(args) < 2 {
			writeRESPError(bw, "ERR wrong number of arguments for 'set' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		val := args[1]
		opts, err := parseRESPSetOptions(args[2:])
		if err != nil {
			status = 400
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		oldVal, hadOld, wrote, err := s.store.CacheSet(ctx, b, k, val, opts)
		if err != nil {
			status = 500
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if opts.ReturnOld {
			if !hadOld {
				writeRESPNull(bw, sess.proto)
			} else {
				writeRESPBulk(bw, sess.proto, oldVal)
			}
			return false
		}
		if !wrote {
			writeRESPNull(bw, sess.proto)
			return false
		}
		writeRESPSimple(bw, "OK")
	case "SETNX":
		if rejectRO() {
			return false
		}
		if len(args) != 2 {
			writeRESPError(bw, "ERR wrong number of arguments for 'setnx' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		_, _, wrote, err := s.store.CacheSet(ctx, b, k, args[1], store.CacheSetOptions{Condition: store.IfAbsent})
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if wrote {
			writeRESPInt(bw, 1)
		} else {
			writeRESPInt(bw, 0)
		}
	case "SETEX", "PSETEX":
		if rejectRO() {
			return false
		}
		if len(args) != 3 {
			writeRESPError(bw, "ERR wrong number of arguments for '"+strings.ToLower(cmd.name)+"' command")
			return false
		}
		n, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil || n <= 0 {
			writeRESPError(bw, "ERR invalid expire time in '"+strings.ToLower(cmd.name)+"' command")
			return false
		}
		unit := time.Second
		if cmd.name == "PSETEX" {
			unit = time.Millisecond
		}
		exp := time.Now().Add(time.Duration(n) * unit)
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		if _, _, _, err := s.store.CacheSet(ctx, b, k, args[2], store.CacheSetOptions{ExpiresAt: &exp}); err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPSimple(bw, "OK")
	case "GETSET":
		if rejectRO() {
			return false
		}
		if len(args) != 2 {
			writeRESPError(bw, "ERR wrong number of arguments for 'getset' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		oldVal, hadOld, _, err := s.store.CacheSet(ctx, b, k, args[1], store.CacheSetOptions{ReturnOld: true})
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if !hadOld {
			writeRESPNull(bw, sess.proto)
		} else {
			writeRESPBulk(bw, sess.proto, oldVal)
		}
	case "GETDEL":
		if rejectRO() {
			return false
		}
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'getdel' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		val, ok, err := s.store.CacheGet(ctx, b, k)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if !ok {
			writeRESPNull(bw, sess.proto)
			return false
		}
		_, _ = s.store.CacheDel(ctx, b, []string{k})
		writeRESPBulk(bw, sess.proto, val)
	case "MSET":
		if rejectRO() {
			return false
		}
		if len(args) == 0 || len(args)%2 != 0 {
			writeRESPError(bw, "ERR wrong number of arguments for 'mset' command")
			return false
		}
		n := len(args) / 2
		keys := make([]string, n)
		vals := make([][]byte, n)
		sameBucket := true
		b0 := sess.bucket
		for i := 0; i < n; i++ {
			b, k := resolveRESPTarget(sess.bucket, string(args[2*i]))
			if b != sess.bucket {
				writeRESPError(bw, "ERR cross-namespace write denied")
				return false
			}
			if i == 0 {
				b0 = b
			} else if b != b0 {
				sameBucket = false
			}
			keys[i], vals[i] = k, args[2*i+1]
		}
		if sameBucket {
			if err := s.store.CacheMSet(ctx, b0, keys, vals); err != nil {
				writeRESPError(bw, "ERR "+err.Error())
				return false
			}
		} else {
			for i := 0; i < n; i++ {
				b, k := resolveRESPTarget(sess.bucket, string(args[2*i]))
				if _, _, _, err := s.store.CacheSet(ctx, b, k, vals[i], store.CacheSetOptions{}); err != nil {
					writeRESPError(bw, "ERR "+err.Error())
					return false
				}
			}
		}
		writeRESPSimple(bw, "OK")
	case "DEL", "UNLINK":
		if rejectRO() {
			return false
		}
		if len(args) < 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'del' command")
			return false
		}
		keys := make([]string, len(args))
		for i, a := range args {
			kb, kk := resolveRESPTarget(sess.bucket, string(a))
			if kb != sess.bucket {
				writeRESPError(bw, "ERR cross-namespace write denied")
				return false
			}
			keys[i] = kk
		}
		n, err := s.store.CacheDel(ctx, sess.bucket, keys)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, n)
	case "EXISTS":
		if len(args) < 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'exists' command")
			return false
		}
		keys := make([]string, len(args))
		for i, a := range args {
			_, keys[i] = resolveRESPTarget(sess.bucket, string(a))
		}
		n, err := s.store.CacheExists(ctx, sess.bucket, keys)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, n)
	case "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT":
		if rejectRO() {
			return false
		}
		if len(args) < 2 {
			writeRESPError(bw, "ERR wrong number of arguments for '"+strings.ToLower(cmd.name)+"' command")
			return false
		}
		n, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			writeRESPError(bw, "ERR value is not an integer or out of range")
			return false
		}
		var exp time.Time
		switch cmd.name {
		case "EXPIRE":
			exp = time.Now().Add(time.Duration(n) * time.Second)
		case "PEXPIRE":
			exp = time.Now().Add(time.Duration(n) * time.Millisecond)
		case "EXPIREAT":
			exp = time.Unix(n, 0)
		case "PEXPIREAT":
			exp = time.UnixMilli(n)
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		curTTL, hasTTL, exists, err := s.store.CacheTTL(ctx, b, k)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if !exists {
			writeRESPInt(bw, 0)
			return false
		}
		want := time.Until(exp)
		for _, opt := range args[2:] {
			switch strings.ToUpper(string(opt)) {
			case "NX":
				if hasTTL {
					writeRESPInt(bw, 0)
					return false
				}
			case "XX":
				if !hasTTL {
					writeRESPInt(bw, 0)
					return false
				}
			case "GT":
				if !hasTTL || want <= curTTL {
					writeRESPInt(bw, 0)
					return false
				}
			case "LT":
				if hasTTL && want >= curTTL {
					writeRESPInt(bw, 0)
					return false
				}
			}
		}
		ok, err := s.store.CacheExpire(ctx, b, k, &exp)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if ok {
			writeRESPInt(bw, 1)
		} else {
			writeRESPInt(bw, 0)
		}
	case "PERSIST":
		if rejectRO() {
			return false
		}
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'persist' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		ok, err := s.store.CacheExpire(ctx, b, k, nil)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if ok {
			writeRESPInt(bw, 1)
		} else {
			writeRESPInt(bw, 0)
		}
	case "TTL", "PTTL":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for '"+strings.ToLower(cmd.name)+"' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		d, hasTTL, exists, err := s.store.CacheTTL(ctx, b, k)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if !exists {
			writeRESPInt(bw, -2)
			return false
		}
		if !hasTTL {
			writeRESPInt(bw, -1)
			return false
		}
		if cmd.name == "PTTL" {
			writeRESPInt(bw, max(d.Milliseconds(), 1))
		} else {
			writeRESPInt(bw, max(int64(d.Seconds()+0.5), 1))
		}
	case "INCR", "DECR", "INCRBY", "DECRBY":
		if rejectRO() {
			return false
		}
		var delta int64 = 1
		if cmd.name == "DECR" {
			delta = -1
		}
		if cmd.name == "INCRBY" || cmd.name == "DECRBY" {
			if len(args) != 2 {
				writeRESPError(bw, "ERR wrong number of arguments for '"+strings.ToLower(cmd.name)+"' command")
				return false
			}
			v, err := strconv.ParseInt(string(args[1]), 10, 64)
			if err != nil {
				writeRESPError(bw, "ERR value is not an integer or out of range")
				return false
			}
			if cmd.name == "DECRBY" {
				delta = -v
			} else {
				delta = v
			}
		} else if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for '"+strings.ToLower(cmd.name)+"' command")
			return false
		}
		b, k := resolveRESPTarget(sess.bucket, string(args[0]))
		if b != sess.bucket {
			writeRESPError(bw, "ERR cross-namespace write denied")
			return false
		}
		next, err := s.store.CacheIncrBy(ctx, b, k, delta)
		if errors.Is(err, store.ErrNotInteger) {
			writeRESPError(bw, "ERR value is not an integer or out of range")
			return false
		}
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, next)
	case "STRLEN":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'strlen' command")
			return false
		}
		raw := string(args[0])
		b, k := resolveRESPTarget(sess.bucket, raw)
		kind := ns.Cache
		if strings.HasPrefix(raw, "s3://") {
			kind = ns.Bucket
		}
		m, err := s.store.MetaHeaderKind(ctx, kind, b, k)
		if errors.Is(err, store.ErrNotFound) {
			writeRESPInt(bw, 0)
			return false
		}
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, m.Size)
	case "GETRANGE", "SUBSTR":
		if len(args) != 3 {
			writeRESPError(bw, "ERR wrong number of arguments for 'getrange' command")
			return false
		}
		startIdx, err1 := strconv.ParseInt(string(args[1]), 10, 64)
		endIdx, err2 := strconv.ParseInt(string(args[2]), 10, 64)
		if err1 != nil || err2 != nil {
			writeRESPError(bw, "ERR value is not an integer or out of range")
			return false
		}
		raw := string(args[0])
		b, k := resolveRESPTarget(sess.bucket, raw)
		kind := ns.Cache
		if strings.HasPrefix(raw, "s3://") {
			kind = ns.Bucket
		}
		m, err := s.store.MetaHeaderKind(ctx, kind, b, k)
		if errors.Is(err, store.ErrNotFound) || (err == nil && m.Size == 0) {
			writeRESPBulk(bw, sess.proto, []byte{})
			return false
		}
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		if startIdx < 0 {
			startIdx = max(m.Size+startIdx, 0)
		}
		if endIdx < 0 {
			endIdx = m.Size + endIdx
		}
		if startIdx > endIdx || startIdx >= m.Size || endIdx < 0 {
			writeRESPBulk(bw, sess.proto, []byte{})
			return false
		}
		endIdx = min(endIdx, m.Size-1)
		sl, err := s.store.GetKind(ctx, kind, b, k, startIdx, endIdx, -1)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPBulk(bw, sess.proto, sl.Bytes)
	case "DBSIZE":
		n, err := s.store.CacheDBSize(ctx, sess.bucket)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPInt(bw, n)
	case "FLUSHDB", "FLUSHALL":
		if rejectRO() {
			return false
		}
		if err := s.store.CacheFlush(ctx, sess.bucket); err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return false
		}
		writeRESPSimple(bw, "OK")
	case "KEYS":
		if len(args) != 1 {
			writeRESPError(bw, "ERR wrong number of arguments for 'keys' command")
			return false
		}
		pat := string(args[0])
		prefix := literalPrefix(pat)
		var matched []string
		after := ""
		for {
			rows, err := s.store.List(ctx, sess.bucket, prefix, prefixUpper(prefix), after, 1024)
			if err != nil {
				writeRESPError(bw, "ERR "+err.Error())
				return false
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
		writeRESPArrayHeader(bw, len(matched))
		for _, k := range matched {
			writeRESPBulkString(bw, k)
		}
	case "INFO":
		st, _ := s.store.StatBucket(ctx, sess.bucket)
		info := fmt.Sprintf("# Server\r\nredis_version:6.2.0\r\npgvs3_version:0.3.0\r\nredis_mode:standalone\r\n# Keyspace\r\ndb0:keys=%d,logical_bytes=%d,physical_bytes=%d\r\n",
			st.Objects, st.LogicalBytes, st.PhysicalBytes)
		writeRESPBulkString(bw, info)
	default:
		status = 400
		writeRESPError(bw, "ERR unknown command '"+strings.ToLower(cmd.name)+"'")
	}
	return false
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

func readRESPCommand(br *bufio.Reader) (respCmd, error) {
	b, err := br.Peek(1)
	if err != nil {
		return respCmd{}, err
	}
	if b[0] != '*' {
		line, err := readRESPLine(br)
		if err != nil {
			return respCmd{}, err
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return respCmd{name: "PING"}, nil
		}
		args := make([][]byte, len(fields)-1)
		for i := 1; i < len(fields); i++ {
			args[i-1] = []byte(fields[i])
		}
		return respCmd{name: strings.ToUpper(fields[0]), args: args}, nil
	}
	header, err := readRESPLine(br)
	if err != nil {
		return respCmd{}, err
	}
	argc, err := strconv.Atoi(header[1:])
	if err != nil || argc < 1 || argc > 100000 {
		return respCmd{}, fmt.Errorf("Protocol error: invalid multibulk length")
	}
	args := make([][]byte, argc)
	for i := 0; i < argc; i++ {
		line, err := readRESPLine(br)
		if err != nil {
			return respCmd{}, err
		}
		if len(line) == 0 || line[0] != '$' {
			return respCmd{}, fmt.Errorf("Protocol error: expected '$'")
		}
		n, err := strconv.Atoi(line[1:])
		if err != nil || n < 0 || n > 512<<20 {
			return respCmd{}, fmt.Errorf("Protocol error: invalid bulk length")
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return respCmd{}, err
		}
		args[i] = buf[:n]
	}
	return respCmd{name: strings.ToUpper(string(args[0])), args: args[1:]}, nil
}

func readRESPLine(br *bufio.Reader) (string, error) {
	s, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}

func writeRESPSimple(bw *bufio.Writer, s string) {
	_, _ = bw.WriteString("+" + s + "\r\n")
}

func writeRESPError(bw *bufio.Writer, msg string) {
	_, _ = bw.WriteString("-" + msg + "\r\n")
}

func writeRESPInt(bw *bufio.Writer, n int64) {
	_, _ = bw.WriteString(":" + strconv.FormatInt(n, 10) + "\r\n")
}

func writeRESPNull(bw *bufio.Writer, proto int) {
	if proto >= 3 {
		_, _ = bw.WriteString("_\r\n")
		return
	}
	_, _ = bw.WriteString("$-1\r\n")
}

func writeRESPBulk(bw *bufio.Writer, proto int, b []byte) {
	if b == nil {
		writeRESPNull(bw, proto)
		return
	}
	_, _ = bw.WriteString("$" + strconv.Itoa(len(b)) + "\r\n")
	_, _ = bw.Write(b)
	_, _ = bw.WriteString("\r\n")
}

func writeRESPBulkString(bw *bufio.Writer, s string) {
	writeRESPBulk(bw, 2, []byte(s))
}

func writeRESPArrayHeader(bw *bufio.Writer, n int) {
	_, _ = bw.WriteString("*" + strconv.Itoa(n) + "\r\n")
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

// respScan implements SCAN cursor/count/match/type over the cache namespace.
func (s *Server) respScan(ctx context.Context, bw *bufio.Writer, sess *respSession, args [][]byte) {
	if len(args) < 1 {
		writeRESPError(bw, "ERR wrong number of arguments for 'scan' command")
		return
	}
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
			// Only strings exist; a non-string TYPE yields no keys.
			if !strings.EqualFold(string(args[i+1]), "string") {
				writeRESPArrayHeader(bw, 2)
				writeRESPBulkString(bw, "0")
				writeRESPArrayHeader(bw, 0)
				return
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
		rows, err := s.store.List(ctx, sess.bucket, prefix, prefixUpper(prefix), after, limit)
		if err != nil {
			writeRESPError(bw, "ERR "+err.Error())
			return
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
	writeRESPArrayHeader(bw, 2)
	writeRESPBulkString(bw, next)
	writeRESPArrayHeader(bw, len(keys))
	for _, k := range keys {
		writeRESPBulkString(bw, k)
	}
}
