// Command sol measures single-stream pgvs3 object I/O against the floors that
// bound it on the same host: a loopback HTTP server that only moves bytes, and
// direct PostgreSQL COPY/SELECT of the identical 8,120-byte chunk rows. A
// perfectly pipelined gateway approaches min(HTTP floor, PostgreSQL floor).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const rowBytes = 8120

type sample struct {
	Size     int64   `json:"size"`
	Op       string  `json:"op"`
	Impl     string  `json:"impl"`
	Bytes    int64   `json:"bytes"`
	Seconds  float64 `json:"seconds"`
	MiBps    float64 `json:"mib_per_s"`
	CPUNsPer float64 `json:"gateway_cpu_ns_per_byte,omitempty"`
}

type row struct {
	Size         int64   `json:"size"`
	Op           string  `json:"op"`
	PGVS3        float64 `json:"pgvs3_mib_per_s"`
	HTTPFloor    float64 `json:"http_floor_mib_per_s"`
	PGFloor      float64 `json:"pg_floor_mib_per_s"`
	PGOneConn    float64 `json:"pg_one_connection_mib_per_s,omitempty"`
	Floor        float64 `json:"floor_mib_per_s"`
	PercentFloor float64 `json:"percent_of_floor"`
	CPUNsPerByte float64 `json:"gateway_cpu_ns_per_byte"`
}

type report struct {
	Started   time.Time `json:"started"`
	Endpoint  string    `json:"endpoint"`
	Repeat    int       `json:"repeat"`
	Readers   int       `json:"pg_floor_readers"`
	RangeSize int64     `json:"range_size"`
	Payload   string    `json:"payload"`
	Method    string    `json:"method"`
	Rows      []row     `json:"rows"`
	Samples   []sample  `json:"samples"`
}

type config struct {
	endpoint, db, out string
	sizes             []int64
	repeat, readers   int
	rangeSize         int64
	gatewayPID        int
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx := context.Background()
	rep, err := run(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sol:", err)
		os.Exit(1)
	}
	fmt.Print(markdown(rep))
	if cfg.out != "" {
		body, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(cfg.out, append(body, '\n'), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "sol:", err)
			os.Exit(1)
		}
	}
}

func parseFlags(args []string) (config, error) {
	var cfg config
	var sizes, rangeSize string
	fs := flag.NewFlagSet("sol", flag.ContinueOnError)
	fs.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:8014", "pgvs3 gateway URL")
	fs.StringVar(&cfg.db, "db", "", "PostgreSQL URL of the gateway's database")
	fs.StringVar(&sizes, "sizes", "10MiB,256MiB,1GiB", "comma-separated object sizes")
	fs.StringVar(&rangeSize, "range", "8MiB", "range GET size")
	fs.IntVar(&cfg.repeat, "repeat", 3, "timed trials per measurement (median reported)")
	fs.IntVar(&cfg.readers, "readers", 8, "PostgreSQL floor read connections (pgvs3 streams up to 8 pieces)")
	fs.IntVar(&cfg.gatewayPID, "gateway-pid", 0, "gateway process ID for CPU accounting (0 skips)")
	fs.StringVar(&cfg.out, "out", "", "write JSON report here")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if cfg.db == "" {
		return cfg, errors.New("-db is required")
	}
	for s := range strings.SplitSeq(sizes, ",") {
		n, err := parseBytes(s)
		if err != nil {
			return cfg, err
		}
		cfg.sizes = append(cfg.sizes, n)
	}
	n, err := parseBytes(rangeSize)
	if err != nil {
		return cfg, err
	}
	cfg.rangeSize = n
	if cfg.repeat < 1 || cfg.readers < 1 {
		return cfg, errors.New("-repeat and -readers must be positive")
	}
	return cfg, nil
}

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.ParseInt(num, 10, 64)
			if err != nil || n <= 0 {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return n * u.mult, nil
		}
	}
	return 0, fmt.Errorf("size %q needs a B/KiB/MiB/GiB suffix", s)
}

func run(ctx context.Context, cfg config) (report, error) {
	rep := report{
		Started: time.Now().UTC(), Endpoint: cfg.endpoint, Repeat: cfg.repeat, Readers: cfg.readers,
		RangeSize: cfg.rangeSize, Payload: "incompressible (ChaCha8)",
		Method: "single client stream; warm caches; median of interleaved trials; floor = min(HTTP floor, PostgreSQL floor)",
	}
	maxSize := slices.Max(cfg.sizes)
	payload := make([]byte, maxSize)
	_, _ = rand.NewChaCha8([32]byte{'p', 'g', 'v', 's', '3'}).Read(payload)

	floorURL, stopFloor, err := startHTTPFloor(payload)
	if err != nil {
		return rep, err
	}
	defer stopFloor()

	pool, err := pgxpool.New(ctx, cfg.db)
	if err != nil {
		return rep, err
	}
	defer pool.Close()
	if err := setupFloorTable(ctx, pool); err != nil {
		return rep, err
	}
	defer func() { _, _ = pool.Exec(context.WithoutCancel(ctx), "DROP SCHEMA IF EXISTS sol_floor CASCADE") }()

	client := &http.Client{Transport: &http.Transport{DisableCompression: true, MaxIdleConnsPerHost: 4}}
	if err := httpDo(client, http.MethodPut, cfg.endpoint+"/sol", nil, nil); err != nil {
		var status statusError
		if !errors.As(err, &status) || status != http.StatusConflict {
			return rep, fmt.Errorf("create bucket: %w", err)
		}
	}

	var fileID int64
	for _, size := range cfg.sizes {
		body := payload[:size]
		key := fmt.Sprintf("%s/sol/object-%d", cfg.endpoint, size)
		rangeLen := min(cfg.rangeSize, size)
		rangeOff := (size - rangeLen) / 3
		rangeHdr := http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", rangeOff, rangeOff+rangeLen-1)}}
		floorRead := fileID + 1

		measure := func(op, impl string, n int64, gateway bool, fn func() error) error {
			before := cpuNanos(cfg.gatewayPID, gateway)
			start := time.Now()
			if err := fn(); err != nil {
				return fmt.Errorf("%s %s %d: %w", op, impl, size, err)
			}
			secs := time.Since(start).Seconds()
			s := sample{Size: size, Op: op, Impl: impl, Bytes: n, Seconds: secs, MiBps: float64(n) / secs / (1 << 20)}
			if after := cpuNanos(cfg.gatewayPID, gateway); gateway && after >= before && before >= 0 {
				s.CPUNsPer = float64(after-before) / float64(n)
			}
			rep.Samples = append(rep.Samples, s)
			return nil
		}

		for trial := range cfg.repeat {
			if err := measure("put", "pgvs3", size, true, func() error {
				return httpDo(client, http.MethodPut, key, body, nil)
			}); err != nil {
				return rep, err
			}
			if err := measure("put", "http", size, false, func() error {
				return httpDo(client, http.MethodPut, floorURL+"/put", body, nil)
			}); err != nil {
				return rep, err
			}
			fileID++
			id := fileID
			if err := measure("put", "pg", size, false, func() error { return copyChunks(ctx, pool, id, body) }); err != nil {
				return rep, err
			}
			if trial == 0 {
				// Untimed warm-up so every read below compares warm caches.
				_ = httpGet(client, key, nil)
				_ = readChunks(ctx, pool, floorRead, 0, size, cfg.readers)
			}
		}
		reads := []struct {
			op     string
			off, n int64
			hdr    http.Header
		}{{"get", 0, size, nil}, {"range", rangeOff, rangeLen, rangeHdr}}
		for _, r := range reads {
			readers := cfg.readers
			for range cfg.repeat {
				if err := measure(r.op, "pgvs3", r.n, true, func() error { return checkLen(httpGet(client, key, r.hdr), r.n) }); err != nil {
					return rep, err
				}
				floorHdr := http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", r.off, r.off+r.n-1)}}
				if err := measure(r.op, "http", r.n, false, func() error {
					return checkLen(httpGet(client, fmt.Sprintf("%s/get?n=%d", floorURL, size), floorHdr), r.n)
				}); err != nil {
					return rep, err
				}
				if err := measure(r.op, "pg", r.n, false, func() error { return readChunks(ctx, pool, floorRead, r.off, r.n, readers) }); err != nil {
					return rep, err
				}
				if readers > 1 {
					if err := measure(r.op, "pg-1conn", r.n, false, func() error { return readChunks(ctx, pool, floorRead, r.off, r.n, 1) }); err != nil {
						return rep, err
					}
				}
			}
		}
		_ = httpDo(client, http.MethodDelete, key, nil, nil)
		if _, err := pool.Exec(ctx, "DELETE FROM sol_floor.chunks WHERE file_id <> $1", floorRead); err != nil {
			return rep, err
		}
	}
	rep.Rows = summarize(rep.Samples, cfg.sizes)
	return rep, nil
}

func summarize(samples []sample, sizes []int64) []row {
	median := func(size int64, op, impl string, field func(sample) float64) float64 {
		var v []float64
		for _, s := range samples {
			if s.Size == size && s.Op == op && s.Impl == impl {
				v = append(v, field(s))
			}
		}
		if len(v) == 0 {
			return 0
		}
		slices.Sort(v)
		return v[len(v)/2]
	}
	rate := func(s sample) float64 { return s.MiBps }
	cpu := func(s sample) float64 { return s.CPUNsPer }
	var rows []row
	for _, size := range sizes {
		for _, op := range []string{"put", "get", "range"} {
			r := row{Size: size, Op: op,
				PGVS3: median(size, op, "pgvs3", rate), HTTPFloor: median(size, op, "http", rate),
				PGFloor: median(size, op, "pg", rate), PGOneConn: median(size, op, "pg-1conn", rate),
				CPUNsPerByte: median(size, op, "pgvs3", cpu)}
			r.Floor = min(r.HTTPFloor, r.PGFloor)
			if r.Floor > 0 {
				r.PercentFloor = 100 * r.PGVS3 / r.Floor
			}
			rows = append(rows, r)
		}
	}
	return rows
}

func markdown(rep report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "| Size | Op | pgvs3 MiB/s | HTTP floor | PG floor | %% of floor | gateway CPU ns/B |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, r := range rep.Rows {
		pg := fmt.Sprintf("%.0f", r.PGFloor)
		if r.PGOneConn > 0 {
			pg += fmt.Sprintf(" (1 conn %.0f)", r.PGOneConn)
		}
		fmt.Fprintf(&b, "| %s | %s | %.0f | %.0f | %s | %.0f%% | %.2f |\n",
			human(r.Size), r.Op, r.PGVS3, r.HTTPFloor, pg, r.PercentFloor, r.CPUNsPerByte)
	}
	return b.String()
}

func human(n int64) string {
	switch {
	case n%(1<<30) == 0:
		return fmt.Sprintf("%d GiB", n>>30)
	case n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func startHTTPFloor(payload []byte) (string, func(), error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /put", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	})
	mux.HandleFunc("GET /get", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
		lo, hi := int64(0), n-1
		if spec, ok := strings.CutPrefix(r.Header.Get("Range"), "bytes="); ok {
			a, b, _ := strings.Cut(spec, "-")
			lo, _ = strconv.ParseInt(a, 10, 64)
			hi, _ = strconv.ParseInt(b, 10, 64)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", lo, hi, n))
			w.Header().Set("Content-Length", strconv.FormatInt(hi-lo+1, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
		}
		_, _ = w.Write(payload[lo : hi+1])
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	return "http://" + ln.Addr().String(), func() { _ = srv.Close() }, nil
}

type statusError int

func (e statusError) Error() string { return "HTTP " + strconv.Itoa(int(e)) }

func httpDo(c *http.Client, method, url string, body []byte, hdr http.Header) error {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return err
	}
	maps.Copy(req.Header, hdr)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return statusError(resp.StatusCode)
	}
	return nil
}

type readResult struct {
	n   int64
	err error
}

func httpGet(c *http.Client, url string, hdr http.Header) readResult {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return readResult{err: err}
	}
	maps.Copy(req.Header, hdr)
	resp, err := c.Do(req)
	if err != nil {
		return readResult{err: err}
	}
	defer resp.Body.Close()
	n, err := io.Copy(io.Discard, resp.Body)
	if err == nil && resp.StatusCode >= 300 {
		err = statusError(resp.StatusCode)
	}
	return readResult{n: n, err: err}
}

func checkLen(r readResult, want int64) error {
	if r.err != nil {
		return r.err
	}
	if r.n != want {
		return fmt.Errorf("read %d bytes, want %d", r.n, want)
	}
	return nil
}

func setupFloorTable(ctx context.Context, pool *pgxpool.Pool) error {
	// Same columns, storage and reloptions as s3b_<name>.chunks in internal/store/schema.sql.
	_, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS sol_floor CASCADE;
		CREATE SCHEMA sol_floor;
		CREATE TABLE sol_floor.chunks (
			file_id int8 NOT NULL,
			no      int4 NOT NULL,
			data    bytea STORAGE EXTERNAL NOT NULL,
			PRIMARY KEY (file_id, no),
			CONSTRAINT chunk_shape CHECK (no >= 0 AND octet_length(data) BETWEEN 1 AND 8120)
		) WITH (toast_tuple_target = 8160, autovacuum_vacuum_scale_factor = 0.01,
			autovacuum_analyze_scale_factor = 0.02, autovacuum_vacuum_threshold = 1000)`)
	return err
}

type chunkSource struct {
	fileID int64
	body   []byte
	no     int
}

func (c *chunkSource) Next() bool {
	if c.no*rowBytes >= len(c.body) {
		return false
	}
	c.no++
	return true
}

func (c *chunkSource) Err() error { return nil }
func (c *chunkSource) Values() ([]any, error) {
	lo := (c.no - 1) * rowBytes
	return []any{c.fileID, int32(c.no - 1), c.body[lo:min(lo+rowBytes, len(c.body))]}, nil
}

func copyChunks(ctx context.Context, pool *pgxpool.Pool, fileID int64, body []byte) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"sol_floor", "chunks"}, []string{"file_id", "no", "data"},
		&chunkSource{fileID: fileID, body: body}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func readChunks(ctx context.Context, pool *pgxpool.Pool, fileID, off, n int64, readers int) error {
	first, last := off/rowBytes, (off+n-1)/rowBytes
	span := last - first + 1
	readers = int(min(int64(readers), span))
	var wg sync.WaitGroup
	errs := make([]error, readers)
	for i := range readers {
		lo := first + span*int64(i)/int64(readers)
		hi := first + span*int64(i+1)/int64(readers) - 1
		wg.Go(func() {
			rows, err := pool.Query(ctx, "SELECT data FROM sol_floor.chunks WHERE file_id = $1 AND no BETWEEN $2 AND $3 ORDER BY no", fileID, lo, hi)
			if err != nil {
				errs[i] = err
				return
			}
			defer rows.Close()
			var got int64
			for rows.Next() {
				got += int64(len(rows.RawValues()[0]))
			}
			if err := rows.Err(); err != nil {
				errs[i] = err
			} else if got == 0 {
				errs[i] = fmt.Errorf("no rows for file %d [%d,%d]", fileID, lo, hi)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// cpuNanos sums on-CPU nanoseconds across every thread of pid (schedstat), or -1.
func cpuNanos(pid int, enabled bool) int64 {
	if !enabled || pid <= 0 {
		return -1
	}
	tasks, err := filepath.Glob(fmt.Sprintf("/proc/%d/task/*/schedstat", pid))
	if err != nil || len(tasks) == 0 {
		return -1
	}
	var total int64
	for _, t := range tasks {
		raw, err := os.ReadFile(t)
		if err != nil {
			continue
		}
		field, _, _ := strings.Cut(string(raw), " ")
		v, _ := strconv.ParseInt(field, 10, 64)
		total += v
	}
	return total
}
