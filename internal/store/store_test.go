package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestValidWorkMem(t *testing.T) {
	for _, v := range []string{"4MB", "16MB", "1GB", "512kB", "16mb"} {
		if !validWorkMem(v) {
			t.Errorf("validWorkMem(%q) = false", v)
		}
	}
	for _, v := range []string{"", "16", "MB", "16 MB", "16MB; DROP TABLE x", "0MB", "16XB", "12345678901234567MB"} {
		if validWorkMem(v) {
			t.Errorf("validWorkMem(%q) = true", v)
		}
	}
}

func TestDefaultOpenOptions(t *testing.T) {
	o := defaultOpenOptions(4)
	if o.MaxConns != 4 || o.WorkMem != "16MB" || o.StatementTimeout != 5*time.Minute ||
		o.LockTimeout != 5*time.Second || o.IdleTxTimeout != time.Minute ||
		o.MaxConnLifetime != time.Hour || o.MaxConnLifetimeJitter != 5*time.Minute ||
		o.MinIdleConns != 4 {
		t.Fatalf("defaults = %+v", o)
	}
}

func TestWriteGateBoundsConcurrentWriters(t *testing.T) {
	s := &Store{writeGate: make(chan struct{}, 2)}
	ctx := context.Background()
	for range 2 {
		if err := s.acquireWriter(ctx); err != nil {
			t.Fatal(err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.acquireWriter(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked acquire = %v, want context.Canceled", err)
	}
	s.releaseWriter()
	if err := s.acquireWriter(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		s.releaseWriter()
	}

	var unguarded Store
	if err := unguarded.acquireWriter(ctx); err != nil {
		t.Fatalf("unguarded store acquire = %v", err)
	}
	unguarded.releaseWriter()
}

func TestPublicationIntents(t *testing.T) {
	s := &Store{}
	cache := ObjectRef{NS: ns.CacheRef("cache"), Key: "key"}
	write := ObjectRef{NS: ns.WriteBackRef("cache"), Key: "key"}
	bucket := ObjectRef{NS: ns.BucketRef("bucket"), Key: "key"}
	lease := FillLease{Namespace: "cache", Key: "key", Owner: "first"}
	fill := FillPublication("v2", "v1", &lease)
	lease.Owner = "changed"
	if fill.lease.Owner != "first" {
		t.Fatal("publication retained a mutable caller lease")
	}
	pending := WriteBackPublication(PendingOrigin{Token: PendingOriginToken("upload"), BaseMissing: true})
	for _, tc := range []struct {
		name   string
		ref    ObjectRef
		intent Publication
		inline bool
		valid  bool
	}{
		{"ordinary", bucket, NormalPublication(""), true, true},
		{"reserved pending version", cache, NormalPublication(PendingOriginToken("x")), true, false},
		{"reserved delete version", cache, NormalPublication(DeletedCacheVersion("x").String()), true, false},
		{"fenced fill", cache, fill, true, true},
		{"fill into bucket", bucket, fill, true, false},
		{"wrong lease", cache, FillPublication("v", "v", &FillLease{Namespace: "other", Key: "key", Owner: "owner"}), true, false},
		{"write-back", write, pending, false, true},
		{"inline write-back", write, pending, true, false},
		{"bucket write-back", bucket, pending, false, false},
		{"empty token", write, WriteBackPublication(PendingOrigin{}), false, false},
		{"contradictory base", write, WriteBackPublication(PendingOrigin{Token: PendingOriginToken("x"), BaseMissing: true, BaseETag: "v"}), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expires := time.Now().Add(time.Hour)
			opts, err := s.prepareWrite(tc.ref, WriteOptions{Publication: tc.intent, ExpiresAt: &expires, KeepTTL: true}, tc.inline)
			if tc.valid && err != nil || !tc.valid && !errors.Is(err, ErrInvalidPublication) {
				t.Fatalf("valid=%t error=%v", tc.valid, err)
			}
			if tc.valid && tc.intent.IsPending() && (opts.ExpiresAt != nil || opts.KeepTTL) {
				t.Fatal("pending publication can expire")
			}
		})
	}
}

func TestSchemaBaselineIsSquashed(t *testing.T) {
	if strings.Contains(schemaSQL, "goose") || strings.Contains(schemaSQL, "migrate_v10") {
		t.Fatal("schema baseline contains migration machinery")
	}
	want := fmt.Sprintf("INSERT INTO s3p.layout (version) VALUES (%d);", LayoutVersion)
	if !strings.Contains(schemaSQL, want) {
		t.Fatalf("schema baseline does not record layout %d", LayoutVersion)
	}
	for _, relation := range layoutManifest.relations {
		short := strings.TrimPrefix(relation, "s3p.")
		if !strings.Contains(schemaSQL, "CREATE TABLE s3p."+short) &&
			!strings.Contains(schemaSQL, "CREATE SEQUENCE s3p."+short) {
			t.Errorf("schema baseline is missing %s", relation)
		}
	}
	for _, function := range layoutManifest.functions {
		name := function.signature
		if i := strings.IndexByte(name, '('); i >= 0 {
			name = name[:i]
		}
		if !strings.Contains(schemaSQL, "FUNCTION s3p."+name+"(") {
			t.Errorf("schema baseline is missing function %s", function.signature)
		}
	}
}

func TestRunMaintenancePassesOrderAndBudget(t *testing.T) {
	type invocation struct {
		name   string
		budget int
	}
	var got []invocation
	passes := []maintenancePass{
		{name: "expire", budget: 128, run: func(_ context.Context, budget int) error {
			got = append(got, invocation{name: "expire", budget: budget})
			return nil
		}},
		{name: "reap", budget: ReapRows, run: func(_ context.Context, budget int) error {
			got = append(got, invocation{name: "reap", budget: budget})
			return nil
		}},
	}
	if err := runMaintenancePasses(context.Background(), passes); err != nil {
		t.Fatal(err)
	}
	want := []invocation{{name: "expire", budget: 128}, {name: "reap", budget: ReapRows}}
	if len(got) != len(want) {
		t.Fatalf("invocations = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("invocation %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestRunMaintenancePassesStopsOnError(t *testing.T) {
	boom := errors.New("boom")
	var ran []string
	passes := []maintenancePass{
		{name: "first", budget: 1, run: func(context.Context, int) error {
			ran = append(ran, "first")
			return nil
		}},
		{name: "second", budget: 2, run: func(context.Context, int) error {
			ran = append(ran, "second")
			return boom
		}},
		{name: "third", budget: 3, run: func(context.Context, int) error {
			ran = append(ran, "third")
			return nil
		}},
	}
	if err := runMaintenancePasses(context.Background(), passes); !errors.Is(err, boom) {
		t.Fatalf("error = %v, want boom", err)
	}
	if len(ran) != 2 || ran[0] != "first" || ran[1] != "second" {
		t.Fatalf("ran = %v", ran)
	}
}

func TestMaintenancePassPlan(t *testing.T) {
	s := &Store{}
	passes := s.maintenancePasses(nil, nil, time.Minute)
	if len(passes) != 2 {
		t.Fatalf("passes = %+v", passes)
	}
	if passes[0].name != "expire" || passes[0].budget != expirePassRounds {
		t.Fatalf("first pass = %+v", passes[0])
	}
	if passes[1].name != "reap" || passes[1].budget != ReapRows {
		t.Fatalf("second pass = %+v", passes[1])
	}
}

func TestReadConditionCheck(t *testing.T) {
	tests := []struct {
		name    string
		cond    ReadCondition
		exists  bool
		etag    string
		wantErr error
	}{
		{"unconditional", ReadCondition{}, false, "", nil},
		{"match", ReadCondition{IfMatch: "a"}, true, "a", nil},
		{"match-quoted", ReadCondition{IfMatch: `"a"`}, true, "a", nil},
		{"match-missing", ReadCondition{IfMatch: "a"}, false, "", ErrPrecondition},
		{"match-wrong", ReadCondition{IfMatch: "a"}, true, "b", ErrPrecondition},
		{"none-match", ReadCondition{IfNoneMatch: "a"}, true, "a", ErrNotModified},
		{"none-wrong", ReadCondition{IfNoneMatch: "a"}, true, "b", nil},
		{"none-missing", ReadCondition{IfNoneMatch: "a"}, false, "", nil},
		{"star-match", ReadCondition{IfMatch: "*"}, true, "a", nil},
		{"star-match-missing", ReadCondition{IfMatch: "*"}, false, "", ErrPrecondition},
		{"star-none-match", ReadCondition{IfNoneMatch: "*"}, true, "a", ErrNotModified},
		{"star-none-missing", ReadCondition{IfNoneMatch: "*"}, false, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cond.Check(tt.exists, tt.etag)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Check(%v, %q) = %v, want %v", tt.exists, tt.etag, err, tt.wantErr)
			}
			if got, want := tt.cond.Allows(tt.exists, tt.etag), tt.wantErr == nil; got != want {
				t.Fatalf("Allows(%v, %q) = %v, want %v", tt.exists, tt.etag, got, want)
			}
		})
	}
}

func TestETagMatching(t *testing.T) {
	for _, tc := range []struct {
		header, etag string
		want         bool
	}{
		{`"abc"`, "abc", true},
		{"abc", "abc", true},
		{`W/"abc"`, "abc", true},
		{`"a", "abc"`, "abc", true},
		{"*", "abc", true},
		{`"other"`, "abc", false},
	} {
		if got := etagMatches(tc.header, tc.etag); got != tc.want {
			t.Fatalf("%q vs %q = %v", tc.header, tc.etag, got)
		}
	}
}

func TestObjectRefValidateAndSchema(t *testing.T) {
	ref, err := NewObjectRef(ns.Bucket, "lake", "path/to/key")
	if err != nil {
		t.Fatal(err)
	}
	if ref.NS != ns.BucketRef("lake") || ref.Key != "path/to/key" {
		t.Fatalf("ref = %+v", ref)
	}
	if sc, err := ref.Schema(); err != nil || sc != `"s3b_lake"` {
		t.Fatalf("schema = %q err=%v", sc, err)
	}
	if ref.String() != "bucket:lake/path/to/key" {
		t.Fatalf("String = %q", ref.String())
	}
	if _, err := NewObjectRef(ns.Bucket, "Bad Name", "k"); err == nil {
		t.Fatal("accepted invalid namespace")
	}
	if _, err := NewObjectRef(ns.Bucket, "lake", ""); err == nil {
		t.Fatal("accepted empty key")
	}
}

func TestSegmentsSingleAndMultipart(t *testing.T) {
	single := &Meta{FileID: 7, Size: 100}
	segs, err := single.Segments()
	if err != nil || len(segs) != 1 || segs[0] != (Segment{FileID: 7, Length: 100}) {
		t.Fatalf("single: %+v %v", segs, err)
	}
	multi := &Meta{FileID: 3, Size: 10, Parts: []int64{3, 4}, PartEnds: []int64{4, 10}}
	segs, err = multi.Segments()
	if err != nil || len(segs) != 2 || segs[1] != (Segment{FileID: 4, Base: 4, Length: 6}) {
		t.Fatalf("multipart: %+v %v", segs, err)
	}
	for _, bad := range []*Meta{
		{FileID: 3, Size: 10, Parts: []int64{3}},
		{FileID: 3, Size: 10, Parts: []int64{3, 4}, PartEnds: []int64{5, 4}},
		{FileID: 3, Size: 10, Parts: []int64{9}, PartEnds: []int64{10}},
	} {
		if _, err := bad.Segments(); err == nil {
			t.Fatalf("malformed manifest accepted: %+v", bad)
		}
	}
}

func TestByteRangeResolve(t *testing.T) {
	tests := []struct {
		name string
		rng  ByteRange
		size int64
		want Span
		bad  bool
	}{
		{"whole", WholeObjectRange(), 100, Span{Start: 0, End: 99}, false},
		{"whole-zero", WholeObjectRange(), 0, Span{Start: 0, End: -1}, false},
		{"explicit-full", Range(0, -1, -1), 100, Span{Start: 0, End: 99, Ranged: true}, false},
		{"bounded", Range(10, 19, -1), 100, Span{Start: 10, End: 19, Ranged: true}, false},
		{"open-end", Range(10, -1, -1), 100, Span{Start: 10, End: 99, Ranged: true}, false},
		{"suffix", Range(0, -1, 10), 100, Span{Start: 90, End: 99, Ranged: true}, false},
		{"suffix-longer-than-object", Range(0, -1, 200), 100, Span{Start: 0, End: 99, Ranged: true}, false},
		{"explicit-full-zero", Range(0, -1, -1), 0, Span{Start: 0, End: -1, Ranged: true}, false},
		{"start-past-end", Range(100, -1, -1), 100, Span{}, true},
		{"last-before-first", Range(20, 10, -1), 100, Span{}, true},
		{"empty-suffix", Range(0, -1, 0), 100, Span{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.rng.Resolve(tt.size)
			if (err != nil) != tt.bad {
				t.Fatalf("err = %v, want bad=%v", err, tt.bad)
			}
			if !tt.bad && got != tt.want {
				t.Fatalf("span = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPreconditionSQL(t *testing.T) {
	tests := []struct {
		name  string
		p     Precondition
		cond  string
		match any
	}{
		{"unconditional", Precondition{}, "none", nil},
		{"absent", Precondition{Mode: IfAbsent}, "if_absent", nil},
		{"present", Precondition{Mode: IfPresent}, "if_present", nil},
		{"match", Precondition{Mode: IfMatch, ETag: "abc"}, "if_match", "abc"},
		{"match-empty", Precondition{Mode: IfMatch}, "if_match", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond, match := tt.p.SQL()
			if cond != tt.cond || match != tt.match {
				t.Fatalf("SQL() = (%q, %#v), want (%q, %#v)", cond, match, tt.cond, tt.match)
			}
		})
	}
}

func TestPreconditionAllows(t *testing.T) {
	tests := []struct {
		name   string
		p      Precondition
		exists bool
		etag   string
		want   bool
	}{
		{"unconditional", Precondition{}, false, "", true},
		{"absent-missing", Precondition{Mode: IfAbsent}, false, "", true},
		{"absent-existing", Precondition{Mode: IfAbsent}, true, "a", false},
		{"present-missing", Precondition{Mode: IfPresent}, false, "", false},
		{"present-existing", Precondition{Mode: IfPresent}, true, "a", true},
		{"match", Precondition{Mode: IfMatch, ETag: "a"}, true, "a", true},
		{"match-missing", Precondition{Mode: IfMatch, ETag: "a"}, false, "", false},
		{"match-wrong", Precondition{Mode: IfMatch, ETag: "a"}, true, "b", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Allows(tt.exists, tt.etag); got != tt.want {
				t.Fatalf("Allows(%v, %q) = %v, want %v", tt.exists, tt.etag, got, tt.want)
			}
		})
	}
}

func TestBlockCodecRawRoundTrip(t *testing.T) {
	src := bytes.Repeat([]byte("raw-block-"), 500)
	enc := compressBlock(nil, src)
	if !bytes.Equal(enc, src) {
		t.Fatalf("compressBlock altered bytes: got %d, want %d", len(enc), len(src))
	}
	dec, err := decompressBlock(nil, enc, len(src))
	if err != nil || !bytes.Equal(dec, src) {
		t.Fatalf("round trip failed: %v", err)
	}
}

type fakeRows struct {
	rows   [][][]byte
	pos    int
	err    error
	closed bool
}

func (f *fakeRows) Close() { f.closed = true }

func (f *fakeRows) Err() error { return f.err }

func (f *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (f *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (f *fakeRows) Next() bool {
	if f.pos >= len(f.rows) {
		return false
	}
	f.pos++
	return true
}

func (f *fakeRows) Scan(...any) error { return errors.New("fakeRows: Scan not implemented") }

func (f *fakeRows) Values() ([]any, error) {
	return nil, errors.New("fakeRows: Values not implemented")
}

func (f *fakeRows) Conn() *pgx.Conn { return nil }

func (f *fakeRows) TypeMap() *pgtype.Map { return nil }

func (f *fakeRows) RawValues() [][]byte {
	if f.pos == 0 || f.pos > len(f.rows) {
		return nil
	}
	return f.rows[f.pos-1]
}

func chunkRow(no int32, data []byte) [][]byte {
	be := make([]byte, 4)
	binary.BigEndian.PutUint32(be, uint32(no))
	return [][]byte{be, data}
}

func chunkRows(rows ...[][]byte) [][][]byte {
	return rows
}

type readRow struct {
	no   int32
	data []byte
}

func collectRows(t *testing.T, r *chunkRowReader) ([]readRow, error) {
	t.Helper()
	var got []readRow
	for {
		no, data, err := r.next()
		if err != nil {
			return got, err
		}
		got = append(got, readRow{no: no, data: bytes.Clone(data)})
	}
}

func newTestReader(t *testing.T, rows *fakeRows, p piece, start, end int64) *chunkRowReader {
	t.Helper()
	r := newChunkRowReader(rows, p, start, end)
	t.Cleanup(r.close)
	return r
}

func testPiece(segLen int64, lo, hi int32) piece {
	return piece{fileID: 42, base: 1000, segLen: segLen, lo: lo, hi: hi}
}

func rowFill(fill byte, n int) []byte {
	return bytes.Repeat([]byte{fill}, n)
}

func TestChunkRowReaderInOrder(t *testing.T) {
	p := testPiece(3*RowBytes, 0, 2)
	rows := &fakeRows{rows: chunkRows(
		chunkRow(0, rowFill('a', RowBytes)),
		chunkRow(1, rowFill('b', RowBytes)),
		chunkRow(2, rowFill('c', RowBytes)),
	)}
	r := newTestReader(t, rows, p, p.base, p.base+3*RowBytes-1)

	got, err := collectRows(t, r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	for i, row := range got {
		if row.no != int32(i) {
			t.Fatalf("row %d number = %d", i, row.no)
		}
		if !bytes.Equal(row.data, rowFill("abc"[i], RowBytes)) {
			t.Fatalf("row %d data mismatch", i)
		}
	}
	r.close()
	if !rows.closed {
		t.Fatal("reader.close did not close rows")
	}
}

func TestChunkRowReaderOutOfOrderAndDuplicate(t *testing.T) {
	p := testPiece(3*RowBytes, 0, 2)
	rows := &fakeRows{rows: chunkRows(
		chunkRow(1, rowFill('b', RowBytes)),
		chunkRow(0, rowFill('a', RowBytes)),
		chunkRow(2, rowFill('c', RowBytes)),
		chunkRow(0, rowFill('x', RowBytes)),
	)}
	r := newTestReader(t, rows, p, p.base, p.base+3*RowBytes-1)

	got, err := collectRows(t, r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	for i, row := range got {
		if row.no != int32(i) {
			t.Fatalf("row %d number = %d", i, row.no)
		}
		if !bytes.Equal(row.data, rowFill("abc"[i], RowBytes)) {
			t.Fatalf("row %d data mismatch", i)
		}
	}
}

func TestChunkRowReaderIgnoresRowsBeyondPiece(t *testing.T) {
	p := testPiece(8*RowBytes, 5, 7)
	rows := &fakeRows{rows: chunkRows(
		chunkRow(7, rowFill('c', RowBytes)),
		chunkRow(9, rowFill('x', RowBytes)),
		chunkRow(5, rowFill('a', RowBytes)),
		chunkRow(8, rowFill('y', RowBytes)),
		chunkRow(6, rowFill('b', RowBytes)),
	)}
	r := newTestReader(t, rows, p, p.base+int64(p.lo)*RowBytes, p.base+int64(p.hi+1)*RowBytes-1)

	got, err := collectRows(t, r)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	for i, row := range got {
		if row.no != int32(i+5) {
			t.Fatalf("row %d number = %d", i, row.no)
		}
		if !bytes.Equal(row.data, rowFill("abc"[i], RowBytes)) {
			t.Fatalf("row %d data mismatch", i)
		}
	}
}

func TestChunkRowReaderGap(t *testing.T) {
	p := testPiece(3*RowBytes, 0, 2)
	rows := &fakeRows{rows: chunkRows(
		chunkRow(0, rowFill('a', RowBytes)),
		chunkRow(2, rowFill('c', RowBytes)),
	)}
	r := newTestReader(t, rows, p, p.base, p.base+3*RowBytes-1)

	got, err := collectRows(t, r)
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want missing-row error", err)
	}
	if !strings.Contains(err.Error(), "row 1 missing") {
		t.Fatalf("err = %v, want missing row 1", err)
	}
	if len(got) != 1 || got[0].no != 0 {
		t.Fatalf("got %d rows before error, want row 0 only", len(got))
	}
}

func TestChunkRowReaderMalformedRow(t *testing.T) {
	p := testPiece(RowBytes, 0, 0)
	tests := []struct {
		name string
		row  [][]byte
	}{
		{name: "wrong columns", row: [][]byte{rowFill(0, 4)}},
		{name: "wrong number width", row: [][]byte{{0, 0, 0}, rowFill('a', RowBytes)}},
		{name: "nil data", row: [][]byte{{0, 0, 0, 0}, nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows := &fakeRows{rows: chunkRows(tt.row)}
			r := newTestReader(t, rows, p, p.base, p.base+RowBytes-1)
			_, _, err := r.next()
			if err == nil || !strings.Contains(err.Error(), "unexpected row shape") {
				t.Fatalf("err = %v, want unexpected row shape", err)
			}
		})
	}
}

func TestChunkRowReaderRangeClipping(t *testing.T) {
	p := testPiece(3*RowBytes, 0, 2)
	rows := &fakeRows{rows: chunkRows(
		chunkRow(0, rowFill('a', RowBytes)),
		chunkRow(1, rowFill('b', RowBytes)),
		chunkRow(2, rowFill('c', RowBytes)),
	)}
	start := p.base + RowBytes - 10
	end := p.base + RowBytes + 9
	r := newTestReader(t, rows, p, start, end)

	var out []byte
	for {
		no, data, err := r.next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		r.appendRow(&out, no, data)
	}
	want := append(rowFill('a', RowBytes)[RowBytes-10:], rowFill('b', RowBytes)[:10]...)
	if !bytes.Equal(out, want) {
		t.Fatalf("clipped output = %d bytes, want %d", len(out), len(want))
	}
}

func TestChunkRowReaderRowsError(t *testing.T) {
	p := testPiece(RowBytes, 0, 0)
	wantErr := errors.New("query failed")
	rows := &fakeRows{rows: chunkRows(chunkRow(0, rowFill('a', RowBytes))), err: wantErr}
	r := newTestReader(t, rows, p, p.base, p.base+RowBytes-1)

	got, err := collectRows(t, r)
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if len(got) != 1 {
		t.Fatalf("got %d rows, want 1", len(got))
	}
}

func TestRowFramerLayout(t *testing.T) {
	if len(copyHeader) != 19 {
		t.Fatalf("COPY header is %d bytes, want 19", len(copyHeader))
	}
	f := newRowFramer(42)
	if _, err := f.push([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	b := f.finish()
	defer putFramerBuf(b)
	out := b.Bytes()
	if string(out[:11]) != "PGCOPY\n\xff\r\n\x00" {
		t.Fatalf("bad signature")
	}
	row := out[19 : len(out)-2]
	if got := binary.BigEndian.Uint16(row[0:2]); got != 3 {
		t.Fatalf("field count = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[2:6]); got != 8 {
		t.Fatalf("file_id length = %d", got)
	}
	if got := binary.BigEndian.Uint64(row[6:14]); got != 42 {
		t.Fatalf("file_id = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[14:18]); got != 4 {
		t.Fatalf("no length = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[18:22]); got != 0 {
		t.Fatalf("no = %d", got)
	}
	if got := binary.BigEndian.Uint32(row[22:26]); got != 5 {
		t.Fatalf("data length = %d", got)
	}
	if string(row[26:]) != "hello" {
		t.Fatalf("data = %q", row[26:])
	}
}

func TestRowFramerSplitsAtRowBoundary(t *testing.T) {

	raw := make([]byte, RowBytes+RowBytes/2)
	_, _ = rand.Read(raw)
	f := newRowFramer(1)
	if _, err := f.push(raw); err != nil {
		t.Fatal(err)
	}
	b := f.finish()
	defer putFramerBuf(b)
	out := b.Bytes()
	if want := len(copyHeader) + RowBytes + RowBytes/2 + 2*26 + 2; len(out) != want {
		t.Fatalf("incompressible frame length %d, want %d", len(out), want)
	}

	zeros := make([]byte, RowBytes+RowBytes/2)
	fc := newRowFramer(2)
	if _, err := fc.push(zeros); err != nil {
		t.Fatal(err)
	}
	bc := fc.finish()
	defer putFramerBuf(bc)
	if want := len(copyHeader) + RowBytes + RowBytes/2 + 2*26 + 2; len(bc.Bytes()) != want {
		t.Fatalf("raw frame length %d, want %d", len(bc.Bytes()), want)
	}
	r0Len := int(binary.BigEndian.Uint32(bc.Bytes()[19+22 : 19+26]))
	r0 := bc.Bytes()[19+26 : 19+26+r0Len]
	dec0, err := decodeRow(nil, int64(len(zeros)), 0, r0)
	if err != nil || !bytes.Equal(dec0, zeros[:RowBytes]) {
		t.Fatalf("row 0 decode failed: %v", err)
	}
}

func TestRowFramerDirectRowsAcrossWrites(t *testing.T) {
	data := make([]byte, 3*RowBytes+123)
	_, _ = rand.Read(data)
	f := newRowFramer(9)
	for off := 0; off < len(data); {
		n := min(5000+off%17, len(data)-off)
		out, err := f.push(data[off : off+n])
		if err != nil {
			t.Fatal(err)
		}
		if out != nil {
			putFramerBuf(out)
		}
		off += n
	}
	b := f.finish()
	defer putFramerBuf(b)

	body := b.Bytes()[len(copyHeader) : len(b.Bytes())-len(copyTrailer)]
	var got []byte
	for row := 0; len(body) > 0; row++ {
		if len(body) < 2 || binary.BigEndian.Uint16(body[:2]) != 3 {
			t.Fatalf("row %d: bad field count", row)
		}
		pos := 2
		fileLen := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		if fileLen != 8 || binary.BigEndian.Uint64(body[pos:]) != 9 {
			t.Fatalf("row %d: bad file id", row)
		}
		pos += fileLen
		noLen := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		no := int32(binary.BigEndian.Uint32(body[pos:]))
		pos += noLen
		if no != int32(row) {
			t.Fatalf("row %d: no = %d", row, no)
		}
		dataLen := int(binary.BigEndian.Uint32(body[pos:]))
		pos += 4
		rowData, err := decodeRow(nil, int64(len(data)), no, body[pos:pos+dataLen])
		if err != nil {
			t.Fatal(err)
		}
		start := int(no) * RowBytes
		end := min(start+RowBytes, len(data))
		if !bytes.Equal(rowData, data[start:end]) {
			t.Fatalf("row %d data mismatch", row)
		}
		got = append(got, rowData...)
		body = body[pos+dataLen:]
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("row data = %d bytes, want %d", len(got), len(data))
	}
}

func storedWithChecksum(p storedPart, value string) storedPart {
	p.checksum = new(value)
	return p
}

func TestPlanMultipartBuildsManifest(t *testing.T) {
	sha1 := sha256.Sum256([]byte("hello "))
	sha2 := sha256.Sum256([]byte("world"))
	md51 := md5.Sum([]byte("hello "))
	md52 := md5.Sum([]byte("world"))
	stored := []storedPart{
		{no: 1, fileID: 11, size: 6, sha: sha1[:], md5: md51[:]},
		{no: 3, fileID: 13, size: 5, sha: sha2[:], md5: md52[:]},
		{no: 4, fileID: 14, size: 9, sha: sha2[:], md5: md52[:]},
	}
	listed := []ListedPart{
		{PartNo: 1, ETag: hex.EncodeToString(md51[:])},
		{PartNo: 3, ETag: hex.EncodeToString(md52[:])},
	}
	plan, err := planMultipart(nil, listed, stored)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.fileIDs) != 2 || plan.fileIDs[0] != 11 || plan.fileIDs[1] != 13 {
		t.Fatalf("file ids = %v", plan.fileIDs)
	}
	if len(plan.partEnds) != 2 || plan.partEnds[0] != 6 || plan.partEnds[1] != 11 {
		t.Fatalf("part ends = %v", plan.partEnds)
	}
	if plan.size != 11 {
		t.Fatalf("size = %d", plan.size)
	}
	if len(plan.unused) != 1 || plan.unused[0] != 14 {
		t.Fatalf("unused = %v", plan.unused)
	}
	if len(plan.checksums) != 0 {
		t.Fatalf("checksums without an algorithm = %+v", plan.checksums)
	}
	shaAll := sha256.New()
	shaAll.Write(sha1[:])
	shaAll.Write(sha2[:])
	if !bytes.Equal(plan.sha256, shaAll.Sum(nil)) {
		t.Fatalf("sha256 = %x", plan.sha256)
	}
	md5All := md5.New()
	md5All.Write(md51[:])
	md5All.Write(md52[:])
	wantETag := hex.EncodeToString(md5All.Sum(nil)) + "-2"
	if plan.etag != wantETag {
		t.Fatalf("etag = %q, want %q", plan.etag, wantETag)
	}
}

func TestPlanMultipartRejectsBadParts(t *testing.T) {
	md5sum := md5.Sum([]byte("body"))
	sha := sha256.Sum256([]byte("body"))
	stored := []storedPart{{no: 1, fileID: 1, size: 4, sha: sha[:], md5: md5sum[:]}}
	good := ListedPart{PartNo: 1, ETag: hex.EncodeToString(md5sum[:])}
	withSum := storedWithChecksum(stored[0], "abc")

	cases := []struct {
		name      string
		algorithm *string
		listed    []ListedPart
		stored    []storedPart
	}{
		{
			name:   "missing stored part",
			listed: []ListedPart{{PartNo: 2, ETag: good.ETag}},
			stored: stored,
		},
		{
			name:   "wrong etag",
			listed: []ListedPart{{PartNo: 1, ETag: "not-the-md5"}},
			stored: stored,
		},
		{
			name:   "checksum without algorithm",
			listed: []ListedPart{{PartNo: 1, ETag: good.ETag, Checksum: &Checksum{Name: "CRC32", Value: "abc"}}},
			stored: stored,
		},
		{
			name:      "algorithm without checksum",
			algorithm: new("CRC32"),
			listed:    []ListedPart{good},
			stored:    stored,
		},
		{
			name:      "checksum name mismatch",
			algorithm: new("CRC32"),
			listed:    []ListedPart{{PartNo: 1, ETag: good.ETag, Checksum: &Checksum{Name: "SHA256", Value: "abc"}}},
			stored:    []storedPart{withSum},
		},
		{
			name:      "checksum value mismatch",
			algorithm: new("CRC32"),
			listed:    []ListedPart{{PartNo: 1, ETag: good.ETag, Checksum: &Checksum{Name: "CRC32", Value: "different"}}},
			stored:    []storedPart{withSum},
		},
		{
			name:      "stored checksum missing",
			algorithm: new("CRC32"),
			listed:    []ListedPart{{PartNo: 1, ETag: good.ETag, Checksum: &Checksum{Name: "CRC32", Value: "abc"}}},
			stored:    stored,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := planMultipart(tc.algorithm, tc.listed, tc.stored); !errors.Is(err, ErrInvalidPart) {
				t.Fatalf("error = %v, want ErrInvalidPart", err)
			}
		})
	}
}

func TestPlanMultipartSelectsChecksums(t *testing.T) {
	md5sum := md5.Sum([]byte("body"))
	sha := sha256.Sum256([]byte("body"))
	stored := storedWithChecksum(storedPart{no: 1, fileID: 7, size: 4, sha: sha[:], md5: md5sum[:]}, "abc")
	listed := []ListedPart{{
		PartNo:   1,
		ETag:     hex.EncodeToString(md5sum[:]),
		Checksum: &Checksum{Name: "CRC32", Value: "abc"},
	}}
	plan, err := planMultipart(new("CRC32"), listed, []storedPart{stored})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.checksums) != 1 || plan.checksums[0].no != 1 || plan.checksums[0].fileID != 7 || plan.checksums[0].value != "abc" {
		t.Fatalf("checksums = %+v", plan.checksums)
	}
}

func TestValidateListedParts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		listed []ListedPart
		ok     bool
	}{
		{name: "empty", ok: false},
		{name: "one", listed: []ListedPart{{PartNo: 1}}, ok: true},
		{name: "ordered", listed: []ListedPart{{PartNo: 1}, {PartNo: 2}, {PartNo: 10000}}, ok: true},
		{name: "zero", listed: []ListedPart{{PartNo: 0}}, ok: false},
		{name: "too large", listed: []ListedPart{{PartNo: 10001}}, ok: false},
		{name: "duplicate", listed: []ListedPart{{PartNo: 1}, {PartNo: 1}}, ok: false},
		{name: "descending", listed: []ListedPart{{PartNo: 2}, {PartNo: 1}}, ok: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateListedParts(tc.listed)
			if tc.ok && err != nil {
				t.Fatalf("error = %v", err)
			}
			if !tc.ok && !errors.Is(err, ErrInvalidPart) {
				t.Fatalf("error = %v, want ErrInvalidPart", err)
			}
		})
	}
}

func TestCacheVersionRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want CacheVersion
	}{
		{name: "live", in: "etag-123", want: LiveCacheVersion("etag-123")},
		{name: "empty live", in: "", want: LiveCacheVersion("")},
		{name: "pending", in: "pending:upload-1", want: PendingCacheVersion("upload-1")},
		{name: "deleted", in: "deleted:12345", want: DeletedCacheVersion("12345")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ParseCacheVersion(tc.in)
			if got != tc.want {
				t.Fatalf("ParseCacheVersion(%q) = %+v, want %+v", tc.in, got, tc.want)
			}
			if out := got.String(); out != tc.in {
				t.Fatalf("CacheVersion.String() = %q, want %q", out, tc.in)
			}
		})
	}
}

func TestCacheVersionState(t *testing.T) {
	if !LiveCacheVersion("x").IsLive() {
		t.Fatal("live version is not live")
	}
	if !PendingCacheVersion("x").IsPending() {
		t.Fatal("pending version is not pending")
	}
	if !DeletedCacheVersion("x").IsDeleted() {
		t.Fatal("deleted version is not deleted")
	}
	if IsPendingOrigin("live") || IsDeletedOrigin("live") {
		t.Fatal("live version classified as pending or deleted")
	}
	if !IsPendingOrigin(PendingOriginToken("upload")) {
		t.Fatal("pending token not recognized")
	}
	if !IsDeletedOrigin(DeletedCacheVersion(NewDeletedCacheToken()).String()) {
		t.Fatal("deleted token not recognized")
	}
}

func TestCacheAdd(t *testing.T) {
	cases := []struct {
		name    string
		current string
		delta   int64
		want    int64
		wantErr bool
	}{
		{name: "new", current: "0", delta: 2, want: 2},
		{name: "negative", current: "5", delta: -2, want: 3},
		{name: "trim", current: " 41 ", delta: -1, want: 40},
		{name: "plus sign", current: "+10", delta: 1, want: 11},
		{name: "leading zero", current: "010", delta: 1, want: 11},
		{name: "max minus one", current: "9223372036854775807", delta: -1, want: math.MaxInt64 - 1},
		{name: "min plus one", current: "-9223372036854775808", delta: 1, want: math.MinInt64 + 1},
		{name: "max delta min", current: "9223372036854775807", delta: math.MinInt64, want: -1},
		{name: "min as delta", current: "0", delta: math.MinInt64, want: math.MinInt64},
		{name: "empty", current: "", delta: 1, wantErr: true},
		{name: "not integer", current: "12x", delta: 1, wantErr: true},
		{name: "float", current: "1.5", delta: 1, wantErr: true},
		{name: "overflow positive", current: "9223372036854775807", delta: 1, wantErr: true},
		{name: "overflow negative", current: "-9223372036854775808", delta: -1, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cacheAdd([]byte(tc.current), tc.delta)
			if tc.wantErr {
				if !errors.Is(err, ErrNotInteger) {
					t.Fatalf("cacheAdd(%q, %d) error = %v, want ErrNotInteger", tc.current, tc.delta, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("cacheAdd(%q, %d): %v", tc.current, tc.delta, err)
			}
			if got != tc.want {
				t.Fatalf("cacheAdd(%q, %d) = %d, want %d", tc.current, tc.delta, got, tc.want)
			}
		})
	}
}

func TestEncodeCacheBatchDeduplicatesLastValueWins(t *testing.T) {
	keys := []string{"a", "b", "a", "c", "b"}
	vals := [][]byte{[]byte("a1"), []byte("b1"), []byte("a2"), []byte("c1"), []byte("b2")}
	batch := encodeCacheBatch(keys, vals)

	wantKeys := []string{"a", "b", "c"}
	wantVals := [][]byte{[]byte("a2"), []byte("b2"), []byte("c1")}
	if len(batch.keys) != len(wantKeys) {
		t.Fatalf("keys = %v, want %v", batch.keys, wantKeys)
	}
	for i, want := range wantKeys {
		if batch.keys[i] != want {
			t.Fatalf("keys = %v, want %v", batch.keys, wantKeys)
		}
		got := batch.stored[i]
		dec, err := decompressBlock(nil, got, len(wantVals[i]))
		if err != nil {
			t.Fatalf("decode %q: %v", want, err)
		}
		if !bytes.Equal(dec, wantVals[i]) {
			t.Fatalf("value %q = %q, want %q", want, dec, wantVals[i])
		}
		if batch.sizes[i] != int64(len(wantVals[i])) {
			t.Fatalf("size %q = %d, want %d", want, batch.sizes[i], len(wantVals[i]))
		}
		sumMD5 := md5.Sum(wantVals[i])
		if batch.etags[i] != hex.EncodeToString(sumMD5[:]) {
			t.Fatalf("etag %q = %q, want md5 %q", want, batch.etags[i], hex.EncodeToString(sumMD5[:]))
		}
		sumSHA := sha256.Sum256(wantVals[i])
		if !bytes.Equal(batch.shas[i], sumSHA[:]) {
			t.Fatalf("sha %q mismatch", want)
		}
	}
}
