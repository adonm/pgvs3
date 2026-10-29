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
	"regexp"
	"slices"
	"strings"
	"sync"
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

func TestCompressibleBlocksShrink(t *testing.T) {
	src := bytes.Repeat([]byte("abcdefgh"), 1015)
	enc := compressBlock(nil, src)
	if len(enc) >= len(src)/4 {
		t.Fatalf("expected >4x compression, got %d -> %d", len(src), len(enc))
	}
	dec, err := decompressBlock(nil, enc, len(src))
	if err != nil || !bytes.Equal(dec, src) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestIncompressibleBlocksStayRaw(t *testing.T) {
	src := make([]byte, 8120)
	if _, err := rand.Read(src); err != nil {
		t.Fatal(err)
	}
	enc := compressBlock(nil, src)
	if len(enc) != len(src) {
		t.Fatalf("random block should stay raw, got %d -> %d", len(src), len(enc))
	}
}

func TestMaxInlineBlockRoundTrip(t *testing.T) {
	src := bytes.Repeat([]byte("maximum-inline-block-"), (MaxInlineBytes+20)/21)[:MaxInlineBytes]
	enc := compressBlock(nil, src)
	if len(enc) >= len(src) {
		t.Fatalf("expected compression, got %d -> %d", len(src), len(enc))
	}
	dec, err := decompressBlock(nil, enc, len(src))
	if err != nil || !bytes.Equal(dec, src) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestBlockCodecConcurrent(t *testing.T) {
	src := bytes.Repeat([]byte("concurrent-zstd-block-"), 400)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			enc := compressBlock(nil, src)
			dec, err := decompressBlock(nil, enc, len(src))
			if err != nil {
				t.Errorf("decompress failed: %v", err)
				return
			}
			if !bytes.Equal(dec, src) {
				t.Error("round-trip mismatch")
			}
		})
	}
	wg.Wait()
}

func TestCompressedBlocksAreZstdFrames(t *testing.T) {
	src := bytes.Repeat([]byte("postgres-zstd-block-"), 500)
	enc := compressBlock(nil, src)
	if len(enc) >= len(src) {
		t.Fatalf("expected compression, got %d -> %d", len(src), len(enc))
	}
	if !bytes.HasPrefix(enc, []byte{0x28, 0xb5, 0x2f, 0xfd}) {
		t.Fatalf("missing Zstd frame magic: %x", enc[:min(4, len(enc))])
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
	if len(bc.Bytes()) >= 200 {
		t.Fatalf("expected compressed frame < 200 bytes, got %d", len(bc.Bytes()))
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

func TestEnqueueJobsValidatesBeforeUsingStore(t *testing.T) {
	s := &Store{}
	if jobs, err := s.EnqueueJobs(context.Background(), nil); jobs != nil || err != nil {
		t.Fatalf("empty enqueue = %v, %v", jobs, err)
	}
	_, err := s.EnqueueJobs(context.Background(), []EnqueueInput{{Type: "x", Queue: "Bad Name"}})
	if !errors.Is(err, ns.ErrInvalidName) {
		t.Fatalf("err = %v, want ErrInvalidName", err)
	}
}

func TestStageEnqueueJobsDefaultsAndTransitions(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Hour)
	timeout := 1500
	expires := now.Add(24 * time.Hour)
	fixedID := "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6"

	jobs, err := stageEnqueueJobs([]EnqueueInput{
		{Type: "email.send", Queue: "email"},
		{
			ID: fixedID, Type: "report.build", Args: []byte(`{"n":1}`),
			Meta: []byte(`{"trace":"t"}`), Queue: "reports", Priority: 7,
			MaxAttempts: 5, ScheduledAt: future, TimeoutMS: &timeout,
			Tags: []string{"nightly"}, ExpiresAt: &expires,
		},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("staged %d jobs", len(jobs))
	}

	immediate := jobs[0]
	if immediate.ID == "" || immediate.ID == fixedID {
		t.Fatalf("generated id = %q", immediate.ID)
	}
	if immediate.State != JobAvailable || immediate.Attempt != 0 {
		t.Fatalf("immediate state = %s attempt=%d", immediate.State, immediate.Attempt)
	}
	if string(immediate.Args) != "[]" {
		t.Fatalf("default args = %q", immediate.Args)
	}
	if immediate.ScheduledAt == nil || !immediate.ScheduledAt.Equal(now) {
		t.Fatalf("default scheduled_at = %v", immediate.ScheduledAt)
	}
	if !immediate.CreatedAt.Equal(now) || !immediate.EnqueuedAt.Equal(now) {
		t.Fatalf("timestamps = %s/%s", immediate.CreatedAt, immediate.EnqueuedAt)
	}
	if immediate.Tags == nil || len(immediate.Tags) != 0 {
		t.Fatalf("default tags = %#v", immediate.Tags)
	}

	scheduled := jobs[1]
	if scheduled.ID != fixedID || scheduled.State != JobScheduled {
		t.Fatalf("scheduled job = %+v", scheduled)
	}
	if scheduled.ScheduledAt == nil || !scheduled.ScheduledAt.Equal(future) {
		t.Fatalf("scheduled_at = %v", scheduled.ScheduledAt)
	}
	if scheduled.Priority != 7 || scheduled.MaxAttempts != 5 ||
		scheduled.TimeoutMS == nil || *scheduled.TimeoutMS != timeout {
		t.Fatalf("scheduled fields = %+v", scheduled)
	}
	if string(scheduled.Args) != `{"n":1}` || string(scheduled.Meta) != `{"trace":"t"}` {
		t.Fatalf("args/meta = %s/%s", scheduled.Args, scheduled.Meta)
	}
	if len(scheduled.Tags) != 1 || scheduled.Tags[0] != "nightly" {
		t.Fatalf("tags = %#v", scheduled.Tags)
	}
	if scheduled.ExpiresAt == nil || !scheduled.ExpiresAt.Equal(expires) {
		t.Fatalf("expires_at = %v", scheduled.ExpiresAt)
	}
}

type stubTx struct {
	pgx.Tx
	exec func(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (tx *stubTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.exec(ctx, sql, args...)
}

func (tx *stubTx) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return &stubBatchResults{ctx: ctx, tx: tx, batch: batch}
}

type stubBatchResults struct {
	ctx   context.Context
	tx    *stubTx
	batch *pgx.Batch
	next  int
	err   error
}

func (r *stubBatchResults) Exec() (pgconn.CommandTag, error) {
	if r.err != nil {
		return pgconn.CommandTag{}, r.err
	}
	if r.next >= r.batch.Len() {
		return pgconn.CommandTag{}, errors.New("batch exhausted")
	}
	query := r.batch.QueuedQueries[r.next]
	r.next++
	r.err = nil
	return r.tx.exec(r.ctx, query.SQL, query.Arguments...)
}

func (r *stubBatchResults) Query() (pgx.Rows, error) {
	return nil, errors.New("not implemented")
}

func (r *stubBatchResults) QueryRow() pgx.Row {
	return stubRow{err: errors.New("not implemented")}
}

func (r *stubBatchResults) Close() error { return nil }

type stubRow struct{ err error }

func (r stubRow) Scan(...any) error { return r.err }

func TestInsertEnqueuedJobsWritesJobThenIndex(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	j := Job{
		ID: "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", Type: "email.send",
		Args: []byte("[]"), Queue: "email", State: JobAvailable,
		MaxAttempts: 3, CreatedAt: now, ScheduledAt: &now, EnqueuedAt: now,
		Tags: []string{},
	}
	var calls []string
	var callsArgs [][]any
	tx := &stubTx{exec: func(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		calls = append(calls, sql)
		callsArgs = append(callsArgs, args)
		return pgconn.CommandTag{}, nil
	}}
	if err := insertEnqueuedJobs(context.Background(), tx,
		[]EnqueueInput{{RetryInitial: 0, RetryCoeff: 0, RetryMax: 0}}, []Job{j}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 {
		t.Fatalf("exec calls = %d, want 2", len(calls))
	}
	if !strings.Contains(calls[0], "INSERT INTO") || !strings.Contains(calls[0], ".jobs") {
		t.Fatalf("first insert = %s", calls[0])
	}
	if !strings.Contains(calls[1], "s3p.job_index") {
		t.Fatalf("second insert = %s", calls[1])
	}
	args := callsArgs[0]
	if len(args) != 17 || args[0] != j.ID || args[12] != 1000 || args[13] != float64(2) || args[14] != 60000 {
		t.Fatalf("job insert args = %#v", args)
	}
	indexArgs := callsArgs[1]
	if len(indexArgs) != 3 || indexArgs[0] != j.ID || indexArgs[1] != "email" || indexArgs[2] != now {
		t.Fatalf("index insert args = %#v", indexArgs)
	}
}

func TestInsertEnqueuedJobsDuplicateSemantics(t *testing.T) {
	now := time.Now().UTC()
	j := Job{
		ID: "019414d4-8b2e-7c3a-b5d1-f0e2a3b4c5d6", Type: "x", Args: []byte("[]"),
		Queue: "email", State: JobAvailable, CreatedAt: now, ScheduledAt: &now,
		EnqueuedAt: now, Tags: []string{},
	}
	inputs := []EnqueueInput{{}}

	t.Run("job unique violation is duplicate", func(t *testing.T) {
		calls := 0
		tx := &stubTx{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			calls++
			return pgconn.CommandTag{}, &pgconn.PgError{Code: "23505"}
		}}
		err := insertEnqueuedJobs(context.Background(), tx, inputs, []Job{j})
		if !errors.Is(err, ErrDuplicateJob) {
			t.Fatalf("err = %v, want ErrDuplicateJob", err)
		}
		if calls != 1 {
			t.Fatalf("calls = %d, want 1", calls)
		}
	})

	t.Run("index unique violation is not remapped", func(t *testing.T) {
		calls := 0
		tx := &stubTx{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			calls++
			if calls == 2 {
				return pgconn.CommandTag{}, &pgconn.PgError{Code: "23505"}
			}
			return pgconn.CommandTag{}, nil
		}}
		err := insertEnqueuedJobs(context.Background(), tx, inputs, []Job{j})
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			t.Fatalf("err = %v, want raw pg unique violation", err)
		}
		if errors.Is(err, ErrDuplicateJob) {
			t.Fatalf("index conflict remapped to ErrDuplicateJob")
		}
		if calls != 2 {
			t.Fatalf("calls = %d, want 2", calls)
		}
	})

	t.Run("job insert error aborts batch", func(t *testing.T) {
		boom := errors.New("boom")
		calls := 0
		tx := &stubTx{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
			calls++
			return pgconn.CommandTag{}, boom
		}}
		err := insertEnqueuedJobs(context.Background(), tx,
			[]EnqueueInput{{}, {}}, []Job{j, j})
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if calls != 1 {
			t.Fatalf("calls = %d, want 1", calls)
		}
	})
}

func TestJobStateStringAndParse(t *testing.T) {
	states := []struct {
		state JobState
		text  string
	}{
		{JobScheduled, "scheduled"},
		{JobAvailable, "available"},
		{JobPending, "pending"},
		{JobActive, "active"},
		{JobCompleted, "completed"},
		{JobRetryable, "retryable"},
		{JobCancelled, "cancelled"},
		{JobDiscarded, "discarded"},
	}
	for _, tc := range states {
		t.Run(tc.text, func(t *testing.T) {
			if got := tc.state.String(); got != tc.text {
				t.Fatalf("String() = %q, want %q", got, tc.text)
			}
			if !tc.state.Valid() {
				t.Fatalf("%s should be valid", tc.text)
			}
			parsed, err := ParseJobState(tc.text)
			if err != nil {
				t.Fatalf("ParseJobState(%q): %v", tc.text, err)
			}
			if parsed != tc.state {
				t.Fatalf("ParseJobState(%q) = %d, want %d", tc.text, parsed, tc.state)
			}
		})
	}

	for _, invalid := range []string{"", "unknown", "PENDING", "running"} {
		if _, err := ParseJobState(invalid); !errors.Is(err, ErrInvalidJobState) {
			t.Fatalf("ParseJobState(%q) error = %v, want ErrInvalidJobState", invalid, err)
		}
	}
	if JobStateUnknown.Valid() {
		t.Fatal("zero state must not be valid")
	}
	if JobState(99).Valid() {
		t.Fatal("out-of-range state must not be valid")
	}
	if got := JobState(99).String(); got != "invalid" {
		t.Fatalf("out-of-range String() = %q", got)
	}
}

func TestJobEventString(t *testing.T) {
	events := []struct {
		event JobEvent
		text  string
	}{
		{JobEventEnqueue, "enqueue"},
		{JobEventScheduleDue, "schedule_due"},
		{JobEventLease, "lease"},
		{JobEventLeaseExpired, "lease_expired"},
		{JobEventAck, "ack"},
		{JobEventNack, "nack"},
		{JobEventHeartbeat, "heartbeat"},
		{JobEventCancel, "cancel"},
		{JobEventExpire, "expire"},
	}
	for _, tc := range events {
		if got := tc.event.String(); got != tc.text {
			t.Fatalf("%d.String() = %q, want %q", tc.event, got, tc.text)
		}
	}
	if got := JobEventUnknown.String(); got != "unknown" {
		t.Fatalf("unknown event String() = %q", got)
	}
	if got := JobEvent(99).String(); got != "invalid" {
		t.Fatalf("out-of-range event String() = %q", got)
	}
	if got := transitionSources(JobEvent(99)); got != nil {
		t.Fatalf("unknown event sources = %v, want nil", got)
	}
}

func TestJobStatesMatchSchemaCheck(t *testing.T) {
	check := regexp.MustCompile(`(?s)state\s+text NOT NULL CHECK \(state IN\s*\((.*?)\)\)`)
	match := check.FindStringSubmatch(schemaSQL)
	if match == nil {
		t.Fatal("jobs state CHECK constraint not found in schema")
	}
	quoted := regexp.MustCompile(`'([a-z]+)'`).FindAllStringSubmatch(match[1], -1)
	got := make([]string, len(quoted))
	for i, item := range quoted {
		got[i] = item[1]
	}
	want := make([]string, 0, len(jobStateNames)-1)
	for state := JobScheduled; state.Valid(); state++ {
		want = append(want, state.String())
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("schema states = %v, typed states = %v", got, want)
	}
}

func TestTransitionValid(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	future := now.Add(time.Hour)

	tests := []struct {
		name string
		in   TransitionInput
		want TransitionResult
	}{
		{
			name: "enqueue immediate",
			in:   TransitionInput{Event: JobEventEnqueue, Now: now, ScheduledAt: new(now)},
			want: TransitionResult{State: JobAvailable},
		},
		{
			name: "enqueue without schedule",
			in:   TransitionInput{Event: JobEventEnqueue, Now: now},
			want: TransitionResult{State: JobAvailable},
		},
		{
			name: "enqueue in the future",
			in:   TransitionInput{Event: JobEventEnqueue, Now: now, ScheduledAt: new(future)},
			want: TransitionResult{State: JobScheduled},
		},
		{
			name: "scheduled becomes due",
			in: TransitionInput{
				State: JobScheduled, Event: JobEventScheduleDue, Now: now, ScheduledAt: new(past),
			},
			want: TransitionResult{State: JobAvailable},
		},
		{
			name: "retryable becomes due",
			in: TransitionInput{
				State: JobRetryable, Event: JobEventScheduleDue, Now: now,
				ScheduledAt: new(past), Attempt: 2,
			},
			want: TransitionResult{State: JobAvailable, Attempt: 2},
		},
		{
			name: "lease first attempt",
			in: TransitionInput{
				State: JobAvailable, Event: JobEventLease, Now: now,
				Attempt: 0, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobActive, Attempt: 1},
		},
		{
			name: "lease retry attempt",
			in: TransitionInput{
				State: JobAvailable, Event: JobEventLease, Now: now,
				Attempt: 2, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobActive, Attempt: 2},
		},
		{
			name: "expired lease requeues",
			in: TransitionInput{
				State: JobActive, Event: JobEventLeaseExpired, Now: now,
				Attempt: 1, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobAvailable, Attempt: 2},
		},
		{
			name: "expired lease exhausts attempts",
			in: TransitionInput{
				State: JobActive, Event: JobEventLeaseExpired, Now: now,
				Attempt: 3, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobDiscarded, Attempt: 3},
		},
		{
			name: "expired lease with zero max attempts",
			in: TransitionInput{
				State: JobActive, Event: JobEventLeaseExpired, Now: now,
				Attempt: 0, MaxAttempts: 0,
			},
			want: TransitionResult{State: JobDiscarded},
		},
		{
			name: "ack",
			in: TransitionInput{
				State: JobActive, Event: JobEventAck, Now: now, Attempt: 2, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobCompleted, Attempt: 2},
		},
		{
			name: "heartbeat",
			in: TransitionInput{
				State: JobActive, Event: JobEventHeartbeat, Now: now, Attempt: 2, MaxAttempts: 3,
			},
			want: TransitionResult{State: JobActive, Attempt: 2},
		},
		{
			name: "cancel available",
			in:   TransitionInput{State: JobAvailable, Event: JobEventCancel, Now: now},
			want: TransitionResult{State: JobCancelled},
		},
		{
			name: "cancel pending legacy",
			in:   TransitionInput{State: JobPending, Event: JobEventCancel, Now: now},
			want: TransitionResult{State: JobCancelled},
		},
		{
			name: "expire scheduled",
			in: TransitionInput{
				State: JobScheduled, Event: JobEventExpire, Now: now, ExpiresAt: new(past),
			},
			want: TransitionResult{State: JobDiscarded},
		},
		{
			name: "expire available",
			in: TransitionInput{
				State: JobAvailable, Event: JobEventExpire, Now: now, ExpiresAt: new(past),
			},
			want: TransitionResult{State: JobDiscarded},
		},
		{
			name: "expire retryable",
			in: TransitionInput{
				State: JobRetryable, Event: JobEventExpire, Now: now, ExpiresAt: new(past),
			},
			want: TransitionResult{State: JobDiscarded},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Transition(tc.in)
			if err != nil {
				t.Fatalf("Transition() error = %v", err)
			}
			if got != tc.want {
				t.Fatalf("Transition() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestTransitionSourceMatrix(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)

	states := []JobState{
		JobStateUnknown, JobScheduled, JobAvailable, JobPending, JobActive,
		JobCompleted, JobRetryable, JobCancelled, JobDiscarded,
	}
	allowed := map[JobEvent]map[JobState]bool{
		JobEventEnqueue:      {JobStateUnknown: true},
		JobEventScheduleDue:  {JobScheduled: true, JobRetryable: true},
		JobEventLease:        {JobAvailable: true},
		JobEventLeaseExpired: {JobActive: true},
		JobEventAck:          {JobActive: true},
		JobEventNack:         {JobActive: true},
		JobEventHeartbeat:    {JobActive: true},
		JobEventCancel:       {JobScheduled: true, JobAvailable: true, JobPending: true, JobActive: true, JobRetryable: true},
		JobEventExpire:       {JobScheduled: true, JobAvailable: true, JobRetryable: true},
	}

	for event, sources := range allowed {
		for _, state := range states {
			name := event.String() + "/" + state.String()
			t.Run(name, func(t *testing.T) {
				_, err := Transition(TransitionInput{
					State: state, Event: event, Now: now,
					ScheduledAt: new(past), ExpiresAt: new(past),
					Attempt: 1, MaxAttempts: 3, Retryable: true,
					RetryInitialMS: 1000, RetryMaxMS: 60000, RetryCoefficient: 2,
				})
				wantValid := sources[state]
				if wantValid && err != nil {
					t.Fatalf("valid transition rejected: %v", err)
				}
				if !wantValid && !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("invalid transition error = %v, want ErrInvalidTransition", err)
				}
			})
		}
	}
}

func TestTransitionTimePreconditions(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Second)

	tests := []struct {
		name string
		in   TransitionInput
	}{
		{
			name: "schedule not due",
			in: TransitionInput{
				State: JobScheduled, Event: JobEventScheduleDue, Now: now, ScheduledAt: new(future),
			},
		},
		{
			name: "schedule without timestamp",
			in: TransitionInput{
				State: JobScheduled, Event: JobEventScheduleDue, Now: now,
			},
		},
		{
			name: "expire not due",
			in: TransitionInput{
				State: JobAvailable, Event: JobEventExpire, Now: now, ExpiresAt: new(future),
			},
		},
		{
			name: "expire without timestamp",
			in: TransitionInput{
				State: JobAvailable, Event: JobEventExpire, Now: now,
			},
		},
		{
			name: "unknown event",
			in: TransitionInput{
				State: JobActive, Event: JobEventUnknown, Now: now,
			},
		},
		{
			name: "out of range event",
			in: TransitionInput{
				State: JobActive, Event: JobEvent(99), Now: now,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Transition(tc.in); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("error = %v, want ErrInvalidTransition", err)
			}
		})
	}
}

func TestNackBackoff(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		attempt     int
		maxAttempts int
		initialMS   int
		maxMS       int
		coefficient float64
		retryable   bool
		wantState   JobState
		wantAttempt int
		wantDelayMS int
	}{
		{
			name: "first retry", attempt: 1, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 2, wantDelayMS: 1000,
		},
		{
			name: "second retry", attempt: 2, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 3, wantDelayMS: 2000,
		},
		{
			name: "third retry", attempt: 3, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 4, wantDelayMS: 4000,
		},
		{
			name: "capped retry", attempt: 7, maxAttempts: 8,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 8, wantDelayMS: 60000,
		},
		{
			name: "fractional coefficient", attempt: 3, maxAttempts: 5,
			initialMS: 100, maxMS: 1000, coefficient: 1.5, retryable: true,
			wantState: JobRetryable, wantAttempt: 4, wantDelayMS: 225,
		},
		{
			name: "max interval below initial", attempt: 1, maxAttempts: 5,
			initialMS: 1000, maxMS: 250, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 2, wantDelayMS: 250,
		},
		{
			name: "zero attempt uses initial", attempt: 0, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobRetryable, wantAttempt: 1, wantDelayMS: 1000,
		},
		{
			name: "attempts exhausted", attempt: 5, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobDiscarded, wantAttempt: 5,
		},
		{
			name: "non retryable", attempt: 1, maxAttempts: 5,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: false,
			wantState: JobDiscarded, wantAttempt: 1,
		},
		{
			name: "zero max attempts", attempt: 0, maxAttempts: 0,
			initialMS: 1000, maxMS: 60000, coefficient: 2, retryable: true,
			wantState: JobDiscarded, wantAttempt: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Transition(TransitionInput{
				State: JobActive, Event: JobEventNack, Now: now,
				Attempt: tc.attempt, MaxAttempts: tc.maxAttempts,
				RetryInitialMS: tc.initialMS, RetryMaxMS: tc.maxMS,
				RetryCoefficient: tc.coefficient, Retryable: tc.retryable,
			})
			if err != nil {
				t.Fatalf("Transition() error = %v", err)
			}
			want := TransitionResult{
				State: tc.wantState, Attempt: tc.wantAttempt, RetryDelayMS: tc.wantDelayMS,
			}
			if got != want {
				t.Fatalf("Transition() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestTransitionSourcesMatchSQL(t *testing.T) {
	tests := []struct {
		event JobEvent
		want  []string
	}{
		{JobEventExpire, []string{"scheduled", "available", "retryable"}},
		{JobEventScheduleDue, []string{"scheduled", "retryable"}},
		{JobEventLeaseExpired, []string{"active"}},
	}
	for _, tc := range tests {
		t.Run(tc.event.String(), func(t *testing.T) {
			got := jobStateStrings(transitionSources(tc.event))
			if !slices.Equal(got, tc.want) {
				t.Fatalf("sources = %v, want %v", got, tc.want)
			}
		})
	}
}

func FuzzRowFramer(f *testing.F) {
	f.Add([]byte("hello"), int64(1))
	f.Add(bytes.Repeat([]byte("x"), RowBytes+1), int64(42))
	f.Fuzz(func(t *testing.T, data []byte, id int64) {
		if len(data) > 4*RowBytes || id < 0 {
			return
		}
		fr := newRowFramer(id)
		if _, err := fr.push(data); err != nil {
			return
		}
		fr.finish()
	})
}

func FuzzBlockCodec(f *testing.F) {
	f.Add([]byte("hello world hello world hello world"))
	f.Add(bytes.Repeat([]byte("abcd1234"), 1024))
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxInlineBytes {
			return
		}
		enc := compressBlock(nil, data)
		if len(enc) > len(data) {
			t.Fatalf("compressed block grew: %d > %d", len(enc), len(data))
		}
		dec, err := decompressBlock(nil, enc, len(data))
		if err != nil {
			t.Fatalf("decompress failed: %v", err)
		}
		if !bytes.Equal(dec, data) {
			t.Fatalf("round-trip mismatch (len=%d, enc=%d)", len(data), len(enc))
		}
	})
}
