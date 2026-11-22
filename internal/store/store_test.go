package store

import (
	"errors"
	"strings"
	"testing"
)

func TestByteRangeResolve(t *testing.T) {
	cases := []struct {
		name    string
		rng     ByteRange
		size    int64
		want    Span
		wantErr bool
	}{
		{"whole", WholeObjectRange(), 100, Span{Start: 0, End: 99}, false},
		{"whole-zero", WholeObjectRange(), 0, Span{Start: 0, End: -1}, false},
		{"bounded", Range(10, 19, -1), 100, Span{Start: 10, End: 19, Ranged: true}, false},
		{"open-end", Range(10, -1, -1), 100, Span{Start: 10, End: 99, Ranged: true}, false},
		{"suffix", Range(0, -1, 10), 100, Span{Start: 90, End: 99, Ranged: true}, false},
		{"suffix-clamped", Range(0, -1, 200), 100, Span{Start: 0, End: 99, Ranged: true}, false},
		{"start-past-end", Range(100, -1, -1), 100, Span{}, true},
		{"zero-range", Range(0, 0, -1), 0, Span{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.rng.Resolve(tc.size)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("got = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestReadConditionCheck(t *testing.T) {
	if err := (ReadCondition{IfMatch: `"abc"`}).Check(true, "abc"); err != nil {
		t.Fatal(err)
	}
	if err := (ReadCondition{IfMatch: `"abc"`}).Check(true, "def"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("got %v, want ErrPrecondition", err)
	}
	if err := (ReadCondition{IfNoneMatch: `"abc"`}).Check(true, "abc"); !errors.Is(err, ErrNotModified) {
		t.Fatalf("got %v, want ErrNotModified", err)
	}
	if err := (ReadCondition{IfNoneMatch: "*"}).Check(true, "abc"); !errors.Is(err, ErrNotModified) {
		t.Fatalf("got %v, want ErrNotModified", err)
	}
}

func TestSchemaContainsExpectedTables(t *testing.T) {
	for _, want := range []string{"cache_layout", "cache_objects", "cache_chunks", "ON DELETE CASCADE", "STORAGE EXTERNAL"} {
		if !strings.Contains(schemaSQL, want) {
			t.Fatalf("schema.sql missing %q", want)
		}
	}
}
