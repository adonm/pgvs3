package s3

import (
	"net/http/httptest"
	"testing"
)

func TestParseRange(t *testing.T) {
	tests := []struct {
		in                  string
		first, last, suffix int64
		ranged              bool
		ok                  bool
	}{
		{"", 0, -1, -1, false, true},
		{"bytes=0-4", 0, 4, -1, true, true},
		{"bytes=10-", 10, -1, -1, true, true},
		{"bytes=-100", 0, -1, 100, true, true},
		{"bytes=0-", 0, -1, -1, true, true},
		{"bytes=5-4", 0, 0, 0, false, false},
		{"bytes=0-1,2-3", 0, 0, 0, false, false},
		{"items=0-1", 0, 0, 0, false, false},
	}
	for _, tt := range tests {
		f, l, s, ranged, err := parseRange(tt.in)
		if (err == nil) != tt.ok {
			t.Fatalf("%q: err=%v", tt.in, err)
		}
		if err != nil {
			continue
		}
		if f != tt.first || l != tt.last || s != tt.suffix || ranged != tt.ranged {
			t.Fatalf("%q: (%d,%d,%d,%v)", tt.in, f, l, s, ranged)
		}
	}
}

func TestPrefixEndUTF8(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", "\U0010FFFF"},
		{"a", "b"},
		{"abc", "abd"},
		{"ab\u00ff", "ab\u0100"},
		{"lake/", "lake0"},
		{"\U0010FFFF", "\U0010FFFF"},
	} {
		if got := prefixEnd(tc.in); got != tc.want {
			t.Fatalf("prefixEnd(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSplitPathKeepsDoubleEncodedSlash(t *testing.T) {
	// SlateDB uses a literal %2F inside a key, so the wire path has %252F.
	r := httptest.NewRequest("GET", "http://x/bucket/objfs/analytics%252Freport.db/manifest/1", nil)
	bucket, key, err := splitPath(r, "")
	if err != nil {
		t.Fatal(err)
	}
	if bucket != "bucket" || key != "objfs/analytics%2Freport.db/manifest/1" {
		t.Fatalf("bucket=%q key=%q", bucket, key)
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
		if got := etagListMatches(tc.header, tc.etag); got != tc.want {
			t.Fatalf("%q vs %q = %v", tc.header, tc.etag, got)
		}
	}
}
