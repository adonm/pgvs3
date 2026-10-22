package s3

import "testing"

func FuzzParseRange(f *testing.F) {
	for _, s := range []string{"bytes=0-0", "bytes=-1", "bytes=0-", "bytes=1-2,3-4", "garbage"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _, _, _, _ = parseRange(s)
	})
}

func FuzzCanonicalQuery(f *testing.F) {
	for _, s := range []string{"a=1&b=2", "list-type=2&prefix=a%2Fb", "uploads", "a=%zz"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_ = canonicalQuery(s)
	})
}
