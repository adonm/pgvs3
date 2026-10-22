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
