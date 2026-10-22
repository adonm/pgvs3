package s3

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// parseRange maps an S3 Range header to the (first,last,suffix) parameters
// accepted by the store. Only a single byte range is supported; DuckDB and
// SlateDB issue one range per request.
func parseRange(h string) (first, last, suffix int64, ranged bool, err error) {
	first, last, suffix = 0, -1, -1
	if h == "" {
		return first, last, suffix, false, nil
	}
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, 0, false, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, 0, false, fmt.Errorf("multiple ranges are not supported")
	}
	start, end, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, 0, false, fmt.Errorf("malformed range")
	}
	start, end = strings.TrimSpace(start), strings.TrimSpace(end)
	if start == "" {
		n, err := strconv.ParseInt(end, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, 0, false, fmt.Errorf("malformed suffix range")
		}
		if n == 0 {
			return 0, 0, 0, false, fmt.Errorf("empty suffix range")
		}
		return 0, -1, n, true, nil
	}
	first, err = strconv.ParseInt(start, 10, 64)
	if err != nil || first < 0 {
		return 0, 0, 0, false, fmt.Errorf("malformed range start")
	}
	if end == "" {
		return first, -1, -1, true, nil
	}
	last, err = strconv.ParseInt(end, 10, 64)
	if err != nil || last < first {
		return 0, 0, 0, false, fmt.Errorf("malformed range end")
	}
	return first, last, -1, true, nil
}

// prefixEnd returns the smallest string greater than every string with the
// given prefix, for a primary-key range scan. It increments runes, not bytes,
// so the bound stays valid UTF-8 for PostgreSQL text.
func prefixEnd(prefix string) string {
	r := []rune(prefix)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] < unicode.MaxRune {
			r[i]++
			return string(r[:i+1])
		}
	}
	return string(unicode.MaxRune)
}
