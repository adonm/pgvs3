package s3

import (
	"net/http"
	"sort"
	"strings"
)

// parseMetadata extracts x-amz-meta-* headers into sorted name/value pairs.
// The layout stores them as a text[] so they travel atomically with the object.
func parseMetadata(h http.Header) ([]string, error) {
	type kv struct{ k, v string }
	var pairs []kv
	total := 0
	for name, values := range h {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "x-amz-meta-") {
			continue
		}
		key := strings.TrimPrefix(lower, "x-amz-meta-")
		if key == "" || len(key) > 128 || !validMetaName(key) {
			return nil, errInvalidRequest("invalid user metadata name")
		}
		for _, value := range values {
			if strings.ContainsAny(value, "\x00\r\n") {
				return nil, errInvalidRequest("invalid user metadata value")
			}
			total += len(key) + len(value)
			if total > 2048 || len(pairs) >= 64 {
				return nil, errInvalidRequest("user metadata exceeds the 2 KiB limit")
			}
			pairs = append(pairs, kv{key, value})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	out := make([]string, 0, len(pairs)*2)
	for _, p := range pairs {
		out = append(out, p.k, p.v)
	}
	return out, nil
}

func validMetaName(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func metadataHeader(pairs []string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Set("x-amz-meta-"+pairs[i], pairs[i+1])
	}
	return h
}

func contentType(v string) (string, error) {
	if v == "" {
		return "application/octet-stream", nil
	}
	if len(v) > 1024 || strings.ContainsAny(v, "\x00\r\n") {
		return "", errInvalidRequest("invalid content type")
	}
	return v, nil
}

func stripETag(v string) string {
	return strings.Trim(strings.TrimPrefix(strings.TrimSpace(v), "W/"), `"`)
}

func validETag(v string) bool {
	v = stripETag(v)
	digest, count, hasCount := strings.Cut(v, "-")
	if len(digest) != 32 {
		return false
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	if hasCount {
		n := 0
		if count == "" {
			return false
		}
		for i := 0; i < len(count); i++ {
			if count[i] < '0' || count[i] > '9' {
				return false
			}
			n = n*10 + int(count[i]-'0')
		}
		return n >= 1 && n <= 10000
	}
	return true
}
