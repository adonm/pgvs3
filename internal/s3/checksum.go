package s3

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
	"strings"

	"github.com/adonm/pgvs3/internal/store"
)

var supportedChecksums = map[string]bool{
	"content-md5":           true,
	"x-amz-checksum-md5":    true,
	"x-amz-checksum-crc32":  true,
	"x-amz-checksum-crc32c": true,
	"x-amz-checksum-sha1":   true,
	"x-amz-checksum-sha256": true,
	"x-amz-checksum-sha512": true,
}

var checksumAlgorithms = map[string]bool{
	"CRC32": true, "CRC32C": true, "SHA1": true, "SHA256": true,
}

type integrity struct {
	h        map[string]hash.Hash
	expected map[string]string
	trailers []string
}

func newIntegrity(h http.Header) (*integrity, error) {
	in := &integrity{h: map[string]hash.Hash{}, expected: map[string]string{}}
	for name, values := range h {
		lower := strings.ToLower(name)
		if lower == "content-md5" || (strings.HasPrefix(lower, "x-amz-checksum-") && lower != "x-amz-checksum-algorithm") {
			if !supportedChecksums[lower] {
				return nil, errNotImplemented()
			}
			if len(values) != 1 {
				return nil, errInvalidRequest("duplicate checksum header")
			}
			in.expected[lower] = values[0]
			in.addHasher(lower)
		}
	}
	if declared := h.Get("x-amz-trailer"); declared != "" {
		for _, name := range strings.Split(declared, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			if !supportedChecksums[name] || name == "content-md5" {
				return nil, errNotImplemented()
			}
			in.trailers = append(in.trailers, name)
			in.addHasher(name)
		}
	}
	for _, algoHeader := range []string{"x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"} {
		algo := h.Get(algoHeader)
		if algo == "" {
			continue
		}
		name := "x-amz-checksum-" + strings.ToLower(algo)
		if !supportedChecksums[name] {
			return nil, errNotImplemented()
		}
		if _, ok := in.expected[name]; !ok && !contains(in.trailers, name) {
			return nil, errInvalidRequest("declared checksum algorithm has no checksum")
		}
	}
	return in, nil
}

func (in *integrity) addHasher(name string) {
	if _, ok := in.h[name]; ok {
		return
	}
	switch name {
	case "content-md5", "x-amz-checksum-md5":
		in.h[name] = md5.New()
	case "x-amz-checksum-crc32":
		in.h[name] = crc32.NewIEEE()
	case "x-amz-checksum-crc32c":
		in.h[name] = crc32.New(crc32.MakeTable(crc32.Castagnoli))
	case "x-amz-checksum-sha1":
		in.h[name] = sha1.New()
	case "x-amz-checksum-sha256":
		in.h[name] = sha256.New()
	case "x-amz-checksum-sha512":
		in.h[name] = sha512.New()
	}
}

// reader hashes bytes as the store consumes them.
type integrityReader struct {
	r  io.ReadCloser
	in *integrity
}

func (r *integrityReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		for _, h := range r.in.h {
			h.Write(p[:n])
		}
	}
	return n, err
}

func (r *integrityReader) Close() error { return r.r.Close() }

// verify checks declared trailers and compares every supplied checksum. It
// must run after the body has been fully consumed, before publication.
func (in *integrity) verify(h http.Header) ([]store.Checksum, error) {
	for _, name := range in.trailers {
		value := h.Get(name)
		if value == "" {
			return nil, errInvalidRequest("missing checksum trailer " + name)
		}
		in.expected[name] = value
	}
	out := make([]store.Checksum, 0, len(in.expected))
	for name, want := range in.expected {
		hasher, ok := in.h[name]
		if !ok {
			return nil, errNotImplemented()
		}
		got := base64.StdEncoding.EncodeToString(hasher.Sum(nil))
		if got != want {
			return nil, apiErr("BadDigest", fmt.Sprintf("The %s you specified did not match the calculated checksum.", name), http.StatusBadRequest)
		}
		out = append(out, store.Checksum{Name: name, Value: got})
	}
	return out, nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
