package peercache

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

// The peer cache stores metadata as a compact, versioned byte string. It is
// much smaller than JSON (no field names) and decodes without reflection.
const metaVersion = 2

func encodeMeta(m *store.Meta) ([]byte, error) {
	b := make([]byte, 0, 256)
	b = append(b, metaVersion)
	b = binary.BigEndian.AppendUint64(b, uint64(m.Size))
	b = binary.BigEndian.AppendUint64(b, uint64(m.FileID))
	b = binary.BigEndian.AppendUint64(b, uint64(m.CreatedAt.UnixNano()))
	var err error
	if b, err = appendBytes(b, m.SHA256); err != nil {
		return nil, err
	}
	if b, err = appendString(b, m.ETag); err != nil {
		return nil, err
	}
	if b, err = appendString(b, m.ContentType); err != nil {
		return nil, err
	}
	if m.Inline != nil {
		b = append(b, 1)
		if b, err = appendBytes(b, m.Inline); err != nil {
			return nil, err
		}
	} else {
		b = append(b, 0)
	}
	if len(m.UserMetadata) > 128 {
		return nil, fmt.Errorf("too many metadata entries")
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(m.UserMetadata)))
	for _, s := range m.UserMetadata {
		if b, err = appendString(b, s); err != nil {
			return nil, err
		}
	}
	if len(m.Parts) > 10000 || len(m.PartEnds) > 10000 {
		return nil, fmt.Errorf("too many parts")
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.Parts)))
	for _, id := range m.Parts {
		b = binary.BigEndian.AppendUint64(b, uint64(id))
	}
	b = binary.BigEndian.AppendUint32(b, uint32(len(m.PartEnds)))
	for _, end := range m.PartEnds {
		b = binary.BigEndian.AppendUint64(b, uint64(end))
	}
	return b, nil
}

func decodeMeta(b []byte) (store.Meta, error) {
	r := reader{b: b}
	if v, err := r.u8(); err != nil || v != metaVersion {
		return store.Meta{}, fmt.Errorf("unsupported metadata encoding")
	}
	size, err := r.u64()
	if err != nil {
		return store.Meta{}, err
	}
	fileID, err := r.u64()
	if err != nil {
		return store.Meta{}, err
	}
	created, err := r.u64()
	if err != nil {
		return store.Meta{}, err
	}
	sha, err := r.bytes()
	if err != nil {
		return store.Meta{}, err
	}
	etag, err := r.string()
	if err != nil {
		return store.Meta{}, err
	}
	contentType, err := r.string()
	if err != nil {
		return store.Meta{}, err
	}
	hasInline, err := r.u8()
	if err != nil {
		return store.Meta{}, err
	}
	var inline []byte
	if hasInline == 1 {
		inline, err = r.bytes()
		if err != nil {
			return store.Meta{}, err
		}
		if inline == nil {
			inline = []byte{}
		}
	}
	count, err := r.u16()
	if err != nil {
		return store.Meta{}, err
	}
	if count > 128 {
		return store.Meta{}, fmt.Errorf("too many metadata entries")
	}
	userMeta := make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		s, err := r.string()
		if err != nil {
			return store.Meta{}, err
		}
		userMeta = append(userMeta, s)
	}
	parts, err := r.int64s()
	if err != nil {
		return store.Meta{}, err
	}
	ends, err := r.int64s()
	if err != nil {
		return store.Meta{}, err
	}
	if len(parts) != len(ends) {
		return store.Meta{}, fmt.Errorf("incomplete multipart manifest")
	}
	return store.Meta{
		Size: int64(size), FileID: int64(fileID), CreatedAt: time.Unix(0, int64(created)),
		SHA256: sha, ETag: etag, ContentType: contentType, UserMetadata: userMeta,
		Parts: parts, PartEnds: ends, Inline: inline,
	}, nil
}

func appendBytes(b, v []byte) ([]byte, error) {
	if len(v) > 65535 {
		return nil, fmt.Errorf("field too long")
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(v)))
	return append(b, v...), nil
}

func appendString(b []byte, s string) ([]byte, error) {
	if len(s) > 65535 {
		return nil, fmt.Errorf("field too long")
	}
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...), nil
}

type reader struct {
	b   []byte
	off int
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || r.off+n > len(r.b) {
		return nil, fmt.Errorf("truncated metadata")
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v, nil
}

func (r *reader) u8() (byte, error) {
	v, err := r.take(1)
	if err != nil {
		return 0, err
	}
	return v[0], nil
}

func (r *reader) u16() (uint16, error) {
	v, err := r.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(v), nil
}

func (r *reader) u64() (uint64, error) {
	v, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.u16()
	if err != nil {
		return nil, err
	}
	v, err := r.take(int(n))
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (r *reader) string() (string, error) {
	n, err := r.u16()
	if err != nil {
		return "", err
	}
	v, err := r.take(int(n))
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (r *reader) int64s() ([]int64, error) {
	b, err := r.take(4)
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(b)
	if n > 10000 {
		return nil, fmt.Errorf("too many parts")
	}
	if n == 0 {
		return nil, nil
	}
	out := make([]int64, n)
	for i := range out {
		v, err := r.u64()
		if err != nil {
			return nil, err
		}
		out[i] = int64(v)
	}
	return out, nil
}
