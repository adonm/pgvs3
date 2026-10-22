package store

import (
	"fmt"
	"time"
)

// Meta is everything needed to serve a range without reading the objects row
// again.
type Meta struct {
	Size         int64
	SHA256       []byte
	ETag         string
	UserMetadata []string
	ContentType  string
	CreatedAt    time.Time
	ExpiresAt    *time.Time
	// OriginETag records the upstream object version for cache-tier entries.
	OriginETag string
	FileID     int64
	// Multipart objects: part file IDs in order and cumulative end offsets.
	// Both nil for a single-file object.
	Parts    []int64
	PartEnds []int64
	// Inline is non-nil when the whole object lives in objects.inline_data.
	// An empty inline object is []byte{}, not nil.
	Inline []byte
}

// Segment is a contiguous run of bytes stored under one file ID.
type Segment struct {
	FileID int64
	Base   int64
	Length int64
}

// Segments validates the multipart manifest and returns the object's storage
// segments in order. A malformed manifest fails closed rather than serving
// bytes from the wrong file.
func (m *Meta) Segments() ([]Segment, error) {
	if m.Parts == nil && m.PartEnds == nil {
		return []Segment{{FileID: m.FileID, Base: 0, Length: m.Size}}, nil
	}
	if m.Parts == nil || m.PartEnds == nil {
		return nil, fmt.Errorf("incomplete multipart manifest")
	}
	if len(m.Parts) == 0 || len(m.Parts) != len(m.PartEnds) ||
		m.Parts[0] != m.FileID || m.PartEnds[len(m.PartEnds)-1] != m.Size {
		return nil, fmt.Errorf("invalid multipart manifest")
	}
	out := make([]Segment, 0, len(m.Parts))
	var prev int64
	for i, id := range m.Parts {
		end := m.PartEnds[i]
		if end < prev {
			return nil, fmt.Errorf("invalid multipart offsets")
		}
		out = append(out, Segment{FileID: id, Base: prev, Length: end - prev})
		prev = end
	}
	return out, nil
}

// Live reports whether the object has not expired.
func (m *Meta) Live(now time.Time) bool {
	return m.ExpiresAt == nil || m.ExpiresAt.After(now)
}
