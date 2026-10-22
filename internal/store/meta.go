package store

import (
	"fmt"
	"time"
)

// Meta is everything needed to serve a range without reading s3p.objects.
type Meta struct {
	Size         int64
	SHA256       []byte
	ETag         string
	UserMetadata []string
	ContentType  string
	CreatedAt    time.Time
	FileID       int64
	// Multipart objects: part file IDs in order and cumulative end offsets.
	// Both nil for a single-file object.
	Parts    []int64
	PartEnds []int64
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
