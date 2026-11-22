package store

import (
	"errors"
	"io"
	"strings"
	"time"
)

var (
	ErrNotFound     = errors.New("object not found")
	ErrInvalidRange = errors.New("invalid range")
	ErrNotModified  = errors.New("not modified")
	ErrPrecondition = errors.New("precondition failed")
)

type ObjectMeta struct {
	ID              int64
	Bucket          string
	Key             string
	ETag            string
	Size            int64
	ContentType     string
	ContentEncoding string
	LastModified    time.Time
	UserMeta        []string
	FilledAt        time.Time
	LastAccess      time.Time
}

type ReadResult struct {
	ObjectMeta
	Body   io.ReadCloser
	Start  int64
	End    int64
	Ranged bool
	Hit    bool
}

func (r *ReadResult) ContentLength() int64 {
	if r == nil {
		return 0
	}
	if r.Ranged {
		return r.End - r.Start + 1
	}
	return r.Size
}

type ByteRange struct {
	First     int64
	Last      int64
	Suffix    int64
	Requested bool
}

type Span struct {
	Start  int64
	End    int64
	Ranged bool
}

func WholeObjectRange() ByteRange { return ByteRange{Last: -1, Suffix: -1} }

func Range(first, last, suffix int64) ByteRange {
	return ByteRange{First: first, Last: last, Suffix: suffix, Requested: true}
}

func (r ByteRange) Resolve(size int64) (Span, error) {
	if size < 0 {
		return Span{}, ErrInvalidRange
	}
	if !r.Requested {
		return Span{Start: 0, End: size - 1}, nil
	}
	if size == 0 {
		return Span{}, ErrInvalidRange
	}
	if r.Suffix >= 0 {
		if r.Suffix == 0 {
			return Span{}, ErrInvalidRange
		}
		start := max(size-r.Suffix, 0)
		return Span{Start: start, End: size - 1, Ranged: true}, nil
	}
	if r.First < 0 || r.First >= size {
		return Span{}, ErrInvalidRange
	}
	end := r.Last
	if end < 0 || end >= size {
		end = size - 1
	}
	if end < r.First {
		return Span{}, ErrInvalidRange
	}
	return Span{Start: r.First, End: end, Ranged: true}, nil
}

type ReadCondition struct {
	IfMatch     string
	IfNoneMatch string
}

func (c ReadCondition) Check(exists bool, etag string) error {
	if c.IfMatch != "" {
		if !exists || !etagMatches(c.IfMatch, etag) {
			return ErrPrecondition
		}
	}
	if c.IfNoneMatch != "" && exists && etagMatches(c.IfNoneMatch, etag) {
		return ErrNotModified
	}
	return nil
}

func etagMatches(header, etag string) bool {
	for part := range strings.SplitSeq(header, ",") {
		part = strings.TrimSpace(part)
		if part == "*" {
			return true
		}
		part = strings.TrimPrefix(part, "W/")
		part = strings.Trim(part, `"`)
		if part != "" && part == strings.Trim(etag, `"`) {
			return true
		}
	}
	return false
}
