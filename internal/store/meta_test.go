package store

import "testing"

func TestSegmentsSingleAndMultipart(t *testing.T) {
	single := &Meta{FileID: 7, Size: 100}
	segs, err := single.Segments()
	if err != nil || len(segs) != 1 || segs[0] != (Segment{FileID: 7, Length: 100}) {
		t.Fatalf("single: %+v %v", segs, err)
	}
	multi := &Meta{FileID: 3, Size: 10, Parts: []int64{3, 4}, PartEnds: []int64{4, 10}}
	segs, err = multi.Segments()
	if err != nil || len(segs) != 2 || segs[1] != (Segment{FileID: 4, Base: 4, Length: 6}) {
		t.Fatalf("multipart: %+v %v", segs, err)
	}
	for _, bad := range []*Meta{
		{FileID: 3, Size: 10, Parts: []int64{3}},
		{FileID: 3, Size: 10, Parts: []int64{3, 4}, PartEnds: []int64{5, 4}},
		{FileID: 3, Size: 10, Parts: []int64{9}, PartEnds: []int64{10}},
	} {
		if _, err := bad.Segments(); err == nil {
			t.Fatalf("malformed manifest accepted: %+v", bad)
		}
	}
}
