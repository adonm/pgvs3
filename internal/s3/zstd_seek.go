package s3

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// The Zstandard Seekable Format stores a jump table in a skippable frame:
//
//	0x184D2A5E | frame size | num frames | descriptor | entries | 0x8F92EAB1
//
// Each entry is the compressed size and the uncompressed size of one
// independent frame. Accumulating those sizes yields both offsets, so a range
// read can open only the frames that contain it.
const (
	zstdSeekMagic      = 0x8F92EAB1
	zstdSkippableMagic = 0x184D2A5E
	zstdSeekTailSize   = 64 << 10
)

type zstdSeekFrame struct {
	compOffset   int64
	compSize     int64
	uncompOffset int64
	uncompSize   int64
}

type zstdSeekTable struct {
	frames           []zstdSeekFrame
	uncompressedSize int64
}

// parseZstdSeekTable parses the seek table at the end of tail. The returned
// required size is larger than len(tail) when the caller must fetch a wider
// tail before parsing can succeed.
func parseZstdSeekTable(tail []byte, totalCompSize int64) (*zstdSeekTable, int64, bool) {
	if len(tail) < 17 {
		return nil, 0, false
	}
	if binary.LittleEndian.Uint32(tail[len(tail)-4:]) != zstdSeekMagic {
		return nil, 0, false
	}
	numFrames := binary.LittleEndian.Uint32(tail[len(tail)-9 : len(tail)-5])
	descriptor := tail[len(tail)-5]
	entrySize := int64(8)
	if descriptor&0x80 != 0 {
		entrySize = 12
	}
	tableBytes := int64(numFrames)*entrySize + 9
	required := tableBytes + 8
	if required > int64(len(tail)) {
		return nil, required, false
	}
	tableStart := len(tail) - int(required)
	if binary.LittleEndian.Uint32(tail[tableStart:]) != zstdSkippableMagic {
		return nil, required, false
	}
	if int64(binary.LittleEndian.Uint32(tail[tableStart+4:])) != tableBytes {
		return nil, required, false
	}
	frames := make([]zstdSeekFrame, 0, numFrames)
	var compOffset, uncompOffset int64
	pos := tableStart + 8
	for range numFrames {
		compSize := int64(binary.LittleEndian.Uint32(tail[pos:]))
		uncompSize := int64(binary.LittleEndian.Uint32(tail[pos+4:]))
		pos += int(entrySize)
		if compSize <= 0 || uncompSize < 0 {
			return nil, required, false
		}
		frames = append(frames, zstdSeekFrame{
			compOffset: compOffset, compSize: compSize,
			uncompOffset: uncompOffset, uncompSize: uncompSize,
		})
		compOffset += compSize
		uncompOffset += uncompSize
	}
	// Entries precede the footer: numFrames (4), descriptor (1), magic (4).
	if pos != len(tail)-9 {
		return nil, required, false
	}
	if compOffset+required != totalCompSize {
		return nil, required, false
	}
	return &zstdSeekTable{frames: frames, uncompressedSize: uncompOffset}, required, true
}

func (t *zstdSeekTable) frameFor(offset int64) int {
	if offset < 0 || len(t.frames) == 0 {
		return -1
	}
	i := sort.Search(len(t.frames), func(i int) bool {
		return t.frames[i].uncompOffset+t.frames[i].uncompSize > offset
	})
	if i == len(t.frames) {
		return -1
	}
	return i
}

// openSeekableZstdRange opens one contiguous compressed span covering the
// frames that intersect [spanStart, spanEnd], then skips and limits in the
// decompressed stream.
func openSeekableZstdRange(ctx context.Context, openRange func(start, length int64) (io.ReadCloser, error), table *zstdSeekTable, spanStart, spanEnd int64) (io.ReadCloser, error) {
	if spanEnd < spanStart {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	first := table.frameFor(spanStart)
	last := table.frameFor(spanEnd)
	if first < 0 || last < 0 || last < first {
		return nil, fmt.Errorf("zstd: range %d-%d is outside the seekable frames", spanStart, spanEnd)
	}
	start := table.frames[first].compOffset
	end := table.frames[last].compOffset + table.frames[last].compSize - 1
	body, err := openRange(start, end-start+1)
	if err != nil {
		return nil, fmt.Errorf("zstd: open compressed %d+%d for uncompressed %d-%d (frames %d-%d): %w",
			start, end-start+1, spanStart, spanEnd, first, last, err)
	}
	decomp := zipZstdDecompressor(io.LimitReader(body, end-start+1))
	if skip := spanStart - table.frames[first].uncompOffset; skip > 0 {
		if _, err := io.CopyN(io.Discard, decomp, skip); err != nil {
			_ = decomp.Close()
			_ = body.Close()
			return nil, err
		}
	}
	out := &zipDecompressBody{
		reader: io.LimitReader(decomp, spanEnd-spanStart+1),
		decomp: decomp,
		body:   body,
	}
	return out, nil
}
