package s3

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

const (
	tarBlockSize          = 512
	tarMaxScanBytes       = 512 << 20
	tarMaxMetadataBytes   = 1 << 20
	tarZstdStreamMaxBytes = 64 << 20
	archiveMethodSeekZstd = 0x100
)

// buildTarIndex walks 512-byte TAR headers only. Member bodies are skipped by
// offset, so indexing a 512 MiB uncompressed TAR does not download its files.
func (s *Server) buildTarIndex(ctx context.Context, backend readBackend, bucket, archiveKey string, info store.ObjectInfo, archiveMeta *store.Meta) (*zipIndex, error) {
	if info.Size > tarMaxScanBytes {
		return nil, tar.ErrHeader
	}
	ra, err := newZipRangeReaderAt(ctx, backend, bucket, archiveKey, info.Size, archiveMeta)
	if err != nil {
		return nil, err
	}
	entries, err := walkTarIndex(ra, info.Size, zip.Store)
	if err != nil {
		return nil, err
	}
	return newArchiveIndex(archiveKey, info, archiveTar, finalizeArchiveMembers(entries), nil, ""), nil
}

// buildTarZstdIndex prefers the Seekable Zstd jump table so headers can be
// hopped in uncompressed space without decompressing from the first frame.
func (s *Server) buildTarZstdIndex(ctx context.Context, backend readBackend, bucket, archiveKey string, info store.ObjectInfo, archiveMeta *store.Meta) (*zipIndex, error) {
	if info.Size > tarMaxScanBytes {
		return nil, tar.ErrHeader
	}
	table, err := readZstdSeekTable(ctx, backend, bucket, archiveKey, info.Size, archiveMeta)
	if err != nil {
		return nil, err
	}
	if table != nil {
		openCompressed := func(start, length int64) (io.ReadCloser, error) {
			return openZipArchiveRange(ctx, backend, bucket, archiveKey, start, start+length-1, archiveMeta)
		}
		ra := &zstdSeekReaderAt{
			ctx:  ctx,
			size: table.uncompressedSize,
			open: func(start, length int64) (io.ReadCloser, error) {
				return openSeekableZstdRange(ctx, openCompressed, table, start, start+length-1)
			},
			table: table,
		}
		entries, err := walkTarIndex(ra, table.uncompressedSize, archiveMethodSeekZstd)
		if err != nil {
			return nil, err
		}
		return newArchiveIndex(archiveKey, info, archiveTarZstd, finalizeArchiveMembers(entries), table, ""), nil
	}
	if info.Size > tarZstdStreamMaxBytes {
		return nil, tar.ErrHeader
	}
	data, err := decompressTarZstd(ctx, backend, bucket, archiveKey, info.Size, archiveMeta)
	if err != nil {
		return nil, err
	}
	entries, err := walkTarIndex(bytes.NewReader(data), int64(len(data)), zip.Store)
	if err != nil {
		return nil, err
	}
	dataKey := archiveTarDataKey(bucket, archiveKey, info.ETag)
	s.cacheDecompressed(dataKey, data)
	return newArchiveIndex(archiveKey, info, archiveTarZstd, finalizeArchiveMembers(entries), nil, dataKey), nil
}

func readZstdSeekTable(ctx context.Context, backend readBackend, bucket, archiveKey string, size int64, archiveMeta *store.Meta) (*zstdSeekTable, error) {
	readTail := func(n int64) ([]byte, error) {
		body, err := openZipArchiveRange(ctx, backend, bucket, archiveKey, size-n, n, archiveMeta)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		data, err := io.ReadAll(io.LimitReader(body, n))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != n {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	}
	n := min(size, int64(zstdSeekTailSize))
	tail, err := readTail(n)
	if err != nil {
		return nil, err
	}
	table, required, ok := parseZstdSeekTable(tail, size)
	if !ok && required > int64(len(tail)) && required <= size {
		tail, err = readTail(required)
		if err != nil {
			return nil, err
		}
		table, _, ok = parseZstdSeekTable(tail, size)
	}
	if !ok {
		return nil, nil
	}
	return table, nil
}

func decompressTarZstd(ctx context.Context, backend readBackend, bucket, archiveKey string, size int64, archiveMeta *store.Meta) ([]byte, error) {
	body, err := openZipArchiveRange(ctx, backend, bucket, archiveKey, 0, size-1, archiveMeta)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	dec := zipZstdDecompressor(body)
	defer dec.Close()
	data, err := io.ReadAll(io.LimitReader(dec, tarZstdStreamMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > tarZstdStreamMaxBytes {
		return nil, tar.ErrHeader
	}
	return data, nil
}

func archiveTarDataKey(bucket, archiveKey, etag string) string {
	return bucket + "\x00" + archiveKey + "\x00" + etag + "\x00tar"
}

func newArchiveIndex(archiveKey string, info store.ObjectInfo, kind archiveKind, entries []zipMember, table *zstdSeekTable, tarDataKey string) *zipIndex {
	idx := &zipIndex{
		archiveKey:  archiveKey,
		archiveETag: info.ETag,
		archiveSize: info.Size,
		createdAt:   time.Now(),
		kind:        kind,
		seekTable:   table,
		tarDataKey:  tarDataKey,
		entries:     entries,
		byName:      make(map[string]int, len(entries)),
	}
	for i := range entries {
		entries[i].seekz = &zstdSeekState{}
		idx.byName[entries[i].name] = i
	}
	return idx
}

func finalizeArchiveMembers(members []zipMember) []zipMember {
	slices.SortStableFunc(members, func(a, b zipMember) int { return strings.Compare(a.name, b.name) })
	out := members[:0]
	for i := range members {
		if len(out) > 0 && out[len(out)-1].name == members[i].name {
			continue
		}
		out = append(out, members[i])
	}
	return out
}

// walkTarIndex reads only 512-byte headers and small PAX/GNU metadata payloads.
func walkTarIndex(ra io.ReaderAt, size int64, method uint16) ([]zipMember, error) {
	members := make([]zipMember, 0, 64)
	var (
		longName string
		paxPath  string
		paxSize  int64 = -1
		paxTime  time.Time
		off      int64
	)
	readPayload := func(dataOffset, length int64) ([]byte, error) {
		if length < 0 || length > tarMaxMetadataBytes || dataOffset < 0 || dataOffset > size || length > size-dataOffset {
			return nil, tar.ErrHeader
		}
		buf := make([]byte, int(length))
		if length > 0 {
			if _, err := ra.ReadAt(buf, dataOffset); err != nil {
				return nil, err
			}
		}
		return buf, nil
	}
	applyPAX := func(payload []byte) {
		for len(payload) > 0 {
			space := bytes.IndexByte(payload, ' ')
			if space <= 0 {
				return
			}
			n, err := strconv.Atoi(string(payload[:space]))
			if err != nil || n <= space+1 || n > len(payload) {
				return
			}
			record := payload[space+1 : n]
			payload = payload[n:]
			record = bytes.TrimSuffix(record, []byte("\n"))
			key, value, ok := strings.Cut(string(record), "=")
			if !ok {
				continue
			}
			switch key {
			case "path":
				paxPath = value
			case "size":
				if parsed, err := strconv.ParseInt(value, 10, 64); err == nil && parsed >= 0 {
					paxSize = parsed
				}
			case "mtime":
				if parsed, err := strconv.ParseFloat(value, 64); err == nil {
					sec := int64(parsed)
					paxTime = time.Unix(sec, int64((parsed-float64(sec))*1e9)).UTC()
				}
			}
		}
	}
	for off+tarBlockSize <= size {
		var block [tarBlockSize]byte
		if _, err := ra.ReadAt(block[:], off); err != nil {
			return nil, err
		}
		if isZeroBlock(block[:]) {
			break
		}
		name, fileSize, typeflag, modified, ok := parseTarHeader(block[:])
		if !ok {
			return nil, tar.ErrHeader
		}
		dataOffset := off + tarBlockSize
		if dataOffset < off || dataOffset > size || fileSize < 0 || fileSize > size-dataOffset {
			return nil, tar.ErrHeader
		}
		switch typeflag {
		case 'x':
			payload, err := readPayload(dataOffset, fileSize)
			if err != nil {
				return nil, err
			}
			applyPAX(payload)
			off = dataOffset + alignTarBlock(fileSize)
			continue
		case 'g':
			// Global PAX headers are rare in analytical archives; skip them.
			off = dataOffset + alignTarBlock(fileSize)
			continue
		case 'L':
			payload, err := readPayload(dataOffset, fileSize)
			if err != nil {
				return nil, err
			}
			longName = strings.TrimRight(string(payload), "\x00")
			off = dataOffset + alignTarBlock(fileSize)
			continue
		case 'K':
			off = dataOffset + alignTarBlock(fileSize)
			continue
		}
		memberName := name
		if longName != "" {
			memberName = longName
		}
		if paxPath != "" {
			memberName = paxPath
		}
		memberSize := fileSize
		if paxSize >= 0 {
			memberSize = paxSize
		}
		memberTime := modified
		if !paxTime.IsZero() {
			memberTime = paxTime
		}
		longName, paxPath, paxSize, paxTime = "", "", -1, time.Time{}
		if typeflag == '0' || typeflag == '7' {
			if clean, valid := cleanZipMemberName(memberName); valid {
				if memberSize < 0 || memberSize > size-dataOffset {
					return nil, tar.ErrHeader
				}
				members = append(members, zipMember{
					name:             clean,
					headerOffset:     dataOffset,
					dataOffset:       dataOffset,
					compressedSize:   memberSize,
					uncompressedSize: memberSize,
					crc32:            crc32.ChecksumIEEE(fmt.Appendf(nil, "%s:%d:%d", clean, dataOffset, memberSize)),
					method:           method,
					modified:         memberTime,
				})
			}
		}
		off = dataOffset + alignTarBlock(memberSize)
	}
	return members, nil
}

func parseTarHeader(block []byte) (name string, size int64, typeflag byte, modified time.Time, ok bool) {
	if len(block) != tarBlockSize {
		return "", 0, 0, time.Time{}, false
	}
	name = cString(block[0:100])
	if string(block[257:263]) == "ustar\x00" {
		if prefix := cString(block[345:500]); prefix != "" {
			name = prefix + "/" + name
		}
	}
	if name == "" {
		return "", 0, 0, time.Time{}, false
	}
	size, ok = tarNumber(block[124:136])
	if !ok {
		return "", 0, 0, time.Time{}, false
	}
	modified = time.Time{}
	if sec, valid := tarNumber(block[136:148]); valid && sec > 0 {
		modified = time.Unix(sec, 0).UTC()
	}
	typeflag = block[156]
	if typeflag == 0 {
		typeflag = '0'
	}
	return name, size, typeflag, modified, true
}

func tarNumber(field []byte) (int64, bool) {
	if len(field) == 0 {
		return 0, false
	}
	if field[0]&0x80 != 0 {
		if field[0]&0x40 != 0 {
			return 0, false // negative base-256 values are not valid sizes/times here
		}
		var value uint64
		for _, b := range field[1:] {
			value = value<<8 | uint64(b)
		}
		if value > 1<<63-1 {
			return 0, false
		}
		return int64(value), true
	}
	text := strings.Trim(string(field), " \x00")
	if text == "" {
		return 0, true
	}
	value, err := strconv.ParseInt(text, 8, 64)
	return value, err == nil
}

func cString(field []byte) string {
	if i := bytes.IndexByte(field, 0); i >= 0 {
		field = field[:i]
	}
	return string(field)
}

func isZeroBlock(block []byte) bool {
	for _, b := range block {
		if b != 0 {
			return false
		}
	}
	return true
}

func alignTarBlock(size int64) int64 {
	return (size + tarBlockSize - 1) / tarBlockSize * tarBlockSize
}

// zstdSeekReaderAt exposes uncompressed Seekable Zstd bytes with a small
// read-ahead cache, so walking TAR headers costs one range per 64 KiB, not one
// range per header.
type zstdSeekReaderAt struct {
	ctx   context.Context
	table *zstdSeekTable
	open  func(start, length int64) (io.ReadCloser, error)
	size  int64

	mu         sync.Mutex
	cacheStart int64
	cache      []byte
}

func (r *zstdSeekReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off >= r.size {
		return 0, io.EOF
	}
	want := len(p)
	if int64(want) > r.size-off {
		want = int(r.size - off)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache != nil && off >= r.cacheStart && off+int64(want) <= r.cacheStart+int64(len(r.cache)) {
		n := copy(p[:want], r.cache[off-r.cacheStart:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	window := max(int64(want), int64(zstdSeekTailSize))
	if off+window > r.size {
		window = r.size - off
	}
	body, err := r.open(off, window)
	if err != nil {
		return 0, fmt.Errorf("zstd seek open %d+%d: %w", off, window, err)
	}
	data, err := io.ReadAll(io.LimitReader(body, window))
	closeErr := body.Close()
	if err != nil {
		return 0, fmt.Errorf("zstd seek read %d+%d: %w", off, window, err)
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if int64(len(data)) != window {
		return 0, fmt.Errorf("zstd seek got %d of %d bytes at %d: %w", len(data), window, off, io.ErrUnexpectedEOF)
	}
	r.cacheStart, r.cache = off, data
	n := copy(p[:want], data[:want])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (s *Server) openTarSeekMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span) (io.ReadCloser, error) {
	if idx.seekTable == nil {
		return nil, errNotImplemented()
	}
	openRange := func(start, length int64) (io.ReadCloser, error) {
		return openZipArchiveRange(ctx, backend, bucket, idx.archiveKey, start, start+length-1, archiveMeta)
	}
	return openSeekableZstdRange(ctx, openRange, idx.seekTable, member.dataOffset+span.Start, member.dataOffset+span.End)
}

func (s *Server) openTarDataMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span) (io.ReadCloser, error) {
	data, ok := s.decompressed(idx.tarDataKey)
	if !ok {
		var err error
		data, err = decompressTarZstd(ctx, backend, bucket, idx.archiveKey, idx.archiveSize, archiveMeta)
		if err != nil {
			return nil, err
		}
		s.cacheDecompressed(idx.tarDataKey, data)
	}
	start := member.dataOffset + span.Start
	end := member.dataOffset + span.End
	if start < 0 || end < start || end >= int64(len(data)) {
		return nil, errInvalidRange()
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
}
