package s3

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"mime"
	"net/http"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/singleflight"
)

// Transparent ZIP/ZIP64 access follows the design in
// https://github.com/LieberInstitute/aws-s3-zip64test/blob/main/docs/zip64-on-s3.md:
// one tail range, the central directory when it does not fit in the tail, then
// one cut-through range per member. Nothing here is written to the archive.

const (
	zipTailSize              = 256 << 10
	zipReadAhead             = 64 << 10
	zipMaxArchives           = 64
	zipMaxCachedMembers      = 64000
	zipMaxCentralSize        = 64 << 20
	zipMaxDecompressedMember = 16 << 20
	zipMaxDecompressedBytes  = 64 << 20
	zipLocalHeaderLen        = 30
	zipDataDescriptor        = 0x08074b50
	zipLocalSignature        = 0x04034b50
	zipCentralSig            = 0x02014b50
	zipEndSignature          = 0x06054b50
	zip64EndSignature        = 0x06064b50
	zip64LocSignature        = 0x07064b50
	zip64ExtraID             = 0x0001
	zipExtTimeExtraID        = 0x5455
	zipNTFSExtraID           = 0x000a
)

var zipZstdDecompressor = zstd.ZipDecompressor()

type zipFlateReadCloser interface {
	io.ReadCloser
	Reset(r io.Reader, dict []byte) error
}

var zipFlatePool = sync.Pool{New: func() any {
	r := flate.NewReader(bytes.NewReader(nil))
	return r.(zipFlateReadCloser)
}}

type zipIndexCache struct {
	mu         sync.Mutex
	indexes    map[string]*zipIndex
	memberRefs int
	group      singleflight.Group

	decompMu    sync.Mutex
	decomp      map[string]*zipDecompressed
	decompBytes int64
}

type zipDecompressed struct {
	data     []byte
	lastUsed time.Time
}

type archiveKind uint8

const (
	archiveZip archiveKind = iota
	archiveTar
	archiveTarZstd
)

type zipIndex struct {
	archiveKey  string
	archiveETag string
	archiveSize int64
	createdAt   time.Time
	kind        archiveKind
	seekTable   *zstdSeekTable
	tarDataKey  string
	entries     []zipMember
	byName      map[string]int
}

type zipMember struct {
	name             string
	headerOffset     int64
	dataOffset       int64
	compressedSize   int64
	uncompressedSize int64
	crc32            uint32
	method           uint16
	modified         time.Time
	seekz            *zstdSeekState
}

type zstdSeekState struct {
	table atomic.Pointer[zstdSeekTable]
}

// noZstdSeek marks a member whose tail was checked and is not seekable, so
// later range reads do not spend another request on the same tail.
var noZstdSeek = &zstdSeekTable{}

func (idx *zipIndex) member(name string) (*zipMember, bool) {
	i, ok := idx.byName[name]
	if !ok {
		return nil, false
	}
	return &idx.entries[i], true
}

func (idx *zipIndex) memberETag(m *zipMember) string {
	return fmt.Sprintf("%s-%08x", strings.Trim(idx.archiveETag, `"`), m.crc32)
}

func (idx *zipIndex) objectInfo(m *zipMember) store.ObjectInfo {
	return store.ObjectInfo{
		Size:        m.uncompressedSize,
		ETag:        idx.memberETag(m),
		ContentType: zipContentType(m.name),
		CreatedAt:   m.modified,
	}
}

func (idx *zipIndex) listing(archiveKey, keyPrefix, delimiter, after string, limit int) []store.Listed {
	if limit <= 0 {
		return nil
	}
	full := func(i int) string { return archiveKey + "/" + idx.entries[i].name }
	start := sort.Search(len(idx.entries), func(i int) bool { return full(i) >= keyPrefix })
	// foldList already emitted the common prefix that contains after on the
	// previous call, so jump past that whole prefix instead of scanning it.
	if delimiter != "" && after != "" {
		if cp, ok := virtualPrefix(after, keyPrefix, delimiter); ok {
			start += sort.Search(len(idx.entries)-start, func(k int) bool { return full(start+k) >= cp })
			start = idx.prefixEnd(full, start, cp)
		}
	}
	out := make([]store.Listed, 0, min(limit, 32))
	for i := start; i < len(idx.entries) && len(out) < limit; i++ {
		key := full(i)
		if !strings.HasPrefix(key, keyPrefix) {
			break
		}
		if key <= after {
			continue
		}
		m := &idx.entries[i]
		out = append(out, store.Listed{
			Key:       key,
			Size:      m.uncompressedSize,
			ETag:      idx.memberETag(m),
			CreatedAt: m.modified,
		})
		// One representative per common prefix is enough for foldList; skip
		// the rest of the prefix with a binary search.
		if delimiter != "" {
			if cp, ok := virtualPrefix(key, keyPrefix, delimiter); ok {
				i = idx.prefixEnd(full, i+1, cp) - 1
			}
		}
	}
	return out
}

func (idx *zipIndex) prefixEnd(full func(int) string, from int, prefix string) int {
	if from >= len(idx.entries) {
		return from
	}
	return from + sort.Search(len(idx.entries)-from, func(k int) bool {
		return !strings.HasPrefix(full(from+k), prefix)
	})
}

// splitArchiveKey turns "datasets/sample.zip/inner/quant.sf" into
// ("datasets/sample.zip", "inner/quant.sf", archiveZip, true). The caller only
// uses it after a literal key lookup failed, so a real S3 object always shadows
// a member. Suffixes are matched case-insensitively; ".tar.zst/" is checked
// before ".tar/" so the compressed TAR suffix wins.
func splitArchiveKey(key string) (archiveKey, memberPath string, kind archiveKind, ok bool) {
	lower := strings.ToLower(key)
	suffixes := []struct {
		text string
		kind archiveKind
	}{
		{".tar.zst/", archiveTarZstd},
		{".tzst/", archiveTarZstd},
		{".zip/", archiveZip},
		{".tar/", archiveTar},
	}
	best := -1
	for _, candidate := range suffixes {
		i := strings.Index(lower, candidate.text)
		if i < 0 || (best >= 0 && i >= best) {
			continue
		}
		best = i
		archiveKey = key[:i+len(candidate.text)-1]
		memberPath = key[i+len(candidate.text):]
		kind = candidate.kind
		ok = true
	}
	if !ok || !safeZipMemberPath(memberPath) {
		return "", "", archiveZip, false
	}
	return archiveKey, memberPath, kind, true
}

// splitZipKey is the ZIP-only form used by callers that predate TAR support.
func splitZipKey(key string) (archiveKey, memberPath string, ok bool) {
	archiveKey, memberPath, kind, ok := splitArchiveKey(key)
	return archiveKey, memberPath, ok && kind == archiveZip
}

func archiveKindForKey(key string) archiveKind {
	lower := strings.ToLower(key)
	switch {
	case strings.HasSuffix(lower, ".tar.zst"), strings.HasSuffix(lower, ".tzst"):
		return archiveTarZstd
	case strings.HasSuffix(lower, ".tar"):
		return archiveTar
	default:
		return archiveZip
	}
}

func safeZipMemberPath(memberPath string) bool {
	if memberPath == "" {
		return true
	}
	if strings.HasPrefix(memberPath, "/") || strings.ContainsRune(memberPath, 0) {
		return false
	}
	for seg := range strings.SplitSeq(memberPath, "/") {
		if seg == ".." {
			return false
		}
	}
	return true
}

func cleanZipMemberName(name string) (string, bool) {
	if name == "" || strings.HasSuffix(name, "/") || strings.ContainsRune(name, 0) {
		return "", false
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || strings.HasPrefix(clean, "/") {
		return "", false
	}
	if len(clean) >= 2 && clean[1] == ':' {
		return "", false
	}
	return clean, true
}

func zipContentType(name string) string {
	switch strings.ToLower(path.Ext(name)) {
	case ".parquet":
		return "application/vnd.apache.parquet"
	case ".tsv", ".sf":
		return "text/tab-separated-values"
	case ".csv":
		return "text/csv"
	case ".json":
		return "application/json"
	case ".zip":
		return "application/zip"
	case ".gz":
		return "application/gzip"
	}
	if ct := mime.TypeByExtension(path.Ext(name)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

func (s *Server) loadZipIndex(ctx context.Context, backend readBackend, bucket, archiveKey string) (*zipIndex, *store.Meta, error) {
	var (
		info        store.ObjectInfo
		archiveMeta *store.Meta
	)
	if lb, isLocal := backend.(localBackend); isLocal && lb.store != nil {
		meta, err := lb.store.Meta(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: archiveKey})
		if err != nil {
			return nil, nil, err
		}
		info, archiveMeta = meta.ObjectInfo, meta
	} else {
		head, err := backend.Head(ctx, bucket, archiveKey)
		if err != nil {
			return nil, nil, err
		}
		info = head.Info
	}
	if info.Size <= 0 {
		return nil, nil, zip.ErrFormat
	}
	idx, err := s.zipIndexFor(ctx, backend, bucket, archiveKey, info, archiveMeta)
	return idx, archiveMeta, err
}

// loadZipIndexWithMeta builds or reuses an archive index from metadata that a
// caller already resolved, avoiding a second metadata round trip.
func (s *Server) loadZipIndexWithMeta(ctx context.Context, backend readBackend, bucket, archiveKey string, archiveMeta *store.Meta) (*zipIndex, error) {
	if archiveMeta == nil {
		idx, _, err := s.loadZipIndex(ctx, backend, bucket, archiveKey)
		return idx, err
	}
	if archiveMeta.Size <= 0 {
		return nil, zip.ErrFormat
	}
	meta := archiveMeta
	if meta.FileID == 0 && meta.Inline == nil {
		// Inline archives have no chunk rows, so the header-only metadata from
		// a HEAD lookup cannot build an index. Fetch the decoded row once.
		if lb, isLocal := backend.(localBackend); isLocal && lb.store != nil {
			decoded, err := lb.store.Meta(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: archiveKey})
			if err != nil {
				return nil, err
			}
			meta = decoded
		}
	}
	return s.zipIndexFor(ctx, backend, bucket, archiveKey, meta.ObjectInfo, meta)
}

func (s *Server) zipIndexFor(ctx context.Context, backend readBackend, bucket, archiveKey string, info store.ObjectInfo, archiveMeta *store.Meta) (*zipIndex, error) {
	cacheKey := bucket + "\x00" + archiveKey
	s.zip.mu.Lock()
	cached := s.zip.indexes[cacheKey]
	s.zip.mu.Unlock()
	if cached != nil && cached.archiveETag == info.ETag && cached.archiveSize == info.Size {
		return cached, nil
	}
	flightKey := cacheKey + "\x00" + info.ETag
	value, err, _ := s.zip.group.Do(flightKey, func() (any, error) {
		idx, err := s.buildZipIndex(ctx, backend, bucket, archiveKey, info, archiveMeta)
		if err != nil {
			return nil, err
		}
		s.cacheZipIndex(cacheKey, idx)
		return idx, nil
	})
	if err != nil {
		return nil, err
	}
	idx, _ := value.(*zipIndex)
	if idx == nil {
		return nil, zip.ErrFormat
	}
	return idx, nil
}

func (s *Server) cacheZipIndex(cacheKey string, idx *zipIndex) {
	s.zip.mu.Lock()
	defer s.zip.mu.Unlock()
	if s.zip.indexes == nil {
		s.zip.indexes = make(map[string]*zipIndex)
	}
	if old := s.zip.indexes[cacheKey]; old != nil {
		s.zip.memberRefs -= len(old.entries)
	}
	s.zip.indexes[cacheKey] = idx
	s.zip.memberRefs += len(idx.entries)
	for len(s.zip.indexes) > zipMaxArchives || s.zip.memberRefs > zipMaxCachedMembers {
		oldestKey := ""
		var oldest time.Time
		for key, candidate := range s.zip.indexes {
			if key == cacheKey {
				continue
			}
			if oldestKey == "" || candidate.createdAt.Before(oldest) {
				oldestKey, oldest = key, candidate.createdAt
			}
		}
		if oldestKey == "" {
			break // a single archive larger than the budget stays hot
		}
		s.zip.memberRefs -= len(s.zip.indexes[oldestKey].entries)
		delete(s.zip.indexes, oldestKey)
	}
}

func (s *Server) decompressed(key string) ([]byte, bool) {
	s.zip.decompMu.Lock()
	defer s.zip.decompMu.Unlock()
	entry := s.zip.decomp[key]
	if entry == nil {
		return nil, false
	}
	entry.lastUsed = time.Now()
	return entry.data, true
}

func (s *Server) cacheDecompressed(key string, data []byte) {
	s.zip.decompMu.Lock()
	defer s.zip.decompMu.Unlock()
	if s.zip.decomp == nil {
		s.zip.decomp = make(map[string]*zipDecompressed)
	}
	if old := s.zip.decomp[key]; old != nil {
		s.zip.decompBytes -= int64(len(old.data))
	}
	s.zip.decomp[key] = &zipDecompressed{data: data, lastUsed: time.Now()}
	s.zip.decompBytes += int64(len(data))
	for s.zip.decompBytes > zipMaxDecompressedBytes {
		oldestKey := ""
		var oldest time.Time
		for candidateKey, candidate := range s.zip.decomp {
			if candidateKey == key {
				continue
			}
			if oldestKey == "" || candidate.lastUsed.Before(oldest) {
				oldestKey, oldest = candidateKey, candidate.lastUsed
			}
		}
		if oldestKey == "" {
			break // a single member larger than the budget stays hot
		}
		s.zip.decompBytes -= int64(len(s.zip.decomp[oldestKey].data))
		delete(s.zip.decomp, oldestKey)
	}
}

func zipDecompressedKey(bucket string, idx *zipIndex, member *zipMember) string {
	return bucket + "\x00" + idx.archiveKey + "\x00" + idx.archiveETag + "\x00" + member.name
}

func (s *Server) buildZipIndex(ctx context.Context, backend readBackend, bucket, archiveKey string, info store.ObjectInfo, archiveMeta *store.Meta) (*zipIndex, error) {
	switch archiveKindForKey(archiveKey) {
	case archiveTar:
		return s.buildTarIndex(ctx, backend, bucket, archiveKey, info, archiveMeta)
	case archiveTarZstd:
		return s.buildTarZstdIndex(ctx, backend, bucket, archiveKey, info, archiveMeta)
	}
	ra, err := newZipRangeReaderAt(ctx, backend, bucket, archiveKey, info.Size, archiveMeta)
	if err != nil {
		return nil, err
	}
	entries, err := parseZipCentralDirectory(ra, info.Size)
	if err != nil {
		return nil, err
	}
	idx := &zipIndex{
		archiveKey:  archiveKey,
		archiveETag: info.ETag,
		archiveSize: info.Size,
		createdAt:   time.Now(),
		kind:        archiveZip,
		entries:     entries,
		byName:      make(map[string]int, len(entries)),
	}
	for i := range entries {
		entries[i].seekz = &zstdSeekState{}
		idx.byName[entries[i].name] = i
	}
	return idx, nil
}

type zipRangeReaderAt struct {
	ctx        context.Context
	backend    readBackend
	bucket     string
	key        string
	size       int64
	tailOffset int64
	tail       []byte
	meta       *store.Meta
}

func newZipRangeReaderAt(ctx context.Context, backend readBackend, bucket, key string, size int64, meta *store.Meta) (*zipRangeReaderAt, error) {
	if size <= 0 {
		return nil, zip.ErrFormat
	}
	ra := &zipRangeReaderAt{ctx: ctx, backend: backend, bucket: bucket, key: key, size: size, meta: meta}
	tailSize := int64(zipTailSize)
	if ob, ok := backend.(originBackend); ok && ob.RangeChunkSize() > tailSize {
		// Align the archive tail to --range-cache-chunk so the footer and
		// central directory land in a cached s3c_ chunk instead of a one-off
		// WAN range that the next request repeats.
		tailSize = ob.RangeChunkSize()
	}
	n := min(size, tailSize)
	ra.tailOffset = size - n
	body, err := openZipArchiveRange(ctx, backend, bucket, key, ra.tailOffset, size-1, meta)
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
	ra.tail = data
	return ra, nil
}

func (r *zipRangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 {
		return 0, fmt.Errorf("zip: invalid offset %d", off)
	}
	if off >= r.size {
		return 0, io.EOF
	}
	want := len(p)
	if int64(want) > r.size-off {
		want = int(r.size - off)
	}
	if off >= r.tailOffset && off+int64(want) <= r.tailOffset+int64(len(r.tail)) {
		n := copy(p[:want], r.tail[off-r.tailOffset:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	end := min(off+max(int64(want), zipReadAhead), r.size) - 1
	body, err := openZipArchiveRange(r.ctx, r.backend, r.bucket, r.key, off, end, r.meta)
	if err != nil {
		return 0, err
	}
	defer body.Close()
	n, err := io.ReadFull(body, p[:want])
	if err != nil {
		return n, err
	}
	if want < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// parseZipCentralDirectory finds the EOCD (and ZIP64 EOCD when needed), reads
// the whole central directory with one ReadAt and parses every 46-byte entry
// directly. archive/zip is not used, so local header offsets are available
// without a second parse or per-member request.
func parseZipCentralDirectory(ra io.ReaderAt, size int64) ([]zipMember, error) {
	eocdOffset, eocd, err := readZipEnd(ra, size)
	if err != nil {
		return nil, err
	}
	entryCount := uint64(binary.LittleEndian.Uint16(eocd[10:]))
	dirSize := uint64(binary.LittleEndian.Uint32(eocd[12:]))
	dirOffset := uint64(binary.LittleEndian.Uint32(eocd[16:]))
	endOffset := eocdOffset
	if entryCount == 0xffff || dirSize == 0xffffffff || dirOffset == 0xffffffff {
		zip64Offset, zip64, found, zerr := findZip64End(ra, eocdOffset)
		if zerr != nil {
			return nil, zerr
		}
		if found {
			endOffset = zip64Offset
			entryCount = binary.LittleEndian.Uint64(zip64[32:])
			dirSize = binary.LittleEndian.Uint64(zip64[40:])
			dirOffset = binary.LittleEndian.Uint64(zip64[48:])
		} else if entryCount != 0xffff || dirSize == 0xffffffff || dirOffset == 0xffffffff {
			return nil, zip.ErrFormat
		}
	}
	if dirSize > uint64(1<<63-1) || dirOffset > uint64(1<<63-1) {
		return nil, zip.ErrFormat
	}
	baseOffset := endOffset - int64(dirSize) - int64(dirOffset)
	if baseOffset > 0 {
		var sig [4]byte
		if _, err := ra.ReadAt(sig[:], int64(dirOffset)); err == nil && binary.LittleEndian.Uint32(sig[:]) == zipCentralSig {
			baseOffset = 0
		}
	}
	start := baseOffset + int64(dirOffset)
	if start < 0 || start > size || dirSize > zipMaxCentralSize || dirSize > uint64(size-start) {
		return nil, zip.ErrFormat
	}
	dir := make([]byte, int(dirSize))
	if len(dir) > 0 {
		n, err := ra.ReadAt(dir, start)
		if err != nil || n != len(dir) {
			if err == nil {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	capacity := int(min(entryCount, 1<<20))
	members := make([]zipMember, 0, capacity)
	var count uint64
	for off := 0; off+46 <= len(dir); {
		if binary.LittleEndian.Uint32(dir[off:]) != zipCentralSig {
			return nil, zip.ErrFormat
		}
		method := binary.LittleEndian.Uint16(dir[off+10:])
		dosTime := binary.LittleEndian.Uint16(dir[off+12:])
		dosDate := binary.LittleEndian.Uint16(dir[off+14:])
		crc := binary.LittleEndian.Uint32(dir[off+16:])
		comp32 := binary.LittleEndian.Uint32(dir[off+20:])
		uncomp32 := binary.LittleEndian.Uint32(dir[off+24:])
		nameLen := int(binary.LittleEndian.Uint16(dir[off+28:]))
		extraLen := int(binary.LittleEndian.Uint16(dir[off+30:]))
		commentLen := int(binary.LittleEndian.Uint16(dir[off+32:]))
		off32 := binary.LittleEndian.Uint32(dir[off+42:])
		end := off + 46 + nameLen + extraLen + commentLen
		if end < off || end > len(dir) {
			return nil, zip.ErrFormat
		}
		name := string(dir[off+46 : off+46+nameLen])
		extra := dir[off+46+nameLen : off+46+nameLen+extraLen]
		headerOffset := int64(off32)
		compressedSize := int64(comp32)
		uncompressedSize := int64(uncomp32)
		if uncomp32 == 0xffffffff || comp32 == 0xffffffff || off32 == 0xffffffff {
			uncompressed, compressed, local, ok := zip64CentralValues(extra,
				uncomp32 == 0xffffffff, comp32 == 0xffffffff, off32 == 0xffffffff)
			if !ok {
				return nil, zip.ErrFormat
			}
			if uncomp32 == 0xffffffff {
				uncompressedSize = int64(uncompressed)
			}
			if comp32 == 0xffffffff {
				compressedSize = int64(compressed)
			}
			if off32 == 0xffffffff {
				headerOffset = int64(local)
			}
		}
		off = end
		count++
		if uncompressedSize < 0 || compressedSize < 0 || headerOffset < 0 ||
			headerOffset >= size || uncompressedSize > 1<<63-1 || compressedSize > 1<<63-1 {
			continue
		}
		headerOffset += baseOffset
		if headerOffset < 0 || headerOffset >= size {
			continue
		}
		clean, ok := cleanZipMemberName(name)
		if !ok {
			continue
		}
		members = append(members, zipMember{
			name:             clean,
			headerOffset:     headerOffset,
			dataOffset:       -1,
			compressedSize:   compressedSize,
			uncompressedSize: uncompressedSize,
			crc32:            crc,
			method:           method,
			modified:         zipModifiedTime(dosDate, dosTime, extra),
		})
	}
	if entryCount != 0xffff && count != entryCount {
		return nil, zip.ErrFormat
	}
	// Stable sort keeps the first entry for a duplicate name, matching the
	// archive order that archive/zip exposes.
	slices.SortStableFunc(members, func(a, b zipMember) int { return strings.Compare(a.name, b.name) })
	out := members[:0]
	for i := range members {
		if len(out) > 0 && out[len(out)-1].name == members[i].name {
			continue
		}
		out = append(out, members[i])
	}
	return out, nil
}

func zip64CentralValues(extra []byte, needUncompressed, needCompressed, needOffset bool) (uint64, uint64, uint64, bool) {
	for len(extra) >= 4 {
		id := binary.LittleEndian.Uint16(extra)
		n := int(binary.LittleEndian.Uint16(extra[2:]))
		extra = extra[4:]
		if n > len(extra) {
			return 0, 0, 0, false
		}
		field := extra[:n]
		extra = extra[n:]
		if id != zip64ExtraID {
			continue
		}
		pos := 0
		read := func() (uint64, bool) {
			if pos+8 > len(field) {
				return 0, false
			}
			value := binary.LittleEndian.Uint64(field[pos:])
			pos += 8
			return value, true
		}
		var uncompressed, compressed, offset uint64
		var ok bool
		if needUncompressed {
			if uncompressed, ok = read(); !ok {
				return 0, 0, 0, false
			}
		}
		if needCompressed {
			if compressed, ok = read(); !ok {
				return 0, 0, 0, false
			}
		}
		if needOffset {
			if offset, ok = read(); !ok {
				return 0, 0, 0, false
			}
		}
		return uncompressed, compressed, offset, true
	}
	return 0, 0, 0, !needUncompressed && !needCompressed && !needOffset
}

func zipModifiedTime(dosDate, dosTime uint16, extra []byte) time.Time {
	modified := zipDOSTime(dosDate, dosTime)
	for len(extra) >= 4 {
		id := binary.LittleEndian.Uint16(extra)
		n := int(binary.LittleEndian.Uint16(extra[2:]))
		extra = extra[4:]
		if n > len(extra) {
			return modified
		}
		field := extra[:n]
		extra = extra[n:]
		switch id {
		case zipExtTimeExtraID:
			if len(field) >= 5 && field[0]&1 != 0 {
				modified = time.Unix(int64(binary.LittleEndian.Uint32(field[1:])), 0).UTC()
			}
		case zipNTFSExtraID:
			if len(field) >= 32 && binary.LittleEndian.Uint16(field[4:]) == 1 && binary.LittleEndian.Uint16(field[6:]) >= 24 {
				ticks := int64(binary.LittleEndian.Uint64(field[8:]))
				modified = time.Unix(ticks/10_000_000-11_644_473_600, 0).UTC()
			}
		}
	}
	return modified
}

func zipDOSTime(dosDate, dosTime uint16) time.Time {
	if dosDate == 0 {
		return time.Time{}
	}
	return time.Date(
		int(dosDate>>9)+1980,
		time.Month(dosDate>>5&0xf),
		int(dosDate&0x1f),
		int(dosTime>>11),
		int(dosTime>>5&0x3f),
		int(dosTime&0x1f)*2,
		0,
		time.UTC,
	)
}

func readZipEnd(ra io.ReaderAt, size int64) (int64, []byte, error) {
	n := min(size, 65<<10)
	buf := make([]byte, n)
	if nread, err := ra.ReadAt(buf, size-n); err != nil || int64(nread) != n {
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, io.ErrUnexpectedEOF
	}
	if len(buf) < 22 {
		return 0, nil, zip.ErrFormat
	}
	for i := len(buf) - 22; i >= 0; i-- {
		if binary.LittleEndian.Uint32(buf[i:]) != zipEndSignature {
			continue
		}
		commentLen := int(binary.LittleEndian.Uint16(buf[i+20:]))
		if i+22+commentLen > len(buf) {
			continue
		}
		return size - n + int64(i), buf[i : i+22], nil
	}
	return 0, nil, zip.ErrFormat
}

func findZip64End(ra io.ReaderAt, eocdOffset int64) (int64, []byte, bool, error) {
	if eocdOffset < 20 {
		return 0, nil, false, zip.ErrFormat
	}
	var locator [20]byte
	if _, err := ra.ReadAt(locator[:], eocdOffset-20); err != nil {
		return 0, nil, false, err
	}
	if binary.LittleEndian.Uint32(locator[:]) != zip64LocSignature {
		return 0, nil, false, nil
	}
	if binary.LittleEndian.Uint32(locator[4:]) != 0 || binary.LittleEndian.Uint32(locator[16:]) != 1 {
		return 0, nil, true, zip.ErrFormat
	}
	offset := int64(binary.LittleEndian.Uint64(locator[8:]))
	if offset < 0 {
		return 0, nil, true, zip.ErrFormat
	}
	var record [56]byte
	if _, err := ra.ReadAt(record[:], offset); err != nil {
		return 0, nil, true, err
	}
	if binary.LittleEndian.Uint32(record[:]) != zip64EndSignature {
		return 0, nil, true, zip.ErrFormat
	}
	return offset, record[:], true, nil
}

func (s *Server) resolveZipMember(ctx context.Context, backend readBackend, bucket, key string) (*zipIndex, *zipMember, *store.Meta, bool) {
	archiveKey, memberPath, _, ok := splitArchiveKey(key)
	if !ok {
		return nil, nil, nil, false
	}
	idx, archiveMeta, err := s.loadZipIndex(ctx, backend, bucket, archiveKey)
	if err != nil {
		if s.log != nil {
			s.log.Debug("zip index unavailable", "bucket", bucket, "archive", archiveKey, "err", err)
		}
		return nil, nil, nil, false
	}
	member, ok := zipMemberForPath(idx, memberPath)
	if !ok {
		return nil, nil, nil, false
	}
	return idx, member, archiveMeta, true
}

// resolveZipMemberWithMeta resolves a member using archive metadata that the
// caller already fetched, so the request pays one metadata query in total.
func (s *Server) resolveZipMemberWithMeta(ctx context.Context, backend readBackend, bucket, key string, archiveMeta *store.Meta) (*zipIndex, *zipMember, bool) {
	archiveKey, memberPath, _, ok := splitArchiveKey(key)
	if !ok {
		return nil, nil, false
	}
	idx, err := s.loadZipIndexWithMeta(ctx, backend, bucket, archiveKey, archiveMeta)
	if err != nil {
		if s.log != nil {
			s.log.Debug("zip index unavailable", "bucket", bucket, "archive", archiveKey, "err", err)
		}
		return nil, nil, false
	}
	member, ok := zipMemberForPath(idx, memberPath)
	if !ok {
		return nil, nil, false
	}
	return idx, member, true
}

func zipMemberForPath(idx *zipIndex, memberPath string) (*zipMember, bool) {
	member, ok := idx.member(memberPath)
	if !ok && (memberPath == "" || strings.HasSuffix(memberPath, "/")) {
		member, ok = idx.member(memberPath + "index.html")
	}
	return member, ok
}

func zipListSource(base listSource, idx *zipIndex, archiveKey, keyPrefix, delimiter string) listSource {
	return func(after string, limit int) ([]store.Listed, error) {
		rows, err := base(after, limit)
		if err != nil {
			return nil, err
		}
		return mergeListed(rows, idx.listing(archiveKey, keyPrefix, delimiter, after, limit), limit), nil
	}
}

func mergeListed(real, virtual []store.Listed, limit int) []store.Listed {
	if limit <= 0 {
		return nil
	}
	out := make([]store.Listed, 0, min(limit, len(real)+len(virtual)))
	i, j := 0, 0
	for len(out) < limit && (i < len(real) || j < len(virtual)) {
		switch {
		case j >= len(virtual), i < len(real) && real[i].Key < virtual[j].Key:
			out = append(out, real[i])
			i++
		case i >= len(real), virtual[j].Key < real[i].Key:
			out = append(out, virtual[j])
			j++
		default:
			// A literal object always shadows an archive member.
			out = append(out, real[i])
			i++
			j++
		}
	}
	return out
}

func (s *Server) headZipMember(w http.ResponseWriter, r *http.Request, idx *zipIndex, member *zipMember) error {
	etag := idx.memberETag(member)
	if err := readCondition(r, etag); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(etag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}
	setObjectInfoHeaders(w, idx.objectInfo(member), member.name)
	w.Header().Set("Content-Length", strconv.FormatInt(member.uncompressedSize, 10))
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) serveZipMember(w http.ResponseWriter, r *http.Request, backend readBackend, bucket, key string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, rng store.ByteRange) error {
	etag := idx.memberETag(member)
	if err := readCondition(r, etag); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(etag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}
	span, err := rng.Resolve(member.uncompressedSize)
	if err != nil || (rng.Requested && (member.uncompressedSize == 0 || span.End < span.Start)) {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(member.uncompressedSize, 10))
		return errInvalidRange()
	}
	body, err := s.openZipMemberBody(r.Context(), backend, bucket, idx, member, archiveMeta, span, rng.Requested)
	if err != nil {
		if errors.Is(err, store.ErrInvalidRange) {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(member.uncompressedSize, 10))
			return errInvalidRange()
		}
		return err
	}
	defer body.Close()
	res := &store.ReadResult{
		ObjectInfo: idx.objectInfo(member),
		Body:       body,
		Start:      span.Start,
		End:        span.End,
		Ranged:     rng.Requested,
	}
	s.serveReadResult(w, r, key, res, false)
	return nil
}

func (s *Server) openZipMemberBody(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span, ranged bool) (io.ReadCloser, error) {
	if member.uncompressedSize == 0 {
		return http.NoBody, nil
	}
	if member.method == archiveMethodSeekZstd {
		return s.openTarSeekMember(ctx, backend, bucket, idx, member, archiveMeta, span)
	}
	if idx.tarDataKey != "" {
		return s.openTarDataMember(ctx, backend, bucket, idx, member, archiveMeta, span)
	}
	switch member.method {
	case zip.Store:
		return s.openZipStoredMember(ctx, backend, bucket, idx, member, archiveMeta, span)
	case zip.Deflate, zstd.ZipMethodWinZip:
		return s.openZipCompressedMember(ctx, backend, bucket, idx, member, archiveMeta, span, ranged)
	default:
		return nil, errNotImplemented()
	}
}

func (s *Server) openZipStoredMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span) (io.ReadCloser, error) {
	return s.openZipMemberData(ctx, backend, bucket, idx, member, archiveMeta, span.Start, span.End-span.Start+1)
}

// openZipMemberData returns length bytes of a member starting at
// offsetInMember. The first access probes the local header to learn where the
// member data begins; later accesses cut straight to the range.
func (s *Server) openZipMemberData(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, offsetInMember, length int64) (io.ReadCloser, error) {
	if dataOff := atomic.LoadInt64(&member.dataOffset); dataOff >= 0 {
		return s.openZipRangeBody(ctx, backend, bucket, idx.archiveKey, dataOff+offsetInMember, length, archiveMeta)
	}
	if length <= 0 {
		return http.NoBody, nil
	}
	end := zipRangeEnd(member.headerOffset, int64(zipLocalHeaderLen)+int64(len(member.name))+256, offsetInMember+length-1, idx.archiveSize)
	body, err := openZipArchiveRange(ctx, backend, bucket, idx.archiveKey, member.headerOffset, end, archiveMeta)
	if err != nil {
		return nil, err
	}
	header := make([]byte, zipLocalHeaderLen)
	if _, err := io.ReadFull(body, header); err != nil {
		body.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(header) != zipLocalSignature {
		body.Close()
		return nil, zip.ErrFormat
	}
	nameLen := int64(binary.LittleEndian.Uint16(header[26:]))
	extraLen := int64(binary.LittleEndian.Uint16(header[28:]))
	actual := member.headerOffset + int64(zipLocalHeaderLen) + nameLen + extraLen
	if actual < 0 || actual > idx.archiveSize {
		body.Close()
		return nil, zip.ErrFormat
	}
	atomic.StoreInt64(&member.dataOffset, actual)
	if zipAddClamp(actual, offsetInMember+length-1, idx.archiveSize-1) <= end {
		skip := nameLen + extraLen + offsetInMember
		if skip > 0 {
			if _, err := io.CopyN(io.Discard, body, skip); err != nil {
				body.Close()
				return nil, err
			}
		}
		return &zipLimitedBody{r: io.LimitReader(body, length), c: body}, nil
	}
	body.Close()
	return s.openZipRangeBody(ctx, backend, bucket, idx.archiveKey, actual+offsetInMember, length, archiveMeta)
}

func (s *Server) openZipCompressedMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span, ranged bool) (io.ReadCloser, error) {
	if member.compressedSize <= 0 {
		if member.uncompressedSize == 0 {
			return http.NoBody, nil
		}
		return nil, zip.ErrFormat
	}
	if member.uncompressedSize > 0 && member.uncompressedSize <= zipMaxDecompressedMember {
		key := zipDecompressedKey(bucket, idx, member)
		if data, ok := s.decompressed(key); ok {
			return io.NopCloser(bytes.NewReader(data[span.Start : span.End+1])), nil
		}
		data, err := s.readZipCompressedMember(ctx, backend, bucket, idx, member, archiveMeta)
		if err != nil {
			return nil, err
		}
		s.cacheDecompressed(key, data)
		return io.NopCloser(bytes.NewReader(data[span.Start : span.End+1])), nil
	}
	if ranged && member.method == zstd.ZipMethodWinZip && member.uncompressedSize > zipMaxDecompressedMember {
		body, ok, err := s.openZipSeekableZstdMember(ctx, backend, bucket, idx, member, archiveMeta, span)
		if err != nil {
			return nil, err
		}
		if ok {
			return body, nil
		}
	}
	out, err := s.openZipCompressedStream(ctx, backend, bucket, idx, member, archiveMeta)
	if err != nil {
		return nil, err
	}
	if !ranged {
		out.reader = &zipCRCReader{
			r:        out.decomp,
			wantCRC:  member.crc32,
			wantSize: member.uncompressedSize,
			hash:     crc32.NewIEEE(),
		}
	} else {
		if span.Start > 0 {
			if _, err := io.CopyN(io.Discard, out.decomp, span.Start); err != nil {
				out.Close()
				return nil, err
			}
		}
		out.reader = io.LimitReader(out.decomp, span.End-span.Start+1)
	}
	return out, nil
}

func (s *Server) openZipSeekableZstdMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta, span store.Span) (io.ReadCloser, bool, error) {
	table, err := s.zipMemberSeekTable(ctx, backend, bucket, idx, member, archiveMeta)
	if err != nil || table == nil || len(table.frames) == 0 {
		return nil, false, err
	}
	openRange := func(start, length int64) (io.ReadCloser, error) {
		return s.openZipMemberData(ctx, backend, bucket, idx, member, archiveMeta, start, length)
	}
	body, err := openSeekableZstdRange(ctx, openRange, table, span.Start, span.End)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func (s *Server) zipMemberSeekTable(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta) (*zstdSeekTable, error) {
	if cached := member.seekz.table.Load(); cached != nil {
		if cached == noZstdSeek || len(cached.frames) == 0 {
			return nil, nil
		}
		return cached, nil
	}
	readTail := func(length int64) ([]byte, error) {
		body, err := s.openZipMemberData(ctx, backend, bucket, idx, member, archiveMeta, member.compressedSize-length, length)
		if err != nil {
			return nil, err
		}
		defer body.Close()
		data, err := io.ReadAll(io.LimitReader(body, length))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != length {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	}
	n := min(int64(zstdSeekTailSize), member.compressedSize)
	tail, err := readTail(n)
	if err != nil {
		return nil, err
	}
	table, required, ok := parseZstdSeekTable(tail, member.compressedSize)
	if !ok && required > int64(len(tail)) && required <= member.compressedSize {
		tail, err = readTail(required)
		if err != nil {
			return nil, err
		}
		table, _, ok = parseZstdSeekTable(tail, member.compressedSize)
	}
	if !ok {
		member.seekz.table.Store(noZstdSeek)
		return nil, nil
	}
	member.seekz.table.Store(table)
	return table, nil
}

func (s *Server) readZipCompressedMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta) ([]byte, error) {
	out, err := s.openZipCompressedStream(ctx, backend, bucket, idx, member, archiveMeta)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	data, err := io.ReadAll(io.LimitReader(out, member.uncompressedSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != member.uncompressedSize {
		return nil, zip.ErrChecksum
	}
	if crc32.ChecksumIEEE(data) != member.crc32 {
		return nil, zip.ErrChecksum
	}
	return data, nil
}

func (s *Server) openZipCompressedStream(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, archiveMeta *store.Meta) (*zipDecompressBody, error) {
	body, err := s.openZipMemberData(ctx, backend, bucket, idx, member, archiveMeta, 0, member.compressedSize)
	if err != nil {
		return nil, err
	}
	compressed := io.LimitReader(body, member.compressedSize)
	var decomp io.ReadCloser
	switch member.method {
	case zip.Deflate:
		fr := zipFlatePool.Get().(zipFlateReadCloser)
		if err := fr.Reset(compressed, nil); err != nil {
			body.Close()
			zipFlatePool.Put(fr)
			return nil, err
		}
		decomp = &zipPooledFlate{reader: fr}
	case zstd.ZipMethodWinZip:
		decomp = zipZstdDecompressor(compressed)
	default:
		body.Close()
		return nil, errNotImplemented()
	}
	return &zipDecompressBody{reader: decomp, decomp: decomp, body: body}, nil
}

type zipPooledFlate struct {
	reader zipFlateReadCloser
	closed bool
}

func (z *zipPooledFlate) Read(p []byte) (int, error) { return z.reader.Read(p) }

func (z *zipPooledFlate) Close() error {
	if z.closed {
		return nil
	}
	z.closed = true
	err := z.reader.Close()
	zipFlatePool.Put(z.reader)
	return err
}

type zipDecompressBody struct {
	reader io.Reader
	decomp io.ReadCloser
	body   io.ReadCloser
}

func (z *zipDecompressBody) Read(p []byte) (int, error) { return z.reader.Read(p) }

func (z *zipDecompressBody) Close() error {
	err := z.decomp.Close()
	if bodyErr := z.body.Close(); err == nil {
		err = bodyErr
	}
	return err
}

type zipLimitedBody struct {
	r io.Reader
	c io.ReadCloser
}

func (z *zipLimitedBody) Read(p []byte) (int, error) { return z.r.Read(p) }
func (z *zipLimitedBody) Close() error               { return z.c.Close() }

type zipCRCReader struct {
	r        io.Reader
	wantCRC  uint32
	wantSize int64
	hash     hash.Hash32
	n        int64
	done     bool
}

func (z *zipCRCReader) Read(p []byte) (int, error) {
	n, err := z.r.Read(p)
	if n > 0 {
		z.n += int64(n)
		_, _ = z.hash.Write(p[:n])
	}
	if errors.Is(err, io.EOF) && !z.done {
		z.done = true
		if z.n != z.wantSize || z.hash.Sum32() != z.wantCRC {
			return n, zip.ErrChecksum
		}
	}
	return n, err
}

func (s *Server) openZipRangeBody(ctx context.Context, backend readBackend, bucket, key string, start, length int64, archiveMeta *store.Meta) (io.ReadCloser, error) {
	if length <= 0 {
		return http.NoBody, nil
	}
	end := start + length - 1
	if end < start {
		return nil, zip.ErrFormat
	}
	return openZipArchiveRange(ctx, backend, bucket, key, start, end, archiveMeta)
}

func openZipArchiveRange(ctx context.Context, backend readBackend, bucket, key string, start, end int64, archiveMeta *store.Meta) (io.ReadCloser, error) {
	if archiveMeta != nil {
		if lb, isLocal := backend.(localBackend); isLocal && lb.store != nil {
			_, body, err := lb.store.GetBodyFromMeta(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, archiveMeta, store.Range(start, end, -1))
			if err != nil {
				return nil, err
			}
			return body, nil
		}
	}
	res, err := openObject(ctx, backend, bucket, key, store.Range(start, end, -1), store.ReadCondition{})
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

func zipAddClamp(base, extra, maxValue int64) int64 {
	if base < 0 || base > maxValue {
		return maxValue
	}
	if extra > 0 && extra > maxValue-base {
		return maxValue
	}
	return base + extra
}

func zipRangeEnd(base, fixed, extra, archiveSize int64) int64 {
	return zipAddClamp(zipAddClamp(base, fixed, archiveSize-1), extra, archiveSize-1)
}
