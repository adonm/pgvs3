package s3

import (
	"archive/zip"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	zipTailSize         = 256 << 10
	zipReadAhead        = 64 << 10
	zipMaxArchives      = 64
	zipMaxCachedMembers = 64000
	zipMaxManifestSize  = 16 << 20
	zipMaxCentralSize   = 64 << 20
	zipWindowSize       = 8 << 20
	zipLocalHeaderLen   = 30
	zipDataDescriptor   = 0x08074b50
	zipLocalSignature   = 0x04034b50
	zipCentralSig       = 0x02014b50
	zipEndSignature     = 0x06054b50
	zip64EndSignature   = 0x06064b50
	zip64LocSignature   = 0x07064b50
	zip64ExtraID        = 0x0001
)

var zipZstdDecompressor = zstd.ZipDecompressor()

type zipIndexCache struct {
	mu         sync.Mutex
	indexes    map[string]*zipIndex
	memberRefs int
	group      singleflight.Group
}

type zipIndex struct {
	archiveKey  string
	archiveETag string
	archiveSize int64
	createdAt   time.Time
	entries     []*zipMember
	byName      map[string]int
}

type zipMember struct {
	name             string
	headerOffset     int64
	dataOffset       atomic.Int64
	compressedSize   int64
	uncompressedSize int64
	crc32            uint32
	method           uint16
	modified         time.Time
}

func (idx *zipIndex) member(name string) (*zipMember, bool) {
	i, ok := idx.byName[name]
	if !ok {
		return nil, false
	}
	return idx.entries[i], true
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

func (idx *zipIndex) listing(archiveKey, keyPrefix, after string, limit int) []store.Listed {
	if limit <= 0 {
		return nil
	}
	full := func(i int) string { return archiveKey + "/" + idx.entries[i].name }
	start := sort.Search(len(idx.entries), func(i int) bool { return full(i) >= keyPrefix })
	out := make([]store.Listed, 0, min(limit, 32))
	for i := start; i < len(idx.entries) && len(out) < limit; i++ {
		key := full(i)
		if !strings.HasPrefix(key, keyPrefix) {
			break
		}
		if key <= after {
			continue
		}
		m := idx.entries[i]
		out = append(out, store.Listed{
			Key:       key,
			Size:      m.uncompressedSize,
			ETag:      idx.memberETag(m),
			CreatedAt: m.modified,
		})
	}
	return out
}

// splitZipKey turns "datasets/sample.zip/inner/quant.sf" into
// ("datasets/sample.zip", "inner/quant.sf"). The caller only uses it after a
// literal key lookup failed, so a real S3 object always shadows a member.
func splitZipKey(key string) (archiveKey, memberPath string, ok bool) {
	i := strings.Index(strings.ToLower(key), ".zip/")
	if i < 0 {
		return "", "", false
	}
	archiveKey = key[:i+len(".zip")]
	memberPath = key[i+len(".zip/"):]
	if !safeZipMemberPath(memberPath) {
		return "", "", false
	}
	return archiveKey, memberPath, true
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

func (s *Server) loadZipIndex(ctx context.Context, backend readBackend, bucket, archiveKey string) (*zipIndex, error) {
	head, err := backend.Head(ctx, bucket, archiveKey)
	if err != nil {
		return nil, err
	}
	if head.Info.Size <= 0 {
		return nil, zip.ErrFormat
	}
	cacheKey := bucket + "\x00" + archiveKey
	s.zip.mu.Lock()
	cached := s.zip.indexes[cacheKey]
	s.zip.mu.Unlock()
	if cached != nil && cached.archiveETag == head.Info.ETag && cached.archiveSize == head.Info.Size {
		return cached, nil
	}
	flightKey := cacheKey + "\x00" + head.Info.ETag
	value, err, _ := s.zip.group.Do(flightKey, func() (any, error) {
		idx, err := s.buildZipIndex(ctx, backend, bucket, archiveKey, head.Info)
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

func (s *Server) buildZipIndex(ctx context.Context, backend readBackend, bucket, archiveKey string, info store.ObjectInfo) (*zipIndex, error) {
	ra, err := newZipRangeReaderAt(ctx, backend, bucket, archiveKey, info.Size)
	if err != nil {
		return nil, err
	}
	// Read the directory offsets first so the central directory lands in the
	// range reader's window; archive/zip then parses it without extra GETs.
	offsets, err := readZipCentralOffsets(ra, info.Size)
	if err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(ra, info.Size)
	if err != nil && !(errors.Is(err, zip.ErrInsecurePath) && zr != nil) {
		return nil, err
	}
	if len(offsets) != len(zr.File) {
		return nil, zip.ErrFormat
	}
	manifest := readZipManifest(ctx, backend, bucket, archiveKey)
	idx := &zipIndex{
		archiveKey:  archiveKey,
		archiveETag: info.ETag,
		archiveSize: info.Size,
		createdAt:   time.Now(),
		byName:      make(map[string]int, len(zr.File)),
	}
	seen := make(map[string]struct{}, len(zr.File))
	for i, f := range zr.File {
		name, ok := cleanZipMemberName(f.Name)
		if !ok {
			continue
		}
		if _, raw := seen[name]; raw {
			continue
		}
		seen[name] = struct{}{}
		if f.CompressedSize64 > uint64(1<<63-1) || f.UncompressedSize64 > uint64(1<<63-1) {
			continue
		}
		headerOffset := offsets[i]
		if headerOffset < 0 || headerOffset >= info.Size {
			continue
		}
		member := &zipMember{
			name:             name,
			headerOffset:     headerOffset,
			compressedSize:   int64(f.CompressedSize64),
			uncompressedSize: int64(f.UncompressedSize64),
			crc32:            f.CRC32,
			method:           f.Method,
			modified:         f.Modified,
		}
		member.dataOffset.Store(-1)
		if entry, ok := manifest[name]; ok {
			if entry.dataOffset >= 0 && entry.dataOffset < info.Size &&
				(entry.compressedSize < 0 || entry.compressedSize == member.compressedSize) &&
				(!entry.hasMethod || entry.method < 0 || entry.method == int64(member.method)) {
				member.dataOffset.Store(entry.dataOffset)
			}
		}
		idx.entries = append(idx.entries, member)
	}
	sort.Slice(idx.entries, func(i, j int) bool { return idx.entries[i].name < idx.entries[j].name })
	for i := range idx.entries {
		idx.byName[idx.entries[i].name] = i
	}
	return idx, nil
}

type zipRangeReaderAt struct {
	ctx         context.Context
	backend     readBackend
	bucket      string
	key         string
	size        int64
	tailOffset  int64
	tail        []byte
	mu          sync.Mutex
	windowStart int64
	window      []byte
}

func newZipRangeReaderAt(ctx context.Context, backend readBackend, bucket, key string, size int64) (*zipRangeReaderAt, error) {
	if size <= 0 {
		return nil, zip.ErrFormat
	}
	ra := &zipRangeReaderAt{ctx: ctx, backend: backend, bucket: bucket, key: key, size: size}
	n := min(size, zipTailSize)
	ra.tailOffset = size - n
	res, err := openObject(ctx, backend, bucket, key, store.Range(ra.tailOffset, size-1, -1), store.ReadCondition{})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, n))
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
	if n, ok := r.readWindow(p[:want], off); ok {
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	end := min(off+max(int64(want), zipReadAhead), r.size) - 1
	res, err := openObject(r.ctx, r.backend, r.bucket, r.key, store.Range(off, end, -1), store.ReadCondition{})
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	length := end - off + 1
	if length <= zipWindowSize {
		data, err := io.ReadAll(io.LimitReader(res.Body, length))
		if err != nil {
			return 0, err
		}
		if int64(len(data)) != length {
			return 0, io.ErrUnexpectedEOF
		}
		r.mu.Lock()
		r.windowStart, r.window = off, data
		r.mu.Unlock()
		n := copy(p[:want], data)
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	n, err := io.ReadFull(res.Body, p[:want])
	if err != nil {
		return n, err
	}
	if want < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *zipRangeReaderAt) readWindow(p []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.window) == 0 || off < r.windowStart || off+int64(len(p)) > r.windowStart+int64(len(r.window)) {
		return 0, false
	}
	return copy(p, r.window[off-r.windowStart:]), true
}

// readZipCentralOffsets extracts each central-directory entry's local header
// offset. archive/zip does not expose headerOffset, and the local header must
// be located without an extra request to cut a member through in one GET.
func readZipCentralOffsets(ra io.ReaderAt, size int64) ([]int64, error) {
	eocdOffset, eocd, err := readZipEnd(ra, size)
	if err != nil {
		return nil, err
	}
	entries := uint64(binary.LittleEndian.Uint16(eocd[10:]))
	dirSize := uint64(binary.LittleEndian.Uint32(eocd[12:]))
	dirOffset := uint64(binary.LittleEndian.Uint32(eocd[16:]))
	endOffset := eocdOffset
	if entries == 0xffff || dirSize == 0xffffffff || dirOffset == 0xffffffff {
		zip64Offset, zip64, found, zerr := findZip64End(ra, eocdOffset)
		if zerr != nil {
			return nil, zerr
		}
		if found {
			endOffset = zip64Offset
			entries = binary.LittleEndian.Uint64(zip64[32:])
			dirSize = binary.LittleEndian.Uint64(zip64[40:])
			dirOffset = binary.LittleEndian.Uint64(zip64[48:])
		} else if entries != 0xffff || dirSize == 0xffffffff || dirOffset == 0xffffffff {
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
	if start < 0 || start > size || dirSize > zipMaxCentralSize {
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
	offsets := make([]int64, 0, int(min(entries, 1<<20)))
	for off := 0; off+46 <= len(dir); {
		if binary.LittleEndian.Uint32(dir[off:]) != zipCentralSig {
			return nil, zip.ErrFormat
		}
		nameLen := int(binary.LittleEndian.Uint16(dir[off+28:]))
		extraLen := int(binary.LittleEndian.Uint16(dir[off+30:]))
		commentLen := int(binary.LittleEndian.Uint16(dir[off+32:]))
		end := off + 46 + nameLen + extraLen + commentLen
		if end < off || end > len(dir) {
			return nil, zip.ErrFormat
		}
		localOffset := uint64(binary.LittleEndian.Uint32(dir[off+42:]))
		if localOffset == 0xffffffff {
			compressed := binary.LittleEndian.Uint32(dir[off+20:])
			uncompressed := binary.LittleEndian.Uint32(dir[off+24:])
			extra := dir[off+46+nameLen : off+46+nameLen+extraLen]
			value, ok := zip64LocalOffset(extra, uncompressed == 0xffffffff, compressed == 0xffffffff)
			if !ok {
				return nil, zip.ErrFormat
			}
			localOffset = value
		}
		offsets = append(offsets, int64(localOffset)+baseOffset)
		off = end
	}
	if uint64(len(offsets)) != entries && entries != 0xffff {
		return nil, zip.ErrFormat
	}
	return offsets, nil
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

func zip64LocalOffset(extra []byte, uncompressedSentinel, compressedSentinel bool) (uint64, bool) {
	for len(extra) >= 4 {
		id := binary.LittleEndian.Uint16(extra)
		n := int(binary.LittleEndian.Uint16(extra[2:]))
		extra = extra[4:]
		if n > len(extra) {
			return 0, false
		}
		field := extra[:n]
		extra = extra[n:]
		if id != zip64ExtraID {
			continue
		}
		pos := 0
		if uncompressedSentinel {
			pos += 8
		}
		if compressedSentinel {
			pos += 8
		}
		if pos+8 > len(field) {
			return 0, false
		}
		return binary.LittleEndian.Uint64(field[pos:]), true
	}
	return 0, false
}

type zipManifestEntry struct {
	dataOffset     int64
	compressedSize int64
	method         int64
	hasMethod      bool
}

func readZipManifest(ctx context.Context, backend readBackend, bucket, archiveKey string) map[string]zipManifestEntry {
	key := archiveKey + ".manifest.tsv"
	res, err := backend.Open(ctx, bucket, key, store.WholeObjectRange())
	if err != nil {
		return nil
	}
	defer res.Body.Close()
	if res.Size > zipMaxManifestSize {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(res.Body, zipMaxManifestSize+1))
	if err != nil || len(data) > zipMaxManifestSize {
		return nil
	}
	out := make(map[string]zipManifestEntry)
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) < 4 {
			continue
		}
		name, ok := cleanZipMemberName(fields[0])
		if !ok {
			continue
		}
		offset, err := strconv.ParseInt(strings.TrimSpace(fields[3]), 10, 64)
		if err != nil || offset < 0 {
			continue
		}
		entry := zipManifestEntry{dataOffset: offset, compressedSize: -1, method: -1}
		if len(fields) > 4 {
			if size, err := strconv.ParseInt(strings.TrimSpace(fields[4]), 10, 64); err == nil && size >= 0 {
				entry.compressedSize = size
			}
		}
		if len(fields) > 5 {
			entry.method, entry.hasMethod = zipManifestMethod(fields[5])
		}
		out[name] = entry
	}
	return out
}

func zipManifestMethod(value string) (int64, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return -1, false
	case "stored", "store", "0":
		return 0, true
	case "deflate", "deflated", "8":
		return 8, true
	case "zstd", "zstandard", "93":
		return zstd.ZipMethodWinZip, true
	default:
		return -1, false
	}
}

func (s *Server) resolveZipMember(ctx context.Context, backend readBackend, bucket, key string) (*zipIndex, *zipMember, bool) {
	archiveKey, memberPath, ok := splitZipKey(key)
	if !ok {
		return nil, nil, false
	}
	idx, err := s.loadZipIndex(ctx, backend, bucket, archiveKey)
	if err != nil {
		if s.log != nil {
			s.log.Debug("zip index unavailable", "bucket", bucket, "archive", archiveKey, "err", err)
		}
		return nil, nil, false
	}
	member, ok := idx.member(memberPath)
	if !ok && (memberPath == "" || strings.HasSuffix(memberPath, "/")) {
		member, ok = idx.member(memberPath + "index.html")
	}
	if !ok {
		return nil, nil, false
	}
	return idx, member, true
}

func zipListSource(base listSource, idx *zipIndex, archiveKey, keyPrefix string) listSource {
	return func(after string, limit int) ([]store.Listed, error) {
		rows, err := base(after, limit)
		if err != nil {
			return nil, err
		}
		return mergeListed(rows, idx.listing(archiveKey, keyPrefix, after, limit), limit), nil
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

func (s *Server) serveZipMember(w http.ResponseWriter, r *http.Request, backend readBackend, bucket, key string, idx *zipIndex, member *zipMember, rng store.ByteRange) error {
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
	body, err := openZipMemberBody(r.Context(), backend, bucket, idx, member, span, rng.Requested)
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

func openZipMemberBody(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, span store.Span, ranged bool) (io.ReadCloser, error) {
	if member.uncompressedSize == 0 {
		return http.NoBody, nil
	}
	switch member.method {
	case zip.Store:
		return openZipStoredMember(ctx, backend, bucket, idx, member, span)
	case zip.Deflate, zstd.ZipMethodWinZip:
		return openZipCompressedMember(ctx, backend, bucket, idx, member, span, ranged)
	default:
		return nil, errNotImplemented()
	}
}

func openZipStoredMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, span store.Span) (io.ReadCloser, error) {
	length := span.End - span.Start + 1
	dataOff := member.dataOffset.Load()
	if dataOff >= 0 {
		return openZipRangeBody(ctx, backend, bucket, idx.archiveKey, dataOff+span.Start, length)
	}
	end := zipRangeEnd(member.headerOffset, int64(zipLocalHeaderLen)+int64(len(member.name))+256, span.End, idx.archiveSize)
	res, err := openObject(ctx, backend, bucket, idx.archiveKey, store.Range(member.headerOffset, end, -1), store.ReadCondition{})
	if err != nil {
		return nil, err
	}
	body := res.Body
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
	member.dataOffset.Store(actual)
	if zipAddClamp(actual, span.End, idx.archiveSize-1) <= end {
		skip := nameLen + extraLen + span.Start
		if skip > 0 {
			if _, err := io.CopyN(io.Discard, body, skip); err != nil {
				body.Close()
				return nil, err
			}
		}
		return &zipLimitedBody{r: io.LimitReader(body, length), c: body}, nil
	}
	body.Close()
	return openZipRangeBody(ctx, backend, bucket, idx.archiveKey, actual+span.Start, length)
}

func openZipCompressedMember(ctx context.Context, backend readBackend, bucket string, idx *zipIndex, member *zipMember, span store.Span, ranged bool) (io.ReadCloser, error) {
	if member.compressedSize <= 0 {
		if member.uncompressedSize == 0 {
			return http.NoBody, nil
		}
		return nil, zip.ErrFormat
	}
	var (
		body       io.ReadCloser
		compressed io.Reader
		err        error
	)
	dataOff := member.dataOffset.Load()
	if dataOff >= 0 {
		body, err = openZipRangeBody(ctx, backend, bucket, idx.archiveKey, dataOff, member.compressedSize)
		if err != nil {
			return nil, err
		}
		compressed = io.LimitReader(body, member.compressedSize)
	} else {
		end := zipRangeEnd(member.headerOffset, int64(zipLocalHeaderLen)+int64(len(member.name))+256, member.compressedSize-1, idx.archiveSize)
		res, openErr := openObject(ctx, backend, bucket, idx.archiveKey, store.Range(member.headerOffset, end, -1), store.ReadCondition{})
		if openErr != nil {
			return nil, openErr
		}
		body = res.Body
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
		member.dataOffset.Store(actual)
		if zipAddClamp(actual, member.compressedSize-1, idx.archiveSize-1) <= end {
			skip := nameLen + extraLen
			if skip > 0 {
				if _, err := io.CopyN(io.Discard, body, skip); err != nil {
					body.Close()
					return nil, err
				}
			}
			compressed = io.LimitReader(body, member.compressedSize)
		} else {
			body.Close()
			body, err = openZipRangeBody(ctx, backend, bucket, idx.archiveKey, actual, member.compressedSize)
			if err != nil {
				return nil, err
			}
			compressed = io.LimitReader(body, member.compressedSize)
		}
	}
	var decomp io.ReadCloser
	switch member.method {
	case zip.Deflate:
		decomp = flate.NewReader(compressed)
	case zstd.ZipMethodWinZip:
		decomp = zipZstdDecompressor(compressed)
	default:
		body.Close()
		return nil, errNotImplemented()
	}
	if err != nil {
		body.Close()
		return nil, err
	}
	out := &zipDecompressBody{reader: decomp, decomp: decomp, body: body}
	if !ranged {
		out.reader = &zipCRCReader{
			r:        decomp,
			wantCRC:  member.crc32,
			wantSize: member.uncompressedSize,
			hash:     crc32.NewIEEE(),
		}
	} else {
		if span.Start > 0 {
			if _, err := io.CopyN(io.Discard, decomp, span.Start); err != nil {
				out.Close()
				return nil, err
			}
		}
		out.reader = io.LimitReader(decomp, span.End-span.Start+1)
	}
	return out, nil
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

func openZipRangeBody(ctx context.Context, backend readBackend, bucket, key string, start, length int64) (io.ReadCloser, error) {
	if length <= 0 {
		return http.NoBody, nil
	}
	end := start + length - 1
	if end < start {
		return nil, zip.ErrFormat
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
