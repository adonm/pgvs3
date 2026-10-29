package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const RowBytes = 8120

const MaxInlineBytes = 65536

func (s *Store) PutInline(ctx context.Context, ref ObjectRef, data []byte, opts WriteOptions) (*Meta, error) {
	opts, err := s.prepareWrite(ref, opts, true)
	if err != nil {
		return nil, err
	}
	if data == nil {
		data = []byte{}
	}
	if opts.ContentType == "" {
		opts.ContentType = "application/octet-stream"
	}
	if int64(len(data)) > int64(ref.NS.Kind.InlineMax()) {
		return nil, ErrInlineTooLarge
	}
	if s.checkQuota(ctx, ref.NS.Kind, ref.NS.Name, int64(len(data))) {
		return nil, ErrQuotaExceeded
	}
	sumMD5 := md5.Sum(data)
	sumSHA := sha256.Sum256(data)
	etag := hex.EncodeToString(sumMD5[:])
	stored := compressBlock(nil, data)
	condText, match := opts.Precondition.SQL()
	userMeta := opts.UserMetadata
	if userMeta == nil {
		userMeta = []string{}
	}
	var expected any
	if opts.Publication.expected != "" {
		expected = opts.Publication.expected
	}
	result, err := withAutoCreate(ctx, s, ref.NS.Kind, ref.NS.Name, func(sc string) (string, error) {
		var res string
		err := s.pool.QueryRow(ctx, "SELECT "+sc+".put_inline($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
			ref.Key, stored, int64(len(data)), sumSHA[:], etag, userMeta, opts.ContentType,
			condText, match, "on", opts.ExpiresAt, opts.KeepTTL, opts.Publication.origin, expected, opts.skipLock(),
			opts.s3MetaJSON()).Scan(&res)
		return res, err
	})
	if err != nil {
		return nil, err
	}
	switch result {
	case "ok":
	case "stale":
		return nil, ErrStaleCache
	case "nobucket":
		return nil, ErrNoBucket
	default:
		return nil, ErrPrecondition
	}
	m := Meta{
		Size: int64(len(data)), ETag: etag, UserMetadata: userMeta,
		ContentType: opts.ContentType, ContentEncoding: opts.ContentEncoding,
		CreatedAt: time.Now(), OriginETag: opts.Publication.origin,
		OriginModified: opts.OriginModified,
		SHA256:         sumSHA[:], ExpiresAt: opts.ExpiresAt, Inline: data,
	}
	return &m, nil
}

var ErrInlineTooLarge = errors.New("inline object exceeds the namespace limit")

var copyHeader = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")

var copyTrailer = []byte{0xFF, 0xFF}

const sendBatch = 4 << 20

var framerPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, sendBatch+RowBytes))
}}

func getFramerBuf() *bytes.Buffer {
	b := framerPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func putFramerBuf(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	framerPool.Put(b)
}

type Checksum struct {
	Name  string
	Value string
}

func FirstObjectChecksum(checks []Checksum) *Checksum {
	for i := range checks {
		if checks[i].Name != "content-md5" {
			return &checks[i]
		}
	}
	return nil
}

type Published struct {
	FileID   int64
	Size     int64
	ETag     string
	SHA256   []byte
	MD5      []byte
	Checksum *Checksum
}

var (
	ErrNoBucket           = errors.New("bucket does not exist")
	ErrInvalidBucket      = errors.New("invalid bucket name")
	ErrBucketNotEmpty     = errors.New("bucket is not empty")
	ErrNoUpload           = errors.New("upload does not exist")
	ErrPrecondition       = errors.New("object precondition failed")
	ErrStaleCache         = errors.New("cache object origin version changed")
	ErrMissingChecksum    = errors.New("part lacks the upload's checksum")
	ErrInvalidPart        = errors.New("invalid part")
	errAborted            = errors.New("ingest aborted")
	ErrInvalidPublication = errors.New("invalid publication intent or write lock")
)

type rowFramer struct {
	fileID  int64
	no      int32
	pending []byte
	scratch []byte
	buf     *bytes.Buffer
	sha     hash.Hash
	md5     hash.Hash
	size    int64
}

func newRowFramer(fileID int64) *rowFramer {
	f := &rowFramer{
		fileID:  fileID,
		scratch: make([]byte, RowBytes),
		buf:     getFramerBuf(),
		sha:     sha256.New(),
		md5:     md5.New(),
	}
	f.buf.Write(copyHeader)
	return f
}

func (f *rowFramer) push(src []byte) (*bytes.Buffer, error) {
	limit := (int64(math.MaxInt32) + 1) * RowBytes
	if int64(len(src)) > limit-f.size {
		return nil, fmt.Errorf("object part exceeds the maximum addressable row count")
	}
	_, _ = f.sha.Write(src)
	_, _ = f.md5.Write(src)
	f.size += int64(len(src))

	if len(f.pending) > 0 {
		need := RowBytes - len(f.pending)
		if need > len(src) {
			f.pending = append(f.pending, src...)
			return nil, nil
		}
		f.pending = append(f.pending, src[:need]...)
		f.frameRows(f.pending)
		f.pending = f.pending[:0]
		src = src[need:]
	}
	if whole := len(src) - len(src)%RowBytes; whole > 0 {
		f.frameRows(src[:whole])
		src = src[whole:]
	}
	f.pending = append(f.pending, src...)
	if f.buf.Len() < sendBatch {
		return nil, nil
	}
	out := f.buf
	f.buf = getFramerBuf()
	return out, nil
}

func (f *rowFramer) finish() *bytes.Buffer {
	if len(f.pending) > 0 {
		f.frameRows(f.pending)
		f.pending = nil
	}
	f.buf.Write(copyTrailer)
	out := f.buf
	f.buf = nil
	return out
}

func (f *rowFramer) release() {
	putFramerBuf(f.buf)
	f.buf = nil
}

func (f *rowFramer) frameRows(data []byte) {
	for start := 0; start < len(data); start += RowBytes {
		end := min(start+RowBytes, len(data))
		payload := compressBlock(f.scratch, data[start:end])

		f.buf.Write([]byte{0, 3})

		var b8 [8]byte
		binary.BigEndian.PutUint64(b8[:], uint64(f.fileID))
		f.buf.Write([]byte{0, 0, 0, 8})
		f.buf.Write(b8[:])

		var b4 [4]byte
		binary.BigEndian.PutUint32(b4[:], uint32(f.no))
		f.buf.Write([]byte{0, 0, 0, 4})
		f.buf.Write(b4[:])

		binary.BigEndian.PutUint32(b4[:], uint32(len(payload)))
		f.buf.Write(b4[:])
		f.buf.Write(payload)
		f.no++
	}
}

func (f *rowFramer) digests() (int64, []byte, []byte) {
	return f.size, f.sha.Sum(nil), f.md5.Sum(nil)
}

type writeKind uint8

const (
	writeObject writeKind = iota
	writePart
)

type writerTarget struct {
	kind       writeKind
	nsKind     ns.Kind
	bucket     string
	schema     string
	key        string
	opts       WriteOptions
	uploadID   string
	partNo     int32
	checksumAl string
}

type Condition int

const (
	Unconditional Condition = iota
	IfAbsent
	IfPresent
	IfMatch
)

type ChunkWriter struct {
	store     *Store
	algorithm string
	pw        *io.PipeWriter
	done      chan error
	framer    *rowFramer

	mu        sync.Mutex
	checksums []Checksum
	published Published
	closed    bool
}

func (s *Store) StartObject(ctx context.Context, ref ObjectRef, opts WriteOptions) (*ChunkWriter, error) {
	return s.startWriter(ctx, writerTarget{
		kind: writeObject, nsKind: ref.NS.Kind, bucket: ref.NS.Name, key: ref.Key, opts: opts,
	})
}

func (s *Store) StartPart(ctx context.Context, ref ObjectRef, uploadID string, partNo int32, expectedSize int64) (*ChunkWriter, error) {
	return s.startWriter(ctx, writerTarget{
		kind: writePart, nsKind: ref.NS.Kind, bucket: ref.NS.Name, key: ref.Key,
		uploadID: uploadID, partNo: partNo,
		opts: WriteOptions{ExpectedSize: expectedSize},
	})
}

func (s *Store) startWriter(ctx context.Context, t writerTarget) (*ChunkWriter, error) {
	var err error
	t.opts, err = s.prepareWrite(ObjectRef{NS: ns.Ref{Kind: t.nsKind, Name: t.bucket}, Key: t.key}, t.opts, false)
	if err != nil {
		return nil, err
	}
	if t.opts.UserMetadata == nil {
		t.opts.UserMetadata = []string{}
	}
	if t.opts.ContentType == "" {
		t.opts.ContentType = "application/octet-stream"
	}
	if t.nsKind == "" {
		t.nsKind = ns.Bucket
	}
	if s.checkQuota(ctx, t.nsKind, t.bucket, max(t.opts.ExpectedSize, 0)) {
		return nil, ErrQuotaExceeded
	}
	if t.kind == writeObject {
		return withAutoCreate(ctx, s, t.nsKind, t.bucket, func(sc string) (*ChunkWriter, error) {
			t.schema = sc
			return s.beginWriter(ctx, t)
		})
	}
	sc, err := nsSQLIdent(t.nsKind, t.bucket)
	if err != nil {
		return nil, err
	}
	t.schema = sc
	w, err := s.beginWriter(ctx, t)
	return w, namespaceResultError(ctx, s.pool, sc, err, ErrNoUpload)
}

func (s *Store) beginWriter(ctx context.Context, t writerTarget) (*ChunkWriter, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = 0"); err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, err
	}
	fail := func(err error) (*ChunkWriter, error) {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, err
	}
	if t.kind == writePart {
		var alg *string
		err = tx.QueryRow(ctx,
			"SELECT checksum_algorithm FROM "+t.schema+".uploads WHERE upload_id = $1 AND key = $2 FOR KEY SHARE",
			t.uploadID, t.key).Scan(&alg)
		if errors.Is(err, pgx.ErrNoRows) {
			return fail(ErrNoUpload)
		}
		if err != nil {
			return fail(err)
		}
		if alg != nil {
			t.checksumAl = *alg
		}
	}
	fileID, err := s.nextFileID(ctx)
	if err != nil {
		return fail(err)
	}

	pr, pw := io.Pipe()
	alg := ""
	if t.checksumAl != "" {
		alg = t.checksumAl
	}
	w := &ChunkWriter{
		store:     s,
		algorithm: alg,
		pw:        pw,
		done:      make(chan error, 1),
		framer:    newRowFramer(fileID),
	}
	go w.run(ctx, conn, tx, pr, t)
	return w, nil
}

func (w *ChunkWriter) run(ctx context.Context, conn *pgxpool.Conn, tx pgx.Tx, pr *io.PipeReader, t writerTarget) {
	defer conn.Release()
	fail := func(err error) {
		_ = tx.Rollback(ctx)
		w.done <- err
	}
	if err := w.copyRows(ctx, tx, pr, t); err != nil {
		fail(err)
		return
	}
	size, sha, md5 := w.framer.digests()
	checksum, err := w.verifyUpload(&t, size)
	if err != nil {
		fail(err)
		return
	}
	pub := Published{FileID: w.framer.fileID, Size: size, ETag: hex.EncodeToString(md5), SHA256: sha, MD5: md5, Checksum: checksum}
	if err := publishUpload(ctx, tx, t, pub); err != nil {
		fail(err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		w.done <- err
		return
	}
	w.mu.Lock()
	w.published = pub
	w.mu.Unlock()
	w.done <- nil
}

func (w *ChunkWriter) copyRows(ctx context.Context, tx pgx.Tx, pr *io.PipeReader, t writerTarget) error {
	copySQL := "COPY " + t.schema + ".chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)"
	if _, err := tx.Conn().PgConn().CopyFrom(ctx, pr, copySQL); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		if missingNamespace(ctx, tx.Conn(), t.schema, err) {
			w.store.forget(ns.Ref{Kind: t.nsKind, Name: t.bucket})
			if t.nsKind == ns.Bucket {
				err = ErrNoBucket
			}
		}
		_ = pr.CloseWithError(err)
		return err
	}
	return nil
}

func (w *ChunkWriter) verifyUpload(t *writerTarget, size int64) (*Checksum, error) {
	if want := t.opts.ExpectedSize; want > 0 && size != want {
		return nil, fmt.Errorf("uploaded %d bytes, expected %d", size, want)
	}
	w.mu.Lock()
	checksums := append([]Checksum(nil), w.checksums...)
	w.mu.Unlock()
	if t.kind == writeObject {
		if t.opts.Checksum == nil {
			if c := FirstObjectChecksum(checksums); c != nil {
				t.opts.Checksum = c
				if t.opts.ChecksumType == "" {
					t.opts.ChecksumType = "FULL_OBJECT"
				}
			}
		}
		return nil, nil
	}
	return lookupPartChecksum(t.checksumAl, checksums)
}

func lookupPartChecksum(algorithm string, checksums []Checksum) (*Checksum, error) {
	if algorithm == "" {
		return nil, nil
	}
	name := "x-amz-checksum-" + strings.ToLower(algorithm)
	for _, c := range checksums {
		if c.Name == name {
			return &c, nil
		}
	}
	return nil, ErrMissingChecksum
}

func publishUpload(ctx context.Context, tx pgx.Tx, t writerTarget, pub Published) error {
	var err error
	switch t.kind {
	case writeObject:
		err = publishObject(ctx, tx, t, pub)
	case writePart:
		err = commitPart(ctx, tx, t.schema, t.uploadID, t.partNo, pub.FileID, pub.Size, pub.SHA256, pub.MD5, pub.Checksum)
	}
	if err != nil {
		return err
	}
	return publishPendingOrigin(ctx, tx, t, pub.Size)
}

func publishObject(ctx context.Context, tx pgx.Tx, t writerTarget, pub Published) error {
	if err := t.opts.validateLock(ObjectRef{NS: ns.Ref{Kind: t.nsKind, Name: t.bucket}, Key: t.key}); err != nil {
		return err
	}
	condText, match := t.opts.Precondition.SQL()
	var expected any
	if t.opts.Publication.expected != "" {
		expected = t.opts.Publication.expected
	}
	var outcome string
	err := tx.QueryRow(ctx, "SELECT "+t.schema+".publish_object($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
		t.key, pub.FileID, pub.Size, pub.SHA256, pub.ETag, t.opts.UserMetadata, t.opts.ContentType, condText, match,
		t.opts.ExpiresAt, t.opts.KeepTTL, t.opts.Publication.origin, expected, t.opts.skipLock(),
		t.opts.s3MetaJSON()).Scan(&outcome)
	if err == nil {
		switch outcome {
		case "precondition":
			err = ErrPrecondition
		case "stale":
			err = ErrStaleCache
		}
	}
	return err
}

func publishPendingOrigin(ctx context.Context, tx pgx.Tx, t writerTarget, size int64) error {
	if !t.opts.Publication.IsPending() {
		return nil
	}
	po := t.opts.Publication.pending
	var base any
	if po.BaseETag != "" {
		base = po.BaseETag
	}
	_, err := tx.Exec(ctx, `INSERT INTO s3p.pending_uploads
			(kind, namespace, key, token, size, attempts, next_attempt, last_error, created_at,
			 base_etag, base_missing, lease_owner, lease_until)
			VALUES ('cache', $1, $2, $3, $4, 0, now(), NULL, now(), $5, $6, NULL, NULL)
			ON CONFLICT (namespace, key) DO UPDATE
			  SET token = EXCLUDED.token, size = EXCLUDED.size,
			      attempts = 0, dead = false, next_attempt = now(), last_error = NULL,
			      created_at = now(), base_etag = EXCLUDED.base_etag,
			      base_missing = EXCLUDED.base_missing,
			      lease_owner = NULL, lease_until = NULL`,
		t.bucket, t.key, po.Token, size, base, po.BaseMissing)
	return err
}

func (w *ChunkWriter) ChecksumAlgorithm() string { return w.algorithm }

func (w *ChunkWriter) Write(p []byte) (int, error) {
	out, err := w.framer.push(p)
	if err != nil {
		return 0, err
	}
	if out != nil {
		_, err := w.pw.Write(out.Bytes())
		putFramerBuf(out)
		if err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (w *ChunkWriter) Finish(checksums []Checksum) (Published, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return Published{}, errAborted
	}
	w.checksums = checksums
	w.mu.Unlock()

	out := w.framer.finish()
	if _, err := w.pw.Write(out.Bytes()); err != nil {
		putFramerBuf(out)
		w.abort()
		return Published{}, err
	}
	putFramerBuf(out)
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	_ = w.pw.Close()
	if err := <-w.done; err != nil {
		return Published{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.published, nil
}

func (w *ChunkWriter) Abort() { w.abort() }

func (w *ChunkWriter) abort() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	_ = w.pw.CloseWithError(errAborted)
	<-w.done
	w.framer.release()
}

type objectPublication struct {
	fileID      int64
	size        int64
	sha         []byte
	etag        string
	userMeta    []string
	contentType string
	parts       []int64
	ends        []int64
}

func swapObjectParts(ctx context.Context, tx pgx.Tx, schema, key string, opts *WriteOptions, pub objectPublication) error {
	if opts != nil && opts.lock != nil {
		lockedSchema, err := opts.lock.ref.Schema()
		if err != nil || lockedSchema != schema || opts.lock.ref.Key != key || opts.lock.released.Load() {
			return ErrInvalidPublication
		}
	}
	if opts == nil || !opts.skipLock() {
		if err := lockObjectKey(ctx, tx, schema, key); err != nil {
			return err
		}
	}
	var oldFile pgtype.Int8
	var oldParts []int64
	var oldETag string
	var oldExpires pgtype.Timestamptz
	err := tx.QueryRow(ctx,
		"SELECT file_id, parts, etag, expires_at FROM "+schema+".objects WHERE key = $1 FOR UPDATE",
		key).Scan(&oldFile, &oldParts, &oldETag, &oldExpires)
	old := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	live := old && (!oldExpires.Valid || oldExpires.Time.After(time.Now()))
	if opts != nil && !opts.Precondition.Allows(live, oldETag) {
		return ErrPrecondition
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".object_part_checksums WHERE key = $1", key); err != nil {
		return err
	}
	var exp, originModified, checksumName, checksumValue, checksumType any
	encoding := ""
	if opts != nil {
		encoding = opts.ContentEncoding
		if opts.ExpiresAt != nil {
			exp = *opts.ExpiresAt
		}
		if !opts.OriginModified.IsZero() {
			originModified = opts.OriginModified
		}
		if opts.Checksum != nil {
			checksumName = opts.Checksum.Name
			checksumValue = opts.Checksum.Value
			checksumType = opts.ChecksumType
		}
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO `+schema+`.objects (key, file_id, size, sha256, etag, user_metadata, content_type,
		   content_encoding, origin_modified, checksum_name, checksum_value, checksum_type,
		   expires_at, parts, part_ends, inline_data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NULL)
		 ON CONFLICT (key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		   sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag,
		   user_metadata = EXCLUDED.user_metadata, content_type = EXCLUDED.content_type,
		   content_encoding = EXCLUDED.content_encoding, origin_modified = EXCLUDED.origin_modified,
		   checksum_name = EXCLUDED.checksum_name, checksum_value = EXCLUDED.checksum_value,
		   checksum_type = EXCLUDED.checksum_type,
		   expires_at = EXCLUDED.expires_at, parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends,
		   inline_data = NULL, created_at = now()`,
		key, pub.fileID, pub.size, pub.sha, pub.etag, pub.userMeta, pub.contentType, encoding, originModified,
		checksumName, checksumValue, checksumType, exp, pub.parts, pub.ends)
	if err != nil {
		return err
	}
	if old && oldFile.Valid {
		return queueFiles(ctx, tx, schema, oldFile.Int64, oldParts)
	}
	return nil
}

func lockObjectKey(ctx context.Context, tx pgx.Tx, schema, key string) error {
	_, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended($1 || E'\x1f' || $2, 0))",
		schema, key)
	return err
}

func queueFiles(ctx context.Context, tx pgx.Tx, schema string, fileID int64, parts []int64) error {
	dead := make([]int64, 0, len(parts)+1)
	dead = append(dead, parts...)
	if fileID != 0 {
		dead = append(dead, fileID)
	}
	return queueFileIDs(ctx, tx, schema, dead)
}

func queueFileIDs(ctx context.Context, tx pgx.Tx, schema string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, "SELECT "+schema+".queue_garbage($1::int8[])", ids)
	return err
}

func commitPart(ctx context.Context, tx pgx.Tx, schema, uploadID string, partNo int32, fileID, size int64, sha, md5sum []byte, checksum *Checksum) error {
	var value *string
	if checksum != nil {
		value = &checksum.Value
	}
	_, err := tx.Exec(ctx, "SELECT "+schema+".commit_part($1,$2,$3,$4,$5,$6,$7)",
		uploadID, partNo, fileID, size, sha, md5sum, value)
	return err
}

type storedPart struct {
	no       int32
	fileID   int64
	size     int64
	sha      []byte
	md5      []byte
	checksum *string
}

type partChecksum struct {
	no     int32
	fileID int64
	value  string
}

type multipartPlan struct {
	fileIDs   []int64
	partEnds  []int64
	unused    []int64
	size      int64
	sha256    []byte
	etag      string
	checksums []partChecksum
}

func validateListedParts(listed []ListedPart) error {
	if len(listed) == 0 {
		return ErrInvalidPart
	}
	for i, p := range listed {
		if p.PartNo < 1 || p.PartNo > 10000 || (i > 0 && listed[i-1].PartNo >= p.PartNo) {
			return ErrInvalidPart
		}
	}
	return nil
}

func planMultipart(algorithm *string, listed []ListedPart, stored []storedPart) (multipartPlan, error) {
	var plan multipartPlan
	shaAll := sha256.New()
	md5All := md5.New()
	want := 0
	for _, p := range stored {
		if want >= len(listed) || listed[want].PartNo != p.no {
			plan.unused = append(plan.unused, p.fileID)
			continue
		}
		lp := listed[want]
		if lp.ETag != hex.EncodeToString(p.md5) || !partChecksumOK(algorithm, lp, p) {
			return multipartPlan{}, ErrInvalidPart
		}
		shaAll.Write(p.sha)
		md5All.Write(p.md5)
		plan.size += p.size
		plan.fileIDs = append(plan.fileIDs, p.fileID)
		plan.partEnds = append(plan.partEnds, plan.size)
		if algorithm != nil && p.checksum != nil {
			plan.checksums = append(plan.checksums, partChecksum{no: p.no, fileID: p.fileID, value: *p.checksum})
		}
		want++
	}
	if want != len(listed) {
		return multipartPlan{}, ErrInvalidPart
	}
	plan.sha256 = shaAll.Sum(nil)
	plan.etag = s3ETag(md5All.Sum(nil), len(plan.fileIDs))
	return plan, nil
}

func partChecksumOK(algorithm *string, listed ListedPart, stored storedPart) bool {
	switch {
	case algorithm == nil && listed.Checksum == nil:
		return true
	case algorithm != nil && listed.Checksum != nil:
		return *algorithm == listed.Checksum.Name && stored.checksum != nil && *stored.checksum == listed.Checksum.Value
	default:
		return false
	}
}

func s3ETag(md5sum []byte, parts int) string {
	h := hex.EncodeToString(md5sum)
	if parts > 0 {
		return h + "-" + strconv.Itoa(parts)
	}
	return h
}

type UploadOptions struct {
	UserMetadata      []string
	ContentType       string
	ContentEncoding   string
	ChecksumAlgorithm *string
}

func (s *Store) CreateUpload(ctx context.Context, ref ObjectRef, opts UploadOptions) (string, error) {
	return withAutoCreate(ctx, s, ref.NS.Kind, ref.NS.Name, func(sc string) (string, error) {
		var id string
		err := s.pool.QueryRow(ctx, `INSERT INTO `+sc+`.uploads (upload_id, key, checksum_algorithm, user_metadata, content_type, content_encoding)
			VALUES (gen_random_uuid()::text, $1, $2, coalesce($3, '{}'::text[]), $4, $5)
			RETURNING upload_id`, ref.Key, opts.ChecksumAlgorithm, opts.UserMetadata, opts.ContentType, opts.ContentEncoding).Scan(&id)
		return id, err
	})
}

type ListedPart struct {
	PartNo   int32
	ETag     string
	Checksum *Checksum
}

type Completed struct {
	Bucket      string
	Key         string
	ETag        string
	Size        int64
	ContentType string
}

func (s *Store) CompleteUpload(ctx context.Context, ref ObjectRef, uploadID string, listed []ListedPart, opts WriteOptions) (*Completed, error) {
	return s.completeUploadRef(ctx, ref, uploadID, listed, opts, pendingUpload{})
}

type PendingCompletion struct {
	BaseETag    string
	BaseMissing bool
	LeaseOwner  string
}

func (s *Store) CompleteUploadPending(ctx context.Context, ref ObjectRef, uploadID string, listed []ListedPart, opts WriteOptions, pending PendingCompletion) (*Completed, error) {
	if ref.NS.Kind != ns.Cache && ref.NS.Kind != ns.WriteBack {
		return nil, fmt.Errorf("pending multipart completion requires a cache or write-back namespace")
	}
	return s.completeUploadRef(ctx, ref, uploadID, listed, opts,
		pendingUpload{
			token:       PendingCacheVersion(uploadID).String(),
			baseETag:    pending.BaseETag,
			baseMissing: pending.BaseMissing,
			leaseOwner:  pending.LeaseOwner,
		})
}

type pendingUpload struct {
	token       string
	baseETag    string
	baseMissing bool
	leaseOwner  string
}

type uploadRecord struct {
	key             string
	contentType     string
	contentEncoding string
	userMeta        []string
	algorithm       *string
}

func loadUploadRecord(ctx context.Context, tx pgx.Tx, schema, uploadID, requestKey string) (uploadRecord, error) {
	var rec uploadRecord
	var algorithm pgtype.Text
	err := tx.QueryRow(ctx, `SELECT key, checksum_algorithm, user_metadata, content_type, content_encoding
		FROM `+schema+`.uploads WHERE upload_id = $1 FOR UPDATE`, uploadID).
		Scan(&rec.key, &algorithm, &rec.userMeta, &rec.contentType, &rec.contentEncoding)
	if errors.Is(err, pgx.ErrNoRows) {
		return uploadRecord{}, ErrNoUpload
	}
	if err != nil {
		return uploadRecord{}, err
	}
	if rec.key != requestKey {
		return uploadRecord{}, ErrNoUpload
	}
	if algorithm.Valid {
		value := algorithm.String
		rec.algorithm = &value
	}
	return rec, nil
}

func loadUploadParts(ctx context.Context, tx pgx.Tx, schema, uploadID string) ([]storedPart, error) {
	rows, err := tx.Query(ctx, `SELECT part_no, file_id, size, sha256, md5, checksum
		FROM `+schema+`.upload_parts WHERE upload_id = $1 ORDER BY part_no`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []storedPart
	for rows.Next() {
		var p storedPart
		var checksum pgtype.Text
		if err := rows.Scan(&p.no, &p.fileID, &p.size, &p.sha, &p.md5, &checksum); err != nil {
			return nil, err
		}
		if checksum.Valid {
			value := checksum.String
			p.checksum = &value
		}
		parts = append(parts, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

func applyMultipartPlan(ctx context.Context, tx pgx.Tx, schema, bucket, key, uploadID string, rec uploadRecord, plan multipartPlan, opts WriteOptions, pending pendingUpload) error {
	if opts.ContentEncoding == "" {
		opts.ContentEncoding = rec.contentEncoding
	}
	pub := objectPublication{
		fileID: plan.fileIDs[0], size: plan.size, sha: plan.sha256, etag: plan.etag,
		userMeta: rec.userMeta, contentType: rec.contentType, parts: plan.fileIDs, ends: plan.partEnds,
	}
	if err := swapObjectParts(ctx, tx, schema, key, &opts, pub); err != nil {
		return err
	}
	if pending.token != "" {
		if _, err := tx.Exec(ctx, `UPDATE `+schema+`.objects SET origin_etag = $2 WHERE key = $1`, key, pending.token); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO s3p.pending_uploads
			(namespace, key, token, size, attempts, next_attempt, last_error, created_at,
			 base_etag, base_missing, lease_owner, lease_until)
			VALUES ($1, $2, $3, $4, 0, now(), NULL, now(), $5, $6, NULLIF($7, ''),
			        CASE WHEN $7 = '' THEN NULL ELSE now() + interval '5 minutes' END)
			ON CONFLICT (namespace, key) DO UPDATE
			  SET token = EXCLUDED.token, size = EXCLUDED.size,
			      attempts = 0, dead = false, next_attempt = now(), last_error = NULL,
			      created_at = now(), base_etag = EXCLUDED.base_etag,
			      base_missing = EXCLUDED.base_missing,
			      lease_owner = EXCLUDED.lease_owner, lease_until = EXCLUDED.lease_until`,
			bucket, key, pending.token, plan.size, pending.baseETag, pending.baseMissing, pending.leaseOwner); err != nil {
			return err
		}
	}
	if err := storePartChecksums(ctx, tx, schema, key, rec.algorithm, plan.checksums); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+schema+".uploads WHERE upload_id = $1", uploadID); err != nil {
		return err
	}
	return queueFileIDs(ctx, tx, schema, plan.unused)
}

func storePartChecksums(ctx context.Context, tx pgx.Tx, schema, key string, algorithm *string, checks []partChecksum) error {
	if len(checks) == 0 {
		return nil
	}
	nos := make([]int32, len(checks))
	fids := make([]int64, len(checks))
	vals := make([]string, len(checks))
	for i, c := range checks {
		nos[i], fids[i], vals[i] = c.no, c.fileID, c.value
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+schema+`.object_part_checksums
		(key, part_no, file_id, algorithm, value)
		SELECT $1, no, fid, $4, val
		FROM unnest($2::int4[], $3::int8[], $5::text[]) AS t(no, fid, val)
		ON CONFLICT (key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
		  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
		key, nos, fids, *algorithm, vals)
	return err
}

func (s *Store) completeUploadRef(ctx context.Context, ref ObjectRef, uploadID string, listed []ListedPart, opts WriteOptions, pending pendingUpload) (completed *Completed, err error) {
	opts, err = s.prepareWrite(ref, opts, false)
	if err != nil {
		return nil, err
	}
	sc, err := ref.Schema()
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		err = namespaceResultError(ctx, s.pool, sc, err, ErrNoUpload)
	}()
	rec, err := loadUploadRecord(ctx, tx, sc, uploadID, ref.Key)
	if err != nil {
		return nil, err
	}
	if err := validateListedParts(listed); err != nil {
		return nil, err
	}
	parts, err := loadUploadParts(ctx, tx, sc, uploadID)
	if err != nil {
		return nil, err
	}
	plan, err := planMultipart(rec.algorithm, listed, parts)
	if err != nil {
		return nil, err
	}
	if s.checkQuota(ctx, ref.NS.Kind, ref.NS.Name, plan.size) {
		return nil, ErrQuotaExceeded
	}
	if err := applyMultipartPlan(ctx, tx, sc, ref.NS.Name, rec.key, uploadID, rec, plan, opts, pending); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Completed{Bucket: ref.NS.Name, Key: rec.key, ETag: plan.etag, Size: plan.size, ContentType: rec.contentType}, nil
}

func (s *Store) AbortUpload(ctx context.Context, ref ObjectRef, uploadID string) (bool, error) {
	sc, err := ref.Schema()
	if err != nil {
		return false, err
	}
	aborted, err := withTx(ctx, s, func(tx pgx.Tx) (bool, error) {
		var one int
		err := tx.QueryRow(ctx, "SELECT 1 FROM "+sc+".uploads WHERE upload_id = $1 AND key = $2 FOR UPDATE", uploadID, ref.Key).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		rows, err := tx.Query(ctx, "SELECT file_id FROM "+sc+".upload_parts WHERE upload_id = $1", uploadID)
		if err != nil {
			return false, err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx, "DELETE FROM "+sc+".uploads WHERE upload_id = $1", uploadID); err != nil {
			return false, err
		}
		return true, queueFileIDs(ctx, tx, sc, ids)
	})
	return aborted, namespaceResultError(ctx, s.pool, sc, err, nil)
}

type PartInfo struct {
	Number   int32
	Size     int64
	FileID   int64
	Checksum *Checksum
}

func (s *Store) Attributes(ctx context.Context, ref ObjectRef) (*Meta, []PartInfo, error) {
	sc, err := ref.Schema()
	if err != nil {
		return nil, nil, err
	}
	m, err := s.Meta(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if m.Parts == nil {
		return m, nil, nil
	}
	type partCheck struct {
		no     int32
		fileID int64
		c      *Checksum
	}
	rows, err := s.pool.Query(ctx, `SELECT part_no, file_id, algorithm, value
		FROM `+sc+`.object_part_checksums WHERE key = $1`, ref.Key)
	if err != nil {
		return nil, nil, err
	}
	checks, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (partCheck, error) {
		var c partCheck
		var alg, value string
		err := row.Scan(&c.no, &c.fileID, &alg, &value)
		c.c = &Checksum{Name: alg, Value: value}
		return c, err
	})
	if err != nil {
		return nil, nil, err
	}
	byPart := make(map[int32]partCheck, len(checks))
	for _, c := range checks {
		byPart[c.no] = c
	}
	parts := make([]PartInfo, 0, len(m.PartEnds))
	var prev int64
	for i, end := range m.PartEnds {
		no := int32(i + 1)
		p := PartInfo{Number: no, Size: end - prev}
		if i < len(m.Parts) {
			p.FileID = m.Parts[i]
		}
		if c, ok := byPart[no]; ok && c.fileID == p.FileID {
			p.Checksum = c.c
		}
		parts = append(parts, p)
		prev = end
	}
	return m, parts, nil
}

type UploadPartInfo struct {
	Number       int32
	Size         int64
	ETag         string
	Checksum     *Checksum
	LastModified time.Time
}

func (s *Store) ListParts(ctx context.Context, ref ObjectRef, uploadID string, max int, marker int32) ([]UploadPartInfo, bool, int32, error) {
	sc, err := ref.Schema()
	if err != nil {
		return nil, false, 0, err
	}
	var algorithm pgtype.Text
	var createdAt time.Time
	err = s.pool.QueryRow(ctx, `SELECT checksum_algorithm, created_at
		FROM `+sc+`.uploads WHERE upload_id = $1 AND key = $2`, uploadID, ref.Key).
		Scan(&algorithm, &createdAt)
	if errors.Is(err, pgx.ErrNoRows) || missingNamespace(ctx, s.pool, sc, err) {
		return nil, false, 0, ErrNoUpload
	}
	if err != nil {
		return nil, false, 0, err
	}
	rows, err := s.pool.Query(ctx, `SELECT part_no, size, md5, checksum FROM `+sc+`.upload_parts
		WHERE upload_id = $1 AND part_no > $2 ORDER BY part_no LIMIT $3`, uploadID, marker, max+1)
	if err != nil {
		return nil, false, 0, err
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (UploadPartInfo, error) {
		var no int32
		var size int64
		var md5sum []byte
		var checksum pgtype.Text
		if err := row.Scan(&no, &size, &md5sum, &checksum); err != nil {
			return UploadPartInfo{}, err
		}
		p := UploadPartInfo{Number: no, Size: size, ETag: hex.EncodeToString(md5sum), LastModified: createdAt}
		if algorithm.Valid && checksum.Valid {
			p.Checksum = &Checksum{Name: algorithm.String, Value: checksum.String}
		}
		return p, nil
	})
	if err != nil {
		return nil, false, 0, err
	}
	truncated := len(out) > max
	if truncated {
		out = out[:max]
	}
	next := marker
	if len(out) > 0 {
		next = out[len(out)-1].Number
	}
	return out, truncated, next, nil
}

type UploadInfo struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

func (s *Store) ListUploads(ctx context.Context, ref ObjectRef, prefix, bound, keyMarker, uploadMarker string, max int) ([]UploadInfo, bool, error) {
	sc, err := ref.Schema()
	if err != nil {
		return nil, false, err
	}
	rows, err := s.pool.Query(ctx, `SELECT key, upload_id, created_at
		FROM `+sc+`.uploads
		WHERE key >= $1 AND key < $2
		  AND (key > $3 OR (key = $3 AND upload_id > $4))
		ORDER BY key, upload_id LIMIT $5`, prefix, bound, keyMarker, uploadMarker, max+1)
	if missingNamespace(ctx, s.pool, sc, err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (UploadInfo, error) {
		var u UploadInfo
		err := row.Scan(&u.Key, &u.UploadID, &u.Initiated)
		return u, err
	})
	if err != nil {
		return nil, false, err
	}
	truncated := len(out) > max
	if truncated {
		out = out[:max]
	}
	return out, truncated, nil
}

type CopyOptions struct {
	ReplaceMetadata bool
	UserMetadata    []string
	ContentType     string
	ContentEncoding string
	OriginModified  time.Time
	Source          ReadCondition
	Dest            Precondition
}

func (s *Store) Copy(ctx context.Context, src, dst ObjectRef, opts CopyOptions) (*Meta, error) {
	if src.NS.Kind != ns.Bucket || dst.NS.Kind != ns.Bucket {
		return nil, ErrInvalidBucket
	}
	srcSchema, err := src.Schema()
	if err != nil {
		return nil, err
	}
	return withAutoCreate(ctx, s, ns.Bucket, dst.NS.Name, func(dstSchema string) (*Meta, error) {
		return s.copyObject(ctx, src, dst, srcSchema, dstSchema, opts)
	})
}

func (s *Store) copyObject(ctx context.Context, src, dst ObjectRef, srcSchema, dstSchema string, opts CopyOptions) (*Meta, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := lockTwo(ctx, tx, srcSchema, src.Key, dstSchema, dst.Key); err != nil {
		return nil, err
	}
	srcMeta, srcFile, err := resolveCopySource(ctx, tx, srcSchema, src.Key, opts.Source)
	if err != nil {
		return nil, err
	}
	if s.checkQuota(ctx, ns.Bucket, dst.NS.Name, srcMeta.Size) {
		return nil, ErrQuotaExceeded
	}
	dest, err := readCopyDest(ctx, tx, dstSchema, dst.Key, opts.Dest)
	if err != nil {
		return nil, err
	}
	return s.publishCopy(ctx, tx, copyPlan{
		src: src, dst: dst, srcSchema: srcSchema, dstSchema: dstSchema,
		srcMeta: srcMeta, srcFile: srcFile, dest: dest, opts: opts,
	})
}

type copyPlan struct {
	src, dst  ObjectRef
	srcSchema string
	dstSchema string
	srcMeta   Meta
	srcFile   pgtype.Int8
	dest      copyDest
	opts      CopyOptions
}

func (s *Store) publishCopy(ctx context.Context, tx pgx.Tx, p copyPlan) (*Meta, error) {
	fields := copyFieldsFor(p.srcMeta, p.opts)
	sameBucket := p.src.NS == p.dst.NS
	dstFileID, dstParts, idMap, err := s.prepareDestFiles(ctx, tx, p.srcSchema, p.dstSchema, sameBucket, p.srcMeta, p.srcFile)
	if err != nil {
		return nil, err
	}
	if err := replaceCopiedObject(ctx, tx, p, fields, dstFileID, dstParts); err != nil {
		return nil, err
	}
	if err := copyPartChecksums(ctx, tx, p.srcSchema, p.src.Key, p.dstSchema, p.dst.Key, sameBucket, len(dstParts) > 0, idMap); err != nil {
		return nil, err
	}
	if p.dest.exists && p.dest.file.Valid {
		if err := queueFiles(ctx, tx, p.dstSchema, p.dest.file.Int64, p.dest.parts); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return copiedMeta(p.srcMeta, fields, dstFileID, dstParts), nil
}

type copyFields struct {
	userMeta        []string
	contentType     string
	contentEncoding string
	originModified  any
	checksumName    any
	checksumValue   any
	checksumType    any
}

func copyFieldsFor(src Meta, opts CopyOptions) copyFields {
	f := copyFields{userMeta: src.UserMetadata, contentType: src.ContentType, contentEncoding: src.ContentEncoding}
	if src.ChecksumName != "" {
		f.checksumName, f.checksumValue, f.checksumType = src.ChecksumName, src.ChecksumValue, src.ChecksumType
	}
	if opts.ReplaceMetadata {
		f.userMeta, f.contentType = opts.UserMetadata, opts.ContentType
		f.contentEncoding = opts.ContentEncoding
		f.checksumName, f.checksumValue, f.checksumType = nil, nil, nil
	}
	if !opts.OriginModified.IsZero() {
		f.originModified = opts.OriginModified
	}
	return f
}

func replaceCopiedObject(ctx context.Context, tx pgx.Tx, p copyPlan, f copyFields, fileID int64, parts []int64) error {
	var fileVal any
	if p.srcFile.Valid {
		fileVal = fileID
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+p.dstSchema+".object_part_checksums WHERE key = $1", p.dst.Key); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+p.dstSchema+`.objects
		(key, file_id, size, sha256, etag, user_metadata, content_type, content_encoding,
		 origin_modified, checksum_name, checksum_value, checksum_type, parts, part_ends, inline_data)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		ON CONFLICT (key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		  sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag, user_metadata = EXCLUDED.user_metadata,
		  content_type = EXCLUDED.content_type, content_encoding = EXCLUDED.content_encoding,
		  origin_modified = EXCLUDED.origin_modified,
		  checksum_name = EXCLUDED.checksum_name, checksum_value = EXCLUDED.checksum_value,
		  checksum_type = EXCLUDED.checksum_type, parts = EXCLUDED.parts,
		  part_ends = EXCLUDED.part_ends, inline_data = EXCLUDED.inline_data, created_at = now()`,
		p.dst.Key, fileVal, p.srcMeta.Size, p.srcMeta.SHA256, p.srcMeta.ETag, f.userMeta, f.contentType, f.contentEncoding,
		f.originModified, f.checksumName, f.checksumValue, f.checksumType, parts, p.srcMeta.PartEnds, p.srcMeta.Inline)
	return err
}

func copiedMeta(src Meta, f copyFields, fileID int64, parts []int64) *Meta {
	out := src
	out.FileID = fileID
	out.Parts = parts
	out.UserMetadata, out.ContentType = f.userMeta, f.contentType
	out.ContentEncoding = f.contentEncoding
	out.ChecksumName, out.ChecksumValue, out.ChecksumType = "", "", ""
	out.OriginModified = time.Time{}
	out.ExpiresAt, out.OriginETag = nil, ""
	out.CreatedAt = time.Now()
	return &out
}

func resolveCopySource(ctx context.Context, tx pgx.Tx, srcSchema, srcKey string, cond ReadCondition) (Meta, pgtype.Int8, error) {
	src, err := queryMetadata(ctx, tx, srcSchema, srcKey, metadataStored)
	if errors.Is(err, ErrNotFound) {
		err = ErrNoBucket
	}
	if err != nil {
		return Meta{}, pgtype.Int8{}, inNamespace(srcSchema, err)
	}
	if !cond.Allows(true, src.ETag) {
		return Meta{}, pgtype.Int8{}, ErrPrecondition
	}
	return *src, pgtype.Int8{Int64: src.FileID, Valid: src.FileID != 0}, nil
}

type copyDest struct {
	exists bool
	file   pgtype.Int8
	parts  []int64
}

func readCopyDest(ctx context.Context, tx pgx.Tx, dstSchema, dstKey string, cond Precondition) (copyDest, error) {
	var d copyDest
	var etag string
	var expires pgtype.Timestamptz
	err := tx.QueryRow(ctx, `SELECT file_id, parts, etag, expires_at FROM `+dstSchema+`.objects WHERE key = $1 FOR UPDATE`,
		dstKey).Scan(&d.file, &d.parts, &etag, &expires)
	d.exists = err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return copyDest{}, err
	}
	live := d.exists && (!expires.Valid || expires.Time.After(time.Now()))
	if !cond.Allows(live, etag) {
		return copyDest{}, ErrPrecondition
	}
	return d, nil
}

func (s *Store) prepareDestFiles(ctx context.Context, tx pgx.Tx, srcSchema, dstSchema string, sameBucket bool, src Meta, srcFile pgtype.Int8) (int64, []int64, map[int64]int64, error) {
	if sameBucket || src.Inline != nil || !srcFile.Valid {
		return src.FileID, src.Parts, nil, nil
	}
	idMap := map[int64]int64{}
	dupOne := func(oldID int64) (int64, error) {
		if newID, ok := idMap[oldID]; ok {
			return newID, nil
		}
		newID, err := s.nextFileID(ctx)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx,
			"INSERT INTO "+dstSchema+".chunks (file_id, no, data) SELECT $1, no, data FROM "+srcSchema+".chunks WHERE file_id = $2",
			newID, oldID); err != nil {
			return 0, err
		}
		idMap[oldID] = newID
		return newID, nil
	}
	if len(src.Parts) == 0 {
		newID, err := dupOne(src.FileID)
		return newID, nil, idMap, err
	}
	dstParts := make([]int64, len(src.Parts))
	for i, oldPartID := range src.Parts {
		newID, err := dupOne(oldPartID)
		if err != nil {
			return 0, nil, nil, err
		}
		dstParts[i] = newID
	}
	return dstParts[0], dstParts, idMap, nil
}

func copyPartChecksums(ctx context.Context, tx pgx.Tx, srcSchema, srcKey, dstSchema, dstKey string, sameBucket, hasParts bool, idMap map[int64]int64) error {
	if sameBucket {
		if srcKey == dstKey {
			return nil
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
			(key, part_no, file_id, algorithm, value)
			SELECT $2, part_no, file_id, algorithm, value
			FROM `+srcSchema+`.object_part_checksums WHERE key = $1
			ON CONFLICT (key, part_no) DO UPDATE SET file_id = EXCLUDED.file_id,
			  algorithm = EXCLUDED.algorithm, value = EXCLUDED.value`,
			srcKey, dstKey)
		return err
	}
	if !hasParts {
		return nil
	}
	oldIDs := make([]int64, 0, len(idMap))
	newIDs := make([]int64, 0, len(idMap))
	for oldID, newID := range idMap {
		oldIDs = append(oldIDs, oldID)
		newIDs = append(newIDs, newID)
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+dstSchema+`.object_part_checksums
		(key, part_no, file_id, algorithm, value)
		SELECT $2, c.part_no, COALESCE(m.new_id, c.file_id), c.algorithm, c.value
		FROM `+srcSchema+`.object_part_checksums c
		LEFT JOIN unnest($3::int8[], $4::int8[]) AS m(old_id, new_id) ON m.old_id = c.file_id
		WHERE c.key = $1`,
		srcKey, dstKey, oldIDs, newIDs)
	return err
}

func lockTwo(ctx context.Context, tx pgx.Tx, b1, k1, b2, k2 string) error {
	if b1+"\x00"+k1 > b2+"\x00"+k2 {
		b1, b2, k1, k2 = b2, b1, k2, k1
	}
	if err := lockObjectKey(ctx, tx, b1, k1); err != nil {
		return err
	}
	if b1 == b2 && k1 == k2 {
		return nil
	}
	return lockObjectKey(ctx, tx, b2, k2)
}

func (s *Store) CopyPart(ctx context.Context, uploadID string, dst, src ObjectRef, partNo int32) (published *Published, copied bool, err error) {
	if src.NS != dst.NS {
		return nil, false, nil
	}
	sc, err := dst.Schema()
	if err != nil {
		return nil, false, err
	}
	srcMeta, err := s.Meta(ctx, src)
	if err != nil {
		return nil, false, err
	}
	if srcMeta.Inline != nil || srcMeta.Parts != nil || srcMeta.FileID == 0 || srcMeta.Size <= 0 {
		return nil, false, nil
	}
	md5sum, ok := etagMD5(srcMeta.ETag)
	if !ok {
		return nil, false, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		err = namespaceResultError(ctx, s.pool, sc, err, ErrNoUpload)
	}()
	var algorithm pgtype.Text
	err = tx.QueryRow(ctx, `SELECT checksum_algorithm FROM `+sc+`.uploads
		WHERE upload_id = $1 AND key = $2 FOR KEY SHARE`,
		uploadID, dst.Key).Scan(&algorithm)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, ErrNoUpload
	}
	if err != nil {
		return nil, false, err
	}
	var chk *Checksum
	if algorithm.Valid {
		if algorithm.String != "SHA256" {
			return nil, false, nil
		}
		chk = &Checksum{Name: "SHA256", Value: base64.StdEncoding.EncodeToString(srcMeta.SHA256)}
	}
	if err := commitPart(ctx, tx, sc, uploadID, partNo, srcMeta.FileID, srcMeta.Size, srcMeta.SHA256, md5sum, chk); err != nil {
		return nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return &Published{
		FileID: srcMeta.FileID, Size: srcMeta.Size, ETag: hex.EncodeToString(md5sum),
		SHA256: srcMeta.SHA256, MD5: md5sum, Checksum: chk,
	}, true, nil
}

func etagMD5(etag string) ([]byte, bool) {
	if i := strings.IndexByte(etag, '-'); i >= 0 {
		etag = etag[:i]
	}
	b, err := hex.DecodeString(etag)
	if err != nil || len(b) != 16 {
		return nil, false
	}
	return b, true
}
