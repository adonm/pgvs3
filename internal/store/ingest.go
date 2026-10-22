package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RowBytes is the payload per chunk row. file_id(8) + no(4) + varlena(4) +
// 8120 bytes plus the 24-byte tuple header is 8160, exactly one 8 KB page.
const RowBytes = 8120

const copySQL = "COPY s3p.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)"

var copyHeader = []byte("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00")
var copyTrailer = []byte{0xFF, 0xFF}

// sendBatch is the COPY flush granularity: enough to amortise the protocol,
// small enough to keep the request body backpressured at PostgreSQL's pace.
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

// Checksum is one verified request checksum, named as S3 spells the header.
type Checksum struct {
	Name  string
	Value string
}

// Published is the result of one successful object or part ingest.
type Published struct {
	FileID   int64
	Size     int64
	ETag     string
	SHA256   []byte
	MD5      []byte
	Checksum *Checksum
}

var (
	ErrNoBucket        = errors.New("bucket does not exist")
	ErrNoUpload        = errors.New("upload does not exist")
	ErrPrecondition    = errors.New("object precondition failed")
	ErrMissingChecksum = errors.New("part lacks the upload's checksum")
	ErrInvalidPart     = errors.New("invalid part")
	errAborted         = errors.New("ingest aborted")
)

// rowFramer turns a byte stream into fixed-size binary COPY rows, hashing the
// same bytes once for the SHA-256 integrity digest and the S3 MD5 ETag.
type rowFramer struct {
	fileID  int64
	no      int32
	pending []byte
	buf     *bytes.Buffer
	sha     hash.Hash
	md5     hash.Hash
	size    int64
}

func newRowFramer(fileID int64) *rowFramer {
	f := &rowFramer{
		fileID: fileID,
		buf:    getFramerBuf(),
		sha:    sha256.New(),
		md5:    md5.New(),
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
	f.pending = append(f.pending, src...)
	whole := len(f.pending) - len(f.pending)%RowBytes
	if whole == 0 {
		return nil, nil
	}
	f.frameRows(f.pending[:whole])
	f.pending = f.pending[whole:]
	if f.buf.Len() < sendBatch {
		return nil, nil
	}
	out := f.buf
	f.buf = getFramerBuf()
	return out, nil
}

// finish frames any partial row and the COPY trailer.
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
		// field count
		f.buf.Write([]byte{0, 3})
		// file_id
		var b8 [8]byte
		binary.BigEndian.PutUint64(b8[:], uint64(f.fileID))
		f.buf.Write([]byte{0, 0, 0, 8})
		f.buf.Write(b8[:])
		// no
		var b4 [4]byte
		binary.BigEndian.PutUint32(b4[:], uint32(f.no))
		f.buf.Write([]byte{0, 0, 0, 4})
		f.buf.Write(b4[:])
		// data
		binary.BigEndian.PutUint32(b4[:], uint32(end-start))
		f.buf.Write(b4[:])
		f.buf.Write(data[start:end])
		f.no++
	}
}

func (f *rowFramer) digests() (int64, []byte, []byte) {
	return f.size, f.sha.Sum(nil), f.md5.Sum(nil)
}

type writerTarget struct {
	kind       int // 0 object, 1 part
	bucket     string
	key        string
	condition  Condition
	ifMatch    string
	userMeta   []string
	content    string
	uploadID   string
	partNo     int32
	checksumAl string
}

type Condition int

const (
	Unconditional Condition = iota
	IfAbsent
	IfMatch
)

// ChunkWriter streams one PUT or multipart part into its own COPY stream and
// commits the chunks and the publication in the same transaction.
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

// StartObject begins an object write. The bucket check and file ID allocation
// happen before this returns, so the HTTP handler can stream the body straight
// into COPY.
func (s *Store) StartObject(ctx context.Context, bucket, key string, cond Condition, ifMatch string, userMeta []string, contentType string) (*ChunkWriter, error) {
	return s.startWriter(ctx, writerTarget{
		kind: 0, bucket: bucket, key: key, condition: cond, ifMatch: ifMatch,
		userMeta: userMeta, content: contentType,
	})
}

// StartPart begins a multipart part write. A re-sent part replaces the earlier
// attempt atomically, rows included.
func (s *Store) StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32) (*ChunkWriter, error) {
	return s.startWriter(ctx, writerTarget{kind: 1, bucket: bucket, key: key, uploadID: uploadID, partNo: partNo})
}

func (s *Store) startWriter(ctx context.Context, t writerTarget) (*ChunkWriter, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, err
	}
	fail := func(err error) (*ChunkWriter, error) {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, err
	}
	if t.kind == 0 && t.condition == Unconditional && s.writeCommit != "on" {
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = "+s.writeCommit); err != nil {
			return fail(err)
		}
	}
	if t.kind == 0 {
		if !s.bucketKnown(t.bucket) {
			var one int
			err = tx.QueryRow(ctx, "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR KEY SHARE", t.bucket).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return fail(ErrNoBucket)
			}
			if err == nil {
				s.markBucket(t.bucket)
			}
		}
	} else {
		var alg *string
		err = tx.QueryRow(ctx,
			"SELECT checksum_algorithm FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR KEY SHARE",
			t.uploadID, t.bucket, t.key).Scan(&alg)
		if errors.Is(err, pgx.ErrNoRows) {
			return fail(ErrNoUpload)
		}
		if alg != nil {
			t.checksumAl = *alg
		}
	}
	if err != nil {
		return fail(err)
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
	if _, err := tx.Conn().PgConn().CopyFrom(ctx, pr, copySQL); err != nil {
		_ = pr.CloseWithError(err)
		_ = tx.Rollback(ctx)
		w.done <- err
		return
	}
	size, sha, md5 := w.framer.digests()
	etag := hex.EncodeToString(md5)

	w.mu.Lock()
	checksums := append([]Checksum(nil), w.checksums...)
	w.mu.Unlock()

	var checksum *Checksum
	switch t.kind {
	case 0:
		condText := "none"
		switch t.condition {
		case IfAbsent:
			condText = "if_absent"
		case IfMatch:
			condText = "if_match"
		}
		var match any
		if t.ifMatch != "" {
			match = t.ifMatch
		}
		var outcome string
		err := tx.QueryRow(ctx, `SELECT s3p.publish_object($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			t.bucket, t.key, fileIDOf(w), size, sha, etag, t.userMeta, t.content, condText, match).Scan(&outcome)
		if err == nil && outcome == "precondition" {
			err = ErrPrecondition
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			w.done <- err
			return
		}
	case 1:
		if t.checksumAl != "" {
			name := "x-amz-checksum-" + lower(t.checksumAl)
			found := false
			for _, c := range checksums {
				if c.Name == name {
					checksum = &c
					found = true
					break
				}
			}
			if !found {
				_ = tx.Rollback(ctx)
				w.done <- ErrMissingChecksum
				return
			}
		}
		if err := commitPart(ctx, tx, t.uploadID, t.partNo, fileIDOf(w), size, sha, md5, checksum); err != nil {
			_ = tx.Rollback(ctx)
			w.done <- err
			return
		}
	}
	if err := tx.Commit(ctx); err != nil {
		w.done <- err
		return
	}
	pub := Published{FileID: fileIDOf(w), Size: size, ETag: etag, SHA256: sha, MD5: md5, Checksum: checksum}
	if t.kind == 0 {
		w.store.cache.Put(ctx, t.bucket, t.key, Meta{
			Size: size, SHA256: sha, ETag: etag, UserMetadata: t.userMeta,
			ContentType: t.content, CreatedAt: time.Now(), FileID: fileIDOf(w),
		})
		w.store.invalidatePeers(ctx, t.bucket, t.key)
		w.store.objects.invalidate(t.bucket, t.key)
	}
	w.mu.Lock()
	w.published = pub
	w.mu.Unlock()
	w.done <- nil
}

// fileIDOf exists because the framer owns the ID; keeping it in one place
// makes the publish code below read naturally.
func fileIDOf(w *ChunkWriter) int64 { return w.framer.fileID }

// ChecksumAlgorithm is the algorithm a multipart part must carry, if any.
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

// Finish publishes the write. Dropping a writer without Finish rolls it back.
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

// Abort rolls the write back. It is safe after Finish.
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

func swapObject(ctx context.Context, tx pgx.Tx, t writerTarget, fileID, size int64, sha []byte, etag string) error {
	return swapObjectParts(ctx, tx, t.bucket, t.key, &t, fileID, size, sha, etag, t.userMeta, t.content, nil, nil)
}

func swapObjectParts(ctx context.Context, tx pgx.Tx, bucket, key string, t *writerTarget, fileID, size int64, sha []byte, etag string, userMeta []string, content string, parts, ends []int64) error {
	if err := lockObjectKey(ctx, tx, bucket, key); err != nil {
		return err
	}
	var oldFile pgtype.Int8
	var oldParts []int64
	var oldETag string
	err := tx.QueryRow(ctx,
		"SELECT file_id, parts, etag FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE",
		bucket, key).Scan(&oldFile, &oldParts, &oldETag)
	old := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if t != nil {
		switch t.condition {
		case Unconditional:
		case IfAbsent:
			if old {
				return ErrPrecondition
			}
		case IfMatch:
			if !old || oldETag != t.ifMatch {
				return ErrPrecondition
			}
		}
	}
	if _, err := tx.Exec(ctx, "DELETE FROM s3p.object_part_checksums WHERE bucket = $1 AND key = $2", bucket, key); err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO s3p.objects (bucket, key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends, inline_data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL)
		 ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size,
		   sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag,
		   user_metadata = EXCLUDED.user_metadata, content_type = EXCLUDED.content_type,
		   parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends, inline_data = NULL,
		   created_at = now()`,
		bucket, key, fileID, size, sha, etag, userMeta, content, parts, ends)
	if err != nil {
		return err
	}
	if old && oldFile.Valid {
		return queueFiles(ctx, tx, oldFile.Int64, oldParts)
	}
	return nil
}

// lockObjectKey serialises mutations of one key. An absent row cannot be
// locked with FOR UPDATE, and a precondition must be checked after the lock.
func lockObjectKey(ctx context.Context, tx pgx.Tx, bucket, key string) error {
	_, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock(hashtextextended($1, 0) # hashtextextended($2, 1))",
		bucket, key)
	return err
}

func queueFiles(ctx context.Context, tx pgx.Tx, fileID int64, parts []int64) error {
	dead := make([]int64, 0, len(parts)+1)
	dead = append(dead, parts...)
	if fileID != 0 {
		dead = append(dead, fileID)
	}
	return queueFileIDs(ctx, tx, dead)
}

// queueFileIDs queues only unreferenced files. CopyObject, UploadPartCopy and
// multipart parts can share a file ID, so "the old row pointed at it" is not
// enough to prove it is dead.
func queueFileIDs(ctx context.Context, tx pgx.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO s3p.garbage (file_id)
		SELECT f FROM unnest($1::int8[]) AS f
		WHERE f <> 0
		  AND NOT EXISTS (SELECT 1 FROM s3p.objects o WHERE o.file_id = f OR f = ANY(o.parts))
		  AND NOT EXISTS (SELECT 1 FROM s3p.upload_parts p WHERE p.file_id = f)
		ON CONFLICT DO NOTHING`, ids)
	return err
}

func commitPart(ctx context.Context, tx pgx.Tx, uploadID string, partNo int32, fileID, size int64, sha, md5sum []byte, checksum *Checksum) error {
	var value *string
	if checksum != nil {
		value = &checksum.Value
	}
	_, err := tx.Exec(ctx, `SELECT s3p.commit_part($1,$2,$3,$4,$5,$6,$7)`,
		uploadID, partNo, fileID, size, sha, md5sum, value)
	return err
}

func lower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
