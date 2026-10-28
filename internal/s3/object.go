package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"iter"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/objcache"
	"github.com/adonm/pgvs3/internal/store"
)

type readHead struct {
	Info     store.ObjectInfo
	Parts    []int64
	PartEnds []int64
	Hit      bool
}

type objectBackend interface {
	readBackend
	writeBackend
	multipartBackend
}

type readBackend interface {
	Head(ctx context.Context, bucket, key string) (readHead, error)
	Open(ctx context.Context, bucket, key string, rng store.ByteRange) (*store.ReadResult, error)
	List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error)
}

func (s *Server) backendFor(ctx context.Context, bucket string) readBackend {
	if s.readOverride != nil {
		return s.readOverride
	}
	return s.objectBackendFor(ctx, bucket)
}

func (s *Server) objectBackendFor(ctx context.Context, bucket string) objectBackend {
	if origin := s.originFor(ctx, bucket); origin != nil {
		return originBackend{Tier: origin}
	}
	return localBackend{store: s.store}
}

type localBackend struct {
	store *store.Store
}

func (b localBackend) Head(ctx context.Context, bucket, key string) (readHead, error) {
	m, err := b.store.MetaHeader(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key})
	if err != nil {
		return readHead{}, err
	}
	return readHead{Info: m.ObjectInfo, Parts: m.Parts, PartEnds: m.PartEnds}, nil
}

func (b localBackend) Open(ctx context.Context, bucket, key string, rng store.ByteRange) (*store.ReadResult, error) {
	sm, body, err := b.store.GetBody(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, rng, nil)
	if err != nil {
		return nil, err
	}
	return &store.ReadResult{
		ObjectInfo: sm.ObjectInfo,
		Body:       body,
		Start:      sm.Start,
		End:        sm.End,
		Ranged:     rng.Requested,
	}, nil
}

func (b localBackend) List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error) {
	return b.store.List(ctx, ns.BucketRef(bucket), prefix, prefixEnd(prefix), after, limit)
}

type originBackend struct {
	*objcache.Tier
}

func (b originBackend) Head(ctx context.Context, bucket, key string) (readHead, error) {
	obj, err := b.Tier.Head(ctx, bucket, key)
	if err != nil {
		return readHead{}, err
	}
	return readHead{Info: obj.ObjectInfo, Hit: obj.Hit}, nil
}

func (s *Server) SetOrigin(t *objcache.Tier) { s.origins.SetDefault(t) }

func (s *Server) originFor(ctx context.Context, bucket string) *objcache.Tier {
	if s.origins == nil {
		return nil
	}
	return s.origins.ForBucket(ctx, bucket)
}

func (s *Server) serveOriginBucket(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	c := routeContext{s: s, w: w, r: r, bucket: bucket, q: q}
	found, err := c.dispatch(originBucketRoutes)
	if !found {
		return errMethodNotAllowed()
	}
	return err
}

type writeBackend interface {
	Put(ctx context.Context, bucket, key string, body io.Reader, contentLength int64, opts store.WriteOptions, validate func() ([]store.Checksum, error)) (string, error)
	Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, opts store.CopyOptions) (*store.Meta, error)
	Delete(ctx context.Context, bucket, key string) error
	DeleteMany(ctx context.Context, bucket string, keys []string) ([]deleteOutcome, error)
}

type deleteOutcome = objcache.DeleteResult

func (s *Server) writeBackendFor(ctx context.Context, bucket string) writeBackend {
	if s.writeOverride != nil {
		return s.writeOverride
	}
	return s.objectBackendFor(ctx, bucket)
}

func (b localBackend) Put(ctx context.Context, bucket, key string, body io.Reader, contentLength int64, opts store.WriteOptions, validate func() ([]store.Checksum, error)) (string, error) {
	if contentLength >= 0 {
		opts.ExpectedSize = contentLength
	}
	if contentLength >= 0 && contentLength <= store.MaxInlineBytes {
		inline, err := readInlineBody(contentLength, body, store.MaxInlineBytes)
		if err != nil {
			return "", err
		}
		defer inline.release()
		checks, err := validate()
		if err != nil {
			return "", err
		}
		if opts.Checksum == nil {
			if c := store.FirstObjectChecksum(checks); c != nil {
				opts.Checksum = c
				opts.ChecksumType = "FULL_OBJECT"
			}
		}
		obj, err := b.store.PutInline(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, inline.data, opts)
		if err != nil {
			return "", err
		}
		return obj.ETag, nil
	}
	writer, err := b.store.StartObject(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, opts)
	if err != nil {
		return "", err
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, body, buf); err != nil {
		writer.Abort()
		return "", err
	}
	checks, err := validate()
	if err != nil {
		writer.Abort()
		return "", err
	}
	pub, err := writer.Finish(checks)
	if err != nil {
		return "", err
	}
	return pub.ETag, nil
}

func (b localBackend) Copy(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string, opts store.CopyOptions) (*store.Meta, error) {
	return b.store.Copy(ctx,
		store.ObjectRef{NS: ns.BucketRef(srcBucket), Key: srcKey},
		store.ObjectRef{NS: ns.BucketRef(dstBucket), Key: dstKey}, opts)
}

func (b localBackend) Delete(ctx context.Context, bucket, key string) error {
	_, err := b.store.Delete(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key})
	return err
}

func (b localBackend) DeleteMany(ctx context.Context, bucket string, keys []string) ([]deleteOutcome, error) {
	if _, err := b.store.DeleteMany(ctx, ns.BucketRef(bucket), keys); err != nil {
		return nil, err
	}
	out := make([]deleteOutcome, 0, len(keys))
	for _, key := range keys {
		out = append(out, deleteOutcome{Key: key})
	}
	return out, nil
}

func (b originBackend) Put(ctx context.Context, bucket, key string, body io.Reader, size int64, opts store.WriteOptions, validate func() ([]store.Checksum, error)) (string, error) {
	m, err := b.Tier.UploadWithOptions(ctx, bucket, key, body, objcache.UploadOptions{
		ExpectedSize:    size,
		UserMetadata:    opts.UserMetadata,
		ContentType:     opts.ContentType,
		ContentEncoding: opts.ContentEncoding,
		Checksum:        opts.Checksum,
		ChecksumType:    opts.ChecksumType,
		Precondition:    opts.Precondition,
		Validate: func() error {
			_, err := validate()
			return err
		},
	})
	if err != nil {
		return "", err
	}
	return m.ETag, nil
}

func (b originBackend) DeleteMany(ctx context.Context, bucket string, keys []string) ([]deleteOutcome, error) {
	return b.Tier.DeleteMany(ctx, bucket, keys), nil
}

type multipartBackend interface {
	CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType, contentEncoding string, checksumAlg *string) (string, error)
	StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32, expectedSize int64) (*store.ChunkWriter, error)
	ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]store.UploadPartInfo, bool, int32, error)
	CompleteUpload(ctx context.Context, uploadID, bucket, key string, listed []store.ListedPart, opts store.WriteOptions) (*store.Completed, error)
	AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error)
	ListUploads(ctx context.Context, bucket, prefix, bound, keyMarker, uploadMarker string, max int) ([]store.UploadInfo, bool, error)
}

func (s *Server) multipartBackendFor(ctx context.Context, bucket string) multipartBackend {
	if s.multipartOverride != nil {
		return s.multipartOverride
	}
	return s.objectBackendFor(ctx, bucket)
}

func (b localBackend) CreateUpload(ctx context.Context, bucket, key string, userMeta []string, contentType, contentEncoding string, checksumAlg *string) (string, error) {
	return b.store.CreateUpload(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, store.UploadOptions{UserMetadata: userMeta, ContentType: contentType, ContentEncoding: contentEncoding, ChecksumAlgorithm: checksumAlg})
}

func (b localBackend) StartPart(ctx context.Context, uploadID, bucket, key string, partNo int32, expectedSize int64) (*store.ChunkWriter, error) {
	return b.store.StartPart(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, uploadID, partNo, expectedSize)
}

func (b localBackend) ListParts(ctx context.Context, uploadID, bucket, key string, max int, marker int32) ([]store.UploadPartInfo, bool, int32, error) {
	return b.store.ListParts(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, uploadID, max, marker)
}

func (b localBackend) CompleteUpload(ctx context.Context, uploadID, bucket, key string, listed []store.ListedPart, opts store.WriteOptions) (*store.Completed, error) {
	return b.store.CompleteUpload(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, uploadID, listed, opts)
}

func (b localBackend) AbortUpload(ctx context.Context, uploadID, bucket, key string) (bool, error) {
	return b.store.AbortUpload(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, uploadID)
}

func (b localBackend) ListUploads(ctx context.Context, bucket, prefix, bound, keyMarker, uploadMarker string, max int) ([]store.UploadInfo, bool, error) {
	return b.store.ListUploads(ctx, store.ObjectRef{NS: ns.BucketRef(bucket)}, prefix, bound, keyMarker, uploadMarker, max)
}

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	backend := s.backendFor(r.Context(), bucket)
	head, err := backend.Head(r.Context(), bucket, key)
	if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
		key += "index.html"
		head, err = backend.Head(r.Context(), bucket, key)
	}
	if err != nil {
		return err
	}
	part, hasPart, err := parsePartNumber(r.URL.Query())
	if err != nil {
		return err
	}
	length := head.Info.Size
	if hasPart {
		if head.Parts == nil && backendIsOrigin(backend) {
			return errNotImplemented()
		}
		m := &store.Meta{Size: head.Info.Size, Parts: head.Parts, PartEnds: head.PartEnds}
		start, end, ok := partBounds(m, part)
		if !ok {
			return errInvalidPartNumber()
		}
		length = max(end-start+1, 0)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, head.Info.Size))
	}
	if err := readCondition(r, head.Info.ETag); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(head.Info.ETag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}
	setObjectInfoHeaders(w, head.Info, key)
	if head.Parts != nil {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(len(head.Parts)))
	}
	if backendIsOrigin(backend) {
		if head.Hit {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusOK)
	return nil
}

func backendIsOrigin(b readBackend) bool {
	_, ok := b.(originBackend)
	return ok
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	backend := s.backendFor(r.Context(), bucket)
	isOrigin := backendIsOrigin(backend)
	part, hasPart, err := parsePartNumber(r.URL.Query())
	if err != nil {
		return err
	}
	rng, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		return errInvalidRange()
	}
	var partCount int
	if hasPart {
		if isOrigin {
			return errNotImplemented()
		}
		if rng.Requested {
			return errInvalidRequest("partNumber and Range are mutually exclusive")
		}
		head, err := backend.Head(r.Context(), bucket, key)
		if err != nil {
			return err
		}
		m := &store.Meta{Size: head.Info.Size, Parts: head.Parts, PartEnds: head.PartEnds}
		start, end, ok := partBounds(m, part)
		if !ok {
			return errInvalidPartNumber()
		}
		rng = store.Range(start, end, -1)
		partCount = partsCount(m)
	}
	res, err := backend.Open(r.Context(), bucket, key, rng)
	if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
		key += "index.html"
		res, err = backend.Open(r.Context(), bucket, key, rng)
	}
	if err != nil {
		if errors.Is(err, store.ErrInvalidRange) {
			var size int64
			if head, herr := backend.Head(r.Context(), bucket, key); herr == nil {
				size = head.Info.Size
			}
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(size, 10))
			return errInvalidRange()
		}
		return err
	}
	defer res.Body.Close()
	if rng.Requested && res.Size == 0 {
		w.Header().Set("Content-Range", "bytes */0")
		return errInvalidRange()
	}
	if err := readCondition(r, res.ETag); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(res.ETag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}
	if partCount > 0 {
		w.Header().Set("x-amz-mp-parts-count", strconv.Itoa(partCount))
	}
	s.serveReadResult(w, r, key, res, isOrigin)
	return nil
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	ing, err := prepareIngest(r)
	if err != nil {
		return err
	}
	opts, in := ing.opts, ing.in
	opts.ContentEncoding = r.Header.Get("Content-Encoding")
	body := io.Reader(r.Body)
	if len(in.h) > 0 {
		body = &integrityReader{r: r.Body, in: in}
	}
	var checks []store.Checksum
	etag, err := s.writeBackendFor(r.Context(), bucket).Put(r.Context(), bucket, key, body, r.ContentLength, opts,
		func() ([]store.Checksum, error) {
			got, err := in.verify(r.Trailer)
			if err == nil {
				checks = got
			}
			return got, err
		})
	if err != nil {
		return err
	}
	w.Header().Set("ETag", quoteETag(etag))
	setChecksumHeaders(w, checks)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if r.URL.Query().Has("versionId") || r.Header.Get("If-Match") != "" ||
		r.Header.Get("x-amz-if-match-size") != "" || r.Header.Get("x-amz-if-match-last-modified-time") != "" {
		return errNotImplemented()
	}
	if err := s.writeBackendFor(r.Context(), bucket).Delete(r.Context(), bucket, key); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) error {
	if r.Header.Get("x-amz-bypass-governance-retention") != "" {
		return errNotImplemented()
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return bodyError(err)
	}
	if want := r.Header.Get("Content-MD5"); want != "" {
		sum := md5.Sum(body)
		if base64.StdEncoding.EncodeToString(sum[:]) != want {
			return apiErr("BadDigest", "The Content-MD5 you specified did not match the request body.", http.StatusBadRequest)
		}
	}
	var req deleteRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		return errInvalidRequest("malformed Delete XML")
	}
	if len(req.Objects) == 0 || len(req.Objects) > 1000 {
		return errInvalidRequest("Delete must contain between 1 and 1000 objects")
	}
	keys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		if o.Key == "" || len(o.Key) > 1024 {
			return errInvalidRequest("invalid object key")
		}
		keys = append(keys, o.Key)
	}
	out := deleteResult{Xmlns: s3Namespace}
	outcomes, err := s.writeBackendFor(r.Context(), bucket).DeleteMany(r.Context(), bucket, keys)
	if err != nil {
		return err
	}
	for _, res := range outcomes {
		if res.Err != nil {
			s.log.Warn("delete failed", "bucket", bucket, "key", res.Key, "err", res.Err)
			out.Errors = append(out.Errors, deleteErrXML{
				Key: res.Key, Code: "InternalError",
				Message: "The object could not be deleted.",
			})
			continue
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, deletedXML{Key: res.Key})
		}
	}
	writeXML(w, http.StatusOK, out)
	return nil
}

func setObjectInfoHeaders(w http.ResponseWriter, info store.ObjectInfo, key string) {
	ct := info.ContentType
	if ct == "" || ct == "application/octet-stream" || ct == "application/x-www-form-urlencoded" {
		if inferred := mime.TypeByExtension(path.Ext(key)); inferred != "" {
			ct = inferred
		}
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.Header().Set("Content-Type", ct)
	if info.ChecksumName != "" && info.ChecksumValue != "" {
		w.Header().Set(checksumHeaderName(info.ChecksumName), info.ChecksumValue)
	}
	if info.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", info.ContentEncoding)
	}
	if !info.LastModified().IsZero() {
		w.Header().Set("Last-Modified", info.LastModified().UTC().Format(http.TimeFormat))
	}
	for k, vs := range metadataHeader(info.UserMetadata) {
		for _, v := range vs {
			w.Header().Set(k, v)
		}
	}
}

func setReadHeaders(w http.ResponseWriter, res *store.ReadResult, key string, cacheHeader bool) {
	info := res.ObjectInfo
	if res.Ranged {

		info.ChecksumName, info.ChecksumValue, info.ChecksumType = "", "", ""
	}
	setObjectInfoHeaders(w, info, key)
	if cacheHeader {
		if res.Hit {
			w.Header().Set("X-Cache", "HIT")
		} else {
			w.Header().Set("X-Cache", "MISS")
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(res.ContentLength(), 10))
}

func (s *Server) serveReadResult(w http.ResponseWriter, r *http.Request, key string, res *store.ReadResult, cacheHeader bool) {
	setReadHeaders(w, res, key, cacheHeader)
	if res.Ranged {
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(res.Start, 10)+"-"+
			strconv.FormatInt(res.End, 10)+"/"+strconv.FormatInt(res.Size, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	if r.Method == http.MethodHead {
		return
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(w, res.Body, buf); err != nil {
		s.log.Debug("GET body interrupted", "key", key, "err", err)
	}
}

func checksumHeaderName(name string) string {
	lower := strings.ToLower(name)
	if strings.HasPrefix(lower, "x-amz-checksum-") {
		return lower
	}
	return "x-amz-checksum-" + lower
}

func storedChecksumXML(name, value, typ string) *checksumXML {
	if name == "" || value == "" {
		return nil
	}
	out := &checksumXML{Type: typ}
	if out.Type == "" {
		out.Type = "FULL_OBJECT"
	}
	switch strings.TrimPrefix(strings.ToLower(name), "x-amz-checksum-") {
	case "crc32":
		out.CRC32 = value
	case "crc32c":
		out.CRC32C = value
	case "crc64nvme":
		out.CRC64NVME = value
	case "sha1":
		out.SHA1 = value
	case "sha256":
		out.SHA256 = value
	case "sha512":
		out.SHA512 = value
	default:
		return nil
	}
	return out
}

func (s *Server) createMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if err := validateWriteHeaders(r); err != nil {
		return err
	}
	meta, err := parseMetadata(r.Header)
	if err != nil {
		return err
	}
	ct, err := contentType(r.Header.Get("Content-Type"))
	if err != nil {
		return err
	}
	var alg *string
	if algorithm := r.Header.Get("x-amz-checksum-algorithm"); algorithm != "" {
		alg = &algorithm
	}
	id, err := s.multipartBackendFor(r.Context(), bucket).CreateUpload(r.Context(), bucket, key, meta, ct, r.Header.Get("Content-Encoding"), alg)
	if err != nil {
		return err
	}
	writeXML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: id,
	})
	return nil
}

func (s *Server) localBucketExists(ctx context.Context, bucket string) (bool, error) {
	if s.store == nil || s.originFor(ctx, bucket) != nil {
		return true, nil
	}
	return s.store.BucketExists(ctx, bucket)
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	if err := validateWriteHeaders(r); err != nil {
		return err
	}
	partNo, err := strconv.ParseInt(q.Get("partNumber"), 10, 32)
	if err != nil || partNo < 1 || partNo > 10000 {
		return errInvalidPart()
	}
	in, err := newIntegrity(r.Header)
	if err != nil {
		return err
	}
	expectedSize := r.ContentLength
	if v := r.Header.Get("x-amz-decoded-content-length"); v != "" {
		if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n >= 0 {
			expectedSize = n
		}
	}
	writer, err := s.multipartBackendFor(r.Context(), bucket).StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo), expectedSize)
	if err != nil {
		return err
	}
	body := io.ReadCloser(r.Body)
	if len(in.h) > 0 {
		body = &integrityReader{r: body, in: in}
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, body, buf); err != nil {
		writer.Abort()
		return bodyError(err)
	}
	checks, err := in.verify(r.Trailer)
	if err != nil {
		writer.Abort()
		return err
	}
	pub, err := writer.Finish(checks)
	if err != nil {
		return err
	}
	w.Header().Set("ETag", quoteETag(pub.ETag))
	if pub.Checksum != nil {
		w.Header().Set(pub.Checksum.Name, pub.Checksum.Value)
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) completeMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	exists, err := s.localBucketExists(r.Context(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errNoSuchBucket()
	}
	if err := validateWriteHeaders(r); err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return bodyError(err)
	}
	var req completeMultipartUploadRequest
	if err := xml.Unmarshal(body, &req); err != nil {
		return errInvalidRequest("malformed CompleteMultipartUpload XML")
	}
	precondition, err := putCondition(r)
	if err != nil {
		return err
	}
	listed := make([]store.ListedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		if p.PartNumber < 1 || p.PartNumber > 10000 || p.ETag == "" {
			return errInvalidPart()
		}
		var checks []store.Checksum
		for name, value := range map[string]string{
			"CRC32": p.ChecksumCRC32, "CRC32C": p.ChecksumCRC32C, "CRC64NVME": p.ChecksumCRC64NVME,
			"SHA1": p.ChecksumSHA1, "SHA256": p.ChecksumSHA256,
		} {
			if value != "" {
				checks = append(checks, store.Checksum{Name: name, Value: value})
			}
		}
		if len(checks) > 1 {
			return errInvalidRequest("a part may carry only one checksum")
		}
		var cs *store.Checksum
		if len(checks) == 1 {
			cs = &checks[0]
		}
		listed = append(listed, store.ListedPart{PartNo: int32(p.PartNumber), ETag: stripETag(p.ETag), Checksum: cs})
	}
	done, err := s.multipartBackendFor(r.Context(), bucket).CompleteUpload(r.Context(), q.Get("uploadId"), bucket, key, listed, store.WriteOptions{Precondition: precondition})
	if err != nil {
		return err
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	writeXML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns: s3Namespace, Location: fmt.Sprintf("%s://%s/%s/%s", scheme, r.Host, done.Bucket, done.Key),
		Bucket: done.Bucket, Key: done.Key, ETag: quoteETag(done.ETag),
	})
	return nil
}

func (s *Server) abortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	exists, err := s.localBucketExists(r.Context(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errNoSuchBucket()
	}
	ok, err := s.multipartBackendFor(r.Context(), bucket).AbortUpload(r.Context(), q.Get("uploadId"), bucket, key)
	if err != nil {
		return err
	}
	if !ok {
		return errNoSuchUpload()
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if err := validateWriteHeaders(r); err != nil {
		return err
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"), bucket)
	if err != nil {
		return errInvalidRequest("invalid x-amz-copy-source")
	}
	replace := strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE")
	contentEncoding := r.Header.Get("Content-Encoding")
	var meta []string
	var ct string
	if replace {
		meta, err = parseMetadata(r.Header)
		if err != nil {
			return err
		}
		ct, err = contentType(r.Header.Get("Content-Type"))
		if err != nil {
			return err
		}
	}
	dest, err := putCondition(r)
	if err != nil {
		return err
	}
	copyOpts := store.CopyOptions{
		ReplaceMetadata: replace, UserMetadata: meta, ContentType: ct, ContentEncoding: contentEncoding,
		Source: copySourceCondition(r), Dest: dest,
	}
	srcTier := s.originFor(r.Context(), srcBucket)
	dstTier := s.originFor(r.Context(), bucket)
	if srcTier == nil && dstTier == nil {
		out, err := s.writeBackendFor(r.Context(), bucket).Copy(r.Context(), srcBucket, srcKey, bucket, key, copyOpts)
		if err != nil {
			return err
		}
		writeCopyObject(w, out)
		return nil
	}
	if srcTier != nil && srcTier == dstTier {
		out, err := dstTier.Copy(r.Context(), srcBucket, srcKey, bucket, key, copyOpts)
		if err != nil {
			return err
		}
		writeCopyObject(w, out)
		return nil
	}
	src, err := s.backendFor(r.Context(), srcBucket).Open(r.Context(), srcBucket, srcKey, store.WholeObjectRange())
	if err != nil {
		return err
	}
	defer src.Body.Close()
	if !copySourceCondition(r).Allows(true, src.ETag) {
		return errPreconditionFailed()
	}
	if !replace {
		meta, ct, contentEncoding = src.UserMetadata, src.ContentType, src.ContentEncoding
	}
	etag, err := s.writeBackendFor(r.Context(), bucket).Put(r.Context(), bucket, key, src.Body,
		src.ContentLength(), store.WriteOptions{Precondition: dest, UserMetadata: meta, ContentType: ct, ContentEncoding: contentEncoding},
		func() ([]store.Checksum, error) { return nil, nil })
	if err != nil {
		return err
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns: s3Namespace, ETag: quoteETag(etag), LastModified: isoTime(time.Now()),
	})
	return nil
}

func writeCopyObject(w http.ResponseWriter, out *store.Meta) {
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns: s3Namespace, ETag: quoteETag(out.ETag), LastModified: isoTime(out.LastModified()),
	})
}

func parseCopySource(v, defaultBucket string) (string, string, error) {
	if v == "" {
		return "", "", errInvalidRequest("missing x-amz-copy-source")
	}
	decoded, err := url.PathUnescape(v)
	if err != nil {
		return "", "", err
	}
	if i := strings.IndexByte(decoded, '?'); i >= 0 {
		decoded = decoded[:i]
	}
	decoded = strings.TrimPrefix(decoded, "/")
	bucket, key, ok := strings.Cut(decoded, "/")
	if !ok || key == "" {
		return defaultBucket, decoded, nil
	}
	return bucket, key, nil
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

func (s *Server) tryFastPartCopy(w http.ResponseWriter, r *http.Request, bucket, key, srcBucket, srcKey, uploadID string, partNo int32) (bool, error) {
	if r.Header.Get("x-amz-copy-source-range") != "" ||
		copySourceCondition(r) != (store.ReadCondition{}) ||
		s.originFor(r.Context(), srcBucket) != nil {
		return false, nil
	}
	pub, ok, err := s.store.CopyPart(r.Context(), uploadID,
		store.ObjectRef{NS: ns.BucketRef(bucket), Key: key},
		store.ObjectRef{NS: ns.BucketRef(srcBucket), Key: srcKey}, partNo)
	if err != nil {
		return true, err
	}
	if !ok {
		return false, nil
	}
	w.Header().Set("ETag", quoteETag(pub.ETag))
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns: s3Namespace, ETag: quoteETag(pub.ETag), LastModified: isoTime(time.Now()),
	})
	return true, nil
}

func (s *Server) uploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	partNo, err := strconv.ParseInt(q.Get("partNumber"), 10, 32)
	if err != nil || partNo < 1 || partNo > 10000 {
		return errInvalidPart()
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"), bucket)
	if err != nil {
		return errInvalidRequest("invalid x-amz-copy-source")
	}
	backend := s.multipartBackendFor(r.Context(), bucket)
	if _, isOrigin := backend.(originBackend); !isOrigin {
		fast, err := s.tryFastPartCopy(w, r, bucket, key, srcBucket, srcKey, q.Get("uploadId"), int32(partNo))
		if err != nil {
			return err
		}
		if fast {
			return nil
		}
	}
	body, expectedSize, err := s.openPartCopySource(r, srcBucket, srcKey)
	if err != nil {
		return err
	}
	defer body.Close()
	writer, err := backend.StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo), expectedSize)
	if err != nil {
		return err
	}
	algorithm := writer.ChecksumAlgorithm()
	src, hasher, err := partCopyReader(body, algorithm)
	if err != nil {
		writer.Abort()
		return err
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, src, buf); err != nil {
		writer.Abort()
		return bodyError(err)
	}
	pub, err := writer.Finish(partCopyChecksums(algorithm, hasher))
	if err != nil {
		return err
	}
	w.Header().Set("ETag", quoteETag(pub.ETag))
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns: s3Namespace, ETag: quoteETag(pub.ETag), LastModified: isoTime(time.Now()),
	})
	return nil
}

func (s *Server) openPartCopySource(r *http.Request, srcBucket, srcKey string) (io.ReadCloser, int64, error) {
	rng := store.WholeObjectRange()
	if v := r.Header.Get("x-amz-copy-source-range"); v != "" {
		var err error
		rng, err = parseRange(v)
		if err != nil || rng.Last < 0 {
			return nil, 0, errInvalidRange()
		}
	}
	res, err := s.backendFor(r.Context(), srcBucket).Open(r.Context(), srcBucket, srcKey, rng)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			return nil, 0, errNoSuchKey()
		case errors.Is(err, store.ErrInvalidRange):
			return nil, 0, errInvalidRange()
		default:
			return nil, 0, errInternal(err, s.log)
		}
	}
	cond := copySourceCondition(r)
	if !cond.Allows(true, res.ETag) {
		_ = res.Body.Close()
		return nil, 0, errPreconditionFailed()
	}
	return res.Body, res.ContentLength(), nil
}

func partCopyReader(body io.Reader, algorithm string) (io.Reader, hash.Hash, error) {
	if algorithm == "" {
		return body, nil, nil
	}
	hasher, _, ok := algorithmHasher(algorithm)
	if !ok {
		return nil, nil, errNotImplemented()
	}
	return io.TeeReader(body, hasher), hasher, nil
}

func partCopyChecksums(algorithm string, hasher hash.Hash) []store.Checksum {
	if hasher == nil {
		return nil
	}
	return []store.Checksum{{
		Name:  "x-amz-checksum-" + strings.ToLower(algorithm),
		Value: base64.StdEncoding.EncodeToString(hasher.Sum(nil)),
	}}
}

func (s *Server) getObjectAttributes(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	requested := map[string]bool{}
	for a := range strings.SplitSeq(r.Header.Get("x-amz-object-attributes"), ",") {
		a = strings.TrimSpace(a)
		if a != "" {
			requested[a] = true
		}
	}
	if len(requested) == 0 {
		return errInvalidRequest("x-amz-object-attributes is required")
	}
	backend := s.backendFor(r.Context(), bucket)
	if backendIsOrigin(backend) {
		head, err := backend.Head(r.Context(), bucket, key)
		if err != nil {
			return err
		}
		if requested["Checksum"] || (requested["ObjectParts"] && head.Parts != nil) {
			return errNotImplemented()
		}
		out := getObjectAttributesResponse{Xmlns: s3Namespace}
		if requested["ETag"] {
			out.ETag = quoteETag(head.Info.ETag)
		}
		if requested["ObjectSize"] {
			size := head.Info.Size
			out.ObjectSize = &size
		}
		if requested["StorageClass"] {
			out.StorageClass = "STANDARD"
		}
		w.Header().Set("Last-Modified", head.Info.LastModified().UTC().Format(http.TimeFormat))
		writeXML(w, http.StatusOK, out)
		return nil
	}
	meta, parts, err := s.store.Attributes(r.Context(), store.ObjectRef{NS: ns.BucketRef(bucket), Key: key})
	if err != nil {
		return err
	}
	maxParts := 1000
	if v := r.Header.Get("x-amz-max-parts"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid x-amz-max-parts")
		}
		maxParts = min(max(n, 1), 1000)
	}
	marker := 0
	if v := r.Header.Get("x-amz-part-number-marker"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid x-amz-part-number-marker")
		}
		marker = n
	}
	out := getObjectAttributesResponse{Xmlns: s3Namespace}
	if requested["ETag"] {
		out.ETag = quoteETag(meta.ETag)
	}
	if requested["ObjectSize"] {
		size := meta.Size
		out.ObjectSize = &size
	}
	if requested["StorageClass"] {
		out.StorageClass = "STANDARD"
	}
	if requested["Checksum"] {

		out.Checksum = storedChecksumXML(meta.ChecksumName, meta.ChecksumValue, meta.ChecksumType)
	}
	if requested["ObjectParts"] && meta.Parts != nil {
		op := &objectPartsXML{MaxParts: maxParts, PartNumberMarker: marker, PartsCount: len(parts)}
		for _, p := range parts {
			if int(p.Number) <= marker {
				continue
			}
			if len(op.Parts) >= maxParts {
				op.IsTruncated = true
				op.NextPartNumberMarker = int(op.Parts[len(op.Parts)-1].PartNumber)
				break
			}
			part := partXML{PartNumber: int(p.Number), Size: p.Size}
			setPartChecksum(&part, p.Checksum)
			op.Parts = append(op.Parts, part)
		}
		out.ObjectParts = op
	}
	w.Header().Set("Last-Modified", meta.LastModified().UTC().Format(http.TimeFormat))
	writeXML(w, http.StatusOK, out)
	return nil
}

func (s *Server) listParts(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	exists, err := s.localBucketExists(r.Context(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errNoSuchBucket()
	}
	uploadID := q.Get("uploadId")
	max := 1000
	if v := q.Get("max-parts"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid max-parts")
		}
		max = min(n, 1000)
	}
	marker := int32(0)
	if v := q.Get("part-number-marker"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid part-number-marker")
		}
		marker = int32(n)
	}
	parts, truncated, next, err := s.multipartBackendFor(r.Context(), bucket).ListParts(r.Context(), uploadID, bucket, key, max, marker)
	if err != nil {
		if errors.Is(err, store.ErrNoUpload) {
			return errNoSuchUpload()
		}
		return err
	}
	storageClass := "STANDARD"
	out := listPartsResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: uploadID,
		PartNumberMarker: int(marker), MaxParts: max, IsTruncated: truncated,
		StorageClass: storageClass,
		Initiator:    ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		Owner:        ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
	}
	if truncated {
		out.NextPartNumberMarker = int(next)
	}
	for _, p := range parts {
		part := partXML{
			PartNumber: int(p.Number), Size: p.Size, LastModified: isoTime(p.LastModified),
			ETag: quoteETag(p.ETag),
		}
		setPartChecksum(&part, p.Checksum)
		out.Parts = append(out.Parts, part)
	}
	writeXML(w, http.StatusOK, out)
	return nil
}

func setPartChecksum(part *partXML, c *store.Checksum) {
	if c == nil {
		return
	}
	switch c.Name {
	case "CRC32":
		part.ChecksumCRC32 = c.Value
	case "CRC32C":
		part.ChecksumCRC32C = c.Value
	case "CRC64NVME":
		part.ChecksumCRC64NVME = c.Value
	case "SHA1":
		part.ChecksumSHA1 = c.Value
	case "SHA256":
		part.ChecksumSHA256 = c.Value
	}
}

func (s *Server) listMultipartUploads(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	exists, err := s.localBucketExists(r.Context(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errNoSuchBucket()
	}
	prefix := q.Get("prefix")
	max := 1000
	if v := q.Get("max-uploads"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return errInvalidRequest("invalid max-uploads")
		}
		max = min(n, 1000)
	}
	keyMarker := q.Get("key-marker")
	uploadMarker := q.Get("upload-id-marker")
	uploads, truncated, err := s.multipartBackendFor(r.Context(), bucket).ListUploads(r.Context(), bucket, prefix, prefixEnd(prefix), keyMarker, uploadMarker, max)
	if err != nil {
		return err
	}
	storageClass := "STANDARD"
	delimiter := q.Get("delimiter")
	out := listMultipartUploadsResult{
		Xmlns: s3Namespace, Bucket: bucket, Prefix: prefix, KeyMarker: keyMarker,
		UploadIDMarker: uploadMarker, MaxUploads: max, IsTruncated: truncated,
		Delimiter: delimiter,
	}
	seen := map[string]bool{}
	if delimiter != "" {
		if rest, ok := strings.CutPrefix(keyMarker, prefix); ok {
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				seen[prefix+rest[:idx+len(delimiter)]] = true
			}
		}
	}
	lastKey, lastID := keyMarker, uploadMarker
	for _, u := range uploads {
		if delimiter != "" {
			rest := strings.TrimPrefix(u.Key, prefix)
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				cp := prefix + rest[:idx+len(delimiter)]
				if !seen[cp] {
					if len(out.Uploads)+len(out.CommonPrefixes) >= max {
						out.IsTruncated = true
						break
					}
					seen[cp] = true
					out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: cp})
					lastKey, lastID = u.Key, u.UploadID
				}
				continue
			}
		}
		if len(out.Uploads)+len(out.CommonPrefixes) >= max {
			out.IsTruncated = true
			break
		}
		out.Uploads = append(out.Uploads, uploadXML{
			Key: u.Key, UploadID: u.UploadID, StorageClass: storageClass, Initiated: isoTime(u.Initiated),
			Initiator: ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
			Owner:     ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		})
		lastKey, lastID = u.Key, u.UploadID
	}
	if out.IsTruncated {
		out.NextKeyMarker, out.NextUploadIDMarker = lastKey, lastID
	}
	writeXML(w, http.StatusOK, out)
	return nil
}

const listPageSize = 1024

type listRequest struct {
	prefix            string
	delimiter         string
	maxKeys           int
	startAfter        string
	continuationToken string
	after             string
	encodeURL         bool
}

type listEntry struct {
	key          string
	lastModified time.Time
	etag         string
	size         int64
}

type listPage struct {
	contents       []listEntry
	commonPrefixes []string
	truncated      bool
	nextToken      string
}

type listSource func(after string, limit int) ([]store.Listed, error)

func parseListRequest(q url.Values) (listRequest, error) {
	req := listRequest{
		prefix:            q.Get("prefix"),
		delimiter:         q.Get("delimiter"),
		maxKeys:           1000,
		startAfter:        q.Get("start-after"),
		continuationToken: q.Get("continuation-token"),
	}
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return listRequest{}, errInvalidRequest("invalid max-keys")
		}
		req.maxKeys = min(n, 1000)
	}
	switch q.Get("encoding-type") {
	case "":
	case "url":
		req.encodeURL = true
	default:
		return listRequest{}, errInvalidRequest("unsupported encoding-type")
	}
	req.after = req.continuationToken
	if req.after == "" {
		req.after = req.startAfter
	}
	return req, nil
}

func iterListed(source listSource, after string) iter.Seq2[store.Listed, error] {
	return func(yield func(store.Listed, error) bool) {
		for {
			rows, err := source(after, listPageSize)
			if err != nil {
				yield(store.Listed{}, err)
				return
			}
			if len(rows) == 0 {
				return
			}
			for _, row := range rows {
				if !yield(row, nil) {
					return
				}
				after = row.Key
			}
			if len(rows) < listPageSize {
				return
			}
		}
	}
}

func virtualPrefix(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	idx := strings.Index(rest, delimiter)
	if idx < 0 {
		return "", false
	}
	return prefix + rest[:idx+len(delimiter)], true
}

func foldList(req listRequest, rows iter.Seq2[store.Listed, error]) (listPage, error) {
	page := listPage{}
	if req.maxKeys <= 0 {
		return page, nil
	}
	seen := make(map[string]struct{})
	if cp, ok := virtualPrefix(req.after, req.prefix, req.delimiter); ok {
		seen[cp] = struct{}{}
	}
	after := req.after
	for row, err := range rows {
		if err != nil {
			return listPage{}, err
		}
		if cp, ok := virtualPrefix(row.Key, req.prefix, req.delimiter); ok {
			if _, dup := seen[cp]; dup {
				after = row.Key
				continue
			}
			if len(page.contents)+len(page.commonPrefixes) >= req.maxKeys {
				page.truncated = true
				break
			}
			seen[cp] = struct{}{}
			page.commonPrefixes = append(page.commonPrefixes, cp)
			after = row.Key
			continue
		}
		if len(page.contents)+len(page.commonPrefixes) >= req.maxKeys {
			page.truncated = true
			break
		}
		page.contents = append(page.contents, listEntry{
			key: row.Key, lastModified: row.CreatedAt, etag: row.ETag, size: row.Size,
		})
		after = row.Key
	}
	if page.truncated {
		page.nextToken = after
	}
	return page, nil
}

func (req listRequest) listView(bucket string, page listPage) listBucketResult {
	out := listBucketResult{
		Xmlns:             s3Namespace,
		Name:              bucket,
		Prefix:            req.display(req.prefix),
		KeyCount:          len(page.contents) + len(page.commonPrefixes),
		MaxKeys:           req.maxKeys,
		IsTruncated:       page.truncated,
		ContinuationToken: req.continuationToken,
		StartAfter:        req.display(req.startAfter),
	}
	if req.delimiter != "" {
		out.Delimiter = req.display(req.delimiter)
	}
	if req.encodeURL {
		out.EncodingType = "url"
	}
	if page.truncated {
		out.NextContinuationToken = page.nextToken
	}
	for _, e := range page.contents {
		out.Contents = append(out.Contents, listObjectXML{
			Key: req.display(e.key), LastModified: e.lastModified, ETag: e.etag, Size: e.size,
		})
	}
	for _, cp := range page.commonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: req.display(cp)})
	}
	return out
}

func (req listRequest) display(s string) string {
	if req.encodeURL {
		return awsEncode(s, true)
	}
	return s
}

func (s *Server) newListSource(ctx context.Context, bucket string, req listRequest) listSource {
	backend := s.backendFor(ctx, bucket)
	return func(after string, limit int) ([]store.Listed, error) {
		return backend.List(ctx, bucket, req.prefix, after, limit)
	}
}

func (s *Server) listObjectsV2(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	req, err := parseListRequest(q)
	if err != nil {
		return err
	}
	page, err := foldList(req, iterListed(s.newListSource(r.Context(), bucket, req), req.after))
	if err != nil {
		return err
	}
	writeListObjectsV2XML(w, http.StatusOK, req.listView(bucket, page))
	return nil
}

func (s *Server) listObjectVersions(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	maxKeys, prefix, keyMarker, apiErr := versionParams(q)
	if apiErr != nil {
		return apiErr
	}
	exists, err := s.store.BucketExists(r.Context(), bucket)
	if err != nil {
		return err
	}
	if !exists {
		return errNoSuchBucket()
	}
	rows, err := s.store.List(r.Context(), ns.BucketRef(bucket), prefix, prefixEnd(prefix), keyMarker, maxKeys+1)
	if err != nil {
		return err
	}
	s.writeVersionList(w, bucket, q, maxKeys, prefix, keyMarker, rows)
	return nil
}

func (s *Server) listOriginObjectVersions(w http.ResponseWriter, r *http.Request, bucket string, q url.Values) error {
	backend := s.backendFor(r.Context(), bucket)
	maxKeys, prefix, keyMarker, apiErr := versionParams(q)
	if apiErr != nil {
		return apiErr
	}
	rows, err := backend.List(r.Context(), bucket, prefix, keyMarker, maxKeys+1)
	if err != nil {
		return err
	}
	s.writeVersionList(w, bucket, q, maxKeys, prefix, keyMarker, rows)
	return nil
}

func versionParams(q url.Values) (maxKeys int, prefix, keyMarker string, err *APIError) {
	if q.Get("delimiter") != "" {
		return 0, "", "", errNotImplemented()
	}
	maxKeys = 1000
	if v := q.Get("max-keys"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 0 {
			return 0, "", "", errInvalidRequest("invalid max-keys")
		}
		maxKeys = min(n, 1000)
	}
	return maxKeys, q.Get("prefix"), q.Get("key-marker"), nil
}

func (s *Server) writeVersionList(w http.ResponseWriter, bucket string, q url.Values,
	maxKeys int, prefix, keyMarker string, rows []store.Listed) {
	truncated := len(rows) > maxKeys
	var nextKey string
	if truncated {
		rows = rows[:maxKeys]
		if len(rows) > 0 {
			nextKey = rows[len(rows)-1].Key
		}
	}
	out := listVersionsResult{
		Xmlns:           s3Namespace,
		Name:            bucket,
		Prefix:          prefix,
		KeyMarker:       keyMarker,
		VersionIdMarker: q.Get("version-id-marker"),
		MaxKeys:         maxKeys,
		IsTruncated:     truncated,
	}
	for _, row := range rows {
		out.Versions = append(out.Versions, versionXML{
			Key:          row.Key,
			VersionId:    "null",
			IsLatest:     true,
			LastModified: isoTime(row.CreatedAt),
			ETag:         quoteETag(row.ETag),
			Size:         row.Size,
			StorageClass: "STANDARD",
			Owner:        ownerXML{ID: "pgvs3", DisplayName: "pgvs3"},
		})
	}
	if truncated {
		out.NextKeyMarker = nextKey
		out.NextVersionIdMarker = "null"
	}
	if q.Get("encoding-type") == "url" {
		out.EncodingType = "url"
		out.Prefix = awsEncode(out.Prefix, true)
		out.KeyMarker = awsEncode(out.KeyMarker, true)
		out.NextKeyMarker = awsEncode(out.NextKeyMarker, true)
		for i := range out.Versions {
			out.Versions[i].Key = awsEncode(out.Versions[i].Key, true)
		}
	}
	writeXML(w, http.StatusOK, out)
}

const CachePrefix = "/_cache/"

func (s *Server) serveCacheHTTP(w http.ResponseWriter, r *http.Request) error {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), CachePrefix)
	name, key, ok := strings.Cut(rest, "/")
	if !ok || key == "" {
		return errNoSuchKey()
	}
	unescaped, err := url.PathUnescape(key)
	if err != nil {
		return errInvalidRequest("invalid cache key")
	}
	key = unescaped
	if s.readOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		return errAccessDenied("gateway is read-only")
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		rng, err := parseRange(r.Header.Get("Range"))
		if err != nil {
			return errInvalidRange()
		}
		ref := store.ObjectRef{NS: ns.CacheRef(name), Key: key}
		cond := headerReadCondition(r)
		m, err := s.store.Meta(r.Context(), ref)
		if err != nil {
			return err
		}
		if store.IsDeletedOrigin(m.OriginETag) {
			return errNoSuchKey()
		}
		if err := cond.Check(true, m.ETag); err != nil {
			if errors.Is(err, store.ErrNotModified) {
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
			return errPreconditionFailed()
		}
		sm, body, err := s.store.GetBodyFromMeta(r.Context(), ref, m, rng)
		if err != nil {
			if errors.Is(err, store.ErrNotModified) {
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
			return err
		}
		defer body.Close()
		if rng.Requested && sm.Len() == 0 {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(sm.Size, 10))
			return errInvalidRange()
		}
		s.serveReadResult(w, r, key, &store.ReadResult{
			ObjectInfo: sm.ObjectInfo,
			Body:       body,
			Start:      sm.Start,
			End:        sm.End,
			Ranged:     rng.Requested,
		}, false)
		return nil

	case http.MethodPut:
		data, err := io.ReadAll(io.LimitReader(r.Body, int64(ns.Cache.InlineMax())+1))
		if err != nil {
			return bodyError(err)
		}
		obj, err := s.store.PutInline(r.Context(), store.ObjectRef{NS: ns.CacheRef(name), Key: key}, data, store.WriteOptions{
			ContentType: "application/octet-stream",
		})
		if err != nil {
			return err
		}
		w.Header().Set("ETag", quoteETag(obj.ETag))
		w.WriteHeader(http.StatusOK)
		return nil

	case http.MethodDelete:
		if _, err := s.store.Cache(name).Del(r.Context(), []string{key}); err != nil {
			return err
		}
		w.WriteHeader(http.StatusNoContent)
		return nil

	default:
		return errMethodNotAllowed()
	}
}
