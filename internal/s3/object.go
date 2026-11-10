package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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

type requestBackendsKey struct{}

type bucketBackend struct {
	bucket  string
	origin  *objcache.Tier
	backend objectBackend
	release func()
}

type requestBackends struct {
	context context.Context
	entries []bucketBackend
}

func (s *Server) bindBackends(r *http.Request) (*http.Request, func()) {
	backends := &requestBackends{context: r.Context()}
	r = r.WithContext(context.WithValue(r.Context(), requestBackendsKey{}, backends))
	return r, func() {
		for _, entry := range backends.entries {
			if entry.release != nil {
				entry.release()
			}
		}
	}
}

func (s *Server) selectedBackend(ctx context.Context, bucket string) bucketBackend {
	backends, ok := ctx.Value(requestBackendsKey{}).(*requestBackends)
	if !ok {
		return bucketBackend{bucket: bucket, backend: localBackend{store: s.store}}
	}
	for _, entry := range backends.entries {
		if entry.bucket == bucket {
			return entry
		}
	}
	tier, release := s.origins.Acquire(backends.context, bucket)
	entry := bucketBackend{bucket: bucket, origin: tier, backend: localBackend{store: s.store}, release: release}
	if tier != nil {
		entry.backend = originBackend{Tier: tier}
	}
	backends.entries = append(backends.entries, entry)
	return entry
}

type readBackend interface {
	Head(ctx context.Context, bucket, key string) (readHead, error)
	Open(ctx context.Context, bucket, key string, rng store.ByteRange) (*store.ReadResult, error)
	List(ctx context.Context, bucket, prefix, after string, limit int) ([]store.Listed, error)
}

// conditionalReadBackend lets a backend reject a conditional GET before it
// opens a body. Backends that do not implement it are checked after Open.
type conditionalReadBackend interface {
	OpenConditional(ctx context.Context, bucket, key string, rng store.ByteRange, cond store.ReadCondition) (*store.ReadResult, error)
}

func openObject(ctx context.Context, backend readBackend, bucket, key string, rng store.ByteRange, cond store.ReadCondition) (*store.ReadResult, error) {
	if cb, ok := backend.(conditionalReadBackend); ok {
		return cb.OpenConditional(ctx, bucket, key, rng, cond)
	}
	return backend.Open(ctx, bucket, key, rng)
}

func (s *Server) backendFor(ctx context.Context, bucket string) readBackend {
	if s.readOverride != nil {
		return s.readOverride
	}
	return s.objectBackendFor(ctx, bucket)
}

func (s *Server) objectBackendFor(ctx context.Context, bucket string) objectBackend {
	return s.selectedBackend(ctx, bucket).backend
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
	return b.open(ctx, bucket, key, rng, nil)
}

// OpenConditional checks the read condition before any stream starts, so a
// 304 or 412 costs only the metadata query.
func (b localBackend) OpenConditional(ctx context.Context, bucket, key string, rng store.ByteRange, cond store.ReadCondition) (*store.ReadResult, error) {
	return b.open(ctx, bucket, key, rng, &cond)
}

func (b localBackend) open(ctx context.Context, bucket, key string, rng store.ByteRange, cond *store.ReadCondition) (*store.ReadResult, error) {
	sm, body, err := b.store.GetBody(ctx, store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, rng, cond)
	if err != nil {
		if sm != nil {
			return &store.ReadResult{ObjectInfo: sm.ObjectInfo, Start: sm.Start, End: sm.End, Ranged: rng.Requested}, err
		}
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
	return s.selectedBackend(ctx, bucket).origin
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
	var head readHead
	var err error
	zipResolved := false
	part, hasPart, err := parsePartNumber(r.URL.Query())
	if err != nil {
		return err
	}
	if !strings.HasSuffix(key, "/") {
		if archiveKey, _, _, isArchive := splitArchiveKey(key); isArchive {
			switch b := backend.(type) {
			case originBackend:
				info, cached, cachedErr := b.HeadCached(r.Context(), bucket, key)
				switch {
				case cachedErr == nil && cached:
					// A literal cached object shadows the archive member.
					head = readHead{Info: info, Hit: true}
					zipResolved = true
				case cachedErr == nil:
					// No literal object is cached. Resolve the archive index
					// without ever HEADing the non-existent member at origin.
					if idx, member, _, ok := s.resolveZipMember(r.Context(), backend, bucket, key); ok {
						if hasPart {
							return errNotImplemented()
						}
						return s.headZipMember(w, r, idx, member)
					}
				}
			case localBackend:
				if b.store != nil {
					var primary, archiveMeta *store.Meta
					primary, archiveMeta, err = b.store.MetaPair(r.Context(), store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, archiveKey, false)
					zipResolved = true
					switch {
					case err != nil:
					case primary != nil:
						head = readHead{Info: primary.ObjectInfo, Parts: primary.Parts, PartEnds: primary.PartEnds}
					case archiveMeta != nil:
						if idx, member, ok := s.resolveZipMemberWithMeta(r.Context(), backend, bucket, key, archiveMeta); ok {
							if hasPart {
								return errNotImplemented()
							}
							return s.headZipMember(w, r, idx, member)
						}
						err = store.ErrNotFound
					default:
						err = store.ErrNotFound
					}
				}
			}
		}
	}
	if !zipResolved {
		head, err = backend.Head(r.Context(), bucket, key)
		if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
			key += "index.html"
			head, err = backend.Head(r.Context(), bucket, key)
		}
	}
	if errors.Is(err, store.ErrNotFound) && !zipResolved {
		if idx, member, _, ok := s.resolveZipMember(r.Context(), backend, bucket, key); ok {
			if hasPart {
				return errNotImplemented()
			}
			return s.headZipMember(w, r, idx, member)
		}
	}
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
	cond := headerReadCondition(r)
	var res *store.ReadResult
	resolved := false
	if !hasPart && !strings.HasSuffix(key, "/") {
		if archiveKey, _, _, isArchive := splitArchiveKey(key); isArchive {
			switch b := backend.(type) {
			case localBackend:
				if b.store != nil {
					primary, archiveMeta, pairErr := b.store.MetaPair(r.Context(), store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, archiveKey, true)
					resolved = true
					switch {
					case pairErr != nil && !errors.Is(pairErr, store.ErrNotFound):
						err = pairErr
					case primary != nil:
						res = &store.ReadResult{ObjectInfo: primary.ObjectInfo}
						if err = cond.Check(true, primary.ETag); err == nil {
							sm, body, bodyErr := b.store.GetBodyFromMeta(r.Context(), store.ObjectRef{NS: ns.BucketRef(bucket), Key: key}, primary, rng)
							switch {
							case errors.Is(bodyErr, store.ErrInvalidRange):
								w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(primary.Size, 10))
								return errInvalidRange()
							case bodyErr != nil:
								err = bodyErr
							default:
								res = &store.ReadResult{ObjectInfo: sm.ObjectInfo, Body: body, Start: sm.Start, End: sm.End, Ranged: rng.Requested}
							}
						}
					case archiveMeta != nil:
						if idx, member, ok := s.resolveZipMemberWithMeta(r.Context(), backend, bucket, key, archiveMeta); ok {
							return s.serveZipMember(w, r, backend, bucket, key, idx, member, archiveMeta, rng)
						}
						err = store.ErrNotFound
					default:
						err = store.ErrNotFound
					}
				}
			case originBackend:
				cached, ok, cachedErr := b.OpenCached(r.Context(), bucket, key, rng)
				switch {
				case cachedErr == nil && ok:
					// A literal cached object shadows the archive member.
					resolved = true
					if condErr := cond.Check(true, cached.ETag); condErr != nil {
						cached.Body.Close()
						err = condErr
					} else {
						s.serveReadResult(w, r, key, cached, true)
						return nil
					}
				case cachedErr == nil:
					// No literal object is cached. Resolve the archive index
					// without ever GETing or HEADing the member at origin.
					if idx, member, archiveMeta, ok := s.resolveZipMember(r.Context(), backend, bucket, key); ok {
						return s.serveZipMember(w, r, backend, bucket, key, idx, member, archiveMeta, rng)
					}
				}
			}
		}
	}
	if !resolved {
		res, err = openObject(r.Context(), backend, bucket, key, rng, cond)
		if errors.Is(err, store.ErrNotFound) && strings.HasSuffix(key, "/") {
			key += "index.html"
			res, err = openObject(r.Context(), backend, bucket, key, rng, cond)
		}
		if errors.Is(err, store.ErrNotFound) {
			if idx, member, archiveMeta, ok := s.resolveZipMember(r.Context(), backend, bucket, key); ok {
				if hasPart {
					return errNotImplemented()
				}
				return s.serveZipMember(w, r, backend, bucket, key, idx, member, archiveMeta, rng)
			}
		}
	}
	if err != nil {
		if errors.Is(err, store.ErrNotModified) {
			etag := ""
			if res != nil {
				etag = res.ETag
			}
			w.Header().Set("ETag", quoteETag(etag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		if errors.Is(err, store.ErrPrecondition) {
			return errPreconditionFailed()
		}
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
	if _, conditional := backend.(conditionalReadBackend); !conditional {
		if err := readCondition(r, res.ETag); err != nil {
			if errors.Is(err, store.ErrNotModified) {
				w.Header().Set("ETag", quoteETag(res.ETag))
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
			return err
		}
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
	var checks []store.Checksum
	etag, err := s.writeBackendFor(r.Context(), bucket).Put(r.Context(), bucket, key, ing.body.reader, ing.body.size, ing.opts,
		func() ([]store.Checksum, error) {
			got, err := ing.body.verify()
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
	if wt, ok := res.Body.(io.WriterTo); ok {
		if _, err := wt.WriteTo(w); err != nil {
			s.log.Debug("GET body interrupted", "key", key, "err", err)
		}
		return
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
		return err
	}
	if srcBucket != bucket {
		if err := s.requireOriginBucket(r.Context(), srcBucket); err != nil {
			return err
		}
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
		return "", "", errInvalidRequest("invalid x-amz-copy-source")
	}
	if i := strings.IndexByte(decoded, '?'); i >= 0 {
		decoded = decoded[:i]
	}
	decoded = strings.TrimPrefix(decoded, "/")
	bucket, key, ok := strings.Cut(decoded, "/")
	if !ok || key == "" {
		bucket, key = defaultBucket, decoded
	}
	if err := validateBucketName(bucket); err != nil {
		return "", "", err
	}
	return bucket, key, nil
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
