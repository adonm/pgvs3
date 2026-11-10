package s3

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

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

func (s *Server) requireNativeBucket(ctx context.Context, bucket string) error {
	if s.store == nil || s.originFor(ctx, bucket) != nil {
		return nil
	}
	return s.requireCatalogBucket(ctx, bucket)
}

func (s *Server) requireOriginBucket(ctx context.Context, bucket string) error {
	if s.store == nil || s.originFor(ctx, bucket) == nil {
		return nil
	}
	return s.requireCatalogBucket(ctx, bucket)
}

func (s *Server) requireCatalogBucket(ctx context.Context, bucket string) error {
	ok, err := s.store.BucketExists(ctx, bucket)
	if err != nil {
		return err
	}
	if !ok {
		return errNoSuchBucket()
	}
	return nil
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	if err := validateWriteHeaders(r); err != nil {
		return err
	}
	partNo, err := strconv.ParseInt(q.Get("partNumber"), 10, 32)
	if err != nil || partNo < 1 || partNo > 10000 {
		return errInvalidPart()
	}
	body, err := prepareUploadBody(r)
	if err != nil {
		return err
	}
	writer, err := s.multipartBackendFor(r.Context(), bucket).StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo), body.expectedSize)
	if err != nil {
		return err
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, body.reader, buf); err != nil {
		writer.Abort()
		return bodyError(err)
	}
	checks, err := body.verify()
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
	if err := s.requireNativeBucket(r.Context(), bucket); err != nil {
		return err
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
	if err := s.requireNativeBucket(r.Context(), bucket); err != nil {
		return err
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
		return err
	}
	if srcBucket != bucket {
		if err := s.requireOriginBucket(r.Context(), srcBucket); err != nil {
			return err
		}
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
func (s *Server) listParts(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) error {
	if err := s.requireNativeBucket(r.Context(), bucket); err != nil {
		return err
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
	if err := s.requireNativeBucket(r.Context(), bucket); err != nil {
		return err
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
