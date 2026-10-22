package s3

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	ETag         string   `xml:"ETag"`
	LastModified string   `xml:"LastModified"`
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, bucket, key string) {
	if err := validateSSE(r); err != nil {
		writeError(w, r, err, s.log)
		return
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"), bucket)
	if err != nil {
		writeError(w, r, errInvalidRequest("invalid x-amz-copy-source"), s.log)
		return
	}
	replace := strings.EqualFold(r.Header.Get("x-amz-metadata-directive"), "REPLACE")
	var meta []string
	var ct string
	if replace {
		meta, err = parseMetadata(r.Header)
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
		ct, err = contentType(r.Header.Get("Content-Type"))
		if err != nil {
			writeError(w, r, err, s.log)
			return
		}
	}
	out, err := s.store.Copy(r.Context(), srcBucket, srcKey, bucket, key, store.CopyOptions{
		ReplaceMetadata: replace, UserMetadata: meta, ContentType: ct,
		SourceIfMatch: r.Header.Get("x-amz-copy-source-if-match"),
		SourceIfNone:  r.Header.Get("x-amz-copy-source-if-none-match"),
		DestIfMatch:   r.Header.Get("If-Match"),
		DestIfNone:    r.Header.Get("If-None-Match"),
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			s.metaError(w, r, srcBucket, err)
		case errors.Is(err, store.ErrPrecondition):
			writeError(w, r, errPreconditionFailed(), s.log)
		default:
			writeError(w, r, errInternal(err, s.log), s.log)
		}
		return
	}
	writeXML(w, http.StatusOK, copyObjectResult{
		Xmlns: s3Namespace, ETag: quoteETag(out.ETag), LastModified: isoTime(out.CreatedAt),
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

func (s *Server) uploadPartCopy(w http.ResponseWriter, r *http.Request, bucket, key string, q url.Values) {
	partNo, err := strconv.ParseInt(q.Get("partNumber"), 10, 32)
	if err != nil || partNo < 1 || partNo > 10000 {
		writeError(w, r, errInvalidPart(), s.log)
		return
	}
	srcBucket, srcKey, err := parseCopySource(r.Header.Get("x-amz-copy-source"), bucket)
	if err != nil {
		writeError(w, r, errInvalidRequest("invalid x-amz-copy-source"), s.log)
		return
	}
	if r.Header.Get("x-amz-copy-source-range") == "" &&
		r.Header.Get("x-amz-copy-source-if-match") == "" &&
		r.Header.Get("x-amz-copy-source-if-none-match") == "" {
		pub, ok, err := s.store.CopyPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo), srcBucket, srcKey)
		if err != nil {
			if errors.Is(err, store.ErrNoUpload) {
				writeError(w, r, errNoSuchUpload(), s.log)
			} else if errors.Is(err, store.ErrNotFound) {
				writeError(w, r, errNoSuchKey(), s.log)
			} else {
				writeError(w, r, errInternal(err, s.log), s.log)
			}
			return
		}
		if ok {
			w.Header().Set("ETag", quoteETag(pub.ETag))
			writeXML(w, http.StatusOK, copyPartResult{
				Xmlns: s3Namespace, ETag: quoteETag(pub.ETag), LastModified: isoTime(time.Now()),
			})
			return
		}
	}
	first, last := int64(0), int64(-1)
	if v := r.Header.Get("x-amz-copy-source-range"); v != "" {
		f, l, _, _, err := parseRange(v)
		if err != nil || l < 0 {
			writeError(w, r, errInvalidRange(), s.log)
			return
		}
		first, last = f, l
	}
	_, body, err := s.store.GetBody(r.Context(), srcBucket, srcKey, first, last, -1, &store.ReadCondition{
		IfMatch: r.Header.Get("x-amz-copy-source-if-match"), IfNoneMatch: r.Header.Get("x-amz-copy-source-if-none-match"),
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotFound):
			writeError(w, r, errNoSuchKey(), s.log)
		case errors.Is(err, store.ErrPrecondition):
			writeError(w, r, errPreconditionFailed(), s.log)
		case errors.Is(err, store.ErrInvalidRange):
			writeError(w, r, errInvalidRange(), s.log)
		default:
			writeError(w, r, errInternal(err, s.log), s.log)
		}
		return
	}
	defer body.Close()
	writer, err := s.store.StartPart(r.Context(), q.Get("uploadId"), bucket, key, int32(partNo))
	if err != nil {
		if errors.Is(err, store.ErrNoUpload) {
			writeError(w, r, errNoSuchUpload(), s.log)
			return
		}
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	src := io.Reader(body)
	var checks []store.Checksum
	var hasher hash.Hash
	if alg := writer.ChecksumAlgorithm(); alg != "" {
		h, header, ok := algorithmHasher(alg)
		if !ok {
			writer.Abort()
			writeError(w, r, errNotImplemented(), s.log)
			return
		}
		hasher = h
		src = io.TeeReader(body, h)
		_ = header
	}
	buf := copyBuf()
	defer putCopyBuf(buf)
	if _, err := io.CopyBuffer(writer, src, buf); err != nil {
		writer.Abort()
		writeError(w, r, bodyError(err), s.log)
		return
	}
	if hasher != nil {
		checks = append(checks, store.Checksum{
			Name:  "x-amz-checksum-" + strings.ToLower(writer.ChecksumAlgorithm()),
			Value: base64.StdEncoding.EncodeToString(hasher.Sum(nil)),
		})
	}
	pub, err := writer.Finish(checks)
	if err != nil {
		writeError(w, r, s.ingestError(err), s.log)
		return
	}
	w.Header().Set("ETag", quoteETag(pub.ETag))
	writeXML(w, http.StatusOK, copyPartResult{
		Xmlns: s3Namespace, ETag: quoteETag(pub.ETag), LastModified: isoTime(time.Now()),
	})
}
