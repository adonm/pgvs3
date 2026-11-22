package s3

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/store"
)

type deleteRequest struct {
	Quiet   bool `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

func quoteETag(etag string) string { return `"` + etag + `"` }

func (s *Server) headObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if s.tier == nil {
		return errNoSuchBucket()
	}
	obj, err := s.tier.Head(r.Context(), bucket, key)
	if err != nil {
		return err
	}
	if err := checkReadPreconditions(r, obj.ETag, obj.LastModified); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(obj.ETag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}
	setReadHeaders(w, obj, true)
	w.WriteHeader(http.StatusOK)
	return nil
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, bucket, key string) error {
	if s.tier == nil {
		return errNoSuchBucket()
	}
	rng, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		return errInvalidRange()
	}
	obj, err := s.tier.Open(r.Context(), bucket, key, rng)
	if err != nil {
		if errors.Is(err, store.ErrInvalidRange) {
			if head, herr := s.tier.Head(r.Context(), bucket, key); herr == nil {
				w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(head.Size, 10))
			}
			return errInvalidRange()
		}
		return err
	}
	defer obj.Body.Close()

	if rng.Requested && obj.Size == 0 {
		w.Header().Set("Content-Range", "bytes */0")
		return errInvalidRange()
	}
	if err := checkReadPreconditions(r, obj.ETag, obj.LastModified); err != nil {
		if errors.Is(err, store.ErrNotModified) {
			w.Header().Set("ETag", quoteETag(obj.ETag))
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
		return err
	}

	s.serveReadResult(w, r, key, obj, true)
	return nil
}

func checkReadPreconditions(r *http.Request, etag string, lastModified time.Time) error {
	if err := readCondition(r, etag); err != nil {
		return err
	}
	if r.Header.Get("If-Match") == "" {
		if raw := r.Header.Get("If-Unmodified-Since"); raw != "" && !lastModified.IsZero() {
			if t, err := http.ParseTime(raw); err == nil {
				if lastModified.UTC().Truncate(time.Second).After(t.UTC().Truncate(time.Second)) {
					return errPreconditionFailed()
				}
			}
		}
	}
	if r.Header.Get("If-None-Match") == "" {
		if raw := r.Header.Get("If-Modified-Since"); raw != "" && !lastModified.IsZero() {
			if t, err := http.ParseTime(raw); err == nil {
				if !lastModified.UTC().Truncate(time.Second).After(t.UTC().Truncate(time.Second)) {
					return store.ErrNotModified
				}
			}
		}
	}
	return nil
}

func (s *Server) proxyToOrigin(w http.ResponseWriter, r *http.Request, bucket, key string, invalidateOnSuccess bool) error {
	if s.tier == nil {
		return errNoSuchBucket()
	}
	var body io.Reader
	var size int64
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		size = r.ContentLength
		if decoded, ok := r.Body.(*chunkedBody); ok {
			size = decoded.decodedLen
		}
		if size < 0 && r.Body != nil {
			spBody, spSize, clean, err := s.tier.SpoolStream(r.Context(), r.Body, -1)
			if err != nil {
				return err
			}
			defer clean()
			body, size = spBody, spSize
		} else if size > 0 {
			body = r.Body
		}
	}
	extra := forwardHeaders(r.Header)
	resp, err := s.tier.Proxy(r.Context(), bucket, key, r.Method, r.URL.Query(), body, size, extra...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if invalidateOnSuccess && key != "" && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.tier.Invalidate(r.Context(), bucket, key); err != nil {
			s.log.Warn("cache invalidation failed", "bucket", bucket, "key", key, "err", err)
		}
	}
	copyResponse(w, resp)
	return nil
}

func (s *Server) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) error {
	if s.tier == nil {
		return errNoSuchBucket()
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		return errInvalidRequest("unable to read delete request body")
	}
	var req deleteRequest
	if err := xml.Unmarshal(raw, &req); err != nil {
		return apiErr("MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", http.StatusBadRequest)
	}
	if len(req.Objects) == 0 || len(req.Objects) > 1000 {
		return apiErr("MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", http.StatusBadRequest)
	}
	keys := make([]string, 0, len(req.Objects))
	for _, o := range req.Objects {
		if o.Key == "" || len(o.Key) > 1024 {
			return errInvalidRequest("invalid object key")
		}
		keys = append(keys, o.Key)
	}
	extra := forwardHeaders(r.Header)
	resp, err := s.tier.Proxy(r.Context(), bucket, "", r.Method, r.URL.Query(), bytes.NewReader(raw), int64(len(raw)), extra...)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := s.tier.InvalidateMany(r.Context(), bucket, keys); err != nil {
			s.log.Warn("batch cache invalidation failed", "bucket", bucket, "count", len(keys), "err", err)
		}
	}
	copyResponse(w, resp)
	return nil
}

func forwardHeaders(h http.Header) [][2]string {
	var out [][2]string
	for name, vals := range h {
		if len(vals) == 0 {
			continue
		}
		lower := strings.ToLower(name)
		switch lower {
		case "host", "authorization", "connection", "transfer-encoding", "content-length",
			"x-amz-date", "x-amz-content-sha256", "x-amz-security-token", "x-amz-decoded-content-length",
			"expect", "accept-encoding":
			continue
		}
		out = append(out, [2]string{lower, vals[0]})
	}
	return out
}

func copyResponse(w http.ResponseWriter, resp *http.Response) {
	for name, vals := range resp.Header {
		if strings.EqualFold(name, "Connection") || strings.EqualFold(name, "Transfer-Encoding") {
			continue
		}
		for _, v := range vals {
			w.Header().Add(name, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	buf := copyBuf()
	defer putCopyBuf(buf)
	_, _ = io.CopyBuffer(w, resp.Body, buf)
}

func setObjectInfoHeaders(w http.ResponseWriter, info store.ObjectMeta) {
	ct := info.ContentType
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", quoteETag(info.ETag))
	w.Header().Set("Content-Type", ct)
	if info.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", info.ContentEncoding)
	}
	if !info.LastModified.IsZero() {
		w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
	}
	for i := 0; i+1 < len(info.UserMeta); i += 2 {
		k := info.UserMeta[i]
		if !strings.HasPrefix(k, "x-amz-meta-") {
			k = "x-amz-meta-" + k
		}
		w.Header().Set(k, info.UserMeta[i+1])
	}
}

func setReadHeaders(w http.ResponseWriter, res *store.ReadResult, cacheHeader bool) {
	setObjectInfoHeaders(w, res.ObjectMeta)
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
	setReadHeaders(w, res, cacheHeader)
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
