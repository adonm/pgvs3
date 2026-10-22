package s3

import (
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

// CachePrefix is the reserved URL prefix for reading cache namespaces over
// HTTP: /_cache/<namespace>/<key>.
const CachePrefix = "/_cache/"

// serveCacheHTTP exposes cache namespaces (the same rows RESP uses) over plain
// HTTP without mixing them into the S3 bucket namespace.
func (s *Server) serveCacheHTTP(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), CachePrefix)
	name, key, ok := strings.Cut(rest, "/")
	if !ok || key == "" {
		http.NotFound(w, r)
		return
	}
	unescaped, err := url.PathUnescape(key)
	if err != nil {
		writeError(w, r, errInvalidRequest("invalid cache key"), s.log)
		return
	}
	key = unescaped
	if s.readOnly && r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeError(w, r, errAccessDenied("gateway is read-only"), s.log)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		first, last, suffix, ranged, err := parseRange(r.Header.Get("Range"))
		if err != nil {
			writeError(w, r, errInvalidRange(), s.log)
			return
		}
		sm, body, err := s.store.GetBodyKind(r.Context(), ns.Cache, name, key, first, last, suffix,
			&store.ReadCondition{IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match")})
		if err != nil {
			switch {
			case errors.Is(err, store.ErrNotFound):
				writeError(w, r, errNoSuchKey(), s.log)
			case errors.Is(err, store.ErrNotModified):
				w.WriteHeader(http.StatusNotModified)
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
		setObjectHeaders(w, &store.Meta{
			Size: sm.Size, ETag: sm.ETag, ContentType: sm.ContentType, CreatedAt: sm.CreatedAt,
			FileID: sm.FileID, Parts: sm.Parts, PartEnds: sm.PartEnds,
		}, key)
		w.Header().Set("Content-Length", strconv.FormatInt(sm.Len(), 10))
		if ranged {
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(sm.Start, 10)+"-"+
				strconv.FormatInt(sm.End, 10)+"/"+strconv.FormatInt(sm.Size, 10))
			w.WriteHeader(http.StatusPartialContent)
		}
		if r.Method == http.MethodHead {
			return
		}
		buf := copyBuf()
		defer putCopyBuf(buf)
		_, _ = io.CopyBuffer(w, body, buf)

	case http.MethodPut:
		data, err := io.ReadAll(io.LimitReader(r.Body, int64(ns.Cache.InlineMax())+1))
		if err != nil {
			writeError(w, r, bodyError(err), s.log)
			return
		}
		obj, err := s.store.PutInlineKind(r.Context(), ns.Cache, name, key, data, store.Unconditional, "",
			nil, "application/octet-stream", nil, false, "")
		if err != nil {
			writeError(w, r, s.ingestError(err), s.log)
			return
		}
		w.Header().Set("ETag", quoteETag(obj.ETag))
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		if _, err := s.store.CacheDel(r.Context(), name, []string{key}); err != nil {
			writeError(w, r, errInternal(err, s.log), s.log)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		s.methodNotAllowed(w, r)
	}
}
