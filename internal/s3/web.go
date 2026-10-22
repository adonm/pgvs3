package s3

import (
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/adonm/pgvs3/internal/store"
)

// WebHandler serves the same objects over plain HTTP for browsers, static
// websites and non-S3 clients. It is intentionally unauthenticated; bind it to
// a private interface or put it behind the service mesh.
func (s *Server) WebHandler(index, errorKey string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "ok\n")
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		bucket, key, err := splitPath(r, s.vhostDomain)
		if err != nil || bucket == "" {
			http.NotFound(w, r)
			return
		}
		if key == "" || strings.HasSuffix(key, "/") {
			key += index
		}
		cond := &store.ReadCondition{
			IfMatch: r.Header.Get("If-Match"), IfNoneMatch: r.Header.Get("If-None-Match"),
		}
		first, last, suffix, ranged, rerr := parseRange(r.Header.Get("Range"))
		if rerr != nil {
			http.Error(w, "invalid range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		sm, body, err := s.store.GetBody(r.Context(), bucket, key, first, last, suffix, cond)
		if errors.Is(err, store.ErrNotModified) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if errors.Is(err, store.ErrPrecondition) {
			http.Error(w, "precondition failed", http.StatusPreconditionFailed)
			return
		}
		if errors.Is(err, store.ErrInvalidRange) {
			w.Header().Set("Content-Range", "bytes */*")
			http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if err != nil {
			if errors.Is(err, store.ErrNotFound) && !strings.HasSuffix(key, "/") {
				if _, probe, perr := s.store.GetBody(r.Context(), bucket, key+"/"+index, 0, -1, -1, nil); perr == nil {
					_ = probe.Close()
					http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
					return
				}
			}
			if !errors.Is(err, store.ErrNotFound) {
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			}
			if errorKey != "" {
				if em, ebody, eerr := s.store.GetBody(r.Context(), bucket, errorKey, 0, -1, -1, nil); eerr == nil {
					defer ebody.Close()
					setWebHeaders(w, em.ContentType, errorKey, em)
					w.WriteHeader(http.StatusNotFound)
					if r.Method != http.MethodHead {
						_, _ = ebody.WriteTo(w)
					}
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		defer body.Close()
		ct := sm.ContentType
		if ct == "" || ct == "application/octet-stream" {
			if inferred := mime.TypeByExtension(path.Ext(key)); inferred != "" {
				ct = inferred
			}
		}
		if ct == "" {
			ct = "application/octet-stream"
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Content-Length", strconv.FormatInt(sm.Len(), 10))
		w.Header().Set("ETag", quoteETag(sm.ETag))
		w.Header().Set("Last-Modified", sm.CreatedAt.UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if ranged {
			w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(sm.Start, 10)+"-"+strconv.FormatInt(sm.End, 10)+"/"+strconv.FormatInt(sm.Size, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		if r.Method != http.MethodHead {
			_, _ = body.WriteTo(w)
		}
	})
}

func setWebHeaders(w http.ResponseWriter, ct, key string, m *store.SliceMeta) {
	if ct == "" || ct == "application/octet-stream" {
		if inferred := mime.TypeByExtension(path.Ext(key)); inferred != "" {
			ct = inferred
		}
	}
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", strconv.FormatInt(m.Size, 10))
	w.Header().Set("ETag", quoteETag(m.ETag))
	w.Header().Set("Last-Modified", m.CreatedAt.UTC().Format(http.TimeFormat))
}
