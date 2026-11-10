package s3

import (
	"context"
	"errors"
	"io"
	"iter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

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
	source := s.newListSource(r.Context(), bucket, req)
	if archiveKey, _, ok := splitZipKey(req.prefix); ok {
		if idx, _, zipErr := s.loadZipIndex(r.Context(), s.backendFor(r.Context(), bucket), bucket, archiveKey); zipErr == nil {
			source = zipListSource(source, idx, archiveKey, req.prefix, req.delimiter)
		}
	}
	page, err := foldList(req, iterListed(source, req.after))
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
	if err := s.requireNativeBucket(r.Context(), bucket); err != nil {
		return err
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
				w.Header().Set("ETag", quoteETag(m.ETag))
				w.WriteHeader(http.StatusNotModified)
				return nil
			}
			return errPreconditionFailed()
		}
		if r.Method == http.MethodHead {
			span, err := rng.Resolve(m.Size)
			if err != nil || (rng.Requested && (m.Size == 0 || span.End < span.Start)) {
				w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(m.Size, 10))
				return errInvalidRange()
			}
			s.serveReadResult(w, r, key, &store.ReadResult{
				ObjectInfo: m.ObjectInfo,
				Start:      span.Start,
				End:        span.End,
				Ranged:     rng.Requested,
			}, false)
			return nil
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
