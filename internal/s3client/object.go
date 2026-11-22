package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type ObjectInfo struct {
	Key             string
	Size            int64
	ETag            string
	LastModified    time.Time
	ContentType     string
	ContentEncoding string
	UserMetadata    []string
}

func (b *Bucket) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	info, _, err := b.StatIfNoneMatch(ctx, key, "")
	return info, err
}

func (b *Bucket) StatIfNoneMatch(ctx context.Context, key, ifNoneMatch string) (info ObjectInfo, notModified bool, err error) {
	c := b.client
	if c == nil {
		return ObjectInfo{}, false, errors.New("s3: nil bucket")
	}
	object, err := c.objectKey(key)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	var extra [][2]string
	if ifNoneMatch != "" {
		extra = append(extra, [2]string{"if-none-match", `"` + strings.Trim(ifNoneMatch, `"`) + `"`})
	}
	resp, err := c.do(ctx, http.MethodHead, object, nil, nil, -1, extra...)
	if err != nil {
		return ObjectInfo{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return ObjectInfo{}, true, nil
	}
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, false, statusError("HEAD", key, resp)
	}
	if resp.ContentLength < 0 {
		return ObjectInfo{}, false, fmt.Errorf("s3: HEAD %s returned no Content-Length", key)
	}
	return objectInfoFromHeader(strings.TrimLeft(key, "/"), resp.Header, resp.ContentLength), false, nil
}

func (b *Bucket) Get(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	rc, _, err := b.GetWithInfo(ctx, key, offset, length, ifMatch)
	return rc, err
}

func (b *Bucket) GetWithInfo(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, ObjectInfo, error) {
	c := b.client
	if c == nil {
		return nil, ObjectInfo{}, errors.New("s3: nil bucket")
	}
	if offset < 0 {
		return nil, ObjectInfo{}, fmt.Errorf("s3: negative offset %d", offset)
	}
	if offset > 0 && length == 0 {
		return nil, ObjectInfo{}, errors.New("s3: ranged read requires positive length")
	}
	object, err := c.objectKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	var headers [][2]string
	wantStatus := http.StatusOK
	if offset > 0 || length > 0 {
		wantStatus = http.StatusPartialContent
		var rng string
		if length > 0 {
			rng = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
		} else {
			rng = fmt.Sprintf("bytes=%d-", offset)
		}
		headers = append(headers, [2]string{"range", rng})
	}
	if ifMatch != "" {
		headers = append(headers, [2]string{"if-match", `"` + strings.Trim(ifMatch, `"`) + `"`})
	}
	resp, err := c.do(ctx, http.MethodGet, object, nil, nil, -1, headers...)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if resp.StatusCode != wantStatus {
		defer resp.Body.Close()
		return nil, ObjectInfo{}, statusError("GET", key, resp)
	}
	size := resp.ContentLength
	if wantStatus == http.StatusPartialContent {
		if total, ok := parseContentRangeTotal(resp.Header.Get("Content-Range")); ok {
			size = total
		}
	}
	info := objectInfoFromHeader(strings.TrimLeft(key, "/"), resp.Header, size)
	return resp.Body, info, nil
}

func parseContentRangeTotal(v string) (int64, bool) {
	_, totalStr, ok := strings.Cut(v, "/")
	if !ok || totalStr == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(totalStr), 10, 64)
	return n, err == nil && n >= 0
}

func objectInfoFromHeader(key string, h http.Header, size int64) ObjectInfo {
	var mod time.Time
	if raw := h.Get("Last-Modified"); raw != "" {
		mod, _ = http.ParseTime(raw)
	}
	var meta []string
	for name, vals := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(vals) > 0 {
			meta = append(meta, lower, vals[0])
		}
	}
	return ObjectInfo{
		Key:             key,
		Size:            size,
		ETag:            strings.Trim(h.Get("ETag"), `"`),
		LastModified:    mod.UTC(),
		ContentType:     h.Get("Content-Type"),
		ContentEncoding: h.Get("Content-Encoding"),
		UserMetadata:    meta,
	}
}
