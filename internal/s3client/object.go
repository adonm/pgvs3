// Copyright 2023 Sneller, Inc.
// Copyright 2025 Roman Atachiants
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package s3client

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"math"
	"net/http"
	"net/url"
	"sort"
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

type PutOptions struct {
	UserMetadata    []string
	ContentType     string
	ContentEncoding string

	IfMatch string

	IfNoneMatch string
}

func (b *Bucket) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	c := b.client
	if c == nil {
		return ObjectInfo{}, errors.New("s3: nil bucket")
	}
	object, err := c.objectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	resp, err := c.do(ctx, http.MethodHead, object, nil, nil, -1)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ObjectInfo{}, statusError("HEAD", key, resp)
	}
	if resp.ContentLength < 0 {
		return ObjectInfo{}, fmt.Errorf("s3: HEAD %s returned no Content-Length", key)
	}
	return objectInfoFromHeader(strings.TrimLeft(key, "/"), resp.Header, resp.ContentLength), nil
}

func (b *Bucket) Walk(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		if b.client == nil {
			yield(ObjectInfo{}, errors.New("s3: nil bucket"))
			return
		}
		if ctx == nil {
			yield(ObjectInfo{}, errors.New("s3 request: nil context"))
			return
		}
		if err := ctx.Err(); err != nil {
			yield(ObjectInfo{}, err)
			return
		}
		logical := strings.TrimLeft(prefix, "/")
		if logical == "." {
			logical = ""
		}
		physical := b.client.listPrefix(logical)
		token, previous := "", ""
		for {
			page, err := b.client.listPage(ctx, physical, token)
			if err != nil {
				yield(ObjectInfo{}, err)
				return
			}
			for _, object := range page.Contents {
				key := object.Key
				if page.EncodingType == "url" {
					key, err = url.PathUnescape(key)
					if err != nil {
						yield(ObjectInfo{}, fmt.Errorf("s3: list %q: decoding key: %w", physical, err))
						return
					}
				}
				if strings.HasSuffix(key, "/") {
					continue
				}
				if physical != "" && !strings.HasPrefix(key, physical) {
					continue
				}
				logicalKey := b.client.logicalKey(key)
				if b.client.prefix != "" && logicalKey == key {
					continue
				}
				info := ObjectInfo{
					Key:          logicalKey,
					Size:         object.Size,
					ETag:         normalizeETag(object.ETag),
					LastModified: object.LastModified,
					ContentType:  object.ContentType,
				}
				if !yield(info, nil) {
					return
				}
			}
			if !page.IsTruncated {
				return
			}
			next := page.NextContinuationToken
			if next == "" || next == token || next == previous {
				yield(ObjectInfo{}, fmt.Errorf("s3: list %q: origin returned a non-advancing continuation token", physical))
				return
			}
			previous, token = token, next
		}
	}
}

func (b *Bucket) Get(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, error) {
	rc, _, err := b.GetWithInfo(ctx, key, offset, length, ifMatch)
	return rc, err
}

func (b *Bucket) GetWithInfo(ctx context.Context, key string, offset, length int64, ifMatch string) (io.ReadCloser, ObjectInfo, error) {
	if offset < 0 {
		return nil, ObjectInfo{}, fmt.Errorf("s3: negative offset %d", offset)
	}
	if length < -1 {
		return nil, ObjectInfo{}, fmt.Errorf("s3: invalid length %d", length)
	}
	c := b.client
	if c == nil {
		return nil, ObjectInfo{}, errors.New("s3: nil bucket")
	}
	object, err := c.objectKey(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	etag := normalizeETag(ifMatch)
	if length == 0 {
		meta, err := b.Stat(ctx, key)
		if err != nil {
			return nil, ObjectInfo{}, err
		}
		if etag != "" && etag != "*" && !etagEqual(etag, meta.ETag) {
			return nil, ObjectInfo{}, fmt.Errorf("%w: GET %s If-Match %s", ErrPrecondition, key, etag)
		}
		return http.NoBody, meta, nil
	}
	if length > 0 && offset > math.MaxInt64-(length-1) {
		return nil, ObjectInfo{}, fmt.Errorf("s3: range %d+%d overflows", offset, length)
	}

	headers := make([][2]string, 0, 2)
	requested := ""
	switch {
	case length > 0:
		requested = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	case offset > 0:
		requested = fmt.Sprintf("bytes=%d-", offset)
	}
	if requested != "" {
		headers = append(headers, [2]string{"range", requested})
	}
	if etag != "" {
		headers = append(headers, [2]string{"if-match", quoteETag(etag)})
	}

	resp, err := c.do(ctx, http.MethodGet, object, nil, nil, -1, headers...)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, ObjectInfo{}, statusError("GET", key, resp)
	}
	if err := checkRangeResponse(resp, offset, length, requested); err != nil {
		resp.Body.Close()
		return nil, ObjectInfo{}, fmt.Errorf("s3: GET %s: %w", key, err)
	}
	size := resp.ContentLength
	if total, ok := parseContentRangeSize(resp.Header.Get("Content-Range")); ok {
		size = total
	}
	info := objectInfoFromHeader(strings.TrimLeft(key, "/"), resp.Header, size)
	if length > 0 {
		return &limitedReadCloser{r: io.LimitReader(resp.Body, length), c: resp.Body}, info, nil
	}
	return resp.Body, info, nil
}

func (b *Bucket) Delete(ctx context.Context, key string) error {
	if b.client == nil {
		return errors.New("s3: nil bucket")
	}
	object, err := b.client.objectKey(key)
	if err != nil {
		return err
	}
	resp, err := b.client.do(ctx, http.MethodDelete, object, nil, nil, -1)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return statusError("DELETE", key, resp)
	}
	return nil
}

func (b *Bucket) CreateBucket(ctx context.Context) error {
	if b.client == nil {
		return errors.New("s3: nil bucket")
	}
	resp, err := b.client.do(ctx, http.MethodPut, "", nil, nil, -1)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusConflict:
		return nil
	default:
		return statusError("PUT bucket", b.client.bucket, resp)
	}
}

func (b *Bucket) DeleteBucket(ctx context.Context) error {
	if b.client == nil {
		return errors.New("s3: nil bucket")
	}
	resp, err := b.client.do(ctx, http.MethodDelete, "", nil, nil, -1)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return statusError("DELETE bucket", b.client.bucket, resp)
	}
}

func (b *Bucket) WriteReader(ctx context.Context, key string, r io.Reader, size int64, opts PutOptions) (ObjectInfo, error) {
	if size < 0 {
		return ObjectInfo{}, fmt.Errorf("s3: PUT %s: negative size %d", key, size)
	}
	if r == nil && size > 0 {
		return ObjectInfo{}, fmt.Errorf("s3: PUT %s: nil body for %d bytes", key, size)
	}
	c := b.client
	if c == nil {
		return ObjectInfo{}, errors.New("s3: nil bucket")
	}
	object, err := c.objectKey(key)
	if err != nil {
		return ObjectInfo{}, err
	}
	headers, userMeta, err := putHeaders(opts)
	if err != nil {
		return ObjectInfo{}, err
	}
	logical := strings.TrimLeft(key, "/")
	if size > c.maxSinglePut {
		return b.putMultipartStream(ctx, logical, r, size, headers, userMeta)
	}
	var body io.Reader
	if size > 0 {
		body = r
	} else {
		body = http.NoBody
	}
	resp, err := c.do(ctx, http.MethodPut, object, nil, body, size, headers...)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ObjectInfo{}, statusError("PUT", key, resp)
	}
	return putObjectInfo(logical, size, opts.ContentType, opts.ContentEncoding, userMeta, resp.Header), nil
}

func (b *Bucket) putMultipartStream(ctx context.Context, key string, r io.Reader, size int64, headers [][2]string, userMeta []string) (ObjectInfo, error) {
	if r == nil {
		return ObjectInfo{}, fmt.Errorf("s3: PUT %s: nil body", key)
	}
	c := b.client
	partSize := calculatePartSize(size, c.minPartSize)
	u := &uploader{
		c:             c,
		object:        key,
		createHeaders: nonConditionalHeaders(headers),
		conditions:    conditionalHeaders(headers),
	}
	if err := u.Start(ctx); err != nil {
		return ObjectInfo{}, err
	}
	finished := false
	defer func() {
		if !finished {
			_ = u.Abort(context.WithoutCancel(ctx))
		}
	}()
	for number, remaining := int64(1), size; remaining > 0; number++ {
		want := min(partSize, remaining)
		if err := u.uploadPart(ctx, number, io.LimitReader(r, want), want); err != nil {
			return ObjectInfo{}, err
		}
		remaining -= want
	}
	if err := u.Close(ctx); err != nil {
		if len(u.conditions) > 0 && Unsupported(err) {
			return ObjectInfo{}, fmt.Errorf("%w: origin cannot apply conditions to CompleteMultipartUpload: %w", ErrUnsupported, err)
		}
		return ObjectInfo{}, err
	}
	finished = true
	return ObjectInfo{
		Key: key, Size: size, ETag: normalizeETag(u.ETag()),
		ContentType: contentTypeHeader(headers), ContentEncoding: headerValue(headers, "content-encoding"),
		UserMetadata: append([]string(nil), userMeta...),
		LastModified: time.Now().UTC(),
	}, nil
}

func putHeaders(opts PutOptions) ([][2]string, []string, error) {
	userMeta, err := normalizeUserMetadata(opts.UserMetadata)
	if err != nil {
		return nil, nil, err
	}
	ifMatch := normalizeETag(opts.IfMatch)
	ifNone := normalizeETag(opts.IfNoneMatch)
	if ifMatch != "" && ifNone != "" {
		return nil, nil, errors.New("s3: If-Match and If-None-Match are mutually exclusive")
	}
	if ifNone != "" && ifNone != "*" {
		return nil, nil, fmt.Errorf("s3: If-None-Match supports only %q", "*")
	}
	headers := make([][2]string, 0, 1+len(userMeta)/2+2)
	if opts.ContentType != "" {
		if !validHeaderValue(opts.ContentType) {
			return nil, nil, errors.New("s3: invalid content type")
		}
		headers = append(headers, [2]string{"content-type", opts.ContentType})
	}
	if opts.ContentEncoding != "" {
		if !validHeaderValue(opts.ContentEncoding) {
			return nil, nil, errors.New("s3: invalid content encoding")
		}
		headers = append(headers, [2]string{"content-encoding", opts.ContentEncoding})
	}
	for i := 0; i+1 < len(userMeta); i += 2 {
		headers = append(headers, [2]string{"x-amz-meta-" + userMeta[i], userMeta[i+1]})
	}
	if ifMatch != "" {
		headers = append(headers, [2]string{"if-match", quoteETag(ifMatch)})
	}
	if ifNone != "" {
		headers = append(headers, [2]string{"if-none-match", "*"})
	}
	return headers, userMeta, nil
}

func conditionalHeaders(headers [][2]string) [][2]string {
	out := make([][2]string, 0, 2)
	for _, h := range headers {
		if h[0] == "if-match" || h[0] == "if-none-match" {
			out = append(out, h)
		}
	}
	return out
}

func nonConditionalHeaders(headers [][2]string) [][2]string {
	out := make([][2]string, 0, len(headers))
	for _, h := range headers {
		if h[0] != "if-match" && h[0] != "if-none-match" {
			out = append(out, h)
		}
	}
	return out
}

func headerValue(headers [][2]string, name string) string {
	for _, h := range headers {
		if h[0] == name {
			return h[1]
		}
	}
	return ""
}

func contentTypeHeader(headers [][2]string) string {
	for _, h := range headers {
		if h[0] == "content-type" {
			return h[1]
		}
	}
	return ""
}

func normalizeUserMetadata(pairs []string) ([]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	if len(pairs)%2 != 0 {
		return nil, errors.New("s3: user metadata must be name/value pairs")
	}
	values := make(map[string]string, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		name := strings.TrimSpace(strings.ToLower(pairs[i]))
		name = strings.TrimPrefix(name, "x-amz-meta-")
		if !validHeaderName(name) {
			return nil, fmt.Errorf("s3: invalid user metadata name %q", pairs[i])
		}
		value := pairs[i+1]
		if value == "" || !validHeaderValue(value) {
			return nil, fmt.Errorf("s3: invalid user metadata value for %q", name)
		}
		values[name] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		out = append(out, key, values[key])
	}
	return out, nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '!' || c == '#' || c == '$' || c == '%' || c == '&' ||
			c == '\'' || c == '*' || c == '+' || c == '-' || c == '.' ||
			c == '^' || c == '_' || c == '`' || c == '|' || c == '~':
		default:
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c < 0x20 && c != '\t' || c == 0x7f {
			return false
		}
	}
	return true
}

func putObjectInfo(key string, size int64, contentType, contentEncoding string, userMeta []string, h http.Header) ObjectInfo {
	info := ObjectInfo{
		Key: key, Size: size, ETag: normalizeETag(h.Get("ETag")),
		ContentType: contentType, ContentEncoding: contentEncoding,
		UserMetadata: append([]string(nil), userMeta...),
	}
	if t, err := http.ParseTime(h.Get("Last-Modified")); err == nil {
		info.LastModified = t
	} else {
		info.LastModified = time.Now().UTC()
	}
	return info
}

func objectInfoFromHeader(key string, h http.Header, size int64) ObjectInfo {
	info := ObjectInfo{
		Key: key, Size: size, ETag: normalizeETag(h.Get("ETag")),
		ContentType: h.Get("Content-Type"), ContentEncoding: h.Get("Content-Encoding"),
	}
	if t, err := http.ParseTime(h.Get("Last-Modified")); err == nil {
		info.LastModified = t
	}
	values := map[string]string{}
	for name, list := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(list) > 0 {
			values[strings.TrimPrefix(lower, "x-amz-meta-")] = list[0]
		}
	}
	info.UserMetadata = pairMap(values)
	return info
}

func normalizeETag(etag string) string {
	etag = strings.TrimSpace(etag)
	if etag == "*" {
		return etag
	}
	return strings.Trim(etag, `"`)
}

func quoteETag(etag string) string {
	if etag == "*" || etag == "" {
		return etag
	}
	return `"` + etag + `"`
}

func etagEqual(a, b string) bool {
	return normalizeETag(a) == normalizeETag(b)
}

func checkRangeResponse(resp *http.Response, offset, length int64, requested string) error {
	if requested == "" {
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		return errors.New("origin returned a partial response for a whole-object GET")
	}
	if resp.StatusCode == http.StatusOK {
		if length > 0 {
			if offset == 0 && resp.ContentLength == length {
				return nil
			}
			return fmt.Errorf("origin ignored Range (wanted %d bytes at %d, got whole object)", length, offset)
		}
		if offset == 0 {
			return nil
		}
		return fmt.Errorf("origin ignored Range (wanted offset %d, got whole object)", offset)
	}
	start, end, ok := parseContentRange(resp.Header.Get("Content-Range"))
	if !ok {
		if resp.ContentLength >= 0 && length > 0 && resp.ContentLength != length {
			return fmt.Errorf("range returned %d bytes, wanted %d", resp.ContentLength, length)
		}
		return nil
	}
	if start != offset {
		return fmt.Errorf("range starts at %d, wanted %d", start, offset)
	}
	if length > 0 && end < offset+length-1 {
		return fmt.Errorf("range ended at %d, wanted %d", end, offset+length-1)
	}
	return nil
}

func parseContentRangeSize(value string) (int64, bool) {
	value = strings.TrimSpace(value)
	const prefix = "bytes "
	if !strings.HasPrefix(value, prefix) {
		return 0, false
	}
	_, total, ok := strings.Cut(value[len(prefix):], "/")
	if !ok || total == "*" {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(total), 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func parseContentRange(value string) (start, end int64, ok bool) {
	value = strings.TrimSpace(value)
	const prefix = "bytes "
	if !strings.HasPrefix(value, prefix) {
		return 0, 0, false
	}
	spec, _, _ := strings.Cut(value[len(prefix):], "/")
	left, right, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(strings.TrimSpace(left), 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end, err = strconv.ParseInt(strings.TrimSpace(right), 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

type limitedReadCloser struct {
	r io.Reader
	c io.Closer
}

func (r *limitedReadCloser) Read(p []byte) (int, error) { return r.r.Read(p) }
func (r *limitedReadCloser) Close() error               { return r.c.Close() }

func pairMap(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		out = append(out, key, m[key])
	}
	return out
}

type listPage struct {
	IsTruncated           bool         `xml:"IsTruncated"`
	EncodingType          string       `xml:"EncodingType"`
	NextContinuationToken string       `xml:"NextContinuationToken"`
	Contents              []listObject `xml:"Contents"`
}

type listObject struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	ContentType  string    `xml:"ContentType"`
}

func (c *client) listPage(ctx context.Context, prefix, token string) (listPage, error) {
	query := url.Values{
		"list-type":     {"2"},
		"encoding-type": {"url"},
		"max-keys":      {"1000"},
	}
	if prefix != "" {
		query.Set("prefix", prefix)
	}
	if token != "" {
		query.Set("continuation-token", token)
	}
	resp, err := c.do(ctx, http.MethodGet, "", query, nil, -1)
	if err != nil {
		return listPage{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return listPage{}, statusError("GET list", prefix, resp)
	}
	var page listPage
	if err := xml.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&page); err != nil {
		return listPage{}, fmt.Errorf("s3: list %q: decoding response: %w", prefix, err)
	}
	return page, nil
}

const (
	minPartSize = 5 * 1024 * 1024

	maxParts = 10000

	abortTimeout = 15 * time.Second
)

type uploader struct {
	c      *client
	object string

	createHeaders [][2]string

	conditions [][2]string

	id       string
	etag     string
	parts    []tagpart
	started  bool
	finished bool
}

type tagpart struct {
	Num  int64  `xml:"PartNumber"`
	ETag string `xml:"ETag"`
}

type multipartResponse struct {
	XMLName xml.Name
	Bucket  string `xml:"Bucket"`
	Key     string `xml:"Key"`
	ID      string `xml:"UploadId"`
	ETag    string `xml:"ETag"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func encodeCompleteMultipart(parts []tagpart) ([]byte, error) {
	var body bytes.Buffer
	body.Grow(110 + len(parts)*96)
	body.WriteString(`<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	var digits [20]byte
	for _, part := range parts {
		body.WriteString("<Part><PartNumber>")
		body.Write(strconv.AppendInt(digits[:0], part.Num, 10))
		body.WriteString("</PartNumber><ETag>")
		if err := xml.EscapeText(&body, []byte(part.ETag)); err != nil {
			return nil, err
		}
		body.WriteString("</ETag></Part>")
	}
	body.WriteString("</CompleteMultipartUpload>")
	return body.Bytes(), nil
}

func decodeMultipartResponse(r io.Reader) (multipartResponse, error) {
	var response multipartResponse
	err := xml.NewDecoder(io.LimitReader(r, 1<<20)).Decode(&response)
	return response, err
}

func (u *uploader) Start(ctx context.Context) error {
	if u.started {
		return errors.New("s3: multipart upload already started")
	}
	if u.c == nil || u.object == "" {
		return errors.New("s3: multipart upload requires a client and object")
	}
	object, err := u.c.objectKey(u.object)
	if err != nil {
		return err
	}
	res, err := u.c.doBytes(ctx, http.MethodPost, object, url.Values{"uploads": {""}}, nil, u.createHeaders...)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return statusError("POST create-multipart", u.object, res)
	}
	rt, err := decodeMultipartResponse(res.Body)
	if err != nil {
		return fmt.Errorf("s3: create-multipart: decoding response: %w", err)
	}
	switch {
	case rt.ID == "":
		return errors.New("s3: create-multipart: origin returned no upload id")
	case rt.Bucket != "" && rt.Bucket != u.c.bucket:
		return fmt.Errorf("s3: create-multipart: returned bucket %q, want %q", rt.Bucket, u.c.bucket)
	case rt.Key != "" && rt.Key != object:
		return fmt.Errorf("s3: create-multipart: returned key %q, want %q", rt.Key, object)
	}
	u.started = true
	u.id = rt.ID
	return nil
}

func (u *uploader) uploadPart(ctx context.Context, num int64, r io.Reader, size int64) error {
	if !u.started || u.finished {
		return errors.New("s3: multipart upload is not active")
	}
	object, err := u.c.objectKey(u.object)
	if err != nil {
		return err
	}
	query := url.Values{
		"partNumber": {strconv.FormatInt(num, 10)},
		"uploadId":   {u.id},
	}
	res, err := u.c.do(ctx, http.MethodPut, object, query, r, size)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return statusError("PUT upload-part", u.object, res)
	}
	etag := res.Header.Get("ETag")
	if etag == "" {
		return errors.New("s3: upload-part: response missing ETag")
	}
	u.parts = append(u.parts, tagpart{Num: num, ETag: etag})
	return nil
}

func (u *uploader) Close(ctx context.Context) error {
	if !u.started {
		return errors.New("s3: multipart upload is not active")
	}
	if u.finished {
		return errors.New("s3: multipart upload already completed")
	}

	sort.Slice(u.parts, func(i, j int) bool { return u.parts[i].Num < u.parts[j].Num })
	buf, err := encodeCompleteMultipart(u.parts)
	if err != nil {
		return err
	}
	object, err := u.c.objectKey(u.object)
	if err != nil {
		return err
	}
	headers := make([][2]string, 0, len(u.conditions)+1)
	headers = append(headers, u.conditions...)
	headers = append(headers, [2]string{"content-type", "application/xml"})
	res, err := u.c.doBytes(ctx, http.MethodPost, object, url.Values{"uploadId": {u.id}}, buf, headers...)
	if err != nil {
		return fmt.Errorf("s3: complete-multipart: %w", err)
	}
	defer res.Body.Close()
	payload, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return bodyError("POST complete-multipart", u.object, res.StatusCode, payload)
	}
	if readErr != nil {
		return fmt.Errorf("s3: complete-multipart: reading response: %w", readErr)
	}
	if len(bytes.TrimSpace(payload)) == 0 {

		u.finished = true
		return nil
	}
	rt, err := decodeMultipartResponse(bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("s3: complete-multipart: decoding response: %w", err)
	}
	switch rt.XMLName.Local {
	default:
		return fmt.Errorf("s3: complete-multipart: unexpected response %s", rt.XMLName.Local)
	case "Error":
		return bodyError("POST complete-multipart", u.object, http.StatusOK, payload)
	case "CompleteMultipartUploadResult":

	}
	u.etag = rt.ETag
	u.finished = true
	return nil
}

func (u *uploader) ETag() string { return u.etag }

func (u *uploader) Abort(ctx context.Context) error {
	if !u.started || u.finished {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, abortTimeout)
	defer cancel()

	object, err := u.c.objectKey(u.object)
	if err != nil {
		return err
	}
	res, err := u.c.doBytes(ctx, http.MethodDelete, object, url.Values{"uploadId": {u.id}}, nil)
	if err != nil {
		return fmt.Errorf("s3: abort-multipart: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return statusError("DELETE abort-multipart", u.object, res)
	}
	u.started = false
	u.finished = false
	u.id = ""
	u.etag = ""
	u.parts = nil
	return nil
}

func calculatePartSize(totalSize, minPart int64) int64 {
	partSize := minPart
	if partSize <= 0 {
		partSize = minPartSize
	}
	if totalSize > 0 {
		for (totalSize-1)/partSize >= maxParts {
			partSize *= 2
		}
	}
	return partSize
}
