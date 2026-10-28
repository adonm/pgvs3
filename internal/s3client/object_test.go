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
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrefixPutStatAndDelete(t *testing.T) {
	mock, server := newTestServer(t)
	bucket := newTestBucket(t, server, " /lake/ ")
	ctx := context.Background()

	body := "hello"
	put, err := bucket.WriteReader(ctx, "/dir/a.txt", strings.NewReader(body), int64(len(body)), PutOptions{
		ContentType: "text/plain; charset=utf-8",
		UserMetadata: []string{
			"Owner", "alice",
			"x-amz-meta-Trace", "abc",
		},
	})
	if err != nil {
		t.Fatalf("WriteReader: %v", err)
	}
	if put.Size != int64(len(body)) || put.ContentType != "text/plain; charset=utf-8" {
		t.Fatalf("Put meta = %+v", put)
	}
	if put.Key != "dir/a.txt" {
		t.Fatalf("Put key = %q", put.Key)
	}
	if want := []string{"owner", "alice", "trace", "abc"}; !reflect.DeepEqual(put.UserMetadata, want) {
		t.Fatalf("Put user metadata = %#v, want %#v", put.UserMetadata, want)
	}

	mock.mu.Lock()
	stored := mock.objects["lake/dir/a.txt"]
	mock.mu.Unlock()
	if stored == nil {
		t.Fatal("object was not stored under the configured prefix")
	}
	if string(stored.data) != body {
		t.Fatalf("stored body = %q", stored.data)
	}
	if stored.contentType != "text/plain; charset=utf-8" {
		t.Fatalf("stored content type = %q", stored.contentType)
	}
	if !reflect.DeepEqual(stored.meta, map[string]string{"owner": "alice", "trace": "abc"}) {
		t.Fatalf("stored metadata = %#v", stored.meta)
	}

	stat, err := bucket.Stat(ctx, "dir/a.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Size != int64(len(body)) || stat.ContentType != "text/plain; charset=utf-8" {
		t.Fatalf("Stat meta = %+v", stat)
	}
	if stat.ETag == "" || strings.Contains(stat.ETag, `"`) || stat.LastModified.IsZero() {
		t.Fatalf("Stat etag/time = %+v", stat)
	}
	if want := []string{"owner", "alice", "trace", "abc"}; !reflect.DeepEqual(stat.UserMetadata, want) {
		t.Fatalf("Stat user metadata = %#v, want %#v", stat.UserMetadata, want)
	}

	if err := bucket.Delete(ctx, "dir/a.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := bucket.Stat(ctx, "dir/a.txt"); !NotFound(err) {
		t.Fatalf("Stat deleted = %v, want not found", err)
	}
}

func TestConditionalPut(t *testing.T) {
	_, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()
	key := "conditional.txt"

	first, err := bucket.WriteReader(ctx, key, strings.NewReader("first"), 5, PutOptions{IfNoneMatch: "*"})
	if err != nil {
		t.Fatalf("Put If-None-Match: %v", err)
	}
	if _, err := bucket.WriteReader(ctx, key, strings.NewReader("second"), 6, PutOptions{IfNoneMatch: "*"}); !Precondition(err) {
		t.Fatalf("second Put If-None-Match = %v, want precondition", err)
	}
	if _, err := bucket.WriteReader(ctx, key, strings.NewReader("second"), 6, PutOptions{IfMatch: "wrong"}); !Precondition(err) {
		t.Fatalf("Put wrong If-Match = %v, want precondition", err)
	}
	second, err := bucket.WriteReader(ctx, key, strings.NewReader("second"), 6, PutOptions{IfMatch: first.ETag})
	if err != nil {
		t.Fatalf("Put If-Match: %v", err)
	}
	if second.ETag == first.ETag {
		t.Fatal("ETag did not change after overwrite")
	}
	if _, err := bucket.WriteReader(ctx, "missing-if-match", strings.NewReader("x"), 1, PutOptions{IfMatch: "*"}); !NotFound(err) {
		t.Fatalf("Put If-Match on missing object = %v, want not found", err)
	}
}

func TestGetExactRange(t *testing.T) {
	_, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()

	data := "0123456789"
	put, err := bucket.WriteReader(ctx, "range.txt", strings.NewReader(data), int64(len(data)), PutOptions{ContentType: "text/plain"})
	if err != nil {
		t.Fatalf("WriteReader: %v", err)
	}

	checkRange := func(offset, length int64, ifMatch, want string) {
		t.Helper()
		rc, err := bucket.Get(ctx, "range.txt", offset, length, ifMatch)
		if err != nil {
			t.Fatalf("Get(%d,%d): %v", offset, length, err)
		}
		body, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			t.Fatalf("read range: %v", err)
		}
		if string(body) != want {
			t.Fatalf("Get(%d,%d) = %q, want %q", offset, length, body, want)
		}
	}
	checkRange(2, 4, "", "2345")
	checkRange(2, 4, put.ETag, "2345")
	checkRange(0, int64(len(data)), put.ETag, data)
	checkRange(9, 1, "", "9")

	rc, err := bucket.Get(ctx, "range.txt", 0, 0, put.ETag)
	if err != nil {
		t.Fatalf("empty Get: %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if len(body) != 0 {
		t.Fatalf("empty range returned %q", body)
	}
	if _, err := bucket.Get(ctx, "range.txt", 0, 0, "wrong"); !Precondition(err) {
		t.Fatalf("empty range with wrong etag = %v, want precondition", err)
	}

	if _, err := bucket.Get(ctx, "range.txt", 2, 4, "wrong"); !Precondition(err) {
		t.Fatalf("wrong If-Match = %v, want precondition", err)
	}
	if _, err := bucket.Get(ctx, "range.txt", 8, 10, put.ETag); err == nil {
		t.Fatal("short range was accepted")
	}
	if _, err := bucket.Get(ctx, "range.txt", 100, 1, ""); !InvalidRange(err) {
		t.Fatalf("out-of-bounds range = %v, want invalid range", err)
	}
	if _, err := bucket.Get(ctx, "missing.txt", 0, 1, ""); !NotFound(err) {
		t.Fatalf("missing range = %v, want not found", err)
	}

	if _, err := bucket.WriteReader(ctx, "empty.txt", nil, 0, PutOptions{}); err != nil {
		t.Fatalf("Put empty: %v", err)
	}
	rc, err = bucket.Get(ctx, "empty.txt", 0, -1, "")
	if err != nil {
		t.Fatalf("Get empty object: %v", err)
	}
	body, _ = io.ReadAll(rc)
	_ = rc.Close()
	if len(body) != 0 {
		t.Fatalf("empty object returned %q", body)
	}
}

func TestRangeIgnoringOriginIsRejected(t *testing.T) {
	const data = "0123456789"
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", `"etag"`)
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = io.WriteString(w, data)
		}
	}))
	defer server.Close()
	bucket, err := New(Config{
		Endpoint: server.URL, Bucket: "test-bucket", AccessKey: "AKID", SecretKey: "SECRET", PathStyle: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.Get(context.Background(), "x", 2, 4, ""); err == nil || !strings.Contains(err.Error(), "ignored Range") {
		t.Fatalf("offset range = %v, want ignored Range", err)
	}
	if _, err := bucket.Get(context.Background(), "x", 0, 4, ""); err == nil || !strings.Contains(err.Error(), "ignored Range") {
		t.Fatalf("short range = %v, want ignored Range", err)
	}
	rc, err := bucket.Get(context.Background(), "x", 0, 10, "")
	if err != nil {
		t.Fatalf("full-size range: %v", err)
	}
	body, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(body) != data {
		t.Fatalf("full-size range body = %q", body)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("requests = %d", got)
	}
}

func TestWalkListing(t *testing.T) {
	mock, server := newTestServer(t)
	bucket := newTestBucket(t, server, "lake")
	ctx := context.Background()

	objects := map[string]string{
		"dir/":      "",
		"dir/a":     "a",
		"dir/a b":   "space",
		"dir/b":     "bb",
		"dir/sub/c": "ccc",
		"other/z":   "z",
	}
	for key, body := range objects {
		opts := PutOptions{}
		if key == "dir/a" {
			opts.ContentType = "text/plain"
		}
		if _, err := bucket.WriteReader(ctx, key, strings.NewReader(body), int64(len(body)), opts); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
	}

	mock.mu.Lock()
	mock.objects["lake-other"] = &mockObject{data: []byte("outside"), etag: `"outside"`}
	headsBefore := mock.headCalls
	mock.mu.Unlock()

	var keys []string
	var metas []ObjectInfo
	for info, err := range bucket.Walk(ctx, "dir/") {
		if err != nil {
			t.Fatalf("Walk: %v", err)
		}
		keys = append(keys, info.Key)
		metas = append(metas, info)
	}
	want := []string{"dir/a", "dir/a b", "dir/b", "dir/sub/c"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("Walk keys = %#v, want %#v", keys, want)
	}
	for i, meta := range metas {
		if meta.Size != int64(len(objects[keys[i]])) {
			t.Fatalf("meta[%d] size = %d for %q", i, meta.Size, keys[i])
		}
		if meta.ETag == "" || meta.LastModified.IsZero() {
			t.Fatalf("meta[%d] = %+v", i, meta)
		}
		if meta.ContentType != "" {
			t.Fatalf("list unexpectedly returned Content-Type %q", meta.ContentType)
		}
	}
	mock.mu.Lock()
	headsAfter := mock.headCalls
	mock.mu.Unlock()
	if headsAfter != headsBefore {
		t.Fatalf("Walk issued %d HEAD requests", headsAfter-headsBefore)
	}

	mock.mu.Lock()
	mock.maxKeys = 2
	listsBefore := mock.listCalls
	mock.mu.Unlock()
	keys = keys[:0]
	for info, err := range bucket.Walk(ctx, "dir/") {
		if err != nil {
			t.Fatalf("paginated Walk: %v", err)
		}
		keys = append(keys, info.Key)
	}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("paginated keys = %#v, want %#v", keys, want)
	}
	mock.mu.Lock()
	pages := mock.listCalls - listsBefore
	mock.maxKeys = 0
	mock.mu.Unlock()
	if pages < 2 {
		t.Fatalf("expected pagination, got %d list calls", pages)
	}

	count := 0
	for _, err := range bucket.Walk(ctx, "dir/") {
		if err != nil {
			t.Fatalf("stopped Walk: %v", err)
		}
		count++
		break
	}
	if count != 1 {
		t.Fatalf("early stop visited %d keys", count)
	}

	keys = keys[:0]
	for info, err := range bucket.Walk(ctx, "") {
		if err != nil {
			t.Fatalf("namespace Walk: %v", err)
		}
		keys = append(keys, info.Key)
	}
	if slices.Contains(keys, "lake-other") {
		t.Fatalf("namespace Walk leaked an outside key: %#v", keys)
	}

	mock.mu.Lock()
	mock.listContentType = true
	mock.mu.Unlock()
	for info, err := range bucket.Walk(ctx, "dir/a") {
		if err != nil {
			t.Fatalf("typed Walk: %v", err)
		}
		if info.ContentType != "text/plain" {
			t.Fatalf("list content type = %q", info.ContentType)
		}
		break
	}
}

func TestWalkBadContinuationToken(t *testing.T) {
	mock, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()
	if _, err := bucket.WriteReader(ctx, "a", strings.NewReader("a"), 1, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	mock.mu.Lock()
	mock.badToken = true
	mock.mu.Unlock()

	var walkErr error
	for _, err := range bucket.Walk(ctx, "") {
		if err != nil {
			walkErr = err
			break
		}
	}
	if walkErr == nil || !strings.Contains(walkErr.Error(), "non-advancing continuation token") {
		t.Fatalf("Walk error = %v", walkErr)
	}
}

func TestBucketLifecycle(t *testing.T) {
	mock, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()

	if err := bucket.CreateBucket(ctx); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	mock.mu.Lock()
	created := mock.createdBucket
	mock.mu.Unlock()
	if !created {
		t.Fatal("CreateBucket did not issue PUT")
	}

	if err := bucket.DeleteBucket(ctx); err != nil {
		t.Fatalf("DeleteBucket: %v", err)
	}
	mock.mu.Lock()
	deleted := mock.deletedBucket
	mock.mu.Unlock()
	if !deleted {
		t.Fatal("DeleteBucket did not issue DELETE")
	}
}

func TestErrorMapping(t *testing.T) {
	_, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()

	if _, err := bucket.Stat(ctx, "missing"); !NotFound(err) {
		t.Fatalf("Stat missing = %v, want not found", err)
	}
	if err := bucket.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete missing = %v, want nil", err)
	}
	if _, err := bucket.Stat(ctx, "err/conflict"); !Conflict(err) {
		t.Fatalf("Stat conflict = %v, want conflict", err)
	}
	var apiErr *Error
	if _, err := bucket.Get(ctx, "err/boom", 0, 1, ""); !errors.As(err, &apiErr) || apiErr.StatusCode != 500 {
		t.Fatalf("Get boom = %#v, want 500 *Error", err)
	}
	if apiErr.Err != nil {
		t.Fatalf("500 mapped to a sentinel: %v", apiErr.Err)
	}
	if err := bucket.Delete(ctx, "err/conflict"); !Conflict(err) {
		t.Fatalf("Delete conflict = %v, want conflict", err)
	}
}

func TestValidation(t *testing.T) {
	_, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	ctx := context.Background()

	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), -1, PutOptions{}); err == nil {
		t.Fatal("negative size accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", nil, 1, PutOptions{}); err == nil {
		t.Fatal("nil body accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{UserMetadata: []string{"a"}}); err == nil {
		t.Fatal("odd metadata accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{UserMetadata: []string{"bad name", "v"}}); err == nil {
		t.Fatal("invalid metadata name accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{UserMetadata: []string{"name", ""}}); err == nil {
		t.Fatal("empty metadata value accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{IfMatch: "*", IfNoneMatch: "*"}); err == nil {
		t.Fatal("conflicting conditions were accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{IfNoneMatch: "etag"}); err == nil {
		t.Fatal("non-star If-None-Match was accepted")
	}
	if _, err := bucket.Stat(ctx, ""); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, err := bucket.Get(ctx, "x", -1, 1, ""); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, err := bucket.Get(ctx, "x", 0, -2, ""); err == nil {
		t.Fatal("invalid length accepted")
	}
	if _, err := bucket.WriteReader(ctx, "x", strings.NewReader("x"), 1, PutOptions{ContentType: "bad\x00type"}); err == nil {
		t.Fatal("invalid content type accepted")
	}

	var walkErr error

	//lint:ignore SA1012 verifying nil-context rejection
	for _, err := range bucket.Walk(nil, "") {
		if err != nil {
			walkErr = err
			break
		}
	}
	if walkErr == nil || !strings.Contains(walkErr.Error(), "nil context") {
		t.Fatalf("Walk nil context = %v", walkErr)
	}
}

func TestMultipartPutStream(t *testing.T) {
	mock, server := newTestServer(t)
	bucket := newTestBucket(t, server, "")
	bucket.client.maxSinglePut, bucket.client.minPartSize = 20, 8
	ctx := context.Background()
	body := "012345678901234567890123456789"
	meta := []string{"owner", "alice"}

	put, err := bucket.WriteReader(ctx, "big", strings.NewReader(body), int64(len(body)), PutOptions{
		ContentType: "application/x-test", UserMetadata: meta, IfNoneMatch: "*",
	})
	if err != nil {
		t.Fatalf("multipart WriteReader: %v", err)
	}
	if put.Size != int64(len(body)) || put.ContentType != "application/x-test" {
		t.Fatalf("multipart meta = %+v", put)
	}
	if !reflect.DeepEqual(put.UserMetadata, meta) {
		t.Fatalf("multipart user metadata = %#v", put.UserMetadata)
	}

	mock.mu.Lock()
	stored := mock.objects["big"]
	createSawConditions := mock.lastCreateSawConditions
	completeIfNone := mock.lastCompleteIfNone
	parts := mock.lastPartCount
	uploads := len(mock.uploads)
	mock.mu.Unlock()
	if stored == nil || string(stored.data) != body {
		t.Fatalf("stored multipart object = %+v", stored)
	}
	if stored.contentType != "application/x-test" || !reflect.DeepEqual(stored.meta, map[string]string{"owner": "alice"}) {
		t.Fatalf("multipart stored metadata = %q %#v", stored.contentType, stored.meta)
	}
	if createSawConditions {
		t.Fatal("conditional headers were sent on CreateMultipartUpload")
	}
	if completeIfNone != "*" {
		t.Fatalf("CompleteMultipartUpload If-None-Match = %q", completeIfNone)
	}
	if parts != 4 {
		t.Fatalf("part count = %d, want 4", parts)
	}
	if uploads != 0 {
		t.Fatalf("%d multipart uploads left behind", uploads)
	}

	if _, err := bucket.WriteReader(ctx, "big", strings.NewReader(body), int64(len(body)), PutOptions{IfNoneMatch: "*"}); !Precondition(err) {
		t.Fatalf("conditional multipart overwrite = %v, want precondition", err)
	}
	mock.mu.Lock()
	uploads = len(mock.uploads)
	mock.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("%d uploads left after failed conditional complete", uploads)
	}

	mock.mu.Lock()
	mock.rejectConditionalComplete = true
	mock.mu.Unlock()
	_, err = bucket.WriteReader(ctx, "big2", strings.NewReader(body), int64(len(body)), PutOptions{IfNoneMatch: "*"})
	if !Unsupported(err) {
		t.Fatalf("unsupported conditional complete = %v, want ErrUnsupported", err)
	}
	mock.mu.Lock()
	uploads = len(mock.uploads)
	mock.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("%d uploads left after unsupported complete", uploads)
	}

	if _, err := bucket.WriteReader(ctx, "short", strings.NewReader("short"), 30, PutOptions{}); err == nil {
		t.Fatal("short multipart body was accepted")
	}
	mock.mu.Lock()
	uploads = len(mock.uploads)
	mock.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("%d uploads left after short body", uploads)
	}
}

func TestCalculatePartSize(t *testing.T) {
	const mib = int64(1 << 20)
	cases := []struct {
		total, min, want int64
	}{
		{0, 8 * mib, 8 * mib},
		{8 * mib, 8 * mib, 8 * mib},
		{maxParts * 8 * mib, 8 * mib, 8 * mib},
		{maxParts*8*mib + 1, 8 * mib, 16 * mib},
		{1, 0, minPartSize},
		{1, -1, minPartSize},
	}
	for _, tc := range cases {
		if got := calculatePartSize(tc.total, tc.min); got != tc.want {
			t.Fatalf("calculatePartSize(%d, %d) = %d, want %d", tc.total, tc.min, got, tc.want)
		}
	}
}

type mockObject struct {
	data        []byte
	etag        string
	contentType string
	meta        map[string]string
	modified    time.Time
}

type mockUpload struct {
	key         string
	contentType string
	meta        map[string]string
	parts       map[int][]byte
	etags       map[int]string
	ifMatch     string
	ifNoneMatch string
}

type mockS3 struct {
	bucket string

	mu        sync.Mutex
	objects   map[string]*mockObject
	uploads   map[string]*mockUpload
	nextID    int
	maxKeys   int
	headCalls int
	listCalls int

	lastCreateSawConditions bool
	lastCompleteIfMatch     string
	lastCompleteIfNone      string
	lastPartCount           int

	rejectConditionalComplete bool
	listContentType           bool
	badToken                  bool
	createdBucket             bool
	deletedBucket             bool
}

func newMockS3() *mockS3 {
	return &mockS3{
		bucket:  "test-bucket",
		objects: map[string]*mockObject{}, uploads: map[string]*mockUpload{},
	}
}

func (m *mockS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/") {
		http.Error(w, "missing signature", http.StatusForbidden)
		return
	}
	if r.Header.Get("X-Amz-Date") == "" {
		http.Error(w, "missing x-amz-date", http.StatusForbidden)
		return
	}
	decoded, err := url.PathUnescape(r.URL.EscapedPath())
	if err != nil {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	decoded = strings.TrimPrefix(decoded, "/")
	bucket, key, _ := strings.Cut(decoded, "/")
	if bucket != m.bucket {
		m.writeError(w, http.StatusNotFound, "NoSuchBucket", "bucket not found")
		return
	}
	if key == "" {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			m.list(w, r)
			return
		}
		m.mu.Lock()
		switch r.Method {
		case http.MethodPut:
			m.createdBucket = true
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			m.deletedBucket = true
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
		m.mu.Unlock()
		return
	}
	if key == "err/conflict" {
		m.writeError(w, http.StatusConflict, "ConditionalRequestConflict", "try again")
		return
	}
	if key == "err/boom" {
		m.writeError(w, http.StatusInternalServerError, "InternalError", "boom")
		return
	}
	query := r.URL.Query()
	switch r.Method {
	case http.MethodHead:
		m.head(w, key)
	case http.MethodGet:
		if query.Get("list-type") == "2" {
			m.list(w, r)
		} else {
			m.get(w, r, key)
		}
	case http.MethodPut:
		if query.Get("uploadId") != "" && query.Get("partNumber") != "" {
			m.uploadPart(w, r, key, query.Get("uploadId"))
		} else {
			m.put(w, r, key)
		}
	case http.MethodPost:
		if _, ok := query["uploads"]; ok {
			m.createUpload(w, r, key)
		} else if query.Get("uploadId") != "" {
			m.completeUpload(w, r, key, query.Get("uploadId"))
		} else {
			m.writeError(w, http.StatusBadRequest, "InvalidRequest", "unknown POST")
		}
	case http.MethodDelete:
		if query.Get("uploadId") != "" {
			m.abortUpload(w, key, query.Get("uploadId"))
		} else {
			m.delete(w, key)
		}
	default:
		m.writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method)
	}
}

func (m *mockS3) head(w http.ResponseWriter, key string) {
	m.mu.Lock()
	m.headCalls++
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		m.writeError(w, http.StatusNotFound, "NoSuchKey", "missing")
		return
	}
	m.setHeaders(w, obj)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) get(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.Lock()
	obj, ok := m.objects[key]
	m.mu.Unlock()
	if !ok {
		m.writeError(w, http.StatusNotFound, "NoSuchKey", "missing")
		return
	}
	if condition := r.Header.Get("If-Match"); condition != "" {
		if condition != "*" && normalizeETag(condition) != normalizeETag(obj.etag) {
			m.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag changed")
			return
		}
	}
	m.setHeaders(w, obj)
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(obj.data)
		return
	}
	start, end, ok := parseTestRange(rangeHeader, int64(len(obj.data)))
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(obj.data)))
		m.writeError(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "bad range")
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(obj.data[start : end+1])
}

func (m *mockS3) put(w http.ResponseWriter, r *http.Request, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if hash := r.Header.Get("X-Amz-Content-Sha256"); hash != "UNSIGNED-PAYLOAD" &&
		hash != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		http.Error(w, "PUT must use a signed payload hash", http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, exists := m.objects[key]
	if m.checkConditions(w, r, current, exists) {
		return
	}
	obj := &mockObject{
		data: append([]byte(nil), data...), etag: testETag(data),
		contentType: r.Header.Get("Content-Type"), meta: testMeta(r.Header),
		modified: time.Now().UTC().Truncate(time.Second),
	}
	m.objects[key] = obj
	w.Header().Set("ETag", obj.etag)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) checkConditions(w http.ResponseWriter, r *http.Request, current *mockObject, exists bool) bool {
	if value := r.Header.Get("If-None-Match"); value != "" {
		if value != "*" {
			m.writeError(w, http.StatusBadRequest, "InvalidRequest", "bad If-None-Match")
			return true
		}
		if exists {
			m.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "exists")
			return true
		}
	}
	if value := r.Header.Get("If-Match"); value != "" {
		if !exists {
			m.writeError(w, http.StatusNotFound, "NoSuchKey", "missing")
			return true
		}
		if value != "*" && normalizeETag(value) != normalizeETag(current.etag) {
			m.writeError(w, http.StatusPreconditionFailed, "PreconditionFailed", "etag changed")
			return true
		}
	}
	return false
}

func (m *mockS3) delete(w http.ResponseWriter, key string) {
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockS3) createUpload(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	id := "upload-" + strconv.Itoa(m.nextID)
	m.lastCreateSawConditions = r.Header.Get("If-Match") != "" || r.Header.Get("If-None-Match") != ""
	m.uploads[id] = &mockUpload{
		key: key, contentType: r.Header.Get("Content-Type"), meta: testMeta(r.Header),
		parts: map[int][]byte{}, etags: map[int]string{},
	}
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
}

func (m *mockS3) uploadPart(w http.ResponseWriter, r *http.Request, key, id string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	number, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
	m.mu.Lock()
	upload, ok := m.uploads[id]
	if !ok || upload.key != key {
		m.mu.Unlock()
		m.writeError(w, http.StatusNotFound, "NoSuchUpload", "missing upload")
		return
	}
	upload.parts[number] = append([]byte(nil), data...)
	etag := testETag(data)
	upload.etags[number] = etag
	m.mu.Unlock()
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (m *mockS3) completeUpload(w http.ResponseWriter, r *http.Request, key, id string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		m.writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	var request struct {
		Parts []tagpart `xml:"Part"`
	}
	if err := xml.Unmarshal(body, &request); err != nil {
		m.writeError(w, http.StatusBadRequest, "MalformedXML", err.Error())
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	upload, ok := m.uploads[id]
	if !ok || upload.key != key {
		m.writeError(w, http.StatusNotFound, "NoSuchUpload", "missing upload")
		return
	}
	upload.ifMatch = r.Header.Get("If-Match")
	upload.ifNoneMatch = r.Header.Get("If-None-Match")
	m.lastCompleteIfMatch = upload.ifMatch
	m.lastCompleteIfNone = upload.ifNoneMatch
	m.lastPartCount = len(request.Parts)
	current, exists := m.objects[key]
	if m.checkConditions(w, r, current, exists) {
		return
	}
	if upload.ifMatch != "" || upload.ifNoneMatch != "" {
		if m.rejectConditionalComplete {
			m.writeError(w, http.StatusNotImplemented, "NotImplemented", "conditions not supported")
			return
		}
	}
	var data []byte
	for _, part := range request.Parts {
		stored, ok := upload.parts[int(part.Num)]
		if !ok || upload.etags[int(part.Num)] != part.ETag {
			m.writeError(w, http.StatusBadRequest, "InvalidPart", "missing part")
			return
		}
		data = append(data, stored...)
	}
	etag := testETag(data)
	m.objects[key] = &mockObject{
		data: data, etag: etag, contentType: upload.contentType, meta: upload.meta,
		modified: time.Now().UTC().Truncate(time.Second),
	}
	delete(m.uploads, id)
	w.Header().Set("Content-Type", "application/xml")
	_, _ = fmt.Fprintf(w, `<CompleteMultipartUploadResult><ETag>%s</ETag></CompleteMultipartUploadResult>`, etag)
}

func (m *mockS3) abortUpload(w http.ResponseWriter, key, id string) {
	m.mu.Lock()
	if upload, ok := m.uploads[id]; ok && upload.key == key {
		delete(m.uploads, id)
	}
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (m *mockS3) list(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listCalls++
	query := r.URL.Query()
	prefix := query.Get("prefix")
	token := query.Get("continuation-token")
	limit := m.maxKeys
	if limit <= 0 {
		limit = 1000
	}
	keys := make([]string, 0, len(m.objects))
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) && (token == "" || key > token) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := struct {
		XMLName               xml.Name         `xml:"ListBucketResult"`
		IsTruncated           bool             `xml:"IsTruncated"`
		EncodingType          string           `xml:"EncodingType"`
		NextContinuationToken string           `xml:"NextContinuationToken,omitempty"`
		Contents              []mockListObject `xml:"Contents"`
	}{EncodingType: "url"}
	last := ""
	for _, key := range keys {
		if len(result.Contents) >= limit {
			result.IsTruncated = true
			break
		}
		obj := m.objects[key]
		contentType := ""
		if m.listContentType {
			contentType = obj.contentType
		}
		result.Contents = append(result.Contents, mockListObject{
			Key:          url.PathEscape(key),
			LastModified: obj.modified.Format(time.RFC3339Nano),
			ETag:         obj.etag,
			Size:         int64(len(obj.data)),
			ContentType:  contentType,
		})
		last = key
	}
	if result.IsTruncated {
		result.NextContinuationToken = last
	}
	if m.badToken {
		result.IsTruncated = true
		if token == "" {
			result.NextContinuationToken = "stuck"
		} else {
			result.NextContinuationToken = token
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	_ = xml.NewEncoder(w).Encode(result)
}

type mockListObject struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	ContentType  string `xml:"ContentType,omitempty"`
}

func (m *mockS3) setHeaders(w http.ResponseWriter, obj *mockObject) {
	w.Header().Set("ETag", obj.etag)
	w.Header().Set("Last-Modified", obj.modified.Format(http.TimeFormat))
	if obj.contentType != "" {
		w.Header().Set("Content-Type", obj.contentType)
	}
	for key, value := range obj.meta {
		w.Header().Set("x-amz-meta-"+key, value)
	}
}

func (m *mockS3) writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: message})
}

func testETag(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

func testMeta(h http.Header) map[string]string {
	meta := map[string]string{}
	for name, values := range h {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-meta-") && len(values) > 0 {
			meta[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
		}
	}
	return meta
}

func parseTestRange(value string, size int64) (start, end int64, ok bool) {
	if !strings.HasPrefix(value, "bytes=") {
		return 0, 0, false
	}
	left, right, found := strings.Cut(strings.TrimPrefix(value, "bytes="), "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(left, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	if right == "" {
		return start, size - 1, true
	}
	end, err = strconv.ParseInt(right, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}

func newTestServer(t *testing.T) (*mockS3, *httptest.Server) {
	t.Helper()
	mock := newMockS3()
	server := httptest.NewServer(mock)
	t.Cleanup(server.Close)
	return mock, server
}

func newTestBucket(t *testing.T, server *httptest.Server, prefix string) *Bucket {
	t.Helper()
	bucket, err := New(Config{
		Endpoint: server.URL, Bucket: "test-bucket", Region: "us-east-1",
		AccessKey: "AKID", SecretKey: "SECRET", PathStyle: true, Prefix: prefix,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return bucket
}
