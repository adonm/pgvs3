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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestAddressing(t *testing.T) {
	ctx := context.Background()
	virtual, err := New(Config{
		Endpoint: "s3.example.com", Bucket: "my-bucket",
		AccessKey: "AKID", SecretKey: "SECRET", SessionToken: "TOKEN",
	})
	if err != nil {
		t.Fatalf("New virtual-hosted: %v", err)
	}
	req, err := virtual.client.newRequest(ctx, http.MethodHead, "dir/a b+%", nil, nil, -1)
	if err != nil {
		t.Fatalf("virtual newRequest: %v", err)
	}
	if req.URL.Host != "my-bucket.s3.example.com" {
		t.Fatalf("virtual host = %q", req.URL.Host)
	}
	if req.URL.EscapedPath() != "/dir/a%20b%2B%25" {
		t.Fatalf("virtual path = %q", req.URL.EscapedPath())
	}
	if !strings.HasPrefix(req.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=AKID/") {
		t.Fatalf("missing signature: %q", req.Header.Get("Authorization"))
	}
	if req.Header.Get("X-Amz-Content-Sha256") == "" {
		t.Fatal("missing payload hash")
	}
	if req.Header.Get("X-Amz-Security-Token") != "TOKEN" {
		t.Fatalf("security token = %q", req.Header.Get("X-Amz-Security-Token"))
	}
	if !strings.Contains(req.Header.Get("Authorization"), "x-amz-security-token") {
		t.Fatalf("security token is not signed: %q", req.Header.Get("Authorization"))
	}

	pathStyle, err := New(Config{
		Endpoint: "s3.example.com", Bucket: "my-bucket", PathStyle: true,
		AccessKey: "AKID", SecretKey: "SECRET",
	})
	if err != nil {
		t.Fatalf("New path-style: %v", err)
	}
	req, err = pathStyle.client.newRequest(ctx, http.MethodHead, "dir/a b", nil, nil, -1)
	if err != nil {
		t.Fatalf("path-style newRequest: %v", err)
	}
	if req.URL.Host != "s3.example.com" {
		t.Fatalf("path-style host = %q", req.URL.Host)
	}
	if req.URL.EscapedPath() != "/my-bucket/dir/a%20b" {
		t.Fatalf("path-style path = %q", req.URL.EscapedPath())
	}

	if _, err := New(Config{
		Endpoint: "127.0.0.1:9000", Bucket: "my-bucket",
		AccessKey: "AKID", SecretKey: "SECRET",
	}); err == nil {
		t.Fatal("virtual-hosted IP endpoint was accepted")
	}
	if _, err := New(Config{
		Endpoint: "s3.example.com/some/path", Bucket: "my-bucket",
		AccessKey: "AKID", SecretKey: "SECRET",
	}); err == nil {
		t.Fatal("endpoint path was accepted")
	}

	dotted, err := New(Config{
		Bucket: "my.bucket", AccessKey: "AKID", SecretKey: "SECRET",
	})
	if err != nil {
		t.Fatalf("dotted bucket: %v", err)
	}
	req, err = dotted.client.newRequest(ctx, http.MethodHead, "a", nil, nil, -1)
	if err != nil {
		t.Fatalf("dotted newRequest: %v", err)
	}
	if req.URL.Host != "s3.us-east-1.amazonaws.com" || req.URL.EscapedPath() != "/my.bucket/a" {
		t.Fatalf("dotted target = %q", req.URL.String())
	}
}

func TestConfigValidation(t *testing.T) {
	valid := Config{Bucket: "bucket", AccessKey: "AKID", SecretKey: "SECRET"}
	cases := []struct {
		name string
		cfg  Config
	}{
		{"missing bucket", Config{AccessKey: "AKID", SecretKey: "SECRET"}},
		{"invalid bucket", Config{Bucket: "bad/name", AccessKey: "AKID", SecretKey: "SECRET"}},
		{"missing access key", Config{Bucket: "bucket", SecretKey: "SECRET"}},
		{"missing secret", Config{Bucket: "bucket", AccessKey: "AKID"}},
		{"bad scheme", Config{Bucket: "bucket", Endpoint: "ftp://example.com", AccessKey: "AKID", SecretKey: "SECRET"}},
		{"endpoint query", Config{Bucket: "bucket", Endpoint: "https://example.com?x=1", AccessKey: "AKID", SecretKey: "SECRET"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Fatal("config was accepted")
			}
		})
	}
	b, err := New(valid)
	if err != nil {
		t.Fatalf("valid config: %v", err)
	}
	if b.client.signer.region != "us-east-1" || b.client.signer.service != "s3" {
		t.Fatalf("default signer = %+v", b.client.signer)
	}

	//lint:ignore SA1012 verifying nil-context rejection
	if _, err := b.client.newRequest(nil, http.MethodHead, "x", nil, nil, -1); err == nil {
		t.Fatal("nil context was accepted")
	}
	var nilBucket *Bucket
	if err := nilBucket.Close(); err != nil {
		t.Fatalf("nil Close = %v", err)
	}
}

func TestInjectedHTTPClientAndTLS(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"tls-etag"`)
			w.Header().Set("Content-Type", "text/plain")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("Content-Length", "3")
			w.WriteHeader(http.StatusOK)
			return
		}
		_, _ = io.WriteString(w, "abc")
	}))
	defer server.Close()

	injected, err := New(Config{
		Endpoint: server.URL, Bucket: "test-bucket", AccessKey: "AKID", SecretKey: "SECRET",
		PathStyle: true, HTTPClient: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := injected.Stat(context.Background(), "a"); err != nil {
		t.Fatalf("injected TLS client: %v", err)
	}

	insecure, err := New(Config{
		Endpoint: server.URL, Bucket: "test-bucket", AccessKey: "AKID", SecretKey: "SECRET",
		PathStyle: true, Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insecure.Stat(context.Background(), "a"); err != nil {
		t.Fatalf("insecure TLS client: %v", err)
	}

	plainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"plain"`)
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
	}))
	defer plainServer.Close()
	hostPort, err := New(Config{
		Endpoint: strings.TrimPrefix(plainServer.URL, "http://"), Bucket: "test-bucket",
		AccessKey: "AKID", SecretKey: "SECRET", PathStyle: true, Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hostPort.Stat(context.Background(), "a"); err != nil {
		t.Fatalf("host:port endpoint: %v", err)
	}
}

func TestSignerAWSSample(t *testing.T) {
	previous := signTime
	signTime = func() time.Time { return time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC) }
	defer func() { signTime = previous }()

	s := newSigner("us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "")
	date, payload, auth := s.signRequest(nil,
		[]byte("GET"), []byte("/test.txt"), nil,
		[]byte("examplebucket.s3.amazonaws.com"), nil,
		[2]string{"range", "bytes=0-9"},
	)
	if string(date) != "20130524T000000Z" {
		t.Fatalf("date = %q", date)
	}
	const empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if payload != empty {
		t.Fatalf("payload hash = %q", payload)
	}
	want := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if string(auth) != want {
		t.Fatalf("authorization = %q\nwant %q", auth, want)
	}
	if !strings.Contains(string(auth), "SignedHeaders=host;range;") {
		t.Fatalf("authorization headers = %q", auth)
	}
}

func TestSignerBodyAndToken(t *testing.T) {
	previous := signTime
	signTime = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	defer func() { signTime = previous }()

	s := newSigner("eu-west-1", "AKID", "SECRET", "TOKEN")
	date, payload, auth := s.signRequest(nil,
		[]byte("PUT"), []byte("/bucket/key"), []byte("a=1"),
		[]byte("bucket.example.com"), []byte{},
		[2]string{"x-amz-meta-owner", "alice"},
	)
	if string(date) != "20260102T030405Z" {
		t.Fatalf("date = %q", date)
	}
	if payload != "UNSIGNED-PAYLOAD" {
		t.Fatalf("payload hash = %q", payload)
	}
	for _, want := range []string{
		"Credential=AKID/20260102/eu-west-1/s3/aws4_request",
		"SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-meta-owner;x-amz-security-token",
		"Signature=",
	} {
		if !strings.Contains(string(auth), want) {
			t.Fatalf("authorization %q does not contain %q", auth, want)
		}
	}

	first := append([]byte(nil), s.pickKey(time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC))...)
	second := append([]byte(nil), s.pickKey(time.Date(2026, 1, 2, 23, 0, 0, 0, time.UTC))...)
	if string(first) != string(second) {
		t.Fatal("derived key changed within the same day")
	}
	nextDay := s.pickKey(time.Date(2026, 1, 3, 0, 0, 1, 0, time.UTC))
	if string(first) == string(nextDay) {
		t.Fatal("derived key did not change on the next day")
	}
}

func TestPublicSignerMatchesFastPath(t *testing.T) {
	previous := signTime
	signTime = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	defer func() { signTime = previous }()

	const region, access, secret = "eu-west-1", "AKID", "SECRET"
	req, err := http.NewRequest(http.MethodPut,
		"https://bucket.example.com/bucket/key?a=1", bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := NewSigner(region, access, secret, "").Sign(req, []byte{}); err != nil {
		t.Fatal(err)
	}
	date, payload, auth := newSigner(region, access, secret, "").signRequest(nil,
		[]byte(http.MethodPut), []byte("/bucket/key"), []byte("a=1"),
		[]byte("bucket.example.com"), []byte{})
	if got := req.Header.Get("X-Amz-Date"); got != string(date) {
		t.Fatalf("date = %q, want %q", got, date)
	}
	if got := req.Header.Get("X-Amz-Content-Sha256"); got != payload {
		t.Fatalf("payload hash = %q, want %q", got, payload)
	}
	if got := req.Header.Get("Authorization"); got != string(auth) {
		t.Fatalf("authorization = %q, want %q", got, auth)
	}
}
