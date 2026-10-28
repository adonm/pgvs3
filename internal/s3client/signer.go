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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	longFormat  = "20060102T150405Z"
	shortFormat = "20060102"
)

var signTime = time.Now

type signer struct {
	region    string
	service   string
	accessKey string
	secret    string
	token     string

	mu    sync.Mutex
	cache *signingCache
}

type signingCache struct {
	year      int
	month     time.Month
	day       int
	accessKey string
	secret    string
	token     string
	region    string
	service   string
	key       [sha256.Size]byte
}

func newSigner(region, accessKey, secret, token string) *signer {
	return &signer{
		region: region, service: "s3",
		accessKey: accessKey, secret: secret, token: token,
	}
}

func canonicalValue(dst *bytes.Buffer, value string) {
	value = strings.TrimSpace(value)
	for {
		at := strings.IndexByte(value, ' ')
		if at < 0 {
			dst.WriteString(value)
			return
		}
		dst.WriteString(value[:at])
		dst.WriteByte(' ')
		value = strings.TrimLeft(value[at:], " ")
	}
}

func (s *signer) toscope(dst *bytes.Buffer, date []byte) {
	dst.Write(date)
	dst.WriteByte('/')
	dst.WriteString(s.region)
	dst.WriteByte('/')
	dst.WriteString(s.service)
	dst.WriteString("/aws4_request")
}

func (s *signer) tosign(dst *bytes.Buffer, stamp, reqhash []byte) {
	dst.WriteString("AWS4-HMAC-SHA256\n")
	dst.Write(stamp)
	dst.WriteByte('\n')
	s.toscope(dst, stamp[:8])
	dst.WriteByte('\n')
	dst.Write(reqhash)
}

func (s *signer) signRequest(dst, method, path, query, host, body []byte, extra ...[2]string) (date []byte, payloadHash string, authorization []byte) {
	buf := bytes.NewBuffer(dst[:0])
	now := signTime().UTC()
	var stampStorage [len(longFormat)]byte
	stamp := now.AppendFormat(stampStorage[:0], longFormat)
	payloadHash = "UNSIGNED-PAYLOAD"
	if body == nil {
		payloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	}
	var headerStorage [16][2]string
	headers := headerStorage[:3]

	headers[0] = [2]string{"host", ""}
	headers[1] = [2]string{"x-amz-content-sha256", payloadHash}
	headers[2] = [2]string{"x-amz-date", ""}
	if s.token != "" {
		headers = append(headers, [2]string{"x-amz-security-token", s.token})
	}
	for _, header := range extra {
		if header[1] != "" {
			headers = append(headers, header)
		}
	}
	if len(extra) > 0 {
		slices.SortFunc(headers, func(a, b [2]string) int { return strings.Compare(a[0], b[0]) })
	}

	buf.Write(method)
	buf.WriteByte('\n')
	if len(path) == 0 {
		buf.WriteByte('/')
	} else {
		buf.Write(path)
	}
	buf.WriteByte('\n')
	buf.Write(query)
	buf.WriteByte('\n')
	for _, header := range headers {
		buf.WriteString(header[0])
		buf.WriteByte(':')
		switch header[0] {
		case "host":
			buf.Write(host)
		case "x-amz-date":
			buf.Write(stamp)
		default:
			canonicalValue(buf, header[1])
		}
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	for i, header := range headers {
		if i > 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(header[0])
	}
	buf.WriteByte('\n')
	buf.WriteString(payloadHash)

	var hexbuf [2 * sha256.Size]byte
	h := sha256.Sum256(buf.Bytes())
	buf.Reset()
	var reqhash [2 * sha256.Size]byte
	hex.Encode(reqhash[:], h[:])
	s.tosign(buf, stamp, reqhash[:])
	s.sign(buf.Bytes(), hexbuf[:], now)

	buf.Reset()
	buf.Write(stamp)
	buf.WriteString("AWS4-HMAC-SHA256 Credential=")
	buf.WriteString(s.accessKey)
	buf.WriteByte('/')
	s.toscope(buf, stamp[:8])
	buf.WriteString(", SignedHeaders=")
	for i, header := range headers {
		if i > 0 {
			buf.WriteByte(';')
		}
		buf.WriteString(header[0])
	}
	buf.WriteString(", Signature=")
	buf.Write(hexbuf[:])
	result := buf.Bytes()
	return result[:len(stamp)], payloadHash, result[len(stamp):]
}

func macinto(key, mem []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(mem)
	return h.Sum(key[:0])
}

func derive(secret string, when time.Time, region, service string) []byte {
	datestr := when.Format(shortFormat)
	k := []byte("AWS4" + secret)
	k = macinto(k, []byte(datestr))
	k = macinto(k, []byte(region))
	k = macinto(k, []byte(service))
	k = macinto(k, []byte("aws4_request"))
	return k
}

func (s *signer) pickKey(when time.Time) []byte {
	when = when.UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.cache; c != nil && c.year == when.Year() && c.month == when.Month() && c.day == when.Day() &&
		c.accessKey == s.accessKey && c.secret == s.secret && c.token == s.token &&
		c.region == s.region && c.service == s.service {
		return c.key[:]
	}
	key := derive(s.secret, when, s.region, s.service)
	c := &signingCache{
		year: when.Year(), month: when.Month(), day: when.Day(),
		accessKey: s.accessKey, secret: s.secret, token: s.token,
		region: s.region, service: s.service,
	}
	copy(c.key[:], key)
	s.cache = c
	return c.key[:]
}

func (s *signer) sign(src, dst []byte, when time.Time) {
	var pad [sha256.BlockSize]byte

	copy(pad[:], s.pickKey(when))
	for i := range pad {
		pad[i] ^= 0x36
	}
	h := sha256.New()
	h.Write(pad[:])
	h.Write(src)
	var sum [sha256.Size]byte
	inner := h.Sum(sum[:0])
	for i := range pad {
		pad[i] ^= 0x36 ^ 0x5c
	}
	h.Reset()
	h.Write(pad[:])
	h.Write(inner)
	hex.Encode(dst, h.Sum(sum[:0]))
}

type Signer struct {
	impl *signer
}

func NewSigner(region, accessKey, secret, token string) *Signer {
	return &Signer{impl: newSigner(region, accessKey, secret, token)}
}

func (s *Signer) Sign(req *http.Request, body []byte) error {
	if s == nil || s.impl == nil {
		return errors.New("s3client: nil signer")
	}
	if req == nil || req.URL == nil {
		return errNilRequest
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	query := canonicalQuery(req.URL.Query())
	req.URL.RawQuery = query
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	var marker []byte
	if body != nil {
		marker = []byte{}
	}
	date, payloadHash, auth := s.impl.signRequest(
		nil, []byte(req.Method), []byte(path), []byte(query), []byte(host), marker,
	)
	req.Header.Set("X-Amz-Date", string(date))
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if s.impl.token != "" {
		req.Header.Set("X-Amz-Security-Token", s.impl.token)
	}
	req.Header.Set("Authorization", string(auth))
	return nil
}

var errNilRequest = errors.New("s3client: nil request")
