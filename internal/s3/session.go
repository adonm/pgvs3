package s3

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SessionIssuer creates the short-lived, bucket-scoped credentials used by the
// S3 Express One Zone zonal API. Tokens are stateless and HMAC-signed so every
// gateway can validate them without a shared session table.
type SessionIssuer struct {
	master []byte
	ttl    time.Duration
}

type Session struct {
	AccessKey string
	SecretKey string
	Token     string
	Bucket    string
	Mode      string
	Expiry    time.Time
}

func NewSessionIssuer(master []byte, ttl time.Duration) *SessionIssuer {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &SessionIssuer{master: master, ttl: ttl}
}

func (s *SessionIssuer) Create(bucket, mode string) Session {
	if mode != "ReadOnly" {
		mode = "ReadWrite"
	}
	exp := time.Now().Add(s.ttl)
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	payload := fmt.Sprintf("v1|%s|%s|%d|%s", bucket, mode, exp.Unix(), hex.EncodeToString(nonce[:]))
	return s.sessionFor(payload, bucket, mode, exp)
}

func (s *SessionIssuer) sessionFor(payload, bucket, mode string, exp time.Time) Session {
	ak := "ASIA" + strings.ToUpper(hex.EncodeToString(s.mac("ak|" + payload)))[:16]
	sk := base64.RawURLEncoding.EncodeToString(s.mac("sk|" + payload))
	token := base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." +
		base64.RawURLEncoding.EncodeToString(s.mac("token|"+payload))
	return Session{AccessKey: ak, SecretKey: sk, Token: token, Bucket: bucket, Mode: mode, Expiry: exp}
}

func (s *SessionIssuer) Validate(token, bucket string) (*Session, error) {
	enc, sig, ok := strings.Cut(token, ".")
	if !ok {
		return nil, fmt.Errorf("malformed session token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return nil, fmt.Errorf("malformed session token")
	}
	want, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return nil, fmt.Errorf("malformed session token")
	}
	if !hmac.Equal(want, s.mac("token|"+string(payload))) {
		return nil, fmt.Errorf("invalid session signature")
	}
	parts := strings.Split(string(payload), "|")
	if len(parts) != 5 || parts[0] != "v1" {
		return nil, fmt.Errorf("invalid session payload")
	}
	if parts[1] != bucket {
		return nil, fmt.Errorf("session is scoped to another bucket")
	}
	if parts[2] != "ReadOnly" && parts[2] != "ReadWrite" {
		return nil, fmt.Errorf("invalid session mode")
	}
	unix, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || time.Now().Unix() >= unix {
		return nil, fmt.Errorf("session expired")
	}
	return &Session{
		AccessKey: "ASIA" + strings.ToUpper(hex.EncodeToString(s.mac("ak|" + string(payload))))[:16],
		SecretKey: base64.RawURLEncoding.EncodeToString(s.mac("sk|" + string(payload))),
		Token:     token, Bucket: parts[1], Mode: parts[2],
		Expiry: time.Unix(unix, 0),
	}, nil
}

func (s *SessionIssuer) mac(data string) []byte {
	m := hmac.New(sha256.New, s.master)
	m.Write([]byte(data))
	return m.Sum(nil)
}
