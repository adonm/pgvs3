package reqguard

import (
	"net/url"
	"testing"
)

func TestPresigned(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{"empty", "", false},
		{"header auth markers", "X-Amz-Date=20260101T000000Z&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD", false},
		{"sigv4 signature", "X-Amz-Signature=abc", true},
		{"sigv4 full", "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak%2Fdate%2Fregion%2Fs3%2Faws4_request&X-Amz-Signature=abc", true},
		{"sigv4a", "X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=ak%2Fdate%2Fs3%2Faws4_request&X-Amz-Signature=abc", true},
		{"algorithm alone", "X-Amz-Algorithm=AWS4-HMAC-SHA256", true},
		{"credential alone", "X-Amz-Credential=ak", true},
		{"legacy sigv2", "AWSAccessKeyId=ak&Expires=1&Signature=abc", true},
		{"legacy key without signature", "AWSAccessKeyId=ak", false},
		{"case insensitive", "x-amz-signature=abc", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := url.ParseQuery(tt.query)
			if err != nil {
				t.Fatal(err)
			}
			if got := Presigned(q); got != tt.want {
				t.Fatalf("Presigned(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}
