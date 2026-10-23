package s3

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func headerRequest(h map[string]string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, "/bucket/key", nil)
	for k, v := range h {
		req.Header.Set(k, v)
	}
	return req
}

func TestValidateWriteHeaders(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    int // 0 means accepted
	}{
		{"plain", nil, 0},
		{"sse aes256", map[string]string{"x-amz-server-side-encryption": "AES256"}, 0},
		{"sse kms", map[string]string{"x-amz-server-side-encryption": "aws:kms"}, http.StatusNotImplemented},
		{"sse kms key id", map[string]string{"x-amz-server-side-encryption-aws-kms-key-id": "k"}, http.StatusNotImplemented},
		{"sse c", map[string]string{"x-amz-server-side-encryption-customer-algorithm": "AES256"}, http.StatusNotImplemented},
		{"acl private", map[string]string{"x-amz-acl": "private"}, 0},
		{"acl public", map[string]string{"x-amz-acl": "public-read"}, http.StatusNotImplemented},
		{"grant", map[string]string{"x-amz-grant-full-control": "id=x"}, http.StatusNotImplemented},
		{"object lock", map[string]string{"x-amz-object-lock-mode": "COMPLIANCE"}, http.StatusNotImplemented},
		{"legal hold", map[string]string{"x-amz-object-lock-legal-hold": "ON"}, http.StatusNotImplemented},
		{"tagging", map[string]string{"x-amz-tagging": "a=b"}, http.StatusNotImplemented},
		{"storage standard", map[string]string{"x-amz-storage-class": "STANDARD"}, 0},
		{"storage glacier", map[string]string{"x-amz-storage-class": "GLACIER"}, http.StatusNotImplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWriteHeaders(headerRequest(tt.headers))
			if tt.want == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var ae *APIError
			if !errors.As(err, &ae) || ae.Status != tt.want {
				t.Fatalf("got %v, want status %d", err, tt.want)
			}
		})
	}
}
