package s3

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPresignedRequestRejected(t *testing.T) {
	s := &Server{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		metrics: newMetrics(),
	}
	for _, target := range []string{
		"/bucket/key?X-Amz-Signature=abc",
		"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ak",
		"/bucket/key?AWSAccessKeyId=ak&Signature=abc&Expires=1",
	} {
		req := httptest.NewRequest("GET", target, nil)
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		if rec.Code != 400 {
			t.Fatalf("%s: got %d, want 400", target, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "InvalidRequest") {
			t.Fatalf("%s: body %q does not contain InvalidRequest", target, rec.Body.String())
		}
	}
}
