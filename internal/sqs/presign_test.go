package sqs

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadOnlyAllowed(t *testing.T) {
	for _, op := range []string{"ListQueues", "GetQueueUrl", "GetQueueAttributes", "ListQueueTags"} {
		if !readOnlyAllowed(op) {
			t.Errorf("%s should be allowed read-only", op)
		}
	}
	for _, op := range []string{"SendMessage", "ReceiveMessage", "DeleteMessage", "PurgeQueue", "CreateQueue", "SetQueueAttributes"} {
		if readOnlyAllowed(op) {
			t.Errorf("%s should not be allowed read-only", op)
		}
	}
}

func TestPresignedRequestRejected(t *testing.T) {
	s := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false, 1)
	for _, target := range []string{
		"/?X-Amz-Signature=abc",
		"/?AWSAccessKeyId=ak&Signature=abc&Expires=1",
	} {
		req := httptest.NewRequest("POST", target, strings.NewReader("Action=ListQueues"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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

func TestPresignedFormFieldRejected(t *testing.T) {
	s := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false, 1)
	req := httptest.NewRequest("POST", "/", strings.NewReader(
		"Action=ListQueues&AWSAccessKeyId=ak&Signature=abc&Expires=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != 400 {
		t.Fatalf("got %d, want 400", rec.Code)
	}
}
