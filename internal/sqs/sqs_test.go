package sqs

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseFormValuesAndDecodeBatch(t *testing.T) {
	vals, err := parseFormValues("Version=2012-11-05", []byte(
		"Action=ChangeMessageVisibilityBatch"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.Id=a"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle=r1"+
			"&ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout=15"))
	if err != nil {
		t.Fatal(err)
	}
	if got := vals.Get("Action"); got != "ChangeMessageVisibilityBatch" {
		t.Fatalf("Action=%q", got)
	}
	entries, apiErr := decodeBatchEntries(formIn(vals), "ChangeMessageVisibilityBatchRequestEntry")
	if apiErr != nil || len(entries) != 1 || entries[0].ID != "a" || entries[0].ReceiptHandle != "r1" || entries[0].VisibilityTimeout != 15 {
		t.Fatalf("entries=%+v err=%v", entries, apiErr)
	}
}

func TestXMLResponseMarshaling(t *testing.T) {
	shapes := []any{
		createQueueResponse{Xmlns: sqsNS, QueueUrl: "http://127.0.0.1:8014/q"},
		getQueueURLResponse{Xmlns: sqsNS, QueueUrl: "http://127.0.0.1:8014/q"},
		sendBatchResponse{Xmlns: sqsNS, Successful: []xmlSendBatchResult{{Id: "1", MessageId: "m1", MD5OfMessageBody: "abc"}}},
		deleteBatchResponse{Xmlns: sqsNS, Successful: []xmlBatchOK{{Id: "1"}}, Failed: []xmlBatchResultError{{Id: "2", Code: "ReceiptHandleIsInvalid", SenderFault: true}}},
		changeVisibilityBatchResponse{Xmlns: sqsNS, Successful: []xmlBatchOK{{Id: "1"}}},
		emptyOK("DeleteMessage", formIn(nil)),
	}
	for _, v := range shapes {
		if _, err := xml.Marshal(v); err != nil {
			t.Fatalf("marshal %T: %v", v, err)
		}
	}
}

func TestWaitChWakeup(t *testing.T) {
	s := New(nil, nil, false, 2)
	ch := s.waitCh("jobs")
	select {
	case <-ch:
		t.Fatal("waitCh closed before notify")
	default:
	}
	s.notify("jobs")
	select {
	case <-ch:
	default:
		t.Fatal("waitCh did not close on notify")
	}
}

func TestQueryProtocolFormBodyAndReadOnly(t *testing.T) {
	s := New(nil, nil, false, 2)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListQueueTags&QueueUrl=http://127.0.0.1/q"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<ListQueueTagsResponse") {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	ro := New(nil, nil, true, 2)
	req2 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=SendMessage&QueueUrl=http://127.0.0.1/q&MessageBody=x"))
	req2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec2 := httptest.NewRecorder()
	ro.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusForbidden {
		t.Fatalf("expected 403 on read-only SQS send, got %d", rec2.Code)
	}

	// Metadata-only operations remain available on a read-only gateway.
	req3 := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListQueueTags&QueueUrl=http://127.0.0.1/q"))
	req3.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec3 := httptest.NewRecorder()
	ro.ServeHTTP(rec3, req3)
	if rec3.Code != http.StatusOK || !strings.Contains(rec3.Body.String(), "<ListQueueTagsResponse") {
		t.Fatalf("read-only metadata: code=%d body=%s", rec3.Code, rec3.Body.String())
	}
}
