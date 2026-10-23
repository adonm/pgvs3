//go:build integration

package sqs_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	pgvs3sqs "github.com/adonm/pgvs3/internal/sqs"
	"github.com/adonm/pgvs3/internal/store"
	"github.com/adonm/pgvs3/internal/testutil"
)

var (
	testStore *store.Store
	testCtx   = context.Background()
)

func TestMain(m *testing.M) {
	url, cleanup, err := testutil.Postgres(testCtx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		os.Exit(1)
	}
	code := 1
	defer func() {
		cleanup()
		os.Exit(code)
	}()
	st, err := store.Open(testCtx, url, 8, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "open store:", err)
		return
	}
	defer st.Close()
	if err := st.Init(testCtx); err != nil {
		fmt.Fprintln(os.Stderr, "init schema:", err)
		return
	}
	testStore = st
	code = m.Run()
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func newClient(t *testing.T, readOnly bool) *awssqs.Client {
	t.Helper()
	srv := httptest.NewServer(pgvs3sqs.New(testStore,
		slog.New(slog.NewTextHandler(io.Discard, nil)), readOnly, 4))
	t.Cleanup(srv.Close)
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}
	return awssqs.NewFromConfig(cfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

func createQueue(t *testing.T, client *awssqs.Client, name string, attrs map[string]string) string {
	t.Helper()
	out, err := client.CreateQueue(testCtx, &awssqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return aws.ToString(out.QueueUrl)
}

func TestStandardQueueRoundTrip(t *testing.T) {
	client := newClient(t, false)
	url := createQueue(t, client, unique("std"), nil)
	if _, err := client.SendMessage(testCtx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("hello"),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := client.ReceiveMessage(testCtx, &awssqs.ReceiveMessageInput{
		QueueUrl: aws.String(url), MaxNumberOfMessages: 10,
	})
	if err != nil || len(out.Messages) != 1 || aws.ToString(out.Messages[0].Body) != "hello" {
		t.Fatalf("receive: %+v err=%v", out, err)
	}
	if _, err := client.DeleteMessage(testCtx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(url), ReceiptHandle: out.Messages[0].ReceiptHandle,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFIFOQueueThroughSDK(t *testing.T) {
	client := newClient(t, false)
	url := createQueue(t, client, unique("fifo")+".fifo", map[string]string{
		"FifoQueue": "true", "ContentBasedDeduplication": "true",
	})
	send := func(body, group, dedup string) *awssqs.SendMessageOutput {
		t.Helper()
		out, err := client.SendMessage(testCtx, &awssqs.SendMessageInput{
			QueueUrl: aws.String(url), MessageBody: aws.String(body),
			MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup),
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := send("a1", "g", "d1")
	dup := send("duplicate", "g", "d1")
	if aws.ToString(first.MessageId) != aws.ToString(dup.MessageId) {
		t.Fatalf("dedup IDs differ: %q vs %q", aws.ToString(first.MessageId), aws.ToString(dup.MessageId))
	}
	if aws.ToString(first.SequenceNumber) == "" {
		t.Fatal("missing sequence number")
	}
	send("a2", "g", "d2")
	send("b1", "h", "d3")

	recv := func() []types.Message {
		t.Helper()
		out, err := client.ReceiveMessage(testCtx, &awssqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10,
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range out.Messages {
			if aws.ToString(m.ReceiptHandle) == "" {
				t.Fatalf("missing receipt: %+v", m)
			}
		}
		return out.Messages
	}
	batch := recv()
	if len(batch) != 2 || aws.ToString(batch[0].Body) != "a1" || aws.ToString(batch[1].Body) != "b1" {
		t.Fatalf("first batch = %+v, want [a1 b1]", batch)
	}
	// a2 stays blocked while a1 is in flight.
	if next := recv(); len(next) != 0 {
		t.Fatalf("a2 was delivered while a1 was in flight: %+v", next)
	}
	if _, err := client.DeleteMessage(testCtx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(url), ReceiptHandle: batch[0].ReceiptHandle,
	}); err != nil {
		t.Fatal(err)
	}
	next := recv()
	if len(next) != 1 || aws.ToString(next[0].Body) != "a2" {
		t.Fatalf("after deleting a1 got %+v, want a2", next)
	}
}

func TestReadOnlyQueueMetadata(t *testing.T) {
	client := newClient(t, true)
	if _, err := client.ListQueues(testCtx, &awssqs.ListQueuesInput{}); err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	url := createQueue(t, newClient(t, false), unique("ro"), nil)
	if _, err := client.SendMessage(testCtx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(url), MessageBody: aws.String("x"),
	}); err == nil {
		t.Fatal("read-only SendMessage succeeded")
	}
}
