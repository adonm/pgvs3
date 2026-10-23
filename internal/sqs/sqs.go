// Package sqs implements the subset of the Amazon SQS API used by AWS SDKs:
// the JSON (AmazonSQS.*) protocol and the legacy Query (Action=) protocol.
package sqs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

const maxBody = 1 << 20

// Server is the SQS HTTP front end.
type Server struct {
	store    *store.Store
	log      *slog.Logger
	readOnly bool
	sem      chan struct{}
}

func New(st *store.Store, log *slog.Logger, readOnly bool, maxConc int) *Server {
	if maxConc <= 0 {
		maxConc = 16
	}
	return &Server{store: st, log: log, readOnly: readOnly, sem: make(chan struct{}, maxConc)}
}

// IsSQS reports whether r targets the SQS API.
func IsSQS(r *http.Request) bool {
	if strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.") {
		return true
	}
	if r.Method == http.MethodPost && r.URL.Path == "/" &&
		strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		return true
	}
	return false
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-r.Context().Done():
		http.Error(w, "canceled", http.StatusRequestTimeout)
		return
	}
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "InvalidAction", "Only POST is supported")
		return
	}
	if s.readOnly {
		s.writeErr(w, r, http.StatusForbidden, "AccessDenied", "gateway is read-only")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", "could not read body")
		return
	}
	target := r.Header.Get("X-Amz-Target")
	if target != "" {
		op := strings.TrimPrefix(target, "AmazonSQS.")
		var in map[string]json.RawMessage
		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", "malformed JSON")
				return
			}
		}
		out, apiErr := s.dispatch(r.Context(), r, op, jsonIn(in))
		if apiErr != nil {
			s.writeErr(w, r, apiErr.status, apiErr.code, apiErr.message)
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(http.StatusOK)
		if out != nil {
			_ = json.NewEncoder(w).Encode(out)
		}
		return
	}
	if err := r.ParseForm(); err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", "malformed form body")
		return
	}
	op := r.Form.Get("Action")
	out, apiErr := s.dispatch(r.Context(), r, op, formIn(r.Form))
	if apiErr != nil {
		s.writeErr(w, r, apiErr.status, apiErr.code, apiErr.message)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

// ---- input decoding ---------------------------------------------------------

type input struct {
	form url.Values
	json map[string]json.RawMessage
}

func (in input) str(name string) string {
	if in.json != nil {
		var s string
		_ = json.Unmarshal(in.json[name], &s)
		return s
	}
	return in.form.Get(name)
}

func (in input) int(name string, def int) int {
	if in.json != nil {
		var v int
		if raw, ok := in.json[name]; ok {
			if json.Unmarshal(raw, &v) == nil {
				return v
			}
			var s string
			if json.Unmarshal(raw, &s) == nil {
				if n, err := strconv.Atoi(s); err == nil {
					return n
				}
			}
		}
		return def
	}
	if v := in.form.Get(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func (in input) raw(name string) json.RawMessage { return in.json[name] }

func jsonIn(m map[string]json.RawMessage) input { return input{json: m} }
func formIn(v url.Values) input                 { return input{form: v} }

// ---- errors -----------------------------------------------------------------

type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func errBad(msg string) *apiError {
	return &apiError{http.StatusBadRequest, "InvalidParameterValue", msg}
}
func errNoQueue() *apiError {
	return &apiError{http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist."}
}
func errReceipt() *apiError {
	return &apiError{http.StatusBadRequest, "ReceiptHandleIsInvalid", "The receipt handle is invalid."}
}
func errUnsupported(op string) *apiError {
	return &apiError{http.StatusBadRequest, "InvalidAction", "unsupported action " + op}
}

type redrivePolicy struct {
	Target string
	Max    int
}

// parseRedrive parses a RedrivePolicy attribute value.
func parseRedrive(raw string) *redrivePolicy {
	if raw == "" {
		return nil
	}
	var p struct {
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
		MaxReceiveCount     any    `json:"maxReceiveCount"`
	}
	if json.Unmarshal([]byte(raw), &p) != nil {
		return nil
	}
	target := p.DeadLetterTargetArn
	if i := strings.LastIndexByte(target, ':'); i >= 0 {
		target = target[i+1:]
	}
	max := 0
	switch v := p.MaxReceiveCount.(type) {
	case float64:
		max = int(v)
	case string:
		max = atoi(v)
	}
	if target == "" || max < 1 {
		return nil
	}
	return &redrivePolicy{Target: target, Max: max}
}

// validateRedrive checks a RedrivePolicy attribute against the catalog.
func (s *Server) validateRedrive(ctx context.Context, attrs map[string]any) *apiError {
	raw, ok := attrs["RedrivePolicy"].(string)
	if !ok || raw == "" {
		return nil
	}
	p := parseRedrive(raw)
	if p == nil || p.Max > 1000 {
		return errBad("invalid RedrivePolicy")
	}
	ok, err := s.store.NamespaceExists(ctx, ns.Queue, p.Target)
	if err != nil {
		return errBad(err.Error())
	}
	if !ok {
		return errBad("dead-letter target queue does not exist")
	}
	return nil
}

func (s *Server) writeErr(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if strings.HasPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.") {
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		w.WriteHeader(status)
		shape := code
		switch code {
		case "AWS.SimpleQueueService.NonExistentQueue":
			shape = "QueueDoesNotExist"
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"__type": "com.amazonaws.sqs#" + shape, "message": msg,
		})
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(ErrorResponse{
		Error:     ErrorBody{Type: "Sender", Code: code, Message: msg},
		RequestID: "pgvs3",
	})
}

type ErrorResponse struct {
	XMLName   xml.Name  `xml:"ErrorResponse"`
	Error     ErrorBody `xml:"Error"`
	RequestID string    `xml:"RequestId"`
}

type ErrorBody struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// ---- queue helpers ----------------------------------------------------------

func queueFromURL(raw, host string) (string, *apiError) {
	if raw == "" {
		return "", errBad("QueueUrl is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", errBad("invalid QueueUrl")
	}
	if host != "" && u.Host != "" && u.Host != host {
		// SDKs may build the URL from the endpoint we returned; accept any host
		// as long as the path names a queue.
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return "", errBad("invalid QueueUrl")
	}
	return parts[len(parts)-1], nil
}

func queueURL(r *http.Request, name string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/%s", scheme, r.Host, name)
}

func defaultAttrs() map[string]any {
	return map[string]any{
		"VisibilityTimeout":             "30",
		"DelaySeconds":                  "0",
		"MaximumMessageSize":            "262144",
		"MessageRetentionPeriod":        "345600",
		"ReceiveMessageWaitTimeSeconds": "0",
	}
}

func (s *Server) queueAttrs(ctx context.Context, name string) (map[string]string, error) {
	out := map[string]string{}
	for k, v := range defaultAttrs() {
		out[k] = v.(string)
	}
	infos, err := s.store.Namespaces(ctx, ns.Queue)
	if err != nil {
		return nil, err
	}
	for _, n := range infos {
		if n.Name != name {
			continue
		}
		for k, v := range n.Attrs {
			if sv, ok := v.(string); ok {
				out[k] = sv
			} else if fv, ok := v.(float64); ok {
				out[k] = strconv.FormatInt(int64(fv), 10)
			}
		}
		return out, nil
	}
	return nil, errNoQueue()
}

func (s *Server) attrInt(ctx context.Context, name, key string, def int) int {
	attrs, err := s.queueAttrs(ctx, name)
	if err != nil {
		return def
	}
	n, err := strconv.Atoi(attrs[key])
	if err != nil {
		return def
	}
	return n
}

// ---- dispatch ---------------------------------------------------------------

func (s *Server) dispatch(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
	switch op {
	case "CreateQueue":
		name := in.str("QueueName")
		if !ns.ValidName(name) {
			return nil, errBad("invalid queue name")
		}
		attrs := map[string]any{}
		if in.json != nil && len(in.raw("Attributes")) > 0 {
			_ = json.Unmarshal(in.raw("Attributes"), &attrs)
		} else if in.form != nil {
			for k, v := range in.form {
				if strings.HasPrefix(k, "Attribute.") && strings.HasSuffix(k, ".Name") {
					idx := strings.TrimSuffix(strings.TrimPrefix(k, "Attribute."), ".Name")
					attrs[v[0]] = in.form.Get("Attribute." + idx + ".Value")
				}
			}
		}
		for k, v := range defaultAttrs() {
			if _, ok := attrs[k]; !ok {
				attrs[k] = v
			}
		}
		if apiErr := s.validateRedrive(ctx, attrs); apiErr != nil {
			return nil, apiErr
		}
		if err := s.store.EnsureQueue(ctx, name, attrs); err != nil {
			return nil, errBad(err.Error())
		}
		return map[string]string{"QueueUrl": queueURL(r, name)}, nil

	case "GetQueueUrl":
		name := in.str("QueueName")
		ok, err := s.store.NamespaceExists(ctx, ns.Queue, name)
		if err != nil {
			return nil, errBad(err.Error())
		}
		if !ok {
			return nil, errNoQueue()
		}
		return map[string]string{"QueueUrl": queueURL(r, name)}, nil

	case "ListQueues":
		prefix := in.str("QueueNamePrefix")
		infos, err := s.store.Namespaces(ctx, ns.Queue)
		if err != nil {
			return nil, errBad(err.Error())
		}
		var urls []string
		for _, q := range infos {
			if prefix == "" || strings.HasPrefix(q.Name, prefix) {
				urls = append(urls, queueURL(r, q.Name))
			}
		}
		if in.json != nil {
			return map[string]any{"QueueUrls": urls}, nil
		}
		return listQueuesResponse{Xmlns: sqsNS, QueueUrls: urls}, nil

	case "DeleteQueue":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		if _, err := s.store.DropNamespace(ctx, ns.Queue, name, true); err != nil {
			return nil, errBad(err.Error())
		}
		return map[string]any{}, nil

	case "PurgeQueue":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		if err := s.store.PurgeQueue(ctx, name); err != nil {
			return nil, errBad(err.Error())
		}
		return map[string]any{}, nil

	case "SendMessage":
		return s.sendOne(ctx, r, in)

	case "SendMessageBatch":
		return s.sendBatch(ctx, r, in)

	case "ReceiveMessage":
		return s.receive(ctx, r, in)

	case "DeleteMessage":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		if _, err := s.store.DeleteMessage(ctx, name, in.str("ReceiptHandle")); err != nil {
			if errors.Is(err, store.ErrReceiptStale) || errors.Is(err, store.ErrReceiptInvalid) {
				return nil, errReceipt()
			}
			return nil, errBad(err.Error())
		}
		return map[string]any{}, nil

	case "DeleteMessageBatch":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		var entries []batchEntry
		if in.json != nil {
			_ = json.Unmarshal(in.raw("Entries"), &entries)
		} else {
			for i := 1; ; i++ {
				id := in.form.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i))
				if id == "" {
					break
				}
				entries = append(entries, batchEntry{
					ID: id, ReceiptHandle: in.form.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i)),
				})
			}
		}
		ok := make([]map[string]string, 0, len(entries))
		for _, e := range entries {
			if _, err := s.store.DeleteMessage(ctx, name, e.ReceiptHandle); err != nil {
				return nil, errBad(err.Error())
			}
			ok = append(ok, map[string]string{"Id": e.ID})
		}
		return map[string]any{"Successful": ok, "Failed": []any{}}, nil

	case "ChangeMessageVisibility":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		vis := in.int("VisibilityTimeout", 30)
		_, err := s.store.ChangeVisibility(ctx, name, in.str("ReceiptHandle"), vis)
		if errors.Is(err, store.ErrReceiptStale) || errors.Is(err, store.ErrReceiptInvalid) {
			return nil, errReceipt()
		}
		if err != nil {
			return nil, errBad(err.Error())
		}
		return map[string]any{}, nil

	case "ChangeMessageVisibilityBatch":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		var entries []struct {
			ID                string `json:"Id"`
			ReceiptHandle     string `json:"ReceiptHandle"`
			VisibilityTimeout int    `json:"VisibilityTimeout"`
		}
		if in.json != nil {
			_ = json.Unmarshal(in.raw("Entries"), &entries)
		}
		ok := make([]map[string]string, 0, len(entries))
		for _, e := range entries {
			if _, err := s.store.ChangeVisibility(ctx, name, e.ReceiptHandle, e.VisibilityTimeout); err != nil {
				return nil, errBad(err.Error())
			}
			ok = append(ok, map[string]string{"Id": e.ID})
		}
		return map[string]any{"Successful": ok, "Failed": []any{}}, nil

	case "GetQueueAttributes":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		attrs, err := s.queueAttrs(ctx, name)
		if err != nil {
			if ae, ok := err.(*apiError); ok {
				return nil, ae
			}
			return nil, errBad(err.Error())
		}
		visible, inflight, delayed, err := s.store.QueueCounts(ctx, name)
		if err != nil {
			return nil, errBad(err.Error())
		}
		attrs["ApproximateNumberOfMessages"] = strconv.FormatInt(visible, 10)
		attrs["ApproximateNumberOfMessagesNotVisible"] = strconv.FormatInt(inflight, 10)
		attrs["ApproximateNumberOfMessagesDelayed"] = strconv.FormatInt(delayed, 10)
		attrs["QueueArn"] = "arn:aws:sqs:us-east-1:000000000000:" + name
		attrs["CreatedTimestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
		if in.json != nil {
			return map[string]any{"Attributes": attrs}, nil
		}
		xattrs := make([]xmlAttribute, 0, len(attrs))
		for k, v := range attrs {
			xattrs = append(xattrs, xmlAttribute{Name: k, Value: v})
		}
		return attributesResponse{Xmlns: sqsNS, Attributes: xattrs}, nil

	case "SetQueueAttributes":
		name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
		if perr != nil {
			return nil, perr
		}
		attrs := map[string]any{}
		if in.json != nil {
			_ = json.Unmarshal(in.raw("Attributes"), &attrs)
		} else if in.form != nil {
			for k, v := range in.form {
				if strings.HasPrefix(k, "Attribute.") && strings.HasSuffix(k, ".Name") {
					idx := strings.TrimSuffix(strings.TrimPrefix(k, "Attribute."), ".Name")
					attrs[v[0]] = in.form.Get("Attribute." + idx + ".Value")
				}
			}
		}
		if apiErr := s.validateRedrive(ctx, attrs); apiErr != nil {
			return nil, apiErr
		}
		if err := s.store.SetNamespaceAttrs(ctx, ns.Queue, name, attrs); err != nil {
			return nil, errBad(err.Error())
		}
		return map[string]any{}, nil

	case "ListQueueTags", "TagQueue", "UntagQueue":
		return map[string]any{}, nil

	default:
		return nil, errUnsupported(op)
	}
}

func (s *Server) sendOne(ctx context.Context, r *http.Request, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	body := []byte(in.str("MessageBody"))
	if len(body) > 262144 {
		return nil, errBad("message body exceeds 256 KiB")
	}
	attrs := in.raw("MessageAttributes")
	sent, err := s.store.SendMessages(ctx, name, []store.OutMessage{{
		Body: body, Attrs: attrs, Delay: in.int("DelaySeconds", s.attrInt(ctx, name, "DelaySeconds", 0)),
	}}, s.retention(ctx, name))
	if err != nil {
		return nil, errBad(err.Error())
	}
	if in.json != nil {
		return map[string]string{
			"MessageId": sent[0].ID, "MD5OfMessageBody": sent[0].MD5OfBody,
		}, nil
	}
	return sendMessageResponse{Xmlns: sqsNS, MessageId: sent[0].ID, MD5OfMessageBody: sent[0].MD5OfBody}, nil
}

type batchEntry struct {
	ID                string          `json:"Id"`
	MessageBody       string          `json:"MessageBody"`
	DelaySeconds      int             `json:"DelaySeconds"`
	ReceiptHandle     string          `json:"ReceiptHandle"`
	MessageAttributes json.RawMessage `json:"MessageAttributes"`
}

func (s *Server) sendBatch(ctx context.Context, r *http.Request, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	var entries []batchEntry
	if in.json != nil {
		_ = json.Unmarshal(in.raw("Entries"), &entries)
	} else {
		for i := 1; ; i++ {
			id := in.form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i))
			if id == "" {
				break
			}
			entries = append(entries, batchEntry{
				ID:           id,
				MessageBody:  in.form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i)),
				DelaySeconds: atoi(in.form.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.DelaySeconds", i))),
			})
		}
	}
	if len(entries) == 0 {
		return nil, errBad("Entries is required")
	}
	if len(entries) > 10 {
		return nil, errBad("batch supports at most 10 entries")
	}
	msgs := make([]store.OutMessage, len(entries))
	for i, e := range entries {
		msgs[i] = store.OutMessage{Body: []byte(e.MessageBody), Attrs: e.MessageAttributes, Delay: e.DelaySeconds}
	}
	sent, err := s.store.SendMessages(ctx, name, msgs, s.retention(ctx, name))
	if err != nil {
		return nil, errBad(err.Error())
	}
	ok := make([]map[string]string, len(entries))
	for i, e := range entries {
		ok[i] = map[string]string{"Id": e.ID, "MessageId": sent[i].ID, "MD5OfMessageBody": sent[i].MD5OfBody}
	}
	return map[string]any{"Successful": ok, "Failed": []any{}}, nil
}

func (s *Server) receive(ctx context.Context, r *http.Request, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	maxN := in.int("MaxNumberOfMessages", 1)
	attrs, aerr := s.queueAttrs(ctx, name)
	if aerr != nil {
		var apiErr *apiError
		if errors.As(aerr, &apiErr) {
			return nil, apiErr
		}
		return nil, errBad(aerr.Error())
	}
	visDefault := 30
	if n, err := strconv.Atoi(attrs["VisibilityTimeout"]); err == nil {
		visDefault = n
	}
	vis := in.int("VisibilityTimeout", visDefault)
	pol := parseRedrive(attrs["RedrivePolicy"])
	wait := min(in.int("WaitTimeSeconds", 0), 20)
	deadline := time.Now().Add(time.Duration(wait) * time.Second)
	for {
		msgs, err := s.store.ReceiveMessages(ctx, name, maxN, vis, true)
		if err != nil {
			return nil, errBad(err.Error())
		}
		if len(msgs) > 0 || time.Now().After(deadline) {
			if pol != nil {
				var keep []store.Message
				var dead []string
				for _, m := range msgs {
					if int(m.ReceiveCount) > pol.Max {
						dead = append(dead, m.Receipt)
					} else {
						keep = append(keep, m)
					}
				}
				if len(dead) > 0 {
					if _, err := s.store.Redrive(ctx, name, pol.Target, dead,
						time.Now().Add(s.retention(ctx, pol.Target))); err != nil {
						return nil, errBad(err.Error())
					}
				}
				msgs = keep
			}
			if in.json != nil {
				return map[string]any{"Messages": jsonMessages(msgs)}, nil
			}
			return receiveMessageResponse{Xmlns: sqsNS, Messages: xmlMessages(msgs)}, nil
		}
		select {
		case <-ctx.Done():
			return nil, errBad(ctx.Err().Error())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *Server) retention(ctx context.Context, name string) time.Duration {
	secs := s.attrInt(ctx, name, "MessageRetentionPeriod", 345600)
	return time.Duration(secs) * time.Second
}

func jsonMessages(msgs []store.Message) []map[string]any {
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		e := map[string]any{
			"MessageId":     m.ID,
			"ReceiptHandle": m.Receipt,
			"Body":          string(m.Body),
			"MD5OfBody":     md5Hex(m.Body),
			"Attributes": map[string]string{
				"ApproximateReceiveCount": strconv.Itoa(int(m.ReceiveCount)),
				"SentTimestamp":           strconv.FormatInt(m.SentAt.UnixMilli(), 10),
			},
		}
		if len(m.Attrs) > 0 {
			var attrs map[string]any
			if json.Unmarshal(m.Attrs, &attrs) == nil {
				e["MessageAttributes"] = attrs
			}
		}
		out = append(out, e)
	}
	return out
}

func xmlMessages(msgs []store.Message) []xmlMessage {
	out := make([]xmlMessage, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, xmlMessage{
			MessageId: m.ID, ReceiptHandle: m.Receipt, Body: string(m.Body), MD5OfBody: md5Hex(m.Body),
			Attributes: []xmlAttribute{
				{Name: "ApproximateReceiveCount", Value: strconv.Itoa(int(m.ReceiveCount))},
				{Name: "SentTimestamp", Value: strconv.FormatInt(m.SentAt.UnixMilli(), 10)},
			},
		})
	}
	return out
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
