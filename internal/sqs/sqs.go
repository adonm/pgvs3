// Package sqs implements the subset of the Amazon SQS API used by AWS SDKs:
// the JSON (AmazonSQS.*) protocol and the legacy Query (Action=) protocol.
package sqs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
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
	"sync"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/reqguard"
	"github.com/adonm/pgvs3/internal/store"
)

const maxBody = 1 << 20

// Server is the SQS HTTP front end.
type Server struct {
	store    *store.Store
	log      *slog.Logger
	readOnly bool
	sem      chan struct{}
	mu       sync.Mutex
	waiters  map[string]chan struct{}
}

func New(st *store.Store, log *slog.Logger, readOnly bool, maxConc int) *Server {
	if maxConc <= 0 {
		maxConc = 16
	}
	return &Server{
		store:    st,
		log:      log,
		readOnly: readOnly,
		sem:      make(chan struct{}, maxConc),
		waiters:  map[string]chan struct{}{},
	}
}

func (s *Server) waitCh(queue string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, ok := s.waiters[queue]
	if !ok {
		ch = make(chan struct{})
		s.waiters[queue] = ch
	}
	return ch
}

func (s *Server) notify(queue string) {
	s.mu.Lock()
	if ch, ok := s.waiters[queue]; ok {
		close(ch)
		delete(s.waiters, queue)
	}
	s.mu.Unlock()
}

func (s *Server) acquire(ctx context.Context) (func(), bool) {
	select {
	case s.sem <- struct{}{}:
		return func() { <-s.sem }, true
	case <-ctx.Done():
		return nil, false
	}
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
	if r.Method != http.MethodPost {
		s.writeErr(w, r, http.StatusMethodNotAllowed, "InvalidAction", "Only POST is supported")
		return
	}
	if reqguard.Presigned(r.URL.Query()) {
		s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", reqguard.PresignRejection)
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
	vals, err := parseFormValues(r.URL.RawQuery, body)
	if err != nil {
		s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", "malformed form body")
		return
	}
	if reqguard.Presigned(vals) {
		s.writeErr(w, r, http.StatusBadRequest, "InvalidRequest", reqguard.PresignRejection)
		return
	}
	op := vals.Get("Action")
	out, apiErr := s.dispatch(r.Context(), r, op, formIn(vals))
	if apiErr != nil {
		s.writeErr(w, r, apiErr.status, apiErr.code, apiErr.message)
		return
	}
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(out)
}

func parseFormValues(rawQuery string, body []byte) (url.Values, error) {
	vals, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return vals, nil
	}
	postVals, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, err
	}
	for k, v := range postVals {
		vals[k] = append(vals[k], v...)
	}
	return vals, nil
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

// has reports whether the field was supplied, which matters for parameters
// whose zero value is meaningful (for example FIFO DelaySeconds).
func (in input) has(name string) bool {
	if in.json != nil {
		_, ok := in.json[name]
		return ok
	}
	return in.form.Get(name) != ""
}

func (in input) raw(name string) json.RawMessage { return in.json[name] }

func (in input) attrs() map[string]any {
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
	return attrs
}

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
		if code == "AWS.SimpleQueueService.NonExistentQueue" {
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

func queueFromURL(raw, _ string) (string, *apiError) {
	if raw == "" {
		return "", errBad("QueueUrl is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", errBad("invalid QueueUrl")
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
		"FifoQueue":                     "false",
		"ContentBasedDeduplication":     "false",
	}
}

func (s *Server) queueAttrs(ctx context.Context, name string) (map[string]string, error) {
	raw, ok, err := s.store.NamespaceAttrs(ctx, ns.Queue, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errNoQueue()
	}
	out := map[string]string{}
	for k, v := range defaultAttrs() {
		out[k] = v.(string)
	}
	for k, v := range raw {
		if sv, ok := v.(string); ok {
			out[k] = sv
		} else if fv, ok := v.(float64); ok {
			out[k] = strconv.FormatInt(int64(fv), 10)
		} else if bv, ok := v.(bool); ok {
			out[k] = strconv.FormatBool(bv)
		}
	}
	return out, nil
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

func emptyOK(op string, in input) any {
	if in.json != nil {
		return map[string]any{}
	}
	return emptyResponse{XMLName: xml.Name{Local: op + "Response"}, Xmlns: sqsNS}
}

// ---- dispatch ---------------------------------------------------------------

type opHandler func(s *Server, ctx context.Context, r *http.Request, op string, in input) (any, *apiError)

var sqsOps = map[string]opHandler{
	"CreateQueue":                  (*Server).opCreateQueue,
	"GetQueueUrl":                  (*Server).opGetQueueURL,
	"ListQueues":                   (*Server).opListQueues,
	"DeleteQueue":                  (*Server).opDeleteQueue,
	"PurgeQueue":                   (*Server).opPurgeQueue,
	"SendMessage":                  (*Server).opSendMessage,
	"SendMessageBatch":             (*Server).opSendMessageBatch,
	"ReceiveMessage":               (*Server).opReceiveMessage,
	"DeleteMessage":                (*Server).opDeleteMessage,
	"DeleteMessageBatch":           (*Server).opDeleteMessageBatch,
	"ChangeMessageVisibility":      (*Server).opChangeVisibility,
	"ChangeMessageVisibilityBatch": (*Server).opChangeVisibilityBatch,
	"GetQueueAttributes":           (*Server).opGetQueueAttrs,
	"SetQueueAttributes":           (*Server).opSetQueueAttrs,
	"ListQueueTags":                (*Server).opNoop,
	"TagQueue":                     (*Server).opNoop,
	"UntagQueue":                   (*Server).opNoop,
}

func (s *Server) dispatch(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
	fn, ok := sqsOps[op]
	if !ok {
		return nil, errUnsupported(op)
	}
	if s.readOnly && !readOnlyAllowed(op) {
		return nil, &apiError{
			status:  http.StatusForbidden,
			code:    "AccessDenied",
			message: "gateway is read-only; sending, leasing, deleting, or purging messages requires a writable primary",
		}
	}
	// ReceiveMessage manages admission per DB poll so waiting long-pollers do
	// not occupy the concurrency semaphore.
	if op != "ReceiveMessage" {
		rel, ok := s.acquire(ctx)
		if !ok {
			return nil, errBad("request canceled")
		}
		defer rel()
	}
	return fn(s, ctx, r, op, in)
}

func (s *Server) opNoop(_ context.Context, _ *http.Request, op string, in input) (any, *apiError) {
	return emptyOK(op, in), nil
}

// readOnlyAllowed reports whether op is safe on a read-only gateway. Message
// leasing is excluded because ReceiveMessage writes visibility state.
func readOnlyAllowed(op string) bool {
	switch op {
	case "ListQueues", "GetQueueUrl", "GetQueueAttributes", "ListQueueTags":
		return true
	}
	return false
}

func (s *Server) opCreateQueue(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name := in.str("QueueName")
	if !ns.ValidName(name) {
		return nil, errBad("invalid queue name")
	}
	attrs := in.attrs()
	fifo := attrBool(attrs, "FifoQueue") || strings.HasSuffix(name, ".fifo")
	if fifo && !strings.HasSuffix(name, ".fifo") {
		return nil, errBad("FIFO queue names must end with .fifo")
	}
	if fifo {
		attrs["FifoQueue"] = "true"
	}
	if v, ok := attrs["ContentBasedDeduplication"]; ok {
		if _, valid := parseBool(v); !valid {
			return nil, errBad("ContentBasedDeduplication must be true or false")
		}
	}
	for _, k := range []string{"DeduplicationScope", "FifoThroughputLimit"} {
		if _, ok := attrs[k]; ok {
			return nil, errBad("high-throughput FIFO attributes are not supported")
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
	return createQueueResponse{Xmlns: sqsNS, QueueUrl: queueURL(r, name)}, nil
}

func attrBool(attrs map[string]any, key string) bool {
	v, ok := attrs[key]
	if !ok {
		return false
	}
	b, _ := parseBool(v)
	return b
}

func parseBool(v any) (bool, bool) {
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		b, err := strconv.ParseBool(t)
		return b, err == nil
	}
	return false, false
}

func (s *Server) opGetQueueURL(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name := in.str("QueueName")
	ok, err := s.store.NamespaceExists(ctx, ns.Queue, name)
	if err != nil {
		return nil, errBad(err.Error())
	}
	if !ok {
		return nil, errNoQueue()
	}
	return getQueueURLResponse{Xmlns: sqsNS, QueueUrl: queueURL(r, name)}, nil
}

func (s *Server) opListQueues(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
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
	return listQueuesResponse{Xmlns: sqsNS, QueueUrls: urls}, nil
}

func (s *Server) opDeleteQueue(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	if _, err := s.store.DropNamespace(ctx, ns.Queue, name, true); err != nil {
		return nil, errBad(err.Error())
	}
	return emptyOK(op, in), nil
}

func (s *Server) opPurgeQueue(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	if err := s.store.PurgeQueue(ctx, name); err != nil {
		return nil, errBad(err.Error())
	}
	return emptyOK(op, in), nil
}

func (s *Server) opDeleteMessage(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
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
	return emptyOK(op, in), nil
}

func (s *Server) opDeleteMessageBatch(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	entries, perr := decodeBatchEntries(in, "DeleteMessageBatchRequestEntry")
	if perr != nil {
		return nil, perr
	}
	receipts := make([]string, len(entries))
	for i, e := range entries {
		receipts[i] = e.ReceiptHandle
	}
	errs, err := s.store.DeleteMessages(ctx, name, receipts)
	if err != nil {
		return nil, errBad(err.Error())
	}
	ok, fail, perr := receiptBatchResult(entries, errs)
	if perr != nil {
		return nil, perr
	}
	return deleteBatchResponse{Xmlns: sqsNS, Successful: ok, Failed: fail}, nil
}

func (s *Server) opChangeVisibility(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
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
	return emptyOK(op, in), nil
}

func (s *Server) opChangeVisibilityBatch(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	entries, perr := decodeBatchEntries(in, "ChangeMessageVisibilityBatchRequestEntry")
	if perr != nil {
		return nil, perr
	}
	receipts := make([]string, len(entries))
	visibility := make([]int32, len(entries))
	for i, e := range entries {
		receipts[i] = e.ReceiptHandle
		visibility[i] = int32(e.VisibilityTimeout)
	}
	errs, err := s.store.ChangeVisibilityBatch(ctx, name, receipts, visibility)
	if err != nil {
		return nil, errBad(err.Error())
	}
	ok, fail, perr := receiptBatchResult(entries, errs)
	if perr != nil {
		return nil, perr
	}
	return changeVisibilityBatchResponse{Xmlns: sqsNS, Successful: ok, Failed: fail}, nil
}

func receiptBatchResult(entries []batchEntry, errs []error) ([]xmlBatchOK, []xmlBatchResultError, *apiError) {
	ok := make([]xmlBatchOK, 0, len(entries))
	fail := make([]xmlBatchResultError, 0)
	for i, e := range entries {
		err := errs[i]
		if err == nil {
			ok = append(ok, xmlBatchOK{Id: e.ID})
			continue
		}
		if errors.Is(err, store.ErrReceiptStale) || errors.Is(err, store.ErrReceiptInvalid) {
			fail = append(fail, xmlBatchResultError{
				Id: e.ID, Code: "ReceiptHandleIsInvalid",
				Message: "The receipt handle is invalid.", SenderFault: true,
			})
			continue
		}
		return nil, nil, errBad(err.Error())
	}
	return ok, fail, nil
}

func (s *Server) opGetQueueAttrs(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
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
}

func (s *Server) opSetQueueAttrs(ctx context.Context, r *http.Request, op string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	attrs := in.attrs()
	if _, ok := attrs["FifoQueue"]; ok {
		return nil, errBad("FifoQueue cannot be changed")
	}
	if apiErr := s.validateRedrive(ctx, attrs); apiErr != nil {
		return nil, apiErr
	}
	if err := s.store.SetNamespaceAttrs(ctx, ns.Queue, name, attrs); err != nil {
		return nil, errBad(err.Error())
	}
	return emptyOK(op, in), nil
}

func (s *Server) opSendMessage(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	body := []byte(in.str("MessageBody"))
	if len(body) > 262144 {
		return nil, errBad("message body exceeds 256 KiB")
	}
	attrs, aerr := s.queueAttrs(ctx, name)
	if aerr != nil {
		if ae, ok := aerr.(*apiError); ok {
			return nil, ae
		}
		return nil, errBad(aerr.Error())
	}
	fifo := strings.EqualFold(attrs["FifoQueue"], "true")
	contentDedup := strings.EqualFold(attrs["ContentBasedDeduplication"], "true")
	delayDefault := atoi(attrs["DelaySeconds"])
	retSecs := atoi(attrs["MessageRetentionPeriod"])
	if retSecs == 0 {
		retSecs = 345600
	}
	delay := delayDefault
	if in.has("DelaySeconds") {
		delay = in.int("DelaySeconds", delayDefault)
		if delay < 0 || delay > 900 {
			return nil, errBad("invalid DelaySeconds")
		}
	}
	groupID := in.str("MessageGroupId")
	dedupID := in.str("MessageDeduplicationId")
	if fifo && in.has("DelaySeconds") && delay != 0 {
		return nil, errBad("per-message DelaySeconds is not supported for FIFO queues")
	}
	groupID, dedupID, ferr := fifoFields(fifo, contentDedup, body, groupID, dedupID)
	if ferr != nil {
		return nil, ferr
	}
	sent, err := s.store.SendMessages(ctx, name, []store.OutMessage{{
		Body:    body,
		Attrs:   in.raw("MessageAttributes"),
		Delay:   delay,
		GroupID: groupID,
		DedupID: dedupID,
	}}, time.Duration(retSecs)*time.Second)
	if err != nil {
		return nil, errBad(err.Error())
	}
	s.notify(name)
	resp := sendMessageResponse{Xmlns: sqsNS, MessageId: sent[0].ID, MD5OfMessageBody: sent[0].MD5OfBody}
	if fifo {
		resp.SequenceNumber = sent[0].SequenceNum
	}
	return resp, nil
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func fifoFields(fifo, contentDedup bool, body []byte, groupID, dedupID string) (string, string, *apiError) {
	if !fifo {
		return groupID, "", nil
	}
	if groupID == "" {
		return "", "", errBad("MessageGroupId is required for FIFO queues")
	}
	if len(groupID) > 128 {
		return "", "", errBad("MessageGroupId exceeds 128 characters")
	}
	if dedupID == "" && contentDedup {
		dedupID = sha256Hex(body)
	}
	if dedupID == "" {
		return "", "", errBad("MessageDeduplicationId is required for FIFO queues")
	}
	if len(dedupID) > 128 {
		return "", "", errBad("MessageDeduplicationId exceeds 128 characters")
	}
	return groupID, dedupID, nil
}

type batchEntry struct {
	ID                     string          `json:"Id"`
	MessageBody            string          `json:"MessageBody"`
	DelaySeconds           int             `json:"DelaySeconds"`
	MessageGroupId         string          `json:"MessageGroupId"`
	MessageDeduplicationId string          `json:"MessageDeduplicationId"`
	ReceiptHandle          string          `json:"ReceiptHandle"`
	VisibilityTimeout      int             `json:"VisibilityTimeout"`
	MessageAttributes      json.RawMessage `json:"MessageAttributes"`
}

func decodeBatchEntries(in input, prefix string) ([]batchEntry, *apiError) {
	var entries []batchEntry
	if in.json != nil {
		_ = json.Unmarshal(in.raw("Entries"), &entries)
	} else {
		for i := 1; ; i++ {
			id := in.form.Get(fmt.Sprintf("%s.%d.Id", prefix, i))
			if id == "" {
				break
			}
			entries = append(entries, batchEntry{
				ID:                     id,
				MessageBody:            in.form.Get(fmt.Sprintf("%s.%d.MessageBody", prefix, i)),
				DelaySeconds:           atoi(in.form.Get(fmt.Sprintf("%s.%d.DelaySeconds", prefix, i))),
				MessageGroupId:         in.form.Get(fmt.Sprintf("%s.%d.MessageGroupId", prefix, i)),
				MessageDeduplicationId: in.form.Get(fmt.Sprintf("%s.%d.MessageDeduplicationId", prefix, i)),
				ReceiptHandle:          in.form.Get(fmt.Sprintf("%s.%d.ReceiptHandle", prefix, i)),
				VisibilityTimeout:      atoi(in.form.Get(fmt.Sprintf("%s.%d.VisibilityTimeout", prefix, i))),
			})
		}
	}
	if len(entries) == 0 {
		return nil, errBad("Entries is required")
	}
	if len(entries) > 10 {
		return nil, errBad("batch supports at most 10 entries")
	}
	return entries, nil
}

func (s *Server) opSendMessageBatch(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	entries, perr := decodeBatchEntries(in, "SendMessageBatchRequestEntry")
	if perr != nil {
		return nil, perr
	}
	attrs, aerr := s.queueAttrs(ctx, name)
	if aerr != nil {
		if ae, ok := aerr.(*apiError); ok {
			return nil, ae
		}
		return nil, errBad(aerr.Error())
	}
	fifo := strings.EqualFold(attrs["FifoQueue"], "true")
	contentDedup := strings.EqualFold(attrs["ContentBasedDeduplication"], "true")
	delayDefault := atoi(attrs["DelaySeconds"])
	retSecs := atoi(attrs["MessageRetentionPeriod"])
	if retSecs == 0 {
		retSecs = 345600
	}
	msgs := make([]store.OutMessage, 0, len(entries))
	idx := make([]int, 0, len(entries))
	failed := make([]xmlBatchResultError, 0)
	for i, e := range entries {
		body := []byte(e.MessageBody)
		if len(body) > 262144 {
			failed = append(failed, xmlBatchResultError{
				Id: e.ID, SenderFault: true, Code: "InvalidParameterValue",
				Message: "message body exceeds 256 KiB",
			})
			continue
		}
		delay := delayDefault
		if e.DelaySeconds != 0 {
			delay = e.DelaySeconds
		}
		if fifo && e.DelaySeconds != 0 {
			failed = append(failed, xmlBatchResultError{
				Id: e.ID, SenderFault: true, Code: "InvalidParameterValue",
				Message: "per-message DelaySeconds is not supported for FIFO queues",
			})
			continue
		}
		groupID, dedupID, ferr := fifoFields(fifo, contentDedup, body, e.MessageGroupId, e.MessageDeduplicationId)
		if ferr != nil {
			failed = append(failed, xmlBatchResultError{
				Id: e.ID, SenderFault: true, Code: "InvalidParameterValue", Message: ferr.message,
			})
			continue
		}
		msgs = append(msgs, store.OutMessage{
			Body: body, Attrs: e.MessageAttributes, Delay: delay,
			GroupID: groupID, DedupID: dedupID,
		})
		idx = append(idx, i)
	}
	var sent []store.SentMessage
	if len(msgs) > 0 {
		var err error
		sent, err = s.store.SendMessages(ctx, name, msgs, time.Duration(retSecs)*time.Second)
		if err != nil {
			return nil, errBad(err.Error())
		}
		s.notify(name)
	}
	ok := make([]xmlSendBatchResult, len(idx))
	for j, i := range idx {
		ok[j] = xmlSendBatchResult{Id: entries[i].ID, MessageId: sent[j].ID, MD5OfMessageBody: sent[j].MD5OfBody}
		if fifo {
			ok[j].SequenceNumber = sent[j].SequenceNum
		}
	}
	return sendBatchResponse{Xmlns: sqsNS, Successful: ok, Failed: failed}, nil
}

func (s *Server) opReceiveMessage(ctx context.Context, r *http.Request, _ string, in input) (any, *apiError) {
	name, perr := queueFromURL(in.str("QueueUrl"), r.Host)
	if perr != nil {
		return nil, perr
	}
	maxN := in.int("MaxNumberOfMessages", 1)
	var (
		attrs map[string]string
		aerr  error
	)
	rel, ok := s.acquire(ctx)
	if !ok {
		return nil, errBad("request canceled")
	}
	attrs, aerr = s.queueAttrs(ctx, name)
	rel()
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
	fifo := strings.EqualFold(attrs["FifoQueue"], "true")
	pol := parseRedrive(attrs["RedrivePolicy"])
	waitDefault := 0
	if n, err := strconv.Atoi(attrs["ReceiveMessageWaitTimeSeconds"]); err == nil {
		waitDefault = n
	}
	wait := min(in.int("WaitTimeSeconds", waitDefault), 20)
	deadline := time.Now().Add(time.Duration(wait) * time.Second)
	for {
		wch := s.waitCh(name)
		rel, ok := s.acquire(ctx)
		if !ok {
			return nil, errBad("request canceled")
		}
		msgs, err := s.store.ReceiveMessages(ctx, name, maxN, vis, true, fifo)
		var redriveErr error
		if err == nil && len(msgs) > 0 && pol != nil {
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
				_, redriveErr = s.store.Redrive(ctx, name, pol.Target, dead,
					time.Now().Add(s.retention(ctx, pol.Target)))
				if redriveErr == nil {
					s.notify(pol.Target)
				}
			}
			msgs = keep
			if redriveErr == nil && len(msgs) == 0 && len(dead) > 0 {
				rel()
				// Every leased message in this batch was poison and moved to the
				// DLQ; immediately poll again so live messages behind them are
				// returned even when WaitTimeSeconds is 0.
				continue
			}
		}
		rel()
		if err != nil {
			return nil, errBad(err.Error())
		}
		if redriveErr != nil {
			return nil, errBad(redriveErr.Error())
		}
		if len(msgs) > 0 || time.Now().After(deadline) {
			return formatReceive(in, msgs), nil
		}
		sleep := min(time.Until(deadline), 250*time.Millisecond)
		select {
		case <-ctx.Done():
			return nil, errBad(ctx.Err().Error())
		case <-wch:
		case <-time.After(sleep):
		}
	}
}

func formatReceive(in input, msgs []store.Message) any {
	if in.json != nil {
		return map[string]any{"Messages": jsonMessages(msgs)}
	}
	return receiveMessageResponse{Xmlns: sqsNS, Messages: xmlMessages(msgs)}
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
		attrs := e["Attributes"].(map[string]string)
		if m.GroupID != "" {
			attrs["MessageGroupId"] = m.GroupID
		}
		if m.DedupID != "" {
			attrs["MessageDeduplicationId"] = m.DedupID
		}
		if m.SequenceNum != "" && m.GroupID != "" {
			attrs["SequenceNumber"] = m.SequenceNum
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
		attrs := []xmlAttribute{
			{Name: "ApproximateReceiveCount", Value: strconv.Itoa(int(m.ReceiveCount))},
			{Name: "SentTimestamp", Value: strconv.FormatInt(m.SentAt.UnixMilli(), 10)},
		}
		if m.GroupID != "" {
			attrs = append(attrs, xmlAttribute{Name: "MessageGroupId", Value: m.GroupID})
		}
		if m.DedupID != "" {
			attrs = append(attrs, xmlAttribute{Name: "MessageDeduplicationId", Value: m.DedupID})
		}
		if m.SequenceNum != "" && m.GroupID != "" {
			attrs = append(attrs, xmlAttribute{Name: "SequenceNumber", Value: m.SequenceNum})
		}
		out = append(out, xmlMessage{
			MessageId: m.ID, ReceiptHandle: m.Receipt, Body: string(m.Body), MD5OfBody: md5Hex(m.Body),
			Attributes: attrs,
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
