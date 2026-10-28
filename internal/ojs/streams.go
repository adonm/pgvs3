package ojs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/store"
)

type streamMessage struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Headers    json.RawMessage `json:"headers,omitempty"`
	RetryCount int             `json:"retry_count,omitempty"`
	CreatedAt  string          `json:"created_at"`
}
type streamSendRequest struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
	Headers json.RawMessage `json:"headers"`
}
type streamBatchSendRequest struct {
	Messages []streamSendRequest `json:"messages"`
}
type streamReceiveRequest struct {
	Consumer string `json:"consumer"`
	Member   string `json:"member"`
	Max      int    `json:"max"`
	WaitMS   int64  `json:"wait_ms"`
}
type streamAckRequest struct {
	Queue   string `json:"queue"`
	BatchID int64  `json:"batch_id"`
}
type streamNackRequest struct {
	streamAckRequest
	MessageID    string `json:"message_id"`
	RetryAfterMS int64  `json:"retry_after_ms"`
	Reason       string `json:"reason"`
}
type streamSubscriptionRequest struct {
	Consumer string `json:"consumer"`
	FromTick *int64 `json:"from_tick"`
}
type streamConfigRequest struct {
	MaxRetries *int    `json:"max_retries"`
	Retention  *string `json:"retention"`
}
type streamQueueInfo struct {
	Events           int64  `json:"events"`
	Consumers        int64  `json:"consumers"`
	Active           int64  `json:"active_batches"`
	Retries          int64  `json:"retries"`
	DeadLetters      int64  `json:"dead_letters"`
	OldestEvent      string `json:"oldest_event,omitempty"`
	LastTick         int64  `json:"last_tick"`
	MaxRetries       int    `json:"max_retries"`
	Retention        string `json:"retention"`
	Segment          int16  `json:"segment"`
	SegmentStartedAt string `json:"segment_started_at"`
	RotationPeriod   string `json:"rotation_period"`
}
type streamConsumer struct {
	Consumer    string `json:"consumer"`
	LastTick    int64  `json:"last_tick"`
	ActiveBatch int64  `json:"active_batch,omitempty"`
	Pending     int64  `json:"pending"`
	UpdatedAt   string `json:"updated_at"`
}
type streamDeadLetter struct {
	ID         int64           `json:"id"`
	BatchID    int64           `json:"batch_id"`
	MessageID  string          `json:"message_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	Headers    json.RawMessage `json:"headers,omitempty"`
	RetryCount int             `json:"retry_count"`
	Reason     string          `json:"reason"`
	CreatedAt  string          `json:"created_at"`
}

func rawObject(b []byte) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage("{}")
	}
	return json.RawMessage(b)
}

func wireStreamMessage(m store.StreamMessage) streamMessage {
	return streamMessage{ID: m.ID, Type: m.Type, Payload: rawObject(m.Payload), Headers: rawObject(m.Headers),
		RetryCount: m.RetryCount, CreatedAt: timeString(m.CreatedAt)}
}

func wireStreamBatch(batch *store.StreamBatch) map[string]any {
	messages := make([]streamMessage, len(batch.Messages))
	for i, m := range batch.Messages {
		messages[i] = wireStreamMessage(m)
	}
	out := map[string]any{"batch_id": batch.ID, "consumer": batch.Consumer,
		"start_tick": batch.StartTick, "end_tick": batch.EndTick, "messages": messages, "count": len(messages)}
	if batch.Member != "" {
		out["member"] = batch.Member
	}
	return out
}

func streamSendInput(r streamSendRequest) (store.StreamMessageInput, *apiError) {
	if r.Type == "" {
		r.Type = "default"
	}
	if len(r.Type) > 256 {
		return store.StreamMessageInput{}, badRequest("stream message type exceeds 256 bytes")
	}
	if len(r.ID) > 1024 {
		return store.StreamMessageInput{}, badRequest("stream message id exceeds 1024 bytes")
	}
	payload, headers := rawObject(r.Payload), rawObject(r.Headers)
	if !json.Valid(payload) {
		return store.StreamMessageInput{}, badRequest("stream message payload must be JSON")
	}
	if !json.Valid(headers) {
		return store.StreamMessageInput{}, badRequest("stream message headers must be JSON")
	}
	return store.StreamMessageInput{ID: r.ID, Type: r.Type, Payload: payload, Headers: headers}, nil
}

type streamMethods struct{ get, post, remove endpoint }

var streamReceiveEndpoint = jsonEndpoint((*Server).handleStreamReceive)
var streamRoutes = map[string]streamMethods{
	"messages":       {post: jsonEndpoint((*Server).handleStreamSend)},
	"messages/batch": {post: jsonEndpoint((*Server).handleStreamSendBatch)},
	"receive":        {post: streamReceiveEndpoint},
	"consumers":      {get: (*Server).handleStreamConsumers, post: jsonEndpoint((*Server).handleStreamSubscribe)},
	"stats":          {get: (*Server).handleQueueStats},
	"info":           {get: (*Server).handleStreamInfo},
	"config":         {get: (*Server).handleStreamConfig, post: jsonEndpoint((*Server).handleStreamConfigUpdate)},
	"dlq":            {get: (*Server).handleStreamDLQ, remove: (*Server).handleStreamDLQPurge},
	"dlq/replay":     {post: (*Server).handleStreamDLQReplayAll},
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) (any, error) {
	queue, action, rest, ok := splitStreamPath(r.URL.Path)
	if !ok {
		return nil, notFound("unknown queue endpoint")
	}
	if !ns.ValidName(queue) {
		return nil, badRequest("invalid queue name")
	}
	if (r.Method == http.MethodPost || r.Method == http.MethodDelete) && s.readOnly {
		return nil, readOnly()
	}
	r.SetPathValue("queue", queue)
	r.SetPathValue("rest", rest)
	key := action
	if rest != "" {
		key += "/" + rest
	}
	methods, found := streamRoutes[key]
	if !found && rest != "" {
		switch action {
		case "consumers":
			methods.remove = (*Server).handleStreamUnsubscribe
		case "batches":
			methods.get = (*Server).handleStreamBatch
		case "dlq":
			if parts := strings.Split(rest, "/"); len(parts) == 2 && parts[1] == "replay" {
				r.SetPathValue("rest", parts[0])
				methods.post = (*Server).handleStreamDLQReplay
			}
		}
	}
	var run endpoint
	switch r.Method {
	case http.MethodGet:
		run = methods.get
	case http.MethodPost:
		run = methods.post
	case http.MethodDelete:
		run = methods.remove
	}
	if run != nil {
		return run(s, w, r)
	}
	var allow []string
	if methods.get != nil {
		allow = append(allow, http.MethodGet)
	}
	if methods.post != nil {
		allow = append(allow, http.MethodPost)
	}
	if methods.remove != nil {
		allow = append(allow, http.MethodDelete)
	}
	if len(allow) != 0 {
		return nil, methodError(strings.Join(allow, ", "))
	}
	return nil, notFound("unknown stream endpoint")
}

func splitStreamPath(path string) (queue, action, rest string, ok bool) {
	const prefix = "/ojs/v1/queues/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(path, prefix), "/", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return "", "", "", false
	}
	if len(parts) == 3 {
		rest = parts[2]
	}
	return parts[0], parts[1], rest, true
}

func (s *Server) handleStreamSend(r *http.Request, req streamSendRequest) (any, error) {
	return s.sendStream(r, []streamSendRequest{req}, false)
}

func (s *Server) handleStreamSendBatch(r *http.Request, req streamBatchSendRequest) (any, error) {
	return s.sendStream(r, req.Messages, true)
}

func (s *Server) sendStream(r *http.Request, requests []streamSendRequest, batch bool) (any, error) {
	if len(requests) == 0 || len(requests) > 1000 {
		return nil, badRequest("stream batch must contain between 1 and 1000 messages")
	}
	inputs := make([]store.StreamMessageInput, len(requests))
	for i, req := range requests {
		in, e := streamSendInput(req)
		if e != nil {
			if batch {
				e.message = "message at index " + strconv.Itoa(i) + ": " + e.message
			}
			return nil, e
		}
		inputs[i] = in
	}
	queue := r.PathValue("queue")
	messages, err := s.st.AppendStreamMessages(r.Context(), queue, inputs)
	if err != nil {
		return nil, err
	}
	if !batch {
		return reply{status: http.StatusCreated, body: map[string]any{"message": wireStreamMessage(messages[0])},
			location: "/ojs/v1/queues/" + queue + "/messages/" + messages[0].ID}, nil
	}
	wire := make([]streamMessage, len(messages))
	for i, m := range messages {
		wire[i] = wireStreamMessage(m)
	}
	return reply{status: http.StatusCreated, body: map[string]any{"messages": wire, "count": len(wire)}}, nil
}

func (s *Server) handleStreamReceiveRoute(w http.ResponseWriter, r *http.Request) (any, error) {
	queue, action, rest, ok := splitStreamPath(r.URL.Path)
	if !ok || action != "receive" || rest != "" || !ns.ValidName(queue) {
		return nil, notFound("unknown stream endpoint")
	}
	r.SetPathValue("queue", queue)
	return streamReceiveEndpoint(s, w, r)
}

func (s *Server) handleStreamReceive(r *http.Request, req streamReceiveRequest) (any, error) {
	if strings.TrimSpace(req.Consumer) == "" {
		return nil, badRequest("'consumer' is required")
	}
	if req.WaitMS < 0 {
		return nil, badRequest("wait_ms must be non-negative")
	}
	wait := min(time.Duration(req.WaitMS)*time.Millisecond, 5*time.Second)
	batch, err := poll(r.Context(), wait, func() (*store.StreamBatch, error) {
		return s.receiveStreamOnce(r.Context(), r.PathValue("queue"), req)
	}, func(b *store.StreamBatch) bool { return b.ID != 0 || len(b.Messages) > 0 })
	if err != nil {
		return nil, err
	}
	return wireStreamBatch(batch), nil
}

func (s *Server) receiveStreamOnce(ctx context.Context, queue string, req streamReceiveRequest) (*store.StreamBatch, error) {
	release, ok := s.acquire(ctx)
	if !ok {
		return nil, badRequest("request canceled")
	}
	defer release()
	if req.Member != "" {
		return s.st.ReceiveStreamCoopBatch(ctx, queue, req.Consumer, req.Member, req.Max)
	}
	return s.st.ReceiveStreamBatch(ctx, queue, req.Consumer, req.Max)
}

func (s *Server) handleStreamAck(r *http.Request, req streamAckRequest) (any, error) {
	if !ns.ValidName(req.Queue) || req.BatchID <= 0 {
		return nil, badRequest("valid queue and batch_id are required")
	}
	tick, err := s.st.AckStreamBatch(r.Context(), req.Queue, req.BatchID)
	return map[string]any{"batch_id": req.BatchID, "acknowledged": true, "tick": tick}, err
}

func (s *Server) handleStreamNack(r *http.Request, req streamNackRequest) (any, error) {
	if !ns.ValidName(req.Queue) || req.BatchID <= 0 || strings.TrimSpace(req.MessageID) == "" {
		return nil, badRequest("queue, batch_id and message_id are required")
	}
	if req.RetryAfterMS < 0 {
		return nil, badRequest("retry_after_ms must be non-negative")
	}
	state, err := s.st.NackStreamMessage(r.Context(), req.Queue, req.BatchID, req.MessageID,
		time.Duration(req.RetryAfterMS)*time.Millisecond, req.Reason)
	return map[string]any{"batch_id": req.BatchID, "message_id": req.MessageID, "state": state}, err
}

func (s *Server) handleStreamConsumers(_ http.ResponseWriter, r *http.Request) (any, error) {
	consumers, err := s.st.ListStreamConsumers(r.Context(), r.PathValue("queue"))
	if err != nil {
		return nil, err
	}
	out := make([]streamConsumer, len(consumers))
	for i, c := range consumers {
		out[i] = streamConsumer{Consumer: c.Consumer, LastTick: c.LastTick, ActiveBatch: c.ActiveBatch, Pending: c.Pending, UpdatedAt: timeString(c.UpdatedAt)}
	}
	return map[string]any{"consumers": out, "count": len(out)}, nil
}

func (s *Server) handleStreamSubscribe(r *http.Request, req streamSubscriptionRequest) (any, error) {
	if strings.TrimSpace(req.Consumer) == "" {
		return nil, badRequest("'consumer' is required")
	}
	sub, err := s.st.SubscribeStream(r.Context(), r.PathValue("queue"), req.Consumer, req.FromTick)
	return reply{status: http.StatusCreated, body: map[string]any{"consumer": sub.Consumer, "last_tick": sub.LastTick,
		"created_at": timeString(sub.CreatedAt), "updated_at": timeString(sub.UpdatedAt)}}, err
}

func (s *Server) handleStreamUnsubscribe(_ http.ResponseWriter, r *http.Request) (any, error) {
	consumer, err := url.PathUnescape(r.PathValue("rest"))
	if err != nil || strings.Contains(consumer, "/") {
		return nil, badRequest("invalid consumer")
	}
	ok, err := s.st.UnsubscribeStream(r.Context(), r.PathValue("queue"), consumer)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("stream consumer not found")
	}
	return reply{status: http.StatusNoContent}, nil
}

func positiveID(raw, message string) (int64, *apiError) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		return 0, badRequest(message)
	}
	return id, nil
}

func (s *Server) handleStreamBatch(_ http.ResponseWriter, r *http.Request) (any, error) {
	id, e := positiveID(r.PathValue("rest"), "invalid batch id")
	if e != nil {
		return nil, e
	}
	batch, err := s.st.GetStreamBatch(r.Context(), r.PathValue("queue"), id)
	if err != nil {
		return nil, err
	}
	return wireStreamBatch(batch), nil
}

func (s *Server) handleStreamInfo(_ http.ResponseWriter, r *http.Request) (any, error) {
	info, err := s.st.StreamInfo(r.Context(), r.PathValue("queue"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"stream": streamQueueInfo{
		Events: info.Events, Consumers: info.Consumers, Active: info.Active, Retries: info.Retries,
		DeadLetters: info.DeadLetters, OldestEvent: timePtr(info.OldestEvent), LastTick: info.LastTick,
		MaxRetries: info.MaxRetries, Retention: info.Retention.String(), Segment: info.Segment,
		SegmentStartedAt: timeString(info.SegmentStartedAt), RotationPeriod: info.RotationPeriod.String(),
	}}, nil
}

func streamConfigBody(cfg store.StreamConfig) any {
	return map[string]any{"stream": map[string]any{"max_retries": cfg.MaxRetries, "retention": cfg.Retention.String()}}
}

func (s *Server) handleStreamConfig(_ http.ResponseWriter, r *http.Request) (any, error) {
	cfg, err := s.st.StreamConfig(r.Context(), r.PathValue("queue"))
	return streamConfigBody(cfg), err
}

func (s *Server) handleStreamConfigUpdate(r *http.Request, req streamConfigRequest) (any, error) {
	maxRetries := -1
	if req.MaxRetries != nil {
		if *req.MaxRetries < 0 {
			return nil, badRequest("max_retries must be non-negative")
		}
		maxRetries = *req.MaxRetries
	}
	var retention time.Duration
	if req.Retention != nil {
		d, err := time.ParseDuration(*req.Retention)
		if err != nil || d <= 0 {
			return nil, badRequest("invalid retention duration")
		}
		retention = d
	}
	cfg, err := s.st.SetStreamConfig(r.Context(), r.PathValue("queue"), maxRetries, retention)
	return streamConfigBody(cfg), err
}

func (s *Server) handleStreamDLQ(_ http.ResponseWriter, r *http.Request) (any, error) {
	limit := 100
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 {
		limit = min(n, 1000)
	}
	rows, err := s.st.ListDeadLetters(r.Context(), r.PathValue("queue"), limit)
	if err != nil {
		return nil, err
	}
	out := make([]streamDeadLetter, len(rows))
	for i, d := range rows {
		out[i] = streamDeadLetter{ID: d.ID, BatchID: d.BatchID, MessageID: d.EventID, Type: d.Type,
			Payload: rawObject(d.Payload), Headers: rawObject(d.Headers), RetryCount: d.RetryCount, Reason: d.Reason, CreatedAt: timeString(d.CreatedAt)}
	}
	return map[string]any{"messages": out, "count": len(out)}, nil
}

func (s *Server) handleStreamDLQPurge(_ http.ResponseWriter, r *http.Request) (any, error) {
	olderThan := 30 * 24 * time.Hour
	if v := r.URL.Query().Get("older_than"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, badRequest("invalid older_than duration")
		}
		olderThan = d
	}
	n, err := s.st.PurgeDeadLetters(r.Context(), r.PathValue("queue"), olderThan)
	return map[string]any{"purged": n}, err
}

func (s *Server) handleStreamDLQReplayAll(_ http.ResponseWriter, r *http.Request) (any, error) {
	n, err := s.st.ReplayAllDeadLetters(r.Context(), r.PathValue("queue"))
	return map[string]any{"replayed": n}, err
}

func (s *Server) handleStreamDLQReplay(_ http.ResponseWriter, r *http.Request) (any, error) {
	id, e := positiveID(r.PathValue("rest"), "invalid dead-letter id")
	if e != nil {
		return nil, e
	}
	idStr, err := s.st.ReplayDeadLetter(r.Context(), r.PathValue("queue"), id)
	return map[string]any{"replayed": true, "message_id": idStr}, err
}
