package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/work"
)

var (
	ErrStreamBatchTooLarge    = errors.New("stream batch exceeds the requested ceiling")
	ErrStreamBatchNotFound    = errors.New("stream batch not found")
	ErrStreamMessageNotFound  = errors.New("stream message not in batch")
	ErrStreamConsumerNotFound = errors.New("stream consumer not found")
	ErrStreamBatchActive      = errors.New("stream has active cooperative batches")
	ErrStreamTickNotFound     = errors.New("stream tick not found")
	ErrInvalidStreamTick      = errors.New("invalid stream tick")
)

const (
	streamErrBatchNotFound   = "PV001"
	streamErrMessageNotFound = "PV002"
	streamErrBatchActive     = "PV003"
	streamErrTickNotFound    = "PV004"
	streamErrBatchTooLarge   = "PV005"
)

type StreamMessage struct {
	ID         string
	Type       string
	Payload    []byte
	Headers    []byte
	RetryCount int
	CreatedAt  time.Time
}

type StreamMessageInput struct {
	ID      string
	Type    string
	Payload []byte
	Headers []byte
}

type StreamSubscription struct {
	Consumer  string
	LastTick  int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type StreamBatch struct {
	ID        int64
	Consumer  string
	Member    string
	StartTick int64
	EndTick   int64
	Messages  []StreamMessage
	CreatedAt time.Time
}

type StreamConfig struct {
	MaxRetries int
	Retention  time.Duration
}

type StreamInfo struct {
	Events           int64
	Consumers        int64
	Active           int64
	Retries          int64
	DeadLetters      int64
	OldestEvent      *time.Time
	LastTick         int64
	MaxRetries       int
	Retention        time.Duration
	Segment          int16
	SegmentStartedAt time.Time
	RotationPeriod   time.Duration
}

type StreamConsumerInfo struct {
	Consumer    string
	LastTick    int64
	ActiveBatch int64
	Pending     int64
	UpdatedAt   time.Time
}

type DeadLetter struct {
	ID         int64
	BatchID    int64
	EventID    string
	Type       string
	Payload    []byte
	Headers    []byte
	RetryCount int
	Reason     string
	CreatedAt  time.Time
}

func streamErr(err, missing error) error {
	if err == nil {
		return nil
	}
	if missing != nil && isMissingBucketSchema(err) {
		return missing
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	switch pgErr.Code {
	case streamErrBatchNotFound:
		return ErrStreamBatchNotFound
	case streamErrMessageNotFound:
		return ErrStreamMessageNotFound
	case streamErrBatchActive:
		return ErrStreamBatchActive
	case streamErrTickNotFound:
		return ErrStreamTickNotFound
	case streamErrBatchTooLarge:
		return ErrStreamBatchTooLarge
	}
	return err
}

func scanStreamMessage(row pgx.CollectableRow) (StreamMessage, error) {
	var m StreamMessage
	err := row.Scan(&m.ID, &m.Type, &m.Payload, &m.Headers, &m.RetryCount, &m.CreatedAt)
	return m, err
}

func streamAppendArgs(inputs []StreamMessageInput) (ids, types, payloads, headers []string, err error) {
	ids = make([]string, len(inputs))
	types = make([]string, len(inputs))
	payloads = make([]string, len(inputs))
	headers = make([]string, len(inputs))
	for i, in := range inputs {
		ids[i] = in.ID
		if ids[i] == "" {
			ids[i] = work.NewID()
		}
		if len(ids[i]) > 1024 {
			return nil, nil, nil, nil, fmt.Errorf("stream event id exceeds 1024 bytes")
		}
		types[i] = in.Type
		if types[i] == "" {
			types[i] = "default"
		}
		payloads[i] = string(in.Payload)
		if len(in.Payload) == 0 {
			payloads[i] = "{}"
		}
		headers[i] = string(in.Headers)
		if len(in.Headers) == 0 {
			headers[i] = "{}"
		}
	}
	return ids, types, payloads, headers, nil
}

func (q Queue) AppendMessages(ctx context.Context, inputs []StreamMessageInput) ([]StreamMessage, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	return withAutoCreate(ctx, q.s, ns.Queue, q.name, func(sc string) ([]StreamMessage, error) {
		var add int64
		for _, in := range inputs {
			add += int64(len(in.Payload) + len(in.Headers))
		}
		if q.s.checkQuota(ctx, ns.Queue, q.name, add) {
			return nil, ErrQuotaExceeded
		}
		ids, types, payloads, headers, err := streamAppendArgs(inputs)
		if err != nil {
			return nil, err
		}
		rows, err := q.s.pool.Query(ctx, `SELECT event_id, type, payload, headers, retry_count, created_at
			FROM `+sc+`.stream_append($1,$2,$3,$4)`, ids, types, payloads, headers)
		if err != nil {
			return nil, err
		}
		return pgx.CollectRows(rows, scanStreamMessage)
	})
}

func (q Queue) Subscribe(ctx context.Context, consumer string, fromTick *int64) (StreamSubscription, error) {
	if err := validateConsumer(consumer); err != nil {
		return StreamSubscription{}, err
	}
	return withAutoCreate(ctx, q.s, ns.Queue, q.name, func(sc string) (StreamSubscription, error) {
		if fromTick != nil && *fromTick < 0 {
			return StreamSubscription{}, ErrInvalidStreamTick
		}
		var sub StreamSubscription
		err := q.s.pool.QueryRow(ctx, `SELECT consumer, last_tick, created_at, updated_at
			FROM `+sc+`.stream_subscribe($1,$2)`, consumer, fromTick).
			Scan(&sub.Consumer, &sub.LastTick, &sub.CreatedAt, &sub.UpdatedAt)
		if err != nil {
			return StreamSubscription{}, streamErr(err, nil)
		}
		return sub, nil
	})
}

func (q Queue) Unsubscribe(ctx context.Context, consumer string) (bool, error) {
	if q.err != nil {
		return false, q.err
	}
	var one int
	err := q.s.pool.QueryRow(ctx, `DELETE FROM `+q.sc+`.stream_subscriptions
		WHERE consumer=$1 RETURNING 1`, consumer).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return false, nil
	}
	return err == nil, err
}

func (q Queue) ReceiveBatch(ctx context.Context, consumer string, max int) (*StreamBatch, error) {
	return q.receiveBatch(ctx, consumer, "", max)
}

func (q Queue) ReceiveCoopBatch(ctx context.Context, consumer, member string, limit int) (*StreamBatch, error) {
	if err := validateConsumer(consumer); err != nil {
		return nil, err
	}
	if member == "" || len(member) > 128 {
		return nil, fmt.Errorf("stream member must be 1..128 bytes")
	}
	return q.receiveBatch(ctx, consumer, member, limit)
}

func (q Queue) receiveBatch(ctx context.Context, consumer, member string, limit int) (*StreamBatch, error) {
	if err := validateConsumer(consumer); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 100000 {
		limit = 100000
	}
	return withAutoCreate(ctx, q.s, ns.Queue, q.name, func(sc string) (*StreamBatch, error) {
		rows, err := q.s.pool.Query(ctx, `SELECT * FROM `+sc+`.stream_receive($1,$2,$3)`,
			consumer, member, limit)
		return collectStreamBatch(rows, err, nil)
	})
}

type streamBatchRow struct {
	batchID        pgtype.Int8
	consumer       pgtype.Text
	member         pgtype.Text
	startTick      pgtype.Int8
	endTick        pgtype.Int8
	batchCreatedAt pgtype.Timestamptz
	seq            pgtype.Int8
	eventID        pgtype.Text
	typ            pgtype.Text
	payload        []byte
	headers        []byte
	retryCount     pgtype.Int4
	createdAt      pgtype.Timestamptz
}

func collectStreamBatch(rows pgx.Rows, err, missing error) (*StreamBatch, error) {
	if err != nil {
		return nil, streamErr(err, missing)
	}
	defer rows.Close()
	var batch *StreamBatch
	for rows.Next() {
		var r streamBatchRow
		if err := rows.Scan(&r.batchID, &r.consumer, &r.member, &r.startTick, &r.endTick,
			&r.batchCreatedAt, &r.seq, &r.eventID, &r.typ, &r.payload, &r.headers,
			&r.retryCount, &r.createdAt); err != nil {
			return nil, streamErr(err, missing)
		}
		if !r.seq.Valid {
			batch = &StreamBatch{ID: r.batchID.Int64, Consumer: r.consumer.String, Member: r.member.String,
				StartTick: r.startTick.Int64, EndTick: r.endTick.Int64, CreatedAt: r.batchCreatedAt.Time}
			continue
		}
		batch.Messages = append(batch.Messages, StreamMessage{ID: r.eventID.String, Type: r.typ.String,
			Payload: r.payload, Headers: r.headers, RetryCount: int(r.retryCount.Int32), CreatedAt: r.createdAt.Time})
	}
	if err := rows.Err(); err != nil {
		return nil, streamErr(err, missing)
	}
	if batch == nil {
		return nil, ErrStreamBatchNotFound
	}
	return batch, nil
}

func (q Queue) GetBatch(ctx context.Context, batchID int64) (*StreamBatch, error) {
	if q.err != nil {
		return nil, q.err
	}
	rows, err := q.s.pool.Query(ctx, `SELECT * FROM `+q.sc+`.stream_batch($1)`, batchID)
	return collectStreamBatch(rows, err, ErrStreamBatchNotFound)
}

func (q Queue) AckBatch(ctx context.Context, batchID int64) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	var endTick int64
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_ack($1)`, batchID).Scan(&endTick)
	if err != nil {
		return 0, streamErr(err, ErrStreamBatchNotFound)
	}
	return endTick, nil
}

func (q Queue) NackMessage(ctx context.Context, batchID int64, eventID string, retryAfter time.Duration, reason string) (string, error) {
	if q.err != nil {
		return "", q.err
	}
	if retryAfter <= 0 {
		retryAfter = 60 * time.Second
	}
	var state string
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_nack($1,$2,$3,$4)`,
		batchID, eventID, retryAfter.Seconds(), reason).Scan(&state)
	if err != nil {
		return "", streamErr(err, ErrStreamBatchNotFound)
	}
	return state, nil
}

func (q Queue) ProcessRetries(ctx context.Context, max int) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	if max <= 0 {
		max = ExpireBatch
	}
	var moved int64
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_process_retries($1)`, max).Scan(&moved)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return moved, err
}

func (q Queue) Config(ctx context.Context) (StreamConfig, error) {
	if q.err != nil {
		return StreamConfig{}, q.err
	}
	var cfg StreamConfig
	var retention pgtype.Interval
	err := q.s.pool.QueryRow(ctx, `SELECT max_retries, retention
		FROM `+q.sc+`.stream_config`).Scan(&cfg.MaxRetries, &retention)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return StreamConfig{}, ErrQueueNotFound
	}
	if err != nil {
		return StreamConfig{}, err
	}
	cfg.Retention = intervalDuration(retention)
	return cfg, nil
}

func intervalDuration(v pgtype.Interval) time.Duration {
	if !v.Valid {
		return 0
	}
	return time.Duration(v.Microseconds) * time.Microsecond
}

func (q Queue) SetConfig(ctx context.Context, maxRetries int, retention time.Duration) (StreamConfig, error) {
	if q.err != nil {
		return StreamConfig{}, q.err
	}
	if maxRetries < 0 && retention <= 0 {
		return q.Config(ctx)
	}
	var cfg StreamConfig
	var outRetention pgtype.Interval
	err := q.s.pool.QueryRow(ctx, `UPDATE `+q.sc+`.stream_config
		SET max_retries = CASE WHEN $1::int4 >= 0 THEN $1 ELSE max_retries END,
		    retention = CASE WHEN $2::float8 > 0
		                     THEN make_interval(secs => $2) ELSE retention END
		RETURNING max_retries, retention`, maxRetries, retention.Seconds()).
		Scan(&cfg.MaxRetries, &outRetention)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return StreamConfig{}, ErrQueueNotFound
	}
	if err != nil {
		return StreamConfig{}, err
	}
	cfg.Retention = intervalDuration(outRetention)
	return cfg, nil
}

func (q Queue) Info(ctx context.Context) (StreamInfo, error) {
	if q.err != nil {
		return StreamInfo{}, q.err
	}
	var info StreamInfo
	var oldest pgtype.Timestamptz
	var retention pgtype.Interval
	err := q.s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM `+q.sc+`.stream_events),
		(SELECT count(*) FROM `+q.sc+`.stream_subscriptions),
		(SELECT count(*) FROM `+q.sc+`.stream_batches WHERE acked_at IS NULL),
		(SELECT count(*) FROM `+q.sc+`.stream_retry_events),
		(SELECT count(*) FROM `+q.sc+`.stream_dead_letters),
		(SELECT min(created_at) FROM `+q.sc+`.stream_events),
		(SELECT coalesce(max(tick_id),0) FROM `+q.sc+`.stream_ticks),
		(SELECT max_retries FROM `+q.sc+`.stream_config),
		(SELECT retention FROM `+q.sc+`.stream_config),
		(SELECT current_segment FROM `+q.sc+`.stream_config),
		(SELECT segment_started_at FROM `+q.sc+`.stream_config)`).
		Scan(&info.Events, &info.Consumers, &info.Active, &info.Retries,
			&info.DeadLetters, &oldest, &info.LastTick, &info.MaxRetries, &retention,
			&info.Segment, &info.SegmentStartedAt)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return StreamInfo{}, ErrQueueNotFound
	}
	if err != nil {
		return StreamInfo{}, err
	}
	if oldest.Valid {
		t := oldest.Time
		info.OldestEvent = &t
	}
	info.Retention = intervalDuration(retention)
	info.RotationPeriod = info.Retention / 2
	return info, nil
}

func (q Queue) ListConsumers(ctx context.Context) ([]StreamConsumerInfo, error) {
	if q.err != nil {
		return nil, q.err
	}
	rows, err := q.s.pool.Query(ctx, `SELECT s.consumer, s.last_tick,
			coalesce((SELECT b.batch_id FROM `+q.sc+`.stream_batches b
			          WHERE b.consumer=s.consumer AND b.acked_at IS NULL
			          ORDER BY b.batch_id LIMIT 1),0),
			(SELECT count(*) FROM `+q.sc+`.stream_events e
			 WHERE e.seq > coalesce((SELECT max_seq FROM `+q.sc+`.stream_ticks
			                         WHERE tick_id=s.last_tick), 0)),
			s.updated_at
		FROM `+q.sc+`.stream_subscriptions s
		ORDER BY s.consumer`)
	if isMissingBucketSchema(err) {
		return nil, ErrQueueNotFound
	}
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (StreamConsumerInfo, error) {
		var c StreamConsumerInfo
		err := row.Scan(&c.Consumer, &c.LastTick, &c.ActiveBatch, &c.Pending, &c.UpdatedAt)
		return c, err
	})
}

func (q Queue) ListDeadLetters(ctx context.Context, limit int) ([]DeadLetter, error) {
	if q.err != nil {
		return nil, q.err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := q.s.pool.Query(ctx, `SELECT dead_id, batch_id, event_id, type, payload,
			headers, retry_count, reason, created_at
		FROM `+q.sc+`.stream_dead_letters ORDER BY dead_id DESC LIMIT $1`, limit)
	if isMissingBucketSchema(err) {
		return nil, ErrQueueNotFound
	}
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (DeadLetter, error) {
		var d DeadLetter
		err := row.Scan(&d.ID, &d.BatchID, &d.EventID, &d.Type, &d.Payload,
			&d.Headers, &d.RetryCount, &d.Reason, &d.CreatedAt)
		return d, err
	})
}

func (q Queue) ReplayDeadLetter(ctx context.Context, id int64) (string, error) {
	if q.err != nil {
		return "", q.err
	}
	var eventID string
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_replay_dead_letter($1)`, id).Scan(&eventID)
	if err != nil {
		return "", streamErr(err, nil)
	}
	return eventID, nil
}

func (q Queue) ReplayAllDeadLetters(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	var replayed int64
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_replay_dead_letters()`).Scan(&replayed)
	return replayed, err
}

func (q Queue) PurgeDeadLetters(ctx context.Context, olderThan time.Duration) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	if olderThan <= 0 {
		olderThan = 30 * 24 * time.Hour
	}
	tag, err := q.s.pool.Exec(ctx, `DELETE FROM `+q.sc+`.stream_dead_letters
		WHERE created_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (q Queue) PruneTicks(ctx context.Context, max int) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	if max <= 0 {
		max = ExpireBatch
	}
	var pruned int64
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_prune_ticks($1)`, max).Scan(&pruned)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return pruned, err
}

func (q Queue) Rotate(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	var events int64
	err := q.s.pool.QueryRow(ctx, `SELECT `+q.sc+`.stream_rotate()`).Scan(&events)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	return events, err
}

func validateConsumer(consumer string) error {
	if consumer == "" || len(consumer) > 128 {
		return fmt.Errorf("stream consumer must be 1..128 bytes")
	}
	return nil
}
