package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

const streamSegmentCount = 3

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

func streamCurrentSegment(ctx context.Context, tx pgx.Tx, sc string) (int16, error) {
	var segment int16
	err := tx.QueryRow(ctx, `WITH _lock AS (
		SELECT pg_advisory_xact_lock_shared(hashtextextended($1, 0))
	)
	SELECT current_segment FROM `+sc+`.stream_config, _lock`, sc).Scan(&segment)
	return segment, err
}

func streamEventPartition(sc string, segment int16) string {
	return sc + ".stream_events_" + strconv.Itoa(int(segment))
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
		return withTx(ctx, q.s, func(tx pgx.Tx) ([]StreamMessage, error) {
			segment, err := streamCurrentSegment(ctx, tx, sc)
			if err != nil {
				return nil, err
			}
			out := make([]StreamMessage, 0, len(inputs))
			for _, in := range inputs {
				id := in.ID
				if id == "" {
					id = work.NewID()
				}
				if len(id) > 1024 {
					return nil, fmt.Errorf("stream event id exceeds 1024 bytes")
				}
				typ := in.Type
				if typ == "" {
					typ = "default"
				}
				payload := in.Payload
				if len(payload) == 0 {
					payload = []byte("{}")
				}
				headers := in.Headers
				if len(headers) == 0 {
					headers = []byte("{}")
				}
				var outMsg StreamMessage
				err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.stream_events
				(segment, event_id, type, payload, headers)
				VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
				RETURNING event_id, type, payload, headers, retry_count, created_at`,
					segment, id, typ, string(payload), string(headers)).
					Scan(&outMsg.ID, &outMsg.Type, &outMsg.Payload, &outMsg.Headers,
						&outMsg.RetryCount, &outMsg.CreatedAt)
				if err != nil {
					return nil, err
				}
				out = append(out, outMsg)
			}
			return out, nil
		})
	})
}

func (q Queue) Subscribe(ctx context.Context, consumer string, fromTick *int64) (StreamSubscription, error) {
	if err := validateConsumer(consumer); err != nil {
		return StreamSubscription{}, err
	}
	return withAutoCreate(ctx, q.s, ns.Queue, q.name, func(sc string) (StreamSubscription, error) {
		return withTx(ctx, q.s, func(tx pgx.Tx) (StreamSubscription, error) {
			if _, err := streamCurrentSegment(ctx, tx, sc); err != nil {
				return StreamSubscription{}, err
			}
			target, err := streamStartTick(ctx, tx, sc, fromTick)
			if err != nil {
				return StreamSubscription{}, err
			}
			var sub StreamSubscription
			err = tx.QueryRow(ctx, `INSERT INTO `+sc+`.stream_subscriptions
			(consumer, last_tick, next_tick) VALUES ($1, $2, $2)
			ON CONFLICT (consumer) DO NOTHING
			RETURNING consumer, last_tick, created_at, updated_at`, consumer, target).
				Scan(&sub.Consumer, &sub.LastTick, &sub.CreatedAt, &sub.UpdatedAt)
			if errors.Is(err, pgx.ErrNoRows) {
				err = tx.QueryRow(ctx, `SELECT consumer, last_tick, created_at, updated_at
				FROM `+sc+`.stream_subscriptions WHERE consumer=$1`, consumer).
					Scan(&sub.Consumer, &sub.LastTick, &sub.CreatedAt, &sub.UpdatedAt)
			}
			if err != nil {
				return StreamSubscription{}, err
			}
			return sub, nil
		})
	})
}

func streamStartTick(ctx context.Context, tx pgx.Tx, sc string, fromTick *int64) (int64, error) {
	if fromTick == nil {
		return latestStreamTick(ctx, tx, sc)
	}
	tick := *fromTick
	switch {
	case tick < 0:
		return 0, ErrInvalidStreamTick
	case tick == 0:
		if err := tx.QueryRow(ctx, `SELECT coalesce(min(tick_id),0) FROM `+sc+`.stream_ticks`).Scan(&tick); err != nil {
			return 0, err
		}
		if tick == 0 {
			return createStreamTick(ctx, tx, sc)
		}
		return tick, nil
	default:
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+sc+`.stream_ticks WHERE tick_id=$1)`, tick).Scan(&exists); err != nil {
			return 0, err
		}
		if !exists {
			return 0, ErrStreamTickNotFound
		}
		return tick, nil
	}
}

func latestStreamTick(ctx context.Context, tx pgx.Tx, sc string) (int64, error) {
	var tick int64
	if err := tx.QueryRow(ctx, `SELECT coalesce(max(tick_id), 0) FROM `+sc+`.stream_ticks`).Scan(&tick); err != nil {
		return 0, err
	}
	if tick == 0 {
		return createStreamTick(ctx, tx, sc)
	}
	return tick, nil
}

func createStreamTick(ctx context.Context, tx pgx.Tx, sc string) (int64, error) {
	var tick int64
	err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.stream_ticks (max_seq)
		VALUES ((SELECT coalesce(max(seq),0) FROM `+sc+`.stream_events)) RETURNING tick_id`).Scan(&tick)
	return tick, err
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
		return withTx(ctx, q.s, func(tx pgx.Tx) (*StreamBatch, error) {
			if _, err := streamCurrentSegment(ctx, tx, sc); err != nil {
				return nil, err
			}
			lastTick, err := ensureStreamSubscription(ctx, tx, sc, consumer)
			if err != nil {
				return nil, err
			}
			if batch, err := loadActiveStreamBatch(ctx, tx, sc, consumer, member); err != nil {
				return nil, err
			} else if batch != nil {
				return batch, nil
			}
			var nextTick int64
			if err := tx.QueryRow(ctx, `SELECT next_tick FROM `+sc+`.stream_subscriptions
			WHERE consumer=$1`, consumer).Scan(&nextTick); err != nil {
				return nil, err
			}
			if member == "" && nextTick > lastTick {
				return nil, ErrStreamBatchActive
			}
			startTick := max(nextTick, lastTick)
			var endTick int64
			if err := tx.QueryRow(ctx, `SELECT coalesce(max(tick_id),0) FROM `+sc+`.stream_ticks
			WHERE tick_id > $1`, startTick).Scan(&endTick); err != nil {
				return nil, err
			}
			if endTick == 0 {
				visible, err := streamHasVisibleEventsSince(ctx, tx, sc, startTick)
				if err != nil {
					return nil, err
				}
				if !visible {
					return &StreamBatch{Consumer: consumer, Member: member, StartTick: startTick, EndTick: startTick}, nil
				}
				if err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.stream_ticks (max_seq)
				VALUES ((SELECT coalesce(max(seq),0) FROM `+sc+`.stream_events))
				RETURNING tick_id`).Scan(&endTick); err != nil {
					return nil, err
				}
			}
			messages, seqs, err := selectStreamBatch(ctx, tx, sc, startTick, endTick, limit)
			if err != nil {
				return nil, err
			}
			if len(messages) == 0 {
				advance := "$2"
				if member != "" {
					advance = `CASE WHEN EXISTS (SELECT 1 FROM ` + sc + `.stream_batches
					WHERE consumer=$1 AND acked_at IS NULL) THEN last_tick ELSE $2 END`
				}
				if _, err := tx.Exec(ctx, `UPDATE `+sc+`.stream_subscriptions
				SET next_tick=$2, last_tick=`+advance+`,
				    updated_at=now()
				WHERE consumer=$1`, consumer, endTick); err != nil {
					return nil, err
				}
				return &StreamBatch{Consumer: consumer, Member: member, StartTick: startTick, EndTick: endTick}, nil
			}
			var batchID int64
			if err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.stream_batches
			(consumer, member, start_tick, end_tick) VALUES ($1,$2,$3,$4)
			RETURNING batch_id`, consumer, member, startTick, endTick).Scan(&batchID); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+sc+`.stream_subscriptions
			SET next_tick=$2, updated_at=now() WHERE consumer=$1`, consumer, endTick); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+sc+`.stream_batch_events (batch_id, seq)
			SELECT $1, unnest($2::bigint[])`, batchID, seqs); err != nil {
				return nil, err
			}
			return &StreamBatch{
				ID: batchID, Consumer: consumer, Member: member, StartTick: startTick,
				EndTick: endTick, Messages: messages,
			}, nil
		})
	})
}

func ensureStreamSubscription(ctx context.Context, tx pgx.Tx, sc, consumer string) (int64, error) {
	var tick int64
	err := tx.QueryRow(ctx, `SELECT last_tick FROM `+sc+`.stream_subscriptions
		WHERE consumer=$1 FOR UPDATE`, consumer).Scan(&tick)
	if err == nil {
		return tick, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	tick, err = streamStartTick(ctx, tx, sc, nil)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+sc+`.stream_subscriptions
		(consumer, last_tick, next_tick) VALUES ($1,$2,$2)`, consumer, tick)
	return tick, err
}

func loadActiveStreamBatch(ctx context.Context, tx pgx.Tx, sc, consumer, member string) (*StreamBatch, error) {
	var batch StreamBatch
	err := tx.QueryRow(ctx, `SELECT batch_id, consumer, member, start_tick, end_tick, created_at
		FROM `+sc+`.stream_batches
		WHERE consumer=$1 AND member=$2 AND acked_at IS NULL`, consumer, member).
		Scan(&batch.ID, &batch.Consumer, &batch.Member, &batch.StartTick, &batch.EndTick, &batch.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	messages, _, err := selectStreamBatchEvents(ctx, tx, sc, batch.ID)
	if err != nil {
		return nil, err
	}
	batch.Messages = messages
	return &batch, nil
}

func streamHasVisibleEventsSince(ctx context.Context, tx pgx.Tx, sc string, startTick int64) (bool, error) {
	var visible bool
	err := tx.QueryRow(ctx, `WITH cur AS (SELECT pg_current_snapshot() AS snapshot)
		SELECT EXISTS (
			SELECT 1 FROM `+sc+`.stream_events e, `+sc+`.stream_ticks last, cur
			WHERE last.tick_id=$1
			  AND pg_visible_in_snapshot(e.txid, cur.snapshot)
			  AND NOT pg_visible_in_snapshot(e.txid, last.snapshot)
			  AND (e.txid >= pg_snapshot_xmax(last.snapshot)
			       OR e.txid IN (
			         SELECT * FROM pg_snapshot_xip(last.snapshot)
			         EXCEPT
			         SELECT * FROM pg_snapshot_xip(cur.snapshot)
			       ))
		)`, startTick).Scan(&visible)
	return visible, err
}

func selectStreamBatch(ctx context.Context, tx pgx.Tx, sc string, startTick, endTick int64, max int) ([]StreamMessage, []int64, error) {
	rows, err := tx.Query(ctx, `SELECT e.seq, e.event_id, e.type, e.payload, e.headers,
			e.retry_count, e.created_at
		FROM `+sc+`.stream_events e, `+sc+`.stream_ticks last, `+sc+`.stream_ticks cur
		WHERE last.tick_id=$1 AND cur.tick_id=$2
		  AND pg_visible_in_snapshot(e.txid, cur.snapshot)
		  AND NOT pg_visible_in_snapshot(e.txid, last.snapshot)
		  AND (e.txid >= pg_snapshot_xmax(last.snapshot)
		       OR e.txid IN (
		         SELECT * FROM pg_snapshot_xip(last.snapshot)
		         EXCEPT
		         SELECT * FROM pg_snapshot_xip(cur.snapshot)
		       ))
		ORDER BY e.seq
		LIMIT $3`, startTick, endTick, max+1)
	if err != nil {
		return nil, nil, err
	}
	messages, seqs, err := collectStreamMessages(rows, max)
	if err != nil {
		return nil, nil, err
	}
	if len(messages) > max {
		return nil, nil, ErrStreamBatchTooLarge
	}
	return messages, seqs, nil
}

func selectStreamBatchEvents(ctx context.Context, tx pgx.Tx, sc string, batchID int64) ([]StreamMessage, []int64, error) {
	rows, err := tx.Query(ctx, `SELECT e.seq, e.event_id, e.type, e.payload, e.headers,
			e.retry_count, e.created_at
		FROM `+sc+`.stream_batch_events be
		JOIN `+sc+`.stream_events e ON e.seq = be.seq
		WHERE be.batch_id=$1 ORDER BY be.seq`, batchID)
	if err != nil {
		return nil, nil, err
	}
	return collectStreamMessages(rows, 0)
}

func collectStreamMessages(rows pgx.Rows, capacity int) ([]StreamMessage, []int64, error) {
	defer rows.Close()
	var messages []StreamMessage
	var seqs []int64
	if capacity > 0 {
		messages = make([]StreamMessage, 0, capacity)
		seqs = make([]int64, 0, capacity)
	}
	for rows.Next() {
		var m StreamMessage
		var seq int64
		if err := rows.Scan(&seq, &m.ID, &m.Type, &m.Payload, &m.Headers,
			&m.RetryCount, &m.CreatedAt); err != nil {
			return nil, nil, err
		}
		messages = append(messages, m)
		seqs = append(seqs, seq)
	}
	return messages, seqs, rows.Err()
}

func (q Queue) GetBatch(ctx context.Context, batchID int64) (*StreamBatch, error) {
	if q.err != nil {
		return nil, q.err
	}
	return withTx(ctx, q.s, func(tx pgx.Tx) (*StreamBatch, error) {
		var batch StreamBatch
		err := tx.QueryRow(ctx, `SELECT batch_id, consumer, member, start_tick, end_tick, created_at
		FROM `+q.sc+`.stream_batches WHERE batch_id=$1`, batchID).
			Scan(&batch.ID, &batch.Consumer, &batch.Member, &batch.StartTick, &batch.EndTick, &batch.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return nil, ErrStreamBatchNotFound
		}
		if err != nil {
			return nil, err
		}
		messages, _, err := selectStreamBatchEvents(ctx, tx, q.sc, batchID)
		if err != nil {
			return nil, err
		}
		batch.Messages = messages
		return &batch, nil
	})
}

func (q Queue) AckBatch(ctx context.Context, batchID int64) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	return withTx(ctx, q.s, func(tx pgx.Tx) (int64, error) {
		var (
			consumer string
			endTick  int64
			acked    pgtype.Timestamptz
		)
		err := tx.QueryRow(ctx, `SELECT consumer, end_tick, acked_at FROM `+q.sc+`.stream_batches
		WHERE batch_id=$1 FOR UPDATE`, batchID).Scan(&consumer, &endTick, &acked)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return 0, ErrStreamBatchNotFound
		}
		if err != nil {
			return 0, err
		}
		if acked.Valid {
			return 0, ErrStreamBatchNotFound
		}
		if _, err := tx.Exec(ctx, `UPDATE `+q.sc+`.stream_batches
		SET acked_at=now() WHERE batch_id=$1`, batchID); err != nil {
			return 0, err
		}

		for {
			var (
				nextID  int64
				nextEnd int64
			)
			err := tx.QueryRow(ctx, `SELECT b.batch_id, b.end_tick
			FROM `+q.sc+`.stream_batches b
			WHERE b.consumer=$1 AND b.acked_at IS NOT NULL
			  AND b.start_tick=(SELECT last_tick FROM `+q.sc+`.stream_subscriptions
			                    WHERE consumer=$1)
			ORDER BY b.start_tick LIMIT 1`, consumer).Scan(&nextID, &nextEnd)
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			if err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+q.sc+`.stream_subscriptions
			SET last_tick=$2, next_tick=GREATEST(next_tick,$2), updated_at=now()
			WHERE consumer=$1`, consumer, nextEnd); err != nil {
				return 0, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM `+q.sc+`.stream_batches
			WHERE batch_id=$1`, nextID); err != nil {
				return 0, err
			}
		}
		return endTick, nil
	})
}

func (q Queue) NackMessage(ctx context.Context, batchID int64, eventID string, retryAfter time.Duration, reason string) (string, error) {
	if q.err != nil {
		return "", q.err
	}
	if retryAfter <= 0 {
		retryAfter = 60 * time.Second
	}
	return withTx(ctx, q.s, func(tx pgx.Tx) (string, error) {
		var consumer string
		err := tx.QueryRow(ctx, `SELECT consumer FROM `+q.sc+`.stream_batches
		WHERE batch_id=$1 AND acked_at IS NULL FOR UPDATE`, batchID).Scan(&consumer)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return "", ErrStreamBatchNotFound
		}
		if err != nil {
			return "", err
		}
		var (
			typ     string
			payload []byte
			headers []byte
			retry   int
		)
		err = tx.QueryRow(ctx, `SELECT e.type, e.payload, e.headers, e.retry_count
		FROM `+q.sc+`.stream_batch_events be
		JOIN `+q.sc+`.stream_events e ON e.seq = be.seq
		WHERE be.batch_id=$1 AND e.event_id=$2`, batchID, eventID).
			Scan(&typ, &payload, &headers, &retry)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrStreamMessageNotFound
		}
		if err != nil {
			return "", err
		}
		var maxRetries int
		err = tx.QueryRow(ctx, `SELECT max_retries FROM `+q.sc+`.stream_config`).Scan(&maxRetries)
		if errors.Is(err, pgx.ErrNoRows) {
			maxRetries = 5
		} else if err != nil {
			return "", err
		}
		next := retry + 1
		if next > maxRetries {
			_, err = tx.Exec(ctx, `INSERT INTO `+q.sc+`.stream_dead_letters
			(batch_id, event_id, type, payload, headers, retry_count, reason)
			VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6,$7)
			ON CONFLICT (batch_id, event_id) DO NOTHING`,
				batchID, eventID, typ, string(payload), string(headers), next, reason)
			if err != nil {
				return "", err
			}
			return "dead_letter", nil
		}
		_, err = tx.Exec(ctx, `INSERT INTO `+q.sc+`.stream_retry_events
		(batch_id, event_id, type, payload, headers, retry_count, due_at)
		VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6, now() + make_interval(secs => $7))
		ON CONFLICT (batch_id, event_id) DO NOTHING`,
			batchID, eventID, typ, string(payload), string(headers), next, retryAfter.Seconds())
		if err != nil {
			return "", err
		}
		return "retryable", nil
	})
}

func (q Queue) ProcessRetries(ctx context.Context, max int) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	if max <= 0 {
		max = ExpireBatch
	}
	tx, err := q.s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	segment, err := streamCurrentSegment(ctx, tx, q.sc)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var moved int64
	err = tx.QueryRow(ctx, `WITH due AS (
			DELETE FROM `+q.sc+`.stream_retry_events WHERE retry_id IN (
				SELECT retry_id FROM `+q.sc+`.stream_retry_events
				WHERE due_at <= now() ORDER BY due_at LIMIT $1
			) RETURNING event_id, type, payload, headers, retry_count
		), inserted AS (
			INSERT INTO `+q.sc+`.stream_events
				(segment, event_id, type, payload, headers, retry_count)
			SELECT $2, event_id, type, payload, headers, retry_count FROM due
			RETURNING 1
		)
		SELECT count(*) FROM inserted`, max, segment).Scan(&moved)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return moved, nil
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
	return withTx(ctx, q.s, func(tx pgx.Tx) (string, error) {
		segment, err := streamCurrentSegment(ctx, tx, q.sc)
		if err != nil {
			return "", err
		}
		var (
			eventID string
			typ     string
			payload []byte
			headers []byte
		)
		err = tx.QueryRow(ctx, `SELECT event_id, type, payload, headers
		FROM `+q.sc+`.stream_dead_letters WHERE dead_id=$1 FOR UPDATE`, id).
			Scan(&eventID, &typ, &payload, &headers)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return "", ErrStreamMessageNotFound
		}
		if err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+q.sc+`.stream_events
		(segment, event_id, type, payload, headers, retry_count)
		VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,0)`,
			segment, eventID, typ, string(payload), string(headers)); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM `+q.sc+`.stream_dead_letters WHERE dead_id=$1`, id); err != nil {
			return "", err
		}
		return eventID, nil
	})
}

func (q Queue) ReplayAllDeadLetters(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	tx, err := q.s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	segment, err := streamCurrentSegment(ctx, tx, q.sc)
	if err != nil {
		return 0, err
	}
	var replayed int64
	err = tx.QueryRow(ctx, `WITH gone AS (
			DELETE FROM `+q.sc+`.stream_dead_letters
			RETURNING event_id, type, payload, headers
		), inserted AS (
			INSERT INTO `+q.sc+`.stream_events
				(segment, event_id, type, payload, headers, retry_count)
			SELECT $1, event_id, type, payload, headers, 0 FROM gone
			RETURNING 1
		)
		SELECT count(*) FROM inserted`, segment).Scan(&replayed)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return replayed, nil
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
	tag, err := q.s.pool.Exec(ctx, `DELETE FROM `+q.sc+`.stream_ticks t
		WHERE t.tick_id <> (SELECT min(tick_id) FROM `+q.sc+`.stream_ticks)
		  AND t.created_at < now() - (SELECT retention FROM `+q.sc+`.stream_config)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_subscriptions s WHERE s.last_tick=t.tick_id)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_batches b
		                  WHERE b.start_tick=t.tick_id OR b.end_tick=t.tick_id)
		  AND t.tick_id IN (
		    SELECT tick_id FROM `+q.sc+`.stream_ticks
		    WHERE tick_id <> (SELECT min(tick_id) FROM `+q.sc+`.stream_ticks)
		    ORDER BY tick_id LIMIT $1
		  )`, max)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (q Queue) Rotate(ctx context.Context) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	tx, err := q.s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, q.sc); err != nil {
		return 0, err
	}

	var (
		current int16
		due     bool
	)
	err = tx.QueryRow(ctx, `SELECT current_segment,
			(segment_started_at + retention / 2.0) <= now()
		FROM `+q.sc+`.stream_config FOR UPDATE`).Scan(&current, &due)
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !due {
		return 0, nil
	}
	target := int16((int(current) + 1) % streamSegmentCount)

	var (
		events  int64
		pinned  bool
		batched bool
	)
	err = tx.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+q.sc+`.stream_events WHERE segment=$1),
			EXISTS (
				SELECT 1 FROM `+q.sc+`.stream_segments seg
				WHERE seg.segment=$1
				  AND (seg.sealed_xid IS NULL OR EXISTS (
					SELECT 1 FROM `+q.sc+`.stream_subscriptions s
					LEFT JOIN `+q.sc+`.stream_ticks t ON t.tick_id=s.last_tick
					WHERE t.tick_id IS NULL
					   OR pg_snapshot_xmin(t.snapshot) <= seg.sealed_xid
				  ))
			),
			EXISTS (
				SELECT 1 FROM `+q.sc+`.stream_batch_events be
				JOIN `+q.sc+`.stream_events e ON e.seq=be.seq
				WHERE e.segment=$1
			)`, target).Scan(&events, &pinned, &batched)
	if err != nil {
		return 0, err
	}
	if events > 0 && (pinned || batched) {
		return 0, nil
	}

	partition := streamEventPartition(q.sc, target)
	if _, err := tx.Exec(ctx, `LOCK TABLE `+partition+` IN ACCESS EXCLUSIVE MODE NOWAIT`); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			return 0, nil
		}
		return 0, err
	}
	if _, err := tx.Exec(ctx, `TRUNCATE TABLE `+partition); err != nil {
		return 0, err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM `+q.sc+`.stream_ticks t
		WHERE t.tick_id <> (SELECT min(tick_id) FROM `+q.sc+`.stream_ticks)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_subscriptions s WHERE s.last_tick=t.tick_id)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_batches b
		                  WHERE b.start_tick=t.tick_id OR b.end_tick=t.tick_id)
		  AND pg_snapshot_xmin(t.snapshot) <= (
		    SELECT sealed_xid FROM `+q.sc+`.stream_segments WHERE segment=$1
		  )`, target); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+q.sc+`.stream_segments
		SET sealed_xid=pg_current_xact_id(), sealed_at=now()
		WHERE segment=$1`, current); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+q.sc+`.stream_segments
		SET sealed_xid=NULL, sealed_at=NULL
		WHERE segment=$1`, target); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `UPDATE `+q.sc+`.stream_config
		SET current_segment=$1, segment_started_at=now()`, target); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+q.sc+`.stream_ticks t
		WHERE t.tick_id <> (SELECT min(tick_id) FROM `+q.sc+`.stream_ticks)
		  AND t.created_at < now() - (SELECT retention FROM `+q.sc+`.stream_config)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_subscriptions s WHERE s.last_tick=t.tick_id)
		  AND NOT EXISTS (SELECT 1 FROM `+q.sc+`.stream_batches b
		                  WHERE b.start_tick=t.tick_id OR b.end_tick=t.tick_id)`); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return events, nil
}

func validateConsumer(consumer string) error {
	if consumer == "" || len(consumer) > 128 {
		return fmt.Errorf("stream consumer must be 1..128 bytes")
	}
	return nil
}
