package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adonm/pgvs3/internal/ns"
)

func totalBodies(msgs []OutMessage) int64 {
	var n int64
	for _, m := range msgs {
		n += int64(len(m.Body))
	}
	return n
}

func md5Hex(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// ErrReceiptInvalid is returned when a receipt handle does not match a
// currently visible message.
var ErrReceiptInvalid = errors.New("receipt handle is invalid")

// ErrReceiptStale is returned when the message still exists but the handle was
// superseded by a later receive (the consumer took too long).
var ErrReceiptStale = errors.New("receipt handle is stale")

// Message is one received queue message.
type Message struct {
	ID           string
	Body         []byte
	Attrs        []byte
	Receipt      string
	ReceiveCount int32
	SentAt       time.Time
	GroupID      string
	DedupID      string
	SequenceNum  string
}

// OutMessage is one message to enqueue.
type OutMessage struct {
	Body    []byte
	Attrs   []byte
	Delay   int // seconds
	GroupID string
	DedupID string
}

// SentMessage is the result of one enqueue.
type SentMessage struct {
	ID          string
	MD5OfBody   string
	SentAt      time.Time
	DedupID     string
	SequenceNum string
}

func newToken(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SendMessages enqueues a batch of messages in one transaction.
func (s *Store) SendMessages(ctx context.Context, queue string, msgs []OutMessage, retention time.Duration) ([]SentMessage, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if s.checkQuota(ctx, ns.Queue, queue, totalBodies(msgs)) {
		return nil, ErrQuotaExceeded
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return nil, err
	}
	if !s.known(ns.Queue, queue) {
		if err := s.EnsureNamespace(ctx, ns.Queue, queue, nil, nil); err != nil {
			return nil, err
		}
	}
	for _, m := range msgs {
		if m.DedupID != "" {
			return s.sendMessagesFIFO(ctx, queue, sc, msgs, retention)
		}
	}
	return s.sendMessagesBulk(ctx, queue, sc, msgs, retention)
}

func (s *Store) sendMessagesBulk(ctx context.Context, queue, sc string, msgs []OutMessage, retention time.Duration) ([]SentMessage, error) {
	ids := make([]string, len(msgs))
	bodies := make([][]byte, len(msgs))
	sizes := make([]int32, len(msgs))
	attrs := make([][]byte, len(msgs))
	groups := make([]string, len(msgs))
	dedups := make([]string, len(msgs))
	delays := make([]int32, len(msgs))
	expiries := make([]time.Time, len(msgs))
	now := time.Now()
	for i, m := range msgs {
		ids[i] = newToken(16)
		bodies[i] = compressBlock(nil, m.Body)
		sizes[i] = int32(len(m.Body))
		attrs[i] = m.Attrs
		groups[i] = m.GroupID
		dedups[i] = m.DedupID
		delays[i] = int32(m.Delay)
		expiries[i] = now.Add(retention)
	}
	rows, err := s.pool.Query(ctx, `INSERT INTO `+sc+`.messages
		(msg_id, body, body_size, attrs, group_id, dedup_id, visible_at, expires_at, sent_at)
		SELECT id, b, sz, a, NULLIF(g, ''), NULLIF(d, ''),
		       now() + make_interval(secs => dl), e, now()
		FROM unnest($1::text[], $2::bytea[], $3::int4[], $4::bytea[], $5::text[], $6::text[], $7::int4[], $8::timestamptz[])
		  AS t(id, b, sz, a, g, d, dl, e)
		RETURNING msg_id, sent_at, id`,
		ids, bodies, sizes, attrs, groups, dedups, delays, expiries)
	if isMissingBucketSchema(err) {
		if err := s.EnsureNamespace(ctx, ns.Queue, queue, nil, nil); err != nil {
			return nil, err
		}
		return s.SendMessages(ctx, queue, msgs, retention)
	}
	if err != nil {
		return nil, err
	}
	byID := make(map[string]SentMessage, len(ids))
	for rows.Next() {
		var id string
		var sent time.Time
		var seq int64
		if err := rows.Scan(&id, &sent, &seq); err != nil {
			rows.Close()
			return nil, err
		}
		byID[id] = SentMessage{ID: id, SentAt: sent, SequenceNum: strconv.FormatInt(seq, 10)}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]SentMessage, len(msgs))
	for i := range msgs {
		out[i] = byID[ids[i]]
		out[i].MD5OfBody = md5Hex(msgs[i].Body)
		out[i].DedupID = msgs[i].DedupID
	}
	return out, nil
}

// sendMessagesFIFO serializes deduplication and enqueue in one transaction.
// A duplicate dedup ID returns the original message ID and sequence number
// without inserting another message.
func (s *Store) sendMessagesFIFO(ctx context.Context, queue, sc string, msgs []OutMessage, retention time.Duration) ([]SentMessage, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err := tx.Exec(ctx, "DELETE FROM "+sc+".dedup WHERE expires_at <= now()"); err != nil {
		return nil, err
	}
	out := make([]SentMessage, len(msgs))
	for i, m := range msgs {
		msgID := newToken(16)
		if m.DedupID != "" {
			var (
				existingID string
				sequence   int64
				inserted   bool
			)
			err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.dedup (dedup_id, msg_id, expires_at)
				VALUES ($1, $2, now() + interval '5 minutes')
				ON CONFLICT (dedup_id) DO UPDATE SET dedup_id = EXCLUDED.dedup_id
				RETURNING msg_id, sequence, (xmax = 0)`, m.DedupID, msgID).
				Scan(&existingID, &sequence, &inserted)
			if err != nil {
				return nil, err
			}
			if !inserted {
				out[i] = SentMessage{
					ID: existingID, MD5OfBody: md5Hex(m.Body), SentAt: time.Now(),
					DedupID: m.DedupID, SequenceNum: strconv.FormatInt(sequence, 10),
				}
				continue
			}
		}
		var rowID int64
		err := tx.QueryRow(ctx, `INSERT INTO `+sc+`.messages
			(msg_id, body, body_size, attrs, group_id, dedup_id, visible_at, expires_at, sent_at)
			VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''),
			        now() + make_interval(secs => $7), $8, now())
			RETURNING id`,
			msgID, compressBlock(nil, m.Body), int32(len(m.Body)), m.Attrs,
			m.GroupID, m.DedupID, m.Delay, time.Now().Add(retention)).Scan(&rowID)
		if err != nil {
			return nil, err
		}
		out[i] = SentMessage{
			ID: msgID, MD5OfBody: md5Hex(m.Body), SentAt: time.Now(),
			DedupID: m.DedupID, SequenceNum: strconv.FormatInt(rowID, 10),
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// ReceiveMessages leases up to max visible messages for visibility seconds.
// When fifo is true, a message group with an in-flight lease is skipped so
// messages in that group are delivered one at a time in id order.
func (s *Store) ReceiveMessages(ctx context.Context, queue string, max, visibility int, attrs, fifo bool) ([]Message, error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return nil, err
	}
	token := newToken(12)
	query := `WITH picked AS (
		SELECT id FROM ` + sc + `.messages
		WHERE visible_at <= now() AND expires_at > now()
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	), upd AS (
		UPDATE ` + sc + `.messages m
		SET receipt = $2 || ':' || m.id,
		    visible_at = now() + make_interval(secs => $3),
		    receive_count = m.receive_count + 1,
		    first_received_at = coalesce(m.first_received_at, now())
		FROM picked WHERE m.id = picked.id
		RETURNING m.id, m.msg_id, m.body, m.body_size, m.attrs, m.receipt, m.receive_count, m.sent_at,
		          m.group_id, m.dedup_id
	)
	SELECT id, msg_id, body, body_size, attrs, receipt, receive_count, sent_at,
	       coalesce(group_id, ''), coalesce(dedup_id, '')
	FROM upd ORDER BY id`
	args := []any{max, token, visibility}
	if fifo {
		query = `WITH groups AS (
			SELECT DISTINCT m.group_id
			FROM ` + sc + `.messages m
			WHERE m.visible_at <= now() AND m.expires_at > now()
			  AND m.group_id IS NOT NULL
			  AND NOT EXISTS (
			    SELECT 1 FROM ` + sc + `.messages o
			    WHERE o.group_id = m.group_id
			      AND o.visible_at > now() AND o.receipt IS NOT NULL)
			LIMIT $1
		), candidates AS (
			SELECT DISTINCT ON (m.group_id) m.id
			FROM ` + sc + `.messages m
			JOIN groups g ON g.group_id = m.group_id
			WHERE m.visible_at <= now() AND m.expires_at > now()
			ORDER BY m.group_id, m.id
		), picked AS (
			SELECT m.id
			FROM ` + sc + `.messages m
			JOIN candidates c ON c.id = m.id
			FOR UPDATE OF m SKIP LOCKED
		), upd AS (
			UPDATE ` + sc + `.messages m
			SET receipt = $2 || ':' || m.id,
			    visible_at = now() + make_interval(secs => $3),
			    receive_count = m.receive_count + 1,
			    first_received_at = coalesce(m.first_received_at, now())
			FROM picked WHERE m.id = picked.id
			RETURNING m.id, m.msg_id, m.body, m.body_size, m.attrs, m.receipt, m.receive_count, m.sent_at,
			          m.group_id, m.dedup_id
		)
		SELECT id, msg_id, body, body_size, attrs, receipt, receive_count, sent_at,
		       coalesce(group_id, ''), coalesce(dedup_id, '')
		FROM upd ORDER BY id`
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if isMissingBucketSchema(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var (
			id       int64
			msgID    string
			body     []byte
			size     int32
			msgAttrs []byte
			receipt  string
			count    int32
			sent     time.Time
			groupID  string
			dedupID  string
		)
		if err := rows.Scan(&id, &msgID, &body, &size, &msgAttrs, &receipt, &count, &sent, &groupID, &dedupID); err != nil {
			return nil, err
		}
		raw, err := decompressBlock(nil, body, int(size))
		if err != nil {
			return nil, err
		}
		m := Message{
			ID: msgID, Body: raw, Receipt: receipt, ReceiveCount: count, SentAt: sent,
			GroupID: groupID, DedupID: dedupID, SequenceNum: strconv.FormatInt(id, 10),
		}
		if attrs {
			m.Attrs = msgAttrs
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteMessages removes a batch of leased messages in one statement. The
// returned slice is aligned with receipts; a nil entry means the message was
// deleted or had already disappeared. Invalid/stale receipts return an error
// for that entry.
func (s *Store) DeleteMessages(ctx context.Context, queue string, receipts []string) ([]error, error) {
	errs := make([]error, len(receipts))
	if len(receipts) == 0 {
		return errs, nil
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(receipts))
	for i, receipt := range receipts {
		id, perr := strconv.ParseInt(receiptMsgID(receipt), 10, 64)
		if perr != nil {
			errs[i] = ErrReceiptInvalid
			continue
		}
		ids = append(ids, id)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	current := make(map[int64]string, len(ids))
	if len(ids) > 0 {
		rows, err := tx.Query(ctx, "SELECT id, receipt FROM "+sc+".messages WHERE id = ANY($1::int8[]) FOR UPDATE", ids)
		if isMissingBucketSchema(err) {
			for i := range errs {
				if errs[i] == nil {
					errs[i] = ErrReceiptInvalid
				}
			}
			return errs, nil
		}
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id int64
			var receipt string
			if err := rows.Scan(&id, &receipt); err != nil {
				rows.Close()
				return nil, err
			}
			current[id] = receipt
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+sc+".messages WHERE receipt = ANY($1::text[])", receipts); err != nil {
		return nil, err
	}
	for i, receipt := range receipts {
		if errs[i] != nil {
			continue
		}
		id, _ := strconv.ParseInt(receiptMsgID(receipt), 10, 64)
		if got, ok := current[id]; ok && got != receipt {
			errs[i] = ErrReceiptStale
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return errs, nil
}

// ChangeVisibilityBatch updates a batch of leases in one statement. The
// returned slice is aligned with receipts; errors identify stale/invalid
// receipts.
func (s *Store) ChangeVisibilityBatch(ctx context.Context, queue string, receipts []string, visibility []int32) ([]error, error) {
	errs := make([]error, len(receipts))
	if len(receipts) == 0 {
		return errs, nil
	}
	if len(receipts) != len(visibility) {
		return nil, fmt.Errorf("receipt/visibility length mismatch")
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `UPDATE `+sc+`.messages m
		SET visible_at = now() + make_interval(secs => t.visibility)
		FROM unnest($1::text[], $2::int4[]) AS t(receipt, visibility)
		WHERE m.receipt = t.receipt
		RETURNING m.receipt`, receipts, visibility)
	if isMissingBucketSchema(err) {
		for i := range errs {
			errs[i] = ErrReceiptInvalid
		}
		return errs, nil
	}
	if err != nil {
		return nil, err
	}
	updated := make(map[string]struct{}, len(receipts))
	for rows.Next() {
		var receipt string
		if err := rows.Scan(&receipt); err != nil {
			rows.Close()
			return nil, err
		}
		updated[receipt] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	for i, receipt := range receipts {
		if _, ok := updated[receipt]; ok {
			continue
		}
		gone, err := s.classifyMissingReceipt(ctx, sc, receipt)
		switch {
		case err != nil:
			errs[i] = err
		case gone:
			errs[i] = ErrReceiptInvalid
		default:
			errs[i] = ErrReceiptStale
		}
	}
	return errs, nil
}

// DeleteMessage removes one leased message. A stale receipt is an error.
func (s *Store) DeleteMessage(ctx context.Context, queue, receipt string) (bool, error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, "DELETE FROM "+sc+".messages WHERE receipt = $1", receipt)
	if isMissingBucketSchema(err) {
		return false, ErrReceiptInvalid
	}
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	return s.classifyMissingReceipt(ctx, sc, receipt)
}

// ChangeVisibility extends or resets one message's visibility timeout.
func (s *Store) ChangeVisibility(ctx context.Context, queue, receipt string, visibility int) (bool, error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE `+sc+`.messages
		SET visible_at = now() + make_interval(secs => $2)
		WHERE receipt = $1`, receipt, visibility)
	if isMissingBucketSchema(err) {
		return false, ErrReceiptInvalid
	}
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	if _, err := s.classifyMissingReceipt(ctx, sc, receipt); err != nil {
		return false, err
	}
	return false, ErrReceiptInvalid
}

// classifyMissingReceipt distinguishes an already-deleted message (returns
// true, nil) from a handle superseded by a later receive (ErrReceiptStale) or a
// malformed handle (ErrReceiptInvalid).
func (s *Store) classifyMissingReceipt(ctx context.Context, sc, receipt string) (bool, error) {
	id, err := strconv.ParseInt(receiptMsgID(receipt), 10, 64)
	if err != nil {
		return false, ErrReceiptInvalid
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+sc+".messages WHERE id = $1)", id).Scan(&exists); err != nil {
		if isMissingBucketSchema(err) {
			return false, ErrReceiptInvalid
		}
		return false, err
	}
	if exists {
		return false, ErrReceiptStale
	}
	return true, nil
}

// Redrive moves messages by receipt from one queue into a dead-letter queue.
func (s *Store) Redrive(ctx context.Context, src, dst string, receipts []string, expires time.Time) (int, error) {
	if len(receipts) == 0 {
		return 0, nil
	}
	ssc, err := nsSQLIdent(ns.Queue, src)
	if err != nil {
		return 0, err
	}
	dsc, err := nsSQLIdent(ns.Queue, dst)
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	rows, err := tx.Query(ctx, `SELECT msg_id, body, body_size, attrs, sent_at, group_id, dedup_id FROM `+ssc+`.messages
		WHERE receipt = ANY($1::text[]) FOR UPDATE`, receipts)
	if err != nil {
		return 0, err
	}
	var (
		ids    []string
		bodies [][]byte
		sizes  []int32
		attrs  [][]byte
		sent   []time.Time
		groups []string
		dedups []string
	)
	for rows.Next() {
		var id string
		var body []byte
		var size int32
		var a []byte
		var ts time.Time
		var g, d pgtype.Text
		if err := rows.Scan(&id, &body, &size, &a, &ts, &g, &d); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		bodies = append(bodies, body)
		sizes = append(sizes, size)
		attrs = append(attrs, a)
		sent = append(sent, ts)
		groups = append(groups, g.String)
		dedups = append(dedups, d.String)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+dsc+`.messages
		(msg_id, body, body_size, attrs, group_id, dedup_id, visible_at, expires_at, sent_at)
		SELECT id, b, sz, a, NULLIF(g, ''), NULLIF(d, ''), now(), $8, ts
		FROM unnest($1::text[], $2::bytea[], $3::int4[], $4::bytea[], $5::timestamptz[], $6::text[], $7::text[])
		  AS t(id, b, sz, a, ts, g, d)
		ON CONFLICT (msg_id) DO NOTHING`, ids, bodies, sizes, attrs, sent, groups, dedups, expires); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+dsc+`.dedup (dedup_id, msg_id, expires_at)
		SELECT d, id, now() + interval '5 minutes'
		FROM unnest($1::text[], $2::text[]) AS t(id, d)
		WHERE d IS NOT NULL AND d <> ''
		ON CONFLICT (dedup_id) DO NOTHING`, ids, dedups); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, "DELETE FROM "+ssc+".messages WHERE receipt = ANY($1::text[])", receipts); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func receiptMsgID(receipt string) string {
	if i := strings.LastIndexByte(receipt, ':'); i >= 0 && i+1 < len(receipt) {
		return receipt[i+1:]
	}
	return ""
}

// PurgeQueue removes every message.
func (s *Store) PurgeQueue(ctx context.Context, queue string) error {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, "DELETE FROM "+sc+".dedup"); err != nil && !isMissingBucketSchema(err) {
		return err
	}
	_, err = s.pool.Exec(ctx, "DELETE FROM "+sc+".messages")
	if isMissingBucketSchema(err) {
		return nil
	}
	return err
}

// QueueCounts returns approximate visible and in-flight message counts.
func (s *Store) QueueCounts(ctx context.Context, queue string) (visible, inflight, delayed int64, err error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return 0, 0, 0, err
	}
	err = s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE visible_at <= now() AND expires_at > now()),
		count(*) FILTER (WHERE visible_at > now() AND expires_at > now()),
		count(*) FILTER (WHERE visible_at > now() AND sent_at > now() - interval '1 second')
		FROM `+sc+`.messages`).Scan(&visible, &inflight, &delayed)
	if isMissingBucketSchema(err) {
		return 0, 0, 0, nil
	}
	return visible, inflight, delayed, err
}

// ExpireQueueMessages drops messages past their retention window.
func (s *Store) ExpireQueueMessages(ctx context.Context, queue string, max int) (int64, error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM `+sc+`.messages WHERE ctid IN
		(SELECT ctid FROM `+sc+`.messages WHERE expires_at <= now() LIMIT $1)`, max)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	_, _ = s.pool.Exec(ctx, "DELETE FROM "+sc+".dedup WHERE expires_at <= now()")
	return tag.RowsAffected(), nil
}
