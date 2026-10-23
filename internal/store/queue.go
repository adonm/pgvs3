package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

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
	ids := make([]string, len(msgs))
	bodies := make([][]byte, len(msgs))
	sizes := make([]int32, len(msgs))
	attrs := make([][]byte, len(msgs))
	delays := make([]int32, len(msgs))
	expiries := make([]time.Time, len(msgs))
	now := time.Now()
	for i, m := range msgs {
		ids[i] = newToken(16)
		bodies[i] = compressBlock(nil, m.Body)
		sizes[i] = int32(len(m.Body))
		attrs[i] = m.Attrs
		delays[i] = int32(m.Delay)
		expiries[i] = now.Add(retention)
	}
	rows, err := s.pool.Query(ctx, `INSERT INTO `+sc+`.messages
		(msg_id, body, body_size, attrs, visible_at, expires_at, sent_at)
		SELECT id, b, sz, a, now() + make_interval(secs => d), e, now()
		FROM unnest($1::text[], $2::bytea[], $3::int4[], $4::bytea[], $5::int4[], $6::timestamptz[])
		  AS t(id, b, sz, a, d, e)
		RETURNING msg_id, sent_at`,
		ids, bodies, sizes, attrs, delays, expiries)
	if isMissingBucketSchema(err) {
		if err := s.EnsureNamespace(ctx, ns.Queue, queue, nil, nil); err != nil {
			return nil, err
		}
		return s.SendMessages(ctx, queue, msgs, retention)
	}
	if err != nil {
		return nil, err
	}
	byID := make(map[string]time.Time, len(ids))
	for rows.Next() {
		var id string
		var sent time.Time
		if err := rows.Scan(&id, &sent); err != nil {
			rows.Close()
			return nil, err
		}
		byID[id] = sent
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]SentMessage, len(msgs))
	for i := range msgs {
		out[i] = SentMessage{ID: ids[i], MD5OfBody: md5Hex(msgs[i].Body), SentAt: byID[ids[i]]}
	}
	return out, nil
}

// ReceiveMessages leases up to max visible messages for visibility seconds.
func (s *Store) ReceiveMessages(ctx context.Context, queue string, max, visibility int, attrs bool) ([]Message, error) {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return nil, err
	}
	token := newToken(12)
	rows, err := s.pool.Query(ctx, `WITH picked AS (
		SELECT id FROM `+sc+`.messages
		WHERE visible_at <= now() AND expires_at > now()
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	), upd AS (
		UPDATE `+sc+`.messages m
		SET receipt = $2 || ':' || m.id,
		    visible_at = now() + make_interval(secs => $3),
		    receive_count = m.receive_count + 1,
		    first_received_at = coalesce(m.first_received_at, now())
		FROM picked WHERE m.id = picked.id
		RETURNING m.id, m.msg_id, m.body, m.body_size, m.attrs, m.receipt, m.receive_count, m.sent_at
	)
	SELECT id, msg_id, body, body_size, attrs, receipt, receive_count, sent_at FROM upd ORDER BY id`,
		max, token, visibility)
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
		)
		if err := rows.Scan(&id, &msgID, &body, &size, &msgAttrs, &receipt, &count, &sent); err != nil {
			return nil, err
		}
		raw, err := decompressBlock(nil, body, int(size))
		if err != nil {
			return nil, err
		}
		m := Message{ID: msgID, Body: raw, Receipt: receipt, ReceiveCount: count, SentAt: sent}
		if attrs {
			m.Attrs = msgAttrs
		}
		out = append(out, m)
	}
	return out, rows.Err()
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
	rows, err := tx.Query(ctx, `SELECT msg_id, body, body_size, attrs, sent_at FROM `+ssc+`.messages
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
	)
	for rows.Next() {
		var id string
		var body []byte
		var size int32
		var a []byte
		var ts time.Time
		if err := rows.Scan(&id, &body, &size, &a, &ts); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
		bodies = append(bodies, body)
		sizes = append(sizes, size)
		attrs = append(attrs, a)
		sent = append(sent, ts)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+dsc+`.messages
		(msg_id, body, body_size, attrs, visible_at, expires_at, sent_at)
		SELECT id, b, sz, a, now(), $6, ts
		FROM unnest($1::text[], $2::bytea[], $3::int4[], $4::bytea[], $5::timestamptz[])
		  AS t(id, b, sz, a, ts)
		ON CONFLICT (msg_id) DO NOTHING`, ids, bodies, sizes, attrs, sent, expires); err != nil {
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

// QueueAttr returns one catalog attribute for a queue.
func (s *Store) QueueAttr(ctx context.Context, queue, name string) (string, bool, error) {
	infos, err := s.Namespaces(ctx, ns.Queue)
	if err != nil {
		return "", false, err
	}
	for _, n := range infos {
		if n.Name != queue {
			continue
		}
		if n.Attrs == nil {
			return "", false, nil
		}
		v, ok := n.Attrs[name]
		if !ok {
			return "", false, nil
		}
		if s, ok := v.(string); ok {
			return s, true, nil
		}
		return "", false, nil
	}
	return "", false, nil
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
	return tag.RowsAffected(), nil
}
