package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
	"github.com/adonm/pgvs3/internal/work"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrJobNotFound   = errors.New("job not found")
	ErrJobConflict   = errors.New("job state conflict")
	ErrLeaseStale    = errors.New("lease token is stale")
	ErrDuplicateJob  = errors.New("job id already exists")
	ErrQueueNotFound = errors.New("queue not found")
)

type Job struct {
	ID          string
	Type        string
	Args        []byte
	Meta        []byte
	Queue       string
	State       JobState
	Attempt     int
	MaxAttempts int
	Priority    int
	TimeoutMS   *int

	CreatedAt   time.Time
	ScheduledAt *time.Time
	EnqueuedAt  time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	CancelledAt *time.Time
	DiscardedAt *time.Time
	ExpiresAt   *time.Time

	WorkerID     string
	LeaseToken   string
	LeaseUntil   *time.Time
	RetryDelayMS *int
	Result       []byte
	Error        []byte
	Tags         []string

	PreviousState JobState
}

type EnqueueInput struct {
	ID           string
	Type         string
	Args         []byte
	Meta         []byte
	Queue        string
	Priority     int
	MaxAttempts  int
	ScheduledAt  time.Time
	TimeoutMS    *int
	RetryInitial int
	RetryMax     int
	RetryCoeff   float64
	Tags         []string
	ExpiresAt    *time.Time
}

type QueueStats struct {
	Available int64
	Active    int64
	Scheduled int64
	Retryable int64
	Completed int64
	Cancelled int64
	Discarded int64
	Total     int64
}

const jobCols = `id, type, args, meta, queue, state, attempt, max_attempts,
	priority, timeout_ms, created_at, scheduled_at, enqueued_at, started_at,
	completed_at, cancelled_at, discarded_at, coalesce(worker_id,''), coalesce(lease_token,''),
	lease_until, retry_delay_ms, result, error, tags, expires_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	var state string
	err := row.Scan(&j.ID, &j.Type, &j.Args, &j.Meta, &j.Queue, &state,
		&j.Attempt, &j.MaxAttempts, &j.Priority, &j.TimeoutMS,
		&j.CreatedAt, &j.ScheduledAt, &j.EnqueuedAt, &j.StartedAt,
		&j.CompletedAt, &j.CancelledAt, &j.DiscardedAt, &j.WorkerID, &j.LeaseToken,
		&j.LeaseUntil, &j.RetryDelayMS, &j.Result, &j.Error, &j.Tags, &j.ExpiresAt)
	if err != nil {
		return Job{}, err
	}
	j.State, err = ParseJobState(state)
	if err != nil {
		return Job{}, fmt.Errorf("scan job %s: %w", j.ID, err)
	}
	for _, stamp := range []*time.Time{j.ScheduledAt, j.StartedAt, j.CompletedAt, j.CancelledAt, j.DiscardedAt, j.LeaseUntil, j.ExpiresAt} {
		if stamp != nil {
			*stamp = stamp.UTC()
		}
	}
	if j.Tags == nil {
		j.Tags = []string{}
	}
	return j, nil
}

func newJobID() string { return work.NewID() }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func (s *Store) EnqueueJobs(ctx context.Context, inputs []EnqueueInput) ([]Job, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	if err := validateEnqueueQueues(inputs); err != nil {
		return nil, err
	}
	var now time.Time
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&now); err != nil {
		return nil, err
	}
	if err := s.prepareEnqueueQueues(ctx, inputs); err != nil {
		return nil, err
	}
	jobs, err := stageEnqueueJobs(inputs, now)
	if err != nil {
		return nil, err
	}

	return withTx(ctx, s, func(tx pgx.Tx) ([]Job, error) {
		return jobs, insertEnqueuedJobs(ctx, tx, inputs, jobs)
	})
}

func validateEnqueueQueues(inputs []EnqueueInput) error {
	for _, in := range inputs {
		if !ns.ValidName(in.Queue) {
			return ns.ErrInvalidName
		}
	}
	return nil
}

func (s *Store) prepareEnqueueQueues(ctx context.Context, inputs []EnqueueInput) error {
	sizes := make(map[string]int64, len(inputs))
	for _, in := range inputs {
		sizes[in.Queue] += int64(len(in.Type) + len(in.Args) + len(in.Meta) + 128)
	}
	for queue, size := range sizes {
		if s.checkQuota(ctx, ns.Queue, queue, size) {
			return ErrQuotaExceeded
		}
		if !s.known(ns.QueueRef(queue)) {
			if err := s.queue(queue).Ensure(ctx, nil); err != nil {
				return err
			}
		}
	}
	return nil
}

func stageEnqueueJobs(inputs []EnqueueInput, now time.Time) ([]Job, error) {
	jobs := make([]Job, len(inputs))
	for i, in := range inputs {
		id := in.ID
		if id == "" {
			id = newJobID()
		}
		args := in.Args
		if len(args) == 0 {
			args = []byte("[]")
		}
		scheduled := in.ScheduledAt
		if scheduled.IsZero() {
			scheduled = now
		}
		transition, err := Transition(TransitionInput{
			Event: JobEventEnqueue, Now: now, ScheduledAt: &scheduled,
		})
		if err != nil {
			return nil, err
		}
		tags := in.Tags
		if tags == nil {
			tags = []string{}
		}
		jobs[i] = Job{
			ID: id, Type: in.Type, Args: args, Meta: in.Meta, Queue: in.Queue,
			State: transition.State, Attempt: transition.Attempt,
			MaxAttempts: in.MaxAttempts, Priority: in.Priority, TimeoutMS: in.TimeoutMS,
			CreatedAt: now, ScheduledAt: &scheduled, EnqueuedAt: now,
			ExpiresAt: in.ExpiresAt, Tags: tags,
		}
	}
	return jobs, nil
}

func insertEnqueuedJobs(ctx context.Context, tx pgx.Tx, inputs []EnqueueInput, jobs []Job) error {
	batch := &pgx.Batch{}
	for i := range inputs {
		sc, err := nsSQLIdent(ns.Queue, jobs[i].Queue)
		if err != nil {
			return err
		}
		j, in := jobs[i], inputs[i]
		batch.Queue(`INSERT INTO `+sc+`.jobs
			(id, type, args, meta, queue, state, attempt, max_attempts, priority,
			 timeout_ms, created_at, scheduled_at, enqueued_at,
			 retry_initial_ms, retry_coefficient, retry_max_ms, expires_at, tags)
			VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6,0,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
			j.ID, j.Type, j.Args, j.Meta, j.Queue, j.State.String(), j.MaxAttempts,
			j.Priority, j.TimeoutMS, j.CreatedAt, *j.ScheduledAt, j.EnqueuedAt,
			initialValue(in.RetryInitial), coefficientValue(in.RetryCoeff),
			maxRetryValue(in.RetryMax), j.ExpiresAt, j.Tags)
		batch.Queue(`INSERT INTO s3p.job_index (id, queue, created_at) VALUES ($1,$2,$3)`,
			j.ID, j.Queue, j.CreatedAt)
	}
	results := tx.SendBatch(ctx, batch)
	for i := range batch.Len() {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			if i%2 == 0 && isUniqueViolation(err) {
				return ErrDuplicateJob
			}
			return err
		}
	}
	return results.Close()
}

func initialValue(v int) int {
	if v <= 0 {
		return 1000
	}
	return v
}

func coefficientValue(v float64) float64 {
	if v < 1 {
		return 2
	}
	return v
}

func maxRetryValue(v int) int {
	if v <= 0 {
		return 60000
	}
	return v
}

func (s *Store) AnyQueueExists(ctx context.Context, queues []string) (bool, error) {
	if len(queues) == 0 {
		return false, nil
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM s3p.namespaces
		WHERE kind='queue' AND name=ANY($1::text[])
	)`, queues).Scan(&exists)
	return exists, err
}

func (s *Store) FetchJobs(ctx context.Context, queues []string, count int, workerID string, visibility time.Duration) ([]Job, error) {
	if count <= 0 {
		count = 1
	}
	if len(queues) == 0 {
		return nil, nil
	}

	existing, err := s.existingQueues(ctx, queues)
	if err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		return nil, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	var out []Job
	visSeconds := visibility.Seconds()
	for _, queue := range queues {
		if len(out) >= count {
			break
		}
		if !existing[queue] {
			continue
		}
		sc, err := nsSQLIdent(ns.Queue, queue)
		if err != nil {
			continue
		}
		jobs, err := fetchQueueTx(ctx, tx, sc, count-len(out), workerID, visSeconds)
		if err != nil {
			return nil, err
		}
		out = append(out, jobs...)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) existingQueues(ctx context.Context, queues []string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT name FROM s3p.namespaces
		WHERE kind = 'queue' AND name = ANY($1::text[])`, queues)
	if err != nil {
		return nil, err
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	existing := make(map[string]bool, len(names))
	for _, name := range names {
		existing[name] = true
	}
	return existing, nil
}

func fetchQueueTx(ctx context.Context, tx pgx.Tx, sc string, count int, workerID string, visSeconds float64) ([]Job, error) {

	states := jobStateStrings([]JobState{JobAvailable, JobScheduled, JobRetryable, JobActive})
	rows, err := tx.Query(ctx, `WITH candidates AS MATERIALIZED (
		SELECT id FROM `+sc+`.jobs
		WHERE state = ANY($1::text[])
		  AND (expires_at IS NULL OR expires_at > now())
		  AND (next_visible_at IS NULL OR next_visible_at <= now())
		  AND NOT (state = $2 AND attempt >= max_attempts)
		ORDER BY priority DESC, next_visible_at, seq
		LIMIT $6 FOR UPDATE SKIP LOCKED
	), leased AS (
		UPDATE `+sc+`.jobs j
		SET state=$2, attempt=CASE WHEN j.state=$2 THEN j.attempt+1 ELSE greatest(j.attempt,1) END,
		    started_at=now(), worker_id=NULLIF($3,''), lease_token=$4,
		    lease_until=now() + make_interval(secs => $5), retry_delay_ms=NULL
		FROM candidates c WHERE j.id=c.id RETURNING j.*
	)
	SELECT `+jobCols+` FROM leased
	ORDER BY priority DESC, coalesce(scheduled_at, enqueued_at, created_at), id`,
		states, JobActive.String(), workerID, newJobID(), visSeconds, count)
	if err != nil {
		if isMissingBucketSchema(err) {
			return nil, ErrQueueNotFound
		}
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Job, error) { return scanJob(row) })
}

func (s *Store) queueForJob(ctx context.Context, id string) (string, error) {
	var queue string
	err := s.pool.QueryRow(ctx, `SELECT queue FROM s3p.job_index WHERE id=$1`, id).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrJobNotFound
	}
	return queue, err
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	queue, err := s.queueForJob(ctx, id)
	if err != nil {
		return Job{}, err
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return Job{}, err
	}
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobCols+` FROM `+sc+`.jobs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
		return Job{}, ErrJobNotFound
	}
	return j, err
}

func (s *Store) CancelJob(ctx context.Context, id string) (Job, error) {
	return s.mutateJob(ctx, id, "", JobEventCancel, nil, false)
}

func (s *Store) AckJob(ctx context.Context, id, leaseToken string, result []byte) (Job, error) {
	return s.mutateJob(ctx, id, leaseToken, JobEventAck, result, false)
}

func (s *Store) NackJob(ctx context.Context, id, leaseToken string, jobErr []byte, retryable bool) (Job, error) {
	return s.mutateJob(ctx, id, leaseToken, JobEventNack, jobErr, retryable)
}

func (s *Store) mutateJob(ctx context.Context, id, leaseToken string, event JobEvent, payload []byte, retryable bool) (Job, error) {
	queue, err := s.queueForJob(ctx, id)
	if err != nil {
		return Job{}, err
	}
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return Job{}, err
	}
	return withTx(ctx, s, func(tx pgx.Tx) (Job, error) {
		var (
			rawState   string
			attempt    int
			maxAttempt int
			initial    int
			maxRetry   int
			coeff      float64
			stored     string
			leaseLive  bool
		)
		err = tx.QueryRow(ctx, `SELECT state, attempt, max_attempts, retry_initial_ms,
		retry_max_ms, retry_coefficient, coalesce(lease_token,''),
		(lease_until IS NOT NULL AND lease_until > now())
		FROM `+sc+`.jobs WHERE id=$1 FOR UPDATE`, id).
			Scan(&rawState, &attempt, &maxAttempt, &initial, &maxRetry, &coeff, &stored, &leaseLive)
		if errors.Is(err, pgx.ErrNoRows) || isMissingBucketSchema(err) {
			return Job{}, ErrJobNotFound
		}
		if err != nil {
			return Job{}, err
		}
		state, err := ParseJobState(rawState)
		if err != nil {
			return Job{}, err
		}
		now := time.Now()
		transition, err := Transition(TransitionInput{
			State: state, Event: event, Now: now,
			Attempt: attempt, MaxAttempts: maxAttempt,
			RetryInitialMS: initial, RetryMaxMS: maxRetry, RetryCoefficient: coeff,
			Retryable: retryable,
		})
		if err != nil {
			return Job{}, ErrJobConflict
		}
		if event != JobEventCancel && (stored != leaseToken || !leaseLive) {
			return Job{}, ErrLeaseStale
		}

		args := []any{id, transition.State.String()}
		var changes string
		switch event {
		case JobEventCancel:
			changes = `state=$2, cancelled_at=now(), completed_at=now(), worker_id=NULL, lease_token=NULL, lease_until=NULL`
		case JobEventAck:
			changes = `state=$2, completed_at=now(), result=NULLIF($3,'')::jsonb, lease_token=NULL, lease_until=NULL`
			args = append(args, string(payload))
		case JobEventNack:
			if transition.State == JobDiscarded {
				changes = `state=$2, discarded_at=now(), completed_at=now(), error=NULLIF($3,'')::jsonb,
				lease_token=NULL, lease_until=NULL, worker_id=NULL`
				args = append(args, string(payload))
			} else {
				changes = `state=$2, attempt=$3, scheduled_at=now() + make_interval(secs => $4::float8 / 1000.0),
				retry_delay_ms=$4, error=NULLIF($5,'')::jsonb, worker_id=NULL, lease_token=NULL, lease_until=NULL`
				args = append(args, transition.Attempt, transition.RetryDelayMS, string(payload))
			}
		default:
			return Job{}, ErrInvalidTransition
		}
		j, err := scanJob(tx.QueryRow(ctx, `UPDATE `+sc+`.jobs SET `+changes+` WHERE id=$1 RETURNING `+jobCols, args...))
		if event == JobEventCancel {
			j.PreviousState = state
		}
		return j, err
	})
}

func (s *Store) HeartbeatJobs(ctx context.Context, workerID string, leases map[string]string, visibility time.Duration) ([]string, error) {
	if len(leases) == 0 {
		return []string{}, nil
	}
	order, byQueue, err := s.resolveLeaseQueues(ctx, leases)
	if err != nil {
		return nil, err
	}

	return withTx(ctx, s, func(tx pgx.Tx) ([]string, error) {
		if err := validateQueuedLeases(ctx, tx, byQueue); err != nil {
			return nil, err
		}
		if err := extendQueuedLeases(ctx, tx, byQueue, workerID, visibility); err != nil {
			return nil, err
		}
		return order, nil
	})
}

type jobQueue struct {
	id    string
	queue string
}

func (s *Store) resolveLeaseQueues(ctx context.Context, leases map[string]string) ([]string, map[string]map[string]string, error) {
	order := slices.Sorted(maps.Keys(leases))

	rows, err := s.pool.Query(ctx,
		`SELECT id, queue FROM s3p.job_index WHERE id = ANY($1::text[])`, order)
	if err != nil {
		return nil, nil, err
	}
	indexed, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (jobQueue, error) {
		var item jobQueue
		err := row.Scan(&item.id, &item.queue)
		return item, err
	})
	if err != nil {
		return nil, nil, err
	}
	if len(indexed) != len(leases) {
		return nil, nil, ErrJobNotFound
	}
	byQueue := make(map[string]map[string]string, len(indexed))
	for _, item := range indexed {
		if byQueue[item.queue] == nil {
			byQueue[item.queue] = make(map[string]string)
		}
		byQueue[item.queue][item.id] = leases[item.id]
	}
	return order, byQueue, nil
}

func validateQueuedLeases(ctx context.Context, tx pgx.Tx, byQueue map[string]map[string]string) error {
	for _, queue := range sortedQueueNames(byQueue) {
		if err := validateQueueLeases(ctx, tx, queue, byQueue[queue]); err != nil {
			return err
		}
	}
	return nil
}

func validateQueueLeases(ctx context.Context, tx pgx.Tx, queue string, tokens map[string]string) error {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return err
	}
	ids := sortedLeaseIDs(tokens)
	rows, err := tx.Query(ctx, `SELECT id, state, coalesce(lease_token,''),
			(lease_until IS NOT NULL AND lease_until > now())
		FROM `+sc+`.jobs WHERE id = ANY($1::text[]) FOR UPDATE`, ids)
	if isMissingBucketSchema(err) {
		return ErrJobNotFound
	}
	if err != nil {
		return err
	}
	now := time.Now()
	seen, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
		var (
			id        string
			rawState  string
			stored    string
			leaseLive bool
		)
		if err := row.Scan(&id, &rawState, &stored, &leaseLive); err != nil {
			return "", err
		}
		state, err := ParseJobState(rawState)
		if err != nil {
			return "", err
		}
		if _, err := Transition(TransitionInput{
			State: state, Event: JobEventHeartbeat, Now: now,
		}); err != nil {
			return "", ErrJobConflict
		}
		if stored != tokens[id] || !leaseLive {
			return "", ErrLeaseStale
		}
		return id, nil
	})
	if err != nil {
		return err
	}
	if len(seen) != len(ids) {
		return ErrJobNotFound
	}
	return nil
}

func extendQueuedLeases(ctx context.Context, tx pgx.Tx, byQueue map[string]map[string]string, workerID string, visibility time.Duration) error {
	for _, queue := range sortedQueueNames(byQueue) {
		if err := extendQueueLeases(ctx, tx, queue, byQueue[queue], workerID, visibility); err != nil {
			return err
		}
	}
	return nil
}

func extendQueueLeases(ctx context.Context, tx pgx.Tx, queue string, tokens map[string]string, workerID string, visibility time.Duration) error {
	sc, err := nsSQLIdent(ns.Queue, queue)
	if err != nil {
		return err
	}
	ids := sortedLeaseIDs(tokens)
	toks := make([]string, len(ids))
	for i, id := range ids {
		toks[i] = tokens[id]
	}
	_, err = tx.Exec(ctx, `UPDATE `+sc+`.jobs j
		SET lease_until=now() + make_interval(secs => $3),
		    worker_id=NULLIF($4,'')
		FROM unnest($1::text[], $2::text[]) AS t(id, token)
		WHERE j.id=t.id AND j.state=$5 AND j.lease_token=t.token`,
		ids, toks, visibility.Seconds(), workerID, JobActive.String())
	return err
}

func sortedLeaseIDs(tokens map[string]string) []string {
	return slices.Sorted(maps.Keys(tokens))
}

func sortedQueueNames(byQueue map[string]map[string]string) []string {
	return slices.Sorted(maps.Keys(byQueue))
}

func (s *Store) ListQueues(ctx context.Context, limit, offset int) ([]NamespaceInfo, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}
	var total int64
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM s3p.namespaces WHERE kind='queue'`).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+namespaceCols+`
		FROM s3p.namespaces WHERE kind='queue' ORDER BY name LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	infos, err := pgx.CollectRows(rows, scanNamespace)
	if err != nil {
		return nil, 0, err
	}
	return infos, total, nil
}

func (q Queue) Stats(ctx context.Context) (QueueStats, error) {
	if q.err != nil {
		return QueueStats{}, q.err
	}
	var st QueueStats
	err := q.s.pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE state=$1),
		count(*) FILTER (WHERE state=$2),
		count(*) FILTER (WHERE state=$3),
		count(*) FILTER (WHERE state=$4),
		count(*) FILTER (WHERE state=$5),
		count(*) FILTER (WHERE state=$6),
		count(*) FILTER (WHERE state=$7),
		count(*) FROM `+q.sc+`.jobs`,
		JobAvailable.String(), JobActive.String(), JobScheduled.String(),
		JobRetryable.String(), JobCompleted.String(), JobCancelled.String(),
		JobDiscarded.String()).
		Scan(&st.Available, &st.Active, &st.Scheduled, &st.Retryable,
			&st.Completed, &st.Cancelled, &st.Discarded, &st.Total)
	if isMissingBucketSchema(err) {
		return QueueStats{}, ErrQueueNotFound
	}
	return st, err
}

func (q Queue) ExpireMessages(ctx context.Context, max int) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	retention := q.s.jobRetention
	if retention < 0 {
		retention = 0
	}
	terminal := jobStateStrings([]JobState{JobCompleted, JobCancelled, JobDiscarded})
	tag, err := q.s.pool.Exec(ctx, `WITH victims AS (
			SELECT ctid FROM `+q.sc+`.jobs
			WHERE (expires_at IS NOT NULL AND expires_at <= now())
			   OR ($2::float8 > 0 AND state = ANY($3::text[]) AND
			       coalesce(completed_at, cancelled_at, discarded_at, created_at)
			         <= now() - make_interval(secs => $2))
			ORDER BY coalesce(completed_at, cancelled_at, discarded_at, created_at, expires_at)
			LIMIT $1)
		, gone AS (
			DELETE FROM `+q.sc+`.jobs WHERE ctid IN (SELECT ctid FROM victims)
			RETURNING id)
		DELETE FROM s3p.job_index WHERE id IN (SELECT id FROM gone)`,
		max, retention.Seconds(), terminal)
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (q Queue) DiscardExpiredLeases(ctx context.Context, max int) (int64, error) {
	if q.err != nil {
		return 0, q.err
	}
	tag, err := q.s.pool.Exec(ctx, `UPDATE `+q.sc+`.jobs j
		SET state=$2, discarded_at=now(), completed_at=now(),
		    worker_id=NULL, lease_token=NULL, lease_until=NULL
		FROM (
			SELECT ctid FROM `+q.sc+`.jobs
			WHERE state='active'
			  AND (lease_until IS NULL OR lease_until <= now())
			  AND attempt >= max_attempts
			ORDER BY lease_until NULLS FIRST
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		) expired
		WHERE j.ctid=expired.ctid
		  AND j.state='active'
		  AND (j.lease_until IS NULL OR j.lease_until <= now())
		  AND j.attempt >= j.max_attempts`,
		max, JobDiscarded.String())
	if isMissingBucketSchema(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (j Job) String() string {
	return fmt.Sprintf("%s(%s) %s/%s attempt=%d", j.ID, j.Type, j.Queue, j.State, j.Attempt)
}

type JobState uint8

const (
	JobStateUnknown JobState = iota
	JobScheduled
	JobAvailable
	JobPending
	JobActive
	JobCompleted
	JobRetryable
	JobCancelled
	JobDiscarded
)

var (
	ErrInvalidJobState   = errors.New("invalid job state")
	ErrInvalidTransition = errors.New("invalid job state transition")
)

var jobStateNames = [...]string{
	JobStateUnknown: "unknown",
	JobScheduled:    "scheduled",
	JobAvailable:    "available",
	JobPending:      "pending",
	JobActive:       "active",
	JobCompleted:    "completed",
	JobRetryable:    "retryable",
	JobCancelled:    "cancelled",
	JobDiscarded:    "discarded",
}

func (s JobState) String() string {
	if int(s) < len(jobStateNames) {
		return jobStateNames[s]
	}
	return "invalid"
}

func (s JobState) Valid() bool {
	return s > JobStateUnknown && int(s) < len(jobStateNames)
}

func ParseJobState(text string) (JobState, error) {
	for state, name := range jobStateNames {
		if state == int(JobStateUnknown) || name != text {
			continue
		}
		return JobState(state), nil
	}
	return JobStateUnknown, fmt.Errorf("%w: %q", ErrInvalidJobState, text)
}

type JobEvent uint8

const (
	JobEventUnknown JobEvent = iota
	JobEventEnqueue
	JobEventScheduleDue
	JobEventLease
	JobEventLeaseExpired
	JobEventAck
	JobEventNack
	JobEventHeartbeat
	JobEventCancel
	JobEventExpire
)

var jobEventNames = [...]string{
	JobEventUnknown:      "unknown",
	JobEventEnqueue:      "enqueue",
	JobEventScheduleDue:  "schedule_due",
	JobEventLease:        "lease",
	JobEventLeaseExpired: "lease_expired",
	JobEventAck:          "ack",
	JobEventNack:         "nack",
	JobEventHeartbeat:    "heartbeat",
	JobEventCancel:       "cancel",
	JobEventExpire:       "expire",
}

func (e JobEvent) String() string {
	if int(e) < len(jobEventNames) {
		return jobEventNames[e]
	}
	return "invalid"
}

type TransitionInput struct {
	State JobState
	Event JobEvent
	Now   time.Time

	ScheduledAt *time.Time
	ExpiresAt   *time.Time

	Attempt     int
	MaxAttempts int

	RetryInitialMS   int
	RetryMaxMS       int
	RetryCoefficient float64
	Retryable        bool
}

type TransitionResult struct {
	State   JobState
	Attempt int

	RetryDelayMS int
}

var jobTransitions = map[JobEvent][]JobState{
	JobEventEnqueue:      {JobStateUnknown},
	JobEventScheduleDue:  {JobScheduled, JobRetryable},
	JobEventLease:        {JobAvailable},
	JobEventLeaseExpired: {JobActive},
	JobEventAck:          {JobActive},
	JobEventNack:         {JobActive},
	JobEventHeartbeat:    {JobActive},
	JobEventCancel:       {JobScheduled, JobAvailable, JobPending, JobActive, JobRetryable},
	JobEventExpire:       {JobScheduled, JobAvailable, JobRetryable},
}

func Transition(in TransitionInput) (TransitionResult, error) {
	sources, ok := jobTransitions[in.Event]
	if !ok {
		return TransitionResult{}, fmt.Errorf("%w: unknown event %d",
			ErrInvalidTransition, in.Event)
	}
	if !slices.Contains(sources, in.State) {
		return TransitionResult{}, fmt.Errorf("%w: %s from %s",
			ErrInvalidTransition, in.Event, in.State)
	}
	out := TransitionResult{State: in.State, Attempt: in.Attempt}
	switch in.Event {
	case JobEventEnqueue:
		out.State = JobAvailable
		if in.ScheduledAt != nil && in.ScheduledAt.After(in.Now) {
			out.State = JobScheduled
		}
	case JobEventScheduleDue:
		if in.ScheduledAt == nil || in.ScheduledAt.After(in.Now) {
			return TransitionResult{}, fmt.Errorf("%w: schedule_due from %s before scheduled_at", ErrInvalidTransition, in.State)
		}
		out.State = JobAvailable
	case JobEventLease:
		out.State = JobActive
		if out.Attempt == 0 {
			out.Attempt = 1
		}
	case JobEventLeaseExpired:
		out.State = JobDiscarded
		if in.Attempt < in.MaxAttempts {
			out.State, out.Attempt = JobAvailable, in.Attempt+1
		}
	case JobEventAck:
		out.State = JobCompleted
	case JobEventNack:
		out.State = JobDiscarded
		if in.Retryable && in.Attempt < in.MaxAttempts {
			out.State, out.Attempt, out.RetryDelayMS = JobRetryable, in.Attempt+1, retryDelayMS(in)
		}
	case JobEventCancel:
		out.State = JobCancelled
	case JobEventExpire:
		if in.ExpiresAt == nil || in.ExpiresAt.After(in.Now) {
			return TransitionResult{}, fmt.Errorf("%w: expire from %s before expires_at", ErrInvalidTransition, in.State)
		}
		out.State = JobDiscarded
	}
	return out, nil
}

func transitionSources(event JobEvent) []JobState {
	return jobTransitions[event]
}

func jobStateStrings(states []JobState) []string {
	out := make([]string, len(states))
	for i, state := range states {
		out[i] = state.String()
	}
	return out
}

func retryDelayMS(in TransitionInput) int {
	delay := work.Backoff{
		Initial: time.Duration(in.RetryInitialMS) * time.Millisecond,
		Factor:  in.RetryCoefficient,
		Max:     time.Duration(in.RetryMaxMS) * time.Millisecond,
	}.Delay(in.Attempt)
	return int(delay / time.Millisecond)
}
