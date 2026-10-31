# Reliable jobs, application outboxes and streams

[Guide index](README.md) · Before: [demo application](postgres.md) · Next: [operations](observe-recover.md)

**Goal:** bridge a committed application change to OJS without pretending HTTP
participates in its SQL transaction. Expect retries; make effects idempotent.
If you need enqueue/completion **inside the caller's SQL transaction**, a library
with an explicit transaction API (for example River) is a better fit than
writing pgvs3's internal tables. [Research and differences](sources.md#jobs-and-transaction-boundaries).

## Choose jobs or streams

| Need | Use | Boundary |
| --- | --- | --- |
| Competing workers, leases, priority and retries | OJS jobs | Each HTTP lifecycle operation commits independently. |
| Independent fan-out/replay cursors | OJS streams | Snapshot-delimited batches; effects and batch ACK are separate. |
| Atomic application change + publish intent | Application-owned SQL outbox | Relay later to HTTP; not a distributed transaction. |
| Atomic application SQL effect + HTTP ACK | Not supplied | Commit a deduplication receipt with the effect, then ACK. |

OJS `args` must be a **JSON array**, e.g. `[{"order_id":42}]`, not an object;
`meta`, if present, is an object. A caller UUID identifies a stored job, not a
permanent unique-work key. A duplicate ID in its original queue returns
`409 conflict`; it does not compare payloads or return the original job as
successful insertion. Jobs can
be removed by retention (`24h` default for terminal jobs), expiry or namespace
deletion. After removal, an old ID can be inserted again. The manifest correctly
advertises `unique_jobs:false`.
Keep the ID **and queue** immutable: reusing an ID in a different queue can
surface a global job-index storage error instead of `409`. Do not use a changed
queue as a retry strategy or treat an arbitrary error as successful insertion.

## 1. Commit business state and intent together

In the disposable `guide_app` schema from the PostgreSQL guide:

<!-- verify:outbox-schema -->
```sql
CREATE TABLE guide_app.outbox (
    event_id UUID PRIMARY KEY,
    job JSONB NOT NULL CHECK (job ? 'id' AND job->>'id' = event_id::TEXT),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unsent ON guide_app.outbox (created_at, event_id)
    WHERE published_at IS NULL;
CREATE TABLE guide_app.receipts (
    event_id UUID PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES guide_app.orders(id)
);
```

The UUID below is a fixed **demo event**. Real producers create a stable event ID
once per business transition and persist it, rather than generating one on every
HTTP retry. Keep its envelope immutable.

<!-- verify:outbox-commit -->
```sql
BEGIN;
UPDATE guide_app.orders SET paid = true WHERE id = 42;
INSERT INTO guide_app.outbox (event_id, job) VALUES (
    '018f0000-0000-7000-8000-000000000042',
    '{"id":"018f0000-0000-7000-8000-000000000042","type":"order.fulfill","args":[{"version":1,"order_id":42}],"queue":"guide-jobs"}'
);
COMMIT;
```

**Expected:** paid order + unsent outbox row, but **no OJS job yet**. Replacing
`COMMIT` with `ROLLBACK` leaves neither business change nor intent. The test does
both. Never call HTTP before commit and assume a worker can see uncommitted data.
This recipe uses an existing order; in a real producer assert the transition
actually happened (or create it and its intent in the same transaction).

## 2. Relay, reconcile an ambiguous response, then mark published

The following is a runnable **single-relay demonstration**, using Python's
standard library and `psql`. Save as `relay.py`, set `PGVS3_URL` and
`PGVS3_ENDPOINT` to the disposable writer endpoints, then run `python3 relay.py`.
The test uses the same code. It holds no SQL transaction/lock across HTTP.

<!-- verify:outbox-relay -->
```python
import json
import os
import subprocess
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

def sql(statement):
    return subprocess.check_output([
        os.environ.get("PSQL", "psql"), os.environ["PGVS3_URL"],
        "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement
    ], text=True, timeout=30).strip()

def call(path, job=None):
    request = Request(os.environ["PGVS3_ENDPOINT"] + "/ojs/v1" + path,
                      data=None if job is None else json.dumps(job).encode(),
                      headers={"Content-Type": "application/openjobspec+json"})
    with urlopen(request, timeout=10) as response:
        return json.load(response)

raw = sql("SELECT job FROM guide_app.outbox WHERE published_at IS NULL "
          "ORDER BY created_at, event_id LIMIT 1")
if raw:
    expected = json.loads(raw)
    event_id = str(uuid.UUID(expected["id"]))
    try:
        actual = call("/jobs", expected)["job"]
    except HTTPError as error:
        if error.code != 409:
            raise
        actual = call("/jobs/" + event_id)["job"]
    for field in ("id", "type", "args", "queue"):
        if actual[field] != expected[field]:
            raise RuntimeError("job ID collision or changed envelope: " + field)
    sql("UPDATE guide_app.outbox SET published_at=now() "
        "WHERE event_id='" + event_id + "'::uuid AND published_at IS NULL")
    print(json.dumps({"published": event_id, "observed_state": actual["state"]}))
```

A network error leaves the row unsent. Retry this program with a bounded backoff
and jitter: a prior committed POST becomes `409`, then GET + envelope comparison
reconciles it. Do not treat **every** `409` as success; a different payload/queue
must stop and alert. Read this reconciliation from the **primary**, not a lagging
replica. If GET races retention and returns `404`, leave unsent and retry/reconcile;
do not invent a new UUID. `published_at` means transport accepted, not business
effect completed—monitor cancelled/discarded jobs separately.

This deliberately uses one relay. For parallel relays add short SQL claim leases,
owner tokens, bounded retry scheduling and dead/error inspection in your
**application** outbox; do not hold `FOR UPDATE` locks during network calls or
copy this loop to hundreds of processes. Persist all relevant immutable fields
and include them in reconciliation if extending the envelope (including options
or metadata). Retain/reconcile intents and worker receipts for your replay/restore
horizon, independently of OJS job retention.

## 3. Commit a deduplicated SQL effect before ACK

Fetch on a writable endpoint:

<!-- verify:job-fetch -->
```json
{"queues":["guide-jobs"],"count":1,"worker_id":"guide-worker","visibility_timeout_ms":30000}
```

Send that JSON to `POST /ojs/v1/workers/fetch`. Validate the returned `type`,
argument version, order ID and stable event ID against your application intent.
For this demo, the effect SQL is:

<!-- verify:job-effect -->
```sql
BEGIN;
WITH first_delivery AS (
    INSERT INTO guide_app.receipts (event_id, order_id)
    VALUES ('018f0000-0000-7000-8000-000000000042', 42)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING order_id
)
UPDATE guide_app.orders AS o
SET fulfillment_count = fulfillment_count + 1
FROM first_delivery AS d WHERE o.id = d.order_id;
COMMIT;
```

**Expected:** running it twice leaves `fulfillment_count=1` and one receipt. The
receipt and effect commit together. It protects this demonstrated SQL effect,
not an email/payment/HTTP call outside the transaction; use that downstream
system's idempotency key and reconciliation as well. Validate payload identity
before accepting a receipt collision. Do not expire receipts before jobs can be
replayed, retried or restored from an older backup.

Then `POST /ojs/v1/workers/ack` with the returned **job ID and current
`lease_token`**, e.g. `{"job_id":"…","lease_token":"…","result":{"applied":true}}`.
If the process dies after SQL commit but before ACK, redelivery sees the receipt
and suppresses a second effect. If a lease expires/reclaims, the old token gets
`409 stale_lease`; that is not permission to overwrite the newer delivery.
Heartbeat well before the visibility deadline, or stop work and reconcile when
ownership is lost. `timeout_ms` is job metadata, not a worker process killer.

## Scheduling, retries and throughput

- Due scheduled/retryable jobs and expired **non-exhausted** active leases are
  claimed lazily by fetch; no minute-long maintenance wait is required. Expired
  jobs are excluded from claims; maintenance removes expired/terminal rows and
  marks exhausted expired leases discarded. Queue-state counts can lag those
  transitions until claim/maintenance; they are not a freshness clock.
- Long polling is at most two seconds for an existing empty queue. Fetch accepts
  at most 64 queues and 100 jobs; requests and batches have a 1 MiB body bound.
  Fetch claims queues in the supplied order, not one global cross-queue priority
  order. Avoid a hot first queue starving later queues; use dedicated workers or
  rotate bounded subscriptions.
- Fetch no more than can start/heartbeat safely. Measure enqueue/claim/ACK tails,
  oldest ready work, retries, abandoned leases and autovacuum, not merely jobs/sec.
  `queue-concurrency` is server request admission, **not** worker concurrency or
  an application SQL resource reservation.
- Use bounded atomic batches (`POST /jobs/batch`, at most 100) for independent
  jobs when useful; it is not atomic with application SQL. Retryable nack follows
  the configured backoff/attempt limit; cancellation/discard is not success.
- No workflows, cron, queue pause/resume, permanent uniqueness or job-level DLQ
  management is supplied. Streams have a **separate** retry/DLQ API.

## Streams: a receive ceiling is not pagination

Save/run this Python snippet with `PGVS3_ENDPOINT` set to a writable gateway.
It creates only `guide-events`; processing here means inspecting IDs, not an
external business effect:

<!-- verify:stream-roundtrip -->
```python
import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

def post(path, body):
    request = Request(os.environ["PGVS3_ENDPOINT"] + "/ojs/v1" + path,
                      data=json.dumps(body).encode(),
                      headers={"Content-Type": "application/openjobspec+json"})
    with urlopen(request, timeout=10) as response:
        return json.load(response)

post("/queues/guide-events/consumers", {"consumer": "audit", "from_tick": 0})
post("/queues/guide-events/messages/batch", {"messages": [
    {"type": "order.paid", "payload": {"id": 42}},
    {"type": "order.paid", "payload": {"id": 43}}
]})
try:
    post("/queues/guide-events/receive", {"consumer": "audit", "max": 1})
except HTTPError as error:
    if error.code != 413:
        raise
else:
    raise RuntimeError("expected batch_too_large, not a partial page")
batch = post("/queues/guide-events/receive", {"consumer": "audit", "max": 100})
again = post("/queues/guide-events/receive", {"consumer": "audit", "max": 100})
assert batch["batch_id"] == again["batch_id"] and batch["count"] == 2
post("/streams/ack", {"queue": "guide-events", "batch_id": batch["batch_id"]})
print(json.dumps({"processed": batch["count"], "redelivered_same_batch": True}))
```

Snapshot ticks prevent the late-commit sequence gap; they do not make the
application's SQL + HTTP append atomic. On an ambiguous append, do not assume a
caller message ID deduplicates insertion. Deduplicate downstream by your stable
business event key. Fan-out consumers advance independently; cooperative members
must eventually ACK contiguous batches. A failed member is not an OJS job lease;
plan recovery of its outstanding batch explicitly.

Retention is a **minimum**, not a disk cap: a slow consumer/active batch pins
segments beyond it. Inspect `/queues/{q}/consumers`, `/info` and `/dlq`; do not
unsubscribe a slow consumer merely to reclaim space without accepting lost
replay history. [Full stream model](../queue-stream.md).

## Validate failure windows, then clean up

`just guides` runs rollback + committed intent, simulates a lost enqueue response,
rejects a mismatched duplicate, commits an effect without ACK, reclaims its lease,
rejects the stale token and verifies one SQL effect after redelivery. It also
checks JSON-array validation, retention-bounded ID reuse and the stream roundtrip.
These are bounded examples, not an exactly-once claim. Keep reports and investigate
failures before retrying cleanup; [remove only owned demo data](observe-recover.md#safe-demo-cleanup).
