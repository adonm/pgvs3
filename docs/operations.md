# Operations

## First run

```sh
just image                                # builds ko.local:latest
docker compose -f deploy/compose.yaml up  # starts PostgreSQL 18 + pgvs3
curl -fsS http://127.0.0.1:8014/healthz
curl -fsS http://127.0.0.1:8014/readyz
```

Create a bucket before writing objects:

```sh
curl -fsS -X PUT http://127.0.0.1:8014/lake
```

Bucket creation is explicit. A write to a missing or deleted bucket returns
`NoSuchBucket`; it never recreates the bucket. RESP cache namespaces and OJS
queues are created on first use.

## Deployment topology

The supported production shape is one PostgreSQL primary (writer) plus
streaming standbys, shared by multiple stateless `pgvs3` gateway workers.

| Component | Expectation |
| --- | --- |
| **PostgreSQL** | PostgreSQL 18+; one primary and one or more streaming standbys for production (a single node is development or small-deployment only); a single database. Not multi-primary or sharded. |
| **Writable workers** | Default `pgvs3 serve`; connect to the writer endpoint; run schema initialization/checks and maintenance. |
| **Read-only workers** | `pgvs3 serve --read-only` (`PGVS3_READ_ONLY=true`); connect to a standby endpoint, or to the primary when you deliberately want a read-only fleet. |
| **Load balancing** | Use separate write and read services. Send S3/OJS/RESP mutations only to writable workers; read-only workers reject them. |
| **Backups** | Physical base backup + WAL archiving/PITR for production. See [Backup and restore](#backup-and-restore). |
| **Trust** | Private network or service mesh only. See [Trust model](#trust-model-no-authentication). |

Point every gateway at a PostgreSQL URL:

```sh
# Writable worker: writer endpoint follows the current primary.
PGVS3_URL='postgres://pgvs3@writer.db.example:5432/pgvs3?sslmode=require' \
  pgvs3 serve --addr 0.0.0.0:8014

# Read-only worker: standby endpoint, mutations rejected.
PGVS3_URL='postgres://pgvs3@standby.db.example:5432/pgvs3?sslmode=require' \
  pgvs3 serve --read-only --addr 0.0.0.0:8014
```

`--read-only` is an explicit guard, not automatic primary/standby detection.
It rejects S3, OJS, and RESP mutations with `403`, skips DDL, reaping, cache
fills, and the origin retry worker, and serves reads and
`/readyz`. A writable gateway pointed at a standby still fails writes at
PostgreSQL; a read-only gateway pointed at a primary still rejects mutations.

On failover, the writer endpoint must be repointed to the promoted standby by
your HA mechanism (DNS, service, or proxy). In-flight requests may fail and
clients should retry. Maintenance leadership re-elects automatically when a
writable gateway reaches the new primary. See
[Graceful shutdown, failover, and Kubernetes](#graceful-shutdown-failover-and-kubernetes).

Size PostgreSQL connections for the whole fleet. Each worker opens a main pool
of `--pool-max` and a chunk pool of up to `min(--pool-max, 64)`, so leave
headroom for rollouts and other clients:

```
max_connections > sum(worker PGVS3_POOL_MAX + min(PGVS3_POOL_MAX, 64)) + operational headroom
```

Large cache fills spool above 8 MiB to `--spool-dir` (or the system temp
directory when unset). Point that at local scratch with enough free space;
using a small memory-backed `/tmp` can make 64 MiB write-through and cold
fills fail with `disk quota exceeded`.

`--spool-max-bytes` / `PGVS3_SPOOL_MAX_BYTES` defaults to **8 GiB** of concurrent
disk-spool payload per gateway, shared across fills, foreground writes and outbox
workers. Keep it below available scratch space (the chart provides 10 GiB) and
above the largest object to spool. Admission is nonblocking: exhausted requests
return `503 SlowDown`, while pending uploads remain durable and retry. Unknown
length spools reserve incrementally and abort cleanly rather than deadlocking
with other partially reserved spools. Small in-memory bodies, Go/copy buffers,
logs and filesystem overhead are outside this payload budget.

The chart requests 10 GiB of ephemeral storage and limits it to 12 GiB, including
logs/other scratch, with a 10 GiB disk `emptyDir`. Adjust the request, limit,
`tmpDir.sizeLimit` and `spoolMaxBytes` together. An `emptyDir` limit alone does
not reserve capacity on a node. Alert on `pgvs3_spool_bytes`,
`pgvs3_spool_limit_bytes` and `pgvs3_spool_rejections_total`.

Every connection applies the configured `work_mem` (default `16MB`), a
five-minute statement timeout, a five-second lock timeout, a one-minute
idle-in-transaction timeout, and a finite connection lifetime with jitter.
Long streaming reads use the chunk pool without a statement timeout; upload
transactions explicitly disable it while `COPY` runs. Tune the database limits
with `--db-*` flags only after measuring the actual workload.

## Amazon Aurora PostgreSQL

pgvs3 targets Aurora PostgreSQL 18.3+ (the first Aurora PostgreSQL 18 release)
and uses no extensions or superuser-only features. Check the engine version in
the target Region before planning a deployment; `pgvs3 doctor` prints both
`server_version_num` and `aurora_version` when connected to Aurora. Aurora
Serverless v2 derives `max_connections` from the maximum ACU setting, so apply
the same fleet-wide pool budget shown above.

The session policy is compatible with a direct Aurora writer or reader endpoint.
Reader endpoints should be used with `--read-only`. Runtime connections need
direct endpoints or session-compatible pooling: maintenance and origin ordering
use **session advisory locks**, so moving `SET`s into a parameter group does not
make transaction-pooled PgBouncer compatible. RDS Proxy behavior/pinning needs
separate validation; it is not a PgBouncer session-mode switch. Size proxy and
database connection limits from the sum above. Aurora failover drops active
connections; the pool, readiness check, and client retries handle reconnects.

Aurora's storage layer makes `effective_io_concurrency` mostly advisory. Keep
the conservative 16 MiB `work_mem` default until an actual query plan shows a
spill or a high-memory read; `work_mem` can be consumed by multiple plan nodes
and parallel workers, not as one fixed reservation per connection.

## Trust model: no authentication

The single listener (`--addr` / `PGVS3_ADDR`, default `0.0.0.0:8014`) serves:

- S3 API requests
- Plain HTTP and static websites (`/{bucket}/{key}`, resolving trailing slashes
  to `index.html`)
- `/healthz`, `/readyz`, and `/metrics`

**pgvs3 does not authenticate or authorize requests, and this is a permanent
design decision, not a roadmap item.** There is no SigV4 validation, TLS
termination, audit log, or per-tenant isolation. Header-based SigV4 is accepted
and ignored so standard S3 SDKs work unchanged. Presigned URLs are rejected
with `400 InvalidRequest`: a time-limited, object-scoped link would otherwise
become an unscoped grant on the whole gateway. Presigned `POST` object forms
are not implemented at all (`405`).

The only access control is network placement. Run the gateway on loopback, a
private VPC/VLAN, or behind a service mesh that enforces authentication,
authorization, and TLS. Anyone who can reach `:8014` can read, overwrite, or
delete every namespace, force-drop buckets, purge queues, and flush cache
namespaces; treat the listener like a database port.

| Control | Enforced by |
| --- | --- |
| Client identity and authorization | network / service mesh |
| TLS in transit | service mesh or private network |
| Audit and request attribution | service mesh / PostgreSQL logs |
| Rate limiting and quotas | network / service mesh |
| Bucket drop, queue purge, cache flush | network / service mesh |

For Kubernetes, add a default-deny `NetworkPolicy` that admits only the
application namespaces that need S3/OJS/RESP, and do not create a public
`Ingress` or `LoadBalancer` Service for `:8014`.

## Probes and Prometheus metrics

- `GET /healthz` — process liveness (does not touch PostgreSQL).
- `GET /readyz` — checks PostgreSQL connectivity and schema layout **v25**;
  returns `503` while draining or if the database is unreachable. It does not
  check whether the endpoint is a primary or standby.
- `GET /metrics` — Prometheus metrics:
  - `pgvs3_requests_total{method,code}` and S3 request duration/bytes
  - `pgvs3_ojs_requests_total{method,code}` and OJS request duration/bytes
  - `pgvs3_resp_requests_total{command,status}`
  - `pgvs3_pg_pool_*` / `pgvs3_chunk_pool_*`, including acquire counts,
    acquire duration, empty-acquire wait, connection creation and recycling
  - `pgvs3_pending_uploads_live`, `pgvs3_pending_uploads_dead`,
    `pgvs3_pending_uploads_bytes` and
    `pgvs3_pending_uploads_oldest_seconds`; alert on dead rows, unbounded pending
    bytes, or an oldest age that is not draining
  - reaper and maintenance pass counters

TTL expiry counts include expired metadata-only origin parent records in layout
v25. Range metadata is used only when range caching is enabled, never substitutes
for a cached body, and expires no later than the validation TTL or cache retention.
Read-only workers can use the replicated metadata and existing range blocks
without filling either. Warm ranged reads report `X-Cache: HIT` only when all
requested blocks were already cached.

Read fills renew two-minute database ownership during slow downloads and check
the live owner again inside publication. Waiters remain cancellable and wait up
to half the fetch budget (normally five minutes for a whole object, one minute
for an aligned range) before fallback. Origin downloads do not retain a database
connection between renewal calls. When a committed write-cache version is retired
after external origin changes, its read-fill fence and old fill ownership are
removed atomically; an authoritative write-back delete tombstone remains distinct.

After committing a write-back body/outbox row, the publishing gateway sends a
lossy local wake hint. Database claims remain authoritative; the one-second poll
still discovers work from other gateways or after a crash. Hints neither carry
acknowledged bodies nor replace durable ownership/retry state.

Tracing is opt-in: set `OTEL_EXPORTER_OTLP_ENDPOINT` (or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`) to export OTLP/HTTP spans. HTTP server
spans, PostgreSQL client spans, and origin HTTP propagation are included;
`OTEL_SERVICE_NAME` and `OTEL_TRACES_SAMPLER_ARG` (0..1) are supported. With no
endpoint, or with `OTEL_SDK_DISABLED=true` / `OTEL_TRACES_EXPORTER=none`,
tracing is a no-op. Successful requests expose the trace ID in `X-Trace-Id`.

## CLI diagnostics and maintenance

```sh
pgvs3 doctor --url "$PGVS3_URL"                         # PG 18+, layout v25, drift, scale and reaper checks
pgvs3 stat   --url "$PGVS3_URL" [--kind K] [--name N] --json
pgvs3 reap   --url "$PGVS3_URL" --all                   # reconcile schemas + garbage + TTL sweep
pgvs3 clone  --url "$PGVS3_URL" [--kind bucket|cache] <src> <dst>
pgvs3 drop   --url "$PGVS3_URL" [--kind bucket|cache|queue] [--force] <name>
pgvs3 origin --url "$PGVS3_URL" [--bucket B] set|clear|list [origin.json]
pgvs3 pending --url "$PGVS3_URL" [--namespace N] list|retry
```

In normal operation, one **writable** gateway per cluster runs background
maintenance every minute, coordinated via `pg_try_advisory_lock`. It reconciles
restored or dropped namespace schemas, expires abandoned multipart uploads, and
reaps up to 65,536 chunk rows (~508 MiB) per pass of files deleted longer ago
than `--reap-grace` (default `10m`), chaining additional passes immediately when
a pass saturates its row budget. If the leader exits, another writable gateway
acquires the lock on a later tick.

## Namespaces and supported scale

Every namespace is a PostgreSQL schema, typed by prefix. **At most 100 schemas
per database are supported**, counting buckets, cache namespaces, and queues;
`EnsureNamespace` enforces the limit (S3 returns `TooManyBuckets`, OJS returns
`insufficient_storage`) instead of relying only on `doctor`.
More than that is outside the operating envelope; `pgvs3 doctor` reports
`over_supported_scale` and exits non-zero. This is not a schema-per-tenant
design.

| Kind | Schema | Tables | Durability |
| --- | --- | --- | --- |
| bucket | `s3b_<name>` | objects, chunks, uploads, garbage | WAL-logged |
| cache | `s3c_<name>` | objects, chunks, garbage | WAL-logged shared cache (origin fills and RESP); replicated |
| writeback | `s3w_<name>` | objects, chunks, uploads, garbage | WAL-logged; acknowledged write-back bodies; do not discard |
| queue | `s3q_<name>` | jobs (+ legacy messages/dedup) | WAL-logged |

`s3p.namespaces` is the authoritative catalog. Buckets are created explicitly
with `CreateBucket`/`PUT /{bucket}`; object writes do not create or recreate a
bucket. A write to a deleted bucket returns `NoSuchBucket` even if a worker had
cached the bucket's existence. Cache namespaces and queues keep first-use
creation through their protocols.

Namespace quotas and `--max-db-bytes` are per-worker guardrails, not a
cluster-wide hard quota: each gateway samples PostgreSQL size on its own.
Multipart part writers reserve their declared size before streaming, while
completion still checks the assembled object against the quota
schedule. Roles and tablespaces alone do not enforce per-namespace byte quotas.
A hard shared logical quota would require transactional reservations/accounting;
physical storage/WAL limits still need monitoring and capacity headroom.

## Namespace snapshots, clones, and deletion

Each namespace `<name>` lives in its own schema, so a single `pg_dump -n`
captures exactly one bucket, cache namespace or queue. This is useful for export
and small deployments; use physical backups for whole-cluster recovery.

- **Single-bucket snapshot and restore:**
  ```sh
  pg_dump -Fc -n '"s3b_lake"' "$PGVS3_URL" > lake.dump
  pg_restore --single-transaction -d "$PGVS3_URL" lake.dump
  ```
  Each namespace schema carries its own `meta` table (quota, attributes, and
  origin config), so a restored `s3b_*`, `s3c_*`, or `s3q_*` schema is
  **immediately live** on the next request with no manual CLI step. On first
  access or on the next maintenance tick (`s3p.reconcile_namespaces()`),
  `s3p.namespaces` and `s3p.file_ids` are reconciled automatically (O(1) in
  steady state; only newly restored schemas are inspected).
- **Whole-namespace clone (server-side):**
  ```sh
  pgvs3 clone --url "$PGVS3_URL" lake lake-staging
  pgvs3 clone --kind cache --url "$PGVS3_URL" cache cache-copy
  curl -X PUT -H 'x-amz-copy-source: /lake' http://127.0.0.1:8014/lake-staging
  ```
- **Instant deletion and OS disk reclaim:**
  ```sh
  pgvs3 drop --url "$PGVS3_URL" --force lake-staging
  pgvs3 drop --kind queue --force jobs
  curl -X DELETE 'http://127.0.0.1:8014/lake-staging?force=true'
  ```
  `DROP SCHEMA CASCADE` unlinks the bucket's table files immediately with no
  reaper lag or `VACUUM FULL`. A queue with any retained job rows (including
  completed, cancelled or discarded jobs) counts as non-empty; use `--force`
  to drop it.

## Backup and restore

All pgvs3 coordination and durable namespace state lives in PostgreSQL; there
is no sidecar store to back up. If you use the cache tier, the origin is a
separate system and must be backed up by its own mechanism. Production recovery
should use the PostgreSQL cluster's physical backup and PITR mechanism. The
read-through `s3c_` cache is WAL-logged and shared; reads refill it
from the origin or miss. The `s3w_` write-back namespace is WAL-logged and is
not disposable while pending outbox rows exist: it holds acknowledged bodies
until the origin copy commits.

- **Physical backup / PITR (recommended for a multi-node cluster):** use
  `pg_basebackup` with continuous WAL archiving, or a managed snapshot/PITR
  service. Because chunks, object metadata, queue rows, and the namespace
  catalog commit in PostgreSQL transactions, any point-in-time snapshot is
  internally consistent. A standby is not a backup by itself; protect the WAL
  archive and take base backups on a schedule.
- **Logical backup (export or small deployments):** stop writes, run
  `pg_dump -Fc "$PGVS3_URL" > pgvs3.dump`, and restore into a fresh database
  with `pg_restore`. A logical dump does not replace PITR for a production
  cluster.
- **Post-restore check:** run `pgvs3 doctor`, start a writable gateway, and
  verify a known object before shifting traffic. A restored database must match
  the current layout; there is no in-place migration.

## Schema initialization and layout changes

The schema is a single embedded baseline, `internal/store/schema.sql`. A
non-read-only gateway creates it on first start against an empty database; a
read-only gateway only verifies the installed layout, so initialize against the
primary first. The current layout is **v25**, adding logged parent metadata for
range caching and consolidated PostgreSQL 18 publication SQL. It retains the
consolidated queue/stream schema, three-segment stream rotation, and the logged
`s3c_` shared read cache plus `s3w_` write-back namespace; the OJS claim and terminal-retention change landed in
v14, and the cache-fill publish fence in v13. There is no in-place migration path between layouts: an incompatible schema
change requires a fresh database and re-import, matching the project's
pre-release compatibility policy.

`pgvs3 doctor` is the schema-as-code drift check. It verifies PostgreSQL 18+,
the layout version, required relations and functions, per-namespace table
settings, catalog/schema agreement, the stream functions of every queue (each missing
one is printed by name) and reaper safety. A gateway refuses to start on the
layout, relation and manifest checks; `doctor` also exits non-zero for missing
queue stream functions.

Before replacing a database, take a logical backup as described above. If you
need to preserve data across a layout change, copy it out through S3/rclone or
DuckDB and load it into the new database; do not edit `schema.sql` in place.

## Graceful shutdown, failover, and Kubernetes

- **Graceful drain:** on `SIGTERM`, `/readyz` immediately returns `503` while
  the server continues accepting requests for 5 seconds so load balancers can
  deregister the pod, then waits for in-flight transfers to finish.
  The chart's 60-second termination grace is finite: it does not guarantee
  completion of every transfer permitted by the 30-minute HTTP deadlines.
  Choose the grace period for actual transfer sizes and retain client retries.
- **Read-only workers on standbys:** pass `--read-only`
  (`PGVS3_READ_ONLY=true`). S3 mutations, OJS enqueue/fetch/ack/nack/heartbeat/
  cancel, RESP writes, and stream append/subscribe/receive/ack/nack/DLQ
  mutations return `403`. OJS metadata and job reads
  (`GET /ojs/v1/jobs/{id}`, `/ojs/v1/queues`, `/ojs/v1/queues/{name}/stats`,
  `/ojs/v1/health`, `/ojs/manifest`), stream metadata reads (`info`, `config`,
  consumers, DLQ, batch inspection), S3 reads, and RESP reads of durable
  buckets work. RESP and `/_cache` reads also work from the logged `s3c_`
  cache, subject to replication lag. Schema
  initialization, reaping, cache fills, and the origin retry worker
  are skipped. Durable `s3w_` write-back objects remain readable. See
  [design.md](design.md#read-only-gateways-and-failover)
  for the full capability table.
- **Failover:** connect writable gateways to a writer endpoint that follows the
  current primary. When PostgreSQL promotes a standby, update the endpoint and
  let gateway pools reconnect. Requests on broken connections may fail; clients
  should retry. Atomic publish and conditional writes make retries safe. The
  maintenance advisory lock is session-scoped, so leadership re-elects
  automatically on the new primary. Read-only workers on standbys keep serving
  reads subject to replication lag.
- **Durability on promotion:** logged writes with `synchronous_commit=on` are
  locally WAL-durable. They are not guaranteed to survive promotion of a lagging
  asynchronous standby. Lossless failover requires an appropriate synchronous
  replication and promotion policy (or the managed service's validated durability
  model), with the associated latency/availability tradeoff. Replica visibility
  additionally depends on WAL replay. pgvs3 does not silently change that policy.
- **Kubernetes / Helm:** `deploy/charts/pgvs3` deploys a stateless multi-AZ
  `Deployment` with optional HPA (`autoscaling.enabled=true`) and
  `PodDisruptionBudget`. Hardened defaults include a nonroot read-only root
  filesystem with a writable `/tmp` emptyDir, no service-account token
  automount, a 60s termination grace period, and a default-deny NetworkPolicy
  that allows same-namespace clients. Add `networkPolicy.ingress.from` peers for
  other namespaces. Some CNIs also evaluate kubelet probes against the policy;
  add the node CIDR to `ingress.from` if probes fail. Setting
  `networkPolicy.egress.allowAll=false` also requires explicit DNS and
  PostgreSQL egress rules. An optional Prometheus Operator `ServiceMonitor` is
  available. `just helm-kind` installs the chart into a disposable kind cluster
  with the restricted Pod Security Standard and checks the pod spec,
  NetworkPolicy, probes and a same-namespace S3 round trip. The chart deploys a
  writable fleet by default; run read-only workers from a manifest that sets
  `PGVS3_READ_ONLY=true` and points at a standby endpoint.
- **Failure handling:**
  - *Gateway worker crash:* remaining workers continue serving immediately (no
    authoritative local state). Its maintenance lock, if held, is released.
  - *PostgreSQL restart or failover:* reads retry once on a fresh connection;
    interrupted writes roll back cleanly. Clients retry failed requests.
  - *Maintenance leader crash:* another writable gateway acquires the advisory
    lock on a later tick; no manual promotion.
  - *Full disk:* writes fail atomically without publishing partial objects.
    Run `pgvs3 reap --all` and `VACUUM` / `pg_repack`, or expand storage. Set
    `--max-db-bytes` (`PGVS3_MAX_DB_BYTES`) to reject writes before
    `pg_database_size` crosses a budget; it is sampled at most once every five
    seconds per gateway and is a guardrail, not a shared quota.
  - *Wrong endpoint role:* writable workers pointed at a standby fail writes;
    read-only workers pointed at a primary still reject mutations. Fix routing;
    do not rely on `/readyz` to detect the role.

`just fault` tests gateway loss, mid-upload PostgreSQL restarts, and high
overwrite churn.

## RESP

RESP shares the `:8014` listener and speaks **RESP2 only**. `HELLO 2` is
accepted; `HELLO 3` returns `NOPROTO`. Clients that negotiate RESP3 by default
must opt out, for example `redis.Redis(..., protocol=2)` in recent `redis-py`
releases. RESP batches are bounded by `--resp-concurrency`, and pipelined `GET`
and `SET` bursts are coalesced into `Cache.MGet`/`Cache.MSet`.

## OJS (OpenJobSpec v1 core)

OJS shares the `:8014` listener under `/ojs/`. This implementation covers the
core HTTP lifecycle only: PUSH (single and atomic batch), FETCH, ACK, FAIL
(nack), BEAT (heartbeat), CANCEL, INFO, queue listing/stats, health and the
manifest. Workflows, cron, SSE/WebSocket events, schema registry, dead-letter
management, pause/resume and full OJS conformance are not implemented.

Use `application/openjobspec+json` (or `application/json`). Every response
carries `OJS-Version: 1.0` and `X-Request-Id`.

```sh
API=http://127.0.0.1:8014/ojs/v1
curl -fsS -X POST "$API/jobs" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"type":"email.send","args":["user@example.com"],"options":{"queue":"jobs","priority":5}}'

curl -fsS -X POST "$API/workers/fetch" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"queues":["jobs"],"count":1,"worker_id":"worker-1","visibility_timeout_ms":30000}'
```

The fetch response includes `lease_token` on each job. Pass it back on
completion or failure:

```sh
curl -fsS -X POST "$API/workers/ack" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"job_id":"<uuid>","lease_token":"<token>","result":{"delivered":true}}'

curl -fsS -X POST "$API/workers/nack" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"job_id":"<uuid>","lease_token":"<token>","error":{"retryable":true,"message":"SMTP timeout"}}'

curl -fsS -X POST "$API/workers/heartbeat" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"worker_id":"worker-1","active_jobs":["<uuid>"],"lease_tokens":{"<uuid>":"<token>"},"visibility_timeout_ms":30000}'
```

`lease_token` is an implementation extension required by this binding (the OJS
core schema has no token on ack/nack). It prevents a worker whose lease was
reclaimed from completing or failing a newer delivery; stale tokens return
`409` with `{"error":{"code":"stale_lease"}}`. Ack/nack also return `409` for a
job in the wrong state and `404` when the id is unknown.

Jobs use the OJS envelope and states: `scheduled`, `available`, `active`,
`retryable`, `completed`, `cancelled`, `discarded` (plus `pending` in the
schema CHECK constraint for forward compatibility). Enqueue creates a UUIDv7 if
the client omits `id`; `scheduled_at`/`run_at` in the future creates a
`scheduled` job. Fetch leases `available`, due `scheduled`/`retryable`, and
expired-lease rows in one `FOR UPDATE SKIP LOCKED` claim ordered by `priority`
descending then claim time. Expiry, due promotion and exhausted leases are
handled by maintenance rather than by every fetch/poll. Completed, cancelled
and discarded jobs are removed by maintenance after `--job-retention` (default
`24h`; `0` keeps them forever).
`nack` with `retryable:false` discards immediately; retryable jobs advance
`attempt` and `${initial_interval_ms} × coefficient^(attempt-1)` (capped by
`max_interval_ms`) until `max_attempts`, then become `discarded`. The fetch
endpoint long-polls up to two seconds when the queue exists but is empty.

Queues are `s3q_<name>` namespaces. The current schema creates the OJS `jobs`
table, the shared `meta`/config, and the stream log (`stream_events` with three
rotating partitions, `stream_segments`, `stream_ticks`, subscriptions, batches,
retry and dead-letter tables) plus `s3p.job_index` for id routing. Legacy `messages`/`dedup` rows are **not read**;
drop the queue namespace with `pgvs3 drop --kind queue --force <name>` to
reclaim them.

### Streams

The same queue namespace exposes a PgQue-shaped stream:

```sh
# Subscribe at the oldest retained tick, append, receive, then ack.
curl -X POST "$API/queues/events/consumers" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"consumer":"audit","from_tick":0}'
curl -X POST "$API/queues/events/messages" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"type":"order.created","payload":{"id":42}}'
curl -X POST "$API/queues/events/receive" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"consumer":"audit","max":100,"wait_ms":2000}'
curl -X POST "$API/streams/ack" \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"queue":"events","batch_id":1}'
```

Receive returns a snapshot-delimited batch; `max` is a ceiling, not a page
size. `nack` retries one message or dead-letters it after
`stream_config.max_retries`. `GET /queues/{name}/info`, `/consumers`,
`/dlq`, `/config` and `/batches/{id}` expose the stream state. `retention`
defaults to seven days; maintenance rotates three event partitions after
`retention / 2` and truncates a sealed partition only when no cursor or batch
still references it. Stream retention is therefore consumer-pinned, table
granular, and zero-bloat: the configured value is a minimum, not a hard upper
bound. Stream mutations are rejected on `--read-only` gateways; metadata reads
remain available. See [queue-stream.md](queue-stream.md) for the full endpoint
table and design.

### Constrained origin mode

For local cache benchmarks without a separate proxy, a self-hosted origin can
simulate network delay and request-rate limits:

```sh
pgvs3 serve --addr 127.0.0.1:18030 \
  --simulate-latency 20ms --simulate-rps 100 \
  --simulate-bandwidth 52428800
```

`--simulate-latency` adds one-way delay on the request and response sides
(`20ms` is about a 40ms RTT). `--simulate-rps` spaces S3 requests globally at
no more than the configured rate. `--simulate-bandwidth` caps each request and
response body at the configured bytes/sec; `52428800` is 50 MiB/s. Health,
readiness and metrics probes bypass all three limits. These flags are for
development and benchmarks, not production admission control; the normal S3
limit is `--s3-concurrency`.

## Cache tier in front of another object store

Origin configurations live inside PostgreSQL (`s3p.namespaces.attrs["origin"]`
and `<schema>.meta.attrs`), so gateways remain stateless and need no local
config files on disk:

```sh
cat > origin.json <<'JSON'
{
  "type": "s3",
  "config": {
    "bucket": "my-origin",
    "endpoint": "s3.us-east-1.amazonaws.com",
    "access_key": "...",
    "secret_key": "..."
  }
}
JSON
# Attach an origin to a single bucket (other buckets remain durable PG buckets):
pgvs3 origin --url "$PGVS3_URL" --bucket edge --cache-namespace origin --cache-ttl 5m set origin.json
# Or attach a global origin for all buckets:
pgvs3 origin --url "$PGVS3_URL" --cache-namespace origin --cache-ttl 5m set origin.json
pgvs3 origin --url "$PGVS3_URL" list
pgvs3 origin --url "$PGVS3_URL" --bucket edge clear
```

Origin configuration is JSON only.

Every running gateway discovers new or updated origin configs from PostgreSQL
within ~1 second (`--origin-config` on `pgvs3 serve` is still accepted as a
process-local fallback).

### Cache modes

| Mode | Behavior |
| --- | --- |
| **Read-through** | A whole-object `GET` miss fetches body and metadata from the origin in one request, spools outside a database transaction, and publishes into the WAL-logged `s3c_<namespace>`; `HEAD` and ranged misses use a metadata probe first. A hit within `--cache-ttl` is served without contacting the origin (`X-Cache: HIT`). Read-only origins pass whole-object misses through from that same single GET. |
| **Cold-fill coalescing** | One writable gateway owns a cold key through a short `s3p.cache_fill_leases` row. Other gateways poll the replicated `s3c_` row and pass through to the origin only if the lease waiter times out. |
| **TTL revalidation** | After `--cache-ttl`, compare the cached size and origin version/mtime. If they still match, refresh the cache entry; otherwise refill from the origin. Cold whole-object fills are single-request; post-TTL revalidation still uses a metadata probe. |
| **Write-through / write-back** | Small or conditional writes are synchronous write-through: checksums are verified and conditions are enforced before success, and the origin is mutated before the response. Large unconditional objects (above 8 MiB) use ack-first write-back: the body is committed to the WAL-logged `s3w_<namespace>` and `s3p.pending_uploads` atomically, the object is immediately readable, and the origin upload runs through the bounded outbox worker. `--cache-write-back=false` restores synchronous behavior. |
| **Multipart** | Completion publishes the assembled write-back object plus an outbox row atomically. With write-back enabled, an unconditional completion returns after that transaction and lets the outbox worker upload the object; conditional completions remain synchronous. A pending body is readable, and the origin upload uses the captured base version when one exists. |
| **Range cache** | Cold ranges of objects larger than 8 MiB pass through to the origin unless `--range-cache-chunk` is set, in which case aligned chunks are cached in `s3c_<namespace>` and reused. |
| **Listings** | `ListObjectsV2` merges the origin walk with pending write-back objects from PostgreSQL, one bounded page in memory. There is no listing index, TTL-stale refresh, or per-write listing upsert. |

### Cross-worker safety

- **Stale fill fencing:** read-through fills carry the expected origin version
  and publish conditionally. A fill that started before a newer write-through
  write cannot overwrite the newer cache row.
- **Delete tombstones:** write-through deletes replace both the read-cache and
  write-back rows with thirty-minute tombstones. A fill that started before the delete fails its
  version check instead of re-inserting deleted bytes.
- **Conditional writes:** `PUT` and `COPY` honor `If-Match` and
  `If-None-Match: *`; unsupported conditional combinations are rejected rather
  than ignored. Conditional support is a hard requirement for a writable
  production origin.
- **Leased outbox:** a bounded pool of gateway workers claims due
  `s3p.pending_uploads` rows with five-minute database leases and flushes them
  in parallel. Each origin upload holds a PostgreSQL advisory lock for its
  write-back key, so cross-gateway copies and ordinary writes cannot reorder.
  An expired lease is reclaimable after a crash. Transient failures retry
  indefinitely with capped backoff; only non-retryable failures (unsupported
  origin or missing durable body) are marked `dead`. Use
  `pgvs3 pending list` and `pgvs3 pending retry` to inspect and repair them.
- **Token guards:** a pending completion is published to the cache only if its
  token still matches. A stale retry cannot overwrite a newer PUT/DELETE or
  resurrect a deleted object. If the origin precondition fails, the pending
  object and row are abandoned rather than served.
- **Read-only pass-through:** read-only gateways can serve durable `s3c_`
  cache and `s3w_` write-back objects, never fill a cache row, and never start
  the retry worker.

### Write-back semantics and limitations

Write-back intentionally prioritizes latency: a successful large PUT can be
visible from the cache before the origin copy exists. The body is committed to
the WAL-logged `s3w_` namespace and the outbox row in the same transaction, and
the object has no expiry while the origin copy is pending, so a slow or failed
origin does not lose acknowledged bytes. Once the origin upload commits, normal
`--cache-retention` applies. The public ETag is the local content ETag and stays
stable across the pending/committed transition; the origin ETag is stored for
conditional origin operations. Per-key database locks serialize writes,
multipart completion, deletes and outbox flushes across gateways.

### Spooling and large fills

Bodies up to 8 MiB stay in memory. Larger fills and write-through uploads spool
to `--spool-dir` (the system temp directory by default), and the file is
removed after the origin/cache operation. Point this at local scratch with
enough free space; a memory-backed `/tmp` can exhaust quickly with concurrent
64 MiB fills. The spool is never visible to readers as a cache object.

### Limits and failure behavior

- **No full external invalidation.** pgvs3 does not receive origin events. An
  object changed directly at the origin may be served stale until TTL
  revalidation or `--cache-retention` eviction. The origin remains the source of
  truth; flushing a read-through cache namespace loses only acceleration. The
  flush takes table locks, re-checks the outbox, and refuses to run while
  `s3p.pending_uploads` has rows for the namespace, so concurrent writes either
  complete before the flush or land after it.
- **Delete tombstones are bounded.** A write-through delete leaves a
  thirty-minute tombstone so an in-flight fill cannot resurrect the object. An
  object recreated directly at the origin may remain hidden through the cache
  until the tombstone expires; recreating it through pgvs3 replaces the
  tombstone immediately.
- **Completion semantics.** With write-back enabled, a successful large
  unconditional `CompleteMultipartUpload` means the cache object and outbox row
  were committed; the origin upload continues asynchronously. Small and
  conditional completions still require the first origin flush to succeed. A
  failed synchronous flush consumes the multipart `uploadId`, so it cannot be
  replayed with the same id; poll `GET`/`HEAD` after the worker retries.
- **Retention.** `--cache-retention` evicts committed idle entries. Pending
  write-back objects have no expiry until the origin upload commits. `DELETE`
  and `DeleteObjects` write tombstones to both read-cache and write-back
  namespaces and remove older outbox rows as well.
- **Dead letters.** Non-retryable failures (or a missing durable body) mark a
  row `dead` and stop claims; the body remains readable. Inspect
  `pgvs3 pending list`, repair with `pgvs3 pending retry`, and alert on
  `pgvs3_pending_uploads_dead > 0`.
- **Spooling.** Fills and single-request uploads buffer the body (memory ≤8 MiB,
  otherwise a temp file) before opening a PostgreSQL writer, so origin latency
  never holds pool connections.

Inspect pending work with:

```sql
SELECT namespace, key, attempts, next_attempt, lease_owner, lease_until,
       last_error
FROM s3p.pending_uploads;
```

The production origin types are `s3` (SigV4 and object operations via the
Apache-2.0 `internal/s3client` fork of `kelindar/s3`) and read-only
`http`/`https`; `filesystem` and `inmem` are local/test providers. They share one
on-disk implementation, and `inmem` uses a private temporary directory under
`TMPDIR` that is removed on clean shutdown, so it needs a writable temporary
directory. Both keep in-flight writes in a `.pgvs3-tmp` directory at their root.
User metadata is process-local for both, and keys that cannot be
stored as paths are rejected as described in the README. HTTP origins are
read-only by default and map the cache bucket path onto a base URL:

```json
{
  "type": "http",
  "config": {
    "base_url": "https://data.source.coop",
    "strip_bucket": true,
    "read_only": true,
    "headers": { "User-Agent": "pgvs3" }
  }
}
```

Use `pgvs3 origin --range-cache-chunk 1048576 set origin.json` to persist range
caching for a stored origin, or pass `--range-cache-chunk 1048576` to
`pgvs3 serve` for a process-local `--origin-config`. The HTTP origin returns an
explicit error for `ListObjectsV2`; use an S3 origin when listing is required.
The integration gate runs the internal S3 client against a second pgvs3
gateway acting as the S3 origin. Set `PGVS3_TEST_S3_ENDPOINT`,
`PGVS3_TEST_S3_ACCESS_KEY`, and `PGVS3_TEST_S3_SECRET_KEY` to test an external
S3-compatible origin instead.

## Profiling

`--pprof-addr` starts an optional Go pprof listener without exposing it on the
S3 port. It is off by default; bind it to `127.0.0.1` on a host you control.

```sh
pgvs3 serve --url "$PGVS3_URL" --pprof-addr 127.0.0.1:6060

# CPU flame graph for the running gateway.
go tool pprof -seconds 30 -http=127.0.0.1:6061 \
  http://127.0.0.1:6060/debug/pprof/profile

# Capture a profile for offline pprof/FlameGraph use.
curl -fsS 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30' \
  -o cpu.pb.gz
go tool pprof -top ./pgvs3 cpu.pb.gz
```

For database-side query and wait analysis, run
[pgbot](https://pgbot.dev/) against the same database with a `pg_monitor` role.

## Admission control

`--s3-concurrency` (default 32), `--queue-concurrency` (default 16) and
`--resp-concurrency` (default 32) bound in-flight protocol work.
`--fill-concurrency` (default 8) is a process-wide budget for cache fills and
pending-upload workers. S3 requests over the limit get `503 SlowDown` with `Retry-After`.
The shared HTTP server has a 30-minute read/write deadline by default; OJS uses
tighter per-request deadlines for its small bodies and responses. Large S3
transfers can tune `--http-read-timeout` / `--http-write-timeout`. `--max-conns`
now holds its slot until the connection closes. Together these limits keep an
OJS backlog or cache storm from occupying all `--pool-max` connections; raise
them only together with the pool and PostgreSQL `max_connections`.
