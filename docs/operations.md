# Operations

## First run

```sh
just image                                # builds ko.local:latest
docker compose -f deploy/compose.yaml up  # starts PostgreSQL 18 + pgvs3
curl -fsS http://127.0.0.1:8014/healthz
curl -fsS http://127.0.0.1:8014/readyz
```

Buckets are created automatically on first write.

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
application namespaces that need S3/SQS/RESP, and do not create a public
`Ingress` or `LoadBalancer` Service for `:8014`.

## Probes and Prometheus metrics

- `GET /healthz` — process liveness (does not touch PostgreSQL).
- `GET /readyz` — checks PostgreSQL connectivity and schema layout v11; returns
  `503` while draining or if the database is unreachable.
- `GET /metrics` — Prometheus metrics:
  - `pgvs3_requests_total{method,code}`
  - `pgvs3_request_duration_seconds_{bucket,sum,count}`
  - `pgvs3_response_bytes_total`
  - `pgvs3_pg_pool_*` and `pgvs3_chunk_pool_*`
  - reaper and maintenance pass counters

## CLI diagnostics and maintenance

```sh
pgvs3 doctor --url "$PGVS3_URL"                         # PG 18+, layout v11, per-namespace settings
pgvs3 stat   --url "$PGVS3_URL" [--kind K] [--name N] --json
pgvs3 reap   --url "$PGVS3_URL" --all                   # reconcile schemas + garbage + TTL sweep
pgvs3 clone  --url "$PGVS3_URL" [--kind bucket|cache] <src> <dst>
pgvs3 drop   --url "$PGVS3_URL" [--kind bucket|cache|queue] [--force] <name>
pgvs3 origin --url "$PGVS3_URL" [--bucket B] set|clear|list [origin.yaml]
pgvs3 migrate --url "$PGVS3_URL"                        # layout v10 -> v11
```

In normal operation, one gateway per cluster runs background maintenance every
minute (coordinated via `pg_try_advisory_lock`), reconciling restored or
dropped namespace schemas, expiring abandoned multipart uploads and reaping up to
65,536 chunk rows (~508 MiB) per pass of files deleted longer ago than
`--reap-grace` (default `10m`), chaining additional passes immediately when a
pass saturates its row budget.

## Namespaces: buckets, caches, queues

Every namespace is a PostgreSQL schema, typed by prefix:

| Kind | Schema | Tables | Durability |
| --- | --- | --- | --- |
| bucket | `s3b_<name>` | objects, chunks, uploads, garbage | WAL-logged |
| cache | `s3c_<name>` | objects, chunks, garbage | UNLOGGED (truncated on crash) |
| queue | `s3q_<name>` | messages | WAL-logged |

## Snapshots, cloning, and deletion

Each namespace `<name>` lives in its own schema, so a single `pg_dump -n`
captures exactly one bucket, cache DB or queue:

- **Single-bucket snapshot & restore:**
  ```sh
  pg_dump -Fc -n '"s3b_lake"' "$PGVS3_URL" > lake.dump
  pg_restore --single-transaction -d "$PGVS3_URL" lake.dump
  ```
  Each namespace schema carries its own `meta` table (quota, attributes, and
  origin config), so a restored `s3b_*`, `s3c_*`, or `s3q_*` schema is
  **immediately live** on the next request with no manual CLI step. On first
  access or on the next maintenance tick (`s3p.reconcile_namespaces()`),
  `s3p.namespaces` and `s3p.file_ids` are reconciled automatically ($O(1)$ in
  steady state; only newly restored schemas are inspected).
- **Whole-namespace clone (server-side):**
  ```sh
  pgvs3 clone --url "$PGVS3_URL" lake lake-staging
  pgvs3 clone --kind cache --url "$PGVS3_URL" cache cache-copy
  curl -X PUT -H 'x-amz-copy-source: /lake' http://127.0.0.1:8014/lake-staging
  ```
- **Instant deletion & OS disk reclaim:**
  ```sh
  pgvs3 drop --url "$PGVS3_URL" --force lake-staging
  pgvs3 drop --kind queue --force jobs
  curl -X DELETE 'http://127.0.0.1:8014/lake-staging?force=true'
  ```
  `DROP SCHEMA CASCADE` unlinks the bucket's table files immediately with no
  reaper lag or `VACUUM FULL`.

## Full-database backup and restore

All state lives in PostgreSQL; there is no sidecar store to back up.

- **Physical backup / PITR (recommended):** use `pg_basebackup` + WAL archiving
  or managed cloud snapshots. Because chunks and object metadata commit in the
  same transaction, any point-in-time snapshot is internally consistent.
- **Logical backup:** stop writes, run `pg_dump -Fc "$PGVS3_URL" > pgvs3.dump`,
  and restore into a fresh database with `pg_restore`.
- **Post-restore check:** run `pgvs3 doctor`, start the gateway, and verify a
  known object before shifting traffic.

## Schema upgrades

Gateways refuse to start against a layout other than v11. Back up first, stop
the gateways, and apply pending migrations (including the in-place v10 → v11
upgrade):

```sh
pg_dump -Fc "$PGVS3_URL" > pgvs3-pre-migrate.dump
pgvs3 migrate --url "$PGVS3_URL"       # prints migrated_buckets=N layout=11
pgvs3 doctor  --url "$PGVS3_URL"
```

Migrations are goose SQL files in `internal/store/migrations/`; the applied
history lives in `s3p.goose_db_version`. `00001_baseline.sql` is the frozen v11
baseline, `00002_v10_to_v11.sql` performs the in-place upgrade, and later
migrations are additive. `pgvs3 migrate` applies every pending migration;
running it again after an upgrade is a no-op unless a newer migration exists.

The v10 migration keeps every bucket schema and object in place. It copies the
`s3p.buckets` catalog into `s3p.namespaces`, adds `objects.origin_etag`, applies
the v11 table settings, recreates per-schema helper functions, and renames the
old catalog table to `v10_buckets` instead of dropping it. Schemas restored
outside the catalog are adopted as part of the upgrade.

Layouts older than v10, and future breaking changes without a migration
function, still require copying data into a fresh database via DuckDB/rclone
and switching `PGVS3_URL`.

## Graceful shutdown, standbys, and Kubernetes

- **Graceful drain:** on `SIGTERM`, `/readyz` immediately returns `503` while
  the server continues accepting requests for 5 seconds so load balancers can
  deregister the pod, then waits for in-flight transfers to finish.
- **Read-only standbys:** pass `--read-only` (`PGVS3_READ_ONLY=true`) when
  pointing gateways at a PostgreSQL streaming standby. Mutating S3 requests
  return `405`/`403`, and schema initialization and reaping are skipped. SQS
  allows metadata-only operations (`ListQueues`, `GetQueueUrl`,
  `GetQueueAttributes`) but rejects sending, leasing, deleting, and purging
  because even `ReceiveMessage` writes a lease.
- **Kubernetes / Helm:** `deploy/charts/pgvs3` deploys a stateless multi-AZ
  `Deployment` with optional HPA (`autoscaling.enabled=true`) and `PodDisruptionBudget`.
  Hardened defaults include a nonroot read-only root filesystem with a writable
  `/tmp` emptyDir, no service-account token automount, a 60s termination grace
  period, and a default-deny NetworkPolicy that allows same-namespace clients.
  Add `networkPolicy.ingress.from` peers for other namespaces. Some CNIs also
  evaluate kubelet probes against the policy; add the node CIDR to
  `ingress.from` if probes fail. Setting `networkPolicy.egress.allowAll=false`
  also requires explicit DNS and PostgreSQL egress rules. An optional
  Prometheus Operator `ServiceMonitor` is available. `just helm-kind` installs
  the chart into a disposable kind cluster with the restricted Pod Security
  Standard and checks the pod spec, NetworkPolicy, probes and a same-namespace
  S3 round trip.
  Ensure PostgreSQL `max_connections` exceeds `replicas × PGVS3_POOL_MAX × 2`
  (main pool + chunk pool).
- **Failure handling:**
  - *Gateway pod crash:* remaining pods continue serving immediately (zero local state).
  - *PostgreSQL restart:* reads retry once on a fresh connection; interrupted
    writes roll back cleanly.
  - *Full disk:* writes fail atomically without publishing partial objects.
    Run `pgvs3 reap --all` and `VACUUM` / `pg_repack`, or expand storage. Set
    `--max-db-bytes` (`PGVS3_MAX_DB_BYTES`) to reject writes before
    `pg_database_size` crosses a budget; it is sampled at most once every five
    seconds per gateway.

`just fault` tests gateway loss, mid-upload PostgreSQL restarts, and high
overwrite churn.

## RESP

RESP shares the `:8014` listener and speaks **RESP2 only**. `HELLO 2` is
accepted; `HELLO 3` returns `NOPROTO`. Clients that negotiate RESP3 by default
must opt out, for example `redis.Redis(..., protocol=2)` in recent `redis-py`
releases. RESP batches are bounded by `--resp-concurrency`, and pipelined `GET`
and `SET` bursts are coalesced into `CacheMGet`/`CacheMSet`.

## SQS

SQS shares the `:8014` listener. Point any SDK at it:

```sh
aws --endpoint-url http://127.0.0.1:8014 sqs create-queue --queue-name jobs
aws --endpoint-url http://127.0.0.1:8014 sqs send-message \
  --queue-url http://127.0.0.1:8014/jobs --message-body '{"job":1}'
aws --endpoint-url http://127.0.0.1:8014 sqs receive-message \
  --queue-url http://127.0.0.1:8014/jobs --max-number-of-messages 10
```

Queues store `VisibilityTimeout`, `DelaySeconds`, `MaximumMessageSize`,
`MessageRetentionPeriod`, `ReceiveMessageWaitTimeSeconds` and `RedrivePolicy`
in `s3p.namespaces.attrs`. Receive leases a batch with `FOR UPDATE SKIP
LOCKED`; `DeleteMessage`/`ChangeMessageVisibility` match on the receipt handle,
which is covered by the partial `messages_receipt` index so acks never scan the
queue.

Receipt handles are superseded when a visibility timeout expires and the
message is received again. Deleting with the old handle returns
`ReceiptHandleIsInvalid` instead of silently acknowledging the new delivery;
repeating the winning delete is idempotent.

Set `RedrivePolicy` on the source queue to move poisoned messages to a
dead-letter queue:

```sh
aws --endpoint-url http://127.0.0.1:8014 sqs set-queue-attributes \
  --queue-url http://127.0.0.1:8014/jobs \
  --attributes 'RedrivePolicy={"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:jobs-dlq","maxReceiveCount":"5"}'
```

The target queue must exist. On the receive that would exceed
`maxReceiveCount`, the message is moved into the dead-letter queue in one
transaction and is not returned to the consumer.

## Cache tier in front of another object store

Origin configurations live inside PostgreSQL (`s3p.namespaces.attrs["origin"]`
and `<schema>.meta.attrs`), so gateways remain completely stateless and need no
local config files on disk:

```sh
cat > origin.yaml <<'YAML'
type: s3
config:
  bucket: my-origin
  endpoint: s3.us-east-1.amazonaws.com
  access_key: ...
  secret_key: ...
YAML
# Attach an origin to a single bucket (other buckets remain durable PG buckets):
pgvs3 origin --url "$PGVS3_URL" --bucket edge --cache-namespace origin --cache-ttl 5m set origin.yaml
# Or attach a global origin for all buckets:
pgvs3 origin --url "$PGVS3_URL" --cache-namespace origin --cache-ttl 5m set origin.yaml
pgvs3 origin --url "$PGVS3_URL" list
pgvs3 origin --url "$PGVS3_URL" --bucket edge clear
```

Every running gateway discovers new or updated origin configs from PostgreSQL
within ~1 second (`--origin-config` on `pgvs3 serve` is still accepted as a
process-local fallback).

Every S3 `GET` and `HEAD` is served from `s3c_<namespace>` (`X-Cache: HIT`)
without contacting the origin while within `--cache-ttl`, or when the cached
size and origin mtime still match on revalidation. Cold ranges of objects
larger than 8 MiB are passed straight through to the origin (`GetRange`,
`X-Cache: MISS`) instead of downloading the whole object, while full reads and
ranges on already-cached objects are served from PostgreSQL.

Single `PUT`, `CopyObject`, and `DELETE`/`DeleteObjects` write through to both
the origin and `s3c_<namespace>`. Multipart completion first publishes the
assembled object to the cache with a `pending:` origin sentinel and an
`s3p.pending_uploads` outbox row, then uploads it to the origin. Readers treat
pending cache objects as misses (and serve an older origin object if one
exists), so pending bytes are never visible. A successful
`CompleteMultipartUpload` response means the first origin flush succeeded; if
that flush fails, the request returns an error but the outbox row is retained
and a background worker retries with bounded backoff. The completion consumes
the multipart `uploadId`, so a failed completion cannot be replayed with the
same id; poll `GET`/`HEAD` after the worker has had time to retry. `DELETE` and
`DeleteObjects` remove cache entries and outbox rows as well. Inspect pending
work with `SELECT namespace, key, attempts, next_attempt, last_error FROM
s3p.pending_uploads;`. `--cache-retention` evicts idle entries. Fills and
single-request uploads buffer the body (memory ≤8 MiB, otherwise a temp file)
before opening a PostgreSQL writer so origin latency never holds pool
connections. The production origin type is `s3` (implemented with `minio-go`);
`filesystem` and `inmem` are local/test providers.

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

`--queue-concurrency` (default 16) and `--resp-concurrency` (default 32) bound
in-flight protocol work, and `--fill-concurrency` (default 8) bounds cache
fills. Together they keep a queue backlog or cache storm from occupying all
`--pool-max` connections; raise them only together with the pool and
PostgreSQL `max_connections`.
