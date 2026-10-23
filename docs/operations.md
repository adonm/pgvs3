# Operations

## First run

```sh
just image                                # builds ko.local:latest
docker compose -f deploy/compose.yaml up  # starts PostgreSQL 18 + pgvs3
curl -fsS http://127.0.0.1:8014/healthz
curl -fsS http://127.0.0.1:8014/readyz
```

Buckets are created automatically on first write.

## Network and security model

The single listener (`--addr` / `PGVS3_ADDR`, default `0.0.0.0:8014`) serves:

- S3 API requests
- Plain HTTP and static websites (`/{bucket}/{key}`, resolving trailing slashes
  to `index.html`)
- `/healthz`, `/readyz`, and `/metrics`

**pgvs3 does not authenticate requests.** SigV4 headers and presigned URL
parameters are accepted and ignored so standard S3 SDKs work out of the box.
Always run the gateway inside a private network or behind a service mesh that
enforces authentication, authorization, and TLS.

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
pgvs3 reap   --url "$PGVS3_URL" --all                   # garbage + TTL sweep
pgvs3 clone  --url "$PGVS3_URL" [--kind bucket|cache] <src> <dst>
pgvs3 drop   --url "$PGVS3_URL" [--kind bucket|cache|queue] [--force] <name>
pgvs3 adopt  --url "$PGVS3_URL" [--kind K] [--all] [name...]
pgvs3 migrate --url "$PGVS3_URL"                        # layout v10 -> v11
```

In normal operation, one gateway per cluster runs background maintenance every
minute (coordinated via `pg_try_advisory_lock`), expiring abandoned multipart
uploads and reaping up to
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
  A restored `s3b_*` schema is inert until it is explicitly adopted:
  ```sh
  pgvs3 adopt --url "$PGVS3_URL" --kind bucket lake
  ```
  Adoption inserts the catalog row and advances `s3p.file_ids` past the
  schema's highest `file_id`. `pgvs3 adopt --all --kind bucket` adopts every
  matching schema and prunes catalog rows whose schema no longer exists.
  Requests for an unadopted schema return `404`, and `pgvs3 doctor` reports it
  under `orphan_schemas`.
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
the gateways, and run the in-place migration from layout v10:

```sh
pg_dump -Fc "$PGVS3_URL" > pgvs3-pre-migrate.dump
pgvs3 migrate --url "$PGVS3_URL"       # prints migrated_buckets=N layout=11
pgvs3 doctor  --url "$PGVS3_URL"
```

The migration keeps every bucket schema and object in place. It copies the
`s3p.buckets` catalog into `s3p.namespaces`, adds `objects.origin_etag`, applies
the v11 table settings, recreates per-schema helper functions, and renames the
old catalog table to `v10_buckets` instead of dropping it. Schemas restored
outside the catalog are adopted as part of the upgrade. Running `pgvs3 migrate`
again on a v11 database is a no-op.

Layouts older than v10, and future breaking changes without a migration
function, still require copying data into a fresh database via DuckDB/rclone
and switching `PGVS3_URL`.

## Graceful shutdown, standbys, and Kubernetes

- **Graceful drain:** on `SIGTERM`, `/readyz` immediately returns `503` while
  the server continues accepting requests for 5 seconds so load balancers can
  deregister the pod, then waits for in-flight transfers to finish.
- **Read-only standbys:** pass `--read-only` (`PGVS3_READ_ONLY=true`) when
  pointing gateways at a PostgreSQL streaming standby. Mutating requests return
  `405`/`403`, and schema initialization and reaping are skipped.
- **Kubernetes / Helm:** `deploy/charts/pgvs3` deploys a stateless multi-AZ
  `Deployment` with optional HPA (`autoscaling.enabled=true`) and `PodDisruptionBudget`.
  Ensure PostgreSQL `max_connections` exceeds `replicas × PGVS3_POOL_MAX × 2`
  (main pool + chunk pool).
- **Failure handling:**
  - *Gateway pod crash:* remaining pods continue serving immediately (zero local state).
  - *PostgreSQL restart:* reads retry once on a fresh connection; interrupted
    writes roll back cleanly.
  - *Full disk:* writes fail atomically without publishing partial objects.
    Run `pgvs3 reap --all` and `VACUUM` / `pg_repack`, or expand storage.

`just fault` tests gateway loss, mid-upload PostgreSQL restarts, and high
overwrite churn.

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

```sh
cat > origin.yaml <<'YAML'
type: s3
config:
  bucket: my-origin
  endpoint: s3.us-east-1.amazonaws.com
  access_key: ...
  secret_key: ...
YAML
pgvs3 serve --origin-config origin.yaml --cache-namespace origin --cache-ttl 5m
```

Every S3 `GET` is served from `s3c_<namespace>` when the cached size and origin
mtime still match, and downloaded otherwise. **Ranges of objects larger than
8 MiB are passed straight through to the origin** (`GetRange`) instead of
downloading the whole object, which is what DuckDB footer and row-group reads
need; a full GET still populates the cache. `PUT` writes through to the origin
and refreshes the cache; `DELETE` removes both. `--cache-retention` evicts idle
entries. Fills are bounded by `--fill-concurrency` and the origin body is
buffered (memory ≤8 MiB, otherwise a temp file) before the PostgreSQL writer
opens, so origin latency never consumes pool connections. Supported origin
types are `s3`, `filesystem` and `inmem`.

## Admission control

`--queue-concurrency` (default 16) and `--resp-concurrency` (default 32) bound
in-flight protocol work, and `--fill-concurrency` (default 8) bounds cache
fills. Together they keep a queue backlog or cache storm from occupying all
`--pool-max` connections; raise them only together with the pool and
PostgreSQL `max_connections`.
