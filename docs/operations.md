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
- `GET /readyz` — checks PostgreSQL connectivity and schema layout v10; returns
  `503` while draining or if the database is unreachable.
- `GET /metrics` — Prometheus metrics:
  - `pgvs3_requests_total{method,code}`
  - `pgvs3_request_duration_seconds_{bucket,sum,count}`
  - `pgvs3_response_bytes_total`
  - `pgvs3_pg_pool_*` and `pgvs3_chunk_pool_*`
  - reaper and maintenance pass counters

## CLI diagnostics and maintenance

```sh
pgvs3 doctor --url "$PGVS3_URL"                   # verify PG 18+, layout v10, and per-bucket reloptions
pgvs3 stat   --url "$PGVS3_URL" [--bucket B] --json # logical/physical bytes, objects, chunks, garbage age
pgvs3 reap   --url "$PGVS3_URL" --all             # drain all eligible garbage outside the grace window
pgvs3 clone  --url "$PGVS3_URL" <src> <dst>       # server-side clone of a whole bucket
pgvs3 drop   --url "$PGVS3_URL" [--force] <bkt>   # drop a bucket schema (`DROP SCHEMA CASCADE`)
```

In normal operation, one gateway per cluster runs background maintenance every
minute (coordinated via `pg_try_advisory_lock`), reconciling restored/dropped
`s3b_<bucket>` schemas, expiring abandoned multipart uploads, and reaping up to
65,536 chunk rows (~508 MiB) per pass of files deleted longer ago than
`--reap-grace` (default `10m`), chaining additional passes immediately when a
pass saturates its row budget.

## Bucket snapshots, cloning, and deletion

Each bucket `<name>` lives in its own PostgreSQL schema `s3b_<name>`:

- **Single-bucket snapshot & restore:**
  ```sh
  pg_dump -Fc -n '"s3b_lake"' "$PGVS3_URL" > lake.dump
  pg_restore --single-transaction -d "$PGVS3_URL" lake.dump
  ```
  On the next maintenance tick (or gateway startup / `pgvs3 reap`),
  `s3p.reconcile_buckets()` automatically registers any restored `s3b_*` schema
  in `s3p.buckets` and advances `s3p.file_ids` past its highest `file_id`.
- **Whole-bucket clone (server-side):**
  ```sh
  pgvs3 clone --url "$PGVS3_URL" lake lake-staging
  # or over HTTP:
  curl -X PUT -H 'x-amz-copy-source: /lake' http://127.0.0.1:8014/lake-staging
  ```
- **Instant bucket deletion & OS disk reclaim:**
  ```sh
  pgvs3 drop --url "$PGVS3_URL" --force lake-staging
  # or over HTTP:
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

Layout v10 is not backwards-compatible with earlier alpha layouts, and gateways
refuse to start against a mismatched version. To migrate across breaking
layouts, copy data into a fresh database via DuckDB/rclone and switch
`PGVS3_URL`.

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
