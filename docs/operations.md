# Operations

## First run

```sh
just image                    # ko.local:latest
docker compose -f deploy/compose.yaml up
curl -fsS http://127.0.0.1:8014/healthz
curl -fsS http://127.0.0.1:8014/readyz
```

Buckets are created automatically on first write.

## Plain HTTP and authentication

`:8014` (`PGVS3_ADDR`, default `0.0.0.0:8014`) serves the S3 API, plain HTTP
and static websites (`/{bucket}/{key}`, with directory paths resolving to
`index.html`), plus `/healthz`, `/readyz` and `/metrics`.

The listener is unauthenticated. SigV4 headers and presigned query parameters
are accepted and ignored. Run the container inside a private network or behind
a service mesh that enforces access control and TLS.

## Probes and metrics

- `GET /healthz` — process liveness, no database access.
- `GET /readyz` — database reachable and layout version matches; returns 503
  while draining or during a database outage.
- `GET /metrics` — Prometheus text:
  - `pgvs3_requests_total{method,code}`
  - `pgvs3_request_duration_seconds_sum` / `_count` and latency buckets
  - `pgvs3_response_bytes_total`
  - `pgvs3_pg_pool_*` and `pgvs3_chunk_pool_*`
  - reaper and maintenance-run counters

## Diagnose

```sh
pgvs3 doctor --url "$PGVS3_URL"
pgvs3 stat   --url "$PGVS3_URL" --json
pgvs3 reap   --url "$PGVS3_URL" --all
```

`doctor` checks PostgreSQL 18, layout v9 and the chunks table settings without
changing the database. `stat` reports logical/physical bytes, objects, chunks,
garbage and the oldest queued file. `reap --all` runs bounded maintenance
passes until no garbage remains.

## Layout upgrades

Layout v9 is not compatible with v8. A gateway refuses any other version; do
not point it at a mixed database. To move data, copy the catalog and data into
a fresh database with DuckDB, then switch the gateway URL. There is no in-place
migration in the alpha.

## Backup and restore

pgvs3 has no separate durable store: back up PostgreSQL.

- **Logical:** `pg_dump -Fc "$PGVS3_URL" > pgvs3.dump`; restore into an empty
  database with `pg_restore`. Stop writes first for a consistent logical copy.
- **Physical/PITR:** use `pg_basebackup` plus WAL archiving, or your managed
  provider's snapshot/PITR. The chunks and object rows are in the same
  transaction, so a snapshot never names objects whose bytes are missing.
- **Restore validation:** run `pgvs3 doctor`, start the gateway, then read a
  known object and compare its SHA-256/ETag before directing clients at it.

## Maintenance

The gateway runs upload expiry and bounded garbage reaping every minute.
Deleted files remain readable for `PGVS3_REAP_GRACE` (10 minutes). One reaper
runs per cluster via an advisory lock.

Watch `pgvs3 stat` for garbage age and physical growth. A single PostgreSQL is
the capacity and write-throughput boundary: 32 TiB per database, about 17.4 TB
per object. Add DuckDB readers rather than more writers; the design assumes one
DuckLake writer.

## Shutdown and read-only gateways

On `SIGTERM`, `/readyz` flips to 503 and the gateway keeps serving for 5 s
before `http.Server.Shutdown` waits for in-flight requests. Set
`PGVS3_READ_ONLY=true` to reject mutating methods, skip DDL and reaping, and
run against a PostgreSQL standby.

`just fault` exercises peer loss, a PostgreSQL restart during a large PUT, and
object churn.

## Failure behavior

- **PostgreSQL restart:** reads retry once on a fresh connection; a write that
  loses its connection publishes nothing.
- **Gateway replica down:** gateways hold no state; remaining pods continue
  serving immediately from PostgreSQL.
- **Full disk:** writes fail; the gateway does not acknowledge partial objects.
  Reclaim with `VACUUM`/`pg_repack` or restore capacity.

## Kubernetes

The Helm chart runs a stateless multi-AZ `Deployment` and can scale freely with
an HPA (`autoscaling.enabled=true`) as long as PostgreSQL `max_connections` is
sized for the replica ceiling.
