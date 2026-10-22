# Operations

## First run

```sh
just image                    # ko.local:latest
docker compose -f deploy/compose.yaml up
curl -fsS http://127.0.0.1:8014/healthz
curl -fsS http://127.0.0.1:8014/readyz
```

Compose sets `PGVS3_BUCKETS=lake`, so no bucket bootstrap request is needed.

For a real deployment put the URL and keys in Secrets and use TLS outside
loopback; `PGVS3_ALLOW_HTTP` and `PGVS3_DB_ALLOW_PLAINTEXT` are for isolated
development only.

## Plain HTTP and no-auth

`PGVS3_WEB_ADDR=127.0.0.1:8080` serves objects over unauthenticated plain HTTP
at `/{bucket}/{key}`. Directory requests append `PGVS3_WEB_INDEX` (default
`index.html`); `PGVS3_WEB_ERROR` names an optional 404 object. Use it for static
sites or non-S3 clients. It supports `HEAD`, ranges, ETags and conditional GETs.

`PGVS3_NO_AUTH=true` accepts unsigned requests on the S3 listener and does not
require access/secret keys. Put either
listener behind a service mesh or private network; neither mode adds
authentication by itself.

## Probes and metrics

- `GET /healthz` — process liveness, no database access.
- `GET /readyz` — database reachable and layout version matches; returns 503
  while draining or during a database outage.
- `GET /metrics` — Prometheus text:
  - `pgvs3_requests_total{method,code}`
  - `pgvs3_request_duration_seconds_sum` / `_count` and latency buckets
  - `pgvs3_response_bytes_total`
  - `pgvs3_pg_pool_*` and `pgvs3_list_pool_*`
  - `pgvs3_meta_cache_*` and `pgvs3_object_cache_*`
  - reaper, maintenance-run and NOTIFY reconnect counters

Set `PGVS3_ADMIN_ADDR=127.0.0.1:8016` to move probes and metrics to a separate
listener; `/metrics` is then removed from the S3 listener. Bind the peer port (`8015`) and any metrics listener to a private
network; neither uses S3 authentication.

## Diagnose

```sh
pgvs3 doctor --url "$PGVS3_URL"
pgvs3 stat   --url "$PGVS3_URL" --json
pgvs3 reap   --url "$PGVS3_URL" --all
```

`doctor` checks PostgreSQL 18, layout v9 and the chunks table settings without
changing the database. `stat` reports logical/physical bytes, objects, chunks,
garbage and the oldest queued file. `reap --all` runs bounded maintenance
passes until no garbage remains, which is useful before a backup or when all
gateways are scaled to zero.

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

`PGVS3_WRITE_COMMIT=off`/`local` acknowledges writes before the local WAL fsync;
after a crash, recent acknowledged writes may be absent. Keep `on` for anything
you cannot reproduce.

## Maintenance

The gateway runs upload expiry and bounded garbage reaping every minute
(`PGVS3_MAINTENANCE_INTERVAL`). Deleted files remain readable for
`PGVS3_REAP_GRACE` (10 minutes). One reaper runs per cluster via an advisory
lock.

Watch `pgvs3 stat` for garbage age and physical growth. A single PostgreSQL is
the capacity and write-throughput boundary: 32 TiB per database, about 17.4 TB
per object. Add DuckDB readers rather than more writers; the design assumes one
DuckLake writer.

## Shutdown and read-only gateways

On `SIGTERM`, `/readyz` flips to 503 and the gateway keeps serving for
`PGVS3_DRAIN` (default 5 s) before `http.Server.Shutdown` waits for in-flight
requests. Set `PGVS3_READ_ONLY=true` to reject mutating methods, skip DDL and
reaping, and run against a PostgreSQL standby.

`just fault` exercises peer loss, a PostgreSQL restart during a large PUT, and
object churn.

## Failure behavior

- **PostgreSQL restart:** reads retry once on a fresh connection; a write that
  loses its connection publishes nothing.
- **Peer gateway down:** the cross-AZ metadata cache falls back to PostgreSQL;
  writes continue.
- **Lost NOTIFY:** cache TTLs bound staleness (5 s metadata, 30 s buckets).
  Inline peer reads revalidate against `s3p.objects`.
- **Full disk:** writes fail; the gateway does not acknowledge partial objects.
  Reclaim with `VACUUM`/`pg_repack` or restore capacity.

## Kubernetes

The Helm chart runs a 3-pod StatefulSet, one pod per zone, with a static peer
list and a headless service. Keep `peerCache.enabled=false` if you scale with an
HPA or use more than the configured replica count without updating peers.
