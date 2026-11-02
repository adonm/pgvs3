# CLI and metrics reference

<!-- Generated from the binary's flag sets by `just reference`; `go test ./cmd/pgvs3` fails on drift.
     The metrics block is checked against a live /metrics scrape by `just contract`. -->

Every `serve` flag can also be set with `PGVS3_<FLAG>` (upper case, `-` becomes `_`); an explicit
flag wins. Maintenance commands default `--url` from `PGVS3_URL`; `reap` also reads
`PGVS3_REAP_GRACE` and `PGVS3_JOB_RETENTION`, `health` reads `PGVS3_HEALTH_URL` and `doctor`
reads `PGVS3_POOL_MAX`.

## Commands

| Command | Purpose |
| --- | --- |
| [`serve`](#pgvs3-serve) | Run the S3/OJS/HTTP/RESP gateway (default when no command is given). |
| [`health`](#pgvs3-health) | Exit 0 when a gateway's readiness URL returns 200. |
| [`reap`](#pgvs3-reap) | Run one bounded maintenance pass (or all passes) without a gateway. |
| [`doctor`](#pgvs3-doctor) | Check the database, its settings and fleet connection capacity; print fixes. |
| [`stat`](#pgvs3-stat) | Print namespace, object, chunk and storage-overhead counts. |
| [`clone`](#pgvs3-clone) | Copy a bucket or cache namespace inside PostgreSQL. |
| [`drop`](#pgvs3-drop) | Delete a namespace and its schema. |
| [`adopt`](#pgvs3-adopt) | Register existing s3b_/s3c_/s3w_/s3q_ schemas in the catalog. |
| [`origin`](#pgvs3-origin) | Set, clear or list the origin a bucket (or every bucket) caches. |
| [`pending`](#pgvs3-pending) | List or retry write-back uploads that have not reached the origin. |
| [`version`](#pgvs3-version) | Print the binary version. |

## `pgvs3 serve`

Run the S3/OJS/HTTP/RESP gateway (default when no command is given).

| Flag | Environment | Default | Description |
| --- | --- | --- | --- |
| `--addr` | `PGVS3_ADDR` | `0.0.0.0:8014` | listen address for S3/HTTP/RESP2/OJS, /healthz, /readyz and /metrics |
| `--cache-namespace` | `PGVS3_CACHE_NAMESPACE` | `origin` | cache namespace for origin objects |
| `--cache-retention` | `PGVS3_CACHE_RETENTION` | `24h0m0s` | evict cached objects after this idle period |
| `--cache-ttl` | `PGVS3_CACHE_TTL` | `1m0s` | origin revalidation interval (0 always revalidates) |
| `--cache-write-back` | `PGVS3_CACHE_WRITE_BACK` | `true` | acknowledge large unconditional cache writes after the logged write-back commit; the origin upload follows (conditional writes stay synchronous) |
| `--db-idle-tx-timeout` | `PGVS3_DB_IDLE_TX_TIMEOUT` | `1m0s` | PostgreSQL idle-in-transaction timeout (0 disables) |
| `--db-lock-timeout` | `PGVS3_DB_LOCK_TIMEOUT` | `5s` | PostgreSQL lock timeout (0 disables) |
| `--db-max-conn-lifetime` | `PGVS3_DB_MAX_CONN_LIFETIME` | `1h0m0s` | PostgreSQL connection lifetime (0 disables) |
| `--db-max-conn-lifetime-jitter` | `PGVS3_DB_MAX_CONN_LIFETIME_JITTER` | `5m0s` | Randomized PostgreSQL lifetime jitter |
| `--db-statement-timeout` | `PGVS3_DB_STATEMENT_TIMEOUT` | `5m0s` | PostgreSQL statement timeout (0 disables) |
| `--db-work-mem` | `PGVS3_DB_WORK_MEM` | `16MB` | session work_mem per sort/hash operation (not a per-connection ceiling) |
| `--fill-concurrency` | `PGVS3_FILL_CONCURRENCY` | `8` | process-wide concurrent origin fills and pending-upload workers |
| `--http-read-timeout` | `PGVS3_HTTP_READ_TIMEOUT` | `30m0s` | HTTP read deadline including request bodies (0 disables) |
| `--http-write-timeout` | `PGVS3_HTTP_WRITE_TIMEOUT` | `30m0s` | HTTP response write deadline (0 disables) |
| `--job-retention` | `PGVS3_JOB_RETENTION` | `24h0m0s` | keep completed/cancelled/discarded OJS jobs this long (0 = forever) |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | debug, info, warn or error |
| `--max-conns` | `PGVS3_MAX_CONNS` | `4096` | concurrent client connections before shedding load |
| `--max-db-bytes` | `PGVS3_MAX_DB_BYTES` | `0` | reject writes when pg_database_size exceeds this (0 = unlimited) |
| `--origin-config` | `PGVS3_ORIGIN_CONFIG` | — | process-local fallback origin JSON; prefer pgvs3 origin set, which every gateway discovers |
| `--pool-max` | `PGVS3_POOL_MAX` | `64` | main-pool PostgreSQL connections; the chunk pool adds up to min(pool-max, 64), so check the fleet with pgvs3 doctor --gateways N |
| `--pprof-addr` | `PGVS3_PPROF_ADDR` | — | optional loopback address for net/http/pprof, e.g. 127.0.0.1:6060 |
| `--queue-concurrency` | `PGVS3_QUEUE_CONCURRENCY` | `16` | concurrent OJS queue requests |
| `--range-cache-chunk` | `PGVS3_RANGE_CACHE_CHUNK` | `0` | cache aligned ranges of large objects at this byte size (0 disables) |
| `--read-only` | `PGVS3_READ_ONLY` | `false` | reject writes and skip DDL, reaping, fills and origin retries (for standbys); pending write-back objects stay readable |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m0s` | how long overwritten or deleted files stay readable before garbage collection |
| `--resp-concurrency` | `PGVS3_RESP_CONCURRENCY` | `32` | concurrent RESP batches |
| `--s3-concurrency` | `PGVS3_S3_CONCURRENCY` | `32` | active S3 requests; excess waits in a queue of twice this, then gets 503 SlowDown |
| `--s3-queue-wait` | `PGVS3_S3_QUEUE_WAIT` | `1s` | maximum S3 admission wait (0 rejects excess immediately) |
| `--simulate-bandwidth` | `PGVS3_SIMULATE_BANDWIDTH` | `0` | benchmark-only per-request bandwidth in bytes/sec (0 disables) |
| `--simulate-latency` | `PGVS3_SIMULATE_LATENCY` | `0s` | benchmark-only one-way delay added to S3 requests and responses (20ms is about a 40ms RTT) |
| `--simulate-rps` | `PGVS3_SIMULATE_RPS` | `0` | benchmark-only maximum S3 requests per second; probes and metrics are exempt (0 disables) |
| `--spool-dir` | `PGVS3_SPOOL_DIR` | — | scratch directory for cache fills over 8 MiB; use local disk, not a small tmpfs (empty uses the system temp dir) |
| `--spool-max-bytes` | `PGVS3_SPOOL_MAX_BYTES` | `8589934592` | process-wide spool byte budget for fills, foreground writes and pending uploads; busy admission returns 503 |
| `--url` | `PGVS3_URL` | `postgres://localhost/pgvs3` | PostgreSQL URL: the writer endpoint, or a standby with --read-only (use sslmode for database TLS) |

## `pgvs3 health`

Exit 0 when a gateway's readiness URL returns 200.

| Flag | Default | Description |
| --- | --- | --- |
| `--url` | `http://127.0.0.1:8014/readyz` | readiness URL |

## `pgvs3 reap`

Run one bounded maintenance pass (or all passes) without a gateway.

| Flag | Default | Description |
| --- | --- | --- |
| `--all` | `false` | repeat the bounded pass until no garbage remains |
| `--grace` | `10m0s` | only reap files deleted at least this long ago |
| `--job-retention` | `24h0m0s` | keep terminal OJS jobs this long (0 = forever) |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 doctor`

Check the database, its settings and fleet connection capacity; print fixes.

| Flag | Default | Description |
| --- | --- | --- |
| `--gateways` | `0` | writable+read-only gateways sharing this database (0 skips the capacity check) |
| `--json` | `false` | print JSON |
| `--other-connections` | `0` | connections reserved for applications, poolers and admin |
| `--pool-max` | `64` | gateway --pool-max |
| `--surge` | `1` | extra gateways that run during a rolling update |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 stat`

Print namespace, object, chunk and storage-overhead counts.

| Flag | Default | Description |
| --- | --- | --- |
| `--json` | `false` | print JSON |
| `--kind` | — | bucket, cache or queue (default: all) |
| `--name` | — | namespace name |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 clone`

Copy a bucket or cache namespace inside PostgreSQL.

| Flag | Default | Description |
| --- | --- | --- |
| `--kind` | `bucket` | bucket or cache |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 drop`

Delete a namespace and its schema.

| Flag | Default | Description |
| --- | --- | --- |
| `--force` | `false` | drop non-empty namespaces and reclaim their schema immediately |
| `--kind` | `bucket` | bucket, cache or queue |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 adopt`

Register existing s3b_/s3c_/s3w_/s3q_ schemas in the catalog.

| Flag | Default | Description |
| --- | --- | --- |
| `--all` | `false` | adopt every matching schema |
| `--kind` | — | bucket, cache or queue (default: all) |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 origin`

Set, clear or list the origin a bucket (or every bucket) caches.

| Flag | Default | Description |
| --- | --- | --- |
| `--bucket` | — | restrict origin passthrough to one bucket (default: global) |
| `--cache-namespace` | `origin` | cache namespace in PostgreSQL |
| `--cache-retention` | `24h0m0s` | idle eviction interval |
| `--cache-ttl` | `1m0s` | origin revalidation interval (0 always revalidates) |
| `--fill-concurrency` | `8` | process-wide concurrent origin fills and pending-upload workers |
| `--range-cache-chunk` | `0` | cache aligned ranges of large objects at this byte size (0 disables) |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 pending`

List or retry write-back uploads that have not reached the origin.

| Flag | Default | Description |
| --- | --- | --- |
| `--namespace` | — | cache namespace (default: all) |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL URL |

## `pgvs3 version`

Print the binary version.

No flags.

## Metrics

<!-- metrics:begin -->
| Metric | Type | Labels |
| --- | --- | --- |
| `pgvs3_chunk_pool_acquire_count_total` | counter | — |
| `pgvs3_chunk_pool_acquire_duration_seconds_total` | counter | — |
| `pgvs3_chunk_pool_acquired_conns` | gauge | — |
| `pgvs3_chunk_pool_canceled_acquire_count_total` | counter | — |
| `pgvs3_chunk_pool_constructing_conns` | gauge | — |
| `pgvs3_chunk_pool_empty_acquire_count_total` | counter | — |
| `pgvs3_chunk_pool_empty_acquire_wait_seconds_total` | counter | — |
| `pgvs3_chunk_pool_idle_conns` | gauge | — |
| `pgvs3_chunk_pool_max_conns` | gauge | — |
| `pgvs3_chunk_pool_max_idle_destroy_count_total` | counter | — |
| `pgvs3_chunk_pool_max_lifetime_destroy_count_total` | counter | — |
| `pgvs3_chunk_pool_new_conns_count_total` | counter | — |
| `pgvs3_chunk_pool_total_conns` | gauge | — |
| `pgvs3_expired_objects_total` | counter | — |
| `pgvs3_maintenance_runs_total` | counter | — |
| `pgvs3_ojs_request_duration_seconds_bucket` | histogram | `le` |
| `pgvs3_ojs_request_duration_seconds_count` | histogram | — |
| `pgvs3_ojs_request_duration_seconds_sum` | histogram | — |
| `pgvs3_ojs_requests_total` | counter | `code`, `method` |
| `pgvs3_ojs_response_bytes_total` | counter | — |
| `pgvs3_pending_uploads_bytes` | gauge | — |
| `pgvs3_pending_uploads_dead` | gauge | — |
| `pgvs3_pending_uploads_live` | gauge | — |
| `pgvs3_pending_uploads_oldest_seconds` | gauge | — |
| `pgvs3_pg_pool_acquire_count_total` | counter | — |
| `pgvs3_pg_pool_acquire_duration_seconds_total` | counter | — |
| `pgvs3_pg_pool_acquired_conns` | gauge | — |
| `pgvs3_pg_pool_canceled_acquire_count_total` | counter | — |
| `pgvs3_pg_pool_constructing_conns` | gauge | — |
| `pgvs3_pg_pool_empty_acquire_count_total` | counter | — |
| `pgvs3_pg_pool_empty_acquire_wait_seconds_total` | counter | — |
| `pgvs3_pg_pool_idle_conns` | gauge | — |
| `pgvs3_pg_pool_max_conns` | gauge | — |
| `pgvs3_pg_pool_max_idle_destroy_count_total` | counter | — |
| `pgvs3_pg_pool_max_lifetime_destroy_count_total` | counter | — |
| `pgvs3_pg_pool_new_conns_count_total` | counter | — |
| `pgvs3_pg_pool_total_conns` | gauge | — |
| `pgvs3_reaped_files_total` | counter | — |
| `pgvs3_reaped_rows_total` | counter | — |
| `pgvs3_request_duration_seconds_bucket` | histogram | `le` |
| `pgvs3_request_duration_seconds_count` | histogram | — |
| `pgvs3_request_duration_seconds_sum` | histogram | — |
| `pgvs3_requests_total` | counter | `code`, `method` |
| `pgvs3_resp_requests_total` | counter | `command`, `status` |
| `pgvs3_response_bytes_total` | counter | — |
| `pgvs3_s3_admission_active_requests` | gauge | — |
| `pgvs3_s3_admission_queue_limit` | gauge | — |
| `pgvs3_s3_admission_queued_total` | counter | — |
| `pgvs3_s3_admission_rejected_total` | counter | — |
| `pgvs3_s3_admission_wait_seconds_total` | counter | — |
| `pgvs3_s3_admission_waiting_requests` | gauge | — |
| `pgvs3_spool_bytes` | gauge | — |
| `pgvs3_spool_limit_bytes` | gauge | — |
| `pgvs3_spool_peak_bytes` | gauge | — |
| `pgvs3_spool_rejections_total` | counter | — |
<!-- metrics:end -->
