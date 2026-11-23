# pgvs3 Operations

## CLI

Invoke `pgvs3 <command> [flags]` (running `pgvs3` with no arguments runs `serve`). Commands: `serve`, `health`, `reap`, `stat`, `version`.

## Configuration

Every `pgvs3 serve` flag can be set via `PGVS3_<FLAG_NAME>` environment variables (e.g. `--origin-endpoint` → `PGVS3_ORIGIN_ENDPOINT`).

All `pgvs3 serve` flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL connection URL |
| `--addr` | `0.0.0.0:8014` | HTTP listen address (`/healthz`, `/readyz`, `/metrics`, S3) |
| `--pool-max` | `64` | PostgreSQL connections; each streaming GET holds one |
| `--spool-dir` | `` | Scratch directory for unknown-length proxied uploads over 8 MiB |
| `--spool-max-bytes` | `8589934592` | Spool byte budget (8 GiB) for unknown-length proxied uploads |
| `--db-work-mem` | `16MB` | Session `work_mem` per sort/hash operation |
| `--db-statement-timeout` | `5m0s` | PostgreSQL statement timeout (`0` disables) |
| `--db-lock-timeout` | `5s` | PostgreSQL lock timeout (`0` disables) |
| `--db-idle-tx-timeout` | `1m0s` | PostgreSQL idle-in-transaction timeout (`0` disables) |
| `--db-max-conn-lifetime` | `1h0m0s` | PostgreSQL connection lifetime (`0` disables) |
| `--db-max-conn-lifetime-jitter` | `5m0s` | Randomized PostgreSQL connection lifetime jitter |
| `--max-conns` | `4096` | Concurrent client connections before shedding load |
| `--http-read-timeout` | `30m0s` | HTTP read deadline including request bodies (`0` disables) |
| `--http-write-timeout` | `30m0s` | HTTP response write deadline (`0` disables) |
| `--read-only` | `false` | Reject writes and skip DDL, eviction, and cache fills (for standbys) |
| `--log-level` | `info` | Log level (`debug`, `info`, `warn`, `error`) |
| `--origin-endpoint` | `` | Custom S3 endpoint URL (empty uses AWS S3 for `--origin-region`) |
| `--origin-region` | `us-east-1` | Origin AWS region (defaults to `AWS_REGION` when set, else `us-east-1`) |
| `--origin-path-style` | `false` | Use path-style addressing for origin |
| `--origin-insecure` | `false` | Skip TLS certificate verification for origin |
| `--buckets` | `` | Optional comma-separated bucket allowlist |
| `--revalidate-after` | `0s` | Origin revalidation interval (`0` = immutable, never revalidate hits) |
| `--cache-retention` | `24h0m0s` | Idle eviction retention period |
| `--fill-concurrency` | `8` | Max concurrent origin fills per gateway |
| `--range-cache-chunk` | `0` | Aligned cache chunk size in bytes (`0` = 1 MiB; clamped to 64 KiB–8 MiB) |
| `--cache-max-bytes` | `0` | Evict least recently used objects above this byte budget (`0` = unlimited) |
| `--s3-concurrency` | `32` | Concurrent S3 requests per gateway before queueing (`--s3-queue-wait`) and `503 SlowDown` |
| `--s3-queue-wait` | `1s` | Maximum S3 admission wait (`0` rejects excess immediately) |

## Sizing

A GET streams over one PostgreSQL connection, so large-object throughput comes
from client range concurrency, not from the gateway. Set `--pool-max` and
`--s3-concurrency` to cover the clients' total concurrent range GETs per
gateway, and keep `pool-max × replicas` within PostgreSQL `max_connections`.

## Upgrade Path

The cache is rebuildable from S3, so there is no in-place migration. Use a fresh
database or drop the `cache_*` tables (`DROP TABLE IF EXISTS cache_chunks, cache_objects, cache_layout CASCADE;`). `pgvs3 serve` initializes the current layout on an empty database and refuses a mismatched `cache_layout` version.

## Monitoring

Prometheus metrics are exposed at `GET /metrics`:

- `pgvs3_cache_hits_total`, `pgvs3_cache_misses_total`
- `pgvs3_cache_hit_bytes_total`, `pgvs3_cache_miss_bytes_total`
- `pgvs3_cache_fill_duration_seconds_sum`, `pgvs3_cache_fill_duration_seconds_count`
- `pgvs3_origin_errors_total`
- `pgvs3_cache_evicted_objects_total`, `pgvs3_expired_objects_total`
- `pgvs3_requests_total`, `pgvs3_request_duration_seconds_{bucket,sum,count}`, `pgvs3_response_bytes_total`
- `pgvs3_spool_bytes`, `pgvs3_spool_peak_bytes`, `pgvs3_spool_limit_bytes`, `pgvs3_spool_rejections_total`
- `pgvs3_s3_admission_*` (active, waiting, queued, rejected)
- PostgreSQL pool stats (`pgvs3_pool_*`)

## Helm

```bash
helm lint deploy/charts/pgvs3
```

## Helm

```bash
helm lint deploy/charts/pgvs3
```

For EKS IRSA, set `serviceAccount.annotations."eks.amazonaws.com/role-arn"` and `serviceAccount.automountServiceAccountToken: true`. For EKS Pod Identity, target `serviceAccount.name` (default `pgvs3`) in the Pod Identity association and set `serviceAccount.automountServiceAccountToken: true`. See `deploy/charts/pgvs3/values.yaml` for `extraEnv` and `serviceMonitor`.

## Public S3 Smoke

`just s3-public` (`test/s3-public.sh`) exercises live anonymous AWS S3 buckets (`overturemaps-us-west-2`, `noaa-gsod-pds`, `tripdata`) and verifies region-mismatch `301 PermanentRedirect` handling. It is network-dependent and excluded from `just ci`.
