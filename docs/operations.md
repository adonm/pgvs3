# pgvs3 Operations

## Configuration

Every `pgvs3 serve` flag can be set via `PGVS3_<FLAG_NAME>` environment variables (e.g. `--origin-endpoint` → `PGVS3_ORIGIN_ENDPOINT`).

Key flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--url` | `postgres://localhost/pgvs3` | PostgreSQL connection URL |
| `--addr` | `0.0.0.0:8014` | HTTP listen address (`/healthz`, `/readyz`, `/metrics`, S3) |
| `--origin-endpoint` | `""` | Custom S3 endpoint URL (empty uses AWS S3 for `--origin-region`) |
| `--origin-region` | `us-east-1` | Origin AWS region |
| `--origin-path-style` | `false` | Use path-style addressing for origin |
| `--buckets` | `""` | Optional comma-separated bucket allowlist |
| `--revalidate-after` | `0` | Origin revalidation interval (`0` = immutable, never revalidate hits) |
| `--range-cache-chunk` | `0` | Aligned cache chunk size in bytes (`0` = 1 MiB; clamped to 64 KiB–8 MiB) |
| `--cache-max-bytes` | `0` | Evict least recently used objects above this byte budget (`0` = unlimited) |
| `--cache-retention` | `24h` | Idle eviction retention period |
| `--pool-max` | `64` | PostgreSQL connections; each streaming GET holds one |
| `--s3-concurrency` | `32` | Concurrent S3 requests per gateway before queueing (`--s3-queue-wait`) and `503 SlowDown` |
| `--fill-concurrency` | `8` | Max concurrent origin fills per gateway |
| `--spool-max-bytes` | `8GiB` | Spool byte budget for fills and unknown-length PUTs |

## Sizing

A GET streams over one PostgreSQL connection, so large-object throughput comes
from client range concurrency, not from the gateway. Set `--pool-max` and
`--s3-concurrency` to cover the clients' total concurrent range GETs per
gateway, and keep `pool-max × replicas` within PostgreSQL `max_connections`.

## Upgrade Path

The cache is rebuildable from S3, so there is no in-place migration. Point
`pgvs3` at a fresh database, or drop the previous pgvs3 schemas (`s3p` and the
`s3c_*`/`s3b_*` namespace schemas) and the `cache_*` tables. `pgvs3 serve`
initializes the current layout on an empty database and refuses a mismatched
`cache_layout` version.

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

See `deploy/charts/pgvs3/values.yaml` for EKS Pod Identity / IRSA (`extraEnv`, `podAnnotations`, `serviceMonitor`).
