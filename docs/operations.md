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
| `--range-cache-chunk` | `0` | Aligned range chunk size in bytes (e.g. `1048576` for 1 MiB) |
| `--cache-max-bytes` | `0` | Evict oldest cached objects above this byte budget (`0` = unlimited) |
| `--cache-retention` | `24h` | Idle eviction retention period |
| `--pool-max` | `64` | Main PostgreSQL connection pool size |
| `--fill-concurrency` | `8` | Max concurrent origin fills per gateway |
| `--spool-max-bytes` | `8GiB` | Spool byte budget for fills and unknown-length PUTs |

## Upgrade Path (Schema v26)

The cache is rebuildable from S3. When upgrading from an older `pgvs3` schema (v25 or earlier):

```sql
DROP SCHEMA IF EXISTS s3p CASCADE;
DROP SCHEMA IF EXISTS s3c_origin CASCADE;
```

Then start `pgvs3 serve`; it will initialize the v26 cache-only baseline automatically.

## Monitoring

Prometheus metrics are exposed at `GET /metrics`:

- `pgvs3_cache_hits_total`, `pgvs3_cache_misses_total`
- `pgvs3_cache_hit_bytes_total`, `pgvs3_cache_miss_bytes_total`
- `pgvs3_cache_fill_duration_seconds_sum`, `pgvs3_cache_fill_duration_seconds_count`
- `pgvs3_origin_errors_total`
- `pgvs3_cache_evicted_objects_total`, `pgvs3_expired_objects_total`
- `pgvs3_requests_total`, `pgvs3_request_duration_seconds_*`, `pgvs3_response_bytes_total`
- `pgvs3_spool_bytes`, `pgvs3_spool_peak_bytes`, `pgvs3_spool_rejections_total`
- PostgreSQL pool stats (`pgvs3_pool_*`, `pgvs3_chunk_pool_*`)

## Helm

```bash
helm lint deploy/charts/pgvs3
```

See `deploy/charts/pgvs3/values.yaml` for EKS Pod Identity / IRSA (`extraEnv`, `podAnnotations`, `serviceMonitor`).
