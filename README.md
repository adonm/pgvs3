# pgvs3

Persistent, shared read-through S3 cache backed by PostgreSQL.

Clients point their S3 endpoint at `pgvs3`. Cached object bytes and metadata live in PostgreSQL so every gateway replica shares one warm cache that survives restarts and scale-out.

## Quick start

```bash
# Start PostgreSQL 18 (e.g. `just dev-db`) and run pgvs3 against S3:
pgvs3 serve \
  --url postgres://postgres:postgres@127.0.0.1:5432/pgvs3_test \
  --origin-region us-east-1
```

Point any S3 client (AWS SDK, boto3, DuckDB `httpfs`, Trino) at `http://127.0.0.1:8014` with path-style addressing (`s3://bucket/key` maps transparently to origin `s3://bucket/key`).

## Features

- **Transparent bucket mapping**: client `s3://B/K` maps to origin `s3://B/K`, optionally restricted via `--buckets`.
- **Read-through cache**: `GET` and `HEAD` (including single byte ranges, suffix ranges, open-ended ranges, `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since`) serve from PostgreSQL; misses fill from origin with singleflight coalescing, aligned range-chunk caching (`--range-cache-chunk`), and bounded spooling outside PG connections.
- **Immutable by default + TTL fallback**: `--revalidate-after=0` (default) makes zero origin calls on cache hits; setting `--revalidate-after` revalidates stale entries via conditional `HEAD` (`If-None-Match`).
- **Write & list passthrough**: `PUT`, `CopyObject`, `DeleteObject`, `DeleteObjects`, and multipart uploads stream straight to origin (re-signed by the gateway) and invalidate affected cache keys in PostgreSQL. `ListObjectsV2` / `ListObjects` pass straight through to origin.
- **Stdlib AWS credential chain**: static env → EKS Pod Identity → IRSA web identity → IMDSv2 → anonymous fallback.
- **Observability & eviction**: `/healthz`, `/readyz`, Prometheus `/metrics`, and bounded eviction by `--cache-max-bytes` and `--cache-retention`.

## Docs

- [Design](docs/design.md) — architecture, trust model, and consistency model
- [Operations](docs/operations.md) — flags, metrics, Helm deployment, and upgrade path
