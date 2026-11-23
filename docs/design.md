# pgvs3 Design

## Architecture

`pgvs3` is a stateless S3 HTTP gateway that caches origin S3 objects in PostgreSQL 18:

- **S3 HTTP frontend (`internal/s3`)**: path-style S3 routing, `aws-chunked` request decoding, conditional/range handling and admission control (`--s3-concurrency`).
- **Read-through cache (`internal/objcache`)**: objects are cached as fixed, aligned chunks (`--range-cache-chunk`, default 1 MiB). A read fills only the chunks it is missing, so partially cached objects are normal. Concurrent fills of the same chunk range coalesce in-process; fills are bounded by `--fill-concurrency` and never hold a PostgreSQL connection while waiting on origin. Unknown-length proxied uploads (> 8 MiB) spool to `--spool-dir` under `--spool-max-bytes`.
- **Origin client (`internal/s3client`)**: stdlib SigV4 signer, object `Stat`/`Get`, and a re-signing proxy for writes and listings. Credentials resolve in order:
  1. Static env (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`)
  2. EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`)
  3. IRSA web identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` via STS `AssumeRoleWithWebIdentity`)
  4. IMDSv2 (`169.254.169.254`)
  5. Anonymous/unsigned
- **PostgreSQL store (`internal/store`)**: two cache tables plus `cache_layout` (`internal/store/schema.sql`):
  - `cache_objects`: one row per `(bucket, key)` with origin ETag, size and response metadata, plus `last_access` for LRU eviction.
  - `cache_chunks`: `(object_id, idx)` → raw chunk bytes (`STORAGE EXTERNAL`), deleted with their object. Bytes are stored uncompressed because lake formats are already compressed.

## Trust Model

`pgvs3` runs inside a trusted cluster network (for example an EKS VPC with a `NetworkPolicy` restricting ingress):

- **No client authentication**: client requests are not authenticated; presigned URLs are rejected.
- **Gateway-signed origin requests**: the gateway signs every upstream request with its own credentials.

## Consistency Model

- **Immutable by default (`--revalidate-after=0`)**: hits are served from PostgreSQL with zero origin calls.
- **TTL revalidation (`--revalidate-after > 0`)**: a stale entry is revalidated with a conditional `HEAD` (`If-None-Match`). `304` refreshes it; a new ETag replaces it.
- **Write invalidation**: `PUT`, `CopyObject`, `DeleteObject`, `DeleteObjects` and `CompleteMultipartUpload` go straight to origin and, on success, delete the cached key in PostgreSQL, so every replica sees the change.
- **Out-of-band origin writes** are not seen until `--revalidate-after` elapses or the key is evicted.

## Performance Model

- **One connection per GET**: a GET streams its chunks sequentially over one PostgreSQL connection. Small reads fetch metadata and the first chunk in one round trip.
- **Clients parallelise large reads**: the gateway does not fan one GET out over several connections. Engines that need throughput on large objects (DuckDB, Trino, Spark, AWS SDK transfer managers) already issue concurrent range GETs, and each lands on its own connection. Size `--pool-max` and `--s3-concurrency` for the client's total range concurrency.
- **No in-process byte cache**: PostgreSQL is the shared tier; clients may keep their own local caches.
