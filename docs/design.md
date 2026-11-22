# pgvs3 Design

## Architecture

`pgvs3` is a stateless S3 HTTP gateway backed by PostgreSQL 18:

- **S3 HTTP frontend (`internal/s3`)**: parses S3 object/bucket requests, decodes `aws-chunked` streaming request bodies, and enforces admission concurrency (`--s3-concurrency`).
- **Read-through cache (`internal/objcache`)**:
  - Whole objects `<= 8 MiB` (and aligned range chunks via `--range-cache-chunk`) are cached in PostgreSQL.
  - Cache fills use in-process `syncx.Group` singleflight, cross-gateway PostgreSQL fill leases (`s3p.cache_fill_leases`), and a bounded `--fill-concurrency` / `--spool-max-bytes` budget.
  - Large fills (> 8 MiB) spool to `--spool-dir` outside any PostgreSQL transaction before streaming into `chunks`.
- **Origin client & credential chain (`internal/s3client`)**: stdlib-only SigV4 signer with automatic credential resolution:
  1. Static env (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`)
  2. EKS Pod Identity (`AWS_CONTAINER_CREDENTIALS_FULL_URI` + `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`)
  3. IRSA Web Identity (`AWS_WEB_IDENTITY_TOKEN_FILE` + `AWS_ROLE_ARN` via STS `AssumeRoleWithWebIdentity`)
  4. IMDSv2 (`169.254.169.254`)
  5. Anonymous/unsigned fallback
- **PostgreSQL store (`internal/store`)**:
  - `s3p` catalog (`layout`, `namespaces`, `cache_fill_leases`, `file_ids`).
  - `s3c_<namespace>` cache schema (`objects`, `origin_metadata`, `chunks` with `STORAGE EXTERNAL`, `garbage`, `meta`).
  - Chunks are stored raw (uncompressed) because lake formats (Parquet, ORC, compressed tiles) are already compressed.

## Trust Model

`pgvs3` is designed to run inside a trusted cluster network (e.g., EKS VPC with `NetworkPolicy` restricting pod ingress):

- **No client authentication**: incoming client requests are not authenticated; presigned URLs are rejected.
- **Gateway-signed origin requests**: the gateway signs all upstream S3 requests using its own IAM credentials (Pod Identity / IRSA / IMDSv2 / static env).

## Consistency Model

- **Immutable by default (`--revalidate-after=0`)**: cache hits serve directly from PostgreSQL with zero origin calls.
- **TTL revalidation (`--revalidate-after > 0`)**: once an entry's age exceeds `--revalidate-after`, the gateway issues a conditional `HEAD` (`If-None-Match: "<etag>"`) to origin. A `304 Not Modified` refreshes the entry without re-downloading bytes.
- **Write invalidation**: writes (`PUT`, `CopyObject`, `DeleteObject`, `DeleteObjects`, `CompleteMultipartUpload`) proxy straight to origin and, on 2xx, delete the affected cache key in PostgreSQL so all gateway replicas immediately see the invalidation.
- **Out-of-band origin writes**: writes that bypass `pgvs3` are not seen until `--revalidate-after` elapses (or `--cache-retention` / `--cache-max-bytes` evicts the key).
