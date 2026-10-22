# pgvs3 — S3 on PostgreSQL

pgvs3 is a small, fast S3, HTTP, and RESP (Redis/Valkey) gateway that stores
objects and cached values as compressed blocks in PostgreSQL. It is built for
[DuckLake](https://ducklake.select/) and co-located services: DuckLake keeps
its catalog in PostgreSQL, and with pgvs3 both lake files and cached blobs live
there too — one database to run, back up, and replicate.

The gateway is written in Go, holds zero in-process state, and ships as a
~14 MB distroless static container built with [ko](https://ko.build/).

> **Private-network only.** pgvs3 does not validate S3 signatures or
> credentials; signed and unsigned requests are both accepted. Run it on a
> loopback interface, private VPC/VLAN, or behind a service mesh that enforces
> access control and TLS.

**Status: early alpha.** Storage layout changes require a fresh database (no
in-place migrations).

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL
18+ instance).

```sh
just setup
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
go run ./cmd/pgvs3 --url 'postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' serve
```

Buckets are created automatically on first write. The single `:8014` port
serves S3 clients, plain HTTP, and RESP (Redis/Valkey) clients on the same
underlying bucket tables:

```sh
# S3 / plain HTTP
curl -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl http://127.0.0.1:8014/lake/README.md

# Redis / Valkey (RESP2 & RESP3 on the same :8014 port)
redis-cli -p 8014 SET greeting "hello from resp" EX 60
curl http://127.0.0.1:8014/cache/greeting
redis-cli -p 8014 GET s3://lake/README.md
```

### Use from DuckDB 2.0+

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
SELECT * FROM read_text('s3://lake/README.md') LIMIT 1;
```

### Use with DuckLake (catalog + files in the same PostgreSQL)

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

## How it works

- **One PostgreSQL schema per bucket (`s3b_<bucket>`).** Every bucket has its
  own isolated `objects`, `chunks`, `uploads`, and `garbage` tables:
  - **32 TiB capacity and independent autovacuum per bucket.**
  - **Instant bucket deletion (`DROP SCHEMA CASCADE`):** returns disk space to
    the OS in milliseconds with no reaper lag or vacuum bloat.
  - **Single-bucket snapshots & clones:** dump/restore individual buckets with
    `pg_dump -n s3b_<bucket>`, or clone a bucket inside PostgreSQL with
    `pgvs3 clone <src> <dst>` (`PUT /{dst}` with `x-amz-copy-source: /{src}`).
- **Length-discriminated LZ4 block compression.** Every inline object
  (`≤64 KiB`) and every `8,120`-byte chunk block is compressed with an
  allocation-free pure-Go LZ4 block codec if it shrinks (`len(stored) < wantLen`),
  or stored as raw bytes with **zero header bytes** if incompressible
  (`len(stored) == wantLen`, e.g. Zstd Parquet). Logical chunk offsets remain
  `no × 8,120`, preserving `O(1)` byte-range seeking and the exact 8,160-byte
  PostgreSQL page budget (`99.6%` fill, no TOAST).
- **Unified S3 + HTTP + RESP (Redis/Valkey) protocol mux.** First-byte
  detection on `:8014` routes HTTP/S3 and RESP2/RESP3 connections to the same
  `s3b_<bucket>.objects` and `s3b_<bucket>.chunks` tables (including optional
  per-object `expires_at` TTLs and pipelined `CacheMGet` / `CacheMSet`
  coalescing).
- **Immutable files, atomic publish.** Writes stream into `chunks` via
  binary `COPY` under a new `file_id` and publish in the same transaction.
  Interrupted writes leave nothing visible.
- **Cut-through streaming reads.** Single-file reads up to 64 MiB run as one
  query; larger and multipart objects stream in 8 MiB parallel pieces under one
  repeatable-read snapshot so concurrent overwrites cannot corrupt an in-flight
  read.
- **PostgreSQL `shared_buffers` is the only cache.** Gateways hold no local
  metadata or byte cache. Every gateway pod sees writes immediately with strict
  MVCC consistency and zero invalidation lag; `--read-only` gateways can run
  against PostgreSQL streaming standbys for local-AZ reads.
- **Built-in garbage collection.** Overwritten and deleted files stay readable
  for `--reap-grace` (default 10 minutes), then an in-process reaper removes
  dead chunk rows in bounded passes. No `pg_cron` extension is needed.

See [docs/performance.md](docs/performance.md) for the full storage and query
design.

## Performance at a glance

Measured with `just load` against PostgreSQL 18 in Docker (`shared_buffers=1GB`)
on a 16-core host (6-vCPU VM numbers in [docs/benchmarks.md](docs/benchmarks.md)):

- **Latency (1 worker):** **`0.081 ms` (81 µs)** p50 for 4 KiB RESP `GET`
  (`0.17 ms` for S3 `GET`, `0.14 ms` for `HEAD`), and **`0.68 ms`** for durable
  4 KiB RESP `SET` (`0.82 ms` for S3 `PUT`, `synchronous_commit = on`).
- **RESP cache throughput (4 KiB, 64 workers):** **91.0k `GET`/s** unpipelined
  and **459.0k `GET`/s (1.79 GiB/s)** at pipeline 16; **23.9k durable `SET`/s**
  unpipelined and **110.2k durable `SET`/s (431 MiB/s)** at pipeline 16.
- **S3 throughput (4 KiB):** **35.5k `GET`/s**, **30.0k `HEAD`/s**, and
  **20.4k durable `PUT`/s**.
- **Large-object bandwidth (64 MiB S3):** **4.16 GiB/s (4,262 MiB/s)** read
  (16 workers) and **509 MiB/s** write (8 workers).

**Compared with AWS S3 Express One Zone (`us-east-1`):**

1. **Latency:** pgvs3 is ~10× faster on loopback/LAN (`0.17 ms` vs
   single-digit ms).
2. **Read ceiling:** one 16-core pgvs3 node (`32.5k GET/s`) is ~6× below
   Express's `200k GET/s` default bucket quota (`2M/s` max).
3. **Write ceiling:** pgvs3 (`17.5k PUT/s`) is ~6× below Express's `100k PUT/s`
   default bucket quota (`200k/s` max).
4. **Bandwidth:** pgvs3 streams `3.45 GiB/s` from cache/NVMe (`125–1,000 MiB/s`
   on cold EBS gp3); Express scales elastically with client network.
5. **500 GB of 64 MiB Parquet files (1× W+R/mo):** Express is ~$57/mo vs
   ~$100–200/mo for a dedicated pgvs3 node, or $40/mo in gp3 disk on an
   existing PostgreSQL.
6. **500 GB of 122M 4 KiB objects (1× W+R/mo):** Express rises to ~$199/mo due
   to `$1.13/M` `PUT` fees; pgvs3 charges $0 per request and stays at ~$165/mo
   dedicated or $40/mo on an existing PostgreSQL.

Full tables and DuckDB `httpfs` benchmarks are in
[docs/benchmarks.md](docs/benchmarks.md).

## S3 and HTTP support

- **Supported S3 operations:** `GET` (byte ranges and conditional headers),
  `HEAD`, `PUT`, `DELETE`, `DeleteObjects`, `ListObjectsV2`, `ListBuckets`,
  `CreateBucket`, `HeadBucket`, `DeleteBucket`, multipart uploads
  (`CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`,
  `ListMultipartUploads`), `CopyObject`, `GetObjectAttributes`, and
  `partNumber` on `GET`/`HEAD`.
- **Zero-copy & server-side copies:** Same-bucket `CopyObject` and whole-object
  `UploadPartCopy` share immutable `file_id`s without copying chunk rows;
  cross-bucket `CopyObject` and whole-bucket clones (`PUT /{dst}` with
  `x-amz-copy-source: /{src}`) copy chunk rows inside PostgreSQL without
  streaming bytes through the gateway.
- **Instant bucket drop:** `DELETE /{bucket}?force=true` (or
  `pgvs3 drop --force <bucket>`) drops `s3b_<bucket>` with `DROP SCHEMA CASCADE`
  and immediately returns OS disk space.
- **Checksums:** verifies MD5, CRC32, CRC32C, CRC64NVME, SHA-1, and SHA-256 on
  upload; stores per-part checksums on multipart uploads.
- **Plain HTTP & static sites:** `GET /{bucket}/{key}` works with any HTTP
  client or browser. Trailing slashes resolve to `index.html`, plain
  `GET /{bucket}` serves `/{bucket}/index.html` when present, and `Content-Type`
  falls back to the filename extension.
- **RESP (Redis/Valkey) cache protocol on `:8014`:** supports `GET`, `SET`
  (`EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`/`GET`), `SETNX`, `SETEX`,
  `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`,
  `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `GETRANGE` (slices compressed
  8,120-byte blocks without full-object reads), `SELECT <db|bucket>`,
  `FLUSHDB`, `DBSIZE`, `KEYS`, `INFO`, and `HELLO 2`/`3`. Pipelined `GET` and
  `SET` bursts automatically coalesce into single-query `CacheMGet` /
  `CacheMSet` database round trips.
- **Out of scope:** IAM/bucket policies, versioning, object tags, Object Lock,
  ACLs, lifecycle rules, replication, event notifications, SSE-C/DSSE-KMS, and
  multipart `FULL_OBJECT` checksums.

## Configuration and CLI

Every flag maps to an environment variable.

| Flag | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | `postgres://localhost/pgvs3` | PostgreSQL connection URL (use `sslmode` for DB TLS). |
| `--addr` | `PGVS3_ADDR` | `0.0.0.0:8014` | Listen address for S3, HTTP, `/healthz`, `/readyz`, and `/metrics`. |
| `--pool-max` | `PGVS3_POOL_MAX` | `64` | Max PostgreSQL connections per gateway pod. |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m` | How long overwritten/deleted files stay readable before GC. |
| `--read-only` | `PGVS3_READ_ONLY` | `false` | Reject writes and skip DDL/reaping (for PostgreSQL standbys). |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`). |

Subcommands:

- `pgvs3 serve` — run the S3/HTTP gateway (default when no subcommand is given).
- `pgvs3 stat [--bucket B] [--json]` — report buckets, objects, chunks, logical
  vs physical size, and queued garbage.
- `pgvs3 clone <src-bucket> <dst-bucket>` — clone a bucket inside PostgreSQL.
- `pgvs3 drop [--force] <bucket>` — drop a bucket schema (`DROP SCHEMA CASCADE`).
- `pgvs3 doctor` — verify PostgreSQL 18+, schema layout v10, and table settings.
- `pgvs3 reap [--all]` — run garbage collection immediately.
- `pgvs3 health` — poll `/readyz` for container health checks.

Build the container image with `ko` or pull the published image and OCI Helm
chart from GHCR:

```sh
docker pull ghcr.io/adonm/pgvs3:v0.2.0
helm install pgvs3 oci://ghcr.io/adonm/charts/pgvs3 --version 0.2.0

just image            # local ko.local image
just image push=true  # multi-arch ghcr.io/adonm/pgvs3 (amd64 + arm64)
just chart push=true  # OCI Helm chart oci://ghcr.io/adonm/charts/pgvs3
```

## Limits to keep in mind

- **32 TiB per bucket:** each bucket's `chunks` table is a single PostgreSQL
  relation capped at 32 TiB (`MaxBlockNumber × 8 KB`, ~31.7 TiB of object
  payload) and ~17.4 TB per object.
- **Single-writer design per lake:** built for one DuckLake writer per bucket
  and many readers. Scale reads with additional DuckDB workers, gateway pods,
  and PostgreSQL read replicas.
- **Disk reclamation:** dropping a bucket (`pgvs3 drop --force <bucket>`)
  immediately returns disk space to the OS via `DROP SCHEMA CASCADE`. Within a
  live bucket, deleted pages are reused by PostgreSQL after `--reap-grace` and
  autovacuum, while returning space to the OS without dropping the bucket
  requires `VACUUM FULL` or `pg_repack`.

## Testing and documentation

```sh
just test       # Go unit tests
just race       # unit tests with race detector
just contract   # end-to-end S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, DuckLake, and SlateDB
just load       # direct S3 load generator benchmark
just bench      # DuckDB httpfs + DuckLake benchmark suite
```

Further reading:

- [docs/performance.md](docs/performance.md) — storage layout, read/write paths,
  and DuckDB tuning
- [docs/benchmarks.md](docs/benchmarks.md) — full benchmark tables and S3
  Express One Zone comparison
- [docs/operations.md](docs/operations.md) — deployment, probes, backups, and
  failure recovery
- [docs/partitioning.md](docs/partitioning.md) — why `s3p.chunks` is unpartitioned
- [CONTRIBUTING.md](CONTRIBUTING.md) — development workflow and invariants

## License

Apache-2.0. See [LICENSE](LICENSE).
