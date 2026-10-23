# pgvs3 — S3 on PostgreSQL

pgvs3 is a small, fast storage server that speaks **S3**, **SQS**,
**HTTP** and **RESP (Redis/Valkey)** over one PostgreSQL database, and can also
run as an **accelerating cache tier in front of any object store** that
[thanos-io/objstore](https://github.com/thanos-io/objstore) supports.

It is built for [DuckLake](https://ducklake.select/) and co-located services:
DuckLake keeps its catalog in PostgreSQL, and with pgvs3 lake files, queues and
cached blobs live there too — one database to run, back up, and replicate.

The gateway is written in Go, holds zero in-process state, and ships as a
~14 MB distroless static container built with [ko](https://ko.build/).

> **Trusted networks only — permanently.** pgvs3 does not authenticate,
> authorize, or encrypt connections, and does not validate S3 signatures or
> credentials. Authentication is not part of the design and is not a future
> goal. Run it on a loopback interface, private VPC/VLAN, or behind a service
> mesh that enforces access control and TLS, and treat `:8014` like a database
> port. Presigned URLs are rejected because a scoped link would otherwise
> grant unscoped access. See [docs/design.md](docs/design.md).

**Status: early alpha.** `pgvs3 migrate` upgrades a layout v10 database to v11
in place. Older or unsupported layouts require a fresh database.

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL
18+ instance).

```sh
just setup
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
go run ./cmd/pgvs3 --url 'postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' serve
```

Namespaces are created automatically on first write. The single `:8014` port
speaks S3, SQS, plain HTTP and RESP (Redis/Valkey); the protocol is detected
from the connection, so there is nothing to configure:

```sh
# S3 / plain HTTP
curl -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl http://127.0.0.1:8014/lake/README.md

# Redis / Valkey (RESP2)
redis-cli -p 8014 SET greeting "hello from resp" EX 60
curl http://127.0.0.1:8014/_cache/cache/greeting
redis-cli -p 8014 GET s3://lake/README.md     # explicit cross-namespace read

# SQS (boto3, aws cli --endpoint-url http://127.0.0.1:8014)
aws --endpoint-url http://127.0.0.1:8014 sqs create-queue --queue-name jobs
aws --endpoint-url http://127.0.0.1:8014 sqs send-message \
  --queue-url http://127.0.0.1:8014/jobs --message-body '{"job":1}'
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

- **One PostgreSQL schema per namespace, typed by prefix.** `s3b_<name>` is a
  durable bucket, `s3c_<name>` a volatile cache database (UNLOGGED tables),
  `s3q_<name>` an SQS queue. Each owns its tables, autovacuum workers and 32 TiB
  chunk budget:
  - **32 TiB capacity and independent autovacuum per bucket.**
  - **Instant bucket deletion (`DROP SCHEMA CASCADE`):** returns disk space to
    the OS in milliseconds with no reaper lag or vacuum bloat.
  - **Single-bucket snapshots & clones:** dump/restore individual buckets with
    `pg_dump -n s3b_<bucket>`, or clone a bucket inside PostgreSQL with
    `pgvs3 clone <src> <dst>` (`PUT /{dst}` with `x-amz-copy-source: /{src}`).
- **Length-discriminated LZ4 block compression.** Every inline object
  (bucket: `≤64 KiB`, cache/queue: one 8 KB page) and every `8,120`-byte chunk
  block is compressed with an
  allocation-free pure-Go LZ4 block codec if it shrinks (`len(stored) < wantLen`),
  or stored as raw bytes with **zero header bytes** if incompressible
  (`len(stored) == wantLen`, e.g. Zstd Parquet). Logical chunk offsets remain
  `no × 8,120`, preserving `O(1)` byte-range seeking and the exact 8,160-byte
  PostgreSQL page budget (`99.6%` fill, no TOAST).
- **Four protocols, one port.** S3/SQS/HTTP share the HTTP listener; RESP2 is
  detected from the first byte. RESP defaults to the `cache` namespace and
  can read any durable bucket with `GET s3://<bucket>/<key>`; writes are
  confined to cache namespaces.
- **Accelerating cache tier (`--origin-config`).** With a Thanos objstore YAML
  file, every S3 `GET` becomes a read-through cache in `s3c_<namespace>` with
  TTL revalidation and write-through `PUT`/`DELETE`. Ranges above 8 MiB are
  passed through to the origin, and fills are bounded (`--fill-concurrency`)
  and spooled to memory or a temp file before touching PostgreSQL, so a slow
  origin never pins a database connection. Supported origins: `s3`,
  `filesystem` and `inmem`.
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

- **Latency (1 worker):** **`0.072 ms` (72 µs)** p50 for 4 KiB RESP `GET`
  (`0.17 ms` for S3 `GET`, `0.14 ms` for `HEAD`), **`0.18 ms`** for durable
  4 KiB RESP `SET` (`0.81 ms` for S3 `PUT`, `synchronous_commit = on`), and
  **`0.86 ms`** for an SQS `SendMessage`.
- **RESP cache throughput (4 KiB, 64 workers):** **97.0k `GET`/s** unpipelined
  and **460.4k `GET`/s (1.76 GiB/s)** at pipeline 16; **48.2k durable `SET`/s**
  unpipelined. The older **213.6k durable `SET`/s** pipeline-16 figure predates
  the redcon move: redcon returns one command per 4 KiB read, so large
  pipelined `SET`s may be processed individually rather than coalesced.
- **S3 throughput (4 KiB):** **35.8k `GET`/s**, **26.1k `HEAD`/s**, and
  **20.9k durable `PUT`/s**.
- **SQS throughput (4 KiB):** **12.7k `SendMessage`/s** and **2.1k
  receive+delete/s** at 32 workers.
- **Large-object bandwidth (64 MiB S3):** **4.17 GiB/s (4,275 MiB/s)** read
  (16 workers) and **529 MiB/s** write (8 workers).
- **Cache tier (64 MiB, filesystem origin, warm):** **4.0 GiB/s** read and
  **488 MiB/s** write through the proxy.

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
- **SQS:** `CreateQueue`, `GetQueueUrl`, `ListQueues`, `DeleteQueue`,
  `PurgeQueue`, `SendMessage`, `SendMessageBatch`, `ReceiveMessage`,
  `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`,
  `ChangeMessageVisibilityBatch`, `GetQueueAttributes`, `SetQueueAttributes`
  over both the JSON (`AmazonSQS.*`) and Query (`Action=`) protocols, with
  visibility timeouts, delays, retention and long polling. FIFO queues support
  `MessageGroupId` ordering, a five-minute `MessageDeduplicationId` window
  (explicit or content-based), and `SequenceNumber`; high-throughput FIFO mode
  is not implemented.
- **S3 cache tier:** read-through and write-through `GET`/`HEAD`/`PUT`/`CopyObject`/multipart uploads/`DELETE`/`DeleteObjects`/`ListObjectsV2` in front of an
  objstore origin (`X-Cache: HIT|MISS`), with TTL revalidation (`--cache-ttl`) and idle eviction
  (`--cache-retention`). `ListObjectsV2` is served from a PostgreSQL listing index that is
  rebuilt from the origin on a cold or stale prefix; write-through updates keep it current.
- **RESP (Redis/Valkey) cache protocol on `:8014`:** supports `GET`, `SET`
  (`EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`/`GET`), `SETNX`, `SETEX`,
  `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`,
  `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `GETRANGE`, `APPEND`,
  `SETRANGE`, `SCAN`, `TYPE`, `SELECT`,
  `FLUSHDB`, `DBSIZE`, `KEYS`, `INFO`, and `HELLO 2`. RESP3 is not supported:
  `HELLO 3` returns `NOPROTO`, and clients that default to RESP3 (for example
  recent `redis-py` releases) must be configured with `protocol=2`. Pipelined
  `GET` bursts coalesce into one `CacheMGet`; consecutive `SET`s coalesce into
  `CacheMSet` when redcon delivers them in the same read batch. `EXPIRE`
  honours `NX`/`XX`/`GT`/`LT`.
  `MULTI`/`EXEC`/`WATCH` are rejected explicitly (use a pipeline; in
  `redis-py` pass `transaction=False`) because pgvs3 has no cross-command
  snapshot, and pretending otherwise would lose updates.
- **Out of scope (not roadmap):** authentication/authorization, SigV4
  validation, presigned URLs, TLS termination, audit logging, per-tenant
  isolation, IAM/bucket policies, versioning, object tags, Object Lock, ACLs,
  lifecycle rules, replication, event notifications, SSE-C/SSE-KMS/DSSE-KMS, and
  multipart `FULL_OBJECT` checksums. The network is the security boundary; see
  [docs/design.md](docs/design.md).

## Configuration and CLI

Every flag maps to an environment variable.

| Flag | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | `postgres://localhost/pgvs3` | PostgreSQL connection URL (use `sslmode` for DB TLS). |
| `--addr` | `PGVS3_ADDR` | `0.0.0.0:8014` | Listen address for S3, HTTP, `/healthz`, `/readyz`, and `/metrics`. |
| `--pool-max` | `PGVS3_POOL_MAX` | `64` | Max PostgreSQL connections per gateway pod. |
| `--max-db-bytes` | `PGVS3_MAX_DB_BYTES` | `0` | Reject writes when `pg_database_size` exceeds this (0 = unlimited). |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m` | How long overwritten/deleted files stay readable before GC. |
| `--read-only` | `PGVS3_READ_ONLY` | `false` | Reject writes and skip DDL/reaping (for PostgreSQL standbys). |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`). |
| `--max-conns` | `PGVS3_MAX_CONNS` | `4096` | Concurrent client connections before shedding load. |
| `--origin-config` | `PGVS3_ORIGIN_CONFIG` | unset | objstore YAML file; enables the S3 cache tier. |
| `--cache-ttl` | `PGVS3_CACHE_TTL` | `1m` | Origin revalidation interval (`0` = always revalidate). |
| `--cache-retention` | `PGVS3_CACHE_RETENTION` | `24h` | Evict idle cached objects after this period. |
| `--cache-namespace` | `PGVS3_CACHE_NAMESPACE` | `origin` | Cache namespace holding origin objects. |
| `--fill-concurrency` | `PGVS3_FILL_CONCURRENCY` | `8` | Concurrent origin cache fills; bounds pool use while downloading. |
| `--queue-concurrency` | `PGVS3_QUEUE_CONCURRENCY` | `16` | Concurrent in-flight SQS requests. |
| `--resp-concurrency` | `PGVS3_RESP_CONCURRENCY` | `32` | Concurrent in-flight RESP batches. |

Subcommands:

- `pgvs3 serve` — run the S3/HTTP gateway (default when no subcommand is given).
- `pgvs3 stat [--kind bucket|cache|queue] [--name N] [--json]` — namespace
  usage, logical vs physical size and queued garbage.
- `pgvs3 clone [--kind bucket|cache] <src> <dst>` — clone inside PostgreSQL.
- `pgvs3 drop [--kind bucket|cache|queue] [--force] <name>` — drop a namespace
  schema (`DROP SCHEMA CASCADE`).
- `pgvs3 origin [--bucket B] [--cache-namespace NS] set|clear|list [origin.yaml]`
  — persist or remove global or per-bucket S3 origin cache configs inside
  PostgreSQL (gateways discover them automatically with no external files).
- `pgvs3 migrate` — upgrade a layout v10 database to v11 in place.
- `pgvs3 doctor` — verify PostgreSQL 18+, schema layout v11, and table settings.
- `pgvs3 reap [--all]` — reconcile schemas and run garbage collection immediately.
- `pgvs3 health` — poll `/readyz` for container health checks.

Build the container image with `ko` or pull the published image and OCI Helm
chart from GHCR:

```sh
docker pull ghcr.io/adonm/pgvs3:v0.2.0
helm install pgvs3 oci://ghcr.io/adonm/charts/pgvs3 --version 0.2.0

just image            # local ko.local image
just image push=true  # multi-arch ghcr.io/adonm/pgvs3 (amd64 + arm64)
CHART_VERSION=v0.3.1 just chart push=true  # OCI chart oci://ghcr.io/adonm/charts/pgvs3
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
just integration # store/queue/cache integration tests against PostgreSQL 18 (testcontainers)
just contract   # end-to-end S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, DuckLake, and SlateDB
just load       # direct S3 load generator benchmark
just bench      # DuckDB httpfs + DuckLake benchmark suite
```

Further reading:

- [docs/design.md](docs/design.md) — design and shape, trust model, and scope
  boundaries
- [docs/performance.md](docs/performance.md) — storage layout, read/write paths,
  and DuckDB tuning
- [docs/benchmarks.md](docs/benchmarks.md) — full benchmark tables and S3
  Express One Zone comparison
- [docs/operations.md](docs/operations.md) — deployment, probes, backups, and
  failure recovery
- [docs/partitioning.md](docs/partitioning.md) — why `s3p.chunks` is unpartitioned
- [docs/proposals/hardening-options.md](docs/proposals/hardening-options.md) —
  open compatibility and hardening options under review
- [CONTRIBUTING.md](CONTRIBUTING.md) — development workflow and invariants

## License

Apache-2.0. See [LICENSE](LICENSE).
