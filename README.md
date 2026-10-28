# pgvs3 — S3 on PostgreSQL

pgvs3 is a small, fast storage server that speaks **S3**, **OJS
(OpenJobSpec)**, **HTTP** and **RESP (Redis/Valkey)** over one PostgreSQL
database, and can also run as an **accelerating cache tier in front of an
S3-compatible or read-only HTTP(S) object store**.

It is built for [DuckLake](https://ducklake.select/) and co-located services:
DuckLake keeps its catalog in PostgreSQL, and with pgvs3 lake files, queues and
cached blobs live there too — one PostgreSQL cluster to run, back up, and
replicate.

The gateway is a stateless Go process with no authoritative local state. It
ships as a ~15 MB distroless static container built with
[ko](https://ko.build/).

> **Trusted networks only — permanently.** pgvs3 does not authenticate,
> authorize, or encrypt connections, and does not validate S3 signatures or
> credentials. Authentication is not part of the design and is not a future
> goal. Run it on a loopback interface, private VPC/VLAN, or behind a service
> mesh that enforces access control and TLS, and treat `:8014` like a database
> port. Presigned URLs are rejected because a scoped link would otherwise
> grant unscoped access. See [docs/design.md](docs/design.md).

**Status: early alpha.** Production PostgreSQL targets Aurora PostgreSQL 18.3+
or community/self-managed PostgreSQL 18. The PostgreSQL schema is one embedded baseline
(`internal/store/schema.sql`), currently layout **v25**, including shared origin
parent metadata and PostgreSQL 18 publication SQL, with no in-place upgrade path.
A layout change requires a fresh database and a re-import; there are no stable
on-disk compatibility guarantees yet.

## Start here

- **What pgvs3 is:** one stateless gateway that projects a PostgreSQL database
  as S3, OJS (OpenJobSpec v1 core), plain HTTP, and RESP2 on a single port.
- **Supported scale:** at most **100 namespaces** (PostgreSQL schemas) per
  database, one PostgreSQL primary plus streaming standbys, and multiple
  gateway workers sharing that cluster.
- **Safety model:** PostgreSQL is the coordination plane. Gateways keep no
  authoritative local state; the namespace catalog, advisory locks, leases,
  conditional writes, and token guards make concurrent workers safe.
- **Read next:** [Quick start](#quick-start) to run it,
  [docs/design.md](docs/design.md) for the model,
  [docs/operations.md](docs/operations.md) to deploy and operate it.

## Project goals and operating envelope

| Dimension | Envelope |
| --- | --- |
| **Namespaces** | At most **100 PostgreSQL schemas per database**, counting buckets, cache namespaces, and queues. This is a bounded single-cluster envelope, not a schema-per-tenant design. |
| **Gateway fleet** | Multiple stateless `pgvs3` workers behind a load balancer; all share one PostgreSQL cluster and coordinate through the database. |
| **PostgreSQL topology** | One primary (writer) plus streaming standbys. A single PostgreSQL database, not sharded or multi-primary. |
| **Reads** | Writable gateways read from the primary. Read-only gateways (`--read-only`) can serve reads from standbys, subject to replication lag. |
| **Writes** | Sent to writable gateways connected to the writer endpoint. DuckLake still has one writer per bucket. |
| **Coordination** | `s3p.namespaces` catalog, advisory locks, database leases, conditional/version-fenced writes, and database sequences. No local process state is authoritative. |
| **Backups** | Physical base backup plus WAL archiving/PITR for production clusters; logical `pg_dump` for export or small deployments. Origin data, when using the cache tier, is backed up separately. |
| **Security** | Trusted network only: no authentication, authorization, TLS termination, audit log, or per-tenant isolation. |
| **Layout** | Current layout is **v25**. There is no in-place migration path; a layout change needs a fresh database and re-import. |

## Tracing

Tracing is optional and disabled unless an OTLP endpoint is configured. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-specific endpoint), optionally
`OTEL_SERVICE_NAME`, and `OTEL_TRACES_SAMPLER_ARG` (0..1, default 1). HTTP
requests start server spans, PostgreSQL statements get client spans, and origin
HTTP requests propagate the trace context. The response includes `X-Trace-Id`.
No endpoint means the OpenTelemetry API remains a no-op.

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL
18+ instance).

```sh
just setup
just hooks    # install hk pre-commit/pre-push hooks (optional but recommended)
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
go run ./cmd/pgvs3 --url 'postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' serve
```

Buckets are explicit: create one before writing objects, and a write to a
missing or deleted bucket returns `NoSuchBucket` instead of recreating it. RESP
cache namespaces and OJS queues are created on first use. The single `:8014`
port speaks S3, OJS, plain HTTP and RESP (Redis/Valkey); the protocol is
detected from the connection, so there is nothing to configure:

```sh
# CreateBucket, then S3 / plain HTTP
curl -fsS -X PUT http://127.0.0.1:8014/lake
curl -fsS -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl -fsS http://127.0.0.1:8014/lake/README.md

# Redis / Valkey (RESP2)
redis-cli -p 8014 SET greeting "hello from resp" EX 60
curl http://127.0.0.1:8014/_cache/cache/greeting
redis-cli -p 8014 GET s3://lake/README.md     # explicit cross-namespace read

# OJS (OpenJobSpec v1 core lifecycle)
curl -X POST http://127.0.0.1:8014/ojs/v1/jobs \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"type":"email.send","args":["user@example.com"],"options":{"queue":"jobs"}}'
curl -X POST http://127.0.0.1:8014/ojs/v1/workers/fetch \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"queues":["jobs"],"count":1,"worker_id":"worker-1","visibility_timeout_ms":30000}'
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

This assumes the `lake` bucket created in the quick start.

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
  durable bucket, `s3c_<name>` the WAL-logged shared cache namespace (origin
  read fills and RESP/cache users), `s3w_<name>` the WAL-logged write-back
  namespace, and `s3q_<name>` an OJS queue/stream namespace. Each owns its tables, autovacuum
  workers and 32 TiB chunk budget:
  - **32 TiB capacity and independent autovacuum per bucket.**
  - **Instant bucket deletion (`DROP SCHEMA CASCADE`):** returns disk space to
    the OS in milliseconds with no reaper lag or vacuum bloat.
  - **Single-bucket snapshots & clones:** dump/restore individual buckets with
    `pg_dump -n s3b_<bucket>`, or clone a bucket inside PostgreSQL with
    `pgvs3 clone <src> <dst>` (`PUT /{dst}` with `x-amz-copy-source: /{src}`).
- **Length-discriminated Zstd compression.** Every inline object
  (bucket: `≤64 KiB`, cache: one 8 KB page) and every `8,120`-byte chunk
  block is compressed with a reusable
  [Zstandard](https://github.com/klauspost/compress) encoder if it shrinks
  (`len(stored) < wantLen`), or stored as raw bytes if incompressible
  (`len(stored) == wantLen`, e.g. already-compressed Parquet). The compressed
  form is a standard Zstd frame; raw blocks remain header-free. OJS job
  `args`/`meta` are JSONB so their envelopes can exceed one page. Logical chunk
  offsets remain `no × 8,120`, preserving `O(1)` byte-range seeking and the
  exact 8,160-byte PostgreSQL page budget (`99.6%` fill, no TOAST).
- **Four protocols, one port.** S3/OJS/HTTP share the HTTP listener; RESP2 is
  detected from the first byte. RESP defaults to the `cache` namespace and
  can read any durable bucket with `GET s3://<bucket>/<key>`; writes are
  confined to cache namespaces.
- **Accelerating cache tier (`--origin-config`).** With an S3, HTTP(S) or
  filesystem origin, S3 `GET`/`HEAD` become read-through cache reads in
  `s3c_<namespace>` (WAL-logged and replicated) with TTL revalidation.
  Writable origins also receive
  `PUT`/`COPY`/`DELETE` write-through; read-only HTTP(S) origins serve reads
  only. Request checksums are verified and conditional writes are enforced
  before the origin is mutated; Content-Type, Content-Encoding,
  `x-amz-meta-*` metadata and the origin `Last-Modified` timestamp round-trip
  through the origin. For large unconditional objects, the default
  `--cache-write-back` path commits the body to the WAL-logged
  `s3w_<namespace>` write-back namespace and the origin outbox row in one
  transaction, then acknowledges before the origin upload. Pending bodies are
   immediately readable and locally WAL-durable. Lossless failover depends on
   the PostgreSQL replication/promotion policy; read-only replicas see them
   after WAL replay. The bounded worker retries the origin upload. Small
   PUT/COPY and conditional writes remain synchronous. Unconditional multipart
   completion can acknowledge before origin upload regardless of body size;
   conditional completion stays synchronous, with the captured origin version
   used as an `If-Match`/`If-None-Match` precondition when available. With
  `--range-cache-chunk`, ranges of large objects are cached as
  aligned chunks instead of being passed through, which makes Parquet row-group
  and PMTiles range reads repeatable. Fills are bounded (`--fill-concurrency`)
   and spooled to memory or a temp file before touching PostgreSQL, so a slow
   read fill never pins a database connection. Disk spools share an 8 GiB
   process-wide payload budget (`--spool-max-bytes`); exhausted admission returns
   `503 SlowDown` before acknowledging a write. Write-back uploads retain a
   session key lock to preserve remote ordering. The production origins are
  S3-compatible (`s3`) and read-only HTTP(S); `filesystem` and `ephemeral` remain
  small local/test providers that share one on-disk implementation. `ephemeral`
  keeps its objects in a private temporary directory under `TMPDIR`, removed on
  clean shutdown. Both keep in-flight writes in a `.pgvs3-tmp` directory at their
  root; that directory is hidden from listings and its name cannot be a key. User
  metadata is process-local for both. Keys that cannot be stored as paths are
  rejected: empty, `.` or `..` segments, NUL bytes, segments over 255 bytes, and
  keys that conflict with an existing object or prefix. PUT,
  COPY and multipart completion reject them with `400 InvalidRequest` before any
  body is acknowledged or committed, including write-back uploads. Reads return
  `404`, and `DELETE` is a no-op.
- **Cache safety across workers.** Read-through fills are fenced by the
  expected origin version, so a fill that started before a newer write-through
  write cannot overwrite the newer cache row. Write-through deletes leave a
  short-lived cache tombstone, so an in-flight fill cannot resurrect a deleted
  object either. Multipart flushes hold database leases and publish with token
  guards; a stale retry or fill is discarded rather than reviving an older
  object. Acknowledged write-back objects are durable in the WAL-logged
  `s3w_<namespace>` until the origin copy commits and normal retention applies;
  there is no full external cache-invalidation channel. Gateways running with
   `--read-only` serve durable `s3c_` cache and `s3w_` write-back objects,
   never fill cache rows, and never start a worker.
- **Shared range metadata and renewable fill ownership.** Range-only hits use
  logged PostgreSQL parent metadata instead of an origin HEAD on every request.
  Metadata respects `--cache-ttl`, never represents a body, and is invalidated
  by writes/deletes/flushes. Read-only workers can consume replicated metadata
  and range blocks. Fill leases renew during slow downloads, and publication
  checks/locks the live database owner as well as the origin version.
- **Immutable files, atomic publish.** Writes stream into `chunks` via
  binary `COPY` under a new `file_id` from a database sequence and publish in
  the same transaction. Interrupted writes leave nothing visible.
- **Cut-through streaming reads.** Single-file reads up to 64 MiB run as one
  query; larger and multipart objects stream in 8 MiB parallel pieces under one
  repeatable-read snapshot so concurrent overwrites cannot corrupt an in-flight
  read.
- **No authoritative local state.** Gateways keep no local object or metadata
  cache and no locally authoritative namespace, file-ID, or lease state;
  PostgreSQL `shared_buffers` is the shared cache. Committed writes are visible
  immediately to other writable gateways with strict MVCC consistency.
  `--read-only` gateways on PostgreSQL streaming standbys serve local-AZ reads
  subject to replication lag. One writable gateway holds the maintenance
  advisory lock; if it exits, another acquires it automatically on a later
  tick.
- **Built-in garbage collection.** Overwritten and deleted files stay readable
  for `--reap-grace` (default 10 minutes), then the advisory-lock leader
  removes dead chunk rows in bounded passes. No `pg_cron` extension is needed.

See [docs/performance.md](docs/performance.md) for the full storage and query
design.

## Performance at a glance

Measured with `just load` against PostgreSQL 18 in Docker (`shared_buffers=1GB`)
on a 16-core host (6-vCPU VM numbers in [docs/benchmarks.md](docs/benchmarks.md)):

- **Latency (1 worker):** **`0.072 ms` (72 µs)** p50 for 4 KiB RESP `GET`
  (`0.17 ms` for S3 `GET`, `0.14 ms` for `HEAD`), and **`0.18 ms`** for durable
  4 KiB RESP `SET` (`0.81 ms` for S3 `PUT`, `synchronous_commit = on`).
- **RESP cache throughput (4 KiB, 64 workers):** **97.0k `GET`/s** unpipelined
  and **460.4k `GET`/s (1.76 GiB/s)** at pipeline 16; **48.2k durable `SET`/s**
  unpipelined. The older **213.6k durable `SET`/s** pipeline-16 figure predates
  the redcon move: redcon returns one command per 4 KiB read, so large
  pipelined `SET`s may be processed individually rather than coalesced.
- **S3 throughput (4 KiB):** **35.8k `GET`/s**, **26.1k `HEAD`/s**, and
  **20.9k durable `PUT`/s**.
- **Large-object bandwidth (64 MiB S3):** **4.17 GiB/s (4,275 MiB/s)** read
  (16 workers) and **529 MiB/s** write (8 workers).
- **Cache tier (64 MiB, filesystem origin, warm):** **4.0 GiB/s** read and
  **488 MiB/s** write through the proxy.

The old SQS queue numbers are not comparable after the OJS v1 rewrite. Run
`just load` with `-op ojs-enqueue` / `-op ojs-fetch` to measure the new path.

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
  upload; stores a single-object checksum for GET/HEAD/`GetObjectAttributes`
  and per-part checksums on multipart uploads.
- **Versioning:** not supported. `ListObjectVersions` is a read-only
  compatibility shim that reports each current object as the `null` version so
  cleanup tools can empty a bucket; it does not create versions or delete
  markers.
- **Plain HTTP & static sites:** `GET /{bucket}/{key}` works with any HTTP
  client or browser. Trailing slashes resolve to `index.html`, plain
  `GET /{bucket}` serves `/{bucket}/index.html` when present, and `Content-Type`
  falls back to the filename extension.
- **OJS v1 core (OpenJobSpec):** implements the HTTP lifecycle subset:
  `POST /ojs/v1/jobs` and `/ojs/v1/jobs/batch`, `GET`/`DELETE
  /ojs/v1/jobs/{id}`, `POST /ojs/v1/workers/{fetch,ack,nack,heartbeat}`,
  `GET /ojs/v1/queues`, `GET /ojs/v1/queues/{name}/stats`,
  `GET /ojs/v1/health` and `GET /ojs/manifest`. Jobs use the OJS states
  (`scheduled`, `available`, `active`, `retryable`, `completed`, `cancelled`,
  `discarded`), higher `priority` first, `FOR UPDATE SKIP LOCKED` leases,
  scheduled delivery, exponential retry backoff and a short long poll.
  `lease_token` is a documented extension required on `ack`/`nack`/`heartbeat`
  so a superseded worker gets `409` instead of completing another lease. Fetch
  accepts at most 64 queues. It resolves the requested queues against the
  catalog before opening one claim transaction, deliberately avoiding
  per-queue savepoints/subtransactions; `SKIP LOCKED` therefore stays on the
  plain-row-lock path instead of contending on MultiXact LWLocks. Expired
  leases, due scheduled jobs and retries are claimed lazily without per-fetch
  maintenance updates; terminal jobs are kept
  for `--job-retention` (default `24h`). Workflows, cron, SSE/WebSocket events, schema registry, job-level
  dead-letter APIs, pause/resume and full OJS conformance are intentionally out
  of scope. Old SQS `messages` rows are ignored by the current schema; drop the
  queue namespace with `--force` to reclaim them.
- **Queue + stream:** every `s3q_<name>` namespace also exposes a PgQue-shaped
  append-only stream under `/ojs/v1/queues/{name}/...`: batch append,
  independent fan-out subscriptions, cooperative members, snapshot-delimited
  batches, batch ack, per-message nack with retry, and a dead-letter queue.
  Events rotate through three `TRUNCATE` partitions under consumer-pinned
  retention, so retention does not leave dead tuples. Jobs and streams share the
  same schema, catalog, quota, backup, `DROP SCHEMA` and maintenance pass; a
  queue can serve both. See [docs/queue-stream.md](docs/queue-stream.md).
- **S3 cache tier:** read-through and write-through/write-back
  `GET`/`HEAD`/`PUT`/`CopyObject`/multipart uploads/`DELETE`/`DeleteObjects`/
  `ListObjectsV2` in front of an origin (`X-Cache: HIT|MISS`), with TTL
  revalidation, idle eviction, optional range caching, and a direct origin
  listing merged with pending write-back objects. Fills are version-fenced,
  deletes leave expiring tombstones, multipart uses a leased outbox, and cache
  publishes are token-guarded, so the tier is safe across workers. See
  [docs/operations.md](docs/operations.md#cache-tier-in-front-of-another-object-store).
- **RESP (Redis/Valkey) cache protocol on `:8014`:** supports `GET`, `SET`
  (`EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`/`GET`), `SETNX`, `SETEX`,
  `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`, `EXISTS`,
  `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `GETRANGE`, `APPEND`,
  `SETRANGE`, `SCAN`, `TYPE`, `SELECT`,
  `FLUSHDB`, `DBSIZE`, `KEYS`, `INFO`, and `HELLO 2`. RESP3 is not supported:
  `HELLO 3` returns `NOPROTO`, and clients that default to RESP3 (for example
  recent `redis-py` releases) must be configured with `protocol=2`. Pipelined
  `GET` bursts coalesce into one `Cache.MGet`; consecutive `SET`s coalesce into
  `Cache.MSet` when redcon delivers them in the same read batch. `EXPIRE`
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
| `--url` | `PGVS3_URL` | `postgres://localhost/pgvs3` | PostgreSQL connection URL (writer endpoint for writable workers; standby endpoint with `--read-only`). Use `sslmode` for DB TLS. |
| `--addr` | `PGVS3_ADDR` | `0.0.0.0:8014` | Listen address for S3, OJS, HTTP, `/healthz`, `/readyz`, and `/metrics`. |
| `--pool-max` | `PGVS3_POOL_MAX` | `64` | Max PostgreSQL connections per gateway worker (main pool; see `--spool-dir` and the chunk pool note). |
| `--spool-dir` | `PGVS3_SPOOL_DIR` | unset | Scratch directory for cache fills larger than 8 MiB; empty uses the system temp directory. Put this on local scratch, not a small tmpfs. |
| `--spool-max-bytes` | `PGVS3_SPOOL_MAX_BYTES` | `8589934592` (8 GiB) | Process-wide disk spool payload byte budget shared by fills, foreground writes and pending uploads. Set below available scratch capacity and above the largest object to spool. Busy admission returns 503; pending work remains logged and retries. |
| `--db-work-mem` | `PGVS3_DB_WORK_MEM` | `16MB` | PostgreSQL `work_mem` per connection; keep conservative and raise for known heavy reads. |
| `--db-statement-timeout` | `PGVS3_DB_STATEMENT_TIMEOUT` | `5m` | PostgreSQL statement timeout; `0` disables. |
| `--db-lock-timeout` | `PGVS3_DB_LOCK_TIMEOUT` | `5s` | PostgreSQL lock timeout; `0` disables. |
| `--db-idle-tx-timeout` | `PGVS3_DB_IDLE_TX_TIMEOUT` | `1m` | PostgreSQL idle-in-transaction timeout; `0` disables. |
| `--db-max-conn-lifetime` | `PGVS3_DB_MAX_CONN_LIFETIME` | `1h` | Recycle PostgreSQL connections after this lifetime; `0` disables. |
| `--db-max-conn-lifetime-jitter` | `PGVS3_DB_MAX_CONN_LIFETIME_JITTER` | `5m` | Randomize connection recycling. |
| `--max-db-bytes` | `PGVS3_MAX_DB_BYTES` | `0` | Reject writes when `pg_database_size` exceeds this (0 = unlimited). |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m` | How long overwritten/deleted files stay readable before GC. |
| `--read-only` | `PGVS3_READ_ONLY` | `false` | Reject writes; skip DDL, reaping, cache fills, and origin retry workers (for PostgreSQL standbys); durable write-back objects remain readable. |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | Log verbosity (`debug`, `info`, `warn`, `error`). |
| `--pprof-addr` | `PGVS3_PPROF_ADDR` | unset | Optional loopback address for Go `net/http/pprof`, e.g. `127.0.0.1:6060`. |
| `--max-conns` | `PGVS3_MAX_CONNS` | `4096` | Concurrent client connections before shedding load. |
| `--http-read-timeout` | `PGVS3_HTTP_READ_TIMEOUT` | `30m` | Absolute HTTP read deadline including request bodies (`0` disables). |
| `--http-write-timeout` | `PGVS3_HTTP_WRITE_TIMEOUT` | `30m` | Absolute HTTP response write deadline (`0` disables). |
| `--origin-config` | `PGVS3_ORIGIN_CONFIG` | unset | S3/HTTP/filesystem origin JSON; enables the cache tier. |
| `--cache-ttl` | `PGVS3_CACHE_TTL` | `1m` | Origin revalidation interval (`0` = always revalidate). |
| `--cache-write-back` | `PGVS3_CACHE_WRITE_BACK` | `true` | Acknowledge large unconditional writes after the WAL-logged write-back/outbox transaction; the origin upload runs asynchronously. Conditional writes stay synchronous. |
| `--simulate-latency` | `PGVS3_SIMULATE_LATENCY` | `0` | Benchmark-only one-way HTTP delay added to S3 requests; the response side adds the same delay, so `20ms` is roughly a 40ms RTT. |
| `--simulate-rps` | `PGVS3_SIMULATE_RPS` | `0` | Benchmark-only maximum S3 requests per second; `0` disables. Health, readiness and metrics are exempt. |
| `--simulate-bandwidth` | `PGVS3_SIMULATE_BANDWIDTH` | `0` | Benchmark-only per-request bandwidth limit in bytes/sec for request and response bodies; `0` disables. |
| `--cache-retention` | `PGVS3_CACHE_RETENTION` | `24h` | Evict idle cached objects after this period. |
| `--cache-namespace` | `PGVS3_CACHE_NAMESPACE` | `origin` | Cache namespace holding origin objects. |
| `--fill-concurrency` | `PGVS3_FILL_CONCURRENCY` | `8` | Process-wide concurrent origin cache fills and pending-upload workers. |
| `--range-cache-chunk` | `PGVS3_RANGE_CACHE_CHUNK` | `0` | Cache aligned ranges of large objects at this byte size; 0 disables. |
| `--s3-concurrency` | `PGVS3_S3_CONCURRENCY` | `32` | Concurrent in-flight S3 requests; excess requests get `503 SlowDown` with `Retry-After`. |
| `--queue-concurrency` | `PGVS3_QUEUE_CONCURRENCY` | `16` | Concurrent in-flight OJS requests. |
| `--resp-concurrency` | `PGVS3_RESP_CONCURRENCY` | `32` | Concurrent in-flight RESP batches. |
| `--job-retention` | `PGVS3_JOB_RETENTION` | `24h` | Keep completed, cancelled and discarded OJS jobs this long before maintenance removes them (`0` = forever). |

Subcommands:

- `pgvs3 serve` — run the S3/OJS/HTTP gateway (default when no subcommand is given).
- `pgvs3 stat [--kind bucket|cache|queue] [--name N] [--json]` — namespace
  usage, logical vs physical size and queued garbage.
- `pgvs3 clone [--kind bucket|cache] <src> <dst>` — clone inside PostgreSQL.
- `pgvs3 drop [--kind bucket|cache|queue] [--force] <name>` — drop a namespace
  schema (`DROP SCHEMA CASCADE`).
- `pgvs3 pending [--namespace N] list|retry` — inspect write-back rows or
  revive dead-lettered rows after repairing the origin.
- `pgvs3 origin [--bucket B] [--cache-namespace NS] set|clear|list [origin.json]`
  — persist or remove global or per-bucket origin cache configs inside
  PostgreSQL (gateways discover them automatically with no external files).
- `pgvs3 doctor` — verify PostgreSQL 18+, schema layout v25, table settings, per-queue stream functions,
  namespace drift, the 100-namespace supported scale, and reaper safety.
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

## HTTP origins

Read-only HTTP(S) origins are useful for static CDNs and Source Cooperative-
style data hosts. Configure a base URL and strip the cache bucket prefix:

```json
{
  "type": "http",
  "config": {
    "base_url": "https://data.source.coop",
    "strip_bucket": true,
    "read_only": true,
    "headers": { "User-Agent": "pgvs3" }
  }
}
```

Add `--range-cache-chunk 1048576` to `pgvs3 serve` (or the matching
`pgvs3 origin ... set` flag) to cache aligned chunks of large Parquet/PMTiles
files. HTTP origins do not support `ListObjectsV2`; use an S3 origin when
listing is required.

## Limits to keep in mind

| Limit | Behavior |
| --- | --- |
| **Namespaces** | At most **100 schemas per database**, counting buckets, cache namespaces, and queues. More than that is outside the supported envelope; `pgvs3 doctor` reports `over_supported_scale` and exits non-zero. |
| **Bucket capacity** | Each bucket's `chunks` table is a single PostgreSQL relation capped at 32 TiB (`MaxBlockNumber × 8 KB`, ~31.7 TiB of object payload); ~17.4 TB per object. |
| **Writer model** | One DuckLake writer per bucket and many readers. Scale reads with additional DuckDB workers, writable gateway workers, and read-only gateways on PostgreSQL standbys. |
| **Disk reclamation** | Dropping a bucket (`pgvs3 drop --force <bucket>`) returns disk space to the OS via `DROP SCHEMA CASCADE`. Inside a live bucket, deleted pages are reused after `--reap-grace` and autovacuum; returning space without dropping requires `VACUUM FULL` or `pg_repack`. |
| **Cache authority** | The origin is the source of truth. pgvs3 has no full external invalidation channel; origin changes made outside pgvs3 may be served stale until TTL revalidation. |
| **Delete tombstones** | A write-through delete leaves a thirty-minute tombstone so an in-flight fill cannot resurrect the object. An object recreated directly at the origin may remain hidden through the cache until the tombstone expires; recreating it through pgvs3 replaces it immediately. |

See [docs/operations.md](docs/operations.md) for topology and backup guidance and
[docs/design.md](docs/design.md) for the multi-worker safety invariants.

## Testing and documentation

```sh
just test       # Go unit tests
just race       # unit tests with race detector
just integration # store/queue/cache tests against PostgreSQL 18 and a self-hosted pgvs3 S3 origin (override with PGVS3_TEST_S3_* URLs)
just s3t        # fast s3t allow-list (Go port of ceph/s3-tests) against a running gateway
just helm-kind  # install the chart on a throwaway kind cluster under restricted Pod Security
just contract   # end-to-end S3, OJS, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, and DuckLake
just load       # direct S3 load generator benchmark
just bench      # DuckDB httpfs + DuckLake benchmark suite
```

Further reading:

- [docs/design.md](docs/design.md) — project goals, trust model, multi-worker
  safety invariants, and scope boundaries
- [docs/performance.md](docs/performance.md) — storage layout, read/write paths,
  and DuckDB tuning
- [docs/benchmarks.md](docs/benchmarks.md) — full benchmark tables and S3
  Express One Zone comparison
- [docs/operations.md](docs/operations.md) — deployment topology, failover,
  probes, backups, and cache-tier operations
- [docs/partitioning.md](docs/partitioning.md) — why `s3p.chunks` is unpartitioned
- [docs/proposals/hardening-options.md](docs/proposals/hardening-options.md) —
  open compatibility and hardening options under review
- [docs/proposals/high-value-improvements.md](docs/proposals/high-value-improvements.md)
  — prioritized correctness, simplification and performance findings
- [docs/proposals/modernization-review.md](docs/proposals/modernization-review.md)
  — sourced capability-by-capability alternatives and modernization roadmap
- [docs/modernization-results.md](docs/modernization-results.md) — v24 changes,
  before/after S3/cache measurements and origin-client comparisons
- [docs/queue-stream.md](docs/queue-stream.md) — the consolidated OJS
  job/stream model and PgQue feature mapping
- [CONTRIBUTING.md](CONTRIBUTING.md) — development workflow and invariants

## License

Apache-2.0. See [LICENSE](LICENSE).
