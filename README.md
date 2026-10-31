# pgvs3 — PostgreSQL as the object data plane

**Durable object storage and shared object caching, served through stateless
S3/HTTP gateways.** PostgreSQL stores the bytes, not just a metadata index.
pgvs3 uses its transactions, MVCC snapshots, WAL, replication and locks instead
of adding separate durability, cache-coherence and coordination services.

The primary workload is **10 MiB–1 GiB Parquet objects for DuckDB and
[DuckLake](https://ducklake.select/)** on trusted networks. Use PostgreSQL as the
object store, or as a shared read cache and durable write-back stage in front of
an origin. Gateway processes are replaceable; authoritative state is not on
their local disks.

RESP2 cache commands and OJS jobs/streams are bounded companion interfaces for
co-located services, not a goal of replacing every Redis or queue feature.
They share the PostgreSQL engine and operating model, not one universal data
model or a cross-protocol transaction.

> **Trusted networks only — permanently.** pgvs3 does not authenticate or
> authorize clients, terminate inbound TLS, or validate S3 signatures or
> credentials. Authentication is not part of the design and is not a future
> goal. Run it on a loopback interface, private VPC/VLAN, or behind a service
> mesh that enforces access control and TLS, and treat `:8014` like a database
> port. Presigned URLs are rejected because a scoped link would otherwise
> grant unscoped access. See [docs/design.md](docs/design.md).

**Status: pre-alpha.** Targets are Aurora PostgreSQL 18.3+ or
community/self-managed PostgreSQL 18+. The PostgreSQL schema is one embedded baseline
(`internal/store/schema.sql`), currently layout **v25**, including shared origin
parent metadata and PostgreSQL 18 publication SQL, with no in-place upgrade path.
A layout change requires a fresh database and a re-import; there are no stable
on-disk compatibility guarantees yet.

## Start here

- **Choose a role:** [PostgreSQL object store or origin-backed cache/stage](#choose-a-storage-role).
- **Try the main workload:** [Quick start](#quick-start), then
  [write and query Parquet with DuckDB](#use-from-duckdb-20).
- **Read next:** [docs/design.md](docs/design.md) for the model,
  [docs/operations.md](docs/operations.md) to deploy and operate it, and
  [docs/roadmap.md](docs/roadmap.md) for the current priorities.

## Choose a storage role

| Role | Where the object bytes live | Why use it |
| --- | --- | --- |
| **PostgreSQL object store** | Logged bucket tables in PostgreSQL; no origin required | Keep lake objects within the PostgreSQL cluster's storage, backup and replication boundary. |
| **Origin-backed shared cache/stage** | Committed objects at the origin; logged PostgreSQL read fills and durable pending writes | Share warm bytes and fill ownership across gateways, and acknowledge eligible writes after durable PostgreSQL publication before origin upload. |

The second role is not a disposable local cache: **a pending acknowledged body
may exist only in PostgreSQL**. Cache TTL does not expire those pending bytes.
Both roles use the PostgreSQL bucket catalog. See the
[authority and acknowledgement contracts](docs/design.md#authority-and-acknowledgement-contracts).

### Good fit—and the trade-off

- You already operate PostgreSQL and want a bounded, private object data plane
  for analytical files and nearby services.
- You value shared MVCC visibility and one cluster's operational machinery more
  than independently scaling an object store and cache fleet.
- You accept that object writes **and read-cache fills** consume PostgreSQL disk,
  WAL, I/O and replication bandwidth. Those resources compete with other cluster
  work; fewer services does not mean free storage or automatic cost savings.

This is not a public multi-tenant S3 service, an unlimited archive, or a claim of
full S3/Redis/OJS replacement. No sharding, multi-primary layer or local-disk
authority is planned. Existing companion interfaces remain supported within
their documented bounds.

## Project goals and operating envelope

| Dimension | Envelope |
| --- | --- |
| **Namespaces** | At most **100 namespaces per database**, counting bucket, read-cache, write-back and queue schemas. This is a bounded single-cluster envelope, not a schema-per-tenant design. |
| **Gateway fleet** | Multiple stateless `pgvs3` workers behind a load balancer; all share one PostgreSQL cluster and coordinate through the database. |
| **PostgreSQL topology** | One primary (writer) plus streaming standbys. A single PostgreSQL database, not sharded or multi-primary. |
| **Reads** | Writable gateways read from the primary. Read-only gateways (`--read-only`) can serve reads from standbys, subject to replication lag. |
| **Writes** | Sent to writable gateways connected to the writer endpoint. DuckLake still has one writer per bucket. |
| **Coordination** | `s3p.namespaces` catalog, advisory locks, database leases, conditional/version-fenced writes, and database sequences. No local process state is authoritative. |
| **Backups** | Physical base backup plus WAL archiving/PITR for production clusters; logical `pg_dump` for export or small deployments. Origin data, when using the cache tier, is backed up separately. |
| **Security** | Trusted network only: no authentication, authorization, TLS termination, audit log, or per-tenant isolation. |
| **Layout** | Current layout is **v25**. There is no in-place migration path; a layout change needs a fresh database and re-import. |

## Quick start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL
18+ instance).

For a disposable walkthrough instead of a persistent dev database, run
`just analytics`. It executes the SQL below in both storage roles, checks
cross-gateway queries/shared hits and shows a PostgreSQL-only pending body reach
the origin. It needs no RESP/OJS setup and retains an `analytics.json` report.

```sh
just setup
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' \
  mise exec go -- go run ./cmd/pgvs3 serve --addr 127.0.0.1:8014
```

In another terminal, create a bucket and write an object. A write to a missing
or deleted bucket returns `NoSuchBucket` instead of recreating it:

```sh
# CreateBucket, then S3 / plain HTTP
curl -fsS -X PUT http://127.0.0.1:8014/lake
curl -fsS -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl -fsS http://127.0.0.1:8014/lake/README.md
```

### Use from DuckDB 2.0+

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, KEY_ID 'test', SECRET 'test',
    REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
COPY (
    SELECT range AS id, (range % 100)::INTEGER AS group_id,
           md5(range::VARCHAR) AS payload
    FROM range(1000000)
) TO 's3://lake/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT group_id, count(*) AS rows
FROM read_parquet('s3://lake/events.parquet')
GROUP BY group_id ORDER BY group_id LIMIT 5;
```

Each returned group has 10,000 rows. The dummy credentials satisfy the client;
they do not enable authentication. This is a walkthrough, not a performance
claim; use the [paired runner](docs/benchmarks.md#paired-acceptance) for acceptance.

### Use with DuckLake (catalog and objects on one cluster)

In the same DuckDB session, using the `lake` bucket from the quick start:

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CALL lake.set_option('data_inlining_row_limit', 0);
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

The catalog database here is `ducklake_catalog`; pgvs3 uses `pgvs3_bench`.
`just dev-db` creates both. They share a PostgreSQL cluster, but DuckLake catalog
transactions and pgvs3 object publications are **separate transactions**, not a
distributed transaction made atomic by co-location.

### Experimental native DuckDB storage with Differential

`just differential` compares pinned [DuckDB Differential](https://github.com/tobilg/duckdb-differential)
snapshot/synchronous-WAL profiles against DuckLake in the same disposable setup.
Start smaller with `just differential --differential-mib 10,100 --passes 3`.

Differential has a client-local repository; an S3 data path alone does not make
each commit PostgreSQL-backed. Its compact call reclaims local SQLite metadata,
not the entire storage pipeline. It also requires an externally fenced single
writer: the negative recovery probe demonstrates that snapshot-head conditions
are not a global WAL-writer fence. See [setup, boundaries and benchmark method](docs/differential.md).
This is additive; DuckLake and the standard storage roles remain unchanged.
See the [measured findings](docs/differential-results.md), including the large
synchronous-commit limit and separate gateway admission results.
S3 bursts now use a [bounded admission wait](docs/operations.md#admission-control)
before falling back to overload 503s; scratch-byte admission remains fail-fast.

### SlateDB-backed DuckDB object storage

`just object-storage` compares the pinned `duckdb_object_storage` 0.2.0 community
extension on DuckDB 2.0 with DuckLake, including default/lean cache and thread
budgets and separately measured sealed-data reads. Start with
`just object-storage --object-storage-mib 10,100 --passes 3`.
See [setup, measurement and endpoint-role limits](docs/object-storage.md).
The [measured results](docs/object-storage-results.md) separate the useful
read-mostly sealing optimization from the lean configuration's retained misses.
SQL read-only ObjFS clients still write managed retention checkpoints, so they
must use a writable object endpoint; gateway read-only protections remain intact.

## How it works

```text
DuckDB / DuckLake / S3 and HTTP clients
                    |
          replaceable pgvs3 gateways ------- optional origin I/O
                    |                              |
          PostgreSQL data database          S3 / HTTP origin
                    |
              WAL replication
                    |
          PostgreSQL streaming standbys
```

- **PostgreSQL-native byte layout.** Small objects are inline; larger objects
  use logical 8,120-byte blocks sized for one PostgreSQL page without TOAST.
  Each block uses Zstd only when it shrinks, avoiding expansion of compressed
  Parquet. Byte offsets remain predictable for range reads.
- **Immutable bytes, transactional publication.** Binary COPY ingests new file
  IDs; one transaction publishes the object pointer and retires old ownership.
  Readers use MVCC snapshots rather than gateway invalidation messages.
- **Specialized I/O.** A metadata pool favors B-tree lookups; a separate chunk
  pool uses PostgreSQL 18 bitmap read-ahead. Reads stream with bounded buffering,
  including parallel pieces under a shared snapshot for large objects.
- **Shared origin orchestration.** Version/lease-fenced fills, delete tombstones
  and a durable outbox coordinate gateways through PostgreSQL. Process-local
  buffers, admission limits and scratch files are resource controls, not copies
  of record. A read-only gateway can consume replayed rows but does not fill or
  flush them.
- **Bounded lifecycle.** Catalog-controlled namespaces, explicit bucket creation,
  one database-elected maintenance leader and bounded reaping keep the system
  inside its single-cluster envelope.

| Namespace | Purpose |
| --- | --- |
| `s3p` | Shared catalog, sequences, fill leases and pending-upload control rows |
| `s3b_<name>` | PostgreSQL object-store buckets |
| `s3c_<name>` | Logged read fills, range metadata/blocks and RESP/cache values |
| `s3w_<name>` | Logged write-back bodies and durable origin-write cache |
| `s3q_<name>` | Companion job and stream state |

For publication/acknowledgement boundaries see [design.md](docs/design.md).
For the byte layout and query paths see [performance.md](docs/performance.md).
For origin modes, provider limits and recovery see
[operations.md](docs/operations.md#cache-tier-in-front-of-another-object-store).

## Performance evidence

The acceptance workload is 10 MiB–1 GiB objects: warm/cold GET, footer and aligned
ranges, metadata, native publication and durable write-back acknowledgement.
Changes are compared per path against a saved binary, with a 20% regression
budget and the already documented small-origin-hit catalog-check exception.

`just load --capacity --replication async` records storage/WAL, actual standby
replay and gateway resource observations without claiming a performance comparison.
See [capacity evidence](docs/benchmarks.md#capacity-evidence) for scope and caveats.

Use [the paired runner](docs/benchmarks.md#paired-acceptance), not a single peak
requests-per-second number. [Recorded comparisons](docs/modernization-results.md)
state the host, payloads, sample counts and limits; they are local measurements,
not cloud-service latency, cost or durability equivalence. Historical small-value
and client benchmarks remain in [benchmarks.md](docs/benchmarks.md).

## S3 and HTTP support

- **Supported S3 operations:** `GET` (byte ranges and conditional headers),
  `HEAD`, `PUT`, `DELETE`, `DeleteObjects`, `ListObjectsV2`, `ListBuckets`,
  `CreateBucket`, `HeadBucket`, `DeleteBucket`, multipart uploads
  (`CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`,
  `ListMultipartUploads`), `CopyObject`, `GetObjectAttributes`, and
  `partNumber` on `GET`/`HEAD`.
- **Native PostgreSQL copies:** Same-bucket `CopyObject` and whole-object
  `UploadPartCopy` share immutable `file_id`s without copying chunk rows;
  cross-bucket `CopyObject` and whole-bucket clones (`PUT /{dst}` with
  `x-amz-copy-source: /{src}`) copy chunk rows inside PostgreSQL without
  streaming bytes through the gateway.
- **Whole-bucket drop:** `DELETE /{bucket}?force=true` (or
  `pgvs3 drop --force <bucket>`) drops `s3b_<bucket>` with `DROP SCHEMA CASCADE`
  rather than walking every object. Table files are unlinked after commit;
  acquiring the required locks can still take time.
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
- **S3 cache tier:** read-through and write-through/write-back
  `GET`/`HEAD`/`PUT`/`CopyObject`/multipart uploads/`DELETE`/`DeleteObjects`/
  `ListObjectsV2` in front of an origin (`X-Cache: HIT|MISS`), with TTL
  revalidation, idle eviction, optional range caching, and an origin listing
  merged with pending write-back objects. See
  [origin modes and limits](docs/operations.md#cache-modes).

## Origin-backed storage

For production origins, use S3-compatible storage (reads and writes) or HTTP(S)
(read-only by default). Persist global/per-bucket settings with `pgvs3 origin`;
see the [S3 configuration](docs/operations.md#cache-tier-in-front-of-another-object-store).
`filesystem` and temporary-directory `ephemeral` are local/test providers, not
production durability backends.

For a read-only HTTP data host, save this as `origin.json`:

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

With `PGVS3_URL` set to the gateway's data database, attach it to a bucket:

```sh
pgvs3 origin --url "$PGVS3_URL" --bucket edge --cache-namespace origin \
  --range-cache-chunk 1048576 set origin.json
```

This creates the bucket catalog row and enables aligned range caching. HTTP
origins do not support `ListObjectsV2`; use an S3 origin when listing is required.
Provider key/metadata limits are in [operations.md](docs/operations.md#limits-and-failure-behavior).

## Companion interfaces

These use the same listener and PostgreSQL engine but are optional for the
object-storage path. RESP cache namespaces and OJS queues are created on first
use. The connection mux detects RESP2; HTTP routes distinguish S3 from OJS.

```sh
# Redis-compatible cache subset (RESP2)
redis-cli -p 8014 SET greeting "hello from resp" EX 60
curl -fsS http://127.0.0.1:8014/_cache/cache/greeting
redis-cli -p 8014 GET s3://lake/README.md     # explicit cross-namespace read

# OJS (OpenJobSpec v1 core lifecycle)
curl -fsS -X POST http://127.0.0.1:8014/ojs/v1/jobs \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"type":"email.send","args":["user@example.com"],"options":{"queue":"jobs"}}'
curl -fsS -X POST http://127.0.0.1:8014/ojs/v1/workers/fetch \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"queues":["jobs"],"count":1,"worker_id":"worker-1","visibility_timeout_ms":30000}'
```

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
  of scope.
- **Queue + stream:** every `s3q_<name>` namespace also exposes a PgQue-shaped
  append-only stream under `/ojs/v1/queues/{name}/...`: batch append,
  independent fan-out subscriptions, cooperative members, snapshot-delimited
  batches, batch ack, per-message nack with retry, and a dead-letter queue.
  Events rotate through three `TRUNCATE` partitions under consumer-pinned
  retention, so retention does not leave dead tuples. Jobs and streams share the
  same schema, catalog, quota, backup, `DROP SCHEMA` and maintenance pass; a
  queue can serve both. See [docs/queue-stream.md](docs/queue-stream.md).
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

## Scope boundaries

Authentication/authorization, SigV4 validation, presigned URLs, gateway TLS,
audit logging and per-tenant isolation are not roadmap items. Neither are S3
IAM/bucket policies, versioning, object tags, Object Lock, ACLs, lifecycle rules,
S3 bucket replication, event notifications, SSE-C/SSE-KMS/DSSE-KMS or multipart
`FULL_OBJECT` checksums. **PostgreSQL WAL replication is part of the architecture;
S3 bucket replication is not.** See [the full boundaries](docs/design.md#scope-boundaries).

## Configuration and CLI

The `serve` flags below map to environment variables; administrative flags such
as `drop --force` do not. Run `pgvs3 <command> --help` for command-specific flags.

| Flag | Environment variable | Default | Description |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | `postgres://localhost/pgvs3` | PostgreSQL connection URL (writer endpoint for writable workers; standby endpoint with `--read-only`). Use `sslmode` for DB TLS. |
| `--addr` | `PGVS3_ADDR` | `0.0.0.0:8014` | Listen address for S3/HTTP/RESP2/OJS, `/healthz`, `/readyz` and `/metrics`. |
| `--pool-max` | `PGVS3_POOL_MAX` | `64` | Main-pool connections per gateway. A separate chunk pool adds up to `min(pool-max, 64)`; size connections for the whole fleet. |
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
| `--origin-config` | `PGVS3_ORIGIN_CONFIG` | unset | Process-local fallback origin JSON; persisted `pgvs3 origin` settings are discovered across gateways. |
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

- `pgvs3 serve` — run the S3/HTTP/RESP2/OJS gateway (default when no subcommand is given).
- `pgvs3 stat [--kind bucket|cache|queue] [--name N] [--json]` — namespace
  usage, logical vs physical size and queued garbage.
- `pgvs3 clone [--kind bucket|cache] <src> <dst>` — clone inside PostgreSQL.
- `pgvs3 drop [--kind bucket|cache|queue] [--force] <name>` — drop a namespace
  schema (`DROP SCHEMA CASCADE`).
- `pgvs3 pending [--namespace N] list|retry` — inspect write-back rows or
  revive dead-lettered rows after repairing the origin.
- `pgvs3 origin [--bucket B] [--cache-namespace NS] set|clear|list [origin.json]`
  — persist or remove global or per-bucket origin cache configs inside
  PostgreSQL (gateways discover them automatically with no external files). A
  global origin serves only buckets that exist in the catalog.
- `pgvs3 doctor` — verify PostgreSQL 18+, layout v25, table settings, exact
  global/object/stream function contracts, namespace drift, supported scale and
  reaper safety.
- `pgvs3 reap [--all]` — reconcile schemas and run garbage collection immediately.
- `pgvs3 health` — poll `/readyz` for container health checks.

Build the image with `ko` and package the Helm chart from this checkout:

```sh
just image  # local ko.local image
just chart  # package under .tmp/pgvs3/
```

Published artifacts use `ghcr.io/adonm/pgvs3` and
`oci://ghcr.io/adonm/charts/pgvs3`. Pin compatible image/chart versions rather
than assuming an older release matches `main`'s layout. Deployment and probes
are described in [operations.md](docs/operations.md).

## Limits to keep in mind

| Limit | Behavior |
| --- | --- |
| **Namespaces** | At most **100 namespaces per database**, counting bucket, read-cache, write-back and queue schemas. `EnsureNamespace` enforces it; `doctor` also reports an over-scale database. |
| **Bucket capacity** | Each bucket's `chunks` table has PostgreSQL's 32 TiB relation limit (~31.7 TiB of payload); ~17.4 TB per object from chunk indexing. These are format ceilings, not measured production capacity targets. |
| **Writer model** | One DuckLake writer per bucket and many readers. Scale reads with additional DuckDB workers, writable gateway workers, and read-only gateways on PostgreSQL standbys. |
| **Disk reclamation** | Dropping a bucket unlinks its relations. In a live bucket, reaping/vacuum make dead space reusable; ordinary vacuum may truncate an empty tail but does not guarantee compaction. Use `VACUUM FULL` or `pg_repack` when compaction is required. |
| **Cache authority** | Origins own committed external objects; PostgreSQL owns pending acknowledged bodies until handoff. External origin changes may remain stale until TTL revalidation. Never discard pending bodies as a disposable cache. |
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
just analytics  # maintained README walkthrough, native + origin-backed, disposable
just interop    # rclone, restic, boto3, and DuckLake
just fault      # gateway/primary loss, real replicas, promotion and pending restore
just ci         # complete local/CI gate set, including Docker, kind and s3t
just load       # exploratory S3/RESP/OJS matrix in an isolated run directory
just load --before-bin /path/to/control --profile origin --cache-state cold --ops get,range
just bench      # DuckDB httpfs + DuckLake benchmark suite
```

Docker-backed test runs use isolated binaries and logs under `.tmp/verify/`.
They are quiet on success and retain diagnostics on failure.

Further reading:

- [docs/design.md](docs/design.md) — project goals, trust model, multi-worker
  safety invariants, and scope boundaries
- [docs/roadmap.md](docs/roadmap.md) — current ranked work and acceptance criteria
- [docs/data-plane-results.md](docs/data-plane-results.md) — replica/recovery proof,
  the maintained analytical scenario and capacity ledger
- [docs/performance.md](docs/performance.md) — storage layout, read/write paths,
  and DuckDB tuning
- [docs/benchmarks.md](docs/benchmarks.md) — canonical paired runner and historical
  benchmark snapshots
- [docs/operations.md](docs/operations.md) — deployment topology, failover,
  probes, backups, and cache-tier operations
- [docs/partitioning.md](docs/partitioning.md) — why each bucket's `chunks` table
  is unpartitioned
- [docs/modernization-results.md](docs/modernization-results.md) — completed
  v24/v25 work, per-path measurements and remaining evidence limits
- [docs/queue-stream.md](docs/queue-stream.md) — the consolidated OJS
  job/stream model and PgQue feature mapping
- [CONTRIBUTING.md](CONTRIBUTING.md) — development workflow and invariants

Historical analyses, **not the active backlog**:
[hardening options](docs/proposals/hardening-options.md),
[improvement findings](docs/proposals/high-value-improvements.md), and
[modernization review](docs/proposals/modernization-review.md).

## License

Apache-2.0. See [LICENSE](LICENSE).
