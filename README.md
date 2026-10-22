# pgvs3 — S3 on PostgreSQL

pgvs3 is a small, fast S3-compatible gateway that stores objects as rows in
PostgreSQL. It is built to be DuckLake's object store: DuckLake keeps its
catalog in PostgreSQL, and with pgvs3 the data files live there too, so every
DuckDB that attaches the catalog sees the same tables and the same data.

The gateway is Go, stateless over PostgreSQL, and runs as a distroless static
container built with [ko](https://ko.build/). It uses the v9 storage layout
(small objects are stored inline in `s3p.objects`).

> **No request authentication.** pgvs3 accepts signed and unsigned requests
> without validation. Bind it to a private network or put it behind a service
> mesh; never expose the S3 or web listener publicly.

**Status: early alpha, no releases.** Any storage layout change requires a fresh
database; there are no migrations. Use disposable databases only.

> Postgres is all you need. ;P — for durable bytes and metadata here; DuckDB
> does the analytics.

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or PostgreSQL 18+ with a
`--url`).

```sh
just setup
just dev-db                 # PostgreSQL 18 on 127.0.0.1:5432
go run ./cmd/pgvs3 --url 'postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' \
  serve --addr 127.0.0.1:8014 --buckets lake
```

`--buckets lake` creates the bucket idempotently at startup. Write an object
(the example key is for loopback development only):

```sh
curl -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl http://127.0.0.1:8014/lake/README.md
```

DuckDB 2.0+:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
SELECT * FROM read_text('s3://lake/README.md') LIMIT 1;
```

DuckLake 1.1 on DuckDB 2.0 (catalog and data in the same PostgreSQL):

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

## Why it is fast

The layout is deliberately narrow:

- **Small objects are one row.** Objects up to 64 KiB live in
  `s3p.objects.inline_data` with `file_id = NULL`; a small PUT is one SQL call
  and a small GET is one primary-key lookup, with no chunk rows.
- **Larger objects use one 8 KB page per chunk row.** A chunk is
  `file_id(8) + no(4) + varlena(4) + 8120` bytes plus the 24-byte tuple header.
  With `toast_tuple_target = 8160` and `STORAGE EXTERNAL`, one row fills one
  page at 99.6% occupancy with no TOAST and no detoasting on read.
- **Files are immutable.** A write streams rows under a new `file_id`; an
  overwrite publishes a new `file_id`. `file_id` is the cache version tag, so a
  warm DuckDB never revalidates bytes it already holds.
- **Reads are primary-key range queries.** A single-file read up to 64 MiB is
  one query; responses up to 8 MiB stream synchronously, while longer reads use
  a cut-through goroutine so PostgreSQL fetch overlaps socket writes. Larger
  spans/multipart reads use one repeatable-read snapshot so an overwrite cannot
  remove later rows mid-response.
- **Writes are one binary `COPY`.** The body is hashed and framed as it
  arrives; chunks and the object row commit in the same transaction. A failed
  or abandoned write publishes nothing.
- **Autovacuum is tuned for whole-file deletes** (`scale_factor = 0.01`,
  `threshold = 1000`), and the unordered chunks table uses bitmap heap scans
  with PostgreSQL 18 read-ahead. Ordered listings temporarily enable index
  scans.
- **Maintenance is in-process.** A background reaper removes at most 65,536
  chunk rows per run after a 10-minute grace, serialised across gateways with
  an advisory try-lock. pg_cron is not required.
- **The connection pool is most-recently-used first**, keeping TCP slow-start
  out of the hot path.

`docs/performance.md` maps each pgvfs technique to the implementation.

## Performance and multi-gateway caching

PostgreSQL's `shared_buffers` on `s3p.objects` is the single shared cache
across all gateways:

- Metadata and small objects (≤64 KiB) live in `s3p.objects`, indexed by
  `(bucket, key)`. A primary-key `Index Scan` in `shared_buffers` takes ~6 µs
  inside PostgreSQL (~0.47 ms p50 end-to-end over HTTP, reaching ~8.2k 4 KiB
  GET/s and ~9.2k HEAD/s at 64 workers).
- Removing `LISTEN/NOTIFY` and in-process caches eliminates commit-time
  notification lock contention in PostgreSQL: durable 4 KiB PUTs reach **3.7k/s**
  at 64 workers (`synchronous_commit = on`).
- Gateways hold **zero in-process object or metadata state**. Any number of
  gateway containers can run behind a load balancer or Kubernetes `Deployment`
  with strict MVCC consistency and zero invalidation lag; `--read-only`
  gateways can point at PostgreSQL streaming standbys for local-AZ read scale.

For context, S3 Express One Zone publishes single-digit-millisecond latency and
200k reads / 100k writes per directory bucket by default. Per-request latency
is in the same band; the aggregate ceiling is not — pgvs3 is one PostgreSQL.
Full methodology and the published-service comparison are in
[docs/benchmarks.md](docs/benchmarks.md).

## S3 surface

`GET` (ranges and conditional), `HEAD`, `PUT`, `DELETE`, `DeleteObjects`,
`ListObjectsV2`, `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`,
multipart uploads, `CopyObject`, `UploadPartCopy`, `GetObjectAttributes`,
`ListParts`, `ListMultipartUploads` and `partNumber` on GET/HEAD.

pgvs3 does not validate SigV4 or any other request authentication. Signed and
unsigned requests are both accepted; clients may keep sending credentials, but
they are ignored. The S3 listener must therefore be private or behind a service
mesh that enforces access control.

- **CopyObject** shares immutable files between object rows; GC checks every
  reference before reaping. **UploadPartCopy** of a whole single-file object is
  metadata-only; ranged and multipart-source copies stream.
- `GetObjectAttributes`, `ListParts` and `ListMultipartUploads` are implemented.
  Single PUTs accept `CRC64NVME` and `x-amz-checksum-type: FULL_OBJECT`; CRC32,
  CRC32C, SHA1, SHA256 and MD5 are also verified. Multipart part checksums are
  returned in `ObjectParts`.
- SSE-S3 (`AES256`) and `aws:kms` headers are accepted as bookkeeping. SSE-C
  and DSSE-KMS are rejected.

**Not yet implemented:** multipart `x-amz-checksum-type: FULL_OBJECT`, lifecycle
expiration, bucket policies/IAM, and access points. pgvs3 has no versioning,
object tags, Object Lock, ACLs, lifecycle transitions, replication, Event
Notifications, S3 Select, S3 website configuration/redirect rules, access logs,
Storage Lens or Transfer Acceleration. ETags are opaque; single PUTs use MD5.

## Plain HTTP and static websites

Run a separate unauthenticated HTTP listener for browsers, static sites and
non-S3 clients:

```sh
pgvs3 serve --web-addr 127.0.0.1:8080 --web-index index.html
curl http://127.0.0.1:8080/lake/index.html
curl http://127.0.0.1:8080/lake/docs/
```

Requests are path-style (`/{bucket}/{key}`); a trailing slash appends
`--web-index`. `--web-error 404.html` serves that object as the 404 page.
Content type is taken from the object metadata, with a filename-extension
fallback. Range, `HEAD`, ETag and conditional reads work. The web listener is
unauthenticated and plain HTTP: bind it to a private interface or put it behind
the service mesh.

The S3 listener is also unauthenticated: any client that can reach it can read,
write and delete objects. Bind it to a private interface or put it behind a
service mesh that enforces access control. TLS still protects transport when
configured, but it is not authentication.

## Configuration

A flag overrides the same-named environment variable.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | local dev DB | PostgreSQL URL. Remote hosts require `sslmode=require`; certificates and hostnames are verified against system CAs plus `PGVS3_DB_CA_FILE`. Loopback may use plaintext. |
| `--addr` | `PGVS3_ADDR` | `127.0.0.1:8014` | S3 listen address. |
| `--vhost-domain` | `PGVS3_VHOST_DOMAIN` | unset | Virtual-host suffix, e.g. `s3.internal`. |
| `--tls-cert`, `--tls-key` | `PGVS3_TLS_CERT`, `PGVS3_TLS_KEY` | unset | PEM files for HTTPS on non-loopback addresses. |
| `--allow-http` | `PGVS3_ALLOW_HTTP` | false | Explicitly permit plaintext HTTP outside loopback. |
| | `PGVS3_DB_CA_FILE` | unset | Extra PostgreSQL CA bundle. |
| | `PGVS3_DB_ALLOW_PLAINTEXT` | false | Permit plaintext to a remote PostgreSQL for an isolated rig. |
| `--pool-min` | `PGVS3_POOL_MIN` | 64 | Connections opened at start and kept warm. |
| `--pool-max` | `PGVS3_POOL_MAX` | 64 | Connections per gateway. Size the fleet against PostgreSQL `max_connections`. |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m` | How long deleted files stay readable. |
| `--maintenance-interval` | `PGVS3_MAINTENANCE_INTERVAL` | `1m` | Reaper and upload-expiry interval. |
| `--whole-read-max` | `PGVS3_WHOLE_READ_MAX` | 64 MiB | Largest single-file span read in one query; larger spans split into pieces. |
| `--write-commit` | `PGVS3_WRITE_COMMIT` | `on` | `synchronous_commit` for unconditional writes: `on`, `local` or `off`. Conditional writes always use `on`. |
| `--verify-reads` | `PGVS3_VERIFY_READS` | true | Re-check the object's current `file_id` on every chunk range query. Disable only for a single writer/immutable lake. |
| `--inline-max` | `PGVS3_INLINE_MAX` | 65536 | Largest object stored inline; `0` stores everything in chunks. |
| `--buckets` | `PGVS3_BUCKETS` | unset | Comma-separated buckets created idempotently at startup. |
| `--read-only` | `PGVS3_READ_ONLY` | false | Reject mutating methods; skip DDL, reaping and bucket creation. Use against a standby. |
| `--drain` | `PGVS3_DRAIN` | `5s` | Keep serving while `/readyz` returns 503, then shut down. |
| `--admin-addr` | `PGVS3_ADMIN_ADDR` | unset | Optional separate listener for `/healthz`, `/readyz` and `/metrics`. |
| `--web-addr` | `PGVS3_WEB_ADDR` | unset | Plain HTTP object/static-site listener. |
| `--web-index` | `PGVS3_WEB_INDEX` | `index.html` | Object appended to directory requests. |
| `--web-error` | `PGVS3_WEB_ERROR` | unset | Optional bucket-relative 404 object. |
| `--region` | `PGVS3_REGION` | `us-east-1` | Region returned by bucket location queries. |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

`GET /healthz`, `GET /readyz` and `GET /metrics` are unauthenticated. Bind
those paths (or `--admin-addr`) to a private network. `pgvs3 health` polls
`/readyz` for container orchestrators. `pgvs3 stat` reports logical vs
physical size, queued files and the age of the oldest queued file.

## Storage cost and limits

- **WAL on write is about 1× the bytes.**
- **One relation caps at 32 TiB** (`MaxBlockNumber × 8 KB`), about 31.7 TiB of
  object data. A single object or part is capped by `int4` row numbers.
- **Peak heap** is live data plus files deleted inside the reap grace plus
  space held by long transactions. Freed pages are reused; plain `VACUUM` does
  not return space to the operating system.
- **One PostgreSQL** bounds capacity and write throughput. The design assumes a
  single DuckLake writer; add read-only DuckDB workers for reads and read
  replicas for PostgreSQL read scale.

## Container

`ko` builds a static, nonroot, distroless image directly from source — no
Dockerfile, no shell, no libc:

```sh
just image            # local ko.local image
just image push=true  # ghcr.io/adonm/pgvs3, amd64 + arm64
```

The image is about 16 MB and exposes `8014` (S3).

## Design decisions

- **PostgreSQL `shared_buffers` is the shared cache.** Gateways keep no
  in-process metadata or object cache; `s3p.objects` serves metadata and ≤64 KiB
  inline objects directly from PostgreSQL RAM, and DuckDB caches larger file
  bytes client-side.
- **Connections are reused most-recently-used first** to keep TCP slow-start out
  of the hot path.
- **Single-file reads up to 64 MiB are one query.** Larger/multipart reads use
  8 MiB pieces, up to eight in parallel under one exported snapshot; rows
  stream through a bounded channel so a slow client backpressures PostgreSQL.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging; Complete publishes the ordered part list without moving data.
- **One unpartitioned chunks table.** DuckLake has one writer; every read is a
  `file_id = $1` primary-key range.
- **Maintenance runs in the gateway.** Removing pg_cron keeps managed
  PostgreSQL and small installations simple.

## Testing and benchmarks

```sh
just test       # Go unit tests
just race       # unit tests with the race detector
just fuzz       # 30s fuzz targets for ranges, canonical queries and COPY framing
just contract   # S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just interop    # rclone, restic, boto3, DuckLake and SlateDB
just ducklake   # DuckLake compatibility through the benchmark harness
just bench      # DuckDB S3 benchmark, DuckLake checks and row-group tuning
```

Every recipe starts a disposable PostgreSQL 18 in Docker and two local
gateways, then removes them. `just contract` covers conditional writes,
pagination, multipart, byte ranges, cross-gateway consistency,
copy, attributes, multipart and both DuckDB 2.0 and
`duckdb-object-storage` on 1.5.6. `just interop` drives rclone, restic, boto3,
DuckLake and a small SlateDB Rust program against the same gateway. DuckDB 2.0
currently ships DuckLake 1.1; set `DUCKLAKE_VERSION=2.1` to make interop fail
until the 2.1 extension is published. `docs/benchmarks.md` has the method and
historical numbers.

## Repo layout

- `cmd/pgvs3/`: CLI (`serve`, `stat --json`, `doctor`, `health`, `reap`).
- `internal/store/`: schema, PostgreSQL pool, streaming COPY/GET, multipart,
  copy, maintenance.
- `internal/s3/`: S3 routing, XML, ranges, checksums and multipart.
- `test/e2e.sh`: the S3 contract.
- `test/interop.sh`: rclone, restic, boto3, DuckLake and SlateDB.
- `test/slatedb/`: the SlateDB Rust smoke program.
- `deploy/bench/`: disposable stack and DuckDB benchmark.
- `deploy/compose.yaml`: local PostgreSQL + gateway stack.
- `deploy/charts/pgvs3/`: Helm chart (stateless multi-AZ Deployment).
- `docs/operations.md`: probes, metrics, backup/restore and failure behavior.
- `docs/partitioning.md`: why the chunks table stays unpartitioned.
- `justfile`, `.ko.yaml`, `mise.toml`: toolchain and build.

## License

Apache-2.0. See [LICENSE](LICENSE).
