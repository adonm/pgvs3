# pgvs3 — S3 on PostgreSQL

pgvs3 is a small, fast S3-compatible gateway that stores objects as rows in
PostgreSQL. It is built to be DuckLake's object store: DuckLake keeps its
catalog in PostgreSQL, and with pgvs3 the data files live there too, so every
DuckDB that attaches the catalog sees the same tables and the same data.

The gateway is Go, stateless over PostgreSQL, and runs as a distroless static
container built with [ko](https://ko.build/). It uses the v9 storage layout
(small objects are stored inline in `s3p.objects`).

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
  serve --addr 127.0.0.1:8014
```

Create a bucket and write an object (the example key is for loopback
development only):

```sh
export PGVS3_ACCESS_KEY=pgvs3 PGVS3_SECRET_KEY=pgvs3-local-only
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
     -X PUT http://127.0.0.1:8014/lake
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
     -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
```

DuckDB 2.0+:

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, KEY_ID 'pgvs3', SECRET 'pgvs3-local-only',
    REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
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
- **Reads are primary-key range queries.** Up to 8 MiB is one query; longer
  reads use one repeatable-read snapshot so an overwrite cannot remove later
  rows mid-response. Rows stream straight into the HTTP response.
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

## Performance

On a shared 6-vCPU loopback host, direct SigV4 load on v9 gives a 0.35 ms p50
for one 4 KiB GET and about 10.4k 4 KiB GET/s at 64 workers. A 4 KiB PUT is
1.35 ms at one worker and 2.2k/s at 16–64 workers. 8 MiB GETs reach 1.0 GiB/s
and 64 MiB GETs about 0.99 GiB/s. S3 Express One Zone publishes
consistent single-digit-millisecond latency and 200k reads / 100k writes per
second per directory bucket by default. Per-request latency is in the same
band; the aggregate ceiling is not — pgvs3 is one PostgreSQL, Express is a
managed service. Full methodology, Go-vs-Express tables and the cost comparison
are in [docs/benchmarks.md](docs/benchmarks.md).

## Cross-AZ metadata cache

Gateways in different AZs can form a peer cache with
[`groupcache-go/v3`](https://github.com/groupcache/groupcache-go) and
[Otter](https://github.com/maypok86/otter) (W-TinyLFU). Each key has one owner;
concurrent misses cluster-wide are coalesced into one PostgreSQL read. A peer
that fetches a remotely-owned key mirrors it in a local hot cache, so repeated
reads in that AZ stay in local RAM.

```sh
PGVS3_PEERS='10.0.1.10:8015,10.0.2.10:8015,10.0.3.10:8015' \
PGVS3_PEER_ADVERTISE='10.0.1.10:8015' \
PGVS3_PEER_ADDR='0.0.0.0:8015' \
PGVS3_CACHE_MAIN_BYTES=$((64<<20)) \
PGVS3_CACHE_HOT_BYTES=$((64<<20)) \
pgvs3 serve --addr 0.0.0.0:8014
```

- The hot cache defaults to **100% of the main cache** (groupcache's default is
  1/8, which causes constant cross-AZ fetches for the two-thirds of keys owned
  by other nodes).
- The peer cache carries **metadata only**. Objects up to 64 KiB are inline in
  the database; a per-gateway byte cache covers non-inline objects up to
  256 KiB. Large object bytes are already cached by DuckDB's external file
  cache.
- A single gateway invalidates its byte cache on every local write. When peers
  are configured, cache hits revalidate against `s3p.objects` before serving,
  so a write through another gateway cannot be masked. If several gateways
  write the same bucket without a peer list, set
  `PGVS3_OBJECT_CACHE_REVALIDATE=true`.
- Writes enqueue a best-effort cluster-wide invalidation. A stale entry can
  cause one retry, never stale bytes: the range query checks that the object
  still points at the cached `file_id`.
- `PGVS3_PEER_SECRET` adds a shared header to peer requests. Keep the peer port
  on a private network either way.

## S3 surface

`GET` (ranges and conditional), `HEAD`, `PUT`, `DELETE`, `DeleteObjects`,
`ListObjectsV2`, `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`,
multipart uploads, `CopyObject`, `UploadPartCopy`, `GetObjectAttributes`,
`ListParts`, `ListMultipartUploads`, `RenameObject`, and appends via
`x-amz-write-offset-bytes`.

S3 Express One Zone additions:

- **Directory buckets** named `base--zone-id--x-s3`, including virtual-hosted
  zonal requests and `HeadBucket`'s `x-amz-bucket-location-type`,
  `x-amz-bucket-arn` and `x-amz-bucket-region` headers.
- **CreateSession** (`GET /{bucket}?session`) issues stateless, HMAC-signed,
  five-minute credentials scoped to one bucket. Zonal requests authenticate with
  `x-amz-s3session-token`; both `s3` and `s3express` SigV4 signing names work.
  Read-only sessions reject mutating methods.
- **RenameObject** (`PUT /{key}?renameObject`) moves metadata only.
- **AppendObject** is `PUT` with `x-amz-write-offset-bytes`; the new bytes become
  another part and existing bytes never move. The 10,000-part limit applies.
- **CopyObject** shares immutable files between object rows; GC checks every
  reference before reaping. **UploadPartCopy** currently copies the requested
  source range through the streaming ingest path.
- `GetObjectAttributes`, `ListParts` and `ListMultipartUploads` are implemented.
  `ListDirectoryBuckets` is selected when the request is signed for
  `s3express` (AWS SDKs use that signing name).
- `x-amz-storage-class: EXPRESS_ONEZONE` and
  `x-amz-server-side-encryption: AES256` are accepted/returned for directory
  buckets. SSE-C and DSSE-KMS are rejected, as Express does.

**Not yet implemented:** presigned-URL query authentication,
`x-amz-checksum-type: FULL_OBJECT` and `CRC64NVME` (CRC32, CRC32C, SHA1, SHA256
and MD5 are supported), `partNumber` on GET/HEAD, per-part checksums in
`GetObjectAttributes`, `x-amz-client-token` idempotency for `RenameObject`,
lifecycle expiration, bucket policies/IAM, and access points. Like Express,
pgvs3 has no versioning, object tags, Object Lock, ACLs, lifecycle transitions,
replication, Event Notifications, S3 Select, website hosting, access logs,
Storage Lens or Transfer Acceleration. ETags are opaque; single PUTs use MD5
and appends use a random strong ETag.

A known AWS SDK quirk: boto3's `create_bucket` for a directory bucket calls
`CreateSession` first, and with a custom endpoint it can return `NoSuchBucket`
before sending `CreateBucket`. Create the bucket once with the CLI or REST
(`PUT /{bucket}` with a `CreateBucketConfiguration`); object operations then
work normally, including session auth.

## Configuration

A flag overrides the same-named environment variable.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | local dev DB | PostgreSQL URL. Remote hosts require `sslmode=require`; certificates and hostnames are verified against system CAs plus `PGVS3_DB_CA_FILE`. Loopback may use plaintext. |
| `--addr` | `PGVS3_ADDR` | `127.0.0.1:8014` | S3 listen address. |
| `--access-key` | `PGVS3_ACCESS_KEY` | required | SigV4 access key. |
| `--secret-key` | `PGVS3_SECRET_KEY` | required | SigV4 secret key. |
| `--session-key` | `PGVS3_SESSION_KEY` | derived from secret | HMAC key for CreateSession tokens. Share it across gateways. |
| `--vhost-domain` | `PGVS3_VHOST_DOMAIN` | unset | Extra virtual-host suffix, e.g. `s3.internal`. `--x-s3` hosts are always recognized. |
| `--account-id` | `PGVS3_ACCOUNT_ID` | `000000000000` | Account ID in `x-amz-bucket-arn`. |
| `--tls-cert`, `--tls-key` | `PGVS3_TLS_CERT`, `PGVS3_TLS_KEY` | unset | PEM files for HTTPS on non-loopback addresses. |
| `--allow-http` | `PGVS3_ALLOW_HTTP` | false | Explicitly permit plaintext HTTP outside loopback. |
| | `PGVS3_DB_CA_FILE` | unset | Extra PostgreSQL CA bundle. |
| | `PGVS3_DB_ALLOW_PLAINTEXT` | false | Permit plaintext to a remote PostgreSQL for an isolated rig. |
| `--pool-min` | `PGVS3_POOL_MIN` | 64 | Connections opened at start and kept warm. |
| `--pool-max` | `PGVS3_POOL_MAX` | 64 | Connections per gateway. Size the fleet against PostgreSQL `max_connections`. |
| `--reap-grace` | `PGVS3_REAP_GRACE` | `10m` | How long deleted files stay readable. |
| `--maintenance-interval` | `PGVS3_MAINTENANCE_INTERVAL` | `1m` | Reaper and upload-expiry interval. |
| `--object-cache-bytes` | `PGVS3_OBJECT_CACHE_BYTES` | 64 MiB | Whole-object cache for objects up to 256 KiB; `0` disables. |
| `--object-cache-ttl` | `PGVS3_OBJECT_CACHE_TTL` | `5s` | Small-object cache TTL. |
| `--object-cache-revalidate` | `PGVS3_OBJECT_CACHE_REVALIDATE` | false single gateway; true with peers | Check each cache hit against `s3p.objects`. |
| `--whole-read-max` | `PGVS3_WHOLE_READ_MAX` | 64 MiB | Largest single-file span read in one query; larger spans split into pieces. |
| `--cache-main-bytes` | `PGVS3_CACHE_MAIN_BYTES` | 64 MiB | Local cache for owned metadata keys. |
| `--cache-hot-bytes` | `PGVS3_CACHE_HOT_BYTES` | 64 MiB | Local mirror of peer-owned metadata keys. |
| `--peers` | `PGVS3_PEERS` | unset | Comma-separated `host:port` peer list. |
| `--peer-addr` | `PGVS3_PEER_ADDR` | `0.0.0.0:8015` | Peer cache listen address. |
| `--peer-advertise` | `PGVS3_PEER_ADVERTISE` | required with peers | This gateway's address as peers see it; must be in `PGVS3_PEERS`. |
| `--peer-secret` | `PGVS3_PEER_SECRET` | unset | Shared secret header for peer requests. |
| `--region` | `PGVS3_REGION` | `us-east-1` | SigV4 region and bucket ARN region. |
| `--log-level` | `PGVS3_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. |

`GET /healthz` is an unauthenticated liveness probe. `pgvs3 stat` reports
logical vs physical size, queued files and the age of the oldest queued file.

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

The image is about 17 MB. It exposes `8014` (S3) and `8015` (peer cache).

## Design decisions

- **No block cache.** A small whole-object cache covers repeated footers and
  metadata (≤256 KiB); larger file bytes are left to DuckDB's external cache.
- **Connections are reused most-recently-used first** to keep TCP slow-start out
  of the hot path.
- **Reads up to 8 MiB are one query.** Larger reads share a repeatable-read
  snapshot; rows stream through a bounded channel so a slow client
  backpressures PostgreSQL.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging; Complete publishes the ordered part list without moving data.
- **One unpartitioned chunks table.** DuckLake has one writer; every read is a
  `file_id = $1` primary-key range.
- **Maintenance runs in the gateway.** Removing pg_cron keeps managed
  PostgreSQL and small installations simple.

## Testing and benchmarks

```sh
just test       # Go unit tests
just contract   # S3, DuckDB 2.0, DuckLake 1.1, duckdb-object-storage, Express APIs
just ducklake   # DuckLake compatibility through the benchmark harness
just bench      # DuckDB S3 benchmark, DuckLake checks and row-group tuning
```

Every recipe starts a disposable PostgreSQL 18 in Docker and two local
gateways, then removes them. `just contract` covers conditional writes,
pagination, multipart, byte ranges, cross-gateway reads, peer invalidation,
CreateSession, copy/rename/append, attributes, and both DuckDB 2.0 and
`duckdb-object-storage` on 1.5.6. `docs/benchmarks.md` has the method and
historical numbers.

## Repo layout

- `cmd/pgvs3/`: CLI (`serve`, `stat`).
- `internal/store/`: schema, PostgreSQL pool, streaming COPY/GET, multipart,
  copy/rename/append, maintenance.
- `internal/s3/`: S3 routing, SigV4, sessions, XML, ranges, checksums, Express
  extensions.
- `internal/peercache/`: groupcache + Otter cross-AZ metadata cache.
- `test/e2e.sh`: the contract.
- `deploy/bench/`: disposable stack and DuckDB benchmark.
- `deploy/charts/pgvs3/`: Helm chart (3-AZ topology and peer cache).
- `justfile`, `.ko.yaml`, `mise.toml`: toolchain and build.

## License

Apache-2.0. See [LICENSE](LICENSE).
