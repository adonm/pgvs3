# pgvs3 — S3 on PostgreSQL

pgvs3 is a small S3-compatible gateway that stores objects in PostgreSQL. It
is built to be DuckLake's object store: DuckLake keeps its catalog in PostgreSQL,
and with pgvs3 the data files live there too, so every DuckDB that attaches
the catalog sees the same tables and the same data.

**Status: early alpha, no releases.** Any storage layout change requires a
fresh database; there are no migrations. Use disposable databases only.

> Postgres is all you need. ;P — for durable bytes and metadata here; DuckDB
> does the analytics.

## pgvs3 vs Amazon S3

pgvs3 was measured locally with `just bench`: DuckDB 1.5.6 as the client, one
16-core host, PostgreSQL 18 over loopback. The S3 column is published
third-party figures for EC2 in the same region. Method, full tables and sources:
[docs/benchmarks.md](docs/benchmarks.md).

| | pgvs3 (local) | Amazon S3 (published) |
| --- | ---: | ---: |
| Small GET, per request p50 | <1 ms (4 KiB; 1.0 ms per HEAD+GET in DuckDB) | ~30 ms ([AnyBlob][anyblob]); AWS: 100–200 ms ([AWS][aws-perf]) |
| 8 MiB GET, one stream | 14 ms · 548 MiB/s | ~190 ms · 55–60 MiB/s median ([AnyBlob][anyblob]) |
| Aggregate GET bandwidth | 2.0 GiB/s at 16 in flight (0.45 GiB/s on the first read after a write) | ~9–11 GiB/s per 100 Gbit/s instance at 200–250 in flight ([AnyBlob][anyblob]) |
| Small GETs per second | ~7,000 (64 in flight, one gateway) | ≥5,500 per prefix, more prefixes scale it ([AWS][aws-perf]) |
| Small PUTs per second | ~4,400 (64 in flight) | ≥3,500 per prefix ([AWS][aws-perf]) |

pgvs3 answers requests 1–2 orders of magnitude sooner, which keeps DuckDB's
many small footer and row-group reads cheap. S3 wins on aggregate scale and
durability. pgvs3 is bounded by one PostgreSQL, 32 TiB of object data, and in
practice a single DuckLake writer.

[anyblob]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf
[aws-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or PostgreSQL 18+ with
`pg_cron` preloaded and installed in the target database: pass `--url`).

```sh
just setup      # toolchain (rust, mbx, just, uv, helm) + cargo fetch
just contract   # S3/DB contract: two gateways on a disposable PostgreSQL
just ducklake   # DuckLake compatibility: writer on one gateway, reader on the other
just bench      # DuckDB S3 benchmark + DuckLake row-group tuning
```

Run the gateway with explicit SigV4 credentials (the example key is for
loopback development only):

```sh
export PGVS3_ACCESS_KEY=cachebench PGVS3_SECRET_KEY=cachebench-local-only
./target/release/pgvs3 --url 'postgres://user:pass@host/db?sslmode=require' serve --addr 127.0.0.1:8014
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
     -X PUT http://127.0.0.1:8014/lake  # create the bucket once
```

DuckDB: `SET s3_endpoint='127.0.0.1:8014'; SET s3_use_ssl=false; SET s3_url_style='path';`

`pgvs3 stat` reports logical vs physical size, queued file count and the age
of the oldest queued file.

## S3 surface

- Operations: `GET` (with ranges), `HEAD`, `PUT`, `DELETE`, `DeleteObjects`,
  `ListObjectsV2`, `ListBuckets`, `CreateBucket` (idempotent), `HeadBucket`,
  `DeleteBucket` and multipart uploads. Anything else returns `NotImplemented`.
- **ETags are standard S3**: the MD5 of the object, or for multipart the MD5 of
  the part MD5s plus `-N`. SHA-256 is stored as the internal integrity digest;
  both are computed in the single streaming write pass and reads do no hashing.
- **Checksums**: `Content-MD5` and `x-amz-checksum-*` headers or trailers are
  verified before publication; signed chunks are verified by s3s. Multipart
  uploads accept a composite checksum algorithm (CRC32, CRC32C, CRC64NVME,
  SHA1, SHA256) at Create: every part must carry it, UploadPart echoes the
  verified value, and Complete must repeat it per part. Whole-object checksums
  on Complete are not implemented.
- **Metadata**: user metadata (up to 2 KiB) and Content-Type are stored with
  the object and returned by HEAD/GET.
- `If-None-Match: *` and strong `If-Match` PUTs are checked atomically.
  Content-Encoding, server-side encryption and conditional or versioned
  DELETEs are rejected rather than silently claimed.

## How it works

Six tables (`crates/pgvs3/schema.sql`): `s3p.buckets`; `s3p.objects` maps
bucket/key to a file id, size, SHA-256, ETag, metadata and, for multipart
objects, the ordered part files; `s3p.chunks` holds each file as numbered
8120-byte rows; `s3p.uploads` and `s3p.upload_parts` track incomplete
multipart work; `s3p.garbage` queues unreferenced files for reclamation.

- **Writes** stream into one binary `COPY` per PUT or part. A PUT commits its
  chunks and object row in one transaction, only after the whole body and any
  supplied checksums verify, so interrupted writes never publish. Parts commit
  with their upload record; Complete publishes the ordered part list without
  moving data. Mutations of one key serialize; acknowledged writes use
  synchronous commit.
- **Reads** turn a byte range into a row range by arithmetic and fetch one
  row-range query per 8 MiB, streaming rows as they arrive. Multi-query reads
  share a repeatable-read snapshot, so an overwrite cannot remove rows
  mid-response. Each query checks the cached metadata still points at the
  current object, so an overwrite through another gateway is never served stale.
- **Maintenance** is PostgreSQL's: DELETE and overwrite queue old files in the
  same transaction; a `pg_cron` job reclaims at most 65,536 chunk rows (about
  508 MiB) per minute and expires multipart uploads abandoned for 24 hours.
  Autovacuum, tuned on the chunks table, reclaims dead rows. Before listening,
  the gateway requires `pg_cron` in its database (`cron.database_name`
  pointing at it), checks the chunk table settings and the job, and installs
  the job only when it creates a fresh layout. Watch `pgvs3 stat` under
  sustained deletes.
- **Buckets** are explicit: create them before writing. Deleting a bucket with
  objects or an incomplete upload returns `BucketNotEmpty`.
- **Capacity** is one PostgreSQL relation: 2^32 − 2 pages × 8 KB = 32 TiB,
  about 31.7 TiB of object data at one chunk row per page. A single object or
  part is further capped by `int4` row numbers (2^31 × 8120 bytes).
- **Layout version** (`s3p.layout`, currently 8): a gateway refuses any other
  layout. Point a new layout at a fresh database.

Why 8120-byte rows (PostgreSQL 18 `heaptoast.c`, `heaptoast.h`,
`reloptions.c`): a value moves out of line only when the row exceeds
`toast_tuple_target`, which can be raised to 8160 bytes. With no nullable
columns, `file_id(8) + no(4) + length(4) + 8120` bytes plus the 24-byte header
fits: one row per 8 KB page, 99.6% full, no TOAST and no detoasting on read.

## Configuration

A flag overrides the same-named variable.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | local dev DB | PostgreSQL URL. Remote hosts require `sslmode=require`, which verifies certificate and hostname against system CAs (plus `PGVS3_DB_CA_FILE` if set). Loopback/local `prefer` can fall back to plaintext. |
| `--addr` | `PGVS3_ADDR` | `127.0.0.1:8014` | Listen address; use `0.0.0.0:8014` in a container. |
| `--access-key` | `PGVS3_ACCESS_KEY` | required | SigV4 access key. |
| `--secret-key` | `PGVS3_SECRET_KEY` | required | SigV4 secret key. |
| `--tls-cert`, `--tls-key` | `PGVS3_TLS_CERT`, `PGVS3_TLS_KEY` | unset | PEM file paths for HTTPS on non-loopback addresses. Set both; mount the private key read-only. |
| `--allow-http` | `PGVS3_ALLOW_HTTP` | false | Explicitly permit plaintext HTTP outside loopback for an isolated test cluster; do not expose that listener to untrusted clients. |
| | `PGVS3_DB_CA_FILE` | unset | Path to an additional PostgreSQL CA PEM bundle, e.g. a managed service's CA. |
| | `PGVS3_DB_ALLOW_PLAINTEXT` | false | Permit `prefer`/`disable` to remote PostgreSQL for an isolated test cluster. |
| | `PGVS3_POOL_MIN` | 64 | Connections opened at start and kept warm. Clamped to the max. |
| | `PGVS3_POOL_MAX` | 64 | Connections open at once, per gateway. Include all replicas, rollout headroom and other clients in the PostgreSQL connection budget. |

`GET /healthz` is an unauthenticated liveness probe.

## Design decisions

- **No row cache.** DuckDB caches the bytes it reads.
- **Connections are reused most-recently-used first** (`src/pg.rs`): idle TCP
  connections restart slow start.
- **Reads up to 8 MiB are one query**; bitmap heap scans use PostgreSQL 18
  read-ahead, and ordered listings enable index scans for their transaction.
- **One unpartitioned chunks table**: DuckLake has a single writer, so there
  is no insert contention to spread; the cost is the 32 TiB relation cap.
- **Multipart parts are separate files**: parts upload in parallel with no
  staging, and Complete moves no data.

## Scaling

pgvs3 gateways are stateless; scale them within the PostgreSQL connection
budget (`replicas × PGVS3_POOL_MAX` plus other clients and rollout headroom).
The design assumes one DuckLake catalog writer; add query-only DuckDB workers
for reads.

## Testing and benchmarks

Every recipe starts a disposable PostgreSQL in Docker and two local gateways
(`deploy/bench/stack.sh`) and removes them on exit. `just contract` runs the
S3/DB contract through both gateways, and `just ducklake` runs the DuckLake
lifecycle checks. `just churn` is opt-in sustained overwrite/delete/multipart
churn with vacuum evidence. `just bench` runs the DuckDB S3 benchmark,
DuckLake compatibility and Parquet row-group tuning, writing
`.tmp/pgvs3/bench.json`; results are in [docs/benchmarks.md](docs/benchmarks.md).
`hk.pkl` checks formatting, Clippy, shell/Python syntax, the chart and the
justfile. Install the hooks with `mise exec -- hk install --mise`. CI runs
`just ci`. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Running in a container

Published to GHCR on every push to `main` (amd64 and arm64), built by
`just image`: `rust:alpine` builds a static musl binary into a `scratch`
image. `:latest` can change layouts; pin a digest. Supply a TLS certificate and
key for any non-loopback listener, and keep credentials in Secrets:

```yaml
containers:
  - name: pgvs3
    image: ghcr.io/adonm/pgvs3:latest
    env:
      - name: PGVS3_URL
        valueFrom: { secretKeyRef: { name: pgvs3, key: url } }
      - name: PGVS3_SECRET_KEY
        valueFrom: { secretKeyRef: { name: pgvs3, key: secret-key } }
      - name: PGVS3_ACCESS_KEY
        valueFrom: { secretKeyRef: { name: pgvs3, key: access-key } }
      - { name: PGVS3_TLS_CERT, value: /tls/tls.crt }
      - { name: PGVS3_TLS_KEY, value: /tls/tls.key }
    volumeMounts:
      - { name: tls, mountPath: /tls, readOnly: true }
    ports: [{ containerPort: 8014 }]
volumes:
  - name: tls
    secret: { secretName: pgvs3-tls }
```

## Repo layout

- `crates/pgvs3/src/`: `main.rs` (CLI), `server.rs` (S3 API on
  [s3s](https://crates.io/crates/s3s) and `/healthz`), `db.rs` (metadata,
  reads, writes, multipart), `ingest.rs` (the `COPY` write path), `cache.rs`
  (metadata cache), `pg.rs` (PostgreSQL pool and TLS).
- `crates/pgvs3/schema.sql`: the storage layout.
- `crates/pgvs3/tests/s3_contract.rs`: the two-gateway S3/DB contract.
- `deploy/bench/`: `stack.sh` (disposable PostgreSQL + gateways for every
  test and benchmark) and `duck_bench.py` (DuckDB S3 benchmark, DuckLake
  checks and tuning). `deploy/postgres/`: PostgreSQL 18 + pg_cron image.
  `deploy/charts/pgvs3/`: Helm chart for the gateway.
- `justfile`: every task (`just` lists them); `mise.toml`: toolchain pins.
