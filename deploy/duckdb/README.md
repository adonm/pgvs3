# pgvs3 DuckDB 2.0.0 + Quack image

Analytics image built from the **official DuckDB 2.0.0 image**
(`duckdb/duckdb:2.0.0`, digest
`sha256:453caf4f1c445ee8363acaa6cd383751396d2eae0d02f362ada64d87267a4221`)
with nine signed extensions baked in. The final image is the official
distroless DuckDB base: **no Python, no shell, no supervisor and no custom CLI
replacement**. `ENTRYPOINT` is the official `/duckdb` binary.

CI publishes `ghcr.io/adonm/pgvs3-duckdb:2.0.0` (plus `:latest`, and
`:vX.Y.Z` on release tags) for **linux/amd64**. `just duckdb-image` builds the
same bytes locally as `pgvs3-duckdb:2.0.0`. The existing pgvs3 Go `.ko` image
and Helm chart are unchanged; this image is not wired into the Helm chart yet.

```
spatial          core       a3dbcd1781
ducklake         core       417a9117f5
postgres_scanner core       c0f5a3c0c4
fts              core       223b8cb9e6
vss              core       3685131eac
quack            core       f96420ed46
httpfs           core       77392a7207
raquet           community  1d900cbb7e (descriptor 0.3.0)
differential     community  v1.1.0 (43203569…)
```

The image is **linux/amd64** only. `raquet` and `differential` have no musl
builds; the official DuckDB image is Debian/glibc distroless. Arm64 hashes are
not pinned. Nothing is downloaded or installed at container runtime.

`raquet` needs `libexpat.so.1`, which the official distroless base does not
ship. The only non-extension runtime byte added is that 186,472-byte library
from a throw-away pinned Debian build stage (`libexpat1=2.5.0-1+deb12u5`, sha256
`b342d94f…c45057`). The Debian stage is not part of the final image.

## Validated result

The checked local image is `pgvs3-duckdb:2.0.0`
(`sha256:40e76ebda4abf91529e0c1978bca50c6887714b69348b5febfe940c858c8dfd1`).
Docker reports **865,315,044 bytes** and `docker save` compressed blobs are
**216,704,920 bytes**. Docker 29's image `Size` field is reported as-is; it may
include both compressed and unpacked data and is not labeled "uncompressed"
here. For comparison, the previous custom Debian/Python image was
1,008,179,186 bytes / 252,036,538 compressed, so this is about 143 MB smaller
reported and 35 MB smaller compressed.

The image is amd64, non-root UID/GID `65532`, read-only-root friendly, and has
no mounted volumes. All nine modules load with
`allow_unsigned_extensions=false`, `autoinstall_known_extensions=false` and
`autoload_known_extensions=false`. There is no Nightly or unsigned bypass.

`just duckdb-integration` passed on the disposable stack against the existing
`pgvs3` database (29 DuckLake metadata tables): a native bucket was created
through gateway A, DuckLake wrote Parquet keys seen through gateway B, 100
qualified `q.lake.main.events` rows with the same row hash were read from a
fresh container through gateway B, and that container added row 101. It also
verified zero persistent mounts, `SIGTERM` exit **143** in about 100-150 ms
with `docker --init`, a missing-bucket 404 without re-creation, a missing database
that stayed uncreated, redacted credentials, and all nine version records.
The local DuckLake restart and a PostgreSQL restart preserved the qualified
lake rows; the unqualified-memory caveat still applies.

## Build and verify

```sh
just duckdb-test                 # 9 native SQL/artifact + 11 compute-helper stdlib tests
just duckdb-image                # fetch+verify artifacts, docker build amd64
just duckdb-check                # host check: entrypoint/user/hash + 9/9 signed loads
just duckdb-smoke                # host-driven local no-PG/no-S3 functional smoke
just duckdb-integration          # disposable PG/S3/gateway compute scenario
just duckdb-integration-bench    # opt-in spatial/vector/FTS sample in that scenario
```

`just duckdb-test` runs the native SQL/artifact tests and the compute-helper
tests without Docker, PostgreSQL or S3. The standard `just ci` also runs the
native tests; it does **not** build or pull this image, and it does not run the
heavy integration recipes.

`just duckdb-check` is host Python driving Docker. It does not execute Python
inside the image. It verifies the official `/duckdb` hash, every baked
extension and `.info` sidecar hash/size, the `ENTRYPOINT`/user/workdir, that
`/bin/sh` and `/usr/bin/python3` are absent, and that all nine modules load in a
read-only non-root container.

`just duckdb-smoke` is also host Python. It starts one read-only container and
runs the image's native `/duckdb` CLI through `docker exec`; it does not use
`docker exec python` and does not run a custom SQL API. It covers all nine
module versions with unsigned loading off, spatial RTree, VSS HNSW recall, FTS,
RaQuet function presence, a local Differential snapshot, Quack memory and a
local DuckLake restart.

`just duckdb-image` runs `deploy/duckdb/fetch_artifacts.py` first. It verifies
every pinned extension archive, extracted extension and DuckDB `.info`
installation-metadata file before `docker build`; the official `/duckdb` hash
is verified in the build stage. The `.info` files are
metadata (source URL, version, origin and artifact hash), not cryptographic
signatures created by this project; DuckDB validates the extension binaries
themselves when loading them with unsigned extensions disabled. The build
stage also verifies the official `/duckdb` hash, `EXTENSIONS.sha256` and the
libexpat hash before the final stage copies the bytes. The image tag is
`pgvs3-duckdb:2.0.0` (override with `DUCKDB_IMAGE`). `just duckdb-image` also
runs `duckdb-check` against the built tag. `just duckdb-image push=true` tags
and pushes `ghcr.io/adonm/pgvs3-duckdb:2.0.0` and `:latest` (override the
repository with `DUCKDB_IMAGE_REPO`); CI logs in to GHCR with
`GITHUB_TOKEN`, and the recipes themselves do not log in.

The build itself runs the offline hash checks, so a mismatched artifact or
missing `.info` file fails before a tag is produced. There is no in-image
`check`, `smoke` or `serve` argument: passing `check` to the image now means a
database filename to the official CLI, not a supervisor command. Do not run the
CLI's bare `-serve`: it prints the Quack auth token. The baked init calls
`quack_serve` explicitly with `getenv('QUACK_TOKEN')` and suppresses its output.

## Run the server

The image is non-root (UID/GID 65532) and works with a read-only root
filesystem when `/tmp` is writable. Only the DuckDB temp directory and Quack
readiness live under `/tmp`.

```sh
export PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable'
export PGVS3_BUCKET='lake-bucket'      # must already exist
export PGVS3_PREFIX='tenant/ducklake'  # optional, default: ducklake
export PGVS3_ENDPOINT='127.0.0.1:8014' # pgvs3 gateway, path-style S3
export QUACK_TOKEN="$(openssl rand -hex 32)"

docker run --rm -d -i --init \
  --name pgvs3-duckdb \
  --read-only --tmpfs /tmp:rw,size=2g \
  --network host \
  -e PGVS3_URL -e PGVS3_BUCKET -e PGVS3_PREFIX -e PGVS3_ENDPOINT \
  -e QUACK_TOKEN \
  pgvs3-duckdb:2.0.0
```

`-i` and `--init` are both required for a long-lived local server:

- Quack is the CLI's `CALL quack_serve(...)`; it is **not a daemon**. With
  stdin closed, the CLI reaches EOF and exits `0` after starting the listener,
  taking the server with it. `docker run -d -i` (no TTY) keeps the process
  alive because stdin stays open.
- The native CLI is PID 1 and ignores `SIGTERM`/`SIGINT`; Docker then escalates
  to `SIGKILL` (`137`). Docker's `--init` (tini) runs outside the image as PID
  1, so `docker stop` produces a normal `143` in about 100-150 ms. The image itself
  does not contain an init, supervisor or signal-aware launcher.
- Kubernetes is **not cluster-validated** in this increment. The equivalent
  documented settings are `stdin: true` and `stdinOnce: false`; without them
  the container sees EOF and the server exits. Kubernetes would also need an
  explicit PID-1/init and SIGTERM policy later; this repo does not claim it is
  wired or validated.

Use `-p 9494:9494` instead of `--network host` when the gateway is reachable
inside the container. The defaults are `PGVS3_ENDPOINT=127.0.0.1:8014`,
`PGVS3_PREFIX=ducklake`, `QUACK_BIND=0.0.0.0:9494`,
`QUACK_ALLOW_OTHER_HOSTNAME=true`, `DUCKDB_MEMORY_LIMIT=2GB`,
`DUCKDB_THREADS=4`, and `DUCKDB_TEMP_DIRECTORY=/tmp/duckdb`.

| Variable | Required | Notes |
| --- | --- | --- |
| `PGVS3_URL` | yes | Existing libpq-compatible `postgres://` or `postgresql://` URL. It is passed only to a **temporary** `TYPE postgres` DuckDB secret via `getenv()`, so SQL and logs contain no URL or password. |
| `PGVS3_BUCKET` | yes | Must already exist. The image never creates the bucket. |
| `PGVS3_PREFIX` | no | Stable S3 prefix under the bucket. Default `ducklake`; leading/trailing slashes are trimmed. |
| `PGVS3_ENDPOINT` | no | S3 endpoint, with or without `http://`/`https://`; path-style is always used. Scheme is stripped for the secret. Default `127.0.0.1:8014`. |
| `PGVS3_S3_USE_SSL` | no | Empty by default; derives from an `https://` endpoint, else plain HTTP. Set `true`/`false` explicitly for a remote endpoint. |
| `PGVS3_S3_ACCESS_KEY` | no | Dummy/default `pgvs3`. |
| `PGVS3_S3_SECRET_KEY` | no | Dummy/default `pgvs3`. |
| `PGVS3_S3_REGION` | no | Default `us-east-1`. |
| `PGVS3_METADATA_SCHEMA` | no | Default `ducklake_pgvs3`; must be a lower-case SQL identifier. |
| `QUACK_TOKEN` | serve | Required; no insecure default. At least 4 non-whitespace characters. |
| `QUACK_BIND` | no | Default `0.0.0.0:9494`. |
| `QUACK_ALLOW_OTHER_HOSTNAME` | no | Default `true`. Keep true for a non-loopback bind. Only use on trusted networks behind a TLS edge. |
| `DUCKDB_MEMORY_LIMIT` | no | Default `2GB`. |
| `DUCKDB_THREADS` | no | Default `4`. |
| `DUCKDB_TEMP_DIRECTORY` | no | Default `/tmp/duckdb`; DuckDB creates it under a writable `/tmp`. |

The fixed lake alias is **`lake`**. `PGVS3_LAKE_ALIAS` is retired because the
native `ATTACH ... AS <alias>` cannot safely take a runtime identifier.
`DUCKDB_MAX_TEMP_DIRECTORY_SIZE` is also retired in this increment (not
invented in native SQL). Use `PGVS3_METADATA_SCHEMA` for isolation.

The bootstrap SQL validates the required URL/bucket/token, URL scheme, bucket
name, prefix characters/path components, endpoint, S3 keys and metadata schema
with `error()`. Errors contain no URL, password or token. A literal `ATTACH`
URI was proven to leak credentials in the probe; the bootstrap only creates
named secrets from `getenv()` and never builds a literal PostgreSQL URI.

### Catalog and bucket semantics

- The URL must point at an **existing PostgreSQL database**. The image never
  runs `CREATE DATABASE`.
- The first attach creates the DuckLake metadata schema
  `PGVS3_METADATA_SCHEMA` (default `ducklake_pgvs3`) and its metadata tables in
  that database.
- `AUTOMATIC_MIGRATION false` is explicit. Supported older DuckLake catalogs
  are not migrated automatically. A catalog that this 1.1 extension cannot
  read fails at attach instead of being migrated behind the operator's back.
- The S3 bucket/prefix must be created explicitly by the operator or test
  bootstrap. The image does not create buckets or objects implicitly.
- Normal DuckLake data inlining is left at its default. There is no setting
  that disables inlining just for this image.
- `PGVS3_URL` is a temporary secret (`CREATE TEMP SECRET`), not a persistent
  secret file. Quack output is suppressed with `.output /dev/null`; the only
  non-secret ready marker printed after attach/listener is `PGVS3_READY`,
  followed by the memory-catalog warning.

### Quack and persistent tables

Quack is beta. This image binds plain HTTP internally on `0.0.0.0:9494` with
token auth and `allow_other_hostname=true`; put a TLS-terminating proxy in
front of it before exposing it. Do not publish `:9494` directly to an
untrusted network. There is no custom HTTP SQL API; Quack is the only server.

A Quack session starts in the **memory** catalog even though the server session
does `USE lake` before listening. This image does not set a persistent default
catalog for Quack sessions. Clients must qualify persistent writes:

```sql
LOAD quack;
ATTACH 'quack:127.0.0.1:9494' AS q (TYPE quack, TOKEN '…');

-- Persistent: qualified lake.main.* names.
CREATE TABLE q.lake.main.events (id INTEGER, kind VARCHAR);
INSERT INTO q.lake.main.events VALUES (1, 'open');
SELECT * FROM q.lake.main.events ORDER BY id;

-- Arbitrary SQL scoped to the server:
FROM q.query('SELECT count(*) FROM lake.main.events');

-- Or stateless:
FROM quack_query('quack:127.0.0.1:9494',
                 'SELECT count(*) FROM lake.main.events',
                 token = '…');
```

An unqualified `CREATE TABLE t (...)` or `CREATE TABLE q.<table>` lands in the
ephemeral memory catalog. Do not do that for data that must survive. There is
no automatic default-lake fallback and no data-loss guarantee for unqualified
memory writes. With the current Python DuckDB 2.0.0 client, direct
`q.lake.main.<table>` DDL/DML/read works; the old CLI 1.5 `q.main.*` form is not
the current client path. The `compute` integration proves durable
`q.lake.main.events` rows, Parquet keys through gateway B, a fresh-container
restart through gateway B, and persistence across a PostgreSQL restart.
`q.query` and `quack_query` remain available for arbitrary server-side SQL.
`CONNECT q` is not used; qualified names are the tested path.

## Measured sample

`just duckdb-integration-bench` adds an opt-in sample on the same compute
fixture: 5,000 points, 5,000 vectors with 32 dimensions, and 20 FTS documents.
These are single-host native Quack round-trip samples, not storage throughput
and not a blanket "fastest" claim. Copy and index build are one-time costs; the
first query can include cold plan/cache work, while repeats are warm.

Measured 2026-11-06 on the official-base image
(`sha256:40e76ebda4abf91529e0c1978bca50c6887714b69348b5febfe940c858c8dfd1`)
from `.tmp/duckdb-large-bench-20261106/compute.json` (SHA-256
`b5d355cb3f14fdab4509940d2d5fa094d59320ca2833a2d36cba864b0f988388`):

- **Spatial, 5,000 points:** lake scan 44.7 ms first / 17.3 ms warm median;
  native copy 9.9 ms + RTree build 4.1 ms; native query 3.8 ms first / 4.07 ms
  warm median, 1,521 matching rows.
- **Vector, 5,000 × 32 dimensions:** lake scan 53.7 ms first / 18.3 ms warm
  median; native copy 27.0 ms + HNSW build 42.3 ms; native query 4.79 ms with
  10/10 exact recall at 10.
- **FTS, 20 documents:** native table + index build 44.8 ms; query 12.9 ms
  first/warm.
- **Large tables:** a 1,000,000-row DuckLake table (`42.7 MiB`, load `677 ms`)
  answered a columnar projection in `13.0 ms`, a 1% range in `11.9 ms` and a
  point lookup in `16.9 ms`. The 1.14 GB LINZ Wellington orthophoto
  (`1,135,383,082` bytes) uploaded in `3.94 s` at `275 MiB/s`, read metadata in
  `5.6 ms`, a z21 point in `38.2 ms`, a z16 overview in `31.7 ms`, a 9% region
  (704 tiles) in `291.2 ms`, and region stats in `983.8 ms`.

Incremental INSERT and DELETE were visible immediately; UPDATE left the index
stale in DuckDB 2.0.0. The lake table is the durable copy; native indexes are
temporary materialized snapshots that must be rebuilt when freshness matters.
An earlier official-base sample measured lower 5k-row medians (RTree `2.96 ms`
vs `10.17 ms` lake scan; HNSW `3.19 ms` vs `13.26 ms` lake scan), so compare
ratios within a session rather than absolute times across runs. These are two
separate **one-query-at-a-time** samples, not two capacity points and not a
range; the `C × W` pool below is a different experiment and neither replaces nor
averages the single-client medians.

### `C` clients × `W` worker pool

`just duckdb-pool-bench` runs `W = 1, 2, 4` worker containers and `C = 1, 4, 8,
16, 32` persistent host Quack clients against a 1,000,000-row DuckLake table
(42.7 MiB) and a per-worker 5,000-point native RTree. Each worker has
`DUCKDB_THREADS=4`, `DUCKDB_MEMORY_LIMIT=1GB`; clients route `i mod W`, disable
their own external-file/Parquet caches, warm once, then run 20 timed
`q.query(...)` calls. The gateway uses `--pool-max 32` and is restarted at
`--read-cache-bytes 0` and `268435456`. Sampled PostgreSQL counts are split by
`application_name`; active is instantaneous, total is the envelope. Measured
2026-11-07 on a 16-core host from `.tmp/verify/pool.mH6Prx/quack_pool.json`
(SHA-256 `3add9c71c3250a48bab62747535c588ce6a1f43ae7719a32213f100705bb7429`):

**Lake range/aggregation, cache 0:**

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 66.9 | 14.94 | 16.18 | 16.40 | 0/18 | 8 | 0/1 |
| 1 | 4 | 153.4 | 24.14 | 30.55 | 50.33 | 1/34 | 18 | 0/4 |
| 1 | 8 | 204.4 | 37.45 | 52.15 | 55.99 | 1/39 | 18 | 1/8 |
| 1 | 16 | 236.0 | 65.04 | 85.56 | 97.09 | 3/48 | 24 | 1/16 |
| 1 | 32 | 180.7 | 172.16 | 247.15 | 270.28 | 3/56 | 31 | 4/27 |
| 2 | 1 | 57.2 | 17.41 | 19.10 | 19.56 | 0/18 | 8 | 0/25 |
| 2 | 4 | 172.9 | 21.95 | 27.19 | 29.90 | 0/34 | 18 | 0/26 |
| 2 | 8 | 212.1 | 35.17 | 51.62 | 67.76 | 0/47 | 24 | 1/28 |
| 2 | 16 | 242.2 | 64.27 | 85.91 | 91.89 | 1/56 | 32 | 2/32 |
| 2 | 32 | 247.0 | 125.98 | 175.40 | 197.16 | 1/61 | 32 | 1/40 |
| 4 | 1 | 69.2 | 14.35 | 15.43 | 15.72 | 0/18 | 8 | 0/42 |
| 4 | 4 | 164.8 | 23.02 | 31.14 | 33.34 | 0/39 | 19 | 0/42 |
| 4 | 8 | 206.2 | 36.98 | 44.89 | 49.05 | 1/52 | 27 | 1/44 |
| 4 | 16 | 208.6 | 74.81 | 97.58 | 104.49 | 0/59 | 31 | 3/48 |
| 4 | 32 | 231.3 | 131.02 | 188.94 | 215.63 | 3/62 | 32 | 1/56 |

**Lake range/aggregation, cache 256 MiB** (warm-up fills; all timed cells had
zero chunkPool acquires/fills):

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 77.8 | 12.79 | 14.32 | 14.72 | 0/18 | 7 | 0/24 |
| 1 | 4 | 224.8 | 17.14 | 20.76 | 26.03 | 0/30 | 7 | 1/24 |
| 1 | 8 | 271.4 | 27.23 | 40.05 | 47.57 | 0/30 | 7 | 2/24 |
| 1 | 16 | 190.5 | 81.31 | 120.83 | 135.65 | 1/41 | 7 | 2/24 |
| 1 | 32 | 222.4 | 138.23 | 193.97 | 226.16 | 1/41 | 7 | 3/26 |
| 2 | 1 | 78.8 | 12.62 | 14.16 | 14.20 | 0/18 | 7 | 0/40 |
| 2 | 4 | 213.2 | 17.97 | 22.41 | 23.39 | 0/32 | 7 | 0/40 |
| 2 | 8 | 175.6 | 39.94 | 68.53 | 73.25 | 1/35 | 7 | 3/40 |
| 2 | 16 | 285.9 | 54.12 | 72.32 | 79.42 | 1/41 | 7 | 2/40 |
| 2 | 32 | 275.5 | 109.82 | 165.79 | 189.32 | 1/41 | 7 | 1/40 |
| 4 | 1 | 75.3 | 13.15 | 14.31 | 14.54 | 0/18 | 8 | 0/56 |
| 4 | 4 | 187.3 | 18.53 | 28.83 | 30.34 | 0/32 | 8 | 1/56 |
| 4 | 8 | 133.0 | 57.89 | 81.04 | 85.47 | 0/35 | 8 | 0/56 |
| 4 | 16 | 283.7 | 54.50 | 68.77 | 76.41 | 1/41 | 8 | 1/56 |
| 4 | 32 | 253.7 | 119.13 | 171.42 | 195.10 | 0/41 | 8 | 2/56 |

**Native per-worker 5,000-point RTree:**

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 258.7 | 3.77 | 4.33 | 4.84 | 0/56 | 31 | 0/24 |
| 1 | 4 | 944.5 | 4.03 | 4.89 | 5.01 | 0/56 | 31 | 0/24 |
| 1 | 8 | 1,181.6 | 5.27 | 11.39 | 13.34 | 0/56 | 31 | 0/24 |
| 1 | 16 | 1,356.7 | 9.98 | 20.44 | 24.48 | 0/56 | 31 | 0/24 |
| 1 | 32 | 1,271.0 | 21.71 | 47.23 | 54.25 | 0/56 | 31 | 0/24 |
| 2 | 1 | 259.3 | 3.69 | 5.47 | 5.49 | 0/61 | 32 | 0/40 |
| 2 | 4 | 1,049.2 | 3.68 | 4.29 | 4.52 | 0/61 | 32 | 0/40 |
| 2 | 8 | 1,580.8 | 4.55 | 6.88 | 8.00 | 0/61 | 32 | 0/40 |
| 2 | 16 | 2,139.9 | 6.19 | 11.36 | 13.51 | 0/61 | 32 | 0/40 |
| 2 | 32 | 2,353.4 | 11.35 | 24.04 | 35.29 | 0/61 | 32 | 0/40 |
| 4 | 1 | 207.5 | 4.79 | 5.25 | 5.37 | 0/62 | 32 | 0/56 |
| 4 | 4 | 1,001.1 | 3.90 | 4.54 | 4.73 | 0/62 | 32 | 0/56 |
| 4 | 8 | 863.9 | 8.56 | 12.35 | 14.76 | 0/62 | 32 | 0/56 |
| 4 | 16 | 1,454.1 | 8.72 | 16.41 | 21.01 | 0/62 | 32 | 0/56 |
| 4 | 32 | 2,037.5 | 13.09 | 26.30 | 32.15 | 0/62 | 32 | 0/56 |

Native RTree scales to two workers (1,271 → 2,353 q/s at `C=32`) and then hits
the 16-core host with four workers × four threads (2,038 q/s, higher p99). The
lake scan plateaus near 230–285 q/s at one gateway; the 256 MiB cache removes
timed chunk-pool traffic and raises median `C >= 8` throughput from 212 to
254 q/s. Full analysis:
[benchmarks.md](../../docs/benchmarks.md#c-clients-x-w-worker-pool-benchmark).
Single host, one session; individual cells are not a monotonic scaling claim.

## Deliberate limits

- **DuckLake 1.1 has no index support.** Spatial RTree, VSS HNSW and FTS
  indexes can only be built on native DuckDB tables or an explicit materialized
  copy. DuckLake also rejects fixed-size vector types such as `FLOAT[4]`;
  materialize a native copy as a list/array if you need a temporary native
  index. The indexed copy is a snapshot and goes stale after lake changes. The
  image does not pretend lake indexes exist or auto-build native caches.
- **VSS HNSW disk persistence is experimental** and must be enabled at index
  creation. HNSW lives fully in RAM, is not covered by DuckDB's `memory_limit`,
  and is rewritten at checkpoints. The smoke covers native HNSW; the compute
  benchmark records 10/10 exact recall on a native `FLOAT[32]` copy, not a
  durable lake vector index.
- **FTS incremental indexes maintain INSERT/DELETE, not UPDATE.** Rebuild or
  delete+insert after updates. FTS on DuckLake fails because the table type
  does not support triggers. The compute benchmark records the stale UPDATE
  behavior and the unsupported lake index error.
- **Differential is not the stateless backend.** It is a local native DuckDB
  repository with optional explicit remote/export ACK, writer fencing, and the
  documented 256 MiB synchronous epoch limit. The image ships the signed
  extension; the compute scenario records a local snapshot only and does not
  claim remote ACK.
- **RaQuet** reads RaQuet/Parquet files; it is not a lake-table index. The
  compute scenario records the baked function presence only. The real
  public-data/RaQuet benchmark remains the existing opt-in harness.
- **PG/S3 compute integration is exercised** on the disposable existing
  `pgvs3` database, a native gateway bucket, unique PostgreSQL metadata schema
  and stable S3 prefix. It checks native pgvs3 operations, Parquet keys through
  gateway B, a fresh-container restart through gateway B, and PostgreSQL
  restart persistence. Missing bucket and missing database failures are
  recorded without implicit bucket or database creation. `no-daemon` remains
  the one-shot native lake-only hook:

  ```sh
  just duckdb-no-daemon   # requires PGVS3_URL and an existing PGVS3_BUCKET
  ```

  It runs the baked `lake-only.sql` (`lake.sql` plus a one-shot marker) with
  `-init` and no Quack listener.
- **Lifecycle limits:** `docker run -d -i` and either `--init` or an external
  PID-1 policy are required. Native PID1 termination is `137` after the Docker
  grace period; `docker --init` gives the tested `143`. Kubernetes stdin/PID-1
  behavior is documented, not cluster-validated.
- **Deployment scope:** this is an amd64 image. CI publishes
  `ghcr.io/adonm/pgvs3-duckdb:2.0.0`, `:latest` and `:vX.Y.Z` on release tags
  for `linux/amd64`; `just duckdb-image` builds `pgvs3-duckdb:2.0.0` locally.
  There is no Helm sidecar wiring in this phase; the existing Go image and
  chart are unchanged.
- **Arm64** is not built or hashed yet. Do not publish an arm64 tag from this
  lock.

## Files

- `Dockerfile` — official DuckDB base, verified extension/libexpat copies,
  native entrypoint, non-root/read-only-friendly final image.
- `init.sql` — native server startup: secret/lake setup, Quack listener,
  `PGVS3_READY` marker and memory-catalog warning.
- `lake.sql` — native DuckLake secret/attach setup (no Quack listener), read by
  `init.sql`; `lake-only.sql` adds the one-shot `PGVS3_NO_DAEMON_OK` marker used
  by `duckdb-no-daemon`.
- `extensions.sql` — settings and the nine `LOAD`s shared by both entry points.
- `extensions.lock` — CLI/extension versions, URLs, archive/extracted hashes,
  sizes, source commits, embedded `.info` metadata, official base digest and
  libexpat pin.
- `EXTENSIONS.sha256` — installed extension hash manifest used by the build.
- `fetch_artifacts.py` — build-time extension fetch/verify/decompress cache
  (stdlib; no CLI archive is fetched for the image).
- `image_tools.py` — host-only image check (entrypoint/user/hash/module loads).
- `smoke.py` — host-only native CLI smoke through Docker.
- `native_test.py` — focused stdlib tests; no Docker/PG/S3 required.
