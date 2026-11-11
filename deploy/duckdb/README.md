# pgvs3 DuckDB 2.0.0 + Hardcharger (ChargeWire) image

Analytics image built on **Hardcharger 2.10.0** (ChargeWire 2.4.0), which
embeds the exact official **DuckDB v2.0.0** (source id `d54cbbe3df`), with
eight signed extensions baked in:

```
spatial          core       a3dbcd1781
ducklake         core       417a9117f5
postgres_scanner core       c0f5a3c0c4
fts              core       223b8cb9e6
vss              core       3685131eac
httpfs           core       77392a7207
raquet           community  1d900cbb7e (descriptor 0.3.0)
differential     community  v1.1.0 (43203569…)
```

The base is pinned by digest:
`ghcr.io/gizmodata/hardcharger:2.10.0@sha256:35e7e161…7ed36`. `ENTRYPOINT` is
`hardcharger-server`; the image runs as the `hardcharger` user (UID/GID 999,
`HOME=/var/lib/hardcharger`). ChargeWire listens on **31337** and a plaintext
**31338** serves only `/health` (`{"status":"serving"}`).

Two additions are needed for the baked modules:

- **`libexpat.so.1`** for `raquet`, taken from a throw-away pinned Debian build
  stage (`libexpat1=2.5.0-1+deb12u5`, sha256 `b342d94f…c45057`). Hardcharger's
  Ubuntu base does not ship it.
- **the official `/duckdb` CLI**, copied in for one-shot `lake-only.sql` runs
  and host-side extension checks. The daemon never uses it.

CI publishes `ghcr.io/adonm/pgvs3-duckdb:2.0.0` (plus `:latest`, and `:vX.Y.Z`
on release tags) for **linux/amd64**. `just duckdb-image` builds the same bytes
locally as `pgvs3-duckdb:2.0.0`. The pgvs3 Go `.ko` image and Helm chart are
unchanged; this image is not wired into the chart yet.

The image is **linux/amd64** only. `raquet` and `differential` have no musl
builds and arm64 hashes are not pinned. Nothing is downloaded or installed at
container runtime: autoinstall, autoload and unsigned loading are all off.

## The `lake` default catalog

`HARDCHARGER_DEFAULT_CATALOG=lake` puts every new client session in the
DuckLake catalog that `init.sql` attaches, so unqualified SQL is durable SQL:

```sql
CREATE TABLE events (id INTEGER, payload VARCHAR);   -- lands in lake.main
INSERT INTO events VALUES (?, ?);                    -- ? and $1 binds both work
SELECT * FROM events;
```

That is the one behavioural change from the earlier Quack CLI image, where
unqualified writes landed in an ephemeral `memory` catalog and every persistent
statement had to spell `lake.main.<table>`. Native index work still lives in
`memory.main` (DuckLake rejects indexes, FTS triggers and fixed-size vector
types), and `USE memory` / `USE lake` switches catalogs inside a session.

| Catalog | Contents | Durable |
| --- | --- | --- |
| `lake` | DuckLake tables under `PGVS3_BUCKET`/`PGVS3_PREFIX` | yes |
| `memory` | native tables, RTree/HNSW/FTS indexes, scratch work | no |
| `system`, `temp`, `_hardcharger_system` | engine and server internals | n/a |

## init SQL is rendered, not templated

`hardcharger-server` executes its init SQL through DuckDB's C++
`Connection::Query`. Two consequences shape `deploy/duckdb/`:

1. **Pure SQL only.** There are no CLI dot-commands (`.read`, `.output`,
   `.print`), so `init.sql` and `lake-only.sql` are plain statement lists.
2. **No `getenv()`.** Hardcharger's DuckDB build deliberately omits it so no
   statement can read the process environment. `init.sql` therefore cannot
   interpolate `PGVS3_URL` itself.

So the committed `extensions.sql`, `lake.sql`, `init.sql` and `lake-only.sql` are
**templates**, and `render_init.py` turns them into a concrete init file at
startup. It substitutes `__PGVS3_*__` / `__DUCKDB_*__` sentinels with
SQL-escaped literals, validates each value, and writes the result owner-only
(mount it read-only into the container). Unrendered, the templates trip their own
`position('__PGVS3_' IN …)` guards and the server refuses to start, naming the
missing variable and never a value.

```sh
PGVS3_URL='postgres://…' PGVS3_BUCKET='lake-bucket' \
  python3 deploy/duckdb/render_init.py --output /tmp/pgvs3-init.sql --mode 644
```

`just duckdb-integration`, `just duckdb-pool-bench` and `just duckdb-smoke` do
this for you.

## Build and verify

```sh
just duckdb-test                 # 10 stdlib template/renderer/artifact tests + compute helpers
just duckdb-image                # fetch+verify artifacts, docker build amd64
just duckdb-check                # entrypoint/user/env/hash + 8/8 signed loads
just duckdb-smoke                # local no-PG/no-S3 functional smoke
just duckdb-integration          # disposable PG/S3/gateway compute scenario
just duckdb-integration-bench    # opt-in spatial/vector/FTS sample in that scenario
just duckdb-pool-bench           # 45-cell C x W ChargeWire worker-pool benchmark
```

`just duckdb-check` is host Python driving Docker; it runs no Python inside the
image. It verifies the copied `/duckdb` hash, every baked extension and `.info`
sidecar hash/size, the `hardcharger-server` entrypoint, the `hardcharger` user,
every default environment value, the absence of `/usr/bin/python3`, the exposed
ports and the stop signal, then loads all eight modules in a read-only container
with unsigned loading off.

`just duckdb-check` / `just duckdb-smoke` require the `uv` and
`adbc-driver-chargewire` client:

```sh
mise exec uv -- uv run --no-project --with adbc-driver-chargewire \
  --with adbc-driver-manager --with pyarrow python deploy/duckdb/smoke.py pgvs3-duckdb:2.0.0
```

`just duckdb-smoke` starts the image's own `hardcharger-server` with no stdin,
no `--init` and a read-only root filesystem, waits for `:31338/health`, writes
an unqualified `lake` table through `?` binds, restarts the container, re-reads
the rows, and asserts `docker stop` exits **0**. It also covers all eight
modules, spatial RTree, VSS HNSW recall, FTS and a local Differential snapshot
through the copied CLI.

`just duckdb-image` runs `deploy/duckdb/fetch_artifacts.py` first, which
verifies every pinned extension archive, extracted extension and DuckDB `.info`
installation-metadata file. The build stage verifies the official `/duckdb`
hash, `EXTENSIONS.sha256` and the libexpat hash before the final stage copies
the bytes. The `.info` files are metadata (source URL, version, origin and
artifact hash), not signatures created by this project; DuckDB validates the
extension binaries themselves with unsigned loading disabled. The image tag is
`pgvs3-duckdb:2.0.0` (override with `DUCKDB_IMAGE`).
`just duckdb-image push=true` tags and pushes `ghcr.io/adonm/pgvs3-duckdb:2.0.0`
and `:latest` (override with `DUCKDB_IMAGE_REPO`); CI logs in to GHCR with
`GITHUB_TOKEN`.

## Run the server

Non-root UID/GID 999, read-only root filesystem, only `/tmp` writable. No
`-i`, no `--init`, no tini: `hardcharger-server` is a PID-1 daemon that handles
`SIGTERM` itself.

```sh
export PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable'
export PGVS3_BUCKET='lake-bucket'      # must already exist
export PGVS3_PREFIX='tenant/ducklake'  # optional, default: ducklake
export PGVS3_ENDPOINT='127.0.0.1:8014' # pgvs3 gateway, path-style S3
export HARDCHARGER_ADMIN_PASSWORD="$(openssl rand -hex 32)"

python3 deploy/duckdb/render_init.py --output /tmp/pgvs3-init.sql --mode 644

docker run --rm -d \
  --name pgvs3-duckdb \
  --read-only --tmpfs /tmp:rw,size=2g \
  -p 31337:31337 -p 31338:31338 \
  -v /tmp/pgvs3-init.sql:/opt/pgvs3-duckdb/init.sql:ro \
  -e PGVS3_URL -e PGVS3_BUCKET -e PGVS3_PREFIX -e PGVS3_ENDPOINT \
  -e HARDCHARGER_ADMIN_PASSWORD \
  pgvs3-duckdb:2.0.0
```

Use `--network host` instead of the two `-p` flags when the gateway is only
reachable on the host. Readiness is
`curl -fsS http://127.0.0.1:31338/health` → `{"status":"serving"}`; ChargeWire
discovery is at `http://127.0.0.1:31337/chargewire`.

`docker stop` drains and exits **0** in about 200 ms
(`HARDCHARGER_GRACEFUL_SHUTDOWN=true`,
`HARDCHARGER_SHUTDOWN_GRACE_PERIOD_SECONDS=15`).

Clients:

```python
from adbc_driver_chargewire import dbapi
conn = dbapi.connect("chargewire://127.0.0.1:31337?tls=false",
                     username="pgvs3", password=password)
cur = conn.cursor()
cur.execute("INSERT INTO events VALUES (?, ?)", [1, "one"])
cur.execute("SELECT * FROM events")
print(cur.fetchall())
```

```sh
mise exec uv -- uv run --no-project --with adbc-driver-chargewire \
  --with adbc-driver-manager --with pyarrow python your_script.py
```

`?` and `$1` bind parameters both work, and
`cur.execute(...).fetch_arrow_table()` returns Arrow batches. JDBC, JavaScript
and plain HTTP clients exist too; see
[ChargeWire & Hardcharger](https://gizmodata.com/chargewire).

| Variable | Required | Default | Notes |
| --- | --- | --- | --- |
| `PGVS3_URL` | yes | — | Existing libpq-compatible `postgres://`/`postgresql://` URL for the DuckLake catalog. |
| `PGVS3_BUCKET` | yes | — | Must already exist. The image never creates a bucket. |
| `PGVS3_PREFIX` | no | `ducklake` | S3 prefix under the bucket; leading/trailing slashes are trimmed. |
| `PGVS3_ENDPOINT` | no | `127.0.0.1:8014` | S3 endpoint with or without a scheme; path-style is always used. |
| `PGVS3_S3_USE_SSL` | no | empty | Empty derives from an `https://` endpoint, else plain HTTP. |
| `PGVS3_S3_ACCESS_KEY` / `PGVS3_S3_SECRET_KEY` | no | `pgvs3` | Dummy defaults for the local gateway. |
| `PGVS3_S3_REGION` | no | `us-east-1` | |
| `PGVS3_METADATA_SCHEMA` | no | `ducklake_pgvs3` | Lower-case SQL identifier. |
| `HARDCHARGER_ADMIN_PASSWORD` | yes | — | Administrator password; never baked into the image. |
| `HARDCHARGER_ADMIN_USERNAME` | no | `pgvs3` | |
| `HARDCHARGER_PORT` / `HARDCHARGER_HEALTH_PORT` | no | `31337` / `31338` | `0` disables the health listener. |
| `HARDCHARGER_PORT` | no | `31337` | ChargeWire listener. |
| `HARDCHARGER_MEMORY_LIMIT` | no | `2GB` | DuckDB `memory_limit`. |
| `DUCKDB_MEMORY_LIMIT` | no | `2GB` | Rendered into `SET memory_limit`. |
| `DUCKDB_THREADS` | no | `4` | Rendered into `SET threads`. |
| `DUCKDB_TEMP_DIRECTORY` | no | `/tmp/duckdb` | Needs a writable `/tmp`. |
| `HARDCHARGER_DEFAULT_CATALOG` | no | `lake` | Starting catalog for every session. |
| `HARDCHARGER_ALLOW_UNSIGNED_EXTENSIONS` | no | `false` | Keep false. |
| `HARDCHARGER_SHUTDOWN_GRACE_PERIOD_SECONDS` | no | `15` | Graceful drain window. |

`PGVS3_LAKE_ALIAS` stays retired: the native `ATTACH … AS <alias>` cannot
safely take a runtime identifier, so the alias is fixed to `lake`.

## Measured result

`just duckdb-integration` on 2026-11-12 against the existing `pgvs3` database
(one gateway pair, `--pool-max 8`) verified:

- A native bucket created through gateway A; DuckLake wrote **3 Parquet keys**
  over two container lifetimes, seen through gateway B.
- Unqualified `CREATE TABLE events` landed in `lake.main` (0 rows in `memory`),
  102 rows with `?` binds, matching row hash read back from a **fresh container**
  through gateway B; that container added row 103.
- `SIGTERM` exit **0** in **197 ms** (479 ms for the second container), read-only
  rootfs, one mount (the rendered init file), no `-i`/`--init`.
- Missing bucket → 404 and no re-creation; missing database stayed uncreated and
  the one-shot attach failed with no credential in its output.

`just duckdb-integration-bench` single-client medians (5k-row fixture, one query
at a time over ChargeWire; **not** a capacity point):

| Query | Median |
| --- | --- |
| 5k-point native RTree (index built on `memory.main`) | **7.7 ms** |
| 5k-point DuckLake scan of the same predicate | **16.8 ms** |
| 5k × 32 vector native HNSW (10/10 exact recall) | **8.7 ms** |
| 5k × 32 vector DuckLake scan | **21.2 ms** |
| FTS query over 20 documents (incremental index) | **17.2 ms** |
| 1M-row DuckLake projection | **14.8 ms** |
| 1M-row 1% range | **13.5 ms** |
| 1M-row point lookup | **20.9 ms** |
| 1.14 GB LINZ orthophoto metadata | **10.8 ms** |
| orthophoto z21 point / z16 overview | **44.9 ms** / **40.3 ms** |
| orthophoto 9% region (704 tiles) | **589.8 ms** |

The 1M-row table loaded in **729 ms** and stored **42.9 MiB** in one object; the
orthophoto uploaded at **206 MiB/s**. Incremental FTS INSERT and DELETE were
visible immediately; UPDATE left the index stale in DuckDB 2.0.0.

### `C` clients × `W` worker pool

`just duckdb-pool-bench` runs `W = 1, 2, 4` worker containers and
`C = 1, 4, 8, 16, 32` persistent host ADBC clients against a 1,000,000-row
DuckLake table (**42.9 MiB**) and a per-worker 5,000-point native RTree. Each
worker has `DUCKDB_THREADS=4`, `HARDCHARGER_MEMORY_LIMIT=1GB` and
`HARDCHARGER_DEFAULT_CATALOG=lake`; clients route `i mod W`, disable their own
external-file/Parquet caches, warm once, then run 20 timed statements. The
gateway uses `--pool-max 32` and is restarted at `--read-cache-bytes 0` and
`268435456`. Sampled PostgreSQL counts are split by `application_name`; active is
instantaneous, total is the envelope. Measured 2026-11-12 on a 16-core host from
`.tmp/verify/pool.UzzpKf/chargewire_pool.json` (SHA-256
`46dc5c065f74b660466b8c36bfcb3db93005b21ac4297ad925a87b38b47315ff`):

**Lake range/aggregation, cache 0:**

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 61.3 | 16.36 | 17.54 | 18.42 | 5/18 | 8 | 0/1 |
| 1 | 4 | 153.1 | 24.30 | 33.66 | 40.94 | 6/39 | 20 | 0/4 |
| 1 | 8 | 181.0 | 43.00 | 51.42 | 55.13 | 0/48 | 25 | 0/8 |
| 1 | 16 | 197.8 | 78.96 | 101.44 | 108.39 | 2/49 | 26 | 3/16 |
| 1 | 32 | 199.1 | 156.11 | 217.06 | 243.99 | 4/57 | 32 | 4/25 |
| 2 | 1 | 60.9 | 16.37 | 17.43 | 17.65 | 0/18 | 8 | 0/25 |
| 2 | 4 | 155.1 | 24.75 | 31.37 | 32.90 | 0/35 | 19 | 0/26 |
| 2 | 8 | 178.3 | 44.67 | 50.84 | 52.87 | 0/57 | 32 | 2/28 |
| 2 | 16 | 197.9 | 79.69 | 97.78 | 111.08 | 6/62 | 32 | 1/32 |
| 2 | 32 | 192.0 | 161.48 | 235.34 | 263.36 | 2/62 | 32 | 2/40 |
| 4 | 1 | 60.0 | 16.45 | 18.30 | 18.42 | 0/18 | 8 | 0/42 |
| 4 | 4 | 142.6 | 27.47 | 32.75 | 34.80 | 0/46 | 23 | 1/42 |
| 4 | 8 | 172.1 | 44.77 | 54.27 | 60.99 | 2/55 | 32 | 1/44 |
| 4 | 16 | 193.3 | 81.17 | 100.81 | 107.65 | 1/59 | 32 | 2/48 |
| 4 | 32 | 200.9 | 148.19 | 195.75 | 218.19 | 3/60 | 32 | 1/56 |

**Lake range/aggregation, cache 256 MiB** (warm-up fills; all timed cells had
zero chunkPool acquires/fills):

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 65.6 | 15.05 | 16.64 | 16.71 | 0/18 | 7 | 0/24 |
| 1 | 4 | 171.4 | 22.39 | 26.23 | 28.30 | 0/34 | 7 | 0/24 |
| 1 | 8 | 207.7 | 37.36 | 46.56 | 49.02 | 0/34 | 7 | 0/24 |
| 1 | 16 | 231.7 | 67.81 | 85.07 | 90.92 | 0/34 | 7 | 2/24 |
| 1 | 32 | 236.4 | 131.98 | 173.32 | 187.90 | 2/41 | 7 | 2/24 |
| 2 | 1 | 65.3 | 15.29 | 16.11 | 16.14 | 0/18 | 7 | 0/40 |
| 2 | 4 | 173.5 | 22.82 | 26.09 | 30.93 | 1/28 | 7 | 0/40 |
| 2 | 8 | 202.9 | 38.40 | 46.79 | 49.77 | 0/40 | 7 | 0/40 |
| 2 | 16 | 234.0 | 67.24 | 84.00 | 90.80 | 1/41 | 7 | 1/40 |
| 2 | 32 | 241.9 | 128.78 | 174.91 | 190.43 | 1/41 | 7 | 2/40 |
| 4 | 1 | 65.5 | 15.13 | 16.34 | 18.02 | 0/18 | 8 | 0/56 |
| 4 | 4 | 164.8 | 24.14 | 28.01 | 28.96 | 0/30 | 8 | 0/56 |
| 4 | 8 | 202.8 | 37.77 | 48.05 | 52.78 | 2/34 | 8 | 0/56 |
| 4 | 16 | 226.2 | 69.35 | 86.02 | 93.08 | 1/41 | 8 | 2/56 |
| 4 | 32 | 242.3 | 128.40 | 170.07 | 187.16 | 1/41 | 8 | 4/56 |

**Native per-worker 5,000-point RTree:**

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk | DuckLake act/total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 127.8 | 7.79 | 8.67 | 8.94 | 0/57 | 32 | 0/24 |
| 1 | 4 | 413.8 | 8.92 | 11.73 | 12.46 | 0/57 | 32 | 1/24 |
| 1 | 8 | 666.6 | 11.56 | 14.65 | 17.09 | 0/57 | 32 | 1/24 |
| 1 | 16 | 795.0 | 19.23 | 26.29 | 29.32 | 0/57 | 32 | 1/24 |
| 1 | 32 | 956.6 | 31.54 | 44.85 | 52.80 | 0/57 | 32 | 1/24 |
| 2 | 1 | 123.7 | 7.98 | 8.98 | 9.03 | 0/62 | 32 | 0/40 |
| 2 | 4 | 432.7 | 9.05 | 10.22 | 11.13 | 0/62 | 32 | 0/40 |
| 2 | 8 | 703.5 | 10.86 | 13.06 | 15.20 | 0/62 | 32 | 0/40 |
| 2 | 16 | 891.7 | 17.24 | 22.15 | 24.12 | 0/62 | 32 | 1/40 |
| 2 | 32 | 976.0 | 31.20 | 43.75 | 49.80 | 0/62 | 32 | 0/40 |
| 4 | 1 | 129.2 | 7.71 | 8.12 | 8.34 | 0/60 | 32 | 0/56 |
| 4 | 4 | 433.8 | 9.04 | 10.46 | 11.05 | 0/60 | 32 | 0/56 |
| 4 | 8 | 702.7 | 11.22 | 13.12 | 14.46 | 0/60 | 32 | 0/56 |
| 4 | 16 | 861.3 | 17.83 | 22.65 | 23.97 | 0/60 | 32 | 2/56 |
| 4 | 32 | 1,026.5 | 30.03 | 38.41 | 44.86 | 0/60 | 32 | 2/56 |

Native RTree scales almost linearly to `W = 4, C = 32` (**1,026 q/s**) and is
bounded by the 16-core host rather than by worker count; the extra workers add
little because each native table is per-worker and one worker's four threads
already saturate a query at low `C`. The lake scan plateaus near
**200–240 q/s**: it is one gateway plus one PostgreSQL catalog, so `W` buys
concurrency headroom, not lake throughput. The 256 MiB cache removes timed
chunk-pool traffic (7–8 chunk connections versus up to 32) and lifts `C = 32`
throughput from 199–201 to 236–242 q/s. Full analysis:
[benchmarks.md](../../docs/benchmarks.md#c-clients-x-w-worker-pool-benchmark).
Single host, one session; individual cells are not a monotonic scaling claim.

## Deliberate limits

- **DuckLake 1.1 has no index support.** Spatial RTree, VSS HNSW and FTS
  indexes can only be built on native DuckDB tables or an explicit materialized
  copy in `memory.main`. DuckLake also rejects fixed-size vector types such as
  `FLOAT[4]`. The indexed copy is a snapshot and goes stale after lake changes;
  the image does not pretend lake indexes exist or auto-build native caches.
- **FTS resolves its index schema in the session's current catalog.** Build and
  query the index with `USE memory`, because `fts_main_<table>` is created in
  the catalog of the indexed table. FTS on DuckLake fails: the table type does
  not support triggers. INSERT and DELETE are maintained incrementally; UPDATE
  leaves the index stale in DuckDB 2.0.0 (rebuild or delete+insert).
- **VSS HNSW lives fully in RAM** and is not covered by `memory_limit`; disk
  persistence is experimental and must be enabled at index creation. The
  benchmark records 10/10 exact recall on a native `FLOAT[32]` copy, not a
  durable lake vector index.
- **Differential is not the stateless backend.** It is a local native DuckDB
  repository with optional explicit remote/export ACK, writer fencing and the
  documented 256 MiB synchronous epoch limit. The compute scenario records a
  local snapshot only and does not claim remote ACK.
- **RaQuet** reads RaQuet/Parquet files; it is not a lake-table index.
- **`getenv()` is unavailable.** Any SQL that read an environment variable under
  the old CLI image must take its value as a bind parameter or a rendered
  literal. The image's own templates are rendered host-side by
  `render_init.py`.
- **One-shot mode** still works through the copied official CLI, and is the only
  consumer of `lake-only.sql`:

  ```sh
  python3 deploy/duckdb/render_init.py --one-shot --output /tmp/lake-only.sql --mode 644
  docker run --rm --read-only --tmpfs /tmp:rw,size=1g --entrypoint /duckdb \
    -v /tmp/lake-only.sql:/opt/pgvs3-duckdb/lake-only.sql:ro pgvs3-duckdb:2.0.0 \
    -no-agent -batch -bail -init /opt/pgvs3-duckdb/lake-only.sql
  ```

  `just duckdb-no-daemon` wraps this. It requires `PGVS3_URL` and an existing
  `PGVS3_BUCKET`.
- **Deployment scope:** amd64 only. There is no Helm sidecar wiring in this
  phase; the Go image and chart are unchanged. **Arm64** is not built or hashed.

## Files

- `Dockerfile` — Hardcharger base, verified extension/libexpat/CLI copies,
  `hardcharger-server` entrypoint, non-root/read-only final image.
- `init.sql` — the ChargeWire init template (`extensions.sql` + `lake.sql`);
  rendered by `render_init.py` and mounted over the baked path.
- `lake.sql` — DuckLake secret/attach template.
- `lake-only.sql` — the same plus a one-shot `PGVS3_NO_DAEMON_OK` marker, for
  the copied CLI.
- `extensions.sql` — extension loading and the `DUCKDB_*` settings template.
- `render_init.py` — host-side template renderer with value validation.
- `extensions.lock` — CLI/extension versions, URLs, archive/extracted hashes,
  sizes, source commits, embedded `.info` metadata, the Hardcharger pin, the
  official CLI digest and the libexpat pin.
- `EXTENSIONS.sha256` — installed extension hash manifest used by the build.
- `fetch_artifacts.py` — build-time extension fetch/verify/decompress cache
  (stdlib).
- `image_tools.py` — host-only image check (entrypoint/user/env/hash/modules).
- `smoke.py` — host-only functional smoke through Docker and ChargeWire.
- `native_test.py` — focused stdlib tests; no Docker/PG/S3 required.
