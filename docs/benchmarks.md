# Benchmarks

The primary workload is 10 MiB–1 GiB Parquet, in native PostgreSQL storage and
origin-backed shared cache/staging roles. Current v24/v25 implementation evidence,
including actual Parquet and incompressible fixtures, is recorded in
[modernization-results.md](modernization-results.md), with rejected trials,
accepted per-path gates and remaining validation limits.

This page describes the runners and retains historical measurements. Snapshots
from different hosts, layouts and cache states are not one current capacity claim.
The [focused roadmap](roadmap.md) ranks physical-replica and WAL/capacity evidence
ahead of more protocol benchmarks.

Two benchmark harnesses are included in the repository:

- **`just load`** runs `test/loadgen` directly against the HTTP gateway to
  measure raw S3 + PostgreSQL throughput and latency, or drives paired acceptance
  through the existing `compare.py` with isolated before/after fixtures.
- **`just bench`** runs DuckDB 2.0 and DuckLake through `httpfs` to measure
  end-to-end client performance.

Both harnesses start disposable PostgreSQL 18 containers on loopback. The direct
matrix uses `shared_buffers=1GB` and pool 128; paired runs use pool 32 per binary
with separate fresh PostgreSQL clusters so WAL/resource observations cannot mix
the two sides. Different layouts never share a database. Each optional physical
standby also has 1 GiB shared buffers.
Numbers below are from a **16-core
local host** (Intel Core Ultra X7 358H, 62 GiB RAM) alongside an earlier
**6-vCPU / 11 GiB VM** baseline.

## Direct S3 gateway load (`just load`)

The exploratory matrix uses 5-second tests. 4 KiB tests use 2,000 keys; 8 MiB and
64 MiB tests use 16 and 8 objects; `ListObjectsV2` lists 1,000 keys per page from a
2,000-key bucket. Gateways hold no in-process cache; metadata and ≤64 KiB
objects are served directly from PostgreSQL `shared_buffers`.

### Paired acceptance

Save a control binary before making changes, then run the current tree against it:

```sh
mkdir -p .tmp
mise exec go -- go build -o .tmp/pgvs3-before ./cmd/pgvs3
# Make the change, then:
just load --before-bin .tmp/pgvs3-before --profile native \
  --payload /path/to/10m.parquet --payload /path/to/100m.parquet \
  --ops head,get,range,put --workers 1,4 --repeats 5
just load --before-bin .tmp/pgvs3-before --profile origin --cache-state cold \
  --payload /path/to/10m.parquet --ops get,range --workers 1,4 \
  --range-size 65536 --repeats 5
```

`--after-bin` selects a saved candidate instead of building the current tree.
`--out` selects a fresh output path; existing outputs are never overwritten.
Otherwise artifacts and `paired.json` remain in the unique printed `.tmp/verify/`
directory. `--no-small` omits the default 4 KiB case. `--requests` overrides the
size/state-aware per-sample budget; all counts and raw samples are recorded.

`compare.py` is the only paired driver and still uses `test/loadgen`, not a second
load engine. It alternates binary order, hashes frozen binaries and payloads,
checks object/range sizes and bytes, checks persisted origin directory/namespace/
TTL/range settings, and rejects unexpected cache paths. Cold filesystem-origin
fixtures use distinct keys with no prior cache bodies or metadata, and validate
the completed fills. Loadgen's opt-in `-unique-reads` shares one read sequence
across workers; its default workload is unchanged. Cold here means cold gateway
cache, not cleared operating-system page cache.

Each case fails independently above +20% median p50 or below -20% median request
rate, or on any request/fixture error. A valid report has `complete: true`; partial
reports retain `complete: false`. `failed_gates` names regressions rather than
hiding them in an aggregate. Range labels distinguish aligned chunks, whole-object
cache ranges and pass-through. The exploratory matrix remains available without
`--before-bin`, reports saturation errors instead of treating them as acceptance,
and writes actual NDJSON. `PGVS3_BENCH_DURATION` shortens that matrix for fixture
smoke tests only; use repeated paired runs for acceptance.

### Capacity evidence

Use **disposable isolated clusters only**. Resource mode deletes its fixture keys,
advances their committed-cache expiry deadlines, reaps and vacuums after each case.
It refuses pending bodies at cleanup and never advances a pending body's expiry.

Generate reproducible inputs with the existing DuckDB tool, without a gateway:

```sh
mise exec uv -- uv run --quiet --no-project --with duckdb==2.0.0 \
  python deploy/bench/duck_bench.py --parts fixtures --out .tmp/capacity/fixtures.json
```

This produces approximately 10/100/1,024 MiB Snappy Parquet and exact-size binary
fixtures under `.tmp/capacity/payloads/`. Actual sizes, generation SQL, seeds and
SHA-256 hashes are recorded. Binary fixtures repeat a seeded 1 MiB block: they are
incompressible at pgvs3's 8,120-byte block size, not unique random streams across
the entire file. `--fixture-mib` selects smaller smoke inputs; existing files are
never overwritten.

```sh
just load --capacity --after-bin .tmp/pgvs3-candidate --profile origin \
  --payload .tmp/capacity/payloads/parquet-100MiB.parquet --no-small \
  --ops get,range,put,overwrite --workers 1,4 --range-size 65536 \
  --replication async --checkpoint-writes --repeats 5
```

- `--capacity` uses one candidate endpoint/cluster and the existing loadgen to
  capture resource costs. It sets `mode: capacity`,
  `performance_gates_evaluated: false` and `failed_gates: null`; completion means
  the fixtures/observations passed, **not** a before/after performance claim.
  Use ordinary uninstrumented paired runs for the regression gate. Separate
  clusters on one host still contend for the same physical disk.
- `--resources` enables snapshots and observations on separate primary clusters.
  `--replication async|sync` adds a real physical standby per side and enables
  resources. `sync` selects one named synchronous standby; it does not mean
  `remote_apply`. Recovery state, system identifiers and replay positions are
  validated. Standbys use `hot_standby_feedback=on`; feedback/snapshots can hold
  back primary vacuum. No production replication policy is changed.
- `overwrite` uses loadgen's opt-in `-overwrite`: each worker reuses one prefilled
  key, retaining retired files for the lifecycle measurement. Ordinary PUT keeps
  its distinct-key behavior. Use `--cache-state cold --ops get,range` for empty
  origin-cache fills; otherwise reads are explicitly warmed and verified.
- `--checkpoint-writes` prepares publication/cold-fill samples with a primary
  checkpoint, replay barrier and standby restartpoint outside timed work. The
  duration is recorded, not hidden as free work. Use a one-request sample budget
  for 1 GiB resource cohorts and retain several paired repetitions; larger bursts
  are a separate sustained-churn experiment, not a peak-throughput comparison.
- `--isolate-origin-put` is a separate **paired PUT-only** filesystem fixture.
  After drain and byte verification it deletes only that sample's origin keys;
  before the next sample it truncates the otherwise empty owned cache tables.
  It refuses pending bodies, incomplete multipart uploads or unrelated keys and
  cannot be combined with resource mode. This isolates fresh publication ACKs;
  it is not a retention/reaping or sustained-upload-throughput measurement.
- Each raw run records pre-run, post-loadgen and post-drain snapshots: logical
  object-row bytes, heap/index/total relation bytes, pending state, WAL insertion
  positions, checkpoint statistics, gateway metrics and standby replay. The
  post-loadgen snapshot is **not the exact instant of the last HTTP ACK**.
  Acknowledgement latency remains loadgen's per-request measurement; after-loadgen
  drain is reported separately, not hidden inside ACK throughput.
- Lifecycle snapshots show delete, injected committed-row expiry, reaping and
  vacuum separately. Ordinary vacuum makes space reusable but need not shrink
  relation files. The logical count includes expired rows until reaped.
  Cleanup verifies the reaper actually drained rather than merely losing the
  maintenance-leader race, records reaping/vacuum time, and uses the explicit
  `--maintenance-timeout` deadline (default ten minutes).
- WAL bytes are isolated-cluster LSN advancement, including headers/padding,
  checkpoint/full-page-image and background work—not a per-table WAL counter.
  Observer `pg_settings` describe its psql session and cluster defaults, not the
  gateway's connection-local `SET` policy.
- Gateway gauges use a 100 ms poll interval plus probe time, so their observed
  peaks are lower bounds. Cross-node WAL position gaps include probe skew; they
  are not simultaneous end-to-end lag measurements. `pgvs3_spool_peak_bytes`
  is exact reserved-payload high water since
  that gateway started, not a resettable per-case peak. Pool counter deltas also
  include metrics observations. Replay catch-up time includes SQL/polling overhead.
- Resource collection can perturb short measurements. Keep the same observations
  on both sides, retain raw records, and use uninstrumented paired runs to resolve
  performance-gate failures. Do not silently discard a failed resource comparison.

The paired driver defaults to a five-minute S3 request timeout for large fixtures;
`--request-timeout` changes it. Standalone loadgen keeps its one-minute default
and exposes `-timeout`. A client deadline failure remains an error, not a retry
or a successful sample; loadgen stdout/stderr are retained in the failure.

`resource_schema: 1` and `complete: true` identify finished observations. Only a
paired report with `performance_gates_evaluated: true` and empty `failed_gates`
also establishes its measured regression gate. Full configuration and binary/
payload hashes remain in the report. See [data-plane-results.md](data-plane-results.md)
for recorded results and deployment-sizing limits.

### Historical exploratory measurements

| Workload | Workers | 16-core host | p50 | p99 | 6-vCPU VM |
| --- | ---: | ---: | ---: | ---: | ---: |
| **RESP 4 KiB GET (pipeline 1)** | 1 | **9,850 req/s** | **0.081 ms** | 0.32 ms | — |
| **RESP 4 KiB GET (pipeline 1)** | 64 | **91,046 req/s** | **0.63 ms** | 1.96 ms | — |
| **RESP 4 KiB GET (pipeline 16)** | 64 | **459,018 req/s (1.79 GiB/s)** | **0.13 ms** | 0.35 ms | — |
| **RESP 4 KiB SET (pipeline 1)** | 1 | **1,363 req/s** | **0.68 ms** | 1.40 ms | — |
| **RESP 4 KiB SET (pipeline 1)** | 64 | **23,866 req/s** | **2.59 ms** | 5.22 ms | — |
| **RESP 4 KiB SET (pipeline 16)** | 64 | **110,222 req/s (431 MiB/s)** | **0.56 ms** | 1.13 ms | — |
| 4 KiB GET (S3) | 1 | 4,993 req/s | 0.17 ms | 0.63 ms | 1,970 req/s (0.47 ms) |
| 4 KiB GET (S3) | 16 | 20,556 req/s | 0.67 ms | 2.04 ms | 6,596 req/s (1.98 ms) |
| 4 KiB GET (S3) | 64 | 25,284 req/s | 2.27 ms | 6.80 ms | 8,243 req/s (6.91 ms) |
| 4 KiB GET (S3) | 256 | 35,458 req/s | 6.42 ms | 20.7 ms | 9,756 req/s |
| HEAD (S3) | 1 | 5,878 req/s | 0.14 ms | 0.52 ms | 1,778 req/s (0.47 ms) |
| HEAD (S3) | 16 | 17,397 req/s | 0.77 ms | 2.60 ms | 7,925 req/s (1.66 ms) |
| HEAD (S3) | 64 | 30,009 req/s | 1.84 ms | 6.15 ms | 9,211 req/s (5.95 ms) |
| 4 KiB PUT (S3) | 1 | 1,114 req/s | 0.82 ms | 1.52 ms | 673 req/s (1.37 ms) |
| 4 KiB PUT (S3) | 16 | 10,474 req/s | 1.39 ms | 2.61 ms | 3,610 req/s (3.93 ms) |
| 4 KiB PUT (S3) | 64 | 17,801 req/s | 3.04 ms | 18.6 ms | 3,735 req/s (14.5 ms) |
| 4 KiB PUT (S3) | 256 | 20,367 req/s | 11.0 ms | 37.9 ms | 1,635 req/s (153 ms) |
| 8 MiB GET (S3) | 1 | 1,450 MiB/s | 5.36 ms | 7.88 ms | 417 MiB/s (18.1 ms) |
| 8 MiB GET (S3) | 8 | 3,496 MiB/s | 18.0 ms | 26.9 ms | 1,006 MiB/s (60.9 ms) |
| 8 MiB GET (S3) | 16 | 3,753 MiB/s | 33.7 ms | 59.4 ms | 1,090 MiB/s (118 ms) |
| 64 MiB GET (S3) | 1 | 1,269 MiB/s | 46.4 ms | 108 ms | 316 MiB/s (183 ms) |
| 64 MiB GET (S3) | 8 | 3,837 MiB/s | 129 ms | 219 ms | 971 MiB/s (530 ms) |
| 64 MiB GET (S3) | 16 | 4,262 MiB/s | 234 ms | 338 ms | 1,055 MiB/s (929 ms) |
| 64 MiB PUT (S3) | 1 | 220 MiB/s | 183 ms | 223 ms | 86 MiB/s (577 ms) |
| 64 MiB PUT (S3) | 8 | 509 MiB/s | 820 ms | 1,109 ms | 171 MiB/s (2,391 ms) |
| 8 KiB range from 64 MiB | 64 | 16,171 req/s | 3.57 ms | 9.67 ms | 6,397 req/s (9.07 ms) |
| ListObjectsV2 (1,000 keys) | 64 | 2,019 req/s | 27.3 ms | 99.1 ms | 638 req/s (98.4 ms) |
| **RESP 4 KiB GET** | 1 / 64 / 64p16 | **12,242 / 96,966 / 460,381 req/s** | **0.072 / 0.60 / 0.128 ms** | 0.21 / 1.74 / 0.34 ms | — |
| **RESP 4 KiB SET (sync)** | 1 / 64 / 64p16 | **5,018 / 48,215 / 213,603 req/s** | **0.183 / 1.12 / 0.284 ms** | 0.46 / 4.13 / 0.66 ms | — |
| **Cache tier 4 KiB GET (warm)** | 1 / 16 | **3,121 / 12,233 req/s** | **0.29 / 1.18 ms** | 0.71 / 2.98 ms | — |
| **Cache tier 4 KiB PUT (write-through)** | 1 / 16 | **974 / 5,326 req/s** | **0.92 / 2.56 ms** | 2.77 / 7.34 ms | — |
| **Cache tier 64 MiB GET (warm)** | 1 / 16 | **1,442 / 4,005 MiB/s** | 43.9 / 253 ms | 60.2 / 366 ms | — |
| **Cache tier 64 MiB PUT (synchronous baseline)** | 1 / 16 | **205 / 488 MiB/s** | 227 / 1,307 ms | 236 / 3,268 ms | — |

The large-object rows predate the default write-back path; see the 2026-10-26
latency-proxy snapshot below for the current ack-first behavior.

Key takeaways:

- **Sub-100 µs RESP reads & 460k GET/s pipelined:** a 1-worker 4 KiB `GET`
  completes in **`0.072 ms` (72 µs) p50** (12.2k req/s), scales to **97.0k
  `GET`/s** unpipelined and **460.4k `GET`/s (1.76 GiB/s)** at pipeline 16 via
  `CacheMGet` coalescing.
- **213k/s durable pipelined RESP writes:** pipelined `SET`s coalesce into one
  `CacheMSet` statement, reaching **213.6k durable `SET`/s (834 MiB/s,
  `0.28 ms` p50)** at pipeline 16 (48.2k unpipelined) with the previous LZ4
  codec. Re-run `just load` after the Zstd switch for current numbers.
- **OJS queue benchmarks:** the SQS rows were removed with the SQS protocol in
  the OJS v1 rewrite. Run `test/loadgen` with `-op ojs-enqueue` / `-op ojs-fetch`
  against a live PostgreSQL to measure the new path; no numbers are claimed
  here yet.
- **Sub-millisecond S3 small reads & 20.9k/s durable S3 writes:** S3 4 KiB
  `GET` reaches **35.8k req/s** (`0.17 ms` p50 at 1 worker) and durable 4 KiB
  `PUT` reaches **20.9k req/s**.
- **Cache tier:** a warm 4 KiB `GET` through the cache proxy costs one
  PostgreSQL read and reaches **12.2k req/s**; 64 MiB reads reach **4.0 GiB/s**
  and write-through PUTs **488 MiB/s** (origin is a local filesystem).
- **Cache ranges are passed through:** a cold 1 MiB range of a 256 MiB origin
  object returns in **2.2 ms** without caching the body; the first full read
  takes 0.90 s and the warm read 0.18 s.
- **Multi-GiB/s large-object reads:** 64 MiB reads stream at **1.30 GiB/s** from
  1 worker and **4.17 GiB/s (4,275 MiB/s)** across 16 workers.
- **Fast byte ranges and listings:** 8 KiB range reads from 64 MiB objects
  reach 16.1k req/s (3.55 ms p50), and 1,000-key `ListObjectsV2` calls reach
  1,840 req/s (~1.84M keys/s).

### 2026-10-25 local profile snapshot

This is a separate snapshot, not a replacement for the canonical 16-core table
above. Host: AMD Ryzen 7 7700X (16 threads), 61 GiB RAM, PostgreSQL 18 with
`shared_buffers=1GB`, gateway pool 128. All runs used `test/loadgen` for 5s.

| Workload | Workers | Result | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 4 KiB GET | 1 / 16 / 64 | 9.5k / 39.4k / 62.9k req/s | 0.10 / 0.37 / 0.88 ms | 0.19 / 0.93 / 2.87 ms |
| 4 KiB PUT | 1 / 16 / 64 | 4.9k / 27.3k / 33.3k req/s | 0.20 / 0.54 / 1.60 ms | 0.33 / 1.24 / 7.32 ms |
| 4 KiB HEAD | 64 | 70.3k req/s | 0.73 ms | 2.89 ms |
| 8 MiB GET | 1 / 8 / 16 | 1.06 / 6.99 / 7.47 GiB/s | 7.5 / 9.2 / 16.5 ms | 9.3 / 11.9 / 29.8 ms |
| 64 MiB GET | 1 / 8 / 16 | 1.01 / 6.83 / 5.97 GiB/s | 61 / 74 / 167 ms | 88 / 98 / 240 ms |
| 64 MiB PUT | 8 | 1.31 GiB/s | 379 ms | 436 ms |
| 8 KiB range from 64 MiB | 64 | 39.4k req/s | 1.45 ms | 4.31 ms |
| ListObjectsV2 (1,000 keys) | 64 | 5.08k req/s | 11.2 ms | 39.0 ms |
| RESP 4 KiB SET (pipeline 16) | 64 | 96.5k req/s | 0.65 ms | 0.88 ms |
| RESP 4 KiB GET (pipeline 16) | 64 | 851k req/s | 0.067 ms | 0.19 ms |
| OJS enqueue | 64 | 27.8k req/s | 2.29 ms | 2.66 ms |
| OJS fetch+ack | 32 | 20.2k req/s | 15.7 ms | 19.0 ms |
| Warm cache 4 KiB GET | 1 / 16 / 64 | 9.4k / 39.0k / 62.9k req/s | 0.10 / 0.37 / 0.88 ms | 0.21 / 0.92 / 2.92 ms |

CPU profiles (15s each, Go pprof):

- **4 KiB GET:** `getObject` 53% cumulative; `metaRef` 33%; pgx query 22%;
  socket writes 34%. A dedicated inline-object read path was prototyped and
  benchmarked. It did not improve throughput (62.4k vs 63.1k req/s at 64
  workers), so it was reverted. Query/transport latency, not metadata field
  decoding, dominates this path.
- **4 KiB PUT:** `putObject` 44%; `putInlineRef` 33%; pgx query 22%; socket
  writes 19%; MD5/SHA ~5.1%; GC ~7%. Preallocating the inline body from
  `Content-Length` removed the `io.ReadAll` growth/copy path and improved the
  16-worker PUT result from 24.2k to 26.7k req/s. Reusing the inline buffer
  from a bounded 64 KiB pool then improved a fresh three-sample mean from
  26.3k to 27.3k req/s at 16 workers and 32.2k to 33.3k at 64 workers; the
  64-worker p99 fell from about 7.9 ms to 7.3 ms.
- **64 MiB GET:** syscalls 70%; `streamRows` 59%; pgx row decoding 44%;
  `appendRow` 14%; socket writes 33%. A direct-decode experiment skipped the
  decoded-row → response-buffer copy for unclipped full-object reads, but the
  result was flat to slightly worse (6.59 vs 6.83 GiB/s at 8 workers), so it
  was reverted. Large reads remain syscall- and transport-bound.
- **64 MiB PUT:** the framer now writes full rows directly from the incoming
  buffer and retains only the trailing partial row, removing a full-write
  copy into `pending`. An 8-worker three-sample mean improved from 19.8 to
  21.0 req/s (1.24 to 1.31 GiB/s); p50 fell from ~399 to ~379 ms and p99 from
  ~467 to ~436 ms. Parallel MD5/SHA changed throughput by at most about 2% and
  raised CPU; probes that skipped hashing or compression changed it by less
  than 1%. Neither path is the next target.
- **Warm cache 4 KiB GET:** the warm-hit path was doing two metadata lookups
  (`MetaHeader` for the cache decision, then `GetBody` for the body).
  Reusing the first metadata row for the body read raised 1/16/64-worker
  throughput from 7.2k/33.9k/49.7k to 9.4k/39.0k/62.9k req/s, matching native
  inline GET. The canonical cache row above predates this change.
- **Query execution:** the default pgx `cache_statement` path remains fastest.
  Switching the whole connection to `cache_describe`, `exec`, or
  `simple_protocol` reduced 4 KiB GET/PUT throughput by roughly 18–30% at 16
  workers, so no query-mode change is retained.

The ownership-aware large-PUT row feed was also built and rejected. It was
correct, but the segment/reader path measured about 20.0 req/s versus 21.3
req/s for direct framing: removing the copy did not repay the extra per-row
bookkeeping. Large PUT is left at direct framing. A `GOGC=200/400` experiment
initially looked much faster, but the gain disappeared when the load generator
was pinned to its own `GOGC=100`; the gateway GC target is not a retained
change.

The next measured slice is the native 4 KiB PUT query/bind path. The current
profile is about 22% pgx query/bind, 19% socket writes, and 5% hashing. The
first experiment is a narrow unconditional bucket inline-write path with fewer
bound parameters and less server-side branching, benchmarked at 16 and 64
workers against the current 33.3k req/s. It will be kept only if it beats that
result. Small GET is at its query/transport ceiling, warm cache GET matches
native, and large reads are at the syscall ceiling.

## DuckDB 2.0 `httpfs` (`just bench s3`)

Measured on the 6-vCPU VM with 2,000 4 KiB objects and 128 8 MiB objects
(`enable_external_file_cache = false`):

| Workload | Workers | Result | p50 |
| --- | ---: | ---: | ---: |
| 4 KiB GET (steady state) | 64 | 1,838 req/s | 32.8 ms |
| 4 KiB GET (first read) | 64 | 1,467 req/s | 40.5 ms |
| 4 KiB PUT | 64 | 668 req/s | 87.5 ms |
| 8 MiB GET | 16 | 674 MiB/s | 181 ms |
| 8 MiB PUT | 16 | 120 MiB/s | 996 ms |

DuckDB's `httpfs` adds its own request scheduling and connection pool; the
direct numbers above show the gateway ceiling underneath it.

## Design ablations

### 64 KiB inline threshold vs 8 KiB chunking (6-vCPU VM, 1 worker)

| Object size & op | 64 KiB inline (default) | 8 KiB cutoff (chunked) |
| --- | ---: | ---: |
| 32 KiB PUT / GET | 509/s · 3,619/s | 227/s · 1,398/s |
| 64 KiB PUT / GET | 356/s · 3,250/s | 200/s · 1,272/s |

Inlining up to 64 KiB in `s3p.objects` is 2–3× faster than splitting into
`s3p.chunks` rows.

### Single table vs partitioned `s3p.chunks`

Hash-partitioning `s3p.chunks` into 16 or 64 partitions slowed 4 KiB `GET`s by
17–28% and `PUT`s by 5–21% due to planner overhead without spreading single-file
reads. A two-level `8×8` hash/range layout improved single-worker 64 MiB `GET`
from 441 to 556 MiB/s but regressed 4 KiB `GET`s by 21–32%. `s3p.chunks`
therefore stays a single unpartitioned table; see
[partitioning.md](partitioning.md).

### 2026-10-26 origin-cache snapshot

This microbenchmark isolates the origin cache from the canonical direct-gateway
table. Host: same Ryzen 7 7700X, embedded PostgreSQL 18.3, gateway pool 64,
loopback, `rclone serve s3` over a local filesystem as the origin. 4 KiB
objects and 5-second runs unless noted. It measures cache mechanics, not a
remote S3 network path.

| Workload | Workers | Result | p99 |
| --- | ---: | ---: | ---: |
| Warm cache GET 4 KiB | 16 | 37.8k req/s | 0.95 ms |
| Direct rclone GET 4 KiB | 16 | 38.0k req/s | 0.96 ms |
| Cold cache GET 4 KiB, previous build | 1 | 1.51k req/s | 0.99 ms |
| Cold cache GET 4 KiB, one-GET fill | 1 | 2.02k req/s | 0.74 ms |
| Cache write-through PUT 4 KiB | 16 | 3.07k req/s | 16.9 ms |
| Direct rclone PUT 4 KiB | 16 | 3.14k req/s | 16.0 ms |
| Warm cache GET 64 MiB | 1 | 16.4 req/s (1.05 GiB/s) | 69 ms |
| Direct rclone GET 64 MiB | 1 | 14.0 req/s (895 MiB/s) | 74 ms |
| Cold cache GET 64 MiB | 1 | 13.5 req/s | 292 ms |

The cold whole-object fill now asks the origin for the body and metadata in one
GET instead of issuing a HEAD first. On the local origin that raised a
single-worker cold read from 1.51k to 2.02k req/s. Warm reads are at local-origin
parity because both sides are on the same host; the cache's advantage appears
when the origin is remote. Write-through is origin-bound at this concurrency.
The first 64 MiB cache fill pays the PostgreSQL publication cost; subsequent
reads avoid the origin body transfer.

### 2026-10-26 latency-proxy cache snapshot

To avoid measuring a cache against a zero-latency loopback origin, the origin
can run in constrained mode:

```sh
pgvs3 serve --addr 127.0.0.1:18030 \
  --simulate-latency 20ms --simulate-rps 100 \
  --simulate-bandwidth 52428800
```

This snapshot used 20 ms one-way latency (40 ms RTT) and 50 MiB/s per request,
with a local pgvs3 origin, embedded PostgreSQL 18.3, and one worker. It is a
simulation, not a real S3 or Aurora measurement.

| Object | Operation | Direct origin through proxy | Cache | Improvement |
| --- | --- | ---: | ---: | ---: |
| 10 MiB | first GET | 394 ms | 440 ms first fill | one-time fill |
| 10 MiB | warm GET | 394 ms | 9.3 ms | ~42x |
| 100 MiB | first GET | 3.59 s | 3.94 s first fill | one-time fill |
| 100 MiB | warm GET | 3.59 s | 92 ms | ~39x |
| 1 GiB | first GET | 36.3 s | 39.6 s first fill | one-time fill |
| 1 GiB | warm GET | 36.3 s | 981 ms | ~37x |
| 10 MiB | PUT | 403 ms | 38 ms | ~10x |
| 100 MiB | PUT | 3.61 s | 297 ms | ~12x |
| 1 GiB | PUT | 36.1 s | 2.76 s | ~13x |

The PUT numbers use the default write-back path: the cache/outbox transaction
completes first, then the origin upload proceeds asynchronously. A 1 GiB
write-back was verified to clear its outbox row and become readable at the
origin after the background upload. Without the proxy's bandwidth limit, large
warm reads are still faster than the direct origin, but the gap narrows because
both sides use local storage.

## pgvs3 vs AWS S3 Express One Zone

S3 Express One Zone figures below are AWS's published service specifications and
`us-east-1` prices ([performance guide][express-perf], [pricing][express-price],
[2025 price reduction][express-price-cut]).

| Dimension | pgvs3 (16-core local / 6-vCPU VM) | S3 Express One Zone (published) |
| --- | --- | --- |
| 4 KiB GET / PUT p50 (1w) | 0.17 ms GET, 0.78 ms PUT (0.47 / 1.37 ms on 6 vCPU) | Consistent single-digit ms |
| 4 KiB GET throughput | 32.5k/s (9.8k/s on 6 vCPU) | 200k/s default, 2M/s max per bucket |
| 4 KiB PUT throughput | 17.5k/s durable at 64w (3.7k/s on 6 vCPU) | 100k/s default, 200k/s max per bucket |
| 8–64 MiB GET | 1.1–3.45 GiB/s (0.3–1.1 GiB/s on 6 vCPU) | Network/concurrency-bound; elastic |
| 64 MiB PUT | 188–458 MiB/s (86–171 MiB/s on 6 vCPU) | Elastic, distributed |
| Capacity | 32 TiB per bucket; ~17.4 TB per object (at most 100 namespaces per database) | Unlimited storage; 50 TB per object |
| Availability / durability | PostgreSQL replication/backups chosen by operator | 99.95% designed (99.9% SLA); 11 nines in one AZ |
| Price model (`us-east-1`) | Hardware + gp3 ($0.08/GB-mo); $0 request/transfer fees | $0.11/GB-mo + $1.13/M PUT + $0.03/M GET + $3.80/TB R+W |

### Top 6 takeaways

1. **Latency:** pgvs3 serves a 4 KiB `GET` in `0.17 ms` and a durable `PUT` in
   `0.78 ms` p50, ~10× faster than S3 Express's single-digit milliseconds.
2. **Small-object reads:** pgvs3 peaks at `32.5k GET/s` on one 16-core node,
   ~6× below S3 Express's `200k/s` default bucket quota (`2M/s` max).
3. **Small-object writes:** pgvs3 reaches `17.5k` durable 4 KiB `PUT/s`, ~6×
   below S3 Express's `100k/s` default quota (`200k/s` max).
4. **Large-object bandwidth:** pgvs3 streams 64 MiB reads at `3.45 GiB/s` and
   writes at `458 MiB/s` in cache/NVMe (`125–1,000 MiB/s` on cold EBS gp3),
   while S3 Express scales elastically with client network.
5. **500 GB large-file cost (7,450 × 64 MiB, 1× W+R/mo):** S3 Express costs
   ~$57/mo vs ~$100–200/mo for a dedicated pgvs3 node, or $40/mo in gp3 disk on
   an existing PostgreSQL.
6. **500 GB small-object cost (122M × 4 KiB, 1× W+R/mo):** S3 Express costs
   ~$199/mo due to `$1.13/M` `PUT` fees, while pgvs3 charges $0 per request and
   stays at ~$165/mo dedicated or $40/mo on an existing PostgreSQL.

## Reproduce

```sh
just contract   # run the correctness suite first
just load       # exploratory matrix (isolated .tmp/verify/ artifacts)
just bench s3   # DuckDB 2.0 httpfs matrix (.tmp/pgvs3/bench.json)
just bench      # full DuckDB + DuckLake compatibility and row-group tuning
```

[express-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html
[express-price]: https://aws.amazon.com/s3/pricing/
[express-price-cut]: https://aws.amazon.com/blogs/aws/announcing-up-to-85-price-reductions-for-amazon-s3-express-one-zone/

### 2026-10-27 shared read cache and durable write-back

Read-through fills now land in the WAL-logged, replicated `s3c_` namespace so
primary and standby workers share the same warm cache; acknowledged write-back
bodies remain in `s3w_`. This snapshot reran the large-object path against a
self-hosted pgvs3 S3 origin constrained to 20 ms one-way latency and 50 MiB/s
per request, on the same PostgreSQL 18 host. It is a local simulation, not an
AWS measurement. This run used the primary; it did not exercise physical standby
replay or promotion.

| Object | Direct origin GET | Cold cache GET | Warm cache GET | Direct origin PUT | Write-back ack |
| --- | ---: | ---: | ---: | ---: | ---: |
| 10 MiB | 0.279 s | 0.350 s | 0.005 s | 0.335 s | 0.068 s |
| 100 MiB | 2.234 s | 2.821 s | 0.048 s | 2.753 s | 0.550 s |
| 1 GiB | 22.374 s | 28.709 s | 0.506 s | 28.083 s | 5.619 s |

Warm reads were about 52x faster on 10 MiB, 47x on 100 MiB and 44x on 1 GiB.
Cold fills were 25%–28% slower than direct origin because the complete body is
now written to the WAL-logged shared read cache. Write-back acknowledgement was
about 5x faster than the constrained origin PUT. That is the cost of making
fills visible to read-only replicas; warm reads themselves are unchanged.

### 2026-10-27 cache maintainability refactor

Compared the pre-refactor `766720e` binary with the reorganized cache layer on
the same local PostgreSQL 18.6 primary (`synchronous_commit=on`, 128 MiB
`shared_buffers`) and constrained S3 origin described above. No layout, chunk
codec, SQL ingest or PostgreSQL body-streaming code changed. Production Go
source in `internal/objcache` decreased from 3,556 to 3,131 lines (12%), without
removing providers or protocol features. Fixtures were incompressible byte
payloads, not a DuckDB query benchmark.

The full sweep used three samples for each cold fill and write-back
acknowledgement; 21/11/7 warm reads for 10 MiB/100 MiB/1 GiB; 12 requests with
four concurrent readers per size; 21 HEADs; five cold aligned-range requests;
and 31 warm 64 KiB range requests. The outbox was drained between writes and
the cache truncated between cold samples. Medians are wall-clock HTTP request
times, including body transfer.

An initial +17.1% result on the 10 MiB cold fill prompted a reverse-order repeat
with nine more cold samples per binary and repeats of the other 10 MiB paths.
The table combines **all** samples, not the faster run: 12 cold fills, 42 warm
GETs, 24 concurrent GETs, 42 HEADs, 10 cold ranges, 62 warm ranges and six write
acknowledgements per binary for 10 MiB. Larger objects use the original sweep.

| Object | Cold GET before → after | Warm GET before → after | Write-back ack before → after |
| --- | ---: | ---: | ---: |
| 10 MiB | 335 → 336 ms | 4.94 → 4.91 ms | 59.8 → 61.7 ms |
| 100 MiB | 2.718 → 2.644 s | 41.2 → 42.4 ms | 496 → 467 ms |
| 1 GiB | 27.869 → 27.466 s | 437 → 439 ms | 4.603 → 4.492 s |

All 21 measured paths stayed within the 20% latency/throughput budget. The
largest combined median increase was a HEAD request (+11.4%, 0.282 → 0.314 ms).
Body-transfer paths were at most 3.2% slower; four-reader GETs ranged from
0.2% slower to 4.9% faster, and warm ranges changed by less than 1%.
Small differences on this shared host should not be treated as statistically
significant improvements. This is not an AWS/Aurora or physical-standby result.

### 2026-10-27 store and RESP refactor

A second pass removed another 197 production Go lines from `store` and `resp`
(622 across both passes). Metadata SELECT/COPY/DELETE decoding and optimistic
cache mutations now have one implementation each. Narrow cache GET and batch
MGET/MSET queries, chunk codecs, COPY ingest, and body-streaming internals remain
specialized. The storage layout remains v23.

The same local PostgreSQL 18.6 primary was used for before/after comparisons,
with synchronous commit enabled. `BenchmarkCacheCommands` covers GET, SET,
GETSET, APPEND, SETRANGE and GETDEL at 256 B, 64 KiB and 10 MiB;
`BenchmarkCacheIncrement` covers serial and four-worker increments. Three
50-operation runs were measured per case with `GOMAXPROCS=4`. A near-budget
64 KiB GET result was repeated in reverse binary order with five 300-operation
runs for 64 KiB and 10 MiB GETs. The figures below include all benchmark runs.

Run the reusable store benchmarks against a disposable v23 database:

```sh
PGVS3_TEST_DATABASE_URL="$TEST_DATABASE_URL" GOMAXPROCS=4 \
  go test -tags=integration -run '^$' -bench BenchmarkCache \
  -benchtime=50x -count=3 ./internal/store
```

| Store operation | Before | After | Median change |
| --- | ---: | ---: | ---: |
| 64 KiB GET | 122 µs | 131 µs | +7.6% |
| 10 MiB GET | 5.57 ms | 5.50 ms | −1.3% |
| 10 MiB APPEND | 49.32 ms | 45.55 ms | −7.6% |
| 10 MiB GETDEL | 8.93 ms | 7.95 ms | −11.0% |
| Four-worker INCR | 4.27 ms | 4.60 ms | +7.8% |

A separate HTTP/RESP check compared the binaries against the same database and
objects. HTTP samples were 21/11/7 whole reads at 10 MiB/100 MiB/1 GiB, twelve
four-reader requests per size, and 41 HEADs and 64 KiB ranges per size. RESP GET,
MGET, EXPIRE, PERSIST and past-deadline EXPIRE each used 101 samples, with setup
outside timing. These are native gateway reads to guard the shared metadata
path, not a repeat of the constrained-origin cold-fill measurement.

| HTTP operation | Before | After |
| --- | ---: | ---: |
| 10 MiB GET | 4.75 ms | 4.89 ms |
| 100 MiB GET | 42.14 ms | 42.81 ms |
| 1 GiB GET | 444.00 ms | 440.44 ms |
| 1 GiB GET, four readers | 602.79 ms | 602.01 ms |
| 64 KiB range of 100 MiB object | 357 µs | 397 µs |

All 37 comparisons stayed within the 20% budget: the largest median increase
was the HTTP range above (+11.2%); store cases were at most 7.8% slower after
combining repeat samples. RESP GET/MGET were slightly faster and expiration
latencies were effectively unchanged. These local measurements do not establish
AWS/Aurora or saturated mixed-workload performance.

### 2026-10-27 protocol and queue consolidation

The broader pass removes another **987 production Go lines**, bringing the
total reduction to **1,609**. OJS production code falls from 2,265 to 1,805
lines (20%). It replaces endpoint-specific transport boilerplate, duplicate
stream receivers, the job read/plan/update/reload claim pipeline, repetitive
job mutation transactions and forwarding-only S3 adapters. No protocol feature,
storage namespace, WAL guarantee or layout-v23 rule was removed.

Before/after binaries were compared on the same local PostgreSQL 18.6 primary.
The existing load generator now includes closed-loop `ojs-cycle`
(enqueue/fetch/ACK) and `ojs-stream-cycle` (append/receive/ACK), avoiding a
depleted prefill during receive benchmarks. Each case used 1 KiB payloads and
four-second runs in **before/after/after/before** order, with fresh queues per
run. Stream cycles use normal delivery with one worker and cooperative members
with eight. Values below are medians of the per-run summaries; all runs had
zero errors.

| Operation | Workers | Throughput before → after | p50 before → after |
| --- | ---: | ---: | ---: |
| Job enqueue | 1 | 667 → 684 jobs/s | 1.419 → 1.487 ms |
| Job enqueue | 8 | 3,318 → 3,093 jobs/s | 2.336 → 2.338 ms |
| Job lifecycle | 1 | 211 → 215 jobs/s | 4.678 → 4.528 ms |
| Job lifecycle | 8 | 1,084 → 1,075 jobs/s | 7.114 → 7.313 ms |
| Stream lifecycle | 1 | 172 → 172 messages/s | 5.558 → 5.509 ms |
| Cooperative stream lifecycle | 8 | 332 → 336 messages/s | 27.528 → 27.192 ms |

Transport-only `BenchmarkHTTPBinding` cases used seven 300 ms runs against a
stub store. Enqueue, fetch, ACK, NACK, stream receive and invalid requests changed
by −1.0% to +2.4%. Live manifest/health checks were repeated in the same ABBA
order with 3,001 requests per endpoint per run. The largest measured increase
was the manifest: **49.17 → 57.75 µs (+17.5%)**, a deliberate small administrative
path tradeoff from simpler response construction. Health increased 6.1%.

The 17-case native HTTP/RESP large-object guard was rerun as well. Single-reader
GETs were 5.04 → 4.98 ms at 10 MiB, 42.55 → 41.76 ms at 100 MiB and
441.66 → 438.92 ms at 1 GiB. Four-reader 1 GiB GETs were 611.50 → 598.05 ms.
The largest increase in this guard was RESP MGET (+11.8%); all other cases were
within +10.1%. Across the six transport cases, six live queue/stream cases, two
administrative endpoints and 17 object/RESP cases, every comparison remained
within the 20% latency/throughput budget. These are local measurements, not
AWS/Aurora, failover or saturated mixed-workload guarantees.
