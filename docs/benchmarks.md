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

For workload selection, freshness, OLTP budgets and executable SQL/API recipes,
use the [workload guides](guides/index.md). `just guides` validates those recipes
in the existing fixture; it is not a third benchmark or performance acceptance.

## Current workload summary

Headline results first; detailed methods and caveats follow. All rows are
single-host samples on the 16-core laptop unless noted, not a fleet capacity
claim.

| Workload | Transport | Headline results |
| --- | --- | --- |
| Speed of light (`just sol`) | S3 HTTP | 256 MiB GET **3,311 MiB/s** (83% of floor); 1 GiB GET **2,077 MiB/s** (84%); 256 MiB PUT **384 MiB/s** (83%). |
| 20M-row DuckLake row-group tuning (`just bench tuning`) | DuckDB `httpfs` | DuckDB 2.0.0, ~823 MiB stored: 122,880 rows/group loads in **8.79 s**, projected full scan **159 ms** (**130.6 MiB** read), 1% range **66 ms** (**3.4 MiB** in 7 GETs), point lookup **64 ms** (**1.2 MiB** in 3 GETs). |
| 1.14 GB LINZ Wellington orthophoto (`just bench ortho`) | DuckDB `httpfs` | 16 tiles/row-group: point lookup **25 q/s** (1w, **39 ms** p50, **4.1 MB** in 10 GETs) and **111 q/s** (16w); 9% central region **0.92 s** (**117 MiB**). |
| Overture Maps `2026-10-21.0` + STAC (`just overture`) | DuckDB `httpfs` | Anonymous public S3, Perth CBD place+building: direct `us-west-2` median **3.62 s** (first pass **7.36 s**), cold 1 MiB range fill **7.56 s**, warm PostgreSQL `s3c_` **79 ms** (**46×**), warm 256 MiB gateway block cache **57 ms** (**64×**). Cached **22.1 MiB** = **1.8%** of **1.23 GiB** source Parquet. |
| Official-base `pgvs3-duckdb:2.0.0` server (`just duckdb-integration-bench`) | DuckDB Quack | 1M-row DuckLake (42.7 MiB): projection **13.0 ms**, 1% range **11.9 ms**, point lookup **16.9 ms**. 1.14 GB LINZ orthophoto: z21 point **38.2 ms**, z16 overview **31.7 ms**, 9% region (704 tiles) **291.2 ms**. 5k-row spatial RTree **2.96 ms** vs **10.17 ms** lake scan; 5k×32 vector HNSW **3.19 ms** (10/10 recall) vs **13.26 ms** lake scan (earlier sample; see the dated section below). |
| 100-snapshot Iceberg (`just iceberg`) | DuckDB `httpfs` | 20–60 ms origin: warm `count(*)` **6 ms** vs **115–316 ms** direct; 100-snapshot group-by **67 ms** vs **434–1,079 ms** direct. |
| Trino 484 + Nessie (`just trino`) | Trino S3 | 20 ms origin: standalone paired 3 GiB cache beats Alluxio across all 7 queries (**36–520 ms** vs **78–529 ms**, paired ratios **0.46–0.97**). |

Two benchmark harnesses are included in the repository:

- **`just load`** runs `test/loadgen` directly against the HTTP gateway to
  measure raw S3 + PostgreSQL throughput and latency, or drives paired acceptance
  through the existing `compare.py` with isolated before/after fixtures.
- **`just bench`** runs DuckDB 2.0 and DuckLake through `httpfs` to measure
  end-to-end client performance. Its opt-in `differential` part also measures
  Differential's native-storage client through the C++ S3 SDK; `just differential`
  is the shortcut. See [provisioning and comparison contracts](differential.md).
  [Recorded findings](differential-results.md) separate client performance from
  gateway retry avoidance and retain unsupported workloads explicitly.
  `just object-storage` uses the same cohort workflow for the signed SlateDB
  ObjFS client; [its method](object-storage.md) keeps cache/thread tuning,
  live-writer reads, sealing costs and read-only endpoint limits distinct.
  [ObjFS results](object-storage-results.md) retain both the sealing benefit and
  the lean profile's per-path budget misses rather than declaring an aggregate win.

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
- **Sub-millisecond S3 small reads & 20.4k/s durable S3 writes:** S3 4 KiB
  `GET` reaches **35.5k req/s at 256 workers** (`25.3k` at 64 workers, `0.17 ms`
  p50 at 1 worker) and durable 4 KiB `PUT` reaches **20.4k req/s at 256 workers**
  (`17.8k` at 64 workers, `0.82 ms` p50 at 1 worker). The headline rates use
  256 workers; the latency samples use 1 worker.
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

## DuckLake 20M-row row-group tuning (`just bench tuning`)

`just bench tuning` builds a 20,000,000-row DuckLake table in two files
(~822–826 MiB stored) at four `parquet_row_group_size` values. It then measures a
projected full scan, a low-selectivity string filter, a 1% key range and a point
lookup through DuckDB 2.0.0 `httpfs`. Measured 2026-11-06 on the 16-core laptop
(`.tmp/bench-tuning-20261106/bench-tuning-2.0.0.json`, SHA-256
`4be783eb2032646c2c96bf18df33a62415257622ff0400b7bec15decf5f1a9cc`, run
`r3a5eadd925c3`; medians of the recorded repeats):

| row_group_rows | load_s | stored_mib | full_scan | string_filter | range_1pct | point_lookup |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 30,720 | 9.295 | 826.1 | 0.256 s / 654 GETs / 132.8 MiB | 0.509 s / 654 GETs / 616.0 MiB | 0.093 s / 17 GETs / 2.4 MiB | 0.069 s / 3 GETs / 0.5 MiB |
| 122,880 | 8.785 | 823.0 | 0.159 s / 165 GETs / 130.6 MiB | 0.433 s / 328 GETs / 615.8 MiB | 0.066 s / 7 GETs / 3.4 MiB | 0.064 s / 3 GETs / 1.2 MiB |
| 491,520 | 8.267 | 822.1 | 0.129 s / 84 GETs / 130.1 MiB | 0.430 s / 328 GETs / 615.7 MiB | 0.074 s / 4 GETs / 4.5 MiB | 0.066 s / 4 GETs / 4.5 MiB |
| 1,966,080 | 8.252 | 821.6 | 0.153 s / 74 GETs / 129.9 MiB | 0.489 s / 318 GETs / 615.3 MiB | 0.121 s / 11 GETs / 17.9 MiB | 0.103 s / 11 GETs / 17.9 MiB |

Query cells are `median_s / GETs / MiB read` from the client.

The trade-off is pruning versus request count, not a single "best" group size.
`30,720` rows/group gives the sharpest point and range pruning (0.5 MiB and
2.4 MiB) but pays the most GETs (654) and the slowest full scan (256 ms).
`122,880` is the default and a balanced point: 3 GETs for a 1.2 MiB point lookup
and 7 GETs for a 3.4 MiB range, with a 159 ms full scan. `491,520` has the
fastest projected scan here (129 ms, 84 GETs) but reads 4.5 MiB for a point.
`1,966,080` minimizes GETs (74 for the full scan) yet pulls 17.9 MiB in 103 ms
for a single point and 121 ms for a 1% range. Sort order and filter selectivity
still matter more than group size alone: the string filter reads roughly the
whole table at every setting.

## RaQuet raster lookups (`just bench raquet`)

[RaQuet](https://www.raquet.io/) stores rasters in Parquet: one row per
256x256 tile keyed by QUADBIN cell, so a point lookup through DuckDB's
`raquet` extension is a footer read plus one row group, not a scan. The part
generates a deterministic 58.9 MB, 1,024-tile float32 raster
(`test/raquet_fixture.py`, 8 tiles per row group), checks lookups against the
generator, then times 400 random point lookups through gateway B, plus one
region query and a full scan. Set `PGVS3_READ_CACHE_BYTES` to measure the
gateway read cache.

Measured 2026-11-03 on the 16-thread Intel Core Ultra laptop (±15% between runs):

| Workload | Read cache | Result | p50 | p99 |
| --- | --- | ---: | ---: | ---: |
| Point lookup, 1 worker | off | 58 q/s | 17.2 ms | 22.1 ms |
| Point lookup, 1 worker | 1 GiB | 69 q/s | 14.4 ms | 19.4 ms |
| Point lookup, 16 workers | off | 287 q/s | 54.3 ms | 85.7 ms |
| Point lookup, 16 workers | 1 GiB | 360 q/s | 42.6 ms | 73.3 ms |

Each lookup makes 9 ranged GETs totalling 645 KiB, with per-GET p50 of 0–3 ms,
so most of the per-query time is spent in DuckDB rather than the gateway. The
region query (9% of the area) reads 34.8 MiB in 160 GETs in about 0.3 s. Most of its bytes
come from Morton-order row groups that only partly overlap the polygon, which
is a file-layout property, not a gateway one.

## RaQuet on a real orthophoto (`just bench ortho`)

The same queries on real imagery: an N x N block of the LINZ Wellington 2021
0.075 m urban aerial sheets (CC BY 4.0, Toitū Te Whenua LINZ, AAM NZ and
Wellington City Council), RGBA uint8. `test/raquet_ortho.py` downloads the
sheets, checks each against its STAC checksum, and converts the mosaic with
CARTO's `raquet-io` 0.9.0 in a GDAL container (`deploy/bench/raquet-io/`),
keeping its default full overview pyramid, gzip and 200 tiles per row group.
The result is cached in `~/.cache/pgvs3/ortho`; `PGVS3_ORTHO_SHEETS` picks
the block (default 8) and `PGVS3_ORTHO_FILE` benchmarks any other RaQuet file.
Each part times 400 RGB point lookups at native zoom, 50 lookups per overview
zoom, a region over the central 9% and a `band_1` scan, all through gateway B
and all checked against DuckDB on the local file.

| Block | Source | RaQuet | Native tiles | Zooms | Conversion |
| --- | ---: | ---: | ---: | --- | ---: |
| 4x4 sheets (0.96 x 1.44 km) | 0.36 GiB | 1.14 GB | 7,140 | 14–21 | 3 min 20 s, one core |
| 8x8 sheets (1.92 x 2.88 km) | 1.38 GiB | 4.48 GB | 28,495 | 13–21 | 21 min, ~14 GiB RAM |

The converter runs on one core and its memory grows with the raster even with
`--streaming`, so 16x16 (18 GB) needs `--overviews none` or a larger machine.

Measured 2026-11-03 on the 16-thread Intel Core Ultra laptop, read cache off
(±15% between runs), 4x4 block:

| Workload | 200 tiles/row group (default) | 16 tiles/row group |
| --- | ---: | ---: |
| RGB point lookup, 1 worker | 18 q/s, p50 54 ms | 25 q/s, p50 39 ms |
| RGB point lookup, 16 workers | 68 q/s, p50 225 ms | 111 q/s, p50 142 ms |
| Bytes per point lookup | 22 MB in 17 GETs | 4.1 MB in 10 GETs |
| Overview lookup, per zoom 14–20 | 20–26 q/s, 23–28 MB | 31–38 q/s, 2.7–3.8 MB |
| Region, central 9% | 1.25 s, 124 MiB | 0.92 s, 117 MiB |
| `band_1` scan, all zooms | 2.0 s, 364 MiB | 1.7 s, 365 MiB |

DuckDB on the local file (page cache) takes 28 ms per point lookup, 1.18 s for
the region and 1.39 s for the scan with the default layout. The region is
bound by tile decoding in DuckDB; point lookups pay about 26 ms to move 22 MB,
because each lookup fetches one row group's column chunk for each of three
bands. Row-group size, not the gateway, is the main lever for raster lookups.

On a cold read-through cache with the contract's 64 KiB `--range-cache-chunk`,
one 22 MB point lookup makes about 315 origin requests, and a cold scan holds
S3 admission slots long enough that the default 1 s `--s3-queue-wait` returns
503 `SlowDown` to some of DuckDB's 64 parallel GETs (DuckDB retries them; with
`PGVS3_S3_QUEUE_WAIT=30s` there are none). Raster origins want a larger range
chunk. These cold figures predate a fix to the test origin proxy, whose 5-entry
listen backlog stalled bursts of new connections for about 1 s; they overstate
cold time and should be re-measured.

## Anonymous Overture Maps + STAC (`just overture`)

`just overture` starts the disposable PostgreSQL 18 fixture, resolves the Perth
CBD files through the public STAC collections index, stores an S3 origin with
empty credentials, lists a release prefix through `ListObjectsV2`, and measures
the same bounding-box + attribute query directly and through the cache tier.
DuckDB runs with `enable_external_file_cache=false` and
`parquet_metadata_cache=false`, so the client adds no file or Parquet metadata
cache of its own.

The measured release is `2026-10-21.0`; STAC discovery took **137 ms** and
returned one file per theme:

| Theme | Object | Size | Row groups |
| --- | --- | ---: | ---: |
| `places/place` | `part-00013-6c366648-...-c000.zstd.parquet` | 719,333,214 B | 256 |
| `buildings/building` | `part-00330-d83f5528-...-c000.zstd.parquet` | 567,357,602 B | 256 |
| **Total** | | **1,286,690,816 B (1.23 GiB)** | |

The query bbox is `[115.80, -32.00, 115.92, -31.90]` (Perth CBD). Places filter
`basic_category = 'restaurant'` (**1,637** rows); buildings filter
`class = 'residential'` (**3,756** rows, mean height **5.217 m**). The S3 origin
uses `overturemaps-us-west-2` in `us-west-2` with no access key, secret key or
session token; the gateway's `ListObjectsV2` over
`release/2026-10-21.0/theme=places/type=place/` returned the expected keys.

Measured 2026-11-10 on the 16-core laptop (single host, single session; direct
3 runs, warm states 5 runs):

| State | Cache / transport | Median | GETs / response bytes | Gateway PostgreSQL |
| --- | --- | ---: | ---: | --- |
| Direct | DuckDB to `s3://overturemaps-us-west-2/...`, `us-west-2`, unsigned | **3,616.8 ms** (3,116.5–7,357.0; first **7,357.0**) | 24 / 6,268,891 | none |
| `pgvs3` cold fill | 1 MiB aligned range chunks, `--read-cache-bytes 0` | **7,562.5 ms** (one pass) | 24 / 6,268,891 | 157 main / 28 chunk pool |
| `pgvs3` warm shared | PostgreSQL `s3c_`, `--read-cache-bytes 0` | **79.1 ms** (76.5–86.1) | 24 / 6,268,891 | 270 main / 270 chunk pool |
| `pgvs3` warm block | 256 MiB gateway cache, zero-copy | **56.7 ms** (53.7–62.8; warm-up **106.3 ms**) | 24 / 6,268,891 | 261 main / **0** chunk pool |

Warm shared `s3c_` reads are **45.7×** faster than the direct median; the warm
256 MiB gateway block cache is **63.8×** faster and records **160** read-cache
hits, **0** fills and **0** `chunkPool` acquires during the timed runs. The
block cache bypasses bytes only: every request still resolves metadata in
PostgreSQL (261 main-pool acquires over 120 GETs in the timed runs). A cold
1 MiB fill is **7.56 s**, essentially the first direct pass (**7.36 s**); the
direct median is lower because later runs reuse client DNS/TLS/HTTP state.
Range alignment makes the cold fill slower than the direct median while still
downloading a small subset of the source.

`pgvs3 stat --kind cache --name overture` after the cold fill records
**23,156,736 logical bytes (22.1 MiB)**, **24 objects**, **2,872** 32-row chunk
blocks and **2** origin metadata rows (24.9 MiB physical). That is **1.80%** of
the **1.23 GiB** source Parquet: aligned 1 MiB chunks over-fetch about 3.7× the
**6.27 MiB** the query itself requested, but skip **98.2%** of the two files.
Warm shared and warm block runs leave the shared cache size unchanged.

Report: `.tmp/verify/analytics.hMbmhY/overture.json`, SHA-256
`e7557a617a3fc478227689941de3ea93b940a8f1a53246bd61c8fbe15e597689`.
The report retains every run, per-query timings and bytes, gateway metric
deltas, cache statistics and the resolved STAC keys.

## Official-base DuckDB 2.0 + Quack container (`just duckdb-integration-bench`)

`just duckdb-integration-bench` runs the prebuilt `pgvs3-duckdb:2.0.0` image
(official DuckDB 2.0.0 distroless base, `sha256:40e76ebda4abf91529e0c1978bca50c6887714b69348b5febfe940c858c8dfd1`)
against the same PostgreSQL database and `PGVS3_ENDPOINT` gateway, with clients
using native Quack over `:9494`. Measured 2026-11-06 on the 16-core laptop
(`.tmp/duckdb-large-bench-20261106/compute.json`, SHA-256
`b5d355cb3f14fdab4509940d2d5fa094d59320ca2833a2d36cba864b0f988388`); these are
single-host Quack round-trip samples, not storage-throughput claims.

The 5,000-row sample compares a lake scan with a native table plus a local index:

| Sample | Lake scan | Native materialization | Native query |
| --- | ---: | --- | ---: |
| Spatial, 5,000 points | 17.3 ms warm (44.7 ms first) | copy 9.9 ms + RTree 4.1 ms | 4.07 ms warm (3.8 ms first), 1,521 rows |
| Vector, 5,000 × 32 dimensions | 18.3 ms warm (53.7 ms first) | copy 27.0 ms + HNSW 42.3 ms | 4.79 ms, 10/10 recall at 10 |
| FTS, 20 documents | unsupported on DuckLake | native build 44.8 ms | 12.9 ms |

An earlier official-base sample (`.tmp/duckdb-official-perf-20261106T020735Z/compute.json`,
SHA-256 `ebfc41428689a21d321ba083c15767c9597885a426ee119c1b69f257bcaf3471`)
measured lower 5k-row medians (RTree 2.96 ms vs 10.17 ms lake scan; HNSW
3.19 ms vs 13.26 ms lake scan). Absolute times vary between runs; compare ratios
within one session. Incremental INSERT and DELETE were visible immediately,
while UPDATE left the FTS index stale in DuckDB 2.0.0.

### C clients x W worker pool benchmark

`just duckdb-pool-bench` measures 45 cells: `W = 1, 2, 4` native Quack workers
× `C = 1, 4, 8, 16, 32` independent host clients. Each client holds one
persistent `ATTACH 'quack:...'` and routes to worker `i mod W`; every worker is
`pgvs3-duckdb:2.0.0` with `DUCKDB_THREADS=4` and `DUCKDB_MEMORY_LIMIT=1GB`.
One dedicated gateway runs at `--pool-max 32`; it is restarted with
`--read-cache-bytes 0` and then `268435456` (256 MiB). The fixture is a
1,000,000-row DuckLake `lake.main.events` (42.7 MiB, one Parquet object) plus a
5,000-point native RTree rebuilt in each worker. Each client disables its own
external-file and Parquet metadata caches, runs one warm-up query, then 20 timed
`FROM q.query(...)` round trips. `q/s` is total timed statements divided by wall
time; `C=1` p99 is the maximum of its 20 samples. PostgreSQL activity is sampled
from `pg_stat_activity` by `application_name` (`pgvs3_pool` versus
`ducklake_pool`); active counts miss sub-sample queries, so total connections are
the capacity figure.

The two single-client samples above are separate 1-query-at-a-time runs, not two
capacity points or a range: the dated 2026-11-06 pass measured RTree 4.07 ms and
lake 13.0/11.9/16.9 ms, while the earlier pass measured RTree 2.96 ms and HNSW
3.19 ms. The pool table below is a different experiment (aggregate `C × W`
throughput, repeated 20-query cells) and neither supersedes nor averages those
single-client medians. Its `W=1, C=1` lake p50 (14.94 ms cache off) is close to
the 13.0 ms single-client projection only because the same host and 1M-row shape
were used; do not compare percentiles across the two protocols.

**Lake range/aggregation, `--read-cache-bytes 0`** (`SELECT group_id, count(*),
sum(val) ... WHERE group_id BETWEEN 100 AND 199 GROUP BY group_id`):

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk conns | DuckLake act/total |
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

**Lake range/aggregation, `--read-cache-bytes 256 MiB`.** Warm-up fills the
block cache; every timed cell recorded zero `chunkPool` acquires and zero fills:

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk conns | DuckLake act/total |
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

**Native 5,000-point RTree in each worker** (pure Quack RPC + worker CPU, no
gateway request in the timed path):

| `W` | `C` | q/s | p50 ms | p95 ms | p99 ms | pgvs3 act/total | chunk conns | DuckLake act/total |
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

What the run says:

- **Native RPC scales to two workers, then hits host CPU.** At `C=32`, W=1
  delivered 1,271 q/s, W=2 2,353 q/s (1.85×), and W=4 2,038 q/s. Four workers ×
  four threads fill all 16 cores; adding workers beyond that raised p99 instead
  of throughput.
- **A lake scan through one gateway plateaus near 230–285 q/s.** W=1/2/4 at
  `C=32` delivered 181/247/231 q/s cache off and 222/276/254 q/s with the 256 MiB
  cache. Worker count does not create PostgreSQL chunk throughput.
- **The 256 MiB cache removed timed PostgreSQL chunk traffic.** Cache-off cells
  acquired the chunk pool 10 times per lake query (one per GET) and kept 31–32
  chunk connections; cache-on cells recorded 0 acquires and 7–8 connections.
  Median q/s at `C >= 8` rose from 212 to 254 (+20%), with no help for one-pass
  scans or the single-gateway ceiling.
- **Every worker count needs catalog headroom.** DuckLake connections grew from 24
  (W=1) to 56 (W=4), independent of pgvs3 pools; the gateway's own total reached
  62. Size `max_connections` from the sum in
  [operations](operations.md#sizing-a-quack-worker-pool), not from `P` alone.

Report: `.tmp/verify/pool.mH6Prx/quack_pool.json`, SHA-256
`3add9c71c3250a48bab62747535c588ce6a1f43ae7719a32213f100705bb7429`.
Single host, one session; non-monotonic cells (for example W=4, C=8) are run
noise, not a scaling claim.

The larger pass adds a 1,000,000-row DuckLake table and the 1.14 GB LINZ
Wellington orthophoto:

| Dataset | Stored / upload | Timings |
| --- | ---: | --- |
| 1M-row DuckLake `large_events` | 42.7 MiB, load 677 ms | projection 13.0 ms; 1% range 11.9 ms; point lookup 16.9 ms |
| 1.14 GB LINZ orthophoto, 7,140 tiles | upload 3.94 s at 275 MiB/s | metadata 5.6 ms; z21 point 38.2 ms; z16 overview 31.7 ms; 9% region (704 tiles) 291.2 ms; region stats 983.8 ms |

**DuckLake 1.1 does not support indexes directly.** The native RTree, HNSW and
FTS measurements operate on materialized native DuckDB tables, which are
snapshots: they must be rebuilt when lake freshness matters. The lake table
remains the durable copy.

## Iceberg through the cache tier (`just iceberg`)

Iceberg on S3 is latency-bound: planning reads `metadata.json` and the manifest
list, then each manifest referenced by the current table state, and each is a
separate object-store round trip. `just iceberg` writes a table with pyiceberg
0.12 (SQLite catalog, identity-partitioned by day, one 20,000-row append and one
snapshot per day, `PGVS3_ICEBERG_COMMITS=100`) straight to the origin. DuckDB
2.0's `iceberg` extension then reads it in three ways: directly from the origin
through a proxy that adds `PGVS3_ICEBERG_ORIGIN_MS` to every request, through
gateway B on a cold cache, and through gateway A warm. The cache bucket maps
1:1 onto the origin bucket (`strip_bucket`), so the table's absolute `s3://`
paths resolve unchanged; clients only switch endpoints. Three more appends then
go through gateway A and are read back through B. Every role must match the
generated data, and the warm and write-through reads must cause zero origin
reads.

Measured 2026-11-03 on the 16-thread Intel Core Ultra laptop (median of 5
runs; cold is the first run on gateway B; default `--fill-concurrency 8`):

| Query (client GETs) | Direct, 20 ms | Cold, 20 ms | Warm | Direct, 60 ms | Cold, 60 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| `iceberg_snapshots` (3) | 95 ms | 94 ms | 6 ms | 255 ms | 208 ms |
| `count(*)` (4) | 115 ms | 34 ms | 6 ms | 316 ms | 78 ms |
| One-partition filter (7) | 190 ms | 93 ms | 12 ms | 510 ms | 208 ms |
| Group-by over all 100 snapshots (304) | 434 ms | 816 ms | 67 ms | 1,079 ms | 1,934 ms |
| pyiceberg append | 500 ms | | 160 ms | 1,240 ms | 245 ms |

Warm reads don't depend on origin latency: planning-bound queries are 16–50x
faster and the full scan 6–15x. Write-through appends are 3–5x faster although
every file (all under the 8 MiB write-back threshold) still reaches the origin
before its PUT returns; the writer's own reads and existence checks hit the
cache. With zero injected latency, direct and warm reads are within 25 ms of
each other, so the proxy itself adds little.

A cold full scan is about 2x slower than reading the origin directly, because
the default eight concurrent fills per tier and per process queue DuckDB's
parallel GETs. With `--fill-concurrency 32` on both `origin set` and `serve`,
the cold scan at 20 ms fell from 855 ms to 531 ms against 428 ms direct.
Lakehouse origins want a higher fill concurrency.

## Trino and Nessie through the cache tier (`just trino`)

`just trino` runs Trino 484 against a Nessie 0.109 catalog whose JDBC version
store lives in the same PostgreSQL, configured as in the
[Nessie Trino guide](https://projectnessie.org/iceberg/trino/) with Trino's
native S3 file system pointed at each endpoint. `lineitem` is TPC-H SF1
committed month by month (78 Nessie commits, 78 files / 136 MiB, partitioned
by month); `big` is one CTAS of SF10 (10 files / 1,448 MiB, about 180 MiB
each). Each catalog reads the same tables directly from a 20 ms origin, cold
through gateway B, warm through gateway A, and through Trino's own Alluxio
file cache; every result must match Trino's `tpch` connector, and warm and
write-through reads must cause zero origin reads across all repetitions. By
default, `just trino` leaves `PGVS3_READ_CACHE_BYTES=0`, so cache gateways serve
the shared PostgreSQL chunk rows; set it to `3221225472` to opt into the bounded
3 GiB gateway-local read cache. That budget bounds retained cache residency, not
process RSS, request/response buffers or other in-flight bodies.

### 2026-11-05 standalone paired run: pgvs3 vs Alluxio

This is the strict standalone comparison, not a layered or hybrid path. pgvs3
used `via_a` with its own 3 GiB gateway-local read cache and both Trino
file-system caches disabled (`iceberg.metadata-cache.enabled=false`, no Alluxio
volume). Alluxio used its full 4 GB tmpfs cache as a separate catalog. Both read
the same SF10 tables with a 20 ms origin, pool 8, fill 8 and a 1 MiB range
chunk. The run took 15 ABBA blocks per query, flipping the starting provider each
block (`PG, Alluxio, Alluxio, PG`, then the reverse), for 30 samples/provider/
query. Profile collection was off. The report and analysis are archived under
`.tmp/verify/standalone-paired.20261105T151816Z/`; `trino.json` SHA-256 is
`2e28bf1f53935f55b7d7222d388858be0218034b1ffbe96a0ef8a99e12cc3cc0`.

Raw coordinator medians are not the paired block ratios. The ratio and interval
below use the geometric mean of the 15 PG/Alluxio block-mean ratios, with 50,000
whole-block bootstrap resamples, seed `20261105 + query index`, and a
Bonferroni-adjusted 99.2857% interval across seven queries:

| Query | PG median ms | Alluxio median ms | Paired ratio | Adjusted 99.2857% CI | PG block wins |
| --- | ---: | ---: | ---: | ---: | ---: |
| `count(*)` | 53 | 97 | 0.5521 | [0.5395, 0.5695] | 15/15 |
| One month | 36 | 78 | 0.4577 | [0.4429, 0.4733] | 15/15 |
| One `orderkey` | 78 | 115 | 0.6745 | [0.6583, 0.6923] | 15/15 |
| TPC-H Q1 | 100.5 | 137 | 0.7449 | [0.7242, 0.7650] | 15/15 |
| `big` one `orderkey` | 96 | 133 | 0.7318 | [0.7137, 0.7532] | 15/15 |
| `big` one month | 174 | 193.5 | 0.8904 | [0.8662, 0.9169] | 15/15 |
| `big` Q1 | 520 | 529 | 0.9689 | [0.9455, 0.9919] | 12/15 |

All seven intervals are below 1 within this session, but `big` Q1 has only a
~3% paired margin and just 12/15 block wins. An earlier unpaired run also
produced lower Alluxio times, so this does not attribute all movement to the
new write path; ABBA only controls within-session drift. This is one host and
one session, not production or fleet proof. pgvs3's 210 paired samples had 0 GET and 0 HEAD
origin requests; Alluxio's 210 had 0 GET and 2 HEAD each. The repeated HEAD is a
real metadata-length probe, so the comparison includes pgvs3's metadata/cache
advantage, not only byte-cache throughput. Alluxio is not a layer in front of
pgvs3.

Configured memory was 3 GiB per pgvs3 cache gateway (6 GiB across two) versus
Alluxio's 4 GB per Trino worker on tmpfs; gateway A reported about 608 MiB
active at the snapshot. Both caches fit the dataset; this is not a fleet
memory, cost or equivalence claim. A separate allocation check reduced
per-1 MiB-part allocations from 32,872 to 184 B/op and per-8 MiB-part from
33,448 to 352 B/op; that microbenchmark is not proof of the query change.
Earlier profiling of this path was mostly kernel/socket time (78%) and the
allocation delta sample was empty, so CPU/alloc attribution remains limited.

The 2026-11-05 shared-PostgreSQL-only table below remains the cache-disabled
baseline. The 2026-11-03 table is historical and local-memory-assisted.

### 2026-11-05 shared-PostgreSQL-only run

`just trino` now defaults the gateway read cache off. The complete report at
`.tmp/verify/analytics.actpBG/trino.json` (SHA-256
`031354c74575ea2dfca09231d4d75f9a0ab23e6ac98602fdf5abb1e9dcff95b2`) used
cache 0, pool 8, gateway/origin fill 8, repeat 3, 20 ms origin latency, a
1 MiB range chunk and SF10; `tuned_months` is 78. It was built from `85d58e9`
plus a dirty working tree, not released v0.6; binary SHA-256
`4991ccda932319e76ccad1b6b836dd82ac4eaef31c8b2a5c6329784cb765fbc4`.
Coordinator milliseconds are medians of three executions; cold is the first
execution in the sequenced run, not a fresh cache per query:

| Query | Direct | Cold, first | Warm, median | Alluxio, median | Direct, MemoryBlobCache | Warm, MemoryBlobCache |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `count(*)` | 223 | 457 | 71 | 112 | 57 | 59 |
| One month | 169 | 118 | 54 | 85 | 40 | 40 |
| One `orderkey` | 318 | 644 | 131 | 148 | 86 | 74 |
| TPC-H Q1 | 348 | 144 | 174 | 161 | 100 | 100 |
| `big` one `orderkey` | 253 | 400 | 164 | 154 | 182 | 146 |
| `big` one month | 460 | 1,013 | 341 | 197 | 403 | 321 |
| `big` Q1 | 835 | 900 | 720 | 560 | 777 | 708 |

All origin-cache warm roles, including the coordinator-MemoryBlobCache variant,
plus tuned-warm and write-through observations had `origin_reads_total` 0.
Large scans remain slower than Alluxio (`big` one month 341 vs 197 ms; `big`
Q1 720 vs 560 ms). The two MemoryBlobCache columns use the legacy
`... trino metadata cache` role keys: Trino's coordinator MemoryBlobCache stores
engine metadata and, when coordinator and worker are co-located, small data
files that pass its 8 MiB filter. They are secondary observations, not the
strict standalone comparison. This engine cache narrows some of the warm
advantage: with it, `count(*)` is 57 ms direct against 59 ms warm. This is one
host's role comparison, not a universal near-Alluxio parity claim, an engine
ranking or a multi-cluster capacity result.

The matched 78-month tuned comparison keeps rows fixed while changing layout:
warm `count(*)` 71 → 38 ms, `big` one month 341 → 149 ms, but `big` one `orderkey`
164 → 257 ms. The rebuild helps these scans and hurts that lookup; it is not a
general layout winner. The 2026-11-03 layout table below remains historical and
changed its row cohort.

### 2026-11-03 local-memory-assisted run (historical)

The 2026-11-03 table below used that 3 GiB local cache on both cache gateways,
so its cold and warm columns are local-memory-assisted, not
shared-PostgreSQL-only latency. Its cold column also predates the synchronous
range-fill publication fix, so those first reads include the old fill behavior.
Cache gateways otherwise ran `--pool-max 8` and a 1 MiB `--range-cache-chunk`
(the origin gateway has no read cache). New runs record the effective gateway
cache, pool and fill settings, source/binary/host identity and engine image tags
in `trino.json`.

Measured 2026-11-03 on the 16-thread Intel Core Ultra laptop, with the 3 GiB
gateway-local cache enabled (coordinator milliseconds, median of 3; cold is the
first run on gateway B; the last two columns keep Trino's coordinator
MemoryBlobCache, the legacy metadata-cache role keys, which can include small
data files when coordinator and worker are co-located). Cold here means the
first execution in a sequenced run, not a cache reset before every query; later
queries can reuse fills published by earlier ones. This table is historical and
local-memory-assisted; it is not a current shared-PostgreSQL result.

| Query | Direct | Cold | Warm | Alluxio | Direct, MemoryBlobCache | Warm, MemoryBlobCache |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `count(*)` | 232 | 457 | 74 | 122 | 61 | 68 |
| One month | 173 | 125 | 62 | 95 | 44 | 43 |
| One `orderkey` | 367 | 665 | 145 | 153 | 98 | 98 |
| TPC-H Q1 | 385 | 138 | 168 | 177 | 105 | 116 |
| `big` one `orderkey` | 274 | 423 | 144 | 161 | 199 | 126 |
| `big` one month | 531 | 1,034 | 250 | 239 | 436 | 236 |
| `big` Q1 | 916 | 784 | 652 | 638 | 879 | 652 |

In that local run, warm reads were 1.4–3.1x faster than direct and roughly
level with Alluxio's local tmpfs cache; the Alluxio comparison is within this
host's run-to-run noise, not a general engine ranking. A first cold read costs
up to 2x direct for selective lookups and the large one-month scan, which wait
on fills, but is faster than direct for the one-month filter and both Q1 scans.
Month-by-month `INSERT`s in that run took 1.15 s through the tier against
1.18 s direct. These use different monthly batches, so they do not isolate
write-path overhead. The harness does not run a matched DuckLake workload;
these Trino-internal role comparisons do not rank Trino against DuckLake.

Run-to-run noise on the laptop is 20–30% even for roles that never touch the
cache gateways, so only ratios within one run are meaningful. One laptop trial
raising `--pool-max` from 8 to 32 changed no ratio; that single observation does
not prove the pool had spare capacity or replace a pool-wait/saturation
measurement. A 4 MiB range chunk moved time from warm to cold reads without a
net gain.

Table layout matters too. The current benchmark rebuilds the 78-month build
cohort with one CTAS, creating a new current table snapshot whose current
metadata points at one manifest; historical Nessie commits remain, but planning
does not replay them. It compares direct/cold/warm on those same 78 months
(recorded as `tuned_months` in `trino.json`), holding rows constant, and gives
`big` the same `sorted_by = ARRAY['shipdate']` rewrite as before.

Historical 2026-11-03 layout run (not an isolated comparison: it rebuilt after
the six write-through/appended months, so layout and row cohort changed
together):

| Query | Direct | Cold | Warm | Warm, original layout |
| --- | ---: | ---: | ---: | ---: |
| `count(*)` | 125 | 155 | 40 | 74 |
| One `orderkey` | 269 | 736 | 85 | 145 |
| TPC-H Q1 | 294 | 90 | 110 | 168 |
| `big` one `orderkey` | 429 | 807 | 189 | 144 |
| `big` one month | 333 | 518 | 105 | 250 |
| `big` Q1 | 737 | 818 | 453 | 652 |

In that historical run, the one-snapshot rebuild improved most `lineitem`
queries, but not every query on every endpoint: cold one-`orderkey` rose from
665 to 736 ms. It cuts `count(*)` origin reads from 81 to 4. Sorting by
`shipdate` lets row-group statistics prune the one-month scan, 2.4x faster warm,
but loses the `orderkey` clustering the original CTAS had, so the key lookup
gets slower. Sort by the columns that queries filter on. Current runs keep the
78-month cohort fixed, so use their `tuned` records for layout conclusions.

Three implementation details shape the warm path. `ReadMany` serves files
whose bytes are in the gateway read cache from memory (metadata still comes
from PostgreSQL) and splits the rest across chunk connections. Range responses
stream the cached chunk slices instead of copying them into one buffer; a CPU
profile had `memmove` and `memclr` at 45% of the warm gateway's time. Range
fills now read each missing run from the origin once, publish each aligned
chunk to PostgreSQL synchronously with bounded store concurrency, and only then
assemble the response. Synchronous publication is a correctness change, not a
measured warm gain. A successfully published fill is immediately visible to
other gateways sharing PostgreSQL; store errors are best-effort, logged, and
the response still uses the fetched bytes.

The first runs of this benchmark found two connection-pool deadlocks that
only a burst of concurrent large reads exposes. Cold `big` one-month took
139 s: range-chunk writers held a main-pool connection while `nextFileID`
waited on the pool for another, so the fills stalled until their 2-minute
timeout. Separately, a multi-piece read held its guard connection while its
workers waited on the pool, which stalled the native origin for over 2 minutes.
File IDs are now allocated on the writer's own session, and the guard session
does the read's first share of work. A warm `big` lookup also lost 2 s to
`ReadMany` sorting wide chunk rows into temporary files; it now assembles rows
by offset without `ORDER BY`.

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
| 4 KiB GET / PUT p50 (1w) | 0.17 ms GET, 0.82 ms PUT (0.47 / 1.37 ms on 6 vCPU) | Consistent single-digit ms |
| 4 KiB GET throughput | 35.5k/s at 256w (25.3k/s at 64w); 9.8k/s at 256w on 6 vCPU | 200k/s default, 2M/s max per bucket |
| 4 KiB PUT throughput | 20.4k/s durable at 256w (17.8k/s at 64w); 3.7k/s at 64w on 6 vCPU | 100k/s default, 200k/s max per bucket |
| 8–64 MiB GET | 1.1–3.45 GiB/s (0.3–1.1 GiB/s on 6 vCPU) | Network/concurrency-bound; elastic |
| 64 MiB PUT | 188–458 MiB/s (86–171 MiB/s on 6 vCPU) | Elastic, distributed |
| Capacity | 32 TiB per bucket; ~17.4 TB per object (at most 100 namespaces per database) | Unlimited storage; 50 TB per object |
| Availability / durability | PostgreSQL replication/backups chosen by operator | 99.95% designed (99.9% SLA); 11 nines in one AZ |
| Price model (`us-east-1`) | Hardware + gp3 ($0.08/GB-mo); $0 request/transfer fees | $0.11/GB-mo + $1.13/M PUT + $0.03/M GET + $3.80/TB R+W |

### Top 6 takeaways

1. **Latency:** pgvs3 serves a 4 KiB `GET` in `0.17 ms` and a durable `PUT` in
   `0.82 ms` p50, ~10× faster than S3 Express's single-digit milliseconds.
2. **Small-object reads:** pgvs3 peaks at `35.5k GET/s` at 256 workers
   (`25.3k` at 64 workers) on one 16-core node, ~6× below S3 Express's `200k/s`
   default bucket quota (`2M/s` max).
3. **Small-object writes:** pgvs3 reaches `20.4k` durable 4 KiB `PUT/s` at 256
   workers (`17.8k` at 64 workers), ~5× below S3 Express's `100k/s` default
   quota (`200k/s` max).
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
just bench s3      # DuckDB 2.0 httpfs matrix (.tmp/pgvs3/bench.json)
just bench tuning  # 20M-row DuckLake row-group tuning
just bench raquet  # RaQuet raster point lookups; PGVS3_READ_CACHE_BYTES=... for the cache
just bench ortho   # the same on a real LINZ orthophoto; PGVS3_ORTHO_SHEETS=4|8|...
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
