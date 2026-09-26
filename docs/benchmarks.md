# Benchmark history

Dated measurements and comparisons from the pgvs3 README. These runs do not
all use the current read path, cluster topology or DuckDB version; read each
setup before comparing numbers.

The AWS and kind GET and analytics throughput numbers in the historical
sections below predate the snapshot-protected multi-query read path. No new
full-scale A/B has been run for that change.

## Current-path quick check

### Quick current-path check, 2026-09-26

A release build against PostgreSQL 18 in an isolated local Docker container,
with two 64 MiB objects and warm caches. The gateway and client used loopback
HTTP; this is a small read-path sanity check, **not** a re-run of the EC2/Aurora
or DuckLake workloads. The 8 MiB range is one query; the 16 MiB range uses
multiple queries in one repeatable-read snapshot.

| GET range | Clients | Samples | p50 | Aggregate throughput |
| --- | ---: | ---: | ---: | ---: |
| 8 MiB | 1 | 37 | 5.40 ms | 1,292 MiB/s |
| 16 MiB | 1 | 18 | 12.04 ms | 1,306 MiB/s |
| 8 MiB | 4 | 40 | 8.46 ms | 3,332 MiB/s |
| 16 MiB | 4 | 20 | 14.57 ms | 3,748 MiB/s |

These quick samples show no obvious local throughput collapse at the
multi-query boundary. They are not a same-rig A/B against the old read path and
cannot establish whether the historical AWS GET or analytics numbers still
hold. Re-run those workloads before using them as current performance claims.

## Historical runs

### AWS rig, 2026-09-25

EC2 c7gn.2xlarge running DuckDB 2.0 (pre-release) and the gateway, Aurora
Serverless v2 (PostgreSQL 18.6) in the same AZ. DuckDB's file cache is off so
every read goes through the gateway.

| Measure | Result |
| --- | --- |
| ClickBench, 10% of `hits` (43 queries) | ~7.4 s per pass; load 8.1 s |
| SpatialBench SF1 (Q1–Q7) | ~5.6 s per pass; load 10.1 s |
| GET p50, one client | 64 KiB 0.4 ms · 1 MiB 1.7 ms · 8 MiB 13.4 ms |
| GET throughput, 8 clients | ~3.8 GiB/s |
| Aurora round trip (`SELECT 1` over TLS) | 0.12 ms |

Where the time goes: about 80% of a pass is DuckDB's own compute (the same
queries served from DuckDB's cache). Reads are network-bound: one connection
moves ~650 MiB/s (EC2's single-flow cap) and the instance ~3.8 GiB/s. The
gateway adds ~0.25 ms per request, and Aurora spends most of its time waiting
to send (`Client:ClientWrite`).

### kind cluster, 2026-09-25

Three-node kind on a 16-core / 62 GiB workstation: PostgreSQL 18 in-cluster
(single instance, 1 GiB `shared_buffers`), DuckLake over pgvs3, Quickwit
0.9.1 single node. **This run used Quickwit's S3 file-backed metastore**;
the current chart uses PostgreSQL for metadata and pgvs3 for index splits.
Caches stay on, so the warm pass is the point. Reproduce the workload shape
with `just kind-up` and `just kind-bench`; results land in
`.tmp/pgvs3/kind-bench.jsonl`.

| Measure | Result |
| --- | --- |
| pgbench scale 10, 16 clients | 1,487 tps · 10.8 ms average |
| TPC-H SF 10, 22 queries over DuckLake | 9.9 s cold · 6.1 s warm; load 111 s |
| ClickBench, 10% of canonical `hits` (10M rows, 43 queries) | 8.4 s cold · 6.3 s warm; load 10 s |
| Quickwit search, 1M logs (template repeats — see below) | p50 7.9 ms · p95 18.6 ms; 96% under 20 ms |
| GET p50, one client | 64 KiB 0.8 ms · 1 MiB 2.2 ms · 8 MiB 12.1 ms |
| GET throughput, aggregate | 8 MiB 2.25 GiB/s at 128 readers · 1 MiB 1.89 GiB/s at 64 · 64 KiB 670 MiB/s |
| HEAD, aggregate | 28k req/s at 8 readers |
| PostgreSQL floor, 256 KiB direct | 0.49 ms · 485 MiB/s |

The ceiling is the PostgreSQL path: a single 8 MiB stream moves 629 MiB/s
and 128 concurrent ones 2.25 GiB/s, where the postgres process saturates
before the gateway does. DuckDB's file cache works as designed — ClickBench's
cold pass pulled 499 MiB through the gateway and the warm pass 209 MiB.

### EC2 kind with Aurora I/O-Optimized, 2026-09-25 UTC

Three-node kind on an m7i.4xlarge (16 vCPU, 64 GiB), two pgvs3 replicas,
Quickwit core plus two search-only nodes, and Aurora PostgreSQL 18.6
Serverless v2 (2–16 ACUs, I/O-Optimized) in the same AZ. Pgvs3 objects,
DuckLake's catalog and Quickwit's metastore are separate databases on that
Aurora cluster; Quickwit split files live on pgvs3. The full run used caches
on and Quickwit's then-enabled disk split cache. The sample was `PARTS=10`
(10M ClickBench rows), TPC-H SF10, 1M Quickwit logs and an 8 GiB object seed.

| Measure | Result |
| --- | --- |
| pgbench scale 10, 16 clients | 1,763 TPS · 9.1 ms average |
| TPC-H SF10, 22 queries | 7.4 s first pass · 5.8 s warm; load 110 s |
| ClickBench 10M rows, 43 queries | 5.2 s first pass · 4.3 s warm; load 8.1 s |
| Quickwit repeated templates, 1M logs | p50 6.9 ms · p95 8.8 ms; 0 errors |
| 64 KiB GET, 32 readers | 847 MiB/s · 13.6k req/s |
| 1 MiB / 8 MiB GET throughput | ~1.4 GiB/s aggregate at 32–64 readers |
| Aurora direct floor, 256 KiB | p50 0.69 ms · 333 MiB/s |

The ~1.4 GiB/s plateau is observed end-to-end throughput, **not** proof that
Aurora itself is saturated; distinguish EC2 networking, pgvs3 and Aurora
with node/CloudWatch metrics before tuning. The full JSONL record is saved
locally under `.tmp/pgvs3/rig-out/` (not tracked in Git).

To test the cost of Quickwit's disk split cache on this same 1,005,000-log
index, we ran 400 fresh random 10%-time-window queries with the in-memory
caches enabled in every pass:

| Whole-split disk cache | p50 | p95 | Under 20 ms |
| --- | ---: | ---: | ---: |
| 20 GiB, before restart | 10.83 ms | 16.30 ms | 99.8% |
| Disabled, first pass after restart | 10.94 ms | 16.15 ms | 99.8% |
| Disabled, next pass | 11.11 ms | 16.41 ms | 99.8% |

There is no measured win for spending 20 GiB **per searcher** on disk here,
so the chart now leaves it disabled. This remote 1M-log A/B and the local
100M-log result below both support that choice, but they do not substitute
for an Aurora-backed 100M-log test. Quickwit's selective in-memory caches
remain on; putting entire splits in tmpfs would spend much more RAM without
evidence of a latency benefit.

### Aurora full rerun, 2026-09-26

The bucket/atomic-write layout on DuckDB 1.5.2 ran the same scale as the earlier EC2
kind full test: TPC-H SF10, ClickBench 10M rows, 1M Quickwit logs, 8 GiB seed,
two gateway replicas, one Quickwit core and two searchers. Disk split caching
is now off. The full result is in `.tmp/pgvs3/rig-out/` (ignored by Git).

| Measure | Current | Earlier full run |
| --- | ---: | ---: |
| pgbench scale 10, 16 clients | 1,675 TPS | 1,763 TPS |
| TPC-H 22 queries | 7.35 s first · 5.79 s warm | 7.4 s first · 5.8 s warm |
| ClickBench 43 queries | 5.36 s first · 4.28 s warm | 5.2 s first · 4.3 s warm |
| Quickwit repeated queries | p50 6.95 ms · p95 9.15 ms, 0 errors | p50 6.9 ms · p95 8.8 ms |
| 1–8 MiB GET, 8–64 readers | ~1.39 GiB/s | ~1.4 GiB/s |
| 64 KiB GET, 32 readers | 611 MiB/s | 847 MiB/s |

An additional 400 fresh 10%-time-window Quickwit queries against the loaded
1M-log index had p50 14.7 ms, p95 18.7 ms and zero errors. Small-GET
throughput merits a controlled A/B; the unchanged large-GET plateau does not
rule out a regression there. Minute-granularity writer metrics reached 16 ACUs
and 147 connections, with CPU at most 57% for a minute. Those metrics cannot
identify the 1.39 GiB/s bottleneck by themselves; EC2 network metrics were
only available at five-minute resolution.

### OpenSearch Benchmark at 100M Quickwit logs, 2026-09-26

The current PostgreSQL-metastore Quickwit 0.9.1 cluster (one indexer and two
search-only nodes, disk split cache off) indexed a fresh 100M-document OTLP
corpus through OSB 2.4's ES `create` bulk operation. The deterministic corpus
was 20,218,013,718 bytes (SHA-256
`1809bdce44079641a640f7eabee1fd922e497279a05d0dcc189501136f04b234`).
OSB reported zero bulk errors and 230k docs/s mean bulk throughput with one
ordered client; the complete corpus became searchable in 558 seconds after
generation. The adapter checks exact indexed count before measuring queries.

| Fresh severity-filtered 10%-time-window search | p50 | p99 | Throughput |
| --- | ---: | ---: | ---: |
| 1 client | 34.1 ms | 44.6 ms | 28.7 ops/s |
| 8 clients | 43.6 ms | 55.0 ms | 179.5 ops/s |

The run used Quickwit's default `stable_log` merge policy, not the tuned
`no_merge` layout from the historical native-API 100M run below. These are
OSB results from a portable custom workload, not stock-track or head-to-head
OpenSearch results. The complete OSB metrics are in ignored
`.tmp/pgvs3/rig-out/osb-100m-20260926.json`.

### Full-scale analytics on Aurora, 2026-09-26

With DuckDB 1.5.2, the canonical ClickBench `hits.parquet` loaded
**99,997,497 rows** into DuckLake in 47.9 seconds. All 43 queries passed:
34.19 seconds on the first pass and 27.21 seconds warm. Per-gateway GET counts
from this run are not reliable: containers shared PID 1, so the harness could
mix replica counters.

Sedona-SpatialBench SF10 loaded 60M trips and 454,710 zones in 27.1 seconds.
The area-of-interest subset Q1–Q3 and Q6 passed in 9.05 seconds first pass
and 3.64 seconds warm:

| AOI query | First pass | Warm |
| --- | ---: | ---: |
| Q1: nearby trip pickups | 1.87 s | 0.44 s |
| Q2: county intersection count | 2.04 s | 0.80 s |
| Q3: buffered-box monthly stats | 0.75 s | 0.59 s |
| Q6: bounding-box zone/trip stats | 4.38 s | 1.81 s |

The first full ClickBench attempt hit the benchmark Pod's 8 GiB memory limit;
the full Aurora profile now permits 24 GiB with a 16 GiB DuckDB working
limit. CI smoke keeps its smaller limit. Results live in ignored
`.tmp/pgvs3/rig-out/`; these are DuckDB-over-DuckLake query times, not
direct PostgreSQL scan timings.

### DuckDB 2.0 full-scale rerun, 2026-09-26

The same 99,997,497 ClickBench rows loaded in 50.39 seconds. All 43 queries
passed in 35.35 seconds first pass and 30.13 seconds warm. SpatialBench SF10
loaded 60M trips in 25.52 seconds; the AOI subset took 7.85 seconds first
pass and 4.54 seconds warm:

| AOI query | First pass | Warm |
| --- | ---: | ---: |
| Q1 | 2.70 s | 0.58 s |
| Q2 | 2.45 s | 1.25 s |
| Q3 | 0.77 s | 0.77 s |
| Q6 | 1.93 s | 1.94 s |

With the retained 100M-log index, a search-only OSB pass had zero errors:
fresh 10%-window searches at eight clients were p50 43.76 ms, p95 52.04 ms,
p99 57.09 ms (176 ops/s). The workloads and query times are valid, but
per-gateway byte/GET deltas in these runs could mix two PID-1 pods; those
counters are excluded from cross-version comparisons. This is one pass per
version, not a controlled multi-run DuckDB A/B.

### Aurora overwrite/abort churn, 2026-09-26

`just rig-churn` overwrote one key 128 times with 32 MiB PUTs (4 GiB written),
periodically deleted it and aborted multipart parts while another gateway read
a stable 256 KiB range. The test found no stranded chunks. Across 683 reads,
read p95 rose from 2.8 ms before churn to 18.0 ms during it; PUT p95 was
449 ms and DELETE p95 46 ms. Estimated dead chunk tuples rose from 3,655 to
26,736 immediately after churn, then fell to 2,844 after 90 seconds as
partition autovacuum counts increased by 93. Partition storage stayed near
1 GiB. This is one run, not proof of the cause of the read-latency increase;
profile pool waits and Aurora I/O before moving deletes to a queue.

### Quickwit search at 100M logs, 2026-09-25

This historical run used a file-backed S3 metastore; current deployments
use PostgreSQL for metadata. 100M OTLP-schema logs through the ingest API
(8 parallel batches, per-shard limit raised 5 → 20 MiB/s): 542 s, 185k
docs/s, zero backpressure retries.
Stored through pgvs3: 6.28 GB (12 published splits, 16.3 GiB uncompressed).

| Query shape, fresh requests | p50 | p95 | under 20 ms |
| --- | --- | --- | --- |
| Template repeats (Quickwit's result cache answers) | 3.0 ms | 29.6 ms | 93% |
| Random 10% time windows, default (`stable_log`) index | 50.3 ms | 146.4 ms | 0% |
| Random 10% windows, `merge_policy: no_merge` | 9.9 ms | 16.2 ms | 99.5% |

What the code says and the measurements confirm:

- Repeated templates are answered by the per-split result cache
  (`leaf_search_cache` in `leaf.rs`) — great for dashboards, flattering in
  benchmarks. Fresh requests are the honest number.
- Config knobs did not move the needle on this rig: a 20 GiB `split_cache`
  (whole splits on local disk), bigger caches, `count_all` off, an indexed
  timestamp field — all within noise. Warmups are ~13 MB per split-search
  and this rig's storage is nearly free (cold ≈ warm). Against slow object
  storage, `split_cache` and the caches might matter more; the chart now
  disables the disk cache by default until that is measured.
- A time-scoped query pays per *overlapping split*: the window filter
  evaluates each split's timestamp column, and the default `stable_log`
  merges smear time ranges — a 10% window overlapped 38M docs across two
  merged splits, and a 0.1% window cost the same as a 10% one.
- The tuning that matters for time-scoped log search is therefore index
  layout, not config: keep splits time-disjoint. The same change at 10M
  docs (p50 24.2 → 9.6 ms) and at 100M (p50 50.3 → 9.9 ms, 99.5% under
  20 ms) with `merge_policy: {type: no_merge}`. The trade-off is more
  splits for wide queries and no compaction.

## Native-engine comparisons

On the EC2/Aurora test rig, pgvs3 served DuckLake's full ClickBench table
(99,997,497 rows) and SpatialBench SF10 (60M trips). With the kind benchmark's
pinned DuckDB **2.0.0 development build**, the 43 ClickBench queries took
**35.35 s first / 30.13 s warm**; four spatial area-of-interest queries took
**7.85 s first / 4.54 s warm**. Quickwit 0.9.1
indexed **100M logs** with OpenSearch Benchmark (OSB) at ~230k docs/s and zero
errors; fresh 10%-time-window search was **43.76 ms p50 / 52.04 ms p95** at
eight clients. Prior DuckDB 1.5.2 numbers remain below for context.

### What native engines would need to match

pgvs3 **requires PostgreSQL** for its object bytes. “Without Postgres” here
means alternative engines keeping their own data, not a pgvs3 standalone mode.
Analytics and indexed search are separate workloads; neither substitutes for
pgvs3's S3 GET API. These are sizing bounds, **not** measured equal-service
configurations:

| Workload and our target | Published native-engine evidence | What that supports |
| --- | --- | --- |
| ClickBench: 99,997,497 rows, 43 DuckDB/DuckLake queries, **35.35 s first / 30.13 s warm** | [ClickHouse Cloud AWS runs](https://github.com/ClickHouse/ClickBench/tree/main/clickhouse-cloud/results/20260925) on the same `hits` dataset: one **32 GiB** replica took 47.95/43.41 s (first/second); one **64 GiB** replica took 25.37/22.75 s. With two replicas: [**32 GiB each**](https://github.com/ClickHouse/ClickBench/blob/main/clickhouse-cloud/results/20260925/aws.2.32.json) took 52.47/47.43 s; [**64 GiB each**](https://github.com/ClickHouse/ClickBench/blob/main/clickhouse-cloud/results/20260925/aws.2.64.json) took 29.39/20.87 s. | Of the published sizes, **64 GiB per native ClickHouse replica clears our query-time target; 32 GiB does not**. This brackets the size, not the exact minimum. |
| Log search: 100M OTLP logs, one bulk client **230k docs/s**, fresh 10%-window severity filter at eight clients **52.04 ms p95** | [AWS's OpenSearch Service OSB study](https://repost.aws/articles/ARBeQf6qJuSNKiSUFrCDLtsA/benchmarking-instance-types-for-amazon-opensearch-workloads) used two **8-vCPU/32-GiB data nodes** plus three **2-vCPU/4-GiB cluster managers** on **247M different HTTP logs**; term/range query p99 was about 29–51 ms, but query shape, concurrency and ingestion differ. | **No verified performance-equivalent OpenSearch size.** Try that published configuration, then sweep 2/4/8 data nodes with [our identical OSB corpus and track](../README.md#benchmarks) until both ingest throughput and eight-client p95 meet the targets. Do not extrapolate node count from the other corpus. |

The ClickHouse figures are the first and second query runs on September 25;
September 23–24 show the same 32-to-64-GiB bracket. [ClickBench's version
benchmark](https://benchmark.clickhouse.com/versions/) also uses 100M `hits`
rows on a 16-vCPU/32-GiB reference host. ClickHouse Cloud defines a compute
unit as [8 GiB RAM and 2 vCPU](https://clickhouse.com/pricing/), so the
published 32–64 GiB tiers correspond to roughly **8–16 vCPU per replica**.
Our first DuckDB pass reads remote
DuckLake files, while published native ClickHouse runs use their own caches and
storage layout; “first” is not an identical cold-cache definition. OpenSearch's
published `http_logs` queries are **not** our OSB time-window query. Matching
either service requires a same-track, same-cache-policy trial before claiming
price/performance equivalence.
