# Benchmarks: DuckLake on pgvs3

All performance runs use the project's **kind charts and Jobs** on one local
workstation. The target is DuckLake-on-pgvs3 query and load performance with
DuckDB 2.0+, measured on pinned official workloads. Older paired-comparison
reports (ClickHouse, OpenSearch) remain in `git log -- docs/benchmarks.md`.

## Scope

| Workload | Official inputs | What is reported |
| --- | --- | --- |
| ClickBench | [ClickBench](https://github.com/ClickHouse/ClickBench) revision `6b85c596`: canonical 100M-row `hits` data and all 43 queries | All queries run three times; a stable tracked subset (below) is summed for run-over-run comparison. |
| SpatialBench | [Apache Sedona SpatialBench](https://github.com/apache/sedona-spatialbench) revision `8e44bddd` | Upstream has 12 queries and no ClickHouse dialect. Only the validated Q1/Q2 subset is reported. |

Each ClickBench query runs three times **consecutively** without clearing the
OS page cache or restarting: **no-cold** results in ClickBench's terminology.
DuckLake downloads its source before timing the load, so load times exclude
the download.

The pinned DuckDB wheel is **2.0.0.dev2609222040**, not 2.0 GA: results are
**prerelease** and must be repeated after stable 2.0.

## Local results, layout v7

One 16-vCPU/62-GiB workstation running kind at commit `0cc4642`: PostgreSQL
18, two pgvs3 gateways, DuckDB `2.0.0.dev2609222040` with DuckLake. Records:
[`results/local.jsonl`](results/local.jsonl).

**ClickBench.** **99,997,497 rows** loaded; all 43 queries ran; timings below
are a reused-data run after a fresh load. The tracked 32-query subset excludes
Q4 (integer-avg accumulator semantics), Q29 (bytes vs characters) and
Q18/Q25/Q31/Q32/Q33/Q39/Q40/Q41/Q42 (LIMIT/OFFSET ties). This is not the
official 43-query score.

| Tracked 32-query subtotal, DuckLake on pgvs3 | First run | Second run | Third run |
| --- | ---: | ---: | ---: |
| DuckDB 2.0 **development** | 27.117 s | 19.785 s | 19.693 s |

Load timer: 50.79 s after downloading the Parquet source.

**SpatialBench SF10.** **60,000,000 trips** and **454,710 zones** loaded.
Q1/Q2 reported; Q3–Q12 excluded (no validated dialect).

| Tracked Q1/Q2 subtotal, DuckLake on pgvs3 | First run | Second run | Third run |
| --- | ---: | ---: | ---: |
| DuckDB 2.0 **development** | 9.302 s | 1.829 s | 1.878 s |

Load timer: 15.26 s after source download.

## DuckDB-native storage: pgvfs

DuckLake can also keep its data files in PostgreSQL through a DuckDB
filesystem extension, with no gateway: [pgvfs](https://github.com/adonm/pgvfs),
split out of this repository. On full ClickBench (DuckDB 1.5.6, local kind),
fresh-instance geomean latency was 0.86× the gateway's (430 → 371 ms), and warm
queries were identical. The records and method are in that repository. The two
never share a database: this gateway refuses one that holds the pgvfs layout.

## Refresh

`just smoke` covers the two-gateway contract, service wiring and smoke-scale
ClickBench and SpatialBench inputs. Full runs: `QUICK=0 SUITES=click` and
`QUICK=0 SPATIAL_QUERIES=1-2 SUITES=spatial`. The QA order is in
[CONTRIBUTING.md](../CONTRIBUTING.md).

Publish only runs that pass on the pinned suite. Replace
`results/local.jsonl` with the latest per-query timing records, omitting raw
answer rows, and never carry old numbers into a new profile.
