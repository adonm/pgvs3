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

## Storage: S3 gateway vs pgvfs

The same DuckLake workload over its two storage modes, each with its own
database on the same PostgreSQL:

- **S3:** DuckDB httpfs → pgvs3 gateways → PostgreSQL.
- **pgvfs:** the `pgvfs://` extension in DuckDB → PostgreSQL
  ([crates/pgvfs](../crates/pgvfs/README.md)).

The `storage` suite runs both from one Job: same download, same DuckDB
(**1.5.6** stable, which the extension is built for), same full ClickBench
data. The two stacks must have identical whole-table checksums, or the run
fails. The run used commit `abcdb09`; records are in
[`results/storage.jsonl`](results/storage.jsonl).

It times three things:

- **Pass 1 / warm:** the usual consecutive passes on one connection.
- **Fresh:** a new DuckDB instance per query, with an empty DuckDB cache and
  the storage connection already open. This is the latency storage actually
  sets. Stacks alternate run by run (5 runs, median), so host drift such as
  the page cache and CPU clocks affects both equally.

| All 43 queries | S3 | pgvfs | pgvfs/S3 |
| --- | ---: | ---: | ---: |
| Fresh, geomean | 430 ms | 371 ms | **0.86** |
| Fresh, total | 49.4 s | 46.4 s | 0.94 |
| Warm (best of passes 2–3), geomean | 229 ms | 229 ms | 1.00 |
| Pass 1, geomean | 260 ms | 253 ms | 0.97 |
| Load, 100M rows (earlier build) | 51.9 s | 58.5 s | 1.13 |

- **Short and medium queries gain most** with a cold cache. Q20 (152 → 106 ms),
  Q11/Q12 (0.63) and Q3 (0.67) lose the HTTP hop and HEAD revalidation.
- **Warm queries are identical.** DuckDB's external file cache serves both
  stacks, and pgvfs's version tag (`file_id`) keeps that cache valid.
- **The heaviest string scans are 2–7% slower** on pgvfs: Q21, Q22, Q28, Q34
  and Q35. The read pattern is identical to httpfs (Q21: 840 reads, 2.49 GB,
  no overlap). 8 MiB read pieces closed about half the original gap.
- Load was measured before the read-path change (2 I/O threads) and not
  re-run.

## Refresh

Storage comparison: `DUCKDB_PY=1.5.6 SUITES=storage FRESH_PASSES=5 just kind-bench`, then
`python3 deploy/bench/harness/storage_report.py .tmp/pgvs3/kind-bench.jsonl`.

`just smoke` covers the two-gateway contract, service wiring and smoke-scale
ClickBench and SpatialBench inputs. Full runs: `QUICK=0 SUITES=click` and
`QUICK=0 SPATIAL_QUERIES=1-2 SUITES=spatial`. The QA order is in
[CONTRIBUTING.md](../CONTRIBUTING.md).

Publish only runs that pass on the pinned suite. Replace
`results/local.jsonl` with the latest per-query timing records, omitting raw
answer rows, and never carry old numbers into a new profile.
