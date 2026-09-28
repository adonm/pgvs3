# Contributing: benchmarking and QA

Every storage, gateway or benchmark change needs the local kind QA below.
CI checks functionality; it does **not** produce performance numbers. Follow
the pinned upstream suites in [benchmark scope](docs/benchmarks.md).

Run `mise install` and `just contract` first.

| Stage | Command | Purpose |
| --- | --- | --- |
| 1. Smoke | `just smoke` | Two-gateway contract, service wiring, smoke-scale ClickBench and SpatialBench. Smoke subsets are QA, not scores. |
| 2. Full | `QUICK=0 SUITES=click just kind-bench`; `QUICK=0 SPATIAL_QUERIES=1-2 SUITES=spatial just kind-bench` | Full input size, three query passes. Save JSONL **between** invocations. |

Use one commit and record the workload revision, input hashes and counts,
versions, settings, machine and resource sizes, per-query coverage and
results, errors, and first and warm passes. Smoke times must never be reported
as official-scale results. The DuckDB 2.0-series wheel is prerelease; label
results and repeat after GA.

**Give the benchmark the host.** It should be the only thing running in
Docker/kind: stop other containers and kind clusters first. Full DuckDB Jobs
are limited to 24 GiB with a 16 GiB DuckDB working limit. The PostgreSQL PVC
requests 20 GiB by default and kind's local-path provisioner does not enforce
quotas, so check host disk before loading full datasets (full ClickBench and
SpatialBench SF10 together need well over 100 GiB). Set `PG_STORAGE` when
first creating a cluster; Helm cannot expand an existing local-path PVC.

A layout change needs fresh databases: `just kind-reset` discards **all** test
data and needs explicit approval. `BENCH_REUSE=1` reuses loaded DuckLake
tables for query-only reruns after row-count checks; never describe those as
fresh loads. `just kind-bench` overwrites `.tmp/pgvs3/kind-bench.jsonl`, so
copy results and logs before the next run.

## Refreshing published results

Re-run the full matrix after a storage/read-path, layout, compiler, engine,
workload-input or harness change. Update the README and `docs/benchmarks.md`
with only the newest complete runs, and replace `docs/results/local.jsonl`
(omit raw answer rows; inspect for secrets first). Git history holds older
runs; do not append dated tables.
