# Research and applicability ledger

[Guide index](index.md) · [Local validation](validation.md)

Primary documentation reviewed **2026-10-31**. These are design/quality
references, **not systems benchmarked by this documentation change**. Links to
moving `stable`/vendor pages may evolve; local results record installed versions.
We adopt the useful question or method, then check pgvs3 code/examples rather
than inheriting another product's guarantees or configuration numbers.

## PostgreSQL and pooling

| Primary source | Adopt for pgvs3 guidance | Difference / validation limit |
| --- | --- | --- |
| [PlanetScale connections](https://planetscale.com/docs/postgres/connecting) | Connection-per-backend cost, whole-fleet pooling, explicit primary/replica routing. | Its normal transaction-pooled application recommendation is **not** safe for pgvs3 runtime session advisory locks. Managed endpoints/TLS/auth are not pgvs3 gateway features. |
| [PlanetScale OOM](https://planetscale.com/docs/postgres/troubleshooting/out-of-memory) | Distinguish RSS from OS cache; concurrent per-operation memory can dominate. | No claim that pgvs3 adds memory isolation or managed OOM recovery. Do not simply copy a resize prescription. |
| [PgBouncer feature map](https://www.pgbouncer.org/features.html) | Session pooling preserves session state; transaction pooling does not support session advisory locks. | Prepared-plan compatibility does not fix that lock boundary. No PgBouncer deployment was benchmarked here. |
| [PostgreSQL 18 resource settings](https://www.postgresql.org/docs/18/runtime-config-resource.html) | `work_mem` per operation, hash/parallel multipliers, OS memory headroom and measured I/O concurrency. | Gateway main/chunk pools specialize their settings. `SET LOCAL` example checked; no global optimum is claimed. |
| [PostgreSQL 18 table partitioning](https://www.postgresql.org/docs/18/ddl-partitioning.html) | Coarse declarative `RANGE` partitions for retention (`DETACH`/`DROP` vs `DELETE`+`VACUUM`); bound partition count to protect planner CPU and session memory. | Unique constraints must include partition keys; pgvs3's own `chunks` table remains unpartitioned per bucket. |
| [PostgreSQL 18 TOAST storage](https://www.postgresql.org/docs/18/storage-toast.html) | ~2 KiB TOAST threshold and 8 KiB pages; narrow queries avoid detoasting unreferenced columns, while wide updates/`SELECT *` amplify I/O and WAL. | Guides vertical hot/cold splitting and externalizing multi-MiB blobs to pgvs3. |
| [PostgreSQL 18 BRIN indexes](https://www.postgresql.org/docs/18/brin.html) | Tiny lossy block-range summaries (`minmax`, `box_inclusion_ops`) when heap order correlates with time/space. | Requires physical correlation; non-clustered updates degrade pruning until summarized/compacted. |
| [PostgreSQL 18 EXPLAIN](https://www.postgresql.org/docs/18/using-explain.html) | Actual rows/loops/buffers; ANALYZE executes; planner units are not milliseconds or network latency. | Demo plans/results checked; no extrapolation from the fixture to all OLTP. |
| [PostgreSQL 18 pgbench](https://www.postgresql.org/docs/18/pgbench.html) | Custom transaction scripts, `-n` without standard tables, raw transaction logs and arrival/schedule-lag accounting. | Two-second same-cluster canary is a tooling smoke, not production p99 isolation. Avoid destructive `-i`; retain failures/skips rather than only means. |
| [PostgreSQL 18 isolation](https://www.postgresql.org/docs/18/transaction-iso.html) | Source snapshots, retries and sequence allocation not implying commit order. | HTTP operations remain independent transactions, even on the same cluster. |

## Analytics and object storage

| Primary source | Adopt | Difference / validation limit |
| --- | --- | --- |
| [ClickHouse S3 performance](https://clickhouse.com/docs/integrations/connectors/data-ingestion/AWS/performance) | Separate ingest/query/merge costs, projection, locality, file layout and request/memory budgets. | MergeTree block/thread formulas and vendor benchmark numbers do not apply to pgvs3's PostgreSQL byte layout. |
| [ClickPipes PostgreSQL ordering keys](https://clickhouse.com/docs/integrations/clickpipes/postgres/ordering-keys) | CDC ordering/deduplication and delete identity must be designed explicitly. | pgvs3 is not ClickPipes, does not decode source WAL and has no ReplacingMergeTree/`FINAL` semantics. |
| [DuckDB 2.0 workload tuning](https://duckdb.org/docs/2.0/guides/performance/how_to_tune_workloads.html) | Avoid unnecessary I/O, inspect plans, reuse connections deliberately and budget spill. | Connection-private/instance caches are not shared gateway hits. No “raise all threads” preset. |
| [DuckDB 2.0 schema performance](https://duckdb.org/docs/2.0/guides/performance/schema.html) | Restrictive integer/timestamp types reduce in-memory vector width; dimension keys/ENUMs speed grouping; omit unnecessary PK constraints on bulk loads. | Validated in modeling guidance; bitpacking saves disk, not execution vector RAM. |
| [DuckDB 2.0 indexing & zonemaps](https://duckdb.org/docs/2.0/guides/performance/indexing.html) | Physical ordering makes min/max zone maps tight; ART indexes hold non-evictable buffer memory and do not speed joins/aggregations. | Explains why ordered keys and `SORTED BY` beat analytical PK indexes. |
| [DuckDB 2.0 Parquet tips](https://duckdb.org/docs/2.0/data/parquet/tips.html) | Row-group sizing trades compression/scan speed against parallel read granularity and writer memory; pre-sorting tightens row-group min/max pruning. | Paired with byte-bounded row groups (`parquet_row_group_size_bytes`) for wide tables. |
| [DuckDB 2.0 file formats](https://duckdb.org/docs/2.0/guides/performance/file_formats.html) | Row groups trade scan/compression against pruning/parallelism; native materialization can help joins. | Upstream's file-size range extends beyond pgvs3's **10 MiB–1 GiB** focus. We do not adopt 10 GiB as a qualified object size. |
| [DuckDB 2.0 I/O](https://duckdb.org/docs/2.0/data/io.html) | Async workers/read-ahead/HTTP pools are distinct from execution threads. | Do not copy a 25 Gbit EC2 preset into a shared PostgreSQL cluster. Native extension runtime/cache costs remain separate. |
| [DuckDB PostgreSQL scanner](https://duckdb.org/docs/2.0/core_extensions/postgres/overview.html) | Read-only attach, repeatable-read extraction, temporary credentials and bounded source access. | Real extract syntax/result checked; scanner/catalog pools still consume PostgreSQL connections. |
| [DuckLake catalog choice](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database.html) | PostgreSQL catalog for remote/multi-client operation. | The locally tested single coordinated writer is a qualification scope, not an upstream single-writer restriction. |
| [DuckLake configuration](https://ducklake.select/docs/stable/duckdb/usage/configuration.html) | Scoped/persistent options; inspect effective configuration and future file layout. | Candidate settings are syntax-tested, not declared fastest; installed version captured. |
| [DuckLake partitioning](https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning.html) | Order-preserving UTC `epoch_day`/`epoch_month` transforms in 1.1+; avoid calendar `month()`/`day()` folding across years. | Tested locally with `PARTITIONED BY (epoch_day(ts))`. Keep partitions coarse to avoid small files. |
| [DuckLake sorted tables](https://ducklake.select/docs/stable/duckdb/advanced_features/sorted_tables.html) | `SORTED BY` (including expressions) orders inserts, flushes and compactions for multi-attribute/spatiotemporal zone-map pruning; `sort_on_insert` can defer sort to batch compaction. | Tested locally with `SORTED BY (cell_id, ts)` and `ducklake_merge_adjacent_files`. |
| [DuckLake merge adjacent files](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files.html) | Compacts small batch files toward `target_file_size` while preserving snapshot time travel and applying current `SORTED BY`. | Executed in `just guides` (`files_processed=2 -> files_created=1` per partition). |
| [DuckLake rewrite data files](https://ducklake.select/docs/stable/duckdb/maintenance/rewrite_data_files.html) | Rewrites files with positional deletes above `delete_threshold` to eliminate merge-on-read overhead for read-optimal serving. | Executed in `just guides` with `delete_threshold => 0.1`. Old files still require snapshot expiry + cleanup + pgvs3 reap. |
| [DuckLake checkpoint](https://ducklake.select/docs/stable/duckdb/maintenance/checkpoint.html) | Bundles flush, snapshot expiry, merge, rewrite and file cleanup. | Convenience wrapper; individual maintenance calls give finer batch control (`max_compacted_files`, `delete_threshold`). |
| [DuckLake inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining.html) | Small changes can reside in the catalog; measure and flush explicitly. | A one-row insert is not necessarily an object PUT. Inlining/flush checked locally. |
| [DuckLake maintenance](https://ducklake.select/docs/stable/duckdb/maintenance/recommended_maintenance.html) | Merge/rewrite, snapshot expiry and file cleanup are distinct from PostgreSQL vacuum. | Do not export zero-age benchmark cleanup to production. No untested automated retention policy is supplied. |
| [Amazon S3 performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html) | Byte ranges, locality, bounded retries and measuring 503s/network/CPU. | AWS's distributed scaling/connection behavior is not pgvs3's capacity model. Retried errors are not hidden in acceptance timings. |

## Caches are not interchangeable

| Primary source | Adopt | Difference / validation limit |
| --- | --- | --- |
| [ClickHouse query cache](https://clickhouse.com/docs/concepts/features/performance/caches/query-cache) | Separate TTL-bounded query results from transactional truth and storage bytes. | pgvs3 has no automatic SQL query cache or ClickHouse per-user cache isolation. |
| [Spice S3 cache](https://spiceai.org/docs/use-cases/caching/s3-cache) | Distinguish locally materialized datasets from SQL-result caching. | pgvs3 caches **object bytes**, not parsed/accelerated dataset rows. No Spice query latency is claimed. |
| [Spice data refresh](https://spiceai.org/docs/features/data-acceleration/data-refresh) | Refresh mode, deletes/late data, last successful refresh, readiness and fallback-source pressure are explicit contracts. | pgvs3 has no dataset scheduler/CDC/SWR fallback policy. Byte TTL, extract age and result TTL can stack; readiness is only connectivity/layout. |
| [Spice PostgreSQL accelerator deployment](https://spiceai.org/docs/components/data-accelerators/postgres/deployment) | Connection sums, refresh peak storage, indexes, WAL/HA ownership and write amplification. | The page identifies this accelerator as Enterprise; no edition/feature parity is inferred. Its pooler advice does not override pgvs3's session-lock requirement. |

## Jobs and transaction boundaries

| Primary source | Adopt | Difference / validation limit |
| --- | --- | --- |
| [River transactional enqueue](https://riverqueue.com/docs/transactional-enqueueing) | Enqueue-before/after-SQL failure windows; commit durable publish intent with business state. | River's direct transaction API can enqueue atomically; pgvs3's **HTTP** enqueue cannot. Recommend the right tool rather than claiming equivalence. |
| [River reliable workers](https://riverqueue.com/docs/reliable-workers) | Expect redelivery after effects commit; external effects do not roll back with SQL. | pgvs3 has leases/tokens, not River's worker runtime or cancellation implementation. Effect + receipt + ACK gap tested. |
| [River unique jobs](https://riverqueue.com/docs/unique-jobs) | Unique insertion is not exactly-once execution; retention bounds remembered uniqueness. | OJS advertises `unique_jobs:false`. Caller ID conflict is not payload deduplication; retention reuse tested. |
| [River transactional completion](https://riverqueue.com/docs/transactional-job-completion) | Name the completion/effect boundary and its throughput trade-off. | No `JobCompleteTx` equivalent is promised for OJS HTTP; do not write internal job tables from the application. |

## Observability, capacity and recovery

| Primary source | Adopt | Difference / validation limit |
| --- | --- | --- |
| [PlanetScale Query Insights](https://planetscale.com/docs/postgres/monitoring/query-insights) | Start from query patterns, frequency, rows, total time and tails, not one cache-hit percentage. | Managed Insights/traffic-control budgets are not pgvs3 features. `pg_stat_statements` means are not p95/p99. |
| [PostgreSQL 18 statement statistics](https://www.postgresql.org/docs/18/pgstatstatements.html) | Optional preload + extension prerequisites, cumulative/delta interpretation and restricted SQL text. | No runtime extension requirement added; diagnostic fixture alone preloads it. |
| [PostgreSQL 18 monitoring views](https://www.postgresql.org/docs/18/monitoring-stats.html) | Waits, `pg_stat_io`, checkpointer/WAL, stats lag/reset and OS-cache ambiguity. | Queries are executed, not copied from old PostgreSQL versions. Host/cloud saturation still needs operator telemetry. |
| [PostgreSQL 18 routine vacuum](https://www.postgresql.org/docs/18/routine-vacuuming.html) | Reuse versus physical shrink, old snapshots and XID age; FULL needs locks/space. | Do not shorten pending-body lifetime or promote reaping to a universal compactor. |
| [PostgreSQL 18 replication settings](https://www.postgresql.org/docs/18/runtime-config-replication.html) | Flush versus replay, eligible sync promotion, slot WAL and feedback/bloat trade-offs. | Existing physical fault gate is bounded evidence, not partition-safe automatic HA. |
| [PostgreSQL 18 PITR](https://www.postgresql.org/docs/18/continuous-archiving.html) | Base backup + continuous WAL + timeline history; dumps are not WAL replay backups. | Production archive/PITR tooling is not qualified by a logical-restore test. No destructive generic restore script is prescribed. |
| [DuckLake backup/recovery](https://ducklake.select/docs/stable/duckdb/guides/backups_and_recovery.html) | Catalog/inlined data + referenced files; pause manual-backup writes and cleanup. | Native pgvs3 has no S3 versioning or bucket replication. External origin and application effects need separate recovery/reconciliation. |

## How this stays honest

The [validation ledger](validation.md) names executable snippets, results, failed
trials and remaining gaps. The guide runner records document/snippet and tool
hashes. Source-page fetch status/content hashes are retained with research
artifacts, not vendored copies of third-party documentation. Sources support
the design rationale; pgvs3 code and executable tests decide actual behavior.
