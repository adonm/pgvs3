# Measure, diagnose and recover

[Guide index](README.md) · [Topology/flag reference](../operations.md)

**Goal:** find the constrained layer before turning a knob, and recover the
complete acknowledgement boundary rather than only its metadata. The SQL below
is checked on PostgreSQL 18. Use a monitoring role (`pg_monitor`/appropriate
statistics privileges) to inspect other sessions; owning pgvs3 data does not
require superuser or these optional monitoring extensions.
[Research basis](sources.md#observability-capacity-and-recovery).

## Measure without mislabeling

Save versions, settings, fixture hashes, concurrency, client caches, role,
replication policy and the old binary/configuration before a trial. Record:

| Path | Measure separately |
| --- | --- |
| Application OLTP | Whole transaction p50/p95/p99, errors, pool wait, rows/buffers and WAL. |
| Object reads | HEAD, whole GET, footer/range, cold fill, warm shared hit and client-private repeat. |
| Object writes | ACK time, bytes/sec, outbox drain/oldest age and replica replay. |
| Materialization | Extract/load + validate + publish + maintenance, refresh lag, query time, extra files/RSS. |
| Jobs/streams | Enqueue, claim, SQL effect, ACK, redelivery/retry, queue age, pinned retention. |

Use [the existing paired runner](../benchmarks.md#paired-acceptance) for changes
to gateway performance and `just load --capacity --replication async` for resource
observations. Retain every affected path and failed trial; compare with the 20%
per-path budget and your stricter application SLO. Do not average a regression
away with a faster unrelated query. `just guides` verifies semantics/syntax,
**not** a production speedup, maximum dataset size or vendor comparison.

An `X-Cache: HIT` means shared object bytes, not cached query results. A fresh
DuckDB connection can still share instance-private state with another connection;
use a new process when that distinction matters. Disable both
`enable_external_file_cache` and `parquet_metadata_cache` when attributing shared
hits, count origin traffic independently, and label OS/PostgreSQL caches that
were not evicted. Never drop production OS caches to make a chart “cold”.

## Triage in this order

### 1. Is it routing, admission, or PostgreSQL?

<!-- verify:health-diagnostics -->
```sh
curl -fsS "$PGVS3_ENDPOINT/healthz"
curl -fsS "$PGVS3_ENDPOINT/readyz"
pgvs3 doctor --url "$PGVS3_URL"
pgvs3 stat --url "$PGVS3_URL" --json
```

Liveness does not touch PostgreSQL. Readiness checks connectivity/layout, **not**
writer role, warmth, freshness, disk headroom or absence of pending writes.
Inspect SQL role explicitly:

<!-- verify:settings-diagnostics -->
```sql
SELECT pg_is_in_recovery(), current_setting('transaction_read_only');
SELECT name, setting, unit, source
FROM pg_settings
WHERE name IN ('server_version', 'max_connections', 'superuser_reserved_connections',
    'reserved_connections', 'shared_buffers', 'work_mem', 'hash_mem_multiplier',
    'max_parallel_workers_per_gather', 'io_method', 'effective_io_concurrency',
    'max_wal_size', 'synchronous_commit', 'synchronous_standby_names')
ORDER BY name;
```

`pg_settings` describes **this SQL session**. A monitoring connection does not
show the per-connection policy of gateway pools or DuckDB's scanner. For example,
gateway chunk reads disable the statement timeout, and the main/chunk planner
settings differ. Do not copy gateway planner controls into application SQL.

Scrape `/metrics`: correlate S3 admission waiting/rejected counters, main/chunk
pool acquired/max connections and empty-acquire waits with request duration and
errors. More client HTTP connections are not more PostgreSQL capacity. For pool
contention, the ratio of the rates of
`pgvs3_pg_pool_empty_acquire_wait_seconds_total` to
`pgvs3_pg_pool_empty_acquire_count_total` is **mean wait per contended acquire**,
not p99. Observe the chunk-pool equivalents too. Guard division by zero and
counter resets in your monitoring system.

### 2. What is PostgreSQL waiting for?

<!-- verify:activity-diagnostics -->
```sql
SELECT pid, usename, application_name, state, wait_event_type, wait_event,
       now() - xact_start AS xact_age, backend_xmin,
       pg_blocking_pids(pid) AS blockers, left(query, 160) AS query
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
ORDER BY xact_start NULLS LAST
LIMIT 30;
```

| Signal | Investigate first | Avoid |
| --- | --- | --- |
| `Lock`/blocker PIDs | Owner transaction, DDL, ordered per-key origin writes | Killing every connection or retrying mutations without reconciliation. |
| `IO`, checkpoints or WAL pressure | Fills/ingest, storage saturation, archive/slot lag | Raising all concurrency limits together. |
| `ClientWrite` | Slow reader/network/backpressure; snapshots stay open longer | Assuming PostgreSQL CPU is the bottleneck. |
| Long idle transaction / old xmin | Leaked session, long extract, standby feedback | Shortening reaper grace or disabling vacuum to hide the symptom. |

Use query text carefully; it can contain business data/secrets. Collect only
what is needed, restrict access and redact before sharing diagnostics.

### 3. Which SQL patterns, relations and I/O consume the budget?

`pg_stat_statements` is optional: add it to `shared_preload_libraries` with your
normal change/restart procedure, then `CREATE EXTENSION pg_stat_statements` in
the diagnostic database. Do not replace an existing preload list. The disposable
guide fixture preloads it; normal pgvs3 operation requires no extension.

<!-- verify:statements-diagnostics -->
```sql
SELECT queryid, calls, round(total_exec_time::NUMERIC, 2) AS total_ms,
       round(mean_exec_time::NUMERIC, 2) AS mean_ms, rows,
       shared_blks_hit, shared_blks_read, temp_blks_written, wal_bytes,
       left(query, 180) AS query
FROM pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
ORDER BY total_exec_time DESC LIMIT 10;
SELECT stats_reset FROM pg_stat_statements_info;
```

These are cumulative successful-statement aggregates since collection/reset,
not latency percentiles or a history of failed requests. Compare deltas over a
known interval; do not reset a shared production collector for your experiment.
Collect application tails separately. `track_io_timing` adds timing detail but
can add overhead; assess it rather than interpreting disabled zero timings as
zero I/O cost. High shared-buffer hit ratio alone does not prove good queries.

<!-- verify:storage-diagnostics -->
```sql
SELECT schemaname, t.relname, n_live_tup, n_dead_tup,
       last_autovacuum, last_autoanalyze,
       age(c.relfrozenxid) AS xid_age, pg_total_relation_size(relid) AS bytes
FROM pg_stat_user_tables AS t JOIN pg_class AS c ON c.oid = t.relid
WHERE schemaname = 'guide_app' OR schemaname = 's3p'
   OR left(schemaname, 4) IN ('s3b_', 's3c_', 's3w_', 's3q_')
ORDER BY bytes DESC LIMIT 30;
SELECT * FROM pg_stat_wal;
SELECT * FROM pg_stat_checkpointer;
SELECT * FROM pg_stat_io WHERE backend_type = 'client backend';
```

Table tuple counts are estimates and statistics can lag; read a fresh transaction
for each sample. PostgreSQL I/O counters do not distinguish kernel-cache reads
from physical device reads. Combine them with host RSS, disk latency/IOPS,
filesystem free bytes and queue depth. `pg_stat_checkpointer`, not older
`pg_stat_bgwriter` checkpoint columns, is the PostgreSQL 18 checkpoint view.
Logical object length, allocated relation bytes, WAL retained on disk and local
gateway scratch are **different quantities**.

### 4. Is data draining and replicating?

Use [pending-write SQL](cache.md#acknowledgement-is-not-drain-throughput) plus
`pgvs3_pending_uploads_{bytes,oldest_seconds,dead}`. Set an oldest-age threshold
from your handoff/freshness SLO, page on nonzero dead rows and forecast disk
exhaustion from growth. The pending gauges are database-wide and repeated on
every gateway: do not sum them across identical fleets as if each owned the rows.
Pool/admission/spool gauges, in contrast, are per process. Scrape failures or
missing metrics are not zero backlog.

On the primary:

<!-- verify:replication-diagnostics -->
```sql
SELECT application_name, state, sync_state, sent_lsn, write_lsn, flush_lsn, replay_lsn
FROM pg_stat_replication;
SELECT slot_name, slot_type, active, restart_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```

An empty replication view on the demo single node is expected, **not** HA proof.
`max_wal_size` is not a hard disk cap: slots/archiving can retain more. Resolve an
abandoned slot with its owning consumer/rebuild plan; never delete `pg_wal` files.
`hot_standby_feedback` can reduce standby cancels while retaining primary bloat.
Measure both, not just replica query latency.

## Recovery decision cards

| Incident | Preserve / fence first | Verify before returning traffic |
| --- | --- | --- |
| Gateway crash | Database is authoritative; remove dead worker | Another worker reads known key/ETag and resumes pending drain. Local spool loss is not body loss after ACK. |
| Origin unavailable | Keep `s3w_` + pending rows; throttle before space exhaustion | Same bytes readable via pgvs3; drain and origin version/integrity after repair. Never flush pending bodies. |
| Ambiguous PUT / enqueue / ACK | Preserve stable key/event ID; no unconditional new identity | GET/HEAD/validator or job/receipt reconciliation on the primary. A disconnect is not proof of rollback. |
| PostgreSQL primary lost | **Fence old primary and writable fleet** with the HA mechanism | Eligible promotion under the configured durability policy, writer routing, layout, known native/pending data, new writes and resumed drain. |
| Restore / PITR | Restore into isolated endpoints; initially read-only, outbox not allowed to hit live origin | Catalog + native bytes + pending bodies/control + application intents/receipts; validate external effects/origin separately. |
| Disk filling | Stop/reduce new load, retain acknowledged state | Identify payload/WAL/retired/history/slot costs; expand or deliberately reclaim unneeded data. `VACUUM FULL` needs extra disk and exclusive locks. |

`synchronous_commit=on` gives local WAL durability and waits for configured
synchronous standbys when present; it does not make an arbitrary asynchronous
replica a lossless promotion target. `flush_lsn` and `replay_lsn` are different:
remote durability is not immediate read visibility. For read-after-write on a
chosen physical standby, record a post-ACK primary WAL barrier and wait until
that standby has replayed it, with a bounded deadline. Verify common cluster/
timeline history; comparing LSN numbers across unrelated/promoted timelines is
not a fencing or consistency protocol. Route freshness-critical reads to the
primary if the contract is not established.

The bounded `just fault` drill proves physical-standby native/shared/pending
reads, synchronous promotion, a deliberate asynchronous loss counterexample and
consistent pending-body logical restore. It is **not** a physical archive/PITR
tool qualification or an automatic HA controller. [Evidence](../data-plane-results.md).

## Restore checklist for operators

1. Record RPO/RTO, backup ID, timeline/target, versions/layout, origin mapping,
   active DuckLake writers/compaction/cleanup and replay/receipt retention.
2. Restore a complete database/cluster into an isolated network. For production,
   use tested physical base backups + continuous WAL/managed PITR; a replica is
   not a backup. A logical `pg_dump` is not input to WAL replay.
3. Restore pgvs3 bodies/control rows together. A dump of one `s3w_` schema omits
   `s3p.pending_uploads`; a metadata-only restore can orphan acknowledged bodies.
   Coordinate application/catalog recovery too. Pause DuckLake writes and cleanup
   for manual multi-component backups; retain every referenced object version.
4. Start **read-only** gateways first; run doctor, verify known full/range object
   hashes/ETags and pending-body readability, and reopen a known DuckLake snapshot.
   Confirm no outbox worker has contacted the live origin.
5. Clone/remap the recovery origin before enabling writable workers. Reconcile
   origin conflicts and external effects that cannot be rolled back with SQL.
   Confirm queue receipts prevent duplicate effects over the chosen replay horizon.
6. Only after checks, switch traffic via the HA/routing mechanism. Test a fresh
   write, observed replay, drain and backlog alarms. Record achieved RPO/RTO and
   any lost/duplicated business effects; do not declare success from `/readyz` alone.

Native pgvs3 has no S3 versioning/replication/Object Lock. Upstream lake backup
recipes relying on those features require a suitable **external origin**, not a
setting in pgvs3. Physical PITR, provider HA, network partitions, production
tail-latency budgets and exact alert thresholds remain deployment validation.

## Safe demo cleanup

`just guides` owns disposable containers and removes them on exit, retaining
reports. For the manual demo only, stop its extractors/relay/workers/readers,
verify no other data uses the reserved names and wait for pending rows to drain.
Then the following removes **all** demo namespace data; it is not a maintenance
recipe for a live dataset:

<!-- verify:demo-cleanup-namespaces -->
```sh
(
set -eu
test "$("${PSQL:-psql}" "$PGVS3_URL" -XAt -v ON_ERROR_STOP=1 \
  -c "SELECT count(*) FROM s3p.pending_uploads WHERE namespace='guide-origin'")" = 0
pgvs3 origin --url "$PGVS3_URL" --bucket guide-edge clear
pgvs3 drop --url "$PGVS3_URL" --kind cache --force guide-origin
pgvs3 drop --url "$PGVS3_URL" --force guide-edge
pgvs3 drop --url "$PGVS3_URL" --force guide-analytics
pgvs3 drop --url "$PGVS3_URL" --kind queue --force guide-jobs
pgvs3 drop --url "$PGVS3_URL" --kind queue --force guide-events
)
```

The subshell stops on a failed guard without changing your interactive shell's
options. Dropping the cache namespace also drops its companion write-back
namespace. Do **not** run this
while new writes can race the check. Remove only the demo key(s) from your
disposable origin; do not delete a shared origin bucket. The fixture's separate
catalog cleanup is run in `ducklake_catalog`, not the data database:

<!-- verify:demo-cleanup-catalog -->
```sql
DROP SCHEMA guide_lake CASCADE;
```

And in the demo application's database:

<!-- verify:demo-cleanup-app -->
```sql
DROP SCHEMA guide_app CASCADE;
```

Save reports outside the destroyed database. Ordinary reaping/vacuum makes live
bucket space reusable but need not shrink relation files; a drop's unlinking is
not evidence of efficient steady-state table reclamation.
