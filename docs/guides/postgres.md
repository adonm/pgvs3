# Protect OLTP while adding pgvs3

[Guide index](README.md) · Next: [analytical snapshots](analytics.md)

**Goal:** keep short SQL transactions responsive while object ingest, cache fills,
refreshes and jobs share the cluster. First fix expensive application queries;
pgvs3 cannot compensate for a missing index or an oversized connection fleet.
The experiment below is executable; fleet sizing and SLO thresholds require
measurement in your deployment. [Research basis](sources.md#postgresql-and-pooling).

## Budget connections before concurrency

For each gateway, reserve up to `pool-max + min(pool-max, 64)` connections
(main + chunk pools). Count maximum replicas **during a rollout**, application
pools, DuckLake catalogs, DuckDB PostgreSQL scanner pools, operators and HA tools.
Keep PostgreSQL's reserved connection slots out of the usable application budget.

Example arithmetic, **not a recommended fleet**: four gateways at `pool-max=16`
can use 128 connections. One surge gateway makes that 160; 40 application,
16 extraction/catalog and 20 operational slots bring the envelope to 236.
That is already tight on a server with `max_connections=256` and reserved slots.
Scaling an HPA without this calculation can remove the headroom needed to recover.

- Gateway runtime requires **direct or session-compatible** PostgreSQL connections.
  Maintenance and ordered origin writes hold session advisory locks. Transaction
  pooling breaks that contract even if prepared statements work or `SET`s move
  into server configuration. PgBouncer may still suit **application SQL** that
  does not depend on session state; validate that application separately.
- Bound extract/refresh jobs in their own client pool. An extraction can open
  multiple PostgreSQL connections; `threads=4` is not a cluster connection quota.
- Start with current gateway admission defaults; inspect waits before raising
  them. [S3 admission](../operations.md#admission-control) allows 32 active and
  64 waiting per gateway for at most one second. Waiting is not a durable queue.
  `fill-concurrency=8` and disk-spool admission are additional, distinct bounds.

## Reproduce an access-path improvement

Use `psql "$PGVS3_URL"` against the disposable quick-start database. This creates
one million deterministic orders; it deliberately refuses an existing schema.
The same data is used by the analytics guide. Allow roughly a few hundred MiB of
database/WAL headroom, not just the eventual Parquet size.

<!-- verify:app-fixture -->
```sql
CREATE SCHEMA guide_app;
CREATE TABLE guide_app.orders (
    id BIGINT PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    amount_cents INTEGER NOT NULL,
    payload TEXT NOT NULL,
    paid BOOLEAN NOT NULL DEFAULT false,
    fulfillment_count INTEGER NOT NULL DEFAULT 0
);
INSERT INTO guide_app.orders (id, customer_id, created_at, amount_cents, payload)
SELECT g, (g % 10000)::INTEGER,
       TIMESTAMPTZ '2026-10-01 00:00:00+00' + g * INTERVAL '1 second',
       (g % 10000)::INTEGER, md5(g::TEXT)
FROM generate_series(0, 999999) AS g;
ANALYZE guide_app.orders;
```

Run the query before and after creating the index. `ANALYZE` in `EXPLAIN`
**executes** the query; do not casually profile production mutations this way.

<!-- verify:oltp-plan -->
```sql
EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, TIMING OFF, FORMAT JSON)
SELECT id, created_at, amount_cents
FROM guide_app.orders
WHERE customer_id = 42
  AND created_at >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

<!-- verify:oltp-index -->
```sql
CREATE INDEX orders_customer_recent
ON guide_app.orders (customer_id, created_at DESC, id DESC)
INCLUDE (amount_cents);
ANALYZE guide_app.orders;
```

**Expected:** the same 20 IDs, from `990042` down to `800042` in steps of 10,000.
The candidate normally uses `orders_customer_recent`, visits far fewer buffers
and avoids sorting a broad scan. Compare actual rows, loops, buffers and whole
application latency, not planner cost units. An index-only scan can still fetch
heap pages until vacuum updates the visibility map. The retained test plan is
evidence for this data distribution, not proof for yours.

This ordinary index build is for the demo. On a live application table, plan a
`CREATE INDEX CONCURRENTLY` outside a transaction, budget disk/WAL and inspect
`pg_index.indisvalid` after failure. Remove an invalid build deliberately before
retrying. Do not add indexes to pgvs3's generated relations or disable scans
globally: its metadata and chunk pools intentionally use different plans.

## Model OLTP schemas for wide columns and billion-row lifecycle

When a PostgreSQL table grows toward a billion rows or carries wide attributes
(`TEXT`, `JSONB`, `BYTEA`, embeddings), physical tuple layout dominates buffer-hit
ratios and vacuum cost:

- **Separate hot narrow state from wide payloads (TOAST boundary):** PostgreSQL
  pages are 8 KiB and TOAST compresses or moves attributes out-of-line once a
  tuple crosses roughly 2 KiB (`TOAST_TUPLE_THRESHOLD`). Unchanged out-of-line
  TOAST values are not rewritten on `UPDATE`, *but* `SELECT *`, wide inline attributes
  or frequent updates to wide `JSONB` columns still dirty heap/TOAST pages and
  amplify WAL. Keep frequently updated status/counters and B-tree filter keys in
  a narrow table, split rarely read wide columns into a 1:1 companion table by
  primary key, and store multi-MiB blobs or batch extracts in pgvs3 buckets.
- **Prefer fixed 32–64 partitions (`HASH` or fixed Hilbert/Morton cells) over
  time-growing DDL:** Time-partitioned tables require ongoing cron jobs to create
  future partitions and risk insert failures when a partition is missing. Unless
  your primary goal is dropping whole expired time windows (`DETACH PARTITION` /
  `DROP TABLE`), use **32 or 64 fixed partitions** created once at initialization:
  - *By ID or tenant:* `PARTITION BY HASH (tenant_id)` (or `id`) with `MODULUS 32`
    (or `64`). At 1 billion rows across 64 partitions, each child table holds
    ~15.6M rows—keeping B-trees shallow in `shared_buffers` and letting autovacuum
    workers clean partitions in parallel across orders of magnitude of growth.
  - *By 2D space:* quantize coordinates into a 6-bit (`0..63`) coarse **Hilbert**
    or **Morton (Z-order)** cell prefix (`hilbert_bucket SMALLINT`) and partition
    into 64 fixed child tables, with a composite B-tree on `(fine_hilbert_id, ts)`
    or `BRIN` index inside each partition.
  - Reserve rolling time `RANGE` partitions strictly for tables where bulk TTL
    deletion via `DROP TABLE` is mandatory.
- **Match the index type to physical correlation:** Use composite B-trees (with
  `INCLUDE` covering columns) for selective point/prefix lookups, `BRIN`
  (`timestamptz_minmax_ops` or `box_inclusion_ops`) when rows are physically
  appended in time/space order and you want a tiny lossy block-range index, or
  `GiST`/`SP-GiST` when selective 2D spatial lookups must run directly in OLTP.
  Remember that unique/primary-key constraints on a partitioned PostgreSQL table
  must include all partition-key columns (and cannot use expression partition keys).

## Bound a known analytical SQL transaction

Prefer per-transaction changes over a cluster-wide memory increase. In a
separate `psql` session, this demonstrates scope, not a universally safe budget:

<!-- verify:local-budgets -->
```sql
BEGIN READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL lock_timeout = '1s';
SET LOCAL work_mem = '16MB';
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT count(*), sum(amount_cents) FROM guide_app.orders;
COMMIT;
```

**Expected:** `1000000 | 4999500000`. `SET LOCAL` ends at transaction completion;
it does not change gateway pools or another application's session. Capture
settings before/after when testing overrides. A PostgreSQL timeout aborts the
transaction: roll back and retry the whole transaction only if its effects are
safe to replay. A lost commit response is ambiguous; reconcile by business key.

`work_mem` is **per sort/hash operation**, not reserved once per connection.
Hash operations can multiply it by `hash_mem_multiplier`; parallel workers and
concurrent queries multiply demand again. Keep room for shared buffers, the OS,
backend overhead, maintenance, gateway RSS and DuckDB/native-extension caches.
`shared_buffers=25%` is upstream's starting heuristic for a dedicated server,
not a prescription for a container that also runs DuckDB. Do not solve spills by
blindly raising global `work_mem` or removing sync/WAL durability.

## Admission is not isolation

| Pressure | First move | Verify before increasing limits |
| --- | --- | --- |
| Application SQL queues for connections | Reduce competing pools/refresh concurrency | Pool wait, active/idle sessions, rollout ceiling. |
| OLTP waits on storage or WAL | Throttle fills, exports and object writes | `pg_stat_io`, `pg_stat_wal`, checkpoint rates, application p95/p99 and errors. |
| Query spills but host has headroom | Inspect plan; scope memory to that transaction | Temp bytes and end-to-end latency under concurrency, not one isolated run. |
| Autovacuum falls behind | Find old snapshots/slots and table churn | Dead-tuple trend, last autovacuum, XID age, primary and standby feedback. |
| Read replicas lag or cancel scans | Shorten/scatter extracts or serve allowed reads elsewhere | Replay barrier/freshness; enabling feedback can shift cost into primary bloat. |

Separate application/catalog databases improve ownership and recovery planning,
not physical isolation. If the application SLO still fails at bounded load,
move analytical bytes/refresh compute to another cluster or an external origin.
Keeping one service fewer is not worth unpredictable OLTP.

## Verify and undo

Run `just guides` for the exact fixture, both plans, result equivalence and scoped
settings checks. For real acceptance, replay your application transactions and
object workload **against the same primary/cluster** and capture both latency/
error distributions. `just load` creates its own isolated clusters; running it
beside an unrelated application database does not test their shared-cluster
contention. Retain the control, sample counts, caches, data layout and failures.
See [measurement and diagnosis](observe-recover.md#measure-without-mislabeling).

### A same-cluster canary with existing tools

After the [analytical export](analytics.md), save this as `probe.sql`. It is a
small read/write **canary**, not your application's representative workload.
The logically unchanged UPDATE still creates tuple/WAL work; use only the demo:

<!-- verify:oltp-canary -->
```sql
\set order_id random(0, 999999)
BEGIN;
SELECT amount_cents FROM guide_app.orders WHERE id = :order_id;
UPDATE guide_app.orders SET fulfillment_count = fulfillment_count WHERE id = :order_id;
COMMIT;
```

Run a control with PostgreSQL 18 `pgbench` (do **not** use `-i`, which initializes
and can destroy standard pgbench tables):

```sh
pgbench "$PGVS3_URL" -n -M prepared -c 2 -j 2 -T 30 -l \
  --log-prefix control -f probe.sql
```

Download the owned snapshot as `orders.parquet` and build the **existing** load
generator with `mise exec go -- go build -o loadgen ./test/loadgen`. In a second
terminal run it against the gateway backed by that same primary:

<!-- verify:mixed-object-load -->
```sh
loadgen -endpoint "$PGVS3_ENDPOINT" -bucket guide-analytics -unsigned \
  -op get -payload-file orders.parquet -prefix guide-read-probe \
  -count 2 -workers 2 -duration "${PGVS3_GUIDE_DURATION:-30s}"
```

While it runs, repeat pgbench with `--log-prefix mixed`. Prefill creates two
objects under the named **demo** prefix; it is not a cold-origin test or a
read-only operation. Check the loadgen JSON's `errors`, not just its exit status.
pgbench stdout reports averages; per-transaction logs store elapsed **microseconds
in column three**. Count failed/skipped rows separately before calculating tails.
For realistic arrivals, use your observed rate (`-R`) and count schedule lag and
skips rather than hiding overload behind closed-loop throughput.

Alternate control/mixed order across several longer repetitions, then change one
admission/pool/layout setting. Require the application SLO **and** each relevant
object-path budget. The guide gate uses two-second runs solely to verify the
canary, concurrency, log format and zero errors; it claims no p99 isolation or
winning configuration from that smoke. Logs stay with the report; remove the
owned probe objects as part of the final demo bucket cleanup.

To undo just this **demo** index: `DROP INDEX guide_app.orders_customer_recent;`.
Keep `guide_app` while following analytics/jobs. To end the manual walkthrough,
follow [owned-data cleanup](observe-recover.md#safe-demo-cleanup); never drop an
unfamiliar schema to make a recipe pass.
