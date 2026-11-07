# pgvs3 — PostgreSQL 18 as the Object Data Plane

**Durable object storage, shared S3/HTTP caching, analytical storage for DuckDB
and [DuckLake](https://ducklake.select/), and bounded companion queues—served
through stateless Go gateways over PostgreSQL 18.** PostgreSQL stores the bytes,
not just a metadata index. pgvs3 uses its transactions, MVCC snapshots, WAL,
physical replication and advisory locks instead of adding separate durability,
cache-coherence and coordination services.

![FIG 01 — System Topology & Parallel Authority Lanes](assets/architecture-flow.svg)

> **Status: pre-alpha.** PostgreSQL 18+ (Aurora 18.3+). Storage layout **v25**
> has no in-place upgrade: a layout change needs a fresh database and a
> re-import.
>
> **Trusted networks only — permanently.** pgvs3 does not authenticate or
> authorize clients, terminate inbound TLS, or validate S3 signatures or
> credentials. Run it on loopback, a private VPC/VLAN, or behind a service mesh
> that enforces access control and TLS, and treat `:8014` like a database port.
> Presigned URLs are rejected (`400 InvalidRequest`). See [Architecture & Trust Model](design.md).

---

## Start Here: Choose Your Path

![FIG 02 — Workload, Schema and Partitioning Decision Tree](assets/decision-tree.svg)

| Goal | Primary Guide | What You Will Build & Verify |
| --- | --- | --- |
| **Choose the right schema, partition & storage model** | [Workload Decision Tree & Resource Trade-offs](guides/index.md) | Map OLTP, DuckLake, cache and queue requirements across RAM, Disk/WAL, Network and CPU. |
| **Protect OLTP tail latency & partition without cron DDL** | [1. Protect OLTP & Fixed Partitioning](guides/postgres.md) | Connection pool math, covering B-tree plans, TOAST splitting, and 32–64 fixed hash/quantile partitions. |
| **Model large-table DuckLake, wide columns & Hilbert curves** | [2. Analytical Snapshots, Wide Columns & Hilbert](guides/analytics.md) | 24-bit hierarchical Hilbert sorting for skewed hotspots, byte-bounded row groups, `skip_stats_columns`, late materialization & batch compaction. |
| **Share S3/HTTP bytes & stage large writes across gateways** | [3. Shared Cache & Write-Back Staging](guides/cache.md) | Coalesced cold fills (`s3c_`), aligned Parquet range caching, and non-expiring ack-first write-back (`s3w_`). |
| **Bridge committed SQL changes to background workers** | [4. Jobs, Outboxes & Streams](guides/jobs.md) | Transactional SQL outbox, HTTP `409` envelope reconciliation, lazy `SKIP LOCKED` claims, receipts & 3-segment `TRUNCATE` streams. |
| **Diagnose bottlenecks & execute safe recovery** | [5. Observe, Diagnose & Recover](guides/observe-recover.md) | PostgreSQL 18 `pg_stat_io` / `pg_stat_checkpointer` triage, LSN replay barriers, and pending-body restore checklists. |

---

## Sizing: measure, don't estimate

Three questions decide most deployments. Each answer below comes from a tool or
exact arithmetic, not a model; rerun them on your own hardware.

| Question | Tool | Example output on the dev host |
| --- | --- | --- |
| How many PostgreSQL connections does the fleet need? | `pgvs3 doctor --gateways N --pool-max P` | 2 gateways at the default pool-max need (2 + 1 surge) × (64 main + 64 chunk) = 384. On a 256-connection server: `FAIL connections … raise max_connections to at least 387, or run --pool-max 42`. |
| How fast is one stream? | `just sol` | The latest dev-host 256 MiB PUT/GET rates and their percentages of the raw PostgreSQL `COPY` and loopback HTTP floors are in the [speed-of-light table](performance.md#speed-of-light). |
| How much writer RAM does a wide DuckLake table need? | 122,880 rows × row bytes | See below. |

DuckDB writes Parquet row groups of `parquet_row_group_size` = 122,880 rows by
default, buffered per writer thread before compression:

| Row shape | Bytes/row | Row group in writer RAM | Setting |
| --- | ---: | ---: | --- |
| Narrow keys and metrics | 64 | 7.5 MiB | default is fine |
| Keys + 2 KiB payload | 2,096 | 246 MiB | `parquet_row_group_size_bytes = '32MB'` |
| Keys + 8 KiB embedding | 8,208 | 962 MiB | `parquet_row_group_size_bytes = '32MB'` plus `skip_stats_columns` |

Hilbert sorting and late materialization ([FIG 05](guides/analytics.md))
are checked functionally by `just guides`; their I/O reduction has not been
measured here, so no number is quoted.

---

## Visual Atlas of Data Layouts & Movements (FIG 03–07)

Figures use stable site-wide numbers, so the same diagram can be reused on a guide page without renumbering. FIG 01 (topology) and FIG 02 (decision tree) appear above; the data-layout and capacity figures follow.

### FIG 03 — 8,192-Byte Page Layout & Cut-Through Streaming I/O

Small objects ($\le 64\text{ KiB}$ native, $\le 8,120\text{ B}$ cache/RESP) live inline in `objects.inline_data`. Larger objects (10 MiB–1 GiB Parquet) split into fixed **8,120-byte logical blocks** compressed per block with Zstd *only when it shrinks*, fitting **one tuple per 8,192-byte PostgreSQL page** with `toast_tuple_target = 8160` and `STORAGE EXTERNAL` (zero out-of-line TOAST pointer chasing, and $O(1)$ byte-range offset seeking at $\text{offset} = \text{no} \times 8,120$).

![FIG 03 — 8,192-Byte Page, 8,120-Byte Chunks & Cut-Through Streaming I/O](assets/page-layout-io.svg)

See [8 KiB Page Layout & I/O Paths](performance.md) and [Why Chunks Are Unpartitioned](partitioning.md).

---

### FIG 04 — Origin-Backed Shared Cache (`s3c_`) & Durable Write-Back Stage (`s3w_`)

Cold misses coalesce behind a renewable 2-minute database lease (`s3p.cache_fill_leases`), spooling bodies $>8\text{ MiB}$ to local scratch outside a database transaction before publishing version-fenced chunks to `s3c_<namespace>`. Unconditional writes $>8\text{ MiB}$ and multipart completions commit atomically to WAL-logged `s3w_<namespace>` + `s3p.pending_uploads` (`expires_at IS NULL`) before returning `200 OK`, then drain asynchronously to the upstream origin under a session advisory key lock.

![FIG 04 — Shared Origin Cache (s3c_) & Ack-First Write-Back Outbox (s3w_)](assets/cache-writeback-lifecycle.svg)

See [Shared Cache & Write-Back Staging](guides/cache.md) and [Operations: Cache Tier](operations.md#cache-tier-in-front-of-another-object-store).

---

### FIG 05 — Large-Table DuckLake: Fixed Buckets, Density-Adaptive Hilbert & Late Materialization

Instead of high-maintenance rolling time partitions or equal-area spatial grids that skew under urban hotspots, combine **fixed 32–64 hash buckets** (`bucket(32, customer_id)`) with **high-resolution 1D Hilbert sorting** (`SORTED BY (hilbert_id, ts)`). Because Parquet row groups (`parquet_row_group_size_bytes`) and DuckLake files (`target_file_size`) slice the sorted stream by **row/byte count (empirical quantiles)** rather than geographic area, sparse regions pack into one row group while dense hotspots automatically subdivide into tight neighborhood bounding boxes.

![FIG 05 — 24-Bit Hilbert Sorting, Equal-Byte Row Groups & Late Materialization](assets/spatiotemporal-hilbert-layout.svg)

See [DuckLake, Wide Columns & Hilbert Modeling](guides/analytics.md#schema-wide-column-spatiotemporal-and-write-for-read-modeling).

---

### FIG 06 — Transactional SQL Outbox, OJS Leases & 3-Segment `TRUNCATE` Streams

Each `s3q_<name>` schema provides two delivery views: **OJS competing jobs** (bridged from application SQL via an outbox, claimed lazily through a single `FOR UPDATE SKIP LOCKED` index on `(priority DESC, next_visible_at, seq)`, and fenced by `lease_token`) and **PgQue-shaped fan-out streams** (using `pg_snapshot` ticks to prevent late-commit sequence gaps and rotating three physical `stream_events_0..2` partitions via zero-bloat `TRUNCATE`).

![FIG 06 — OJS Outbox Claim/ACK Boundaries & 3-Segment TRUNCATE Streams](assets/outbox-jobs-streams.svg)

See [Jobs, Outboxes & Streams](guides/jobs.md) and [Queue & Stream Model](queue-stream.md).

---

### FIG 07 — Quack Client-to-Worker Pool, Gateway Admission & PostgreSQL Capacity

`C` Quack clients hold one active statement per persistent connection and route across `W` `pgvs3-duckdb:2.0.0` workers, each one `DatabaseInstance` with shared `T = 4` threads, an `A = min(4 × CPU, 256)` async HTTP pool, a `256 MiB ≤ M/4` stream buffer, and per-worker native RTree/HNSW memory. `G` gateways admit `32` active + `64` queued S3 requests each (`G ≥ ⌈R/32⌉` zero queue, `G ≥ ⌈R/96⌉` burst), reserve `1 + w` chunk connections per multi-piece read, and can bypass the chunk pool with a bounded `--read-cache-bytes` block cache. PostgreSQL `max_connections` must cover `(G + surge) × (P + min(P, 64)) + Σ a_w` because DuckLake catalog connections bypass pgvs3.

![FIG 07 — Quack Client-to-Worker Pool, Gateway Admission and PostgreSQL Capacity Model](assets/quack-worker-pool.svg)

See [Quack client-to-worker pool capacity model](performance.md#quack-client-to-worker-pool-capacity-model), [worker-pool sizing](operations.md#sizing-a-quack-worker-pool), and the [measured `C × W` pool](benchmarks.md#c-clients-x-w-worker-pool-benchmark).

---

## Quick Start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL 18+ instance).

For a self-contained executable walkthrough of all workload recipes (OLTP plans, PostgreSQL-to-Parquet extraction, same-cluster `pgbench` + `loadgen` canary, DuckLake 24-bit Hilbert + 32-bucket hash compaction, shared cache hits, pending write-back outage recovery, and OJS outbox/stream failure windows), run:

```sh
just setup
just guides     # executes all 35 bound guide snippets in a disposable stack
just analytics  # runs the README Parquet + DuckLake native & origin walkthrough
```

To run a persistent local development database and gateway by hand:

```sh
just setup
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' \
  mise exec go -- go run ./cmd/pgvs3 serve --addr 127.0.0.1:8014
```

Create a bucket explicitly before writing objects:

```sh
curl -fsS -X PUT http://127.0.0.1:8014/lake
curl -fsS -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl -fsS http://127.0.0.1:8014/lake/README.md
```

### Use from DuckDB 2.0+

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, KEY_ID 'test', SECRET 'test',
    REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
COPY (
    SELECT range AS id, (range % 100)::INTEGER AS group_id,
           md5(range::VARCHAR) AS payload
    FROM range(1000000)
) TO 's3://lake/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT group_id, count(*) AS rows
FROM read_parquet('s3://lake/events.parquet')
GROUP BY group_id ORDER BY group_id LIMIT 5;
```

### Use with DuckLake (Catalog and Objects on One Cluster)

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CALL lake.set_option('data_inlining_row_limit', 0);
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

---

## Operating Envelope & Storage Roles

| Namespace | Schema Prefix | Copy of Record & Durability |
| --- | --- | --- |
| **Shared Catalog & Control** | `s3p` | Authoritative namespace catalog ($\le 100$ namespaces), `file_ids` sequence, `cache_fill_leases`, and `pending_uploads` outbox control rows. |
| **Native Object Store** | `s3b_<name>` | WAL-logged PostgreSQL `objects`, `chunks`, `uploads` and `garbage`. No external origin required. |
| **Shared Origin Read Cache & RESP** | `s3c_<name>` | WAL-logged origin read fills, aligned range blocks, `origin_metadata` validators, and standalone RESP2 cache keys. |
| **Durable Origin Write-Back Stage** | `s3w_<name>` | WAL-logged acknowledged write-back bodies (`expires_at IS NULL` while pending handoff to origin; never treat as disposable cache). |
| **Companion Jobs & Streams** | `s3q_<name>` | WAL-logged OJS competing `jobs` and PgQue-shaped 3-segment `stream_events_0..2` fan-out log. |
