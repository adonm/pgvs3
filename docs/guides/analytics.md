# Publish analytical snapshots, not repeated OLTP scans

[Guide index](README.md) · Before: [demo source tables](postgres.md) · Next: [cache](cache.md)

**Goal:** extract a deliberate source snapshot once, then run repeated scans on
Parquet/DuckLake with a measured freshness budget. PostgreSQL remains the OLTP
system; DuckDB executes the analytical SQL; pgvs3 stores or caches the object
bytes. This is application-managed materialization, **not built-in CDC**.
[Research basis](sources.md#analytics-and-object-storage).

## Choose an analytical storage format

| Need | Choice | Cost or limit not to overlook |
| --- | --- | --- |
| Portable immutable extracts; simplest publication | Parquet + application manifest | You own refresh, schema evolution, deletes and manifest/reader retention. |
| Catalog snapshots, table DDL and multi-file lifecycle | DuckLake + PostgreSQL catalog + pgvs3 | Catalog/object commits are separate; maintain files, snapshots and PostgreSQL. |
| Experiment with native DuckDB small commits | [Differential](../differential.md) | Local repository, external writer fencing and a retained large sync-WAL failure. Not the default. |
| Native format over SlateDB | [ObjFS](../object-storage.md) | Extra memory, write/retention costs and SQL read-only clients that still PUT retention checkpoints. Not a standby endpoint client. |

Choose [native objects or origin-backed staging](../operations.md#choosing-and-sizing-a-storage-role)
independently of the file format. Object cache hits reduce remote I/O, not
decoding, joins or aggregation. For repeated join-heavy queries, a local DuckDB
materialization can help, but its refresh, extra storage, RSS and restart/recovery
are separate costs; pgvs3 does not manage that accelerator.

The locally qualified DuckLake workflow is **one coordinated writer per dataset
prefix/catalog and many readers**. That is not an upstream single-writer limit:
DuckLake's PostgreSQL catalog supports multi-client concurrency with conflict
handling. Concurrent-writer/failover policies need their own qualification; a
shared bucket is neither a global writer lock nor a transaction coordinator.

## Export a bounded source snapshot

Prerequisites: DuckDB **2.0.0**, `httpfs`/`postgres` extensions, the disposable
`guide_app.orders` fixture and the quick-start gateway. Create a fresh native
bucket with `curl -fsS -X PUT "$PGVS3_ENDPOINT/guide-analytics"`. Use a source
read-only role in real deployments; the local credentials below are demo-only.

In a new DuckDB session (the test substitutes only host/port/database):

<!-- verify:extract-setup -->
```sql
INSTALL httpfs; LOAD httpfs;
INSTALL postgres; LOAD postgres;
CREATE SECRET (TYPE s3, KEY_ID 'test', SECRET 'test', REGION 'us-east-1',
    ENDPOINT '127.0.0.1:8014', URL_STYLE 'path', USE_SSL false);
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
    DATABASE 'pgvs3_bench', USER 'postgres', PASSWORD 'postgres');
SET threads = 4;
SET memory_limit = '1GB';
SET pg_pool_max_connections = 4;
SET pg_statement_timeout_millis = 30000;
ATTACH '' AS source (TYPE postgres, READ_ONLY, SCHEMA 'guide_app',
    ISOLATION_LEVEL 'REPEATABLE READ');
```

The bounded client settings illustrate syntax, not optimal throughput. Configure
a local disk `temp_directory` and its capacity for larger-than-memory operators.
The PostgreSQL scanner's connections count separately from pgvs3's pools.
Use temporary DuckDB secrets or your deployment's secret injection; do not put
production passwords in URLs, SQL transcripts or persistent secret files.

<!-- verify:extract-copy -->
```sql
BEGIN;
COPY (
    SELECT id, customer_id, created_at, amount_cents, payload
    FROM source.guide_app.orders
    ORDER BY id
) TO 's3://guide-analytics/orders/snapshot-v1.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880);
COMMIT;
```

Use a **new immutable key per generation**. `REPEATABLE READ` keeps source reads
inside this extraction transaction consistent, but a long snapshot can retain
old PostgreSQL tuples. Bound duration and refresh concurrency. Close/commit
promptly; do not keep the source transaction open while dashboards query files.
An S3 publication is not rolled back by a later DuckDB/source transaction error.
Failed exports can leave unpublished files: retain them for investigation, then
clean them only after the reader/publication safety window.

## Validate, then publish discovery

In a fresh DuckDB client pointing at gateway B/the read service, run:

<!-- verify:extract-check -->
```sql
SELECT count(*) AS rows, sum(id) AS id_sum, sum(amount_cents) AS cents
FROM read_parquet('s3://guide-analytics/orders/snapshot-v1.parquet');
```

**Expected:** `1000000 | 499999500000 | 4999500000`. Also validate schema,
business invariants and projected string columns; a metadata-only `count(*)` is
not proof that every data page is readable. The guide test hashes the transferred
body and checks the payload length sum (`32000000`) with client caches disabled.

Only after validation, publish a small discovery object. This example is for a
**native** pgvs3 bucket and creates the pointer only if absent:

<!-- verify:extract-manifest -->
```sh
curl -fsS -X PUT "$PGVS3_ENDPOINT/guide-analytics/current.json" \
  -H 'Content-Type: application/json' -H 'If-None-Match: *' \
  --data-binary '{"generation":"v1","files":["orders/snapshot-v1.parquet"],"rows":1000000,"id_sum":499999500000}'
```

**Expected:** `200`; another create returns `412`, not a silent overwrite. Readers
read one manifest generation and pin its listed keys for the query. Advance a
pointer with the previous **quoted ETag** in `If-Match`; on `412`, reread and
reconcile instead of blindly retrying. Never remove files used by retained
manifests or active readers. A production manifest should also record schema
version, source snapshot/window, completion time, integrity checks and freshness.

Objects and pointer are separate publications, not a multi-key transaction.
With an origin-backed bucket, a new object can still be pending in PostgreSQL
when a synchronous pointer reaches the origin. Do **not** expose that pointer to
origin-only readers until all referenced bodies are verified at the origin.
Monitor outbox drain or use synchronous staging for that delivery contract.

## Freshness is a pipeline, not one TTL

| Stage | Record | Failure policy |
| --- | --- | --- |
| Source extraction | Snapshot/window and source version/watermark | Fail the generation, retain previous known-good manifest. |
| Publication | Validated file set and pointer generation | Reconcile ambiguous PUTs by key/validator; never advertise partial data. |
| Origin staging | Pending bytes/oldest age, remote validation | Alert on drain lag; preserve PostgreSQL-only bodies. |
| Query serving | Manifest/catalog generation, replica lag, result-cache age if any | Reject too-old data or explicitly serve labeled stale data; do not silently query OLTP on every miss. |

These delays can stack. A five-minute refresh plus cache/result TTL and replica
lag is not a five-minute freshness guarantee. A failed refresh has no finite
bound unless you detect it and enforce a maximum served age. Test updates,
deletes, late commits, clock skew and schema changes before incremental refresh.
`max(id)` or `max(updated_at)` alone can miss late-committing rows; overlap alone
does not solve deletes. For continuous source mutations use a separately operated
CDC pipeline with replica identity, keys, deduplication, deletes and slot/WAL
retention designed explicitly. pgvs3 supplies none of that automatically.

## DuckLake: tune a candidate and inspect it

In a **separate** DuckDB session, create the same temporary S3 secret first.
This uses the quick-start `ducklake_catalog` database, not the application one.
The following is a **syntax-validated candidate**, not a measured winning preset:

<!-- verify:lake-options -->
```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
    DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake
    (DATA_PATH 's3://guide-analytics/ducklake/', METADATA_SCHEMA 'guide_lake');
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_row_group_size', 122880);
CALL lake.set_option('target_file_size', '256MB');
CALL lake.set_option('data_inlining_row_limit', 10);
CREATE TABLE lake.orders AS
    SELECT * FROM read_parquet('s3://guide-analytics/orders/snapshot-v1.parquet');
FROM lake.options();
```

Compare projected full scans **and** selective filters at 10/100/1,024 MiB with
`just bench tuning`. Row groups are rows, file targets are bytes; neither is a
hard exact physical file size. Larger groups improve scan/compression efficiency;
smaller ones can improve pruning but cost metadata/requests. Sorting by useful
filters matters more than blindly choosing 8,192-row groups. DuckDB 2.0 Parquet
uses async I/O: inspect `async_threads`, read-ahead and HTTP pool capacity as well
as execution `threads`; keep aggregate client pressure within gateway admission.

Inlining is another choice, not an invisible benchmark advantage:

<!-- verify:lake-inlining -->
```sql
CREATE TABLE lake.inline_probe (i INTEGER);
INSERT INTO lake.inline_probe VALUES (42);
SELECT * FROM lake.inline_probe;
CALL ducklake_flush_inlined_data('lake', table_name => 'inline_probe');
```

**Expected:** `42` before and after flushing. The tiny insert initially lives in
the catalog, not an object; flushing creates a Parquet file. Benchmark pure
object writes with inlining disabled and label that choice. Retain it when it
helps your actual small-commit workload. Persistent `set_option` settings affect
future writes; undo by restoring the saved scoped values, not merely `RESET`ting
an unrelated session option.

## Schema, wide-column, spatiotemporal and write-for-read modeling

When scaling DuckLake and Parquet on pgvs3 to **billion-row collections**, **wide
columns**, **spatiotemporal relations**, or **batch-write / fast-read** workloads,
physical layout determines whether queries scan kilobytes or terabytes.

### 1. Schema and type discipline

- **Use the narrowest native types (`BIGINT`, `INTEGER`, `SMALLINT`, `TIMESTAMPTZ`,
  `DOUBLE`):** Never store numeric IDs or timestamps as `VARCHAR`. DuckDB bitpacks
  integers on disk, but in-memory execution vectors, hash tables and sort buffers
  allocate according to the declared SQL type. Narrow types cut RAM, spill I/O
  and join CPU.
- **Prefer ordered integers, timestamps or `UUIDv7` over random `UUIDv4`:**
  DuckDB and DuckLake rely on file- and row-group-level **zone maps** (`min`/`max`
  bounds). Random `UUIDv4` values span `[0000…, ffff…]` in every row group, making
  zone-map pruning useless. Monotonic integers or timestamps (or `UUIDv7` paired
  with a timestamp filter) keep row-group bounds disjoint.
- **Encode low-cardinality strings as `ENUM` or integer-keyed dimension tables:**
  Parquet dictionary-encodes strings on disk, but repeated joins and `GROUP BY`
  on integer keys execute faster and use less hash memory.
- **Omit `PRIMARY KEY` / `UNIQUE` constraints on analytical tables:** In native
  DuckDB tables, constraints build ART indexes that slow bulk loads by 2–4×,
  consume non-evictable buffer memory, and are not used by joins or aggregations.
  Enforce uniqueness in the OLTP source or batch deduplication step instead.

### 2. Billion-row collections with wide columns

| Bottleneck on wide tables | Mechanism | Concrete fix |
| --- | --- | --- |
| **Row-group memory explosion** | `parquet_row_group_size` defaults to `122880` *rows*. At 8 KiB/row across wide columns, one row group is ~960 MiB uncompressed in writer RAM. | Set `parquet_row_group_size_bytes` (e.g. `'32MB'` to `'128MB'`) or reduce `parquet_row_group_size` (e.g. `16384`–`32768`) on wide tables. |
| **PostgreSQL catalog bloat** | DuckLake records per-file, per-column `min`/`max` stats in `ducklake_file_column_stats`. Hundreds of wide `VARCHAR`/JSON/blob columns across thousands of files bloat the catalog. | Call `lake.set_option('skip_stats_columns', ['wide_col', ...], table_name => '...')` so min/max bounds are only stored for filter/join columns. |
| **Unnecessary byte transfers** | `SELECT *` fetches and decompresses every column chunk in matching row groups. | Project only required columns. For top-K or selective lookups needing wide payloads, filter/rank on narrow keys first, then join back by key (**late materialization**). |
| **Compaction rewrite cost** | Updating or compacting a table rewrites wide blobs alongside narrow metrics. | Split immutable multi-KiB/MiB blobs into a companion table (joined by `id`) or store large blobs as direct pgvs3 objects referenced by key/ETag. |

### 3. Consistent spatiotemporal and multi-dimensional partitioning

Partitioning and sorting solve different problems—confusing them causes small-file
storms:

- **Keep `PARTITIONED BY` coarse (1D temporal lifecycle):** Partitioning splits
  each write into separate physical Parquet files per partition key. In DuckLake
  1.1+, use UTC order-preserving epoch transforms such as `PARTITIONED BY (epoch_day(ts))`
  or `epoch_month(ts)`—**never** bare `month(ts)` or `day(ts)`, which fold
  January 2025 and January 2026 into the same partition (`1`). Size the temporal
  unit so each active partition accumulates at least `64 MiB–512 MiB` of data;
  never partition directly by fine spatial cells or high-cardinality IDs.
- **Cluster 2D space + time with `SORTED BY (spatial_cell_id, ts)`:** Sorting by
  raw `(lat, lon)` orders `lat` but leaves `lon` spanning the entire globe inside
  every `lat` stripe, so 2D bounding-box queries cannot prune row groups on `lon`.
  Instead, compute a deterministic 2D locality-preserving cell ID—such as a
  bit-interleaved Morton (Z-order) key, Hilbert index, H3/S2 cell ID, or quantized
  grid key—and sort by `(cell_id, ts)` while retaining explicit `lat`/`lon` (or
  bounding-box `min_x, max_x, min_y, max_y`) columns.
- **Query with both the coordinate box and the cell range:** File/row-group zone
  maps prune on `ts`, `cell_id`, `lat` and `lon` before exact geometry predicates
  run on the surviving rows.

### 4. Trade slow/batch transactions for maximum query performance

When you can batch writes (minutes/hours) to maximize read speed:

1. **Ingest in batches and sort + compress heavily:** Accumulate batches rather
   than committing tiny files. Either insert pre-sorted batches (`SORTED BY (...)`
   with `parquet_compression = 'zstd'` and `parquet_compression_level = 9` to `19`),
   or set `sort_on_insert = false` during fast micro-batch ingest and let the
   batch compaction step sort the merged output.
2. **Compact and rewrite before serving:** Run `ducklake_flush_inlined_data`,
   `ducklake_merge_adjacent_files` (which merges small files into sorted
   `target_file_size` Parquet files without losing snapshot history), and
   `ducklake_rewrite_data_files` with a low `delete_threshold` so readers pay
   **zero** merge-on-read positional-delete overhead.
3. **Pre-compute rollups or seal a native database when joins dominate:** Parquet
   lacks DuckDB's internal HyperLogLog cardinality statistics. For complex
   multi-table joins, either materialize batch rollup/denormalized tables in
   DuckLake, or build a read-mostly native DuckDB database on [ObjFS](../object-storage-results.md)
   and **cleanly close the writer** (`CHECKPOINT` + `writer.close()`), which
   flushes SlateDB memtables and cut 1 GiB fresh selective reads **6×** (313 ms →
   52.6 ms) and full scans **2.2×** (1,026 ms → 476 ms) in our benchmark.

Run this executable spatiotemporal + wide-column + batch-compaction recipe in the
same DuckLake session:

<!-- verify:lake-spatiotemporal -->
```sql
CREATE TABLE lake.spatiotemporal (
    id BIGINT,
    ts TIMESTAMPTZ,
    lat DOUBLE,
    lon DOUBLE,
    cell_id BIGINT,
    wide_payload VARCHAR
)
PARTITIONED BY (epoch_day(ts))
SORTED BY (cell_id, ts);

CALL lake.set_option('skip_stats_columns', ['wide_payload'], table_name => 'spatiotemporal');
CALL lake.set_option('parquet_row_group_size_bytes', '2MB', table_name => 'spatiotemporal');
CALL lake.set_option('parquet_compression', 'zstd', table_name => 'spatiotemporal');
CALL lake.set_option('parquet_compression_level', 9, table_name => 'spatiotemporal');

INSERT INTO lake.spatiotemporal
SELECT i,
       TIMESTAMPTZ '2026-10-01 00:00:00+00' + (i % 2) * INTERVAL '1 day' + (i % 3600) * INTERVAL '1 second',
       (i % 100) * 0.01,
       ((i // 100) % 100) * 0.01,
       ((i % 100) // 10) * 10 + (((i // 100) % 100) // 10),
       repeat(md5(i::VARCHAR), 8)
FROM range(20000) AS t(i);

INSERT INTO lake.spatiotemporal
SELECT i + 20000,
       TIMESTAMPTZ '2026-10-01 00:00:00+00' + (i % 2) * INTERVAL '1 day' + (i % 3600) * INTERVAL '1 second',
       (i % 100) * 0.01,
       ((i // 100) % 100) * 0.01,
       ((i % 100) // 10) * 10 + (((i // 100) % 100) // 10),
       repeat(md5((i + 20000)::VARCHAR), 8)
FROM range(20000) AS t(i);

CALL ducklake_merge_adjacent_files('lake', 'spatiotemporal');
DELETE FROM lake.spatiotemporal WHERE id < 5000;
CALL ducklake_rewrite_data_files('lake', 'spatiotemporal', delete_threshold => 0.1);

WITH matched AS (
    SELECT id, cell_id
    FROM lake.spatiotemporal
    WHERE ts >= TIMESTAMPTZ '2026-10-01 00:00:00+00'
      AND ts < TIMESTAMPTZ '2026-10-02 00:00:00+00'
      AND cell_id = 12
      AND lat BETWEEN 0.10 AND 0.19
      AND lon BETWEEN 0.20 AND 0.29
)
SELECT count(*) AS rows, min(m.cell_id) AS min_cell, max(length(s.wide_payload)) AS payload_bytes
FROM matched AS m
JOIN lake.spatiotemporal AS s USING (id);
```

**Expected:** `150 | 12 | 256`. The two inserts are partitioned by UTC epoch day,
sorted by `(cell_id, ts)`, compacted from two files per day into one sorted file
per day, and rewritten after the delete so readers scan clean ZSTD files. The
catalog omits `min_value`/`max_value` strings for `wide_payload`, and the query
demonstrates late materialization on the matched IDs.

DuckLake maintenance (merge/rewrite, expire snapshots, clean old/orphan files),
pgvs3 reaping and PostgreSQL vacuum are **three distinct stages**. `DROP TABLE`
is not remote space reclamation. Use the installed extension's maintenance
reference and a retention window longer than active readers/writers and recovery
needs; zero-age cleanup examples from benchmarks are not a production policy.
Coordinate catalog, data database and any external origin backups.

## Verification and cleanup

`just guides` executes these snippets, reopens the catalog through another
gateway, demonstrates inlining/flush and proves a later source change does not
update the existing extract. [Recorded scope](validation.md). For tuning
acceptance use the [paired runner](../benchmarks.md#paired-acceptance), not these
functional timings. Keep the demo data for jobs, then use
[owned-data cleanup](observe-recover.md#safe-demo-cleanup).
