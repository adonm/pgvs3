# Performance

This page maps the techniques in [pgvfs.adonm.dev](https://pgvfs.adonm.dev)
to the Go gateway. The two projects share the same PostgreSQL layout and read
path; pgvs3 adds an HTTP boundary and multipart semantics.

## Storage layout

| Technique | Where |
| --- | --- |
| 8,120-byte payload per row | `RowBytes` in `internal/store/ingest.go` |
| One row fills one 8 KB page at 99.6% | `toast_tuple_target = 8160`, `STORAGE EXTERNAL` and the 24-byte tuple header: `file_id(8) + no(4) + varlena(4) + 8120 = 8136` data bytes |
| No TOAST, no detoasting on read | `internal/store/schema.sql`: `data bytea STORAGE EXTERNAL NOT NULL` |
| Immutable files | A write allocates a new `file_id`; an overwrite publishes it; rows are never updated |
| `file_id` as a cache version tag | ETags are strong; DuckDB's external file cache keys on the URL and version tag |
| Ordered listings are index scans | `key COLLATE "C"` primary key; `SET LOCAL enable_indexscan = on` for one listing |
| Chunk ranges are bitmap heap scans | Session `enable_indexscan = off`; PostgreSQL 18 read-ahead; `effective_io_concurrency = 32` |
| One unpartitioned chunks table | `s3p.chunks`; every read is `file_id = $1 AND no BETWEEN ...` |
| 32 TiB relation cap | `MaxBlockNumber × 8 KB`; about 31.7 TiB of object data |
| Autovacuum at 1% dead rows | Table reloptions, plus `autovacuum_analyze_scale_factor = 0.02` and `threshold = 1000` |

## Read path

- **Up to 64 MiB is one query for a single-file object.** `wholeReadMax = 64 << 20`;
  the query returns whole rows for a contiguous `no` range and checks the
  object's current `file_id`. One query avoids the per-piece planning and
  connection overhead that a 64 MiB object would otherwise pay.
- **Larger and multipart reads split into 8 MiB pieces.** Up to eight pieces are
  fetched in parallel on separate connections under one exported
  repeatable-read snapshot, so a concurrent overwrite cannot remove later rows.
  More than eight pieces fall back to a sequential snapshot to bound connection
  use.
- **Rows are cut through to HTTP.** `Body.WriteTo` writes each PostgreSQL chunk
  straight to the response. A bounded channel (32 chunks) backpressures
  PostgreSQL when the client is slow.
- **Out-of-order rows are held.** Bitmap scans return TID order; `streamRows`
  buffers the few rows that run ahead and emits `no` order.
- **256 KiB response chunks** amortise syscalls without adding a large copy.
- **Metadata is cached.** Otter (W-TinyLFU) serves local metadata reads without
  a lock; `HEAD` always reads fresh metadata so size/ETag cannot be stale.
- **Small objects are cached.** A per-gateway Otter byte cache holds whole
  objects up to 256 KiB. Single-gateway writes invalidate it locally; with
  peers, each hit revalidates `file_id`/ETag against `s3p.objects`. See
  “Small-object cache” below.
- **Stale entries fail safe.** A range query verifies the current object still
  points at the cached `file_id`; a stale entry causes one fresh retry, never
  stale bytes.
- **Conditional reads are cheap.** `If-Match` compares the cached ETag first and
  refreshes only on a mismatch. `If-None-Match` reads fresh metadata before a
  `304`.
- **No block read-ahead.** As in pgvfs, DuckDB already merges nearby reads;
  fetching 254 KiB blocks was 30–38% slower cold.
- **No partitioning by default.** See the measured 16/64-partition regression
  above.
- **No index scans for chunk ranges.** PostgreSQL execution was already
  0.01–0.03 ms; the bitmap plan wins on cold pages.

## Small-object cache

Parquet footers, delete files and manifests are small and repeated. The gateway
caches whole objects up to 256 KiB in Otter (default 64 MiB, 5 s TTL).

- **Single gateway:** local writes invalidate the key; hits are served from RAM
  with no PostgreSQL round trip. 4 KiB GET p50 drops from 0.76 ms to 0.56 ms,
  and 64-worker throughput from about 3.9k/s to 6.8k/s on the benchmark host.
- **Peer mode:** a hit first checks `SELECT file_id, etag FROM s3p.objects`.
  That costs about as much as reading a 4 KiB row, so the cache mainly saves
  bytes and PostgreSQL CPU rather than latency. Set
  `PGVS3_OBJECT_CACHE_REVALIDATE=false` only for an immutable lake with a
  single writer.
- **TTL:** five seconds bounds staleness if an invalidation is lost; a
  revalidated hit does not rely on it.

This is not a block cache. Large objects are streamed and left to DuckDB's
external file cache.

## Partitioning

See [partitioning.md](partitioning.md) for the full key-by-key analysis.

`s3p.chunks` is one unpartitioned table by default. Hash-partitioning by
`file_id` was measured at 16 and 64 partitions:

| 4 KiB workload | 1 partition | 16 | 64 |
| --- | ---: | ---: | ---: |
| GET, 1 worker | 1,104/s | 888/s | 945/s |
| GET, 16 workers | 3,852/s | 2,784/s | 3,227/s |
| PUT, 1 worker | 218/s | 187/s | 213/s |
| PUT, 64 workers | 1,177/s | 1,118/s | 933/s |

A two-level `HASH(file_id) → RANGE(no)` layout (8 hash × 8 row ranges, read in
8 MiB parallel pieces) improves a single 64 MiB GET from 441 to 556 MiB/s, but
still costs 16–21% on 4 KiB GETs and 10% on 16-worker PUTs. It does not make
partitioning a default win.

The reason is structural: a point read needs `file_id`, so any key that spreads
a file's rows (`no`, `hash(no)`, `(file_id,no)`) either scans every partition or
adds range-pruning/planner work. A key that keeps a file together
(`file_id`, `bucket`) leaves each file in one partition and gains nothing for a
single-file read. With DuckLake's single writer there is no insert contention
to spread. The one-table layout wins; a future multi-writer design can revisit
partitioning.

## Write path

- **One binary `COPY` per PUT or part.** `internal/store/ingest.go` frames
  4 MiB batches; the body is hashed (SHA-256 for integrity, MD5 for the ETag) in
  the same pass.
- **Publish in the same transaction.** Chunks and the `s3p.objects` row commit
  together. An interrupted or failed write publishes nothing.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging; Complete publishes the ordered part list. It validates ETags and
  checksums but moves no data.
- **CopyObject is metadata-only.** The destination row shares the source's
  immutable file IDs. `RenameObject` updates the key. The reaper only queues a
  file when no object or upload part references it.
- **AppendObject writes only new bytes.** `x-amz-write-offset-bytes` becomes
  another part; the object's `parts`/`part_ends` arrays grow in one transaction.
- **No sharded COPY.** One stream already writes about 300 MiB/s and more than
  four streams were slower on an 8-core PostgreSQL. Publishing shards early
  would also leak rows from a crashed writer.
- **No `wal_compression`.** It only compresses full-page images, and the pages
  hold already-compressed Parquet.
- **No `full_page_writes = off`.** That risks the whole cluster.

## Connections

- **Most-recently-used first.** pgxpool/puddle reuses the hottest idle
  connection, avoiding TCP slow-start after an idle RTO.
- **Warm minimum.** `PGVS3_POOL_MIN` connections are opened at startup; opening
  one (TCP, TLS, SCRAM, session `SET`s) costs about 10 ms.
- **No ping on checkout.** A dead connection fails its query and is discarded.
- **Session settings** are applied once per connection: bitmap scans for chunks,
  `work_mem = 64MB`, read-ahead concurrency, TCP keepalives, a 5-minute
  `idle_in_transaction_session_timeout`, and synchronous commit.

## Reaping and vacuum

- **Grace before reaping.** Deleted files stay readable for
  `PGVS3_REAP_GRACE` (10 minutes). A read that resumes after the grace fails on
  its next batch, with a message that names the setting.
- **Bounded work.** Each run removes at most 65,536 chunk rows (about 508 MiB),
  then stops; the next run continues.
- **One reaper per cluster.** `pg_try_advisory_lock` serialises reapers across
  gateways.
- **Whole-file deletes reuse pages.** Autovacuum at 1% keeps the heap near live
  data instead of waiting for 20% dead rows.
- **Long transactions hold pages.** A reader with an old snapshot, or a long
  write, prevents vacuum from freeing pages of files dropped during it. Plain
  `VACUUM` does not return them to the operating system.

## Cross-AZ metadata cache

- **Consistent hashing.** Every metadata key has one owner among the peers.
- **Global singleflight.** Concurrent misses for one key are coalesced on the
  owner, so PostgreSQL sees one fill even when all three AZs miss together.
- **Otter hot cache.** A gateway mirrors remotely-owned keys after fetching
  them. The default hot cache is 100% of the main cache; size it from the
  working set, not the lake size.
- **Best-effort invalidation.** Writes enqueue `Remove` in the background. If a
  removal is lost, the 10-second TTL plus the `file_id` check bounds it to one
  retry.
- **Why not object bytes.** DuckDB already caches file bytes keyed by ETag.
  Caching them again in the gateway doubles memory and sends the same object
  across the AZ boundary twice.

The 3-AZ penalty is real: two-thirds of keys are owned by another node. A
metadata hit in the local hot cache avoids the cross-AZ hop; the first read of
a remotely-owned key pays it once. Measure with `PGVS3_LOG_LEVEL=debug` or the
groupcache stats before sizing the caches.

## Client-side settings

These are DuckDB settings, not gateway settings, but they decide most of the
read speed:

```sql
-- DuckLake, once per lake, before the first insert
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_version', 2);
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('target_file_size', '64MB');

-- Immutable lake files make these safe
SET parquet_metadata_cache = true;
```

DuckDB 2.0 enables the external file cache for remote files by default. It
caches only what it reads; the gateway is not a second cache.

## Deliberately not done

- Pipelining a read's pieces: every measured read was a single piece.
- Hedged retries: tails come from CPU queueing, so a hedge adds load exactly
  when PostgreSQL is saturated.
- A gateway block cache: DuckDB's external cache already serves warm reads
  within `memory_limit`.
- Partitioning `s3p.chunks`: DuckLake has one writer; the cost is the 32 TiB
  relation cap.
