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

- **Up to 8 MiB is one query.** `smallMax = 8 << 20`; a query returns whole rows
  for a contiguous `no` range and checks the object's current `file_id`.
- **Larger reads share a repeatable-read snapshot.** `streamSnapshot` opens one
  read-only transaction for all pieces, so a concurrent overwrite cannot remove
  later rows.
- **Rows are cut through to HTTP.** `Body.WriteTo` writes each PostgreSQL chunk
  straight to the response. A bounded channel (32 chunks) backpressures
  PostgreSQL when the client is slow.
- **Out-of-order rows are held.** Bitmap scans return TID order; `streamRows`
  buffers the few rows that run ahead and emits `no` order.
- **256 KiB response chunks** amortise syscalls without adding a large copy.
- **Metadata is cached.** Otter (W-TinyLFU) serves local metadata reads without
  a lock; `HEAD` always reads fresh metadata so size/ETag cannot be stale.
- **Stale entries fail safe.** A range query verifies the current object still
  points at the cached `file_id`; a stale entry causes one fresh retry, never
  stale bytes.
- **Conditional reads are cheap.** `If-Match` compares the cached ETag first and
  refreshes only on a mismatch. `If-None-Match` reads fresh metadata before a
  `304`.
- **No block read-ahead.** As in pgvfs, DuckDB already merges nearby reads;
  fetching 254 KiB blocks was 30–38% slower cold.
- **No index scans for chunk ranges.** PostgreSQL execution was already
  0.01–0.03 ms; the bitmap plan wins on cold pages.

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
