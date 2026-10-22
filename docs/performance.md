# Performance Architecture

pgvs3 shares its PostgreSQL storage layout with
[pgvfs](https://pgvfs.adonm.dev) and exposes it over an HTTP/S3 boundary.
This document explains why the storage layout and query paths are shaped the
way they are.

## Storage layout

| Mechanism | How it works |
| --- | --- |
| **One schema per namespace, typed by prefix** | `s3b_<name>` (durable bucket), `s3c_<name>` (cache, UNLOGGED tables) and `s3q_<name>` (SQS queue) each own their tables, autovacuum workers and 32 TiB chunk budget. Namespaces can be snapshotted with `pg_dump -n`, cloned in-database and dropped with `DROP SCHEMA CASCADE`. |
| **Inline small objects (≤64 KiB)** | Stored directly in `objects.inline_data` (`file_id NULL`, LZ4-compressed when compressible) with optional `expires_at`. Written in one `put_inline` call and read in one primary-key lookup without touching `chunks`. |
| **Length-discriminated LZ4 8,120-byte chunk blocks** | Larger objects split into logical 8,120-byte blocks in `chunks` (`internal/store/block.go`). Each block is LZ4-compressed if it shrinks (`len(stored) < wantLen`) or stored raw (`len(stored) == wantLen`) with zero header bytes, so incompressible blocks never exceed the 8,160-byte page budget and logical offsets remain `no × 8,120` for `O(1)` byte-range seeking. |
| **One-port protocol mux** | `internal/gateway` routes HTTP (`S3`, `SQS`, plain web) and RESP2/RESP3 by first bytes on `:8014`; RESP batches pipelined `GET`/`SET` bursts into single-query `CacheMGet` / `CacheMSet` round trips against `s3c_<namespace>`. |
| **Message queues** | `s3q_<name>.messages` holds one compressed body per row with `visible_at`/`receipt` leases; receive is a single `FOR UPDATE SKIP LOCKED` statement and delete is keyed by receipt. |
| **Read-through origin cache** | `internal/objcache` wraps a thanos `objstore.Bucket`: `GET` verifies size+mtime, downloads into `s3c_<namespace>`, and `PUT`/`DELETE` write through and refresh the cached copy. |
| **No TOAST overhead** | `chunks` sets `toast_tuple_target = 8160` and `STORAGE EXTERNAL`, so PostgreSQL never detoasts or chases out-of-line pointers on read. |
| **Immutable files** | Every write allocates a fresh `file_id`; overwrites swap the pointer on `objects` and queue the old `file_id` for reaping. Chunk rows are never updated in place. |
| **Strong ETag caching** | `file_id` backs strong ETags so DuckDB's external file cache can reuse local blocks without re-fetching. |
| **Split connection pools** | The main pool keeps `enable_indexscan = on` for fast B-tree lookups on `objects` and `ListObjectsV2`; a dedicated `chunkPool` sets `enable_indexscan = off` so `chunks` range reads use PostgreSQL 18 bitmap heap scans with read-ahead (`effective_io_concurrency = 32`). |
| **Unpartitioned chunks table per bucket** | Each bucket's `chunks` is a single table (capped at 32 TiB / ~31.7 TiB of payload). Partitioning within a bucket by hash or range was slower on point reads and writes; see [partitioning.md](partitioning.md). |
| **Aggressive autovacuum** | Tuned to `autovacuum_vacuum_scale_factor = 0.01` and `analyze_scale_factor = 0.02` so freed pages are recycled quickly after whole-file deletes. |

## Read path

1. **Inline fast path (≤64 KiB):** `Meta` fetches metadata and `inline_data` in
   a single primary-key `Index Scan` on `objects` (~6 µs in `shared_buffers`,
   ~0.17 ms p50 over HTTP). `HEAD` queries only header columns and never
   transfers `inline_data`.
2. **Single-query reads (64 KiB – 64 MiB):** A single-file read up to 64 MiB
   runs as one `chunks` range query (`file_id = $1 AND no BETWEEN $2 AND $3`).
3. **Parallel snapshot reads (>64 MiB and multipart):** Larger spans split into
   8 MiB pieces and fetch up to 8 pieces concurrently on separate connections
   under one exported `REPEATABLE READ` snapshot.
4. **Synchronous vs cut-through streaming:** Reads up to 8 MiB stream
   synchronously on the HTTP goroutine. Above 8 MiB, a background fetcher feeds
   a bounded 32-chunk channel while `Body.WriteTo` flushes 256 KiB buffers to
   the socket, overlapping database I/O with network writes and backpressuring
   slow clients.
5. **TID-to-row-number reordering:** Bitmap heap scans return pages in physical
   TID order; `streamRows` buffers any out-of-order rows in memory and emits
   strict `no` order.

## Write path

1. **Inline writes (≤64 KiB):** `s3b_<bucket>.put_inline` takes a per-key
   transaction advisory lock, checks `If-Match` / `If-None-Match`, queues any
   replaced `file_id` for GC, and upserts the row in one round trip. If the
   bucket schema does not exist yet, the gateway calls `s3p.ensure_bucket` once
   and retries.
2. **Streaming binary `COPY` (>64 KiB):** `internal/store/ingest.go` hashes the
   request body (MD5 + SHA-256/CRC) while framing 4 MiB binary `COPY` batches
   from a `sync.Pool` into `s3b_<bucket>.chunks`.
3. **Atomic publish:** In the same transaction as `COPY`,
   `s3b_<bucket>.publish_object` evaluates conditional headers, updates
   `objects`, and queues any replaced `file_id`. If a client disconnects
   mid-upload, the transaction rolls back and publishes nothing.
4. **Zero-copy multipart, `CopyObject`, and bucket clones:** Multipart parts
   upload in parallel as independent `file_id`s; `CompleteMultipartUpload`
   validates checksums and records the ordered `file_id` array without moving
   bytes. Same-bucket `CopyObject` and whole-file `UploadPartCopy` share the
   source's immutable `file_id`s; cross-bucket `CopyObject` and
   `s3p.clone_bucket` copy chunk rows inside PostgreSQL without streaming bytes
   through the gateway.
5. **Single-query batch delete:** `DeleteObjects` locks and deletes up to 1,000
   keys and queues unreferenced files in two SQL statements instead of 1,000
   round trips.

## Multi-gateway consistency and connection pooling

- **Stateless gateways:** Gateways hold no in-process object or metadata cache.
  PostgreSQL `shared_buffers` is the single shared cache across the fleet, so
  writes committed on one pod are immediately visible on every other pod with
  no `LISTEN/NOTIFY` traffic or cache invalidation window.
- **MRU connection reuse:** `pgxpool` reuses the most-recently-used idle
  connection first, keeping TCP connections warm and avoiding slow-start
  penalties.
- **Once-per-connection setup:** Each connection applies `work_mem = 64MB`,
  `effective_io_concurrency = 32`, TCP keepalives, a 5-minute
  `idle_in_transaction_session_timeout`, and `synchronous_commit = on`.

## Reaping and vacuum

- **Read grace window:** Deleted or overwritten files remain in `s3p.chunks` for
  `--reap-grace` (default 10 minutes) so in-flight readers can finish without
  holding long transactions.
- **Bounded, chained GC passes:** Every minute, one gateway acquires a
  cluster-wide `pg_try_advisory_lock` and deletes up to 65,536 chunk rows
  (~508 MiB) per pass; a saturated pass immediately chains another.
- **TTL expiry is chained too:** `expire_objects(4096)` runs repeatedly until a
  pass comes back short, so a burst of expiring keys drains in one tick instead
  of ~4 keys/s/bucket.
- **Space reuse:** Freed pages are reused by subsequent writes once autovacuum
  marks them dead. Long-running snapshots hold back vacuum horizon; avoid
  multi-hour open transactions on the storage database.

## Recommended DuckDB / DuckLake client settings

Client settings dominate end-to-end analytics query speed:

```sql
-- Configure once per DuckLake catalog before loading data
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_version', 2);
CALL lake.set_option('parquet_row_group_size', 8192);
CALL lake.set_option('target_file_size', '64MB');

-- Safe because lake files are immutable
SET parquet_metadata_cache = true;
```

DuckDB 2.0+ caches remote file slices in its own `external_file_cache` by
default, keyed by URL and strong ETag (`file_id`).
