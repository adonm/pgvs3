# Performance Architecture

pgvs3 shares its PostgreSQL storage layout with
[pgvfs](https://pgvfs.adonm.dev) and exposes it over an HTTP/S3 boundary.
This document explains why the storage layout and query paths are shaped the
way they are.

## Storage layout

| Mechanism | How it works |
| --- | --- |
| **One schema per namespace, typed by prefix** | `s3b_<name>` (durable bucket), `s3c_<name>` (WAL-logged shared cache), `s3w_<name>` (WAL-logged write-back) and `s3q_<name>` (OJS queue/stream) each own their tables, autovacuum workers and 32 TiB chunk budget. Namespaces can be snapshotted with `pg_dump -n`, cloned in-database and dropped with `DROP SCHEMA CASCADE`. |
| **Inline small objects (≤64 KiB)** | Stored directly in `objects.inline_data` (`file_id NULL`, Zstd-compressed when compressible) with optional `expires_at`. Written in one `put_inline` call and read in one primary-key lookup without touching `chunks`. |
| **PostgreSQL 18 publication** | Inline/chunked entry points share `publish_value`; `RETURNING old.file_id, old.parts` supplies overwritten-file ownership without a pre-read on unconditional writes. Preconditions, TTL, metadata, lease fencing and garbage publication remain transactional. |
| **Shared range parent metadata** | A logged schema-local `origin_metadata` record caches size/validator/headers with validation TTL but never pretends to hold an object body. Write-back/read-cache shadowing resolves in one SQL round trip; read-only workers can use replicated metadata and cached range blocks. |
| **Length-discriminated Zstd 8,120-byte chunk blocks** | Larger objects split into logical 8,120-byte blocks in `chunks` (the block-encoding section of `internal/store/object.go`). Each block is compressed with a reusable Zstd frame if it shrinks (`len(stored) < wantLen`) or stored raw (`len(stored) == wantLen`), so incompressible blocks never exceed the 8,160-byte page budget and logical offsets remain `no × 8,120` for `O(1)` byte-range seeking. |
| **One-port protocol mux** | `internal/gateway` routes HTTP (`S3`, `OJS`, plain web) and RESP2 by first bytes on `:8014`; RESP batches pipelined `GET`/`SET` bursts into single-query `Cache.MGet` / `Cache.MSet` round trips against `s3c_<namespace>`. |
| **Message queues** | `s3q_<name>.jobs` holds one explicit OJS job envelope per row with state, priority, attempt and lease columns; `s3p.job_index` routes an id back to its queue. Fetch is a single `FOR UPDATE SKIP LOCKED` claim over available, due scheduled/retryable and expired-lease rows, ordered by priority then `next_visible_at`; ack/nack/heartbeat match the lease token and the database clock so a reclaimed delivery returns `409`. Multi-queue fetch resolves missing queues against the catalog before opening one claim transaction; it deliberately avoids per-queue savepoints, so `SKIP LOCKED` scans stay on the fast plain-row-lock path instead of contending on MultiXact LWLocks (see [Notes on avoiding subtransactions](https://riverqueue.com/blog/subtransaction-free)). The queue list is capped at 64. Expiry, due promotion and exhausted leases run in maintenance, not on every poll; terminal jobs are removed after `--job-retention`. The same namespace holds an append-only stream log split into three rotating partitions with snapshot ticks, fan-out subscription cursors, batch acks, retry rows and dead letters; maintenance moves due retries and truncates a sealed partition only after consumer cursors and batches have passed its seal, so stream retention is zero-bloat. |
| **Read-through origin cache** | `internal/objcache` wraps origin adapters (the Apache-2.0 `internal/s3client` fork of `kelindar/s3`, read-only HTTP(S), filesystem or temporary-directory `inmem` for local tests): `GET` revalidates against origin size/version after the TTL and downloads into the WAL-logged `s3c_<namespace>`; reads within the TTL come from PostgreSQL, and a whole-object miss uses one origin GET carrying size/version metadata instead of a separate HEAD. Ranges above 8 MiB are passed through with `GetRange` so a footer read never pulls the whole file, unless `--range-cache-chunk` is enabled, in which case aligned chunks are cached independently. Fills are capped by `--fill-concurrency`, spool to memory (≤8 MiB) or a temp file in `--spool-dir` before the PostgreSQL writer opens, and publish conditionally against the expected origin version so a slow fill cannot overwrite a newer write-through write. Cross-gateway cold fills take a short database lease (`s3p.cache_fill_leases`); waiters poll the replicated cache and only pass through to the origin after the lease wait expires, avoiding an N-worker origin stampede. For writable origins, small and conditional `PUT`/`COPY`/`DELETE` writes are synchronous write-through with checksum validation, conditions, Content-Type and user metadata. Large unconditional `PUT`/`COPY` and multipart completions are ack-first write-back: the body is committed to the WAL-logged `s3w_<namespace>` and an `s3p.pending_uploads` row atomically, the body is immediately readable, survives crash/failover, and a bounded worker retries the origin upload. The WAL-logged `s3c_` namespace holds shared read fills and RESP/cache values. DELETE writes a thirty-minute tombstone to both namespaces so an in-flight fill cannot resurrect the object. `ListObjectsV2` merges the origin walk with pending write-back cache objects and retains only one page in memory; there is no listing index or TTL-stale refresh path. |
| **Admission control** | S3 requests, OJS requests and RESP batches are bounded by `--s3-concurrency`, `--queue-concurrency` and `--resp-concurrency`; cache fills and pending-upload workers use the process-wide `--fill-concurrency` budget. S3 overflow gets `503 SlowDown` with `Retry-After`, so a queue backlog or cache storm cannot occupy the whole `--pool-max`. |
| **No TOAST overhead** | `chunks` sets `toast_tuple_target = 8160` and `STORAGE EXTERNAL`, so PostgreSQL never detoasts or chases out-of-line pointers on read. |
| **Immutable files** | Every write allocates a fresh `file_id` from `s3p.file_ids`; overwrites swap the pointer on `objects` and queue the old `file_id` for reaping. Chunk rows are never updated in place. |
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
6. **Warm cache hit:** the cache tier reuses the metadata row it already read
   for the TTL/lifecycle decision when opening the body, so a warm cache GET
   does not repeat the primary-key lookup. Inline cache objects are served
   directly from that row; larger ones start the normal chunk stream.

## Write path

1. **Inline writes (≤64 KiB):** `s3b_<bucket>.put_inline` takes a per-key
   transaction advisory lock, checks `If-Match` / `If-None-Match`, queues any
   replaced `file_id` for GC, and upserts the row in one round trip. The bucket
   must already exist in the namespace catalog; a missing or deleted bucket
   returns `NoSuchBucket` and is never recreated implicitly. The HTTP handler
   reuses a bounded 64 KiB request buffer and releases it after the SQL call,
   so a stream of small PUTs does not allocate a fresh body for every request.
2. **Streaming binary `COPY` (>64 KiB):** `internal/store/write.go` hashes the
   request body (MD5 + SHA-256/CRC) while framing 4 MiB binary `COPY` batches
   from a `sync.Pool` into `s3b_<bucket>.chunks`. Full rows are framed straight
   from the incoming read buffer; only the trailing partial row is retained.
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

- **Stateless gateways:** Gateways hold no authoritative in-process object,
  metadata, namespace, file-ID, or lease state. The namespace catalog in
  PostgreSQL decides what exists; `shared_buffers` is the single shared cache
  across the fleet, so committed writes are immediately visible to every other
  **writable** gateway with no `LISTEN/NOTIFY` traffic or cache invalidation
  window. Read-only gateways connected to standbys are subject to PostgreSQL
  replication lag.
- **One maintenance leader:** a single writable gateway holds the maintenance
  advisory lock; the others skip the tick. If the leader exits, another elects
  itself on a later tick.
- **Conditional cache publishes:** read-through fills are fenced by the
  expected origin version, and multipart flushes carry a lease plus a token
  guard. A stale fill or flush is discarded instead of overwriting a newer
  write-through row. Write-through deletes leave an expiring tombstone so a
  fill that started before the delete cannot reinsert the object.
- **MRU connection reuse:** `pgxpool` reuses the most-recently-used idle
  connection first, keeping TCP connections warm and avoiding slow-start
  penalties. Connections recycle after a finite lifetime with jitter.
- **Once-per-connection setup:** Each connection applies a configurable
  `work_mem` (default `16MB`), `effective_io_concurrency = 32`, TCP keepalives,
  statement/lock/idle-transaction timeouts, and `synchronous_commit = on`.
  The chunk pool deliberately disables the statement timeout for long streaming
  reads; upload transactions disable it around `COPY`.

## Reaping and vacuum

- **Read grace window:** Deleted or overwritten files remain in `s3p.chunks` for
  `--reap-grace` (default 10 minutes) so in-flight readers can finish without
  holding long transactions.
- **Bounded, chained GC passes:** Every minute, one **writable** gateway
  acquires a cluster-wide `pg_try_advisory_lock` and deletes up to 65,536 chunk
  rows (~508 MiB) per pass; a saturated pass immediately chains another. If
  the leader exits, another writable gateway acquires the lock on a later tick.
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
