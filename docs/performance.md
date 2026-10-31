# Object data-plane performance

The primary workload is 10 MiB–1 GiB Parquet objects.
The layout and I/O paths below make PostgreSQL practical as the byte store for
native buckets, shared read fills and durable origin staging. The page-oriented
layout is also used by [pgvfs](https://pgvfs.adonm.dev).

[design.md](design.md#authority-and-acknowledgement-contracts) defines ownership
and acknowledgement; this page explains the mechanisms, not a durability or
capacity benchmark. Use [paired acceptance](benchmarks.md#paired-acceptance) for
per-path evidence and the [data-plane proof/capacity ledger](data-plane-results.md)
for replica, recovery and resource measurements. The [roadmap](roadmap.md) keeps
the remaining deployment-specific limits explicit.

## Storage layout

| Mechanism | How it works |
| --- | --- |
| **Namespace-local relations** | `s3b_<name>` (native bucket), `s3c_<name>` (shared read cache/RESP values), `s3w_<name>` (write-back) and `s3q_<name>` (jobs/streams) have their own tables and settings. They share cluster I/O, WAL and autovacuum capacity, not dedicated workers per namespace. All are WAL-logged. |
| **Inline small objects** | Native/write-cache objects can inline up to 64 KiB and read-cache/RESP values up to 8,120 bytes in `objects.inline_data` with `file_id NULL`. Zstd is used only when it shrinks the body. A primary-key lookup avoids `chunks`; multipart/pending paths can retain chunked body/outbox publication even when small. |
| **PostgreSQL 18 publication** | Inline/chunked entry points share `publish_value`; `RETURNING old.file_id, old.parts` supplies overwritten-file ownership without a pre-read on unconditional writes. Preconditions, TTL, metadata, lease fencing and garbage publication remain transactional. |
| **Shared range parent metadata** | A logged schema-local `origin_metadata` record caches size/validator/headers with validation TTL but never pretends to hold an object body. Write-back/read-cache shadowing resolves in one SQL round trip; read-only workers can use replicated metadata and cached range blocks. |
| **Length-discriminated Zstd 8,120-byte chunk blocks** | Larger objects split into logical 8,120-byte blocks in `chunks` (the block-encoding section of `internal/store/object.go`). Each block is compressed with a reusable Zstd frame if it shrinks (`len(stored) < wantLen`) or stored raw (`len(stored) == wantLen`), so incompressible blocks never exceed the 8,160-byte page budget and logical offsets remain `no × 8,120` for `O(1)` byte-range seeking. |
| **Admission control** | Protocol requests/batches and background fill/upload classes have bounded concurrency. Disk spools share a process-wide byte budget. These are resource controls, not independent PostgreSQL capacity reservations; size the whole fleet's pools and monitor waits. |
| **No TOAST overhead** | `chunks` sets `toast_tuple_target = 8160` and `STORAGE EXTERNAL`, so PostgreSQL never detoasts or chases out-of-line pointers on read. |
| **Immutable files** | Chunked writes allocate fresh `file_id`s from `s3p.file_ids`; overwrites swap object ownership and queue retired files for reaping. Chunk rows are never updated in place. |
| **HTTP validators** | Ordinary native ETags are content MD5s and multipart ETags are composite values, not `file_id`s. Origin versions are retained separately for revalidation/conditions. Clients can reuse cached slices using the public validator. |
| **Split connection pools** | The main pool keeps `enable_indexscan = on` for fast B-tree lookups on `objects` and `ListObjectsV2`; a dedicated `chunkPool` sets `enable_indexscan = off` so `chunks` range reads use PostgreSQL 18 bitmap heap scans with read-ahead (`effective_io_concurrency = 32`). |
| **Unpartitioned chunks table per bucket** | Each bucket's `chunks` is a single table (capped at 32 TiB / ~31.7 TiB of payload). Partitioning within a bucket by hash or range was slower on point reads and writes; see [partitioning.md](partitioning.md). |
| **Aggressive autovacuum** | Tuned to `autovacuum_vacuum_scale_factor = 0.01` and `analyze_scale_factor = 0.02` so freed pages are recycled quickly after whole-file deletes. |

## Read path

1. **Inline fast path:** metadata and `inline_data` come from one primary-key
   lookup on `objects`, within the namespace's inline limit. `HEAD` queries only
   header columns and never transfers `inline_data`.
2. **Single-query chunk reads:** a single-file span up to 64 MiB runs as one
   `chunks` range query (`file_id = $1 AND no BETWEEN $2 AND $3`).
3. **Snapshot reads (>64 MiB and multipart):** Larger/multipart spans plan roughly
   8 MiB pieces. Two to eight pieces run concurrently on separate connections
   under one exported `REPEATABLE READ` snapshot; larger plans run sequentially
   inside one repeatable-read transaction. Connections and buffering stay bounded.
4. **Synchronous vs cut-through streaming:** Reads up to 8 MiB stream
   synchronously on the HTTP goroutine. Above 8 MiB, a background fetcher feeds
   a bounded 32-buffer channel while `Body.WriteTo` streams 256 KiB buffers to
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
   `s3p.clone_namespace` copy chunk rows inside PostgreSQL without streaming bytes
   through the gateway.
5. **Native batch delete:** `DeleteObjects` locks and deletes up to 1,000
   keys and queues unreferenced files in two SQL statements instead of 1,000
   round trips.

## Origin-backed I/O

- **Read fills:** a whole-object miss uses one origin GET carrying size/version
  metadata. Bodies spool to memory (up to 8 MiB) or scratch disk before opening
  the PostgreSQL body writer. Renewable database leases coalesce cross-gateway
  fills; publication checks the live owner and expected origin version. Waiters
  can fall back to origin pass-through after their bounded wait.
- **Warm and ranged reads:** within TTL, a hit needs no origin request. Warm GET
  reuses its already-read metadata. Cold partial reads of objects larger than
  8 MiB pass through unless aligned range caching is enabled; logged parent
  metadata avoids repeated origin HEADs. Read-only gateways can consume replayed
  metadata/blocks but never fill them.
- **Synchronous mutations:** small/conditional PUT/COPY and deletes require
  origin work before success. Origin mutation and cache publication remain
  separate commits. Foreground mutations and outbox uploads deliberately retain
  a PostgreSQL session key lock across origin I/O; read-fill downloads do not.
- **Write-back:** by default, unconditional PUT/COPY above 8 MiB and unconditional
  multipart completion at any size commit logged `s3w_` bytes plus an outbox row
  before acknowledging. Pending bodies have no expiry. Measure acknowledgement
  latency **and** backlog drain; early acknowledgement is not upload throughput
  or a lossless-promotion promise independent of PostgreSQL policy.
- **Listings:** merge an origin walk with pending objects, retaining one bounded
  page in memory. There is no separate listing index or refresh generation.

Read-cache fills are writes to PostgreSQL: they consume disk, WAL and replica
replay bandwidth even when client traffic is read-only. Rebuildable origin fills
and standalone RESP values share `s3c_`, but only the former are disposable.
See [operations.md](operations.md#cache-tier-in-front-of-another-object-store)
for provider limits, TTLs, tombstones, retries and spool sizing.

## Multi-gateway consistency and connection pooling

- **Stateless gateways:** Gateways hold no authoritative in-process object,
  metadata, namespace, file-ID, or lease state. The namespace catalog in
  PostgreSQL decides what exists. Gateways sharing a primary read the same
  committed rows under MVCC, without a gateway invalidation bus. Each PostgreSQL
  instance has its own `shared_buffers`; standbys share the logged data through
  WAL replay, subject to replication lag, not coherent distributed RAM.
- **One maintenance leader:** a single writable gateway holds the maintenance
  advisory lock; the others skip the tick. If the leader exits, another elects
  itself on a later tick.
- **Conditional cache publishes:** read-through fills are fenced by the
  expected origin version and live fill owner; outbox flushes carry a lease plus
  a token guard. A stale fill or flush is discarded instead of overwriting a newer
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

- **Read grace window:** Deleted or overwritten files remain in their namespace's
  `chunks` table for `--reap-grace` (default 10 minutes), allowing metadata/body
  lookups to bridge a concurrent overwrite. Multi-piece reads additionally hold
  repeatable-read snapshots while streaming.
- **Bounded, chained GC passes:** Every minute, one **writable** gateway
  acquires a database-scoped `pg_try_advisory_lock` and deletes up to 65,536 chunk
  rows (~508 MiB) per pass; a saturated pass immediately chains another. If
  the leader exits, another writable gateway acquires the lock on a later tick.
- **Bounded TTL expiry:** `expire_objects(4096)` repeats until a batch is short or
  the per-tick round budget is reached; expiry is not an unbounded full-table sweep.
- **Space reuse:** Freed pages are reused by subsequent writes once autovacuum
  makes dead-tuple space reusable. Long-running snapshots hold back vacuum
  horizon; avoid multi-hour open transactions on the storage database.

## Tuning DuckDB / DuckLake clients

File layout, projection and client caches can dominate analytics speed. Use the
[executable extraction/configuration recipe](guides/analytics.md) and
`just bench tuning`, not a universal 8,192-row/64 MB preset. Small row groups can
help selective pruning but hurt scan/compression/request overhead; file targets
are not exact physical sizes. Persisted catalog options affect future writes,
and tiny DuckLake inserts can be inlined in the catalog rather than creating
objects. The guide tests both configuration syntax and inlining/flush behavior;
it does not declare that candidate the fastest layout.

DuckDB 2.0 Parquet uses async I/O. Inspect `async_threads`, read-ahead and HTTP
pool capacity as well as execution `threads`, and budget aggregate client
pressure against gateway/cluster limits. Its remote-file and Parquet metadata
caches are client-private, not pgvs3 shared PostgreSQL rows. Disable both when
attributing shared hits; use fresh processes for a genuinely fresh native-client
cache. See [measurement labels](guides/observe-recover.md#measure-without-mislabeling).

## Companion paths

RESP2 pipelines coalesce eligible GET/SET batches into `Cache.MGet`/`Cache.MSet`
against logged `s3c_` values. OJS jobs use database-clock leases and `SKIP LOCKED`
claims without per-queue subtransactions. Streams use snapshot-delimited batches
and consumer-pinned rotation of three event partitions; retry/dead-letter state
has its own lifecycle. See [queue-stream.md](queue-stream.md).

These paths retain their targeted tests and benchmarks, but are not a reason to
reshape the specialized object COPY/streaming loops into a universal data model.
