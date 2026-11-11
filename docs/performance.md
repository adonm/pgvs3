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

## Speed of light

`just sol` measures one client stream through the gateway against the floors
that bound it on the same host. The HTTP floor is a loopback server that only
moves bytes. The PostgreSQL floor is a direct `COPY` (write) or 8-connection
`SELECT` (read) of the identical 8,120-byte rows in a table with the same
storage settings, using the gateway's chunk query and chunk-pool planner
settings, assembled into 256 KiB chunks (the minimum any implementation must
do). A perfectly pipelined gateway would approach `min(HTTP, PostgreSQL)`.
Payloads are incompressible; caches are warm; the gateway-local read cache is
off; each cell is the median of five interleaved trials.

| Size | Op | pgvs3 MiB/s | HTTP floor | PG floor (1 conn) | % of floor | gateway CPU ns/B |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 10 MiB | PUT | 188 | 3,455 | 330 | 57% | 2.97 |
| 10 MiB | GET | 2,168 | 2,444 | 4,986 (1,009) | 89% | 0.56 |
| 10 MiB | 8 MiB range | 1,982 | 3,717 | 5,121 (1,187) | 53% | 0.60 |
| 256 MiB | PUT | 384 | 3,555 | 461 | 83% | 2.31 |
| 256 MiB | GET | 3,311 | 4,011 | 5,767 (1,227) | 83% | 0.53 |
| 256 MiB | 8 MiB range | 1,476 | 3,145 | 2,556 (873) | 58% | 0.76 |
| 1 GiB | PUT | 309 | 4,063 | 406 | 76% | 2.98 |
| 1 GiB | GET | 2,077 | 3,086 | 2,468 (811) | 84% | 0.74 |
| 1 GiB | 8 MiB range | 984 | 2,978 | 1,777 (755) | 55% | 0.94 |

Intel Core Ultra X7 358H laptop (16 hybrid cores, no SMT), NVMe, PostgreSQL 18
in Docker with `shared_buffers=1GB`, `max_wal_size=4GB`, gateway default pools.
Report: `.tmp/verify/sol.LTDw0T/sol.json`, SHA-256
`61fc82be48534a0d463e047ed8bdb3e0e78f922e59cb1687fd283706b4820534`. Single runs
on this host move by about ±15%; the loopback HTTP floor alone ranged from 2.4
to 3.2 GiB/s for 10 MiB between runs. The 1 GiB rows exceed `shared_buffers`, so
both pgvs3 and the floor read through the operating-system page cache there.

Until 2026-11-02 the PostgreSQL read floor used `ORDER BY no` on an un-analyzed
table. That planned an external merge sort spilling to disk and understated the
floor about 2.5× (one connection ~0.45 GiB/s instead of ~0.8–1.3; eight ~2.5
instead of 3.4–5.8). Earlier "% of floor" figures, including those above 100%,
compare against that understated floor.

What it says:

- **Writes have a remaining gap to the PostgreSQL floor.** PUT is 57–83% of a
  raw PostgreSQL `COPY` here (77–96% in the earlier desktop run). The gap alone
  does not show whether gateway, WAL or disk accounts for it; profile those
  before attributing it. It does not prove the gateway has no headroom.
- **Reads sit below the loopback HTTP floor in this run.** Eight connections
  deliver 2.5–5.8 GiB/s and one ~0.8–1.2 GiB/s; whole-object GET reaches
  2.1–3.3 GiB/s (83–89% of that floor). The floor is a measured local
  reference, not a proven universal bottleneck.
- **Ranges pay fixed setup.** An 8 MiB range reaches 1.0–2.0 GiB/s (53–58%).
  Metadata, the read guard and each worker's first rows cost about 1–1.5 ms
  before the first byte on this host, a quarter to a third of the request.
- **Under concurrency the ceiling is CPU per byte.** With 8 clients, uncached
  reads plateau near 4.3 GiB/s here with either read guard. The gateway spends
  ~0.8–0.9 ns/byte, mostly kernel socket reads from PostgreSQL and writes to
  clients. Only the [gateway read cache](#gateway-read-cache) changes that.

Run `just sol` (defaults `-sizes 10MiB,256MiB,1GiB -repeat 5 -range 8MiB
-readers 8`) on your hardware before quoting any of these numbers.

### Read-path changes, paired

The [paired gate](benchmarks.md#paired-acceptance) against the previous binary
(snapshot-per-piece window, unbuffered chunk connections) passes every case:
native 10 MiB GET 11.7% faster at one worker and 15.8% at four; 100 MiB GET
16.1% and 11.4% faster; HEAD, small, range and PUT cases within ±13%; the cold
origin profile within ±9%. Reports: `.tmp/verify/load.n64lvy` and
`.tmp/verify/load.F7vjJz`, both `complete: true` with no failed gates.

A direct single-client loop on the same host (median of three interleaved
rounds, MiB/s):

| Read | Previous | Current | Current, warm read cache |
| --- | ---: | ---: | ---: |
| 10 MiB GET | 1,986 | 2,132 | 4,900 |
| 8 MiB range of 10 MiB | 1,603 | 1,917 | 4,163 |
| 64 MiB GET | 1,844 | 2,266 | 5,058 |
| 8 MiB range of 256 MiB | 1,652 | 1,644 | 4,829 |
| 256 MiB GET | 1,868 | 2,612 | 5,508 |

With 8 concurrent clients the previous and current paths are equal within noise
(10 MiB GET ~4.4 GiB/s, 64 MiB ~4.3 GiB/s, 1 MiB ranges 2.7 → 3.0 GiB/s); a warm
read cache serves 15.7, 15.1 and 9.5 GiB/s respectively at ~0.3 ns of gateway
CPU per byte and no PostgreSQL work.

![FIG 03 — 8,192-Byte PostgreSQL Page, 8,120-Byte Chunks & Cut-Through Streaming I/O](assets/page-layout-io.svg)

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
| **Split connection pools** | The main pool keeps `enable_indexscan = on` for fast B-tree lookups on `objects` and `ListObjectsV2`; a dedicated `chunkPool` sets `enable_indexscan = off` so `chunks` range reads use PostgreSQL 18 bitmap heap scans with read-ahead (`effective_io_concurrency = 32`), and reads its sockets through a 256 KiB buffer. |
| **Unpartitioned chunks table per bucket** | Each bucket's `chunks` is a single table (capped at 32 TiB / ~31.7 TiB of payload). Partitioning within a bucket by hash or range was slower on point reads and writes; see [partitioning.md](partitioning.md). |
| **Aggressive autovacuum** | Tuned to `autovacuum_vacuum_scale_factor = 0.01` and `analyze_scale_factor = 0.02` so freed pages are recycled quickly after whole-file deletes. |

## Read path

1. **Inline fast path:** metadata and `inline_data` come from one primary-key
   lookup on `objects`, within the namespace's inline limit. `HEAD` queries only
   header columns and never transfers `inline_data`.
2. **Small single-query reads:** spans up to 1 MiB, or any plan that gets one
   reader, run as one `chunks` range query
   (`file_id = $1 AND no BETWEEN $2 AND $3`), consistent within its statement.
3. **Ordered pull window:** larger spans plan balanced pieces (1 MiB minimum,
   ~8 MiB for large objects). A single `lookupPieces` pass classifies every
   piece against the block cache; cached pieces are emitted without a query and
   only misses are scheduled. Up to eight workers pull missing pieces in order
   from a shared counter and stream each into its own two-buffer channel; the
   consumer drains them in piece order through a window bounded by
   `min(max(2*workers, 4), 16)` results, so output stays contiguous and memory
   is bounded. The window reserves half the chunk pool for other work and
   shrinks as reads overlap.
4. **Read pins, with a snapshot fallback:** before workers start, one guard
   connection takes shared advisory locks (`pg_try_advisory_lock_shared`) on
   every file in the span. The reaper takes the exclusive lock before deleting a
   file and skips pinned ones, so workers run plain autocommit queries and a pin
   holds no MVCC snapshot back from vacuum. On a standby (`pg_is_in_recovery()`),
   for spans over 64 files, or when a pin finds the reaper deleting that file,
   the read exports one `REPEATABLE READ` snapshot that each worker imports once.
   The exporting transaction ends when every worker has imported it, and worker
   transactions lift `idle_in_transaction_session_timeout` locally, so a slow
   client cannot get a stream cut off mid-object.
5. **Synchronous vs cut-through streaming:** Reads up to 8 MiB stream
   synchronously on the HTTP goroutine. Above 8 MiB, a background fetcher feeds
   a `min(max(2*workers, 4), 16)` result window while `Body.WriteTo` streams
   256 KiB buffers to the socket, overlapping database I/O with network writes
   and backpressuring slow clients. Rows assembled into newly cached blocks are
   returned as shared cache bytes, so a cold fill does not copy them again.
6. **Buffered chunk connections:** chunk-pool connections read the socket
   through a 256 KiB buffer under pgx's 8 KiB frame reader, so one read system
   call carries ~32 rows instead of one. This is what lifted large GETs.
7. **TID-to-row-number reordering:** Bitmap heap scans return pages in physical
   TID order; `streamRows` buffers any out-of-order rows in memory and emits
   strict `no` order.
8. **Gateway read cache (opt-in):** see [below](#gateway-read-cache).
9. **Warm cache hit:** the cache tier reuses the metadata row it already read
   for the TTL/lifecycle decision when opening the body, so a warm cache GET
   does not repeat the primary-key lookup. Inline cache objects are served
   directly from that row; larger ones start the normal chunk stream.

## Gateway read cache

`--read-cache-bytes` (default `0`, off) gives each gateway a bounded in-memory
cache of decoded chunk blocks (32 rows, 253.75 KiB). It changes where hot bytes
come from, not who owns them:

- **No authority.** Every request still reads metadata from PostgreSQL first;
  only bytes of the file that metadata currently references are served. Keys are
  the namespace `objects` table OID plus `file_id` and block number. File IDs are
  never reused within a table and the OID changes on drop/restore, so a key
  never names different content and nothing needs invalidating.
- **Memory hits and shared fills.** A fully cached span needs no chunk
  connection, pin or snapshot; partly cached spans serve cached pieces in
  order. Local misses read PostgreSQL rows, and batched `ReadMany` may cache
  whole aligned range-chunk files as well as 32-row blocks. A cold fill publishes
  each completed block and hands that cache-owned buffer to the response
  instead of copying it into a separate output buffer. Only missing shared
  origin range chunks fetch the origin once, publish each chunk to PostgreSQL
  synchronously with bounded store concurrency, then assemble the response.
  Store failures are logged and the response still uses the fetched bytes.
- **Scan resistant.** Each shard runs S3-FIFO: new blocks enter a small queue
  and only blocks read again move to the main queue, so one large scan cannot
  flush hot footers and row groups.
- **Per gateway.** Replicas do not share it; PostgreSQL remains the shared cache
  across gateways and standbys. Count the budget inside `GOMEMLIMIT`.

The budget bounds retained cache residency, not process RSS, scratch spool,
request/response buffers or other in-flight work. A request can span many
chunks, so keep headroom for concurrent fills and response assembly.

Measured above: warm whole-object reads 2.3–2.6× faster for one client and about
3.5× higher aggregate for eight, with gateway CPU per byte falling from ~0.9 to
~0.3 ns. It helps repeated reads of the same objects (Parquet footers, popular
row groups, shared lake files) and does nothing for one-pass scans. Watch
`pgvs3_read_cache_hit_bytes_total` against total read volume before sizing it up.

## Archive virtual folders

An archive member is a normal `GET`/`HEAD` target that resolves through the
cached central-directory index, not a separate data plane. Two mechanisms keep
it at parity with standalone objects:

- **`Store` cut-through:** a stored member maps to one exact byte range of the
  archive, so the read is a single range GET with no inflate step and no
  whole-archive download. The index supplies the member offset; the chunk path
  is unchanged.
- **`MetaPair`, one round trip:** `WHERE key IN ($1, $2)` tests the literal key
  and the archive-member key in one statement, so resolving `<archive>/<member>`
  costs the same `s.pool` acquire a plain object pays. A second lookup for the
  archive index is not needed on the request path.

That is why archive members reach 1-RTT parity: at `c = 64` on the 16-core host,
parallel 4 KiB retrieval measured **35.8k req/s** for `Store` members, **37.3k
req/s** for Zstd members and **40.5k `HEAD`/s**, all at **1.000** `s.pool`
acquire per request, against **37.1k req/s** for standalone 4 KiB inline
objects. Cached streaming scales the same way: a 64 MiB `Store` member served
**12.86 GiB/s** at 8 workers with a 256 MiB cache (**1.01×** the same bytes as a
direct object) and **3.34 GiB/s** uncached (**0.90×**). A 10,000-member
`delimiter=/` listing costs **1.11 ms** warm because pagination binary-searches
past common prefixes. Full tables, fixtures and caveats are in the
[archive virtual-folder benchmarks](benchmarks.md#archive-virtual-folders-zip--zip64).

Compressed members add gateway CPU rather than PostgreSQL work: a Zstd member
decompresses **1.8–2.0×** faster than SIMD Deflate, and ≤16 MiB hot members are
served from the bounded 64 MiB decompressed cache.

## Write path

A streaming writer acquires one slot from a per-gateway gate of
`max(MaxConns/2, 1)` before opening its `ChunkWriter` transaction, so metadata
operations and reads always keep part of the main pool free.

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
  reuses its already-read metadata. With `--range-cache-chunk`, cold partial
  reads of objects larger than the chunk size cache aligned chunks for requests
  up to 64 MiB; without it they pass through. Logged parent metadata avoids
  repeated origin HEADs and coalesces concurrent cold HEADs per key. Read-only
  gateways can consume replayed metadata/blocks but never fill them.
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
  WAL replay, subject to replication lag, not coherent distributed RAM. The
  optional [read cache](#gateway-read-cache) holds only immutable chunk bytes
  and is never consulted without fresh metadata, so it needs no invalidation.
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

## ChargeWire client-to-worker pool capacity model

![FIG 07 — ChargeWire Client-to-Worker Pool, Gateway Admission and PostgreSQL Capacity Model](assets/chargewire-worker-pool.svg)

A ChargeWire pool has four independent limits. Size each tier from `W`, `C`, the
gateway count `G` and the PostgreSQL connection envelope; a fast worker does not
create gateway or database capacity.

**Tier 1 — `C` clients to `W` workers.** One ADBC connection carries one active
statement and holds one server session. A worker accepts
`--max-sessions` sessions (`0` is unlimited), so the session bound is

```
C <= W × Q
```

Route stateless lake reads round-robin (`client i -> worker i mod W`) or by load.
Keep a client sticky when it uses temp tables or native `memory.main` objects
(RTree/HNSW/FTS indexes and their tables): those belong to one worker's embedded
DuckDB instance and are invisible on another worker. `lake` tables are shared —
every worker attaches the same DuckLake catalog — so they need no stickiness.
The measured run used `C <= 32` and `W <= 4`.

**Tier 2 — `W` workers to `G` gateways.** Each `pgvs3-duckdb:2.0.0` container runs
one `hardcharger-server` (PID 1, ChargeWire on `:31337`, plaintext `/health` on
`:31338`). All sessions share `T = 4` execution threads, the async `httpfs` pool
admits `A = min(4 × CPU, 256)` requests, and `HARDCHARGER_MEMORY_LIMIT` bounds
DuckDB's buffer manager. Native RTree/HNSW indexes are per-worker, in RAM, not
covered by the memory limit, and are snapshots that must be rebuilt when lake
freshness matters. Here a 5,000-point native RTree sustained **1,026 q/s at
`W=4, C=32`**, while the lake scan stayed near **200–242 q/s** because one
gateway and one PostgreSQL catalog bound it, not the worker count.

`hardcharger-server` drains on the first `SIGTERM` and exits `0` inside
`HARDCHARGER_SHUTDOWN_GRACE_PERIOD_SECONDS`, so it needs no stdin keepalive and
no `--init`; a rolling update loses no in-flight statement inside that window.

**Tier 3 — `G` gateways to PostgreSQL `chunkPool`.** Each gateway's S3 guard
admits **32 active + 64 queued = 96** requests, so

```
G >= ceil(R / 32)      zero queue
G >= ceil(R / 96)      burst tolerance
```

A multi-piece read reserves one guard connection and shares the rest of the
chunk pool across `w` workers per read:

```
w = clamp(floor((B / 2 - R) / R), 1, 8)
B = min(P, 64)
```

`B = 64` scales one read from nine chunk connections at `R=1` to two at `R=32`:

| Concurrent reads `R` | Available `B/2 - R` | Workers `w` | `1 + w` connections |
| ---: | ---: | ---: | ---: |
| 1 | 31 | 8 | 9 |
| 2 | 30 | 8 | 9 |
| 4 | 28 | 7 | 8 |
| 8 | 24 | 3 | 4 |
| 16 | 16 | 1 | 2 |
| 32 | 0 | 1 | 2 |

`--read-cache-bytes` adds a bounded, per-gateway zero-copy block cache. A hit is
served from gateway memory and bypasses the chunk pool and PostgreSQL entirely;
metadata still comes from PostgreSQL on every request. In the measured run the
256 MiB cache held the projected lake columns: timed cells recorded **7–8**
chunk-pool connections versus **up to 32** with the cache off, and lifted
`C=32` lake throughput from **199–201** to **236–242 q/s**.

The same effect holds for a real public S3 origin: `just overture` measured a
Perth CBD place + building query through the range cache at **79 ms** warm from
shared PostgreSQL `s3c_` and **57 ms** with a warm 256 MiB gateway block cache,
against **3.62 s** direct to `us-west-2`. Timed block-cache runs recorded **0**
`chunkPool` acquires, and the 1 MiB range cache held **22.1 MiB (1.8%)** of the
**1.23 GiB** source Parquet. See
[benchmarks](benchmarks.md#anonymous-overture-maps--stac-just-overture).

**Tier 4 — PostgreSQL 18 and the S3 origin.** DuckLake catalog connections go
directly from each worker to PostgreSQL and bypass pgvs3. Add them to the gateway
envelope:

```
max_connections > (G + surge) × (P + min(P, 64)) + Σ a_w + other + reserved
```

`a_w` is worker `w`'s active DuckLake catalog pool; `P` is `--pool-max`;
`G + surge` includes a rolling-update gateway. A starting point is
`pg_pool_max_connections = clamp(1.5 × CPU, 4, 32)` per gateway; the measured run
deliberately used `P = 32`. This run observed `Σ a_w` of at most **24 / 40 / 56**
for `W = 1 / 2 / 4` and at most **62** pgvs3 connections. For `G=1`, `surge=1`,
`P=32` the gateway term is `2 × (32 + 32) = 128`; 128 + 56 = 184 leaves 72 of
the 256-connection server for other clients and reserved slots. Budget it
explicitly with `pgvs3 doctor --compute-workers $W`, which reserves **24**
DuckLake catalog connections per worker, instead of assuming `max_connections`
covers only gateway pools. On the S3 side, `s3b_`/`s3c_`/`s3w_` store
8,120-byte chunks and origin fills obey `--fill-concurrency F`.

Measured `C × W` aggregate rate on the 16-core host (`q/s`; 20 repeats per
client after one warm-up, one gateway at `P=32`):

| `C` | Lake, cache 0: `W=1` | `W=2` | `W=4` | Lake, 256 MiB cache: `W=1` | `W=2` | `W=4` | Native RTree: `W=1` | `W=2` | `W=4` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 61.3 | 60.9 | 60.0 | 65.6 | 65.3 | 65.5 | 127.8 | 123.7 | 129.2 |
| 4 | 153.1 | 155.1 | 142.6 | 171.4 | 173.5 | 164.8 | 413.8 | 432.7 | 433.8 |
| 8 | 181.0 | 178.3 | 172.1 | 207.7 | 202.9 | 202.8 | 666.6 | 703.5 | 702.7 |
| 16 | 197.8 | 197.9 | 193.3 | 231.7 | 234.0 | 226.2 | 795.0 | 891.7 | 861.3 |
| 32 | 199.1 | 192.0 | 200.9 | 236.4 | 241.9 | 242.3 | 956.6 | 976.0 | 1,026.5 |

Full p50/p95/p99 and PostgreSQL connection tables are in
[benchmarks](benchmarks.md#c-clients-x-w-worker-pool-benchmark). Report:
`.tmp/verify/pool.UzzpKf/chargewire_pool.json`, SHA-256
`46dc5c065f74b660466b8c36bfcb3db93005b21ac4297ad925a87b38b47315ff`.
This is one host and one session; cells are not a monotonic scaling claim.

## Reaping and vacuum

- **Read grace window:** Deleted or overwritten files remain in their namespace's
  `chunks` table for `--reap-grace` (default 10 minutes), allowing metadata/body
  lookups to bridge a concurrent overwrite. Multi-piece reads additionally pin
  their files (or hold an imported snapshot on a standby) until they finish, so
  a read that outlives the grace window still completes. The reaper skips pinned
  files and counts them in `pgvs3_reap_pinned_skips_total`.
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
