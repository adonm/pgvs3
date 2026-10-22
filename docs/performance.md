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
| Small objects are one row | `s3p.objects.inline_data`, `file_id NULL`, up to 64 KiB (tunable with `PGVS3_INLINE_MAX`); one-round-trip `s3p.put_inline` |
| Immutable files | A write allocates a new `file_id`; an overwrite publishes it; rows are never updated |
| `file_id` as a cache version tag | ETags are strong; DuckDB's external file cache keys on the URL and version tag |
| Ordered listings are index scans | `key COLLATE "C"` primary key; `SET LOCAL enable_indexscan = on` for one listing |
| Chunk ranges are bitmap heap scans | Session `enable_indexscan = off`; PostgreSQL 18 read-ahead; `effective_io_concurrency = 32` |
| One unpartitioned chunks table | `s3p.chunks`; every read is `file_id = $1 AND no BETWEEN ...` |
| 32 TiB relation cap | `MaxBlockNumber × 8 KB`; about 31.7 TiB of object data |
| Autovacuum at 1% dead rows | Table reloptions, plus `autovacuum_analyze_scale_factor = 0.02` and `threshold = 1000` |

## Read path

- **Up to 64 KiB is one primary-key row.** Inline objects are returned from
  the metadata lookup; there is no chunk query and no `file_id`.
- **Up to 64 MiB is one query for a single-file object.** `wholeReadMax = 64 << 20`;
  the query returns whole rows for a contiguous `no` range and checks the
  object's current `file_id`. One query avoids the per-piece planning and
  connection overhead that a 64 MiB object would otherwise pay.
- **Larger and multipart reads split into 8 MiB pieces.** Up to eight pieces are
  fetched in parallel on separate connections under one exported
  repeatable-read snapshot, so a concurrent overwrite cannot remove later rows.
  More than eight pieces fall back to a sequential snapshot to bound connection
  use.
- **Small single-piece reads are synchronous.** Up to 8 MiB, rows are pulled on
  the HTTP goroutine with no channel hop; the first chunk is prefetched before
  headers are sent.
- **Long reads are cut through to HTTP.** Above 8 MiB, `Body.WriteTo` writes
  each PostgreSQL chunk straight to the response while a bounded channel (32
  chunks) overlaps the database fetch and backpressures a slow client.
- **Out-of-order rows are held.** Bitmap scans return TID order; `streamRows`
  buffers the few rows that run ahead and emits `no` order.
- **256 KiB response chunks** amortise syscalls without adding a large copy.
- **Checksums cover modern SDKs.** CRC64NVME and FULL_OBJECT are accepted on
  single PUTs; multipart uses per-part CRC32/CRC32C/SHA1/SHA256/CRC64NVME
  checksums and a composite ETag.
- **PostgreSQL `shared_buffers` is the shared cache.** Gateways keep no
  in-process metadata or object cache. `s3p.objects` holds both metadata and
  ≤64 KiB inline objects; with `enable_indexscan = on` on the main pool, a
  primary-key `Index Scan` from `shared_buffers` takes ~6 µs inside PostgreSQL
  (0.17–0.47 ms p50 end-to-end over HTTP) and is strictly MVCC-consistent
  across any number of gateways.
- **`HEAD` never transfers `inline_data`.** Because there is no local cache to
  populate, `MetaHeader` projects only the header columns.
- **Concurrent overwrites fail safe.** A chunk range query verifies the current
  object still points at `file_id`; a concurrent replacement between `Meta` and
  `startStream` causes one fresh retry, never stale bytes.
- **The `EXISTS` check is optional.** Every chunk range normally re-checks the
  object row; `PGVS3_VERIFY_READS=false` drops that probe for a single-writer /
  immutable lake.
- **No block read-ahead.** As in pgvfs, DuckDB already merges nearby reads;
  fetching 254 KiB blocks was 30–38% slower cold.
- **No partitioning by default.** See the measured 16/64-partition regression
  above.
- **Split pools for index vs bitmap scans.** The main pool keeps
  `enable_indexscan = on` for `s3p.objects`, `s3p.buckets`, `s3p.uploads` and
  `ListObjectsV2`; a dedicated `chunkPool` sets `enable_indexscan = off` so
  `s3p.chunks` range reads use PostgreSQL 18's bitmap heap scan read-ahead.

## Partitioning

See [partitioning.md](partitioning.md) for the full key-by-key analysis.

`s3p.chunks` is one unpartitioned table by default. Hash-partitioning by
`file_id` was measured at 16 and 64 partitions:

| 4 KiB workload | 1 partition | 16 | 64 |
| --- | ---: | ---: | ---: |
| GET, 1 worker | 1,104/s | 888/s | 945/s |
| GET, 16 workers | 3,852/s | 2,784/s | 3,227/s |
| PUT, 1 worker (v8, chunked) | 218/s | 187/s | 213/s |
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

### Inline threshold

Measured at 8 KiB / 32 KiB / 64 KiB object sizes:

| Object | 64 KiB inline | 8 KiB cutoff (chunked) |
| --- | ---: | ---: |
| 32 KiB PUT, 1 worker | 509/s | 227/s |
| 32 KiB GET, 1 worker | 3,619/s | 1,398/s |
| 64 KiB PUT, 1 worker | 356/s | 200/s |
| 64 KiB GET, 1 worker | 3,250/s | 1,272/s |

Even a TOASTed 64 KiB row beats a chunked object by 2–3× at these sizes, so the
default stays 64 KiB. 8 KiB behaves the same either way.

## Write path

- **Small objects are one SQL call.** `s3p.put_inline` checks the bucket,
  serialises the key, evaluates `If-Match`/`If-None-Match`, queues replaced
  files and upserts the inline row in one round trip. Removing `pg_notify`
  and keeping `enable_indexscan = on` on the main pool reaches 17.5k durable
  4 KiB PUT/s at 64 workers on 16 cores (3.7k/s on 6 vCPU,
  `synchronous_commit = on`).
- **One binary `COPY` per larger PUT or part.** `internal/store/ingest.go` frames
  4 MiB batches; the body is hashed (SHA-256 for integrity, MD5 for the ETag) in
  the same pass.
- **Publish in one SQL call.** After `COPY`, `s3p.publish_object` takes the
  advisory lock, evaluates conditions, swaps the object row and queues replaced
  files. Chunks and metadata commit together; an interrupted
  write publishes nothing.
- **The COPY framer reuses 4 MiB buffers.** A `sync.Pool` removes per-flush
  allocations under concurrent large writes; `commit_part` replaces an old
  part and queues it in one SQL call.
- **Batch deletes take one query.** `DeleteMany` locks all keys, deletes the
  rows and queues only unreferenced shared files in two statements instead of
  1,000 round trips. The reaper self-heals false-positive garbage rows left by
  older gateways.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging; Complete publishes the ordered part list. It validates ETags and
  checksums but moves no data.
- **CopyObject is metadata-only.** The destination row shares the source's
  immutable file IDs. The reaper only queues a file when no object or upload
  part references it.
- **No sharded COPY.** One stream already writes about 300 MiB/s and more than
  four streams were slower on an 8-core PostgreSQL. Publishing shards early
  would also leak rows from a crashed writer.
- **No `wal_compression`.** It only compresses full-page images, and the pages
  hold already-compressed Parquet.
- **No `full_page_writes = off`.** That risks the whole cluster.

## Multi-gateway consistency

- **Zero gateway state.** Gateways hold no metadata, object or bucket cache.
  Every read queries PostgreSQL directly, so a write committed through any
  gateway is immediately visible on every other gateway with no `LISTEN/NOTIFY`
  channel and no TTL staleness window.
- **Scale-out via stateless replicas.** Gateways can run as an ordinary
  Kubernetes `Deployment` behind a load balancer or service mesh. For cross-AZ
  read locality, run `--read-only` gateways against local-AZ PostgreSQL
  streaming standbys.

## Commit modes

`PGVS3_WRITE_COMMIT` defaults to `on`. `local`/`off` (or `remote_write`/
`remote_apply`) let unconditional writes return before the local WAL fsyncs;
this trades crash durability for write throughput. Conditional writes always
force `on`, so SlateDB manifest/WAL fencing and `If-Match`/`If-None-Match`
remain durable.

## Connections

- **Most-recently-used first.** pgxpool/puddle reuses the hottest idle
  connection, avoiding TCP slow-start after an idle RTO.
- **Warm minimum.** `PGVS3_POOL_MIN` connections are opened at startup; opening
  one (TCP, TLS, SCRAM, session `SET`s) costs about 10 ms.
- **No ping on checkout.** A dead connection fails its query and is discarded.
- **Session settings** are applied once per connection: `work_mem = 64MB`,
  read-ahead concurrency, TCP keepalives, a 5-minute
  `idle_in_transaction_session_timeout`, and synchronous commit.
- **Main pool vs chunk pool.** The main pool keeps `enable_indexscan = on` so
  `s3p.objects` point lookups, writes and `ListObjectsV2` use B-tree index
  scans; `chunkPool` sets `enable_indexscan = off` so `s3p.chunks` range reads
  use PostgreSQL 18 bitmap heap scans.

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
