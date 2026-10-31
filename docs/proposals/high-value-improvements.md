# High-value improvements and simplifications

> Historical analysis, not the active backlog. Priorities and status labels below
> describe the reviewed snapshots and may be superseded. Use the
> [current architecture](../design.md), [ranked roadmap](../roadmap.md) and
> [implementation evidence](../modernization-results.md).

Status: research note, 2026-10-25, at commit `1a7e94b`. It records what exists,
how it compares with current practice, and which changes look worth making.
Findings are ranked by risk and effort, not by novelty.

Source paths and line numbers below refer to the reviewed snapshot. Current
ownership after consolidation is listed in
[CONTRIBUTING.md](https://github.com/adonm/pgvs3/blob/main/CONTRIBUTING.md#package-map).

Implementation note (2026-10-26): the OJS queue fixes recommended here are
implemented in layout v14 and carried into v18: fetch uses one lazy claim over
available, due and expired-lease rows; expiry/due promotion/exhausted leases run
in maintenance; lease validation uses the database clock; terminal jobs are
removed after `--job-retention`. Layout v18 adds the consolidated PgQue-shaped
stream side to the same `s3q_` namespace (snapshot batches, fan-out
subscriptions, ack/nack, retry, DLQ, queue/consumer info and three-segment
`TRUNCATE` rotation). The three urgent user-visible findings below (`/metrics`,
connection lifetime and read-only origin errors) and the OJS fetch-maintenance
gap and the origin listing-index removal are fixed in the current tree. The
other P1/P0 findings remain open.

## Executive summary

pgvs3 is in good shape: `go test ./...`, `go test -race ./...`, `go vet` and
`staticcheck` are clean, and the repository has 205 unit/integration test
functions plus four fuzzers. The core design is coherent and several choices
match or lead current practice (schema-per-namespace, immutable chunks, strong
ETags, `SKIP LOCKED` queues, conditional origin writes).

Three findings were urgent because they were small and user-visible; all are
now fixed:

1. **`/metrics` panicked after any normal request.** The metrics map used string
   keys but asserted `requestKey` on scrape.
2. **`--max-conns` only capped connections during protocol sniffing.** The slot
   is now held until the wrapped connection closes.
3. **Writes to a read-only HTTP origin returned `500 InternalError`.** S3 now
   maps the capability error to `501 NotImplemented`.

The origin listing-index simplification is now implemented. Listings walk the
origin directly with one bounded page in memory, so `origin_listing`,
`origin_listing_state`, `origin_generations`, the refresh generation logic and
per-write listing upserts are gone. Listings are exact rather than TTL-stale;
the tradeoff is an O(N) origin walk for deep `StartAfter` pages until adapters
gain a native start-after cursor.

The biggest correctness gap is a cluster of **backend-selection and error
semantics** issues: missing buckets return `NoSuchKey`/empty listings instead of
`NoSuchBucket`, mixed local/origin `CopyObject`, `UploadPartCopy` and bucket
clone silently use the wrong backend, and clone is not snapshot-isolated.

The biggest performance gap at review time was **OJS fetch maintenance**; the
lazy-claim change moved expiry, lease-expiry and due promotion into maintenance.
The process-wide DB session budget is now bounded by conservative defaults.
Cache fills and pending-upload workers use process-wide semaphores, so
`--fill-concurrency` is no longer multiplied by the number of origin tiers.

## What is implemented

- One stateless Go gateway, one listener, five protocols: S3, plain HTTP, OJS,
  RESP2, and the origin cache tier.
- One PostgreSQL schema per namespace (`s3b_`, `s3c_`, `s3q_`), with typed
  catalog rows, per-namespace functions, instant `DROP SCHEMA CASCADE`, and
  per-namespace autovacuum settings.
- Immutable `file_id` objects, inline objects up to 64 KiB, 8,120-byte chunk
  rows mapped to one 8 KiB page, length-discriminated Zstd, binary `COPY`
  ingest, atomic publish, bounded GC, MVCC reads.
- Cache tier with read-through fills, write-through PUT/COPY/DELETE,
  multipart pending/outbox, version-fenced fills, tombstones, range chunks, and
  direct bounded origin listings.
- OJS core lifecycle with typed state transitions, leases, retries, delayed
  delivery, and a short long poll.
- RESP2 with pipelined GET/SET coalescing.
- Strong ETags, byte ranges, conditional reads, conditional writes, S3
  checksums, multipart, zero-copy copy, and s3t/contract/interop/fault gates.

Current package sizes (non-test / test lines):

| Package | Non-test | Test |
| --- | ---: | ---: |
| `internal/store` | 6,752 | 2,770 |
| `internal/s3` | 3,846 | 1,718 |
| `internal/objcache` | 2,843 | 3,074 |
| `internal/ojs` | 1,341 | 1,464 |
| `internal/resp` | 1,075 | 121 |
| `cmd/pgvs3` | 632 | 179 |
| `internal/gateway` | 269 | 0 |

## How it compares with current practice

| Problem | pgvs3 | Current comparable practice | Verdict |
| --- | --- | --- | --- |
| Blob bytes in PostgreSQL | 8,120-byte chunk rows, one per page, no TOAST; inline ≤64 KiB | App-level chunk table or `bytea STORAGE EXTERNAL`; JuiceFS uses 64 MiB chunks / 4 MiB blocks; large objects are slower for streaming | The page-aligned design is deliberate and benchmarked. Worth one controlled experiment with 64–256 KiB `EXTERNAL` rows, not a rewrite. |
| Metadata store | PostgreSQL catalog and per-namespace schemas | Supabase Storage: PG metadata, bytes in S3; SeaweedFS filer on PG: one table per bucket, dropped on bucket delete | Schema-per-bucket is validated by SeaweedFS; pgvs3 goes further by keeping bytes in PG. |
| Immutable files | `file_id` sequence, atomic pointer swap, GC grace | RisingWave/JuiceFS: immutable blobs and PG metadata; GC by reference/grace | Aligned. |
| Queue | `SKIP LOCKED`, leases, typed transitions, lazy maintenance outside fetch | River, pgmq, pg-boss, Oban: `SKIP LOCKED`, transactional enqueue, background/lazy maintenance, retention/partitioning | Claim pattern and maintenance placement now align; terminal retention is bounded. |
| Cache tier | Read/write-through, full-object cache, range chunks, outbox, direct origin listing | Rclone serve s3: stream multipart through; AWS hybrid cache sample: range cache with streaming and bounded memory; most caches invalidate on write | The outbox is durable but heavy; direct listing removed the clearest overbuild. |
| Conditional writes | `If-Match`/`If-None-Match` at the origin, multipart conditions on complete | S3 gained `If-None-Match` (2024) and conditional copy (2025) | Ahead of many S3-compatible gateways. |
| Cold-read I/O | PG18 bitmap heap scans, `effective_io_concurrency=32` | PG18 AIO: `io_method=worker`/`io_uring`, `io_workers`, `io_combine_limit`; 10–25% gains on bitmap/seq scans are reported | Add operator guidance and benchmark with the real deployment. |
| Verification | Unit + integration + s3t allow-list + contract/interop/fault; gateway and metrics tests added | River/pgmq have extensive DB-level queue tests; S3 gateways use s3-tests and SDK matrices | Strong breadth; keep adding concurrency/load coverage. |

References:

- Supabase Storage: <https://github.com/supabase/storage>
- SeaweedFS filer `postgres2` one table per bucket:
  <https://github.com/gntik-ai/falcone/blob/main/docs-site/architecture/seaweedfs.md>
- JuiceFS chunk/block design:
  <https://juicefs.com/en/blog/engineering/design-metadata-data-storage>
- RisingWave S3-primary storage:
  <https://risingwave.com/blog/how-we-built-risingwave-on-s3-a-deep-dive-into-s3-as-primary-storage-architecture/>
- River: <https://github.com/riverqueue/river/>; pgmq/PGQue queue
  benchmarking: <https://github.com/hardbyte/postgresql-job-queue-benchmarking>
- AWS S3 conditional writes:
  <https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/>
  and conditional copy:
  <https://aws.amazon.com/about-aws/whats-new/2025/10/amazon-s3-conditional-write-functionality-copy-operations/>
- PostgreSQL 18 AIO: <https://www.postgresql.org/about/press/presskit18/en/>
- PostgreSQL binary-data guidance:
  <https://www.cybertec-postgresql.com/en/binary-data-performance-in-postgresql/>
  and the chunk-table recommendation:
  <https://www.postgresql.org/message-id/CAFCRh--KMuUHPvSb5KL6TJoJmykwQTgh3mAfP4B3KxHMULXMTw%40mail.gmail.com>
- Rclone S3 multipart streaming:
  <https://rclone.org/commands/rclone_serve_s3/>
- AWS S3 hybrid cache sample:
  <https://github.com/aws-samples/sample-s3-hybrid-cache/blob/main/docs/ARCHITECTURE.md>

## Urgent findings

### 1. `/metrics` panics after any normal request (P0, fixed)

`internal/s3/metrics.go:31-37` builds a string key:

```go
key := fmt.Sprintf("%s\x00%d", method, code)
v, ok := m.requests.Load(key)
...
m.requests.LoadOrStore(key, new(atomic.Uint64))
```

but `metrics.write` ranges with `k.(requestKey)` at line 57. The first normal
request stores a string key; the next `/metrics` request panics on the type
assertion. The endpoint is therefore unusable in a running gateway.

Fix: use `requestKey{method: method, code: code}` as the map key, and add a
`metrics_test.go` that observes one request and writes the exposition. This also
removes the per-request `Sprintf` allocation.

### 2. `--max-conns` only limits connections during protocol sniffing (P0, fixed)

`internal/gateway/gateway.go:144-179`: the semaphore is acquired in
`acceptLoop` and released by the deferred `<-m.sem` as soon as the connection is
classified and handed to `httpCh`/`respCh`. The HTTP and RESP servers then own
the connection with no further cap. A client can classify any number of
connections and keep them open; `--max-conns` does not bound active sockets or
server goroutines as documented.

Fix: hold the slot for the connection lifetime. Wrap `peekConn` with a
`Close` that releases the semaphore exactly once, remove the deferred release,
and drain/close pending channel connections in `muxListener.Close`. Add a test
with a fake listener and two connections to prove the second is shed while the
first is live.

### 3. Read-only origins fail writes with 500, not an explicit error (P0/P1, fixed)

`objcache.origin.Upload/Delete/Copy` correctly return
`ErrOriginUnsupported` when the origin lacks `capWrite`
(`internal/objcache/origin.go:233-263`), and `origin_caps_test.go` verifies it.
But `s3.Server.ingestError` (`internal/s3/server.go:724-750`) does not map
`objcache.ErrOriginUnsupported`, so a PUT, DELETE, CopyObject or multipart
completion against a read-only HTTP origin becomes
`500 InternalError`. The operations guide promises an explicit error
(`docs/operations.md:458-479`), and a read-only HTTP origin is a documented
production mode.

Fix:

- In `objcache.Tier`, check `t.origin.Supports(capWrite)` at the top of
  `UploadWithOptions`, `Delete`, `DeleteMany`, `Copy`, `CreateUpload`,
  `StartPart` and `CompleteUpload`, returning `ErrOriginUnsupported` before
  staging any bytes.
- Map `ErrOriginUnsupported` and `ErrOriginListingUnsupported` to
  `501 NotImplemented` (or `403 AccessDenied` for writes) in the S3 layer.
- Map the same errors per key in `DeleteObjects`.
- Add an S3 handler test using a read-only HTTP origin.

## Correctness findings

### 4. Missing buckets return the wrong S3 errors (P1)

The catalog is authoritative, but reads do not distinguish a missing bucket
from a missing key:

- `store.metaRef` (`internal/store/object.go:103`) maps a missing schema to
  `ErrNotFound`, so `GET`/`HEAD /missing/key` returns `NoSuchKey`.
- `store.List` (`internal/store/object.go:849-866`) returns `nil, nil` for a
  missing schema, so `ListObjectsV2 /missing` returns `200` with zero keys.
- `store.DeleteMany` (`internal/store/object.go:825`) returns `0, nil`, so
  `DELETE /missing/key` returns `204` instead of `NoSuchBucket`.
- `readCopySource` maps a missing source schema to `ErrNotFound`, so a copy
  from a missing bucket returns `NoSuchKey`.

Fix: when the SQLSTATE says the schema/relation is missing and the namespace
kind is `bucket`, return `store.ErrNoBucket`; keep empty/not-found behavior for
cache namespaces. Add `ErrNoBucket` cases to `metaError`, `ingestError`,
`listObjectsV2`, `listMultipartUploads`, `listParts`, `copyObject`,
`uploadPartCopy`, `deleteObject` and `deleteObjects`. This is mostly mechanical
and improves conformance without adding a hot-path existence query.

### 5. Mixed local/origin copy uses the wrong backend (P1)

Per-bucket origins are supported (`pgvs3 origin --bucket`). With a local bucket
and an origin-backed bucket, copy operations silently use one side only:

- `s3.copyObject` selects `writeBackendFor(destination)` and calls `Copy`
  (`internal/s3/copy.go:56-61`). `Tier.Copy` reads the source through the
  destination tier and cache namespace
  (`internal/objcache/objcache.go:809-834`); a local source is never read.
- `tryFastPartCopy` checks the source through the local store whenever the
  destination is local (`internal/s3/copy.go:143-147`); an origin-backed source
  returns a spurious 404 instead of streaming.
- `createBucket` clone always calls `store.CloneBucket`
  (`internal/s3/server.go:193-214`); cloning an origin-backed source creates an
  empty/partial local copy instead of failing or copying origin objects.

Fix: choose source and destination independently. Keep the zero-copy path for
local→local, keep `Tier.Copy` for same-tier origin→origin, and stream
`source.Open` → `destination.Put` for mixed cases (with source conditions
checked against the opened result). At minimum, detect mixed backends and
return `501 NotImplemented` rather than returning wrong bytes or a wrong 404.
Skip the local fast path when the source bucket has an origin. Reject or
implement origin bucket clones explicitly.

### 6. Clone is not snapshot-isolated and drops part checksums (P1/P2)

`s3p.clone_namespace` (`internal/store/schema.sql:136-171`) runs two
statements, `INSERT INTO dst.chunks ... SELECT` and `INSERT INTO dst.objects ...
SELECT`, under READ COMMITTED. A concurrent overwrite between them can publish
an object that references chunks not copied into the clone. Clone should run in
a repeatable-read transaction (or lock the source tables) so chunks and objects
come from one snapshot.

The clone also does not copy `object_part_checksums`. A cloned multipart object
loses per-part checksums, so `GetObjectAttributes`/`ObjectParts` differs from
the source. Add the copy, mapping no file IDs because the clone shares them.

`setval('s3p.file_ids', GREATEST(last_value, max_id))` in `adopt_namespace`
and `clone_namespace` is racy across different namespaces: two concurrent
adoptions can read the same `last_value` and the later `setval` can move the
sequence backwards. Add a shared transaction advisory lock around `setval` (and
document that restore/clone should not race writes), or replace the sequence
with a locked counter row if exactness matters.

### 7. Empty `If-Match` bypasses the SQL write condition (P1)

`store.Precondition.SQL` (now in `internal/store/object.go`) returns a nil match
value when `Mode == IfMatch` and `ETag == ""`. The SQL functions compare
`old_etag <> p_if_match`; with `NULL` the expression is `NULL`, not true, so the
precondition passes. `Precondition.Allows` correctly rejects the same input, so
the inline/streaming paths disagree.

This is not reachable through the S3 handler today (`putCondition` rejects an
empty ETag), but it is a parity bug in the shared API and exactly the class of
problem the recent condition refactors were removing. Return `"if_match", ""`
so the SQL comparison fails, and map `IfMatch: "*"` to `if_present` for parity
with `Allows`. Add a table test for `SQL`/`Allows` parity.

More broadly, `swapObjectParts` (`internal/store/ingest.go:458-498`) evaluates
conditions in Go while `publish_object`/`put_inline` evaluate them in SQL. They
already differ for expired objects and `IfPresent`. One source of truth (an SQL
function called by all write paths, or a Go helper that receives liveness) would
prevent future drift.

### 8. Deleted tombstones are treated as a hard miss; TTL equals fill timeout (P1)

`Tier.resolveCache` returns `ErrNotFound` immediately for `CacheDeleted`
(`internal/objcache/objcache.go:236-239`). That intentionally hides an object
recreated directly at the origin for ten minutes, but it also creates a
cross-gateway race: a concurrent PUT can write a newer origin object and a cache
row, then a delayed DELETE tombstone overwrites the cache row; readers see 404
until the tombstone expires even though the origin has the newer object.

Treating a tombstone as "revalidate against the origin" fixes this: remove the
early return, fetch origin attrs, and plan a pass-through (do not fill while the
tombstone row exists). The tombstone still fences an in-flight fill because the
fill's expected origin version no longer matches the tombstone token. This also
removes the documented "recreated directly at origin stays hidden" limitation,
which is surprising for an origin-authoritative cache.

Related races:

- `resolveCache` deletes a stale row with unconditional `CacheDel`
  (`internal/objcache/objcache.go:266-268`). If a concurrent delete writes a
  tombstone between the read and this delete, the tombstone is removed and an
  old fill can reinsert deleted bytes. Delete conditionally on the observed
  `origin_etag` (add `CacheDelExpected`).
- `originTombstoneTTL` is 10 minutes and the fill timeout is also 10 minutes
  (`internal/objcache/objcache.go:72`, `568-603`). A fill can outlive its
  tombstone at the boundary. Make the tombstone TTL clearly longer (15–20
  minutes) or shorten the fill timeout.

### 9. RESP cross-namespace and command semantics (P1/P2)

Concrete gaps in `internal/resp/commands.go`:

- `MGET`, `EXISTS` and `TTL`/`PTTL` call `resolveTarget` but discard the
  kind/bucket result, then query the current cache namespace with the literal
  `s3://...` key. A cross-namespace key silently becomes a cache miss or a
  lookup for a key that cannot exist. Either support cross-namespace reads or
  reject `s3://` consistently, as `writeTarget` already does.
- `FLUSHALL` is aliased to `FLUSHDB` (`resp.go:259-260`); it clears only the
  current cache namespace, not all cache namespaces. Implement all-namespace
  flush or return an explicit unsupported error.
- `GETSET`, `SET ... GET`, `APPEND` and `SETRANGE` are read-then-unconditional-
  write. Concurrent updates can return a stale old value or lose an append.
  Use the same optimistic conditional-retry pattern as `CacheIncrBy`, or add a
  single store operation.
- `MSET` is atomic only when every value fits inline; the large-value fallback
  writes keys one at a time (`internal/store/cache_mset.go:20-30`). Either keep
  atomicity with a transaction or reject large values explicitly.
- `KEYS`/`SCAN` use `path.Match`, which treats `/` as a segment separator.
  Redis globs do not; `foo/*` should match `foo/bar/baz`. Use a Redis-compatible
  glob matcher.
- `INFO` calls `StatNamespace`, which scans every object and expands
  `part_ends` to estimate chunks (`internal/store/namespace.go:646-724`). INFO
  can be expensive on a large cache. Use `DBSIZE`-style counts and relation
  sizes for INFO; keep the detailed scan for `pgvs3 stat`.
- A cross-namespace `GET s3://bucket/key` reads the whole object into memory.
  Either cap the response size or document it as small-value only.

Add store-backed RESP tests; the current 121-line test file only covers pure
parsing and dispatch.

### 10. `ListMultipartUploads` loses one upload at a page boundary (P1)

`internal/s3/attributes.go:219-249` sets `lastKey, lastID = u.Key, u.UploadID`
before checking whether the output budget is full. When the budget is hit, the
current (unreturned) upload becomes the next marker, so the next page starts
after it and drops it. Common-prefix roll-ups have the same problem. Set the
marker only when an item is actually returned; add a test with `max-uploads=1`
and two uploads.

### 11. Invalid-range responses report `bytes */0` (P2)

`getObject` on `ErrInvalidRange` sets `Content-Range` using
`resSize(res)` where `res` is nil (`internal/s3/server.go:375-413`), so a 416
for a non-empty object says the total size is zero. On the error path, fetch
the object size with `Head` (or return a typed range error carrying the size)
and emit `bytes */<size>`. s3-tests checks the status but may not check the
header; a real client can.

### 12. Cache `Last-Modified` and write-through ordering are approximate (P2)

`setObjectInfoHeaders` uses `ObjectInfo.CreatedAt` as `Last-Modified`
(`internal/s3/response.go:28-30`). For native objects this is the write time;
for cache hits it is the cache fill/touch time, not the origin
`Last-Modified`. Clients that use date conditionals see the wrong value, and
write-through cache rows have no ordering key to reject a delayed older write
that arrives after a newer one. Add an `origin_modified` column (origin
Last-Modified) to cache objects, set it on fill/write-through, use it for
`Last-Modified`, and make cache publication conditional on
`origin_modified >= current` (or keep a generation). This is a schema change,
so schedule it with the next layout bump.

## Simplifications

### 13. Remove the origin listing index (highest-value simplification, fixed)

The index and its generation machinery are removed. `Tier.List` now walks the
origin directly and keeps only the requested page in memory. This removes the
global tables, write-through upserts, TTL-stale listings and refresh races.
The remaining follow-up is a native start-after cursor in origin adapters to
avoid rescanning a prefix for deep pages.

### 14. Revisit the multipart outbox / write-through scope

`pending_uploads`, pending cache objects, token guards, leased flush workers,
and `flushOneLocked` are coherent but are the most complex part of the cache
tier. They exist to make a failed first origin upload retryable in the
background after the multipart `uploadId` has been consumed. Two alternatives:

- **Origin-native multipart for S3 origins**: proxy `CreateMultipartUpload`,
  `UploadPart` and `Complete` to the origin, store part ETags in PG, and let the
  origin assemble. This removes the staged part data and the whole-object flush;
  cache population can be a next-read fill. It requires extending the vendored
  client and changes retry semantics.
- **Read-only cache tier**: if write-through is not a target workload, dropping
  it removes the outbox, tombstones, origin write capability checks and most of
  the cache-tier failure modes. This is the largest scope lever; decide it
  before investing more in the outbox.

If write-through stays, at least make a failed completion retryable without
consuming the staging rows (origin-first with staging retained until success),
or add a bounded attempt/dead-letter state so a poison row does not retry
forever.

### 15. Unify the three publish paths

`publish_object` and `put_inline` in `schema.sql` and `swapObjectParts` in
`internal/store/ingest.go` each implement advisory locking, old-value capture,
conditions, `origin_etag`, TTL handling and garbage queueing. They already
differ on empty `If-Match`, expiry and `IfPresent`. A single SQL function that
accepts the common bundle (including inline data) or a single Go transaction
helper would reduce the surface and make condition tests meaningful.

### 16. Remove legacy SQS remnants

`JobPending` is never written (`internal/store/ojs_state.go:19-22`); the
`messages` checks in `adopt_namespace`/`ensure_namespace`/`drop_namespace` and
the `job_index.kind` column are historical. The layout was v13 at review time (v14 after the OJS fix) with no
migration path, so this can be deleted in the next layout bump. Small, but it
removes misleading compatibility code.

## Performance and operations

### 17. OJS fetch does maintenance on every poll (P1)

**Implemented in layout v14.** Fetch now performs one claim over available,
due and expired-lease rows using the generated `next_visible_at` column;
maintenance owns expiry, due promotion and exhausted leases; enqueue/fetch/
ack/nack/heartbeat use the database clock. The notes below are the original
analysis and alternatives.

`Store.FetchJobs` (`internal/store/ojs.go:320-394`) runs `expireJobs`,
`expireLeases` and `promoteDueJobs` in a transaction, per queue, on every
fetch. `ojs.waitForFetch` (`internal/ojs/ojs.go:546-560`) retries every 100 ms
for up to two seconds, so an idle worker can issue roughly 20 transactions and
60 update statements per long poll. With many workers, this is a write and lock
amplification problem even when the queue is empty.

Modern queues use a cheap claim query and run expiry/promotion outside the
fetch path. Options, in increasing effort:

1. Add a `maintain bool` parameter to fetch: run maintenance on the first
   attempt and then at most once per second during a long poll; later attempts
   only select/lease.
2. Fold due scheduled/retryable rows into the candidate query and drop
   `promoteDueJobs` from fetch. Expired active rows can be treated as
   candidates, with the lease transition handling exhaustion/discard in Go.
3. Move queue expiry and terminal-job cleanup to the maintenance leader (or a
   dedicated ticker), leaving fetch with one `SELECT ... SKIP LOCKED` and one
   `UPDATE`.

Also cap `expireLeases` with `LIMIT`/`ORDER BY lease_until`; today it selects
every expired active row with no bound. Use the same database clock for lease
validation (`AckJob`, `NackJob`, `validateQueueLeases` compare DB
`lease_until` to gateway `time.Now()`; skew can cause spurious 409s or accept
an expired lease).

### 17a. PgQue is not a drop-in replacement for OJS (evaluated 2026-10-26)

Implementation note: the PgQue-shaped **stream** half of this comparison is now
implemented as OJS streams in the same `s3q_` schema (`docs/queue-stream.md`),
including fan-out, cooperative members and three-segment `TRUNCATE` rotation.
The OJS **job** half remains lease-based. This section is retained as the
rationale for that split.

[PgQue](https://pgque.dev/) is a well-designed pure-SQL queue, but it targets a
different problem: a **shared append-only event log with independent consumer
cursors**, closer to an event stream than a job queue. Its own docs say:

> "PgQue is an event/message queue optimized for high-throughput streaming with
> fan-out — not a job-queue framework."

and:

> "Choose a job queue when you need per-job lifecycle, sub-millisecond dispatch,
> priority queues, cron scheduling, unique jobs, or deep ecosystem integration."

OJS is exactly that job-queue shape. The mappings do not line up:

| OJS requirement | PgQue behavior |
| --- | --- |
| Per-job state (`scheduled`, `available`, `active`, `retryable`, `completed`, `cancelled`, `discarded`) | Events have `ev_type`/`ev_data` and a retry count; no job state machine |
| `fetch(count)` returns up to `count` individually leased jobs | `receive()` returns the **whole next tick batch** or raises `54000` if it exceeds `max_return`; `max_return` is a safety ceiling, not a page size |
| Per-job `lease_token`, `visibility_timeout_ms`, heartbeat | Batch-level `ack(batch_id)`; `receive_coop` has stale-batch takeover via `dead_interval`, not a per-job lease/heartbeat |
| `ack`/`nack` per job | `ack` is per batch; `nack(batch_id, msg, retry_after)` is per message but only inside the active batch |
| Priority, `scheduled_at`, `expires_at`, `timeout`, tags | No priority or delayed send; retry queue only (`nack` after delivery) |
| `GET`/`DELETE /jobs/{id}`, queue state stats | No job-id index, cancel, or OJS state counts |
| Per-queue schema, `DROP SCHEMA`, `pg_dump -n`, namespace quota | One global `pgque` schema with rotating tables; roles are database-global, not per-queue; per-tenant installs are not yet officially supported |
| No scheduler dependency | Requires `pg_cron` or an external ticker; no ticks means no delivery. Default tick is 100 ms; p50 latency is ~52 ms (p99 ~103 ms), and 10 ms ticks improve the median but push the tail to ~1 s |
| Low-latency co-located queue | Batch/tick design is explicitly a latency trade-off; current OJS fetches are immediate and lease per job |

Mapping OJS onto PgQue would not remove the complex part:

- OJS still needs a `jobs` table for state, id lookup, cancellation, per-job
  results/errors and stats. PgQue would only replace the claim/receive
  mechanism.
- OJS `count`/visibility/heartbeat/priority/scheduling do not map to PgQue's
  batch cursor. A compatibility shim would have to emulate them, likely with
  one event per job, a side state table, a custom ticker and a worker
  heartbeat layer.
- The global `pgque` schema conflicts with the current one-schema-per-queue
  model (`DROP SCHEMA CASCADE`, per-queue `pg_dump`, per-namespace quota and
  maintenance). We would either give up those properties or install/track
  PgQue queues separately.
- `pg_cron` or a new high-frequency ticker becomes an availability dependency.
  The current OJS fetch works as soon as a writable gateway is up.
- PgQue 0.2 is young, and its cooperative-consumer API is marked
  **experimental**.

The useful part of the suggestion is the **bloat diagnosis**: row-mutating
`SKIP LOCKED` queues can suffer under a held xmin horizon, and pgvs3's large
repeatable-read reads can hold xmin for the duration of a slow 64 MiB+ stream.
That risk now drives a separate stream path with three-segment `TRUNCATE`
rotation, while OJS hot-row churn is handled by the lease/retention changes in
items 17 and 19. It does not justify replacing a per-job lifecycle with an event
log.

The cache-tier outbox is a similarly poor fit: it needs one unique mutable row
per `(namespace, key)`, a captured origin base version, leases, immediate first
flush, and token-fenced completion. PgQue is an append-only shared log with
batch acks and no per-key dedupe/update; using it would add a side table and
tick latency without removing the conditional origin-flush logic.

The implemented shape is a **stream view inside the queue namespace**, not an
OJS storage backend. PgQue itself is not vendored; its snapshot/batch/rotation
ideas are implemented in the queue schema and share its lifecycle. The OJS job
table remains the job authority.

### 18. Outbox workers are per tier and poll every second (P1/P2, fixed)

`newTier` starts `min(maxFills, 8)` workers per origin tier
(`internal/objcache/objcache.go:128-140`). Each worker claims one row and
sleeps one second when idle (`runPendingWorker`). With per-bucket origins, 100
tiers can mean 800 idle workers and ~800 queries/s per gateway; the docs'
"`--fill-concurrency` bounds the pending-upload worker pool" is not true across
tiers. `fills`/`completes` are also per-tier, so total fill concurrency is
tiers × `--fill-concurrency`.

Status: fixed with process-wide fill and pending-upload semaphores. Idle
per-tier workers still exist but no longer generate per-tier claim traffic, and
global concurrency is bounded. One dispatcher and a terminal dead-letter state
for permanently failing uploads remain possible follow-ups.

### 19. OJS terminal jobs accumulate (P2)

**Implemented in layout v14.** `ExpireQueueMessages` now also removes
completed/cancelled/discarded jobs older than `--job-retention` (default 24h)
and their `job_index` rows. The notes below are the original analysis.

`ExpireQueueMessages` only deletes rows with `expires_at` set
(`internal/store/ojs.go:1016-1042`). Completed/cancelled/discarded jobs without
an explicit TTL remain forever, along with `s3p.job_index` rows. Modern queues
keep a bounded retention/archive. Add a `--job-retention` (e.g., 24 h after
terminal timestamp) with a bounded maintenance delete, or require/default
`expires_at`.

### 20. Origin listing/copy/delete can use fewer origin calls (P2)

- `Tier.DeleteMany` performs one origin DELETE per key
  (`internal/objcache/objcache.go:1152-1182`); S3 `DeleteObjects` supports up
  to 1,000 keys per request.
- `Tier.Copy` streams the source through the gateway; for the same S3 origin a
  native `CopyObject` would avoid data movement. `UploadPartCopy` could use the
  origin's multipart copy for large parts.
- `Tier.Copy` spools the whole source before uploading; a large copy can need a
  full temp file in addition to the origin read.

These are worthwhile for large-bucket operations but are not required for
correctness.

### 21. PG18 AIO is available but not guided

The storage design already relies on bitmap heap scans and sets
`effective_io_concurrency=32` per connection. PG18's new `io_method=worker`
(default) / `io_uring`, `io_workers` (default 3), `io_combine_limit` and
`maintenance_io_concurrency` are not mentioned in the compose/chart/ops docs.
PG18 benchmarks report 10–25% gains on bitmap/sequential scans. Add a short
operations note and benchmark with `io_method=worker`, `io_workers≈cores/4`,
`io_combine_limit=16` (or `io_uring` where available). This is environment
specific, so measure before changing defaults.

### 22. Chunk-size experiment (only if write throughput matters)

The current 8,120-byte row is a clever page-budget fit, but it is at the small
extreme compared with JuiceFS's 4 MiB blocks and the PostgreSQL community's
128 KiB–32 MiB chunk-table guidance. A 64 KiB `EXTERNAL` row would reduce row
count/index size/WAL records by ~8×, improve compression ratio, and may improve
large-write throughput; it would also introduce TOAST/detoast on every chunk
read and change range planning. Run a controlled benchmark before changing the
layout. Do not switch to `pg_largeobject` or a single `bytea`; both lose
streaming/VACUUM/immutability advantages.

### 23. Metrics, observability and docs

- Fix the metrics panic and add cache counters (`hit`, `miss`, `fill_error`,
  `pending_uploads`, `listing_refresh_error`), OJS fetch duration/claimed jobs,
  and S3 error-code counters.
- `s3.writeError` writes `x-amz-request-id` three times with different values
  (`internal/s3/errors.go:82-98`); set one ID per request and reuse it in logs.
- Update docs that currently overstate behavior: the HTTP-origin listing error
  is a 500 today; `--fill-concurrency` is per tier; the image/chart examples in
  README are older than the current tags; `ListObjectVersions`/`partNumber` on
  origin buckets are more limited than the general statement.
- Add `just s3t` to the PR CI path (the allow-list is only 36 tests and already
  runs nightly); handler regressions currently wait for the nightly job.

## Testing gaps worth closing first

- `internal/s3/metrics_test.go`: observe then write (would have caught P0 1).
- `internal/gateway`: mux classification and `--max-conns` lifetime.
- `internal/resp`: store-backed MGET/EXISTS/TTL/FLUSHALL, atomic GETSET/APPEND/
  SETRANGE, Redis glob.
- `internal/s3`: missing-bucket GET/HEAD/LIST/DELETE, invalid-range
  `Content-Range`, `ListMultipartUploads` page boundary, read-only-origin
  writes.
- `internal/store`: `Precondition.SQL`/`Allows` parity, self-copy multipart part
  checksums, clone checksum/snapshot tests, sequence `setval` race (or a
  documented test for the lock).
- `internal/objcache`: tombstone revalidation, tombstone/fill boundary,
  conditional stale delete, per-tier worker bounds.
- Strengthen `FuzzChunkedBody` to assert that a truncated non-final body
  returns `IncompleteBody`, not a silent EOF.

## Recommended sequence

1. **Small P0/P1 fixes (0.5–1 day):** metrics key, mux connection lifetime,
   origin capability mapping, empty `If-Match`, invalid-range header,
   `ListMultipartUploads` marker.
2. **S3 error fidelity and copy routing (3–5 days):** missing-bucket errors,
   mixed-backend copy/part-copy/clone, RESP cross-namespace/FLUSHALL/atomic
   commands, clone snapshot/checksums/sequence lock.
3. **Origin listing cursor (1–2 days):** add native start-after support to
   origin adapters so direct `Tier.List` does not rescan a prefix for deep
   pages. The index itself is already removed.
4. **Queue/cache performance follow-ups:** one outbox dispatcher with terminal
   failure state, cheap `INFO`, and any remaining queue hot-path profiling.
5. **Cache consistency and scope decision (2–4 days):** tombstone revalidation,
   conditional stale delete, `origin_modified`, then decide whether
   write-through/outbox stays or the tier becomes read-only.
6. **Ops/documentation:** PG18 AIO guidance, metrics, s3t on PR, version/chart
   examples, restore/clone caveats.

Do not spend time on authentication, TLS, S3 versioning, RESP3 or in-place
schema migrations for this envelope; those are explicit non-goals or premature
before a public beta.
