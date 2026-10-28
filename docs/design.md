# Design and shape

pgvs3 is a stateless Go gateway that projects one PostgreSQL database as S3,
OJS (OpenJobSpec v1 core), plain HTTP and RESP2 on a single port.

For a quick start see [README.md](../README.md); for deployment see
[operations.md](operations.md); for storage internals and query paths see
[performance.md](performance.md).

## Start here

- **What it is:** one gateway binary, one listener, one shared PostgreSQL
  database. There is no sidecar, local object store, or invalidation bus.
- **Supported scale:** at most **100 namespaces** (PostgreSQL schemas) per
  database, one PostgreSQL primary plus streaming standbys, and multiple
  gateway workers sharing that cluster.
- **Safety model:** PostgreSQL is the coordination plane. Gateways keep no
  authoritative local state; the catalog, advisory locks, leases, conditional
  writes, and token guards coordinate concurrent workers.
- **Read next:** [README.md](../README.md) for the quick start,
  [operations.md](operations.md) for topology and backups, and
  [performance.md](performance.md) for the storage layout.

## Operating envelope

| Dimension | Envelope |
| --- | --- |
| **Namespaces** | At most **100 PostgreSQL schemas per database**, counting buckets, cache namespaces, and queues. This is a bounded single-cluster envelope, not a schema-per-tenant or sharded design. |
| **PostgreSQL** | One primary (writer) plus streaming standbys. A single database, not multi-primary or sharded. |
| **Gateway workers** | Multiple stateless workers behind a load balancer. Writable workers connect to the writer endpoint; read-only workers may connect to standbys. |
| **Reads and lag** | Writable workers read from the primary. Read-only workers on standbys serve reads subject to PostgreSQL replication lag. |
| **Writes** | Only through writable workers connected to the primary. DuckLake keeps one writer per bucket; many gateway workers share the database, but the lake writer remains single. |
| **Backups** | Physical base backup plus WAL archiving/PITR for production; logical `pg_dump` for export or small deployments. Origin data is backed up separately. See [operations.md](operations.md#backup-and-restore). |
| **Security** | Trusted network only. Authentication, authorization, TLS termination, audit logging, and per-tenant isolation are permanent non-goals. |
| **Layout** | Current layout is **v25** (logged parent metadata for range caching, renewable owner-fenced fills, and consolidated PostgreSQL 18 publication SQL, retaining the v23 logged `s3c_`/`s3w_` split and segmented streams). There is no in-place migration path; a layout change requires a fresh database and a re-import. |
| **Coordination** | PostgreSQL catalog, advisory locks, database leases, conditional writes, token fences, and sequences. No local process state is authoritative. |

## Trust model (permanent)

**pgvs3 is designed for trusted networks. Authentication is not part of the
design and is not a future goal.**

There is no authentication, authorization, request signing, credential
validation, TLS termination, audit log, or per-tenant isolation. These are
non-goals, not gaps on a roadmap. The gateway accepts header-based SigV4
because standard S3 SDKs send it, but it does not verify the signature and does
not use the credentials for anything. Presigned URLs are rejected because a
time-limited, object-scoped link would otherwise become an unscoped grant on
the entire gateway.

The only access control is network placement. Run pgvs3 on loopback, a private
VPC/VLAN, or behind a service mesh that enforces identity, authorization, and
TLS. Never expose `:8014` to an untrusted network.

What this means in practice:

| Capability | Where it is enforced |
| --- | --- |
| Who may connect | Network / service mesh / firewall |
| Who may read or write a namespace | Network / service mesh |
| TLS in transit | Service mesh or private network |
| Audit and request attribution | Service mesh / PostgreSQL logs |
| Bucket drop, queue drop, cache flush | Network / service mesh |

Anyone who can open a TCP connection to `:8014` can read, overwrite, or delete
every namespace, force-drop buckets (`DROP SCHEMA CASCADE`), drop queues, and
flush cache namespaces. Treat the listener as a database port, not as a public
API.

## Shape of the system

```
clients ── S3 / OJS / HTTP / RESP2 ──┐
                                     │  one listener :8014 per worker
                          ┌──────────▼──────────┐
                          │  gateway workers    │  stateless
                          └──────┬───────┬──────┘
                                 │       │
                    writer endpoint│       │read endpoints
                          ┌──────▼──┐  ┌─▼────────────┐
                          │PostgreSQL│─▶│ PostgreSQL   │
                          │ primary  │  │ standbys     │
                          └──────────┘  └──────────────┘
                              s3p catalog + one schema per namespace
```

- **One binary, one port.** S3, OJS and plain HTTP share the HTTP listener;
  RESP2 is detected from the first byte. There is no sidecar, no local object
  store, and no per-pod cache of object bytes or metadata.
- **One PostgreSQL schema per namespace.** `s3b_<name>` is a durable bucket,
  `s3c_<name>` the WAL-logged shared cache namespace (origin fills and
  RESP/cache users), `s3w_<name>` a WAL-logged write-back namespace, and
  `s3q_<name>` an OJS queue/stream namespace. Each owns its tables, indexes, autovacuum workers
  and 32 TiB chunk budget. `DROP SCHEMA CASCADE` is the delete primitive.
- **The catalog is authoritative.** `s3p.namespaces` records namespace kind,
  name, schema, quota, and attributes. Buckets are created explicitly through
  `CreateBucket` (`PUT /{bucket}`). An object write to a missing or deleted
  bucket returns `NoSuchBucket`; it never recreates the bucket from a stale
  local view.
- **Immutable files, atomic publish.** A write allocates a new `file_id` from
  the database sequence `s3p.file_ids`, streams 8,120-byte chunk rows, and
  publishes by swapping the object pointer in one transaction. Overwrites queue
  the old `file_id` for the reaper; readers use MVCC snapshots, so an in-flight
  read never observes a torn object.
- **Small objects inline, large objects chunked.** Objects up to 64 KiB live in
  `objects.inline_data` (Zstd when it shrinks). Larger objects use 8,120-byte
  chunk rows, one per PostgreSQL page, with logical offsets fixed at
  `no × 8,120` for O(1) range seeks.
- **PostgreSQL is the shared cache and coordination plane.**
  `shared_buffers` holds the hot metadata and bytes. Committed writes are
  visible immediately to other writable workers; read-only workers on standbys
  may lag. One writable gateway holds a PostgreSQL advisory lock and runs
  reconciliation, TTL expiry and bounded garbage collection.
- **The cache tier is optional and asymmetric.** With `--origin-config`, S3
  reads become read-through fills in the WAL-logged `s3c_` namespace, while
  writes use the WAL-logged `s3w_` namespace. `s3c_` is the shared WAL-logged
  fill and RESP/cache namespace. The origin remains the long-term
  source of truth; `s3w_` is a durable staging area for acknowledged writes
  until the outbox commits them. See [Origin cache tier](#origin-cache-tier).

## Internal data shapes

### Cache code boundaries

`internal/objcache` is organized by lifecycle, not by protocol or a second
storage abstraction:

| File | Owns |
| --- | --- |
| `cache.go` | Tier lifecycle, one namespace/TTL resolver, range planning, renewable fills, bounded spooling, foreground mutations and token-guarded outbox drain |
| `origin.go` | Compiler-checked driver interface, capability/prefix/error boundary and S3/HTTP/filesystem/in-memory adapters |
| `registry.go` | Persisted configuration and one tier-replacement policy for global and per-bucket origins |

Named sections follow the request lifecycle. Foreground mutations and outbox
flushes use `lockWrite`: local ordering, optional body validation, then a
PostgreSQL session lock held across origin I/O. Fill ownership remains a
renewable database lease, not a session lock. Registry replacements retain a
working tier when opening a changed configuration fails.

Whole-object and aligned-range fills share coordination, not body buffering:
large bodies still spool before taking a database writer connection. A waiting
request can cancel without cancelling the fill owner. Write-back bodies remain
logged, non-expiring while pending, and separate from the read cache. Neither
this organization nor adding an adapter requires a layout change.

Keep simplification above the chunk loop: preserve the specialized COPY ingest,
bitmap-read streaming, buffer reuse and bounded parallel reads in `store`.
Moving files is not itself a code reduction; prefer eliminating duplicate
decisions and invariants over introducing generic frameworks or removing tests.

For a performance-sensitive refactor, compare old and new binaries on the same
PostgreSQL/origin setup with 10 MiB, 100 MiB and 1 GiB incompressible objects.
Measure cold/warm GET, four-reader GET, HEAD, cold/warm 64 KiB ranges and durable
write-back acknowledgement separately. Drain the outbox between samples and
use repeated measurements. Reject a change that increases median latency by
more than 20% or reduces throughput by more than 20%; do not hide a regression
in an aggregate average. See [benchmarks.md](benchmarks.md).

### Store and RESP code boundaries

- `store/store.go` owns connections, embedded schema, namespace lifecycle,
  admission and maintenance. Both pool policies share failed-open cleanup;
  writable startup and read-only startup share layout validation.
- `store/object.go` owns object contracts, metadata decoding, streaming reads
  and block encoding. `store/write.go` owns inline/COPY publication, multipart
  planning and server-side copies. Specialized algorithms remain named sections,
  not a new generic streaming layer.
- The metadata section of `store/object.go` owns the column list and decoder.
  Header reads omit inline bytes, ordinary reads decode them, and server-side
  COPY keeps their stored encoding. Do not add another partial metadata decoder
  when adding a field.
- `store/cache.go` owns origin metadata, fill fences, durable outbox persistence
  and the optimistic snapshot/transform/publish loop
  for GETSET, APPEND, SETRANGE and INCR. Transforms receive owned bytes and can
  run again after contention. Conditions and TTL preservation use the canonical
  writer; INCR remains inline-only and retains its cancellation-bounded retry
  behavior. `Cache.Set(ReturnOld)` uses this same atomic path.
- `Cache.GetDel` decodes `DELETE RETURNING` with the shared metadata decoder and
  reads the pinned body with the normal object reader before committing. It
  supports multipart bodies and rolls back on a corrupt/truncated read.
- Cache expiration, including a past deadline, is one conditional UPDATE.
  Readers hide expired rows immediately; ordinary TTL maintenance reclaims
  storage. PERSIST and NX/XX/GT/LT use that same predicate path.
- RESP handlers share value lookup and expiration-unit conversion. Narrow
  cache GET queries and batched MGET/MSET are intentionally specialized: a
  generic metadata query or per-key loop would penalize the small-value path.

### Protocol and queue boundaries

- S3 transport/routing is in `s3/server.go`, parsing/framing/checksums in
  `s3/request.go`, object operations and response streaming in `s3/object.go`, and
  XML models/encoding in `s3/xml.go`. One native and one origin adapter implement
  read, write and multipart capabilities; narrow handler interfaces remain.
- Job persistence and transitions live together in `store/ojs.go`; stream
  persistence stays in `store/stream.go`. Ordinary stream transactions use the
  store's existing `withTx` lifecycle. Maintenance paths with deliberate
  rollback-on-no-op behavior retain explicit transactions.
- OJS endpoints return a body or an error. Typed `jsonEndpoint` binding owns
  read-only checks and bounded JSON decoding; one response writer owns status,
  Location/Allow headers, error privacy and JSON encoding. Request validators
  retain their operation-specific rules and error precedence.
- A stream route's method table is also its Allow-header definition. Job fetch
  and stream receive share a polling loop, but take admission only during
  database attempts, never while sleeping or waiting on a client.
- `FetchJobs` uses one `SKIP LOCKED` candidate/update/return statement per queue,
  not a read/Go-plan/update/reload pipeline. The database clock and generated
  `next_visible_at` govern schedules and expired leases; exhausted attempts stay
  with maintenance. All requested queues still share one claim transaction.
- ACK, NACK and CANCEL share locked-row loading, transition validation and
  transaction ownership. The direct transition switch and its source-state
  table remain the policy for row mutations and maintenance. Claim-case tests
  exercise the actual SQL implementation against PostgreSQL.
- Normal and cooperative stream receives share one snapshot/claim engine.
  Cursor selection and empty-batch advancement are the only policy branches;
  snapshot visibility, batch ceilings, replay, segment locks and publication
  are common. Cooperative acknowledgements still advance over contiguous work.
- Serve flags define environment types and defaults once. Environment values
  go through the flag parser, retaining yes/no booleans and decimal integers;
  CLI arguments override them. Bad environment values no longer silently fall
  back for unvalidated flags. Administrative force flags are not enabled by
  this environment binding.
- S3 adapters use method promotion for exact `Tier` methods. Only operations
  with genuinely different native/origin contracts retain wrappers.

### Shared values

The protocol adapters share a small set of canonical values instead of
repeating positional bundles:

- **`store.ByteRange` / `store.Span`** carry the parsed request and the
  resolved inclusive byte span. `Requested` distinguishes an absent Range
  header from an explicit `bytes=0-`, so native, cache, and origin reads agree
  on `200` versus `206`.
- **`store.WriteOptions` / `store.Precondition` / `store.ReadCondition`**
  carry write conditions, metadata, TTL, origin versions, and conditional-read
  and copy-source checks for inline, streaming, multipart-part, cache, and
  origin paths. Copy uses one `ReadCondition` for the source and one
  `Precondition` for the destination.
- **S3 `readBackend` / `writeBackend` / `multipartBackend`** normalize native
  PostgreSQL and origin-cache reads, ordinary writes, and multipart operations
  behind explicit seams while preserving each path's transaction, checksum,
  lease, and tombstone rules. Multipart completion uses the same
  `WriteOptions`/`Precondition` value as ordinary writes.
- **`ObjectRef` addresses object operations**, including copy and copy-part,
  across native storage, cache and origin paths. Bucket/namespace lifecycle methods remain bucket-oriented
  because creating, dropping and listing namespaces is a catalog operation,
  not an object read or write.
- Namespace catalog changes and cache-object touch operations live in
  `store`; `objcache` does not issue catalog SQL directly.
- **Queue/stream namespaces** carry both the OJS job state table and an
  append-only PgQue-shaped stream (`stream_events` partitioned into three
  rotating segments, snapshot ticks, subscriptions, batches, retry and
  dead-letter rows). They share one `s3q_<name>` schema, catalog row, quota,
  backup boundary and maintenance pass. The job path remains lease-based; the
  stream path uses independent fan-out cursors and snapshot-delimited batches,
  and maintenance truncates a sealed segment only after every cursor is past
  its seal and no batch references its events. See [queue-stream.md](queue-stream.md).

## Multi-worker safety invariants

Every cross-worker decision is made in PostgreSQL or at the origin with a
condition. In-process maps and gauges may be used as hints, but never as the
authority.

| Invariant | Coordination primitive | What it prevents |
| --- | --- | --- |
| **Namespace existence** | `s3p.namespaces` catalog and namespace DDL functions | A stale worker view or racing write recreating a deleted bucket |
| **Unique immutable file IDs** | `s3p.file_ids` database sequence | Two workers or pods allocating the same `file_id` from local counters |
| **Same-key conditional writes** | Per-key transaction advisory lock plus `If-Match`/`If-None-Match` SQL checks | Concurrent PUT/COPY requests both satisfying a condition that only one should |
| **Maintenance leader** | Session-scoped `pg_try_advisory_lock` | Duplicate reconciliation, TTL expiry, and reaping passes across the fleet |
| **Origin upload retries** | `s3p.pending_uploads` rows claimed with database leases and `FOR UPDATE SKIP LOCKED` | Duplicate retries, a crashed worker holding a row forever, or an expired worker racing a live one |
| **Multipart cache publish** | `pending:` object token and a token-guarded conditional cache update | A stale completion publishing over a newer PUT/DELETE or resurrecting a deleted object |
| **Read-through cache fill** | Expected origin version recorded with the fill and checked conditionally when publishing | A slow fill overwriting a newer write-through cache row |
| **Cold origin fill** | Lease row in `s3p.cache_fill_leases`, then expected-origin publish | Multiple gateways downloading the same cold object at once |
| **Origin listings** | Direct bounded origin walk | TTL-stale listing rows or refresh races |

What happens when a worker dies:

- **Maintenance leader:** the advisory lock is released with the database
  session. Another writable gateway acquires it on a later maintenance tick;
  there is no manual election step.
- **Origin upload:** the outbox row stays durable. Its lease expires and
  another worker claims it. The flush still carries the origin precondition
  captured at completion, so late work fails safely instead of overwriting a
  newer object.
- **Cache fill or multipart publish:** the conditional cache update fails its
  version/token check and is discarded. Readers continue to see the newer
  object or a cache miss.
- **Client request:** the request may fail. Clients retry; atomic publish means
  an interrupted write leaves nothing visible.

## Read-only gateways and failover

`--read-only` is an explicit gateway mode for PostgreSQL standbys and for
read-only fleets on a primary. It is a fail-fast guard, not automatic standby
detection.

| Operation | Read-only gateway |
| --- | --- |
| S3 `GET`/`HEAD`, listings, bucket metadata, object attributes | Allowed |
| S3 `PUT`/`COPY`, multipart mutations, `DELETE`, `CreateBucket`/`DeleteBucket` | Rejected (`403`) |
| OJS job/queue metadata reads | Allowed |
| OJS enqueue, fetch, ack, nack, heartbeat, cancel | Rejected (`403`) |
| RESP reads (`GET`, `MGET`, range reads, metadata) | Allowed, including logged `s3c_` cache reads |
| RESP writes (`SET`, `DEL`, expiry changes, `FLUSHDB`, counters) | Rejected |
| Schema initialization, DDL, reaping, cache fills, origin retry worker | Skipped |
| Durable `s3w_` write-back reads (logged tables) | Allowed, subject to replication lag |
| Shared `s3c_` read-cache reads (logged tables) | Allowed, subject to replication lag |
| `/_cache/...` raw cache reads (logged `s3c_`) | Allowed, subject to replication lag |

Writes are rejected because a standby cannot accept them, and because leases,
file IDs, cache fills, and maintenance leadership all require a writable
primary. Rejecting at the protocol boundary avoids partially executing a
request that would fail at the database. `/readyz` checks connectivity and the
installed layout; it does not detect whether the endpoint is a primary or a
standby, so deploy writable and read-only workers with the correct endpoints.

During a PostgreSQL failover:

1. The writer endpoint is repointed to the promoted standby by the cluster's
   HA mechanism (DNS, service, or proxy). Writable gateways reconnect.
2. In-flight requests on connections to the old primary may fail. Clients
   should retry; conditional writes and atomic publish make retries safe.
3. Maintenance leadership re-elects automatically when a writable gateway
   reaches the new primary and acquires the advisory lock.
4. Read-only workers connected to standbys continue serving reads, subject to
   replication lag and their own reconnection.

A gateway pointed at the wrong role is still safe: a writable gateway pointed
at a standby fails writes at PostgreSQL, and a read-only gateway pointed at a
primary still rejects mutations. Monitor errors and route endpoints correctly.

## Origin cache tier

The cache tier is enabled with `--origin-config` or a persisted origin
configuration. It is optional; without it, buckets, cache namespaces, and
queues store their data directly in PostgreSQL.

| Path | Behavior |
| --- | --- |
| **Read-through miss** | Fetch from the origin, spool the body outside a database transaction, then publish into the WAL-logged `s3c_<namespace>` with the expected origin version. Durable writes use `s3w_<namespace>`. |
| **Read hit within TTL** | Serve from PostgreSQL without contacting the origin (`X-Cache: HIT`). |
| **TTL revalidation** | After `--cache-ttl`, compare the cached object with origin size/mtime/version; refresh the cache or refill it if the origin changed. |
| **Write-through / write-back** | Small and conditional `PUT`/`CopyObject` writes mutate the origin and cache synchronously. Large unconditional writes commit a WAL-logged `s3w_` object plus the `s3p.pending_uploads` row atomically, acknowledge immediately, and let the outbox upload the origin; pending bodies are readable and survive crash/failover. The WAL-logged `s3c_` namespace holds both shared read fills and RESP/cache values. `DELETE`/`DeleteObjects` remain synchronous and write a thirty-minute tombstone to the origin cache and write-back namespaces. |
| **Multipart completion** | Publish the assembled write-back object plus an `s3p.pending_uploads` row atomically. Large unconditional completions use ack-first write-back; conditional completions flush synchronously under the captured base-version precondition. |
| **Range cache** | With `--range-cache-chunk`, cache aligned chunks of large objects independently; without it, ranges above 8 MiB pass through to the origin. |
| **Listings** | Serve `ListObjectsV2` by merging the origin walk with live durable write-back objects, one bounded page in memory; no listing index or refresh generation. |

Cross-worker safety:

- **Stale fill fencing:** a read-through fill records the origin version it
  read and publishes conditionally. If a newer write-through write lands first,
  the stale fill cannot overwrite it.
- **Delete tombstones:** a write-through delete replaces the cache row with an
  expiring tombstone. A fill that started before the delete fails the version
  check instead of re-inserting deleted bytes.
- **Conditional origin writes:** production writable origins must support
  `If-Match`/`If-None-Match`. Unsupported conditional combinations are rejected
  rather than ignored.
- **Leased outbox:** a bounded pool of workers claims pending uploads with
  database leases and flushes rows in parallel. Expired leases are reclaimable
  after a crash.
- **Token guards:** multipart flushes and cache publishes check the pending
  token or expected version, so late work is discarded instead of reviving an
  older object.

Limits:

- **No full external invalidation.** pgvs3 does not receive origin events. An
  object changed directly at the origin may be served from cache until TTL
  revalidation or eviction.
- **Delete tombstones are bounded.** A write-through delete leaves a
  thirty-minute tombstone so in-flight fills cannot resurrect the object. An object
  recreated directly at the origin (outside pgvs3) may remain hidden through the
  cache until that tombstone expires; a write through pgvs3 replaces it
  immediately.
- **Write-back changes the authority window.** Acknowledged large writes are
  durable in PostgreSQL and may exist only in the cache until the outbox upload
  completes. Losing the cache namespace before that upload can lose those
  bytes; once the outbox drains, the origin is authoritative again. Persistent
  origin failures keep cache objects unexpired so acknowledged data is not
  silently evicted.
- **Pending writes and listings.** A pending write is immediately readable by
  key, and `ListObjectsV2` merges pending cache objects with the origin listing,
  preferring the write-back metadata for duplicate keys. A durable write-back
  object keeps its local content ETag across the pending/committed transition;
  the origin ETag is retained for conditional origin operations. Outbox origin uploads
  take a PostgreSQL advisory lock per key, so concurrent gateways cannot
  reorder background copies; small synchronous writes still use the local key
  lock only.
- **Origin is authoritative for committed objects.** Losing or flushing the
  cache namespace loses acceleration for committed objects; reads refill from
  the origin.
- **Read-only workers do not fill the cache.** They pass cold reads through
  and never start the outbox worker.

See [operations.md](operations.md#cache-tier-in-front-of-another-object-store)
for configuration and [performance.md](performance.md) for the fetch and flush
paths.

## Scope boundaries

Supported behavior is documented in [README.md](../README.md). The following
are explicitly out of scope and return errors or are ignored by design:

| Area | Out of scope |
| --- | --- |
| Trust | authentication, authorization, SigV4 validation, presigned URLs, TLS, audit, per-tenant isolation, rate limiting |
| S3 | IAM/bucket policies, versioning, ACLs, Object Lock, tags, lifecycle, replication, event notifications, SSE-C, SSE-KMS, DSSE-KMS, multipart `FULL_OBJECT` checksums |
| OJS | workflows, cron, SSE/WebSocket events, schema registry, dead-letter management, pause/resume, full conformance |
| RESP | RESP3, `MULTI`/`EXEC`/`WATCH` |
| Scale | More than 100 namespaces per database, schema-per-tenant layouts, sharding, multi-primary PostgreSQL |
| Layout | In-place migrations; every layout change requires a fresh database |

The one versioning affordance is a read-only `ListObjectVersions` shim: it
reports each current object as the `null` version so test harnesses and
cleanup tools can empty a bucket. It does not create versions or delete
markers, and `versionId` on GET/DELETE remains unsupported.

When a feature is not implemented, the preferred behavior is to reject it
explicitly rather than accept and silently ignore it. Presigned URLs follow
that rule. The remaining compatibility gaps and their options are tracked in
[proposals/hardening-options.md](proposals/hardening-options.md).

## Design principles

1. **PostgreSQL is the only authority.** No local metadata cache, byte cache,
   namespace registry, or file-ID allocator may be authoritative. A pod can die
   at any point; PostgreSQL locks, leases, catalog rows, and sequences decide.
2. **Safety before availability during role changes.** If a gateway cannot
   prove it is talking to a writable primary for a mutation, it should fail the
   request rather than risk duplicate work or stale state.
3. **Bounded work per request and per maintenance pass.** No query or reaper
   pass may grow with total bucket size.
4. **Atomic publish.** A client either sees the old object or the new one,
   never a mixture.
5. **Explicit rejection over silent approximation.** If pgvs3 cannot honor a
   protocol guarantee, it should say so.
6. **The network is the security boundary.** Do not build half an auth system
   into the data path.
