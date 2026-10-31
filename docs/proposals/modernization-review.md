# Capability-by-capability modernization review

> Historical analysis, not the active backlog. Priorities and status labels below
> describe the reviewed snapshots and may be superseded. Use the
> [current architecture](../design.md), [ranked roadmap](../roadmap.md) and
> [implementation evidence](../modernization-results.md).

Reviewed **2026-10-28**, against commit **a879472**, layout **v23**.

Source paths in this historical review refer to that snapshot. Related files
were subsequently consolidated; see the current file map in
[CONTRIBUTING.md](https://github.com/adonm/pgvs3/blob/main/CONTRIBUTING.md#package-map).

The capability map below records the pre-implementation review. The subsequent
implementation uses layout **v24** (the current baseline is **v25**): shared range parent metadata, one-round-trip
cache shadow resolution, renewable owner-fenced fills, bounded shared spool bytes,
and PostgreSQL 18 publication SQL. The S3 client experiments are isolated in the
`test/s3clients` module (`just s3-clients`); the existing fork remains the production client because the
initial SDK metadata/small-object comparisons exceeded the 20% gate. No S3 server,
job engine, RESP transport or cache-namespace replacement was made.

## Conclusion

**Keep the PostgreSQL-centered architecture and its specialized streaming paths.
Modernize the surrounding coordination and adapters before replacing whole
subsystems.** The best near-term opportunities are fewer origin round trips for
range reads, byte-aware resource admission, better fill leases, PostgreSQL 18
publication SQL, and retiring the owned S3 client fork if an SDK prototype passes.

There is no demonstrated drop-in library that replaces all of pgvs3's S3,
durable shared cache, RESP, jobs, and streams while retaining its operating model.
Several alternatives are good products but simplify a different problem.

This is a source-backed design review, not a replacement benchmark. It covers
the credible implementation families and named candidates below, not every
project on the internet. Expected performance and code reduction are hypotheses
unless explicitly identified as existing measurements.

## Guardrails and evaluation

- Optimize for **performance, then compatibility, then correctness**, while
  retaining the project's durability and multi-worker safety requirements.
- The main workload is **10 MiB–1 GiB Parquet objects**. Accept no more than a
  **20% regression on an individual relevant path**; do not hide it in averages.
- Keep acknowledged write-back bodies WAL-logged. No UNLOGGED substitution.
- Keep shared fills/cache visibility across gateways and read-only replicas;
  no authoritative gateway-local object or metadata cache.
- Keep one primary plus standbys, at most 100 namespaces, one port, and the
  permanent trusted-network/no-auth model. Extensions must not become an
  unverified requirement for managed PostgreSQL.
- Fresh databases and version-pinned layouts are acceptable. A new layout does
  not need a migration framework or compatibility decoder.
- Judge simplicity by **owned behavior, adapters, failure modes, dependencies,
  and operational burden**, not only deleted Go lines.

Decision labels:

| Label | Meaning |
| --- | --- |
| **Keep** | No convincing simpler equivalent under the guardrails. |
| **Simplify** | A bounded change worth planning next; still requires tests and the performance gate. |
| **Prototype** | Plausible benefit, but compatibility or performance fit is unproven. |
| **Scope change** | Useful alternative only if a product requirement is relaxed. |

## Complete capability map

| Capability | Modern/simpler avenues reviewed | Recommendation |
| --- | --- | --- |
| Durable PostgreSQL object bytes | One TOASTed `bytea`, larger TOASTed segments, PostgreSQL large objects, external object storage | **Keep** page-sized chunks. A single datum cannot cover a full 1 GiB payload; large objects move complexity into lifecycle/backup. External storage changes the premise. [PG storage](#postgresql-storage-and-publication) |
| Streaming ingest / atomic publish | High-level pgx `CopyFrom`, existing low-level binary COPY, row inserts/batches | **Keep** binary COPY as the default. Prototype high-level COPY only if it removes meaningful framing code within the 20% gate. [COPY](#streaming-and-database-access) |
| Streaming GET, ranges, multipart reads | Ordered binary rows, `CopyTo`, serial reads, exported-snapshot parallel reads | **Keep** bounded parallel pieces and snapshot consistency. A COPY export is not a raw HTTP body and does not eliminate block decoding. [Streaming](#streaming-and-database-access) |
| Inline values, compression, integrity | TOAST compression, existing Zstd frames/raw fallback, disabling redundant compression for selected content | **Keep** the current format. Benchmark any compression fast-path separately on Parquet and compressible data; do not skip requested checksum verification. [Storage](#postgresql-storage-and-publication) |
| Conditional writes / metadata publication | PostgreSQL 18 `RETURNING old/new`, shared mutation SQL, generated fixed-schema queries | **Simplify** duplicated publication/old-file bookkeeping; preserve missing-key locking, TTL, CAS, checksum metadata, and GC. [Publication](#postgresql-storage-and-publication) |
| S3 server API, listings, conditions, XML | VersityGW embedding, GoFakeS3 backend, external S3 frontend | **Keep** current server pending a narrow frontend spike. Neither inspected embedding/backend API is a proven drop-in. [S3 frontend](#s3-and-http-frontends) |
| Multipart uploads / part checksums | Library multipart managers, persistent backend interfaces, current PostgreSQL upload tables | **Keep** persistent server-side state. Origin SDK upload managers do not replace the gateway's multipart server. [Multipart](#s3-and-http-frontends) |
| Same-bucket zero-copy / cross-bucket copy | Shared immutable IDs, SQL `INSERT ... SELECT`, generic stream-copy helpers | **Keep** specialized server-side copies; a generic reader/writer copy is a regression in architecture. [Copy and clone](#namespaces-copy-clone-and-garbage-collection) |
| Namespace clone / dump / restore | Current schema clone, physical storage snapshots, large-object backing, globally shared blobs | **Keep** schema-local ownership. Review clone/adoption sequence adjustment separately; physical snapshots are cluster/volume-level, not bucket clones. [Clone](#namespaces-copy-clone-and-garbage-collection) |
| Namespace isolation / fast drop / GC | Shared tables, declarative partitioning, pg_partman/pg_cron, current schema-local tables and bounded leader | **Keep** for the 100-namespace envelope. More partitioning adds DDL and GC coordination without an established benefit here. [Namespaces](#namespaces-copy-clone-and-garbage-collection) |
| S3 origin client | AWS SDK Go v2, MinIO Go SDK, Go CDK blob, existing owned fork | **Prototype** AWS SDK first, MinIO SDK second. Retire the fork only after origin contracts and binary/resource costs are measured. [Origin adapters](#origin-adapters) |
| HTTP and filesystem origins | Standard `net/http`, Go CDK fileblob, generic blob abstraction | **Keep** the small HTTP driver; it has different range/list/write semantics. Local providers remain development tools unless their metadata persistence is redesigned. [Origin adapters](#origin-adapters) |
| Shared read-through / range cache | Shared parent metadata, explicit immutable prefixes, NGINX slices, DuckDB client cache, local caches | **Simplify** range-hit metadata resolution in PostgreSQL. Other caches are complementary or scope-changing, not equivalent shared authority. [Read caching](#shared-read-and-range-caching) |
| Cross-gateway fill coalescing | Renewed/token-fenced database leases, size-aware wait, session locks, local singleflight | **Simplify** leases and waiting. Keep local singleflight as an optimization, not cross-process authority. [Fill leases](#fill-coordination-and-write-back) |
| Durable write-through / write-back / merged listing | Transactional outbox, generic job library, CDC worker, direct-to-origin writes | **Keep** the transactional body/outbox and remote ordering protection. Prototype worker changes only after proving stale uploads cannot win. [Write-back](#fill-coordination-and-write-back) |
| Cache states, stable public ETags, flush guards | Explicit state/source/generation columns; merge `s3c_` and `s3w_` | **Prototype** explicit state columns. **Do not merge by default**: persistence, shadowing, ETags, quotas, RESP, and flush semantics differ. [Cache model](#cache-state-model) |
| RESP2 / TTL / mutation / pipelining | Redcon, RESP3, gnet, external Valkey, shared SQL mutation helpers | **Keep** Redcon/RESP2. Review oversized MSET semantics and batching before transport replacement. [RESP](#resp) |
| OJS HTTP binding / JSON envelopes | Standard `ServeMux`, JSON v2 APIs, official Go SDK, reference backend | **Simplify** only bounded routing/envelope boilerplate. SDK is a client; reference backend is not a small server binding. [OJS](#ojs-jobs-and-streams) |
| Job lifecycle / priorities / retries / leases | Current atomic `SKIP LOCKED` SQL, River, PGMQ, reference OJS backend | **Keep** the recently consolidated claim path. Replacing it entails semantic adapters and possibly a different product scope. [Jobs](#ojs-jobs-and-streams) |
| Stream replay / fan-out / retry / DLQ / retention | PgQue library/backend, PGMQ topics, external log, current snapshot batches and rotation | **Keep** the segmented model; PgQue is the closest architectural reference, not automatically the same API. [Streams](#ojs-jobs-and-streams) |
| Long polling / wakeups | Polling, debounced `LISTEN/NOTIFY`, a new notification service | **Prototype** database wakeup hints only if idle polling is material; retain polling fallback. [Wakeups](#ojs-jobs-and-streams) |
| SQL typing / connections / coordination | pgx helpers, sqlc, ORM, PgBouncer transaction pooling | **Keep** pgx. Selective sqlc is plausible for `s3p`; dynamic namespace SQL and session advisory locks limit broader substitution. [Database access](#streaming-and-database-access) |
| Quotas / load shedding / scratch admission | Existing sampled guardrails, weighted semaphore, database reservation ledger | **Simplify** byte admission now; hard cluster quota is a separate, potentially contended design. [Resources](#quotas-and-resource-admission) |
| DuckDB / DuckLake integration | DuckLake inlining/compaction, DuckDB file cache, pg_ducklake, direct cloud storage | **Keep** S3 interoperability; use DuckLake's existing small-change inlining rather than adding a staging engine. [Analytics](#duckdb-and-ducklake) |
| Metrics / tracing / logging / profiling | `otelhttp`, `otelpgx`, Prometheus collectors, standard `slog` and pprof | **Prototype** standard boundary instrumentation; keep no-export operation cheap and existing metric names stable. [Observability](#observability) |
| Read replicas / failover / readiness | pgx target-session attributes, managed endpoints, established HA operator | **Simplify** role diagnostics and durability claims. Do not implement another HA controller inside pgvs3. [HA](#ha-backups-and-deployment) |
| Backups / PITR / origin recovery | Managed PITR, pgBackRest, CloudNativePG backup plugin, logical namespace exports | **Keep** PostgreSQL-native physical recovery; improve restore drills and external-origin reconciliation. [Recovery](#ha-backups-and-deployment) |
| Container / Helm / drain / network boundary | Current ko/distroless/Deployment, fixed release digests, scratch sizing, external DB operator | **Keep** the stack; align storage, pool, and rollout budgets. No authentication expansion. [Deployment](#ha-backups-and-deployment) |
| CLI / environment / layout initialization | Standard `flag`, declarative Kong CLI, generated option documentation, migrations | **Keep** current parser unless a Kong spike clearly reduces total code. Keep the pinned baseline, not a migration framework. [Tooling](#cli-toolchain-and-tests) |
| Unit / concurrency / SQL / conformance / interoperability tests | `testing/synctest`, fake-network httptest, existing testcontainers and pinned s3t, differential tests | **Simplify** timer tests with standard Go tools; retain real database/client tests and one harness per layer. [Tests](#cli-toolchain-and-tests) |

## PostgreSQL storage and publication

### What is already appropriate

The current 8,120-byte blocks, inline small values, raw-or-Zstd encoding,
immutable file IDs, and transactional metadata publish are specialized, but
they provide range addressing, no-TOAST chunk rows, crash-safe visibility,
zero-copy references, and bounded streaming together. Removing the block code
does not necessarily remove those responsibilities.

PostgreSQL 18 AIO covers sequential scans, bitmap heap scans, and vacuum. The
separate bitmap-oriented chunk pool already targets that capability; simply
upgrading PostgreSQL or switching drivers is not a new optimization. [PG18]

### Alternatives and their actual limits

1. **One `bytea` per object:** much smaller application data model, but the
   TOAST datum limit is `2^30 - 1` bytes, below a full 1 GiB payload before any
   further practical constraints. `STORAGE EXTERNAL` can make uncompressed
   substring access efficient; that does not solve the maximum object size or
   all ingest/memory costs. Not a full-range replacement. [TOAST]
2. **Larger TOASTed segments:** retain a segment layer and range arithmetic,
   exchange current page behavior for TOAST behavior, and need new WAL, storage,
   read-amplification, and memory measurements. This is a storage experiment,
   not a recommended change to the current chunk invariant. [TOAST]
3. **Large objects:** genuine seek/stream support, but descriptors are
   transaction-scoped and bytes have a separate global lifecycle. Namespace
   drop, object copies, reference tracking, orphan cleanup, permissions, and
   schema-selective exports become harder. A shorter upload loop is not enough
   to establish a simpler system. [LO]
4. **S3/SeaweedFS/Ceph plus PostgreSQL metadata:** often the better choice for
   much larger data sets or independent storage scaling. It introduces another
   authoritative storage/recovery system and changes pgvs3's central premise.
   It is an architectural alternative, not an internal refactor. [SeaweedFS]

### Best SQL simplification

`internal/store/schema.sql` contains parallel `put_inline` and `publish_object`
paths with repeated old-row selection, preconditions, metadata updates, and
garbage handling. PostgreSQL 18 can return both **old and new values**, including
the conflicting old row on `INSERT ... ON CONFLICT DO UPDATE`. [RETURNING]

Prototype one publication primitive with small inline/chunked entry points:

- Return overwritten `file_id`/`parts` from the mutation rather than separately
  reading them solely for garbage bookkeeping.
- Share metadata projection and condition evaluation across put/copy/cache
  mutations where the semantics actually match.
- Keep the key lock or another demonstrated serialization mechanism for absent
  keys, origin publication fences, and multi-table work. `RETURNING` does not
  replace locking or make every conditional insert trivial.
- Preserve TTL/expired-row semantics, stable ETags, multipart checksum cleanup,
  old-file references, pending-outbox atomicity, and return outcomes.

This is a strong code-reduction candidate because PostgreSQL 18 is already the
minimum, so it does not introduce a new service or library. The reduction and
speedup have not been measured.

Keep existing integrity algorithms. WAL compression is not a replacement for
object compression: `wal_compression` compresses **full-page images**, not every
arbitrary Parquet payload written into WAL. [WAL]

## Streaming and database access

The project already uses standard low-level
`PgConn.CopyFrom(ctx, reader, sql)` in `internal/store/ingest.go`. Do not describe
the entire PostgreSQL protocol implementation as custom: the owned part is
primarily binary row framing, block processing, and lifecycle orchestration.

- **High-level `pgx.Conn.CopyFrom`:** could remove framing using a streaming
  `CopyFromSource`. Compare allocations and throughput, not only source length.
  Retain compression/checksum processing and close/unblock readers on errors.
- **`PgConn.CopyTo`:** writes COPY output to an `io.Writer`, but binary COPY
  still has headers/rows/fields. Block decompression, ordering, clipping, errors,
  and exported-snapshot lifecycle remain. It is not zero-framing raw download.
- **Current GET:** binary `pgx.Rows.RawValues()` already avoids ordinary scan
  decoding. Preserve cut-through reads, bounded out-of-order buffers, client
  cancellation, and consistent parallel-piece snapshots.
- **Cancellation:** pgconn documents that COPY cancellation interrupts database
  network operations but may not unblock an arbitrary input reader. A library
  substitution must retain pipe/body closure and abort tests. [PGX]

For ordinary SQL, use existing shared scanners and pgx collection helpers before
adding an ORM. sqlc generates pgx/v5-compatible typed code, but its static schema
model is a better fit for fixed `s3p` tables than dynamic per-namespace relation
names. A small pilot is reasonable; manufacturing a template/generation layer
for every schema may add more complexity than it removes. Pin a sqlc release
that understands the chosen PostgreSQL syntax before adopting it. [SQLC]

**Do not put all runtime traffic behind transaction-pooled PgBouncer.** Current
maintenance and origin-key coordination use session advisory locks. PgBouncer
also lists `LISTEN` and session settings as transaction-pooling incompatibilities.
Moving `SET`s to database defaults does not fix the locks. Direct or session-pooled
connections remain the straightforward supported path; splitting pools by
capability is possible but adds another operational boundary. [PgBouncer]

## S3 and HTTP frontends

### Replacing the S3 server

The owned S3 frontend is roughly 4,000 production Go lines at the review commit.
It is a reasonable library-replacement target, but its backend contract is not
just CRUD: it includes streaming/cancellation, conditional mutation, persistent
multipart, ranges, checksum metadata, shared cache behavior, and SQL-native copies.

| Candidate | Why consider it | Why it is not yet a replacement |
| --- | --- | --- |
| **VersityGW `embedgw`** | Apache-2.0 gateway, broad S3 surface, backend interface and unsupported-operation defaults | The inspected v1.10.0 embedding runner owns listener ports, requires root credentials, uses Fiber/fasthttp, and documents one gateway per process. A handler/listener seam compatible with the existing no-auth, one-port gateway must be proved. Broader API coverage also needs explicit rejection of pgvs3 non-goals. [Versity] |
| **GoFakeS3** | Embeddable Go HTTP S3 implementation with custom backend and conditional-operation work | Its backend is explicitly unstable and lacks `context.Context`; multipart defaults to process-local state unless a persistent multipart backend is supplied. Generic fallback copies can move bytes through Go. Existing tests do not prove equivalent cancellation, atomic conditions, checksums, or zero-copy. [GoFakeS3] |
| **External S3 gateway/store** | Delegate compatibility maintenance and deployment to a dedicated product | Extra process/storage/backup or integration boundary; auth and routing behavior may differ. Only simpler if those product constraints are relaxed. |

**Verdict:** keep the current S3 handlers for now. A time-boxed VersityGW
frontend spike is reasonable only after the smaller origin-client experiment.
Do not spend weeks adapting a larger server merely to delete the current router.

### Multipart, conditions, and static HTTP

- Preserve multipart persistence across gateway loss, checksum enforcement,
  concurrent complete/abort/overwrite ordering, and part-copy optimization.
  An SDK's upload manager is an **origin client**, not a server implementation.
- Current AWS documentation includes conditional `PutObject`,
  `CompleteMultipartUpload`, and `CopyObject`. A multipart conflict can require
  starting a new upload and re-uploading parts; retrying completion blindly is
  not always enough. [S3 conditions]
- Multipart **FULL_OBJECT** checksums remain explicitly out of scope here.
  Newer AWS support is not authorization to expand pgvs3's compatibility surface.
- Keep raw S3 key handling separate from ordinary web routing. Standard
  `ServeMux` path cleaning, redirects, wildcard decoding, automatic 405s, and
  GET-to-HEAD matching require compatibility review. Its method/path patterns
  are more attractive for `/ojs` than opaque object keys. [ServeMux]
- `http.ServeContent` can help a seekable static-file service, but adapting a
  PostgreSQL streaming body into a seekable file abstraction is not necessarily
  simpler than the current range/condition path. Keep index resolution and S3
  error behavior explicit.
- The current first-byte HTTP/RESP mux is small. `cmux` is a genuine generic
  alternative, but does not remove connection admission, sniff deadlines,
  shutdown, or protocol-drain responsibilities. Keep it unless a measured
  replacement demonstrably reduces those responsibilities. [cmux]

## Origin adapters

The `internal/s3client` fork is roughly 1,850 production Go lines
of owned client behavior. **This is the strongest dependency-substitution
candidate**, because pgvs3 needs a client adapter rather than a replacement server.

| Option | Assessment |
| --- | --- |
| **AWS SDK Go v2** | First prototype. Generated, current S3 request/response models and explicit conditional fields reduce owned protocol/signing/retry behavior. Measure binary size, dependency growth, buffering, request count, checksum behavior, and first-byte latency. [AWS SDK] |
| **MinIO Go SDK v7** | Credible second candidate with straightforward S3-compatible APIs; the SDK is Apache-2.0. Verify low-level multipart completion preconditions and metadata/checksum fidelity instead of assuming high-level `PutObject` covers them. Do not conflate SDK licensing with server-product licensing. [MinIO SDK] |
| **Go CDK `blob`** | Useful if adding multiple cloud providers becomes a goal. The inspected API includes `IfNotExist` and driver escape hatches, so it is not accurate to claim it has no conditional writes. However, ETag-match writes/completion, checksums, and S3-specific response details still need capability mapping. HTTP origins are not ordinary writable blob buckets. [Go CDK] |
| **Keep the fork** | Smallest immediate dependency/build change, but every new S3 compatibility issue remains owned work. Keep only if SDK adapter/behavior costs outweigh retirement of the fork. |

Prototype acceptance checklist:

1. Path-style custom endpoints, signing/credentials, escaped keys, range and
   conditional reads, correct origin not-found/precondition classification.
2. `If-Match` / `If-None-Match` upload and multipart completion, copy conditions,
   retry/re-upload behavior, cancellation, and operation timeout ownership.
3. Seekable spooled bodies; no hidden whole-object memory upload/download.
4. Exact Content-Type, Content-Encoding, user metadata, ETag, Last-Modified,
   object/part checksum round-tripping required by the current server.
5. Known checksum defaults: AWS SDK S3 v1.74.1 introduced automatic CRC32 upload
   checksums; download validation is also configurable. Decide intentionally
   rather than accepting a wire/performance change accidentally. [AWS SDK]
6. Error-injected and cross-gateway write-back tests, then the benchmark matrix
   below. A self-hosted origin is useful but not sufficient proof of real AWS
   interoperability.

Keep the existing small `net/http` origin implementation unless a replacement
preserves validators, range handling, redirects, and its read-only/no-listing
contract. `filesystem`/`inmem` remain development origins; current filesystem
user metadata is process-local, not production-persistent metadata.

## Shared read and range caching

### Highest-value request-path opportunity

`internal/objcache/read.go` resolves parent object metadata before looking up
aligned range blocks. When only range blocks are cached and the parent body
row is absent, it calls `origin.Attributes` again. Thus an apparently warm
range workload can still pay an origin HEAD RTT.

**Recommendation:** a small **logged PostgreSQL parent-metadata record**, shared
by all gateways, containing size, validator/generation, response metadata, and
validation time. Resolve ranges against it and bind every range block to that
generation. Preserve TTL revalidation, tombstones, write-back shadowing, and
stale-fill fences. Read-only workers must be able to use the replicated record
without trying to renew or mutate it.

This potentially removes a network round trip on the workload that matters most,
rather than optimizing a few Go allocations. It adds state and therefore needs
a clear benefit in request counts and latency. It is not an in-process metadata
cache.

An **explicit immutable-prefix mode** is another avenue for immutable DuckLake
data files: reduce repeated validation and simplify state transitions for those
keys. Do not silently infer immutability from `.parquet`, and do not apply it to
arbitrary mutable objects or delete/recreate keys.

### Other cache architectures

- **NGINX slice caching:** well-established ranged HTTP caching, but introduces
  another cache/process and does not provide PostgreSQL-replicated cache hits,
  transactional write-back, or shared RESP data. Useful for a read-only edge
  variant, not an equivalent internal replacement. Its documentation also warns
  about slice/background-update interaction. [NGINX]
- **DuckDB's external file cache:** already caches remote bytes in memory and is
  enabled by default in the inspected DuckDB 2.0 configuration. It complements
  pgvs3 but is local to the client, not a cross-gateway cache. Benchmark with
  client caching both enabled and disabled to avoid attributing client hits to
  the server. Do not use old `enable_object_cache` advice: that pragma is a
  deprecated no-op in DuckDB 2.0. [DuckDB cache]
- **Local LRU/Ristretto/filesystem cache:** could be a valid different design,
  but violates the current zero-local-object/metadata-cache invariant and does
  not make shared PostgreSQL state unnecessary.
- **UNLOGGED read fills:** excluded by the requested design and replica-sharing
  requirement; not a proposed optimization.

## Fill coordination and write-back

### Improve the existing fill lease

At this commit, `internal/objcache/tier.go` sets a **2-minute fill lease** and
**45-second waiter budget**; a whole-object fill may run for **10 minutes**.
`internal/store/filllease.go` has acquire/release/cleanup but no renewal path.
Slow origins can therefore outlive ownership, and waiters can fall back to
duplicate origin traffic before the owner finishes.

Plan one cohesive improvement:

- Renew using the database clock and owner token while work is alive.
- Fence final publication by the live lease owner/generation **as well as**
  the expected origin version. Release ownership conditionally.
- Cancel/discard a fill on ownership loss; do not let renewal become local
  authority or a reason to hold a DB connection during download.
- Make waiting/fallback policy explicit and size-aware. More waiting reduces
  duplicate traffic but can worsen tail latency; measure both.
- Test a fill exceeding 2 minutes, owner crash, cancelled waiter, lost renewal,
  overwrite/delete/recreate, and several gateways requesting the same range.

Local `singleflight` remains useful but cannot coordinate separate gateways.
Session advisory locks during the entire download would be shorter to write but
consume a database session per slow origin request, undoing current isolation.

### Keep durable outbox ownership

`internal/objcache/writeback.go` detaches the database body into a spool before
uploading, but **retains the session key-lock connection** through the origin
upload. That is intentional ordering protection, not merely an accidental
connection leak.

**Do not replace it with a token check after upload and call it safe.** A token
can reject stale local publication after an old upload has already overwritten
the remote object. A lock-free alternative needs demonstrated remote ordering:
conditional origin mutations with the correct base version, or immutable
generation objects plus a conditional authoritative pointer. The latter changes
origin key/layout semantics and is not a transparent refactor.

River/PGMQ/generic workers can supply claim/retry machinery, but do not remove
the atomic body-plus-outbox commit, supersession, public ETag, merged listings,
delete coordination, pending-row flush guards, or remote side-effect ordering.
CDC/Debezium would add a service and delivery semantics, not simplify this bounded
in-process worker by default.

Retain logged bodies until they are safely committed at the origin under the
existing policy. Scratch files are disposable; acknowledged pending bodies are
not. Processing and remote side effects must be treated as **at least once**,
with idempotency/preconditions where required, not end-to-end exactly once.

## Cache state model

`origin_etag` currently carries both origin versions and lifecycle sentinels;
the shared parser improves organization, but SQL and Go still need to interpret
the same encoded state.

A future layout could use explicit `state`, `source`, `generation`,
`origin_version`, and `validated_at` fields with constraints and typed accessors.
This is a maintainability experiment, not a claim that more columns are faster.
First design the transition table and the indexes required for workers/expiry.

Keep `s3c_` and `s3w_` separate until there is a complete equivalence proof for:

- durable pending-body ownership and outbox references;
- stable gateway ETags versus changing origin validators;
- range rows, parent metadata, deletes, and read shadowing;
- RESP mutation/flush behavior, cache eviction, quotas, and namespace drop;
- logged replica reads and restored data.

One tagged table may eventually simplify lookups, but a tagged table plus many
exceptional retention/flush paths can be more complex than two namespaces.

## RESP

Redcon is a compact server dependency, not an obvious source of excess owned
code. RESP3 has a different type/handshake surface and is a compatibility
expansion, not a necessary modernization. gnet is an event-loop transport;
PostgreSQL's blocking work still needs dispatch/backpressure and all command
semantics remain owned. External Valkey changes the one-database authority and
backup model. [RESP sources]

Better avenues within scope:

1. Measure SET batching independently: current README records that 4 KiB
   redcon read batches can fragment the old pipelined write coalescing.
   Tune/prototype batching only against latency and throughput together.
2. Keep SQL-native atomic operations, TTL, numeric mutation, NX/XX, GETDEL,
   APPEND/SETRANGE, and shared-cache visibility; do not replace them with
   read/modify/write in Go.
3. `internal/store/cache_mset.go` falls back to sequential single-key writes
   when any value exceeds the inline ceiling. That path is not atomic across
   keys. Review a staged-body/one-publication-transaction implementation, or an
   explicit documented/rejected unsupported case; do not imply full Redis MSET
   semantics for the existing fallback.
4. Cross-namespace `FLUSHALL` is also sequential. State that boundary explicitly
   rather than adding a broad transaction/locking framework by default.
5. Continue explicitly rejecting `MULTI`/`EXEC`/`WATCH`; a pipeline is not a
   transaction. Keep `HELLO 3` rejection and RESP2 client instructions unless
   RESP3 becomes an explicit requirement.

## OJS, jobs, and streams

### HTTP and JSON

The official OJS Go SDK is a **client/worker SDK**, not a server binding. The
reference PostgreSQL backend is a broader server with features deliberately
excluded from pgvs3. Its inspected module also uses a sibling-directory module
replacement; packaging/release maturity needs verification before embedding.
Its actual LICENSE is Apache-2.0, despite an inconsistent changelog statement.
Validate lease-token fencing against any upstream implementation. [OJS sources]

For the current bounded API:

- Standard `http.ServeMux` method/wildcard routes may remove dispatch plumbing
  in `internal/ojs`. Preserve exact error envelopes, HEAD behavior, trailing
  slash handling, methods, and `/ojs/manifest` routing. [ServeMux]
- Go **1.27 already backs `encoding/json` with the v2 implementation**. Changing
  the import is not an automatic speed upgrade. Stable v2 offers
  `MarshalWrite`/`UnmarshalRead` and stricter defaults, but duplicate-name,
  invalid-UTF-8, nil collection, case matching, and omission behavior can change
  the wire contract. Use golden fixtures and compatible options where needed.
  Do not rely on old experimental `format` tags: the released API removed them.
  [Go27] [JSON migration]
- Broad OpenAPI/server generation is only attractive if an authoritative spec
  exactly captures pgvs3's implemented subset and token extension. Generating a
  larger server and then maintaining exceptions is not an established saving.

### Job engine alternatives

| Candidate | Fit |
| --- | --- |
| **Existing atomic claim SQL** | Already consolidated to candidate/update/return with `SKIP LOCKED`, including due/expired eligibility. Highest confidence for retaining current priorities, leases, retention, and namespace behavior. |
| **River** | Strong PostgreSQL job framework and a sensible choice for applications wanting its worker ecosystem. An OJS server would still need lifecycle/state/token/namespace adapters; some advanced features are Pro. Do not replace working claim SQL solely to recreate the current server on top. [River] |
| **PGMQ** | Lightweight visibility-timeout queue, with SQL-only installation as well as an extension. Managed hosting is therefore not categorically disqualifying, but OJS states, priority/retry policy, token fencing, stream batches, and namespace lifecycle remain to be mapped. A visibility window is not end-to-end exactly-once processing. [PGMQ] |
| **Reference OJS PostgreSQL backend** | Closest wire-family alternative, but much broader scope, deployment surface, and dependencies. Most relevant if full OJS becomes a goal, not as a proven small dependency replacement. [OJS sources] |

Do not unify jobs and streams into one delivery algorithm just because both
carry JSON. Mutable competing-worker jobs and replayable fan-out streams have
different row ownership, retention, and acknowledgment semantics.

### Streams and wakeups

PgQue's snapshot batches and rotating log are directly relevant to the existing
three-segment stream model. Replacing current code with an upstream package
still requires checking member cooperation, batch leases/acks, nack retries,
DLQ, payload/quota limits, schema ownership, and managed-PostgreSQL operation.
Keep the design; use upstream as a comparison and potential future extraction
target rather than assuming method names establish equivalence. [PgQue]

Consider **debounced `LISTEN/NOTIFY` as a lossy wakeup hint** for job/stream
long polls and outbox workers only if polling load is significant. One dedicated
session can wake eligible local waiters; reconnect and periodic polling must
recover missed notifications. Notifications must not become queue authority or
cache invalidation correctness, and one notification per item can introduce
global-lock/contention costs. River documents batching/throttling concerns.
This is not a default replacement for the already-small polling path. [Notify]

## Namespaces, copy, clone, and garbage collection

Keep schema-per-namespace while the supported limit is 100. It preserves a
clear physical/drop/export boundary. A single shared table makes SQL generation
easier but complicates fast bucket disk reclamation and isolation; partitioning
can restore some benefits while adding DDL and maintenance. No evidence in this
review justifies that trade yet.

Keep SQL-native same-bucket shared-file copies and cross-bucket
`INSERT ... SELECT`. Global content-addressed bytes/dedup could save storage,
but require cross-namespace reference tracking and undermine independent drop,
clone, and schema exports. Generic S3 copy helpers must not silently replace
these paths with gateway byte streaming.

Keep bounded GC and the existing advisory-lock leader. `pg_cron` or pg_partman
would add installation/version requirements and would not eliminate reference
safety, snapshot lifetime, or cooperative stream retention.

**Sequence review item:** `clone_namespace` copies already-issued IDs, then
calls `setval(GREATEST(last_value, max_id))`; adoption has a related pattern.
The read/max/set sequence is not itself a concurrency-safe monotonic update
relative to concurrent allocation. PostgreSQL documents that `setval` changes
are immediately visible and not rolled back. Audit with a deterministic
clone/adopt-versus-allocation test before changing it; do not claim a reproduced
collision from source inspection alone. An ordinary in-database clone should
not need to readjust an already-ahead sequence; imported schema adoption needs
its own safe allocation/reconciliation design. [Sequences]

The existing Go block of 64 file IDs is a cache of values **already allocated
by PostgreSQL**, not a locally authoritative ID generator. Replacing it with
UUIDs merely because Go 1.27/PostgreSQL 18 supply new UUID facilities would add
key/index costs without an established need.

## Quotas and resource admission

### Byte-aware scratch admission: do next

The chart provides **10 GiB** of scratch. Default fill and pending-worker
budgets can each allow eight spools; sixteen 1 GiB bodies already exceed that,
before foreground write spools, other temporary usage, and logs. Count-only
limits do not bound bytes. `GOMEMLIMIT` does not bound scratch storage or all
process/cgroup memory.

Use one process-wide scratch-byte budget across fills, pending uploads, and
foreground spooling, in addition to operation counts. `x/sync/semaphore.Weighted`
is already available through the existing dependency. [Weighted] [K8s resources]

- Reserve known sizes before creating large spools; reject or use an explicit
  pass-through policy when a request cannot fit.
- Define a hard bound for unknown-length inputs. Naively blocking all partial
  spools while each waits for more bytes can deadlock the budget; reserve a
  bounded maximum or use nonblocking growth with a clean abort/fallback policy.
- Release on every error/cancel/close path and account for simultaneous spool
  copies rather than just logical object size.
- Align chart `ephemeral-storage` requests/limits, `emptyDir.sizeLimit`, spool
  configuration, and metrics. `emptyDir.sizeLimit` is not a scheduler reservation.
- Preserve disk, not tmpfs, for large scratch bodies. A scratch file is not a
  reason to move acknowledged bodies out of PostgreSQL.

### Namespace and database quota

Current quota and `--max-db-bytes` checks are documented sampled, per-worker
guardrails. They are not hard cross-worker storage quotas. PostgreSQL roles and
tablespaces alone do not create a general per-namespace byte-quota mechanism;
the operational wording should not suggest that they do.

Audit unknown-length multipart admission and overwrite accounting separately.
A hard logical quota would need transactional reservations, commit/release,
crash cleanup, and unknown-size policy; a single shared counter can become a
write hotspot. Sharded reservation blocks trade exactness for throughput. Do
not add that machinery without a hard-quota requirement. Physical disk/WAL
headroom still needs monitoring regardless of logical reservations.

## DuckDB and DuckLake

DuckLake **1.1**, released 2026-10-21 with DuckDB 2.0, improves catalog/pruning
and stores row-group counts in metadata. Small insertion/deletion inlining
already places changes directly in the PostgreSQL catalog instead of creating
a Parquet file for every change. Prefer configuring existing inlining and
compaction over adding another pgvs3 small-file staging layer. [DuckLake]

Pin exact client/catalog versions in compatibility and performance runs.
Existing DuckLake catalogs do not automatically become 1.1. The inspected
stable inlining page still conflicts with the 1.1 release description on
PostgreSQL VARIANT support; use versioned release behavior and a contract test,
not an unqualified support claim.

`pg_ducklake` is a potentially simpler analytics deployment for people wanting
SQL analytics inside PostgreSQL. It is not an S3 server, RESP cache, or OJS
replacement, and its extension availability on Aurora has not been verified.
Keep it in the scope-changing alternatives list. Direct DuckLake-on-cloud-S3
is likewise simpler if storing all file bytes in PostgreSQL ceases to be a
requirement. [pg_ducklake]

## Observability

`internal/telemetry/telemetry.go` owns HTTP tracing, a ResponseWriter wrapper,
and basic query spans; S3/OJS/RESP/store metrics have separate exposition code.

- **`otelhttp`** can own standard server/client spans and response-body lifetime
  handling. Preserve `X-Trace-Id`, filter probes/metrics if desired, use bounded
  route names, and test streaming/Flush/ResponseController behavior. Disable
  per-read/write events for large bodies unless explicitly wanted. [OTel HTTP]
- **`otelpgx`** covers queries, batches, connect/acquire, prepare, high-level
  COPY, and pool statistics. Direct `PgConn` calls and custom streaming spans
  still need explicit coverage; a new tracer does not magically observe every
  low-level operation. Do not include query parameters or object keys in metric
  labels, and decide whether SQL text should be exported. [OTel pgx]
- **Prometheus client collectors** can replace manual text formatting while
  retaining current cheap atomic observations: use a custom collector at scrape
  time, not necessarily a dynamic `CounterVec` lookup on every request. Preserve
  metric names/labels and test cardinality plus histogram correctness. Native
  histograms or a full OTel metrics pipeline are optional scope, not prerequisites
  to removing duplicate exposition. [Prometheus]
- Keep standard **`slog` and pprof**. Go 1.27's goroutine-leak profile helps
  diagnose some unreachable blocked goroutines, but is not a complete leak or
  shutdown detector. [Go27]

The current tracing module is small enough that dependency/version cost might
outweigh deletion. Benchmark **export disabled**, sampled tracing, and export
enabled under collector failure. Do not accidentally turn disabled telemetry
into a hot-path allocation or network requirement.

## HA, backups, and deployment

### Correct the durability contract, not the trust model

The README says pending write-back bodies survive crash/failover without
qualifying replication policy. Logged storage and `synchronous_commit=on`
guarantee local WAL durability, but **do not by themselves guarantee lossless
promotion of an asynchronous standby**. Synchronous-standby configuration and
promotion policy determine remote durability; immediate visibility on arbitrary
standbys additionally depends on replay. Aurora uses a different storage/HA
model and needs service-specific validation. [WAL]

Clarify this operationally rather than silently enabling synchronous replication
and changing latency/availability. Benchmark the intended production durability
mode, not an async mode while claiming RPO=0. Keep all current data logged.

pgx already supports `target_session_attrs=read-write` and related host-role
validation. This is a useful optional writer-connection guard alongside managed
writer endpoints, plus role reporting in `doctor`/readiness. It does not replace
endpoint failover, retries, or fencing. Read-only gateways may intentionally
connect to the primary, so do not require a physical standby in that mode. [PGX]

### Recovery and operational simplicity

- Keep managed PITR or a standard tool such as **pgBackRest** for physical base
  backup/WAL retention. pgBackRest supports parallelism, standby backup, bundled
  files, and block-incremental repositories; choose using restore and archive
  throughput measurements, not a new backup implementation in pgvs3. [pgBackRest]
- If self-managing PostgreSQL on Kubernetes, an established operator such as
  **CloudNativePG** can own database HA and backup orchestration. Prefer its
  plugin-based Barman Cloud path for new configurations; old native backup
  examples are version-sensitive/deprecated. This is an operator choice, not
  another dependency in the gateway. [CNPG]
- A schema `pg_dump` is an export boundary, not a complete standalone system
  restore: restore into an initialized matching layout, reconcile global
  catalog/IDs, and verify functions/config/outbox dependencies. Large-object
  backing would weaken this simple boundary further.
- Whole-cluster PITR preserves PostgreSQL transactional state, but it cannot
  rewind external origin writes or worker side effects. Test restoration with
  pending uploads and reconcile remote versions before restarting retry workers.
- Keep backups/WAL archives outside the pgvs3 database being protected. Using
  the same gateway as its only backup object store creates a circular recovery
  dependency.
- Drill recovery of DuckLake catalog plus files, RESP TTL data, jobs, stream
  consumer positions, tombstones, and acknowledged pending bodies. A reachable
  database or a successful dump alone is not the acceptance test.

### Deployment

Keep **ko, static distroless images, Helm, stateless Deployments, network policy,
and external PostgreSQL**. Replacing this with a gateway operator would add an
API/controller without simplifying the current resource set. Pin release images
or digests for repeatable rollout instead of relying on `latest`.

Budget connections across main + chunk pools, maintenance/key-lock sessions,
other applications, HPA maximums, and rollout surge. The chart's three workers
at pool 64 can have a much larger database connection footprint than the
apparent `poolMax` value. HPA cannot make PostgreSQL capacity infinite.

Match termination grace to actual transfer/drain policy: the chart's 60 seconds
does not guarantee completion of requests whose HTTP deadlines permit 30 minutes.
Keep bounded cancellation/retry behavior rather than claiming arbitrary-size
transfers always finish on rollout.

Trusted private networking remains the security boundary. No proposal here adds
gateway auth, signature validation, or TLS termination. Preserve default-deny
ingress, loopback/private exposure, and existing external mesh options.

## CLI, toolchain, and tests

### CLI and layout lifecycle

The refactor already consolidated environment parsing. Keep standard `flag`
unless command growth makes declarative **Kong** schemas cheaper: Kong supports
subcommands, env/default tags, validation, and help generation. Compare a complete
small command group, including parsing precedence, global flag placement,
explicit false/zero values, exit codes, and existing scripts. Cobra/Viper would
be more machinery than demonstrated need here. [Kong]

A shared option definition used for help/config documentation may yield more
benefit than changing parsers. Go 1.27 generic methods can improve selected
helper ergonomics, but a broad generics rewrite is not a capability improvement.

Keep the **single version-pinned baseline** and drift doctor. Goose/Atlas-style
migration histories solve in-place upgrades, which are not currently required.
New third-party schemas must not accidentally introduce an unplanned migration
policy.

### Tests and performance evidence

Use stable **`testing/synctest`** for retry/backoff, timer, long-poll, cancellation,
and waiter unit tests. Go 1.27's fake-network `httptest.NewTestServer` can make
HTTP timing tests deterministic without real sockets. Real PostgreSQL/network
I/O is not durably blocked inside synctest; keep database and failover integration
tests. [Synctest] [Go27]

Keep one harness per layer as CONTRIBUTING requires:

- Unit/model/fuzz tests for state transitions, range/key/XML/JSON/RESP parsing.
- PostgreSQL integration for locks, MVCC, sequence allocation, quotas, leases,
  GC, and concurrent mutation; do not validate replacement SQL only against mocks.
- The existing **pinned Go s3t allow-list**, not a reintroduced Python Ceph suite.
- Existing real-client contract/interop tests for boto3, DuckDB/DuckLake,
  rclone/restic, OJS workers, and RESP2 clients.
- Differential/golden fixtures for adapter and wire-format changes, plus race,
  crash, shutdown, read-only replica, and origin-error cases.

PG19/Go experimental SIMD/event-loop rewrites are not default modernization
targets. Stable features already in the project's baseline offer better
near-term risk/reward.

## Prioritized roadmap

This is an implementation order **for approval**, not work already performed.

| Order | Change | Expected payoff | Risk / proof required |
| --- | --- | --- | --- |
| 1 | Shared parent metadata for range-only cache hits | Remove avoidable origin RTTs on Parquet range workloads | New shared state; prove TTL/generation/delete/write-back and read-only semantics; count origin HEADs |
| 2 | One byte-weighted scratch budget + matching chart resources | Predictable 1 GiB concurrency; avoid scratch exhaustion and cascading retries | Unknown-length policy, fairness, clean abort, no budget leaks/deadlock; measure loaded throughput |
| 3 | Renewable, owner-fenced fill leases and explicit wait policy | Reduce duplicate slow-origin traffic across gateways | Renewal/owner loss/crash tests; tail latency and duplicate-byte measurements |
| 4 | PostgreSQL 18 old/new publication SQL | Less duplicated state/GC/CAS code without a new dependency | Full absent-key/CAS/TTL/copy/multipart/outbox tests; new layout if structural changes |
| 5 | AWS SDK origin adapter versus existing fork | Retire substantial owned S3-client behavior | Conditional multipart, checksums, cancellation, buffering, errors, binary size; no claim of benefit before comparison |
| 6 | `synctest` timer tests, then bounded OJS routing/envelope cleanup | Less flaky timing and smaller protocol plumbing | Wire goldens and real integration retained; JSON v2 semantic changes opt-in |
| 7 | Standard tracing/Prometheus exposition pilots | Less instrumentation maintenance; better boundary coverage | Disabled/enabled overhead, metric compatibility, streaming, dependency growth |
| 8 | Explicit cache state columns; optional debounced wakeups | Clearer lifecycle model; possibly lower idle polling | Only after state table and workload evidence; do not merge namespaces or require NOTIFY |

**Small operational/documentation work can proceed independently:** qualify
failover durability, clarify PgBouncer restrictions, distinguish guardrails from
hard quotas, document oversized MSET behavior, and align connection/scratch/drain
budgets. Reproduce the clone/adopt sequence hazard before selecting its remedy.

**Not recommended now:** whole S3-server replacement, River/PGMQ rewrite,
job/stream algorithm unification, RESP3/gnet rewrite, one-bytea storage,
large-object storage, global dedup, a migration framework, another cache service,
or merging logged cache/write-back namespaces without a semantic proof.

## Required experiment gates

Use the current binary as the control and change one major variable at a time.
Record exact tool/library/database versions, hardware, origin bandwidth/latency,
concurrency, warm/cold state, durability settings, repetitions, and variance.

| Area | Required comparison |
| --- | --- |
| Large objects | 10 MiB, 64 MiB, 256 MiB, 1 GiB Parquet and incompressible fixtures; single-client and saturated GET/PUT |
| Ranges | Footer/tiny ranges, aligned 1 MiB/8 MiB spans, multipart boundaries; cold fill, warm body, warm range-only, post-TTL; origin HEAD/GET counts |
| Write-back | ACK latency **and** origin drain rate, pending oldest age/bytes, retry amplification, conditional completion, concurrent overwrite/delete |
| Small values | 4 KiB HEAD/GET/SET, pipeline depth 1/16, MGET/MSET inline and oversized, mutation/TTL races |
| Jobs/streams | Enqueue/claim/ack and receive/ack p50/p95/p99, idle poll query rate, contention/vacuum, fan-out and stalled consumer retention |
| Resources | CPU, allocations/RSS/cgroup memory, spool peak bytes, WAL bytes, DB pool wait/connections, origin requests/bytes, build/image size |
| Failure/replication | Owner expiry/crash, origin disconnect/timeout/conflict, mid-COPY cancel/restart, stale worker, primary promotion, replica lag, restored pending outbox |

For latency/time metrics require **candidate <= 1.20 × baseline** on each
relevant path. For throughput, define the gate explicitly rather than using an
inverse accidentally: no more than 20% throughput loss means
**candidate >= 0.80 × baseline**; also report elapsed-time ratios for fixed work.
Resource growth and correctness/compatibility requirements remain independent
acceptance conditions. Improvements must not merely shift cost from ACK time to
an unbounded background backlog.

Existing refactoring measurements met the prior 20% budget; they are not
evidence that any replacement above will meet it. At the time of the original
review no candidate had been implemented or benchmarked; see the implementation
status at the top for the subsequent work. Real AWS/Aurora and real promotion
remain separate validation requirements. A constrained local origin is useful
for repeatable comparisons, not proof of those environments.

## Online sources

Primary documentation/source inspected during this review; links can evolve.
Version-specific statements above identify inspected versions where material.

[PG18]: https://www.postgresql.org/docs/release/18.0/
[TOAST]: https://www.postgresql.org/docs/18/storage-toast.html
[LO]: https://www.postgresql.org/docs/18/largeobjects.html
[RETURNING]: https://www.postgresql.org/docs/18/dml-returning.html
[WAL]: https://www.postgresql.org/docs/18/runtime-config-wal.html
[Sequences]: https://www.postgresql.org/docs/18/functions-sequence.html
[PGX]: https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/pgconn
[SQLC]: https://docs.sqlc.dev/en/v1.31.1/tutorials/getting-started-postgresql.html
[PgBouncer]: https://www.pgbouncer.org/features.html
[SeaweedFS]: https://github.com/seaweedfs/seaweedfs
[Versity]: https://pkg.go.dev/github.com/versity/versitygw/embedgw
[GoFakeS3]: https://raw.githubusercontent.com/johannesboyne/gofakes3/master/backend.go
[S3 conditions]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
[ServeMux]: https://go.dev/blog/routing-enhancements
[cmux]: https://github.com/soheilhy/cmux
[AWS SDK]: https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/s3-checksums.html
[MinIO SDK]: https://github.com/minio/minio-go
[Go CDK]: https://pkg.go.dev/gocloud.dev/blob
[NGINX]: https://nginx.org/en/docs/http/ngx_http_slice_module.html
[DuckDB cache]: https://duckdb.org/docs/2.0/configuration/overview.html
[RESP sources]: https://github.com/tidwall/redcon
[OJS sources]: https://github.com/openjobspec/ojs-backend-postgres
[Go27]: https://go.dev/doc/go1.27
[JSON migration]: https://go.dev/doc/jsonv2-migration
[River]: https://riverqueue.com/docs
[PGMQ]: https://pgmq.github.io/pgmq/latest/
[PgQue]: https://pgque.dev/docs/concepts/
[Notify]: https://github.com/riverqueue/river/discussions/977
[Weighted]: https://pkg.go.dev/golang.org/x/sync/semaphore
[K8s resources]: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#local-ephemeral-storage
[DuckLake]: https://ducklake.select/2026/10/21/ducklake-11/
[pg_ducklake]: https://pgducklake.select/blog/releasing-v1/
[OTel HTTP]: https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp@v0.72.0
[OTel pgx]: https://pkg.go.dev/github.com/exaring/otelpgx@v0.13.0
[Prometheus]: https://github.com/prometheus/client_golang/blob/main/prometheus/doc.go
[pgBackRest]: https://pgbackrest.org/user-guide.html
[CNPG]: https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/backup.md
[Kong]: https://github.com/alecthomas/kong
[Synctest]: https://go.dev/blog/testing-time

Additional source details:

- [VersityGW backend contract](https://raw.githubusercontent.com/versity/versitygw/main/backend/backend.go)
- [S3 CompleteMultipartUpload conflict/checksum API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)
- [pgx COPY implementation and reader cancellation caveat](https://github.com/jackc/pgx/blob/v5.11.0/pgconn/pgconn.go)
- [Go CDK blob guide](https://gocloud.dev/howto/blob/)
- [Valkey RESP specification](https://valkey.io/topics/protocol/) and [gnet](https://github.com/panjf2000/gnet)
- [OJS Go client SDK](https://github.com/openjobspec/ojs-go-sdk), [backend module packaging](https://raw.githubusercontent.com/openjobspec/ojs-backend-postgres/main/go.mod), and [backend license](https://raw.githubusercontent.com/openjobspec/ojs-backend-postgres/main/LICENSE)
- [River SQL/notification guidance](https://riverqueue.com/docs/sql)
- [DuckLake data inlining](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining.html)
- [DuckDB 2.0 obsolete object-cache pragma](https://duckdb.org/docs/2.0/configuration/pragmas.html#object-cache)
- [CloudNativePG Barman Cloud plugin concepts](https://cloudnative-pg.github.io/plugin-barman-cloud/docs/concepts/)
