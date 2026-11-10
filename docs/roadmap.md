# Focused roadmap

Updated 2026-11-11. The three priorities below are implemented on layout v25;
[data-plane-results.md](data-plane-results.md) records the proof and measurements.

**Make the PostgreSQL object data plane easy to explain, demonstrate and
operate.** The primary workload is 10 MiB–1 GiB Parquet through S3/HTTP, in two
roles: native PostgreSQL storage and origin-backed shared cache/durable staging.
RESP2 and OJS remain bounded companions, not separate product roadmaps.

This is the current focus and evidence map, not an expansion of protocol scope.
[design.md](design.md) defines the architecture;
[CONTRIBUTING.md](https://github.com/adonm/pgvs3/blob/main/CONTRIBUTING.md#change-admission) defines the feature filter.
Historical proposals are research, not the active backlog.

## Delivered: S3 origin acceleration, Quack pool model and archive virtual folders (v0.8.0)

- **Bounded origin cache:** `--cache-max-bytes` evicts the oldest `s3c_` and
  committed `s3w_` objects instead of growing without limit. Aligned range
  chunks up to 64 MiB keep Parquet footers and column chunks in PostgreSQL, so
  warm ranged reads stay local. See [cache modes](operations.md#cache-modes).
- **One round trip for warm range metadata:** `MetadataOrOrigin` resolves a
  cached object and the parent origin metadata in one statement, so a warm
  range hit does not pay a second PostgreSQL query.
- **Anonymous public S3 and measured speedup:** the S3 origin accepts empty
  credentials and sends unsigned requests. `just overture` queries 1.23 GiB of
  Overture Maps Parquet through STAC; warm shared `s3c_` reads are **45.7×**
  and the 256 MiB gateway block cache **63.8×** faster than the direct median
  while caching only **1.80%** of source bytes. See
  [Overture measurements](benchmarks.md#anonymous-overture-maps--stac-just-overture).
- **S3 client hardening:** TigerBeetle-derived pooling and DNS lessons landed as
  reusable multipart part buffers, fixed-interval S3 host resolution with
  round-robin dialing, checksum pass-through on single PUTs, and seeded-PRNG
  fault-injection tests for retries and multipart cleanup.
- **Quack `C × W` pool model:** documented client/worker/gateway/PostgreSQL
  capacity limits with [FIG 07](index.md#fig-07--quack-client-to-worker-pool-gateway-admission--postgresql-capacity),
  a [sizing procedure](operations.md#sizing-a-quack-worker-pool) and measured
  aggregate pool rates. See
  [benchmarks](benchmarks.md#c-clients-x-w-worker-pool-benchmark).
- **Single-command origin startup:** `pgvs3 serve --s3-origin
  overture=s3://overturemaps-us-west-2?region=us-west-2 --auto-bucket lake`
  creates the local bucket and attaches the origin on writable startup, with no
  separate `origin set` process.
- **Archive virtual folders:** `.zip`/`.zip64`, `.tar`, and seekable
  `.tar.zst`/`.tzst` keys resolve members with one metadata query, index TAR
  headers by offset, jump Zstd frames through the `0x8F92EAB1` seek table on
  large members, and answer origin-backed missing members without a WAN 404.
  See [interfaces and scope](interfaces.md).

## Starting point

Before this pass, the following evidence existed. The gaps in the right column
motivated the now-delivered work below; they are not all current limitations.

| Existing evidence | What it does not establish |
| --- | --- |
| Transactional publication, exact Go/SQL contracts, logged relations and owner/token/version-fence regression tests | Lossless promotion of an arbitrary asynchronous standby |
| Shared whole/range hits and read-only gateway tests | Those read-only tests use a primary, not a physical streaming replica |
| `just fault`: gateway loss, PostgreSQL restart and overwrite/delete churn | Physical replica replay, promotion or restored pending-outbox behavior |
| `just contract` / `just interop`: real DuckDB, DuckLake and S3 clients | One maintained two-role analytical walkthrough with observable state transitions |
| Canonical paired runner; 51 passing 10 MiB–1 GiB incompressible-fixture cases in the latest pass; earlier actual-Parquet comparisons | Production capacity, tail latency, WAL amplification or AWS/Aurora equivalence |
| `stat`, pending-upload and spool/pool metrics | A measured end-to-end storage/WAL/replay budget for the core workload |

The completed work and its limits are recorded in
[modernization-results.md](modernization-results.md). Do not repeat the completed
contract/benchmark consolidation as a new refactor merely to change file counts.

## Delivered: read pins, slow-client fix and read cache (2026-11-02)

Re-measuring the floor found two problems. The `just sol` PostgreSQL read floor
used `ORDER BY no` on an un-analyzed table, which planned an on-disk sort and
understated PostgreSQL about 2.5×; corrected, the loopback HTTP floor binds
every read. And a multi-piece read kept its snapshot-exporting transaction idle
for the whole stream, so `--db-idle-tx-timeout` (default 1 minute) cut off any
large GET slower than that with a truncated `200`.

- The floor uses the gateway's chunk query and planner settings.
- Writable gateways pin streamed files with shared advisory locks; the reaper
  skips pinned files. Standbys, spans over 64 files and pins that race the
  reaper fall back to one snapshot imported once per worker. The export ends
  after the imports and worker transactions lift the idle timeout locally.
- Workers pull pieces in order instead of one task per piece, and chunk
  connections read through a 256 KiB socket buffer.
- `--read-cache-bytes` adds an opt-in gateway cache of immutable chunk blocks.
- The paired gate passes every case (whole-object GET 11–16% faster).
  `just fault` adds a slow read across an overwrite and a no-grace reap.

Remaining: uncached ranges still spend ~1–1.5 ms before the first byte on the
test laptop, and with eight clients uncached throughput is bounded by CPU per
byte (~0.8–0.9 gateway ns/byte, mostly kernel socket copies). Repeated reads
can use the read cache; one-pass scans cannot. See
[performance.md](performance.md#speed-of-light).

## Delivered: ordered read window (2026-11-02)

`just sol` before this change showed every read plateauing near 1.1–1.2 GiB/s
(25–50% of floor below 1 GiB) with ~0.6 gateway CPU ns/byte. The cause was
serialization: spans up to 64 MiB used one query, and single-PUT objects above
64 MiB planned more than eight pieces and took the sequential path.

The read path now plans balanced pieces for any span and streams them through
an ordered window of up to eight workers under one exported snapshot. The window
reserves half the chunk pool and shrinks as reads overlap. Results:

- Whole-object GET: 2.1–2.8 GiB/s (10 MiB 62–76% of floor; 256 MiB 106%;
  1 GiB 195%). Large-size percentages above 100% reflect the floor's
  `shared_buffers` limit, not a claim of beating PostgreSQL I/O.
- 8 MiB range: 1.9–2.0 GiB/s (50–51% of floor), up from 1.1–1.2 (26–28%).
- [Paired gate](benchmarks.md#paired-acceptance): 8 MiB GET 0.3% faster at 8
  workers and 0.8% faster at 16; 64 MiB GET 21% faster at 16; no read path
  regressed more than 20%.
- `just contract` and `just fault` pass, including overwrite/reap churn under
  the 1 s test reap grace.

Its percentages compared against the understated floor described above; the
read pins it called for are now delivered.

## Delivered high-value work

| Priority | Delivered work | Evidence |
| --- | --- | --- |
| **1** | Physical-replica reads, promotion and pending restore | `just fault`: real synchronous/asynchronous standbys, replay barriers, preserved bytes, controlled unreceived-WAL loss, and a consistent whole-database restore |
| **2** | One maintained two-role Parquet/DuckLake scenario | `just analytics`, also part of `just contract`: actual README SQL, cross-gateway queries, shared hits, pending publication and outbox drain |
| **3** | Core workload capacity ledger | Existing paired runner with `--resources` / `--replication`, reproducible Parquet/binary fixtures, and recorded storage/WAL/replay/spool/lifecycle evidence |

### 1. Prove physical-replica reads and promotion

Extend the existing Docker/fault harness, not a second HA framework. Keep
promotion and endpoint management outside pgvs3.

**Delivered:** `test/harness.sh` creates the physical replicas; the fault scenario
in `test/data_plane.py` exercises them. The restore drill exposed and closed an
idle-recovery gap: persisted tiers now open on an explicitly owned background
refresh, allowing restored outboxes to resume without an S3 request.

**Acceptance:**

- Start a primary and a real physical standby; assert recovery state and gate
  visibility on observed WAL replay positions, not fixed sleeps. Use multiple
  writable gateways plus a read-only gateway on the standby.
- Verify native bytes, whole-object fills, aligned range blocks and parent
  metadata after replay, including both parallel and sequential snapshot-read
  shapes. Check hashes, metadata, public validators and zero-origin warm hits.
- Block the origin, acknowledge a logged write-back body, and prove that body
  and outbox state remain readable together after a policy-qualified promotion.
  Resume the upload on the new writer without allowing an expired worker to
  publish over a newer write/delete. Include multipart publication.
- Exercise both a documented synchronous durability policy and a controlled
  asynchronous replay gap. Record which acknowledged commits that policy can
  preserve; never label unreplayed asynchronous commits lossless.
- Verify failed in-flight publication exposes no partial object, leadership and
  leases become reclaimable, and clients can reconcile ambiguous outcomes.
  Retain topology, replication settings and LSNs in failure artifacts.
- Carry the same native/pending fixtures through one consistent backup/restore
  drill. Restore the body, catalog and outbox together, then run `doctor`, read
  known bytes and drain pending work. Update the operations runbook from evidence.

Community PostgreSQL proof does not establish Aurora's storage/failover behavior;
managed-service validation remains a separately labeled deployment exercise.
The test fences the old primary/writer fleet and injects lease expiry afterward.
Its whole-database logical restore is not a physical archive/PITR proof.

### 2. Maintain one two-role Parquet/DuckLake scenario

Use `test/e2e.sh`, `deploy/bench/stack.sh` and the existing DuckDB tooling. The
README walkthrough is the entry point; bind its queries/expected outputs to the
contract rather than maintaining a disconnected demo harness.

**Delivered:** one million-row Parquet scenario in `test/data_plane.py` runs the
README blocks, queries through another gateway with DuckDB caches disabled,
counts origin requests and verifies DuckLake after reconnecting. The standalone
command retains an `analytics.json` record; the full contract reuses the same code.

**Acceptance:**

- Generate a reproducible Parquet file in the target size range. Write and query
  it through different gateways, then read footer/aligned ranges and compare
  content/aggregates after reconnecting.
- Run the same workload as native PostgreSQL storage and against an S3 origin.
  Show a cold shared fill, a warm hit from another gateway, and a pending
  acknowledged write followed by outbox drain. Make the active storage role and
  copy of record visible in the example output.
- Keep DuckDB's client cache separate from pgvs3 cache state. Verify origin
  request counts for claimed shared hits; a client-local hit is not that proof.
- Include DuckLake's separate catalog and an explicit no-shared-transaction
  caveat. Check actual query results, not only HTTP status or file existence.
- Keep one disposable entry point and its normal cleanup/failure artifacts.
  Companion examples/tests remain available but are not prerequisites for the
  analytical path. No new protocol is required.

Existing contract tests already cover Parquet and DuckLake. The value here is a
single maintained explanatory scenario across both roles, not duplicate coverage.

### 3. Publish the core workload's capacity budget

Extend the canonical paired runner's evidence, not the number of load engines.
Use the replica fixture from item 1 for replay measurements.

**Delivered:** `deploy/bench/capacity.py` observes the existing loadgen rather than
generating another workload. Explicit single-endpoint capacity capture, isolated
paired clusters, replay/system-identifier checks,
raw sample records, per-path gates and delete/expiry/reap/vacuum snapshots make
the scope explicit. See [the method](benchmarks.md#capacity-evidence) and
[recorded results](data-plane-results.md).

**Acceptance:**

- For native publication, cold whole/range fills, warm hits, overwrite/delete
  reaping and write-back, report logical payload bytes, table/index footprint,
  WAL deltas, replay lag/catch-up, spool peak, pool waits and connection counts.
  State the measurement scope; cluster-wide WAL counters need isolated work.
- Measure 10 MiB, 100 MiB and 1 GiB Parquet/incompressible fixtures. Record the
  host, working set, checkpoints, TTL/range settings and replication policy;
  distinguish gateway-cold from operating-system/client-cold states.
- Report acknowledgement latency separately from origin completion and backlog
  growth/drain. Show retained and reclaimed space after overwrite/expiry/GC,
  not only the size of the latest live object.
- Retain raw measurements and per-path latency/rate gates. Translate them into
  deployment sizing examples with headroom for co-located workloads, WAL retention
  and replica recovery. Treat the 32 TiB relation limit as a format ceiling, not
  a demonstrated deployment size or a free capacity allocation per namespace.
- Publish a concise native-versus-origin decision table in `operations.md`, with
  links to the measurements. Do not infer cost or durability equivalence with a
  cloud object service from loopback timings or disk price alone.

## Remaining proof limits

Validate the actual deployment's managed-service failover, physical backup/WAL
archive/PITR tooling, production working sets and tail latency. Brief local
measurements do not establish sustainable maximum capacity, co-location isolation
or cost equivalence with a cloud object store. These are deployment/evidence
limits, not reasons to add an HA controller or more protocols.

## What not to prioritize

- Full S3/Redis/OJS parity, another protocol, or removing existing companions.
- Authentication, gateway TLS, authoritative local caches (the opt-in read cache
  holds only immutable bytes behind fresh metadata), UNLOGGED staging,
  multi-primary writes, sharding or an in-process HA controller.
- Replacing the page layout, streaming engine or S3 client merely to reduce line
  counts. Reopen such experiments only for a named maintenance burden or measured
  bottleneck, with preserved capabilities and independent 20% performance gates.
- Broad restructuring without a named maintenance burden or measured bottleneck.
  Targeted client fixes and invariant repairs remain valid.

Every implementation keeps acknowledged bodies WAL-logged, shared fills/hits
available across gateways and read-only replicas, and the trusted-network model.
The already accepted small-origin-hit catalog-check exception remains documented;
it is not a new blanket exception to the performance budget.
