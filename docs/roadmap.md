# Focused roadmap

Updated 2026-10-29. The three priorities below are implemented on layout v25;
[data-plane-results.md](data-plane-results.md) records the proof and measurements.

**Make the PostgreSQL object data plane easy to explain, demonstrate and
operate.** The primary workload is 10 MiB–1 GiB Parquet through S3/HTTP, in two
roles: native PostgreSQL storage and origin-backed shared cache/durable staging.
RESP2 and OJS remain bounded companions, not separate product roadmaps.

This is the current focus and evidence map, not an expansion of protocol scope.
[design.md](design.md) defines the architecture;
[CONTRIBUTING.md](../CONTRIBUTING.md#change-admission) defines the feature filter.
Historical proposals are research, not the active backlog.

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
- Authentication, gateway TLS, authoritative local caches, UNLOGGED staging,
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
