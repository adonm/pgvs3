# Focused roadmap

Updated 2026-10-29 against the layout-v25 code at `8532344`.

**Make the PostgreSQL object data plane easy to explain, demonstrate and
operate.** The primary workload is 10 MiB–1 GiB Parquet through S3/HTTP, in two
roles: native PostgreSQL storage and origin-backed shared cache/durable staging.
RESP2 and OJS remain bounded companions, not separate product roadmaps.

This is the current ranked plan, not a claim that the work below is complete.
[design.md](design.md) defines the architecture;
[CONTRIBUTING.md](../CONTRIBUTING.md#change-admission) defines the feature filter.
Historical proposals are research, not the active backlog.

## Evidence already in place

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

## Ranked next work

| Rank | Work | Why it comes next |
| --- | --- | --- |
| **1** | Prove physical-replica reads and promotion | Replicated bytes and replaceable gateways are the central distinction; this is the largest unproven operating-model claim. |
| **2** | Maintain one two-role Parquet/DuckLake scenario | Show the product's purpose before its protocol inventory, using runnable evidence rather than another feature list. |
| **3** | Publish the core workload's capacity budget | Explain when storing bytes in PostgreSQL is a good trade, including WAL, replay and co-location costs—not just peak throughput. |

### 1. Prove physical-replica reads and promotion

Extend the existing Docker/fault harness, not a second HA framework. Keep
promotion and endpoint management outside pgvs3.

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

### 2. Maintain one two-role Parquet/DuckLake scenario

Use `test/e2e.sh`, `deploy/bench/stack.sh` and the existing DuckDB tooling. The
README walkthrough is the entry point; bind its queries/expected outputs to the
contract rather than maintaining a disconnected demo harness.

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

## What not to prioritize

- Full S3/Redis/OJS parity, another protocol, or removing existing companions.
- Authentication, gateway TLS, authoritative local caches, UNLOGGED staging,
  multi-primary writes, sharding or an in-process HA controller.
- Replacing the page layout, streaming engine or S3 client merely to reduce line
  counts. Reopen such experiments only for a named maintenance burden or measured
  bottleneck, with preserved capabilities and independent 20% performance gates.
- Broad restructuring before the replica, example and capacity evidence closes
  the core questions. Targeted client fixes and invariant repairs remain valid.

Every implementation keeps acknowledged bodies WAL-logged, shared fills/hits
available across gateways and read-only replicas, and the trusted-network model.
The already accepted small-origin-hit catalog-check exception remains documented;
it is not a new blanket exception to the performance budget.
