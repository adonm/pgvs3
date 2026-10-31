# Workload guides

**Keep OLTP in PostgreSQL SQL. Use pgvs3 for object bytes, shared object
caching, durable origin staging and bounded companion queues.** It does not
make arbitrary application SQL faster, implement CDC or turn PostgreSQL into a
distributed columnar engine. Start with the path that removes your bottleneck,
not with every interface enabled.

## Choose the path

| I need to… | Start here | Success means |
| --- | --- | --- |
| Protect transaction latency while adding analytics/cache/jobs | [Protect OLTP](postgres.md) | Measured application tails, connection waits, WAL and vacuum remain inside your budget. |
| Stop repeatedly scanning application tables for reports | [Publish analytical snapshots](analytics.md) | A validated, versioned extract is queried by DuckDB; its freshness and refresh cost are explicit. |
| Reuse S3/HTTP bytes across clients and gateways | [Cache and staging](cache.md) | Another gateway serves the same bytes without an origin fetch; TTL and pending-write authority are understood. |
| Reliably hand committed application work to HTTP workers | [Jobs, outboxes and streams](jobs.md) | Rollbacks enqueue nothing; retries and worker crashes do not duplicate the demonstrated SQL effect. |
| Diagnose latency, capacity or recovery trouble | [Observe and recover](observe-recover.md) | Identify the constrained resource, preserve acknowledged data and verify recovery before routing traffic. |

For setup use the [quick start](../../README.md#quick-start). For flags/API and
topology details use [operations](../operations.md), not a copied deployment
preset. [Sources](sources.md) records the research behind these decisions;
[validation](validation.md) separates executable evidence from deployment advice.

## Six different kinds of state

| Layer | Who computes/owns it? | Freshness and durability boundary |
| --- | --- | --- |
| Application SQL rows | PostgreSQL/application | SQL transaction; indexes/plans matter. pgvs3 does not intercept these queries. |
| Native objects | pgvs3 + PostgreSQL | One object's committed publication; logged bytes. No transaction spanning an application's SQL and an HTTP PUT. |
| Shared origin-byte cache | pgvs3 `s3c_` rows | TTL/version revalidation, not SQL-result invalidation. Fills write PostgreSQL WAL. |
| Pending origin writes | pgvs3 `s3w_` + outbox | Acknowledged bytes may exist **only in PostgreSQL** until origin handoff. Not disposable or TTL-expiring. |
| Analytical materialization | DuckDB/DuckLake/application | Extract/catalog snapshot and its refresh schedule. pgvs3 transports bytes, not source-table changes. |
| Jobs / stream deliveries | OJS / application workers | Leases or acknowledged batches; effects and HTTP acknowledgements are separate commits. Expect redelivery. |

DuckDB buffers, Parquet metadata caches and native-extension caches are
**client-private**, not pgvs3 shared-cache hits. A SQL results cache is yet another
layer: pgvs3 has no automatic one. If storing application results via RESP, the
application must define keys, invalidation, TTL, stale serving and sensitive-data
scope; co-location does not supply them.

## Decide what you are optimizing

Write down before changing settings:

1. **Correct answer:** source of record, allowed staleness, delete/update handling,
   idempotency key, RPO/RTO and which replica may serve the read.
2. **Budget:** application p95/p99 and error rate, object ACK latency **and drain
   rate**, refresh lag, queue age, disk/WAL/scratch headroom and connection ceiling.
3. **Experiment:** control and candidate, identical 10 MiB–1 GiB files/queries,
   cold/warm definitions, concurrent OLTP and retained failures. Change one axis.
4. **Acceptance:** report each affected path independently. The project's 20%
   regression budget is not permission to breach a stricter application SLO.
   Roll back the setting or move the workload off-cluster if that SLO fails.

More gateways increase client handling capacity, **not** PostgreSQL CPU, WAL,
disk or autovacuum isolation. Separate schemas, databases, roles and pools are
ownership/admission boundaries, not resource reservations. Use another cluster
or external analytical storage when a shared cluster cannot meet both budgets.

## Try it without touching a real database

```sh
just setup
just guides
```

Requires Docker, the mise toolchain and access to the official DuckDB extension
repository. This reuses the existing disposable two-gateway/origin harness,
runs named snippets from these pages and retains `guides.json`, query plans and
failure evidence under the printed `.tmp/verify/guides.*/` directory. It creates
only fixture-owned data; it is not a load test or cloud qualification.

For a manual walkthrough, start the quick-start gateway, use a **disposable**
database, and have `pgvs3`, `psql` and `curl` on `PATH` plus a DuckDB 2.0 session.
In the client terminal set:

```sh
export PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable'
export PGVS3_ENDPOINT='http://127.0.0.1:8014'
```

Then follow PostgreSQL → analytics → jobs. These examples reserve
`guide_app`, `guide-analytics`, `guide-jobs` and `guide-events`; stop if those names
already contain your data. Recipe SQL mutations are to application-owned demo
tables, not pgvs3 internals. The test alone advances one owned terminal job's age
to exercise normal retention without a day-long wait. Cache experiments use a
separately owned test origin.

> **No public endpoint or tenant boundary.** The listener never authenticates,
> authorizes or terminates TLS. Anyone who can reach it can read or destroy all
> namespaces. Keep it private; dummy S3 credentials are not access control.

## What to read after a successful trial

- [Native versus origin-backed costs and replica proof](../data-plane-results.md).
- [Per-path benchmark acceptance and capacity capture](../benchmarks.md).
- [DuckLake, Differential and ObjFS trade-offs](analytics.md#choose-an-analytical-storage-format).
- [Authority, acknowledgements and multi-worker invariants](../design.md).

The quality bar here is inspectable: prerequisites, state ownership, runnable
recipes, expected results, negative cases, measurement, recovery and safe cleanup.
The named reference projects inform that bar; no unmeasured vendor-performance
or universal-replacement claim is implied.
