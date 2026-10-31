# Contributing

pgvs3 is a PostgreSQL object data plane with stateless Go gateways, tuned for
10 MiB–1 GiB Parquet objects on PostgreSQL 18+.

## Scope

The core roles are PostgreSQL object storage and origin-backed shared caching /
durable write-back staging through S3/HTTP. RESP2 values and OJS jobs/streams
remain supported companions; expanding toward full Redis or queue-product
parity is not a default priority. Preserve their documented capabilities rather
than removing them to meet a line-count target.

pgvs3 is for trusted networks. Authentication, authorization, gateway TLS and
presigned URLs are permanent non-goals, not unfinished work. See the
[architecture and ownership contracts](docs/design.md) and the
[current ranked roadmap](docs/roadmap.md), not historical proposal priorities.

### Change admission

A proposal should answer:

1. **Which core path or operating decision improves?** Name the native object,
   origin-cache/staging or shared-cluster problem; protocol breadth alone is
   not a benefit. Companion fixes should have a concrete client need.
2. **Who owns the state and acknowledgement?** Keep committed gateway state in
   PostgreSQL, pending bodies logged and non-expiring, and fills/cache hits
   shared across gateways and read-only replicas. Origins still own committed
   external objects; deployment policy determines promotion durability.
3. **Does it reduce owned behavior?** Prefer an existing PostgreSQL primitive
   or shared decision over another service, generic storage layer or special
   case. Fewer files/lines alone do not establish simpler semantics.
4. **What is the evidence?** Reuse the existing test layers and paired runner.
   Compare affected paths independently within the 20% budget. Among options
   preserving the fixed durability/multi-worker contracts, evaluate performance
   first, then compatibility and additional semantic coverage.

No new authentication, local authority, UNLOGGED substitute, sharding or
multi-primary layer belongs in an incidental refactor. Describe trade-offs and
scope changes explicitly before implementation.

## Toolchain

```sh
mise install   # Go, just, ko, hk, Go linters, uv, Python, Helm, Rust, rclone
just setup     # download Go modules
just hooks     # install hk Git hooks for this clone
```

Tool versions are pinned in `mise.toml`. Container images are built directly
from Go source with `ko` (no `Dockerfile`, `CGO_ENABLED=0`).

Git hooks are managed by [hk](https://hk.jdx.dev). `just hooks` installs the
project hooks through mise; `just fix` applies formatting, import, tidy and
`go fix` fixes to the working tree, and `just lint` runs the full check. The
pre-commit hook applies safe fixes to staged files and restores unstaged work;
the pre-push hook runs module tests and `govulncheck`.

## Verification

```sh
just lint       # hk: go fix, imports, gofmt, go mod tidy, vet, staticcheck
just fix        # apply the same safe fixes to the working tree
just test       # unit tests
just race       # unit tests with race detector
just integration # store/queue/cache integration against disposable PostgreSQL 18
just s3t        # fast s3t allow-list (Go port of ceph/s3-tests) against a running gateway
just contract   # end-to-end S3, boto3, DuckDB 2.0, DuckLake, duckdb-object-storage
just analytics  # same README SQL and two-role scenario used by the contract
just interop    # rclone, restic, boto3, and DuckLake
```

`just ci` is the same complete entry point locally and in GitHub: hk (including
`govulncheck`), integration-tagged vet, race tests, uncached race-enabled
integration, fuzz, Helm/kind, contract, fault, interop and s3t.

Docker-backed suites share `test/harness.sh`. Each run owns its database container,
gateway binary, processes and logs under a unique `.tmp/verify/` directory. Passing
runs print one summary and remove their scratch files. Failed runs keep their
artifacts, print the relevant logs and preserve the failure exit code; GitHub
uploads the retained logs. Concurrent suites never reuse a gateway binary or log.

Docker must be on `PATH` with a context supported by kind. Select a different
engine for one run with `DOCKER_CONTEXT`; rootless providers also need cgroup
delegation. The runner does not modify host settings or skip unavailable gates.

`just contract` is the required correctness gate before merging. It starts a
disposable PostgreSQL 18 container and two local gateways, then verifies
conditional writes, byte ranges, multipart uploads, zero-copy copies,
cross-gateway read-after-write consistency, garbage collection, DuckDB 2.0,
DuckLake, and `duckdb-object-storage`.

## Style

Prefer self-describing names, small capability interfaces, and focused
functions over explanatory comments. Comments in first-party Go code are
reserved for compiler directives (`//go:build`, `//go:embed`) and vendored
license headers. Keep decision logic readable enough that a comment is not
needed to reconstruct it.

## Test surface

Keep one harness per layer. Do not add a second framework for the same job.

| Layer | Harness |
| --- | --- |
| Pure logic | Go unit tests |
| Benchmark tooling | Python standard-library unit tests, included in `just ci` |
| SQL, store, queues, cache | `just integration` (shared Docker harness) |
| S3 conformance | `just s3t` (pinned Go port of `ceph/s3-tests`) |
| Real clients and end-to-end flows | `just contract`, `just interop` |
| Gateway/primary loss, physical replica replay/promotion, pending restore | `just fault` (same Docker harness) |

`just s3-clients` is an opt-in transport benchmark in the isolated
`test/s3clients` Go module. AWS/MinIO experiment dependencies must not upgrade
the gateway module or enter its production binary. It is not another S3
conformance harness and does not establish full replacement compatibility.

`just differential` is an opt-in part of the existing DuckDB benchmark and Docker
harness. It pins an experimental external client only; no dependency enters the
gateway binary or default CI contract. Its standard-library tooling tests run in
CI, while the real-client/recovery/reclamation experiment is explicit. See
[differential.md](docs/differential.md) for durable-ACK comparison rules, retained
negative fencing evidence and upstream artifact expiry.

S3 compatibility is **`s3t` only**. The Python `ceph/s3-tests`/tox/pytest path
was removed deliberately: it required a separate Python toolchain, duplicated
the Go integration layer, and was slow and Ceph-specific. Do not reintroduce
it or vendor a second conformance suite.

Grow coverage by adding node IDs to `test/s3t-allow.txt` when the pinned `s3t`
version already has the test, or by contributing the port upstream. The pinned
commit lives in `test/s3t.sh`; bump it deliberately and re-run the allow-list.
A new test harness must replace an existing layer, not add a parallel one.

`test/data_plane.py` is the shared scenario for the existing contract/fault
entry points, not another conformance framework. It uses the README's actual
SQL, disables DuckDB client caches, and counts requests through a controllable
S3-origin fixture. Physical-standby checks gate on recovery state and replay LSNs;
the old primary/writer fleet is fenced before promotion. The recovery drill uses
a consistent whole-database logical dump, not a PITR claim. Passing fault runs
remain quiet; `test_keep_artifacts=true just fault` retains the evidence JSON.

Format code before committing:

```sh
just fix
```

`hk` uses Go 1.27's `go fix` modernizers, `goimports`, `gofmt -s`, `go mod tidy`,
`go vet`, and `staticcheck`. Install the hooks once per clone with `just hooks`;
`just lint` is the same check used in CI.

## Schema changes

There is one embedded baseline, `internal/store/schema.sql`, applied only to an
empty database. The current baseline is layout **v25**. `store.LayoutVersion` is
the on-disk compatibility marker and there is no in-place migration path
between layouts.

- Change `schema.sql` directly, or regenerate it from a fresh database that has
  been initialized with the desired schema.
- Bump `store.LayoutVersion` for any schema change. A gateway refuses a database
  whose layout does not match.
- A layout change requires a fresh database and re-import; do not add a
  compatibility shim or a legacy decoder for pre-release databases.
- Keep `layoutManifest` in `internal/store/store.go` in sync with the global
  catalog relations and functions. Keep the object/stream function contracts in
  sync too: startup and doctor check exact input signatures, result types,
  set cardinality and returned column names/types/order in one catalog query.
  These checks do not run on object requests.
- Run `just test`, `just integration`, and the contract suite against a fresh
  database before merging. The tests intentionally do not exercise upgrades.

## Shared-database safety invariants

pgvs3 supports multiple stateless gateway workers sharing one PostgreSQL
primary/standby cluster. Keep these invariants when changing storage or
background work:

- **The catalog is authoritative.** Never add a code path that implicitly
  creates or recreates a deleted bucket, especially from a stale in-process
  view. Bucket writes require an existing bucket; cache and queue namespaces
  keep their documented first-use creation.
- **No local authority.** In-process maps and gauges may be hints only.
  Coordinate cross-worker work with PostgreSQL advisory locks, leases,
  conditional updates, token guards, or database sequences.
- **One maintenance leader.** Reaping, TTL expiry, and reconciliation stay
  serialized by `pg_try_advisory_lock`; a dead leader must be replaceable
  automatically.
- **Fence origin work.** Read-through fills publish against the expected origin
  version, write-through deletes leave an expiring tombstone, and multipart
  publishes keep their pending token guard. Pending uploads are claimed with
  database leases and ordered key locks. Claims/publication must remain fenced;
  retries are at least once, not a cross-system exactly-once guarantee.
  Read-fill leases renew while fetching; publication must lock/check the live
  owner row in the same transaction. Cancelling a stale owner is not a SQL fence.
  Removing a write-cache fill fence after origin revalidation must revoke old
  fill owners atomically; preserve real write-back delete tombstones and newer
  gateway publications.
- **Shared parent metadata is not a body.** Range metadata stays logged in the
  namespace's `origin_metadata` table, respects validation TTL, and cannot hide
  write-back ownership or tombstones. Never substitute an in-process metadata
  cache. Reserve disk spool bytes across all origin paths, including retries.
- **Respect the envelope.** The supported target is at most 100 namespaces per
  database and one PostgreSQL primary plus standbys. Do not add design
  complexity beyond this envelope, such as multi-primary writes.

## Performance invariants

Refactoring must preserve the 10 MiB–1 GiB object workload within a 20%
performance budget. Keep before/after binaries, compare individual read/write
and range paths rather than an aggregate, and record the environment and sample
counts. The [cache code boundaries](docs/design.md#cache-code-boundaries)
describe where shared orchestration belongs and which streaming fast paths
should remain specialized.

- **8,120-byte chunk payload:** keep full chunk payloads at 8,120 bytes with
  `toast_tuple_target = 8160` and `STORAGE EXTERNAL` so each row fits one 8 KB
  page without TOAST.
- **No gateway object cache:** do not add in-process object or metadata caches.
  Logged namespace rows provide sharing; each PostgreSQL instance has its own
  `shared_buffers`, and DuckDB may also cache remote file bytes client-side.
- **Split pools:** keep `enable_indexscan = on` on the main pool for B-tree
  point lookups and listings, and `enable_indexscan = off` on `chunkPool` for
  PostgreSQL 18 bitmap heap scan read-ahead on namespace-local `chunks` tables.
- **Cut-through streaming:** stream rows from PostgreSQL to HTTP without
  buffering whole objects in memory.
- **Bounded maintenance:** keep background reaping bounded per pass and
  serialized across gateways with `pg_try_advisory_lock`.

## Commits

- Keep one logical change per commit and state the user-visible effect.
- Update `README.md` and `docs/` when flags, S3 behavior, or layout rules
  change.
- Never commit `.tmp/`, database dumps, or credentials.

## License

Apache-2.0. See [LICENSE](LICENSE).

## Package map

Group related operations by capability, not one function/type per file. There
are 49 Go files, including tests and the five-file `internal/s3client`. Use the
named sections within each implementation rather than adding a file for every
helper. Package boundaries, integration build tags and isolated benchmark
dependencies stay separate. Shared orchestration belongs above specialized
block encoding, streaming reads and COPY loops, not inside them.

| File | Responsibility |
| --- | --- |
| `internal/store/store.go` | Connections, embedded schema, namespace lifecycle, transactions, quotas and maintenance |
| `internal/store/handles.go` | Cache and queue namespace handles; each validates its name and resolves its schema once |
| `internal/store/object.go` | Object/publication contracts, metadata, reads and block encoding |
| `internal/store/write.go` | Inline/COPY publication, multipart lifecycle and server-side copies |
| `internal/store/cache.go` | Cache mutations, scoped locked writers, origin metadata, fill fences and durable outbox persistence |
| `internal/store/ojs.go` | Job persistence, claims and state transitions |
| `internal/store/stream.go` | Stream Go wrappers over the layout-v25 `stream_*` functions in `schema.sql`: validation, sentinel mapping and result types |
| `internal/objcache/cache.go` | Shared runtime limits, immutable tier options, cache resolution, fills, bounded spooling and ordered mutations/outbox drain |
| `internal/objcache/origin.go` | Origin contract, capabilities and production/local adapters |
| `internal/objcache/registry.go` | Persisted configuration, request borrowing and deferred retirement of global/per-bucket tiers |
| `internal/s3/server.go` | HTTP transport, routing, bucket lifecycle, errors and metrics |
| `internal/s3/object.go` | Request-scoped native/origin adapters, object and multipart operations, listings and response streaming |
| `internal/s3/request.go` | Parsing/header policy, conditions and shared upload-body framing, length and checksum handling |
| `internal/s3/xml.go` | S3 XML shapes and encoding |
| `internal/ojs/ojs.go` | HTTP binding, validation, job operations and metrics |
| `internal/ojs/streams.go` | Stream HTTP operations |
| `internal/resp/resp.go` | RESP transport, commands and metrics |
| `internal/s3client/client.go` | S3 config, pooled transport, request signing and typed errors |
| `internal/s3client/object.go` | Object/range/listing and streaming multipart operations |
| `internal/s3client/signer.go` | Shared AWS Signature Version 4 signer |

Each package keeps one unit-test file and, where needed, one integration-test
file. `store/pool_integration.go` stays build-tagged production support because
other packages' integration tests use its exported pool accessor.

| Package | Responsibility |
| --- | --- |
| `internal/ns` | namespace kinds, name validation, schema naming |
| `internal/store` | PostgreSQL layout, objects, blocks, cache, queues, GC |
| `internal/s3` | S3 + plain HTTP handlers, cache-tier proxy |
| `internal/ojs` | OJS v1 core HTTP binding (jobs, workers, queues, manifest) |
| `internal/resp` | RESP2 server |
| `internal/objcache` | Shared read/range cache, origin adapters, durable staging and leased outbox |
| `internal/reqguard` | protocol-independent request policy checks (presigned URLs) |
| `internal/gateway` | protocol mux and HTTP server |
| `internal/s3client` | Apache-2.0 S3 client derived from `kelindar/s3`, with cache-origin extensions |
