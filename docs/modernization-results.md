# Modernization implementation and performance comparison

This is completed implementation/evidence history across v24/v25, not the active
backlog. See [design.md](design.md) for the current contracts and
[roadmap.md](roadmap.md) for ranked next work.

Date: 2026-10-28. Control: `a879472` (layout v23). Candidate: layout **v24**.

This implements the highest-value bounded changes from the
[capability review](proposals/modernization-review.md), retaining PostgreSQL 18+,
logged shared state, the one-port trusted-network model, and specialized object
streaming. Fresh databases are required; there is no layout migration.

## Implemented

- **Shared parent metadata for range-only caching.** Logged, schema-local
  records cache size, origin validator and headers, but never represent a body.
  Validation uses the database clock and excludes time spent waiting for HEAD.
  TTL/retention, mutation fences, flush/drop and read-only consumption are covered.
- **One-round-trip cache shadow resolution.** Write-back ownership/tombstones
  take precedence over shared read cache in one PostgreSQL query. Missing or
  partially initialized cache namespaces retain independent-lookup fallback.
- **Renewable, publication-fenced fill ownership.** Owner-token renewal uses the
  database clock. Publication locks/checks live ownership and the origin version;
  an expired, cancelled or superseded owner cannot publish. Waiters do not cancel
  the owner and have explicit, fetch-budget-aware fallback limits.
- **Shared disk spool byte admission.** Fills, foreground writes and pending
  uploads share a configurable 8 GiB payload budget. Unknown-length growth is
  nonblocking to avoid partial-reservation deadlock. Cleanup is idempotent;
  known-size copies retain file-copy fast paths. Exhaustion is retryable before
  acknowledgment; acknowledged pending bodies remain logged in PostgreSQL.
- **Outbox wake hints.** Successful durable publication wakes a local worker
  instead of always waiting for the idle poll. The buffered hint is lossy and
  carries no job authority; database claims and one-second cross-gateway/crash
  fallback polling remain unchanged.
- **PostgreSQL 18 publication SQL.** Inline and chunked entry points share one
  mutation. `RETURNING old.file_id, old.parts` supplies garbage ownership without
  an unconditional pre-read. TTL, CAS, multipart metadata cleanup, origin fences
  and body/outbox atomicity remain transactional.
- **Cache retirement safety.** External origin changes retire only the observed
  write-cache generation. Old fill ownership and read-cache fences are removed
  atomically, preventing stale-fill resurrection without hiding the new origin.
  Newer publications and authoritative delete tombstones remain protected.
- **Compatibility and diagnostics.** Missing keys are distinct from missing
  buckets, restoring static-index fallback. Namespace stats have the correct
  result shape. Partial payloads do not advertise whole-object checksums. Warm
  range hits are reported accurately. Helm scratch resources and spool metrics
  are aligned, and durability/pooling/quota documentation is qualified.

**Preserved:** unconditional multipart write-back, including small multipart
bodies. The end-to-end test now waits for asynchronous origin persistence rather
than incorrectly requiring the origin file to exist before acknowledgment.
Session key locks remain around remote writes; a local token check alone cannot
stop a stale remote upload.

## S3 origin-client experiments

Compared the existing fork with AWS SDK Go v2 S3 **v1.114.6** and MinIO Go SDK
**v7.4.1**. Experiments live in the isolated `test/s3clients` module, not the
gateway dependency graph or binary. These are equivalent HEAD/GET/single-PUT
transport comparisons, not a claim of full replacement compatibility.

The isolated experiment used a loopback HTTP origin with no storage backend,
checksum validation or signature verification. Clients still signed requests;
implicit retries, payload hashing and additional checksum work were disabled
where appropriate to match the existing client. MinIO used its low-level Core
single-PUT API rather than silently switching to multipart at smaller sizes.
Five repetitions used 250 ms benchmark windows; table values are medians.

| Payload / operation | Existing fork | AWS SDK | MinIO SDK |
| --- | ---: | ---: | ---: |
| 4 KiB HEAD | 39.17 µs | 87.70 µs (+123.9%) | 51.68 µs (+31.9%) |
| 4 KiB GET | 47.15 µs | 93.37 µs (+98.0%) | 60.70 µs (+28.7%) |
| 4 KiB PUT | 40.65 µs | 103.80 µs (+155.4%) | 59.97 µs (+47.5%) |
| 10 MiB GET | 1.781 ms | 1.826 ms (+2.5%) | 1.751 ms (-1.7%) |
| 10 MiB PUT | 2.627 ms | 2.693 ms (+2.5%) | 2.649 ms (+0.8%) |
| 64 MiB GET | 11.678 ms | 11.618 ms (-0.5%) | 11.746 ms (+0.6%) |
| 64 MiB PUT | 18.819 ms | 18.646 ms (-0.9%) | 18.297 ms (-2.8%) |
| 256 MiB GET | 46.389 ms | 46.549 ms (+0.3%) | 46.797 ms (+0.9%) |
| 256 MiB PUT | 64.862 ms | 64.142 ms (-1.1%) | 63.209 ms (-2.5%) |
| 1 GiB GET | 189.010 ms | 187.424 ms (-0.8%) | 182.744 ms (-3.3%) |
| 1 GiB PUT | 254.765 ms | 252.580 ms (-0.9%) | 246.378 ms (-3.3%) |

The fork needed about 92 allocations per warm HEAD, AWS about 476, and MinIO
about 186. All clients were close on large transfers, but metadata and small
operations exceeded the 20% gate. **The existing client remains in production.**
Real network RTT can dominate these differences; that does not establish that a
larger client is free on the gateway's low-latency paths. A complete conditional,
multipart, metadata/checksum and failure-mode adapter was therefore not adopted.

Rerun the isolated experiment with `just s3-clients`. This is not a replacement
for the pinned s3t conformance or real-client contract suites.
Raw results: `.tmp/modernization/s3-clients-isolated.txt`. Earlier prototype
results are retained separately in `s3-clients-final.txt`; they reached the same
decision and are not mixed into this table.

## Gateway comparison methodology

- Same host: AMD Ryzen 7 7700X, Linux/amd64, Go 1.27.3.
- Same PostgreSQL 18 instance for both disposable databases, 1 GiB
  `shared_buffers`, `synchronous_commit=on`, and worker AIO. No UNLOGGED storage.
- Separate control/candidate processes and databases; identical client and
  fixture bytes. Request order alternates before/after across five repetitions.
- Actual Snappy Parquet fixtures generated with DuckDB 2.0, with 122,880-row
  groups: 10,490,851; 67,128,878; 268,489,564; and 1,073,940,706 bytes. The largest
  is approximately 1 GiB, 0.019% above that exact byte boundary.
- HEAD, GET, footer ranges and PUT at concurrency 1 and 4. S3 signing is omitted
  in the gateway load generator because signatures are intentionally ignored;
  both binaries use that same client. Transport experiments above still sign.
- Relevant per-case gates: candidate median p50 time <= 1.20 × control and
  median throughput >= 0.80 × control. Percentiles from small fixed large-object
  batches are diagnostics, not a production p99/SLO claim.

### Rejected initial run

The first cumulative-overwrite run is retained in
`.tmp/modernization/native.json` and `native.log`. It was **not accepted**:

- 10 MiB GET measured 6.372 → 10.289 ms at concurrency 1 and 8.365 → 11.321 ms
  at concurrency 4, over budget.
- Concurrent 1 GiB candidate PUTs eventually hit the client's 60-second timeout.
- PostgreSQL logged frequent checkpoints, heavy checkpoint sync, vacuum and
  approximately 39 GiB chunk relations in each database. The workload accumulated
  overwritten files while the default 1 GiB WAL budget introduced checkpoint
  phase differences; planner statistics also differed between the databases.
- A subsequent GET check on the unchanged binaries measured 10.963 → 9.762 ms,
  demonstrating that the original read difference was not stable. The initial
  query plans were not captured, so this is not a definitive single-cause diagnosis.

The controlled rerun explicitly analyzes read fixtures, uses fresh buckets for
each PUT batch, matches checkpoints outside timed work, and uses a 16 GiB WAL
budget / 30-minute checkpoint timeout on the **benchmark instance only**. This
does not change production durability or silently tune a user's database.

### Native S3 acceptance

The controlled matrix completed all 400 runs without request errors or timeouts.
Thirty-nine of forty short-run cases passed directly. Concurrent 10 MiB GET
still showed +26.2% p50 in its seven-request batches, so it was **not waived**:
five paired 1,000-request steady-state runs per binary followed. Median p50 was
11.304 → 11.240 ms (-0.6%), median throughput 326.416 → 328.873 requests/s
(+0.8%), with zero errors. Both databases used matching bitmap heap/index scan
plans. The tiny-batch anomaly was not stable under adequate sampling; the harness
now requires at least 128 requests for short/medium GETs.

The table includes that explicitly identified steady-state replacement. Each
cell is candidate p50 change at **concurrency 1 / concurrency 4**:

| Payload | HEAD | GET | Footer range | PUT |
| --- | ---: | ---: | ---: | ---: |
| 4 KiB | +3.7% / +0.7% | +3.4% / +1.3% | -2.5% / +0.0% | +0.6% / +0.3% |
| ~10 MiB | -0.9% / -1.4% | -0.3% / -0.6% | -1.6% / +1.2% | +3.1% / -4.5% |
| ~64 MiB | +5.7% / -2.1% | -1.4% / +2.8% | -2.6% / -2.3% | -1.4% / -4.7% |
| ~256 MiB | -1.9% / +3.7% | -0.3% / -1.1% | +1.0% / -0.8% | -2.3% / +1.3% |
| ~1 GiB | -0.9% / +0.7% | -0.8% / -1.0% | -3.4% / +0.4% | +0.4% / -1.7% |

Largest accepted p50 regression: **5.7%**. Largest throughput regression: **5.3%**.
Both satisfy the 20% gates. Representative absolute times:

| Case | Control | Candidate |
| --- | ---: | ---: |
| ~1 GiB GET, concurrency 1 | 948.724 ms | 941.211 ms |
| ~1 GiB PUT, concurrency 1 | 2,420.956 ms | 2,430.950 ms |
| ~1 GiB GET, concurrency 4 | 1,225.301 ms | 1,213.307 ms |
| ~1 GiB PUT, concurrency 4 | 5,623.894 ms | 5,528.264 ms |

Evidence: `.tmp/modernization/native-isolated.json`, `native-isolated.log` and
`get10m-steady.json`. Initial failures remain separate; they are not counted as
passes. Fresh-bucket/checkpoint-controlled results are not a claim that arbitrary
overwrite churn on an undersized WAL budget has no storage-pressure failures.

### Origin-cache acceptance

The origin was another PostgreSQL-backed S3 gateway with **20 ms one-way delay**
(approximately 40 ms RTT) and a **256 MiB/s per-request bandwidth limit**. Both
cache versions used the same origin, 1 MiB aligned range blocks and a one-hour
validation TTL. Objects were seeded directly at the origin; no whole-object
proxy read/PUT was allowed to contaminate the range-only comparison.

All 200 range/HEAD runs passed the gates. Median times at concurrency 1:

| Object size | Range before → after | HEAD before → after |
| --- | ---: | ---: |
| 4 KiB | 0.172 → 0.148 ms (-14.0%) | 0.140 → 0.130 ms (-7.1%) |
| ~10 MiB | 43.225 → 1.702 ms (-96.1%) | 41.634 → 0.171 ms (-99.6%) |
| ~64 MiB | 42.065 → 0.354 ms (-99.2%) | 41.655 → 0.154 ms (-99.6%) |
| ~256 MiB | 42.043 → 0.437 ms (-99.0%) | 41.654 → 0.154 ms (-99.6%) |
| ~1 GiB | 42.237 → 0.684 ms (-98.4%) | 41.119 → 0.157 ms (-99.6%) |

At concurrency 4, large-object ranges improved by **93.4–98.7%**, and HEAD by
**99.4–99.5%**. A direct origin-counter check on three repeated ~1 GiB footer
reads recorded:

| Gateway | Origin HEADs | Origin GETs | Response X-Cache |
| --- | ---: | ---: | --- |
| Control writable | 3 | 0 | MISS |
| Candidate writable | 0 | 0 | HIT |
| Candidate read-only, separate process | 0 | 0 | HIT |

The read-only process used the primary with write rejection enabled, not a
physical standby; the logged relation checks and shared-state tests do not
replace a real replication/promotion exercise.

Three further paired repetitions covered cold whole-object fills and warm body
reads. Five final paired write-back repetitions also include local wake hints
and the dependency security fix. Checkpoint preparation occurred outside timed
work; pending uploads were allowed to drain between measurements:

| Size / operation | Control | Candidate | Change |
| --- | ---: | ---: | ---: |
| ~10 MiB cold fill | 157.973 ms | 161.055 ms | +2.0% |
| ~10 MiB warm body | 10.434 ms | 10.497 ms | +0.6% |
| ~10 MiB write-back ACK | 58.441 ms | 63.315 ms | +8.3% |
| ~64 MiB cold fill | 586.556 ms | 581.788 ms | -0.8% |
| ~64 MiB warm body | 26.411 ms | 25.939 ms | -1.8% |
| ~64 MiB write-back ACK | 252.381 ms | 249.241 ms | -1.2% |
| ~1 GiB cold fill | 8,410.639 ms | 8,397.532 ms | -0.2% |
| ~1 GiB warm body | 944.061 ms | 939.189 ms | -0.5% |
| ~1 GiB write-back ACK | 3,303.447 ms | 3,319.613 ms | +0.5% |

No undrained backlog was hidden behind the ACK numbers. With local wake hints,
median time after ACK until outbox drain was **0.449 → 0.181 seconds** at ~10 MiB
(-59.8%), **0.878 → 0.746 seconds** at ~64 MiB (-15.0%), and
**11.196 → 10.943 seconds** at ~1 GiB (-2.3%). Every pending body stayed logged
and readable. The largest final request-latency regression across the native and
cache checks was **8.3%**, within the 20% gate.

The earlier no-wake samples remain in `cache-bodies.json`: small idle-drain times
were 0.459 → 0.665 seconds at ~10 MiB, beyond a 20% comparison because idle polling
dominated the short operation. That result was not labeled upload throughput or
silently accepted. Local hints address the idle delay without requiring NOTIFY,
a new service or local authoritative queue state. Delivery remains at least once,
not an exactly-once guarantee.

Evidence: `.tmp/modernization/cache-ranges.json`, `cache-bodies.json`,
`cache-bodies.py`, `writeback-wake.json` and `origin-request-counts.json`.

## Final dependency/security check

The full vulnerability check found pre-existing **GO-2026-6617** in
`golang.org/x/net v0.59.0`. Production now uses the fixed **v0.60.0**; no AWS or
MinIO dependency was added to the gateway module. A same-database follow-up
compared the pre-patch candidate with the final binary: 4 KiB HEAD
0.106 → 0.109 ms, concurrent ~10 MiB GET 10.567 → 10.791 ms, ~1 GiB GET
939.534 → 946.253 ms, and concurrent ~1 GiB footer ranges 0.284 → 0.287 ms.
All were within 3%, without errors. The primary matrix above predates only this
HTTP/2 dependency security patch and local outbox wake hints (which do not run
on native read paths); the representative final-binary check is
recorded in `.tmp/modernization/security-http-smoke.json`.

## Reproduction and evidence

`test/loadgen` now supports fixed S3 request counts, unsigned comparisons,
Parquet payload files and range offsets. It checks body-copy errors rather than
counting a truncated response as success.

`deploy/bench/compare.py` drives that existing load generator; it is not a new
load engine or conformance framework. It requires disposable endpoints/databases.
`--postgres-before` / `--postgres-after` enable explicit ANALYZE/checkpoint
preparation, and `--isolate-writes` creates/drops a fresh bucket per PUT run.

The measured run records and client benchmark output are also retained in
[benchmarks/modernization-2026-10.json](benchmarks/modernization-2026-10.json),
without database credentials. It includes the rejected short-run gate and its
explicit steady-state replacement, not just favorable aggregate numbers.

Example, after building control/candidate binaries and the common load generator:

```sh
python3 deploy/bench/compare.py \
  --before http://127.0.0.1:19114 --after http://127.0.0.1:19124 \
  --loadgen .tmp/modernization/loadgen \
  --payload .tmp/modernization/parquet-10MiB.parquet \
  --payload .tmp/modernization/parquet-64MiB.parquet \
  --payload .tmp/modernization/parquet-256MiB.parquet \
  --payload .tmp/modernization/parquet-1024MiB.parquet \
  --postgres-before "$BENCH_BEFORE_URL" --postgres-after "$BENCH_AFTER_URL" \
  --isolate-writes --repeats 5 --out .tmp/modernization/native-isolated.json
```

Use `--seed-endpoint` with `--ops range,head` for a range-only origin-cache
comparison. Seed directly at the origin, not through the proxy: a proxy PUT or
full GET would warm the write-back/whole-body cache and invalidate that experiment.

Validation: unit/race tests, PostgreSQL integration **with the race detector**,
the pinned **36-test s3t allow-list**, codec/range fuzzing, Helm lint/render,
`hk check --all --profile ci` including vulnerability checks, and the full Docker
end-to-end contract including boto3, RESP, OJS, cache tier, DuckDB and DuckLake.
New regression tests cover owner renewal/loss, stale inline/COPY publication,
read-only range hits, metadata TTL/flush/logging, scratch cleanup/admission,
same-ETag newer-write protection and external-origin refresh fencing.

Real AWS/Aurora performance, actual replica promotion and production tail latency
remain unverified. Logged state and local commit durability are not a promise of
lossless promotion of an asynchronous replica.

## Capability-consolidation follow-up

The organization-only pass (173 → 140 Go files) was followed by a capability
refactor against the captured 140-file binary. Both use layout v24. The result
is **47 Go files**, including tests. The 10-file vendored client is now the
five-file `internal/s3client` package:

| Package | Before | After |
| --- | ---: | ---: |
| `internal/store` | 45 | 9 |
| `internal/s3` | 30 | 6 |
| `internal/objcache` | 19 | 5 |
| `internal/ojs` | 10 | 4 |
| `internal/gateway` | 4 | 2 |
| `internal/resp` | 4 | 2 |
| `internal/s3client` | 10 vendored | 5 (3 production) |

This pass also changes ownership and shared implementations:

- One native and one origin S3 adapter implement read/write/multipart
  capabilities, with one backend-selection policy. In particular, origin part
  copies cannot accidentally take the native fast-copy path.
- One registry map and replacement loop own both global and per-bucket tiers.
  Unchanged specs reuse their tier; failed replacements retain the working tier.
- Foreground and outbox mutations share local/session-lock ordering and failed
  acquisition cleanup. Body validation still precedes database-lock acquisition.
- Six ordinary stream operations now use the store's existing transaction
  lifecycle. Maintenance transactions that deliberately roll back no-op or
  missing-schema paths remain explicit.
- Both pool policies share failed-open cleanup. Writable and read-only startup
  use the same layout validation.

### Code-size accounting

Counts include all tracked Go files, first-party and formerly vendored. The
earlier 19,715-line figure excluded the S3 client while it lived under
`third_party`; it now lives in `internal/s3client`, so the first-party-only
figures before and after that move are not directly comparable.

| Whole-repo Go | Before | After | Now |
| --- | ---: | ---: | ---: |
| Files | 51 | 47 | 48 |
| Total lines | 35,750 | 35,292 | 35,440 |
| Production lines | 21,567 | 21,069 | 21,142 |
| Test lines | 14,183 | 14,223 | 14,298 |

The client itself shrank from 1,852 to 1,728 production lines and from 1,336 to
1,365 test lines while its ten files were merged into five. Adding those 1,728
production lines to the pre-move first-party count of 19,715 gives 21,443;
the actual 21,069 is therefore a net **374-line reduction** after accounting for
absorption. Declaration and statement counts cross the same boundary and are
not compared across the move.

The 13,230-line `docs/benchmarks/modernization-2026-10.json` file is benchmark
evidence, not Go code. Commit-level insertion totals that include it are not a
code-size measure. Relocation was separately checked for declaration identity
after the intentional refactors. Four added regression tests cover backend
selection, tier replacement/fallback, lock cleanup (including panicking
validation) and transaction rollback. The origin HTTP integration test also
exercises multipart part-copy routing through the unified adapter.

### Readability and code-quality follow-up

The final pass removes ordinary comments from first-party Go code. Only compiler
directives (`//go:build`, `//go:embed`) and vendored license headers remain.
Readability now comes from names, capability-sized interfaces and sectioned
implementations.

Specific simplifications:

- `pgx.Batch` now sends job rows and global index rows in one round trip instead
  of two statements per job.
- RESP `EXISTS` groups cache keys into one query while preserving duplicate-key
  semantics and `s3://` bucket reads.
- Origin registry lookups use an atomic refresh clock and reader lock; tier
  construction and closure happen outside the reader lock.
- Streamed bodies return their first pooled chunk correctly and share one
  next-chunk/error path between `Read` and `WriteTo`.
- One HTTP-origin request builder applies configured headers and trace
  propagation consistently to reads, writes and deletes.
- Namespace relation-size SQL and chunk-layout checks have one definition each.
- CRC-64/NVME uses slicing-by-8 tables and is verified against a bitwise
  reference.
- Standard library helpers replace local `itoa`, `lower`, manual hex/ETag
  validation, manual ISO-8601 assembly and one-off sorted-map loops.
- The S3 client now lives in `internal/s3client`; the misleading nested module
  and `replace` chain are gone. Loadgen and the client share one SigV4 signer,
  and the plain-HTTP origin shares the same pooled transport policy.
- S3 route handlers return errors; one mapper converts store/origin failures
  into S3 codes. 145 scattered `writeError` branches collapsed into that path.
- Store `*Ref` suffixes are gone, upload/part/completion variants use one method
  with options, and `withSchema` centralises missing-schema reads.
- S3, OJS and RESP metrics share `internal/telemetry`; parameter structs replace
  the longest positional call sites; time-driven tests use `testing/synctest`.

### Same-database paired check

A final paired check pointed both binaries at the same PostgreSQL database and
the same 10 MiB Parquet object, so page cache, statistics and chunk pool state
were identical. Four alternating 500-request runs gave:

| Workload | Control median p50 | Candidate median p50 |
| --- | ---: | ---: |
| 10 MiB GET, 1 worker | 9.19 ms | 9.22 ms |
| 10 MiB GET, 4 workers | 10.96 ms | 11.05 ms |
| 1 GiB GET, 1 worker | 931 ms | 939 ms |

The differences are below 1% and far inside the 20% budget.
Separate matrices measured HEAD and 8 KiB footer ranges within 3%. The earlier
10 MiB matrix outlier came from a temporary driver reusing one object key for
10 MiB, 64 MiB and 1 GiB payloads; it was not a gateway regression.

### Handles, splits, read views and loadgen

That round kept layout v24 and the S3, OJS and RESP behaviour. The **Now**
column above counts the tree at `2857200`.

- Namespaces are bound once. `Store.Cache(name)` and `Store.Queue(name)` validate
  the name and resolve its schema when the handle is created, so cache and stream
  methods stop repeating that work per call. OJS depends on the `store.QueueHandle`
  interface, which keeps its HTTP tests on a small fake. Lifecycle methods stay
  unexported on the concrete queue type.
- Long functions were split. Top-level production functions longer than 80 lines
  fell from 17 to 7, and the longest fell from 230 to 109 lines. The split covered
  `serve`, `OpenWithOptions`, `ChunkWriter.run`, `copyObject`, `Diagnose` and
  `StatNamespace`.
- Object reads pass one `readView` value that carries the span, size, validators
  and metadata. `newObject` is the only place that builds `&Object{...}`.
  `openRange` takes four parameters instead of twelve, and the unused `resolveCache`
  key argument is gone.
- The load generator runs every protocol through one measurement harness.
  `test/loadgen/main.go` fell from 751 to 612 lines. The 550-line target was not
  reached: the remaining lines are flag declarations and per-protocol requests.
- Not done: moving stream SQL into schema functions, which is a layout change that
  needs an explicit decision, and collapsing the in-memory and filesystem origins.
  Their keys mean different things (opaque strings versus paths), so a collapse
  would change behaviour.

Whole-repo lines rose by 148 in this round. Extracted helpers repeat signatures
(the split adds 200 net lines), the handle file and its test adapter add code, and
the loadgen harness shrank by 139. The gain is in complexity, not line count.

### Paired checks for this round

Old is the `ace432d` gateway build and new is the `7c756a2` build. The later
commit `2857200` only drops an unused argument. Both sides used the same loadgen
binary against the same PostgreSQL 18.6 database on one host (AMD Ryzen 7 7700X,
16 threads, 61 GiB RAM, PostgreSQL defaults). Sessions alternated old, new, new,
old. S3 workloads used fixed request counts of 100, 200, 400 and 6, and OJS and
RESP used 5-second runs with 4 workers. Each row is the mean over its sessions:
four, except OJS enqueue, which has eight. The 1 GiB row has only six requests per
session, so it is a coarse check.

| Workload | Old p50 | New p50 | Δ p50 | Old throughput | New throughput | Δ throughput |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 MiB GET, 1 worker | 10.13 ms | 10.58 ms | +4.4% | 861 MiB/s | 960 MiB/s | +11.6% |
| 10 MiB GET, 4 workers | 10.53 ms | 10.55 ms | +0.2% | 3,822 MiB/s | 3,837 MiB/s | +0.4% |
| 10 MiB range (1 MiB), 4 workers | 1.18 ms | 1.17 ms | -0.6% | 3,354 MiB/s | 3,354 MiB/s | 0.0% |
| 1 GiB GET, 1 worker | 957 ms | 953 ms | -0.5% | 1,001 MiB/s | 1,070 MiB/s | +6.9% |
| OJS enqueue (8 sessions) | 2.33 ms | 2.39 ms | +2.3% | 1,676 req/s | 1,619 req/s | -3.4% |
| OJS fetch | 27.48 ms | 28.46 ms | +3.6% | 1,443 req/s | 1,394 req/s | -3.4% |
| OJS stream send | 2.33 ms | 2.33 ms | -0.1% | 1,589 req/s | 1,676 req/s | +5.5% |
| OJS stream cycle | 12.28 ms | 12.47 ms | +1.6% | 321 req/s | 322 req/s | +0.3% |
| RESP SET, pipeline 4 | 1.75 ms | 1.76 ms | +0.4% | 1,976 req/s | 2,173 req/s | +10.0% |
| RESP GET, pipeline 4 | 0.033 ms | 0.033 ms | 0.0% | 115,232 req/s | 114,349 req/s | -0.8% |

Every row is inside the 20% budget. A first four-session enqueue pass showed
-11.5% throughput; the eight-session repeat is the better estimate, and the only
enqueue-path change sits inside the branch that runs before a queue is known.

Gates on `2857200`: `go build`, `go vet` with and without `-tags=integration`,
`go test -race ./...`, the integration suite under `-race` on a fresh database,
s3t (36 of 36), `hk check --all --profile ci`, and `test/e2e.sh` with two gateways.
The Docker CLI was unavailable in the working environment, so the contract run
followed `deploy/bench/stack.sh contract` step for step against local PostgreSQL
18.6 instead of the container.

### Layout v25, local origins and test trim

This round moves to layout v25 and simplifies two more areas. Layout v25 needs
fresh databases; there is no migration from v24.

- **Stream operations are layout-v25 SQL functions.** Each queue schema gets
  `stream_*` functions for append, subscribe, receive (plain and cooperative),
  batch read, ack, nack, retry processing, prune, rotate and replay. Each call is
  one statement, so a request is one round trip instead of five to thirteen.
  `stream.go` lost about 800 lines and `schema.sql` gained about 430. Go keeps
  validation, namespace creation and sentinel mapping. A missing queue or batch
  still returns the same not-found result. Partial schemas (an undefined table or
  function) propagate as storage errors instead of reading as empty, and doctor
  lists queue namespaces that lack stream functions.
- **Local origins collapse into one implementation.** `filesystem` and `ephemeral`
  share one `os.Root`-based implementation. The temporary-directory origin kept its
  old type name in this round because `origin set` accepted it; it now stores in a
  private temporary directory that is removed on close. Keys the local origin cannot represent are rejected with 400
  before any body is acknowledged. Overwrites write a temporary file under
  `.pgvs3-tmp` and rename it under the lock, so concurrent readers never see
  partial bytes.
- **The test surface is trimmed.** Integration test functions went from 100 to 70
  (excluding `TestMain`), integration-tagged test lines from 5,317 to 4,646, and
  e2e checks from 239 to 159. e2e has no fixed sleeps. Four compact tests cover
  invariants that had none: layout refusal, a single maintenance leader, chunk
  storage settings, and split pools. Shared `TestMain`, discard-logger, deadline
  poll and namespace-cleanup helpers live in `internal/testutil`. The 4,400-line
  target was not met; the largest tests need table rewrites.

| Whole-repo Go | Start of round (`b170638`) | Now (`39249fc`) |
| --- | ---: | ---: |
| Files | 48 | 48 |
| Total lines | 35,440 | 34,817 |
| Production lines | 21,142 | 20,770 |
| Test lines | 14,298 | 14,047 |

#### Paired checks, layout v25

Layout v24 and v25 binaries cannot share a database, so each side ran against its
own fresh PostgreSQL 18.6 database, prefilled by its own gateway. Old is the
`b170638` build and new is the `0142d1d` build, which has the same product code as
`39249fc`. Sessions alternated old, new, new, old on the same host as the earlier
checks. S3 rows used fixed request counts. OJS and RESP rows used 5-second runs with
4 workers.

| Workload | Old p50 | New p50 | Δ p50 | Δ throughput |
| --- | ---: | ---: | ---: | ---: |
| 10 MiB GET, 1 worker | 10.69 ms | 10.65 ms | -0.4% | -10.2% |
| 10 MiB GET, 4 workers | 10.57 ms | 10.56 ms | 0.0% | -0.9% |
| 10 MiB range (1 MiB), 4 workers | 1.19 ms | 1.19 ms | -0.3% | -0.6% |
| 1 GiB GET, 1 worker | 965 ms | 957 ms | -0.9% | +1.1% |
| OJS enqueue | 2.32 ms | 2.33 ms | +0.4% | -4.7% |
| OJS fetch | 28.82 ms | 28.75 ms | -0.3% | +2.7% |
| OJS stream send | 2.38 ms | 2.34 ms | -1.5% | +1.9% |
| OJS stream cycle | 11.54 ms | 10.74 ms | -7.0% | +15.1% |
| RESP SET, pipeline 4 | 1.75 ms | 1.75 ms | +0.3% | -3.0% |
| RESP GET, pipeline 4 | 0.033 ms | 0.033 ms | 0.0% | 0.0% |

Every row is inside the 20% budget. The stream cycle improvement fits the round-trip
reduction. The -10.2% single-worker 10 MiB throughput has a flat p50 and is within
run-to-run noise at 100 requests; the previous round measured +11.6% on the same path.

Gates on `39249fc`: gofmt, vet with and without `-tags=integration`,
`go test -race ./...`, the integration suite under `-race` on a fresh database
(45 s, 0 failing), s3t (36 of 36), `hk check --all --profile ci`, and the contract
suite (16 s). The Docker CLI is unavailable here, so the contract ran against local
PostgreSQL 18.6, as in the previous round.

### Catalog bucket rule, ephemeral rename, fault/interop trim and streaming-upload fix

This round completes the three deferred items and fixes a gateway bug that the
interop suite exposed.

- **`inmem` is renamed to `ephemeral`** (`64ea56f`). The origin stores in a private
  temporary directory that is removed on close, so the old name was misleading.
  Persisted specs that use `inmem` no longer parse.
- **Origin-served requests require a catalog bucket** (`721b742`). Every bucket-scoped
  S3 request on an origin-served bucket checks the catalog first and returns 404
  `NoSuchBucket` if the bucket has no row. A global origin serves only buckets created
  with `PUT /{bucket}`, and `PUT` on an origin bucket creates the row. A DELETE on a
  missing bucket now returns 404 instead of 204. Origin-only test servers keep the old
  behaviour.
- **`test/fault.sh` and `test/interop.sh` are fixed and trimmed** (`7267b34`, `4cda816`).
  `fault.sh` was failing before this round because it wrote to a bucket it never
  created. Fixed sleeps are replaced by deadline polls, SKIP branches are gone, and
  missing tools fail.
- **Streaming SigV4 uploads are sized by their decoded length** (`430ebf4`). restic
  uploads small objects with `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`, where the wire
  `Content-Length` includes chunk headers and signatures. `putObject`, `prepareIngest`
  and `uploadPart` fell back to the wire length, so the inline read hit EOF early and
  returned 500. They now use `x-amz-decoded-content-length`, and a missing or invalid
  decoded length returns `InvalidRequest`.

| Whole-repo Go | Start of round (`39249fc`) | Now (`430ebf4`) |
| --- | ---: | ---: |
| Files | 48 | 48 |
| Total lines | 34,817 | 35,048 |
| Production lines | 20,770 | 20,825 |
| Test lines | 14,047 | 14,223 |

#### Paired checks, catalog bucket rule

Old is `c15bbfb` and new is `430ebf4`. Origin rows used a global filesystem origin
and a Warmed cache, so every measured request is a cache hit. Sessions alternated
old, new, new, old. A separate three-way run compared `prev` (`c15bbfb`), `head`
(`430ebf4`) and `nocheck` (`430ebf4` with the catalog check disabled) across three
rotated rounds of 4,000 requests.

| Workload | Old p50 | New p50 | Δ p50 | Δ throughput |
| --- | ---: | ---: | ---: | ---: |
| Origin-served 10 MiB GET, 1 worker | 10.37 ms | 10.49 ms | +1.1% | -0.2% |
| Origin-served 10 MiB GET, 4 workers | 10.61 ms | 10.53 ms | -0.8% | +1.5% |
| Native 10 MiB GET, 1 worker (control) | 10.50 ms | 10.45 ms | -0.5% | -1.4% |
| Origin-served 4 KiB GET, 4 workers | 0.145 ms | 0.185 ms | +27.9% | -20.7% |
| Same 4 KiB GET with check disabled | 0.145 ms | 0.145 ms | +0.5% | -0.6% |

The 10 MiB target workload is unaffected. The 4 KiB origin-hit row crosses the 20%
rule because the per-request `to_regclass` check adds about 40 µs, and the three-way
run shows the check is the entire cost. This is an accepted exception: keeping the
per-request check preserves immediate catalog authority across gateways when a bucket
is dropped, without a layout bump or an in-process cache.

Gates on `430ebf4`, with the host Docker CLI: gofmt, vet with and without
`-tags=integration`, `go test -race ./...`, the integration suite on a fresh database
(0 failing), s3t (36 of 36), `hk check --all --profile ci`, and `deploy/bench/stack.sh`
in `contract`, `fault` and `interop` modes (all exit 0, with restic passing).

At the end of that round, native invalid bucket names could still return 500 while
origin-served requests returned 404. The contracts/validation follow-up below
resolves this inconsistency with `400 InvalidBucketName` on both paths.

### Explicit ownership and verification follow-up (2026-10-29)

This pass keeps layout v25 and leaves `schema.sql` byte-identical. It addresses
five maintenance boundaries rather than moving files or targeting a line count:

1. `objcache.Runtime` owns the shared fill/pending limits and disk-spool budget.
   Immutable `TierOptions` replace package configuration and post-construction
   setters. Both persisted and fallback tiers use the gateway's runtime, with
   settings finalized before workers start.
2. PUT and multipart use one upload-body descriptor for effective lengths,
   integrity readers and trailer verification. Ordinary Content-Length and the
   existing multipart length hint keep their behavior. A parsed-HTTP regression
   also covers trailers allocated at EOF across shallow request copies.
3. S3 resolves and borrows a backend once per request bucket, including copy
   sources. Replaced tiers close only after their last borrower releases them.
   The registry owns shutdown of persisted and fallback tiers. These references
   carry resource lifetime, not cached bucket existence; catalog checks remain.
4. Store errors distinguish an absent namespace from a damaged existing schema.
   Catalog probes run only after lookup errors, transactions roll back before
   classification, and missing tables/functions are no longer silently treated as
   empty resources or repaired by ordinary writes. One-connection integration cases
   cover metadata, cache, queues and multipart operations.
5. `just ci` matches the GitHub gate set. Docker-backed suites share setup helpers,
   use isolated binaries/logs and keep explicit PostgreSQL port mappings stable
   across restart. Failures retain artifacts and restore the caller's output
   streams before printing diagnostics; success prints one summary. Kind also owns
   its kubeconfig, cluster name and image tag, and creates its bucket explicitly.

| Whole-repo Go | Before (`49b3f0a`) | After (`5069038`) |
| --- | ---: | ---: |
| Files | 48 | 48 |
| Total lines | 35,048 | 35,479 |
| Production lines | 20,825 | 20,993 |
| Test lines | 14,223 | 14,486 |

Production grows by 168 lines and tests by 263. The gain is explicit ownership,
one upload contract and narrower error policy, not a code-size reduction. No
non-directive Go comments or new Go files were added.

#### Paired performance check

The `49b3f0a` control and the final Go tree at `5069038` used the same PostgreSQL
18.6 database on the Ryzen 7 7700X host
(16 threads, 61 GiB RAM, 128 MiB PostgreSQL shared buffers), alternating before,
after, after, before. Payloads were incompressible. The per-bucket filesystem
origin used a one-hour validation TTL and 64 KiB aligned-range caching, configured
on its persisted spec. Cold keys were distinct per session; pending uploads were
drained before switching binaries. Each row averages two sessions per binary.

Request counts per session were: native GET 200/400/20/8 for the first four rows;
footer ranges 1,600; HEAD 2,000; warm origin GET 100/8,000; cold origin GET 20/4/1;
cold/warm aligned ranges 20/800; native and write-back PUT 12 each. The cold 1 GiB
row therefore has only two observations per binary and is a coarse check.

| Workload | Before p50 | After p50 | Throughput change |
| --- | ---: | ---: | ---: |
| Native 10 MiB GET, 1 worker | 9.63 ms | 9.45 ms | +3.3% |
| Native 10 MiB GET, 4 workers | 10.61 ms | 10.61 ms | +0.1% |
| Native 100 MiB GET, 1 worker | 94.42 ms | 93.72 ms | +1.8% |
| Native 1 GiB GET, 1 worker | 965 ms | 955 ms | +1.2% |
| Native 8 KiB footer range, 4 workers | 0.167 ms | 0.171 ms | -2.0% |
| Native HEAD, 4 workers | 0.097 ms | 0.095 ms | +3.1% |
| Warm origin 10 MiB GET, 1 worker | 9.40 ms | 9.49 ms | -0.5% |
| Warm origin 4 KiB GET, 4 workers | 0.164 ms | 0.157 ms | +4.8% |
| Cold origin 10 MiB GET, 1 worker | 53.12 ms | 55.17 ms | -3.4% |
| Cold origin 100 MiB GET, 1 worker | 475 ms | 486 ms | -4.4% |
| Cold origin 1 GiB GET, 1 worker | 5,833 ms | 5,837 ms | 0.0% |
| Cold origin 64 KiB range, 1 worker | 6.04 ms | 5.98 ms | +3.5% |
| Warm origin 64 KiB range, 4 workers | 0.495 ms | 0.500 ms | -0.8% |
| Native 10 MiB PUT, 1 worker | 38.98 ms | 38.48 ms | +0.7% |
| Origin 10 MiB write-back acknowledgement, 1 worker | 52.44 ms | 52.91 ms | -1.5% |

All measured latency and throughput changes are within 5%, with zero errors. This
comparison includes the already accepted per-request catalog check on both sides;
it does not withdraw or remeasure the earlier small-object exception.

#### Verification

The complete `mise exec -- just ci` passed using the host's rootful Docker context:
hk/goimports/gofmt/go fix/tidy/vet/staticcheck/govulncheck, integration-tagged vet,
race tests, uncached race integration (45 s), four fuzz targets, Helm lint,
Helm/kind (69 s), contract (19 s), fault (12 s), interop including restic (17 s),
and s3t (36 allowed tests; 12 s including fixture setup). ShellCheck and explicit
nested-failure diagnostics probes also pass. Independent contract and interop
runs passed concurrently with isolated fixtures.

The selected rootless Podman context could not satisfy kind's cgroup check. The
successful run selected the already available rootful Docker engine with
`DOCKER_CONTEXT=default` and its socket for that command only; no host settings,
default contexts or gate requirements were changed.

### Function, publication and benchmark contracts (2026-10-29)

The next pass keeps layout v25 and leaves `schema.sql` byte-identical to `7f533ae`.
It completes five further maintainability changes:

1. **Exact Go/PostgreSQL contracts:** manifests cover 11 global, seven object and
   13 stream functions. One catalog query checks input signatures, result types,
   set cardinality and returned column names/types/order at startup and in doctor.
   Regression cases include missing functions, wrong overloads and wrong results.
   No checks are added to object requests.
2. **Canonical paired benchmarks:** `load.sh` reuses the shared fixture harness,
   with frozen before/after binaries and separate databases. `compare.py` remains
   the sole paired driver over the existing Go loadgen. It verifies payload hashes,
   object/range sizes and bytes, persisted origin settings, cache preconditions and
   completed fills. Results record binary/payload hashes, explicit cache paths,
   raw samples and independent latency/rate gates. Interrupted reports remain
   `complete: false`; fresh outputs never overwrite earlier evidence. The existing
   exploratory S3/RESP/OJS matrix remains, now with isolated scratch and NDJSON.
3. **Explicit publication authority:** immutable normal/fill/write-back intents
   replace the loose origin/fill/pending fields in `WriteOptions`. Fill leases are
   copied, pending bodies cannot expire, and ordinary versions cannot impersonate
   reserved pending/deleted state. Lock skipping is private to store-owned scoped
   writers; released/mismatched scopes fail before publication. Existing SQL,
   streaming loops, shared fills and outbox fencing remain.
4. **Complete persisted-spec validation:** SaveSpec and NewFromSpec share duration,
   limit, namespace and scope normalization. Provider validation is separate from
   opening resources. Malformed options and HTTP headers fail before catalog
   changes, while external availability/permission errors remain runtime errors.
5. **Consistent malformed-name errors:** destination and copy-source bucket names
   are checked before backend selection. Native and origin-backed operations now
   agree on `400 InvalidBucketName`; valid missing buckets retain `404 NoSuchBucket`.

The original 300 ms wall-clock renewal test failed once under CI scheduling. It now
waits for an observed database expiry extension rather than sleeping across several
short TTLs. The competitor and stale-publication assertions are retained; the
production lease duration is unchanged. Loadgen also gains opt-in `-unique-reads`,
tested with uneven worker progress, so concurrent cold samples really use distinct
keys without changing its default workload.

| Whole-repo Go | Before (`7f533ae`) | After (`1ff9994`) |
| --- | ---: | ---: |
| Files | 48 | 49 |
| Total lines | 35,479 | 36,051 |
| Production lines | 20,993 | 21,357 |
| Test lines | 14,486 | 14,694 |

The extra Go file tests loadgen; Python's standard-library tests cover benchmark
guards and run in CI. This is an explicit-contract and reproducibility pass, not
a claim of fewer lines. No non-directive Go comments were added.

#### Canonical acceptance results

The saved `7f533ae` control was compared with the final Go tree using the new runner,
not an ad hoc script. Environment: Ryzen 7 7700X, 16 threads, 61 GiB RAM, loopback,
PostgreSQL 18.6 with 1 GiB shared buffers and pool 32 per gateway. Origin cases use
separate per-bucket filesystem roots with one-hour TTL and persisted 64 KiB range
chunks. Payloads are incompressible. Cold means an empty gateway cache, not an
emptied operating-system page cache. Outboxes drain between samples.

All 51 large-object cases passed their independent +20% p50 / -20% request-rate
gates, with 306 raw sample runs and zero request errors. Each case has three
alternating paired repetitions. Counts are recorded in the reports: short metadata
and range samples use 100 or 300 requests; GET budgets depend on size/state; PUT
samples use seven requests unless explicitly overridden. The 1 GiB GET warm samples
use four requests and the cold/PUT samples use one, so the latter are coarse checks.

| Profile | Cases | Largest p50 increase | Largest rate reduction |
| --- | ---: | ---: | ---: |
| Native 10/100 MiB HEAD, GET, 8 KiB range, PUT; workers 1/4 | 16 | 11.0% | 8.3% |
| Native 1 GiB GET; workers 1/4 | 2 | 1.9% | 1.0% |
| Warm origin 10/100 MiB HEAD, GET, 64 KiB range, PUT; workers 1/4 | 16 | 11.1% | 8.3% |
| Cold origin 10/100 MiB HEAD, GET, 64 KiB range; workers 1/4 | 12 | 9.5% | 5.0% |
| Warm origin 1 GiB GET; workers 1/4 | 2 | 1.1% | 2.6% |
| Cold origin 1 GiB GET; worker 1 | 1 | none (-1.9%) | none (+1.9%) |
| Native 1 GiB PUT; worker 1 | 1 | 4.7% | 4.4% |
| Origin 1 GiB write-back acknowledgement; worker 1 | 1 | none (-3.6%) | none (+3.8%) |

Representative p50s: native 10 MiB GET c1 9.894→9.917 ms; native 100 MiB GET c1
79.437→79.581 ms; native 1 GiB GET c1 850.082→866.048 ms; cold origin 100 MiB GET
c1 386.469→383.615 ms; cold origin 1 GiB GET 4,646→4,559 ms. Native 1 GiB PUT was
2,090→2,188 ms and origin write-back acknowledgement 3,129→3,016 ms.

Separate 4 KiB native/warm/cold probes also passed. The largest small warm-range
movement was 0.161→0.186 ms (+15.5%), still inside budget and not hidden by the
large-object results. Both binaries retain the already accepted catalog lookup.

The server restart interrupted the first native 1 GiB PUT comparison after one
sample. Its incomplete report was kept; only that incomplete case and the not-yet
started origin PUT case were run afterward, with fresh reports. Completed CI and
read/fill/range work was not repeated.

#### Verification

The complete `mise exec -- just ci` passed against the rootful Docker context:
format/import/modernizer/tidy/vet/staticcheck/vulnerability checks, integration-tagged
vet, race-enabled Go tests, five Python tooling tests, uncached race integration
(45 s), four fuzz targets, Helm lint and kind (78 s), contract (20 s), fault (12 s),
interop including restic (16 s), and s3t (21 s including setup; 36 allowed tests).
ShellCheck passed. The preserved exploratory matrix completed a 100 ms fixture
smoke across S3/RESP/OJS/origin modes; high-concurrency saturation errors were
reported explicitly and were not treated as acceptance results.

The [live file map](https://github.com/adonm/pgvs3/blob/main/CONTRIBUTING.md#package-map) describes the final structure.
Package/build-tag boundaries, the isolated SDK benchmark module, streaming
algorithms and logged durability/coordination semantics are preserved.
