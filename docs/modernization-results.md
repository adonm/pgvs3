# Modernization implementation and performance comparison

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

| Whole-repo Go | Before | After |
| --- | ---: | ---: |
| Files | 51 | 47 |
| Total lines | 35,750 | 35,292 |
| Production lines | 21,567 | 21,069 |
| Test lines | 14,183 | 14,223 |

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

The [live file map](../CONTRIBUTING.md#package-map) describes the final structure.
Package/build-tag boundaries, the isolated SDK benchmark module, streaming
algorithms and logged durability/coordination semantics are preserved.
