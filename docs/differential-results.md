# Differential evaluation and S3 burst admission

Date: 2026-10-31. PostgreSQL 18.6, DuckDB 2.0.0, layout **v25**.

**Keep DuckLake and the existing storage roles as the default.** Differential is
now an opt-in, pinned comparison in the standard setup. Small native DDL can be
cheaper, but that does not establish a universally better PostgreSQL-backed
DuckDB configuration. In particular, the release's synchronous-WAL bulk limit
and writer-fencing boundary prevent a blanket replacement recommendation.

The accompanying S3 admission fix is independent: short request bursts now queue
within bounds instead of immediately triggering client retry backoff.

## What was compared

See [differential.md](differential.md) for provisioning, exact contracts and the
measurement method. Inputs are deterministic Snappy Parquet, approximately
10 MiB, 100 MiB and 1 GiB. The three profiles are:

- DuckLake's PostgreSQL catalog plus native pgvs3 object bytes, with data inlining
  disabled.
- Differential ordinary statements **plus synchronous snapshot export** before
  the measured remote acknowledgement.
- Differential `SHIP_WAL 'sync'`, with separately timed snapshot visibility.

These compare remote durability under the stated single-writer/primary policy,
not identical reader-refresh semantics. A plain Differential head reader sees
the last exported snapshot, not every shipped commit. Fresh-reader results include
attachment, schema discovery and first query; process/extension setup is separate.
Warm repeats can use native DuckDB buffers and must not be called shared-cache
or PostgreSQL speedups.

The final matrix completed **24 cohorts**: three repetitions of every profile
at 10/100 MiB, and of DuckLake/snapshot export at 1 GiB. One 1 GiB sync-WAL attempt
failed its remote-ACK contract; its other two repetitions were explicitly marked
unavailable rather than rerunning a known size limit. All 96 per-path comparisons
retain sample counts and availability. Timed successful samples contain no 5xx
responses. Profile order rotates between repetitions.

Host: Ryzen 7 7700X / 16 threads / 61.9 GiB RAM, Linux amd64 on local Docker.
PostgreSQL uses 1 GiB shared buffers and a 4 GiB WAL target; clients use four
execution threads, a 2 GB memory limit and 16-request HTTP/SDK caps. This is
loopback, not a WAN or cloud-service comparison.

## Measured client latency

Medians in milliseconds. Empty-table DDL and small appends have 15 statement
samples per profile/size; other displayed measurements have three cohorts.

| Input / operation | DuckLake | Snapshot: statement + export | Sync-WAL commit |
| --- | ---: | ---: | ---: |
| 100 MiB / CREATE empty table | 21.20 | 32.31 | 4.26 |
| 100 MiB / ADD column to loaded table | 19.08 | 48.46 | 10.12 |
| 100 MiB / 32-row append | 7.15 | 38.18 | 4.83 |
| 100 MiB / bulk CTAS | 276.95 | 393.90 | 745.36 |
| 1 GiB / bulk CTAS | 1,682.30 | 3,083.57 | **Failed: epoch limit** |
| 100 MiB / fresh attach + schema + full query | 102.74 | 100.58 | 97.21 |
| 1 GiB / fresh attach + schema + full query | 426.64 | 433.72 | Unavailable |
| 1 GiB / full query, same-client repeat | 375.22 | 153.72 | Unavailable |

**The schema hypothesis holds for sync-WAL's recoverable commits:** approximately
4.3 ms versus 21 ms for empty-table creation. It does not hold for synchronous
snapshot visibility on every statement. The 100 MiB sync bulk load is about
2.7× DuckLake's latency, and the 1 GiB bulk transaction fails outright.

Fresh full-query totals are similar once attachment/schema work is included.
For the 100 MiB scan, attachment alone is 13.1 ms for DuckLake versus 46.1/41.9 ms
for snapshot/sync. Native warm buffers improve repeats, but this is not evidence
of a faster shared store, and other client cache settings can change that result.

Snapshot export misses the 20% latency budget in **46 of 96** reported paths.
Sync-WAL has **11 measured misses among 64 paths**, plus 32 unavailable paths at
1 GiB. These remain visible; neither profile passes a blanket adoption gate.

## Measured storage cost

The footprints include the preceding DDL/history workload, not just one isolated
file. Point inventories are not peak RSS or a hard scratch budget. MiB below:

| 100 MiB input, after load | Current S3 object bytes | Allocated PG bucket + catalog relations | Writer-local files |
| --- | ---: | ---: | ---: |
| DuckLake | 100.1 | 94.1 | 0 |
| Snapshot export | 73.7 | 58.4 | 72.6 |
| Sync-WAL | 115.3 | 114.7 | 60.3 |

Snapshot export uses less PostgreSQL storage here but adds a material client-local
copy. Sync-WAL adds recovery-base/epoch costs. Those are not free merely because
the native SQL catalog is efficient.

| 100 MiB input, after measured cleanup | DuckLake | Snapshot | Sync-WAL |
| --- | ---: | ---: | ---: |
| Current object bytes after half deletion + rewrite/GC | 54.6 | 63.8 | 71.8 |
| Current object bytes after DROP + quiescent GC | <0.001 | 0.805 | 63.3 |
| Local files after writer exit and finalizer exit | 0 | 0.93 | 72.9 |

The data table is absent in post-DROP readers; the retained bytes are not evidence
that it is still queryable. Current native images, shared packs, recovery chains
and metadata can retain storage. Compaction returned roughly 0.23–0.88 MiB of
SQLite pages per call in the 100 MiB snapshot cohorts, not tens of MiB of payload.
Closing/reopening the writer and collecting retired segments is measured
separately from that metadata operation.

After the 1 GiB snapshot DROP/cleanup, current objects still occupy 11.7 MiB and
allocated PostgreSQL relations approximately 589 MiB. Ordinary vacuum can leave
reusable interior pages when small live data remains at the heap tail. DuckLake's
corresponding post-DROP bucket is about 4.9 MiB, plus catalog relations. Neither
logical deletion nor `differential_compact` promises filesystem compaction.

## Recovery and storage findings

The release artifact successfully exercises SDK multipart uploads/SHA-256
metadata, conditional publication, ranged native-pack reads and fresh-repository
recovery after an abrupt writer exit. Recovery continues shipping with the same
identity, with explicit branch-head takeover for subsequent public snapshots.
Fresh recovery still verifies after remote collection.

Two negative findings remain visible:

1. **Large synchronous commit:** the 24,268,800-row / 1,074,782,393-byte Parquet
   fixture's bulk CTAS exceeds Differential's **256 MiB shipping-epoch limit**.
   The SQL commit reports failure after becoming durable locally, without the
   promised remote acknowledgement. This is a client limit, not pgvs3's object
   size limit. The benchmark does not hide it by retrying, batching or switching
   shipping off. Local state must be reconciled before retrying an INSERT.
2. **Unfenced old repository:** after identity-preserving recovery, restarting the
   old repository acknowledged another WAL commit and produced a newer chain.
   A later fresh recovery lost the newer writer's marker. Conflicting snapshot
   publication was rejected; that was **not** a global WAL-writer fence. Fence
   the old writer and repository before takeover and never reuse that stale
   repository as another writer.

Reclamation is also not one interchangeable operation. Differential's compact
call returns **SQLite metadata pages**. Local segment collection, active-journal
retirement, remote pack/chain retention, pgvs3 reaping and PostgreSQL vacuum are
separate costs. The latest native images and recovery roots can retain bytes
after SQL deletion. DuckLake's explicit 0.5 delete threshold actually selects the
half-deleted file for rewriting; the no-option call did not.

These are release-client probes on Linux amd64, not complete upstream acceptance,
power-loss, arbitrary partition, follower or shared-writer qualification.
Upstream still reports `v1_complete=false`.

## S3 admission: bounded waiting, not unbounded buffering

The original S3 guard returned `503 SlowDown` immediately once 32 requests were
active. A real Parquet prefetch burst with HTTP pool capacity 64 hit that guard,
even though individual reads could finish quickly. Retrying those errors added
about a second to the query. OJS already waited on admission; S3 did not.

The new S3 behavior preserves the uncontended acquisition fast path and caps
waiting at **twice the active concurrency** (64 waiting by default), for at most
**one second** by default. Cancellation/deadlines remove waiters, and waiting
occurs before application body reads, origin borrowing or PostgreSQL acquisition.
A full queue or expired budget still gets 503/`Retry-After`; scratch-byte admission
remains fail-fast. `--s3-queue-wait=0` restores immediate rejection. Health/readiness
and metrics stay outside the guard. See [operations.md](operations.md#admission-control).

Five alternating pairs scanned the same roughly 100 MiB Parquet object through
the saved and updated gateways against one shared PostgreSQL fixture:

| Observation | Immediate rejection | Bounded queue |
| --- | ---: | ---: |
| Median full query | 1,037.8 ms | 66.0 ms |
| Retried 503s per query | 7–16 | 0 |
| Requests entering the queue per query | Not implemented | 16 |
| Queue rejections | Immediate concurrency rejection | 0 |

All returned row-count/key/string/hash aggregates matched. This is **retry
avoidance**, not a Differential engine optimization or a promise that unlimited
load will be served without rejection.

The separate uninstrumented regression checks cover **45 paths / 438 raw sample
runs**: native metadata/GET/range/PUT at 4 KiB, approximately 10/100 MiB, large
GETs at workers 1/4 and a bounded 1 GiB PUT; warm origin hits, durable write-back
and cold whole/range fills. All paths passed both independent 20% p50 and rate
gates with zero request errors. The largest accepted p50 increase was **12.4%**;
the largest rate reduction was **8.9%**, both the cold 10 MiB range/four-worker
case. This validates the admission change, not replacement of DuckLake.

Control gateway SHA-256:
`c1ac63728f9eb0979d8513d5344a680aa73367da47f26004a97738fb0f784067`.
Candidate gateway SHA-256:
`43d6b13f977bed3ca4ceadacf65df041394cc58aa43d8c0e57944b253ac2a2bd`.

## Reproducibility and retained trials

`just differential` retains worker jobs/results, source/input/binary hashes,
resource snapshots and per-path timing comparisons under its printed
`.tmp/verify/bench.*/` directory. A known unsupported profile remains a failed
remote acknowledgement and is not assigned zero latency. The experiment can
finish enumerating cases while exiting nonzero for that recorded failure.
Raw transcripts/implementation notes remain ignored rather than entering source
control.

Final client report: `.tmp/verify/bench.M0hxvM/differential.json`, SHA-256
`1b3f5308a25c458512a7f77b48e7b57b79a9db33f879a969097f9bf67fb27b1b`.
It records Differential v1.1.0, DuckLake `417a9117f5`, httpfs `77392a7207` and
postgres scanner `c0f5a3c0c4`, along with tool/input/extension hashes. The
standalone burst report is `.tmp/verify/s3-queue-burst.6NK2GQ/burst.json`, SHA-256
`e323d713c32f3d322fa38dbf5b68882c57cd7477cf00c821982756fd1514ecb3`.

Preserved earlier trials include the initial metadata-only smoke, the overly
strong automatic-fencing assertion, snapshot-number/head-takeover probes and the
first repeated comparison with retried DuckLake 503s. That last scan gap is not
credited as a native-engine win. Matching the HTTP/SDK request caps removed the
throttling before the client comparison. The admission fix was then measured
separately with the larger HTTP burst.

One direct-runner origin PUT attempt stopped because its isolation guard found
the previous worker-count case's tombstones in the otherwise-owned cache. It is
retained as an incomplete fixture trial. The two worker counts were checked in
separate fresh fixtures; the guard was not relaxed or changed to truncate
unrelated data.

No embedded SQL/layout changes, UNLOGGED substitutions, gateway-local object
authority, authentication or gateway TLS were introduced. Existing analytical,
object, cache/staging and companion paths remain available.

## Final verification

The complete `just ci` passed: hk formatting/modernizers/import/tidy/vet/
staticcheck/vulnerability checks, integration-tagged vet, race-enabled Go tests,
16 standard-library Python tooling tests, uncached race integration (46 s), four
fuzz targets, Helm lint and kind (81 s), contract including the two-role
analytical walkthrough (32 s), physical-replica/promotion/restore fault checks
(91 s), interop (16 s) and s3t (16 s including setup). ShellCheck and local
documentation-link checks passed separately. The Differential experiment remains
opt-in; its explicitly unsupported workload is not silently added as a passing
default CI contract.

The final legacy-runner smoke exposed missing explicit `bench`/`lake` bucket
bootstrap, predating this evaluation. The legacy-only startup fix was verified
with `just bench compat --passes 1` and a small `just bench s3` smoke. It does not
change the measured Differential branch, timing/client sources or gateway binary;
the raw report retains the pre-bootstrap `stack.sh` checksum.
