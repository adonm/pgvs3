# Object data-plane proof and capacity evidence

Date: 2026-10-29. Control: `07e8731`; layout **v25** on both sides.

This pass implements the three priorities in [roadmap.md](roadmap.md): prove the
PostgreSQL operating model, maintain one analytical scenario and measure the
byte-store trade-off. It does not add a protocol or replace the storage engine.
`schema.sql` is byte-identical; acknowledged bodies and shared fills remain logged.

## Replica, promotion and recovery proof

`just fault` extends the existing disposable Docker harness with a PostgreSQL
18.6 primary, two physical standbys and an independent S3 origin fixture.
`pg_is_in_recovery()`, streaming state and replay LSNs are asserted, not inferred
from a gateway's `--read-only` flag.

- Native Parquet, 80 MiB sequential reads, multipart snapshot reads, whole-object
  cache hits, aligned blocks/parent metadata and two pending bodies are read from
  the real standby after replay. Bytes, metadata and public ETags match; shared
  hits generate no origin requests. Mutations remain rejected on the read-only
  gateway.
- `synchronous_commit=on` plus one named synchronous standby defines the eligible
  promotion target. The old primary/writable fleet is fenced. An in-flight COPY
  publishes no partial object; the promoted writer preserves the acknowledged
  native, single-write-back and multipart data.
- A second standby's receiver is deliberately disconnected before a marker is
  acknowledged. Promoting that ineligible, unreplayed copy loses the marker while
  retaining its earlier baseline. This is the expected asynchronous durability
  boundary, not an accepted promise of lossless arbitrary promotion.
- A consistent whole-database `pg_dump`/`pg_restore` carries native bytes, pending
  bodies, metadata and outbox state together. Reads and `doctor` succeed before
  resuming uploads. The pending restore uses a fresh isolated recovery-origin
  bucket; this is not a restore of an entire external origin or a PITR exercise.
- After fencing the old fleet, the fixture injects lease expiry to bound the test.
  New writes replace old pending tokens; upload drain preserves the newer bytes,
  and delete tombstones prevent resurrection in the exercised workflow. Session
  locks are not fencing across two independently writable PostgreSQL timelines.

The initial restore drill found a real liveness gap: persisted origin tiers were
opened only by S3 traffic. `Registry.Start` now owns a cancellable one-second
refresh, joined by `Close`, so an idle restored gateway resumes pending work.
The request catalog check, tier borrowing and read-only no-fill/no-flush rules
remain. No HA controller or exactly-once PostgreSQL/origin commit is implied.

## One maintained analytical scenario

`just analytics` runs the README's actual DuckDB/DuckLake SQL on disposable
gateways; `just contract` reuses it. The 19,438,062-byte Parquet object has one
million rows, the expected aggregates and valid footer ranges. Native and
origin-backed queries run through another gateway with client caches disabled.
The S3 origin fixture counts calls, proving shared whole/range hits rather than
mistaking a client-local cache for shared storage.

The scenario also reads a PostgreSQL-only pending body while origin writes fail,
then verifies the origin copy and stable ETag after drain. DuckLake tables reopen
through another gateway in each role. Data inlining is disabled for that example
so the table exercises object files; catalog/object commits remain separate.

## Resource method

The existing `test/loadgen` remains the only paired load engine. Resource mode
adds observations and lifecycle checks, not another benchmark framework:

- Separate primary clusters prevent before/after WAL accounting from mixing.
  Each optional real standby is checked against its primary's system identifier.
- Reproducible Snappy Parquet and seeded block-incompressible inputs target
  10 MiB, 100 MiB and 1 GiB. Actual sizes, SQL/seeds and hashes are retained.
- Snapshots record logical object-row bytes, heap/index/total relation footprint,
  WAL positions, pending state, checkpoints, pool metrics and replay positions.
  Delete, fixture-only committed expiry, reaping and vacuum are separate stages.
- ACK latency is loadgen's request measurement. Post-loadgen observations and
  remaining outbox drain are separate; they are not the exact instant of the last
  ACK or a claim of steady-state origin upload throughput.

The initial paired resource trial accumulated nine roughly 1 GiB objects per
side, triggered heavy checkpoint/reclamation pressure and exceeded the original
120-second cleanup deadline. Its incomplete report is retained. The 18 fully
completed cohorts were not repeated; the unfinished cases use smaller sample
budgets and explicit checkpoint preparation. Another large seed hit the old
one-minute client deadline under that pressure; it is retained as a failed setup,
not a request sample or data-plane speedup.

The large apparent before/after timing swings in those resource trials are not
credible application improvements: the supposedly isolated clusters share a
physical disk. The driver now separates single-endpoint `--capacity` observations
from uninstrumented paired performance gates, while reusing the same fixtures and
loadgen. Capacity-only reports explicitly say no performance gate was evaluated.
Cleanup records its duration, verifies actual reaper drain and has an explicit
deadline. Request deadlines are configurable rather than silently retrying.

`pgvs3_spool_peak_bytes` adds exact reserved-payload high water since gateway
startup. Sampled gateway gauge peaks remain lower bounds; cross-node WAL gaps
include probe skew, and replay barrier times include SQL/polling overhead.
WAL is whole isolated-cluster LSN advancement,
including headers/padding, checkpoints and background work. Warm/cold describes
gateway state, not cleared OS caches. See [the full method](benchmarks.md#capacity-evidence).

## Capacity ledger

Host: Ryzen 7 7700X, 16 threads, 61 GiB RAM; rootful Docker/overlayfs on the local
host. Primaries use 1 GiB shared buffers, pool 32 and a 4 GiB WAL target; physical
standbys use 1 GiB shared buffers and `hot_standby_feedback=on`. The target is a
single PostgreSQL data plane, not disk isolation from other processes.

The ledger covers 60 size/path cohorts with three observations per candidate:
native GET/range/PUT/overwrite, warm-origin equivalents and cold whole/range
fills for six inputs. Eighteen completed native cohorts were retained from the
first partial run; four large-write cohorts completed separately, and the
remaining 38 use the explicit single-endpoint capacity mode. Their timing values
are observations, not evidence that the application became faster.

Median WAL insertion bytes per input payload byte:

| Fixture | Actual bytes | Native PUT | Cold whole fill | Staged PUT + drain |
| --- | ---: | ---: | ---: | ---: |
| Parquet ~10 MiB | 10,496,623 | 0.940 | 0.942 | 0.942 |
| Binary 10 MiB | 10,485,760 | 1.022 | 1.030 | 1.025 |
| Parquet ~100 MiB | 104,955,544 | 0.940 | 0.941 | 0.940 |
| Binary 100 MiB | 104,857,600 | 1.022 | 1.023 | 1.023 |
| Parquet ~1 GiB | 1,074,782,393 | 0.940 | 0.940 | 0.940 |
| Binary 1 GiB | 1,073,741,824 | 1.023 | 1.022 | 1.060 |

These are **WAL bytes, not total write amplification**: heap/index writes, each
replica and backup/archiving add work. This Snappy Parquet fixture still shrinks
under per-block Zstd; it is not a universal Parquet compression ratio. Checkpoint
state and background work can change the figures.

Warm 10/100 MiB GET observations mostly generated zero WAL. The 1 GiB binary
warm-GET samples recorded roughly 0.55–0.62 MiB of cluster WAL instead: this metric
includes hint/checkpoint/background work and is not a claim that every warm GET
is WAL-free. Aligned ranges publish only their blocks plus coordination/metadata,
not a whole-file copy; unaligned footers can span two aligned blocks.

The largest exact reserved spool high water was **2,149,564,786 bytes**—twice the
largest Parquet input—even with one foreground worker, because foreground staging
and asynchronous upload can overlap. The largest sampled pending body was one
1,074,782,393-byte object. Neither is an unlimited concurrency or backlog budget.

Immediate reclamation is not guaranteed. Representative single-endpoint origin
cohorts (all sizes below are MiB of logical object rows / allocated relations):

| Cohort | Before delete | After delete | After reaping + ordinary vacuum |
| --- | ---: | ---: | ---: |
| 1 GiB binary PUT, three retained objects | 3,072 / 4,162.5 | 0 / 4,162.5 | 0 / 4,162.5 |
| 1 GiB binary overwrite | 1,024 / 4,190.0 | 0 / 4,190.0 | 0 / 4,190.0 |
| Parquet warm range, including prior allocated history | ~0.1 / 955.6 | ~0.1 / 955.6 | 0 / 4.8 |

Allocated relation length includes retired/reusable history, not just the current
object or physical filesystem blocks. Reaper drain is verified independently of
file shrinkage. In the binary PUT example, reaping took 117.1 seconds and vacuum
44.7 seconds; those observed local maintenance costs are not hidden inside ACK
latency or replaced with a namespace drop. Snapshot/feedback horizons, vacuum
behavior and storage pressure need real headroom.

The practical decision is unchanged: native storage keeps all bytes inside the
PostgreSQL recovery boundary; origin-backed storage retains the origin but pays
PostgreSQL disk/WAL/replay costs for fills and staging. Use the
[sizing checklist](operations.md#choosing-and-sizing-a-storage-role), not only live
payload bytes or a peak read rate.

## Regression checks and retained failures

The uninstrumented comparisons cover **56 paths**, workers 1/4, both fixture
types, native/warm/cold origin reads, metadata/ranges and native/write-back PUTs.
There are **396 raw sample runs**, including all six failed original cases.
Every selected path has a passing qualified comparison within the 20% budget;
the largest accepted p50 increase is **18.6%**, and the largest rate reduction
is **15.6%**. No request errors were accepted as samples.

This is not a claim that the first sweep passed:

- A seven-request native 100 MiB PUT trial missed p50 while its median rate
  differed by only 3%. Five paired 15-request repetitions on a fresh fixture
  passed (230.476 → 226.180 ms). The original tail-heavy runs remain retained.
- Three origin 100 MiB PUT cases failed in the accumulated write/churn sweep.
  Five paired bounded cohorts with checkpoint preparation passed independently;
  their timings were not averaged into the originals.
- The cold binary 100 MiB range/four-reader case followed a disk-heavy whole-fill
  cohort and failed. A fresh five-pair, 100-request cold-range comparison passed
  (9.433 → 9.411 ms). Its original samples are also retained.
- A 1 GiB Parquet origin PUT failed in the retained-data comparison. Explicit
  PUT isolation drains/verifies/deletes the sample and resets only the empty
  owned cache between repetitions. Five pairs passed (3,149.927 → 3,160.448 ms).
  This qualifies the publication-ACK path, not sustained origin throughput.

The source store/COPY/read queries and layout are unchanged. The measured runtime
changes are idle origin-registry refresh and spool-reservation telemetry. Large
favorable swings under storage pressure are not presented as improvements.
The [measurement archive](benchmarks/data-plane-2026-10.json) retains original
failures, per-case followups, actual inputs/configuration and binary hashes.

## Final verification

The complete `just ci` passed: formatting/modernizer/import/tidy/vet/staticcheck
and vulnerability checks; race-enabled Go tests; 11 standard-library Python
tooling tests; uncached race integration (44 s); four fuzz targets; Helm lint
and kind (76 s); contract including the maintained analytical scenario (28 s);
the expanded physical-replica/fault/restore gate (92 s); interop including restic
(16 s); and s3t (17 s including setup). ShellCheck and local documentation links
were checked separately. Failure artifacts include the evidence JSON.

## Limits

These are PostgreSQL 18.6/DuckDB 2.0.0 loopback fixtures, not AWS/Aurora results.
The resource origin is a filesystem fixture; the analytical/fault scenarios use
the S3 driver against an independent gateway. Neither substitutes for a remote
service's latency, TLS/authentication, cost or durability validation.

The promotion test explicitly fences the old fleet; arbitrary partitions and
ambiguous in-flight origin mutations still need operational reconciliation.
Physical backup/PITR tooling, production tails, large sustained working sets and
co-location interference remain deployment-specific proof obligations. A 32 TiB
relation ceiling is not a measured production dataset size.
