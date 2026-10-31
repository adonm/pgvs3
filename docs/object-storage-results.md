# ObjFS performance and operating results

Date: 2026-10-31. Base pgvs3 `1fdd1a2`, layout v25. Linux amd64, Ryzen 7 7700X,
16 threads and approximately 62 GiB RAM; loopback PostgreSQL 18.6 / DuckDB 2.0.0.
ObjFS is the checksum-pinned 0.2.0 community release, not a modified fork.

**Useful for small native commits, but not a blanket DuckLake replacement.**
The measured optimization is an explicit read-mostly sealing workflow: close the
writer cleanly before delivering data to fresh readers. The smaller cache/thread
configuration is retained as a candidate, not made a universal default, because
three measured paths missed its 20% latency budget.

## Comparable workload

`just object-storage` completed **27 cohorts / 96 per-path comparisons**: three
repetitions of DuckLake, ObjFS defaults and the lean candidate at approximately
10/100/1,024 MiB. Identical input hashes, schema, updates/deletes, row hashes and
post-cleanup sentinel reads are checked. Successful timing samples have no 5xx
responses. Each scan/range reader is a fresh process through the other gateway.

The [method and contracts](object-storage.md) distinguish statement acknowledgement,
checkpoint, fresh attachment, schema discovery, live/sealed reads, memory budgets
and storage retention. ObjFS caches add to the 2 GB DuckDB memory limit; they are
not included in that limit. Persistent local SST caching is disabled.

## Small commits versus bulk delivery

Medians, milliseconds. DDL/32-row appends have 15 statement samples per
profile/size; bulk and fresh-read totals have three independent cohorts.

| Input / operation | DuckLake | ObjFS default | ObjFS lean |
| --- | ---: | ---: | ---: |
| 100 MiB / CREATE empty table | 21.05 | 1.61 | 1.59 |
| 100 MiB / 32-row append | 6.90 | 2.17 | 2.12 |
| 10 MiB / bulk CTAS | 121.38 | 95.41 | 94.58 |
| 100 MiB / bulk CTAS | 255.50 | 428.74 | 441.96 |
| 1 GiB / bulk CTAS | 1,539.52 | 3,821.80 | 4,026.59 |
| 100 MiB / fresh attach + schema + full query | 102.41 | 76.41 | 70.88 |
| 1 GiB / fresh attach + schema + full query, live writer | 427.80 | 1,026.27 | 985.97 |
| 1 GiB / same-client repeated full query | 372.32 | 154.69 | 154.55 |

Native small DDL/commits are fast, and the 1 GiB workload completes—unlike the
Differential sync-WAL epoch-limit case. That does not imply cheap large delivery:
ObjFS's 1 GiB bulk write is approximately 2.5× DuckLake's latency. The native
buffer-manager repeat is also not a faster shared PostgreSQL cache; it can avoid
remote work using client-private state.

## Measured optimization: seal read-mostly data

Large live-reader attachment can replay substantial object WAL data. A clean
filesystem close flushes remaining memtables and removes much of that bootstrap
work. All profiles receive the same close/read/reopen sequence, and closing time
is retained rather than treated as free work.

| 1 GiB input | ObjFS default: live writer | ObjFS default: sealed writer |
| --- | ---: | ---: |
| Fresh full-read total | 1,026.3 ms | 476.0 ms |
| Fresh 1% range-read total | 313.0 ms | 52.6 ms |
| Full-read gateway response bytes | 1,197.2 MiB | 613.1 MiB |
| Range-read gateway response bytes | 590.3 MiB | 12.9 MiB |

The default-profile writer close costs **79.7 ms**. It saves approximately
550 ms per fresh full reader in this workload, so it amortizes within the first
reader. The range path improves about **6×** and the full-read path about **2.2×**.
For a quiescent/read-mostly delivery workflow:

```python
writer.execute("CHECKPOINT lake")
writer.close()
# Open fresh SQL READ_ONLY clients against the same object data.
```

Close all references to that DuckDB instance; merely detaching and reattaching
can reuse cached SlateDB state. Do not apply this as a transparent optimization
for an uninterrupted live writer. It also is **not universally faster**: at
100 MiB, sealing increased the default-profile fresh full-read total from
76.4 to 85.9 ms and cost 167.4 ms to close. DuckLake's 1 GiB fresh full read remains
about 428 ms, versus 476 ms for sealed ObjFS default.

## Cache/thread candidate: not universally accepted

The lean candidate halves the extra configured cache budget from **640 MiB to
320 MiB**, uses four Foyer shards and four async workers. The observed ObjFS
default uses automatic shards and DuckDB's initial **64 async workers**, even
with four execution threads. Both must be configured before first filesystem use.

Most measured latencies stayed close to the default, but retain these misses:

| Case | Default | Lean | Increase |
| --- | ---: | ---: | ---: |
| 100 MiB table DROP | 2.95 ms | 3.89 ms | 31.7% |
| 1 GiB half DELETE | 20.15 ms | 24.72 ms | 22.7% |
| 1 GiB half-state scan attachment | 30.45 ms | 37.13 ms | 21.9% |

These are not discarded or averaged into an aggregate win. The reduction is a
resource-budget option, not a universal latency recommendation. Whole-writer
peak RSS for the 1 GiB cohorts was approximately **4.80 GiB default / 4.59 GiB
lean**, versus approximately 1.06 GiB for DuckLake. The configured DuckDB memory
limit and cache limits do not bound the entire native/Rust working set.

## Storage and reclamation

Representative medians in MiB. PostgreSQL values are allocated bucket relation
lengths, including reusable/retired history—not only live filesystem payload.

| 100 MiB input | DuckLake | ObjFS default | ObjFS lean |
| --- | ---: | ---: | ---: |
| Current object bytes after load | 100.1 | 56.9 | 56.9 |
| Allocated PG bucket relations after load | 93.8 | 57.5 | 57.5 |
| Current object bytes after DROP/checkpoint/observation | <0.001 | 128.1 | 128.1 |

At 1 GiB, ObjFS starts with about **1,154 MiB** of current objects and retains
about **1,172 MiB** after the data-table DROP/checkpoint/short observation. The
table is absent; the sentinel remains correct. Historical WAL/SST/manifest and
managed-checkpoint retention still cost storage. No manual object deletion,
retention timestamp editing or namespace drop is credited as compaction.
ObjFS 0.2.0 exposes no SQL compaction/retention override, so this is not a claim
that its normal long-horizon background GC cannot reclaim anything.

Client-local spill/cache files were zero in these configurations; memory and
remote recovery logs are still real costs. Ordinary PostgreSQL vacuum need not
shrink relation files. Native SSTs are not Parquet files usable by other engines.

## Compatibility boundaries and evidence

The required client contract now uses the pinned 2.0-compatible release and
separate processes: terminate the writer without explicit checkpoint/detach,
open a SQL read-only client through another gateway, then recover writable and
resume commits. This replaces the earlier 1.5.6 same-session/checkpoint-only toy.
An additional 24,268,800-row probe omitted the explicit checkpoint before
process death; its separate readers recovered the row count, key sum, schema
default and string total, then resumed writes. This is bounded process-death
evidence, not a power-loss or full upstream acceptance claim.

A SQL read-only reader writes a managed SlateDB retention checkpoint. A bounded
negative probe against the gateway's read-only role observed **eight forbidden
checkpoint PUTs**, then stopped the non-returning client after five seconds.
That endpoint combination is unsupported; the gateway remains read-only. This
does not establish ObjFS over physical read-only PostgreSQL replicas. Manifest
polling/follower freshness, arbitrary partitions, power loss and concurrent-writer
acceptance are not certified by these measurements.

Canonical report: `.tmp/verify/bench.se0fyi/object-storage.json`, SHA-256
`336f2bb16fb3ac273f4538d88e6995d756d291db51b8df899613a70a8e0e7e9f`.
It includes artifact/tool/input hashes, all repetitions, configured and effective
settings, OpenDAL/cache statistics, process RSS, gateway counters and allocated
PostgreSQL footprints. Raw transcripts and implementation notes remain ignored.
The default storage/client recommendation remains unchanged; no production Go,
embedded SQL, durability, authentication or gateway TLS behavior was changed.

## Final verification

The complete `just ci` passed: formatting/modernizer/import/tidy/vet/staticcheck/
vulnerability checks; race-enabled Go tests; 20 standard-library Python tooling
tests; uncached race integration (44 s); four fuzz targets; Helm lint and kind
(75 s); contract including the new pinned ObjFS remote-WAL recovery proof
(29 s); physical-replica/promotion/restore fault checks (91 s); interop (16 s);
and s3t (16 s including setup). ShellCheck, local documentation links and source/
artifact hashes were checked separately. The shared Differential workflow also
passed its three-profile functional smoke after the reuse changes.
