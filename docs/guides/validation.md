# Guide validation

[Guide index](index.md) · [Research ledger](sources.md)

`just guides` is the disposable executable specification for the named snippets
on these pages. It uses `test/data_plane.py` and `deploy/bench/stack.sh`, the same
SQL/HTTP/origin fixtures as the existing analytical and recovery gates—not a new
load engine or conformance framework. The required `just contract` runs it too.

Verification markers bind tests to the actual SQL, JSON, shell and Python shown
in the guides. The report records document/snippet/tool hashes, actual versions,
query plans, positive and negative outcomes. Missing/unexecuted markers fail the
gate. Endpoint/port/demo database substitution and intentional failure injections
are test fixture parameters, not alternative examples.

## Scope

| Area | Executable check |
| --- | --- |
| PostgreSQL | Million-row application fixture; same 20 results before/after index, chosen index/buffers captured; transaction-local budgets revert. |
| Mixed-load procedure | Custom pgbench canary and existing `test/loadgen` run against the same cluster; raw transaction logs/error counts retained. Two-second smoke only, no SLO or speedup claim. |
| Extraction | Actual PostgreSQL scanner → Parquet → pgvs3; another client verifies counts/sums/payload; conditional manifest rejects a repeated create. |
| DuckLake | Candidate configuration syntax; fixed-64 2D Hilbert (`lake.hilbert64`) and fixed-32 hash (`bucket(32, customer_id)`) partitioning + `SORTED BY`, wide-column `skip_stats_columns` + byte-bounded row groups, batch `merge_adjacent_files` + `rewrite_data_files`; catalog reopen through B; inline tiny insert produces no object until flush. |
| Freshness/cache | Direct source mutations do not change the old extract; direct origin mutation stays stale inside TTL, then revalidates; shared full/range hits cause no extra origin reads. |
| Staging | Block origin writes, ACK logged pending body, read on B, inspect pending SQL/metrics, unblock and compare origin bytes. |
| Jobs | Rollback leaves no intent/job; lost enqueue response reconciles; mismatched ID payload refuses success; redelivery after SQL effect suppresses duplicates and rejects old lease token. |
| Scheduling/retention | Due work/retry fetch without forcing maintenance; terminal pruning allows ID reuse, while an application receipt still suppresses the effect. |
| Streams | Oversized snapshot batch returns 413 without partial pagination; repeat receive returns the active batch; ACK advances it. |
| Operations | PostgreSQL 18 diagnostic queries, optional statement statistics, CLI/probes/metric names and guarded demo cleanup. |

## Verified 2026-11-02

Base pgvs3 `4c39cd0` plus this change set, layout v25; Linux amd64, PostgreSQL/pgbench **18.6**, DuckDB
**2.0.0**. Actual loaded extension versions/hashes are in the report: DuckLake
`417a9117f5` (catalog format 1.1), httpfs `77392a7207`, PostgreSQL scanner
`c0f5a3c0c4`. Normal signed/official extensions are used. The 2026-11-02 pass
changed CLI/test tooling (`pgvs3 doctor`, `ServerFacts`, flag help, `just sol`,
generated `docs/reference.md`) and the object read path (ordered piece window).
No storage layout, schema SQL, write path or dependency changed.

```text
$ just guides
guides: 35 bound snippets, 12 scenario checks passed
guides ok (36s); artifacts: .tmp/verify/guides.6k844Q

$ just docs
Output created: _site/index.html

$ python3 test/documentation.py
{'documents': 26, 'local_links': 205, 'snippets': 35, 'svgs': 6}

$ mise exec python -- python -m unittest discover -s deploy/bench -p '*_test.py'
Ran 27 tests
OK
```

| Observation | Verified result | Not established by it |
| --- | --- | --- |
| OLTP access path | Identical 20 IDs; plan root buffer visits 12,436 → 23; candidate uses `orders_customer_recent`. `SET LOCAL` restores `work_mem=4MB` and `max_parallel_workers_per_gather=2`. | Universal index benefit or production p99. Buffers are plan observations, not a throughput ratio. |
| Extraction | 1,000,000 rows, 23,217,273-byte Parquet; ID/cents/payload aggregates match on B. Source UPDATE/DELETE does not alter the old export. Repeated manifest create is 412. | CDC, cross-system transactions or a bounded age after an undetected failed refresh. |
| Same-cluster canary | Control and mixed logged SQL transactions, zero failed/skipped; concurrent object GETs, zero errors. | Performance acceptance: two-second runs are explicitly `performance_gates_evaluated:false`. Raw logs remain; the count difference is not a tuning conclusion. |
| DuckLake layout, skewed-hotspot 24-bit Hilbert sort & hash-32 compaction | Persisted candidate options read back; actual ZSTD row groups of 122,880 and 16,960 rows. 90%-skewed NYC hotspot across 2026/2027 sorted by 24-bit hierarchical `hilbert_id` (`lake.hilbert64` catalog macro) merges two batches into one ZSTD file, rewrites deleted slice, suppresses `min_value`/`max_value` in `ducklake_file_column_stats` for `wide_payload`, and returns `3240 | 256 | 72` (including the 32-bucket hash table) on A and B. Tiny insert is catalog-only until flush. | That 256 MB file target or this layout is fastest for every query shape. |
| Cache/staging | Warm whole/range reads on B add zero origin reads; v1 remains inside TTL, then v2 revalidates. Pending bytes are non-expiring/readable, cleanup refuses them, and origin drain preserves bytes/ETag. | A generic SWR policy, independent capacity or lossless async promotion. |
| Jobs | Rollback/no job, lost enqueue reconciliation, mismatched envelope refusal, effect-before-ACK redelivery and stale-token 409. Receipt/effect count stays one even after retained ID removal/reuse. | Permanent uniqueness or exactly-once external work. Retention age is advanced only on one owned fixture row. |
| Streams | Ceiling 413, same active batch on repeat, ACK then empty receive. | Pagination, job leases on batches or a hard retention/disk cap. |

Canonical report: `.tmp/verify/guides.6k844Q/guides.json`, SHA-256
`c0ec6fb6c593526f05e6b28a68c401d2ed593d3e5ad7f4832939a7c634a0e360`
(superseding `.tmp/verify/guides.op1nSl/guides.json`, SHA-256
`b9e16e2ab98a01bb5e1d4a9828f94d810105f18197987e297206482f07ab16da`,
`.tmp/verify/guides.f84hII/guides.json`, SHA-256
`6368026609cc85667c8b2713d86e489f88088d1700bec2e41faf29bc3f3a81f8`,
`.tmp/verify/guides.MPLHYo/guides.json`, SHA-256
`375bf056b87755af7a04cb8657c2907bf35746d972e8119177f07f25de823db9`,
`.tmp/verify/guides.htfvGk/guides.json`, SHA-256
`d54cc76c63c56a9937667b1fbdcd0d3502c1a9f188712839a891592784deafc3`,
`.tmp/verify/guides.LO55Zg/guides.json`, SHA-256
`c95072f77924f3ab8ee2ac514615a650f144c0c69415cba875f88c116ec2a31f`,
`.tmp/verify/guides.dJ5kp4/guides.json`, SHA-256
`04fa2c41f6ec1c7421e869e97c5becf8227c4b2296b83a2344fb57f774fe9ef4`,
`.tmp/verify/guides.rVqHQl/guides.json`, SHA-256
`8f63be5765a85f21108db9948e7c6c654dcc3429b05c46f24ae6b812fa737b71`,
`.tmp/verify/guides.YWTtwr/guides.json`, SHA-256
`45d1c5f70f09c227da6300be5f4b7a09db7a5d337cf2ee469722461852af1123`,
`.tmp/verify/guides.Roi71e/guides.json`, SHA-256
`fd4f7d697b8f8f5f85afc2d7a057983326b3e958df0d99f1d489f22c78b0ed8b`,
`.tmp/verify/guides.YSR72E/guides.json`, SHA-256
`cc57880f7f178d0f7b8ea2111964c06b2dc67aacd18b98b43ac4422046099339`,
`.tmp/verify/guides.c8g6AD/guides.json`, SHA-256
`4f5fd634f3755b6eec59524de47c86873b32d610c713f72398b4d39b35d7b6cd`,
`.tmp/verify/guides.VLXXPz/guides.json`, SHA-256
`333b97c06044baac9fa7f5dc904d9fb9c44191ab61dea1d3e8a51b70b8871ad7`, and
the 34-snippet `.tmp/verify/guides.sUjCDv/guides.json`, SHA-256
`bdf47490d977c7cbc14c69309b617d06fc5d5a5d6ad0b7abe543f713bf517788`, which
remain retained).
It contains the checks, plans, capacity snapshot, actual versions and document/
snippet/tool/binary hashes. All recorded document/tool hashes were compared with
the delivered sources. This validation page is excluded from document hashes to
avoid a self-referential report-hash cycle. Reports/transcripts/data stay ignored;
GitHub's existing failure upload includes the guide JSON and canary logs.

The complete `just ci` passed on both the frozen source and the delivered tree:
format/modernizer/import/tidy/vet/staticcheck/vulnerability checks, race Go tests,
**27 Python tooling tests**, the generated CLI reference test, the live metric-reference check, uncached race integration, four fuzz targets,
Helm/kind, the contract (including these guides and pinned ObjFS recovery),
physical-replica/promotion/pending-restore fault checks, interop and s3t.
ShellCheck and Python compilation were checked separately. This is a local gate
result, not a claim that a hosted CI run or cloud deployment passed.

The frozen-source run retained integration (44 s), Helm/kind (72 s), contract
(50 s), fault (91 s), interop (16 s) and s3t (13 s) artifacts, including
`.tmp/verify/contract.E4QASf/guides.json` (SHA-256
`76a9f5cf74317b49cc1c38e928da81313537ec97a127072611f06a94ffd709ce`). The
delivered-tree rerun passed integration (50 s), Helm/kind (68 s), contract
(52 s), fault (91 s), interop (17 s) and s3t (13 s); its success-only contract
artifacts were cleaned, so the canonical current guide report is the standalone
`.tmp/verify/guides.6k844Q/guides.json`. Transcripts:
`.tmp/guidance-20261031/ci-frozen.log` (SHA-256
`32c0e44297bde13dac56934b321f8521625a1292b9ba626c49b079bb5a8fc0bf`) and
`.tmp/guidance-20261031/ci-readwindow3.log` (SHA-256
`0d78f16e2969da01f7e53b7690f2870b84a05667713d22e25a0bc67f726e7832`).

## Research and retained failed trials

**44/44 primary source URLs returned HTTP 200.** Archived URL/date/body hashes
are in `.tmp/guidance-20261031/sources/fetch-44.json`, SHA-256
`55cb4a10d55ec6acfc826e723b648d599e465fe2c18145dbaeac9abb0593a989`
(retaining the earlier 32-page `fetch.json` and 33-page `fetch-final.json`);
the archived body hashes were checked too. These are research snapshots, not
vendor binaries or vendor performance tests.

- `.tmp/verify/guides.OFzcOK`: diagnostic query failed on ambiguous `relname`.
  Fixed the **shown** query to `t.relname`; the failed report/log remain.
- `.tmp/verify/guides.7mk8au`: harness assumed `serve --help` exited zero. The
  existing CLI exits two; the harness now checks its real status/output instead
  of changing runtime behavior. Failed evidence remains.
- `.tmp/verify/guides.GPIO9D`: expected count for `events_hash32` after deleting
  `id < 4096` was written as `72` instead of `71` (`4096 = 8 × 500 + 96`, removing
  9 of the 80 rows for `customer_id = 42`). Fixed the documented expected value
  and assertion; failed evidence remains.
- `.tmp/verify/guides.BYo7Gd`: a CTE-based `CREATE MACRO lake.spread16` worked in
  a local DuckDB catalog but failed binder resolution when serialized into a
  PostgreSQL DuckLake catalog (`ducklake:postgres:`). Replaced it with the
  pure-scalar `list_extract` macro `lake.hilbert64` applied at 4 hierarchical
  levels (24-bit = $4,096 \times 4,096$ grid); failed evidence remains.
- Later successful 32-snippet trials and the expanded 34-snippet canary trial
  remain alongside the canonical run; none replaces or overwrites a failure.

## What this does not establish

These are correctness/syntax/failure-window checks, **not** performance acceptance.
The existing 10 MiB–1 GiB paired/capacity runners and published measurement
ledgers remain authoritative for their tested workloads. No performance test of
PlanetScale, ClickHouse, River or Spice.ai, managed-provider deployment, arbitrary
network partition, production SLO, full CDC pipeline, parallel application relay,
permanent deduplication or exactly-once external effect is claimed.

Physical standby/promotion and pending-body logical restore belong to `just fault`;
physical archive/PITR and recovery of external effects still require an operator
drill. Link/fence/Python/JSON checks run in standard-library CI tests; external
page status is dated research evidence, not an always-online CI dependency.
