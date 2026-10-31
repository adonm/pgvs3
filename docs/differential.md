# Differential on the PostgreSQL object data plane

[DuckDB Differential](https://github.com/tobilg/duckdb-differential) is an
**experimental client option**, not a new pgvs3 storage backend or a replacement
for DuckLake. The standard disposable benchmark includes it with `just differential`.
Logged layout v25, shared fills, replicas and existing clients remain. The
accompanying [bounded S3 admission queue](operations.md#admission-control) is a
separate gateway improvement, not a Differential storage optimization.

## Design and acknowledgement boundary

Differential runs stock native DuckDB storage over a local repository: SQLite
metadata plus append-only payload/journal segments. It exports content-addressed
256 KiB chunks in packs of up to 8 MiB. With a native pgvs3 bucket, these S3
objects are logged PostgreSQL rows, just like DuckLake's Parquet objects.

| Profile | What returns from a write | Remote visibility |
| --- | --- | --- |
| DuckLake + PostgreSQL catalog + pgvs3 objects | Object upload and catalog commit | Other attached DuckLake clients can discover the committed catalog state |
| Differential, ordinary commit | Native local sync in its repository | Not a PostgreSQL-backed commit merely because `DATA_PATH` is S3 |
| Differential snapshot export | Local commit **plus** synchronous `differential_create_snapshot` | A new plain head attachment reads that exported snapshot |
| Differential `SHIP_WAL 'sync'` | Local sync **plus** remote epoch publication | Recoverable shipped state; plain head attachments still read the last exported snapshot, not necessarily the latest commit |

The benchmark compares the first, third and fourth rows. It retains ordinary
statement time separately and never credits a local-only return as a remotely
durable acknowledgement. Additional snapshot publication for the sync-WAL profile
is measured separately. A commit-level `FOLLOW` reader is an upstream option,
but its polling/transaction semantics are not benchmarked here.

The native catalog can make small DDL/transactions cheaper than a PostgreSQL
DuckLake catalog. That hypothesis does not establish faster fresh attachment,
remote scans, lower total storage, or equivalent operational capabilities.
The exported packs are native DuckDB storage, **not Parquet files for other
engines**. Producing interoperable Parquet remains an explicit export operation;
the existing DuckLake/Parquet path is not removed.
Native DuckDB buffers and the writer's repository must not be mistaken for shared
pgvs3 caching. Gateways remain stateless; the additional repository belongs to the
**DuckDB client**.

## Provisioning

```sh
just setup
just differential-setup
just differential --differential-mib 10,100 --passes 3
# Include the large cohort, or run it separately to bound storage churn:
just differential --differential-mib 1024 --passes 3
```

Equivalent benchmark entry point: `just bench differential ...`. A quick
functional smoke is:

```sh
just differential --differential-mib 1 --passes 1 \
  --differential-ddl-iterations 1 --differential-small-commits 2
```

The opt-in setup pins **Differential v1.1.0**, source commit
`43203569f46b3a7f0c3a88fa8b4c34a50052ceb0`, for **DuckDB 2.0.0**. Automatic
provisioning supports the upstream Linux amd64/arm64 and macOS arm64 artifacts.
It verifies the complete archive SHA-256 before extracting only the extension,
records the extension hash, and refuses to overwrite a different cached binary.
Only Linux amd64 is exercised in this repository's recorded run.

These are **upstream CI artifacts, not permanent release assets**. Workflow
`40585660508` artifacts currently expire **2027-01-25**. Preserve the verified
`.tmp/deps/differential/` cache or use a source build; do not silently substitute
the latest artifact. Community installation is not assumed to be available.

To supply an explicitly built extension:

```sh
just differential --differential-extension /absolute/path/differential.duckdb_extension \
  --differential-mib 10 --passes 3
```

`PGVS3_DIFFERENTIAL_EXTENSION` is the equivalent environment override. It is
hashed and checked for release version/test-hook absence when loaded. Build the
pinned commit with its submodules and upstream build instructions:

```sh
git clone https://github.com/tobilg/duckdb-differential .tmp/differential-source
git -C .tmp/differential-source checkout --detach 43203569f46b3a7f0c3a88fa8b4c34a50052ceb0
git -C .tmp/differential-source submodule update --init --recursive
cd .tmp/differential-source
# With CMake/Ninja and the upstream prerequisites:
make vcpkg
export VCPKG_TOOLCHAIN_PATH="$PWD/build/vcpkg/scripts/buildsystems/vcpkg.cmake" GEN=ninja
make release
# Artifact: build/release/extension/differential/differential.duckdb_extension
```

The source-build fallback is documented, not exercised by the pgvs3 measurements.
No agent plugin, patched DuckDB, FUSE service or daemon is installed. Unsigned
extension loading is enabled **only in the experimental DuckDB processes**;
there is no global DuckDB setting change. Credentials use temporary `TYPE s3`
secrets, never attach options or persisted secrets. Client processes disable EC2
metadata discovery so an unavailable metadata service cannot dominate attachment.

## Benchmark method

`deploy/bench/differential.py` is a part of the existing DuckDB runner, not a
separate infrastructure harness. `deploy/bench/stack.sh` owns the disposable
PostgreSQL 18 container and two gateways; `capacity.py` supplies its storage/WAL
observations. Work is sequential, with profile order rotated between repetitions.
It does not rerun or replace the existing direct-gateway performance matrices.

- Identical deterministic Snappy Parquet inputs target 10 MiB, 100 MiB and
  1 GiB; reports retain actual sizes, generation SQL and hashes. Each cohort has
  fresh namespaces and a fresh client repository. DuckLake data inlining is off.
- DDL on empty tables, schema evolution on the loaded table, bulk loading,
  32-row small commits, a 1% string update, 50% deletion and a full table drop
  are measured individually. `--differential-ddl-iterations` and
  `--differential-small-commits` default to five; `--passes` defaults to three.
- Reader processes use gateway B, a fresh repository and no writer-local files.
  Each scan/range reader is separate. External file caching, Parquet metadata
  caching and Differential's remote disk cache are off; memory is 2 GB with four
  execution threads per client. HTTP client pool capacity and the Differential
  SDK request cap are both 16, below the gateway's default 32-request admission
  limit. Retried 5xx responses invalidate a timed sample; they cannot inflate a
  claimed client speedup. The repeated query can still use **native DuckDB
  buffer pages** and is labeled `same_client_repeat`, not another cold read.
- Fresh attachment, schema discovery and first query are separate, with their
  sum also reported. A faster query after an expensive native attachment is not
  automatically a faster fresh-client operation. Process startup and extension
  setup are outside that sum (`client_setup_seconds` is retained separately).
  Neither server buffers nor OS
  caches are flushed; this is not a cold-disk or WAN benchmark.
- Results are checked against an independent local-input reference using row
  count, key sum, string length and a 128-bit sum of row hashes. Schema, updates,
  deletions, range filters and post-cleanup reads must match. A sentinel table
  verifies fresh access even after the data table is dropped.
- Actual gateway request/response-byte deltas cover **both** httpfs and the
  Differential C++ SDK. DuckDB's HTTP log alone would miss the SDK. PostgreSQL
  catalog traffic is not S3 traffic and is not counted as such.
- Capacity snapshots include object bytes, allocated bucket/catalog relations,
  cluster WAL positions/retention, local repository/spill bytes and allocated
  filesystem blocks. Server WAL includes other work on the isolated cluster;
  ordinary vacuum does not guarantee relation shrinkage. Inventories do not
  measure RSS, shared extension/input files or filesystem metadata overhead.
- DuckLake's explicit `delete_threshold => 0.5` makes its file rewrite eligible
  after half the rows are deleted. Its default 0.95 threshold would leave those
  files unchanged. Rewrite time and new/retired objects are measured, not treated
  as free compaction. Differential's metadata compaction is not an equivalent
  table rewrite; that distinction is part of the storage comparison.

Each path reports its own latency ratio and 20% budget result against DuckLake.
This is an **alternative-client evaluation**, not a gateway regression pass.
Budget misses remain visible and do not remove the existing path or silently
change the preferred setup. A completed experiment means its stated checks ran,
not that every optional profile passed a replacement/adoption gate.

Known client limits remain failures, not zero-latency samples. The release cannot
synchronously ship an epoch larger than 256 MiB; the 1 GiB fixture's bulk CTAS
hits that limit. Its error says the commit is durable locally but was not shipped.
The runner retains that failed remote acknowledgement, marks the corresponding
profile unavailable, and does not retry it by batching or weakening durability.
It finishes other profiles and writes `complete: true` with explicit `failures`
and `all_profiles_completed: false`; the command still exits nonzero. Do not
blindly retry a failed bulk INSERT: local state may already have committed.

All reports live in the unique printed `.tmp/verify/bench.*/` directory, including
per-worker jobs/results/logs and incremental `differential/progress.json`.
`--out` must be fresh. Failures and partial reports are retained. Management
workers reject paths/provider roots outside their owned run; namespace teardown
happens **after**, never instead of, the measured reclamation stages.

## Reclamation is a pipeline, not one compact call

1. Release reader attachments and expire eligible old snapshots, keeping current
   branch heads. Snapshot, branch and WAL-chain roots still retain data.
2. Differential local GC removes unreferenced mappings/payload; active journals
   and retained readers can delay physical segment reclamation. A separate fresh
   process measures cleanup after the writer exits.
3. `differential_compact` performs **SQLite incremental vacuum**. Its page/byte
   result is local metadata reclamation—not a native table rewrite, remote-pack
   compaction, or PostgreSQL vacuum.
4. Remote GC first condemns unreachable objects, then deletes eligible condemned
   objects after a grace period. Indexes/descriptors sharing live packs can remain.
   The newest shipped chain and its base are roots as well as public snapshots.
5. Deleted pgvs3 objects retire body ownership; pgvs3 reaping and PostgreSQL vacuum
   then make their physical space reusable. These timings/footprints are separate.

The disposable GC drill deliberately uses a **one-second grace**, with waits
between phases. That is not an operational retention recommendation. The current
DuckDB file can still map free blocks; dropping SQL data need not immediately
shrink exported payload. The benchmark records the actual result rather than
attributing all observed reclamation to `differential_compact`.

## Recovery and the writer-fencing limit

The release-client proof exercises SDK multipart uploads/SHA-256 metadata,
conditional publication, ranged pack reads and a writer exiting with `os._exit`
without detach or shutdown checkpoint. Another repository recovers every shipped
acknowledgement, continues with the same identity through the other gateway, and
recovers again after remote GC. Explicit branch-head takeover is needed to resume
snapshot publication/numbering after identity-preserving recovery.

**This is not an HA controller or a global writer fence.** A deliberately
restarted old repository acknowledged another sync-WAL commit and produced a
newer chain; a subsequent fresh recovery no longer contained the recovered
writer's later marker. Conflicting snapshot publication was rejected, which did
not prevent the WAL-state replacement. The negative probe is retained separately
from the valid one-writer recovery proof; it is not a successful safety claim.

Fence the old **DuckDB writer and its repository** before failover; do not merely
switch gateways or take over branch heads. Do not restart that stale repository
as another writer. PostgreSQL's object durability does not arbitrate the native
client's independent histories. Multi-host/shared writers, followers, merge
semantics, arbitrary partitions, power loss, physical backup/PITR coordination
and origin-backed Differential locations remain outside this evaluation.

Upstream's release status still reports `v1_complete=false`; loading successfully
is not full Linux or power-loss qualification. Retain DuckLake and the existing
PostgreSQL roles unless the measured workload and operating constraints justify
an explicit client choice.
