# SlateDB ObjFS on pgvs3

[`duckdb_object_storage`](https://duckdb.org/community_extensions/extensions/duckdb_object_storage)
is an optional native DuckDB filesystem backed by SlateDB/OpenDAL. It uses pgvs3's
existing S3 interface; it does not replace the gateway's logged PostgreSQL layout,
shared caching or other clients. Physical objects are SlateDB WAL/SST/manifest
data, **not interoperable Parquet files**.
See the [measured results and accepted optimization](object-storage-results.md)
before choosing a configuration.

## Run the standard comparison

```sh
just object-storage-setup
just object-storage --object-storage-mib 10,100 --passes 3
just object-storage --object-storage-mib 1024 --passes 3
```

`just bench object-storage ...` is equivalent. The default includes all three
sizes; use a smaller functional smoke when changing tooling:

```sh
just object-storage --object-storage-mib 1 --passes 1 \
  --object-storage-ddl-iterations 1 --object-storage-small-commits 2
```

Provisioning pins **ObjFS 0.2.0 / DuckDB 2.0.0**, upstream commit
`56d9c7b928836cfaffa6790c77b7640d50102fe3`. The qualified Linux amd64 community
binary SHA-256 is
`dfd7040b4a0204aaa9aff142932ae7b5d2ec78634541d2c9f6e3bc0f329b4e12`.
DuckDB's installer handles the community signing key; the benchmark verifies the
binary before loading it from its private `.tmp/deps/duckdb-object-storage/`
installation directory. It does not change global DuckDB configuration or enable
unsigned extensions for normal community-artifact runs.

The community endpoint is mutable, and its version-qualified `0.2.0` URL was not
available during this evaluation. Keep a verified cache. A changed binary fails
the pin rather than silently becoming the new baseline. Other upstream-supported
platforms/source builds can be supplied explicitly with
`--object-storage-extension` or `PGVS3_OBJECT_STORAGE_EXTENSION`; the hash/version
is recorded. Such an override permits unsigned code only in that experimental
client process. The recorded qualification is Linux amd64, not all platforms.

Core `httpfs` supplies the temporary `TYPE s3` secret; `cache_httpfs` and a second
DuckDB runtime are unnecessary. The required contract now uses the pinned 2.0
client and verifies separate-process recovery, replacing the older 1.5.6
same-session-only check. Credentials stay in temporary secrets, not settings,
jobs, storage descriptors or persistent secret files.

## Profiles and configuration order

The existing DuckDB/Docker runner compares DuckLake with two ObjFS configurations:

| Setting | ObjFS defaults | Lean candidate |
| --- | ---: | ---: |
| Native DuckDB execution threads | 4 | 4 |
| DuckDB memory limit | 2 GB | 2 GB |
| SlateDB data cache | 512 MiB | 256 MiB |
| SlateDB metadata cache | 128 MiB | 64 MiB |
| Foyer shards | Automatic | 4 |
| Async/Tokio workers | DuckDB's initial default | 4 |
| Persistent SST cache | Disabled | Disabled |

SlateDB cache limits are **in addition to** DuckDB's buffer-manager limit.
Configured budgets and whole-process peak RSS are both recorded. The lean profile
halves the extra configured cache budget; it is a measured candidate, not an
assumption that less cache is universally faster.

Configure `threads`, **`async_threads`**, caches, bucket/root and a local
`temp_directory` before the first `duckdb_objfs://` access. In DuckDB 2.0 the
Tokio runtime takes its async-worker count from `async_threads`, not merely
`threads`; the observed default on this 16-thread host is 64. Changing settings
after filesystem initialization does not resize the existing runtime, and the
extension rejects changes to its frozen storage/cache settings.

## What the benchmark measures

This is another part of `deploy/bench/duck_bench.py`, using the existing
`stack.sh` fixture and analytical cohort workflow. It reuses the same deterministic
Parquet generation, row-hash validation, S3 counters and PostgreSQL capacity
observations as the Differential comparison, rather than another load engine.

- Identical approximately 10/100/1,024 MiB inputs, actual sizes and hashes; three
  repetitions with rotated profile order. DuckLake data inlining is disabled.
- Empty/loaded-table DDL, bulk loading, 32-row commits, a 1% string update,
  half deletion and a full data-table drop. Each completed mutation's statement
  latency is the remote-sync acknowledgement, not a locally buffered return.
- Fresh processes through gateway B, without the writer's buffers or local
  authoritative files; full and selective scans each get a separate reader.
  External file and Parquet metadata caches are off, while native DuckDB buffers
  and explicitly budgeted SlateDB caches remain visible in same-client repeats.
- Attachment, schema discovery, first query and repeated query are separate.
  Their fresh-read sum excludes process/extension setup, which is also recorded.
- OpenDAL/cache statistics plus actual gateway request/response-byte deltas.
  DuckDB's httpfs log alone misses OpenDAL traffic. Successful timing samples
  reject retried 5xx responses instead of crediting backoff as an engine difference.
- Object bytes, allocated PostgreSQL bucket/catalog relations, WAL positions,
  client files and process RSS. These are point inventories, not a hard total
  memory/scratch admission budget or a cloud cost model.

All input/key/string/hash aggregates, schema defaults, updates/deletes and
post-cleanup sentinel reads must match. Reports retain per-path medians/budget
results; a gain in one query cannot hide a regression in another.

## Live-writer versus sealed-data reads

SlateDB can retain recently committed data in its object WAL/memtables. Opening a
new reader may replay substantial WAL data **during ATTACH**, even when a query
alone looks cheap. Small probes can show zero SST-cache hits because they never
exercise SSTs, not because cache settings were ineffective.

For each profile the benchmark measures both:

1. Fresh readers while the writer remains open.
2. A clean writer close, then new readers over the same data.

Closing the filesystem flushes its remaining state and can greatly reduce fresh
reader bootstrap. The **closing cost is measured**, and a break-even reader
count is reported when it saves time. All profiles, including DuckLake, go
through the same close/read/reopen workflow. The old writer is closed before the
new writer starts; this is not concurrent writing or a relaxed durability mode.

This is useful for explicitly quiescent/read-mostly delivery. It is **not** a
transparent optimization for a continuously active writer. Long-lived readers
can also reuse buffers/caches, but those private caches are not pgvs3 shared hits.

## Durability, read-only endpoints and writers

The extension routes the native database and recovery WAL files through SlateDB.
The contract/probe terminates the writer with `os._exit`, without an explicit
checkpoint/detach/shutdown, then verifies it using a different process/gateway
and resumes writing. That is process-death evidence, not power-loss or arbitrary
partition qualification.

**SQL read-only does not mean read-only object access.** The pinned extension
opens SlateDB readers in managed-checkpoint mode, which writes a retention
checkpoint to the object store. A SQL `READ_ONLY` attachment issued a PUT in the
probe. Through pgvs3's read-only gateway that PUT is correctly rejected; the
client then failed to complete within the bounded negative-probe deadline.
Neither the gateway's role restriction nor PostgreSQL standby safety is weakened
to accommodate it. Route these SQL read-only clients to a writable object
endpoint; this evaluation does not establish ObjFS on physical read-only replicas.

Existing readonly clients may follow the writer manifest on roughly a 10-second
poll interval, and detach/reattach can reuse the cached reader. Fresh-process
readers and same-process readers are therefore different consistency cases.
The benchmark does not claim instant refresh or certify follower behavior.

Use one externally coordinated writer. Upstream documents manifest-epoch
takeover as a delayed fencing backstop, not a fail-fast file lock. Concurrent
writers, automatic failover coordination and arbitrary network partitions are
outside this evaluation; do not infer them from a successful single-writer probe.

## Reclamation and limits

Native `CHECKPOINT` is not a request to compact/expire all SlateDB objects.
ObjFS 0.2.0 exposes cache/I/O statistics but no SQL override for SlateDB compaction
or object-retention policy. The benchmark records checkpoint, a bounded background
observation, writer close, pgvs3 reaping and ordinary PostgreSQL vacuum separately.
It does **not** delete SSTs/WAL/manifests manually or shorten their retention by
editing stored metadata. Objects retained by normal backend policy remain a cost.

Ordinary PostgreSQL vacuum may make dead pages reusable without shrinking relation
files. Deleting the disposable namespace happens only after measurements and is
not credited as table-level reclamation. Local/kernel/server cache state, encoded
file layout and extra memory all affect results; no default storage/client choice
changes merely because one native query is fast.

Raw jobs, results, profiles and failed probes remain in the unique printed
`.tmp/verify/bench.*/` directory. `--out` must be fresh. Standard-library tooling
tests run in CI; the large opt-in matrix is not a replacement for the existing
gateway performance acceptance or required client contract.
