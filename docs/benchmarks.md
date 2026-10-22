# Benchmarks

Two harnesses:

- **`just load`** drives the gateway directly with signed requests (signatures are ignored), so
  the numbers isolate HTTP, PostgreSQL and the storage layout.
- **`just bench`** drives DuckDB 2.0 / DuckLake through `httpfs`, which is the
  real client path and includes DuckDB's own request handling.

Everything runs on a disposable PostgreSQL 18 in Docker. The host for the
numbers below is a shared **6-vCPU / 11 GiB** Linux VM; the gateway uses
`PGVS3_POOL_MAX=128`, no peer cache, and loopback networking. Treat these as
indicative, not capacity planning.

## Go gateway, direct S3 load

Each run is 5 seconds. 4 KiB objects use 2,000 pre-created keys; 8 MiB and
64 MiB reads use 8 and 4 objects; the list case has 2,000 keys and returns
1,000 per page. The table below is **cache-off**; the small-object cache is
measured separately.

| Workload | Workers | Throughput | p50 | p99 |
| --- | ---: | ---: | ---: | ---: |
| 4 KiB GET | 1 | 2,965 req/s | 0.28 ms | 0.81 ms |
| 4 KiB GET | 16 | 12,969 req/s | 0.92 ms | 4.92 ms |
| 4 KiB GET | 64 | 15,141 req/s | 3.17 ms | 16.8 ms |
| 4 KiB GET | 256 | 18,387 req/s | 10.4 ms | 52.0 ms |
| HEAD | 1 | 3,320 req/s | 0.24 ms | 0.81 ms |
| HEAD | 16 | 14,431 req/s | 0.79 ms | 4.28 ms |
| HEAD | 64 | 17,610 req/s | 2.40 ms | 15.6 ms |
| 4 KiB PUT | 1 | 707 req/s | 1.32 ms | 2.81 ms |
| 4 KiB PUT | 16 | 2,142 req/s | 6.88 ms | 22.1 ms |
| 4 KiB PUT | 64 | 1,802 req/s | 32.9 ms | 62.9 ms |
| 4 KiB PUT | 256 | 1,635 req/s | 153 ms | 305 ms |
| 8 MiB GET | 1 | 417 MiB/s | 18.1 ms | 30.7 ms |
| 8 MiB GET | 8 | 1,006 MiB/s | 60.9 ms | 111 ms |
| 8 MiB GET | 16 | 1,090 MiB/s | 118 ms | 207 ms |
| 64 MiB GET | 1 | 316 MiB/s | 183 ms | 337 ms |
| 64 MiB GET | 8 | 971 MiB/s | 530 ms | 715 ms |
| 64 MiB GET | 16 | 1,055 MiB/s | 929 ms | 1,279 ms |
| 64 MiB PUT | 1 | 86 MiB/s | 577 ms | 605 ms |
| 64 MiB PUT | 8 | 171 MiB/s | 2,391 ms | 3,052 ms |
| 8 KiB range from 64 MiB | 64 | 6,397 req/s | 9.07 ms | 27.4 ms |
| ListObjectsV2 (1,000 keys) | 64 | 638 req/s | 98.4 ms | 127 ms |

Observations:

- **Small reads are the strongest path.** v9 stores ≤64 KiB objects inline
  and caches metadata/HEAD. One worker gets a 0.28 ms GET p50 and 0.24 ms HEAD
  p50; the gateway reaches 18.4k 4 KiB GET/s at 256 workers and 17.6k HEAD/s at
  64. `MetaHeader` now fills the cache with the full inline row, so a repeated
  HEAD does not keep paying the metadata query and a following GET hits too.
  Latency rises with queueing rather than per-request work.
- **Small writes are one SQL call.** A ≤64 KiB PUT is `s3p.put_inline`, which
  locks, checks conditions, queues replaced files, upserts the inline row and
  notifies caches in one round trip. 707/s at one worker, 2.1k/s at 16.
  Larger writes still use COPY and are PostgreSQL commit-bound.
- **Large reads stream well.** 8 MiB reads reach 1.09 GiB/s and 64 MiB reads
  1.06 GiB/s at 16 workers. Single-stream 64 MiB throughput varies with host
  contention (316–441 MiB/s across runs); the 8-way snapshot path sustains
  ~1 GiB/s. Responses up to 8 MiB use synchronous rows; longer spans keep the
  cut-through goroutine so the database fetch overlaps socket writes.
- **Ranges are cheap after the single-piece path.** An 8 KiB range from a
  64 MiB object reaches 6.4k/s at a 9.1 ms p50.
- **Lists improved with a dedicated index-scan pool.** 638/s is about 638,000
  keys/s; each request still materialises 1,000 XML entries.

## Inline threshold

| Object | 64 KiB inline | 8 KiB cutoff (chunked) |
| --- | ---: | ---: |
| 32 KiB PUT, 1 worker | 509/s | 227/s |
| 32 KiB GET, 1 worker | 3,619/s | 1,398/s |
| 64 KiB PUT, 1 worker | 356/s | 200/s |
| 64 KiB GET, 1 worker | 3,250/s | 1,272/s |

64 KiB remains the default; chunking only wins below roughly 8 KiB, where both
paths are already one row.

## Async commit

Unconditional writes can use `PGVS3_WRITE_COMMIT=off`/`local`; conditional
writes still force `on`. With `off`:

| 4 KiB PUT | `on` | `off` |
| --- | ---: | ---: |
| 1 worker | 777/s · 1.24 ms | 983/s · 0.94 ms |
| 16 workers | 2,201/s · 6.89 ms | 3,970/s · 3.75 ms |
| 64 workers | 1,844/s · 33.0 ms | 2,736/s · 21.9 ms |

This trades crash durability for throughput; the acknowledged write is in
PostgreSQL's WAL but the local fsync may lag. Keep `on` unless the workload is
replaceable.

## Small-object cache

v9 inlines objects up to 64 KiB, so the 4 KiB numbers above already bypass the
chunk table and do not use the byte cache. The Otter cache remains for
non-inline objects between 64 KiB and 256 KiB; a single gateway invalidates
locally, while peer mode revalidates each hit. The earlier v8 4 KiB cache
measurements are obsolete and are not shown here.

## Partitioning A/B

Same 4 KiB workload, hash-partitioned `s3p.chunks`:

| Workload | 1 partition | 16 partitions | 64 partitions |
| --- | ---: | ---: | ---: |
| GET, 1 worker | 1,104/s | 888/s | 945/s |
| GET, 16 workers | 3,852/s | 2,784/s | 3,227/s |
| GET, 64 workers | 3,940/s | 3,232/s | 3,281/s |
| PUT, 1 worker | 218/s | 187/s | 213/s |
| PUT, 16 workers | 1,051/s | 908/s | 981/s |
| PUT, 64 workers | 1,177/s | 1,118/s | 933/s |

Partitioning loses on every point-read row and mostly loses on writes. All
rows of one file hash to one partition, so it cannot parallelize a single-file
read; planner overhead grows with partition count. DuckLake's single writer
has no insert contention to spread.

A two-level layout — `HASH(file_id)` then `RANGE(no)` into 8×8 leaves — is the
best alternative key. With 8 MiB parallel pieces it reaches **556 MiB/s** for a
64 MiB GET at one worker (versus 441 MiB/s unpartitioned), but 4 KiB GET falls
to 868/s at one worker and 2,628/s at 16 (versus 1,104 and 3,852), and
16-worker PUTs lose about 10%. Large-read gains do not pay for the small-read
regression. The default stays **1 partition**.

## DuckDB 2.0 through the gateway

`just bench s3`, same host, 2,000 four-KiB and 128 eight-MiB objects:

| Workload | Workers | Result | p50 |
| --- | ---: | ---: | ---: |
| 4 KiB GET | 64 | 1,838 req/s | 32.8 ms |
| 4 KiB GET, first read | 64 | 1,467 req/s | 40.5 ms |
| 4 KiB PUT | 64 | 668 req/s | 87.5 ms |
| 8 MiB GET | 16 | 674 MiB/s | 181 ms |
| 8 MiB PUT | 16 | 120 MiB/s | 996 ms |

DuckDB's httpfs client adds request scheduling, HTTP parsing and its own
connection pool; the direct numbers above show the gateway's ceiling without
that layer. `duckdb-object-storage` wrote, checkpointed and reread a 5M-row
database in about 2.5 s in the contract environment.

## S3 Express One Zone: published characteristics

These are AWS's published service characteristics, **not measurements on this
host**. Sources: [performance guide][express-perf], [pricing][express-price],
[2025 price reduction][express-price-cut], [FAQ][s3-faq], [launch post][express-launch].

| Dimension | S3 Express One Zone |
| --- | --- |
| First-byte latency | Consistent single-digit milliseconds |
| Reads per directory bucket | 200,000/s default; up to 2,000,000/s with a limit increase |
| Writes per directory bucket | 100,000/s default; up to 200,000/s with a limit increase |
| Availability | 99.95% designed; 99.9% SLA |
| Durability | 99.999999999% within one AZ; loss of the AZ can lose data |
| Namespace | Directory bucket, hierarchical, `/` is the only delimiter |
| Max object | 50 TB; 5 GB per single PUT |
| Auth | IAM plus `CreateSession` (5-minute bucket-scoped credentials) |
| Storage price, us-east-1 | $0.11/GB-month |
| PUT/COPY/POST/LIST | $0.00113 per 1,000 requests up to 512 KB |
| GET | $0.00003 per 1,000 requests up to 512 KB |
| Large-request transfer | $0.0032/GB upload, $0.0006/GB retrieval |

## Contrast

| Dimension | pgvs3 (measured, 6 vCPU) | S3 Express One Zone (published) |
| --- | --- | --- |
| 4 KiB GET p50 | 0.28–3.2 ms at 1–64 workers | Consistent single-digit ms |
| 4 KiB GET throughput | ~18.4k/s at 256 workers | 200k/s default, 2M/s max per bucket |
| 4 KiB PUT throughput | ~2.2k/s (4k/s async at 16) | 100k/s default, 200k/s max per bucket |
| 8–64 MiB GET | 0.3–1.1 GiB/s | Network/concurrency-bound; elastic |
| 64 MiB PUT | 86–171 MiB/s | Elastic, distributed |
| Capacity | 32 TiB per database; ~17.4 TB per object | Unlimited storage; 50 TB per object |
| Availability | Whatever PostgreSQL/your deployment provides | 99.95% designed, 99.9% SLA |
| Durability | PostgreSQL replication/backups chosen by the operator | 11 nines within one AZ; AZ loss risk |
| Price model | Hardware + PostgreSQL; no per-request fee | $0.11/GB-month + per-request + per-GB |

**What the numbers mean**

- **Per-request latency is competitive.** At one to sixteen workers, pgvs3's
  4 KiB GET p50 is 0.28–0.92 ms, below S3 Express's single-digit-millisecond
  target. The loopback path removes the network and object-store metadata
  hops; inline objects remove the chunk lookup entirely.
- **Aggregate throughput is not.** Six cores and one PostgreSQL reach about 4k
  GET/s and 1.2k PUT/s. S3 Express's default per-bucket limits are 200k reads
  and 100k writes per second; the ceilings are 2M and 200k. Reaching those
  numbers needs a fleet and a much larger PostgreSQL, not one gateway.
- **Large-object reads are bandwidth-bound in both systems.** pgvs3 reaches
  about 1 GiB/s on loopback at 16 readers. S3 Express's per-stream throughput
  depends on the client's network path and concurrency; AWS does not publish a
  single-stream number.
- **Writes are the local bottleneck.** 4 KiB PUTs pay a synchronous PostgreSQL
  commit per object; 64 MiB PUTs reach 186 MiB/s at eight workers. S3 Express
  spreads writes across its directory bucket.
- **Cost flips with request volume.** 1 TB on Express is $112.64/month. One
  billion 4 KiB GETs is about $30; one billion 4 KiB PUTs is about $1,130.
  pgvs3 has no request bill, so request-heavy workloads can be much cheaper if
  the PostgreSQL hardware is already paid for. Low-volume or spiky workloads
  usually favor the managed service.
- **Durability and blast radius differ.** Express replicates within one AZ and
  can lose data if that AZ is lost. pgvs3's durability is whatever your
  PostgreSQL deployment provides; a single primary with no backup or standby
  is a single point of failure.

## Scope

pgvs3 implements the standard S3 data plane and is not an S3 Express One Zone
replacement. Directory buckets, `CreateSession`, `RenameObject` and
`AppendObject` have been removed to keep the gateway focused on DuckDB,
DuckLake and ordinary S3 clients.

Still missing:

- IAM/bucket policies, access points and VPC endpoints.
- Lifecycle expiration and transitions.
- Multipart `x-amz-checksum-type: FULL_OBJECT`.
- 50 TB objects (pgvs3 caps at about 17.4 TB per object and 32 TiB per
  database).
- The request-rate scale above.

Features Express itself does not support — versioning, object tags, Object
Lock, ACLs, replication, Event Notifications, S3 Select, website hosting,
access logs, Storage Lens, Transfer Acceleration, SSE-C and DSSE-KMS — are
also unsupported here.

## Reproduce

```sh
just load                 # direct gateway matrix, cache off
OBJECT_CACHE_BYTES=67108864 just load   # direct matrix, small-object cache on
just bench s3             # DuckDB 2.0 httpfs matrix, writes .tmp/pgvs3/bench.json
just bench                # plus DuckLake compatibility and tuning
just contract             # correctness gate before trusting a number
```

[express-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html
[express-price]: https://aws.amazon.com/s3/pricing/
[express-price-cut]: https://aws.amazon.com/blogs/aws/announcing-up-to-85-price-reductions-for-amazon-s3-express-one-zone/
[express-launch]: https://aws.amazon.com/blogs/aws/new-amazon-s3-express-one-zone-high-performance-storage-class/
[s3-faq]: https://aws.amazon.com/s3/faqs/
