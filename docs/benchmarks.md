# Benchmarks

Two harnesses:

- **`just load`** drives the gateway directly with signed requests (signatures are ignored), so
  the numbers isolate HTTP, PostgreSQL and the storage layout.
- **`just bench`** drives DuckDB 2.0 / DuckLake through `httpfs`, which is the
  real client path and includes DuckDB's own request handling.

Everything runs on a disposable PostgreSQL 18 in Docker (`shared_buffers=1GB`,
`PGVS3_POOL_MAX=128`, loopback networking). Numbers below are from a **16-core
local host** (Intel Core Ultra X7 358H, 62 GiB RAM), with earlier **6-vCPU /
11 GiB VM** figures noted for comparison. Treat these as indicative, not
capacity planning.

## Go gateway, direct S3 load

Each run is 5 seconds. 4 KiB objects use 2,000 pre-created keys; 8 MiB and
64 MiB reads use 16 and 8 objects; the list case has 2,000 keys and returns
1,000 per page. Gateways hold no in-process cache; metadata and inline objects
are served directly from PostgreSQL `shared_buffers`.

| Workload | Workers | 16-core host | p50 | p99 | 6-vCPU VM |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 KiB GET | 1 | 4,765 req/s | 0.17 ms | 0.67 ms | 1,970 req/s (0.47 ms) |
| 4 KiB GET | 16 | 17,817 req/s | 0.78 ms | 2.29 ms | 6,596 req/s (1.98 ms) |
| 4 KiB GET | 64 | 24,582 req/s | 2.32 ms | 6.93 ms | 8,243 req/s (6.91 ms) |
| 4 KiB GET | 256 | 32,531 req/s | 6.87 ms | 23.5 ms | 9,756 req/s |
| HEAD | 1 | 5,711 req/s | 0.15 ms | 0.52 ms | 1,778 req/s (0.47 ms) |
| HEAD | 16 | 20,984 req/s | 0.65 ms | 2.03 ms | 7,925 req/s (1.66 ms) |
| HEAD | 64 | 30,223 req/s | 1.85 ms | 5.93 ms | 9,211 req/s (5.95 ms) |
| 4 KiB PUT | 1 | 1,142 req/s | 0.78 ms | 1.58 ms | 673 req/s (1.37 ms) |
| 4 KiB PUT | 16 | 10,000 req/s | 1.41 ms | 3.53 ms | 3,610 req/s (3.93 ms) |
| 4 KiB PUT | 64 | 17,470 req/s | 3.10 ms | 18.9 ms | 3,735 req/s (14.5 ms) |
| 4 KiB PUT | 256 | 11,406 req/s | 19.6 ms | 61.3 ms | 1,635 req/s (153 ms) |
| 8 MiB GET | 1 | 1,086 MiB/s | 7.19 ms | 11.4 ms | 417 MiB/s (18.1 ms) |
| 8 MiB GET | 8 | 2,881 MiB/s | 22.1 ms | 33.7 ms | 1,006 MiB/s (60.9 ms) |
| 8 MiB GET | 16 | 3,085 MiB/s | 39.9 ms | 75.8 ms | 1,090 MiB/s (118 ms) |
| 64 MiB GET | 1 | 1,185 MiB/s | 51.4 ms | 101 ms | 316 MiB/s (183 ms) |
| 64 MiB GET | 8 | 3,307 MiB/s | 152 ms | 204 ms | 971 MiB/s (530 ms) |
| 64 MiB GET | 16 | 3,455 MiB/s | 288 ms | 427 ms | 1,055 MiB/s (929 ms) |
| 64 MiB PUT | 1 | 188 MiB/s | 207 ms | 237 ms | 86 MiB/s (577 ms) |
| 64 MiB PUT | 8 | 458 MiB/s | 962 ms | 1,151 ms | 171 MiB/s (2,391 ms) |
| 8 KiB range from 64 MiB | 64 | 16,074 req/s | 3.55 ms | 9.83 ms | 6,397 req/s (9.07 ms) |
| ListObjectsV2 (1,000 keys) | 64 | 1,840 req/s | 30.3 ms | 109 ms | 638 req/s (98.4 ms) |

Observations:

- **Small reads come straight from PostgreSQL `shared_buffers`.** v9 stores
  ≤64 KiB objects inline in `s3p.objects`. With `enable_indexscan = on` on the
  main pool and no per-request `BucketExists` probe, a primary-key `Index Scan`
  takes ~6 µs inside PostgreSQL (0.17 ms GET / 0.15 ms HEAD p50 end-to-end over
  HTTP at 1 worker, reaching 32.5k GET/s and 30.2k HEAD/s on 16 cores) with
  zero gateway cache state and zero invalidation lag across multiple gateways.
- **Small writes are one SQL call.** A ≤64 KiB PUT is `s3p.put_inline`.
  Removing `pg_notify` and keeping `enable_indexscan = on` on the main pool
  reaches **17.5k durable 4 KiB PUT/s** at 64 workers on 16 cores (3.7k/s on the
  6-vCPU VM) with `synchronous_commit = on`.
- **Large reads stream at multi-GiB/s from cache.** 64 MiB reads reach
  1.19 GiB/s at 1 worker and 3.45 GiB/s at 16 workers on 16 cores (~1.06 GiB/s
  on 6 vCPU). Responses up to 8 MiB use synchronous rows; longer spans keep the
  cut-through goroutine so the database fetch overlaps socket writes.
- **Ranges and listings scale with the index-scan pool.** An 8 KiB range from a
  64 MiB object reaches 16.1k/s at 3.55 ms p50; `ListObjectsV2` reaches 1,840
  req/s (~1.84M keys/s) at 64 workers.

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

| Dimension | pgvs3 (16-core local / 6-vCPU VM) | S3 Express One Zone (published) |
| --- | --- | --- |
| 4 KiB GET / PUT p50 (1w) | 0.17 ms GET, 0.78 ms PUT (0.47 / 1.37 ms on 6 vCPU) | Consistent single-digit ms |
| 4 KiB GET throughput | 32.5k/s (9.8k/s on 6 vCPU) | 200k/s default, 2M/s max per bucket |
| 4 KiB PUT throughput | 17.5k/s durable at 64w (3.7k/s on 6 vCPU) | 100k/s default, 200k/s max per bucket |
| 8–64 MiB GET | 1.1–3.45 GiB/s (0.3–1.1 GiB/s on 6 vCPU) | Network/concurrency-bound; elastic |
| 64 MiB PUT | 188–458 MiB/s (86–171 MiB/s on 6 vCPU) | Elastic, distributed |
| Capacity | 32 TiB per database; ~17.4 TB per object | Unlimited storage; 50 TB per object |
| Availability / durability | PostgreSQL replication/backups chosen by the operator | 99.95% designed (99.9% SLA); 11 nines in one AZ |
| Price model (`us-east-1`) | Hardware + gp3 ($0.08/GB-mo); $0 request/transfer fees | $0.11/GB-mo + $1.13/M PUT + $0.03/M GET + $3.80/TB R+W |

**Top 6 takeaways**

1. **Latency:** pgvs3 serves a 4 KiB GET in 0.17 ms and a durable PUT in 0.78 ms p50, ~10× faster than S3 Express's single-digit milliseconds.
2. **Small-object reads:** pgvs3 peaks at 32.5k GET/s on one 16-core node, ~6× below S3 Express's 200k/s default bucket quota (2M/s max).
3. **Small-object writes:** pgvs3 reaches 17.5k durable 4 KiB PUT/s, ~6× below S3 Express's 100k/s default quota (200k/s max).
4. **Large-object bandwidth:** pgvs3 streams 64 MiB reads at 3.45 GiB/s and writes at 458 MiB/s in cache/NVMe (125–1,000 MiB/s on cold gp3), while S3 Express scales elastically with client network.
5. **500 GB large-file cost (7,450 × 64 MiB, 1× W+R/mo):** S3 Express costs ~$57/mo vs ~$100–200/mo for a dedicated pgvs3 node, or $40/mo in gp3 disk on an existing PostgreSQL.
6. **500 GB small-object cost (122M × 4 KiB, 1× W+R/mo):** S3 Express costs ~$199/mo due to $1.13/M PUT fees, while pgvs3 charges $0 per request and stays at ~$165/mo dedicated or $40/mo on an existing PostgreSQL.

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
just load                 # direct gateway matrix
just bench s3             # DuckDB 2.0 httpfs matrix, writes .tmp/pgvs3/bench.json
just bench                # plus DuckLake compatibility and tuning
just contract             # correctness gate before trusting a number
```

[express-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html
[express-price]: https://aws.amazon.com/s3/pricing/
[express-price-cut]: https://aws.amazon.com/blogs/aws/announcing-up-to-85-price-reductions-for-amazon-s3-express-one-zone/
[express-launch]: https://aws.amazon.com/blogs/aws/new-amazon-s3-express-one-zone-high-performance-storage-class/
[s3-faq]: https://aws.amazon.com/s3/faqs/
