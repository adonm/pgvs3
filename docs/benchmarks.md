# Benchmarks

Two benchmark harnesses are included in the repository:

- **`just load`** runs `test/loadgen` directly against the HTTP gateway to
  measure raw S3 + PostgreSQL throughput and latency.
- **`just bench`** runs DuckDB 2.0 and DuckLake through `httpfs` to measure
  end-to-end client performance.

Both harnesses start a disposable PostgreSQL 18 container (`shared_buffers=1GB`,
`PGVS3_POOL_MAX=128`, loopback networking). Numbers below are from a **16-core
local host** (Intel Core Ultra X7 358H, 62 GiB RAM) alongside an earlier
**6-vCPU / 11 GiB VM** baseline.

## Direct S3 gateway load (`just load`)

Each test runs for 5 seconds. 4 KiB tests use 2,000 keys; 8 MiB and 64 MiB
tests use 16 and 8 objects; `ListObjectsV2` lists 1,000 keys per page from a
2,000-key bucket. Gateways hold no in-process cache; metadata and ≤64 KiB
objects are served directly from PostgreSQL `shared_buffers`.

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

Key takeaways:

- **Sub-millisecond small reads from `shared_buffers`:** A ≤64 KiB `GET` or
  `HEAD` is a primary-key B-tree lookup in `s3p.objects` (~6 µs in PostgreSQL,
  0.15–0.17 ms p50 over HTTP at 1 worker, scaling to 32.5k `GET`/s and 30.2k
  `HEAD`/s on 16 cores).
- **17.5k/s durable small writes:** A ≤64 KiB `PUT` executes in one SQL call
  (`s3p.put_inline`) with `synchronous_commit = on`. Removing `pg_notify`
  eliminated PostgreSQL commit-queue serialization.
- **Multi-GiB/s large-object reads:** 64 MiB reads stream at 1.19 GiB/s from 1
  worker and 3.45 GiB/s across 16 workers.
- **Fast byte ranges and listings:** 8 KiB range reads from 64 MiB objects
  reach 16.1k req/s (3.55 ms p50), and 1,000-key `ListObjectsV2` calls reach
  1,840 req/s (~1.84M keys/s).

## DuckDB 2.0 `httpfs` (`just bench s3`)

Measured on the 6-vCPU VM with 2,000 4 KiB objects and 128 8 MiB objects
(`enable_external_file_cache = false`):

| Workload | Workers | Result | p50 |
| --- | ---: | ---: | ---: |
| 4 KiB GET (steady state) | 64 | 1,838 req/s | 32.8 ms |
| 4 KiB GET (first read) | 64 | 1,467 req/s | 40.5 ms |
| 4 KiB PUT | 64 | 668 req/s | 87.5 ms |
| 8 MiB GET | 16 | 674 MiB/s | 181 ms |
| 8 MiB PUT | 16 | 120 MiB/s | 996 ms |

DuckDB's `httpfs` adds its own request scheduling and connection pool; the
direct numbers above show the gateway ceiling underneath it.

## Design ablations

### 64 KiB inline threshold vs 8 KiB chunking (6-vCPU VM, 1 worker)

| Object size & op | 64 KiB inline (default) | 8 KiB cutoff (chunked) |
| --- | ---: | ---: |
| 32 KiB PUT / GET | 509/s · 3,619/s | 227/s · 1,398/s |
| 64 KiB PUT / GET | 356/s · 3,250/s | 200/s · 1,272/s |

Inlining up to 64 KiB in `s3p.objects` is 2–3× faster than splitting into
`s3p.chunks` rows.

### Single table vs partitioned `s3p.chunks`

Hash-partitioning `s3p.chunks` into 16 or 64 partitions slowed 4 KiB `GET`s by
17–28% and `PUT`s by 5–21% due to planner overhead without spreading single-file
reads. A two-level `8×8` hash/range layout improved single-worker 64 MiB `GET`
from 441 to 556 MiB/s but regressed 4 KiB `GET`s by 21–32%. `s3p.chunks`
therefore stays a single unpartitioned table; see
[partitioning.md](partitioning.md).

## pgvs3 vs AWS S3 Express One Zone

S3 Express One Zone figures below are AWS's published service specifications and
`us-east-1` prices ([performance guide][express-perf], [pricing][express-price],
[2025 price reduction][express-price-cut]).

| Dimension | pgvs3 (16-core local / 6-vCPU VM) | S3 Express One Zone (published) |
| --- | --- | --- |
| 4 KiB GET / PUT p50 (1w) | 0.17 ms GET, 0.78 ms PUT (0.47 / 1.37 ms on 6 vCPU) | Consistent single-digit ms |
| 4 KiB GET throughput | 32.5k/s (9.8k/s on 6 vCPU) | 200k/s default, 2M/s max per bucket |
| 4 KiB PUT throughput | 17.5k/s durable at 64w (3.7k/s on 6 vCPU) | 100k/s default, 200k/s max per bucket |
| 8–64 MiB GET | 1.1–3.45 GiB/s (0.3–1.1 GiB/s on 6 vCPU) | Network/concurrency-bound; elastic |
| 64 MiB PUT | 188–458 MiB/s (86–171 MiB/s on 6 vCPU) | Elastic, distributed |
| Capacity | 32 TiB per database; ~17.4 TB per object | Unlimited storage; 50 TB per object |
| Availability / durability | PostgreSQL replication/backups chosen by operator | 99.95% designed (99.9% SLA); 11 nines in one AZ |
| Price model (`us-east-1`) | Hardware + gp3 ($0.08/GB-mo); $0 request/transfer fees | $0.11/GB-mo + $1.13/M PUT + $0.03/M GET + $3.80/TB R+W |

### Top 6 takeaways

1. **Latency:** pgvs3 serves a 4 KiB `GET` in `0.17 ms` and a durable `PUT` in
   `0.78 ms` p50, ~10× faster than S3 Express's single-digit milliseconds.
2. **Small-object reads:** pgvs3 peaks at `32.5k GET/s` on one 16-core node,
   ~6× below S3 Express's `200k/s` default bucket quota (`2M/s` max).
3. **Small-object writes:** pgvs3 reaches `17.5k` durable 4 KiB `PUT/s`, ~6×
   below S3 Express's `100k/s` default quota (`200k/s` max).
4. **Large-object bandwidth:** pgvs3 streams 64 MiB reads at `3.45 GiB/s` and
   writes at `458 MiB/s` in cache/NVMe (`125–1,000 MiB/s` on cold EBS gp3),
   while S3 Express scales elastically with client network.
5. **500 GB large-file cost (7,450 × 64 MiB, 1× W+R/mo):** S3 Express costs
   ~$57/mo vs ~$100–200/mo for a dedicated pgvs3 node, or $40/mo in gp3 disk on
   an existing PostgreSQL.
6. **500 GB small-object cost (122M × 4 KiB, 1× W+R/mo):** S3 Express costs
   ~$199/mo due to `$1.13/M` `PUT` fees, while pgvs3 charges $0 per request and
   stays at ~$165/mo dedicated or $40/mo on an existing PostgreSQL.

## Reproduce

```sh
just contract   # run the correctness suite first
just load       # direct S3 load matrix (.tmp/pgvs3/load.json)
just bench s3   # DuckDB 2.0 httpfs matrix (.tmp/pgvs3/bench.json)
just bench      # full DuckDB + DuckLake compatibility and row-group tuning
```

[express-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-performance.html
[express-price]: https://aws.amazon.com/s3/pricing/
[express-price-cut]: https://aws.amazon.com/blogs/aws/announcing-up-to-85-price-reductions-for-amazon-s3-express-one-zone/
