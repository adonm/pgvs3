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
| **RESP 4 KiB GET (pipeline 1)** | 1 | **9,850 req/s** | **0.081 ms** | 0.32 ms | — |
| **RESP 4 KiB GET (pipeline 1)** | 64 | **91,046 req/s** | **0.63 ms** | 1.96 ms | — |
| **RESP 4 KiB GET (pipeline 16)** | 64 | **459,018 req/s (1.79 GiB/s)** | **0.13 ms** | 0.35 ms | — |
| **RESP 4 KiB SET (pipeline 1)** | 1 | **1,363 req/s** | **0.68 ms** | 1.40 ms | — |
| **RESP 4 KiB SET (pipeline 1)** | 64 | **23,866 req/s** | **2.59 ms** | 5.22 ms | — |
| **RESP 4 KiB SET (pipeline 16)** | 64 | **110,222 req/s (431 MiB/s)** | **0.56 ms** | 1.13 ms | — |
| 4 KiB GET (S3) | 1 | 4,993 req/s | 0.17 ms | 0.63 ms | 1,970 req/s (0.47 ms) |
| 4 KiB GET (S3) | 16 | 20,556 req/s | 0.67 ms | 2.04 ms | 6,596 req/s (1.98 ms) |
| 4 KiB GET (S3) | 64 | 25,284 req/s | 2.27 ms | 6.80 ms | 8,243 req/s (6.91 ms) |
| 4 KiB GET (S3) | 256 | 35,458 req/s | 6.42 ms | 20.7 ms | 9,756 req/s |
| HEAD (S3) | 1 | 5,878 req/s | 0.14 ms | 0.52 ms | 1,778 req/s (0.47 ms) |
| HEAD (S3) | 16 | 17,397 req/s | 0.77 ms | 2.60 ms | 7,925 req/s (1.66 ms) |
| HEAD (S3) | 64 | 30,009 req/s | 1.84 ms | 6.15 ms | 9,211 req/s (5.95 ms) |
| 4 KiB PUT (S3) | 1 | 1,114 req/s | 0.82 ms | 1.52 ms | 673 req/s (1.37 ms) |
| 4 KiB PUT (S3) | 16 | 10,474 req/s | 1.39 ms | 2.61 ms | 3,610 req/s (3.93 ms) |
| 4 KiB PUT (S3) | 64 | 17,801 req/s | 3.04 ms | 18.6 ms | 3,735 req/s (14.5 ms) |
| 4 KiB PUT (S3) | 256 | 20,367 req/s | 11.0 ms | 37.9 ms | 1,635 req/s (153 ms) |
| 8 MiB GET (S3) | 1 | 1,450 MiB/s | 5.36 ms | 7.88 ms | 417 MiB/s (18.1 ms) |
| 8 MiB GET (S3) | 8 | 3,496 MiB/s | 18.0 ms | 26.9 ms | 1,006 MiB/s (60.9 ms) |
| 8 MiB GET (S3) | 16 | 3,753 MiB/s | 33.7 ms | 59.4 ms | 1,090 MiB/s (118 ms) |
| 64 MiB GET (S3) | 1 | 1,269 MiB/s | 46.4 ms | 108 ms | 316 MiB/s (183 ms) |
| 64 MiB GET (S3) | 8 | 3,837 MiB/s | 129 ms | 219 ms | 971 MiB/s (530 ms) |
| 64 MiB GET (S3) | 16 | 4,262 MiB/s | 234 ms | 338 ms | 1,055 MiB/s (929 ms) |
| 64 MiB PUT (S3) | 1 | 220 MiB/s | 183 ms | 223 ms | 86 MiB/s (577 ms) |
| 64 MiB PUT (S3) | 8 | 509 MiB/s | 820 ms | 1,109 ms | 171 MiB/s (2,391 ms) |
| 8 KiB range from 64 MiB | 64 | 16,171 req/s | 3.57 ms | 9.67 ms | 6,397 req/s (9.07 ms) |
| ListObjectsV2 (1,000 keys) | 64 | 2,019 req/s | 27.3 ms | 99.1 ms | 638 req/s (98.4 ms) |
| **RESP 4 KiB GET** | 1 / 64 / 64p16 | **12,242 / 96,966 / 460,381 req/s** | **0.072 / 0.60 / 0.128 ms** | 0.21 / 1.74 / 0.34 ms | — |
| **RESP 4 KiB SET (sync)** | 1 / 64 / 64p16 | **5,018 / 48,215 / 213,603 req/s** | **0.183 / 1.12 / 0.284 ms** | 0.46 / 4.13 / 0.66 ms | — |
| **SQS SendMessage 4 KiB** | 1 / 64 / 64p16 | **1,083 / 19,795 / 12,500 msg/s** | **0.86 / 2.98 / 4.90 ms** | 1.66 / 9.32 / 10.9 ms | — |
| **SQS Receive+Delete 4 KiB** | 1 / 32 | **96 / 18,524 msg/s** | **99.7 / 16.6 ms** | 149 / 66 ms | — |
| **Cache tier 4 KiB GET (warm)** | 1 / 16 | **3,121 / 12,233 req/s** | **0.29 / 1.18 ms** | 0.71 / 2.98 ms | — |
| **Cache tier 4 KiB PUT (write-through)** | 1 / 16 | **974 / 5,326 req/s** | **0.92 / 2.56 ms** | 2.77 / 7.34 ms | — |
| **Cache tier 64 MiB GET (warm)** | 1 / 16 | **1,442 / 4,005 MiB/s** | 43.9 / 253 ms | 60.2 / 366 ms | — |
| **Cache tier 64 MiB PUT (write-through)** | 1 / 16 | **205 / 488 MiB/s** | 227 / 1,307 ms | 236 / 3,268 ms | — |

Key takeaways:

- **Sub-100 µs RESP reads & 460k GET/s pipelined:** a 1-worker 4 KiB `GET`
  completes in **`0.072 ms` (72 µs) p50** (12.2k req/s), scales to **97.0k
  `GET`/s** unpipelined and **460.4k `GET`/s (1.76 GiB/s)** at pipeline 16 via
  `CacheMGet` coalescing.
- **213k/s durable pipelined RESP writes:** pipelined `SET`s coalesce into one
  `CacheMSet` statement with LZ4 block compression, reaching **213.6k durable
  `SET`/s (834 MiB/s, `0.28 ms` p50)** at pipeline 16 (48.2k unpipelined).
- **SQS:** **19.8k `SendMessage`/s** at 64 workers and **18.5k
  receive+delete/s** at 32 workers after indexing receipts (`messages_receipt`),
  up from 2.1k/s when acks scanned the queue. Each receive is one `SKIP LOCKED`
  lease, each delete one indexed receipt lookup.
- **Sub-millisecond S3 small reads & 20.9k/s durable S3 writes:** S3 4 KiB
  `GET` reaches **35.8k req/s** (`0.17 ms` p50 at 1 worker) and durable 4 KiB
  `PUT` reaches **20.9k req/s**.
- **Cache tier:** a warm 4 KiB `GET` through the cache proxy costs one
  PostgreSQL read and reaches **12.2k req/s**; 64 MiB reads reach **4.0 GiB/s**
  and write-through PUTs **488 MiB/s** (origin is a local filesystem).
- **Cache ranges are passed through:** a cold 1 MiB range of a 256 MiB origin
  object returns in **2.2 ms** without caching the body; the first full read
  takes 0.90 s and the warm read 0.18 s.
- **Multi-GiB/s large-object reads:** 64 MiB reads stream at **1.30 GiB/s** from
  1 worker and **4.17 GiB/s (4,275 MiB/s)** across 16 workers.
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
