# Benchmarks

`just bench` runs [`deploy/bench/duck_bench.py`](../deploy/bench/duck_bench.py)
against a disposable PostgreSQL 18 and two local gateways, with DuckDB (httpfs)
as the S3 client. The client is the workload that matters here: DuckLake. The
raw record from the run below is [`results/bench.json`](results/bench.json).

**Machine:** one Intel Core Ultra X7 358H workstation (16 cores, 62 GiB). Client,
gateways and PostgreSQL (Docker, `shared_buffers=1GB`) share the host over
loopback. DuckDB 1.5.6, DuckDB's external file cache off, so every read reaches
the gateway. Run-to-run variation is about ±15%.

## S3 requests

Each object is one statement from one of N concurrent DuckDB cursors:
`COPY … TO 's3://…'` for PUT and `read_blob('s3://…')` for GET, which DuckDB
sends as HEAD + GET. *Per object* is the time DuckDB takes for that
statement, measured by the client at microsecond resolution. *Per request* comes
from DuckDB's HTTP log, which records whole milliseconds.

| 4 KiB objects (2,000) | Objects/s | Per object p50 / p99 | Per GET request p50 / p99 |
| --- | ---: | ---: | ---: |
| PUT, 1 in flight | 385 | 2.5 / 4.2 ms | — |
| GET, 1 in flight | 910 | 1.0 / 2.0 ms | <1 / 1 ms |
| PUT, 64 in flight | 4,395 | 13 / 26 ms | — |
| GET, 64 in flight | 7,000–7,600 | 7.3 / 16.8 ms | 1 / 3 ms |

| 8 MiB objects (128, 1 GiB) | MiB/s | Per object p50 / p99 |
| --- | ---: | ---: |
| PUT, 1 in flight | 82 | 97 / 106 ms |
| GET, 1 in flight | 548 | 14 / 22 ms |
| PUT, 16 in flight | 450 | 287 / 378 ms |
| GET, 16 in flight, first read after write | 450 | 257 / 621 ms |
| GET, 16 in flight, repeat read | 2,069 | 56 / 93 ms |

The first read of freshly written rows sets PostgreSQL hint bits. With data
checksums (the PostgreSQL 18 default) that WAL-logs whole pages, so the first
concurrent read of new data runs at about a quarter of the steady-state rate.
DuckLake writes a data file once and reads it many times.

### Amazon S3, published figures

Not measured here. S3 figures are from EC2 in the same region.

| | Amazon S3 | Source |
| --- | --- | --- |
| Small-object / first-byte latency | ~30 ms median (1 KiB, base latency); AWS quotes "roughly 100–200 ms" | [AnyBlob §2.3, §2.8][anyblob]; [AWS performance guide][aws-perf] |
| 8 MiB GET | ~190 ms median: 30 ms + ~20 ms/MiB | [AnyBlob model, §2.8][anyblob] |
| Bandwidth per request | 25–95 MiB/s, median 55–60 MiB/s | [AnyBlob §2.3][anyblob] |
| Aggregate bandwidth | 75–90 Gbit/s (~9–11 GiB/s) on a 100 Gbit/s instance with 200–250 requests in flight | [AnyBlob §2.4, §2.8][anyblob] |
| Request rate | ≥5,500 GET/HEAD and ≥3,500 PUT/COPY/POST/DELETE per second per prefix; unlimited prefixes | [AWS performance guide][aws-perf] |

pgvs3 answers a request 1–2 orders of magnitude faster, and one stream moves
about 10× more bytes per second. That is why a DuckDB query touching many
Parquet footers and row groups is cheap. S3 scales request rate and aggregate
bandwidth by adding prefixes and instances. pgvs3 scales to what one PostgreSQL
serves: about 7,000 small GETs/s and 2 GiB/s here, with PostgreSQL, the
gateways and DuckDB sharing 16 cores.

[anyblob]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf "Durner, Leis, Neumann. Exploiting Cloud Object Storage for High-Performance Analytics. PVLDB 16(11), 2023"
[aws-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

## DuckLake compatibility

`just ducklake` (also in CI) attaches one DuckLake catalog (PostgreSQL) from two
DuckDB instances: a writer through gateway A and a reader through gateway B.
All 9 checks pass on DuckDB 1.5.6:

- the reader sees the writer's table;
- UPDATE and DELETE are visible to the reader, and time travel reads the first
  snapshot;
- schema evolution (ADD COLUMN) and small appends are visible;
- `ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files` reduce the
  file count, and the reader stays correct afterwards;
- after `ducklake_expire_snapshots` and `ducklake_cleanup_old_files`, the
  bucket holds exactly the referenced files: nothing leaked, nothing missing;
- dropping the table and cleaning up leaves no objects.

## DuckLake tuning: Parquet row group size

A 20M-row table (`id` sorted, `k` int, `v` double, `s` 32-char string;
~830 MiB in 2 Parquet files) is loaded under each
`CALL lake.set_option('parquet_row_group_size', N)`. Each query runs in a fresh
DuckDB (median of 3), so Parquet metadata is fetched every time.

| Row group rows | Load | Full scan `sum(v), sum(k)` | String filter `s LIKE 'ab%'` | 1% id range | Point lookup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 30,720 | 9.5 s | 0.146 s · 654 GETs | 0.414 s | 0.054 s · 2.4 MiB | 0.044 s · 0.4 MiB |
| **122,880** (default) | 9.8 s | 0.132 s · 165 GETs | 0.375 s | 0.052 s · 3.4 MiB | 0.041 s · 1.2 MiB |
| 491,520 | 9.8 s | 0.132 s · 43 GETs | 0.370 s | 0.064 s · 4.5 MiB | 0.053 s · 4.5 MiB |
| 1,966,080 | 9.6 s | 0.128 s · 13 GETs | 0.684 s | 0.170 s · 17.9 MiB | 0.143 s · 17.9 MiB |

- **Keep the default (122,880 rows).** It is at or near the best for every
  query.
- **Large row groups hurt selective reads.** A point lookup or narrow range
  reads a whole row group, so time grows with row-group size. Very large groups
  also leave too few units to parallelise: at 1.97M rows the string filter
  slows by 1.8× on 16 threads.
- **Small row groups are cheap on pgvs3** because a request costs about 1 ms.
  On S3, 654 GETs at ~30 ms each would dominate the scan, and the usual advice
  to grow row groups for object storage applies.
- **Load time is flat.** Writing is bound by DuckDB's Parquet encoding, not the
  store.
