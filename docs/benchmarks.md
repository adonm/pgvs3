# Benchmarks

pgvs3 is measured with `just bench`, which starts a disposable PostgreSQL 18 in
Docker and one or two local gateways, then runs DuckDB 2.0. The benchmark
writes `.tmp/pgvs3/bench.json`.

## What is measured

- **Small GETs.** DucksDB reads many small Parquet footers and row groups; the
  gateway's advantage is the absence of an HTTP object-store hop and
  revalidation.
- **Ranged GETs.** 8 KiB, 115 KiB and 8 MiB ranges; PostgreSQL execution is
  reported separately from queueing.
- **PUTs and multipart.** Small PUTs, 8 MiB PUTs, 5 MiB parts, complete, abort.
- **DuckLake.** Write with one gateway, read with the other; row-group and file
  size variants.
- **Lists.** Prefix listings, pagination, and concurrent globs.

## Historical Rust results

The published numbers on the old README were measured with the Rust gateway
(DuckDB 1.5.6, PostgreSQL 18, one 16-core host, loopback):

| | pgvs3 (Rust, local) | Amazon S3 (published) |
| --- | ---: | ---: |
| Small GET, per request p50 | <1 ms (4 KiB; 1.0 ms per HEAD+GET in DuckDB) | ~30 ms ([AnyBlob][anyblob]); AWS: 100–200 ms ([AWS][aws-perf]) |
| 8 MiB GET, one stream | 14 ms · 548 MiB/s | ~190 ms · 55–60 MiB/s median |
| Aggregate GET bandwidth | 2.0 GiB/s at 16 in flight | ~9–11 GiB/s per 100 Gbit/s instance at 200–250 in flight |
| Small GETs per second | ~7,000 (64 in flight, one gateway) | ≥5,500 per prefix |
| Small PUTs per second | ~4,400 (64 in flight) | ≥3,500 per prefix |

These are kept as a baseline, not as Go results.

## Go smoke numbers

A local loopback smoke test on a shared 6-core VM, one gateway, warm
PostgreSQL 18, is a sanity check only:

| Operation | Result |
| --- | ---: |
| 20 MiB PUT (single stream) | 0.35 s · about 60 MiB/s |
| 20 MiB GET (warm) | 0.057 s · about 350 MiB/s |
| `duckdb_object_storage`: 5M-row database write + checkpoint + read | 2.5 s |

Absolute numbers move with CPU contention; run `just bench` on the target
hardware.

## Method

```sh
just bench             # S3, DuckLake compatibility and tuning
just bench s3          # S3 microbenchmark only
just bench compat      # DuckLake writer/reader through two gateways
just bench tuning      # Parquet row-group and target-file-size variants
```

The stack creates databases `pgvs3` and `ducklake_catalog`, two gateways with
the peer metadata cache, and buckets `pgvs3-contract`, `bench` and `lake`.
Every run is disposable. `just contract` is the correctness gate; run it before
trusting a benchmark.

## Sources

[anyblob]: https://www.vldb.org/pvldb/vol16/p2769-durner.pdf
[aws-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
[pgvfs-performance]: https://pgvfs.adonm.dev/performance.html
