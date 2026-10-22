# Notes on chunk partitioning

The question is whether `s3p.chunks` should be hash-partitioned by a key other
than `file_id`.

For a point read the query is `file_id = $1 AND no BETWEEN $2 AND $3`. To keep
that read on one partition, the partition key must contain `file_id` and the
planner must be able to prune it. That rules out:

- `no` / `hash(no)`: a file's rows spread, but a point read has to scan every
  partition (hash) or scan all row ranges (range).
- `(file_id, no)` hash: the same problem; hashing the pair destroys `file_id`
  pruning.
- `bucket` / `bucket, key`: needs denormalising bucket and key into every chunk
  row; all files in a bucket share a partition, so it neither spreads a single
  file nor helps a single-lake workload.

The only key that keeps point reads to one leaf and spreads a large file is a
two-level layout:

```sql
PARTITION BY HASH (file_id)       -- 8
  PARTITION BY RANGE (no)         -- 8 row ranges of 1,024 rows
```

Measured on the same host with 8 MiB parallel read pieces:

| Workload | 1 table | 8×8 hash/range |
| --- | ---: | ---: |
| 4 KiB GET, 1 worker | 1,104/s | 868/s |
| 4 KiB GET, 16 workers | 3,852/s | 2,628/s |
| 4 KiB PUT, 16 workers | 1,051/s | 941/s |
| 64 MiB GET, 1 worker | 441 MiB/s | 556 MiB/s |
| 64 MiB GET, 16 workers | 985 MiB/s | 1,002 MiB/s |

The large-read gain does not pay for the small-read loss. All rows of a file
still route through one hash partition, so writes do not gain either; a 64 MiB
COPY is routed across eight leaves. With DuckLake's single writer there is no
insert contention to spread.

Conclusion: keep one unpartitioned table. The useful levers are the read path
(one query up to 64 MiB, parallel pieces for multipart objects) and the
small-object cache, not the partition key.
