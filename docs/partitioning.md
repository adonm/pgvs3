# Why `s3p.chunks` Is Unpartitioned

Every chunk read in pgvs3 is a range scan on one file:

```sql
WHERE file_id = $1 AND no BETWEEN $2 AND $3
```

To keep point reads to a single partition (so PostgreSQL's planner can prune
everything else), the partition key must start with `file_id`:

- **`no` or `hash(no)`:** spreads a file's chunks across partitions, but forces
  every point read to scan all partitions.
- **`hash(file_id, no)`:** breaks `file_id` partition pruning, so reads again
  hit every partition.
- **`bucket`:** requires denormalizing `bucket` onto every chunk row and puts an
  entire lake into one partition anyway.

The only partitioning scheme that both prunes point reads and spreads large
files is a two-level `HASH(file_id)` → `RANGE(no)` layout:

```sql
PARTITION BY HASH (file_id)   -- 8 hash partitions
  PARTITION BY RANGE (no)     -- 8 ranges of 1,024 rows (~8 MiB each)
```

Measured on the 6-vCPU benchmark host against a single unpartitioned table:

| Workload | 1 table (default) | 16 hash | 64 hash | 8×8 hash/range |
| --- | ---: | ---: | ---: | ---: |
| 4 KiB GET, 1 worker | 1,104/s | 888/s | 945/s | 868/s |
| 4 KiB GET, 16 workers | 3,852/s | 2,784/s | 3,227/s | 2,628/s |
| 4 KiB PUT, 16 workers | 1,051/s | 908/s | 981/s | 941/s |
| 64 MiB GET, 1 worker | 441 MiB/s | — | — | 556 MiB/s |
| 64 MiB GET, 16 workers | 985 MiB/s | — | — | 1,002 MiB/s |

## Conclusion

Partitioning adds planner and catalog overhead on every query, hurting small
reads by 15–32% and writes by ~10%, while DuckLake's single-writer model has no
insert contention to spread across tables.

`s3p.chunks` therefore remains a single unpartitioned table (up to PostgreSQL's
32 TiB single-relation limit), while ≤64 KiB objects live inline in
`s3p.objects` and large reads scale via 8 MiB parallel snapshot queries.
