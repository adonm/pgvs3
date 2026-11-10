# Interfaces and scope

pgvs3 has one focus: **10 MiB–1 GiB Parquet objects for DuckDB and DuckLake,
stored in PostgreSQL**. Everything else is labelled by how much weight it carries.

| Scope | Interface | What that means |
| --- | --- | --- |
| **Core** | S3/HTTP object store (`s3b_`), origin-backed shared cache and durable write-back stage (`s3c_`/`s3w_`) | Drives the design, the performance gates and the [speed-of-light](performance.md#speed-of-light) table. Changes here must pass [paired acceptance](benchmarks.md#paired-acceptance). |
| **Companion** | RESP2 cache subset, OJS jobs and streams (`s3q_`) | Supported within documented bounds for co-located services. Not a goal to match Redis or a queue product; new features need a concrete co-located use. |
| **Experimental** | [Differential](differential.md) and [SlateDB/ObjFS](object-storage.md) comparisons | Client-side benchmark harnesses, not gateway features. They can change or be removed without notice. |

Permanent non-goals are listed in [design.md](design.md#scope-boundaries).

## Core: S3 and HTTP

- **Supported S3 operations:** `GET` (byte ranges and conditional headers),
  `HEAD`, `PUT`, `DELETE`, `DeleteObjects`, `ListObjectsV2`, `ListBuckets`,
  `CreateBucket`, `HeadBucket`, `DeleteBucket`, multipart uploads
  (`CreateMultipartUpload`, `UploadPart`, `UploadPartCopy`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListParts`,
  `ListMultipartUploads`), `CopyObject`, `GetObjectAttributes`, and
  `partNumber` on `GET`/`HEAD`.
- **Native PostgreSQL copies:** same-bucket `CopyObject` and whole-object
  `UploadPartCopy` share immutable `file_id`s without copying chunk rows;
  cross-bucket `CopyObject` and whole-bucket clones (`PUT /{dst}` with
  `x-amz-copy-source: /{src}`) copy chunk rows inside PostgreSQL without
  streaming bytes through the gateway.
- **Whole-bucket drop:** `DELETE /{bucket}?force=true` (or
  `pgvs3 drop --force <bucket>`) drops `s3b_<bucket>` with `DROP SCHEMA CASCADE`
  rather than walking every object. Table files are unlinked after commit;
  acquiring the required locks can still take time.
- **Checksums:** verifies MD5, CRC32, CRC32C, CRC64NVME, SHA-1 and SHA-256 on
  upload; stores a single-object checksum for GET/HEAD/`GetObjectAttributes`
  and per-part checksums on multipart uploads.
- **Versioning:** not supported. `ListObjectVersions` is a read-only
  compatibility shim that reports each current object as the `null` version so
  cleanup tools can empty a bucket; it does not create versions or delete markers.
- **Plain HTTP and static sites:** `GET /{bucket}/{key}` works with any HTTP
  client or browser. Trailing slashes resolve to `index.html`, plain
  `GET /{bucket}` serves `/{bucket}/index.html` when present, and `Content-Type`
  falls back to the filename extension.

### ZIP and ZIP64 virtual folders

`GET` and `HEAD` accept `<archive>.zip/<member>` (and case-insensitive
`<archive>.ZIP/...`) when no literal object with that key exists. The first
read of any `.zip` reads the last 256 KiB, parses the ZIP32 or ZIP64 central
directory (EOCD, ZIP64 EOCD locator/record, `0x0001` extra fields, extended
timestamps and bit-3 data descriptors) in one pass, and caches an immutable
member index keyed by the archive bucket, ETag and size. If the directory is
larger than the tail, one additional ranged GET reads it. Each member's exact
data offset is learned on its first read and kept in the cached index; there is
no sidecar file to create or consult.

- **Listing:** `ListObjectsV2` treats members as virtual keys. `prefix`,
  `delimiter=/` (nested folders become `CommonPrefixes`), `max-keys`,
  `start-after` and continuation tokens all work, so `aws s3 ls
  s3://bucket/archive.zip/` browses the archive. Delimiter pagination
  binary-searches past a common prefix already emitted instead of scanning its
  members. Literal objects shadow members with the same key.
- **Member reads:** `HEAD`, whole `GET`, byte ranges, `If-Match`/`If-None-Match`
  and `Accept-Ranges` work. The member ETag is
  `<archive-etag>-<crc32>`; `Content-Type` comes from the member name
  (`.parquet` → `application/vnd.apache.parquet`, `.tsv`/`.sf` →
  `text/tab-separated-values`, `.csv`, `.json`, …).
- **Compression:** `Store` (method 0) cuts through with one exact range
  request; `Deflate` (method 8) and `Zstd` (method 93) stream through a pooled
  `klauspost/compress` inflater or the pooled Zstd decoder. Compressed members
  up to 16 MiB are decompressed once, CRC-checked and kept in a bounded 64 MiB
  cache, so later whole or ranged reads copy from memory. Zstd is recommended
  for new archives: it decompresses about 4–6× faster than Deflate at similar
  ratios, and pgvs3 decompresses it natively.
- **Limits:** the index cache holds 64 archives / 64,000 members; archives
  whose central directory exceeds 64 MiB fall back to normal object semantics.
  Encrypted, spanned/multi-disk and self-extracting archives are not supported,
  and writes always create literal S3 keys rather than modifying an archive.

## Core: origin-backed cache and write-back

Read-through and write-through/write-back `GET`/`HEAD`/`PUT`/`CopyObject`/
multipart uploads/`DELETE`/`DeleteObjects`/`ListObjectsV2` in front of an origin
(`X-Cache: HIT|MISS`), with TTL revalidation, idle eviction, optional range
caching, and an origin listing merged with pending write-back objects. **A
pending acknowledged body may exist only in PostgreSQL**; cache TTL never expires
it. See [origin modes and limits](operations.md#cache-modes) and the
[authority contracts](design.md#authority-and-acknowledgement-contracts).

For production origins use S3-compatible storage (reads and writes) or HTTP(S)
(read-only by default). `filesystem` and temporary-directory `ephemeral` are
local/test providers, not production durability backends. For a read-only HTTP
data host, save this as `origin.json`:

```json
{
  "type": "http",
  "config": {
    "base_url": "https://data.source.coop",
    "strip_bucket": true,
    "read_only": true,
    "headers": { "User-Agent": "pgvs3" }
  }
}
```

Attach it to a bucket; every gateway discovers the setting from PostgreSQL:

```sh
pgvs3 origin --url "$PGVS3_URL" --bucket edge --cache-namespace origin \
  --range-cache-chunk 1048576 set origin.json
```

This creates the bucket catalog row and enables aligned range caching. HTTP
origins do not support `ListObjectsV2`; use an S3 origin when listing is required.
Provider key/metadata limits are in [operations.md](operations.md#limits-and-failure-behavior).

## Companion: RESP2 and OJS

These share the `:8014` listener and the PostgreSQL engine but are optional for
the object path. RESP cache namespaces and OJS queues are created on first use.
The connection mux detects RESP2; HTTP routes distinguish S3 from OJS.

```sh
# Redis-compatible cache subset (RESP2)
redis-cli -p 8014 SET greeting "hello from resp" EX 60
curl -fsS http://127.0.0.1:8014/_cache/cache/greeting
redis-cli -p 8014 GET s3://lake/README.md     # explicit cross-namespace read

# OJS (OpenJobSpec v1 core lifecycle)
curl -fsS -X POST http://127.0.0.1:8014/ojs/v1/jobs \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"type":"email.send","args":["user@example.com"],"options":{"queue":"jobs"}}'
curl -fsS -X POST http://127.0.0.1:8014/ojs/v1/workers/fetch \
  -H 'Content-Type: application/openjobspec+json' \
  -d '{"queues":["jobs"],"count":1,"worker_id":"worker-1","visibility_timeout_ms":30000}'
```

- **OJS v1 core:** `POST /ojs/v1/jobs` and `/ojs/v1/jobs/batch`,
  `GET`/`DELETE /ojs/v1/jobs/{id}`, `POST /ojs/v1/workers/{fetch,ack,nack,heartbeat}`,
  `GET /ojs/v1/queues`, `GET /ojs/v1/queues/{name}/stats`, `GET /ojs/v1/health`
  and `GET /ojs/manifest`. Jobs use the OJS states (`scheduled`, `available`,
  `active`, `retryable`, `completed`, `cancelled`, `discarded`), higher `priority`
  first, `FOR UPDATE SKIP LOCKED` leases, scheduled delivery, exponential retry
  backoff and a short long poll. `lease_token` is a documented extension required
  on `ack`/`nack`/`heartbeat` so a superseded worker gets `409` instead of
  completing another lease. Fetch accepts at most 64 queues and resolves them
  before one claim transaction, avoiding per-queue savepoints so `SKIP LOCKED`
  stays on the plain row-lock path. Expired leases, due scheduled jobs and
  retries are claimed lazily; terminal jobs are kept for `--job-retention`
  (default `24h`). Workflows, cron, SSE/WebSocket events, schema registry,
  job-level dead-letter APIs, pause/resume and full OJS conformance are out of scope.
- **Streams:** every `s3q_<name>` namespace also exposes a PgQue-shaped
  append-only stream under `/ojs/v1/queues/{name}/...`: batch append,
  independent fan-out subscriptions, cooperative members, snapshot-delimited
  batches, batch ack, per-message nack with retry, and a dead-letter queue.
  Events rotate through three `TRUNCATE` partitions under consumer-pinned
  retention. See [queue-stream.md](queue-stream.md).
- **RESP2 commands:** `GET`, `SET` (`EX`/`PX`/`EXAT`/`PXAT`/`NX`/`XX`/`KEEPTTL`/`GET`),
  `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `MGET`, `MSET`, `DEL`, `UNLINK`,
  `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`,
  `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `GETRANGE`, `APPEND`, `SETRANGE`,
  `SCAN`, `TYPE`, `SELECT`, `FLUSHDB`, `DBSIZE`, `KEYS`, `INFO` and `HELLO 2`.
  `EXPIRE` honours `NX`/`XX`/`GT`/`LT`. RESP3 is not supported (`HELLO 3`
  returns `NOPROTO`); clients that default to RESP3, such as recent `redis-py`,
  need `protocol=2`. Pipelined `GET`/`SET` bursts coalesce into `Cache.MGet`/
  `Cache.MSet`. `MULTI`/`EXEC`/`WATCH` are rejected (pass `transaction=False` in
  `redis-py`) because pgvs3 has no cross-command snapshot.

## Experimental: client-side storage comparisons

- `just differential` compares pinned
  [DuckDB Differential](https://github.com/tobilg/duckdb-differential)
  snapshot/synchronous-WAL profiles against DuckLake. Differential keeps a
  client-local repository and needs an externally fenced single writer. See the
  [method](differential.md) and [results](differential-results.md).
- `just object-storage` compares the pinned `duckdb_object_storage` 0.2.0
  (SlateDB/ObjFS) extension against DuckLake. SQL read-only ObjFS clients still
  write retention checkpoints, so they need a writable endpoint. See the
  [method](object-storage.md) and [results](object-storage-results.md).
