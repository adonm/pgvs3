# Hardening and compatibility options

Status: **decisions recorded 2026-10-23; implementation in progress**. The
trust-model wording, presigned-URL rejection, decoder hardening, SSE/ACL
policy, per-key deletes, pending/outbox multipart write-through, origin ETags, FIFO queues,
the listing index, read-only SQS metadata, and the database budget guard are
implemented. Verification and remaining polish are tracked below.

## Decisions

| Section | Decision |
| --- | --- |
| 1. aws-chunked | **A+D** — strict 8 MiB cap, 128-byte header, reject unknown streaming algorithms, enforce decoded length when present, rolling buffer with no declared-size allocation |
| 2. cache listing | **B** — PostgreSQL listing index |
| 3. write-through | **C+outbox** — pending sentinel plus `s3p.pending_uploads` |
| 4. DeleteObjects | **A** — HTTP 200 with per-key `Deleted`/`Error`, `Quiet` honored |
| 5. origin ETag/Content-Type | **B** — origin ETag plus real Content-Type |
| 6. SQS FIFO | **B** — full FIFO (dedup, per-group ordering, sequence numbers; high-throughput deferred) |
| 7. SSE/Object Lock/ACL/tags | **B** — compatibility tier |
| 8. read-only SQS | **B** — metadata-only operations allowed |
| 9. quotas | **A+D** — best-effort gauge plus write-admission disk guard |
| 10. verification | **B+C+D** — integration, fuzzers, SDK matrix |

Each section states the problem, what the relevant AWS/objstore behavior is,
several implementation options with trade-offs, and a recommendation. Pick one
option per section (or defer) before code changes begin.

Priority is about risk to data or availability, not effort:

| Priority | Meaning |
| --- | --- |
| P0 | remote crash / unbounded work / silent data-visible divergence |
| P1 | protocol lie that a normal client can rely on |
| P2 | scalability or operator ergonomics |

---

## 1. aws-chunked decoder is attacker-controlled (P0)

### Problem

`internal/s3/chunked.go` trusts the declared chunk size:

```go
data := make([]byte, size)   // line 86
```

`size` is parsed from an attacker-supplied hex line. A `PUT` with
`x-amz-content-sha256: STREAMING-...` and a line such as `ffffffffff` asks Go
to allocate ~1 TiB. A failed large allocation is a fatal runtime OOM, not a
recoverable panic, so one small request can kill the gateway. Separately, a
body made of blank lines recurses once per line (`return c.Read(p)`, line 58)
and grows the goroutine stack without bound.

There is also no total-size or chunk-count limit, and no consistency check
against `x-amz-decoded-content-length`.

### Research

- The wire format is
  `<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n`, terminated by a zero-size
  chunk and optional trailers
  ([AWS chunked upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/sigv4-streaming.html),
  [trailers](https://docs.aws.amazon.com/AmazonS3/latest/dev/sigv4-streaming-trailers.html)).
- AWS requires chunks of at least 8 KiB (except the last) and recommends at
  least 64 KiB. The official Rust SDK defaults to 64 KiB
  ([aws-runtime](https://docs.rs/aws-runtime/latest/src/aws_runtime/content_encoding.rs.html)).
- Other gateways cap the declared size: Supabase's SigV4 stream parser defaults
  `maxChunkSize` to 8 MB and `maxHeaderLength` to 128 bytes
  ([source](https://github.com/supabase/storage/blob/master/src/storage/protocols/s3/signature-v4-stream.ts)).
- The values pgvs3 must accept are `STREAMING-UNSIGNED-PAYLOAD-TRAILER`,
  `STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`, and
  `STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD[-TRAILER]`
  ([AWS auth overview](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-auth-using-authorization-header.html)).

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Strict AWS-shaped limits | Header line ≤ 128 B; chunk size ≤ 8 MiB; reject unknown `STREAMING-*` values; enforce `x-amz-decoded-content-length` when present; iterative parser | Safest. A client using > 8 MiB chunks fails (rare; SDKs use 64 KiB). |
| **B** | Compatibility-first | Cap at 64 MiB, allow any `STREAMING-*`, no decoded-length check, iterative parser | Preserves unusual clients; a single 64 MiB allocation per connection is still a memory-amplification vector. |
| **C** | Configurable | `--max-chunk-size` (default 8 MiB), `--max-upload-bytes` (default 0 = unlimited), strict parsing otherwise | Flexible; two more knobs and a way to misconfigure production. |
| **D** | Stream without buffering | Keep a rolling buffer, never allocate the declared size | Removes the allocation bug entirely, but the decoder must handle a chunk spanning many `Read` calls and is more code. |

### Recommendation

**A + D.** Parse iteratively with a fixed 128-byte header buffer and a bounded
chunk buffer; never allocate from the declared size. Enforce the decoded length
when the header is present. Reject unknown streaming algorithms with
`400 InvalidRequest` instead of attempting to parse them.

Add `--max-upload-bytes` as a separate, optional disk-protection control (not a
substitute for the chunk cap).

### Open questions

- Is any known client (rclone, restic, DuckDB, SlateDB) using chunks above
  8 MiB? If yes, use 64 MiB.
- Should a missing `x-amz-decoded-content-length` be rejected for signed
  streaming (AWS requires it) but allowed for unsigned trailer streaming?

---

## 2. Cache-tier `ListObjectsV2` is O(N) per page and issues a HEAD per key (P0/P1)

### Problem

`Tier.List` (`internal/objcache/objcache.go:535`) recursively walks the entire
origin prefix, sorts everything, then truncates to `limit`. `listObjectsV2`
calls it once per page, so listing a bucket is O(N²). For each key not fresh in
the PostgreSQL cache it then calls `origin.Attributes`, i.e. one origin HEAD
per key — up to 1,000 HEADs for a single page.

### Research

- S3 `ListObjectsV2` is already paginated: up to 1,000 keys per request, with
  `ContinuationToken` / `NextContinuationToken`, `KeyCount`, `MaxKeys`, and
  `StartAfter`
  ([AWS API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)).
- thanos objstore exposes `IterWithAttributes` and `WithUpdatedAt`, and the S3
  provider supports both, but `IterObjectAttributes` only carries `Name` and
  `LastModified`; it does not expose `Size` or `ETag`
  ([objstore.go](https://github.com/thanos-io/objstore/blob/main/objstore.go),
  [s3 provider](https://github.com/thanos-io/objstore/blob/main/providers/s3/s3.go)).
  So `IterWithAttributes` removes the mtime HEAD but not the size/ETag HEAD.
- `minio-go/v7` is already an indirect dependency (`go.mod:48`), so a native
  S3 lister does not add a new module.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | `IterWithAttributes` only | Avoid HEAD for `LastModified`; still one HEAD per key for size/ETag | Small change, does not fix the actual cost. Not sufficient alone. |
| **B** | PostgreSQL listing index | `s3c_<ns>.listing(prefix, key, size, etag, mtime, updated_at)`; list with `LIMIT`; fill on read/write; TTL refresh | O(page) when warm, O(N) on cold start; new table + DDL + invalidation semantics; stale within TTL. |
| **C** | Provider-aware lister | New `Lister` interface: S3 uses `minio-go` `ListObjectsV2`; filesystem uses `WalkDir` + `Stat`; inmem uses its map | O(page) always, exact metadata, no HEADs; provider-specific code and config duplication; bypasses objstore for list only. |
| **D** | Bound the origin listing | Require a prefix or cap at N keys, return `501`/`InvalidArgument` beyond | Simple and honest; breaks large-bucket listing. |
| **E** | Hybrid | C for `s3`; current `Iter` for filesystem/inmem (local `stat` is cheap) | Best practical behavior; two listing paths to test. |

### Recommendation

**E.** Keep objstore for `GET`/`PUT`/`DELETE`, add a narrow `Lister` for S3
origins, and keep the current path for local providers with a hard key cap and
context timeout. If a native lister is too much, **B** is the fallback, but it
only helps after the first listing and adds cache-invalidation work.

### Open questions

- Should the continuation token stay the raw last key (current) or become an
  opaque base64 `{prefix, lastKey}`? S3 says tokens are obfuscated; clients
  must treat them as opaque either way.
- Does the cache tier need `delimiter` support from the origin, or is
  client-side roll-up after a bounded page acceptable?
- Is listing an origin bucket a supported use case, or should it be documented
  as best-effort with a key cap?

---

## 3. Cache-tier write-through is not atomic and has a phantom window (P1)

### Problem

The two write paths order their effects differently:

- `Tier.Upload` writes the origin first, then PostgreSQL.
- Multipart completion must assemble part rows into one object. Publishing the
  cache first exposes bytes the origin does not have; uploading first holds a
  PostgreSQL transaction and connection open across the origin network call.

The implemented design uses a pending sentinel (`origin_etag =
'pending:<upload-id>'`) plus an `s3p.pending_uploads` outbox row, published in
the same transaction. Readers treat the sentinel as a miss, so pending bytes are
never served. `CompleteMultipartUpload` then spools the pending object, releases
the PostgreSQL connection, uploads it to the origin, and atomically swaps the
sentinel for the origin version while deleting the outbox row. If the first
flush fails, the request returns an error but the outbox row remains for a
background worker to retry with bounded exponential backoff. Token guards and
per-key locks prevent an old flush from overwriting a newer PUT, DELETE, or
multipart completion.

`Tier.Copy` goes through `Upload`, so it inherits the origin-first order.
`DeleteMany` uses per-key results (section 4) and evicts each cache key and
outbox row immediately after its origin delete succeeds.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Origin-first everywhere | PUT stays as-is; multipart completion streams the assembled object from PostgreSQL to the origin, then publishes the cache | Origin is authoritative; no phantom; one extra PostgreSQL read per completed multipart; client waits for the origin upload either way. |
| **B** | Pending state column | Add `state` (`pending`/`committed`) to cache objects; readers treat `pending` as a miss; mark committed after the origin upload | No double read; crash leaves pending rows for GC; schema + read-path changes. |
| **C** | Sentinel + outbox | Publish with `origin_etag = 'pending:<uuid>'` plus an `s3p.pending_uploads` row; readers miss on the sentinel; replace with the real version after the origin upload; retry failures in the background | No per-object schema change; small read-path checks; one additive table; crash recovery and retries. |
| **D** | Keep current + delete on failure | Smallest change; the phantom window and partial-batch behavior remain | Does not meet the "explicit rejection over silent approximation" principle. |

### Recommendation

**C + outbox** (implemented). A successful `CompleteMultipartUpload` still
waits for the first origin upload, but the PostgreSQL transaction and connection
are released before the network call; crash recovery and retries are durable in
`s3p.pending_uploads`. Make cache population best-effort on PUT: if the origin
succeeds and the cache write fails, return success and log; the next read is a
miss.

`DeleteMany` uses per-key results (section 4) and evicts each cache key and
outbox row immediately after its origin delete succeeds.

### Open questions

- Is the cache tier expected to survive an origin outage for reads (current TTL
  behavior), or is the origin always authoritative?
- Should a failed cache write fail the client request when the origin succeeded?
  (Recommendation: no, except for read-after-write within the TTL.)

---

## 4. `DeleteObjects` does not return per-key results (P1)

### Problem

`deleteObjects` (`internal/s3/server.go:580`) returns one `500` if any delete
fails, and otherwise marks every key deleted. S3 returns `200 OK` with a
`<Deleted>` or `<Error>` element per key, and `Quiet` mode returns only errors.
For an origin bucket, a mid-batch failure can leave earlier keys deleted while
the client sees a single failure.

### Research

- AWS `DeleteObjects` returns per-key `Deleted`/`Error` entries in a 200
  response; `Quiet` suppresses successful entries
  ([AWS API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)).
- General-purpose buckets require `Content-MD5` for multi-object delete;
  directory buckets also accept a checksum header.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Per-key results | PG: one transaction, but report each key; origin: sequential deletes, collect per-key errors; always 200 unless the request itself is invalid | Matches S3; PG batch remains atomic even if individual keys "fail" only for not-found (which is success). |
| **B** | All-or-nothing | Current behavior; 500 on any failure | Not S3-compatible; cannot be made atomic for origin deletes. |
| **C** | Hybrid with savepoints | PG: savepoint per key, so a key-level failure does not roll back the batch; origin: per-key | Closest to S3; more SQL and more round trips. |

### Recommendation

**A** with **C** only if PG key-level failures can actually occur (they
currently cannot for a normal delete; missing keys are success). Add the
`<Error>` XML type and honor `Quiet`. Decide separately whether to require
`Content-MD5` (strict AWS behavior) or keep it optional for client
compatibility.

### Open questions

- Should a malformed key in a 1,000-key batch fail the whole request or produce
  one `<Error>` entry? AWS fails the request for malformed XML/keys.
- Do we need `VersionId` fields in the response for wire compatibility even
  though versioning is out of scope? (Probably omit; clients tolerate absence.)

---

## 5. Origin ETags and Content-Type are synthetic (P2)

### Problem

`etagFor(version)` returns `md5("<size>-<mtime-ns>")`, and `Head` derives
`Content-Type` from the file extension (`objcache.go:624`). Origin ETags and
stored content types are not authoritative. Conditional requests and client
caches therefore cannot use the origin's version identity.

### Research

- S3 `ListObjectsV2` returns `ETag`, `Size`, `LastModified`, and
  `StorageClass`; `HeadObject` returns `Content-Type`
  ([ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)).
- objstore's `ObjectAttributes` exposes `Size`, `LastModified`, and `Version`
  (ETag/generation), but not `ContentType`
  ([objstore.go](https://github.com/thanos-io/objstore/blob/main/objstore.go)).
  Content-Type requires a provider-specific `StatObject`/`Stat`.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Keep synthetic | Cache ETags are cache-local; document it | No work; clients cannot correlate with origin. |
| **B** | Return origin ETag | Use the origin's ETag for origin objects and store it in `origin_etag`; cache-filled objects keep the content MD5 or adopt the origin ETag | Better compatibility; one-time client cache invalidation; needs provider metadata. |
| **C** | Dual headers | `ETag` = origin ETag, `X-Pgvs3-Content-MD5` = local content hash | Preserves both identities; non-standard header. |
| **D** | Extend objstore upstream | Add `ContentType`/`ETag` to `ObjectAttributes` and implement in providers | Best long-term; dependency/PR cycle. |

### Recommendation

**B** for ETag, plus a provider-specific stat for Content-Type on S3 origins;
**C** if local content MD5 must remain observable. **D** is the clean upstream
fix if the project is willing to carry a fork/PR.

### Open questions

- Is an ETag change acceptable in the current alpha, or should it be gated
  behind a flag for one release?
- Should write-through uploads return the origin ETag or the pgvs3 content
  MD5? (Origin ETag is more useful for read-after-write.)

---

## 6. SQS FIFO is accepted but not implemented (P1)

### Problem

`CreateQueue` accepts `FifoQueue=true` and `.fifo` names, and `SendMessage`
accepts `MessageGroupId` / `MessageDeduplicationId`, but `SendMessages` ignores
`GroupID`/`DedupID`. A client that asks for FIFO gets a standard queue with no
ordering or deduplication guarantee. That is a silent correctness downgrade.

### Research

- FIFO queue names must end in `.fifo`; `MessageGroupId` is required;
  deduplication uses `MessageDeduplicationId` or content hashing over a
  5-minute interval; messages in a group are delivered in order; per-message
  `DelaySeconds` is not supported
  ([AWS FIFO identifiers](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queue-message-identifiers.html),
  [CreateQueue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html),
  [SendMessage](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html)).
- Throughput is 300 API calls/s per method without batching, up to 3,000
  messages/s with batches; high-throughput mode raises this further
  ([queue types](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-types.html)).

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Reject FIFO | `.fifo` names or `FifoQueue=true` → `400 InvalidParameterValue`; `MessageGroupId` on standard queues is accepted (AWS fair queues) | Honest; small change; no FIFO users. |
| **B** | Full FIFO | Dedup table with a 5-minute window, per-group sequence/order, `SequenceNumber`, visibility, batch dedup; no high-throughput mode initially | Correct; new schema, ordering query, more tests. |
| **C** | Minimal FIFO | Dedup + per-group ordering, reject unsupported attributes (`ContentBasedDeduplication` optional), no sequence numbers | Smaller than B; still a schema and receive-path change. |
| **D** | Status quo | Accept and treat as standard | Dangerous; violates the explicit-rejection principle. |

### Recommendation

**A now.** Add FIFO only if a real consumer needs it; then **B**, not **C**,
because partial FIFO is another silent approximation. Regardless of choice,
validate `FifoQueue` and `.fifo` consistency at `CreateQueue`.

### Open questions

- Is any target workload (DuckLake, SlateDB, restic) using FIFO queues?
- Should `MessageGroupId` on a standard queue be stored and returned as an
  attribute for fair-queue compatibility, or ignored?

---

## 7. SSE, Object Lock, ACL, and tagging headers (P1)

### Problem

- `x-amz-server-side-encryption: AES256` and `aws:kms` are accepted
  (`internal/s3/sse.go:13`) but no encryption is performed.
- Object Lock, ACL, and tagging headers are ignored; `x-amz-tagging` is not
  persisted.
- `x-amz-storage-class` is ignored and listings always report `STANDARD`.

A client that sets Object Lock or SSE-KMS reasonably believes a guarantee
exists. Object Lock is the dangerous one: it is a WORM/retention promise.

### Research

- S3 stores the SSE algorithm and returns it on `PUT`/`GET`/`HEAD`; since
  January 5, 2023, new objects are encrypted by default with SSE-S3
  ([AWS SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html)).
- Object Lock provides retention/legal hold and requires versioning; ACLs and
  tagging are separate authorization/metadata features. All are documented as
  out of scope for pgvs3.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Strict reject | Any SSE header → `501 NotImplemented`; Object Lock → `501`; ACL other than `private` → `501`; tagging → `501`; storage class other than `STANDARD` → `501` | Honest; may break clients that send defaults. |
| **B** | Compatibility tier | Accept `AES256` and `x-amz-acl: private` as documented no-ops (at-rest encryption delegated to PostgreSQL); reject `aws:kms`, public ACLs, Object Lock, tagging, non-STANDARD classes | Keeps common clients working; still no encryption guarantee for AES256. |
| **C** | Store and echo | Persist SSE/ACL/tagging metadata and echo it, without enforcement | Better wire fidelity; misleading for Object Lock and security-sensitive headers. |
| **D** | Full support | Implement encryption (pgcrypto/KMS), Object Lock enforcement, ACL model | Large; ACL/authorization contradicts the trust model. |

### Recommendation

**B** for SSE/ACL, with an explicit log line and docs stating that `AES256` is
satisfied only if the PostgreSQL volume is encrypted. **A** for Object Lock,
tagging, and non-STANDARD storage classes. Never accept `aws:kms` as a no-op.
If the project wants zero ambiguity, choose **A** across the board.

### Open questions

- Do rclone/restic/boto3 send `x-amz-acl: private` or `AES256` by default in
  the supported test matrix? (If yes, A breaks them.)
- Is `x-amz-tagging` used by any target workload? If not, reject it.

---

## 8. Read-only gateways and SQS (P2)

### Problem

`--read-only` rejects every SQS request (`internal/sqs/sqs.go:97`), including
`ListQueues`, `GetQueueUrl`, and `GetQueueAttributes`. The docs say read-only
gateways serve local-AZ reads; queue consumers cannot even inspect queues.

`ReceiveMessage` is genuinely a write: it sets `visible_at`, increments
`receive_count`, and stores a receipt. On a PostgreSQL hot standby,
`SELECT ... FOR UPDATE` is rejected anyway, so a read-only SQS consumer cannot
work without changing semantics.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Keep blanket 403 | Current behavior; document that SQS is primary-only | Simple; metadata inspection unavailable. |
| **B** | Allow metadata-only ops | `ListQueues`, `GetQueueUrl`, `GetQueueAttributes` work; `Send`/`Receive`/`Delete`/`Purge` return `403` | Useful introspection; per-op read-only checks; works on standbys. |
| **C** | Peek mode | `ReceiveMessage` returns messages without leasing | Not SQS semantics; two consumers could process the same message; not recommended. |

### Recommendation

**B**, with a clear `403 AccessDenied` message for `ReceiveMessage` explaining
that leasing requires a writable primary. Document that SQS consumers need a
primary gateway.

### Open questions

- Does `GetQueueAttributes` need `ApproximateNumberOfMessages` on a standby
  (a `SELECT` over `messages`), or are static attributes enough?
- Should `ListQueues` be exposed when the gateway is read-only but the database
  is a primary? (It is a read, so yes.)

---

## 9. Quota enforcement is per-pod and best-effort (P2)

### Problem

`checkQuota` (`internal/store/namespace.go:412`) keeps an in-process gauge
refreshed at most once per second from `pg_total_relation_size`. Each pod has
its own gauge, and only the maintenance leader calls `refreshQuotas`. With
multiple pods, a namespace can overshoot its quota by roughly one second of
writes per pod, plus concurrent writers.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Document best-effort | Keep current; call it a soft limit | No work; quotas are not a billing boundary. |
| **B** | Shared counter | `namespace_usage(kind,name,used,quota)` with `UPDATE ... WHERE used + $delta <= quota`; rollback on failure | Near-hard across pods; one hot row per namespace; must reconcile with actual relation size. |
| **C** | Advisory lock + recalc | Lock the namespace, compute `pg_total_relation_size`, then write | Exact; serializes writes per namespace and adds a size query. |
| **D** | PostgreSQL disk guard | Reject writes when free space is below a threshold, independent of quota | Protects the database, not tenant fairness; useful as a complement. |

### Recommendation

**A** for now, **B** if quotas become a contract. Add **D** as an operational
safety net regardless (e.g. `--min-free-bytes` checked on write admission).

### Open questions

- Is quota a hard tenant boundary or just a guardrail?
- Should `refreshQuotas` run on every gateway instead of only the leader?

---

## 10. Verification for the above (P1)

### Problem

The repository has ~12k lines of Go and 22 unit tests; `internal/s3/server.go`
(~1k lines), the cache tier, and the SQS receive/redrive path have little unit
coverage. The shell `contract`/`fault`/`interop` suites are strong but do not
exercise malformed aws-chunked bodies, origin listing at scale, partial
`DeleteObjects`, FIFO rejection, or presigned rejection.

### Options

| | Approach | Behavior | Cost / risk |
| --- | --- | --- | --- |
| **A** | Store interface + fake | Handler-level `httptest` tests for S3/SQS with a fake store | Fast, broad; requires extracting an interface from `*store.Store` (large refactor). |
| **B** | Integration cases | Add cases to `test/e2e.sh` and `test/fault.sh` for each chosen behavior | Exercises the real SQL; slower; still needs Docker/PostgreSQL. |
| **C** | Fuzzers | Fuzz the chunked decoder, presign detection, XML parsing, and range parsing | Finds crashes; no semantic coverage. |
| **D** | SDK matrix | Run boto3, aws-sdk-go-v2, rclone, restic, DuckDB against a gateway in CI | Catches client-specific breakage; CI time and credentials. |

### Recommendation

**B + C now**, **D** for any section that changes wire behavior (presign, SSE,
FIFO, DeleteObjects). Defer **A** until the handler surface stabilizes.

---

## Review checklist

For each section, record one of: `A`, `B`, `C`, `D`, `defer`, or `reject`.

| Section | Decision | Notes |
| --- | --- | --- |
| 1. aws-chunked | A+D | strict 8 MiB cap, rolling buffer |
| 2. cache listing | B | PostgreSQL listing index |
| 3. write-through | C+outbox | pending sentinel + retry outbox |
| 4. DeleteObjects | A | per-key results |
| 5. origin ETag/Content-Type | B | origin ETag + real Content-Type |
| 6. SQS FIFO | B | full FIFO |
| 7. SSE/Object Lock/ACL/tags | B | compatibility tier |
| 8. read-only SQS | B | metadata-only |
| 9. quotas | A+D | best-effort + `--max-db-bytes` guard |
| 10. verification | B+C+D | integration + fuzz + SDK matrix |

Schema changes are additive and keep layout v11: `s3p.origin_listing`,
`s3p.origin_listing_state`, `s3p.pending_uploads`, and queue
`group_id`/`dedup_id` columns plus the per-queue `dedup` table are created with
idempotent DDL or goose migrations on startup. The disk
guard is implemented as `--max-db-bytes` because PostgreSQL exposes no portable
free-space SQL function to a remote client.

## References

- [S3 presigned URLs (SigV4 query)](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/sigv4-query-string-auth.html)
- [S3 chunked upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/sigv4-streaming.html)
- [S3 chunked trailers](https://docs.aws.amazon.com/AmazonS3/latest/dev/sigv4-streaming-trailers.html)
- [S3 DeleteObjects](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html)
- [S3 ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html)
- [S3 server-side encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html)
- [SQS FIFO identifiers](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queue-message-identifiers.html)
- [SQS CreateQueue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html)
- [SQS queue types](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-types.html)
- [thanos objstore BucketReader](https://github.com/thanos-io/objstore/blob/main/objstore.go)
- [thanos objstore S3 provider](https://github.com/thanos-io/objstore/blob/main/providers/s3/s3.go)
- [Supabase SigV4 stream parser limits](https://github.com/supabase/storage/blob/master/src/storage/protocols/s3/signature-v4-stream.ts)
- [aws-runtime 64 KiB chunk default](https://docs.rs/aws-runtime/latest/src/aws_runtime/content_encoding.rs.html)
