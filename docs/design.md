# Design and shape

This document describes what pgvs3 is, how the pieces fit together, and — just
as importantly — what it deliberately is not. For storage internals and query
paths see [performance.md](performance.md); for deployment see
[operations.md](operations.md).

## One sentence

pgvs3 is a stateless Go gateway that projects one PostgreSQL database as S3,
SQS, plain HTTP and RESP2 on a single port.

## Trust model (permanent)

**pgvs3 is designed for trusted networks. Authentication is not part of the
design and is not a future goal.**

There is no authentication, authorization, request signing, credential
validation, TLS termination, audit log, or per-tenant isolation. These are
non-goals, not gaps on a roadmap. The gateway accepts header-based SigV4
because standard S3 SDKs send it, but it does not verify the signature and does
not use the credentials for anything. Presigned URLs are rejected because a
time-limited, object-scoped link would otherwise become an unscoped grant on
the entire gateway.

The only access control is network placement. Run pgvs3 on loopback, a private
VPC/VLAN, or behind a service mesh that enforces identity, authorization, and
TLS. Never expose `:8014` to an untrusted network.

What this means in practice:

| Capability | Where it is enforced |
| --- | --- |
| Who may connect | Network / service mesh / firewall |
| Who may read or write a namespace | Network / service mesh |
| TLS in transit | Service mesh or private network |
| Audit and request attribution | Service mesh / PostgreSQL logs |
| Bucket drop, queue purge, cache flush | Network / service mesh |

Anyone who can open a TCP connection to `:8014` can read, overwrite, or delete
every namespace, force-drop buckets (`DROP SCHEMA CASCADE`), purge queues, and
flush cache namespaces. Treat the listener as a database port, not as a public
API.

## Shape of the system

```
clients ── S3 / SQS / HTTP / RESP2 ──┐
                                     │  one listener :8014
                              ┌──────▼──────┐
                              │  gateway    │  protocol mux by first bytes
                              │  (stateless)│
                              └──────┬──────┘
                     ┌───────────────┼────────────────┐
                     │               │                │
                  s3/http          sqs             resp2
                     │               │                │
                     └───────────────┼────────────────┘
                                     │
                              ┌──────▼──────┐
                              │ PostgreSQL  │  s3p catalog + one schema
                              │     18+     │  per namespace
                              └─────────────┘
```

- **One binary, one port.** S3, SQS and plain HTTP share the HTTP listener;
  RESP2 is detected from the first byte. There is no sidecar, no local object
  store, and no per-pod cache of object bytes or metadata.
- **One PostgreSQL schema per namespace.** `s3b_<name>` is a durable bucket,
  `s3c_<name>` a volatile cache namespace (UNLOGGED tables), `s3q_<name>` an
  SQS queue. Each owns its tables, indexes, autovacuum workers and 32 TiB chunk
  budget. `DROP SCHEMA CASCADE` is the delete primitive.
- **Immutable files, atomic publish.** A write allocates a new `file_id`,
  streams 8,120-byte chunk rows, and publishes by swapping the object pointer in
  one transaction. Overwrites queue the old `file_id` for the reaper; readers
  use MVCC snapshots, so an in-flight read never observes a torn object.
- **Small objects inline, large objects chunked.** Objects up to 64 KiB live in
  `objects.inline_data` (LZ4 when it shrinks). Larger objects use 8,120-byte
  chunk rows, one per PostgreSQL page, with logical offsets fixed at
  `no × 8,120` for O(1) range seeks.
- **PostgreSQL is the cache.** `shared_buffers` holds the hot metadata and
  bytes. Additional gateway pods see writes immediately; there is no
  invalidation protocol.
- **Maintenance is a leader election.** One gateway holds a PostgreSQL advisory
  lock and runs reconciliation, TTL expiry and bounded garbage collection.
- **Read-only gateways** skip DDL and reaping and reject mutations, for
  PostgreSQL streaming standbys. SQS allows metadata-only reads
  (`ListQueues`, `GetQueueUrl`, `GetQueueAttributes`) but rejects sends and
  leases because even `ReceiveMessage` writes visibility state.
- **The cache tier is optional and asymmetric.** With `--origin-config`, S3
  reads become read-through cache fills and writes write through to a thanos
  objstore origin. The origin remains the source of truth; the PostgreSQL cache
  is an accelerator, not a second durable copy.

## Scope boundaries

Supported behavior is documented in [README.md](../README.md). The following
are explicitly out of scope and return errors or are ignored by design:

| Area | Out of scope |
| --- | --- |
| Trust | authentication, authorization, SigV4 validation, presigned URLs, TLS, audit, per-tenant isolation, rate limiting |
| S3 | IAM/bucket policies, versioning, ACLs, Object Lock, tags, lifecycle, replication, event notifications, SSE-C, SSE-KMS, DSSE-KMS, multipart `FULL_OBJECT` checksums |
| SQS | high-throughput FIFO mode, fair queues |
| RESP | RESP3, `MULTI`/`EXEC`/`WATCH` |
| Layout | incompatible changes without a migration; editing the frozen baseline |

The one versioning affordance is a read-only `ListObjectVersions` shim: it
reports each current object as the `null` version so test harnesses and
cleanup tools can empty a bucket. It does not create versions or delete
markers, and `versionId` on GET/DELETE remains unsupported.

When a feature is not implemented, the preferred behavior is to reject it
explicitly rather than accept and silently ignore it. Presigned URLs follow
that rule. The remaining compatibility gaps and their options are tracked in
[proposals/hardening-options.md](proposals/hardening-options.md).

## Design principles

1. **PostgreSQL is the only state.** No local metadata cache, no local byte
   cache, no invalidation bus. A pod can die at any point.
2. **Bounded work per request and per maintenance pass.** No query or reaper
   pass may grow with total bucket size.
3. **Atomic publish.** A client either sees the old object or the new one,
   never a mixture.
4. **Explicit rejection over silent approximation.** If pgvs3 cannot honor a
   protocol guarantee, it should say so.
5. **The network is the security boundary.** Do not build half an auth system
   into the data path.
