# pgvs3 — S3 on PostgreSQL

pgvs3 is a small S3-compatible gateway that stores objects in PostgreSQL. It is
built to be DuckLake's object store: DuckLake keeps its catalog in PostgreSQL,
and with pgvs3 the data files live there too, so every DuckDB that attaches
the catalog sees the same tables and the same data.

- One Rust binary with no local state: buckets, objects and in-progress
  multipart uploads live in PostgreSQL, so gateways share one S3 namespace.
- An S3 subset: `GET` (with ranges), `HEAD`, `PUT`, `DELETE`, `ListObjectsV2`,
  `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket` and multipart
  uploads. Anything else returns `NotImplemented`.
- No data cache: clients such as DuckDB cache what they read. A per-process
  metadata cache only removes the lookup round trip per GET.

The full-scale performance figures below were measured before the
consistent-snapshot change for multi-query GETs. A
[quick current-path check](docs/benchmarks.md#quick-current-path-check-2026-09-26)
covers GETs on local PostgreSQL, but does not revalidate the AWS or analytics
figures.

> Postgres is all you need. ;P — for durable bytes and metadata here; DuckDB
> and Quickwit still do the actual analytics and search.

## Results at a glance

| Workload | Measured result | Status |
| --- | --- | --- |
| 8 / 16 MiB GET, one client | 1,292 / 1,306 MiB/s | Current snapshot read path, local Docker; not an Aurora result |
| 8 / 16 / 64 MiB GET, one client | 454 / 454 / 449 MiB/s | Current snapshot read path, quick EC2/Aurora check; not a full-scale analytics result |
| ClickBench, 100M rows, 43 queries | 35.35 s first / 30.13 s warm | Historical EC2/Aurora run before the snapshot read change |
| SpatialBench SF10, four AOI queries | 7.85 s first / 4.54 s warm | Historical EC2/Aurora run before the snapshot read change |
| Quickwit, 100M logs, 8 clients | 52.04 ms search p95 | Historical OSB run |

The methodology, caveats and dated runs are in
[benchmark history](docs/benchmarks.md). The quick GET checks do not validate
DuckLake performance; rerun those workloads before using the historical
analytics numbers as current claims.

## Quick start

Needs [mise](https://mise.jdx.dev/) and Docker (or PostgreSQL 13+ with
`pg_cron` preloaded and installed in the target database: pass `--url`).

```sh
just setup    # toolchain (rust, mbx, just, python, uv, kind, helm, kubectl) + cargo fetch
just dev-db   # PostgreSQL 18 in Docker, for running the gateway by hand
just smoke    # the tests: kind cluster up, every workload once at smoke scale
```

Run the gateway with explicit SigV4 credentials (the example key is for
loopback development only):

```sh
export PGVS3_ACCESS_KEY=cachebench PGVS3_SECRET_KEY=cachebench-local-only
./target/release/pgvs3 --url 'postgres://user:pass@host/db?sslmode=require' serve --addr 127.0.0.1:8014
```

Use it like any path-style S3:

```sh
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
     -X PUT http://127.0.0.1:8014/lake  # create the bucket once
curl --aws-sigv4 aws:amz:us-east-1:s3 --user "$PGVS3_ACCESS_KEY:$PGVS3_SECRET_KEY" \
     -H 'Range: bytes=0-1023' http://127.0.0.1:8014/lake/some-object
```

DuckDB: `SET s3_endpoint='127.0.0.1:8014'; SET s3_use_ssl=false; SET s3_url_style='path';`

Other subcommands: `seed` (load generator), `bench` (GET latency/throughput
matrix plus the direct-PostgreSQL floor), `stat` (logical vs physical size).
For `just micro` or other standalone benchmark commands, also export
`AWS_ACCESS_KEY_ID=$PGVS3_ACCESS_KEY` and
`AWS_SECRET_ACCESS_KEY=$PGVS3_SECRET_KEY`. The kind harness reads these from
its generated Kubernetes Secret instead.

## How it works

Five storage tables (`crates/pgvs3/schema.sql`): `s3p.buckets` stores S3-managed
bucket existence and creation times; `s3p.objects` maps bucket/key to a
file id, size, sha256 ETag and, for multipart objects, the list of part files;
`s3p.chunks` holds each file as numbered rows of 8120 bytes. The separate
`s3p.uploads` and `s3p.upload_parts` tables track incomplete multipart work.

- **Writes** stream into a binary `COPY`, one per PUT and one per multipart
  part. A direct PUT commits its chunks and object row in one transaction:
  interrupted writes cannot leave committed, unaddressable bytes. Parts
  commit with their upload record; Complete publishes the ordered part list
  without moving data. `If-None-Match: *` and strong `If-Match` PUTs are
  checked atomically at publication; mutations of the same key serialize
  so concurrent creates cannot strand chunks. Unsupported conditional DELETEs
  fail instead of silently deleting. Acknowledged writes use synchronous commits.
- **Reads** turn a byte range into a row range by arithmetic (row =
  offset / 8120) and run one row-range query per 8 MiB. Multi-query reads
  (including multipart ranges) use one repeatable-read snapshot, so an
  overwrite cannot remove later rows during a response. They use one
  connection and fetch spans sequentially; measure the throughput trade-off
  before relying on the older parallel-read benchmark numbers. Rows stream to
  the client as they arrive.
- **Maintenance** belongs to PostgreSQL: a bounded cleanup function reaps
  abandoned multipart parts after 24 hours, and per-partition autovacuum runs
  without a gateway janitor or manual table VACUUM. Before listening, the
  gateway verifies the partition settings, requires `pg_cron` in the target
  database with `cron.database_name` pointing at it, and installs or refreshes
  the minute-by-minute cleanup job. It exits with an error if any of these
  requirements cannot be met. `just dev-db` and `just kind-up` install the
  extension; other deployments must preload `pg_cron` and install its extension
  before starting the gateway. The database role must be able to schedule jobs.
- **Buckets** are explicit: create them through S3 before writing. Empty
  buckets remain listed; deleting a bucket with objects or an incomplete upload
  returns `BucketNotEmpty`. The kind harness provisions its two workload
  buckets through S3 at startup. The `seed` load generator creates its own
  bucket if absent.
- **Layout version** (`s3p.layout`): a gateway refuses to run against an
  unknown layout. v4 permits independent simultaneous multipart uploads to
  the same key. Older layouts require an explicit migration or a fresh database.

Why 8120-byte rows (from the PostgreSQL 18 source, `heaptoast.c`,
`heaptoast.h`, `reloptions.c`): PostgreSQL moves a value out of line only when
the row exceeds the table's `toast_tuple_target`, which can be raised to at
most 8160 bytes. With that target, and no nullable columns, a row of
`file_id(8) + no(4) + length(4) + 8120` bytes plus its 24-byte header fits:
one row per 8 KB page, 99.6% full, no TOAST table and no detoasting on read.
Storage overhead is about 1.3% including indexes.

## Configuration

A flag overrides the same-named variable.

| Flag | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `--url` | `PGVS3_URL` | local dev DB | PostgreSQL URL. Remote hosts require `sslmode=require`, which verifies certificate and hostname against system CAs (plus `PGVS3_DB_CA_FILE` if set). Loopback/local `prefer` can fall back to plaintext. |
| `--addr` | `PGVS3_ADDR` | `127.0.0.1:8014` | Listen address; use `0.0.0.0:8014` in a container. |
| `--access-key` | `PGVS3_ACCESS_KEY` | required | SigV4 access key. |
| `--secret-key` | `PGVS3_SECRET_KEY` | required | SigV4 secret key. |
| `--tls-cert`, `--tls-key` | `PGVS3_TLS_CERT`, `PGVS3_TLS_KEY` | unset | PEM file paths for HTTPS on non-loopback addresses. Set both; mount the private key read-only. |
| `--allow-http` | `PGVS3_ALLOW_HTTP` | false | Explicitly permit plaintext HTTP outside loopback for an isolated benchmark rig; do not expose that listener to untrusted clients. |
| | `PGVS3_DB_CA_FILE` | unset | Path to additional PostgreSQL CA PEM bundle (Aurora rig mounts the RDS CA from a Secret). |
| | `PGVS3_DB_ALLOW_PLAINTEXT` | false | Permit `prefer`/`disable` to remote PostgreSQL for an isolated test rig. Local kind sets this explicitly. |
| | `PGVS3_POOL_MIN` | 64 | Connections opened at start and kept warm. Clamped to the max. |
| | `PGVS3_POOL_MAX` | 64 | Connections open at once, per gateway. Include all replicas, rollout headroom and other clients in the local Postgres `maxConnections` or Aurora connection budget. |

`GET /healthz` is an unauthenticated liveness probe.

## Design decisions

Each is backed by a measurement on the AWS rig:

- **No row cache.** It hit ~0% of the time behind DuckDB's own cache and cost
  ~0.5 ms of CPU per request.
- **Connections are reused most-recently-used first** (`src/pg.rs`). TCP
  restarts slow start on a connection idle for over ~200 ms, so a FIFO pool
  hands out its coldest connection: 1.18 ms vs 2.17 ms for a 64 KiB fetch.
- **Rows stream to the client as they arrive**, not after the whole range:
  ClickBench passes 9% faster, 8 MiB GETs 15.2 → 13.4 ms.
- **Reads up to 8 MiB are one query.** Splitting them across connections
  (2 MiB or 1 MiB parts) made ClickBench and SpatialBench 5–20% slower.
- **Bitmap heap scans for chunk bytes.** On cold data they are ~5× faster than
  index scans, thanks to PostgreSQL 18's read-ahead. Ordered bucket listings
  enable the metadata index scan for only their transaction.
- **Chunks are hash-partitioned 32 ways.** One table caps at 32 TiB, parallel
  writers spread over 32 heaps, and each GET touches one partition.
- **Multipart parts are separate files.** Parts upload in parallel with no
  staging, and Complete moves no data.

## Performance

Current-path quick checks and the full historical AWS/kind benchmark record
are in [benchmark history](docs/benchmarks.md). The full-scale reads have not
been re-measured since multi-query GETs gained a consistent snapshot.

## Read-scaling topology

Keep writes deliberately simple: one DuckLake catalog writer and one
Quickwit indexer/control-plane node; add query-only DuckDB workers, pgvs3
gateways and Quickwit **searcher-only** nodes as read load grows. Quickwit
[recommends PostgreSQL for distributed metadata](https://quickwit.io/docs/configuration/metastore-config):
the former file-backed S3 metastore has no cross-process write lock. The
three databases share one PostgreSQL cluster (Aurora on EC2), while immutable
Quickwit splits stay on pgvs3. The one-indexer chart uses a `Recreate` rollout
to avoid temporarily running two writers. `QUICKWIT_SEARCHERS=2 just kind-up`
adds search-only replicas behind the `quickwit` query Service; ingestion and
admin calls use `quickwit-core`. A
[headless DNS seed](https://quickwit.io/docs/configuration/node-config)
exposes direct gossip **UDP 7280** and gRPC **TCP 7281**. In kind, the core
and both searchers joined the same cluster, and the query service searched
logs ingested through the core (100 queries, zero errors). This verifies
discovery and routing, **not** performance scaling at 100M records. The
earlier two-node experiment failed because a ClusterIP seed did not carry
gossip.

Search performance depends on time-pruning **and** bounded split counts.
`no_merge` won on our sequential timestamped dataset, but Quickwit discourages
it for general searches. A better next A/B is an ingest-time day/hour key and
[partitioned splits](https://quickwit.io/docs/overview/concepts/querying#partitioning):
Quickwit keeps partitions isolated during merges, so it can compact within a
time bucket without smearing time ranges across buckets. Measure wide scans
and scale transitions too, not only the 10%-window p95.

KEDA can eventually control that searcher Deployment from Prometheus
`quickwit_search_root_search_requests_total{kind="server"}` (QPS) and
`quickwit_search_leaf_search_single_split_tasks{status="pending"}` (queue
pressure); both are exposed by this Quickwit version. Keep at least one warm
searcher and a slow scale-down to avoid cache churn; use the root-search
latency histogram as an SLO check, not an untested scaling threshold. If a
disk `split_cache` is enabled later, budget its full size per replica; do not
mount whole splits in tmpfs just to cache them in RAM. Quickwit's fast-field
and footer caches already do that selectively. Scale pgvs3 replicas from
read concurrency, but cap the fleet by Aurora's
connection budget (`replicas × PGVS3_POOL_MAX`, plus other clients and
rollout headroom). Neither Aurora ACUs nor a KEDA trigger make connections
unlimited. Do **not** autoscale the one ingest writer: Quickwit's ingest
queue uses local disk, so reliable restart needs a persistent WAL or an
upstream durable queue/replay path before treating that pod as disposable.

## Benchmarks

Testing lives here too: `just contract` runs S3/DB tests against a disposable
local PostgreSQL and two gateways. `just smoke` is `kind-up`, the kind contract
tests and smoke workloads. CI runs both after format, Clippy and unit checks.
`hk.pkl` checks Rust formatting/Clippy, shell and Python syntax, chart
rendering and the justfile with the same mise-pinned tools on developer
machines and CI. Install repository-local, mise-aware Git hooks once with
`mise exec -- hk install --mise`; checks never fix/stash/stage files during a
commit. One stack, one way to be wrong.
Mise installs Mr Boxington (`mbx`) for the native Rust builds in `just ci`
and the benchmark image build; the CI check job restores its Cargo cache with
`jdx/mr-boxington-action`. The published multi-arch Docker image compiles
inside its own builder without a remote compiler cache.

### The kind stack (local or EC2)

`just kind-up && just kind-validate && just kind-bench` brings up one cluster
and runs every workload with caching on — the same
commands on a laptop and on an EC2 instance, so numbers stay comparable.
`QUICK=1 just kind-bench` runs the whole set at smoke scale first: minutes,
to prove the wiring before spending an hour. `just kind-down` tears it down.
The scripts always address Kubernetes context `kind-pgvs3` explicitly; they
cannot accidentally install benchmarks into another current cluster.
Use `QUICKWIT_SEARCHERS=2 just kind-up` to prove read-only searcher scaling;
default zero additional searchers preserves the single-node CI shape.

The stack: one PostgreSQL cluster holds three logical databases — pgvs3's
object rows, the DuckLake catalog and Quickwit's metastore. Quickwit runs
one indexer/control-plane node and stores immutable index splits on pgvs3.
One idempotent kind Job creates the three databases before Quickwit starts,
both locally and against Aurora.

| Chart | Local and CI kind | EC2 kind |
| --- | --- | --- |
| `postgres` | One standalone PostgreSQL 18 pod and PVC; not a PostgreSQL HA set | Omitted; Aurora is external |
| `pgvs3` | Two stateless gateway replicas | Same Deployment; DB URL from a Kubernetes Secret |
| `quickwit` | One core node + optional search-only replicas; PostgreSQL metastore, splits on pgvs3 | Same chart |
| `kind-bench` | One short-lived Job per invocation | Same Jobs; DB settings from the same Secret |

`just` is the interface; `deploy/kind/up.sh`, `db.sh` and `bench.sh`
orchestrate these small charts. `PGVS3_DB_SECRET` selects an external database
configuration; unset uses local PostgreSQL.

The suites: `pgbench` (OLTP against the rows database), `tpch` (22 queries
over DuckLake), `click` (43 ClickBench queries over the full 100M-row
`hits.parquet`), `spatial` (Sedona-SpatialBench AOIs Q1–Q3 and Q6 over SF10),
`search` (OpenSearch Benchmark 2.4 bulk ingestion and fresh time-window queries
against Quickwit) and `stress` (the gateway's GET concurrency ceiling).
The standard full run defaults to 100M Quickwit logs, the full ClickBench
dataset and SpatialBench SF10; `QUICK=1` keeps CI small (5k logs, 1M ClickBench
rows, SpatialBench SF0.1). Use the EC2 rig for the large profile; its source
downloads and generated corpus need tens of GiB of temporary disk. Reset the
disposable rig databases before repeating a 100M-log ingestion run.
Full Aurora benchmark Jobs allow 24 GiB of container memory with a 16 GiB
DuckDB working limit; CI smoke retains the smaller default Job budget.

Scale from the environment — `SF` (TPC-H), `PARTS` (ClickBench 1% slices;
`0` is the full dataset), `SPATIAL_SF` and `SPATIAL_QUERIES` (AOI subset),
`DOCS` and `WINDOW_FRAC` (Quickwit logs and query windows), `SEED_GB`,
`REQUESTS`, `CONCURRENCY`, `SIZES`, `SECONDS_RUN` — for example
`SF=30 CONCURRENCY=1,8,32,64,128 just kind-bench`. The local-only PostgreSQL
chart caps connections at 384: two 64-connection gateway pools normally,
temporary warm pools during rolling updates, DuckLake/benchmark clients and
headroom. If the gateway replica or pool count changes, update
`maxConnections` in `deploy/charts/postgres/values.yaml`.
Aurora-backed kind does not install that chart. Results land in
`.tmp/pgvs3/kind-bench.jsonl`, one line per measurement; individual logs
are in `.tmp/pgvs3/jobs/`.

The Quickwit search suite uses the same OSB corpus, ES `create` bulk requests,
and query DSL that can be run against OpenSearch. A loopback-only benchmark
adapter supplies two OSB node-info responses missing from Quickwit 0.9.1;
`_bulk` goes to the core and `_search` to the search service unchanged. OSB's
per-item errors must be zero **and** the indexed count must match the corpus
before search starts. The driver prints the corpus SHA-256 and both OSB runs'
operation metrics. This is a portable custom workload, **not** a stock OSB
track or a claim that the old native-API search numbers are cross-engine
comparable. Quickwit's ES compatibility root identifies itself as ES 7.17 in
OSB's raw metadata; the deployment is Quickwit 0.9.1, and OpenSearch node
telemetry is not reported for it.

To generate exactly the same corpus and OSB workloads for another ES-compatible
stack:

```sh
OSB_OUT_DIR=.tmp/pgvs3/osb-portable python3 deploy/bench/suites/osb_run.py \
  --generate-only --docs 100000000 --index otel-logs-v0_9
```

An example OpenSearch mapping is in
`deploy/bench/workloads/opensearch-otel-mapping.json`. Pre-create the index,
then run OSB `benchmark-only` against the generated `ingest/` workload,
followed by `search/` with `OSB_DOCS=100000000 OSB_WINDOW_FRAC=0.1` and
`--randomization-enabled --randomization-repeat-frequency=0`. Compare only
equivalent index settings, data counts and client counts. The `search/`
workload uses one and eight concurrent clients; ingestion has one ordered bulk
client. Corpus generation is outside OSB ingest timing; `indexed_s` also
includes the wait until every document is searchable.
The generated 1k-document corpus and both OSB stages were smoke-tested against
OpenSearch 2.19.4 with zero errors, an exact indexed-count check and nonempty
matching time-window searches. No 100M OpenSearch comparison has been run yet.

### Harness entry points (development)

For working on the gateway or the harness without a cluster: `just dev-db`
runs PostgreSQL with `pg_cron`, `just seed && just micro` is the GET matrix, and
`just clickbench`, `just spatialbench sf=10` and `just tpch sf=10` drive the
DuckLake harnesses directly (pass arguments through
`extra='--set name=value'` for A/B runs).

### EC2 rig (kind with external Aurora PostgreSQL)

CI and a laptop use in-kind PostgreSQL. The EC2 rig runs **the same kind
charts and benchmark Jobs**, but pgvs3's objects, the DuckLake catalog and
Quickwit's metastore are three logical databases on **Aurora PostgreSQL 18.6
Serverless v2, I/O-Optimized**. Quickwit has one core writer and optional
search-only replicas, with its index splits on pgvs3's S3 endpoint. This
separates Aurora round trips and I/O from the kind host while retaining the CI
workload shape.

Configure the rig in the ignored `.env` (see `.env.example`) and authenticate
with the selected AWS profile before running the commands below.

```sh
just rig-up                       # tagged CloudFormation stack; sync, deploy, validate
just rig-contract                 # Rust S3 contract against Aurora's gateway pods
just rig-churn                    # opt-in large overwrite/delete + multipart churn and DB stats
just rig-validate                 # CI's kind smoke gate against Aurora
SUITES=tpch,click just rig-bench  # or run all six suites with just rig-bench
QUICK=1 just rig-bench            # smoke-scale benchmark
just rig-reset                    # pre-release test data only; explicit confirmation
just rig-results                  # download latest JSONL after a disconnected run
just rig-status                   # stack status and endpoints
just rig-teardown                 # terminates the rig and its related resources
```

`just kind-churn` and `just rig-churn` run a separate, opt-in two-gateway test
over 64 × 32 MiB PUTs (override with `PGVS3_CHURN_ROUNDS` and
`PGVS3_CHURN_MIB`). They audit old chunk file IDs and aborted parts, measure
read and write p95, and sample partition dead tuples/autovacuum before and
90 seconds after. Aurora churn logs go to `.tmp/pgvs3/rig-out/`; the test
cleans its objects through S3. These are measurements, not an assertion that
autovacuum shrinks relation files.

The rig uses an m7i.4xlarge (16 vCPU, 64 GiB), a 250 GiB gp3 volume and
Aurora scaling from 2 to 16 ACUs. Results are copied to
`.tmp/pgvs3/rig-out/`. **Both EC2 and Aurora keep accruing charges** until
`just rig-teardown`.

The AMI needs only a small mise installer in cloud-init. After the checkout
arrives, `mise -E ec2 bootstrap --yes` applies `mise.ec2.toml`: host Docker
packages/service/group plus the same pinned tools as CI. CI and local
development run only `mise install`, so EC2 host changes never leak into CI.

## Potential further gains

Small ones: reads are ~20% of a pass and network-bound. Measure before
building.

- **DuckDB read merging.** Try `parquet_prefetch_column_gap` values through
  `extra`; DuckDB already read ~10% fewer bytes once GETs got faster.
  A win is a user setting, not code.
- **Single large streams.** Earlier parallel reads reached ~1.7 GiB/s per
  64 MiB GET on an instance with ~3.8 GiB/s aggregate capacity; remeasure
  under the current snapshot path before tuning. SpatialBench's zone queries
  read ~640 MiB chunks.
- **Small-GET overhead.** The gateway adds ~0.25 ms per request, at most
  2–3% of a pass. Act only on a clear profile hotspot (SigV4, s3s, hyper).
- **Cold reads.** Aurora caps PostgreSQL 18's read merging at 128 KiB
  (`io_max_combine_limit`). Raising it is a parameter-group experiment on a
  dataset larger than the cache.
- **Outside the gateway.** A larger instance raises the ~3.8 GiB/s ceiling;
  fleets spread over AZs want an Aurora reader in each (a cross-AZ round trip
  costs ~7×).
- **Tried, no gain:** a proxy cache or prefetcher, splitting reads under
  8 MiB, bigger TCP buffers, DuckDB's curl HTTP client.

Before more tuning: run `just kind-contract` against the two gateway pods
(bucket lifecycle, range edges, multipart, overwrite/delete, scheduled expiry).

## Running in a container

Published to GHCR on every push to `main` (amd64 and arm64, so AWS Graviton
works), built by `just image` from a two-stage Dockerfile: `rust:alpine` builds
a static musl binary, and the image is `scratch`. Supply a TLS certificate and
key for any non-loopback listener; the kind rig alone opts into HTTP. Store S3
credentials and certificates in Secrets, not Helm values or ConfigMaps:

```sh
docker run -p 8014:8014 \
  -v /path/to/tls:/tls:ro \
  -e PGVS3_URL="postgres://user:pass@host/db?sslmode=require" \
  -e PGVS3_ACCESS_KEY=admin -e PGVS3_SECRET_KEY=change-me \
  -e PGVS3_TLS_CERT=/tls/tls.crt -e PGVS3_TLS_KEY=/tls/tls.key \
  ghcr.io/adonm/pgvs3:latest
```

```yaml
containers:
  - name: pgvs3
    image: ghcr.io/adonm/pgvs3:latest
    env:
      - name: PGVS3_URL
        valueFrom: { secretKeyRef: { name: pgvs3, key: url } }
      - name: PGVS3_SECRET_KEY
        valueFrom: { secretKeyRef: { name: pgvs3, key: secret-key } }
      - name: PGVS3_ACCESS_KEY
        valueFrom: { secretKeyRef: { name: pgvs3, key: access-key } }
      - { name: PGVS3_TLS_CERT, value: /tls/tls.crt }
      - { name: PGVS3_TLS_KEY, value: /tls/tls.key }
    volumeMounts:
      - { name: tls, mountPath: /tls, readOnly: true }
    ports: [{ containerPort: 8014 }]
volumes:
  - name: tls
    secret: { secretName: pgvs3-tls }
```

## Repo layout

- `crates/pgvs3/src/`: `main.rs` (CLI), `server.rs` (the S3 API on
  [s3s](https://crates.io/crates/s3s): routes and `/healthz`),
  `db.rs` (object storage: metadata, reads, writes, multipart), `ingest.rs`
  (the `COPY` write path), `cache.rs` (metadata cache), `pg.rs` (PostgreSQL pool
  and TLS), `bench.rs` and `seed.rs` (the GET matrix and load generator).
- `crates/pgvs3/schema.sql`: the storage layout.
- `deploy/bench/harness/` and `queries/`: the benchmark harness (ClickBench,
  SpatialBench, TPC-H).
- `deploy/`: `kind/cluster.yaml`, `kind/up.sh`, `kind/db.sh`, `kind/bench.sh`,
  `kind/rig.yaml` (the EC2 +
  Aurora stack), `charts/` — `postgres`, `pgvs3`, `quickwit`, `kind-bench` —
  and `bench/`, the suite runners and benchmark image.
- `justfile`: every task (`just` lists them); `mise.ec2.toml`: the opt-in EC2
  host bootstrap; `.env.example`: the rig's
  settings.
