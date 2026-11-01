# pgvs3 — PostgreSQL 18 as the Object Data Plane

**Durable object storage, shared S3/HTTP caching, analytical storage for DuckDB
and [DuckLake](https://ducklake.select/), and bounded companion queues—served
through stateless Go gateways over PostgreSQL 18.** PostgreSQL stores the bytes,
not just a metadata index. pgvs3 uses its transactions, MVCC snapshots, WAL,
physical replication and advisory locks instead of adding separate durability,
cache-coherence and coordination services.

![FIG 01 — System Topology & Parallel Authority Lanes](assets/architecture-flow.svg)

> **Trusted networks only — permanently.** pgvs3 does not authenticate or
> authorize clients, terminate inbound TLS, or validate S3 signatures or
> credentials. Run it on loopback, a private VPC/VLAN, or behind a service mesh
> that enforces access control and TLS, and treat `:8014` like a database port.
> Presigned URLs are rejected (`400 InvalidRequest`). See [Architecture & Trust Model](design.md).

---

## Start Here: Choose Your Path

![FIG 02 — Workload, Schema and Partitioning Decision Tree](assets/decision-tree.svg)

| Goal | Primary Guide | What You Will Build & Verify |
| --- | --- | --- |
| **Choose the right schema, partition & storage model** | [Workload Decision Tree & Resource Trade-offs](guides/index.md) | Map OLTP, DuckLake, cache and queue requirements across RAM, Disk/WAL, Network and CPU. |
| **Protect OLTP tail latency & partition without cron DDL** | [1. Protect OLTP & Fixed Partitioning](guides/postgres.md) | Connection pool math, covering B-tree plans, TOAST splitting, and 32–64 fixed hash/quantile partitions. |
| **Model billion-row DuckLake, wide columns & Hilbert curves** | [2. Analytical Snapshots, Wide Columns & Hilbert](guides/analytics.md) | 24-bit hierarchical Hilbert sorting for skewed hotspots, byte-bounded row groups, `skip_stats_columns`, late materialization & batch compaction. |
| **Share S3/HTTP bytes & stage large writes across gateways** | [3. Shared Cache & Write-Back Staging](guides/cache.md) | Coalesced cold fills (`s3c_`), aligned Parquet range caching, and non-expiring ack-first write-back (`s3w_`). |
| **Bridge committed SQL changes to background workers** | [4. Jobs, Outboxes & Streams](guides/jobs.md) | Transactional SQL outbox, HTTP `409` envelope reconciliation, lazy `SKIP LOCKED` claims, receipts & 3-segment `TRUNCATE` streams. |
| **Diagnose bottlenecks & execute safe recovery** | [5. Observe, Diagnose & Recover](guides/observe-recover.md) | PostgreSQL 18 `pg_stat_io` / `pg_stat_checkpointer` triage, LSN replay barriers, and pending-body restore checklists. |

---

## Interactive Scaling, Partitioning & Resource Estimator

Adjust your collection size, wide-column payload width, spatial hotspot skew, and gateway fleet below to compare **Naive (`SELECT *` + Equal-Area Grid)** with **column projection only** and **24-bit Hilbert (`SORTED BY (hilbert_id, ts)` + late materialization)**. Hash buckets help entity filters; they do not replace spatial pruning.

<p class="pgvs3-sim-note"><strong>Heuristic model, not a benchmark.</strong> These are closed-form estimates from the sliders (ZSTD 0.42× narrow / 0.55× wide, 8–22% lat-stripe scan, 0.8% narrow + 0.15% wide Hilbert fetch). Re-measure with your own data; measured gates are in <a href="guides/validation.md">Executable Validation Evidence</a>.</p>

```{=html}
<div class="pgvs3-sim" id="pgvs3-estimator">
  <div class="pgvs3-sim-grid">
    <div class="pgvs3-sim-ctrl">
      <label for="sim-rows">Dataset Rows <span class="val" id="val-rows">1,000M (1.0B)</span></label>
      <input type="range" id="sim-rows" min="10" max="5000" step="10" value="1000">
    </div>
    <div class="pgvs3-sim-ctrl">
      <label for="sim-narrow">Narrow Filter/Key Columns <span class="val" id="val-narrow">48 B/row</span></label>
      <input type="range" id="sim-narrow" min="16" max="256" step="8" value="48">
    </div>
    <div class="pgvs3-sim-ctrl">
      <label for="sim-wide">Wide Payload / Embedding <span class="val" id="val-wide">2,048 B/row</span></label>
      <input type="range" id="sim-wide" min="0" max="8192" step="128" value="2048">
    </div>
    <div class="pgvs3-sim-ctrl">
      <label for="sim-skew">Hotspot Skew (Rows in 1% Area) <span class="val" id="val-skew">90%</span></label>
      <input type="range" id="sim-skew" min="10" max="95" step="5" value="90">
    </div>
    <div class="pgvs3-sim-ctrl">
      <label for="sim-gw">Gateway Replicas (pool-max 32) <span class="val" id="val-gw">4</span></label>
      <input type="range" id="sim-gw" min="1" max="12" step="1" value="4">
    </div>
    <div class="pgvs3-sim-ctrl">
      <label for="sim-outage">Origin Outage Buffer (@25 MiB/s) <span class="val" id="val-outage">15 min</span></label>
      <input type="range" id="sim-outage" min="0" max="60" step="5" value="15">
    </div>
  </div>

  <div class="pgvs3-sim-kpis" aria-live="polite">
    <div class="pgvs3-kpi bad" id="kpi-ram-box">
      <div class="k-label">Writer Row-Group RAM / Thread</div>
      <div class="k-val" id="kpi-ram">246 MiB → 32 MiB</div>
      <div class="k-sub" id="kpi-ram-sub">122,880 rows vs byte-bounded 32MB</div>
    </div>
    <div class="pgvs3-kpi good">
      <div class="k-label">Selective 2D Query I/O (0.1% Match)</div>
      <div class="k-val" id="kpi-io">220 GiB → 1.7 GiB</div>
      <div class="k-sub" id="kpi-io-sub">128× less network &amp; decompression I/O</div>
    </div>
    <div class="pgvs3-kpi good">
      <div class="k-label">Hotspot Row-Group Adaptation</div>
      <div class="k-val" id="kpi-files">90% → 9 dense RGs</div>
      <div class="k-sub" id="kpi-files-sub">Equal-byte RGs subdivide the hotspot; sparse rows share one</div>
    </div>
    <div class="pgvs3-kpi warn" id="kpi-conn-box">
      <div class="k-label">PostgreSQL Fleet Connections</div>
      <div class="k-val" id="kpi-conn">356 / 448 max</div>
      <div class="k-sub" id="kpi-conn-sub">4 (+1 surge) × 64 + 36 app/scanner/admin</div>
    </div>
    <div class="pgvs3-kpi">
      <div class="k-label">PG Compressed Storage</div>
      <div class="k-val" id="kpi-disk">1068 GiB</div>
      <div class="k-sub" id="kpi-disk-sub">≈ 4272 × 256MB Parquet files</div>
    </div>
    <div class="pgvs3-kpi">
      <div class="k-label">Origin Outage Stage</div>
      <div class="k-val" id="kpi-outbox">22.0 GiB</div>
      <div class="k-sub" id="kpi-outbox-sub">15 min @ 25 MiB/s in s3w_</div>
    </div>
  </div>

  <div class="pgvs3-bars">
    <div class="pgvs3-bar-row">
      <div class="pgvs3-bar-hdr">
        <span><strong>Naive:</strong> <code>SELECT *</code> + Equal-Area Grid + <code>SORTED BY (lat, lon)</code> (no lon pruning)</span>
        <span id="bar-naive-txt" style="color:var(--danger);font-family:monospace;font-weight:600;">220 GiB scanned</span>
      </div>
      <div class="pgvs3-bar-track">
        <div class="pgvs3-bar-fill" id="bar-naive" style="width:100%;background:var(--danger);"></div>
      </div>
    </div>
    <div class="pgvs3-bar-row">
      <div class="pgvs3-bar-hdr">
        <span><strong>Column Projection Only:</strong> Narrow + Wide fetched for entire lat stripe</span>
        <span id="bar-proj-txt" style="color:var(--warning);font-family:monospace;font-weight:600;">30.1 GiB scanned</span>
      </div>
      <div class="pgvs3-bar-track">
        <div class="pgvs3-bar-fill" id="bar-proj" style="width:13.7%;background:var(--warning);"></div>
      </div>
    </div>
    <div class="pgvs3-bar-row">
      <div class="pgvs3-bar-hdr">
        <span><strong>pgvs3 + DuckLake Recommended:</strong> 24-Bit Hilbert <code>SORTED BY (hilbert_id, ts)</code> + Late Materialization</span>
        <span id="bar-hilb-txt" style="color:var(--success);font-family:monospace;font-weight:600;">1.7 GiB scanned (128× less I/O)</span>
      </div>
      <div class="pgvs3-bar-track">
        <div class="pgvs3-bar-fill" id="bar-hilb" style="width:2%;background:var(--success);"></div>
      </div>
    </div>
  </div>

  <pre class="pgvs3-sql-out" id="sim-sql"></pre>
</div>

<script>
(function() {
  const el = id => document.getElementById(id);
  const inputs = ['sim-rows', 'sim-narrow', 'sim-wide', 'sim-skew', 'sim-gw', 'sim-outage'];
  if (!el('sim-rows')) return;

  function fmtGiB(bytes) {
    const gib = bytes / (1024 ** 3);
    if (gib >= 100) return gib.toFixed(0) + ' GiB';
    if (gib >= 1) return gib.toFixed(1) + ' GiB';
    const mib = bytes / (1024 ** 2);
    return Math.max(0.1, mib).toFixed(1) + ' MiB';
  }

  function update() {
    const rowsM = Number(el('sim-rows').value);
    const rows = rowsM * 1e6;
    const narrowB = Number(el('sim-narrow').value);
    const wideB = Number(el('sim-wide').value);
    const skewPct = Number(el('sim-skew').value);
    const gw = Number(el('sim-gw').value);
    const outageMin = Number(el('sim-outage').value);

    const totalRowB = narrowB + wideB;
    el('val-rows').textContent = rowsM >= 1000 ? (rowsM / 1000).toFixed(1) + 'B rows' : rowsM + 'M rows';
    el('val-narrow').textContent = narrowB + ' B/row';
    el('val-wide').textContent = wideB + ' B/row';
    el('val-skew').textContent = skewPct + '%';
    el('val-gw').textContent = gw + ' gateways';
    el('val-outage').textContent = outageMin + ' min';

    // 1. Writer RAM per thread for 122,880 rows vs byte-bounded
    const defaultRgMiB = Math.round((122880 * totalRowB) / (1024 * 1024));
    const boundedRgMiB = Math.min(defaultRgMiB, 32);
    const reduced = boundedRgMiB < defaultRgMiB;
    el('kpi-ram').textContent = reduced
      ? defaultRgMiB + ' MiB → ' + boundedRgMiB + ' MiB'
      : defaultRgMiB + ' MiB (fits)';
    el('kpi-ram-box').className = 'pgvs3-kpi ' + (defaultRgMiB > 128 ? 'bad' : defaultRgMiB > 64 ? 'warn' : 'good');
    el('kpi-ram-sub').textContent = reduced
      ? 'Set parquet_row_group_size_bytes = \'' + boundedRgMiB + 'MB\''
      : 'Default 122,880 rows fits comfortably';
    // 2. Compressed storage (approx 0.42x on narrow + 0.55x on wide with ZSTD level 9)
    const compNarrow = rows * narrowB * 0.42;
    const compWide = rows * wideB * 0.55;
    const totalCompBytes = compNarrow + compWide;

    // 3. Selective 2D bbox + time query (0.1% true match inside hotspot):
    // Naive (lat, lon) sort scans ~12% of lat stripe across all columns (because lon is unpruned in hotspot)
    const stripeFrac = 0.08 + (skewPct / 100) * 0.14;
    const naiveScanBytes = totalCompBytes * stripeFrac;
    const projScanBytes = (compNarrow * stripeFrac) + (compWide * 0.025);
    // Hilbert 24-bit quantile row groups scan ~0.8% of narrow column chunks + 0.15% of wide chunks via late materialization
    const hilbScanBytes = (compNarrow * 0.008) + (compWide * 0.0015);
    const speedup = Math.max(1, Math.round(naiveScanBytes / Math.max(1, hilbScanBytes)));

    el('kpi-io').textContent = fmtGiB(naiveScanBytes) + ' → ' + fmtGiB(hilbScanBytes);
    el('kpi-io-sub').textContent = speedup + '× less network & decompression I/O';

    el('bar-naive-txt').textContent = fmtGiB(naiveScanBytes) + ' scanned';
    el('bar-proj-txt').textContent = fmtGiB(projScanBytes) + ' scanned';
    el('bar-hilb-txt').textContent = fmtGiB(hilbScanBytes) + ' scanned (' + speedup + '× less I/O)';
    el('bar-proj').style.width = Math.max(4, Math.min(95, (projScanBytes / Math.max(1, naiveScanBytes)) * 100)).toFixed(1) + '%';
    el('bar-hilb').style.width = Math.max(2, Math.min(90, (hilbScanBytes / Math.max(1, naiveScanBytes)) * 100)).toFixed(1) + '%';

    // 4. File / partition balance
    const targetFileCount = Math.max(1, Math.ceil(totalCompBytes / (256 * 1024 * 1024)));
    const denseRgs = Math.max(1, Math.round(skewPct / 10));
    el('kpi-files').textContent = skewPct + '% → ' + denseRgs + ' dense ' + (denseRgs === 1 ? 'RG' : 'RGs');
    el('kpi-files-sub').textContent = 'Equal-byte RGs subdivide the hotspot; sparse rows share one';

    // 5. Connections
    const poolMax = 32;
    const gwConns = ( gw + 1 ) * (poolMax + Math.min(poolMax, 64)); // includes +1 rollout surge gateway
    const totalConns = gwConns + 36;
    const recMaxConns = Math.max(256, Math.ceil((totalConns + 32) / 64) * 64);
    el('kpi-conn').textContent = totalConns + ' / ' + recMaxConns + ' max';
    el('kpi-conn-box').className = 'pgvs3-kpi ' + (totalConns > 512 ? 'bad' : totalConns > 240 ? 'warn' : 'good');
    el('kpi-conn-sub').textContent = gw + ' (+1 surge) × (32 main + 32 chunk) + 36';

    // 6. Storage + Outbox buffer
    const outboxBytes = outageMin * 60 * 25 * 1024 * 1024;
    el('kpi-disk').textContent = fmtGiB(totalCompBytes);
    el('kpi-disk-sub').textContent = '≈ ' + targetFileCount + ' × 256MB Parquet files';
    el('kpi-outbox').textContent = outboxBytes === 0 ? '0 MiB' : fmtGiB(outboxBytes);
    el('kpi-outbox-sub').textContent = outboxBytes === 0 ? 'no outage buffer' : outageMin + ' min @ 25 MiB/s in s3w_';

    // 7. Live SQL & Config snippet
    const rgSetting = reduced
      ? "CALL lake.set_option('parquet_row_group_size_bytes', '" + boundedRgMiB + "MB', table_name => 'events');" +
        (wideB > 0 ? "\nCALL lake.set_option('skip_stats_columns', ['wide_payload'], table_name => 'events');" : "")
      : "CALL lake.set_option('parquet_row_group_size', 122880, table_name => 'events');";
    const rgComment = reduced
      ? "let 256MB files & " + boundedRgMiB + "MB row groups slice"
      : "let 256MB files & 122,880-row row groups slice";

    el('sim-sql').textContent =
`-- Tailored for ${el('val-rows').textContent} (${narrowB}B narrow + ${wideB}B wide, ${skewPct}% spatial hotspot skew):
CREATE TABLE lake.events (
    id BIGINT, customer_id INTEGER, ts TIMESTAMPTZ,
    lat DOUBLE, lon DOUBLE, hilbert_id BIGINT${wideB > 0 ? ", wide_payload VARCHAR" : ""}
)
-- Omit equal-area spatial PARTITIONED BY under ${skewPct}% skew; ${rgComment} hilbert_id by quantiles:
SORTED BY (hilbert_id, ts);

CALL lake.set_option('parquet_compression', 'zstd', table_name => 'events');
CALL lake.set_option('parquet_compression_level', 9, table_name => 'events');
CALL lake.set_option('target_file_size', '256MB', table_name => 'events');
${rgSetting}
-- PostgreSQL connection budget: set max_connections >= ${recMaxConns} for ${gw} gateway${gw === 1 ? '' : 's'} (--pool-max ${poolMax}) + 1 rollout surge pod.`;
  }

  inputs.forEach(id => el(id).addEventListener('input', update));
  update();
})();
</script>
```

---

## Visual Atlas of Data Layouts & Movements (FIG 03–06)

Figures use stable site-wide numbers, so the same diagram can be reused on a guide page without renumbering. FIG 01 (topology) and FIG 02 (decision tree) appear above; the four data-layout figures follow.

### FIG 03 — 8,192-Byte Page Layout & Cut-Through Streaming I/O

Small objects ($\le 64\text{ KiB}$ native, $\le 8,120\text{ B}$ cache/RESP) live inline in `objects.inline_data`. Larger objects (10 MiB–1 GiB Parquet) split into fixed **8,120-byte logical blocks** compressed per block with Zstd *only when it shrinks*, fitting **one tuple per 8,192-byte PostgreSQL page** with `toast_tuple_target = 8160` and `STORAGE EXTERNAL` (zero out-of-line TOAST pointer chasing, and $O(1)$ byte-range offset seeking at $\text{offset} = \text{no} \times 8,120$).

![FIG 03 — 8,192-Byte Page, 8,120-Byte Chunks & Cut-Through Streaming I/O](assets/page-layout-io.svg)

See [8 KiB Page Layout & I/O Paths](performance.md) and [Why Chunks Are Unpartitioned](partitioning.md).

---

### FIG 04 — Origin-Backed Shared Cache (`s3c_`) & Durable Write-Back Stage (`s3w_`)

Cold misses coalesce behind a renewable 2-minute database lease (`s3p.cache_fill_leases`), spooling bodies $>8\text{ MiB}$ to local scratch outside a database transaction before publishing version-fenced chunks to `s3c_<namespace>`. Unconditional writes $>8\text{ MiB}$ and multipart completions commit atomically to WAL-logged `s3w_<namespace>` + `s3p.pending_uploads` (`expires_at IS NULL`) before returning `200 OK`, then drain asynchronously to the upstream origin under a session advisory key lock.

![FIG 04 — Shared Origin Cache (s3c_) & Ack-First Write-Back Outbox (s3w_)](assets/cache-writeback-lifecycle.svg)

See [Shared Cache & Write-Back Staging](guides/cache.md) and [Operations: Cache Tier](operations.md#cache-tier-in-front-of-another-object-store).

---

### FIG 05 — Billion-Row DuckLake: Fixed Buckets, Density-Adaptive Hilbert & Late Materialization

Instead of high-maintenance rolling time partitions or equal-area spatial grids that skew under urban hotspots, combine **fixed 32–64 hash buckets** (`bucket(32, customer_id)`) with **high-resolution 1D Hilbert sorting** (`SORTED BY (hilbert_id, ts)`). Because Parquet row groups (`parquet_row_group_size_bytes`) and DuckLake files (`target_file_size`) slice the sorted stream by **row/byte count (empirical quantiles)** rather than geographic area, sparse regions pack into one row group while dense hotspots automatically subdivide into tight neighborhood bounding boxes.

![FIG 05 — 24-Bit Hilbert Sorting, Equal-Byte Row Groups & Late Materialization](assets/spatiotemporal-hilbert-layout.svg)

See [DuckLake, Wide Columns & Hilbert Modeling](guides/analytics.md#schema-wide-column-spatiotemporal-and-write-for-read-modeling).

---

### FIG 06 — Transactional SQL Outbox, OJS Leases & 3-Segment `TRUNCATE` Streams

Each `s3q_<name>` schema provides two delivery views: **OJS competing jobs** (bridged from application SQL via an outbox, claimed lazily through a single `FOR UPDATE SKIP LOCKED` index on `(priority DESC, next_visible_at, seq)`, and fenced by `lease_token`) and **PgQue-shaped fan-out streams** (using `pg_snapshot` ticks to prevent late-commit sequence gaps and rotating three physical `stream_events_0..2` partitions via zero-bloat `TRUNCATE`).

![FIG 06 — OJS Outbox Claim/ACK Boundaries & 3-Segment TRUNCATE Streams](assets/outbox-jobs-streams.svg)

See [Jobs, Outboxes & Streams](guides/jobs.md) and [Queue & Stream Model](queue-stream.md).

---

## Quick Start

Requires [mise](https://mise.jdx.dev/) and Docker (or an existing PostgreSQL 18+ instance).

For a self-contained executable walkthrough of all workload recipes (OLTP plans, PostgreSQL-to-Parquet extraction, same-cluster `pgbench` + `loadgen` canary, DuckLake 24-bit Hilbert + 32-bucket hash compaction, shared cache hits, pending write-back outage recovery, and OJS outbox/stream failure windows), run:

```sh
just setup
just guides     # executes all 35 bound guide snippets in a disposable stack
just analytics  # runs the README Parquet + DuckLake native & origin walkthrough
```

To run a persistent local development database and gateway by hand:

```sh
just setup
just dev-db   # starts PostgreSQL 18 on 127.0.0.1:5432
PGVS3_URL='postgres://postgres:postgres@127.0.0.1:5432/pgvs3_bench?sslmode=disable' \
  mise exec go -- go run ./cmd/pgvs3 serve --addr 127.0.0.1:8014
```

Create a bucket explicitly before writing objects:

```sh
curl -fsS -X PUT http://127.0.0.1:8014/lake
curl -fsS -X PUT --data-binary @README.md http://127.0.0.1:8014/lake/README.md
curl -fsS http://127.0.0.1:8014/lake/README.md
```

### Use from DuckDB 2.0+

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET (
    TYPE s3, KEY_ID 'test', SECRET 'test',
    REGION 'us-east-1', ENDPOINT '127.0.0.1:8014',
    URL_STYLE 'path', USE_SSL false
);
COPY (
    SELECT range AS id, (range % 100)::INTEGER AS group_id,
           md5(range::VARCHAR) AS payload
    FROM range(1000000)
) TO 's3://lake/events.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT group_id, count(*) AS rows
FROM read_parquet('s3://lake/events.parquet')
GROUP BY group_id ORDER BY group_id LIMIT 5;
```

### Use with DuckLake (Catalog and Objects on One Cluster)

```sql
INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
CREATE SECRET (TYPE POSTGRES, HOST '127.0.0.1', PORT 5432,
               DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'postgres');
ATTACH 'ducklake:postgres:' AS lake (DATA_PATH 's3://lake/ducklake/');
CALL lake.set_option('data_inlining_row_limit', 0);
CREATE TABLE lake.events (site_id INTEGER, day DATE, value DOUBLE);
INSERT INTO lake.events VALUES (42, DATE '2026-09-01', 1.0);
SELECT * FROM lake.events;
```

---

## Operating Envelope & Storage Roles

| Namespace | Schema Prefix | Copy of Record & Durability |
| --- | --- | --- |
| **Shared Catalog & Control** | `s3p` | Authoritative namespace catalog ($\le 100$ namespaces), `file_ids` sequence, `cache_fill_leases`, and `pending_uploads` outbox control rows. |
| **Native Object Store** | `s3b_<name>` | WAL-logged PostgreSQL `objects`, `chunks`, `uploads` and `garbage`. No external origin required. |
| **Shared Origin Read Cache & RESP** | `s3c_<name>` | WAL-logged origin read fills, aligned range blocks, `origin_metadata` validators, and standalone RESP2 cache keys. |
| **Durable Origin Write-Back Stage** | `s3w_<name>` | WAL-logged acknowledged write-back bodies (`expires_at IS NULL` while pending handoff to origin; never treat as disposable cache). |
| **Companion Jobs & Streams** | `s3q_<name>` | WAL-logged OJS competing `jobs` and PgQue-shaped 3-segment `stream_events_0..2` fan-out log. |
