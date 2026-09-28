//! PostgreSQL storage: an object is numbered 8120-byte rows in `s3p.chunks`
//! plus one row in `s3p.objects` (layout: schema.sql).
//!
//! Reads cache metadata, never row bytes. Ranges stream rows in 8 MiB spans;
//! multi-query GETs hold a snapshot until the last part has been fetched.
//!
//! Writes use a binary COPY per PUT or multipart part. Object publication
//! shares the COPY transaction; multipart Complete publishes existing parts.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::Context;
use std::time::SystemTime;

use anyhow::{Context as _, Result};
use bytes::{Bytes, BytesMut};
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use md5::Md5;
use sha2::{Digest, Sha256};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, IsolationLevel, Row, Transaction};

use crate::cache::{epoch, meta_get, meta_invalidate, meta_put, Meta};
use crate::ingest::{
    Digests, IngestMsg, IngestResult, Published, RowFramer, COPY_HEADER, COPY_SQL, SEND_BATCH,
};
pub use crate::pg::Pool;

pub const SCHEMA: &str = include_str!("../schema.sql");

/// Row payload: file_id(8) + no(4) + varlena(4) + 8120 = 8136 data bytes,
/// tuple 8160 bytes = one row per 8 KB page.
pub const ROW_BYTES: i64 = 8120;
const SMALL_MAX: usize = 8 << 20;

/// Whole rows for a contiguous `no` range (cheaper than `= ANY` on Aurora:
/// 1.74 vs 2.02 ms server time per warm 8 MiB span).
const GET_RANGE_SQL: &str =
    "SELECT c.no, c.data FROM s3p.chunks c WHERE c.file_id = $1 AND c.no >= $2 AND c.no <= $3 \
     AND EXISTS (SELECT 1 FROM s3p.objects o WHERE o.bucket = $4 AND o.key = $5 \
                 AND o.file_id = $6)";

/// Rows per part: any span up to SMALL_MAX (it may start mid-row) fits one;
/// longer reads are split into snapshot-protected queries.
const PART_ROWS: usize = SMALL_MAX / ROW_BYTES as usize + 2;
/// Rows go to the response in chunks of about this size.
const CHUNK: usize = 256 << 10;

pub async fn connect(url: &str) -> Result<Pool> {
    // Bitmap scans use PostgreSQL 18 read-ahead for cold ranges; the session
    // timeouts prevent dead gateways from holding locks indefinitely.
    let session = String::from(
        "SET enable_indexscan = off; SET work_mem = '64MB'; SET effective_io_concurrency = 32; \
         SET tcp_keepalives_idle = 30; SET tcp_keepalives_interval = 10; \
         SET tcp_keepalives_count = 3; SET idle_in_transaction_session_timeout = '5min'; \
         SET synchronous_commit = on;",
    );
    Pool::connect(
        url,
        crate::pg::Options {
            // Opening a connection (TCP, TLS, SCRAM, the SETs) costs ~10 ms,
            // so keep enough open for DuckDB's bursts.
            min: pool_min().min(pool_max()),
            // The budget the cluster really has, not its ceiling: 64 covers a
            // DuckDB worker's burst (measured wait-free), and the same default
            // must fit a small shared Postgres alongside DuckLake.
            // `PGVS3_POOL_MAX=256` where the cluster allows it.
            max: pool_max(),
            session,
            range_sql: GET_RANGE_SQL,
            range_types: &[
                Type::INT8,
                Type::INT4,
                Type::INT4,
                Type::TEXT,
                Type::TEXT,
                Type::INT8,
            ],
        },
    )
    .await
}

/// Storage layout this binary reads and writes (schema.sql); bumped only by
/// breaking layout changes.
pub const LAYOUT_VERSION: i32 = 7;

/// S3 ETag: the MD5, or for multipart the MD5 of the part MD5s plus "-count".
fn s3_etag(md5: &[u8], parts: Option<usize>) -> String {
    match parts {
        Some(n) => format!("{}-{n}", hex(md5)),
        None => hex(md5),
    }
}

/// Multipart checksum algorithms (S3 composite); stored as S3 spells them.
pub const CHECKSUM_ALGORITHMS: [&str; 5] = ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"];

/// Create the layout if absent, and fail closed on any other version rather
/// than misread it. Serialized across gateways by an advisory lock, so
/// concurrent first starts do not race the DDL.
pub async fn init(pool: &Pool) -> Result<()> {
    const LOCK: i64 = 0x7067_7673; // "pgvs"
    let mut conn = pool.get().await?;
    conn.query_typed("SELECT pg_advisory_lock($1)", &[(&LOCK, Type::INT8)])
        .await?;
    let result = init_locked(&mut conn).await;
    let _ = conn
        .query_typed("SELECT pg_advisory_unlock($1)", &[(&LOCK, Type::INT8)])
        .await;
    result
}

async fn init_locked(client: &mut Client) -> Result<()> {
    let version: i32 = client
        .query_one("SHOW server_version_num", &[])
        .await?
        .try_get::<_, String>(0)?
        .parse()?;
    anyhow::ensure!(version >= 180000, "pgvs3 requires PostgreSQL 18 or later");
    let tx = client.transaction().await?;
    let pgvfs: bool = tx
        .query_typed_one("SELECT to_regclass('pgvfs.chunks') IS NOT NULL", &[])
        .await?
        .try_get(0)?;
    anyhow::ensure!(
        !pgvfs,
        "this database holds the pgvfs DuckDB filesystem layout; the S3 gateway needs its own database"
    );
    let chunks: bool = tx
        .query_typed_one("SELECT to_regclass('s3p.chunks') IS NOT NULL", &[])
        .await?
        .try_get(0)?;
    if chunks {
        let marked: bool = tx
            .query_typed_one("SELECT to_regclass('s3p.layout') IS NOT NULL", &[])
            .await?
            .try_get(0)?;
        anyhow::ensure!(
            marked,
            "s3p.layout is missing: refusing to guess the storage layout"
        );
        let found: Option<i32> = tx
            .query_typed_one("SELECT max(version) FROM s3p.layout", &[])
            .await?
            .try_get(0)?;
        anyhow::ensure!(
            found == Some(LAYOUT_VERSION),
            "s3p holds storage layout {found:?}; this pgvs3 reads v{LAYOUT_VERSION} \
             (migrate the data explicitly, or point it at a fresh database)"
        );
        tx.commit().await?;
        return Ok(());
    } else {
        let marked: bool = tx
            .query_typed_one("SELECT to_regclass('s3p.layout') IS NOT NULL", &[])
            .await?
            .try_get(0)?;
        anyhow::ensure!(
            !marked,
            "s3p.layout exists without chunks: refusing a partial layout"
        );
    }
    tx.batch_execute(SCHEMA).await?;
    tx.query_typed(
        "INSERT INTO s3p.layout (version) SELECT $1 WHERE NOT EXISTS (SELECT 1 FROM s3p.layout)",
        &[(&LAYOUT_VERSION, Type::INT4)],
    )
    .await?;
    tx.query_typed_one(
        "SELECT cron.schedule($1, $2, $3)",
        &[
            (&"pgvs3-maintain", Type::TEXT),
            (&"* * * * *", Type::TEXT),
            (&"SELECT s3p.maintain()", Type::TEXT),
        ],
    )
    .await
    .context("scheduling pgvs3 maintenance on the new layout")?;
    tx.commit().await?;
    Ok(())
}

/// A serving gateway must not accumulate abandoned multipart bytes silently.
/// The schema is installed by `init`; pg_cron is a database prerequisite and
/// this check verifies the cleanup job before listening. Startup never alters
/// an existing schema or cron job; the first init schedules it transactionally.
pub async fn ensure_maintenance(pool: &Pool) -> Result<()> {
    let conn = pool.get().await?;
    let cron_installed: bool = conn
        .query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron')",
            &[],
        )
        .await?
        .try_get(0)?;
    anyhow::ensure!(
        cron_installed,
        "pg_cron is required for multipart cleanup; preload it and CREATE EXTENSION pg_cron in this database before serving"
    );
    let row = conn
        .query_typed_one(
            "SELECT current_database(), current_setting('cron.database_name', true)",
            &[],
        )
        .await?;
    let database: String = row.try_get(0)?;
    let cron_database: Option<String> = row.try_get(1)?;
    anyhow::ensure!(
        cron_database.as_deref() == Some(database.as_str()),
        "pg_cron must have cron.database_name={database} to run multipart cleanup"
    );
    let partitions: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM pg_partition_tree('s3p.chunks') p \
             JOIN pg_class c ON c.oid = p.relid \
             WHERE p.isleaf AND c.reloptions @> \
               ARRAY['toast_tuple_target=8160', 'autovacuum_vacuum_scale_factor=0.01', \
                     'autovacuum_analyze_scale_factor=0.02', 'autovacuum_vacuum_threshold=1000']",
            &[],
        )
        .await?
        .try_get(0)?;
    anyhow::ensure!(
        partitions == 32,
        "s3p.chunks must have 32 partitions with the expected inline storage and autovacuum settings (found {partitions})"
    );
    for relation in [
        "s3p.garbage",
        "s3p.objects_parts_idx",
        "s3p.upload_parts_file_id_key",
    ] {
        let exists: bool = conn
            .query_typed_one(
                "SELECT to_regclass($1) IS NOT NULL",
                &[(&relation, Type::TEXT)],
            )
            .await?
            .try_get(0)?;
        anyhow::ensure!(exists, "required layout relation {relation} is missing");
    }
    let jobs: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM cron.job WHERE jobname = 'pgvs3-maintain' \
             AND database = current_database() AND username = current_user \
              AND schedule = '* * * * *' AND command = 'SELECT s3p.maintain()' AND active",
            &[],
        )
        .await
        .context("checking pg_cron multipart cleanup job")?
        .try_get(0)?;
    anyhow::ensure!(jobs == 1, "pg_cron maintenance job is not active");
    Ok(())
}

/// Warm connections per gateway (PGVS3_POOL_MIN, default 64: measured
/// wait-free for one DuckDB worker's bursts). Every gateway holds this many
/// backends open, so large fleets should lower it. Clamped to the max.
fn pool_min() -> usize {
    std::env::var("PGVS3_POOL_MIN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

/// Connections open at once (PGVS3_POOL_MAX, default 64). Sized to the
/// cluster's shared budget — the pool is one of several clients, and a fleet
/// of gateways multiplies this — not to the server's ceiling.
fn pool_max() -> usize {
    std::env::var("PGVS3_POOL_MAX")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64)
}

/// Everything but the body of a served range: `[start, end]` inclusive.
pub struct SliceMeta {
    pub size: i64,
    pub sha256: Vec<u8>,
    pub etag: String,
    pub user_metadata: Vec<String>,
    pub content_type: String,
    pub created_at: SystemTime,
    pub file_id: i64,
    pub start: i64,
    pub end: i64,
}

impl SliceMeta {
    pub fn len(&self) -> i64 {
        (self.end - self.start + 1).max(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A cut-through response body: the first chunk (fetched before the response
/// started), then the rest in `no` order as the span task forwards it.
pub struct PieceStream {
    first: Option<Bytes>,
    rx: tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>,
}

impl Stream for PieceStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.first.take() {
            Some(head) => std::task::Poll::Ready(Some(Ok(head))),
            None => this.rx.poll_recv(cx),
        }
    }
}

fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

// ---------------------------------------------------------------------------

/// Metadata lookup, cached (a repeat open costs no round trip).
pub async fn meta(pool: &Pool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    if let Some(m) = meta_get(bucket, key) {
        return Ok(Some(m));
    }
    meta_fresh(pool, bucket, key).await
}

/// Current metadata for operations such as HEAD that cannot serve a stale
/// size, ETag or existence result after another gateway modifies the key.
pub async fn meta_fresh(pool: &Pool, bucket: &str, key: &str) -> Result<Option<Meta>> {
    let row = pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT file_id, size, sha256, EXTRACT(EPOCH FROM created_at)::float8, parts, part_ends, \
                    etag, user_metadata, content_type \
             FROM s3p.objects WHERE bucket = $1 AND key = $2",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let Some(r) = row else {
        meta_invalidate(bucket, key);
        return Ok(None);
    };
    let meta = Meta {
        file_id: r.try_get(0)?,
        size: r.try_get(1)?,
        sha256: r.try_get(2)?,
        etag: r.try_get(6)?,
        user_metadata: r.try_get(7)?,
        content_type: r.try_get(8)?,
        created_at: epoch(r.try_get(3)?),
        parts: r.try_get(4)?,
        part_ends: r.try_get(5)?,
    };
    meta_put(bucket, key, meta.clone());
    Ok(Some(meta))
}

/// Serve `[start, end]` of an object, cut through: rows go to the response as
/// they arrive from PostgreSQL (`stream_span`), not after the whole span (on
/// an 8 MiB GET that was a serial ~3 ms, a 0.5 ms assembly copy plus the
/// loopback send, after a 12 ms fetch). The response starts only once the
/// first chunk exists, so a failure before any byte is a clean S3 error; a
/// span that fits in the first chunk goes out as one buffer.
pub async fn get_body(
    pool: Pool,
    bucket: String,
    key: String,
    first: i64,
    last: i64,
    suffix: i64,
) -> Result<Option<(SliceMeta, PieceBody)>> {
    // A stale meta-cache entry (replaced through another gateway) cannot
    // serve queued old bytes: the range query checks the current object file
    // ID, so it produces missing rows. A stale size can also give a bogus 416.
    // Before any byte has gone out, retry once with fresh metadata.
    for attempt in 0..2 {
        let Some(m) = meta(&pool, &bucket, &key).await? else {
            return Ok(None);
        };
        let (start, end) = eff_range(m.size, first, last, suffix);
        if m.size > 0 && (start > end || start >= m.size) && attempt == 0 {
            meta_invalidate(&bucket, &key);
            continue;
        }
        let smeta = SliceMeta {
            size: m.size,
            sha256: m.sha256.clone(),
            etag: m.etag.clone(),
            user_metadata: m.user_metadata.clone(),
            content_type: m.content_type.clone(),
            created_at: m.created_at,
            file_id: m.file_id,
            start,
            end,
        };
        let len = smeta.len() as usize;
        if len == 0 && m.size == 0 && attempt == 0 {
            // No rows can reveal a zero-byte object replaced on another gateway.
            meta_fresh(&pool, &bucket, &key).await?;
            continue;
        }
        if len == 0 {
            return Ok(Some((smeta, PieceBody::OneShot(Bytes::new()))));
        }
        // ~8 MiB of chunks queue for the response; parts buffer their own rows.
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(32);
        let pool_c = pool.clone();
        let bucket_c = bucket.clone();
        let key_c = key.clone();
        tokio::spawn(async move {
            if let Err(e) = stream_span(&pool_c, &bucket_c, &key_c, &m, start, end, &tx).await {
                let _ = tx.send(Err(io_err(e))).await;
            }
        });
        let head = rx.recv().await;
        match head {
            Some(Ok(head)) if head.len() == len => {
                return Ok(Some((smeta, PieceBody::OneShot(head))))
            }
            Some(Ok(head)) => {
                return Ok(Some((
                    smeta,
                    PieceBody::Streamed(PieceStream {
                        first: Some(head),
                        rx,
                    }),
                )))
            }
            Some(Err(e)) => {
                meta_invalidate(&bucket, &key);
                if attempt == 1 {
                    return Err(e.into());
                }
            }
            None => {
                return Err(anyhow::anyhow!(
                    "GET of {bucket}/{key} ended before any data"
                ))
            }
        }
    }
    unreachable!("get_body returns on every attempt")
}

/// Response body for a served range: one buffer when the span fits in the
/// first chunk, else the cut-through stream.
pub enum PieceBody {
    OneShot(Bytes),
    Streamed(PieceStream),
}

/// One fetch unit: rows `[lo, hi]` of segment file `file_id`, which starts
/// at object byte offset `base`.
#[derive(Clone, Copy)]
struct Piece {
    file_id: i64,
    base: i64,
    lo: i32,
    hi: i32,
}

/// Pieces covering object bytes `[start, end]` across the object's segments
/// (one file, or its part files), in order, at most `step` rows each.
fn plan(m: &Meta, start: i64, end: i64, step: usize) -> Result<Vec<Piece>> {
    let mut out = Vec::new();
    for (file_id, base, len) in m.segments()? {
        if len <= 0 || base + len - 1 < start || base > end {
            continue;
        }
        let lo = ((start.max(base) - base) / ROW_BYTES) as i32;
        let hi = ((end.min(base + len - 1) - base) / ROW_BYTES) as i32;
        out.extend(part_ranges(lo, hi, step).map(|(lo, hi)| Piece {
            file_id,
            base,
            lo,
            hi,
        }));
    }
    Ok(out)
}

/// The part of row `no` of a segment at `base` inside object bytes `[start, end]`.
fn row_slice(base: i64, no: i32, data: &[u8], start: i64, end: i64) -> Option<&[u8]> {
    let row_start = base + i64::from(no) * ROW_BYTES;
    let lo = start.max(row_start) - row_start;
    let hi = end.min(row_start + data.len() as i64 - 1) - row_start;
    (hi >= lo).then(|| &data[lo as usize..=hi as usize])
}

/// Contiguous `[lo, hi]` ranges of at most `step` rows covering the span.
fn part_ranges(first_row: i32, last_row: i32, step: usize) -> impl Iterator<Item = (i32, i32)> {
    let step = step.clamp(1, i32::MAX as usize);
    (first_row..=last_row)
        .step_by(step)
        .map(move |lo| (lo, lo.saturating_add(step as i32 - 1).min(last_row)))
}

/// Rows of `[start, end]` in `no` order, forwarded in ~CHUNK pieces. A single
/// query has one statement snapshot. Multiple queries share a repeatable-read
/// transaction, so concurrent overwrites cannot remove later rows mid-GET.
async fn stream_span(
    pool: &Pool,
    bucket: &str,
    key: &str,
    m: &Meta,
    start: i64,
    end: i64,
    tx: &tokio::sync::mpsc::Sender<Result<Bytes, std::io::Error>>,
) -> Result<()> {
    let pieces = plan(m, start, end, PART_ROWS)?;
    let mut snapshot = (pieces.len() > 1)
        .then(|| spawn_snapshot_span(pool, bucket, key, m.file_id, pieces.clone()));
    let mut chunk = BytesMut::with_capacity(CHUNK + ROW_BYTES as usize);
    let mut sent = 0i64;
    for p in pieces {
        let mut single = None;
        let rows = match &mut snapshot {
            Some(rows) => rows,
            None => single.get_or_insert_with(|| spawn_part(pool, bucket, key, m.file_id, p)),
        };
        // Bitmap heap scans return TID order: hold any row that runs ahead.
        let mut early: BTreeMap<i32, Row> = BTreeMap::new();
        let mut next = p.lo;
        while let Some(row) = rows.recv().await {
            let Some(row) = row? else { break };
            let no: i32 = row.try_get(0)?;
            if no != next {
                early.insert(no, row);
                continue;
            }
            put_row(&mut chunk, &p, no, &row, start, end)?;
            next += 1;
            while let Some(row) = early.remove(&next) {
                put_row(&mut chunk, &p, next, &row, start, end)?;
                next += 1;
            }
            if chunk.len() >= CHUNK {
                sent += chunk.len() as i64;
                if tx.send(Ok(chunk.split().freeze())).await.is_err() {
                    return Ok(()); // the client went away
                }
                chunk.reserve(CHUNK + ROW_BYTES as usize);
            }
        }
        anyhow::ensure!(
            next > p.hi,
            "file {} rows {}..={}: row {next} missing",
            p.file_id,
            p.lo,
            p.hi
        );
    }
    anyhow::ensure!(
        sent + chunk.len() as i64 == end - start + 1,
        "short read: {} of {} bytes",
        sent + chunk.len() as i64,
        end - start + 1
    );
    if !chunk.is_empty() {
        let _ = tx.send(Ok(chunk.freeze())).await;
    }
    Ok(())
}

/// Append row `no`'s part of `[start, end]` to `chunk`.
fn put_row(
    chunk: &mut BytesMut,
    p: &Piece,
    no: i32,
    row: &Row,
    start: i64,
    end: i64,
) -> Result<()> {
    if let Some(s) = row_slice(p.base, no, row.try_get(1)?, start, end) {
        chunk.extend_from_slice(s);
    }
    Ok(())
}

/// A one-query GET streams without opening an explicit transaction.
fn spawn_part(
    pool: &Pool,
    bucket: &str,
    key: &str,
    root_id: i64,
    p: Piece,
) -> tokio::sync::mpsc::Receiver<Result<Option<Row>>> {
    let (tx, rx) = tokio::sync::mpsc::channel((p.hi - p.lo + 2) as usize);
    let pool = pool.clone();
    let bucket = bucket.to_owned();
    let key = key.to_owned();
    tokio::spawn(async move {
        if let Err(e) = fetch_part(&pool, &bucket, &key, root_id, p, &tx).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

/// A multi-query GET holds one snapshot until its last row is read. The
/// bounded channel backpressures PostgreSQL when the HTTP client is slow.
fn spawn_snapshot_span(
    pool: &Pool,
    bucket: &str,
    key: &str,
    root_id: i64,
    pieces: Vec<Piece>,
) -> tokio::sync::mpsc::Receiver<Result<Option<Row>>> {
    let (tx, rx) = tokio::sync::mpsc::channel(64);
    let pool = pool.clone();
    let bucket = bucket.to_owned();
    let key = key.to_owned();
    tokio::spawn(async move {
        if let Err(e) = fetch_snapshot_span(&pool, &bucket, &key, root_id, pieces, &tx).await {
            let _ = tx.send(Err(e)).await;
        }
    });
    rx
}

async fn fetch_snapshot_span(
    pool: &Pool,
    bucket: &str,
    key: &str,
    root_id: i64,
    pieces: Vec<Piece>,
    tx: &tokio::sync::mpsc::Sender<Result<Option<Row>>>,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let range = conn.range().await?.clone();
    let snapshot = conn
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    for p in pieces {
        let params: [&(dyn ToSql + Sync); 6] = [&p.file_id, &p.lo, &p.hi, &bucket, &key, &root_id];
        let mut rows = std::pin::pin!(snapshot.query_raw(&range, params).await?);
        while let Some(row) = rows.try_next().await? {
            if tx.send(Ok(Some(row))).await.is_err() {
                return Ok(());
            }
        }
        if tx.send(Ok(None)).await.is_err() {
            return Ok(());
        }
    }
    snapshot.commit().await?;
    Ok(())
}

/// One contiguous row-range query (the prepared GET_RANGE_SQL) on its own
/// pool connection; rows go to `tx` as they arrive. Each keeps its bytes in
/// the connection's receive buffer until copied into a response chunk.
async fn fetch_part(
    pool: &Pool,
    bucket: &str,
    key: &str,
    root_id: i64,
    p: Piece,
    tx: &tokio::sync::mpsc::Sender<Result<Option<Row>>>,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let params: [&(dyn ToSql + Sync); 6] = [&p.file_id, &p.lo, &p.hi, &bucket, &key, &root_id];
    let range = conn.range().await?.clone();
    let mut rows = std::pin::pin!(conn.query_raw(&range, params).await?);
    while let Some(row) = rows.try_next().await? {
        if tx.send(Ok(Some(row))).await.is_err() {
            break; // the span was abandoned
        }
    }
    Ok(())
}

/// Effective `[start, end]` inclusive for a request (mirrors the old SQL clamp).
fn eff_range(size: i64, first: i64, last: i64, suffix: i64) -> (i64, i64) {
    if suffix >= 0 {
        ((size - suffix).max(0), size - 1)
    } else {
        (
            first.max(0),
            if last >= 0 {
                last.min(size - 1)
            } else {
                size - 1
            },
        )
    }
}

/// Buffered variant for contract tests.
pub async fn get(
    pool: &Pool,
    bucket: &str,
    key: &str,
    first: i64,
    last: i64,
    suffix: i64,
) -> Result<Option<Slice>> {
    let Some((meta, body)) = get_body(
        pool.clone(),
        bucket.to_owned(),
        key.to_owned(),
        first,
        last,
        suffix,
    )
    .await?
    else {
        return Ok(None);
    };
    let bytes = match body {
        PieceBody::OneShot(b) => b,
        PieceBody::Streamed(mut s) => {
            let mut out = BytesMut::with_capacity(meta.len() as usize);
            while let Some(piece) = s.next().await {
                let piece = piece?;
                out.extend_from_slice(&piece);
            }
            out.freeze()
        }
    };
    Ok(Some(Slice {
        size: meta.size,
        sha256: meta.sha256,
        created_at: meta.created_at,
        start: meta.start,
        end: meta.end,
        bytes,
    }))
}

/// A served byte range, fully buffered.
pub struct Slice {
    pub size: i64,
    pub sha256: Vec<u8>,
    pub created_at: SystemTime,
    pub start: i64,
    pub end: i64,
    pub bytes: Bytes,
}

/// Overwrite-or-create a buffered object through the same atomic write path
/// as the S3 PUT handler. The writer hashes and publishes in its COPY
/// transaction; failed writes leave no committed chunk rows behind.
pub async fn put(pool: &Pool, bucket: &str, key: &str, data: &[u8]) -> Result<()> {
    let writer = ChunkWriter::start_object(pool.clone(), bucket.to_owned(), key.to_owned());
    for chunk in data.chunks(SEND_BATCH) {
        writer.push(Bytes::copy_from_slice(chunk)).await?;
    }
    writer.finish(Vec::new()).await?;
    Ok(())
}

enum WriterTarget {
    Object {
        bucket: String,
        key: String,
        condition: PutCondition,
        user_metadata: Vec<String>,
        content_type: String,
    },
    Part {
        upload_id: String,
        bucket: String,
        key: String,
        part_no: i32,
    },
}

pub enum PutCondition {
    Unconditional,
    IfAbsent,
    IfMatch(String),
}

#[derive(Debug)]
pub struct PreconditionFailed;

impl std::fmt::Display for PreconditionFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("object precondition failed")
    }
}

impl std::error::Error for PreconditionFailed {}

#[derive(Debug)]
pub enum MissingResource {
    Bucket,
    Upload,
}

impl std::fmt::Display for MissingResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} does not exist", self)
    }
}

impl std::error::Error for MissingResource {}

/// A part of a checksummed multipart upload arrived without that checksum.
#[derive(Debug)]
pub struct MissingChecksum;

impl std::fmt::Display for MissingChecksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("part lacks the upload's checksum")
    }
}

impl std::error::Error for MissingChecksum {}

/// Streaming ingest: bytes flow into one open binary COPY stream through
/// `push` (rows cut across pushes through a single cursor). Every ingest has
/// its own connection and COPY, so PUTs and multipart parts write in parallel.
pub struct ChunkWriter {
    tx: Option<tokio::sync::mpsc::Sender<IngestMsg>>,
    done: Option<tokio::task::JoinHandle<IngestResult>>,
}

impl ChunkWriter {
    pub fn start_object(pool: Pool, bucket: String, key: String) -> Self {
        Self::start_object_if(
            pool,
            bucket,
            key,
            PutCondition::Unconditional,
            Vec::new(),
            "application/octet-stream".to_owned(),
        )
    }

    pub fn start_object_if(
        pool: Pool,
        bucket: String,
        key: String,
        condition: PutCondition,
        user_metadata: Vec<String>,
        content_type: String,
    ) -> Self {
        Self::begin(
            pool,
            WriterTarget::Object {
                bucket,
                key,
                condition,
                user_metadata,
                content_type,
            },
        )
    }

    /// A multipart part: its rows and its `upload_parts` record commit in one
    /// transaction, so a part is either fully recorded or absent.
    pub fn start_part(
        pool: Pool,
        upload_id: String,
        bucket: String,
        key: String,
        part_no: i32,
    ) -> Self {
        Self::begin(
            pool,
            WriterTarget::Part {
                upload_id,
                bucket,
                key,
                part_no,
            },
        )
    }

    fn begin(pool: Pool, target: WriterTarget) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel::<IngestMsg>(8);
        let done = tokio::spawn(ingest_writer(pool, target, rx));
        Self {
            tx: Some(tx),
            done: Some(done),
        }
    }

    pub async fn push(&self, chunk: Bytes) -> Result<()> {
        self.tx
            .as_ref()
            .expect("writer open")
            .send(IngestMsg::Data(chunk))
            .await
            .map_err(|_| anyhow::anyhow!("ingest writer gone"))
    }

    /// Only an explicit Finish may publish a COPY; dropping a request rolls it back.
    /// `checksums` are the request's verified `(x-amz-checksum-*, value)` pairs.
    pub async fn finish(mut self, checksums: Vec<(String, String)>) -> Result<Published> {
        let send = self
            .tx
            .take()
            .expect("writer open")
            .send(IngestMsg::Finish(checksums))
            .await;
        // If the reader closed first, join it to report the original database
        // error (e.g. NoSuchBucket), not a generic channel failure.
        if send.is_err() {
            return self.done.take().expect("writer joined").await?;
        }
        self.done.take().expect("writer joined").await?
    }

    /// Roll the ingest back (drops the COPY transaction; no rows are visible
    /// since the object row never published).
    pub async fn abort(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(IngestMsg::Abort).await;
        }
        if let Some(h) = self.done.take() {
            let _ = h.await;
        }
    }
}

async fn ingest_writer(
    pool: Pool,
    target: WriterTarget,
    mut rx: tokio::sync::mpsc::Receiver<IngestMsg>,
) -> IngestResult {
    // No global write lock: each ingest owns a connection and a COPY, and the
    // bounded channel backpressures the request body at COPY speed.
    let t0 = std::time::Instant::now();

    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let algorithm: Option<String> = match &target {
        WriterTarget::Object { bucket, .. } => {
            if tx
                .query_typed_opt(
                    "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR KEY SHARE",
                    &[(&bucket, Type::TEXT)],
                )
                .await?
                .is_none()
            {
                return Err(MissingResource::Bucket.into());
            }
            None
        }
        WriterTarget::Part {
            upload_id,
            bucket,
            key,
            ..
        } => {
            let Some(row) = tx.query_typed_opt(
                "SELECT checksum_algorithm FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR KEY SHARE",
                &[(&upload_id, Type::TEXT), (&bucket, Type::TEXT), (&key, Type::TEXT)],
            )
            .await? else {
                return Err(MissingResource::Upload.into());
            };
            row.try_get(0)?
        }
    };
    let file_id: i64 = tx
        .query_typed_one(
            "SELECT nextval(pg_get_serial_sequence('s3p.objects', 'file_id'))",
            &[],
        )
        .await?
        .try_get(0)?;
    let mut sink = std::pin::pin!(tx.copy_in::<_, Bytes>(COPY_SQL).await?);
    sink.send(Bytes::from_static(COPY_HEADER)).await?;

    let mut framer = RowFramer::new(file_id);
    let checksums = loop {
        let msg = rx.recv().await.context("upload ended without Finish")?;
        match msg {
            IngestMsg::Data(b) => {
                if let Some(buf) = framer.push(&b)? {
                    sink.send(buf).await?;
                }
            }
            IngestMsg::Finish(checksums) => break checksums,
            IngestMsg::Abort => anyhow::bail!("ingest aborted"),
        }
    };
    // A part of a checksummed upload must carry that checksum (already verified).
    let checksum = match algorithm {
        None => None,
        Some(algorithm) => {
            let header = format!("x-amz-checksum-{}", algorithm.to_ascii_lowercase());
            let Some((_, value)) = checksums.into_iter().find(|(name, _)| *name == header) else {
                return Err(MissingChecksum.into());
            };
            Some((algorithm, value))
        }
    };
    let (last, (total, digests)) = framer.finish();
    let etag = s3_etag(&digests.md5, None);
    sink.send(last).await?;
    sink.as_mut().finish().await?;
    match &target {
        WriterTarget::Object {
            bucket,
            key,
            condition,
            user_metadata,
            content_type,
        } => {
            swap_object(
                &tx,
                bucket,
                key,
                ObjectWrite {
                    file_id,
                    size: total,
                    sha256: &digests.sha256,
                    etag: &etag,
                    user_metadata,
                    content_type,
                    parts: None,
                    condition,
                },
            )
            .await?;
        }
        WriterTarget::Part {
            upload_id, part_no, ..
        } => {
            let value = checksum.as_ref().map(|(_, value)| value.as_str());
            commit_part(&tx, upload_id, *part_no, file_id, total, &digests, value).await?;
        }
    }
    tx.commit().await?;
    if let WriterTarget::Object {
        bucket,
        key,
        user_metadata,
        content_type,
        ..
    } = target
    {
        meta_put(
            &bucket,
            &key,
            Meta {
                size: total,
                sha256: digests.sha256.clone(),
                etag: etag.clone(),
                user_metadata,
                content_type,
                created_at: SystemTime::now(),
                file_id,
                parts: None,
                part_ends: None,
            },
        );
    }
    eprintln!(
        "pgvs3: ingest {:.1} MiB at {:.0} MiB/s",
        total as f64 / 1024.0 / 1024.0,
        total as f64 / 1024.0 / 1024.0 / t0.elapsed().as_secs_f64().max(1e-9)
    );
    Ok(Published {
        size: total,
        etag,
        checksum,
    })
}

/// Record a multipart part in the same transaction as its rows; a re-sent
/// part replaces the earlier attempt, rows included. Unpublished rows are
/// invisible: objects appear atomically at publish / Complete.
async fn commit_part(
    tx: &Transaction<'_>,
    upload_id: &str,
    part_no: i32,
    file_id: i64,
    total: i64,
    digests: &Digests,
    checksum: Option<&str>,
) -> Result<()> {
    let old = tx
        .query_typed_opt(
            "DELETE FROM s3p.upload_parts WHERE upload_id = $1 AND part_no = $2 RETURNING file_id",
            &[(&upload_id, Type::TEXT), (&part_no, Type::INT4)],
        )
        .await?;
    if let Some(old) = old {
        let old: i64 = old.try_get(0)?;
        tx.query_typed(
            "INSERT INTO s3p.garbage (file_id) VALUES ($1) ON CONFLICT DO NOTHING",
            &[(&old, Type::INT8)],
        )
        .await?;
    }
    tx.query_typed(
        "INSERT INTO s3p.upload_parts (upload_id, part_no, file_id, size, sha256, md5, checksum) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
        &[
            (&upload_id, Type::TEXT),
            (&part_no, Type::INT4),
            (&file_id, Type::INT8),
            (&total, Type::INT8),
            (&digests.sha256, Type::BYTEA),
            (&digests.md5, Type::BYTEA),
            (&checksum, Type::TEXT),
        ],
    )
    .await?;
    Ok(())
}

/// Point (bucket, key) at new storage inside `tx` and queue the storage of any
/// object it replaces (all of its files: the single file or every part).
struct ObjectWrite<'a> {
    file_id: i64,
    size: i64,
    sha256: &'a [u8],
    etag: &'a str,
    user_metadata: &'a [String],
    content_type: &'a str,
    parts: Option<(&'a [i64], &'a [i64])>,
    condition: &'a PutCondition,
}

async fn swap_object(
    tx: &Transaction<'_>,
    bucket: &str,
    key: &str,
    write: ObjectWrite<'_>,
) -> Result<()> {
    lock_object_key(tx, bucket, key).await?;
    let old = tx
        .query_typed_opt(
            "SELECT file_id, parts, etag FROM s3p.objects WHERE bucket = $1 AND key = $2 FOR UPDATE",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let allowed = match write.condition {
        PutCondition::Unconditional => true,
        PutCondition::IfAbsent => old.is_none(),
        PutCondition::IfMatch(want) => match &old {
            Some(row) => row.try_get::<_, String>(2)? == *want,
            None => false,
        },
    };
    if !allowed {
        return Err(PreconditionFailed.into());
    }
    let (ids, ends) = write.parts.unzip();
    tx.query_typed(
        "INSERT INTO s3p.objects (bucket, key, file_id, size, sha256, etag, user_metadata, content_type, parts, part_ends) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) \
         ON CONFLICT (bucket, key) DO UPDATE SET file_id = EXCLUDED.file_id, size = EXCLUDED.size, \
           sha256 = EXCLUDED.sha256, etag = EXCLUDED.etag, \
           user_metadata = EXCLUDED.user_metadata, content_type = EXCLUDED.content_type, \
           parts = EXCLUDED.parts, part_ends = EXCLUDED.part_ends, created_at = now()",
        &[
            (&bucket, Type::TEXT),
            (&key, Type::TEXT),
            (&write.file_id, Type::INT8),
            (&write.size, Type::INT8),
            (&write.sha256, Type::BYTEA),
            (&write.etag, Type::TEXT),
            (&write.user_metadata, Type::TEXT_ARRAY),
            (&write.content_type, Type::TEXT),
            (&ids, Type::INT8_ARRAY),
            (&ends, Type::INT8_ARRAY),
        ],
    )
    .await?;
    if let Some(r) = old {
        queue_files(tx, r.try_get(0)?, r.try_get(1)?).await?;
    }
    Ok(())
}

// An absent row cannot be locked with FOR UPDATE. Mutations of the same key
// must serialize before looking up the old file or checking a precondition.
async fn lock_object_key(tx: &Transaction<'_>, bucket: &str, key: &str) -> Result<()> {
    tx.query_typed(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0) # hashtextextended($2, 1))",
        &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
    )
    .await?;
    Ok(())
}

/// Atomically remove a file's final reference and enqueue its chunks for GC.
async fn queue_files(tx: &Transaction<'_>, file_id: i64, parts: Option<Vec<i64>>) -> Result<()> {
    let mut dead = parts.unwrap_or_default();
    dead.push(file_id);
    tx.query_typed(
        "INSERT INTO s3p.garbage (file_id) SELECT DISTINCT unnest($1::int8[]) ON CONFLICT DO NOTHING",
        &[(&dead, Type::INT8_ARRAY)],
    )
    .await?;
    Ok(())
}

pub async fn delete(pool: &Pool, bucket: &str, key: &str) -> Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    lock_object_key(&tx, bucket, key).await?;
    let old = tx
        .query_typed_opt(
            "DELETE FROM s3p.objects WHERE bucket = $1 AND key = $2 RETURNING file_id, parts",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    let found = old.is_some();
    if let Some(r) = old {
        queue_files(&tx, r.try_get(0)?, r.try_get(1)?).await?;
    }
    tx.commit().await?;
    meta_invalidate(bucket, key);
    Ok(found)
}

/// Idempotent: creating an existing bucket succeeds (S3 BucketAlreadyOwnedByYou).
pub async fn create_bucket(pool: &Pool, name: &str) -> Result<()> {
    pool.get()
        .await?
        .query_typed(
            "INSERT INTO s3p.buckets (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[(&name, Type::TEXT)],
        )
        .await?;
    Ok(())
}

pub async fn bucket_exists(pool: &Pool, name: &str) -> Result<bool> {
    Ok(pool
        .get()
        .await?
        .query_typed_opt(
            "SELECT 1 FROM s3p.buckets WHERE name = $1",
            &[(&name, Type::TEXT)],
        )
        .await?
        .is_some())
}

pub enum BucketDeletion {
    Deleted,
    NotEmpty,
    NotFound,
}

pub async fn delete_bucket(pool: &Pool, name: &str) -> Result<BucketDeletion> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if tx
        .query_typed_opt(
            "SELECT 1 FROM s3p.buckets WHERE name = $1 FOR UPDATE",
            &[(&name, Type::TEXT)],
        )
        .await?
        .is_none()
    {
        return Ok(BucketDeletion::NotFound);
    }
    let occupied: bool = tx
        .query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM s3p.objects WHERE bucket = $1) \
                    OR EXISTS (SELECT 1 FROM s3p.uploads WHERE bucket = $1)",
            &[(&name, Type::TEXT)],
        )
        .await?
        .try_get(0)?;
    if occupied {
        return Ok(BucketDeletion::NotEmpty);
    }
    tx.query_typed(
        "DELETE FROM s3p.buckets WHERE name = $1",
        &[(&name, Type::TEXT)],
    )
    .await?;
    tx.commit().await?;
    Ok(BucketDeletion::Deleted)
}

/// Each Create has its own upload ID, even for the same key.
pub async fn create_upload(
    pool: &Pool,
    bucket: &str,
    key: &str,
    user_metadata: Vec<String>,
    content_type: String,
    checksum_algorithm: Option<&str>,
) -> Result<Option<String>> {
    let row = pool
        .get()
        .await?
        .query_typed_opt(
            "INSERT INTO s3p.uploads (upload_id, bucket, key, checksum_algorithm, user_metadata, content_type) \
             SELECT gen_random_uuid()::text, name, $2, $3, $4, $5 FROM s3p.buckets WHERE name = $1 \
             RETURNING upload_id",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT), (&checksum_algorithm, Type::TEXT),
              (&user_metadata, Type::TEXT_ARRAY), (&content_type, Type::TEXT)],
        )
        .await?;
    row.map(|r| r.try_get(0)).transpose().map_err(Into::into)
}

/// A part listed in CompleteMultipartUpload: number, ETag and any checksum
/// as `(algorithm, base64 value)`.
pub struct ListedPart {
    pub part_no: i32,
    pub etag: String,
    pub checksum: Option<(String, String)>,
}

pub enum Completed {
    Done {
        bucket: String,
        key: String,
        etag: String,
        size: i64,
    },
    InvalidPart,
    NoSuchUpload,
}

/// Complete: listed parts must be ascending with matching ETags and, for a
/// checksummed upload, matching part checksums. Unlisted uploaded parts are
/// discarded in the publication transaction. No selected data moves.
pub async fn complete_upload(
    pool: &Pool,
    upload_id: &str,
    request_bucket: &str,
    request_key: &str,
    listed: &[ListedPart],
) -> Result<Completed> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let Some(up) = tx
        .query_typed_opt(
            "SELECT bucket, key, checksum_algorithm, user_metadata, content_type \
             FROM s3p.uploads WHERE upload_id = $1 FOR UPDATE",
            &[(&upload_id, Type::TEXT)],
        )
        .await?
    else {
        return Ok(Completed::NoSuchUpload);
    };
    let (bucket, key): (String, String) = (up.try_get(0)?, up.try_get(1)?);
    let algorithm: Option<String> = up.try_get(2)?;
    let user_metadata: Vec<String> = up.try_get(3)?;
    let content_type: String = up.try_get(4)?;
    if bucket != request_bucket || key != request_key {
        return Ok(Completed::NoSuchUpload);
    }
    let rows = tx
        .query_typed(
            "SELECT part_no, file_id, size, sha256, md5, checksum FROM s3p.upload_parts \
             WHERE upload_id = $1 ORDER BY part_no",
            &[(&upload_id, Type::TEXT)],
        )
        .await?;
    if listed.is_empty()
        || listed.iter().any(|p| !(1..=10_000).contains(&p.part_no))
        || listed
            .windows(2)
            .any(|pair| pair[0].part_no >= pair[1].part_no)
    {
        return Ok(Completed::InvalidPart);
    }
    let mut want = listed.iter().peekable();
    let (mut ids, mut ends, mut size) = (
        Vec::with_capacity(listed.len()),
        Vec::with_capacity(listed.len()),
        0i64,
    );
    let mut unused = Vec::new();
    let mut hasher = Sha256::new();
    let mut md5_hasher = Md5::new();
    for r in &rows {
        let no: i32 = r.try_get(0)?;
        let id: i64 = r.try_get(1)?;
        if want.peek().is_none_or(|p| p.part_no != no) {
            unused.push(id);
            continue;
        }
        let p = want.next().expect("matched part");
        let sha: Vec<u8> = r.try_get(3)?;
        let md5: Vec<u8> = r.try_get(4)?;
        let stored: Option<String> = r.try_get(5)?;
        let checksum_ok = match (&algorithm, &p.checksum) {
            (None, None) => true,
            (Some(want), Some((given, value))) => {
                want == given && stored.as_deref() == Some(value.as_str())
            }
            _ => false,
        };
        if p.etag != hex(&md5) || !checksum_ok {
            return Ok(Completed::InvalidPart);
        }
        hasher.update(&sha);
        md5_hasher.update(&md5);
        size = size
            .checked_add(r.try_get::<_, i64>(2)?)
            .context("multipart object exceeds maximum size")?;
        ids.push(id);
        ends.push(size);
    }
    if want.next().is_some() {
        return Ok(Completed::InvalidPart);
    }
    let digests = Digests {
        sha256: hasher.finalize().to_vec(),
        md5: md5_hasher.finalize().to_vec(),
    };
    let etag = s3_etag(&digests.md5, Some(ids.len()));
    swap_object(
        &tx,
        &bucket,
        &key,
        ObjectWrite {
            file_id: ids[0],
            size,
            sha256: &digests.sha256,
            etag: &etag,
            user_metadata: &user_metadata,
            content_type: &content_type,
            parts: Some((&ids, &ends)),
            condition: &PutCondition::Unconditional,
        },
    )
    .await?;
    if !unused.is_empty() {
        tx.query_typed(
            "INSERT INTO s3p.garbage (file_id) SELECT unnest($1::int8[]) ON CONFLICT DO NOTHING",
            &[(&unused, Type::INT8_ARRAY)],
        )
        .await?;
    }
    tx.query_typed(
        "DELETE FROM s3p.uploads WHERE upload_id = $1",
        &[(&upload_id, Type::TEXT)],
    )
    .await?;
    tx.commit().await?;
    meta_put(
        &bucket,
        &key,
        Meta {
            size,
            sha256: digests.sha256,
            etag: etag.clone(),
            user_metadata,
            content_type,
            created_at: SystemTime::now(),
            file_id: ids[0],
            parts: Some(ids),
            part_ends: Some(ends),
        },
    );
    Ok(Completed::Done {
        bucket,
        key,
        etag,
        size,
    })
}

/// Drop a multipart upload and the rows of every part it recorded.
pub async fn abort_upload(pool: &Pool, upload_id: &str, bucket: &str, key: &str) -> Result<bool> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    if tx
        .query_typed_opt(
            "SELECT 1 FROM s3p.uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3 FOR UPDATE",
            &[
                (&upload_id, Type::TEXT),
                (&bucket, Type::TEXT),
                (&key, Type::TEXT),
            ],
        )
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let mut ids: Vec<i64> = Vec::new();
    for r in tx
        .query_typed(
            "SELECT file_id FROM s3p.upload_parts WHERE upload_id = $1",
            &[(&upload_id, Type::TEXT)],
        )
        .await?
    {
        ids.push(r.try_get(0)?);
    }
    tx.query_typed(
        "DELETE FROM s3p.uploads WHERE upload_id = $1",
        &[(&upload_id, Type::TEXT)],
    )
    .await?;
    tx.query_typed(
        "INSERT INTO s3p.garbage (file_id) SELECT unnest($1::int8[]) ON CONFLICT DO NOTHING",
        &[(&ids, Type::INT8_ARRAY)],
    )
    .await?;
    tx.commit().await?;
    Ok(true)
}

#[derive(Debug, Clone)]
pub struct Listed {
    pub key: String,
    pub size: i64,
    pub etag: String,
    pub created_at: SystemTime,
}

/// Key-ordered listing in `[prefix, prefix_end)` (byte order); `after` is
/// exclusive.
pub async fn list(
    pool: &Pool,
    bucket: &str,
    prefix: &str,
    prefix_end: &str,
    after: &str,
    limit: i64,
) -> Result<Vec<Listed>> {
    let mut conn = pool.get().await?;
    let tx = conn.build_transaction().read_only(true).start().await?;
    // Chunk ranges favor bitmap scans on cold pages, but ordered metadata
    // pages need the primary-key index. Scope this override to one listing.
    tx.batch_execute("SET LOCAL enable_indexscan = on").await?;
    let rows = tx
        .query_typed(
            "SELECT key, size, etag, EXTRACT(EPOCH FROM created_at)::float8 \
             FROM s3p.objects \
             WHERE bucket = $1 AND key COLLATE \"C\" >= $2 AND key COLLATE \"C\" < $3 \
               AND key COLLATE \"C\" > $4 \
             ORDER BY key COLLATE \"C\" LIMIT $5",
            &[
                (&bucket, Type::TEXT),
                (&prefix, Type::TEXT),
                (&prefix_end, Type::TEXT),
                (&after, Type::TEXT),
                (&limit, Type::INT8),
            ],
        )
        .await?;
    tx.commit().await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in &rows {
        out.push(Listed {
            key: r.try_get(0)?,
            size: r.try_get(1)?,
            etag: r.try_get(2)?,
            created_at: epoch(r.try_get(3)?),
        });
    }
    Ok(out)
}

pub async fn buckets(pool: &Pool) -> Result<Vec<(String, SystemTime)>> {
    let mut out = Vec::new();
    for r in pool
        .get()
        .await?
        .query_typed(
            "SELECT name, EXTRACT(EPOCH FROM created_at)::float8 \
             FROM s3p.buckets ORDER BY name",
            &[],
        )
        .await?
    {
        out.push((r.try_get(0)?, epoch(r.try_get(1)?)));
    }
    Ok(out)
}

/// `(logical_bytes, physical_bytes)` over objects + chunks (tables + indexes;
/// the chunk partitions, as the partitioned parent has no storage).
pub async fn sizes(pool: &Pool) -> Result<(i64, i64)> {
    let row = pool
        .get()
        .await?
        .query_typed_one(
            "SELECT (SELECT COALESCE(sum(size), 0)::int8 FROM s3p.objects), \
                    pg_total_relation_size('s3p.objects') \
                      + (SELECT COALESCE(sum(pg_total_relation_size(relid)), 0)::int8 \
                         FROM pg_partition_tree('s3p.chunks')), \
                    (SELECT count(*) FROM s3p.objects), \
                    (SELECT count(*) FROM s3p.chunks), \
                     pg_database_size(current_database()), \
                     (SELECT count(*) FROM s3p.garbage), \
                     (SELECT EXTRACT(EPOCH FROM now() - min(queued_at))::int8 FROM s3p.garbage)",
            &[],
        )
        .await?;
    let logical: i64 = row.try_get(0)?;
    let physical: i64 = row.try_get(1)?;
    println!(
        "objects={} chunks={} logical={} MiB physical={} MiB db={} MiB overhead={:.2}% garbage_files={} oldest_garbage_s={:?}",
        row.try_get::<_, i64>(2)?,
        row.try_get::<_, i64>(3)?,
        logical / 1024 / 1024,
        physical / 1024 / 1024,
        row.try_get::<_, i64>(4)? / 1024 / 1024,
        (physical as f64 / logical.max(1) as f64 - 1.0) * 100.0,
        row.try_get::<_, i64>(5)?,
        row.try_get::<_, Option<i64>>(6)?,
    );
    Ok((logical, physical))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path;
    use object_store::ObjectStoreExt;

    async fn last_file_id(pool: &Pool) -> Result<i64> {
        Ok(pool
            .get()
            .await?
            .query_typed_one("SELECT last_value FROM s3p.objects_file_id_seq", &[])
            .await?
            .try_get(0)?)
    }

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn starting_again_does_not_rewrite_existing_layout() -> Result<()> {
        let pool = connect(&std::env::var("PGVS3_TEST_DB_URL")?).await?;
        let sql =
            "SELECT xmin::text FROM pg_proc WHERE oid = 's3p.reap_garbage(integer)'::regprocedure";
        let before: String = pool
            .get()
            .await?
            .query_typed_one(sql, &[])
            .await?
            .try_get(0)?;
        init(&pool).await?;
        ensure_maintenance(&pool).await?;
        let after: String = pool
            .get()
            .await?
            .query_typed_one(sql, &[])
            .await?
            .try_get(0)?;
        anyhow::ensure!(before == after, "startup rewrote an existing SQL function");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn large_get_survives_overwrite_after_first_chunk() -> Result<()> {
        let pool = connect(&std::env::var("PGVS3_TEST_DB_URL")?).await?;
        let bucket = "pgvs3-contract";
        let key = format!("contract/snapshot-{}", std::process::id());
        let old = vec![0x5a; SMALL_MAX * 2 + 8120];
        let result: Result<()> = async {
            put(&pool, bucket, &key, &old).await?;
            let old_id = last_file_id(&pool).await?;
            let (_, body) = get_body(pool.clone(), bucket.into(), key.clone(), 0, -1, -1)
                .await?
                .context("missing object")?;
            let mut got = Vec::new();
            match body {
                PieceBody::OneShot(bytes) => got.extend_from_slice(&bytes),
                PieceBody::Streamed(mut stream) => {
                    // get_body has already fetched its first response chunk.
                    let writer_pool = pool.clone();
                    let writer_key = key.clone();
                    let overwrite = tokio::spawn(async move {
                        put(&writer_pool, bucket, &writer_key, b"replacement").await
                    });
                    let mut read_after_overwrite = false;
                    let mut gc_during_read = false;
                    while let Some(bytes) =
                        tokio::time::timeout(std::time::Duration::from_secs(15), stream.next())
                            .await
                            .context("GET stalled during concurrent overwrite")?
                    {
                        got.extend_from_slice(&bytes?);
                        read_after_overwrite |= overwrite.is_finished();
                        if overwrite.is_finished() && !gc_during_read {
                            let conn = pool.get().await?;
                            for _ in 0..8 {
                                conn.query_typed_one("SELECT s3p.reap_garbage()", &[])
                                    .await?;
                                let queued: bool = conn.query_typed_one(
                                    "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
                                    &[(&old_id, Type::INT8)],
                                ).await?.try_get(0)?;
                                if !queued {
                                    gc_during_read = true;
                                    break;
                                }
                            }
                        }
                        // Keep consuming: a paused 8+ MiB result fills the
                        // kubectl port-forward tunnel and stalls the writer.
                        tokio::time::sleep(std::time::Duration::from_millis(4)).await;
                    }
                    overwrite.await??;
                    anyhow::ensure!(read_after_overwrite, "overwrite did not overlap GET");
                    anyhow::ensure!(gc_during_read, "old rows were not reaped during GET");
                }
            }
            anyhow::ensure!(got == old, "concurrent overwrite interrupted GET");
            Ok(())
        }
        .await;
        let _ = delete(&pool, bucket, &key).await;
        result
    }

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn a_failed_publish_rolls_back_its_copy_rows() -> Result<()> {
        let url = std::env::var("PGVS3_TEST_DB_URL")?;
        let pool = connect(&url).await?;
        let before = last_file_id(&pool).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tx.send(IngestMsg::Data(Bytes::from(vec![
            42;
            ROW_BYTES as usize + 19
        ])))
        .await?;
        tx.send(IngestMsg::Finish(Vec::new())).await?;
        drop(tx);

        // The precondition fails after COPY has received its rows.
        let result = ingest_writer(
            pool.clone(),
            WriterTarget::Object {
                bucket: "pgvs3-contract".to_owned(),
                key: format!("contract/failed-publish-{}", std::process::id()),
                condition: PutCondition::IfMatch("\"missing\"".to_owned()),
                user_metadata: Vec::new(),
                content_type: "application/octet-stream".to_owned(),
            },
            rx,
        )
        .await;
        anyhow::ensure!(
            result
                .unwrap_err()
                .downcast_ref::<PreconditionFailed>()
                .is_some(),
            "conditional publish did not fail at the expected boundary"
        );
        let file_id = last_file_id(&pool).await?;
        anyhow::ensure!(
            file_id == before + 1,
            "failed publish did not allocate one file"
        );
        let count: i64 = pool
            .get()
            .await?
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        anyhow::ensure!(count == 0, "failed publish left orphan chunk rows");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn concurrent_creates_leave_no_hidden_chunks() -> Result<()> {
        let pool = connect(&std::env::var("PGVS3_TEST_DB_URL")?).await?;
        let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
        let store = AmazonS3Builder::new()
            .with_bucket_name("pgvs3-contract")
            .with_region("us-east-1")
            .with_endpoint(&endpoint)
            .with_access_key_id(std::env::var("PGVS3_ACCESS_KEY")?)
            .with_secret_access_key(std::env::var("PGVS3_SECRET_KEY")?)
            .with_allow_http(true)
            .build()?;
        let key = format!("contract/concurrent-{}", std::process::id());
        let path = Path::from(key.clone());
        let result: Result<()> = async {
            let before = last_file_id(&pool).await?;
            let mut writers = Vec::new();
            for id in 0..2 {
                let (tx, rx) = tokio::sync::mpsc::channel(2);
                tx.send(IngestMsg::Data(Bytes::from(vec![
                    (id + 1) as u8;
                    ROW_BYTES as usize + 1
                ])))
                .await?;
                tx.send(IngestMsg::Finish(Vec::new())).await?;
                drop(tx);
                writers.push(ingest_writer(
                    pool.clone(),
                    WriterTarget::Object {
                        bucket: "pgvs3-contract".to_owned(),
                        key: key.clone(),
                        condition: PutCondition::IfAbsent,
                        user_metadata: Vec::new(),
                        content_type: "application/octet-stream".to_owned(),
                    },
                    rx,
                ));
            }
            let (one, two) = tokio::join!(writers.remove(0), writers.remove(0));
            anyhow::ensure!(
                one.is_ok() != two.is_ok(),
                "exactly one CREATE should commit"
            );
            for failed in [one, two].into_iter().filter_map(Result::err) {
                anyhow::ensure!(failed.downcast_ref::<PreconditionFailed>().is_some());
            }
            let after = last_file_id(&pool).await?;
            anyhow::ensure!(after == before + 2, "each CREATE should allocate one file");
            let conn = pool.get().await?;
            let current: i64 = conn
                .query_typed_one(
                    "SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2",
                    &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
                )
                .await?
                .try_get(0)?;
            for id in before + 1..=after {
                let count: i64 = conn
                    .query_typed_one(
                        "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                        &[(&id, Type::INT8)],
                    )
                    .await?
                    .try_get(0)?;
                anyhow::ensure!(
                    count == if id == current { 2 } else { 0 },
                    "failed COPY left hidden chunks"
                );
            }
            Ok(())
        }
        .await;
        let cleanup = store.delete(&path).await;
        result?;
        cleanup?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires kind; run just kind-contract"]
    async fn abandoned_writer_never_publishes_partial_bytes() -> Result<()> {
        let pool = connect(&std::env::var("PGVS3_TEST_DB_URL")?).await?;
        let bucket = "pgvs3-contract";
        let key = format!("contract/abandoned-{}", std::process::id());
        put(&pool, bucket, &key, b"intact").await?;
        let before = last_file_id(&pool).await?;
        let result: Result<()> = async {
            let mut writer = ChunkWriter::start_object(pool.clone(), bucket.into(), key.clone());
            writer
                .push(Bytes::from(vec![42; ROW_BYTES as usize + 1]))
                .await?;
            let done = writer.done.take().context("missing writer task")?;
            drop(writer); // no Finish, as on cancellation mid-body
            let error = done.await?.expect_err("cancelled writer committed");
            anyhow::ensure!(error.to_string().contains("without Finish"), "{error}");
            let new_id = last_file_id(&pool).await?;
            anyhow::ensure!(new_id == before + 1, "writer didn't allocate a file");
            let count: i64 = pool
                .get()
                .await?
                .query_typed_one(
                    "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                    &[(&new_id, Type::INT8)],
                )
                .await?
                .try_get(0)?;
            anyhow::ensure!(count == 0, "cancelled COPY left rows");
            let old = get(&pool, bucket, &key, 0, -1, -1)
                .await?
                .context("lost object")?;
            anyhow::ensure!(
                old.bytes == "intact",
                "cancelled writer replaced the object"
            );
            Ok(())
        }
        .await;
        let _ = delete(&pool, bucket, &key).await;
        result
    }
}
