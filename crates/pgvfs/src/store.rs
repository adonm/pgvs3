//! pgvfs storage: a file is numbered 8120-byte rows in `pgvfs.chunks` under
//! an immutable `file_id`, published as `(volume, path)` in `pgvfs.files`
//! (layout: schema.sql).
//!
//! Unlike the S3 gateway there is no overwrite guard, snapshot or metadata
//! cache: a file_id never changes bytes, so a read is one primary-key range
//! query per piece, and large reads fetch their pieces in parallel.

use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context as _, Result};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, TryStreamExt};
use pgvs3::pg::{Options, Pool};
use tokio::sync::mpsc;
use tokio_postgres::types::{ToSql, Type};

pub const SCHEMA: &str = include_str!("../schema.sql");
pub const LAYOUT_VERSION: i32 = 1;

pub const ROW_BYTES: i64 = 8120;
/// Rows per parallel read piece: 8 MiB, the gateway's span. Measured on full
/// ClickBench heavy scans, 2 MiB pieces were 3-4% slower (4x the range
/// queries) and 32 MiB no faster.
const PIECE_ROWS: i64 = 1032;
/// Writers hand the COPY task whole rows in batches of about 4 MiB.
pub const WRITE_BATCH: usize = 516 * ROW_BYTES as usize;

const RANGE_SQL: &str =
    "SELECT no, data FROM pgvfs.chunks WHERE file_id = $1 AND no >= $2 AND no <= $3";
const COPY_SQL: &str = "COPY pgvfs.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)";
const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";
const COPY_TRAILER: &[u8] = &[0xFF, 0xFF];

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One pool per DuckDB database. Small warm set (PGVFS_POOL_MIN, default 4):
/// every DuckDB process holds these backends open. PGVFS_POOL_MAX (default
/// 32) bounds parallel pieces across DuckDB's threads.
pub async fn connect(url: &str) -> Result<Pool> {
    let max = env_usize("PGVFS_POOL_MAX", 32).max(1);
    Pool::connect(
        url,
        Options {
            min: env_usize("PGVFS_POOL_MIN", 4).min(max),
            max,
            session: "SET enable_indexscan = off; SET effective_io_concurrency = 32; \
                      SET tcp_keepalives_idle = 30; SET tcp_keepalives_interval = 10; \
                      SET tcp_keepalives_count = 3; \
                      SET idle_in_transaction_session_timeout = '5min'; \
                      SET application_name = 'pgvfs';"
                .into(),
            range_sql: RANGE_SQL,
            range_types: &[Type::INT8, Type::INT4, Type::INT4],
        },
    )
    .await
}

/// Create the layout if absent; fail closed on another version, a partial
/// layout, or a database that holds the S3 gateway's schema.
pub async fn init(pool: &Pool) -> Result<()> {
    const LOCK: i64 = 0x7067_7666; // "pgvf"
    let mut conn = pool.get().await?;
    conn.query_typed("SELECT pg_advisory_lock($1)", &[(&LOCK, Type::INT8)])
        .await?;
    let result = async {
        let tx = conn.transaction().await?;
        let row = tx
            .query_typed_one(
                "SELECT to_regclass('s3p.chunks') IS NOT NULL, \
                        to_regclass('pgvfs.chunks') IS NOT NULL, \
                        to_regclass('pgvfs.layout') IS NOT NULL",
                &[],
            )
            .await?;
        let (s3p, chunks, marked): (bool, bool, bool) =
            (row.try_get(0)?, row.try_get(1)?, row.try_get(2)?);
        anyhow::ensure!(
            !s3p,
            "this database holds the pgvs3 S3 gateway layout (s3p); pgvfs needs its own database"
        );
        if chunks {
            anyhow::ensure!(
                marked,
                "pgvfs.layout is missing: refusing to guess the layout"
            );
            let found: Option<i32> = tx
                .query_typed_one("SELECT max(version) FROM pgvfs.layout", &[])
                .await?
                .try_get(0)?;
            anyhow::ensure!(
                found == Some(LAYOUT_VERSION),
                "pgvfs holds layout {found:?}; this build reads v{LAYOUT_VERSION} \
                 (point it at a fresh database)"
            );
        } else {
            anyhow::ensure!(
                !marked,
                "pgvfs.layout exists without chunks: refusing a partial layout"
            );
            tx.batch_execute(SCHEMA).await?;
            tx.query_typed(
                "INSERT INTO pgvfs.layout (version) VALUES ($1)",
                &[(&LAYOUT_VERSION, Type::INT4)],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
    .await;
    let _ = conn
        .query_typed("SELECT pg_advisory_unlock($1)", &[(&LOCK, Type::INT8)])
        .await;
    result
}

/// Reap unpublished files older than the grace period (bounded work).
pub async fn reap(pool: &Pool) -> Result<i32> {
    let conn = pool.get().await?;
    Ok(conn
        .query_typed_one("SELECT pgvfs.reap_garbage()", &[])
        .await?
        .try_get(0)?)
}

pub fn check_volume(volume: &str) -> Result<()> {
    let b = volume.as_bytes();
    let ok = (1..=63).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b.iter()
            .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"._-".contains(&c));
    anyhow::ensure!(
        ok,
        "invalid pgvfs volume {volume:?} (want [a-z0-9][a-z0-9._-]{{0,62}})"
    );
    Ok(())
}

fn check_path(path: &str) -> Result<()> {
    anyhow::ensure!(
        (1..=1024).contains(&path.len()),
        "pgvfs path must be 1..=1024 bytes"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileInfo {
    pub file_id: i64,
    pub size: i64,
    pub created_at: SystemTime,
}

pub async fn open(pool: &Pool, volume: &str, path: &str) -> Result<Option<FileInfo>> {
    let conn = pool.get().await?;
    let row = conn
        .query_typed_opt(
            "SELECT file_id, size, created_at FROM pgvfs.files WHERE volume = $1 AND path = $2",
            &[(&volume, Type::TEXT), (&path, Type::TEXT)],
        )
        .await?;
    row.map(|r| {
        Ok(FileInfo {
            file_id: r.try_get(0)?,
            size: r.try_get(1)?,
            created_at: r.try_get(2)?,
        })
    })
    .transpose()
}

/// Fill `buf` with file bytes starting at `pos`; `buf` must lie inside the
/// file. Pieces go out on separate pooled connections concurrently, each
/// copying rows straight into its slice of `buf`.
pub async fn read_at(pool: &Pool, file: &FileInfo, pos: i64, buf: &mut [u8]) -> Result<()> {
    let len = buf.len() as i64;
    if len == 0 {
        return Ok(());
    }
    anyhow::ensure!(
        pos >= 0 && pos + len <= file.size,
        "read [{pos}, {}) outside file of {} bytes",
        pos + len,
        file.size
    );
    let first = pos / ROW_BYTES;
    let last = (pos + len - 1) / ROW_BYTES;
    let mut pieces = Vec::new();
    let mut rest = buf;
    let mut lo = first;
    while lo <= last {
        let hi = (lo + PIECE_ROWS - 1).min(last);
        let start = (lo * ROW_BYTES).max(pos);
        let end = ((hi + 1) * ROW_BYTES).min(pos + len);
        let (piece, tail) = rest.split_at_mut((end - start) as usize);
        rest = tail;
        pieces.push(read_piece(pool, file.file_id, lo, hi, start, piece));
        lo = hi + 1;
    }
    futures::future::try_join_all(pieces).await?;
    Ok(())
}

/// Rows `[lo, hi]` into `out`, which holds file bytes from `start`.
async fn read_piece(
    pool: &Pool,
    file_id: i64,
    lo: i64,
    hi: i64,
    start: i64,
    out: &mut [u8],
) -> Result<()> {
    let mut conn = pool.get().await?;
    let stmt = conn.range().await?.clone();
    let (lo32, hi32) = (lo as i32, hi as i32);
    // Streamed: each row is copied into `out` as it arrives, not buffered.
    let params: [&(dyn ToSql + Sync); 3] = [&file_id, &lo32, &hi32];
    let rows = conn.query_raw(&stmt, params).await?;
    futures::pin_mut!(rows);
    let end = start + out.len() as i64;
    let mut filled = 0usize;
    while let Some(row) = rows.try_next().await? {
        let no: i32 = row.try_get(0)?;
        let data: &[u8] = row.try_get(1)?;
        let row_start = no as i64 * ROW_BYTES;
        let a = start.max(row_start);
        let b = end.min(row_start + data.len() as i64);
        if a >= b {
            continue;
        }
        let src = &data[(a - row_start) as usize..(b - row_start) as usize];
        out[(a - start) as usize..(b - start) as usize].copy_from_slice(src);
        filled += src.len();
    }
    anyhow::ensure!(
        filled == out.len(),
        "file {file_id}: rows {lo}..={hi} are missing (removed while reading?)"
    );
    Ok(())
}

/// Keys under `prefix`, in byte order, after `after`, at most `limit`.
pub async fn list(
    pool: &Pool,
    volume: &str,
    prefix: &str,
    after: &str,
    limit: i64,
) -> Result<Vec<String>> {
    let conn = pool.get().await?;
    let rows = conn
        .query_typed(
            "SELECT path FROM pgvfs.files WHERE volume = $1 AND path > $2 \
             AND left(path, length($3)) = $3 ORDER BY path LIMIT $4",
            &[
                (&volume, Type::TEXT),
                (&after, Type::TEXT),
                (&prefix, Type::TEXT),
                (&limit, Type::INT8),
            ],
        )
        .await?;
    rows.iter().map(|r| Ok(r.try_get(0)?)).collect()
}

/// Unpublish one file. Returns false if nothing was there.
pub async fn remove(pool: &Pool, volume: &str, path: &str) -> Result<bool> {
    let conn = pool.get().await?;
    let n = conn
        .execute_typed(
            "WITH gone AS (DELETE FROM pgvfs.files WHERE volume = $1 AND path = $2 \
             RETURNING file_id) INSERT INTO pgvfs.garbage (file_id) SELECT file_id FROM gone",
            &[(&volume, Type::TEXT), (&path, Type::TEXT)],
        )
        .await?;
    Ok(n > 0)
}

/// Unpublish every file under `prefix` (a directory removal). Returns count.
pub async fn remove_prefix(pool: &Pool, volume: &str, prefix: &str) -> Result<u64> {
    let conn = pool.get().await?;
    Ok(conn
        .execute_typed(
            "WITH gone AS (DELETE FROM pgvfs.files WHERE volume = $1 \
             AND left(path, length($2)) = $2 RETURNING file_id) \
             INSERT INTO pgvfs.garbage (file_id) SELECT file_id FROM gone",
            &[(&volume, Type::TEXT), (&prefix, Type::TEXT)],
        )
        .await?)
}

/// Rename within a volume, replacing any file at `to`.
pub async fn rename(pool: &Pool, volume: &str, from: &str, to: &str) -> Result<()> {
    check_path(to)?;
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let found = tx
        .query_typed_opt(
            "SELECT file_id FROM pgvfs.files WHERE volume = $1 AND path = $2 FOR UPDATE",
            &[(&volume, Type::TEXT), (&from, Type::TEXT)],
        )
        .await?;
    if found.is_none() {
        bail!("pgvfs://{volume}/{from}: no such file");
    }
    if from != to {
        replace_garbage(&tx, volume, to).await?;
        tx.execute_typed(
            "UPDATE pgvfs.files SET path = $3 WHERE volume = $1 AND path = $2",
            &[
                (&volume, Type::TEXT),
                (&from, Type::TEXT),
                (&to, Type::TEXT),
            ],
        )
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// Queue whatever is published at `path` for reaping and unpublish it.
async fn replace_garbage(
    tx: &tokio_postgres::Transaction<'_>,
    volume: &str,
    path: &str,
) -> Result<()> {
    tx.execute_typed(
        "WITH gone AS (DELETE FROM pgvfs.files WHERE volume = $1 AND path = $2 \
         RETURNING file_id) INSERT INTO pgvfs.garbage (file_id) SELECT file_id FROM gone",
        &[(&volume, Type::TEXT), (&path, Type::TEXT)],
    )
    .await?;
    Ok(())
}

pub enum WriteMsg {
    /// Whole rows except possibly in the final batch.
    Data(Bytes),
    /// Publish: every byte has been sent.
    Publish,
}

/// Spawn the COPY for one new file. Bytes flow in as `WriteMsg::Data`;
/// `Publish` commits the rows and the path together. Dropping the sender
/// without `Publish` rolls everything back: nothing is left behind.
pub fn spawn_writer(
    rt: &tokio::runtime::Handle,
    pool: Pool,
    volume: String,
    path: String,
) -> Result<(mpsc::Sender<WriteMsg>, tokio::task::JoinHandle<Result<()>>)> {
    check_volume(&volume)?;
    check_path(&path)?;
    let (tx, rx) = mpsc::channel(4);
    let task = rt.spawn(write_file(pool, volume, path, rx));
    Ok((tx, task))
}

async fn write_file(
    pool: Pool,
    volume: String,
    path: String,
    mut rx: mpsc::Receiver<WriteMsg>,
) -> Result<()> {
    let mut conn = pool.get().await?;
    let tx = conn.transaction().await?;
    let file_id: i64 = tx
        .query_typed_one("SELECT nextval('pgvfs.file_ids')", &[])
        .await?
        .try_get(0)?;
    let sink = tx.copy_in::<_, Bytes>(COPY_SQL).await?;
    futures::pin_mut!(sink);
    sink.send(Bytes::from_static(COPY_HEADER)).await?;
    let mut no: i64 = 0;
    let mut size: i64 = 0;
    loop {
        match rx.recv().await {
            Some(WriteMsg::Data(data)) => {
                anyhow::ensure!(
                    size % ROW_BYTES == 0,
                    "pgvfs writer sent data after a partial row"
                );
                size += data.len() as i64;
                let mut frame = BytesMut::with_capacity(data.len() + data.len() / 256 + 64);
                for row in data.chunks(ROW_BYTES as usize) {
                    anyhow::ensure!(no < i32::MAX as i64, "file too large");
                    frame.put_i16(3);
                    frame.put_i32(8);
                    frame.put_i64(file_id);
                    frame.put_i32(4);
                    frame.put_i32(no as i32);
                    frame.put_i32(row.len() as i32);
                    frame.put_slice(row);
                    no += 1;
                }
                sink.send(frame.freeze()).await?;
            }
            Some(WriteMsg::Publish) => break,
            None => return Err(anyhow!("pgvfs write abandoned")),
        }
    }
    sink.send(Bytes::from_static(COPY_TRAILER)).await?;
    sink.as_mut().finish().await.context("pgvfs COPY")?;
    replace_garbage(&tx, &volume, &path).await?;
    tx.execute_typed(
        "INSERT INTO pgvfs.files (volume, path, file_id, size) VALUES ($1, $2, $3, $4)",
        &[
            (&volume, Type::TEXT),
            (&path, Type::TEXT),
            (&file_id, Type::INT8),
            (&size, Type::INT8),
        ],
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// For tests and tools: the whole file.
pub async fn read_all(pool: &Pool, file: &FileInfo) -> Result<Vec<u8>> {
    let mut out = vec![0u8; file.size as usize];
    read_at(pool, file, 0, &mut out).await?;
    Ok(out)
}

/// Microseconds since the epoch, for DuckDB's last-modified time.
pub fn micros(t: SystemTime) -> i64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_micros() as i64
}
