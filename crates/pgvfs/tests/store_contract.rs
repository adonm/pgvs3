//! pgvfs storage contract against a real PostgreSQL (PGVFS_TEST_DB_URL, a
//! database the S3 gateway never uses). Run by deploy/bench/contract.sh.

use anyhow::Result;
use pgvfs::store::{self, WriteMsg, ROW_BYTES, WRITE_BATCH};
use pgvs3::pg::Pool;
use tokio_postgres::types::Type;

async fn pool() -> Result<Pool> {
    let pool = store::connect(&std::env::var("PGVFS_TEST_DB_URL")?).await?;
    store::init(&pool).await?;
    Ok(pool)
}

fn volume(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("t-{tag}-{}-{nanos}", std::process::id())
}

fn bytes(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| {
            (i as u64)
                .wrapping_mul(2654435761)
                .wrapping_add(seed as u64) as u8
        })
        .collect()
}

async fn write(pool: &Pool, volume: &str, path: &str, data: &[u8]) -> Result<()> {
    let (tx, task) = store::spawn_writer(
        &tokio::runtime::Handle::current(),
        pool.clone(),
        volume.into(),
        path.into(),
    )?;
    for batch in data.chunks(WRITE_BATCH) {
        tx.send(WriteMsg::Data(batch.to_vec().into())).await?;
    }
    tx.send(WriteMsg::Publish).await?;
    task.await?
}

async fn count(pool: &Pool, sql: &str, id: i64) -> Result<i64> {
    let conn = pool.get().await?;
    Ok(conn
        .query_typed_one(sql, &[(&id, Type::INT8)])
        .await?
        .try_get(0)?)
}

#[tokio::test]
#[ignore]
async fn round_trips_exact_ranges() -> Result<()> {
    let pool = pool().await?;
    let vol = volume("rt");
    let row = ROW_BYTES as usize;
    for (i, n) in [
        0,
        1,
        row - 1,
        row,
        row + 1,
        WRITE_BATCH + 5,
        3 * 258 * row + 17,
    ]
    .into_iter()
    .enumerate()
    {
        let data = bytes(n, i as u8);
        let path = format!("d/f{i}.bin");
        write(&pool, &vol, &path, &data).await?;
        let f = store::open(&pool, &vol, &path).await?.expect("published");
        assert_eq!(f.size, n as i64);
        assert_eq!(store::read_all(&pool, &f).await?, data, "size {n}");
        for (pos, len) in [
            (0, 1),
            (row - 3, 7),
            (n / 3, n / 2),
            (n.saturating_sub(5), 5),
        ] {
            if pos + len > n || len == 0 {
                continue;
            }
            let mut buf = vec![0; len];
            store::read_at(&pool, &f, pos as i64, &mut buf).await?;
            assert_eq!(buf, data[pos..pos + len], "size {n} at {pos}+{len}");
        }
        let mut past = vec![0; 1];
        assert!(store::read_at(&pool, &f, n as i64, &mut past)
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
#[ignore]
async fn abandoned_write_leaves_nothing() -> Result<()> {
    let pool = pool().await?;
    let vol = volume("ab");
    let (tx, task) = store::spawn_writer(
        &tokio::runtime::Handle::current(),
        pool.clone(),
        vol.clone(),
        "x".into(),
    )?;
    tx.send(WriteMsg::Data(bytes(WRITE_BATCH, 1).into()))
        .await?;
    drop(tx);
    assert!(task.await?.is_err());
    assert!(store::open(&pool, &vol, "x").await?.is_none());
    let conn = pool.get().await?;
    let orphans: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM pgvfs.chunks c WHERE NOT EXISTS \
             (SELECT 1 FROM pgvfs.files f WHERE f.file_id = c.file_id) \
             AND NOT EXISTS (SELECT 1 FROM pgvfs.garbage g WHERE g.file_id = c.file_id)",
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(orphans, 0);
    Ok(())
}

#[tokio::test]
#[ignore]
async fn overwrite_queues_old_file_and_reap_honours_grace() -> Result<()> {
    let pool = pool().await?;
    let vol = volume("ow");
    write(&pool, &vol, "a", &bytes(3 * ROW_BYTES as usize, 1)).await?;
    let old = store::open(&pool, &vol, "a").await?.unwrap();
    write(&pool, &vol, "a", &bytes(10, 2)).await?;
    let new = store::open(&pool, &vol, "a").await?.unwrap();
    assert_ne!(old.file_id, new.file_id);
    assert_eq!(store::read_all(&pool, &new).await?, bytes(10, 2));
    let garbage = "SELECT count(*) FROM pgvfs.garbage WHERE file_id = $1";
    let chunks = "SELECT count(*) FROM pgvfs.chunks WHERE file_id = $1";
    assert_eq!(count(&pool, garbage, old.file_id).await?, 1);
    // Inside the grace period the old bytes stay readable.
    store::reap(&pool).await?;
    assert_eq!(store::read_all(&pool, &old).await?.len(), old.size as usize);
    let conn = pool.get().await?;
    conn.query_typed_one("SELECT pgvfs.reap_garbage(interval '0 seconds')", &[])
        .await?;
    assert_eq!(count(&pool, chunks, old.file_id).await?, 0);
    assert_eq!(count(&pool, garbage, old.file_id).await?, 0);
    let err = store::read_all(&pool, &old).await.unwrap_err();
    assert!(format!("{err:#}").contains("missing"), "{err:#}");
    Ok(())
}

#[tokio::test]
#[ignore]
async fn list_remove_rename() -> Result<()> {
    let pool = pool().await?;
    let vol = volume("ls");
    for p in ["a/1", "a/2", "a/b/3", "ab", "c"] {
        write(&pool, &vol, p, p.as_bytes()).await?;
    }
    assert_eq!(
        store::list(&pool, &vol, "a/", "", 100).await?,
        ["a/1", "a/2", "a/b/3"]
    );
    assert_eq!(
        store::list(&pool, &vol, "a", "a/1", 2).await?,
        ["a/2", "a/b/3"]
    );
    assert_eq!(
        store::list(&pool, &vol, "a_", "", 100).await?,
        Vec::<String>::new()
    );

    assert!(store::remove(&pool, &vol, "c").await?);
    assert!(!store::remove(&pool, &vol, "c").await?);

    store::rename(&pool, &vol, "ab", "a/1").await?;
    let f = store::open(&pool, &vol, "a/1").await?.unwrap();
    assert_eq!(store::read_all(&pool, &f).await?, b"ab");
    assert!(store::open(&pool, &vol, "ab").await?.is_none());
    assert!(store::rename(&pool, &vol, "missing", "x").await.is_err());

    assert_eq!(store::remove_prefix(&pool, &vol, "a/").await?, 3);
    assert!(store::list(&pool, &vol, "", "", 100).await?.is_empty());
    Ok(())
}

#[tokio::test]
#[ignore]
async fn refuses_an_s3_gateway_database() -> Result<()> {
    let Ok(url) = std::env::var("PGVFS_TEST_S3_DB_URL") else {
        return Ok(());
    };
    let pool = store::connect(&url).await?;
    let err = store::init(&pool).await.unwrap_err();
    assert!(format!("{err:#}").contains("S3 gateway"), "{err:#}");
    Ok(())
}

#[test]
fn volume_names() {
    for ok in ["lake", "a", "l-1.x_y"] {
        store::check_volume(ok).unwrap();
    }
    for bad in ["", "Lake", "-a", "a/b", &"x".repeat(64)] {
        assert!(store::check_volume(bad).is_err(), "{bad}");
    }
}
