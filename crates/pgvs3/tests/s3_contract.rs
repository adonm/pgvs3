//! S3 contract against two local gateways on one disposable PostgreSQL. Run with
//! `just contract` (or `just churn`); plain `cargo test` only builds it.

use anyhow::{ensure, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio_postgres::types::Type;

fn bucket_request(endpoint: &str, method: &str, bucket: &str) -> Result<(u16, String)> {
    signed_request(endpoint, method, bucket, &[])
}

fn xml_tag<'a>(body: &'a str, tag: &str) -> Option<&'a str> {
    let (_, rest) = body.split_once(&format!("<{tag}>"))?;
    rest.split_once(&format!("</{tag}>"))
        .map(|(value, _)| value)
}

fn signed_request(
    endpoint: &str,
    method: &str,
    path: &str,
    headers: &[&str],
) -> Result<(u16, String)> {
    signed_request_body(endpoint, method, path, headers, None)
}

fn signed_request_body(
    endpoint: &str,
    method: &str,
    path: &str,
    headers: &[&str],
    body: Option<&str>,
) -> Result<(u16, String)> {
    let mut cmd = std::process::Command::new("curl");
    let user = format!(
        "{}:{}",
        std::env::var("PGVS3_ACCESS_KEY")?,
        std::env::var("PGVS3_SECRET_KEY")?
    );
    cmd.args([
        "-sS",
        "--aws-sigv4",
        "aws:amz:us-east-1:s3",
        "--user",
        &user,
        "-X",
        method,
        "-w",
        "\n%{http_code}",
    ]);
    if method == "HEAD" {
        cmd.arg("--head");
    }
    for header in headers {
        cmd.args(["-H", header]);
    }
    if let Some(body) = body {
        cmd.args(["--data-binary", body]);
    }
    let output = cmd.arg(format!("{endpoint}/{path}")).output()?;
    ensure!(output.status.success(), "curl failed: {:?}", output.stderr);
    let text = String::from_utf8(output.stdout)?;
    let (body, code) = text
        .rsplit_once('\n')
        .ok_or_else(|| anyhow::anyhow!("no HTTP status"))?;
    Ok((code.parse()?, body.to_owned()))
}

fn signed_headers(
    endpoint: &str,
    method: &str,
    path: &str,
    headers: &[&str],
    body: Option<&str>,
) -> Result<(u16, String)> {
    let user = format!(
        "{}:{}",
        std::env::var("PGVS3_ACCESS_KEY")?,
        std::env::var("PGVS3_SECRET_KEY")?
    );
    let mut cmd = std::process::Command::new("curl");
    cmd.args([
        "-sS",
        "--aws-sigv4",
        "aws:amz:us-east-1:s3",
        "--user",
        &user,
        "-X",
        method,
        "-D",
        "-",
        "-o",
        "/dev/null",
        "-w",
        "\n%{http_code}",
    ]);
    if method == "HEAD" {
        cmd.arg("--head");
    }
    for header in headers {
        cmd.args(["-H", header]);
    }
    if let Some(body) = body {
        cmd.args(["--data-binary", body]);
    }
    let output = cmd.arg(format!("{endpoint}/{path}")).output()?;
    ensure!(output.status.success(), "curl failed: {:?}", output.stderr);
    let text = String::from_utf8(output.stdout)?;
    let (headers, code) = text
        .rsplit_once('\n')
        .ok_or_else(|| anyhow::anyhow!("no status"))?;
    Ok((code.parse()?, headers.to_owned()))
}

fn header_value<'a>(headers: &'a str, key: &str) -> Option<&'a str> {
    headers.lines().rev().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(key).then_some(value.trim())
    })
}

fn store(endpoint: &str, bucket: &str) -> Result<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region("us-east-1")
        .with_endpoint(endpoint)
        .with_access_key_id(std::env::var("PGVS3_ACCESS_KEY")?)
        .with_secret_access_key(std::env::var("PGVS3_SECRET_KEY")?)
        .with_allow_http(true)
        .build()?)
}

fn prefix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after 1970")
        .as_nanos();
    format!("contract/{}-{nanos}", std::process::id())
}

fn endpoints() -> Result<(AmazonS3, AmazonS3)> {
    let bucket = "pgvs3-contract";
    Ok((
        store(&std::env::var("PGVS3_TEST_ENDPOINT_A")?, bucket)?,
        store(&std::env::var("PGVS3_TEST_ENDPOINT_B")?, bucket)?,
    ))
}

async fn pool() -> Result<pgvs3::db::Pool> {
    let url = std::env::var("PGVS3_TEST_DB_URL")?;
    pgvs3::db::connect(&url).await
}

async fn chunk_maintenance(pool: &pgvs3::db::Pool) -> Result<(i64, i64, i64)> {
    let row = pool
        .get()
        .await?
        .query_typed_one(
            "SELECT coalesce(sum(n_dead_tup), 0)::int8, \
                coalesce(sum(autovacuum_count), 0)::int8, \
                coalesce(sum(pg_total_relation_size(relid)), 0)::int8 \
         FROM pg_stat_user_tables WHERE relid = 's3p.chunks'::regclass",
            &[],
        )
        .await?;
    Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?))
}

async fn queued_then_reaped(pool: &pgvs3::db::Pool, file_id: i64) -> Result<()> {
    let conn = pool.get().await?;
    let queued: bool = conn
        .query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
            &[(&file_id, Type::INT8)],
        )
        .await?
        .try_get(0)?;
    // pg_cron may have already drained this file between the S3 response and
    // this assertion. If so, the absence of both queue entry and rows is fine.
    for _ in 0..64 {
        conn.query_typed_one("SELECT s3p.reap_garbage(65536)", &[])
            .await?;
        let queued: bool = conn
            .query_typed_one(
                "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        if !queued {
            let count: i64 = conn
                .query_typed_one(
                    "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                    &[(&file_id, Type::INT8)],
                )
                .await?
                .try_get(0)?;
            ensure!(count == 0, "garbage file {file_id} still has rows");
            return Ok(());
        }
    }
    anyhow::bail!("garbage file {file_id} did not drain (initially queued: {queued})")
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn bucket_lifecycle_is_visible_on_both_gateways() -> Result<()> {
    let a = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let b = std::env::var("PGVS3_TEST_ENDPOINT_B")?;
    let bucket = format!(
        "pgvs3-{:x}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos()
    );
    let store = store(&b, &bucket)?;
    let path = Path::from("contract/bucket-lifecycle");
    let result: Result<()> = async {
        ensure!(bucket_request(&a, "HEAD", &bucket)?.0 == 404);
        ensure!(bucket_request(&a, "PUT", &bucket)?.0 == 200);
        ensure!(bucket_request(&b, "HEAD", &bucket)?.0 == 200);
        let (status, list) = bucket_request(&b, "GET", "")?;
        ensure!(status == 200 && list.contains(&format!("<Name>{bucket}</Name>")));
        ensure!(list.contains("<CreationDate>"));
        ensure!(bucket_request(&b, "PUT", &bucket)?.0 == 200);

        store
            .put(&path, Bytes::from_static(b"bucket-test").into())
            .await?;
        let (status, body) = bucket_request(&a, "DELETE", &bucket)?;
        ensure!(status == 409 && body.contains("BucketNotEmpty"));
        store.delete(&path).await?;

        let mut upload = store.put_multipart(&path).await?;
        let result: Result<()> = async {
            let conn = pool().await?.get().await?;
            let upload_id: String = conn
                .query_typed_one(
                    "SELECT upload_id FROM s3p.uploads WHERE bucket = $1 AND key = $2",
                    &[
                        (&bucket, Type::TEXT),
                        (&"contract/bucket-lifecycle", Type::TEXT),
                    ],
                )
                .await?
                .try_get(0)?;
            let (status, body) = bucket_request(
                &a,
                "DELETE",
                &format!("{bucket}/wrong-key?uploadId={upload_id}"),
            )?;
            ensure!(status == 404 && body.contains("NoSuchUpload"));
            let (status, body) = bucket_request(&a, "DELETE", &bucket)?;
            ensure!(status == 409 && body.contains("BucketNotEmpty"));
            Ok(())
        }
        .await;
        let aborted = upload.abort().await;
        result?;
        aborted?;

        ensure!(bucket_request(&a, "DELETE", &bucket)?.0 == 204);
        ensure!(bucket_request(&b, "HEAD", &bucket)?.0 == 404);
        let (_, list) = bucket_request(&b, "GET", "")?;
        ensure!(!list.contains(&format!("<Name>{bucket}</Name>")));
        let (status, body) = bucket_request(&a, "GET", &format!("{bucket}?list-type=2"))?;
        ensure!(status == 404 && body.contains("NoSuchBucket"));
        let (status, body) = bucket_request(&b, "PUT", &format!("{bucket}/missing"))?;
        ensure!(status == 404 && body.contains("NoSuchBucket"));
        Ok(())
    }
    .await;

    let _ = store.delete(&path).await;
    let _ = bucket_request(&a, "DELETE", &bucket);
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn clean_failed_prior_contract_objects() -> Result<()> {
    let (store, _) = endpoints()?;
    let objects = store
        .list(Some(&Path::from("contract")))
        .try_collect::<Vec<_>>()
        .await?;
    for object in objects {
        store.delete(&object.location).await?;
    }
    Ok(())
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn listing_pages_keep_every_key_and_emit_common_prefixes_once() -> Result<()> {
    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let store = store(&endpoint, "pgvs3-contract")?;
    let base = format!("{}/listing/", prefix());
    let paths: Vec<_> = ["a", "b", "group/one", "group/two", "z"]
        .iter()
        .map(|name| Path::from(format!("{base}{name}")))
        .collect();
    let result: Result<()> = async {
        for path in &paths {
            store.put(path, Bytes::from_static(b"data").into()).await?;
        }
        let (status, empty_page) = signed_request(
            &endpoint,
            "GET",
            &format!("pgvs3-contract?list-type=2&prefix={base}&max-keys=0"),
            &[],
        )?;
        ensure!(status == 200, "LIST failed: {empty_page}");
        ensure!(xml_tag(&empty_page, "KeyCount") == Some("0"));
        ensure!(xml_tag(&empty_page, "IsTruncated") == Some("false"));
        let (status, after_page) = signed_request(
            &endpoint,
            "GET",
            &format!(
                "pgvs3-contract?list-type=2&prefix={base}&delimiter=/&start-after={base}group/one"
            ),
            &[],
        )?;
        ensure!(status == 200, "LIST failed: {after_page}");
        ensure!(xml_tag(&after_page, "KeyCount") == Some("1"));
        ensure!(after_page.contains(&format!("<Key>{base}z</Key>")));
        ensure!(!after_page.contains("<CommonPrefixes>"));
        for (delimiter, expected) in [
            ("", vec!["a", "b", "group/one", "group/two", "z"]),
            ("/", vec!["a", "b", "group/", "z"]),
        ] {
            let mut token = None;
            let mut listed = Vec::new();
            for _ in 0..10 {
                let mut path = format!(
                    "pgvs3-contract?list-type=2&prefix={base}&max-keys=1&delimiter={delimiter}"
                );
                if let Some(ref token) = token {
                    path.push_str(&format!("&continuation-token={token}"));
                }
                let (status, body) = signed_request(&endpoint, "GET", &path, &[])?;
                ensure!(status == 200, "LIST failed: {body}");
                ensure!(xml_tag(&body, "KeyCount") == Some("1"), "{body}");
                let entry = if let Some((_, contents)) = body.split_once("<Contents>") {
                    xml_tag(contents, "Key")
                } else {
                    body.split_once("<CommonPrefixes>")
                        .and_then(|(_, common)| xml_tag(common, "Prefix"))
                }
                .ok_or_else(|| anyhow::anyhow!("LIST page has no entry: {body}"))?;
                listed.push(entry.strip_prefix(&base).unwrap_or(entry).to_owned());
                if xml_tag(&body, "IsTruncated") == Some("false") {
                    break;
                }
                token = Some(
                    xml_tag(&body, "NextContinuationToken")
                        .ok_or_else(|| anyhow::anyhow!("LIST has no next token: {body}"))?
                        .to_owned(),
                );
            }
            ensure!(
                listed == expected,
                "LIST with delimiter {delimiter:?}: {listed:?}"
            );
        }
        Ok(())
    }
    .await;
    for path in &paths {
        let _ = store.delete(path).await;
    }
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn database_maintenance_is_automatic() -> Result<()> {
    let pool = pool().await?;
    let conn = pool.get().await?;
    let cron_jobs: i64 = conn
        .query_typed_one(
            "SELECT count(*) FROM cron.job \
             WHERE jobname = 'pgvs3-maintain' AND database = current_database() AND active",
            &[],
        )
        .await?
        .try_get(0)?;
    ensure!(cron_jobs == 1, "maintenance is not scheduled in PostgreSQL");
    let configured: bool = conn
        .query_typed_one(
            "SELECT reloptions @> ARRAY['autovacuum_vacuum_scale_factor=0.01', \
                     'autovacuum_analyze_scale_factor=0.02', \
                     'autovacuum_vacuum_threshold=1000'] \
             FROM pg_class WHERE oid = 's3p.chunks'::regclass",
            &[],
        )
        .await?
        .try_get(0)?;
    ensure!(configured, "s3p.chunks lacks autovacuum tuning");
    let upload_age_index: bool = conn
        .query_typed_one("SELECT to_regclass('s3p.uploads_by_age') IS NOT NULL", &[])
        .await?
        .try_get(0)?;
    ensure!(upload_age_index, "multipart expiry lacks its age index");
    Ok(())
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn overwrite_and_delete_are_visible_on_both_gateways() -> Result<()> {
    let (a, b) = endpoints()?;
    let key = format!("{}/object", prefix());
    let path = Path::from(key.clone());
    let old = Bytes::from(vec![0x41; 8120 * 2 + 23]);
    let new = Bytes::from(vec![0x42; 8120 * 3 + 31]);

    let result: Result<()> = async {
        a.put(&path, old.clone().into()).await?;
        let first = b.head(&path).await?;
        ensure!(
            first.size == old.len() as u64,
            "initial HEAD returned wrong size"
        );
        ensure!(
            b.get_range(&path, 8117..8140).await?.as_ref() == &old[8117..8140],
            "cross-row range disagrees with PUT"
        );

        // Gateway B has cached the old file ID. An overwrite through A must
        // not leave a stale HEAD or return old bytes/416 on a new range.
        a.put(&path, new.clone().into()).await?;
        let second = b.head(&path).await?;
        ensure!(second.size == new.len() as u64, "HEAD kept the old size");
        ensure!(second.e_tag != first.e_tag, "HEAD kept the old ETag");
        let off = old.len() + 1;
        ensure!(
            b.get_range(&path, off as u64..(off + 17) as u64)
                .await?
                .as_ref()
                == &new[off..off + 17],
            "GET range past the old EOF disagrees with PUT"
        );

        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let file_id: i64 = row.try_get(0)?;
        a.delete(&path).await?;
        ensure!(
            matches!(
                b.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "HEAD still exposes a deleted object"
        );
        ensure!(
            a.list(Some(&Path::from(key.clone())))
                .try_collect::<Vec<_>>()
                .await?
                .is_empty(),
            "LIST still exposes a deleted object"
        );
        queued_then_reaped(&pool().await?, file_id).await?;
        Ok(())
    }
    .await;

    // Even when an assertion fails, leave no published test object behind.
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn zero_byte_cache_entry_not_used_after_remote_overwrite() -> Result<()> {
    let (a, b) = endpoints()?;
    let path = Path::from(format!("{}/zero-to-nonempty", prefix()));
    let result: Result<()> = async {
        a.put(&path, Bytes::new().into()).await?;
        ensure!(b.get(&path).await?.bytes().await?.is_empty());
        a.put(&path, Bytes::from_static(b"new bytes").into())
            .await?;
        ensure!(b.get(&path).await?.bytes().await? == b"new bytes"[..]);
        Ok(())
    }
    .await;
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn direct_database_overwrite_refreshes_cached_gets() -> Result<()> {
    let (gateway, _) = endpoints()?;
    let bucket = "pgvs3-contract";
    let key = format!("{}/direct-overwrite", prefix());
    let path = Path::from(key.clone());
    let result: Result<()> = async {
        let old = vec![0x41; 8120 * 2];
        gateway.put(&path, Bytes::from(old).into()).await?;
        gateway.get_range(&path, 8000..16000).await?;
        let pool = pool().await?;
        let same_size = vec![0x42; 8120 * 2];
        pgvs3::db::put(&pool, bucket, &key, &same_size).await?;
        ensure!(gateway.get_range(&path, 8000..16000).await?.as_ref() == &same_size[8000..16000]);
        let grown = vec![0x43; 12 * 1024 * 1024];
        pgvs3::db::put(&pool, bucket, &key, &grown).await?;
        ensure!(
            gateway
                .get_range(&path, 10_485_760..10_486_760)
                .await?
                .as_ref()
                == &grown[10_485_760..10_486_760]
        );
        Ok(())
    }
    .await;
    let _ = gateway.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn conditional_puts_are_atomic_across_gateways() -> Result<()> {
    let (a, b) = endpoints()?;
    let path = Path::from(format!("{}/conditional", prefix()));
    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let result: Result<()> = async {
        let first = a
            .put_opts(
                &path,
                Bytes::from_static(b"first").into(),
                PutMode::Create.into(),
            )
            .await?;
        let duplicate = b
            .put_opts(
                &path,
                Bytes::from_static(b"duplicate").into(),
                PutMode::Create.into(),
            )
            .await;
        ensure!(matches!(
            duplicate,
            Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. })
        ));
        ensure!(a.get(&path).await?.bytes().await? == b"first"[..]);

        let old = UpdateVersion {
            e_tag: first.e_tag,
            version: None,
        };
        let updated = b
            .put_opts(
                &path,
                Bytes::from_static(b"updated").into(),
                PutMode::Update(old.clone()).into(),
            )
            .await?;
        let stale = a
            .put_opts(
                &path,
                Bytes::from_static(b"stale").into(),
                PutMode::Update(old).into(),
            )
            .await;
        ensure!(matches!(
            stale,
            Err(object_store::Error::Precondition { .. })
        ));
        ensure!(b.get(&path).await?.bytes().await? == b"updated"[..]);
        ensure!(updated.e_tag.is_some());

        let (status, body) = signed_request(
            &endpoint,
            "PUT",
            &format!("pgvs3-contract/{path}"),
            &["If-None-Match: not-a-wildcard"],
        )?;
        ensure!(status == 400 && body.contains("InvalidRequest"));
        let (status, body) = signed_request(
            &endpoint,
            "DELETE",
            &format!("pgvs3-contract/{path}"),
            &["If-Match: *"],
        )?;
        ensure!(status == 501 && body.contains("NotImplemented"));
        ensure!(a.get(&path).await?.bytes().await? == b"updated"[..]);

        a.delete(&path).await?;
        let left = a.put_opts(
            &path,
            Bytes::from_static(b"left").into(),
            PutOptions::from(PutMode::Create),
        );
        let right = b.put_opts(
            &path,
            Bytes::from_static(b"right").into(),
            PutOptions::from(PutMode::Create),
        );
        let (left, right) = tokio::join!(left, right);
        ensure!(
            left.is_ok() != right.is_ok(),
            "exactly one conditional create must win"
        );
        ensure!(
            matches!(
                left,
                Ok(_)
                    | Err(object_store::Error::Precondition { .. })
                    | Err(object_store::Error::AlreadyExists { .. })
            ) && matches!(
                right,
                Ok(_)
                    | Err(object_store::Error::Precondition { .. })
                    | Err(object_store::Error::AlreadyExists { .. })
            ),
            "a conditional create failed for an unrelated reason"
        );
        let actual = a.get(&path).await?.bytes().await?;
        ensure!(actual == b"left"[..] || actual == b"right"[..]);
        Ok(())
    }
    .await;
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn s3_etags_metadata_and_multipart_checksums() -> Result<()> {
    use md5::{Digest as _, Md5};
    use sha2::Sha256;

    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let other = std::env::var("PGVS3_TEST_ENDPOINT_B")?;
    let bucket = format!("pgvs3-md5-{}", prefix().replace('/', "-"));
    let (status, body) = signed_request(&endpoint, "PUT", &bucket, &[])?;
    ensure!(status == 200, "bucket creation failed: {status}: {body}");
    let (status, body) = signed_request(&other, "PUT", &bucket, &[])?;
    ensure!(
        status == 200,
        "bucket re-creation is not idempotent: {status}: {body}"
    );

    let key = "single";
    let payload = "metadata survives an overwrite";
    let expected_md5 = pgvs3::db::hex(&Md5::digest(payload.as_bytes()));
    let (status, headers) = signed_headers(
        &endpoint,
        "PUT",
        &format!("{bucket}/{key}"),
        &[
            "x-amz-meta-ckp-data: checkpoint-1",
            "Content-Type: text/plain",
        ],
        Some(payload),
    )?;
    ensure!(status == 200, "PUT failed: {status}: {headers}");
    ensure!(header_value(&headers, "ETag") == Some(format!("\"{expected_md5}\"").as_str()));
    let (status, headers) = signed_headers(&other, "HEAD", &format!("{bucket}/{key}"), &[], None)?;
    ensure!(status == 200, "HEAD failed: {status}: {headers}");
    ensure!(header_value(&headers, "ETag") == Some(format!("\"{expected_md5}\"").as_str()));
    ensure!(header_value(&headers, "x-amz-meta-ckp-data") == Some("checkpoint-1"));
    ensure!(header_value(&headers, "Content-Type") == Some("text/plain"));
    let conn = pool().await?.get().await?;
    let row = conn
        .query_typed_one(
            "SELECT sha256, etag, user_metadata FROM s3p.objects WHERE bucket = $1 AND key = $2",
            &[(&bucket, Type::TEXT), (&key, Type::TEXT)],
        )
        .await?;
    ensure!(row.try_get::<_, Vec<u8>>(0)? == Sha256::digest(payload.as_bytes()).to_vec());
    ensure!(row.try_get::<_, String>(1)? == expected_md5);
    ensure!(row.try_get::<_, Vec<String>>(2)? == ["ckp-data", "checkpoint-1"]);
    let (status, body) = signed_request_body(
        &other,
        "PUT",
        &format!("{bucket}/{key}"),
        &[
            "Content-MD5: AAAAAAAAAAAAAAAAAAAAAA==",
            "x-amz-meta-ckp-data: changed",
        ],
        Some("corrupt replacement"),
    )?;
    ensure!(
        status == 400 && body.contains("BadDigest"),
        "{status}: {body}"
    );
    let (status, headers) =
        signed_headers(&endpoint, "HEAD", &format!("{bucket}/{key}"), &[], None)?;
    ensure!(status == 200 && header_value(&headers, "x-amz-meta-ckp-data") == Some("checkpoint-1"));
    ensure!(header_value(&headers, "ETag") == Some(format!("\"{expected_md5}\"").as_str()));
    let next = "conditional replacement";
    let next_md5 = pgvs3::db::hex(&Md5::digest(next.as_bytes()));
    let (status, headers) = signed_headers(
        &other,
        "PUT",
        &format!("{bucket}/{key}"),
        &[
            &format!("If-Match: \"{expected_md5}\""),
            "x-amz-meta-ckp-data: checkpoint-3",
        ],
        Some(next),
    )?;
    ensure!(
        status == 200 && header_value(&headers, "ETag") == Some(format!("\"{next_md5}\"").as_str()),
        "MD5 conditional PUT failed: {status}: {headers}"
    );
    let (status, headers) =
        signed_headers(&endpoint, "GET", &format!("{bucket}/{key}"), &[], None)?;
    ensure!(status == 200 && header_value(&headers, "x-amz-meta-ckp-data") == Some("checkpoint-3"));
    ensure!(
        store(&endpoint, &bucket)?
            .get(&Path::from(key))
            .await?
            .bytes()
            .await?
            == next.as_bytes()
    );

    let path = format!("{bucket}/multipart");
    let (status, xml) = signed_request(
        &endpoint,
        "POST",
        &format!("{path}?uploads"),
        &[
            "x-amz-meta-ckp-data: checkpoint-2",
            "Content-Type: text/plain",
        ],
    )?;
    ensure!(
        status == 200,
        "CreateMultipartUpload failed: {status}: {xml}"
    );
    let upload_id = xml_tag(&xml, "UploadId").ok_or_else(|| anyhow::anyhow!("no upload ID"))?;
    let mut tags = Vec::new();
    for (n, text) in [(1, "alpha"), (2, "beta")] {
        let (status, headers) = signed_headers(
            &other,
            "PUT",
            &format!("{path}?partNumber={n}&uploadId={upload_id}"),
            &[],
            Some(text),
        )?;
        ensure!(status == 200, "UploadPart failed: {status}: {headers}");
        let tag = header_value(&headers, "ETag")
            .ok_or_else(|| anyhow::anyhow!("part {n} has no ETag"))?;
        let want = pgvs3::db::hex(&Md5::digest(text.as_bytes()));
        ensure!(tag == format!("\"{want}\""), "unexpected part ETag: {tag}");
        tags.push(tag.to_owned());
    }
    let manifest = format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{}</ETag></Part><Part><PartNumber>2</PartNumber><ETag>{}</ETag></Part></CompleteMultipartUpload>", tags[0], tags[1]);
    let mut md5_of_parts = Md5::new();
    md5_of_parts.update(Md5::digest(b"alpha"));
    md5_of_parts.update(Md5::digest(b"beta"));
    let final_tag = format!("{}-2", pgvs3::db::hex(&md5_of_parts.finalize()));
    let (status, body) = signed_request_body(
        &endpoint,
        "POST",
        &format!("{path}?uploadId={upload_id}"),
        &["Content-Type: application/xml"],
        Some(&manifest),
    )?;
    ensure!(
        status == 200 && xml_tag(&body, "ETag") == Some(format!("\"{final_tag}\"").as_str()),
        "CompleteMultipartUpload failed: {status}: {body}"
    );
    let (status, headers) = signed_headers(&other, "HEAD", &path, &[], None)?;
    ensure!(
        status == 200
            && header_value(&headers, "ETag") == Some(format!("\"{final_tag}\"").as_str()),
        "multipart HEAD failed: {status}: {headers}"
    );
    ensure!(header_value(&headers, "x-amz-meta-ckp-data") == Some("checkpoint-2"));
    ensure!(
        store(&other, &bucket)?
            .get(&Path::from("multipart"))
            .await?
            .bytes()
            .await?
            == b"alphabeta"[..]
    );

    // S3 multipart integrity path: CRC32 declared at Create, echoed by every
    // UploadPart response and repeated per part at Complete.
    use s3s::checksum::ChecksumHasher;
    use s3s::crypto::{Checksum as _, Crc32};
    let crc = |text: &str| {
        let mut hasher = ChecksumHasher {
            crc32: Some(Crc32::new()),
            ..Default::default()
        };
        hasher.update(text.as_bytes());
        hasher.finalize().checksum_crc32.unwrap()
    };
    let path = format!("{bucket}/checksummed");
    let (status, xml) = signed_request(
        &endpoint,
        "POST",
        &format!("{path}?uploads"),
        &["x-amz-checksum-algorithm: CRC32"],
    )?;
    ensure!(status == 200, "checksummed Create failed: {status}: {xml}");
    let upload_id = xml_tag(&xml, "UploadId").ok_or_else(|| anyhow::anyhow!("no upload ID"))?;
    let (status, body) = signed_request_body(
        &other,
        "PUT",
        &format!("{path}?partNumber=1&uploadId={upload_id}"),
        &[],
        Some("gamma"),
    )?;
    ensure!(
        status == 400 && body.contains("InvalidRequest"),
        "part without the upload's checksum was accepted: {status}: {body}"
    );
    let mut parts = Vec::new();
    for (n, text) in [(1, "gamma"), (2, "delta")] {
        let sum = crc(text);
        let (status, headers) = signed_headers(
            &other,
            "PUT",
            &format!("{path}?partNumber={n}&uploadId={upload_id}"),
            &[&format!("x-amz-checksum-crc32: {sum}")],
            Some(text),
        )?;
        ensure!(
            status == 200,
            "checksummed UploadPart failed: {status}: {headers}"
        );
        ensure!(
            header_value(&headers, "x-amz-checksum-crc32") == Some(sum.as_str()),
            "UploadPart did not echo its CRC32: {headers}"
        );
        let tag = header_value(&headers, "ETag")
            .unwrap_or_default()
            .to_owned();
        parts.push((n, tag, sum));
    }
    let manifest = |sums: &[&str]| {
        let body: String = parts
            .iter()
            .zip(sums)
            .map(|((n, tag, _), sum)| {
                let sum = if sum.is_empty() {
                    String::new()
                } else {
                    format!("<ChecksumCRC32>{sum}</ChecksumCRC32>")
                };
                format!("<Part><PartNumber>{n}</PartNumber><ETag>{tag}</ETag>{sum}</Part>")
            })
            .collect();
        format!("<CompleteMultipartUpload>{body}</CompleteMultipartUpload>")
    };
    for (sums, what) in [
        (["", ""], "missing"),
        ([parts[1].2.as_str(), parts[0].2.as_str()], "swapped"),
    ] {
        let (status, body) = signed_request_body(
            &endpoint,
            "POST",
            &format!("{path}?uploadId={upload_id}"),
            &["Content-Type: application/xml"],
            Some(&manifest(&sums)),
        )?;
        ensure!(
            status == 400 && body.contains("InvalidPart"),
            "{what} part checksums completed: {status}: {body}"
        );
    }
    let (status, body) = signed_request_body(
        &endpoint,
        "POST",
        &format!("{path}?uploadId={upload_id}"),
        &["Content-Type: application/xml"],
        Some(&manifest(&[parts[0].2.as_str(), parts[1].2.as_str()])),
    )?;
    ensure!(
        status == 200,
        "checksummed Complete failed: {status}: {body}"
    );
    ensure!(
        store(&other, &bucket)?
            .get(&Path::from("checksummed"))
            .await?
            .bytes()
            .await?
            == b"gammadelta"[..]
    );
    Ok(())
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn supplied_checksums_are_verified_before_publication() -> Result<()> {
    use s3s::checksum::ChecksumHasher;
    use s3s::crypto::{Checksum as _, Crc32, Md5};

    let (store, _) = endpoints()?;
    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let key = format!("{}/checksums", prefix());
    let path = Path::from(key.clone());
    let result: Result<()> = async {
        store
            .put(&path, Bytes::from_static(b"original").into())
            .await?;
        let bad = ["Content-MD5: AAAAAAAAAAAAAAAAAAAAAA=="];
        let (status, body) = signed_request_body(
            &endpoint,
            "PUT",
            &format!("pgvs3-contract/{key}"),
            &bad,
            Some("replacement"),
        )?;
        ensure!(
            status == 400 && body.contains("BadDigest"),
            "{status}: {body}"
        );
        ensure!(store.get(&path).await?.bytes().await? == b"original"[..]);
        let (status, body) = signed_request_body(
            &endpoint,
            "PUT",
            &format!("pgvs3-contract/{key}"),
            &["Content-Encoding: gzip"],
            Some("replacement"),
        )?;
        ensure!(
            status == 501 && body.contains("NotImplemented"),
            "{status}: {body}"
        );
        ensure!(store.get(&path).await?.bytes().await? == b"original"[..]);

        let mut hasher = ChecksumHasher {
            md5: Some(Md5::new()),
            crc32: Some(Crc32::new()),
            ..Default::default()
        };
        hasher.update(b"replacement");
        let sums = hasher.finalize();
        let headers = [
            format!("Content-MD5: {}", sums.checksum_md5.unwrap()),
            format!("x-amz-checksum-crc32: {}", sums.checksum_crc32.unwrap()),
        ];
        let (status, body) = signed_request_body(
            &endpoint,
            "PUT",
            &format!("pgvs3-contract/{key}"),
            &[&headers[0], &headers[1]],
            Some("replacement"),
        )?;
        ensure!(status == 200, "{status}: {body}");
        ensure!(store.get(&path).await?.bytes().await? == b"replacement"[..]);

        let mut upload = store.put_multipart(&path).await?;
        let check_part: Result<()> = async {
            let conn = pool().await?.get().await?;
            let upload_id: String = conn
                .query_typed_one(
                    "SELECT upload_id FROM s3p.uploads WHERE bucket = $1 AND key = $2",
                    &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
                )
                .await?
                .try_get(0)?;
            let (status, body) = signed_request_body(
                &endpoint,
                "PUT",
                &format!("pgvs3-contract/{key}?partNumber=1&uploadId={upload_id}"),
                &["x-amz-checksum-crc32: AAAAAA=="],
                Some("part-data"),
            )?;
            ensure!(
                status == 400 && body.contains("BadDigest"),
                "{status}: {body}"
            );
            let staged: i64 = conn
                .query_typed_one(
                    "SELECT count(*) FROM s3p.upload_parts WHERE upload_id = $1",
                    &[(&upload_id, Type::TEXT)],
                )
                .await?
                .try_get(0)?;
            ensure!(staged == 0, "bad multipart checksum published a part");
            let (status, body) = signed_request_body(
                &endpoint,
                "POST",
                &format!("pgvs3-contract/{key}?uploadId={upload_id}"),
                &[
                    "Content-Type: application/xml",
                    "x-amz-checksum-sha256: AAAAAA==",
                ],
                Some("<CompleteMultipartUpload/>"),
            )?;
            ensure!(
                status == 501 && body.contains("NotImplemented"),
                "{status}: {body}"
            );
            Ok(())
        }
        .await;
        let aborted = upload.abort().await;
        check_part?;
        aborted?;
        Ok(())
    }
    .await;
    let _ = store.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn multipart_completion_and_abort_manage_staged_rows() -> Result<()> {
    let (a, b) = endpoints()?;
    let key = format!("{}/multipart", prefix());
    let path = Path::from(key.clone());
    let part1 = Bytes::from(vec![0x33; 5 * 1024 * 1024 + 1]);
    let part2 = Bytes::from(vec![0x44; 8120 * 2 + 7]);

    let result: Result<()> = async {
        let mut upload = a.put_multipart(&path).await?;
        let first = upload.put_part(part1.clone().into());
        let second = upload.put_part(part2.clone().into());
        futures::future::try_join(first, second).await?;
        ensure!(
            matches!(
                b.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "multipart parts appeared before Complete"
        );
        upload.complete().await?;
        let meta = b.head(&path).await?;
        ensure!(meta.size == (part1.len() + part2.len()) as u64);
        let off = part1.len() - 10;
        let got = b.get_range(&path, off as u64..(off + 20) as u64).await?;
        ensure!(got[..10] == part1[off..] && got[10..] == part2[..10]);

        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT parts FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let parts: Vec<i64> = row.try_get(0)?;
        ensure!(
            parts.len() == 2,
            "multipart object does not reference both parts"
        );
        a.delete(&path).await?;
        for id in parts {
            queued_then_reaped(&pool().await?, id).await?;
        }

        let mut abandoned = a.put_multipart(&path).await?;
        abandoned.put_part(part1.into()).await?;
        let row = conn
            .query_typed_one(
                "SELECT p.file_id FROM s3p.uploads u JOIN s3p.upload_parts p USING (upload_id) \
                 WHERE u.bucket = $1 AND u.key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let staged_id: i64 = row.try_get(0)?;
        abandoned.abort().await?;
        queued_then_reaped(&pool().await?, staged_id).await?;
        Ok(())
    }
    .await;

    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn missing_upload_cannot_complete_an_existing_object() -> Result<()> {
    let (a, _) = endpoints()?;
    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let path = Path::from(format!("{}/already-present", prefix()));
    let result: Result<()> = async {
        a.put(&path, Bytes::from_static(b"original").into()).await?;
        let (status, body) = signed_request_body(
            &endpoint,
            "POST",
            &format!("pgvs3-contract/{path}?uploadId=missing-upload"),
            &["Content-Type: application/xml"],
            Some("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"unknown\"</ETag></Part></CompleteMultipartUpload>"),
        )?;
        ensure!(status == 404 && body.contains("NoSuchUpload"), "{status}: {body}");
        let (status, body) = signed_request_body(
            &endpoint,
            "PUT",
            &format!("pgvs3-contract/{path}?partNumber=1&uploadId=missing-upload"),
            &[],
            Some("orphan attempt"),
        )?;
        ensure!(status == 404 && body.contains("NoSuchUpload"), "{status}: {body}");
        ensure!(a.get(&path).await?.bytes().await? == b"original"[..]);
        Ok(())
    }
    .await;
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn concurrent_uploads_of_one_key_remain_independent() -> Result<()> {
    let (a, b) = endpoints()?;
    let path = Path::from(format!("{}/parallel-uploads", prefix()));
    let mut first = a.put_multipart(&path).await?;
    let mut second = b.put_multipart(&path).await?;
    let result: Result<()> = async {
        let conn = pool().await?.get().await?;
        let count: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.uploads WHERE bucket = $1 AND key = $2",
                &[
                    (&"pgvs3-contract", Type::TEXT),
                    (&path.as_ref(), Type::TEXT),
                ],
            )
            .await?
            .try_get(0)?;
        ensure!(count == 2, "two Creates shared one upload ID");
        first.put_part(Bytes::from_static(b"winner").into()).await?;
        second.put_part(Bytes::from_static(b"loser").into()).await?;
        first.complete().await?;
        second.abort().await?;
        ensure!(b.get(&path).await?.bytes().await? == b"winner"[..]);
        Ok(())
    }
    .await;
    let _ = first.abort().await;
    let _ = second.abort().await;
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn completion_can_select_only_uploaded_parts() -> Result<()> {
    let (a, _) = endpoints()?;
    let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
    let path = Path::from(format!("{}/selected-parts", prefix()));
    let mut upload = a.put_multipart(&path).await?;
    let result: Result<()> = async {
        upload.put_part(Bytes::from_static(b"unused").into()).await?;
        upload.put_part(Bytes::from_static(b"selected").into()).await?;
        let conn = pool().await?.get().await?;
        let rows = conn
            .query_typed(
                "SELECT u.upload_id, p.part_no, p.file_id, p.md5 \
                 FROM s3p.uploads u JOIN s3p.upload_parts p USING (upload_id) \
                 WHERE u.bucket = $1 AND u.key = $2 ORDER BY p.part_no",
                &[(&"pgvs3-contract", Type::TEXT), (&path.as_ref(), Type::TEXT)],
            )
            .await?;
        ensure!(rows.len() == 2);
        let id: String = rows[1].try_get(0)?;
        let unused_id: i64 = rows[0].try_get(2)?;
        let tag = pgvs3::db::hex(&rows[1].try_get::<_, Vec<u8>>(3)?);
        let manifest = format!(
            "<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>\"{tag}\"</ETag></Part></CompleteMultipartUpload>"
        );
        let (status, body) = signed_request_body(
            &endpoint,
            "POST",
            &format!("pgvs3-contract/{path}?uploadId={id}"),
            &["Content-Type: application/xml"],
            Some(&manifest),
        )?;
        ensure!(status == 200, "{status}: {body}");
        ensure!(a.get(&path).await?.bytes().await? == b"selected"[..]);
        queued_then_reaped(&pool().await?, unused_id).await?;
        Ok(())
    }
    .await;
    let _ = upload.abort().await;
    let _ = a.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn expired_upload_is_atomically_unavailable_and_reaped() -> Result<()> {
    let (store, other_gateway) = endpoints()?;
    let key = format!("{}/expired", prefix());
    let path = Path::from(key.clone());
    let mut upload = store.put_multipart(&path).await?;

    let result: Result<()> = async {
        upload
            .put_part(Bytes::from(vec![0x45; 5 * 1024 * 1024 + 17]).into())
            .await?;
        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "SELECT u.upload_id, p.file_id FROM s3p.uploads u \
                 JOIN s3p.upload_parts p USING (upload_id) \
                 WHERE u.bucket = $1 AND u.key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let upload_id: String = row.try_get(0)?;
        let file_id: i64 = row.try_get(1)?;
        let original_rows: i64 = conn
            .query_typed_one(
                "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                &[(&file_id, Type::INT8)],
            )
            .await?
            .try_get(0)?;
        ensure!(original_rows > 32, "the test needs several GC batches");
        let removed: i32 = conn.query_typed_one(
            "SELECT s3p.expire_uploads(interval '0 seconds', $1, $2)",
            &[(&1i32, Type::INT4), (&"pgvs3-contract", Type::TEXT)],
        ).await?.try_get(0)?;
        ensure!(removed == 1, "expired upload not removed in one transaction");
        let still_queued: bool = conn.query_typed_one(
            "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
            &[(&file_id, Type::INT8)],
        ).await?.try_get(0)?;
        ensure!(still_queued, "expired part wasn't enqueued");
        let remaining: i64 = conn.query_typed_one(
            "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
            &[(&file_id, Type::INT8)],
        ).await?.try_get(0)?;
        ensure!(remaining == original_rows, "expiry deleted rows outside garbage collector");
        let reaped: i32 = conn.query_typed_one("SELECT s3p.reap_garbage(32)", &[]).await?.try_get(0)?;
        ensure!(reaped <= 32, "garbage collector exceeded its row budget");
        let endpoint = std::env::var("PGVS3_TEST_ENDPOINT_A")?;
        let (status, body) = signed_request_body(
            &endpoint, "POST", &format!("pgvs3-contract/{key}?uploadId={upload_id}"),
            &["Content-Type: application/xml"],
            Some("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"old\"</ETag></Part></CompleteMultipartUpload>"),
        )?;
        ensure!(status == 404 && body.contains("NoSuchUpload"), "Complete after expiry: {body}");
        queued_then_reaped(&pool().await?, file_id).await?;
        ensure!(
            matches!(
                other_gateway.head(&path).await,
                Err(object_store::Error::NotFound { .. })
            ),
            "uncompleted upload became visible through S3"
        );
        Ok(())
    }
    .await;

    let _ = upload.abort().await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn scheduled_expiry_removes_old_empty_uploads() -> Result<()> {
    let (store, _) = endpoints()?;
    let key = format!("{}/cron-expiry", prefix());
    let path = Path::from(key.clone());
    let mut upload = store.put_multipart(&path).await?;
    let result: Result<()> = async {
        let conn = pool().await?.get().await?;
        let row = conn
            .query_typed_one(
                "UPDATE s3p.uploads SET created_at = now() - interval '25 hours' \
                 WHERE bucket = $1 AND key = $2 RETURNING upload_id",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?;
        let upload_id: String = row.try_get(0)?;
        for _ in 0..45 {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            let exists = conn
                .query_typed_opt(
                    "SELECT 1 FROM s3p.uploads WHERE upload_id = $1",
                    &[(&upload_id, Type::TEXT)],
                )
                .await?
                .is_some();
            if !exists {
                return Ok(());
            }
        }
        anyhow::bail!("pg_cron did not expire the old empty upload")
    }
    .await;
    let _ = upload.abort().await;
    result
}

#[tokio::test]
#[ignore = "needs PostgreSQL and two gateways; run just contract"]
async fn garbage_collector_refuses_a_live_file() -> Result<()> {
    let (store, _) = endpoints()?;
    let key = format!("{}/live-gc", prefix());
    let path = Path::from(key.clone());
    let result: Result<()> = async {
        store
            .put(&path, Bytes::from_static(b"still-live").into())
            .await?;
        let pool = pool().await?;
        let mut conn = pool.get().await?;
        let tx = conn.transaction().await?;
        let id: i64 = tx
            .query_typed_one(
                "SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&key, Type::TEXT)],
            )
            .await?
            .try_get(0)?;
        tx.query_typed(
            "INSERT INTO s3p.garbage (file_id, queued_at) VALUES ($1, now() - interval '100 years')",
            &[(&id, Type::INT8)],
        )
        .await?;
        ensure!(
            tx.query_typed_one("SELECT s3p.reap_garbage(1)", &[])
                .await
                .is_err(),
            "garbage collector deleted a referenced file"
        );
        drop(tx); // roll back the artificial queue entry
        ensure!(store.get(&path).await?.bytes().await? == b"still-live"[..]);
        Ok(())
    }
    .await;
    let _ = store.delete(&path).await;
    result
}

#[tokio::test]
#[ignore = "opt-in large-object churn; run just kind-churn"]
async fn sustained_churn_and_db_reclaim() -> Result<()> {
    let rounds: usize = std::env::var("PGVS3_CHURN_ROUNDS")
        .unwrap_or_else(|_| "64".to_owned())
        .parse()?;
    let mib: usize = std::env::var("PGVS3_CHURN_MIB")
        .unwrap_or_else(|_| "32".to_owned())
        .parse()?;
    ensure!((1..=128).contains(&rounds) && (1..=64).contains(&mib));
    let (a, b) = endpoints()?;
    let pool = pool().await?;
    let run = prefix();
    let hot = Path::from(format!("{run}/hot"));
    let stable = Path::from(format!("{run}/stable"));
    let partial = Path::from(format!("{run}/partial"));
    let mut bytes = vec![0; mib * 1024 * 1024];
    let mut state = 0x5EEDu64;
    for chunk in bytes.chunks_mut(8) {
        state = state.wrapping_add(0x9E3779B97F4A7C15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
        let value = (value ^ (value >> 31)).to_le_bytes();
        chunk.copy_from_slice(&value[..chunk.len()]);
    }
    let stable_body = Bytes::from(vec![0xa5; 1024 * 1024]);
    a.put(&stable, stable_body.clone().into()).await?;
    let before = chunk_maintenance(&pool).await?;
    let running = Arc::new(AtomicBool::new(true));
    let reading = running.clone();
    let reader_path = stable.clone();
    let reader = tokio::spawn(async move {
        let mut reads = 0usize;
        while reading.load(Ordering::Relaxed) {
            let got = b.get_range(&reader_path, 0..256 * 1024).await?;
            ensure!(got.as_ref() == &stable_body[..256 * 1024]);
            reads += 1;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok::<_, anyhow::Error>(reads)
    });

    let work: Result<()> = async {
        for round in 0..rounds {
            let conn = pool.get().await?;
            let old = conn.query_typed_opt(
                "SELECT file_id FROM s3p.objects WHERE bucket = $1 AND key = $2",
                &[(&"pgvs3-contract", Type::TEXT), (&hot.as_ref(), Type::TEXT)],
            ).await?.map(|r| r.try_get::<_, i64>(0)).transpose()?;
            if round % 8 == 7 {
                a.delete(&hot).await?;
            }
            bytes[0] = round as u8;
            a.put(&hot, Bytes::copy_from_slice(&bytes).into()).await?;
            ensure!(a.head(&hot).await?.size == bytes.len() as u64);
            if let Some(id) = old {
                let count: i64 = conn.query_typed_one(
                    "SELECT count(*) FROM s3p.chunks WHERE file_id = $1",
                    &[(&id, Type::INT8)],
                ).await?.try_get(0)?;
                let queued: bool = conn.query_typed_one(
                    "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
                    &[(&id, Type::INT8)],
                ).await?.try_get(0)?;
                ensure!(queued || count == 0, "overwrite/delete left {count} unqueued rows for {id}");
            }
            if round % 8 == 7 {
                let mut upload = a.put_multipart(&partial).await?;
                let part: Result<i64> = async {
                    upload.put_part(Bytes::from(vec![round as u8; 5 * 1024 * 1024 + 17]).into()).await?;
                    Ok(conn.query_typed_one(
                        "SELECT p.file_id FROM s3p.uploads u JOIN s3p.upload_parts p USING (upload_id) \
                         WHERE u.bucket = $1 AND u.key = $2",
                        &[(&"pgvs3-contract", Type::TEXT), (&partial.as_ref(), Type::TEXT)],
                    ).await?.try_get(0)?)
                }.await;
                let aborted = upload.abort().await;
                let id = part?;
                aborted?;
                let queued: bool = conn.query_typed_one(
                    "SELECT EXISTS (SELECT 1 FROM s3p.garbage WHERE file_id = $1)",
                    &[(&id, Type::INT8)],
                ).await?.try_get(0)?;
                ensure!(queued, "aborted part was not queued");
            }
        }
        Ok(())
    }.await;
    running.store(false, Ordering::Relaxed);
    let read_result = reader.await;
    let _ = a.delete(&hot).await;
    let _ = a.delete(&stable).await;
    let _ = a.delete(&partial).await;
    work?;
    let reads = read_result??;
    ensure!(reads > 0, "no reads overlapped churn");
    let after = chunk_maintenance(&pool).await?;
    // Draining checks physical reclamation, not throughput under churn.
    for _ in 0..256 {
        let conn = pool.get().await?;
        let pending: i64 = conn
            .query_typed_one("SELECT count(*) FROM s3p.garbage", &[])
            .await?
            .try_get(0)?;
        if pending == 0 {
            break;
        }
        conn.query_typed_one("SELECT s3p.reap_garbage()", &[])
            .await?;
    }
    let pending: i64 = pool
        .get()
        .await?
        .query_typed_one("SELECT count(*) FROM s3p.garbage", &[])
        .await?
        .try_get(0)?;
    ensure!(pending == 0, "churn left {pending} queued files");
    tokio::time::sleep(std::time::Duration::from_secs(90)).await;
    let settled = chunk_maintenance(&pool).await?;
    println!(
        "{{\"qa\":\"churn\",\"rounds\":{rounds},\"object_mib\":{mib},\"reads_verified\":{reads},\"dead_before\":{},\"dead_after\":{},\"dead_settled\":{},\"autovac_before\":{},\"autovac_settled\":{},\"chunks_mib_before\":{},\"chunks_mib_settled\":{}}}",
        before.0, after.0, settled.0, before.1, settled.1,
        before.2 / (1024 * 1024), settled.2 / (1024 * 1024),
    );
    Ok(())
}
