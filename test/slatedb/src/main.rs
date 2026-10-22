use std::{env, sync::Arc};

use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::Db;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let endpoint = env::var("S3_ENDPOINT")?;
    let bucket = env::var("S3_BUCKET")?;
    let access = env::var("S3_ACCESS_KEY")?;
    let secret = env::var("S3_SECRET_KEY")?;
    let path = env::var("SLATEDB_PATH")?;

    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_endpoint(&endpoint)
        .with_allow_http(endpoint.starts_with("http://"))
        .with_access_key_id(access)
        .with_secret_access_key(secret)
        .with_region("us-east-1")
        .with_virtual_hosted_style_request(false)
        .build()?;

    let db = Db::open(path, Arc::new(store)).await?;
    db.put(b"pgvs3", b"slatedb").await?;
    assert_eq!(db.get(b"pgvs3").await?.as_deref(), Some(b"slatedb".as_slice()));
    db.flush().await?;
    db.close().await?;
    Ok(())
}
