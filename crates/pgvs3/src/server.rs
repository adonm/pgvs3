//! The S3 service: `s3s` REST/SigV4 layer over `db::` PostgreSQL storage.
//! Only HEAD, GET(+Range), PUT, DELETE, multipart and bucket operations are
//! implemented; everything else stays `NotImplemented` (s3s trait defaults).

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use hyper::service::Service as HyperService;
use hyper::{Request, Response};
use s3s::auth::SimpleAuth;
use s3s::checksum::ChecksumHasher;
use s3s::crypto::Checksum as _;
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, CommonPrefix,
    CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketOutput,
    CreateMultipartUploadInput, CreateMultipartUploadOutput, DeleteBucketOutput,
    DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag,
    ETagCondition, GetObjectInput, GetObjectOutput, HeadBucketOutput, HeadObjectInput,
    HeadObjectOutput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
    PutObjectInput, PutObjectOutput, Range, StreamingBlob, Timestamp, UploadPartInput,
    UploadPartOutput,
};
use s3s::service::{S3Service, S3ServiceBuilder};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::db;

/// Stateless over PostgreSQL: multipart upload state lives in `s3p.uploads`, so
/// any gateway instance can serve any request of any upload.
#[derive(Clone)]
pub struct PgS3 {
    pool: db::Pool,
}

fn etag(value: &str) -> Option<ETag> {
    format!("\"{value}\"").parse().ok()
}

fn valid_etag(value: &str) -> bool {
    let (digest, count) = value.split_once('-').unwrap_or((value, ""));
    digest.len() == 32
        && (count.is_empty()
            || count
                .parse::<u16>()
                .is_ok_and(|n| (1..=10_000).contains(&n)))
        && digest.bytes().all(|b| b.is_ascii_hexdigit())
}

/// The one checksum a CompletedPart may carry, as `(algorithm, value)`.
fn part_checksum(part: &s3s::dto::CompletedPart) -> S3Result<Option<(String, String)>> {
    if part.checksum_md5.is_some()
        || part.checksum_sha512.is_some()
        || part.checksum_xxhash64.is_some()
        || part.checksum_xxhash3.is_some()
        || part.checksum_xxhash128.is_some()
    {
        return Err(s3_error!(NotImplemented));
    }
    let given: Vec<_> = [
        ("CRC32", &part.checksum_crc32),
        ("CRC32C", &part.checksum_crc32c),
        ("CRC64NVME", &part.checksum_crc64nvme),
        ("SHA1", &part.checksum_sha1),
        ("SHA256", &part.checksum_sha256),
    ]
    .into_iter()
    .filter_map(|(algorithm, value)| value.clone().map(|v| (algorithm.to_owned(), v)))
    .collect();
    match given.len() {
        0 => Ok(None),
        1 => Ok(given.into_iter().next()),
        _ => Err(s3_error!(InvalidRequest)),
    }
}

/// Custom S3 metadata is bounded and preserved atomically with its object.
fn validated_metadata(meta: Option<HashMap<String, String>>) -> S3Result<Vec<String>> {
    let mut sorted = Vec::new();
    let mut total = 0;
    for (name, value) in meta.unwrap_or_default() {
        let name = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > 128
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || value.bytes().any(|b| b < 0x20 || b == 0x7f)
        {
            return Err(s3_error!(InvalidRequest));
        }
        total += name.len() + value.len();
        if total > 2048 || sorted.len() >= 64 {
            return Err(s3_error!(InvalidRequest));
        }
        sorted.push((name, value));
    }
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    if sorted.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(s3_error!(InvalidRequest));
    }
    Ok(sorted
        .into_iter()
        .flat_map(|(key, value)| [key, value])
        .collect())
}

fn metadata_map(meta: &[String]) -> Option<HashMap<String, String>> {
    (!meta.is_empty()).then(|| {
        meta.as_chunks::<2>()
            .0
            .iter()
            .map(|[name, value]| (name.clone(), value.clone()))
            .collect()
    })
}

fn validated_content_type(value: Option<String>) -> S3Result<String> {
    let value = value.unwrap_or_else(|| "application/octet-stream".to_owned());
    if value.is_empty() || value.len() > 1024 || value.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(s3_error!(InvalidRequest));
    }
    Ok(value)
}

fn validate_key(key: &str) -> S3Result<()> {
    if key.is_empty() || key.len() > 1024 {
        return Err(s3_error!(InvalidRequest));
    }
    Ok(())
}

fn internal(e: impl std::fmt::Display) -> s3s::S3Error {
    eprintln!("pgvs3 internal error: {e}");
    s3_error!(InternalError)
}

/// Only compute extra digests the client actually supplied. s3s verifies
/// signed chunks, but exposes unsigned checksum trailers without checking them.
struct UploadIntegrity {
    expected: Vec<(String, String)>,
    trailing: Vec<String>,
    handle: Option<s3s::TrailingHeaders>,
    hasher: ChecksumHasher,
}

impl UploadIntegrity {
    fn new<T>(req: &S3Request<T>) -> S3Result<Self> {
        let mut expected = Vec::new();
        let mut trailing = Vec::new();
        for (name, value) in &req.headers {
            let name = name.as_str();
            if name == "content-md5"
                || (name.starts_with("x-amz-checksum-") && name != "x-amz-checksum-algorithm")
            {
                if !Self::supported(name) {
                    return Err(s3_error!(NotImplemented));
                }
                expected.push((
                    name.to_owned(),
                    value
                        .to_str()
                        .map_err(|_| s3_error!(InvalidRequest))?
                        .to_owned(),
                ));
            }
        }
        if let Some(declared) = req.headers.get("x-amz-trailer") {
            for name in declared
                .to_str()
                .map_err(|_| s3_error!(InvalidRequest))?
                .split(',')
            {
                let name = name.trim().to_ascii_lowercase();
                if !Self::supported(&name) || name == "content-md5" {
                    return Err(s3_error!(NotImplemented));
                }
                trailing.push(name);
            }
            if req.trailing_headers.is_none() || trailing.is_empty() {
                return Err(s3_error!(InvalidRequest));
            }
        }
        for algo in ["x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"] {
            let Some(algo) = req.headers.get(algo) else {
                continue;
            };
            let name = format!(
                "x-amz-checksum-{}",
                algo.to_str()
                    .map_err(|_| s3_error!(InvalidRequest))?
                    .to_ascii_lowercase()
            );
            if !expected.iter().any(|(n, _)| n == &name) && !trailing.contains(&name) {
                return Err(s3_error!(InvalidRequest));
            }
        }
        let mut hasher = ChecksumHasher::default();
        for name in expected.iter().map(|(n, _)| n).chain(trailing.iter()) {
            match name.as_str() {
                "content-md5" | "x-amz-checksum-md5" => hasher.md5 = Some(s3s::crypto::Md5::new()),
                "x-amz-checksum-crc32" => hasher.crc32 = Some(s3s::crypto::Crc32::new()),
                "x-amz-checksum-crc32c" => hasher.crc32c = Some(s3s::crypto::Crc32c::new()),
                "x-amz-checksum-crc64nvme" => {
                    hasher.crc64nvme = Some(s3s::crypto::Crc64Nvme::new())
                }
                "x-amz-checksum-sha1" => hasher.sha1 = Some(s3s::crypto::Sha1::new()),
                "x-amz-checksum-sha256" => hasher.sha256 = Some(s3s::crypto::Sha256::new()),
                "x-amz-checksum-sha512" => hasher.sha512 = Some(s3s::crypto::Sha512::new()),
                "x-amz-checksum-xxhash64" => hasher.xxhash64 = Some(s3s::crypto::XxHash64::new()),
                "x-amz-checksum-xxhash3" => hasher.xxhash3 = Some(s3s::crypto::XxHash3::new()),
                "x-amz-checksum-xxhash128" => {
                    hasher.xxhash128 = Some(s3s::crypto::XxHash128::new())
                }
                _ => unreachable!("validated checksum name"),
            }
        }
        Ok(Self {
            expected,
            trailing,
            handle: req.trailing_headers.clone(),
            hasher,
        })
    }

    fn supported(name: &str) -> bool {
        matches!(
            name,
            "content-md5"
                | "x-amz-checksum-md5"
                | "x-amz-checksum-crc32"
                | "x-amz-checksum-crc32c"
                | "x-amz-checksum-crc64nvme"
                | "x-amz-checksum-sha1"
                | "x-amz-checksum-sha256"
                | "x-amz-checksum-sha512"
                | "x-amz-checksum-xxhash64"
                | "x-amz-checksum-xxhash3"
                | "x-amz-checksum-xxhash128"
        )
    }

    /// The verified `(name, value)` checksums, or None on any mismatch.
    fn verify(mut self) -> Option<Vec<(String, String)>> {
        if let Some(handle) = self.handle.take() {
            let Some(headers) = handle.take() else {
                return (self.trailing.is_empty() && self.matches_computed())
                    .then_some(self.expected);
            };
            if !self.trailing.is_empty() {
                for name in &self.trailing {
                    let value = headers.get(name).and_then(|v| v.to_str().ok())?;
                    self.expected.push((name.clone(), value.to_owned()));
                }
            }
            // Do not silently ignore any undeclared checksum trailer.
            if headers.keys().any(|h| {
                h.as_str().starts_with("x-amz-checksum-")
                    && !self.trailing.iter().any(|n| n == h.as_str())
            }) {
                return None;
            }
        }
        self.matches_computed().then_some(self.expected)
    }

    fn matches_computed(&mut self) -> bool {
        let computed = std::mem::take(&mut self.hasher).finalize();
        self.expected.iter().all(|(name, want)| {
            let actual = match name.as_str() {
                "content-md5" | "x-amz-checksum-md5" => computed.checksum_md5.as_deref(),
                "x-amz-checksum-crc32" => computed.checksum_crc32.as_deref(),
                "x-amz-checksum-crc32c" => computed.checksum_crc32c.as_deref(),
                "x-amz-checksum-crc64nvme" => computed.checksum_crc64nvme.as_deref(),
                "x-amz-checksum-sha1" => computed.checksum_sha1.as_deref(),
                "x-amz-checksum-sha256" => computed.checksum_sha256.as_deref(),
                "x-amz-checksum-sha512" => computed.checksum_sha512.as_deref(),
                "x-amz-checksum-xxhash64" => computed.checksum_xxhash64.as_deref(),
                "x-amz-checksum-xxhash3" => computed.checksum_xxhash3.as_deref(),
                "x-amz-checksum-xxhash128" => computed.checksum_xxhash128.as_deref(),
                _ => None,
            };
            actual == Some(want.as_str())
        })
    }
}

enum UploadError {
    BadDigest,
    Failed(anyhow::Error),
}

async fn ingest_verified(
    writer: db::ChunkWriter,
    mut body: StreamingBlob,
    mut integrity: UploadIntegrity,
) -> std::result::Result<crate::ingest::Published, UploadError> {
    while let Some(chunk) = body.next().await {
        match chunk {
            Ok(bytes) => {
                integrity.hasher.update(&bytes);
                if writer.push(bytes).await.is_err() {
                    return writer.finish(Vec::new()).await.map_err(UploadError::Failed);
                }
            }
            Err(e) => {
                writer.abort().await;
                return Err(UploadError::Failed(anyhow::anyhow!("request body: {e}")));
            }
        }
    }
    let Some(checksums) = integrity.verify() else {
        writer.abort().await;
        return Err(UploadError::BadDigest);
    };
    writer.finish(checksums).await.map_err(UploadError::Failed)
}

/// Upper bound (exclusive) for `key LIKE prefix%` as a range scan bound.
fn prefix_end(prefix: &str) -> String {
    let mut s = prefix.to_string();
    while let Some(last) = s.pop() {
        if let Some(next) = char::from_u32(last as u32 + 1) {
            s.push(next);
            return s;
        }
    }
    "\u{10FFFF}".to_owned()
}

/// Map the S3 Range header to `get()`'s (first, last, suffix) parameters.
fn range_params(range: Option<Range>) -> (i64, i64, i64) {
    match range {
        None => (0, -1, -1),
        Some(Range::Int { first, last }) => {
            (first as i64, last.map(|v| v as i64).unwrap_or(-1), -1)
        }
        Some(Range::Suffix { length }) => (0, -1, length as i64),
    }
}

/// One row of a `list_objects_v2` scan: push it onto `contents` or, past a
/// `delimiter`, onto `common`. Returns whether the listing is full; the caller
/// must only advance its cursor past a row that was handled.
fn listing_step(
    r: &db::Listed,
    prefix: &str,
    delimiter: &str,
    max: usize,
    contents: &mut Vec<Object>,
    common: &mut Vec<CommonPrefix>,
    seen: &mut BTreeSet<String>,
) -> bool {
    let rest = &r.key[prefix.len()..];
    if !delimiter.is_empty() {
        if let Some(idx) = rest.find(delimiter) {
            let cp = format!("{prefix}{}", &rest[..idx + delimiter.len()]);
            if seen.insert(cp.clone()) {
                if contents.len() + common.len() >= max {
                    return true;
                }
                common.push(CommonPrefix { prefix: Some(cp) });
            }
            return false;
        }
    }
    if contents.len() + common.len() >= max {
        return true;
    }
    contents.push(Object {
        key: Some(r.key.clone()),
        size: Some(r.size),
        e_tag: etag(&r.etag),
        last_modified: Some(Timestamp::from(r.created_at)),
        ..Default::default()
    });
    false
}

#[async_trait::async_trait]
impl S3 for PgS3 {
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let Some(meta) = db::meta_fresh(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?
        else {
            return Err(
                if db::bucket_exists(&self.pool, &input.bucket)
                    .await
                    .map_err(internal)?
                {
                    s3_error!(NoSuchKey)
                } else {
                    s3_error!(NoSuchBucket)
                },
            );
        };
        let out = HeadObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(meta.size),
            e_tag: etag(&meta.etag),
            metadata: metadata_map(&meta.user_metadata),
            content_type: Some(meta.content_type),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let ranged = input.range.is_some();
        let (first, last, suffix) = range_params(input.range);
        // One round trip: metadata + a stream of exactly the requested bytes.
        let Some((meta, body)) = db::get_body(
            self.pool.clone(),
            input.bucket.clone(),
            input.key,
            first,
            last,
            suffix,
        )
        .await
        .map_err(internal)?
        else {
            return Err(
                if db::bucket_exists(&self.pool, &input.bucket)
                    .await
                    .map_err(internal)?
                {
                    s3_error!(NoSuchKey)
                } else {
                    s3_error!(NoSuchBucket)
                },
            );
        };

        if meta.size > 0 && (meta.start > meta.end || meta.start >= meta.size) {
            return Err(s3_error!(InvalidRange));
        }
        let body_len = meta.len();
        let out = GetObjectOutput {
            accept_ranges: Some("bytes".to_owned()),
            body: Some(match body {
                db::PieceBody::OneShot(b) => StreamingBlob::from_bytes(b),
                db::PieceBody::Streamed(s) => StreamingBlob::wrap(s),
            }),
            content_length: Some(body_len),
            content_range: ranged
                .then(|| format!("bytes {}-{}/{}", meta.start, meta.end, meta.size)),
            content_type: Some(meta.content_type),
            e_tag: etag(&meta.etag),
            metadata: metadata_map(&meta.user_metadata),
            last_modified: Some(Timestamp::from(meta.created_at)),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let integrity = UploadIntegrity::new(&req)?;
        let mut input = req.input;
        validate_key(&input.key)?;
        if input.content_encoding.is_some()
            || input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.sse_customer_key_md5.is_some()
            || input.server_side_encryption.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        let user_metadata = validated_metadata(input.metadata.take())?;
        let content_type = validated_content_type(input.content_type.take())?;
        let condition = match (input.if_match.as_ref(), input.if_none_match.as_ref()) {
            (None, None) => db::PutCondition::Unconditional,
            (None, Some(ETagCondition::Any)) => db::PutCondition::IfAbsent,
            (Some(ETagCondition::ETag(ETag::Strong(value))), None) if valid_etag(value) => {
                db::PutCondition::IfMatch(value.to_owned())
            }
            _ => return Err(s3_error!(InvalidRequest)),
        };
        let body = input
            .body
            .take()
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        // Streams into its own COPY (never buffered whole); the writer hashes.
        let writer = db::ChunkWriter::start_object_if(
            self.pool.clone(),
            input.bucket.clone(),
            input.key,
            condition,
            user_metadata,
            content_type,
        );
        let published = match ingest_verified(writer, body, integrity).await {
            Ok(done) => done,
            Err(UploadError::BadDigest) => return Err(s3_error!(BadDigest)),
            Err(UploadError::Failed(e)) => {
                if e.downcast_ref::<db::PreconditionFailed>().is_some() {
                    return Err(s3_error!(PreconditionFailed));
                }
                if matches!(
                    e.downcast_ref::<db::MissingResource>(),
                    Some(db::MissingResource::Bucket)
                ) {
                    return Err(s3_error!(NoSuchBucket));
                }
                return Err(internal(e));
            }
        };
        Ok(S3Response::new(PutObjectOutput {
            e_tag: etag(&published.etag),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<s3s::dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let input = req.input;
        if input.if_match.is_some()
            || input.if_match_size.is_some()
            || input.if_match_last_modified_time.is_some()
            || input.version_id.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        if !db::bucket_exists(&self.pool, &input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        db::delete(&self.pool, &input.bucket, &input.key)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        if !db::bucket_exists(&self.pool, &input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        if input.delete.objects.len() > 1000 {
            return Err(s3_error!(InvalidRequest));
        }
        // This store is unversioned. Validate the whole batch before any
        // deletion so an unsupported conditional/versioned request cannot
        // partially delete it.
        for obj in &input.delete.objects {
            if obj.version_id.is_some()
                || obj.e_tag.is_some()
                || obj.last_modified_time.is_some()
                || obj.size.is_some()
            {
                return Err(s3_error!(NotImplemented));
            }
        }
        let mut deleted = Vec::new();
        for obj in input.delete.objects {
            db::delete(&self.pool, &input.bucket, &obj.key)
                .await
                .map_err(internal)?;
            if input.delete.quiet != Some(true) {
                deleted.push(DeletedObject {
                    key: Some(obj.key),
                    ..Default::default()
                });
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            ..Default::default()
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let input = req.input;
        validate_key(&input.key)?;
        let algorithm = input.checksum_algorithm.as_ref().map(|a| a.as_str());
        if algorithm.is_some_and(|a| !db::CHECKSUM_ALGORITHMS.contains(&a))
            || input
                .checksum_type
                .as_ref()
                .is_some_and(|t| algorithm.is_none() || t.as_str() != "COMPOSITE")
            || input.content_encoding.is_some()
            || input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.server_side_encryption.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        let user_metadata = validated_metadata(input.metadata)?;
        let content_type = validated_content_type(input.content_type)?;
        // Each Create starts an independent upload, including for a key that
        // already has another active upload.
        let id = db::create_upload(
            &self.pool,
            &input.bucket,
            &input.key,
            user_metadata,
            content_type,
            algorithm,
        )
        .await
        .map_err(internal)?
        .ok_or_else(|| s3_error!(NoSuchBucket))?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(input.bucket),
            key: Some(input.key),
            upload_id: Some(id),
            checksum_algorithm: input.checksum_algorithm,
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let integrity = UploadIntegrity::new(&req)?;
        let mut input = req.input;
        validate_key(&input.key)?;
        if input.sse_customer_algorithm.is_some()
            || input.sse_customer_key.is_some()
            || input.sse_customer_key_md5.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        if !(1..=10_000).contains(&input.part_number) {
            return Err(s3_error!(InvalidPart));
        }
        // Each part streams straight into its own COPY, in any arrival order
        // and in parallel with its siblings; a re-sent part replaces the
        // earlier attempt atomically.
        let writer = db::ChunkWriter::start_part(
            self.pool.clone(),
            input.upload_id.clone(),
            input.bucket,
            input.key,
            input.part_number,
        );
        let body = input
            .body
            .take()
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        let published = ingest_verified(writer, body, integrity)
            .await
            .map_err(|e| match e {
                UploadError::BadDigest => s3_error!(BadDigest),
                UploadError::Failed(e) if e.downcast_ref::<db::MissingChecksum>().is_some() => {
                    s3_error!(InvalidRequest)
                }
                UploadError::Failed(e)
                    if matches!(
                        e.downcast_ref::<db::MissingResource>(),
                        Some(db::MissingResource::Upload)
                    ) =>
                {
                    s3_error!(NoSuchUpload)
                }
                UploadError::Failed(e) => internal(e),
            })?;
        eprintln!(
            "pgvs3: upload_part {} no={} {size} bytes",
            input.upload_id,
            input.part_number,
            size = published.size
        );
        let mut out = UploadPartOutput {
            e_tag: etag(&published.etag),
            ..Default::default()
        };
        if let Some((algorithm, value)) = published.checksum {
            let field = match algorithm.as_str() {
                "CRC32" => &mut out.checksum_crc32,
                "CRC32C" => &mut out.checksum_crc32c,
                "CRC64NVME" => &mut out.checksum_crc64nvme,
                "SHA1" => &mut out.checksum_sha1,
                "SHA256" => &mut out.checksum_sha256,
                _ => return Err(internal(format!("stored checksum algorithm {algorithm}"))),
            };
            *field = Some(value);
        }
        Ok(S3Response::new(out))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let input = req.input;
        validate_key(&input.key)?;
        if input.checksum_crc32.is_some()
            || input.checksum_crc32c.is_some()
            || input.checksum_crc64nvme.is_some()
            || input.checksum_md5.is_some()
            || input.checksum_sha1.is_some()
            || input.checksum_sha256.is_some()
            || input.checksum_sha512.is_some()
            || input
                .checksum_type
                .as_ref()
                .is_some_and(|t| t.as_str() != "COMPOSITE")
            || input.checksum_xxhash64.is_some()
            || input.checksum_xxhash3.is_some()
            || input.checksum_xxhash128.is_some()
            || input.if_match.is_some()
            || input.if_none_match.is_some()
        {
            return Err(s3_error!(NotImplemented));
        }
        let listed = input
            .multipart_upload
            .as_ref()
            .and_then(|m| m.parts.clone())
            .unwrap_or_default();
        let mut parts = Vec::with_capacity(listed.len());
        for cp in &listed {
            parts.push(db::ListedPart {
                part_no: cp.part_number.ok_or_else(|| s3_error!(InvalidPart))?,
                etag: cp
                    .e_tag
                    .as_ref()
                    .map(|e| e.value().to_owned())
                    .unwrap_or_default(),
                checksum: part_checksum(cp)?,
            });
        }
        // Parts are already rows in PostgreSQL: Complete validates and publishes,
        // moving no data (so no long response window to lose).
        match db::complete_upload(
            &self.pool,
            &input.upload_id,
            &input.bucket,
            &input.key,
            &parts,
        )
        .await
        .map_err(internal)?
        {
            db::Completed::Done {
                bucket,
                key,
                etag: object_etag,
                size,
            } => {
                eprintln!(
                    "pgvs3: multipart {bucket}/{key} = {size} bytes in {} parts",
                    parts.len()
                );
                Ok(S3Response::new(CompleteMultipartUploadOutput {
                    location: Some(format!("/{bucket}/{key}")),
                    bucket: Some(bucket),
                    key: Some(key),
                    e_tag: etag(&object_etag),
                    ..Default::default()
                }))
            }
            db::Completed::InvalidPart => Err(s3_error!(InvalidPart)),
            db::Completed::NoSuchUpload => Err(s3_error!(NoSuchUpload)),
        }
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let input = req.input;
        if !db::abort_upload(&self.pool, &input.upload_id, &input.bucket, &input.key)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchUpload));
        }
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let bucket = &input.bucket;
        if !db::bucket_exists(&self.pool, bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter.unwrap_or_default();
        let max = input.max_keys.unwrap_or(1000).clamp(0, 1000) as usize;
        let echo_token = input.continuation_token.clone();
        let mut after = input
            .continuation_token
            .or(input.start_after)
            .unwrap_or_default();
        let bound = prefix_end(&prefix);

        let mut contents: Vec<Object> = Vec::new();
        let mut common: Vec<CommonPrefix> = Vec::new();
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;
        // A continuation inside a common prefix must not emit that prefix
        // again on the next page (or when using StartAfter).
        if !delimiter.is_empty() {
            if let Some(rest) = after.strip_prefix(&prefix) {
                if let Some(idx) = rest.find(&delimiter) {
                    seen.insert(format!("{prefix}{}", &rest[..idx + delimiter.len()]));
                }
            }
        }

        if max > 0 {
            'outer: loop {
                let rows = db::list(&self.pool, bucket, &prefix, &bound, &after, 1024)
                    .await
                    .map_err(internal)?;
                if rows.is_empty() {
                    break;
                }
                let last_row = rows.len() < 1024;
                for r in rows {
                    let full = listing_step(
                        &r,
                        &prefix,
                        &delimiter,
                        max,
                        &mut contents,
                        &mut common,
                        &mut seen,
                    );
                    if full {
                        truncated = true;
                        break 'outer;
                    }
                    after = r.key;
                }
                if last_row {
                    break;
                }
            }
        }

        let out = ListObjectsV2Output {
            name: Some(bucket.clone()),
            prefix: Some(prefix),
            max_keys: Some(max as i32),
            key_count: Some((contents.len() + common.len()) as i32),
            continuation_token: echo_token,
            is_truncated: Some(truncated),
            next_continuation_token: truncated.then(|| after.clone()),
            contents: Some(contents),
            common_prefixes: (!common.is_empty()).then_some(common),
            delimiter: (!delimiter.is_empty()).then_some(delimiter),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }

    async fn create_bucket(
        &self,
        req: S3Request<s3s::dto::CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        db::create_bucket(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{}", req.input.bucket)),
            ..Default::default()
        }))
    }

    async fn head_bucket(
        &self,
        req: S3Request<s3s::dto::HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if !db::bucket_exists(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(NoSuchBucket));
        }
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<s3s::dto::DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        match db::delete_bucket(&self.pool, &req.input.bucket)
            .await
            .map_err(internal)?
        {
            db::BucketDeletion::Deleted => Ok(S3Response::new(DeleteBucketOutput {})),
            db::BucketDeletion::NotEmpty => Err(s3_error!(BucketNotEmpty)),
            db::BucketDeletion::NotFound => Err(s3_error!(NoSuchBucket)),
        }
    }

    async fn list_buckets(
        &self,
        _req: S3Request<s3s::dto::ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let names = db::buckets(&self.pool).await.map_err(internal)?;
        let out = ListBucketsOutput {
            buckets: Some(
                names
                    .into_iter()
                    .map(|(name, created_at)| Bucket {
                        name: Some(name),
                        creation_date: Some(Timestamp::from(created_at)),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        };
        Ok(S3Response::new(out))
    }
}

/// Dispatch layer: an unauthenticated health path for probes, everything else
/// to the SigV4-protected S3 service. Kubernetes probes cannot sign requests,
/// and `s3s` rejects them with 403 (which used to make liveness kill the pod).
#[derive(Clone)]
struct Gateway {
    s3: S3Service,
}

impl HyperService<Request<hyper::body::Incoming>> for Gateway {
    type Response = Response<s3s::Body>;
    type Error = s3s::HttpError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        if req.uri().path() == "/healthz" {
            let body = s3s::Body::from(Bytes::from_static(b"ok\n"));
            return Box::pin(async move { Ok(Response::new(body)) });
        }
        let svc = self.s3.clone();
        Box::pin(async move { HyperService::call(&svc, req).await })
    }
}

pub struct ServeConfig {
    pub addr: String,
    pub access_key: String,
    pub secret_key: String,
    pub tls_cert: Option<String>,
    pub tls_key: Option<String>,
    pub allow_http: bool,
}

fn tls_acceptor(cert_path: &str, key_path: &str) -> Result<tokio_rustls::TlsAcceptor> {
    let cert = std::fs::read(cert_path).context("reading HTTPS certificate")?;
    let key = std::fs::read(key_path).context("reading HTTPS private key")?;
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::Cursor::new(cert)).collect::<std::io::Result<_>>()?;
    anyhow::ensure!(
        !certs.is_empty(),
        "HTTPS certificate file has no certificates"
    );
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(key))?
        .context("HTTPS private key file has no private key")?;
    let config = rustls::ServerConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
    Ok(tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)))
}

fn listener_config(cfg: &ServeConfig) -> Result<(SocketAddr, Option<tokio_rustls::TlsAcceptor>)> {
    anyhow::ensure!(
        !cfg.access_key.is_empty() && !cfg.secret_key.is_empty(),
        "S3 access and secret keys must not be empty"
    );
    let addr: SocketAddr = cfg.addr.parse().context("invalid listen address")?;
    let tls = match (&cfg.tls_cert, &cfg.tls_key) {
        (Some(cert), Some(key)) => Some(tls_acceptor(cert, key)?),
        (None, None) => {
            anyhow::ensure!(
                addr.ip().is_loopback() || cfg.allow_http,
                "HTTPS certificate/key required outside loopback (or explicitly set PGVS3_ALLOW_HTTP=true for an isolated rig)"
            );
            None
        }
        _ => anyhow::bail!("both PGVS3_TLS_CERT and PGVS3_TLS_KEY must be set"),
    };
    Ok((addr, tls))
}

pub async fn serve(pool: db::Pool, cfg: ServeConfig) -> Result<()> {
    let (addr, tls) = listener_config(&cfg)?;
    let s3 = PgS3 { pool };
    let mut builder = S3ServiceBuilder::new(s3);
    builder.set_auth(SimpleAuth::from_single(
        cfg.access_key.as_str(),
        cfg.secret_key.as_str(),
    ));
    let service = Gateway {
        s3: builder.build(),
    };

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!(
        "pgvs3 serving on {}://{} (sigv4 key: {})",
        if tls.is_some() { "https" } else { "http" },
        listener.local_addr()?,
        cfg.access_key
    );
    loop {
        let (stream, peer) = listener.accept().await?;
        stream.set_nodelay(true).ok(); // no Nagle: request/response latency matters
        let svc = service.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let builder =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            let result = if let Some(tls) = tls {
                match tls.accept(stream).await {
                    Ok(stream) => builder
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                        .await
                        .map_err(|e| e.to_string()),
                    Err(e) => Err(e.to_string()),
                }
            } else {
                builder
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await
                    .map_err(|e| e.to_string())
            };
            if let Err(e) = result {
                eprintln!("connection {peer}: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_http_requires_an_explicit_opt_in() {
        let mut cfg = ServeConfig {
            addr: "0.0.0.0:8014".into(),
            access_key: "test".into(),
            secret_key: "test-secret".into(),
            tls_cert: None,
            tls_key: None,
            allow_http: false,
        };
        assert!(listener_config(&cfg).is_err());
        cfg.addr = "127.0.0.1:8014".into();
        assert!(listener_config(&cfg).is_ok());
        cfg.addr = "0.0.0.0:8014".into();
        cfg.allow_http = true;
        assert!(listener_config(&cfg).is_ok());
        cfg.secret_key.clear();
        assert!(listener_config(&cfg).is_err());
    }
}
