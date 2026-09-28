//! Metadata cache: bucket/key -> Meta, count-capped. Its only job is removing
//! the lookup round trip per GET.
//!
//! Coherence: a write through this process updates or drops its entry, and a
//! GET that finds stale rows (missing) or a stale size (range clamped into a
//! bogus 416) re-resolves and retries before failing. HEAD fetches metadata
//! from PostgreSQL to avoid stale existence, size and ETag across gateways.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

/// Object metadata: enough to serve a range without re-reading `s3p.objects`.
#[derive(Debug, Clone)]
pub struct Meta {
    pub size: i64,
    pub sha256: Vec<u8>,
    pub etag: String,
    pub user_metadata: Vec<String>,
    pub content_type: String,
    pub created_at: SystemTime,
    pub file_id: i64,
    /// Multipart objects: part file_ids in order and cumulative end offsets.
    /// None = one file (`file_id`) holds all `size` bytes.
    pub parts: Option<Vec<i64>>,
    pub part_ends: Option<Vec<i64>>,
}

impl Meta {
    /// `(file_id, object byte offset, length)` of each stored segment.
    pub fn segments(&self) -> anyhow::Result<Vec<(i64, i64, i64)>> {
        match (&self.parts, &self.part_ends) {
            (Some(ids), Some(ends)) => {
                anyhow::ensure!(
                    !ids.is_empty()
                        && ids.len() == ends.len()
                        && ids[0] == self.file_id
                        && ends.last() == Some(&self.size)
                        && ends[0] >= 0
                        && ends.windows(2).all(|w| w[0] <= w[1]),
                    "invalid multipart manifest"
                );
                let mut prev = 0;
                let segments = ids
                    .iter()
                    .zip(ends)
                    .map(|(&id, &end)| {
                        let seg = (id, prev, end - prev);
                        prev = end;
                        seg
                    })
                    .collect();
                Ok(segments)
            }
            (None, None) => Ok(vec![(self.file_id, 0, self.size)]),
            _ => anyhow::bail!("incomplete multipart manifest"),
        }
    }
}

const META_CAP: usize = 1 << 18;

fn cache() -> &'static Mutex<MetaCache> {
    static C: OnceLock<Mutex<MetaCache>> = OnceLock::new();
    C.get_or_init(|| {
        Mutex::new(MetaCache {
            map: HashMap::new(),
        })
    })
}

struct MetaCache {
    map: HashMap<(String, String), Meta>,
}

pub fn meta_get(bucket: &str, key: &str) -> Option<Meta> {
    cache()
        .lock()
        .unwrap()
        .map
        .get(&(bucket.to_owned(), key.to_owned()))
        .cloned()
}

pub fn meta_put(bucket: &str, key: &str, meta: Meta) {
    let mut c = cache().lock().unwrap();
    let k = (bucket.to_owned(), key.to_owned());
    if c.map.len() >= META_CAP && !c.map.contains_key(&k) {
        c.map.clear();
    }
    c.map.insert(k, meta);
}

pub fn meta_invalidate(bucket: &str, key: &str) {
    let k = (bucket.to_owned(), key.to_owned());
    cache().lock().unwrap().map.remove(&k);
}

/// Timestamp from a `EXTRACT(EPOCH FROM ...)` float.
pub fn epoch(secs: f64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_secs_f64(secs.max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidation_removes_metadata() {
        let bucket = "test-cache-invalidation";
        let key = "same-key";
        for _ in 0..1000 {
            meta_put(
                bucket,
                key,
                Meta {
                    size: 0,
                    sha256: Vec::new(),
                    etag: String::new(),
                    user_metadata: Vec::new(),
                    content_type: "application/octet-stream".to_owned(),
                    created_at: SystemTime::UNIX_EPOCH,
                    file_id: 0,
                    parts: None,
                    part_ends: None,
                },
            );
            meta_invalidate(bucket, key);
        }
        let c = cache().lock().unwrap();
        let k = (bucket.to_owned(), key.to_owned());
        assert!(!c.map.contains_key(&k));
    }

    #[test]
    fn invalid_multipart_offsets_fail_closed() {
        let meta = Meta {
            size: 5,
            sha256: vec![0; 32],
            etag: "0".repeat(32),
            user_metadata: Vec::new(),
            content_type: "application/octet-stream".to_owned(),
            created_at: SystemTime::UNIX_EPOCH,
            file_id: 1,
            parts: Some(vec![1, 2]),
            part_ends: Some(vec![10, 5]),
        };
        assert!(meta.segments().is_err());
        assert!(Meta {
            part_ends: None,
            ..meta
        }
        .segments()
        .is_err());
    }
}
