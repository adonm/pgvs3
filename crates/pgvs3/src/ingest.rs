//! Binary COPY framing for the ingest path: fixed-size rows, one header per
//! stream, then row tuples. Pure byte transform (easy to get subtly wrong and
//! worth isolating) — the transaction and multipart bookkeeping stay in `db`.

use anyhow::{ensure, Result};
use bytes::{Bytes, BytesMut};
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::db::ROW_BYTES;

pub const COPY_SQL: &str = "COPY s3p.chunks (file_id, no, data) FROM STDIN WITH (FORMAT binary)";
pub const COPY_HEADER: &[u8] = b"PGCOPY\n\xff\r\n\0\x00\x00\x00\x00\x00\x00\x00\x00";
pub const COPY_TRAILER: &[u8] = &[0xFF, 0xFF];
/// Flush to the COPY stream at about this granularity.
pub const SEND_BATCH: usize = 4 << 20;

pub enum IngestMsg {
    Data(Bytes),
    /// Publish, with the request's verified `(x-amz-checksum-*, value)` pairs.
    Finish(Vec<(String, String)>),
    Abort,
}

/// Both digests are computed as the same bytes pass into COPY.
pub struct Digests {
    pub sha256: Vec<u8>,
    pub md5: Vec<u8>,
}

#[derive(Debug)]
pub struct Published {
    pub size: i64,
    pub etag: String,
    /// A multipart part's stored `(algorithm, base64 value)`.
    pub checksum: Option<(String, String)>,
}

pub type IngestResult = Result<Published, anyhow::Error>;

/// Accumulates request bytes into COPY rows: hashes them as they pass through
/// and frames each whole row. Bytes not yet framed are always less than one
/// row between pushes.
pub struct RowFramer {
    file_id: i64,
    hasher: Sha256,
    md5: Md5,
    pending: Vec<u8>,
    frame: BytesMut,
    next_no: i64,
    total: i64,
}

impl RowFramer {
    pub fn new(file_id: i64) -> Self {
        Self {
            file_id,
            hasher: Sha256::new(),
            md5: Md5::new(),
            pending: Vec::new(),
            frame: BytesMut::with_capacity(SEND_BATCH),
            next_no: 0,
            total: 0,
        }
    }

    /// Frame one incoming chunk; returns a buffer to flush to the COPY stream
    /// once it is worth a round trip.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Option<Bytes>> {
        let max_bytes = (i32::MAX as i64 + 1) * ROW_BYTES;
        ensure!(
            i64::try_from(chunk.len()).is_ok_and(|len| self.total <= max_bytes - len),
            "object part exceeds the maximum addressable row count"
        );
        self.hasher.update(chunk);
        self.md5.update(chunk);
        self.total += chunk.len() as i64;
        self.pending.extend_from_slice(chunk);
        let whole = self.pending.len() - self.pending.len() % ROW_BYTES as usize;
        if whole == 0 {
            return Ok(None);
        }
        frame_rows(
            self.file_id,
            self.next_no,
            &self.pending[..whole],
            &mut self.frame,
        );
        self.next_no += (whole / ROW_BYTES as usize) as i64;
        self.pending.drain(..whole);
        Ok((self.frame.len() >= SEND_BATCH).then(|| self.frame.split().freeze()))
    }

    /// Flush any partial row and the COPY trailer.
    pub fn finish(mut self) -> (Bytes, (i64, Digests)) {
        if !self.pending.is_empty() {
            frame_rows(self.file_id, self.next_no, &self.pending, &mut self.frame);
        }
        self.frame.extend_from_slice(COPY_TRAILER);
        let out = self.frame.split().freeze();
        let digests = Digests {
            sha256: self.hasher.finalize().to_vec(),
            md5: self.md5.finalize().to_vec(),
        };
        (out, (self.total, digests))
    }
}

/// Binary COPY framing: field count, then the (file_id, no, data) tuple.
fn frame_rows(file_id: i64, first_no: i64, data: &[u8], out: &mut BytesMut) {
    let nrows = data.len().div_ceil(ROW_BYTES as usize);
    for i in 0..nrows {
        let start = i * ROW_BYTES as usize;
        let end = ((i + 1) * ROW_BYTES as usize).min(data.len());
        out.extend_from_slice(&3i16.to_be_bytes()); // field count
        out.extend_from_slice(&8i32.to_be_bytes());
        out.extend_from_slice(&file_id.to_be_bytes());
        out.extend_from_slice(&4i32.to_be_bytes());
        let no = i32::try_from(first_no + i as i64).expect("bounded by RowFramer::push");
        out.extend_from_slice(&no.to_be_bytes());
        out.extend_from_slice(&((end - start) as i32).to_be_bytes());
        out.extend_from_slice(&data[start..end]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_both_digests_in_the_same_pass() {
        let mut framer = RowFramer::new(1);
        framer.push(b"a").unwrap();
        framer.push(b"bc").unwrap();
        let (_, (size, digests)) = framer.finish();
        assert_eq!(size, 3);
        assert_eq!(
            crate::db::hex(&digests.sha256),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            crate::db::hex(&digests.md5),
            "900150983cd24fb0d6963f7d28e17f72"
        );
    }

    #[test]
    fn row_number_limit_rejects_an_extra_byte_without_changing_state() {
        let mut framer = RowFramer::new(1);
        framer.total = i32::MAX as i64 * ROW_BYTES;
        framer.next_no = i32::MAX as i64;
        framer.push(&vec![1; ROW_BYTES as usize]).unwrap();
        assert_eq!(framer.next_no, i32::MAX as i64 + 1);
        assert!(framer.push(b"x").is_err());
        assert_eq!(framer.total, (i32::MAX as i64 + 1) * ROW_BYTES);
    }
}
