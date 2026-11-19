//! Shared types and helpers used by both the controller and the workers.

pub mod constants;
pub mod errors;
pub mod models;
pub mod ring;
pub mod runtime;

pub use constants::*;
pub use errors::{AppError, AppResult};
pub use models::*;
pub use ring::HashRing;

use sha2::{Digest, Sha256};

/// Lowercase hex SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Incremental hasher, for checksumming a whole object chunk-by-chunk
/// without holding it all in one buffer.
#[derive(Default)]
pub struct StreamingHash(Sha256);

impl StreamingHash {
    pub fn new() -> Self {
        Self(Sha256::new())
    }
    pub fn update(&mut self, data: &[u8]) {
        self.0.update(data);
    }
    pub fn finish(self) -> String {
        hex::encode(self.0.finalize())
    }
}

/// Splits `data` into `CHUNK_SIZE` slices. Empty input gives zero chunks.
pub fn split_chunks(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    data.chunks(CHUNK_SIZE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_value() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn streaming_matches_oneshot() {
        let data = vec![7u8; CHUNK_SIZE * 2 + 123];
        let mut h = StreamingHash::new();
        for c in split_chunks(&data) {
            h.update(c);
        }
        assert_eq!(h.finish(), sha256_hex(&data));
    }

    #[test]
    fn chunking_sizes() {
        let data = vec![0u8; CHUNK_SIZE * 2 + 10];
        let sizes: Vec<_> = split_chunks(&data).map(|c| c.len()).collect();
        assert_eq!(sizes, vec![CHUNK_SIZE, CHUNK_SIZE, 10]);
        assert_eq!(split_chunks(&[]).count(), 0);
    }
}