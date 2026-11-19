//! The on-disk chunk store.
//!
//! Layout on disk (inside DATA_DIR/chunks):
//!   <chunk_id>          raw chunk bytes
//!   <chunk_id>.sha256   expected SHA-256 hex, written when the chunk was stored
//!   *.tmp               half-written files; deleted on startup
//!
//! The in-memory index mirrors what's on disk so list/stats are cheap.
//! The checksum in the index is the *expected* one; actual bytes are re-hashed
//! on every read, so on-disk corruption is always caught.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use common::{AppError, AppResult, StoredChunk, is_valid_chunk_id, sha256_hex};
use dashmap::DashMap;
use tokio::{fs, io::AsyncWriteExt};
use tracing::{info, warn};

pub struct ChunkStore {
    dir: PathBuf,
    index: DashMap<String, StoredChunk>,
    /// Chunks that were in the index but whose file turned out to be gone.
    /// They're removed from the index, so the scrubber can't find them by
    /// listing; it reports these to the controller instead.
    lost: DashMap<String, ()>,
}

impl ChunkStore {
    /// Opens (or creates) the store and rebuilds the index from disk.
    pub async fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join("chunks");
        fs::create_dir_all(&dir).await?;

        let index = DashMap::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();

            if name.ends_with(".tmp") {
                let _ = fs::remove_file(entry.path()).await;
                continue;
            }
            if name.ends_with(".sha256") || !is_valid_chunk_id(&name) {
                continue;
            }

            let size = entry.metadata().await?.len();
            match fs::read_to_string(dir.join(format!("{name}.sha256"))).await {
                Ok(checksum) => {
                    index.insert(
                        name.clone(),
                        StoredChunk {
                            chunk_id: name,
                            size,
                            checksum: checksum.trim().to_string(),
                        },
                    );
                }
                Err(_) => {
                    // Crash between writing data and checksum: can't trust it.
                    warn!(chunk_id = %name, "chunk has no checksum file; removing");
                    let _ = fs::remove_file(entry.path()).await;
                }
            }
        }

        info!(dir = %dir.display(), chunks = index.len(), "chunk store opened");
        Ok(Self {
            dir,
            index,
            lost: DashMap::new(),
        })
    }

    fn data_path(&self, id: &str) -> PathBuf {
        self.dir.join(id)
    }

    fn sum_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.sha256"))
    }

    fn check_id(id: &str) -> AppResult<()> {
        if is_valid_chunk_id(id) {
            Ok(())
        } else {
            Err(AppError::BadRequest(format!("invalid chunk id '{id}'")))
        }
    }

    /// Verifies `data` against `expected` and stores it atomically.
    /// Overwrites an existing copy (used when repairing a corrupt chunk).
    pub async fn put(&self, id: &str, data: Bytes, expected: &str) -> AppResult<StoredChunk> {
        Self::check_id(id)?;
        let expected = expected.trim().to_lowercase();
        let actual = sha256_hex(&data);
        if actual != expected {
            return Err(AppError::BadRequest(format!(
                "checksum mismatch for {id}: expected {expected}, got {actual}"
            )));
        }

        let data_tmp = self.dir.join(format!("{id}.tmp"));
        let sum_tmp = self.dir.join(format!("{id}.sha256.tmp"));

        write_synced(&data_tmp, &data).await?;
        write_synced(&sum_tmp, actual.as_bytes()).await?;
        // Rename is atomic: readers see either the old file or the new one.
        fs::rename(&data_tmp, self.data_path(id)).await?;
        fs::rename(&sum_tmp, self.sum_path(id)).await?;

        let stored = StoredChunk {
            chunk_id: id.to_string(),
            size: data.len() as u64,
            checksum: actual,
        };
        self.index.insert(id.to_string(), stored.clone());
        self.lost.remove(id); // restored
        Ok(stored)
    }

    /// Reads a chunk and verifies it. Never returns corrupted bytes.
    pub async fn get(&self, id: &str) -> AppResult<(Bytes, String)> {
        Self::check_id(id)?;
        let expected = self
            .index
            .get(id)
            .map(|e| e.checksum.clone())
            .ok_or_else(|| AppError::NotFound(format!("chunk {id} not found")))?;

        let data = match fs::read(self.data_path(id)).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File vanished behind our back. Remember it so the
                // scrubber reports it and the controller restores it.
                self.index.remove(id);
                self.lost.insert(id.to_string(), ());
                return Err(AppError::NotFound(format!("chunk {id} not found")));
            }
            Err(e) => return Err(e.into()),
        };

        let actual = sha256_hex(&data);
        if actual != expected {
            warn!(chunk_id = %id, %expected, %actual, "CORRUPTED chunk detected on read");
            return Err(AppError::Corrupted(format!(
                "chunk {id} failed checksum verification"
            )));
        }
        Ok((Bytes::from(data), expected))
    }

    pub async fn delete(&self, id: &str) -> AppResult<()> {
        Self::check_id(id)?;
        self.lost.remove(id);
        if self.index.remove(id).is_none() {
            return Err(AppError::NotFound(format!("chunk {id} not found")));
        }
        for p in [self.data_path(id), self.sum_path(id)] {
            if let Err(e) = fs::remove_file(&p).await
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Chunks found missing since the last call (and not restored since).
    pub fn take_lost(&self) -> Vec<String> {
        let ids: Vec<String> = self.lost.iter().map(|e| e.key().clone()).collect();
        for id in &ids {
            self.lost.remove(id);
        }
        ids
    }

    /// Takes one chunk off the lost list; true if it was on it.
    pub fn take_lost_one(&self, id: &str) -> bool {
        self.lost.remove(id).is_some()
    }

    pub fn list(&self) -> Vec<StoredChunk> {
        let mut v: Vec<_> = self.index.iter().map(|e| e.value().clone()).collect();
        v.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
        v
    }

    /// (chunk count, total bytes)
    pub fn stats(&self) -> (usize, u64) {
        let bytes = self.index.iter().map(|e| e.size).sum();
        (self.index.len(), bytes)
    }
}

async fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut f = fs::File::create(path).await?;
    f.write_all(data).await?;
    f.sync_all().await
}
