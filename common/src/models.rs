//! Request/response and metadata structs shared across the cluster.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub type ObjectId = Uuid;
pub type ChunkId = String;
pub type WorkerId = String;

// ─────────────────────────────── Object metadata ───────────────────────────────

/// Full metadata for one stored object (system + custom).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub object_id: ObjectId,
    pub name: String,
    pub size: u64,
    pub content_type: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    /// SHA-256 (hex) of the whole object.
    pub checksum: String,
    pub chunk_count: usize,
    /// Ordered by `index`.
    pub chunks: Vec<ChunkInfo>,
    /// User-defined key/value metadata.
    #[serde(default)]
    pub custom: HashMap<String, String>,
}

impl ObjectMeta {
    pub fn summary(&self) -> ObjectSummary {
        ObjectSummary {
            object_id: self.object_id,
            name: self.name.clone(),
            size: self.size,
            content_type: self.content_type.clone(),
            created_at: self.created_at,
        }
    }
}

/// One chunk of an object and where its copies live.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkInfo {
    pub chunk_id: ChunkId,
    /// Position of this chunk inside the object (0-based).
    pub index: usize,
    pub size: u64,
    /// SHA-256 (hex) of this chunk's bytes.
    pub checksum: String,
    /// Workers currently holding a verified copy.
    pub locations: Vec<WorkerId>,
}

/// Chunk ids are `<object_id>_<index>` (index zero-padded), so a worker's chunk
/// list can be mapped back to its owning object during reconciliation.
pub fn make_chunk_id(object_id: ObjectId, index: usize) -> ChunkId {
    format!("{object_id}_{index:06}")
}

/// Inverse of [`make_chunk_id`]. Returns `None` for malformed ids.
pub fn parse_chunk_id(chunk_id: &str) -> Option<(ObjectId, usize)> {
    let (obj, idx) = chunk_id.rsplit_once('_')?;
    Some((obj.parse().ok()?, idx.parse().ok()?))
}

/// Rejects anything that could escape a worker's data directory.
pub fn is_valid_chunk_id(chunk_id: &str) -> bool {
    parse_chunk_id(chunk_id).is_some()
}

// ─────────────────────────────── Client API bodies ─────────────────────────────

/// Entry in `GET /api/v1/objects`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSummary {
    pub object_id: ObjectId,
    pub name: String,
    pub size: u64,
    pub content_type: String,
    pub created_at: DateTime<Utc>,
}

/// Query string for `GET /api/v1/objects?offset=0&limit=50`.
#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
    pub objects: Vec<ObjectSummary>,
}

/// Body of `PATCH /api/v1/objects/{id}/metadata`.
/// A string value adds/updates the key; `null` removes it.
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct MetadataPatch(pub HashMap<String, Option<String>>);

impl MetadataPatch {
    pub fn apply(self, target: &mut HashMap<String, String>) {
        for (k, v) in self.0 {
            match v {
                Some(v) => {
                    target.insert(k, v);
                }
                None => {
                    target.remove(&k);
                }
            }
        }
    }
}

// ─────────────────────────────── Internal API bodies ───────────────────────────

/// `POST /internal/workers/register`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    pub worker_id: WorkerId,
    /// Base URL the controller should use, e.g. `http://worker2:8081`.
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    pub worker_id: WorkerId,
    pub heartbeat_interval_ms: u64,
}

/// `POST /internal/workers/heartbeat`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub worker_id: WorkerId,
    pub address: String,
    pub chunk_count: usize,
    pub bytes_used: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    /// True if the controller doesn't know this worker (e.g. controller
    /// restarted) and the worker should call register again.
    pub reregister: bool,
}

/// One chunk as reported by a worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredChunk {
    pub chunk_id: ChunkId,
    pub size: u64,
    pub checksum: String,
}

/// `GET /internal/workers/{worker_id}/chunks`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerChunkList {
    pub worker_id: WorkerId,
    pub chunks: Vec<StoredChunk>,
}

/// Sent by a worker's scrubber when a chunk fails its checksum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorruptChunkReport {
    pub worker_id: WorkerId,
    pub chunk_id: ChunkId,
}

// ─────────────────────────────── Cluster status (demo) ─────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Alive,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerView {
    pub worker_id: WorkerId,
    pub address: String,
    pub status: WorkerStatus,
    pub last_heartbeat: DateTime<Utc>,
    pub chunk_count: usize,
    pub bytes_used: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub workers: Vec<WorkerView>,
    pub object_count: usize,
    pub chunk_count: usize,
    /// Chunks with fewer than `REPLICATION_FACTOR` live copies.
    pub under_replicated_chunks: usize,
    /// Chunks with no live copy at all.
    pub unavailable_chunks: usize,
        /// Bad copies (corrupted or missing on disk) found since the controller started.
    #[serde(default)]
    pub corrupt_copies_detected: u64,
    /// Bad copies that were overwritten with a verified good copy.
    #[serde(default)]
    pub corrupt_copies_repaired: u64,
}

/// One copy of a chunk, as shown by `GET /api/v1/objects/{id}/chunks`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkLocationView {
    pub worker_id: WorkerId,
    /// False if the worker is dead or hasn't registered (yet).
    pub alive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkView {
    pub index: usize,
    pub chunk_id: ChunkId,
    pub size: u64,
    pub checksum: String,
    /// How many of `locations` are on alive workers.
    pub live_copies: usize,
    pub locations: Vec<ChunkLocationView>,
}

/// `GET /api/v1/objects/{id}/chunks`: where every chunk of one object lives
/// right now. Used in the demo to show the 2 copies before, during and after
/// a failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectChunksView {
    pub object_id: ObjectId,
    pub name: String,
    pub size: u64,
    pub chunk_count: usize,
    pub replication_factor: usize,
    /// Chunks with fewer than `replication_factor` live copies.
    pub under_replicated_chunks: usize,
    pub chunks: Vec<ChunkView>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_roundtrip() {
        let id = Uuid::new_v4();
        let cid = make_chunk_id(id, 42);
        assert_eq!(parse_chunk_id(&cid), Some((id, 42)));
        assert!(!is_valid_chunk_id("../../etc/passwd"));
        assert!(!is_valid_chunk_id("abc_1"));
    }

    #[test]
    fn metadata_patch() {
        let mut m = HashMap::from([("a".into(), "1".into()), ("b".into(), "2".into())]);
        let p: MetadataPatch = serde_json::from_str(r#"{"a":"9","b":null,"c":"3"}"#).unwrap();
        p.apply(&mut m);
        assert_eq!(m.get("a").unwrap(), "9");
        assert!(!m.contains_key("b"));
        assert_eq!(m.get("c").unwrap(), "3");
    }
}