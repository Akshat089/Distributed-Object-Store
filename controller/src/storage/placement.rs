//! The data path (Phase 3): where a new chunk goes, how a verified copy is
//! read back, and deleting every copy.

use bytes::Bytes;
use common::{
    AppError, AppResult, ChunkInfo, ObjectId, REPLICATION_FACTOR, WorkerId, make_chunk_id,
    parse_chunk_id, sha256_hex,
};
use tracing::{debug, error, warn};

use super::integrity::repair_bad_copy;
use crate::{
    state::SharedState,
    worker_client::{FetchError, delete_chunk_on, get_chunk_from, put_chunk_on},
};

/// Stores one chunk on REPLICATION_FACTOR distinct alive workers.
/// Tries workers in ring order and skips any that fail.
/// If it can't reach the replication factor, it removes partial copies.
pub async fn place_chunk(
    s: SharedState,
    object_id: ObjectId,
    index: usize,
    data: Bytes,
) -> AppResult<ChunkInfo> {
    let chunk_id = make_chunk_id(object_id, index);
    let checksum = sha256_hex(&data);
    let candidates = s.placement_candidates(&chunk_id);

    if candidates.len() < REPLICATION_FACTOR {
        return Err(AppError::Unavailable(format!(
            "need {REPLICATION_FACTOR} alive workers, only {} available",
            candidates.len()
        )));
    }

    let mut locations = Vec::with_capacity(REPLICATION_FACTOR);
    for worker in candidates {
        if locations.len() == REPLICATION_FACTOR {
            break;
        }
        match put_chunk_on(&s, &worker, &chunk_id, data.clone(), &checksum).await {
            Ok(()) => {
                debug!(%chunk_id, %worker, "chunk stored");
                locations.push(worker);
            }
            Err(e) => warn!(%chunk_id, %worker, "store failed, trying next worker: {e}"),
        }
    }

    if locations.len() < REPLICATION_FACTOR {
        for w in &locations {
            let _ = delete_chunk_on(&s, w, &chunk_id).await;
        }
        return Err(AppError::Unavailable(format!(
            "could only store {} of {REPLICATION_FACTOR} copies of chunk {chunk_id}",
            locations.len()
        )));
    }

    Ok(ChunkInfo {
        chunk_id,
        index,
        size: data.len() as u64,
        checksum,
        locations,
    })
}

/// Returns a verified copy of the chunk, trying alive holders first.
/// A copy whose checksum doesn't match is skipped, never returned.
/// Every bad copy found on the way is repaired in the background using the
/// good bytes we just read (Phase 7).
pub async fn fetch_chunk(s: SharedState, chunk: ChunkInfo) -> AppResult<Bytes> {
    let mut order = chunk.locations.clone();
    order.sort_by_key(|w| !s.is_alive(w)); // alive first

    let mut bad: Vec<WorkerId> = Vec::new();
    for worker in &order {
        match get_chunk_from(&s, worker, &chunk.chunk_id).await {
            Ok(data) if sha256_hex(&data) == chunk.checksum => {
                if let Some((object_id, _)) = parse_chunk_id(&chunk.chunk_id) {
                    for w in bad {
                        tokio::spawn(repair_bad_copy(
                            s.clone(),
                            object_id,
                            chunk.chunk_id.clone(),
                            w,
                            Some(data.clone()),
                        ));
                    }
                }
                return Ok(data);
            }
            Ok(_) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "copy failed checksum at controller");
                bad.push(worker.clone());
            }
            Err(FetchError::Corrupted) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "CORRUPTED copy detected during download, trying next copy");
                bad.push(worker.clone());
            }
            Err(FetchError::Missing) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "copy MISSING on worker, trying next copy");
                bad.push(worker.clone());
            }
            Err(FetchError::Other(e)) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "fetch failed: {e}")
            }
        }
    }

    if !bad.is_empty() {
        error!(chunk_id = %chunk.chunk_id, bad = ?bad, "every reachable copy is bad; refusing to return corrupted data");
    }
    Err(AppError::Unavailable(format!(
        "no valid copy of chunk {} is reachable",
        chunk.chunk_id
    )))
}

/// Best-effort delete of every copy of every chunk. Failures are logged;
/// leftovers get cleaned up by reconciliation when a worker returns.
pub async fn delete_chunks(s: &SharedState, chunks: &[ChunkInfo]) {
    let mut tasks = Vec::new();
    for c in chunks {
        for w in &c.locations {
            let (s, w, id) = (s.clone(), w.clone(), c.chunk_id.clone());
            tasks.push(tokio::spawn(async move {
                if let Err(e) = delete_chunk_on(&s, &w, &id).await {
                    warn!(chunk_id = %id, worker = %w, "delete failed: {e}");
                }
            }));
        }
    }
    for t in tasks {
        let _ = t.await;
    }
}

/// Reads a chunk from the first of `holders` that returns a verified copy.
/// Used as the source for re-replication and integrity repair.
pub(crate) async fn fetch_verified(
    s: &SharedState,
    chunk: &ChunkInfo,
    holders: &[WorkerId],
) -> Option<Bytes> {
    for w in holders {
        match get_chunk_from(s, w, &chunk.chunk_id).await {
            Ok(data) if sha256_hex(&data) == chunk.checksum => return Some(data),
            Ok(_) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "repair source failed checksum")
            }
            Err(FetchError::Corrupted) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "repair source is corrupted")
            }
            Err(FetchError::Missing) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "repair source is missing the chunk")
            }
            Err(FetchError::Other(e)) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "repair source unreachable: {e}")
            }
        }
    }
    None
}
