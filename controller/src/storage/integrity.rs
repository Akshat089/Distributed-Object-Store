//! Phase 7: integrity repair. Overwrites a corrupted (or missing) copy with
//! a verified good one. Triggered by downloads and by worker scrubbers.

use bytes::Bytes;
use common::{ChunkId, ObjectId, WorkerId, sha256_hex};
use tracing::{debug, error, info, warn};

use super::{placement::fetch_verified, with_chunk_mut};
use crate::{state::SharedState, worker_client::put_chunk_on};

/// Replaces `bad_worker`'s copy of a chunk (corrupted, or missing from its
/// disk) with a verified good copy. Called from:
///  - `fetch_chunk`, when a download hits a bad copy (`good` = the good bytes
///    it already has, so nothing is fetched twice);
///  - `POST /internal/chunks/corrupt`, when a worker's scrubber finds one
///    (`good` = None, so a healthy copy is fetched from another holder).
///
/// If the bad worker can't take the new copy (it went down), it is dropped
/// from the chunk's locations and the normal re-replication takes over.
pub async fn repair_bad_copy(
    s: SharedState,
    object_id: ObjectId,
    chunk_id: ChunkId,
    bad_worker: WorkerId,
    good: Option<Bytes>,
) {
    // One repair per (chunk, worker) at a time.
    let key = format!("{chunk_id}@{bad_worker}");
    if s.corrupt_repairs.insert(key.clone(), ()).is_some() {
        return;
    }
    repair_bad_copy_inner(&s, object_id, &chunk_id, &bad_worker, good, &key).await;
    s.corrupt_repairs.remove(&key);
}

async fn repair_bad_copy_inner(
    s: &SharedState,
    object_id: ObjectId,
    chunk_id: &str,
    bad_worker: &str,
    good: Option<Bytes>,
    key: &str,
) {
    // Current metadata for this chunk (the object may have been deleted).
    let Some(chunk) = s
        .objects
        .get(&object_id)
        .and_then(|o| o.chunks.iter().find(|c| c.chunk_id == chunk_id).cloned())
    else {
        debug!(%chunk_id, "bad copy belongs to a deleted object; reconciliation will remove it");
        return;
    };
    if !chunk.locations.iter().any(|w| w == bad_worker) {
        // Not supposed to be there (extra / orphan): reconciliation's job.
        debug!(%chunk_id, worker = %bad_worker, "bad copy is not a listed location; ignoring");
        return;
    }

    if s.known_bad.insert(key.to_string(), ()).is_none() {
        s.integrity.add_detected();
    }
    warn!(%chunk_id, worker = %bad_worker, "repairing bad copy");

    // Good bytes: the ones the download already read, or a fresh verified
    // copy from another alive holder.
    let data = match good {
        Some(d) if sha256_hex(&d) == chunk.checksum => Some(d),
        _ => {
            let sources: Vec<WorkerId> = chunk
                .locations
                .iter()
                .filter(|w| *w != bad_worker && s.is_alive(w))
                .cloned()
                .collect();
            fetch_verified(s, &chunk, &sources).await
        }
    };
    let Some(data) = data else {
        error!(%chunk_id, worker = %bad_worker, "no healthy copy left to repair from; chunk is DAMAGED");
        return;
    };

    match put_chunk_on(s, bad_worker, chunk_id, data, &chunk.checksum).await {
        Ok(()) => {
            s.known_bad.remove(key);
            s.integrity.add_repaired();
            info!(%chunk_id, worker = %bad_worker, "corrupted copy REPAIRED from a healthy copy");
        }
        Err(e) => {
            // Couldn't fix it in place: stop trusting that copy and let the
            // repair loop place a fresh one on another worker.
            warn!(%chunk_id, worker = %bad_worker, "in-place repair failed ({e}); re-replicating elsewhere");
            s.known_bad.remove(key);
            let changed = with_chunk_mut(s, object_id, chunk_id, |c| {
                let before = c.locations.len();
                c.locations.retain(|w| w != bad_worker);
                c.locations.len() != before
            })
            .unwrap_or(false);
            if changed {
                if let Err(e) = s.persist_object(object_id).await {
                    warn!(%object_id, "could not save metadata: {e}");
                }
                s.request_repair();
            }
        }
    }
}
