//! Phase 6: reconciliation. When a worker (re)registers or comes back from
//! the dead, compare what it actually stores with the metadata and fix the
//! differences (see the policy in `storage/mod.rs`).

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use common::{ChunkId, ObjectId, REPLICATION_FACTOR, StoredChunk, WorkerId, parse_chunk_id};
use tracing::{debug, info, warn};

use crate::{
    state::SharedState,
    worker_client::{delete_chunk_on, list_worker_chunks},
};

#[derive(Debug, Default)]
pub struct ReconcileReport {
    /// Copies that are listed in metadata and match it.
    pub kept: usize,
    /// Copies of under-replicated chunks that were added back to metadata.
    pub adopted: usize,
    /// Copies of deleted objects (or unknown chunks) — removed.
    pub orphans_deleted: usize,
    /// Copies of chunks that already have enough copies elsewhere — removed.
    pub extras_deleted: usize,
    /// Copies whose size/checksum don't match the metadata — removed.
    pub mismatched_deleted: usize,
    /// Metadata said the worker has it, but the worker doesn't.
    pub missing: usize,
}

enum Decision {
    Keep,
    Adopt,
    Orphan,
    Extra,
    Mismatch,
}

/// Called (in the background) whenever a worker registers or comes back
/// from the dead. Compares what the worker actually stores with what the
/// metadata says, and fixes the differences.
pub async fn reconcile_worker(s: SharedState, worker: WorkerId) {
    // One reconciliation per worker at a time.
    if s.reconciling.insert(worker.clone(), ()).is_some() {
        return;
    }
    let result = reconcile_inner(&s, &worker).await;
    s.reconciling.remove(&worker);

    match result {
        Ok(r) => info!(
            %worker,
            kept = r.kept,
            adopted = r.adopted,
            orphans_deleted = r.orphans_deleted,
            extras_deleted = r.extras_deleted,
            mismatched_deleted = r.mismatched_deleted,
            missing = r.missing,
            "reconciliation finished"
        ),
        Err(e) => warn!(%worker, "reconciliation failed: {e}"),
    }
    // Chunks that were waiting for a spare worker can be repaired now.
    s.request_repair();
}

async fn reconcile_inner(s: &SharedState, worker: &str) -> Result<ReconcileReport, String> {
    // The worker has just started; give it a few tries to answer.
    let mut attempt = 0;
    let list = loop {
        attempt += 1;
        match list_worker_chunks(s, worker).await {
            Ok(list) => break list,
            Err(e) if attempt >= 3 => return Err(e),
            Err(e) => {
                debug!(%worker, attempt, "chunk list not available yet: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };
    let reported: HashMap<ChunkId, StoredChunk> = list
        .chunks
        .into_iter()
        .map(|c| (c.chunk_id.clone(), c))
        .collect();

    let mut report = ReconcileReport::default();
    let mut to_delete: Vec<ChunkId> = Vec::new();
    let mut touched: HashSet<ObjectId> = HashSet::new();

    // 1. Everything the worker has: keep, adopt or delete.
    for (chunk_id, stored) in &reported {
        let Some((object_id, _)) = parse_chunk_id(chunk_id) else {
            continue;
        };
        if s.pending_uploads.contains_key(&object_id) {
            report.kept += 1; // upload in progress; metadata not saved yet
            continue;
        }

        let decision = match s.objects.get_mut(&object_id) {
            None => Decision::Orphan,
            Some(mut obj) => match obj.chunks.iter_mut().find(|c| &c.chunk_id == chunk_id) {
                None => Decision::Orphan,
                Some(c) => {
                    let listed = c.locations.iter().any(|w| w == worker);
                    if c.checksum != stored.checksum || c.size != stored.size {
                        c.locations.retain(|w| w != worker);
                        Decision::Mismatch
                    } else if listed {
                        Decision::Keep
                    } else {
                        let live_elsewhere = c
                            .locations
                            .iter()
                            .filter(|w| *w != worker && s.is_alive(w))
                            .count();
                        if live_elsewhere < REPLICATION_FACTOR {
                            c.locations.push(worker.to_string());
                            Decision::Adopt
                        } else {
                            Decision::Extra
                        }
                    }
                }
            },
        }; // DashMap guard dropped here

        match decision {
            Decision::Keep => report.kept += 1,
            Decision::Adopt => {
                report.adopted += 1;
                touched.insert(object_id);
                info!(%chunk_id, %worker, "adopted copy of under-replicated chunk");
            }
            Decision::Orphan => {
                report.orphans_deleted += 1;
                to_delete.push(chunk_id.clone());
            }
            Decision::Extra => {
                report.extras_deleted += 1;
                to_delete.push(chunk_id.clone());
            }
            Decision::Mismatch => {
                report.mismatched_deleted += 1;
                touched.insert(object_id);
                to_delete.push(chunk_id.clone());
            }
        }
    }

    // 2. Everything the metadata says the worker has, but it doesn't
    //    (e.g. its volume was wiped): forget that location.
    for mut obj in s.objects.iter_mut() {
        let object_id = obj.object_id;
        let mut changed = false;
        for c in obj.chunks.iter_mut() {
            if c.locations.iter().any(|w| w == worker) && !reported.contains_key(&c.chunk_id) {
                c.locations.retain(|w| w != worker);
                report.missing += 1;
                changed = true;
                warn!(chunk_id = %c.chunk_id, %worker, "copy missing on worker");
            }
        }
        if changed {
            touched.insert(object_id);
        }
    }

    // 3. Save metadata first, then delete bytes: if we crash in between,
    //    the leftovers are just orphans that the next reconciliation removes.
    for oid in touched {
        if let Err(e) = s.persist_object(oid).await {
            warn!(object_id = %oid, "could not save reconciled metadata: {e}");
        }
    }
    for chunk_id in &to_delete {
        if let Err(e) = delete_chunk_on(s, worker, chunk_id).await {
            warn!(%chunk_id, %worker, "reconciliation delete failed: {e}");
        }
    }

    Ok(report)
}
