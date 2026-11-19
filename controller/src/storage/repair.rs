//! Phase 5: re-replication. After a worker dies, bring every chunk back to
//! exactly REPLICATION_FACTOR live copies (see the policy in `storage/mod.rs`).

use std::collections::HashSet;

use common::{
    ChunkInfo, ObjectId, REPAIR_CONCURRENCY, REPAIR_INTERVAL, REPLICATION_FACTOR, STARTUP_GRACE,
    WorkerId,
};
use futures::StreamExt;
use tracing::{debug, info, warn};

use super::{placement::fetch_verified, with_chunk_mut};
use crate::{
    state::SharedState,
    worker_client::{delete_chunk_on, put_chunk_on},
};

/// Background task: waits until asked (worker died, copy missing, worker
/// came back) or until REPAIR_INTERVAL passes, then runs one repair sweep.
pub async fn repair_loop(s: SharedState) {
    loop {
        tokio::select! {
            _ = s.repair_notify.notified() => {}
            _ = tokio::time::sleep(REPAIR_INTERVAL) => {}
        }

        let since_start = s.started_at.elapsed();
        if since_start < STARTUP_GRACE {
            tokio::time::sleep(STARTUP_GRACE - since_start).await;
        }

        let r = repair_under_replicated(&s).await;
        if r.repaired + r.trimmed + r.failed + r.unavailable > 0 {
            info!(
                repaired = r.repaired,
                trimmed = r.trimmed,
                failed = r.failed,
                unavailable = r.unavailable,
                "repair sweep finished"
            );
        }
    }
}

#[derive(Debug, Default)]
pub struct RepairReport {
    /// Chunks that got a new copy.
    pub repaired: usize,
    /// Chunks whose extra copies / dead locations were removed.
    pub trimmed: usize,
    /// Chunks that are under-replicated but couldn't be fixed right now
    /// (no spare alive worker, or every source failed).
    pub failed: usize,
    /// Chunks with no live copy at all — must wait for a worker to return.
    pub unavailable: usize,
}

enum Outcome {
    Nothing,
    Repaired,
    Trimmed,
    Failed,
    Unavailable,
}

/// Scans every chunk and fixes the ones that don't have exactly
/// REPLICATION_FACTOR live copies.
pub async fn repair_under_replicated(s: &SharedState) -> RepairReport {
    // 1. Snapshot the chunks that need attention. No awaits while the
    //    DashMap is being iterated.
    let mut work: Vec<(ObjectId, ChunkInfo)> = Vec::new();
    for o in s.objects.iter() {
        for c in &o.chunks {
            let alive = c.locations.iter().filter(|w| s.is_alive(w)).count();
            if alive != REPLICATION_FACTOR || c.locations.len() != REPLICATION_FACTOR {
                work.push((o.object_id, c.clone()));
            }
        }
    }

    let mut report = RepairReport::default();
    if work.is_empty() {
        return report;
    }

    // 2. Fix them, a few at a time.
    let outcomes: Vec<(ObjectId, Outcome)> = futures::stream::iter(work)
        .map(|(oid, c)| {
            let s = s.clone();
            async move { (oid, fix_chunk(&s, oid, c).await) }
        })
        .buffer_unordered(REPAIR_CONCURRENCY)
        .collect()
        .await;

    // 3. Save every object whose chunk locations changed (once each).
    let mut touched = HashSet::new();
    for (oid, outcome) in outcomes {
        match outcome {
            Outcome::Nothing => {}
            Outcome::Repaired => {
                report.repaired += 1;
                touched.insert(oid);
            }
            Outcome::Trimmed => {
                report.trimmed += 1;
                touched.insert(oid);
            }
            Outcome::Failed => report.failed += 1,
            Outcome::Unavailable => report.unavailable += 1,
        }
    }
    for oid in touched {
        if let Err(e) = s.persist_object(oid).await {
            warn!(object_id = %oid, "could not save repaired metadata: {e}");
        }
    }
    report
}

async fn fix_chunk(s: &SharedState, object_id: ObjectId, chunk: ChunkInfo) -> Outcome {
    let alive: Vec<WorkerId> = chunk
        .locations
        .iter()
        .filter(|w| s.is_alive(w))
        .cloned()
        .collect();

    if alive.len() >= REPLICATION_FACTOR {
        settle_chunk(s, object_id, &chunk).await
    } else if alive.is_empty() {
        Outcome::Unavailable
    } else {
        re_replicate(s, object_id, &chunk, &alive).await
    }
}

/// Copies a chunk from a live holder to new workers until it has
/// REPLICATION_FACTOR live copies again.
async fn re_replicate(
    s: &SharedState,
    object_id: ObjectId,
    chunk: &ChunkInfo,
    alive: &[WorkerId],
) -> Outcome {
    let need = REPLICATION_FACTOR - alive.len();

    // Every alive worker that doesn't already appear in `locations`, in ring
    // order for this chunk (the "exclude" part of pick_targets). Dead holders
    // are excluded too: they may come back with their copy intact.
    let candidates = {
        let ring = s.ring.read().unwrap();
        ring.pick_targets(&chunk.chunk_id, ring.len(), |w| {
            s.is_alive(w) && !chunk.locations.iter().any(|l| l == w)
        })
    };
    if candidates.is_empty() {
        debug!(chunk_id = %chunk.chunk_id, "no spare alive worker for re-replication");
        return Outcome::Failed;
    }

    let Some(data) = fetch_verified(s, chunk, alive).await else {
        warn!(chunk_id = %chunk.chunk_id, "no healthy source copy for re-replication");
        return Outcome::Failed;
    };

    let mut added: Vec<WorkerId> = Vec::new();
    for w in candidates {
        if added.len() == need {
            break;
        }
        match put_chunk_on(s, &w, &chunk.chunk_id, data.clone(), &chunk.checksum).await {
            Ok(()) => added.push(w),
            Err(e) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "re-replication PUT failed: {e}")
            }
        }
    }
    if added.is_empty() {
        return Outcome::Failed;
    }

    // Record the new copies. Dead locations are dropped only if we're back
    // to a full set of live copies; otherwise they're our best hope.
    let updated = with_chunk_mut(s, object_id, &chunk.chunk_id, |c| {
        for w in &added {
            if !c.locations.contains(w) {
                c.locations.push(w.clone());
            }
        }
        let live = c.locations.iter().filter(|w| s.is_alive(w)).count();
        if live >= REPLICATION_FACTOR {
            c.locations.retain(|w| s.is_alive(w));
        }
        c.locations.clone()
    });

    match updated {
        Some(locations) => {
            info!(
                chunk_id = %chunk.chunk_id, from = ?alive, to = ?added, now = ?locations,
                "chunk re-replicated"
            );
            Outcome::Repaired
        }
        None => {
            // Object was deleted while we copied: undo our copies.
            for w in &added {
                let _ = delete_chunk_on(s, w, &chunk.chunk_id).await;
            }
            Outcome::Nothing
        }
    }
}

/// The chunk has at least REPLICATION_FACTOR live copies. Remove dead
/// workers from its location list, and delete copies beyond the
/// replication factor (keeping the earliest-listed ones).
async fn settle_chunk(s: &SharedState, object_id: ObjectId, chunk: &ChunkInfo) -> Outcome {
    // Decide using the current metadata, not our snapshot.
    let extras = with_chunk_mut(s, object_id, &chunk.chunk_id, |c| {
        let alive: Vec<WorkerId> = c
            .locations
            .iter()
            .filter(|w| s.is_alive(w))
            .cloned()
            .collect();
        if alive.len() < REPLICATION_FACTOR {
            return None; // changed under us; next sweep will handle it
        }
        let extras = alive[REPLICATION_FACTOR..].to_vec();
        let keep = alive[..REPLICATION_FACTOR].to_vec();
        if keep == c.locations {
            return None;
        }
        c.locations = keep;
        Some(extras)
    })
    .flatten();

    let Some(extras) = extras else {
        return Outcome::Nothing;
    };
    for w in &extras {
        match delete_chunk_on(s, w, &chunk.chunk_id).await {
            Ok(()) => info!(chunk_id = %chunk.chunk_id, worker = %w, "deleted extra copy"),
            Err(e) => {
                warn!(chunk_id = %chunk.chunk_id, worker = %w, "deleting extra copy failed: {e}")
            }
        }
    }
    Outcome::Trimmed
}
