//! Moving chunks between the controller and workers.
//!
//! Phase 3: place a chunk on REPLICATION_FACTOR workers, fetch a verified
//! copy, delete all copies.
//! Phase 5: `repair_loop` / `repair_under_replicated` bring every chunk back
//!          to exactly REPLICATION_FACTOR live copies after a worker dies.
//! Phase 6: `reconcile_worker` cleans up a worker that comes back.
//! Phase 7: `repair_bad_copy` overwrites a corrupted (or missing) copy with a
//!          verified good one. Triggered by downloads and by worker scrubbers.
//!
//! ## Policy for extra copies ("drop the extra")
//!
//! The invariant we converge to is: *every chunk's `locations` lists exactly
//! REPLICATION_FACTOR alive workers, and only those workers hold a copy.*
//!
//! - When a worker dies, its chunks get a new copy on another worker and the
//!   dead worker is removed from `locations` (only once the new copy exists).
//! - When that worker comes back, its old copies are no longer listed, so
//!   reconciliation sees them as extras and deletes them.
//! - If a returning worker holds a copy of a chunk that is *still*
//!   under-replicated (repair hadn't finished, or there was no spare worker),
//!   the copy is adopted instead: cheaper than copying the bytes again.
//! - If a chunk somehow ends up with more than REPLICATION_FACTOR live copies
//!   (races between repair and reconciliation), the repair sweep trims it.
//!
//! Why drop rather than keep? Storage use stays predictable (2x, not "2x
//! or 3x depending on history"), placement stays balanced, and the cluster
//! status is easy to explain: every chunk has exactly 2 copies.

use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use bytes::Bytes;
use common::{
    AppError, AppResult, CHECKSUM_HEADER, ChunkId, ChunkInfo, HEARTBEAT_TIMEOUT_MS, ObjectId,
    REPLICATION_FACTOR, WorkerChunkList, WorkerId, make_chunk_id, parse_chunk_id, sha256_hex,
};
use futures::StreamExt;
use reqwest::StatusCode;
use tracing::{debug, error, info, warn};

use crate::state::SharedState;

/// The repair sweep runs at least this often, even if nothing asked for it
/// (catches repairs that failed earlier, e.g. while no spare worker existed).
const REPAIR_INTERVAL: Duration = Duration::from_secs(10);
/// How many chunks are repaired at the same time.
const REPAIR_CONCURRENCY: usize = 8;

/// After a controller restart, wait this long for workers to re-register
/// before deciding that chunks are under-replicated.
fn startup_grace() -> Duration {
    Duration::from_millis(HEARTBEAT_TIMEOUT_MS * 2)
}

// ─────────────────────────────── Low-level worker calls ───────────────

pub async fn put_chunk_on(
    s: &SharedState,
    worker: &str,
    chunk_id: &str,
    data: Bytes,
    checksum: &str,
) -> Result<(), String> {
    let addr = s
        .worker_addr(worker)
        .ok_or_else(|| format!("unknown worker {worker}"))?;
    let resp = s
        .http
        .put(format!("{addr}/internal/chunks/{chunk_id}"))
        .header(CHECKSUM_HEADER, checksum)
        .body(data)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Err(format!("{status}: {body}"))
    }
}

pub enum FetchError {
    /// The worker has the chunk but its bytes fail the checksum (HTTP 409).
    Corrupted,
    /// The worker should have the chunk but doesn't (HTTP 404).
    Missing,
    /// Unreachable, timeout, 5xx, ...
    Other(String),
}

pub async fn get_chunk_from(
    s: &SharedState,
    worker: &str,
    chunk_id: &str,
) -> Result<Bytes, FetchError> {
    let addr = s
        .worker_addr(worker)
        .ok_or_else(|| FetchError::Other(format!("unknown worker {worker}")))?;
    let resp = s
        .http
        .get(format!("{addr}/internal/chunks/{chunk_id}"))
        .send()
        .await
        .map_err(|e| FetchError::Other(e.to_string()))?;
    match resp.status() {
        StatusCode::OK => resp
            .bytes()
            .await
            .map_err(|e| FetchError::Other(e.to_string())),
        StatusCode::CONFLICT => Err(FetchError::Corrupted),
        StatusCode::NOT_FOUND => Err(FetchError::Missing),
        other => Err(FetchError::Other(format!("HTTP {other}"))),
    }
}

pub async fn delete_chunk_on(s: &SharedState, worker: &str, chunk_id: &str) -> Result<(), String> {
    let addr = s
        .worker_addr(worker)
        .ok_or_else(|| format!("unknown worker {worker}"))?;
    let resp = s
        .http
        .delete(format!("{addr}/internal/chunks/{chunk_id}"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    // 404 = already gone, which is what we wanted.
    if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
        Ok(())
    } else {
        Err(format!("HTTP {}", resp.status()))
    }
}

// ─────────────────────────────── High-level operations ────────────────

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

/// Lists the chunks a worker actually has on disk.
pub async fn list_worker_chunks(s: &SharedState, worker: &str) -> Result<WorkerChunkList, String> {
    let addr = s
        .worker_addr(worker)
        .ok_or_else(|| format!("unknown worker {worker}"))?;
    let resp = s
        .http
        .get(format!("{addr}/internal/workers/{worker}/chunks"))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<WorkerChunkList>()
        .await
        .map_err(|e| format!("bad chunk list: {e}"))
}

/// Reads a chunk from the first of `holders` that returns a verified copy.
async fn fetch_verified(s: &SharedState, chunk: &ChunkInfo, holders: &[WorkerId]) -> Option<Bytes> {
    for w in holders {
        match get_chunk_from(s, w, &chunk.chunk_id).await {
            Ok(data) if sha256_hex(&data) == chunk.checksum => return Some(data),
            Ok(_) => warn!(chunk_id = %chunk.chunk_id, worker = %w, "repair source failed checksum"),
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

/// Runs `f` on one chunk of one object inside the in-memory metadata map.
/// Returns `None` if the object or chunk no longer exists (e.g. it was
/// deleted while we were copying bytes around).
///
/// Never `.await` inside `f`: it runs while a DashMap shard lock is held.
fn with_chunk_mut<R>(
    s: &SharedState,
    object_id: ObjectId,
    chunk_id: &str,
    f: impl FnOnce(&mut ChunkInfo) -> R,
) -> Option<R> {
    let mut obj = s.objects.get_mut(&object_id)?;
    let chunk = obj.chunks.iter_mut().find(|c| c.chunk_id == chunk_id)?;
    Some(f(chunk))
}

// ─────────────────────────────── Phase 5: re-replication ──────────────

/// Background task: waits until asked (worker died, copy missing, worker
/// came back) or until REPAIR_INTERVAL passes, then runs one repair sweep.
pub async fn repair_loop(s: SharedState) {
    loop {
        tokio::select! {
            _ = s.repair_notify.notified() => {}
            _ = tokio::time::sleep(REPAIR_INTERVAL) => {}
        }

        let since_start = s.started_at.elapsed();
        if since_start < startup_grace() {
            tokio::time::sleep(startup_grace() - since_start).await;
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
            Err(e) => warn!(chunk_id = %chunk.chunk_id, worker = %w, "re-replication PUT failed: {e}"),
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
            Err(e) => warn!(chunk_id = %chunk.chunk_id, worker = %w, "deleting extra copy failed: {e}"),
        }
    }
    Outcome::Trimmed
}

// ─────────────────────────────── Phase 6: reconciliation ──────────────

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
    let reported: HashMap<ChunkId, common::StoredChunk> = list
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

// ─────────────────────────────── Phase 7: integrity repair ────────────

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