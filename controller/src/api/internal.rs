//! Internal endpoints, called by workers (never by clients):
//! register, heartbeat, and the scrubber's corrupt-chunk report.

use axum::{Json, extract::State, http::StatusCode};
use common::{
    AppError, AppResult, CorruptChunkReport, HEARTBEAT_INTERVAL_MS, HeartbeatRequest,
    HeartbeatResponse, RegisterRequest, RegisterResponse, WorkerStatus, parse_chunk_id,
};
use tracing::{info, warn};

use crate::{
    state::SharedState,
    storage::{integrity, reconcile},
};

/// POST /internal/workers/register
pub async fn register_worker(
    State(s): State<SharedState>,
    Json(req): Json<RegisterRequest>,
) -> Json<RegisterResponse> {
    match s.upsert_worker(&req.worker_id, &req.address) {
        None => info!(worker = %req.worker_id, addr = %req.address, "worker registered"),
        Some(WorkerStatus::Dead) => {
            info!(worker = %req.worker_id, "worker is BACK (re-registered)")
        }
        Some(WorkerStatus::Alive) => info!(worker = %req.worker_id, "worker re-registered"),
    }
    // A (re)starting worker may hold orphans, extra copies, or have lost
    // chunks. Compare its disk with the metadata in the background.
    tokio::spawn(reconcile::reconcile_worker(
        s.clone(),
        req.worker_id.clone(),
    ));
    Json(RegisterResponse {
        worker_id: req.worker_id,
        heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
    })
}

/// POST /internal/workers/heartbeat
pub async fn heartbeat(
    State(s): State<SharedState>,
    Json(req): Json<HeartbeatRequest>,
) -> Json<HeartbeatResponse> {
    if !s.workers.contains_key(&req.worker_id) {
        // We restarted and lost the registry — ask the worker to register.
        return Json(HeartbeatResponse { reregister: true });
    }
    if s.upsert_worker(&req.worker_id, &req.address) == Some(WorkerStatus::Dead) {
        // Was unreachable (e.g. network partition / paused) but never restarted.
        info!(worker = %req.worker_id, "worker is BACK (heartbeat resumed)");
        tokio::spawn(reconcile::reconcile_worker(
            s.clone(),
            req.worker_id.clone(),
        ));
    }
    if let Some(mut w) = s.workers.get_mut(&req.worker_id) {
        w.chunk_count = req.chunk_count;
        w.bytes_used = req.bytes_used;
    }
    Json(HeartbeatResponse { reregister: false })
}

/// POST /internal/chunks/corrupt — a worker's scrubber found a copy that
/// fails its checksum (or vanished from disk). Repair runs in the background.
pub async fn report_corrupt(
    State(s): State<SharedState>,
    Json(r): Json<CorruptChunkReport>,
) -> AppResult<StatusCode> {
    let (object_id, _) = parse_chunk_id(&r.chunk_id)
        .ok_or_else(|| AppError::BadRequest(format!("invalid chunk id '{}'", r.chunk_id)))?;
    warn!(worker = %r.worker_id, chunk_id = %r.chunk_id, "scrubber reported a bad copy");
    tokio::spawn(integrity::repair_bad_copy(
        s.clone(),
        object_id,
        r.chunk_id,
        r.worker_id,
        None,
    ));
    Ok(StatusCode::ACCEPTED)
}
