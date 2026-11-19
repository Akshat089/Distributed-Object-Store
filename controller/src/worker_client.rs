//! Low-level HTTP calls from the controller to one worker's internal API.
//! No decisions here: callers (placement, repair, reconciliation, integrity)
//! decide what to do with the results.

use bytes::Bytes;
use common::{CHECKSUM_HEADER, WorkerChunkList};
use reqwest::StatusCode;

use crate::state::SharedState;

/// PUT /internal/chunks/{id} — the worker verifies `checksum` before storing.
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

/// GET /internal/chunks/{id}
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

/// DELETE /internal/chunks/{id} — a 404 counts as success.
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

/// GET /internal/workers/{id}/chunks — what the worker actually has on disk.
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
