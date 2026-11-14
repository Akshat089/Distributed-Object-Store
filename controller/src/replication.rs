//! Moving chunks between the controller and workers.
//!
//! Phase 3: place a chunk on REPLICATION_FACTOR workers, fetch a verified
//! copy, delete all copies.
//! Phase 5+ adds re-replication and repair here.

use bytes::Bytes;
use common::{
    AppError, AppResult, CHECKSUM_HEADER, ChunkInfo, ObjectId, REPLICATION_FACTOR, make_chunk_id,
    sha256_hex,
};
use reqwest::StatusCode;
use tracing::{debug, warn};

use crate::state::SharedState;

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
    Corrupted,
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
pub async fn fetch_chunk(s: SharedState, chunk: ChunkInfo) -> AppResult<Bytes> {
    let mut order = chunk.locations.clone();
    order.sort_by_key(|w| !s.is_alive(w)); // alive first

    for worker in &order {
        match get_chunk_from(&s, worker, &chunk.chunk_id).await {
            Ok(data) if sha256_hex(&data) == chunk.checksum => return Ok(data),
            Ok(_) => warn!(chunk_id = %chunk.chunk_id, %worker, "copy failed checksum at controller"),
            Err(FetchError::Corrupted) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "worker reports copy corrupted")
            }
            Err(FetchError::Other(e)) => {
                warn!(chunk_id = %chunk.chunk_id, %worker, "fetch failed: {e}")
            }
        }
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