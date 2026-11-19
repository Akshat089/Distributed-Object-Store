//! Worker HTTP endpoints (internal API only — clients never call these).

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use common::{AppError, AppResult, CHECKSUM_HEADER, CHUNK_SIZE, StoredChunk, WorkerChunkList};
use serde_json::json;

use crate::state::SharedState;

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/internal/chunks/{chunk_id}",
            get(get_chunk).put(put_chunk).delete(delete_chunk),
        )
        .route("/internal/workers/{worker_id}/chunks", get(list_chunks))
        // A chunk is at most 1 MiB; leave a little headroom.
        .layer(DefaultBodyLimit::max(CHUNK_SIZE + 64 * 1024))
        .with_state(state)
}

async fn health(State(s): State<SharedState>) -> Json<serde_json::Value> {
    let (chunks, bytes) = s.store.stats();
    Json(json!({
        "status": "ok",
        "worker_id": s.config.worker_id,
        "chunk_count": chunks,
        "bytes_used": bytes,
    }))
}

/// PUT /internal/chunks/{chunk_id}
/// Body: raw bytes. Header `x-chunk-checksum`: SHA-256 hex of the body.
async fn put_chunk(
    State(s): State<SharedState>,
    Path(chunk_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<(StatusCode, Json<StoredChunk>)> {
    let expected = headers
        .get(CHECKSUM_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AppError::BadRequest(format!("missing {CHECKSUM_HEADER} header")))?;

    if body.len() > CHUNK_SIZE {
        return Err(AppError::BadRequest(format!(
            "chunk is {} bytes; max is {CHUNK_SIZE}",
            body.len()
        )));
    }

    let stored = s.store.put(&chunk_id, body, expected).await?;
    tracing::info!(chunk_id = %stored.chunk_id, size = stored.size, "stored chunk");
    Ok((StatusCode::CREATED, Json(stored)))
}

/// GET /internal/chunks/{chunk_id}
/// Returns raw bytes (verified) with the checksum in `x-chunk-checksum`.
/// 404 if missing, 409 `corrupted` if the stored bytes fail verification.
async fn get_chunk(
    State(s): State<SharedState>,
    Path(chunk_id): Path<String>,
) -> AppResult<Response> {
    let (data, checksum) = s.store.get(&chunk_id).await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        CHECKSUM_HEADER,
        HeaderValue::from_str(&checksum).map_err(|e| AppError::Internal(e.to_string()))?,
    );
    Ok((headers, data).into_response())
}

/// DELETE /internal/chunks/{chunk_id} → 204, or 404 if not stored here.
async fn delete_chunk(
    State(s): State<SharedState>,
    Path(chunk_id): Path<String>,
) -> AppResult<StatusCode> {
    s.store.delete(&chunk_id).await?;
    tracing::info!(%chunk_id, "deleted chunk");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /internal/workers/{worker_id}/chunks
async fn list_chunks(
    State(s): State<SharedState>,
    Path(worker_id): Path<String>,
) -> AppResult<Json<WorkerChunkList>> {
    if worker_id != s.config.worker_id {
        return Err(AppError::NotFound(format!(
            "this is {}, not {worker_id}",
            s.config.worker_id
        )));
    }
    Ok(Json(WorkerChunkList {
        worker_id,
        chunks: s.store.list(),
    }))
}
