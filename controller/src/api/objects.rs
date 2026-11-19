//! Object endpoints other than upload: download, metadata GET/PATCH,
//! delete, list.

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use common::{
    AppError, AppResult, DOWNLOAD_PREFETCH, ListObjectsResponse, ListQuery, MetadataPatch,
    ObjectMeta,
};
use futures::StreamExt;
use tracing::info;

use super::parse_id;
use crate::{state::SharedState, storage::placement};

// ─────────────────────────────── Download ─────────────────────────────

/// GET /api/v1/objects/{id} — streams the reassembled object.
pub async fn download_object(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<Response> {
    let meta = s.get_object(parse_id(&id)?)?;

    // Fail fast with 503 (before sending headers) if some chunk has
    // no alive holder at all.
    if let Some(c) = meta
        .chunks
        .iter()
        .find(|c| !c.locations.iter().any(|w| s.is_alive(w)))
    {
        return Err(AppError::Unavailable(format!(
            "chunk {} has no available copy",
            c.chunk_id
        )));
    }

    let mut chunks = meta.chunks.clone();
    chunks.sort_by_key(|c| c.index);
    let state = s.clone();
    let stream = futures::stream::iter(chunks)
        .map(move |c| placement::fetch_chunk(state.clone(), c))
        .buffered(DOWNLOAD_PREFETCH);

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&meta.content_type)
            .unwrap_or(HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(meta.size));
    let safe_name: String = meta
        .name
        .chars()
        .map(|c| if (c.is_ascii_graphic() && c != '"' && c != '\\') || c == ' ' { c } else { '_' })
        .collect();
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{safe_name}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    if let Ok(v) = HeaderValue::from_str(&meta.checksum) {
        headers.insert("x-object-checksum", v);
    }

    Ok((headers, Body::from_stream(stream)).into_response())
}

// ─────────────────────────────── Metadata ─────────────────────────────

/// GET /api/v1/objects/{id}/metadata
pub async fn get_metadata(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<Json<ObjectMeta>> {
    Ok(Json(s.get_object(parse_id(&id)?)?))
}

/// PATCH /api/v1/objects/{id}/metadata
/// Body: {"key": "value", "remove_me": null}
pub async fn patch_metadata(
    State(s): State<SharedState>,
    Path(id): Path<String>,
    body: Bytes,
) -> AppResult<Json<ObjectMeta>> {
    let id = parse_id(&id)?;
    let patch: MetadataPatch = serde_json::from_slice(&body).map_err(|e| {
        AppError::BadRequest(format!(
            "body must be a JSON object of string (or null) values: {e}"
        ))
    })?;

    let updated = {
        let mut entry = s
            .objects
            .get_mut(&id)
            .ok_or_else(|| AppError::object_not_found(id))?;
        patch.apply(&mut entry.custom);
        entry.modified_at = Utc::now();
        entry.clone()
    }; // lock released before the disk write

    // Writes the latest in-memory version (repair may be editing locations
    // of the same object concurrently).
    s.persist_object(id).await?;
    info!(object_id = %id, "metadata updated");
    Ok(Json(updated))
}

// ─────────────────────────────── Delete / list ────────────────────────

/// DELETE /api/v1/objects/{id} → 204
pub async fn delete_object(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    let id = parse_id(&id)?;
    let (_, meta) = s
        .objects
        .remove(&id)
        .ok_or_else(|| AppError::object_not_found(id))?;

    s.meta_store.delete(id).await?;
    placement::delete_chunks(&s, &meta.chunks).await;
    info!(object_id = %id, chunks = meta.chunk_count, "object deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/objects?offset=0&limit=100
pub async fn list_objects(
    State(s): State<SharedState>,
    Query(q): Query<ListQuery>,
) -> Json<ListObjectsResponse> {
    let mut all: Vec<_> = s.objects.iter().map(|o| o.summary()).collect();
    all.sort_by_key(|o| o.created_at);

    let limit = q.limit.clamp(1, 1000);
    let total = all.len();
    let objects = all.into_iter().skip(q.offset).take(limit).collect();
    Json(ListObjectsResponse {
        total,
        offset: q.offset,
        limit,
        objects,
    })
}
