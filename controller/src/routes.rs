//! Client-facing REST API + internal worker endpoints.

use std::collections::HashMap;

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Multipart, Path, Query, State,
        multipart::{Field, MultipartRejection},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::BytesMut;
use chrono::Utc;
use common::{
    AppError, AppResult, CHUNK_SIZE, ChunkInfo, ClusterStatus, CorruptChunkReport,
    DOWNLOAD_PREFETCH, HEARTBEAT_INTERVAL_MS, HeartbeatRequest, HeartbeatResponse,
    ListObjectsResponse, ListQuery, MAX_INFLIGHT_CHUNKS, MetadataPatch, ObjectId, ObjectMeta,
    REPLICATION_FACTOR, RegisterRequest, RegisterResponse, StreamingHash, WorkerStatus,
    parse_chunk_id,
};
use futures::StreamExt;
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{replication, state::SharedState};

pub fn router(state: SharedState) -> Router {
    let max_upload = state.config.max_upload_bytes;

    let api = Router::new()
        .route(
            "/objects",
            get(list_objects)
                .post(upload_object)
                .layer(DefaultBodyLimit::max(max_upload)),
        )
        .route("/objects/{id}", get(download_object).delete(delete_object))
        .route(
            "/objects/{id}/metadata",
            get(get_metadata).patch(patch_metadata),
        )
        .route("/cluster", get(cluster_status));

    let internal = Router::new()
        .route("/workers/register", post(register_worker))
        .route("/workers/heartbeat", post(heartbeat))
        .route("/chunks/corrupt", post(report_corrupt));

    Router::new()
        .nest("/api/v1", api)
        .nest("/internal", internal)
        .route("/health", get(|| async { Json(serde_json::json!({"status": "ok"})) }))
        .fallback(|| async { AppError::NotFound("no such endpoint".into()) })
        .with_state(state)
}

fn parse_id(raw: &str) -> AppResult<ObjectId> {
    raw.parse()
        .map_err(|_| AppError::BadRequest(format!("'{raw}' is not a valid object id")))
}

// ─────────────────────────────── Upload ───────────────────────────────

/// POST /api/v1/objects  (multipart/form-data)
///   file      required — the object bytes
///   metadata  optional — JSON object of string key/values
async fn upload_object(
    State(s): State<SharedState>,
    multipart: Result<Multipart, MultipartRejection>,
) -> AppResult<(StatusCode, Json<ObjectMeta>)> {
    let mut mp = multipart.map_err(|e| AppError::BadRequest(e.body_text()))?;
    let object_id = Uuid::new_v4();

    // Tell reconciliation these chunks are not orphans (metadata not saved yet).
    s.pending_uploads.insert(object_id, ());
    let _pending = PendingGuard(&s, object_id);

    // Chunks already stored on workers — deleted again if anything fails.
    let mut stored: Vec<ChunkInfo> = Vec::new();

    let result: AppResult<ObjectMeta> = async {
        let mut file: Option<(String, String, u64, String)> = None; // name, type, size, checksum
        let mut custom = HashMap::new();

        while let Some(mut field) = mp
            .next_field()
            .await
            .map_err(|e| AppError::BadRequest(e.body_text()))?
        {
            let field_name = field.name().unwrap_or_default().to_string();
            match field_name.as_str() {
                "file" => {
                    if file.is_some() {
                        return Err(AppError::BadRequest("only one 'file' field allowed".into()));
                    }
                    let name = field.file_name().unwrap_or("unnamed").to_string();
                    let content_type = detect_content_type(field.content_type(), &name);
                    let (size, checksum) =
                        receive_file(&s, object_id, &mut field, &mut stored).await?;
                    file = Some((name, content_type, size, checksum));
                }
                "metadata" => {
                    let text = field
                        .text()
                        .await
                        .map_err(|e| AppError::BadRequest(e.body_text()))?;
                    if !text.trim().is_empty() {
                        let patch: MetadataPatch = serde_json::from_str(&text).map_err(|e| {
                            AppError::BadRequest(format!(
                                "'metadata' must be a JSON object of string values: {e}"
                            ))
                        })?;
                        patch.apply(&mut custom);
                    }
                }
                _ => {} // ignore unknown fields
            }
        }

        let (name, content_type, size, checksum) =
            file.ok_or_else(|| AppError::BadRequest("missing 'file' field".into()))?;

        stored.sort_by_key(|c| c.index);
        let now = Utc::now();
        let meta = ObjectMeta {
            object_id,
            name,
            size,
            content_type,
            created_at: now,
            modified_at: now,
            checksum,
            chunk_count: stored.len(),
            chunks: stored.clone(),
            custom,
        };
        s.meta_store.save(&meta).await?;
        Ok(meta)
    }
    .await;

    match result {
        Ok(meta) => {
            info!(
                %object_id, name = %meta.name, size = meta.size, chunks = meta.chunk_count,
                "object uploaded"
            );
            s.objects.insert(object_id, meta.clone());
            Ok((StatusCode::CREATED, Json(meta)))
        }
        Err(e) => {
            if !stored.is_empty() {
                warn!(%object_id, "upload failed, removing {} stored chunks", stored.len());
                replication::delete_chunks(&s, &stored).await;
            }
            Err(e)
        }
    }
}

/// Removes an upload from `pending_uploads` when the handler returns,
/// whichever way it returns (success, error, or client disconnect).
struct PendingGuard<'a>(&'a SharedState, ObjectId);

impl Drop for PendingGuard<'_> {
    fn drop(&mut self) {
        self.0.pending_uploads.remove(&self.1);
    }
}

fn detect_content_type(declared: Option<&str>, file_name: &str) -> String {
    match declared {
        Some(ct) if !ct.is_empty() && ct != "application/octet-stream" => ct.to_string(),
        _ => mime_guess::from_path(file_name)
            .first_or_octet_stream()
            .to_string(),
    }
}

/// Streams the file field, cutting it into CHUNK_SIZE pieces and sending
/// each to workers as soon as it's full. Never holds the whole file.
/// Returns (total size, SHA-256 of whole object).
async fn receive_file(
    s: &SharedState,
    object_id: ObjectId,
    field: &mut Field<'_>,
    stored: &mut Vec<ChunkInfo>,
) -> AppResult<(u64, String)> {
    let mut hasher = StreamingHash::new();
    let mut buf = BytesMut::with_capacity(CHUNK_SIZE);
    let mut size: u64 = 0;
    let mut index = 0usize;
    let mut tasks: JoinSet<AppResult<ChunkInfo>> = JoinSet::new();

    let result: AppResult<()> = async {
        while let Some(piece) = field
            .chunk()
            .await
            .map_err(|e| AppError::BadRequest(format!("upload interrupted: {}", e.body_text())))?
        {
            hasher.update(&piece);
            size += piece.len() as u64;

            let mut rest = &piece[..];
            while !rest.is_empty() {
                let take = (CHUNK_SIZE - buf.len()).min(rest.len());
                buf.extend_from_slice(&rest[..take]);
                rest = &rest[take..];

                if buf.len() == CHUNK_SIZE {
                    let data = std::mem::replace(&mut buf, BytesMut::with_capacity(CHUNK_SIZE));
                    tasks.spawn(replication::place_chunk(
                        s.clone(),
                        object_id,
                        index,
                        data.freeze(),
                    ));
                    index += 1;
                    while tasks.len() >= MAX_INFLIGHT_CHUNKS {
                        collect_one(&mut tasks, stored).await?;
                    }
                }
            }
        }

        if !buf.is_empty() {
            let data = std::mem::take(&mut buf).freeze();
            tasks.spawn(replication::place_chunk(s.clone(), object_id, index, data));
        }
        while !tasks.is_empty() {
            collect_one(&mut tasks, stored).await?;
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        // Let in-flight chunks finish so the caller can clean them up too.
        while let Some(r) = tasks.join_next().await {
            if let Ok(Ok(c)) = r {
                stored.push(c);
            }
        }
        return Err(e);
    }
    Ok((size, hasher.finish()))
}

async fn collect_one(
    tasks: &mut JoinSet<AppResult<ChunkInfo>>,
    stored: &mut Vec<ChunkInfo>,
) -> AppResult<()> {
    match tasks.join_next().await {
        Some(Ok(Ok(chunk))) => {
            stored.push(chunk);
            Ok(())
        }
        Some(Ok(Err(e))) => Err(e),
        Some(Err(e)) => Err(AppError::Internal(format!("chunk task panicked: {e}"))),
        None => Ok(()),
    }
}

// ─────────────────────────────── Download ─────────────────────────────

/// GET /api/v1/objects/{id} — streams the reassembled object.
async fn download_object(
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
        .map(move |c| replication::fetch_chunk(state.clone(), c))
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
async fn get_metadata(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<Json<ObjectMeta>> {
    Ok(Json(s.get_object(parse_id(&id)?)?))
}

/// PATCH /api/v1/objects/{id}/metadata
/// Body: {"key": "value", "remove_me": null}
async fn patch_metadata(
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
async fn delete_object(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<StatusCode> {
    let id = parse_id(&id)?;
    let (_, meta) = s
        .objects
        .remove(&id)
        .ok_or_else(|| AppError::object_not_found(id))?;

    s.meta_store.delete(id).await?;
    replication::delete_chunks(&s, &meta.chunks).await;
    info!(object_id = %id, chunks = meta.chunk_count, "object deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// GET /api/v1/objects?offset=0&limit=100
async fn list_objects(
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

/// GET /api/v1/cluster — worker status + replication health.
async fn cluster_status(State(s): State<SharedState>) -> Json<ClusterStatus> {
    let (mut chunk_count, mut under, mut unavailable) = (0, 0, 0);
    for o in s.objects.iter() {
        for c in &o.chunks {
            chunk_count += 1;
            let live = c.locations.iter().filter(|w| s.is_alive(w)).count();
            if live == 0 {
                unavailable += 1;
            }
            if live < REPLICATION_FACTOR {
                under += 1;
            }
        }
    }
    Json(ClusterStatus {
        workers: s.worker_views(),
        object_count: s.objects.len(),
        chunk_count,
        under_replicated_chunks: under,
        unavailable_chunks: unavailable,
        corrupt_copies_detected: s.integrity.detected(),
        corrupt_copies_repaired: s.integrity.repaired(),
    })
}

// ─────────────────────────────── Internal ─────────────────────────────

/// POST /internal/workers/register
async fn register_worker(
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
    tokio::spawn(replication::reconcile_worker(s.clone(), req.worker_id.clone()));
    Json(RegisterResponse {
        worker_id: req.worker_id,
        heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
    })
}

/// POST /internal/workers/heartbeat
async fn heartbeat(
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
        tokio::spawn(replication::reconcile_worker(s.clone(), req.worker_id.clone()));
    }
    if let Some(mut w) = s.workers.get_mut(&req.worker_id) {
        w.chunk_count = req.chunk_count;
        w.bytes_used = req.bytes_used;
    }
    Json(HeartbeatResponse { reregister: false })
}

/// POST /internal/chunks/corrupt — a worker's scrubber found a copy that
/// fails its checksum (or vanished from disk). Repair runs in the background.
async fn report_corrupt(
    State(s): State<SharedState>,
    Json(r): Json<CorruptChunkReport>,
) -> AppResult<StatusCode> {
    let (object_id, _) = parse_chunk_id(&r.chunk_id)
        .ok_or_else(|| AppError::BadRequest(format!("invalid chunk id '{}'", r.chunk_id)))?;
    warn!(worker = %r.worker_id, chunk_id = %r.chunk_id, "scrubber reported a bad copy");
    tokio::spawn(replication::repair_bad_copy(
        s.clone(),
        object_id,
        r.chunk_id,
        r.worker_id,
        None,
    ));
    Ok(StatusCode::ACCEPTED)
}