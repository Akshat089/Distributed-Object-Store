//! POST /api/v1/objects — streaming, chunked upload.

use std::collections::HashMap;

use axum::{
    Json,
    extract::{
        Multipart, State,
        multipart::{Field, MultipartRejection},
    },
    http::{HeaderMap, StatusCode, header},
};
use bytes::BytesMut;
use chrono::Utc;
use common::{
    AppError, AppResult, CHUNK_SIZE, ChunkInfo, MAX_INFLIGHT_CHUNKS, MULTIPART_OVERHEAD,
    MetadataPatch, ObjectId, ObjectMeta, StreamingHash,
};
use tokio::task::JoinSet;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{state::SharedState, storage::placement};

/// POST /api/v1/objects  (multipart/form-data)
///   file      required — the object bytes
///   metadata  optional — JSON object of string key/values
pub async fn upload_object(
    State(s): State<SharedState>,
    headers: HeaderMap,
    multipart: Result<Multipart, MultipartRejection>,
) -> AppResult<(StatusCode, Json<ObjectMeta>)> {
    // Refuse an obviously too-large upload before reading any of it.
    let declared: Option<usize> = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    if declared.is_some_and(|n| n > s.config.max_upload_bytes.saturating_add(MULTIPART_OVERHEAD)) {
        return Err(too_large(s.config.max_upload_bytes));
    }

    let mut mp = multipart.map_err(|e| multipart_error(e.status(), e.body_text()))?;
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
            .map_err(|e| multipart_error(e.status(), e.body_text()))?
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
                        .map_err(|e| multipart_error(e.status(), e.body_text()))?;
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
                placement::delete_chunks(&s, &stored).await;
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

/// Multipart errors keep their HTTP status (413 if axum's body limit was
/// hit, 400 otherwise) but get the usual JSON error body.
fn multipart_error(status: StatusCode, text: String) -> AppError {
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        AppError::PayloadTooLarge(format!("upload too large: {text}"))
    } else {
        AppError::BadRequest(text)
    }
}

fn too_large(max_bytes: usize) -> AppError {
    AppError::PayloadTooLarge(format!(
        "upload is larger than MAX_UPLOAD_MB ({} MB)",
        max_bytes / (1024 * 1024)
    ))
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
        while let Some(piece) = field.chunk().await.map_err(|e| {
            multipart_error(e.status(), format!("upload interrupted: {}", e.body_text()))
        })? {
            hasher.update(&piece);
            size += piece.len() as u64;
            // Enforced here, not by axum's body limit (see MULTIPART_OVERHEAD).
            if size > s.config.max_upload_bytes as u64 {
                return Err(too_large(s.config.max_upload_bytes));
            }

            let mut rest = &piece[..];
            while !rest.is_empty() {
                let take = (CHUNK_SIZE - buf.len()).min(rest.len());
                buf.extend_from_slice(&rest[..take]);
                rest = &rest[take..];

                if buf.len() == CHUNK_SIZE {
                    let data = std::mem::replace(&mut buf, BytesMut::with_capacity(CHUNK_SIZE));
                    tasks.spawn(placement::place_chunk(
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
            tasks.spawn(placement::place_chunk(s.clone(), object_id, index, data));
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
