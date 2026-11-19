//! HTTP API: the client-facing REST API (`/api/v1/...`) and the internal
//! endpoints workers call (`/internal/...`). This file only wires routes;
//! the handlers live in the submodules.

mod cluster;
mod internal;
mod objects;
mod upload;

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    routing::{get, post},
};
use common::{AppError, AppResult, MULTIPART_OVERHEAD, ObjectId};

use crate::state::SharedState;

pub fn router(state: SharedState) -> Router {
    // The handler enforces MAX_UPLOAD_MB itself; see MULTIPART_OVERHEAD.
    let body_limit = state
        .config
        .max_upload_bytes
        .saturating_add(MULTIPART_OVERHEAD);

    let api = Router::new()
        .route(
            "/objects",
            get(objects::list_objects)
                .post(upload::upload_object)
                .layer(DefaultBodyLimit::max(body_limit)),
        )
        .route(
            "/objects/{id}",
            get(objects::download_object).delete(objects::delete_object),
        )
        .route(
            "/objects/{id}/metadata",
            get(objects::get_metadata).patch(objects::patch_metadata),
        )
        .route("/objects/{id}/chunks", get(cluster::object_chunks))
        .route("/cluster", get(cluster::cluster_status))
        .route("/cluster/status", get(cluster::cluster_status));

    let internal = Router::new()
        .route("/workers/register", post(internal::register_worker))
        .route("/workers/heartbeat", post(internal::heartbeat))
        .route("/chunks/corrupt", post(internal::report_corrupt));

    Router::new()
        .nest("/api/v1", api)
        .nest("/internal", internal)
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({"status": "ok"})) }),
        )
        // Unknown paths and wrong methods get the JSON error format too.
        .fallback(|| async { AppError::NotFound("no such endpoint".into()) })
        .method_not_allowed_fallback(|| async {
            AppError::MethodNotAllowed("method not allowed on this endpoint".into())
        })
        .with_state(state)
}

fn parse_id(raw: &str) -> AppResult<ObjectId> {
    raw.parse()
        .map_err(|_| AppError::BadRequest(format!("'{raw}' is not a valid object id")))
}
