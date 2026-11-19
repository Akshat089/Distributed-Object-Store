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
use common::{AppError, AppResult, ObjectId};

use crate::state::SharedState;

pub fn router(state: SharedState) -> Router {
    let max_upload = state.config.max_upload_bytes;

    let api = Router::new()
        .route(
            "/objects",
            get(objects::list_objects)
                .post(upload::upload_object)
                .layer(DefaultBodyLimit::max(max_upload)),
        )
        .route(
            "/objects/{id}",
            get(objects::download_object).delete(objects::delete_object),
        )
        .route(
            "/objects/{id}/metadata",
            get(objects::get_metadata).patch(objects::patch_metadata),
        )
        .route("/cluster", get(cluster::cluster_status));

    let internal = Router::new()
        .route("/workers/register", post(internal::register_worker))
        .route("/workers/heartbeat", post(internal::heartbeat))
        .route("/chunks/corrupt", post(internal::report_corrupt));

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
