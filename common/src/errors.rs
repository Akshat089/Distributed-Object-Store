//! One error type for every handler; converts into a JSON HTTP response.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    /// Right path, wrong HTTP method.
    #[error("{0}")]
    MethodNotAllowed(String),
    #[error("{0}")]
    Conflict(String),
    /// Upload bigger than MAX_UPLOAD_MB.
    #[error("{0}")]
    PayloadTooLarge(String),
    /// Stored data failed checksum verification.
    #[error("{0}")]
    Corrupted(String),
    /// Not enough workers / no valid copy reachable.
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Internal(String),
}

/// JSON body of every error response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
}

impl AppError {
    pub fn status(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::MethodNotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            AppError::Conflict(_) | AppError::Corrupted(_) => StatusCode::CONFLICT,
            AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            AppError::BadRequest(_) => "bad_request",
            AppError::NotFound(_) => "not_found",
            AppError::MethodNotAllowed(_) => "method_not_allowed",
            AppError::Conflict(_) => "conflict",
            AppError::PayloadTooLarge(_) => "payload_too_large",
            AppError::Corrupted(_) => "corrupted",
            AppError::Unavailable(_) => "unavailable",
            AppError::Internal(_) => "internal_error",
        }
    }

    pub fn object_not_found(id: impl std::fmt::Display) -> Self {
        AppError::NotFound(format!("object {id} not found"))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.kind().to_string(),
            message: self.to_string(),
        };
        (self.status(), Json(body)).into_response()
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::NotFound {
            AppError::NotFound(e.to_string())
        } else {
            AppError::Internal(format!("io error: {e}"))
        }
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::BadRequest(format!("invalid JSON: {e}"))
    }
}