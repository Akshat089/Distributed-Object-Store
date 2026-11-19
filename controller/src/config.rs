//! Controller configuration, read from environment variables.

use std::{env, path::PathBuf};

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    /// Where object metadata is persisted.
    pub data_dir: PathBuf,
    /// Largest accepted upload, in bytes.
    pub max_upload_bytes: usize,
}

impl Config {
    pub fn from_env() -> Self {
        let port = env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(9000);
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/controller"));
        let max_upload_mb: usize = env::var("MAX_UPLOAD_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        Self {
            port,
            data_dir,
            max_upload_bytes: max_upload_mb * 1024 * 1024,
        }
    }
}
