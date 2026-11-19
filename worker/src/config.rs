//! Worker configuration, read from environment variables.

use std::{env, path::PathBuf, time::Duration};

use common::DEFAULT_SCRUB_INTERVAL_SECS;

#[derive(Debug, Clone)]
pub struct Config {
    /// Stable id, e.g. "worker2". Must stay the same across restarts.
    pub worker_id: String,
    pub port: u16,
    /// URL the controller uses to reach this worker, e.g. http://worker2:8081
    pub advertise_addr: String,
    pub controller_url: String,
    pub data_dir: PathBuf,
    /// How often the scrubber re-checks every chunk. Zero disables it.
    pub scrub_interval: Duration,
}

impl Config {
    /// Reads env vars, with defaults that work for local `cargo run`.
    pub fn from_env() -> Self {
        let worker_id = env::var("WORKER_ID").unwrap_or_else(|_| "worker1".into());
        let port: u16 = env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8081);
        let advertise_addr = env::var("ADVERTISE_ADDR")
            .unwrap_or_else(|_| format!("http://localhost:{port}"));
        let controller_url = env::var("CONTROLLER_URL")
            .unwrap_or_else(|_| "http://localhost:9000".into());
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(format!("./data/{worker_id}")));

        let scrub_secs: u64 = env::var("SCRUB_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SCRUB_INTERVAL_SECS);

        Self {
            worker_id,
            port,
            advertise_addr: advertise_addr.trim_end_matches('/').to_string(),
            controller_url: controller_url.trim_end_matches('/').to_string(),
            data_dir,
            scrub_interval: Duration::from_secs(scrub_secs),
        }
    }
}
