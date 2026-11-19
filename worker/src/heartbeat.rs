//! Registers with the controller, then sends heartbeats forever.
//!
//! - Controller not up yet → keep retrying registration (worker still serves
//!   its HTTP API meanwhile, so you can test it standalone with curl).
//! - Controller restarted and forgot us → it answers `reregister: true`
//!   (or 404), and we register again.

use std::time::Duration;

use common::{
    HEARTBEAT_INTERVAL_MS, HeartbeatRequest, HeartbeatResponse, RegisterRequest, RegisterResponse,
};
use reqwest::StatusCode;
use tracing::{info, warn};

use crate::state::SharedState;

pub async fn run(state: SharedState) {
    let mut interval = Duration::from_millis(HEARTBEAT_INTERVAL_MS);

    loop {
        if let Some(ms) = register_until_ok(&state).await {
            interval = Duration::from_millis(ms);
        }

        let mut failures = 0u32;
        loop {
            tokio::time::sleep(interval).await;
            match send_heartbeat(&state).await {
                Ok(false) => {
                    if failures > 0 {
                        info!("heartbeat recovered after {failures} failures");
                    }
                    failures = 0;
                }
                Ok(true) => {
                    warn!("controller does not know us; re-registering");
                    break;
                }
                Err(e) => {
                    failures += 1;
                    if failures == 1 || failures.is_multiple_of(15) {
                        warn!("heartbeat failed ({failures}x): {e}");
                    }
                }
            }
        }
    }
}

/// Returns the heartbeat interval the controller asked for.
async fn register_until_ok(state: &SharedState) -> Option<u64> {
    let cfg = &state.config;
    let url = format!("{}/internal/workers/register", cfg.controller_url);
    let body = RegisterRequest {
        worker_id: cfg.worker_id.clone(),
        address: cfg.advertise_addr.clone(),
    };

    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let res = async {
            state
                .http
                .post(&url)
                .json(&body)
                .send()
                .await?
                .error_for_status()?
                .json::<RegisterResponse>()
                .await
        }
        .await;

        match res {
            Ok(r) => {
                info!(controller = %cfg.controller_url, "registered as {}", r.worker_id);
                return Some(r.heartbeat_interval_ms);
            }
            Err(e) => {
                if attempt == 1 || attempt.is_multiple_of(15) {
                    warn!("registration attempt {attempt} failed: {e} (retrying)");
                }
                tokio::time::sleep(Duration::from_millis(HEARTBEAT_INTERVAL_MS)).await;
            }
        }
    }
}

/// Ok(true) means "please re-register".
async fn send_heartbeat(state: &SharedState) -> reqwest::Result<bool> {
    let cfg = &state.config;
    let (chunk_count, bytes_used) = state.store.stats();
    let resp = state
        .http
        .post(format!("{}/internal/workers/heartbeat", cfg.controller_url))
        .json(&HeartbeatRequest {
            worker_id: cfg.worker_id.clone(),
            address: cfg.advertise_addr.clone(),
            chunk_count,
            bytes_used,
        })
        .send()
        .await?;

    if resp.status() == StatusCode::NOT_FOUND {
        return Ok(true);
    }
    let r: HeartbeatResponse = resp.error_for_status()?.json().await?;
    Ok(r.reregister)
}
