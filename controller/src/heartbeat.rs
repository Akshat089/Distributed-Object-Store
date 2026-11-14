//! Failure detection: marks workers dead when heartbeats stop.
//! (Phase 5 will trigger re-replication from here.)

use std::time::Duration;

use common::{HEARTBEAT_TIMEOUT_MS, WorkerStatus};
use tracing::warn;

use crate::state::SharedState;

pub async fn monitor(state: SharedState) {
    let timeout = Duration::from_millis(HEARTBEAT_TIMEOUT_MS);
    let mut tick = tokio::time::interval(Duration::from_secs(1));

    loop {
        tick.tick().await;
        for mut w in state.workers.iter_mut() {
            if w.status == WorkerStatus::Alive && w.last_heartbeat.elapsed() > timeout {
                w.status = WorkerStatus::Dead;
                warn!(
                    worker = %w.key(),
                    "worker marked DEAD (no heartbeat for {:.1}s)",
                    w.last_heartbeat.elapsed().as_secs_f32()
                );
            }
        }
    }
}