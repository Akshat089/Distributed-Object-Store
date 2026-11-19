//! Failure detection: marks workers dead when heartbeats stop, and wakes the
//! repair loop so their chunks get copied elsewhere (Phase 5).
//!
//! The opposite direction (a dead worker coming back) is noticed in the
//! register/heartbeat handlers in `routes.rs`, which start reconciliation.

use std::time::Duration;

use common::{FAILURE_CHECK_INTERVAL, HEARTBEAT_TIMEOUT_MS, WorkerStatus};
use tracing::warn;

use crate::state::SharedState;

pub async fn monitor(state: SharedState) {
    let timeout = Duration::from_millis(HEARTBEAT_TIMEOUT_MS);
    let mut tick = tokio::time::interval(FAILURE_CHECK_INTERVAL);

    loop {
        tick.tick().await;
        let mut newly_dead = 0;
        for mut w in state.workers.iter_mut() {
            if w.status == WorkerStatus::Alive && w.last_heartbeat.elapsed() > timeout {
                w.status = WorkerStatus::Dead;
                newly_dead += 1;
                warn!(
                    worker = %w.key(),
                    "worker marked DEAD (no heartbeat for {:.1}s)",
                    w.last_heartbeat.elapsed().as_secs_f32()
                );
            }
        }
        // Wake the repair loop *after* releasing the DashMap guards above.
        if newly_dead > 0 {
            state.request_repair();
        }
    }
}
