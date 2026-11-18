//! Background scrubber (Phase 7).
//!
//! Every SCRUB_INTERVAL_SECS it re-reads every chunk on this worker and
//! re-hashes it. A chunk that fails its checksum, or whose file has
//! disappeared, is reported to the controller, which overwrites it with a
//! verified copy from another worker.
//!
//! Without this, corruption is only noticed when somebody downloads that
//! chunk. With it, bad copies are found and fixed even if nobody reads them.

use std::time::Duration;

use common::{AppError, CorruptChunkReport};
use tracing::{debug, info, warn};

use crate::state::SharedState;

pub async fn run(state: SharedState) {
    let interval = state.config.scrub_interval;
    if interval.is_zero() {
        info!("scrubber disabled (SCRUB_INTERVAL_SECS=0)");
        return;
    }
    info!("scrubber checks every chunk every {}s", interval.as_secs());

    loop {
        tokio::time::sleep(interval).await;
        scrub_once(&state).await;
    }
}

async fn scrub_once(state: &SharedState) {
    let (mut ok, mut bad) = (0usize, 0usize);

    // Chunks a read already found missing (they're no longer in the index,
    // so the loop below can't see them).
    for id in state.store.take_lost() {
        bad += 1;
        warn!(chunk_id = %id, "scrubber: chunk file is MISSING, reporting to controller");
        report(state, &id).await;
    }

    let ids: Vec<String> = state.store.list().into_iter().map(|c| c.chunk_id).collect();

    for id in ids {
        match state.store.get(&id).await {
            Ok(_) => ok += 1,
            Err(AppError::Corrupted(_)) => {
                bad += 1;
                warn!(chunk_id = %id, "scrubber: chunk is CORRUPTED, reporting to controller");
                report(state, &id).await;
            }
            Err(AppError::NotFound(_)) => {
                // Was in the index but the file is gone (e.g. deleted by
                // hand). `get` moved it to the lost list; report it now.
                if state.store.take_lost_one(&id) {
                    bad += 1;
                    warn!(chunk_id = %id, "scrubber: chunk file is MISSING, reporting to controller");
                    report(state, &id).await;
                }
            }
            Err(e) => warn!(chunk_id = %id, "scrubber could not read chunk: {e}"),
        }
        // Don't hog the runtime on workers with many chunks.
        tokio::task::yield_now().await;
    }

    if bad > 0 {
        info!(checked = ok + bad, bad, "scrub pass finished");
    } else {
        debug!(checked = ok, "scrub pass finished, all chunks healthy");
    }
}

async fn report(state: &SharedState, chunk_id: &str) {
    let cfg = &state.config;
    let res = state
        .http
        .post(format!("{}/internal/chunks/corrupt", cfg.controller_url))
        .json(&CorruptChunkReport {
            worker_id: cfg.worker_id.clone(),
            chunk_id: chunk_id.to_string(),
        })
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .and_then(|r| r.error_for_status());
    if let Err(e) = res {
        // Controller down: the next pass will report it again.
        warn!(%chunk_id, "could not report bad chunk: {e}");
    }
}