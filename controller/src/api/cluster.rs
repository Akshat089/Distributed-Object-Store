//! Cluster-wide status, for monitoring and the demo.

use axum::{Json, extract::State};
use common::{ClusterStatus, REPLICATION_FACTOR};

use crate::state::SharedState;

/// GET /api/v1/cluster — worker status + replication health.
pub async fn cluster_status(State(s): State<SharedState>) -> Json<ClusterStatus> {
    let (mut chunk_count, mut under, mut unavailable) = (0, 0, 0);
    for o in s.objects.iter() {
        for c in &o.chunks {
            chunk_count += 1;
            let live = c.locations.iter().filter(|w| s.is_alive(w)).count();
            if live == 0 {
                unavailable += 1;
            }
            if live < REPLICATION_FACTOR {
                under += 1;
            }
        }
    }
    Json(ClusterStatus {
        workers: s.worker_views(),
        object_count: s.objects.len(),
        chunk_count,
        under_replicated_chunks: under,
        unavailable_chunks: unavailable,
        corrupt_copies_detected: s.integrity.detected(),
        corrupt_copies_repaired: s.integrity.repaired(),
    })
}
