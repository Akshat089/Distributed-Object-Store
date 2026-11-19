//! Demo visibility (Phase 8): cluster-wide status and where every chunk of
//! an object lives.

use axum::{
    Json,
    extract::{Path, State},
};
use common::{
    AppResult, ChunkLocationView, ChunkView, ClusterStatus, ObjectChunksView, REPLICATION_FACTOR,
};

use super::parse_id;
use crate::state::SharedState;

/// GET /api/v1/objects/{id}/chunks — every chunk, its copies, and whether
/// each holder is alive right now.
pub async fn object_chunks(
    State(s): State<SharedState>,
    Path(id): Path<String>,
) -> AppResult<Json<ObjectChunksView>> {
    let meta = s.get_object(parse_id(&id)?)?;
    let mut chunks: Vec<ChunkView> = meta
        .chunks
        .iter()
        .map(|c| {
            let locations: Vec<ChunkLocationView> = c
                .locations
                .iter()
                .map(|w| ChunkLocationView {
                    worker_id: w.clone(),
                    alive: s.is_alive(w),
                })
                .collect();
            ChunkView {
                index: c.index,
                chunk_id: c.chunk_id.clone(),
                size: c.size,
                checksum: c.checksum.clone(),
                live_copies: locations.iter().filter(|l| l.alive).count(),
                locations,
            }
        })
        .collect();
    chunks.sort_by_key(|c| c.index);
    let under = chunks
        .iter()
        .filter(|c| c.live_copies < REPLICATION_FACTOR)
        .count();

    Ok(Json(ObjectChunksView {
        object_id: meta.object_id,
        name: meta.name,
        size: meta.size,
        chunk_count: meta.chunk_count,
        replication_factor: REPLICATION_FACTOR,
        under_replicated_chunks: under,
        chunks,
    }))
}

/// GET /api/v1/cluster (and /api/v1/cluster/status) — worker status +
/// replication health.
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
