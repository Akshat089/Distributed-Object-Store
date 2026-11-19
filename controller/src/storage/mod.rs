//! Moving chunks between the controller and workers.
//!
//! - `placement` (Phase 3): place a chunk on REPLICATION_FACTOR workers,
//!   fetch a verified copy, delete all copies.
//! - `repair` (Phase 5): `repair_loop` / `repair_under_replicated` bring every
//!   chunk back to exactly REPLICATION_FACTOR live copies after a worker dies.
//! - `reconcile` (Phase 6): `reconcile_worker` cleans up a worker that comes back.
//! - `integrity` (Phase 7): `repair_bad_copy` overwrites a corrupted (or
//!   missing) copy with a verified good one. Triggered by downloads and by
//!   worker scrubbers.
//!
//! ## Policy for extra copies ("drop the extra")
//!
//! The invariant we converge to is: *every chunk's `locations` lists exactly
//! REPLICATION_FACTOR alive workers, and only those workers hold a copy.*
//!
//! - When a worker dies, its chunks get a new copy on another worker and the
//!   dead worker is removed from `locations` (only once the new copy exists).
//! - When that worker comes back, its old copies are no longer listed, so
//!   reconciliation sees them as extras and deletes them.
//! - If a returning worker holds a copy of a chunk that is *still*
//!   under-replicated (repair hadn't finished, or there was no spare worker),
//!   the copy is adopted instead: cheaper than copying the bytes again.
//! - If a chunk somehow ends up with more than REPLICATION_FACTOR live copies
//!   (races between repair and reconciliation), the repair sweep trims it.
//!
//! Why drop rather than keep? Storage use stays predictable (2x, not "2x
//! or 3x depending on history"), placement stays balanced, and the cluster
//! status is easy to explain: every chunk has exactly 2 copies.

pub mod integrity;
pub mod placement;
pub mod reconcile;
pub mod repair;

use common::{ChunkInfo, ObjectId};

use crate::state::SharedState;

/// Runs `f` on one chunk of one object inside the in-memory metadata map.
/// Returns `None` if the object or chunk no longer exists (e.g. it was
/// deleted while we were copying bytes around).
///
/// Never `.await` inside `f`: it runs while a DashMap shard lock is held.
fn with_chunk_mut<R>(
    s: &SharedState,
    object_id: ObjectId,
    chunk_id: &str,
    f: impl FnOnce(&mut ChunkInfo) -> R,
) -> Option<R> {
    let mut obj = s.objects.get_mut(&object_id)?;
    let chunk = obj.chunks.iter_mut().find(|c| c.chunk_id == chunk_id)?;
    Some(f(chunk))
}
