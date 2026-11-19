//! Every size, count and timing knob of the cluster, in one place.
//! Re-exported from the crate root, so `common::CHUNK_SIZE` etc. still work.

use std::time::Duration;

// ─────────────────────────────── Data layout ──────────────────────────

/// Size of every chunk except (possibly) the last one: 1 MiB.
pub const CHUNK_SIZE: usize = 1024 * 1024;

/// Number of copies each chunk must have during normal operation.
pub const REPLICATION_FACTOR: usize = 2;

/// Header carrying the SHA-256 (hex) of a chunk body on internal PUT/GET.
pub const CHECKSUM_HEADER: &str = "x-chunk-checksum";

// ─────────────────────────────── Failure detection ────────────────────

/// How often workers send heartbeats.
pub const HEARTBEAT_INTERVAL_MS: u64 = 2_000;

/// A worker with no heartbeat for this long is considered dead.
pub const HEARTBEAT_TIMEOUT_MS: u64 = 6_000;

/// How often the controller checks heartbeat ages.
pub const FAILURE_CHECK_INTERVAL: Duration = Duration::from_secs(1);

// ─────────────────────────────── Repair (controller) ──────────────────

/// The repair sweep runs at least this often, even if nothing asked for it
/// (catches repairs that failed earlier, e.g. while no spare worker existed).
pub const REPAIR_INTERVAL: Duration = Duration::from_secs(10);

/// How many chunks are repaired at the same time.
pub const REPAIR_CONCURRENCY: usize = 8;

/// After a controller restart, wait this long for workers to re-register
/// before deciding that chunks are under-replicated.
pub const STARTUP_GRACE: Duration = Duration::from_millis(HEARTBEAT_TIMEOUT_MS * 2);

// ─────────────────────────────── Data path (controller) ───────────────

/// How many chunks of one upload are sent to workers at the same time.
pub const MAX_INFLIGHT_CHUNKS: usize = 8;

/// How many chunks of one download are fetched ahead.
pub const DOWNLOAD_PREFETCH: usize = 4;

/// Extra room on top of MAX_UPLOAD_MB for the multipart framing and the
/// `metadata` field. The upload handler enforces MAX_UPLOAD_MB on the file
/// bytes itself (clean JSON 413); axum's own body limit sits this much higher
/// so it never trips first (when it does, the multipart stream stalls
/// instead of failing, and the client never gets a response).
pub const MULTIPART_OVERHEAD: usize = 1024 * 1024;

// ─────────────────────────────── Integrity (worker) ───────────────────

/// Default for `SCRUB_INTERVAL_SECS`: how often a worker's scrubber
/// re-hashes every chunk it stores. 0 disables the scrubber.
pub const DEFAULT_SCRUB_INTERVAL_SECS: u64 = 30;
