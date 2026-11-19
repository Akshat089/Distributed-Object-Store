//! Controller shared state: worker registry, in-memory metadata, and the
//! handles every task needs (HTTP client, repair trigger, counters).

use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use common::{
    AppError, AppResult, HashRing, ObjectId, ObjectMeta, WorkerId, WorkerStatus, WorkerView,
};
use dashmap::DashMap;
use tokio::sync::Notify;

use crate::{config::Config, meta_store::MetaStore};

pub type SharedState = Arc<AppState>;

// ─────────────────────────────── Workers ──────────────────────────────

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub address: String,
    pub status: WorkerStatus,
    /// Monotonic clock, used for timeout checks.
    pub last_heartbeat: Instant,
    /// Wall clock, shown in the cluster status.
    pub last_seen: DateTime<Utc>,
    pub chunk_count: usize,
    pub bytes_used: u64,
}

// ─────────────────────────────── App state ────────────────────────────

pub struct AppState {
    pub config: Config,
    pub workers: DashMap<WorkerId, WorkerInfo>,
    /// Every worker that ever registered. Liveness is checked separately.
    pub ring: RwLock<HashRing>,
    pub objects: DashMap<ObjectId, ObjectMeta>,
    pub meta_store: MetaStore,
    pub http: reqwest::Client,
    /// Wakes the repair loop (a worker died, a copy went missing, ...).
    pub repair_notify: Notify,
    /// Objects whose upload is still in progress. Their chunks already sit on
    /// workers but the metadata isn't saved yet, so reconciliation must not
    /// mistake them for orphans.
    pub pending_uploads: DashMap<ObjectId, ()>,
    /// Workers currently being reconciled (avoids running two at once).
    pub reconciling: DashMap<WorkerId, ()>,
    /// When the controller started. Right after a restart no worker has
    /// registered yet, so every chunk *looks* under-replicated; repair waits
    /// out a grace period instead of copying everything around.
    pub started_at: Instant,
    /// `<chunk_id>@<worker>` pairs whose bad copy is being repaired right now,
    /// so a download and the scrubber don't repair the same copy twice.
    pub corrupt_repairs: DashMap<String, ()>,
    /// Bad copies seen but not yet repaired (so a copy that can't be fixed
    /// isn't counted again every time a scrubber re-reports it).
    pub known_bad: DashMap<String, ()>,
    /// Counters shown in `GET /api/v1/cluster` (Phase 7 demo).
    pub integrity: IntegrityStats,
}

#[derive(Debug, Default)]
pub struct IntegrityStats {
    pub detected: AtomicU64,
    pub repaired: AtomicU64,
}

impl IntegrityStats {
    pub fn detected(&self) -> u64 {
        self.detected.load(Ordering::Relaxed)
    }
    pub fn repaired(&self) -> u64 {
        self.repaired.load(Ordering::Relaxed)
    }
    pub fn add_detected(&self) {
        self.detected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_repaired(&self) {
        self.repaired.fetch_add(1, Ordering::Relaxed);
    }
}

impl AppState {
    pub fn new(config: Config, meta_store: MetaStore, objects: Vec<ObjectMeta>) -> SharedState {
        let http = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(15))
            .build()
            .expect("failed to build HTTP client");

        let map = DashMap::new();
        for o in objects {
            map.insert(o.object_id, o);
        }

        Arc::new(Self {
            config,
            workers: DashMap::new(),
            ring: RwLock::new(HashRing::default()),
            objects: map,
            meta_store,
            http,
            repair_notify: Notify::new(),
            pending_uploads: DashMap::new(),
            reconciling: DashMap::new(),
            started_at: Instant::now(),
            corrupt_repairs: DashMap::new(),
            known_bad: DashMap::new(),
            integrity: IntegrityStats::default(),
        })
    }

    /// Asks the repair loop to run soon.
    pub fn request_repair(&self) {
        self.repair_notify.notify_one();
    }

    /// Writes the *current* in-memory version of an object to disk.
    ///
    /// Several tasks (PATCH, repair, reconciliation) may change the same
    /// object. Each one edits the DashMap entry, then calls this. Because the
    /// clone is taken while holding the store's write lock, the last write to
    /// disk always contains every change, never an older snapshot.
    /// If the object was deleted in the meantime nothing is written.
    pub async fn persist_object(&self, id: ObjectId) -> AppResult<()> {
        let _guard = self.meta_store.write_lock.lock().await;
        let Some(meta) = self.objects.get(&id).map(|o| o.clone()) else {
            return Ok(());
        };
        self.meta_store.write_file(&meta).await
    }

    pub fn is_alive(&self, id: &str) -> bool {
        self.workers
            .get(id)
            .is_some_and(|w| w.status == WorkerStatus::Alive)
    }

    pub fn worker_addr(&self, id: &str) -> Option<String> {
        self.workers.get(id).map(|w| w.address.clone())
    }

    /// All alive workers, in ring order for `key` (best placement first).
    pub fn placement_candidates(&self, key: &str) -> Vec<WorkerId> {
        let ring = self.ring.read().unwrap();
        ring.pick_targets(key, ring.len(), |w| self.is_alive(w))
    }

    /// Adds a worker or refreshes an existing one; marks it alive.
    /// Returns the previous status (None if brand new).
    pub fn upsert_worker(&self, id: &str, address: &str) -> Option<WorkerStatus> {
        let now = Instant::now();
        let prev = self.workers.get(id).map(|w| w.status);
        self.workers
            .entry(id.to_string())
            .and_modify(|w| {
                w.address = address.to_string();
                w.status = WorkerStatus::Alive;
                w.last_heartbeat = now;
                w.last_seen = Utc::now();
            })
            .or_insert_with(|| WorkerInfo {
                address: address.to_string(),
                status: WorkerStatus::Alive,
                last_heartbeat: now,
                last_seen: Utc::now(),
                chunk_count: 0,
                bytes_used: 0,
            });
        self.ring.write().unwrap().add_node(id);
        prev
    }

    pub fn worker_views(&self) -> Vec<WorkerView> {
        let mut v: Vec<_> = self
            .workers
            .iter()
            .map(|e| WorkerView {
                worker_id: e.key().clone(),
                address: e.address.clone(),
                status: e.status,
                last_heartbeat: e.last_seen,
                chunk_count: e.chunk_count,
                bytes_used: e.bytes_used,
            })
            .collect();
        v.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        v
    }

    pub fn get_object(&self, id: ObjectId) -> AppResult<ObjectMeta> {
        self.objects
            .get(&id)
            .map(|o| o.clone())
            .ok_or_else(|| AppError::object_not_found(id))
    }
}
