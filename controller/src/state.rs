//! Controller configuration, shared state, worker registry and the
//! on-disk metadata store.

use std::{
    env,
    path::{Path, PathBuf},
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
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{Mutex, Notify},
};
use tracing::{info, warn};

pub type SharedState = Arc<AppState>;

// ─────────────────────────────── Config ───────────────────────────────

#[derive(Debug, Clone)]
pub struct Config {
    pub port: u16,
    /// Where object metadata is persisted.
    pub data_dir: PathBuf,
    /// Largest accepted upload, in bytes.
    pub max_upload_bytes: usize,
}

impl Config {
    pub fn from_env() -> Self {
        let port = env::var("PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(9000);
        let data_dir = env::var("DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data/controller"));
        let max_upload_mb: usize = env::var("MAX_UPLOAD_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        Self {
            port,
            data_dir,
            max_upload_bytes: max_upload_mb * 1024 * 1024,
        }
    }
}

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

// ─────────────────────────────── Metadata store ───────────────────────
//
// One JSON file per object: DATA_DIR/objects/<object_id>.json
// Written to a .tmp file then renamed, so a crash never leaves half a file.
// Easy to inspect during the demo.

pub struct MetaStore {
    dir: PathBuf,
    /// Serialises all writes/deletes: they share `<id>.json.tmp` names, and
    /// a delete must not race with a write that would bring the file back.
    pub write_lock: Mutex<()>,
}

impl MetaStore {
    pub async fn open(data_dir: &Path) -> std::io::Result<(Self, Vec<ObjectMeta>)> {
        let dir = data_dir.join("objects");
        fs::create_dir_all(&dir).await?;

        let mut objects = Vec::new();
        let mut entries = fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("json") => match fs::read(&path).await {
                    Ok(bytes) => match serde_json::from_slice::<ObjectMeta>(&bytes) {
                        Ok(meta) => objects.push(meta),
                        Err(e) => warn!(path = %path.display(), "skipping bad metadata: {e}"),
                    },
                    Err(e) => warn!(path = %path.display(), "cannot read metadata: {e}"),
                },
                Some("tmp") => {
                    let _ = fs::remove_file(&path).await;
                }
                _ => {}
            }
        }

        info!(dir = %dir.display(), objects = objects.len(), "metadata store opened");
        Ok((
            Self {
                dir,
                write_lock: Mutex::new(()),
            },
            objects,
        ))
    }

    fn path(&self, id: ObjectId) -> PathBuf {
        self.dir.join(format!("{id}.json"))
    }

    pub async fn save(&self, meta: &ObjectMeta) -> AppResult<()> {
        let _guard = self.write_lock.lock().await;
        self.write_file(meta).await
    }

    /// Caller must hold `write_lock`.
    async fn write_file(&self, meta: &ObjectMeta) -> AppResult<()> {
        let json = serde_json::to_vec_pretty(meta)
            .map_err(|e| AppError::Internal(format!("serialize metadata: {e}")))?;
        let tmp = self.dir.join(format!("{}.json.tmp", meta.object_id));
        let mut f = fs::File::create(&tmp).await?;
        f.write_all(&json).await?;
        f.sync_all().await?;
        drop(f);
        fs::rename(&tmp, self.path(meta.object_id)).await?;
        Ok(())
    }

    pub async fn delete(&self, id: ObjectId) -> AppResult<()> {
        let _guard = self.write_lock.lock().await;
        match fs::remove_file(self.path(id)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}