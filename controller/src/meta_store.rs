//! On-disk metadata store: the controller's source of truth.
//!
//! One JSON file per object: DATA_DIR/objects/<object_id>.json
//! Written to a .tmp file then renamed, so a crash never leaves half a file.
//! Easy to inspect during the demo.

use std::path::{Path, PathBuf};

use common::{AppError, AppResult, ObjectId, ObjectMeta};
use tokio::{fs, io::AsyncWriteExt, sync::Mutex};
use tracing::{info, warn};

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

    /// Caller must hold `write_lock` (see `AppState::persist_object`).
    pub(crate) async fn write_file(&self, meta: &ObjectMeta) -> AppResult<()> {
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
