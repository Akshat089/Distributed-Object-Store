//! Worker shared state: config, the chunk store, and an HTTP client for
//! talking to the controller.

use std::{sync::Arc, time::Duration};

use crate::{config::Config, store::ChunkStore};

pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub config: Config,
    pub store: ChunkStore,
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(config: Config, store: ChunkStore) -> SharedState {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .no_proxy()
            .build()
            .expect("failed to build HTTP client");
        Arc::new(Self {
            config,
            store,
            http,
        })
    }
}
