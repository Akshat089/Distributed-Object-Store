mod config;
mod heartbeat;
mod meta_store;
mod replication;
mod routes;
mod state;
mod worker_client;

use common::runtime::{init_tracing, load_env_file, shutdown_signal};
use config::Config;
use meta_store::MetaStore;
use state::AppState;
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Must run before anything reads env vars (including the log filter).
    let env_file = load_env_file();
    init_tracing();

    info!("loaded env file: {env_file}");
    let config = Config::from_env();
    info!(?config, "starting controller");

    let (meta_store, objects) = MetaStore::open(&config.data_dir).await?;
    let port = config.port;
    let state = AppState::new(config, meta_store, objects);

    tokio::spawn(heartbeat::monitor(state.clone()));
    tokio::spawn(replication::repair_loop(state.clone()));

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, routes::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
}
