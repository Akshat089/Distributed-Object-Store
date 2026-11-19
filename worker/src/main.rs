mod heartbeat;
mod routes;
mod scrubber;
mod state;

use common::runtime::{init_tracing, load_env_file, shutdown_signal};
use state::{AppState, ChunkStore, Config};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Must run before anything reads env vars (including the log filter).
    let env_file = load_env_file();
    init_tracing();

    info!("loaded env file: {env_file}");
    let config = Config::from_env();
    info!(?config, "starting worker");

    let store = ChunkStore::open(&config.data_dir).await?;
    let port = config.port;
    let state = AppState::new(config, store);

    tokio::spawn(heartbeat::run(state.clone()));
    tokio::spawn(scrubber::run(state.clone()));

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, routes::router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await
}
