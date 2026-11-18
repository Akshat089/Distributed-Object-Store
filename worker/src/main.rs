mod heartbeat;
mod routes;
mod state;
mod scrubber; 
use state::{AppState, ChunkStore, Config};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Must run before anything reads env vars (including the log filter).
    let env_file = load_env_file();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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

/// Ctrl+C locally, SIGTERM from `docker stop`.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let term = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    info!("shutting down");
}

/// `cargo run -p worker -- .env.worker2` loads that file;
/// with no argument it loads `.env` if present.
/// Variables already set in the shell (or by Docker) take priority.
fn load_env_file() -> String {
    match std::env::args().nth(1) {
        Some(path) => {
            if let Err(e) = dotenvy::from_filename(&path) {
                eprintln!("error: could not load env file '{path}': {e}");
                std::process::exit(1);
            }
            path
        }
        None => match dotenvy::dotenv() {
            Ok(p) => p.display().to_string(),
            Err(_) => "(none, using defaults)".into(),
        },
    }
}