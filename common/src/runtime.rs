//! Process startup / shutdown helpers shared by the controller and worker
//! binaries (previously copy-pasted in both `main.rs` files).

use tracing::info;
use tracing_subscriber::EnvFilter;

/// `cargo run -p worker -- .env.worker2` loads that file;
/// with no argument it loads `.env` if present.
/// Variables already set in the shell (or by Docker) take priority.
///
/// Must run before anything reads env vars (including the log filter).
/// Returns a description of what was loaded, for the startup log.
pub fn load_env_file() -> String {
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

/// Logging to stdout. The level comes from `RUST_LOG` (default `info`).
///
/// Colours are on only when stdout is a terminal (`cargo run`), so
/// `docker logs` shows plain text instead of `[2m...[0m` escape codes.
/// `LOG_COLOR=1` / `LOG_COLOR=0` forces them on / off.
pub fn init_tracing() {
    use std::io::IsTerminal;

    let ansi = match std::env::var("LOG_COLOR").as_deref() {
        Ok("1" | "true") => true,
        Ok("0" | "false") => false,
        _ => std::io::stdout().is_terminal(),
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_ansi(ansi)
        .init();
}

/// Resolves on Ctrl+C locally, or SIGTERM from `docker stop`.
/// Passed to `axum::serve(...).with_graceful_shutdown(...)`.
pub async fn shutdown_signal() {
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
