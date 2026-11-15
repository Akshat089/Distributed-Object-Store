# syntax=docker/dockerfile:1
#
# Two-stage build: one image that contains BOTH binaries.
# docker-compose picks which one runs with `command:`.
#
#   Stage 1 (builder): full Rust toolchain, compiles the workspace.
#   Stage 2 (runtime): tiny Debian with just the two binaries (~100 MB
#                      instead of ~1.5 GB for the toolchain image).

# ─────────────────────────────── Stage 1: build ───────────────────────────────
# Rust >= 1.88 is needed (edition 2024 + let-chains used in the code).
FROM rust:1.90-slim-bookworm AS builder
WORKDIR /app

COPY . .

# Cache mounts keep downloaded crates and compiled dependencies between
# builds, so changing one .rs file doesn't recompile all of tokio/axum.
# (target/ lives in the cache, so copy the binaries out before the step ends.)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p controller -p worker \
 && mkdir -p /out \
 && cp target/release/controller target/release/worker /out/

# ─────────────────────────────── Stage 2: runtime ─────────────────────────────
FROM debian:bookworm-slim

# ca-certificates: TLS roots (reqwest/rustls); curl: healthchecks + debugging.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/controller /out/worker /usr/local/bin/

ENV RUST_LOG=info
# Controller metadata or worker chunks live here (a named volume in compose).
VOLUME ["/data"]

# Exec form: the binary is PID 1 and receives SIGTERM from `docker stop`
# directly, so its graceful-shutdown handler runs.
CMD ["worker"]
