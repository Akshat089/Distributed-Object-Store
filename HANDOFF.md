# Handoff: Mini Distributed Cloud Object Store (Rust)

**Status: all 9 phases are done and tested.** The README is now the main
document (how to run, API, config, design, tests, demo, limitations). This file
lists what changed in Phases 8–9, things to know before the presentation, and
what could still be done.

---

## 1. Phases at a glance

| Phase | What | Who | Where |
|---|---|---|---|
| 1 | Shared types, errors, hash ring, SHA-256 | Akshat | `common/` |
| 2 | Worker on-disk chunk store, register + heartbeat | Akshat | `worker/` |
| 3 | Controller: upload, download, metadata, list, delete, persistence | Akshat | `controller/src/api`, `storage/placement.rs` |
| 4 | Docker: two-stage Dockerfile, compose with 1 + 4 containers | Lakshya | root |
| 5 | Failure detection, re-replication | Lakshya | `failure_detector.rs`, `storage/repair.rs` |
| 6 | Recovery / reconciliation | Lakshya | `storage/reconcile.rs` |
| 7 | Integrity: scrubber, repair of bad copies, counters | Akshat | `worker/src/scrubber.rs`, `storage/integrity.rs` |
| 8 | Modular refactor, demo endpoints, `demo.ps1` | Lakshya | see 2a–2c |
| 9 | README, JSON errors everywhere, edge cases, cleanup | Lakshya | see 2d–2f |

---

## 2. What changed in Phases 8–9

### 2a. Refactor (behaviour unchanged)

Done as in the old plan, one move per commit, with clippy + unit tests after
each and both integration suites after the controller and worker moves.
Every moved file was checked line by line: only `use` lines, `pub`, and
section headers differ.

- `controller/src/replication.rs` (830 lines) → `storage/{mod,placement,repair,reconcile,integrity}.rs`
  and `worker_client.rs`. The "drop the extra copy" policy doc is now at the top of `storage/mod.rs`.
- `controller/src/routes.rs` (490 lines) → `api/{mod,upload,objects,cluster,internal}.rs`.
- `controller/src/state.rs` → `config.rs` + `meta_store.rs` + `state.rs`.
- `controller/src/heartbeat.rs` → `failure_detector.rs` (rename only).
- `worker/src/state.rs` → `config.rs` + `store.rs` + `state.rs`.
- `load_env_file`, `init_tracing`, `shutdown_signal` → `common/src/runtime.rs`
  (both `main.rs` files are now ~30 lines; `dotenvy`/`tracing-subscriber` moved to `common`).
- Every constant (chunk size, RF, heartbeat, repair interval/concurrency, startup grace,
  upload/download concurrency, default scrub interval) → `common/src/constants.rs`.
  They're re-exported, so `common::CHUNK_SIZE` etc. still work.

### 2b. Demo-visibility endpoints (from the original Phase 8 in the spec)

- `GET /api/v1/objects/{id}/chunks`: each chunk with its copies and whether
  each holder is alive, plus `under_replicated_chunks`.
- `GET /api/v1/cluster/status`: same as `/api/v1/cluster` (the spec's name for it).

### 2c. `scripts/demo.ps1`

The 6 required demonstrations, in order, with a pause between steps (`-Fast` skips them):
object storage, chunking, replication, metadata, worker failure (stop → dead →
download still works → re-replicated → start → reconciled), and data integrity
(read-time detection + repair, then the scrubber fixing an unread copy).
After steps 5 and 6 it prints the matching controller log lines.
By default it uploads the Windows wallpaper JPEG (a real ~5 MB image);
`-SampleFile <path>` uses any file, e.g. a PDF.

### 2d. Bug fixed: over-size uploads hung forever

The old plan said an upload over `MAX_UPLOAD_MB` "returns 413 as plain text".
In fact it **never answered**: when axum's body limit trips in the middle of a
multipart stream, the stream stalls instead of failing, so the request hung
until the client gave up. Now the handler enforces the limit itself:
- `Content-Length` too big → 413 at once, before reading anything;
- chunked uploads → 413 as soon as the file bytes pass the limit.

axum's own limit is set 1 MiB higher (`MULTIPART_OVERHEAD`) so it never trips first.
Both paths are tested.

### 2e. Consistent JSON errors

Every error now uses `{"error","message"}`, including:
- 413 `payload_too_large` (new `AppError` variant);
- 405 `method_not_allowed` (new variant, via `method_not_allowed_fallback`);
- a bad list query string (`?limit=abc`), which is now 400 JSON instead of axum's plain text.

### 2f. Cleanup

- Deleted `meta.json`, `patch.json`, `run.txt` (folded into the README), and the
  stray duplicate `controller/replication.rs`.
- Added `.env.controller.example` and `.env.worker.example` (run.txt referred to an
  `.env.example` that never existed). `.gitignore` allows `.env.*.example`.
- `test-phase-7.ps1` moved to `scripts/`. `test-phases-4-6.ps1` now builds with `docker build`.
- Logs: ANSI colours only when stdout is a terminal, so `docker logs` is plain text.
  `LOG_COLOR=1/0` overrides.
- `cargo fmt --all` applied (own commit). `cargo clippy --workspace --all-targets -- -D warnings` is clean.
- New `scripts/check.ps1`: fmt + clippy + tests in Docker (no local Rust needed).
- New `scripts/test-edge-cases.ps1` covering every Phase 9 edge case.

---

## 3. How to verify everything (definition of done)

From a clean slate (this **wipes all stored data**):

```powershell
docker compose down -v
powershell -ExecutionPolicy Bypass -File scripts\check.ps1            # fmt, clippy, 11 unit tests
docker build -t distributed-object-store:latest .
docker compose up -d
powershell -ExecutionPolicy Bypass -File scripts\test-phases-4-6.ps1 -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\test-phase-7.ps1   -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\test-edge-cases.ps1 -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1 -Fast
```

Every script must end with `ALL CHECKS PASSED` (the demo ends with `Demo complete.`).
Results of this run are in section 5.

---

## 4. Things that will bite you

- **Rebuild the image after every code change**: `docker compose up -d --build`
  (or `docker build ...` then `docker compose up -d --force-recreate`). Plain
  `docker compose up -d` reuses the old image. The old warning about parallel builds
  colliding no longer applies: only the controller service has `build:`, so
  compose builds the image once and the workers reuse it (tested from scratch with no image).
  (Note: compose has no `pull_policy: never`, despite what the old handoff said; it isn't needed.)
- **`docker compose down -v` deletes everything**, and if only the controller's
  volume is lost, reconciliation deletes every chunk as an orphan.
- PowerShell: use **`curl.exe`**; pass JSON through a file (PS 5.1 strips quotes).
- Port 8080 is taken on Akshat's machine; the controller uses **9000**.
- Wait for 4 alive workers (and ~12 s after a controller restart) before
  uploading or demoing failures; the scripts already do this.
- Both copies corrupted → the download is **aborted** mid-stream (curl exit 18).
  This is documented as a known limitation and is deliberate (no whole-object buffering).

---

## 5. Verification results

Run on 2026-11-19 (Windows 11, Docker Desktop with engine 29.9.3, Rust 1.90), starting from
`docker compose down -v` with the image deleted. A plain `docker compose up -d` rebuilt
it from scratch.

| Check | Result |
|---|---|
| `scripts\check.ps1` (fmt, clippy `-D warnings`, 11 unit tests) | ALL CHECKS PASSED |
| `scripts\test-phases-4-6.ps1` | 15/15 passed |
| `scripts\test-phase-7.ps1` | 12/12 passed |
| `scripts\test-edge-cases.ps1` | 30/30 passed |
| `scripts\demo.ps1 -Fast` | all 13 steps OK. worker2 DEAD after ~5 s; re-replication done ~5.5 s after `docker stop` |

**One unexplained slow repair (seen once, not reproduced).** In one demo run started a few
seconds after `test-edge-cases.ps1` had stopped and restarted workers 2–4, one chunk's
re-replication took ~80 s instead of ~1 s:
- DEAD detection was on time, and the other chunks in the same sweep were repaired in 2 s;
- no request failed or timed out (each worker call has a 15 s timeout and failures are logged);
- the data stayed correct and the cluster healed by itself.

Three attempts to reproduce it, including the same stop/restart sequence, all repaired
within a second. The most likely cause is the Docker Desktop network settling after a burst
of container restarts, but that isn't proven. Mitigation: the demo's API calls now have
timeouts and longer waits. **Before presenting, start the cluster and leave it idle for a
few seconds instead of running the test suites right before the demo.** If it happens again,
`docker compose logs controller` around the `repair sweep finished` line is where to look.

---

## 6. Possible next steps (not required by the spec)

- Unit tests for the repair / reconciliation decision logic (currently covered
  only by the integration scripts).
- Persist the integrity counters (they reset on controller restart).
- Replicate the controller's metadata (Raft) to remove the single point of failure.
- Zone-aware placement (2 copies in different racks/zones).
