# Handoff: Mini Distributed Cloud Object Store (Rust)

Phases 1–7 are **done and tested**. What's left: **Phase 8** (make the codebase
more modular, plus the demo script) and **Phase 9** (README and polish).

---

## 1. What the system does today

```
                 ┌─────────────────────────────┐
  client ──HTTP──►  controller  (port 9000)     │  metadata = source of truth
                 │  - REST API /api/v1/...      │  (one JSON file per object,
                 │  - chunking + placement      │   in the controller volume)
                 │  - failure detection         │
                 │  - re-replication / repair   │
                 └──────┬───────┬───────┬───────┘
                        │       │       │   /internal/... (HTTP)
                   worker1  worker2  worker3  worker4   (each: own volume,
                   chunk bytes + .sha256 files on disk,   heartbeat, scrubber)
```

- Objects are split into **1 MB chunks**. Each chunk is stored on **2 different workers**,
  chosen with a consistent hash ring.
- The client only talks to the controller. It never needs to know where chunks are stored.
- Every chunk has a SHA-256. Workers check it on every write and every read.
  The controller checks it again before returning data to a client.

### Done, by phase

| Phase | What it covers | Where |
|---|---|---|
| 1 | Shared types, errors (JSON + correct HTTP codes), hash ring, SHA-256 helpers | `common/` |
| 2 | Worker: on-disk chunk store (atomic write via tmp+rename, checksum on every read), register + heartbeat | `worker/` |
| 3 | Controller: upload (streamed, chunked, 2 copies), download (streamed, verified), metadata GET/PATCH, list (paginated), delete, `/cluster` status, metadata persisted to disk | `controller/` |
| 4 | Docker: two-stage `Dockerfile` (one image with both binaries), `docker-compose.yml` (1 controller + 4 workers, named volumes, `container_name` set) | root |
| 5 | Failure detection (6 s heartbeat timeout), automatic re-replication back to 2 copies | `controller/src/heartbeat.rs`, `replication.rs` |
| 6 | Recovery: a returning worker is reconciled (orphan and extra copies deleted, mismatched copies dropped) | `controller/src/replication.rs` |
| 7 | Data integrity: worker **scrubber** re-hashes every chunk every `SCRUB_INTERVAL_SECS`. Bad or missing copies are overwritten from a good copy. Downloads skip bad copies and repair them. Counters appear in `/cluster`. | `worker/src/scrubber.rs`, `controller/src/replication.rs` |

### API (all JSON errors look like `{"error":"not_found","message":"..."}`)

| Method | Path | Notes |
|---|---|---|
| POST | `/api/v1/objects` | multipart: `file` (required), `metadata` (optional JSON). Returns 201 + full metadata |
| GET | `/api/v1/objects/{id}` | streams the original bytes with the right `Content-Type` |
| GET | `/api/v1/objects/{id}/metadata` | system + custom metadata, **including chunk locations** |
| PATCH | `/api/v1/objects/{id}/metadata` | `{"k":"v"}` adds or updates, `{"k":null}` removes |
| DELETE | `/api/v1/objects/{id}` | 204 |
| GET | `/api/v1/objects?offset=&limit=` | paginated list |
| GET | `/api/v1/cluster` | workers alive/dead, `under_replicated_chunks`, `unavailable_chunks`, `corrupt_copies_detected/repaired` |
| — | `/internal/...` | worker register/heartbeat, chunk PUT/GET/DELETE, worker chunk list, corrupt report |

### How to run and test (Windows PowerShell, from the repo root)

```powershell
cargo test --workspace                                   # 11 unit tests (common)
docker build -t distributed-object-store:latest .        # build image (ALWAYS after code changes)
docker compose up -d --force-recreate                    # start 5 containers
powershell -ExecutionPolicy Bypass -File scripts\test-phases-4-6.ps1 -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\test-phase-7.ps1   -NoBuild
docker compose logs -f controller                        # watch what happens
```
Both scripts must end with **`ALL CHECKS PASSED`**. They are the safety net for the
refactor below.

### Things that will bite you

- **Build the image with `docker build`, not `docker compose up --build`.** All 5 services share one image, and parallel builds collide (`image already exists`). Compose uses `pull_policy: never`.
- **Never run `docker compose down -v` unless you mean it.** If the controller's volume is wiped, reconciliation treats every chunk on every worker as an orphan and deletes it.
- Port **8080** is taken on Akshat's machine, so the controller uses **9000**.
- In PowerShell use **`curl.exe`**, not `curl`. Pass JSON through a file (`-d "@patch.json"`), because PS 5.1 mangles quotes.
- Rust **1.88+** is needed (edition 2024, `if let ... &&` chains).
- When both copies of a chunk are corrupted, the download starts with HTTP 200 and is then **aborted** (curl exit 18), because headers are already sent. No bad bytes are ever returned. Document this as a known limitation; don't "fix" it by buffering whole objects.

---

## 2. Phase 8: modular codebase + demo script

### 8a. Refactor (behaviour must not change)

Today `controller/src/replication.rs` is **~830 lines** covering 5 jobs, and
`controller/src/routes.rs` is **~490 lines** covering 3. Split them by responsibility.

**Target layout, controller:**
```
controller/src/
  main.rs              startup only: config, state, spawn tasks, serve
  config.rs            Config::from_env            (from state.rs)
  state.rs             AppState, WorkerInfo, IntegrityStats, worker registry fns
  meta_store.rs        MetaStore                   (from state.rs)
  worker_client.rs     put/get/delete/list chunk HTTP calls + FetchError
  failure_detector.rs  heartbeat monitor           (rename of heartbeat.rs)
  storage/
    mod.rs
    placement.rs       place_chunk, fetch_chunk, delete_chunks, fetch_verified
    repair.rs          Phase 5: repair_loop, repair_under_replicated, fix/re_replicate/settle
    reconcile.rs       Phase 6: reconcile_worker
    integrity.rs       Phase 7: repair_bad_copy
  api/
    mod.rs             router() only
    objects.rs         download, metadata GET/PATCH, delete, list
    upload.rs          upload_object, receive_file, collect_one, PendingGuard, detect_content_type
    cluster.rs         cluster_status
    internal.rs        register_worker, heartbeat, report_corrupt
```

**Target layout, worker:**
```
worker/src/
  main.rs
  config.rs     Config::from_env   (from state.rs)
  store.rs      ChunkStore + write_synced (from state.rs)
  state.rs      AppState only
  routes.rs     unchanged
  heartbeat.rs  unchanged
  scrubber.rs   unchanged
```

**Remove duplication:**
- `load_env_file()` and `shutdown_signal()` are copy-pasted in both `main.rs` files. Also the
  `tracing_subscriber` setup. Move them into `common/src/runtime.rs`
  (`common` then needs `tokio` with the `signal` feature, plus `dotenvy` and `tracing-subscriber`).
- Put the timing constants in one place. `REPAIR_INTERVAL`, `REPAIR_CONCURRENCY` and the
  startup grace are in `replication.rs`, while heartbeat values are in `common/lib.rs`.

**Rules for the refactor:**
1. **Move code, don't rewrite it.** Change only `mod`/`use` lines and visibility (`pub(crate)`).
2. One move per commit. After each one: `cargo clippy --workspace --all-targets` (no warnings),
   `cargo test --workspace`, rebuild the image, and run **both** test scripts.
3. Keep the doc comments. They explain the design decisions (drop-extra policy,
   startup grace, pending uploads, persist_object locking).
4. Run `cargo fmt --all` at the end, as its own commit.

### 8b. Demo script `scripts/demo.ps1`

The spec's section 10 lists **6 required demonstrations**. Write one script that walks through
them **in order**, with a heading and a `Read-Host "Press Enter to continue"` between steps,
so it can be presented live. Reuse the helpers from the two test scripts.

| # | Demonstration | What to show |
|---|---|---|
| 1 | Object storage | upload a file (use a real image or PDF), download it, show both SHA-256s match |
| 2 | Chunking & distribution | upload a file > 1 MB; print chunk index, size, workers |
| 3 | Replication | for every chunk, show 2 locations on different workers; show `/cluster` chunk counts per worker |
| 4 | Metadata | upload with custom metadata, GET it, PATCH it (add/update/remove), GET again; show the chunks didn't change |
| 5 | Worker failure | `/cluster` before → `docker stop worker2` → wait for DEAD → download still works → wait for `under_replicated_chunks = 0` → `docker start worker2` → reconciliation → every chunk has 2 copies again |
| 6 | Data integrity | `docker exec` to corrupt a chunk → worker returns 409 → download still correct → copy repaired → also show the scrubber fixing one that was never read; show the counters |

Nice to have: a `-Fast` switch that skips the pauses, and printing the matching
`docker compose logs controller --since 30s` lines after steps 5 and 6.

---

## 3. Phase 9: README and polish

**`README.md`** (the spec requires build/start/interaction instructions):
- What it is, the architecture diagram above, and the tech used (Rust, axum, tokio, Docker)
- Prerequisites: Docker Desktop, plus Rust 1.88+ only for local dev
- Quick start: `docker build` → `docker compose up -d` → upload/download with `curl.exe`
- Full API reference with one `curl.exe` example per endpoint, plus the error table (400/404/409/500/503)
- Configuration: every env var (`PORT`, `DATA_DIR`, `MAX_UPLOAD_MB`, `WORKER_ID`,
  `ADVERTISE_ADDR`, `CONTROLLER_URL`, `SCRUB_INTERVAL_SECS`, `RUST_LOG`)
- Design decisions: consistent hashing, chunk ID format `<uuid>_<index>`, atomic writes,
  "drop the extra copy" policy, 6 s failure detection, startup grace, scrubber,
  metadata as the source of truth
- How to run the tests and the demo
- Known limitations: single controller (single point of failure), aborted download when
  both copies are bad, wiping the controller volume deletes all data, no auth

**Cleanup:**
- Delete the stray `meta.json` and `patch.json` in the repo root.
- Fold `run.txt` into the README and delete it. It also mentions a `.env.example`
  that doesn't exist: either add `.env.example` + `.env.controller.example` or drop that section.
- Turn off ANSI colour codes in logs: `.with_ansi(false)` on the `tracing_subscriber`, or make it
  conditional on an env var (`LOG_COLOR=0`) set in `docker-compose.yml`.
- `Makefile` or `scripts/` shortcuts are optional (Windows users mostly run the `.ps1` files).

**Edge cases to check (they should already work; just verify and list them in the README):**
- empty file → 201 with `chunk_count: 0`, downloads as 0 bytes
- missing `file` field → 400; bad `metadata` JSON → 400; bad PATCH body → 400
- unknown / malformed object id → 404 / 400
- upload with only 1 worker alive → 503
- Upload larger than `MAX_UPLOAD_MB` → axum returns **413 as plain text, not JSON**. Optionally map it to the JSON error format.

**Definition of done:** both test scripts and `demo.ps1` pass on a fresh
`docker compose down -v && docker build ... && docker compose up -d`; `cargo clippy` is
clean; `cargo fmt --check` passes; the README is enough for someone who has never seen the
project to run the demo.
