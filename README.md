# Mini Distributed Object Store

A small, S3-style object store written in Rust. You upload a file to one
**controller**; it splits the file into 1 MiB chunks, stores **2 copies of every
chunk** on different **workers**, and gives you back an object ID. The cluster
keeps your data available and correct while workers crash, come back, or have
their disks silently corrupted.

```
                 ┌──────────────────────────────────┐
  client ──HTTP──►  controller  (port 9000)          │  metadata = source of truth
                 │  - REST API  /api/v1/...          │  (one JSON file per object,
                 │  - chunking + placement (ring)    │   on the controller's volume)
                 │  - failure detection (heartbeats) │
                 │  - re-replication / reconciliation│
                 │  - integrity repair               │
                 └──────┬────────┬────────┬─────────┘
                        │        │        │    /internal/... (HTTP)
                   worker1  worker2  worker3  worker4
                   each: own Docker volume, chunk files + .sha256 files,
                         heartbeat every 2 s, background scrubber
```

- The client only talks to the controller and never needs to know where chunks live.
- Every chunk has a SHA-256 checksum, verified on write, on every read, by the
  controller before returning data, and in the background by each worker's scrubber.
- When a worker dies, its chunks are copied elsewhere until every chunk has 2
  live copies again. When it comes back, its stale copies are cleaned up.

**Tech:** Rust (edition 2024), [tokio](https://tokio.rs) (async runtime),
[axum](https://github.com/tokio-rs/axum) (HTTP server), reqwest (HTTP client),
serde (JSON), Docker + Docker Compose.

---

## Contents

1. [Quick start](#quick-start)
2. [API reference](#api-reference)
3. [Configuration](#configuration)
4. [Design decisions](#design-decisions)
5. [Running the tests and the demo](#running-the-tests-and-the-demo)
6. [Running without Docker](#running-without-docker)
7. [Project layout](#project-layout)
8. [Known limitations](#known-limitations)
9. [Troubleshooting](#troubleshooting)

---

## Quick start

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/).
Rust 1.88+ is only needed if you want to run the code without Docker.

All commands are for **Windows PowerShell**, run from the repository root.
In PowerShell use `curl.exe`, not `curl` (which is an alias for something else).

```powershell
# 1. Build the image (one image holds both binaries; ~3 min the first time)
docker build -t distributed-object-store:latest .

# 2. Start 1 controller + 4 workers
docker compose up -d
docker compose ps                         # all 5 should become "healthy"

# 3. Upload a file
curl.exe -F "file=@C:\Windows\Web\Wallpaper\Windows\img0.jpg" http://localhost:9000/api/v1/objects
#    -> 201 Created, JSON metadata including "object_id"

# 4. Download it again
curl.exe -o copy.jpg http://localhost:9000/api/v1/objects/<object_id>

# 5. See where its chunks live
curl.exe http://localhost:9000/api/v1/objects/<object_id>/chunks

# 6. Cluster health
curl.exe http://localhost:9000/api/v1/cluster/status
```

Stop the cluster with `docker compose down`. Data is kept in named volumes, so
the next `docker compose up -d` finds every object again.
`docker compose down -v` also **deletes all stored data**.

Rebuild the image (`docker build ...` then `docker compose up -d --force-recreate`)
after every code change.

---

## API reference

Base URL: `http://localhost:9000`. All bodies are JSON unless noted.

| Method | Path | Does | Success |
|---|---|---|---|
| POST | `/api/v1/objects` | Upload (multipart: `file` required, `metadata` optional) | 201 + metadata |
| GET | `/api/v1/objects?offset=0&limit=100` | List objects (paginated, oldest first) | 200 |
| GET | `/api/v1/objects/{id}` | Download the original bytes (streamed) | 200 |
| GET | `/api/v1/objects/{id}/metadata` | System + custom metadata, with chunk locations | 200 |
| PATCH | `/api/v1/objects/{id}/metadata` | Add / update / remove custom metadata keys | 200 + metadata |
| DELETE | `/api/v1/objects/{id}` | Delete the object and all its chunks | 204 |
| GET | `/api/v1/objects/{id}/chunks` | Every chunk, its 2 copies, and whether each holder is alive | 200 |
| GET | `/api/v1/cluster/status` | Workers (alive/dead, chunk counts) and replication / integrity counters. `/api/v1/cluster` is the same | 200 |
| GET | `/health` | Liveness probe | 200 |

### Examples

PowerShell 5.1 strips the double quotes from JSON passed on the command line,
so JSON goes through a file (`-d "@file.json"`, or `metadata=<file.json` in a form).

```powershell
# Upload with custom metadata
'{"owner":"akshat","course":"cloud"}' | Set-Content -Encoding ascii meta.json
curl.exe -F "file=@report.pdf" -F "metadata=<meta.json" http://localhost:9000/api/v1/objects

# List (paginated)
curl.exe "http://localhost:9000/api/v1/objects?offset=0&limit=10"

# Download (Content-Type is the one stored at upload; x-object-checksum header = SHA-256)
curl.exe -o report.pdf http://localhost:9000/api/v1/objects/<id>

# Metadata
curl.exe http://localhost:9000/api/v1/objects/<id>/metadata

# Update metadata: a string adds/updates the key, null removes it. Chunks are never touched.
'{"owner":"lakshya","course":null,"grade":"A"}' | Set-Content -Encoding ascii patch.json
curl.exe -X PATCH -H "Content-Type: application/json" -d "@patch.json" http://localhost:9000/api/v1/objects/<id>/metadata

# Delete
curl.exe -X DELETE http://localhost:9000/api/v1/objects/<id>

# Where are the chunks? (demo)
curl.exe http://localhost:9000/api/v1/objects/<id>/chunks

# Cluster status (demo)
curl.exe http://localhost:9000/api/v1/cluster/status
```

An upload response (shortened):

```json
{
  "object_id": "f54d31f0-181f-48c2-94c8-d5950fb7f82c",
  "name": "report.pdf",
  "size": 5255225,
  "content_type": "application/pdf",
  "created_at": "2026-11-19T09:06:35Z",
  "modified_at": "2026-11-19T09:06:35Z",
  "checksum": "97eefa63...",
  "chunk_count": 6,
  "chunks": [
    { "chunk_id": "f54d31f0-..._000000", "index": 0, "size": 1048576,
      "checksum": "3b1c...", "locations": ["worker1", "worker2"] }
  ],
  "custom": { "owner": "akshat", "course": "cloud" }
}
```

### Errors

Every error has the same JSON body: `{"error": "<kind>", "message": "<details>"}`.

| Status | `error` | When |
|---|---|---|
| 400 | `bad_request` | Missing `file` field, invalid `metadata` / PATCH JSON, malformed object ID, bad query string, not multipart |
| 404 | `not_found` | Unknown object ID or unknown endpoint |
| 405 | `method_not_allowed` | Wrong HTTP method for an existing path |
| 409 | `corrupted` | (internal API) a worker's stored chunk fails its checksum; it never returns the bad bytes |
| 413 | `payload_too_large` | Upload larger than `MAX_UPLOAD_MB` |
| 500 | `internal_error` | Unexpected server error (e.g. disk write failed) |
| 503 | `unavailable` | Fewer than 2 alive workers for an upload, or no valid copy of some chunk is reachable for a download |

### Edge cases (all covered by `scripts\test-edge-cases.ps1`)

| Case | Result |
|---|---|
| Empty file | 201 with `"chunk_count": 0`; downloads as 0 bytes |
| No `file` field | 400 |
| `metadata` not a JSON object of strings | 400 |
| PATCH body not JSON | 400 |
| Unknown ID / malformed ID | 404 / 400 |
| Only 1 worker alive | Upload returns 503 (nothing is stored with fewer than 2 copies) |
| Upload over `MAX_UPLOAD_MB` | 413 |

### Internal API (worker ⇄ controller, not for clients)

| Called on | Method + path | Purpose |
|---|---|---|
| controller | `POST /internal/workers/register` | Worker announces itself (on start, retried until it works) |
| controller | `POST /internal/workers/heartbeat` | Every 2 s, with chunk count and bytes used |
| controller | `POST /internal/chunks/corrupt` | A worker's scrubber reports a bad copy |
| worker | `PUT /internal/chunks/{chunk_id}` | Store a chunk; `x-chunk-checksum` header must match the body |
| worker | `GET /internal/chunks/{chunk_id}` | Read a chunk; 409 if it fails its checksum, 404 if missing |
| worker | `DELETE /internal/chunks/{chunk_id}` | Delete a chunk |
| worker | `GET /internal/workers/{worker_id}/chunks` | Everything the worker has on disk (used by reconciliation) |

Workers are published on host ports 8081–8084, so you can call them directly
while debugging, e.g. `curl.exe http://localhost:8082/internal/workers/worker2/chunks`.

---

## Configuration

Everything is configured with environment variables (set in
`docker-compose.yml`, or in a `.env` file when running without Docker).

**Controller**

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `9000` | HTTP port |
| `DATA_DIR` | `./data/controller` (`/data` in Docker) | Where object metadata is stored |
| `MAX_UPLOAD_MB` | `1024` | Largest accepted upload |
| `RUST_LOG` | `info` | Log level, e.g. `debug`, `info,controller=debug` |
| `LOG_COLOR` | auto | `1`/`0` forces coloured logs on/off; by default only on a terminal |

**Worker**

| Variable | Default | Meaning |
|---|---|---|
| `WORKER_ID` | `worker1` | Stable ID; must stay the same across restarts so the controller recognises it |
| `PORT` | `8081` | HTTP port |
| `ADVERTISE_ADDR` | `http://localhost:<PORT>` | URL the controller uses to reach this worker (`http://worker2:8081` in Docker) |
| `CONTROLLER_URL` | `http://localhost:9000` | Where to register and send heartbeats |
| `DATA_DIR` | `./data/<WORKER_ID>` (`/data` in Docker) | Chunks are stored in `DATA_DIR/chunks/` |
| `SCRUB_INTERVAL_SECS` | `30` (`10` in compose) | Seconds between scrubber passes; `0` disables the scrubber |
| `RUST_LOG`, `LOG_COLOR` | | As for the controller |

Sizes and timings that aren't meant to be changed per deployment (chunk size,
replication factor, heartbeat interval/timeout, repair interval, concurrency
limits) are all in [`common/src/constants.rs`](common/src/constants.rs).

---

## Design decisions

| Topic | Decision | Why |
|---|---|---|
| Chunking | Fixed 1 MiB chunks, ID `<object_uuid>_<index:06>` | Parallel transfers, even spread, cheap repair. The ID encodes its owner, so a worker's chunk list can be mapped back to objects, and anything that doesn't parse as `<uuid>_<number>` is rejected (no path traversal). |
| Replication | 2 copies, written synchronously; upload fails with 503 if 2 workers aren't available | An object that exists was always fully replicated when it was created. |
| Placement | Consistent-hash ring, 100 virtual nodes per worker; walk clockwise to the first 2 distinct alive workers | Deterministic, balanced, and adding/removing a worker moves few chunks. The same `pick_targets` function, with an "exclude current holders" filter, picks re-replication targets. |
| Metadata | One JSON file per object on the controller's volume, loaded at startup | Survives restarts; easy to inspect. The metadata is the **source of truth**: bytes it doesn't mention are garbage. |
| Atomic writes | Write `.tmp`, `fsync`, `rename` (controller metadata and worker chunks) | A crash never leaves a half-written file; `.tmp` leftovers are deleted at startup. |
| Failure detection | Heartbeat every 2 s; Dead after 6 s of silence (3 missed beats) | Fast reaction without declaring a briefly busy worker dead. |
| Re-replication | A repair loop (on demand + every 10 s) copies under-replicated chunks from a verified copy to a new worker | It looks at the current state each time, so a failed repair is simply retried on the next pass. |
| Startup grace | No repair in the first 12 s after the controller starts | Right after a restart no worker has re-registered, so every chunk *looks* under-replicated. |
| Extra copies | **Dropped.** When a dead worker returns, copies that were replaced elsewhere are deleted | Every chunk converges to exactly 2 copies: storage stays predictable (2x, not "2x or 3x depending on history"). If the chunk is *still* under-replicated, the returning copy is adopted instead. |
| Reconciliation | On every (re)registration, compare the worker's chunk list with the metadata: keep / adopt / delete orphan / delete extra / delete mismatched / forget missing | Cleans up after failures and deletes that happened while a worker was down. Uploads still in progress are skipped. |
| Delete | Remove metadata first, then delete chunks best-effort | Anything missed becomes an orphan that reconciliation removes later. |
| Integrity | Checksums on write and every read; worker returns 409 instead of bad bytes; controller re-verifies; bad copies are overwritten from a good copy; a scrubber re-hashes every chunk in the background | Corruption is caught even in chunks nobody reads. |
| Concurrency | Every metadata change edits the in-memory map, then `persist_object` writes the *latest* version under a lock | PATCH, repair and reconciliation can't overwrite each other's changes with stale copies. |

---

## Running the tests and the demo

All scripts are Windows PowerShell and run from the repo root.

| Script | What it checks | Time |
|---|---|---|
| `scripts\check.ps1` | `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test` (11 unit tests: hashing, chunking, chunk IDs, metadata patch, hash ring). Runs in Docker, so no local Rust is needed. | ~1 min (first run ~5 min) |
| `scripts\test-phases-4-6.ps1` | Upload/download hashes; `docker stop worker2` → dead → re-replication → `docker start worker2` → reconciliation | ~1.5 min |
| `scripts\test-phase-7.ps1` | Corrupted copy found by a download, by the scrubber, and a deleted chunk file; both copies corrupted → download aborted | ~1.5 min |
| `scripts\test-edge-cases.ps1` | Every edge case and error code above, PATCH semantics, the demo endpoints, 413 and 503 | ~1 min |
| `scripts\demo.ps1` | The 6 required demonstrations, step by step, pausing between steps (`-Fast` skips pauses) | ~3 min |

```powershell
powershell -ExecutionPolicy Bypass -File scripts\check.ps1
docker build -t distributed-object-store:latest .
docker compose up -d --force-recreate
powershell -ExecutionPolicy Bypass -File scripts\test-phases-4-6.ps1 -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\test-phase-7.ps1   -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\test-edge-cases.ps1 -NoBuild
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
```

Each test script ends with `ALL CHECKS PASSED` (exit code 0) or lists the failures (exit code 1).

Useful while presenting:

```powershell
docker compose logs -f controller              # DEAD, re-replicated, reconciliation, REPAIRED ...
docker stop worker2 ; docker start worker2     # failure / recovery
docker exec worker3 ls /data/chunks            # a worker's chunk files
docker exec worker3 sh -c 'echo junk >> /data/chunks/<chunk_id>'   # corrupt a copy
```

---

## Running without Docker

Needs Rust 1.88+ ([rustup.rs](https://rustup.rs)). One terminal per process:

```powershell
Copy-Item .env.controller.example .env.controller
Copy-Item .env.worker.example .env.worker1      # then edit WORKER_ID / PORT / ADVERTISE_ADDR / DATA_DIR
# ... same for .env.worker2 .. .env.worker4

cargo run -p controller -- .env.controller
cargo run -p worker -- .env.worker1             # one terminal each
```

- `registration attempt N failed` is normal until the controller is up.
- Ctrl+C stops a process; its data stays under `./data/`.
- `Remove-Item -Recurse -Force .\data` wipes everything.
- Variables already set in the terminal override the `.env` file.

---

## Project layout

```
common/src/              shared by both binaries
  constants.rs           chunk size, replication factor, all timings and limits
  models.rs              JSON types: ObjectMeta, ChunkInfo, requests/responses
  errors.rs              AppError -> HTTP status + {"error","message"} body
  ring.rs                consistent-hash ring, pick_targets()
  runtime.rs             env-file loading, logging setup, graceful shutdown
  lib.rs                 re-exports, sha256_hex, StreamingHash

controller/src/
  main.rs                startup: config, state, background tasks, serve
  config.rs              Config::from_env
  state.rs               AppState: worker registry, objects, counters, persist_object
  meta_store.rs          metadata JSON files (atomic writes)
  worker_client.rs       HTTP calls to workers (put/get/delete/list chunks)
  failure_detector.rs    marks workers dead after 6 s without a heartbeat
  storage/
    mod.rs               the "drop the extra copy" policy; shared helpers
    placement.rs         place_chunk, fetch_chunk (verified), delete_chunks
    repair.rs            Phase 5: repair loop, re-replication, trimming extras
    reconcile.rs         Phase 6: reconcile a returning worker
    integrity.rs         Phase 7: overwrite a bad copy with a good one
  api/
    mod.rs               router
    upload.rs            streaming chunked upload
    objects.rs           download, metadata GET/PATCH, delete, list
    cluster.rs           cluster status, per-object chunk view
    internal.rs          register, heartbeat, corrupt-chunk report

worker/src/
  main.rs                startup
  config.rs              Config::from_env
  state.rs               AppState
  store.rs               on-disk chunk store (checksum on every read)
  routes.rs              internal chunk API
  heartbeat.rs           register + heartbeat loop
  scrubber.rs            background re-hashing of every chunk

scripts/                 check, tests, demo (see above)
Dockerfile               two-stage build, one image with both binaries
docker-compose.yml       controller + worker1..worker4, one named volume each
```

---

## Known limitations

- **Single controller = single point of failure.** If it is down, nothing can be
  uploaded or downloaded (workers keep their data). A production system would
  replicate the metadata (e.g. with Raft).
- **Wiping the controller's volume deletes all data.** The metadata is the
  source of truth: after `docker compose down -v` (or losing `controller-data`),
  reconciliation treats every chunk on every worker as an orphan and deletes it.
- **When both copies of a chunk are bad**, a download that has already started
  is **aborted** (curl exit code 18) rather than finished with wrong bytes:
  the 200 status and headers were sent before that chunk was read. Bad data is
  never returned; buffering whole objects to avoid this would defeat streaming.
- **Replication factor 2** survives one failure at a time. Losing both holders
  of a chunk before repair finishes makes it unavailable (503) until one returns.
- **No authentication or TLS**; every endpoint, including `/internal`, is open.
- Placement is not rack/zone-aware (all workers are equal).
- Counters in `/cluster/status` (`corrupt_copies_*`) reset when the controller restarts.

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `curl` gives odd PowerShell errors | Use `curl.exe` |
| JSON in a command is rejected (400) | PowerShell 5.1 strips quotes; put the JSON in a file (`-d "@file.json"`) |
| Port 9000 or 8081–8084 already in use | Change the left side of `ports:` in `docker-compose.yml` |
| Code changes don't show up | `docker build -t distributed-object-store:latest .` then `docker compose up -d --force-recreate` |
| Upload returns 503 right after start | Workers are still registering; wait until `/api/v1/cluster/status` shows 4 alive |
| `docker compose down -v` lost everything | Expected: it deletes the volumes (see limitations) |
