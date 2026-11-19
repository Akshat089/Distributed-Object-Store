<#
Static checks + unit tests, run inside Docker (no local Rust needed):

  cargo fmt --all --check
  cargo clippy --workspace --all-targets -- -D warnings
  cargo test --workspace

Run from the repo root:
    powershell -ExecutionPolicy Bypass -File scripts\check.ps1
    ... -SkipFmt   skip the formatting check
    ... -Fix       run `cargo fmt --all` (rewrites files) instead of checking

The first run builds a small `dos-dev` image (Rust + clippy + rustfmt) and
compiles every dependency (a few minutes). Later runs reuse the cached
`dos-target` / `dos-cargo` Docker volumes and take seconds.
If you have Rust 1.88+ installed you can run the same cargo commands directly.
#>
param([switch]$SkipFmt, [switch]$Fix)

$ErrorActionPreference = "Stop"
$repo = (Resolve-Path "$PSScriptRoot\..").Path
$image = "dos-dev:1.90"

docker image inspect $image *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Building $image (one time)..." -ForegroundColor Cyan
    "FROM rust:1.90-slim-bookworm`nRUN rustup component add clippy rustfmt" | docker build -t $image -
    if ($LASTEXITCODE -ne 0) { throw "could not build $image" }
}

function Cargo([string]$cmd) {
    Write-Host "`n> cargo $cmd" -ForegroundColor Cyan
    # Out-Host: print cargo's output instead of returning it with the exit code.
    docker run --rm -v "${repo}:/src" -v dos-target:/target -v dos-cargo:/usr/local/cargo/registry `
        -e CARGO_TARGET_DIR=/target -w /src $image sh -c "cargo $cmd 2>&1" | Out-Host
    return $LASTEXITCODE
}

$failed = @()
if ($Fix) {
    if ((Cargo "fmt --all") -ne 0) { $failed += "fmt" }
} elseif (-not $SkipFmt) {
    if ((Cargo "fmt --all --check") -ne 0) { $failed += "fmt (run with -Fix)" }
}
if ((Cargo "clippy --workspace --all-targets -- -D warnings") -ne 0) { $failed += "clippy" }
if ((Cargo "test --workspace") -ne 0) { $failed += "test" }

if ($failed.Count -eq 0) { Write-Host "`nALL CHECKS PASSED" -ForegroundColor Green; exit 0 }
Write-Host "`nFAILED: $($failed -join ', ')" -ForegroundColor Red
exit 1
