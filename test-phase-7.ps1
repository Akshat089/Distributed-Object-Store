<#
End-to-end test for Phase 7: data integrity (Windows PowerShell 5.1+,
Docker Desktop running, cluster built from the current code).

  A  Corrupt one copy, then download: the download skips the bad copy,
     returns the correct bytes, and the bad copy is repaired.
  B  Corrupt one copy and DON'T read it: the worker's scrubber finds it
     and the controller repairs it.
  C  Delete a chunk file by hand: the scrubber reports it missing and it
     is restored.
  D  Corrupt BOTH copies of one chunk: the download fails rather than
     returning corrupted data.

Run from the repo root:
    powershell -ExecutionPolicy Bypass -File scripts\test-phase-7.ps1
Add -NoBuild to skip building the image.
#>
param(
    [string]$Controller = "http://localhost:9000",
    [string]$WorkDir = "$env:TEMP\dos-test-7",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$script:failures = 0

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "  [PASS] $msg" -ForegroundColor Green }
function Bad($msg)  { Write-Host "  [FAIL] $msg" -ForegroundColor Red; $script:failures++ }
function Check($cond, $msg) { if ($cond) { Ok $msg } else { Bad $msg } }

function Cluster { Invoke-RestMethod "$Controller/api/v1/cluster" }
function Meta($id) { Invoke-RestMethod "$Controller/api/v1/objects/$id/metadata" }

function WaitFor([scriptblock]$cond, [int]$seconds, [string]$what) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        try { if (& $cond) { return $true } } catch {}
        Start-Sleep -Milliseconds 1000
    }
    Write-Host "  timed out waiting for: $what" -ForegroundColor Yellow
    return $false
}

function New-RandomFile($path, [int]$size, [int]$seed) {
    $bytes = New-Object byte[] $size
    (New-Object System.Random $seed).NextBytes($bytes)
    [IO.File]::WriteAllBytes($path, $bytes)
}

function Upload($path) {
    $out = curl.exe -s -F "file=@$path" "$Controller/api/v1/objects"
    return $out | ConvertFrom-Json
}

# workerN is published on host port 808N.
function WorkerPort($w) { 8080 + [int]($w -replace 'worker', '') }

# HTTP status of reading one chunk straight from one worker
# (200 = healthy, 409 = corrupted, 404 = missing).
function ChunkStatus($w, $chunkId) {
    curl.exe -s -o NUL -w "%{http_code}" "http://localhost:$(WorkerPort $w)/internal/chunks/$chunkId"
}

function Chunk($objectId, $index) {
    (Meta $objectId).chunks | Where-Object index -eq $index
}

function Counters {
    $c = Cluster
    Write-Host "  integrity counters: detected=$($c.corrupt_copies_detected) repaired=$($c.corrupt_copies_repaired)"
    return $c
}

New-Item -ItemType Directory -Force $WorkDir | Out-Null

# ─────────────────────────────── Setup ────────────────────────────────
Step "Setup: start cluster and upload a 4-chunk object"
if (-not $NoBuild) { docker build -t distributed-object-store:latest . }
docker compose up -d | Out-Null
$up = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 4 } 90 "4 alive workers"
Check $up "controller sees 4 alive workers"
# Controller skips repair for 12 s after it starts; don't race that.
Start-Sleep -Seconds 13

$file = Join-Path $WorkDir "integrity.bin"
New-RandomFile $file (3 * 1MB + 4321) 7    # 4 chunks
$obj = Upload $file
$id = $obj.object_id
Check ($obj.chunk_count -eq 4) "object uploaded with 4 chunks (id $id)"
foreach ($c in $obj.chunks) {
    Write-Host ("    chunk {0}  -> {1}" -f $c.index, ($c.locations -join ", "))
}
$origHash = (Get-FileHash $file -Algorithm SHA256).Hash
$before = Counters

# ─────────────────────── A: corruption found by a download ────────────
Step "A: corrupt one copy of chunk 0, then download"
$c0 = Chunk $id 0
$w = $c0.locations[0]
docker exec $w sh -c "echo CORRUPTED >> /data/chunks/$($c0.chunk_id)"
Write-Host "  appended junk to $($c0.chunk_id) on $w"
Check ((ChunkStatus $w $c0.chunk_id) -eq "409") "$w now refuses the copy (409 corrupted)"

$dl = Join-Path $WorkDir "dl-a.bin"
$code = curl.exe -s -o $dl -w "%{http_code}" "$Controller/api/v1/objects/$id"
Check ($code -eq "200" -and (Get-FileHash $dl -Algorithm SHA256).Hash -eq $origHash) `
      "download still returns the exact original (bad copy skipped)"

$fixedA = WaitFor { (ChunkStatus $w $c0.chunk_id) -eq "200" } 15 "copy on $w repaired"
Check $fixedA "bad copy on $w was overwritten with a good one (200 again)"

# ─────────────────────── B: corruption found by the scrubber ──────────
Step "B: corrupt one copy of chunk 1 and never read it (scrubber)"
$c1 = Chunk $id 1
$w = $c1.locations[1]
# Flip bytes in the middle of the file: same size, different content.
docker exec $w sh -c "printf XXXXXXXX | dd of=/data/chunks/$($c1.chunk_id) bs=1 seek=5000 conv=notrunc 2>/dev/null"
Write-Host "  overwrote 8 bytes inside $($c1.chunk_id) on $w"
Check ((ChunkStatus $w $c1.chunk_id) -eq "409") "$w detects the change (409 corrupted)"
Write-Host "  waiting for the scrubber (runs every 10 s)..."
$fixedB = WaitFor { (ChunkStatus $w $c1.chunk_id) -eq "200" } 40 "scrubber repair on $w"
Check $fixedB "scrubber reported it and the controller repaired it, no download needed"

# ─────────────────────── C: a chunk file disappears ───────────────────
Step "C: delete chunk 2's file on one worker"
$c2 = Chunk $id 2
$w = $c2.locations[0]
docker exec $w rm "/data/chunks/$($c2.chunk_id)"
Write-Host "  deleted $($c2.chunk_id) on $w"
$fixedC = WaitFor { (ChunkStatus $w $c2.chunk_id) -eq "200" } 40 "missing copy restored on $w"
Check $fixedC "missing copy was restored on $w"

$dl = Join-Path $WorkDir "dl-c.bin"
curl.exe -s -o $dl "$Controller/api/v1/objects/$id"
Check ((Get-FileHash $dl -Algorithm SHA256).Hash -eq $origHash) "object still downloads intact after A, B, C"

$after = Counters
Check ($after.corrupt_copies_repaired - $before.corrupt_copies_repaired -ge 3) `
      "controller counted at least 3 repaired copies"

# ─────────────────────── D: no good copy left ─────────────────────────
Step "D: corrupt BOTH copies of chunk 3"
$c3 = Chunk $id 3
foreach ($w in $c3.locations) {
    docker exec $w sh -c "echo CORRUPTED >> /data/chunks/$($c3.chunk_id)"
    Write-Host "  corrupted $($c3.chunk_id) on $w"
}
$dl = Join-Path $WorkDir "dl-d.bin"
curl.exe -s -o $dl "$Controller/api/v1/objects/$id"
$curlExit = $LASTEXITCODE
$same = (Test-Path $dl) -and ((Get-FileHash $dl -Algorithm SHA256).Hash -eq $origHash)
Check ($curlExit -ne 0 -and -not $same) `
      "download is aborted (curl exit $curlExit) instead of returning corrupted data"

# Clean up the damaged object so later runs start from a healthy cluster.
$del = curl.exe -s -o NUL -w "%{http_code}" -X DELETE "$Controller/api/v1/objects/$id"
Check ($del -eq "204") "damaged object deleted"

# ─────────────────────────────── Result ───────────────────────────────
Write-Host ""
if ($script:failures -eq 0) {
    Write-Host "ALL CHECKS PASSED" -ForegroundColor Green
    exit 0
} else {
    Write-Host "$script:failures CHECK(S) FAILED" -ForegroundColor Red
    exit 1
}