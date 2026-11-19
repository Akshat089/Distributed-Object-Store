<#
End-to-end test for Phases 4-6 (Windows PowerShell 5.1+, Docker Desktop running).

  Phase 4  Docker: 1 controller + 4 workers; upload a multi-MB file,
           download it, compare SHA-256.
  Phase 5  Failure: `docker stop worker2`; worker2 is marked dead,
           downloads still work, every chunk gets back to 2 live copies.
  Phase 6  Recovery: an object is deleted while worker2 is down, then
           `docker start worker2`; reconciliation deletes worker2's orphan
           and extra copies so every chunk has exactly 2 copies again.

Run from the repo root:
    powershell -ExecutionPolicy Bypass -File scripts\test-phases-4-6.ps1
Add -NoBuild to skip `docker build` (use it when the image is already current).
#>
param(
    [string]$Controller = "http://localhost:9000",
    [string]$WorkDir = "$env:TEMP\dos-test",
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
    # JSON goes through a file: PowerShell 5.1 mangles quotes in native args.
    $metaFile = Join-Path $WorkDir "meta.json"
    [IO.File]::WriteAllText($metaFile, '{"test":"phases-4-6"}')
    $out = curl.exe -s -F "file=@$path" -F "metadata=<$metaFile" "$Controller/api/v1/objects"
    return $out | ConvertFrom-Json
}

function DownloadAndCompare($id, $original, $label) {
    $dl = Join-Path $WorkDir "download-$id.bin"
    $code = curl.exe -s -o $dl -w "%{http_code}" "$Controller/api/v1/objects/$id"
    if ($code -ne "200") { Bad "$label download returned HTTP $code"; return }
    $a = (Get-FileHash $original -Algorithm SHA256).Hash
    $b = (Get-FileHash $dl -Algorithm SHA256).Hash
    Check ($a -eq $b) "$label download SHA-256 matches original ($($a.Substring(0,16))...)"
}

function Show-Chunks($id) {
    foreach ($c in (Meta $id).chunks) {
        Write-Host ("    chunk {0,2}  {1,8} B  -> {2}" -f $c.index, $c.size, ($c.locations -join ", "))
    }
}

function WorkerChunks($port, $workerId) {
    (Invoke-RestMethod "http://localhost:$port/internal/workers/$workerId/chunks").chunks
}

New-Item -ItemType Directory -Force $WorkDir | Out-Null

# ───────────────────────────── Phase 4 ─────────────────────────────
Step "Phase 4: start cluster"
# All 5 services share one image; build it once, explicitly.
if (-not $NoBuild) { docker build -t distributed-object-store:latest . }
docker compose up -d | Out-Null
$up = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 4 } 90 "4 alive workers"
Check $up "controller sees 4 alive workers"
(Cluster).workers | Format-Table worker_id, status, chunk_count, address -AutoSize | Out-Host

Step "Phase 4: upload, download, compare"
$fileA = Join-Path $WorkDir "objA.bin"
$fileB = Join-Path $WorkDir "objB.bin"
New-RandomFile $fileA (5 * 1MB + 12345) 1   # 6 chunks, last one partial
New-RandomFile $fileB (3 * 1MB) 2           # 3 chunks
$a = Upload $fileA
$b = Upload $fileB
Check ($a.chunk_count -eq 6) "object A uploaded with 6 chunks (id $($a.object_id))"
Check ($b.chunk_count -eq 3) "object B uploaded with 3 chunks (id $($b.object_id))"
Write-Host "  Object A chunk placement:"; Show-Chunks $a.object_id
DownloadAndCompare $a.object_id $fileA "object A"

# ───────────────────────────── Phase 5 ─────────────────────────────
Step "Phase 5: docker stop worker2"
docker stop worker2 | Out-Null
$dead = WaitFor { ((Cluster).workers | Where-Object worker_id -eq "worker2").status -eq "dead" } 20 "worker2 dead"
Check $dead "worker2 marked dead by heartbeat timeout"

DownloadAndCompare $a.object_id $fileA "object A (worker2 down, fallback copies)"

$healed = WaitFor { (Cluster).under_replicated_chunks -eq 0 } 60 "under_replicated_chunks = 0"
Check $healed "repair_under_replicated brought every chunk back to 2 live copies"
$c = Cluster
Write-Host "  cluster: chunks=$($c.chunk_count) under_replicated=$($c.under_replicated_chunks) unavailable=$($c.unavailable_chunks)"
Write-Host "  Object A chunk placement after repair:"; Show-Chunks $a.object_id

$allGood = $true
foreach ($id in @($a.object_id, $b.object_id)) {
    foreach ($ch in (Meta $id).chunks) {
        if ($ch.locations.Count -ne 2 -or $ch.locations -contains "worker2") { $allGood = $false }
    }
}
Check $allGood "every chunk lists exactly 2 locations, none on worker2"
DownloadAndCompare $a.object_id $fileA "object A (after repair)"

# ───────────────────────────── Phase 6 ─────────────────────────────
Step "Phase 6: delete object B while worker2 is down, then docker start worker2"
$code = curl.exe -s -o NUL -w "%{http_code}" -X DELETE "$Controller/api/v1/objects/$($b.object_id)"
Check ($code -eq "204") "object B deleted (worker2 still holds stale copies of it)"

docker start worker2 | Out-Null
$back = WaitFor { ((Cluster).workers | Where-Object worker_id -eq "worker2").status -eq "alive" } 30 "worker2 alive"
Check $back "worker2 re-registered and is alive again"

# Reconciliation runs right after registration; give it a moment.
# Compare against EVERY object (earlier runs may have left objects behind).
function ListedFor($worker) {
    $ids = @()
    foreach ($o in (Invoke-RestMethod "$Controller/api/v1/objects?limit=1000").objects) {
        foreach ($ch in (Meta $o.object_id).chunks) {
            if ($ch.locations -contains $worker) { $ids += $ch.chunk_id }
        }
    }
    return $ids
}
$settled = WaitFor {
    $expected = ListedFor "worker2"
    $held = @(WorkerChunks 8082 "worker2" | ForEach-Object { $_.chunk_id })
    (@($held | Where-Object { $expected -notcontains $_ })).Count -eq 0
} 30 "worker2 holds only chunks listed in metadata"
Check $settled "reconciliation removed worker2's orphan (object B) and extra (object A) copies"

# Totals: every chunk of every object exists exactly twice across workers.
$totalChunks = (Cluster).chunk_count
$held = 0
foreach ($p in @(@{p=8081;w="worker1"}, @{p=8082;w="worker2"}, @{p=8083;w="worker3"}, @{p=8084;w="worker4"})) {
    $n = @(WorkerChunks $p.p $p.w).Count
    Write-Host "  $($p.w) holds $n chunk files"
    $held += $n
}
# Other objects may exist from earlier runs, so compare against all objects.
Check ($held -eq 2 * $totalChunks) "workers hold exactly 2 x $totalChunks = $(2*$totalChunks) chunk copies (found $held)"
Check ((Cluster).under_replicated_chunks -eq 0) "no under-replicated chunks"
DownloadAndCompare $a.object_id $fileA "object A (after recovery)"

Write-Host "`nRecent controller log lines about repair/reconciliation:" -ForegroundColor Cyan
docker logs controller 2>&1 | Select-String "DEAD|BACK|re-replicated|repair sweep|reconciliation finished|extra copy" | Select-Object -Last 15 | ForEach-Object { "  $_" }

Step "Result"
if ($script:failures -eq 0) { Write-Host "ALL CHECKS PASSED" -ForegroundColor Green }
else { Write-Host "$($script:failures) CHECK(S) FAILED" -ForegroundColor Red; exit 1 }
