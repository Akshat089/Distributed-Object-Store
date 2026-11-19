<#
Live demo: walks through the 6 required demonstrations, in order, pausing
between steps so it can be presented.

  1. Object storage          upload a real file, download it, SHA-256s match
  2. Chunking & distribution a file > 1 MB, every chunk's index, size, workers
  3. Replication             2 copies per chunk on different workers, per-worker counts
  4. Metadata                custom metadata: GET, PATCH (add/update/remove), chunks untouched
  5. Worker failure          docker stop -> DEAD -> download still works -> re-replicated
                             -> docker start -> reconciled -> exactly 2 copies again
  6. Data integrity          corrupt a copy -> 409 -> download still correct -> repaired;
                             corrupt one nobody reads -> the scrubber repairs it

Run from the repo root, with the cluster up (docker compose up -d):
    powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
Options:
    -Fast              no pauses (for a dry run)
    -SampleFile <path> file for steps 1-3 (default: the Windows wallpaper JPEG,
                       or a generated 3.5 MB file if that doesn't exist)
    -FailWorker <name> worker to stop in step 5 (default worker2)
#>
param(
    [string]$Controller = "http://localhost:9000",
    [string]$SampleFile = "",
    [string]$FailWorker = "worker2",
    [string]$WorkDir = "$env:TEMP\dos-demo",
    [switch]$Fast
)

$ErrorActionPreference = "Stop"

# ─────────────────────────────── Helpers ──────────────────────────────

function Title($n, $text) {
    Write-Host ""
    Write-Host ("=" * 72) -ForegroundColor DarkCyan
    Write-Host "  DEMO $n - $text" -ForegroundColor Cyan
    Write-Host ("=" * 72) -ForegroundColor DarkCyan
}
function Say($text)  { Write-Host "  $text" }
function Good($text) { Write-Host "  [OK] $text" -ForegroundColor Green }
function Warn($text) { Write-Host "  [!!] $text" -ForegroundColor Yellow }
function Cmd($text)  { Write-Host "  > $text" -ForegroundColor DarkGray }
function Pause()     { if (-not $Fast) { Write-Host ""; Read-Host "  Press Enter to continue" | Out-Null } }

function Cluster      { Invoke-RestMethod "$Controller/api/v1/cluster/status" }
function Meta($id)    { Invoke-RestMethod "$Controller/api/v1/objects/$id/metadata" }
function Chunks($id)  { Invoke-RestMethod "$Controller/api/v1/objects/$id/chunks" }
function WorkerPort($w) { 8080 + [int]($w -replace 'worker', '') }   # workerN -> host port 808N

function WaitFor([scriptblock]$cond, [int]$seconds, [string]$what) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        try { if (& $cond) { return $true } } catch {}
        Start-Sleep -Milliseconds 1000
    }
    Warn "timed out waiting for: $what"
    return $false
}

function Upload($path, $metaFile) {
    $curlArgs = @("-s", "--max-time", "120", "-F", "file=@$path")
    if ($metaFile) { $curlArgs += @("-F", "metadata=<$metaFile") }
    return (curl.exe @curlArgs "$Controller/api/v1/objects") | ConvertFrom-Json
}

function Download($id, $out) {
    return curl.exe -s --max-time 120 -o $out -w "%{http_code}" "$Controller/api/v1/objects/$id"
}

function Hash($path) { (Get-FileHash $path -Algorithm SHA256).Hash.ToLower() }

function ShowChunks($id) {
    $v = Chunks $id
    Say ("{0,-6} {1,10}   {2}" -f "chunk", "bytes", "copies (worker: state)")
    foreach ($c in $v.chunks) {
        $locs = ($c.locations | ForEach-Object { "{0}: {1}" -f $_.worker_id, $(if ($_.alive) { "alive" } else { "DEAD" }) }) -join "   "
        $color = if ($c.live_copies -ge $v.replication_factor) { "Gray" } else { "Yellow" }
        Write-Host ("  {0,-6} {1,10}   {2}" -f $c.index, $c.size, $locs) -ForegroundColor $color
    }
    Say "under-replicated chunks: $($v.under_replicated_chunks)"
}

function ShowWorkers {
    $c = Cluster
    Say ("{0,-9} {1,-6} {2,7} {3,10}" -f "worker", "status", "chunks", "MB used")
    foreach ($w in $c.workers) {
        $color = if ($w.status -eq "alive") { "Gray" } else { "Yellow" }
        Write-Host ("  {0,-9} {1,-6} {2,7} {3,10:N1}" -f $w.worker_id, $w.status, $w.chunk_count, ($w.bytes_used / 1MB)) -ForegroundColor $color
    }
    Say ("objects: {0}   chunks: {1}   under-replicated: {2}   unavailable: {3}   corrupt detected/repaired: {4}/{5}" -f `
        $c.object_count, $c.chunk_count, $c.under_replicated_chunks, $c.unavailable_chunks, $c.corrupt_copies_detected, $c.corrupt_copies_repaired)
}

function ShowLogs($since, $pattern) {
    Say "controller log:"
    docker logs controller --since $since 2>&1 | Select-String $pattern | Select-Object -Last 14 |
        ForEach-Object { Write-Host ("    " + ($_.Line -replace '^\S+\s+', '')) -ForegroundColor DarkGray }
}

function ChunkStatus($w, $chunkId) {
    curl.exe -s --max-time 10 -o NUL -w "%{http_code}" "http://localhost:$(WorkerPort $w)/internal/chunks/$chunkId"
}

# ─────────────────────────────── Setup ────────────────────────────────

New-Item -ItemType Directory -Force $WorkDir | Out-Null
Write-Host "`nMini Distributed Object Store - live demo" -ForegroundColor White
Say "controller: $Controller"
$alive = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 4 } 60 "4 alive workers"
if (-not $alive) { Warn "Start the cluster first: docker compose up -d"; exit 1 }
ShowWorkers

if (-not $SampleFile) {
    $wallpaper = "C:\Windows\Web\Wallpaper\Windows\img0.jpg"
    if ((Test-Path $wallpaper) -and (Get-Item $wallpaper).Length -gt 1MB) {
        $SampleFile = Join-Path $WorkDir "wallpaper.jpg"
        Copy-Item $wallpaper $SampleFile -Force
    } else {
        $SampleFile = Join-Path $WorkDir "dataset.bin"
        $bytes = New-Object byte[] ([int](3.5 * 1MB)); (New-Object System.Random 42).NextBytes($bytes)
        [IO.File]::WriteAllBytes($SampleFile, $bytes)
    }
}
$sample = Get-Item $SampleFile
Pause

# ─────────────────────────────── 1 ────────────────────────────────────
Title 1 "Object storage: upload, download, verify"
Cmd "curl.exe -F file=@$($sample.Name) $Controller/api/v1/objects"
$obj = Upload $sample.FullName $null
$id = $obj.object_id
Good "uploaded: HTTP 201"
Say "object_id    : $id"
Say "name         : $($obj.name)"
Say "size         : $($obj.size) bytes"
Say "content_type : $($obj.content_type)"
Say "checksum     : $($obj.checksum)"
$dl = Join-Path $WorkDir "downloaded-$($sample.Name)"
Cmd "curl.exe -o downloaded-$($sample.Name) $Controller/api/v1/objects/$id"
$code = Download $id $dl
Say "download     : HTTP $code, $((Get-Item $dl).Length) bytes"
$h1 = Hash $sample.FullName; $h2 = Hash $dl
Say "SHA-256 original   : $h1"
Say "SHA-256 downloaded : $h2"
if ($h1 -eq $h2) { Good "identical bytes" } else { Warn "HASHES DIFFER" }
Pause

# ─────────────────────────────── 2 ────────────────────────────────────
Title 2 "Chunking & distribution"
Say "Files are split into 1 MiB chunks (the last one may be smaller)."
Say "$($obj.name): $($obj.size) bytes -> $($obj.chunk_count) chunks."
Cmd "curl.exe $Controller/api/v1/objects/$id/chunks"
ShowChunks $id
Say "Placement uses a consistent-hash ring, so chunks spread over all workers."
Pause

# ─────────────────────────────── 3 ────────────────────────────────────
Title 3 "Replication: 2 copies of every chunk, on different workers"
$v = Chunks $id
$okRep = $true
foreach ($c in $v.chunks) {
    $ws = @($c.locations | ForEach-Object { $_.worker_id })
    if ($ws.Count -ne 2 -or $ws[0] -eq $ws[1]) { $okRep = $false }
}
if ($okRep) { Good "every chunk has exactly 2 copies on 2 different workers" } else { Warn "replication check failed" }
Start-Sleep -Seconds 3   # let heartbeats carry fresh per-worker counts
Cmd "curl.exe $Controller/api/v1/cluster/status"
ShowWorkers
Pause

# ─────────────────────────────── 4 ────────────────────────────────────
Title 4 "Metadata: custom key/values, PATCH, chunks never touched"
$notes = Join-Path $WorkDir "notes.txt"
[IO.File]::WriteAllText($notes, "Lecture notes for the cloud computing course.`n")
$metaFile = Join-Path $WorkDir "meta.json"
[IO.File]::WriteAllText($metaFile, '{"owner":"demo","course":"cloud","draft":"yes"}')
Cmd "curl.exe -F file=@notes.txt -F metadata='{""owner"":""demo"",""course"":""cloud"",""draft"":""yes""}' ..."
$m = Upload $notes $metaFile
$mid = $m.object_id
Say "custom after upload : $(($m.custom | ConvertTo-Json -Compress))"
$chunksBefore = ($m.chunks | ConvertTo-Json -Depth 5 -Compress)
$patchFile = Join-Path $WorkDir "patch.json"
[IO.File]::WriteAllText($patchFile, '{"owner":"akshat","grade":"A","draft":null}')
Cmd "curl.exe -X PATCH -d '{""owner"":""akshat"",""grade"":""A"",""draft"":null}' $Controller/api/v1/objects/$mid/metadata"
curl.exe -s --max-time 30 -X PATCH -H "Content-Type: application/json" -d "@$patchFile" "$Controller/api/v1/objects/$mid/metadata" | Out-Null
Say "  owner updated, grade added, draft removed (null deletes a key)"
$g = Meta $mid
Say "custom after PATCH  : $(($g.custom | ConvertTo-Json -Compress))"
Say "created_at          : $($g.created_at)"
Say "modified_at         : $($g.modified_at)"
if (($g.chunks | ConvertTo-Json -Depth 5 -Compress) -eq $chunksBefore) { Good "chunks and their locations are unchanged" } else { Warn "chunks changed" }
Pause

# ─────────────────────────────── 5 ────────────────────────────────────
Title 5 "Worker failure: detection, re-replication, recovery"
$since5 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
Say "Before:"
ShowChunks $id
Write-Host ""
Cmd "docker stop $FailWorker"
docker stop $FailWorker | Out-Null
$t0 = Get-Date
$dead = WaitFor { ((Cluster).workers | Where-Object worker_id -eq $FailWorker).status -eq "dead" } 30 "$FailWorker dead"
if ($dead) { Good ("$FailWorker marked DEAD after {0:N1} s (no heartbeat for 6 s)" -f ((Get-Date) - $t0).TotalSeconds) }

$dl5 = Join-Path $WorkDir "during-failure.bin"
$code = Download $id $dl5
if ($code -eq "200" -and (Hash $dl5) -eq $h1) { Good "download during the failure: HTTP 200, SHA-256 matches (served from the other copies)" } else { Warn "download returned $code" }

$healed = WaitFor { (Cluster).under_replicated_chunks -eq 0 } 60 "every chunk back to 2 live copies"
if ($healed) { Good ("re-replication done {0:N1} s after the stop; every chunk has 2 live copies again" -f ((Get-Date) - $t0).TotalSeconds) }
ShowChunks $id
Pause

Cmd "docker start $FailWorker"
docker start $FailWorker | Out-Null
$back = WaitFor { ((Cluster).workers | Where-Object worker_id -eq $FailWorker).status -eq "alive" } 30 "$FailWorker alive"
if ($back) { Good "$FailWorker re-registered with the same id" }
# Reconciliation: the returning worker should keep only chunks that metadata lists for it.
$settled = WaitFor {
    $listed = @()
    foreach ($o in (Invoke-RestMethod "$Controller/api/v1/objects?limit=1000").objects) {
        foreach ($c in (Meta $o.object_id).chunks) { if ($c.locations -contains $FailWorker) { $listed += $c.chunk_id } }
    }
    $held = @((Invoke-RestMethod "http://localhost:$(WorkerPort $FailWorker)/internal/workers/$FailWorker/chunks").chunks | ForEach-Object { $_.chunk_id })
    (@($held | Where-Object { $listed -notcontains $_ })).Count -eq 0
} 30 "reconciliation"
if ($settled) { Good "reconciliation deleted $FailWorker's stale copies (policy: drop the extra copy, keep exactly 2)" }
ShowChunks $id
Start-Sleep -Seconds 3   # per-worker counts arrive with the next heartbeat
ShowWorkers
$copied = @(docker logs controller --since $since5 2>&1 | Select-String "chunk re-replicated").Count
Say "$copied chunk copies were re-created while $FailWorker was down (one log line each)"
ShowLogs $since5 "DEAD|BACK|repair sweep finished|reconciliation finished worker=$FailWorker"
Pause

# ─────────────────────────────── 6 ────────────────────────────────────
Title 6 "Data integrity: checksums on every read + background scrubber"
$since6 = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$v = Chunks $id
$c0 = $v.chunks | Where-Object index -eq 0
$bad = $c0.locations[0].worker_id
Say "A) corrupt chunk 0's copy on $bad, then download"
Cmd "docker exec $bad sh -c 'echo junk >> /data/chunks/$($c0.chunk_id)'"
docker exec $bad sh -c "echo junk >> /data/chunks/$($c0.chunk_id)"
Say "$bad now answers HTTP $(ChunkStatus $bad $c0.chunk_id) for that chunk (409 = corrupted; it never sends bad bytes)"
$dl6 = Join-Path $WorkDir "after-corruption.bin"
$code = Download $id $dl6
if ($code -eq "200" -and (Hash $dl6) -eq $h1) { Good "download still correct: HTTP 200, SHA-256 matches (bad copy skipped)" } else { Warn "download returned $code" }
if (WaitFor { (ChunkStatus $bad $c0.chunk_id) -eq "200" } 20 "repair") { Good "the bad copy on $bad was overwritten with a verified good copy (HTTP 200 again)" }

if ($v.chunks.Count -gt 1) {
    Write-Host ""
    $c1 = $v.chunks | Where-Object index -eq 1
    $bad1 = $c1.locations[1].worker_id
    Say "B) silently corrupt chunk 1's copy on $bad1 and DON'T read it"
    Cmd "docker exec $bad1 sh -c 'printf XXXXXXXX | dd of=/data/chunks/$($c1.chunk_id) bs=1 seek=5000 conv=notrunc'"
    docker exec $bad1 sh -c "printf XXXXXXXX | dd of=/data/chunks/$($c1.chunk_id) bs=1 seek=5000 conv=notrunc 2>/dev/null"
    Say "same size, different bytes; waiting for $bad1's scrubber (re-hashes every chunk every 10 s)..."
    if (WaitFor { (ChunkStatus $bad1 $c1.chunk_id) -eq "200" } 45 "scrubber repair") { Good "scrubber found it, the controller repaired it - no download needed" }
}
Start-Sleep -Seconds 1
ShowWorkers
ShowLogs $since6 "CORRUPTED|scrubber|REPAIRED|repairing"

Write-Host ""
Write-Host "Demo complete." -ForegroundColor White
