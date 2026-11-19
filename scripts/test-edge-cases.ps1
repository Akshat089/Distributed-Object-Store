<#
End-to-end test for Phase 8 (demo endpoints) and Phase 9 (edge cases and
consistent JSON errors). Windows PowerShell 5.1+, Docker Desktop running.

  - empty file -> 201 with 0 chunks, downloads as 0 bytes
  - missing `file` field, bad `metadata` JSON, bad PATCH body, bad query -> 400
  - unknown id -> 404, malformed id -> 400, unknown path -> 404, wrong method -> 405
  - every error body is JSON: {"error": "...", "message": "..."}
  - PATCH adds/updates/removes keys and never touches chunks
  - GET /api/v1/objects/{id}/chunks and /api/v1/cluster/status
  - upload larger than MAX_UPLOAD_MB -> 413 (on a throwaway controller)
  - upload with only 1 worker alive -> 503 (stops 3 workers, then restarts them)

Run from the repo root:
    powershell -ExecutionPolicy Bypass -File scripts\test-edge-cases.ps1
Add -NoBuild to skip building the image, -SkipSlow to skip the 413/503 checks.
#>
param(
    [string]$Controller = "http://localhost:9000",
    [string]$WorkDir = "$env:TEMP\dos-test-edge",
    [switch]$NoBuild,
    [switch]$SkipSlow
)

$ErrorActionPreference = "Stop"
$script:failures = 0

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok($msg)   { Write-Host "  [PASS] $msg" -ForegroundColor Green }
function Bad($msg)  { Write-Host "  [FAIL] $msg" -ForegroundColor Red; $script:failures++ }
function Check($cond, $msg) { if ($cond) { Ok $msg } else { Bad $msg } }

function Cluster { Invoke-RestMethod "$Controller/api/v1/cluster" }

function WaitFor([scriptblock]$cond, [int]$seconds, [string]$what) {
    $deadline = (Get-Date).AddSeconds($seconds)
    while ((Get-Date) -lt $deadline) {
        try { if (& $cond) { return $true } } catch {}
        Start-Sleep -Milliseconds 1000
    }
    Write-Host "  timed out waiting for: $what" -ForegroundColor Yellow
    return $false
}

# Runs curl.exe; returns @{ code = "404"; body = "..."; json = <object or $null> }.
function Call([string[]]$curlArgs) {
    $out = Join-Path $WorkDir "resp.bin"
    if (Test-Path $out) { Remove-Item $out }
    # --max-time: a request that never gets an answer fails the check
    # instead of hanging the script.
    $code = curl.exe -s --max-time 60 -o $out -w "%{http_code}" @curlArgs
    $body = if (Test-Path $out) { [IO.File]::ReadAllText($out) } else { "" }
    $json = $null
    try { $json = $body | ConvertFrom-Json } catch {}
    return @{ code = $code; body = $body; json = $json; file = $out }
}

# Asserts an HTTP status AND the JSON error format with the expected kind.
function CheckError($r, [string]$code, [string]$kind, [string]$what) {
    $isJson = $r.json -and $r.json.error -eq $kind -and $r.json.message
    Check ($r.code -eq $code -and $isJson) "$what -> $code {`"error`":`"$kind`"} (got $($r.code): $($r.body))"
}

function WriteFile($name, $text) {
    $p = Join-Path $WorkDir $name
    [IO.File]::WriteAllText($p, $text)
    return $p
}

New-Item -ItemType Directory -Force $WorkDir | Out-Null

Step "Setup"
if (-not $NoBuild) { docker build -t distributed-object-store:latest . }
docker compose up -d | Out-Null
$up = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 4 } 90 "4 alive workers"
Check $up "controller sees 4 alive workers"

# ─────────────────────────────── Empty file ───────────────────────────
Step "Empty file"
$empty = WriteFile "empty.txt" ""
$r = Call @("-F", "file=@$empty", "$Controller/api/v1/objects")
Check ($r.code -eq "201" -and $r.json.chunk_count -eq 0 -and $r.json.size -eq 0) "upload empty file -> 201, size 0, chunk_count 0"
$emptyId = $r.json.object_id
$r = Call @("$Controller/api/v1/objects/$emptyId")
Check ($r.code -eq "200" -and (Get-Item $r.file).Length -eq 0) "download empty object -> 200, 0 bytes"
$r = Call @("-X", "DELETE", "$Controller/api/v1/objects/$emptyId")
Check ($r.code -eq "204") "delete empty object -> 204"

# ─────────────────────────────── Bad uploads ──────────────────────────
Step "Bad uploads"
$meta = WriteFile "meta.json" '{"owner":"demo"}'
$r = Call @("-F", "metadata=<$meta", "$Controller/api/v1/objects")
CheckError $r "400" "bad_request" "upload without a file field"

$small = WriteFile "small.txt" "hello object store"
$badMeta = WriteFile "badmeta.json" '{"owner": '
$r = Call @("-F", "file=@$small", "-F", "metadata=<$badMeta", "$Controller/api/v1/objects")
CheckError $r "400" "bad_request" "upload with invalid metadata JSON"

$notStrings = WriteFile "nested.json" '{"owner": {"nested": true}}'
$r = Call @("-F", "file=@$small", "-F", "metadata=<$notStrings", "$Controller/api/v1/objects")
CheckError $r "400" "bad_request" "upload with non-string metadata values"

$r = Call @("-X", "POST", "-H", "Content-Type: application/json", "-d", "@$meta", "$Controller/api/v1/objects")
CheckError $r "400" "bad_request" "upload that isn't multipart"

# ─────────────────────────────── Metadata PATCH ───────────────────────
Step "Metadata PATCH (and chunks never change)"
$two = Join-Path $WorkDir "two-chunks.bin"
$bytes = New-Object byte[] (1MB + 100); (New-Object System.Random 3).NextBytes($bytes); [IO.File]::WriteAllBytes($two, $bytes)
$r = Call @("-F", "file=@$two", "-F", "metadata=<$meta", "$Controller/api/v1/objects")
Check ($r.code -eq "201" -and $r.json.custom.owner -eq "demo") "upload with metadata -> 201, custom.owner = demo"
$id = $r.json.object_id
$chunksBefore = ($r.json.chunks | ConvertTo-Json -Depth 5 -Compress)

$patch = WriteFile "patch.json" '{"owner":"akshat","course":"cloud","missing":null}'
$r = Call @("-X", "PATCH", "-H", "Content-Type: application/json", "-d", "@$patch", "$Controller/api/v1/objects/$id/metadata")
Check ($r.code -eq "200" -and $r.json.custom.owner -eq "akshat" -and $r.json.custom.course -eq "cloud") "PATCH updates owner and adds course"
$remove = WriteFile "remove.json" '{"course":null}'
$r = Call @("-X", "PATCH", "-H", "Content-Type: application/json", "-d", "@$remove", "$Controller/api/v1/objects/$id/metadata")
Check ($r.code -eq "200" -and -not ($r.json.custom.PSObject.Properties.Name -contains "course")) "PATCH with null removes course"
Check (($r.json.chunks | ConvertTo-Json -Depth 5 -Compress) -eq $chunksBefore) "chunks are unchanged by PATCH"
Check ($r.json.modified_at -ne $r.json.created_at) "modified_at was bumped"

$badPatch = WriteFile "badpatch.txt" "this is not json"
$r = Call @("-X", "PATCH", "-H", "Content-Type: application/json", "-d", "@$badPatch", "$Controller/api/v1/objects/$id/metadata")
CheckError $r "400" "bad_request" "PATCH with a non-JSON body"

# ─────────────────────────────── Not found / bad ids ──────────────────
Step "Unknown and malformed ids, unknown paths, wrong methods"
$ghost = "00000000-0000-4000-8000-000000000000"
CheckError (Call @("$Controller/api/v1/objects/$ghost")) "404" "not_found" "download unknown id"
CheckError (Call @("$Controller/api/v1/objects/$ghost/metadata")) "404" "not_found" "metadata of unknown id"
CheckError (Call @("$Controller/api/v1/objects/$ghost/chunks")) "404" "not_found" "chunks of unknown id"
CheckError (Call @("-X", "DELETE", "$Controller/api/v1/objects/$ghost")) "404" "not_found" "delete unknown id"
CheckError (Call @("$Controller/api/v1/objects/not-a-uuid")) "400" "bad_request" "malformed id"
CheckError (Call @("$Controller/api/v1/nope")) "404" "not_found" "unknown endpoint"
CheckError (Call @("-X", "PUT", "$Controller/api/v1/objects")) "405" "method_not_allowed" "PUT on /objects"
CheckError (Call @("$Controller/api/v1/objects?limit=abc")) "400" "bad_request" "list with a bad query string"

# ─────────────────────────────── Demo endpoints ───────────────────────
Step "Demo endpoints (Phase 8)"
$r = Call @("$Controller/api/v1/objects/$id/chunks")
$okChunks = $r.code -eq "200" -and $r.json.chunk_count -eq 2 -and $r.json.under_replicated_chunks -eq 0
foreach ($c in $r.json.chunks) {
    $workers = @($c.locations | ForEach-Object { $_.worker_id })
    if ($c.live_copies -ne 2 -or ($workers | Select-Object -Unique).Count -ne 2) { $okChunks = $false }
    Write-Host ("    chunk {0}  {1,8} B  -> {2}" -f $c.index, $c.size, (($c.locations | ForEach-Object { "$($_.worker_id)($(if ($_.alive) {'alive'} else {'DEAD'}))" }) -join ", "))
}
Check $okChunks "GET /objects/{id}/chunks shows 2 live copies on 2 different workers per chunk"
$r = Call @("$Controller/api/v1/cluster/status")
Check ($r.code -eq "200" -and @($r.json.workers).Count -eq 4) "GET /cluster/status lists 4 workers"
$r = Call @("$Controller/api/v1/objects?offset=0&limit=1")
Check ($r.code -eq "200" -and @($r.json.objects).Count -eq 1 -and $r.json.limit -eq 1) "list is paginated (limit=1 returns 1 object)"

Call @("-X", "DELETE", "$Controller/api/v1/objects/$id") | Out-Null

if (-not $SkipSlow) {
    # ─────────────────────────── 413: upload too large ────────────────
    Step "Upload larger than MAX_UPLOAD_MB (throwaway controller with a 1 MB limit)"
    docker rm -f dos-limit-test *> $null
    docker run -d --name dos-limit-test -p 9100:9100 -e PORT=9100 -e MAX_UPLOAD_MB=1 `
        -e DATA_DIR=/tmp/meta distributed-object-store:latest controller | Out-Null
    $ready = WaitFor { (Invoke-RestMethod "http://localhost:9100/health").status -eq "ok" } 20 "limit-test controller"
    $big = Join-Path $WorkDir "big.bin"
    $bytes = New-Object byte[] (3MB); (New-Object System.Random 9).NextBytes($bytes); [IO.File]::WriteAllBytes($big, $bytes)
    $r = Call @("-F", "file=@$big", "http://localhost:9100/api/v1/objects")
    CheckError $r "413" "payload_too_large" "3 MB upload with MAX_UPLOAD_MB=1 (rejected from Content-Length)"
    # Chunked transfer has no Content-Length: the limit is enforced while streaming.
    $r = Call @("-H", "Transfer-Encoding: chunked", "-F", "file=@$big", "http://localhost:9100/api/v1/objects")
    CheckError $r "413" "payload_too_large" "3 MB chunked upload with MAX_UPLOAD_MB=1 (rejected while streaming)"
    docker rm -f dos-limit-test | Out-Null

    # ─────────────────────────── 503: not enough workers ──────────────
    Step "Upload with only 1 worker alive"
    docker stop worker2 worker3 worker4 | Out-Null
    $down = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 1 } 20 "only worker1 alive"
    Check $down "worker2-4 marked dead"
    $r = Call @("-F", "file=@$small", "$Controller/api/v1/objects")
    CheckError $r "503" "unavailable" "upload needing 2 copies with 1 alive worker"
    docker start worker2 worker3 worker4 | Out-Null
    $back = WaitFor { (@((Cluster).workers | Where-Object status -eq "alive")).Count -eq 4 } 40 "4 alive workers"
    $healed = WaitFor { (Cluster).under_replicated_chunks -eq 0 } 60 "under_replicated_chunks = 0"
    Check ($back -and $healed) "workers back and every chunk has 2 live copies again"
}

Step "Result"
if ($script:failures -eq 0) { Write-Host "ALL CHECKS PASSED" -ForegroundColor Green; exit 0 }
Write-Host "$($script:failures) CHECK(S) FAILED" -ForegroundColor Red
exit 1
