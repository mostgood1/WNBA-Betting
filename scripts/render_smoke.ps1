param(
  [string]$BaseUrl = "http://localhost:5051",
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [int]$TimeoutSec = 180,
  [switch]$RunDailyUpdate
)

$ErrorActionPreference = 'Stop'

function Write-Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }

# Local-only: no auth headers
$headers = @{}

Write-Info "BaseUrl = $BaseUrl  Date = $Date"

# Optional: run local daily update before hitting endpoints
if ($RunDailyUpdate) {
  try {
    $root = Split-Path -Parent $MyInvocation.MyCommand.Path
    $repoRoot = (Resolve-Path (Join-Path $root '..')).Path
    $scriptPath = Join-Path $root 'daily_update.ps1'
    if (Test-Path $scriptPath) {
      Write-Info "Running local daily_update.ps1 before checks..."
      powershell -NoProfile -ExecutionPolicy Bypass -File $scriptPath -Date $Date -GitPush
      Write-Info "daily_update completed"
    } else {
      Write-Warn "daily_update.ps1 not found; skipping pre-flight"
    }
  } catch {
    Write-Warn ("daily_update failed: {0}" -f $_.Exception.Message)
  }
}

# Version check
try {
  $v = Invoke-WebRequest -UseBasicParsing -Uri ("$BaseUrl/api/version") -TimeoutSec 20
  Write-Info ("Version: {0}" -f $v.Content)
} catch {
  Write-Warn ("/api/version failed: {0}" -f $_.Exception.Message)
}

# Trigger run-all
try {
  Write-Info "Triggering /api/cron/run-all locally ..."
  $resp = Invoke-WebRequest -UseBasicParsing -Headers $headers -Method Post -Uri ("$BaseUrl/api/cron/run-all?date=$Date&push=1") -TimeoutSec 180
  Write-Info ("run-all status {0}" -f $resp.StatusCode)
} catch {
  Write-Warn ("run-all failed: {0}" -f $_.Exception.Message)
}

# Poll data-status
$deadline = (Get-Date).AddSeconds($TimeoutSec)
$last = $null
while ((Get-Date) -lt $deadline) {
  try {
    $ds = Invoke-WebRequest -UseBasicParsing -Uri ("$BaseUrl/api/data-status?date=$Date") -TimeoutSec 20
    $last = $ds.Content
    $json = $ds.Content | ConvertFrom-Json
    $pred = [int]$json.predictions_rows
    $odds = [int]$json.game_odds_rows
    $props = [int]$json.props_edges_rows
    Write-Info ("Status: preds={0} odds={1} props={2}" -f $pred, $odds, $props)
    if ($pred -gt 0 -and $props -gt 0) { break }
  } catch {
    Write-Warn ("data-status failed: {0}" -f $_.Exception.Message)
  }
  Start-Sleep -Seconds 5
}

if ($last) {
  Write-Host $last
}

# Page checks (best effort)
foreach ($path in @('/', '/props', '/reconciliation')) {
  try {
    $r = Invoke-WebRequest -UseBasicParsing -Uri ("$BaseUrl$path") -TimeoutSec 20
    Write-Info ("GET {0} -> {1}" -f $path, $r.StatusCode)
  } catch {
    Write-Warn ("GET {0} failed: {1}" -f $path, $_.Exception.Message)
  }
}

# Verify recommendations API returns items
try {
  $u = "$BaseUrl/api/recommendations/all?date=$Date&compact=1&regular_only=1"
  $content = Invoke-WebRequest -UseBasicParsing -Uri $u -TimeoutSec 60 | Select-Object -ExpandProperty Content
  $json = $content | ConvertFrom-Json
  $games = [int]$json.counts.games
  $props = [int]$json.counts.props
  Write-Info ("Recommendations: games={0} props={1}" -f $games, $props)
} catch {
  Write-Warn ("Recommendations check failed: {0}" -f $_.Exception.Message)
}
