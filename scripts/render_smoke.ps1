param(
  [string]$BaseUrl = "https://nba-betting-5qgf.onrender.com",
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [int]$TimeoutSec = 180,
  [switch]$UseAdmin
)

$ErrorActionPreference = 'Stop'

function Write-Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }

# Build headers
$headers = @{}
if ($UseAdmin -and $env:ADMIN_KEY) {
  $headers['X-Admin-Key'] = $env:ADMIN_KEY
} elseif ($env:CRON_TOKEN) {
  $headers['Authorization'] = "Bearer $env:CRON_TOKEN"
}

Write-Info "BaseUrl = $BaseUrl  Date = $Date"

# Version check
try {
  $v = Invoke-WebRequest -UseBasicParsing -Uri ("$BaseUrl/api/version") -TimeoutSec 20
  Write-Info ("Version: {0}" -f $v.Content)
} catch {
  Write-Warn ("/api/version failed: {0}" -f $_.Exception.Message)
}

# Trigger run-all
try {
  Write-Info "Triggering /api/cron/run-all ..."
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
