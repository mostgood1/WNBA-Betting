Param(
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$Quiet,
  [string]$LogDir = "logs",
  # If set, stage/commit/pull --rebase/push repo changes (data/processed etc.)
  [switch]$GitPush,
  # If set, do a 'git pull --rebase' before running to reduce conflicts
  [switch]$GitSyncFirst,
  # Optional: Remote server base URL (updated to the correct Render site)
  [string]$RemoteBaseUrl = "https://nba-betting-5qgf.onrender.com"
)

$ErrorActionPreference = 'Stop'

# Resolve paths
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# Repo root is the parent of the scripts folder
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# Python resolution (prefer venv at repo root)
$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Python = if (Test-Path $VenvPy) { $VenvPy } else { 'python' }

# Logs (under repo root)
$LogPath = Join-Path $RepoRoot $LogDir
if (-not (Test-Path $LogPath)) { New-Item -ItemType Directory -Path $LogPath | Out-Null }
$Stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
$LogFile = Join-Path $LogPath ("local_daily_update_{0}.log" -f $Stamp)

function Write-Log {
  param([string]$Msg)
  $ts = (Get-Date).ToString('u')
  $line = "[$ts] $Msg"
  $line | Out-File -FilePath $LogFile -Append -Encoding UTF8
  if (-not $Quiet) { Write-Host $line }
}

Write-Log "Starting NBA local daily update for date=$Date"
Write-Log "Python: $Python"

# Optionally sync repo to reduce push conflicts
if ($GitSyncFirst) {
  try {
    Write-Log 'Git: pull --rebase'
    & git pull --rebase 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  } catch { Write-Log ("Git sync failed: {0}" -f $_.Exception.Message) }
}

# Helper to run a python module and record exit codes
function Invoke-PyMod {
  param([string[]]$plist)
  $cmd = @($Python) + $plist
  Write-Log ("Run: {0}" -f ($cmd -join ' '))
  & $Python @plist 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  return $LASTEXITCODE
}

# If local Flask app is running, prefer calling the composite cron endpoint (does props+predictions+recon)
# Default to local; if RemoteBaseUrl is provided, try that first
$BaseUrl = if ($RemoteBaseUrl) { $RemoteBaseUrl } else { "http://127.0.0.1:5050" }
$UseServer = $false
try {
  $resp = Invoke-WebRequest -UseBasicParsing -Uri ($BaseUrl + '/health') -TimeoutSec 5
  if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) { $UseServer = $true }
} catch { $UseServer = $false }

if ($UseServer) {
  Write-Log "Local server detected at $BaseUrl; invoking /api/cron/run-all"
  try {
    $token = $env:CRON_TOKEN
    $headers = @{}
    if ($token) { $headers['Authorization'] = "Bearer $token" }
    # Run predict-date first (async), then refresh-bovada, then props-predictions, then props-edges, then reconcile-yesterday
    $u1 = "$BaseUrl/api/cron/predict-date?date=$Date&skip_if_no_games=1&async=1&push=0"
    $u2 = "$BaseUrl/api/cron/refresh-bovada?date=$Date&push=0"
  $u3 = "$BaseUrl/api/cron/props-predictions?date=$Date&slate_only=1&calibrate=1&calib_window=7&push=0"
    $u4 = "$BaseUrl/api/cron/props-edges?date=$Date&source=auto&push=0"
    try { $r1 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u1 -TimeoutSec 120; ($r1.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("predict-date call failed: {0}" -f $_.Exception.Message) }
    try { $r2 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u2 -TimeoutSec 180; ($r2.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("refresh-bovada call failed: {0}" -f $_.Exception.Message) }
    try { $r3 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u3 -TimeoutSec 180; ($r3.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("props-predictions call failed: {0}" -f $_.Exception.Message) }
    try { $r4 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u4 -TimeoutSec 240; ($r4.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("props-edges call failed: {0}" -f $_.Exception.Message) }
    # Reconcile yesterday last
    try {
      $yesterday = (Get-Date ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null))).AddDays(-1).ToString('yyyy-MM-dd')
    } catch { $yesterday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
    $u5 = "$BaseUrl/api/cron/reconcile-games?date=$yesterday&push=0"
    try { $r5 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u5 -TimeoutSec 180; ($r5.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("reconcile-games call failed: {0}" -f $_.Exception.Message) }
    Write-Log "Server-driven pipeline calls completed"
  } catch {
    Write-Log ("server pipeline failed; falling back to CLI: {0}" -f $_.Exception.Message)
    $UseServer = $false
  }
}

if (-not $UseServer) {
  Write-Log 'Running pipeline via CLI (no server or failed request)'
  # 1) Predictions for the target date (writes data/processed/predictions_<date>.csv and attempts to save odds)
  $rc1 = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-date','--date', $Date)
  Write-Log ("predict-date exit code: {0}" -f $rc1)

  # 2) Reconcile yesterday's games (best-effort; requires server endpoint for meta but CLI not needed)
  try {
    $yesterday = (Get-Date ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null))).AddDays(-1).ToString('yyyy-MM-dd')
  } catch { $yesterday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
  Write-Log ("Reconcile games for {0} via server endpoint (if available)" -f $yesterday)
  try {
    $token = $env:CRON_TOKEN
    $headers = @{}
    if ($token) { $headers['Authorization'] = "Bearer $token" }
    $uri = "$BaseUrl/api/cron/reconcile-games?date=$yesterday"
    $r2 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $uri -TimeoutSec 120
    ($r2.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null
  } catch {
    Write-Log ("reconcile-games call failed: {0}" -f $_.Exception.Message)
    # Fallback: run reconcile via CLI
    $rc_recon = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-date','--date', $yesterday)
    Write-Log ("reconcile-date exit code: {0}" -f $rc_recon)
  }

  # 3) Props actuals upsert for yesterday (CLI)
  $rc3 = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-prop-actuals','--date', $yesterday)
  Write-Log ("props-actuals exit code: {0}" -f $rc3)

  # 4) Props edges for today (auto source: OddsAPI if available else Bovada)
  $rc4 = Invoke-PyMod -plist @('-m','nba_betting.cli','props-edges','--date', $Date, '--source','auto')
  Write-Log ("props-edges exit code: {0}" -f $rc4)
}

# Simple retention: keep last 21 local_daily_update_* logs
Get-ChildItem -Path $LogPath -Filter 'local_daily_update_*.log' | Sort-Object LastWriteTime -Descending | Select-Object -Skip 21 | ForEach-Object { Remove-Item $_.FullName -ErrorAction SilentlyContinue }

# Optionally commit and push updated artifacts
if ($GitPush) {
  try {
    Write-Log 'Git: staging and pushing updated artifacts'
    & git add -- data data\processed 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    # Try to include predictions.csv at root if present (legacy)
    if (Test-Path 'predictions.csv') { git add -- predictions.csv | Out-Null }
    $cached = & git diff --cached --name-only
    if ($cached) {
      $msg = "local daily: $Date (predictions/odds/props)"
      & git commit -m $msg 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      & git pull --rebase 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      & git push 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      Write-Log 'Git push complete'
    } else {
      Write-Log 'Git: no staged changes; skipping push'
    }
  } catch {
    Write-Log ("Git push failed: {0}" -f $_.Exception.Message)
  }
}

Write-Log 'Local daily update complete.'
