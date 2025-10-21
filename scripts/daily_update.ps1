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

# Python resolution (prefer local venv which has all dependencies)
$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$NpuPy = 'C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe'

# First try: use local venv if it exists and has pandas
$Python = $null
if (Test-Path $VenvPy) {
  try {
    & $VenvPy -c "import pandas" 2>$null
    if ($LASTEXITCODE -eq 0) {
      $Python = $VenvPy
      $env:PYTHONPATH = Join-Path $RepoRoot 'src'
      Write-Host "Using local venv with pandas"
    }
  } catch { }
}

# Second try: use NPU environment if local venv failed
if (-not $Python -and (Test-Path $NpuPy)) {
  try {
    & $NpuPy -c "import pandas" 2>$null
    if ($LASTEXITCODE -eq 0) {
      $Python = $NpuPy
      $env:PYTHONPATH = Join-Path $RepoRoot 'src'
      Write-Host "Using NPU venv"
    }
  } catch { }
}

# Fallback to system python
if (-not $Python) {
  $Python = 'python'
  Write-Host "Using system python"
}

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
  # Capture both stdout and stderr, but don't fail on stderr output
  $ErrorActionPreference = 'Continue'
  & $Python @plist 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  $exitCode = $LASTEXITCODE
  $ErrorActionPreference = 'Stop'
  return $exitCode
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
  Write-Log "Server detected at $BaseUrl; will call refresh/reconcile, but all model CSVs will still be generated locally."
  try {
    $token = $env:CRON_TOKEN
    $headers = @{}
    if ($token) { $headers['Authorization'] = "Bearer $token" }
    # Warm server props edges via auto source (OddsAPI first) without pushing
    $u2 = "$BaseUrl/api/cron/props-edges?date=$Date&source=auto&push=0"
    try { $r2 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u2 -TimeoutSec 180; ($r2.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("props-edges warm call failed: {0}" -f $_.Exception.Message) }
    # Reconcile yesterday on server (best effort)
    try {
      $yesterday = (Get-Date ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null))).AddDays(-1).ToString('yyyy-MM-dd')
    } catch { $yesterday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
    $u5 = "$BaseUrl/api/cron/reconcile-games?date=$yesterday&push=0"
    try { $r5 = Invoke-WebRequest -UseBasicParsing -Headers $headers -Uri $u5 -TimeoutSec 180; ($r5.Content) | Tee-Object -FilePath $LogFile -Append | Out-Null } catch { Write-Log ("reconcile-games call failed: {0}" -f $_.Exception.Message) }
    Write-Log "Server props-edges warm + reconcile attempted"
  } catch {
    Write-Log ("Server calls failed (non-fatal): {0}" -f $_.Exception.Message)
  }
}

# Always run local pipeline to produce site CSVs
Write-Log 'Running local pipeline to produce predictions/odds/props/edges/exports'
# 0) Ensure current season rosters are fetched/updated prior to projections
try {
  # Compute NBA season string like 2025-26 from the target date
  $dt = [datetime]::ParseExact($Date, 'yyyy-MM-dd', $null)
  $yr = $dt.Year
  $mo = $dt.Month
  if ($mo -ge 7) {
    $season = ('{0}-{1:00}' -f $yr, ($yr + 1) % 100)
  } else {
    $season = ('{0}-{1:00}' -f ($yr - 1), $yr % 100)
  }
  Write-Log ("Fetching team rosters for season {0}" -f $season)
  $rc0 = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-rosters','--season', $season)
  Write-Log ("fetch-rosters exit code: {0}" -f $rc0)
} catch {
  Write-Log ("fetch-rosters error (non-fatal): {0}" -f $_.Exception.Message)
}
# 1) Predictions for the target date (writes data/processed/predictions_<date>.csv and may save odds)
# NOTE: --use-npu flag available but requires sklearn in NPU environment (currently blocked on ARM64 Windows)
$rc1 = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-date','--date', $Date)
Write-Log ("predict-date exit code: {0}" -f $rc1)

# Ensure we have standardized game odds CSV locally; prefer OddsAPI, fall back to Bovada only if empty/missing
$GameOddsPath = Join-Path $RepoRoot ("data/processed/game_odds_{0}.csv" -f $Date)
if (-not (Test-Path $GameOddsPath)) {
  Write-Log "game_odds CSV missing; attempting OddsAPI first (fall back to Bovada if needed)"
  try {
    $pyOdds = @"
import os, pandas as pd
from datetime import datetime
from nba_betting.odds_api import OddsApiConfig, fetch_game_odds_current, consensus_lines_at_close
date_str = os.environ.get('TARGET_DATE')
api_key = os.environ.get('ODDS_API_KEY')
out = os.environ.get('OUT_PATH')
ok = False
if api_key and date_str and out:
    d = datetime.strptime(date_str, '%Y-%m-%d')
    cfg = OddsApiConfig(api_key=api_key)
    long_df = fetch_game_odds_current(cfg, d)
    if long_df is not None and not long_df.empty:
        wide = consensus_lines_at_close(long_df)
        if wide is not None and not wide.empty:
            tmp = wide.copy()
            tmp['date'] = pd.to_datetime(tmp['commence_time']).dt.strftime('%Y-%m-%d')
            tmp = tmp.rename(columns={'away_team':'visitor_team'})
            if 'spread_point' in tmp.columns:
                tmp['home_spread'] = tmp['spread_point']
                tmp['away_spread'] = tmp['home_spread'].apply(lambda x: -x if pd.notna(x) else pd.NA)
            if 'total_point' in tmp.columns:
                tmp['total'] = tmp['total_point']
            cols = [c for c in ['date','commence_time','home_team','visitor_team','home_ml','away_ml','home_spread','away_spread','total'] if c in tmp.columns]
            out_df = tmp[cols].copy()
            out_df['bookmaker'] = 'oddsapi_consensus'
            out_df.to_csv(out, index=False)
            ok = True
print('OK' if ok else 'NO')
"@
    $env:TARGET_DATE = $Date
    $env:OUT_PATH = $GameOddsPath
    $out = & $Python -c $pyOdds 2>&1 | Tee-Object -FilePath $LogFile -Append
    if ($out -match 'OK' -and (Test-Path $GameOddsPath)) {
      Write-Log "Saved game odds via OddsAPI -> $GameOddsPath"
    } else {
      Write-Log "OddsAPI produced no rows or key missing; trying Bovada fallback"
      try {
        & $Python 'scripts/fetch_bovada_game_odds.py' $Date 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      } catch { Write-Log ("Bovada odds fetch failed: {0}" -f $_.Exception.Message) }
    }
  } catch { Write-Log ("Odds fetch block failed: {0}" -f $_.Exception.Message) }
}

# 2) Reconcile yesterday's games (best-effort)
try {
  $yesterday = (Get-Date ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null))).AddDays(-1).ToString('yyyy-MM-dd')
} catch { $yesterday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
Write-Log ("Reconcile games for {0} via server endpoint (if available), else CLI" -f $yesterday)
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

# 3) Props predictions for today (calibrated) to CSV
# NOTE: --use-pure-onnx flag enables pure ONNX with NPU acceleration (NO sklearn required!)
# CHANGED: Removed --slate-only to predict for ALL players (not just today's slate)
# This populates the full props table at /props, then edges are calculated separately
$rc3a = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-props','--date', $Date, '--no-slate-only','--calibrate','--calib-window','7','--use-pure-onnx')
Write-Log ("props-predictions exit code: {0}" -f $rc3a)

# 4) Props actuals upsert for yesterday (CLI)
$rc3 = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-prop-actuals','--date', $yesterday)
Write-Log ("props-actuals exit code: {0}" -f $rc3)
# Safeguard: ensure a dated props_actuals_<yesterday>.csv snapshot exists; if missing but parquet updated, derive CSV
try {
  $snapPath = Join-Path $RepoRoot ("data/processed/props_actuals_{0}.csv" -f $yesterday)
  if (-not (Test-Path $snapPath)) {
    Write-Log "Snapshot $($snapPath) missing; attempting to derive from parquet store"
    $parq = Join-Path $RepoRoot 'data/processed/props_actuals.parquet'
    if (Test-Path $parq) {
      try {
        # Use python to extract rows for that date
        $pycode = @"
import pandas as pd, sys
parq = r'$parq'
date = '$yesterday'
out = r'$snapPath'
df = pd.read_parquet(parq)
if not df.empty:
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    day = df[df['date'] == date]
    if not day.empty:
        day.to_csv(out, index=False)
"@
        $null = & $Python -c $pycode
        if (Test-Path $snapPath) { Write-Log "Derived missing snapshot for $yesterday" } else { Write-Log "No rows found in parquet for $yesterday; snapshot not created" }
      } catch { Write-Log ("Snapshot derive failed: {0}" -f $_.Exception.Message) }
    } else {
      Write-Log "Parquet store missing; cannot derive snapshot"
    }
  }
} catch { Write-Log ("Snapshot safeguard error: {0}" -f $_.Exception.Message) }

# 5) Props edges for today: Prefer OddsAPI, fall back to Bovada only if OddsAPI has no data
$edgesPath = Join-Path $RepoRoot ("data/processed/props_edges_{0}.csv" -f $Date)
$rc4a = Invoke-PyMod -plist @('-m','nba_betting.cli','props-edges','--date', $Date, '--source','oddsapi','--file-only')
Write-Log ("props-edges (oddsapi) exit code: {0}" -f $rc4a)
function Test-File-NonEmpty($path){ if (-not (Test-Path $path)) { return $false }; try { return ((Get-Item $path).Length -gt 8) } catch { return $false } }
if (-not (Test-File-NonEmpty $edgesPath)) {
  Write-Log 'OddsAPI props edges empty; trying Bovada fallback'
  $rc4b = Invoke-PyMod -plist @('-m','nba_betting.cli','props-edges','--date', $Date, '--source','bovada','--file-only')
  Write-Log ("props-edges (bovada) exit code: {0}" -f $rc4b)
}

# 6) Export recommendations CSVs for site consumption
$rc5 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-recommendations','--date', $Date)
Write-Log ("export-recommendations exit code: {0}" -f $rc5)
$rc6 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-props-recommendations','--date', $Date)
Write-Log ("export-props-recommendations exit code: {0}" -f $rc6)

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
