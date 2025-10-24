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

# Load .env (if present) into the current PowerShell process environment so child Python sees keys (e.g., ODDS_API_KEY)
function Import-DotEnv {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  try {
    Get-Content -Path $Path -Encoding UTF8 | ForEach-Object {
      $line = $_.Trim()
      if (-not $line) { return }
      if ($line.StartsWith('#')) { return }
      $idx = $line.IndexOf('=')
      if ($idx -lt 1) { return }
      $key = $line.Substring(0, $idx).Trim()
      $val = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
  if ($key) { Set-Item -Path "Env:$key" -Value $val }
    }
    Write-Log "Loaded environment from .env"
  } catch {
    Write-Log (".env load failed (non-fatal): {0}" -f $_.Exception.Message)
  }
}

# Import .env at repo root if available
$DotEnvPath = Join-Path $RepoRoot '.env'
Import-DotEnv -Path $DotEnvPath

# Ensure Python writes UTF-8 to stdout/stderr to avoid UnicodeEncodeError on Windows PowerShell consoles
$env:PYTHONIOENCODING = 'utf-8'

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
  # Compute NBA season starting year (e.g., 2025 for 2025-26) for CLI that expects an int
  $dt = [datetime]::ParseExact($Date, 'yyyy-MM-dd', $null)
  $yr = $dt.Year
  $mo = $dt.Month
  if ($mo -ge 7) {
    $seasonYear = $yr
  } else {
    $seasonYear = $yr - 1
  }
  Write-Log ("Fetching team rosters for season start {0}" -f $seasonYear)
  $rc0 = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-rosters','--season', $seasonYear)
  Write-Log ("fetch-rosters exit code: {0}" -f $rc0)
} catch {
  Write-Log ("fetch-rosters error (non-fatal): {0}" -f $_.Exception.Message)
}
# 1) Predictions for the target date (writes data/processed/predictions_<date>.csv and may save odds)
# NOTE: --use-npu flag available but requires sklearn in NPU environment (currently blocked on ARM64 Windows)
$rc1 = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-date','--date', $Date)
Write-Log ("predict-date exit code: {0}" -f $rc1)

# Always write standardized game odds via OddsAPI (US/Eastern slate), replacing any existing file
$GameOddsPath = Join-Path $RepoRoot ("data/processed/game_odds_{0}.csv" -f $Date)
Write-Log "Writing game odds via OddsAPI (consensus) to $GameOddsPath"
try {
  $pyOdds = @'
import os, pandas as pd, requests
from datetime import datetime
from nba_betting.odds_api import OddsApiConfig, fetch_game_odds_current, consensus_lines_at_close, ODDS_HOST, NBA_SPORT_KEY
date_str = os.environ.get('TARGET_DATE')
api_key = os.environ.get('ODDS_API_KEY')
out = os.environ.get('OUT_PATH')
out_events = os.environ.get('OUT_EVENTS')
ok = False
why = ''
if api_key and date_str and out:
  d = datetime.strptime(date_str, '%Y-%m-%d')
  cfg = OddsApiConfig(api_key=api_key)
  long_df = fetch_game_odds_current(cfg, d)
  # Write a debug events coverage file
  try:
    ev = requests.get("{}/v4/sports/{}/events".format(ODDS_HOST, NBA_SPORT_KEY), params={"apiKey": api_key}, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=45)
    ev.raise_for_status()
    events = ev.json() or []
  except Exception:
    events = []
  target = pd.to_datetime(d).date()
  try:
    from zoneinfo import ZoneInfo
    et=ZoneInfo("US/Eastern")
  except Exception:
    et=None
  rows = []
  have = set(long_df['event_id'].unique()) if long_df is not None and not long_df.empty else set()
  for e in events:
    try:
      ct_raw = pd.to_datetime(e.get("commence_time"), utc=True)
      ct_et = ct_raw.tz_convert(et).date() if et is not None else ct_raw.date()
    except Exception:
      ct_et = None
    if ct_et == target:
      rows.append({
        "event_id": e.get("id"),
        "commence_time": e.get("commence_time"),
        "home_team": e.get("home_team"),
        "away_team": e.get("away_team"),
        "has_odds": e.get("id") in have,
      })
  if out_events and rows:
    pd.DataFrame(rows).to_csv(out_events, index=False)
  if long_df is None or long_df.empty:
    why = 'no_events_for_date'
  else:
    wide = consensus_lines_at_close(long_df)
    if wide is None or wide.empty:
      why = 'no_consensus_rows'
    else:
      tmp = wide.copy()
      # Date by US/Eastern calendar day
      tmp['date'] = pd.to_datetime(tmp['commence_time'], utc=True).dt.tz_convert('US/Eastern').dt.strftime('%Y-%m-%d')
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
print('OK' if ok else f'NO:{why}')
'@
  $env:TARGET_DATE = $Date
  $env:OUT_PATH = $GameOddsPath
  $env:OUT_EVENTS = (Join-Path $RepoRoot ("data/processed/oddsapi_events_{0}.csv" -f $Date))
  $tmpPy = Join-Path $LogPath ("oddsapi_write_{0}.py" -f $Stamp)
  Set-Content -Path $tmpPy -Value $pyOdds -Encoding UTF8
  $out = & $Python $tmpPy 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($out -match 'OK' -and (Test-Path $GameOddsPath)) {
  Write-Log "Saved game odds via OddsAPI -> $GameOddsPath"
  } else {
  Write-Log ("OddsAPI wrote no rows: {0}" -f $out)
  }
} catch { Write-Log ("Odds fetch block failed: {0}" -f $_.Exception.Message) }

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

# 2.2) Ensure finals CSV for yesterday (best-effort; helps UI backfill and offline environments)
try {
  Write-Log ("Export finals CSV for {0}" -f $yesterday)
  $rc_fin = Invoke-PyMod -plist @('-m','nba_betting.cli','finals-export','--date', $yesterday)
  Write-Log ("finals-export exit code: {0}" -f $rc_fin)
} catch {
  Write-Log ("finals-export failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.1) Best-effort finals export for yesterday (writes data/processed/finals_<date>.csv)
try {
  Write-Log ("Exporting finals CSV for {0}" -f $yesterday)
  $pyFinals = @'
import os
from app import _write_finals_csv_for_date
d = os.environ.get("YDAY")
if d:
    p, n = _write_finals_csv_for_date(d)
    print(f"WROTE:{p}:{n}")
else:
    print("NO_DATE")
'@
  $env:YDAY = $yesterday
  $tmpPyF = Join-Path $LogPath ("finals_export_{0}.py" -f $Stamp)
  Set-Content -Path $tmpPyF -Value $pyFinals -Encoding UTF8
  $outF = & $Python $tmpPyF 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($outF -match 'WROTE:') { Write-Log ("Finals export result: {0}" -f $outF) } else { Write-Log ("Finals export returned: {0}" -f $outF) }
} catch { Write-Log ("Finals export block failed (non-fatal): {0}" -f $_.Exception.Message) }

# 2.5) Fetch injuries before building props projections (ensures inactive players are filtered)
try {
  Write-Log "Fetching injuries from ESPN before props predictions"
  $rcInj = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-injuries')
  Write-Log ("fetch-injuries exit code: {0}" -f $rcInj)
} catch { Write-Log ("fetch-injuries error (non-fatal): {0}" -f $_.Exception.Message) }

# 3) Props predictions for today (calibrated) to CSV
# NOTE: --use-pure-onnx flag enables pure ONNX with NPU acceleration (NO sklearn required!)
# IMPORTANT: Restrict predictions to today's slate only (do NOT generate for all rostered players)
$rc3a = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-props','--date', $Date, '--slate-only','--calibrate','--calib-window','7','--use-pure-onnx')
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

# 5) Props edges for today: OddsAPI only (no Bovada fallback)
$edgesPath = Join-Path $RepoRoot ("data/processed/props_edges_{0}.csv" -f $Date)
# Remove any stale file so an empty OddsAPI result doesn't leave prior Bovada content around
try { if (Test-Path $edgesPath) { Remove-Item $edgesPath -Force } } catch { }
# 4.9) Explicit props odds snapshot (current) for the day (writes processed diagnostics + raw per-day archive)
try {
  Write-Log "Fetching current player props odds (OddsAPI) and writing snapshots"
  $pyProps = @'
import os, pandas as pd
from datetime import datetime
from nba_betting.odds_api import OddsApiConfig, fetch_player_props_current
from nba_betting.config import paths

date_str = os.environ.get('TARGET_DATE')
api_key = os.environ.get('ODDS_API_KEY')
ok = False; why = ''
if api_key and date_str:
    d = datetime.strptime(date_str, '%Y-%m-%d')
    cfg = OddsApiConfig(api_key=api_key)
    df = fetch_player_props_current(cfg, date=d, markets=None, verbose=True)
    # fetch_player_props_current already writes processed per-day diagnostics under data/processed
    # Write a raw per-day archive as well for auditing
    if df is not None and not df.empty:
        out_csv = paths.data_raw / f"odds_nba_player_props_{d.date()}.csv"
        df.to_csv(out_csv, index=False)
        ok = True
    else:
        why = 'no_player_props'
else:
    why = 'missing_api_key_or_date'
print('OK' if ok else f'NO:{why}')
'@
  $env:TARGET_DATE = $Date
  $tmpPy2 = Join-Path $LogPath ("props_odds_snapshot_{0}.py" -f $Stamp)
  Set-Content -Path $tmpPy2 -Value $pyProps -Encoding UTF8
  $out2 = & $Python $tmpPy2 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($out2 -match 'OK') { Write-Log 'Saved player props odds snapshots (processed + raw per-day)' } else { Write-Log ("Props odds snapshot returned: {0}" -f $out2) }
} catch { Write-Log ("Props odds snapshot block failed: {0}" -f $_.Exception.Message) }

# 5) Props edges for today: force mode=current to ensure processed per-day odds snapshots are written
$rc4a = Invoke-PyMod -plist @('-m','nba_betting.cli','props-edges','--date', $Date, '--source','oddsapi','--mode','current','--file-only')
Write-Log ("props-edges (oddsapi, mode=current) exit code: {0}" -f $rc4a)

# 6) Export recommendations CSVs for site consumption
$rc5 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-recommendations','--date', $Date)
Write-Log ("export-recommendations exit code: {0}" -f $rc5)
$rc6 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-props-recommendations','--date', $Date)
Write-Log ("export-props-recommendations exit code: {0}" -f $rc6)

# Simple retention: keep last 21 local_daily_update_* logs
Get-ChildItem -Path $LogPath -Filter 'local_daily_update_*.log' | Sort-Object LastWriteTime -Descending | Select-Object -Skip 21 | ForEach-Object { Remove-Item $_.FullName -ErrorAction SilentlyContinue }

# Ensure push to Git is the final action of the script when enabled
if (-not $GitPush) {
  Write-Log 'Local daily update complete (no Git push requested).'
} else {
  # Optionally commit and push updated artifacts (LAST STEP)
  try {
    Write-Log 'Git: staging and pushing updated artifacts (final step)'
    & git add -- data data\processed 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    # Try to include predictions.csv at root if present (legacy)
    if (Test-Path 'predictions.csv') { git add -- predictions.csv | Out-Null }
    $cached = & git diff --cached --name-only
    if ($cached) {
      $msg = "local daily: $Date (predictions/odds/props)"
      & git commit -m $msg 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      & git pull --rebase 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      & git push 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    } else {
      Write-Log 'Git: no staged changes; skipping push'
    }
  } catch {
    Write-Log ("Git push failed: {0}" -f $_.Exception.Message)
  }
}
