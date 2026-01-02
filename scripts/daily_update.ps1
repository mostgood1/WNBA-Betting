Param(
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$Quiet,
  [string]$LogDir = "logs",
  # If set, stage/commit/pull --rebase/push repo changes (data/processed etc.)
  [switch]$GitPush,
  # If set, do a 'git pull --rebase' before running to reduce conflicts
  [switch]$GitSyncFirst,
  # If set, skip applying totals calibration to predictions (safety valve)
  [switch]$SkipTotalsCalib,
  # Optional: Remote server base URL (updated to the correct Render site)
  [string]$RemoteBaseUrl = "https://nba-betting-5qgf.onrender.com",
  # Optional: Bare -CronToken flag is accepted (no value) to avoid task failures
  [switch]$CronToken,
  # Explicit cron token text (overrides env/.env/file discovery)
  [string]$CronTokenParam,
  # Bare -Token should behave like -CronToken (switch); -TokenValue supplies a token string
  [switch]$Token,
  [string]$TokenValue
)

$ErrorActionPreference = 'Stop'

# Ignore any token-related parameters to enforce local-only execution
$CronToken = $false
$CronTokenParam = $null
$Token = $false
$TokenValue = $null

# Default behavior: push to Git at the end unless explicitly disabled.
# If caller omitted -GitPush, honor env DAILY_UPDATE_ALWAYS_PUSH (default = true)
if (-not $PSBoundParameters.ContainsKey('GitPush')) {
  $always = $env:DAILY_UPDATE_ALWAYS_PUSH
  if ($null -eq $always -or $always -eq '') { $always = '1' }
  if ($always -match '^(1|true|yes)$') { $GitPush = $true } else { $GitPush = $false }
}

# Default behavior for totals calibration: allow skipping via env DAILY_SKIP_TOTALS_CALIB
if (-not $PSBoundParameters.ContainsKey('SkipTotalsCalib')) {
  $stc = $env:DAILY_SKIP_TOTALS_CALIB
  if ($null -ne $stc -and $stc -match '^(1|true|yes)$') { $SkipTotalsCalib = $true } else { $SkipTotalsCalib = $false }
}

# Resolve paths
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# Repo root is the parent of the scripts folder
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# Python resolution (prefer local venv which has all dependencies)
$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$NpuPy = 'C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe'

# Remove stale git index.lock to avoid interactive prompts during unattended runs
function Remove-StaleGitLock {
  try {
    $lock = Join-Path $RepoRoot '.git/index.lock'
    if (Test-Path $lock) {
      $age = (Get-Date) - (Get-Item $lock).LastWriteTime
      if ($age.TotalSeconds -ge 60) {
        Remove-Item $lock -Force -ErrorAction SilentlyContinue
        Write-Log 'Git: removed stale index.lock'
      } else {
        Write-Log 'Git: index.lock present (fresh); leaving in place'
      }
    }
  } catch {
    Write-Log ("Git: lock cleanup failed: {0}" -f $_.Exception.Message)
  }
}

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

# Early exit: skip full run on days with no NBA games
try {
  $pyCheck = @'
import os, sys, json
import pandas as pd
from pathlib import Path

repo = Path(os.environ.get("REPO_ROOT", ".")).resolve()
proc = repo / "data" / "processed"
jf = proc / "schedule_2025_26.json"
df = None
if jf.exists():
    try:
        df = pd.read_json(jf)
    except Exception:
        df = None
if df is None or df.empty:
    try:
        from nba_betting.schedule import fetch_schedule_2025_26
        df = fetch_schedule_2025_26()
    except Exception as e:
        print(f"ERR:{e}")
        raise SystemExit(0)
if "date_utc" in df.columns:
    df["date_utc"] = pd.to_datetime(df["date_utc"], errors="coerce").dt.date.astype(str)
mask = (df["date_utc"].astype(str) == os.environ.get("DATE"))
cnt = int(df[mask].shape[0])
print(f"COUNT:{cnt}")
'@
  $env:REPO_ROOT = $RepoRoot
  $env:DATE = $Date
  $out = & $Python -c $pyCheck 2>&1
  if ($out -match 'COUNT:(\d+)') {
    $games = [int]([regex]::Match($out, 'COUNT:(\d+)').Groups[1].Value)
    if ($games -le 0) {
      Write-Log "No NBA games scheduled today; exiting early"
      return
    } else {
      Write-Log ("Slate size: {0} games" -f $games)
    }
  } elseif ($out -match '^ERR:') {
    Write-Log ("Schedule check encountered error (continuing): {0}" -f $out)
  } else {
    Write-Log ("Schedule check returned: {0}" -f $out)
  }
} catch { Write-Log ("Schedule gating failed (continuing): {0}" -f $_.Exception.Message) }

# Disable remote server authentication; enforce local-only execution
$ServerHeaders = @{}
Write-Log "Server auth: disabled; enforcing local-only run"

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

# Enforce local-only pipeline; skip any server detection/calls
$BaseUrl = "http://127.0.0.1:5050"
$UseServer = $false
Write-Log 'Remote server calls disabled; running everything locally'

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
# 0.5) Fetch current-season player logs (used for roster sanity checks and calibration)
try {
  $seasonStr = "{0}-{1}" -f $seasonYear, ("{0:d2}" -f (($seasonYear + 1) % 100))
  Write-Log ("Fetching player logs for season {0}" -f $seasonStr)
  $rcLogs = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-player-logs','--seasons', $seasonStr)
  Write-Log ("fetch-player-logs exit code: {0}" -f $rcLogs)
} catch {
  Write-Log ("fetch-player-logs error (non-fatal): {0}" -f $_.Exception.Message)
}
# Optional: Report roster overrides applied
try {
  $ovDir = Join-Path $RepoRoot 'data/overrides'
  if (-not (Test-Path $ovDir)) { New-Item -ItemType Directory -Path $ovDir -Force | Out-Null }
  $ovPath = Join-Path $ovDir 'roster_overrides.csv'
  if (Test-Path $ovPath) {
    $ovCount = 0
    try {
      $ovCount = (Import-Csv -Path $ovPath).Count
    } catch { $ovCount = 0 }
    Write-Log ("Roster overrides present: {0} rows" -f $ovCount)
  } else {
    Write-Log "No roster overrides file found (optional)"
  }
} catch { }
# 1) Predictions for the target date (writes data/processed/predictions_<date>.csv and may save odds)
# NOTE: --use-npu flag available but requires sklearn in NPU environment (currently blocked on ARM64 Windows)
$rc1 = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-date','--date', $Date)
Write-Log ("predict-date exit code: {0}" -f $rc1)

# Always write standardized game odds via CLI (OddsAPI consensus + Bovada fill), including prices
try {
  Write-Log "Writing game odds via CLI odds-snapshots (includes prices, prefers OddsAPI)"
  $rcOdds = Invoke-PyMod -plist @('-m','nba_betting.cli','odds-snapshots','--date', $Date)
  Write-Log ("odds-snapshots exit code: {0}" -f $rcOdds)
} catch { Write-Log ("odds-snapshots block failed: {0}" -f $_.Exception.Message) }

# 1.5) NPU game predictions using enhanced features (CSV-based; no parquet engine required)
try {
  Write-Log ("Running NPU game predictions for {0}" -f $Date)
  $rcNpu = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-games-npu','--date', $Date)
  Write-Log ("predict-games-npu exit code: {0}" -f $rcNpu)
} catch {
  Write-Log ("predict-games-npu failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2) Reconcile yesterday's games (best-effort)
try {
  $yesterday = (Get-Date ([datetime]::ParseExact($Date, 'yyyy-MM-dd', $null))).AddDays(-1).ToString('yyyy-MM-dd')
} catch { $yesterday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
Write-Log ("Reconcile games for {0} via local CLI" -f $yesterday)
$rc_recon = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-date','--date', $yesterday)
Write-Log ("reconcile-date exit code: {0}" -f $rc_recon)

# 1.6) Calibrate games probabilities via market blend (train over last 30 days, apply to today)
try {
  $skipBlend = $env:DAILY_SKIP_GAMES_BLEND
  if ($null -eq $skipBlend -or $skipBlend -notmatch '^(1|true|yes)$') {
    Write-Log ("Calibrating games probs via market blend (30d) -> today {0}" -f $Date)
    $tmpOut = Join-Path $LogPath ("predictions_games_blend_tmp_{0}.csv" -f $Stamp)
    $blendScript = Join-Path $RepoRoot 'tools/games_blend.py'
    $args = @($blendScript, '--train-days','30','--apply-date', $Date, '--out', $tmpOut)
    $null = & $Python @args 2>&1 | Tee-Object -FilePath $LogFile -Append
    # Validate output: ensure home_win_prob_cal exists and within [0,1]
    if (Test-Path $tmpOut) {
      $ok = $false
      try {
        $pyVal = @"
import pandas as pd, sys
p = pd.read_csv(r'$tmpOut')
if 'home_win_prob_cal' in p.columns:
  s = pd.to_numeric(p['home_win_prob_cal'], errors='coerce')
  ok = s.dropna().between(0.0, 1.0).all() and s.notna().any()
  print('OK' if ok else 'NO')
else:
  print('NO')
"@
        $out = & $Python -c $pyVal
        if ($out -match '^OK') { $ok = $true }
      } catch {}
      if ($ok) {
        $predPath = Join-Path $RepoRoot ("data/processed/predictions_{0}.csv" -f $Date)
        Copy-Item -Path $tmpOut -Destination $predPath -Force
        Remove-Item $tmpOut -Force -ErrorAction SilentlyContinue
        Write-Log "Games market blend applied -> predictions updated with home_win_prob_cal"
      } else {
        Write-Log "Games blend validation failed; keeping original predictions"
        Remove-Item $tmpOut -Force -ErrorAction SilentlyContinue
      }
    } else {
      Write-Log "Games blend wrote no output; skipping apply"
    }
  } else {
    Write-Log 'Skipping games market blend (DAILY_SKIP_GAMES_BLEND=1)'
  }
} catch {
  Write-Log ("Games market blend failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 1.6a) Optional isotonic calibration (adds home_win_prob_iso) if sufficient recent samples
try {
  $isoSkip = $env:DAILY_SKIP_ISOTONIC
  if ($null -eq $isoSkip -or $isoSkip -notmatch '^(1|true|yes)$') {
    Write-Log ("Attempt isotonic calibration for {0} (lookback=30d, min_samples=200)" -f $Date)
    $isoScript = Join-Path $RepoRoot 'tools/calibrate_isotonic.py'
    if (Test-Path $isoScript) {
      $rcIso = & $Python $isoScript --date $Date --days 30 --min-samples 200 2>&1 | Tee-Object -FilePath $LogFile -Append
      if ($rcIso -match 'train_brier_before') { Write-Log "Isotonic calibration attempted (see log for details)" } else { Write-Log "Isotonic calibration skipped or insufficient samples" }
    } else {
      Write-Log 'Isotonic script missing; skipping'
    }
  } else {
    Write-Log 'Skipping isotonic calibration (DAILY_SKIP_ISOTONIC=1)'
  }
} catch { Write-Log ("Isotonic calibration block failed (non-fatal): {0}" -f $_.Exception.Message) }

# 1.6b) Reliability curve + HTML (60d) automation
try {
  $relCsv = Join-Path $RepoRoot 'data/processed/reliability_games.csv'
  $relHtml = Join-Path $RepoRoot 'data/processed/reliability_games.html'
  Write-Log 'Computing reliability curve (60d)' 
  $null = Invoke-PyMod -plist @('-m','nba_betting.cli','evaluate-reliability','--days','60')
  if (Test-Path $relCsv) {
    Write-Log 'Generating reliability HTML'
    $plotScript = Join-Path $RepoRoot 'tools/plot_reliability.py'
    if (Test-Path $plotScript) { & $Python $plotScript 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null }
  } else { Write-Log 'Reliability CSV missing after compute step' }
  # 1.6b+) Calibration comparison (raw vs blend vs market)
  $calibCmp = Join-Path $RepoRoot 'tools/calibration_compare.py'
  if (Test-Path $calibCmp) {
    Write-Log 'Computing calibration comparison (60d)'
    & $Python $calibCmp --date $Date --days 60 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  }
} catch { Write-Log ("Reliability automation failed (non-fatal): {0}" -f $_.Exception.Message) }

# 1.6c) Drift monitoring (reference 30d vs current 7d)
try {
  $driftScript = Join-Path $RepoRoot 'tools/drift_monitor.py'
  $skipDrift = $env:DAILY_SKIP_DRIFT
  if ($null -eq $skipDrift -or $skipDrift -notmatch '^(1|true|yes)$') {
    if (Test-Path $driftScript) {
      Write-Log 'Running drift monitor (ref=30d, cur=7d)'
      & $Python $driftScript --date $Date --ref-days 30 --cur-days 7 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      $driftHtmlScript = Join-Path $RepoRoot 'tools/drift_report_html.py'
      if (Test-Path $driftHtmlScript) {
        Write-Log 'Rendering drift HTML summary'
        & $Python $driftHtmlScript --date $Date 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      }
      # Produce weekly drift trend rollup (last 60 days)
      $driftWeeklyScript = Join-Path $RepoRoot 'tools/drift_weekly.py'
      if (Test-Path $driftWeeklyScript) {
        Write-Log 'Rendering weekly drift trend (60d)'
        & $Python $driftWeeklyScript 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      }
    } else { Write-Log 'drift_monitor.py missing; skipping drift check' }
  } else { Write-Log 'Skipping drift monitor (DAILY_SKIP_DRIFT=1)' }
} catch { Write-Log ("Drift monitoring failed (non-fatal): {0}" -f $_.Exception.Message) }

# 1.6d) Interval estimation (spread/total)
try {
  $intScript = Join-Path $RepoRoot 'tools/interval_estimation.py'
  $skipIntervals = $env:DAILY_SKIP_INTERVALS
  if ($null -eq $skipIntervals -or $skipIntervals -notmatch '^(1|true|yes)$') {
    if (Test-Path $intScript) {
      Write-Log 'Estimating predictive intervals (ref=30d, z=1.96)'
      & $Python $intScript --date $Date --ref-days 30 --z 1.96 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    } else { Write-Log 'interval_estimation.py missing; skipping intervals' }
  } else { Write-Log 'Skipping interval estimation (DAILY_SKIP_INTERVALS=1)' }
} catch { Write-Log ("Interval estimation failed (non-fatal): {0}" -f $_.Exception.Message) }

# 2.2) Ensure finals CSV for yesterday (best-effort; helps UI backfill and offline environments)
try {
  Write-Log ("Export finals CSV for {0}" -f $yesterday)
  $rc_fin = Invoke-PyMod -plist @('-m','nba_betting.cli','finals-export','--date', $yesterday)
  Write-Log ("finals-export exit code: {0}" -f $rc_fin)
} catch {
  Write-Log ("finals-export failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.3) Fetch yesterday's play-by-play logs (finals-only)
try {
  Write-Log ("Fetching PBP logs for {0} (finals only)" -f $yesterday)
  $rc_pbp = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-pbp','--date', $yesterday, '--finals-only')
  Write-Log ("fetch-pbp exit code: {0}" -f $rc_pbp)
} catch {
  Write-Log ("fetch-pbp error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4) Fetch yesterday's boxscores (finals-only)
try {
  Write-Log ("Fetching boxscores for {0} (finals only)" -f $yesterday)
  $rc_bs = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-boxscores','--date', $yesterday, '--finals-only')
  Write-Log ("fetch-boxscores exit code: {0}" -f $rc_bs)
} catch {
  Write-Log ("fetch-boxscores error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.5) Backfill missing recon files for the season to date (idempotent)
try {
  $seasonStart = '2025-10-21'
  Write-Log ("Recon backfill scan from {0} to {1}" -f $seasonStart, $yesterday)
  $d0 = [datetime]::ParseExact($seasonStart, 'yyyy-MM-dd', $null)
  $d1 = [datetime]::ParseExact($yesterday, 'yyyy-MM-dd', $null)
  $cur = $d0
  $toBuild = @()
  while ($cur -le $d1) {
    $ds = $cur.ToString('yyyy-MM-dd')
    $p = Join-Path $RepoRoot ("data/processed/recon_games_{0}.csv" -f $ds)
    if (-not (Test-Path $p)) { $toBuild += $ds }
    $cur = $cur.AddDays(1)
  }
  if ($toBuild.Count -gt 0) {
    Write-Log ("Recon backfill missing dates: {0}" -f ($toBuild -join ', '))
    Remove-StaleGitLock
    $built = @()
    foreach ($ds in $toBuild) {
      Write-Log ("Build recon for {0}" -f $ds)
      $rc_bf = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-date','--date', $ds)
      Write-Log ("reconcile-date ({0}) exit code: {1}" -f $ds, $rc_bf)
      $pp = Join-Path $RepoRoot ("data/processed/recon_games_{0}.csv" -f $ds)
      if ($rc_bf -eq 0 -and (Test-Path $pp)) { $built += $pp }
    }
    if ($built.Count -gt 0) {
      try {
        foreach ($bf in $built) { & git add -- $bf 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null }
        $changedBf = & git diff --cached --name-only -- data/processed/recon_games_*.csv
        if ($changedBf) {
          $msgBf = "data(processed): backfill recon games ($seasonStart..$yesterday)"
          Remove-StaleGitLock
          & git commit -m $msgBf 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
          & git pull --rebase 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
          & git push 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
          Write-Log 'Git: pushed recon backfill commit'
        } else {
          Write-Log 'Git: no recon backfill changes to push'
        }
      } catch { Write-Log ("Git push (recon backfill) failed: {0}" -f $_.Exception.Message) }
    }
  } else {
    Write-Log 'Recon backfill: no missing dates'
  }
} catch { Write-Log ("Recon backfill block failed: {0}" -f $_.Exception.Message) }

# 2.4a) Reconcile PBP-derived markets for yesterday (tip, first-basket, early-threes)
try {
  Write-Log ("Reconciling PBP markets for {0}" -f $yesterday)
  $rc_pbp_recon = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-pbp-markets','--date', $yesterday)
  Write-Log ("reconcile-pbp-markets exit code: {0}" -f $rc_pbp_recon)
} catch {
  Write-Log ("reconcile-pbp-markets failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4b) Update calibration for PBP markets using rolling window
try {
  Write-Log ("Calibrating PBP markets from reconciliation (window=7) anchored at {0}" -f $yesterday)
  $rc_pbp_cal = Invoke-PyMod -plist @('-m','nba_betting.cli','calibrate-pbp-markets','--anchor', $yesterday, '--window', '7')
  Write-Log ("calibrate-pbp-markets exit code: {0}" -f $rc_pbp_cal)
} catch {
  Write-Log ("calibrate-pbp-markets failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4c) Log daily PBP market health metrics for yesterday (CSV-first)
try {
  Write-Log ("Logging daily PBP market metrics for {0}" -f $yesterday)
  $pyMetrics = @'
import os, pandas as pd, numpy as np

root = os.environ.get("ROOT")
date = os.environ.get("DATE")
out = os.environ.get("OUT")
if not (root and date and out):
    print("NO:missing_env"); raise SystemExit(0)
rec = os.path.join(root, "data", "processed", f"pbp_reconcile_{date}.csv")
if not os.path.exists(rec):
    print("NO:missing_reconcile"); raise SystemExit(0)
df = pd.read_csv(rec)

def _mean(s):
    s = pd.to_numeric(s, errors="coerce").dropna()
    return float(s.mean()) if len(s) > 0 else float("nan")

tip_brier = _mean(df.get("tip_brier"))
tip_logloss = _mean(df.get("tip_logloss"))
tip_prob = pd.to_numeric(df.get("tip_prob_home"), errors="coerce") if "tip_prob_home" in df.columns else pd.Series(dtype=float)
tip_out = pd.to_numeric(df.get("tip_outcome_home"), errors="coerce") if "tip_outcome_home" in df.columns else pd.Series(dtype=float)
tip_acc = float(np.mean([(ph >= 0.5) == (oh == 1.0) for ph, oh in zip(tip_prob.dropna(), tip_out.dropna())])) if (len(tip_prob.dropna()) > 0 and len(tip_out.dropna()) > 0) else float("nan")
tip_n = int(pd.notna(df.get("tip_brier")).sum()) if "tip_brier" in df.columns else 0

fb_hit1 = pd.to_numeric(df.get("first_basket_hit_top1"), errors="coerce").dropna() if "first_basket_hit_top1" in df.columns else pd.Series(dtype=float)
fb_hit5 = pd.to_numeric(df.get("first_basket_hit_top5"), errors="coerce").dropna() if "first_basket_hit_top5" in df.columns else pd.Series(dtype=float)
fb_prob_act = pd.to_numeric(df.get("first_basket_prob_actual"), errors="coerce").dropna() if "first_basket_prob_actual" in df.columns else pd.Series(dtype=float)
fb_top1 = float(fb_hit1.mean()) if len(fb_hit1) > 0 else float("nan")
fb_top5 = float(fb_hit5.mean()) if len(fb_hit5) > 0 else float("nan")
fb_mean_prob_actual = float(fb_prob_act.mean()) if len(fb_prob_act) > 0 else float("nan")
fb_n = int(max(len(fb_hit1), len(fb_hit5))) if (len(fb_hit1) > 0 or len(fb_hit5) > 0) else 0

thr_err = pd.to_numeric(df.get("early_threes_error"), errors="coerce").dropna() if "early_threes_error" in df.columns else pd.Series(dtype=float)
thr_mae = float(thr_err.abs().mean()) if len(thr_err) > 0 else float("nan")
thr_rmse = float(np.sqrt((thr_err ** 2).mean())) if len(thr_err) > 0 else float("nan")
thr_brier = _mean(df.get("early_threes_brier_ge1"))
thr_n = int(len(thr_err))

row = {
    "date": date,
    "tip_n": tip_n, "tip_brier": tip_brier, "tip_logloss": tip_logloss, "tip_acc": tip_acc,
    "fb_n": fb_n, "fb_top1": fb_top1, "fb_top5": fb_top5, "fb_mean_prob_actual": fb_mean_prob_actual,
    "thr_n": thr_n, "thr_mae": thr_mae, "thr_rmse": thr_rmse, "thr_brier_ge1": thr_brier,
}

cols = list(row.keys())
if os.path.exists(out):
    try:
        ex = pd.read_csv(out)
        if not ex.empty and "date" in ex.columns:
            ex = ex[ex["date"].astype(str) != str(date)]
        ex = pd.concat([ex, pd.DataFrame([row])], ignore_index=True)
        ex.to_csv(out, index=False)
    except Exception:
        pd.DataFrame([row])[cols].to_csv(out, index=False)
else:
    pd.DataFrame([row])[cols].to_csv(out, index=False)
print("OK")
'@
  $metricsYear = ($yesterday.Substring(0,4))
  $env:ROOT = $RepoRoot
  $env:DATE = $yesterday
  $env:OUT = (Join-Path $RepoRoot ("data/processed/pbp_metrics_daily_{0}.csv" -f $metricsYear))
  $tmpPyM = Join-Path $LogPath ("pbp_metrics_daily_{0}.py" -f $Stamp)
  Set-Content -Path $tmpPyM -Value $pyMetrics -Encoding UTF8
  $outM = & $Python $tmpPyM 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($outM -match 'OK') {
    Write-Log ("Logged PBP metrics -> {0}" -f $env:OUT)
  } else {
    Write-Log ("PBP metrics logging returned: {0}" -f $outM)
  }
} catch {
  Write-Log ("PBP metrics logging failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4c.pre) Ensure PBP inputs exist for recent days to support recon_quarters backfill (7 days)
try {
  $start = (Get-Date ([datetime]::ParseExact($yesterday, 'yyyy-MM-dd', $null))).AddDays(-7).ToString('yyyy-MM-dd')
  $end = $yesterday
  Write-Log ("Backfilling PBP for {0}..{1} (finals-only, last 7d) to enable recon_quarters" -f $start, $end)
  $rc_bfpbp = Invoke-PyMod -plist @('-m','nba_betting.cli','backfill-pbp','--start', $start, '--end', $end, '--finals-only')
  Write-Log ("backfill-pbp exit code: {0}" -f $rc_bfpbp)
} catch {
  Write-Log ("backfill-pbp failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4d) Reconcile quarters/halves vs predictions for yesterday
try {
  Write-Log ("Reconciling quarters for {0}" -f $yesterday)
  $rc_qrecon = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-quarters','--date', $yesterday)
  Write-Log ("reconcile-quarters exit code: {0}" -f $rc_qrecon)
} catch {
  Write-Log ("reconcile-quarters failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4d.a) Backfill recent recon_quarters (last 7 days) to seed calibration
try {
  $start = (Get-Date ([datetime]::ParseExact($yesterday, 'yyyy-MM-dd', $null))).AddDays(-7)
  $end = [datetime]::ParseExact($yesterday, 'yyyy-MM-dd', $null)
  $cur = $start
  $missing = @()
  while ($cur -le $end) {
    $ds = $cur.ToString('yyyy-MM-dd')
    $p = Join-Path $RepoRoot ("data/processed/recon_quarters_{0}.csv" -f $ds)
    if (-not (Test-Path $p)) { $missing += $ds }
    $cur = $cur.AddDays(1)
  }
  if ($missing.Count -gt 0) {
    Write-Log ("Recon quarters backfill (last 7d) missing: {0}" -f ($missing -join ', '))
    foreach ($ds in $missing) {
      try {
        Write-Log ("Build recon-quarters for {0}" -f $ds)
        $rc_rq = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-quarters','--date', $ds)
        Write-Log ("reconcile-quarters ({0}) exit code: {1}" -f $ds, $rc_rq)
      } catch { Write-Log ("reconcile-quarters ({0}) failed: {1}" -f $ds, $_.Exception.Message) }
    }
  } else {
    Write-Log 'Recon quarters backfill: none missing in last 7 days'
  }
} catch { Write-Log ("Recon quarters backfill block failed (non-fatal): {0}" -f $_.Exception.Message) }

# 2.4e) Calibrate game totals (global + team) using rolling window anchored at yesterday
try {
  Write-Log ("Calibrating game totals (window=14) anchored at {0}" -f $yesterday)
  $rc_cal_tot = Invoke-PyMod -plist @('-m','nba_betting.cli','calibrate-totals','--anchor', $yesterday, '--window', '14')
  Write-Log ("calibrate-totals exit code: {0}" -f $rc_cal_tot)
} catch {
  Write-Log ("calibrate-totals failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4f) Apply totals calibration to today's predictions (adjusts totals and period totals if present)
if (-not $SkipTotalsCalib) {
  try {
    Write-Log ("Applying totals calibration from {0} to predictions for {1}" -f $yesterday, $Date)

    # Back up the predictions file before applying calibration (safety valve)
    $predPath = Join-Path $RepoRoot ("data/processed/predictions_{0}.csv" -f $Date)
    $backupPath = Join-Path $RepoRoot ("data/processed/_predictions_backup_{0}.csv" -f $Date)
    if (Test-Path $predPath) {
      try { Copy-Item -Path $predPath -Destination $backupPath -Force } catch { Write-Log ("Backup create failed (non-fatal): {0}" -f $_.Exception.Message) }
    } else {
      Write-Log "No predictions CSV found prior to calibration; skipping backup"
    }

    # Apply into a temporary file first; only replace original if validation passes
    $tmpOut = Join-Path $LogPath ("predictions_calib_tmp_{0}.csv" -f $Stamp)
    $rc_apply_tot = Invoke-PyMod -plist @('-m','nba_betting.cli','apply-totals-calibration','--date', $Date, '--calib-date', $yesterday, '--in', $predPath, '--out', $tmpOut)
    Write-Log ("apply-totals-calibration exit code: {0}" -f $rc_apply_tot)

    # Validate predictions after apply; if invalid, restore backup
    try {
      $tmpPy = Join-Path $LogPath ("validate_predictions_{0}.py" -f $Stamp)
      $pyCode = @"
  import sys, os, pandas as pd, numpy as np
  from pathlib import Path
  pred_path = Path(r"$tmpOut")
  ok=True; why=[]; stats={}
  if not pred_path.exists():
    print("NO:file_missing"); sys.exit(0)
  df = pd.read_csv(pred_path)

  # Thresholds from environment with sensible defaults
  def _getf(name, default):
    try:
      v = os.environ.get(name)
      return float(v) if v is not None and str(v).strip() != '' else float(default)
    except Exception:
      return float(default)
  TOT_MIN = _getf('TOTALS_MIN', 120)
  TOT_MAX = _getf('TOTALS_MAX', 300)
  QTR_MIN = _getf('QTR_MIN', 15)
  QTR_MAX = _getf('QTR_MAX', 80)
  HALF_MIN = _getf('HALF_MIN', 30)
  HALF_MAX = _getf('HALF_MAX', 160)
  QSUM_TOL = _getf('QSUM_TOL', 25)

  def in_range(s, lo, hi):
    import pandas as _pd
    s = _pd.to_numeric(s, errors="coerce")
    if s.isna().all():
      return False
    v = s.dropna().astype(float)
    try:
      key = getattr(s, 'name', None) or 'col'
      if len(v) > 0:
        stats[key] = {'min': float(np.nanmin(v)), 'max': float(np.nanmax(v))}
    except Exception:
      pass
    return bool(((s >= lo) & (s <= hi)).fillna(True).all())

  cols = set(df.columns)
  if ("totals" not in cols) or (not in_range(df["totals"], TOT_MIN, TOT_MAX)):
    ok=False; why.append("totals_out_of_range")
  for c, lo, hi in [
    ("quarters_q1_total", QTR_MIN, QTR_MAX),
    ("quarters_q2_total", QTR_MIN, QTR_MAX),
    ("quarters_q3_total", QTR_MIN, QTR_MAX),
    ("quarters_q4_total", QTR_MIN, QTR_MAX),
    ("halves_h1_total", HALF_MIN, HALF_MAX),
    ("halves_h2_total", HALF_MIN, HALF_MAX),
  ]:
    if c in cols and not in_range(df[c], lo, hi):
      ok=False; why.append(f"{c}_out_of_range")
  # Optional: quarter sum approx equals game totals within tolerance where all quarters present
  try:
    need = ["quarters_q1_total","quarters_q2_total","quarters_q3_total","quarters_q4_total","totals"]
    if all(n in cols for n in need):
      import pandas as _pd
      qsum = sum(_pd.to_numeric(df[n], errors="coerce") for n in need[:-1])
      tot = _pd.to_numeric(df["totals"], errors="coerce")
      diff = (qsum - tot).abs()
      if not (diff <= QSUM_TOL).fillna(True).all():
        ok=False; why.append("quarter_sum_mismatch")
  except Exception:
    pass
  if ok:
    print("OK:"+str({k:{'min':v['min'],'max':v['max']} for k,v in stats.items()}))
  else:
    print("NO:"+",".join(why)+";stats:"+str({k:{'min':v['min'],'max':v['max']} for k,v in stats.items()}))
"@
      Set-Content -Path $tmpPy -Value $pyCode -Encoding UTF8
      $valOut = & $Python $tmpPy 2>&1 | Tee-Object -FilePath $LogFile -Append
      if ($valOut -notmatch '^OK') {
        Write-Log ("Calibration validation failed: {0}" -f $valOut)
        try { if (Test-Path $tmpOut) { Remove-Item $tmpOut -Force } } catch { }
        if ((Test-Path $backupPath) -and (Test-Path $predPath)) {
          Write-Log "Kept original predictions (pre-calibration) due to failed validation"
        }
      } else {
        Write-Log "Calibration validation: OK"
        try {
          if (Test-Path $tmpOut) {
            Copy-Item -Path $tmpOut -Destination $predPath -Force
            Remove-Item $tmpOut -Force
          }
        } catch { Write-Log ("Finalizing calibrated predictions failed: {0}" -f $_.Exception.Message) }
      }
    } catch {
      Write-Log ("Validation block failed (non-fatal): {0}" -f $_.Exception.Message)
    }
  } catch {
    Write-Log ("apply-totals-calibration failed (non-fatal): {0}" -f $_.Exception.Message)
  }
} else {
  Write-Log 'Skipping totals calibration (SkipTotalsCalib=true)'
}

# 2.5) Roster audit for yesterday (requires boxscores); writes roster_audit_<yesterday>.csv
try {
  Write-Log ("Running roster audit for {0}" -f $yesterday)
  $rc_audit = Invoke-PyMod -plist @('-m','nba_betting.cli','audit-rosters','--date', $yesterday)
  Write-Log ("audit-rosters exit code: {0}" -f $rc_audit)
} catch {
  Write-Log ("audit-rosters error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.1) Finals export dedupe: only run custom export if CSV missing and CLI path didn't produce it
try {
  $finalsCsv = Join-Path $RepoRoot ("data/processed/finals_{0}.csv" -f $yesterday)
  if (-not (Test-Path $finalsCsv)) {
    Write-Log ("Exporting finals CSV for {0} (custom path)" -f $yesterday)
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
  } else {
    Write-Log ("Finals CSV already present for {0}; skipping custom export" -f $yesterday)
  }
} catch { Write-Log ("Finals export block failed (non-fatal): {0}" -f $_.Exception.Message) }

# 2.6) Fetch injuries before building props projections (ensures inactive players are filtered)
try {
  Write-Log "Fetching injuries from ESPN before props predictions"
  $rcInj = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-injuries')
  Write-Log ("fetch-injuries exit code: {0}" -f $rcInj)
} catch { Write-Log ("fetch-injuries error (non-fatal): {0}" -f $_.Exception.Message) }

# 2.6a) Snapshot injuries counts (team-level + excluded players) for explainability caches
try {
  Write-Log ("Snapshot injuries counts cache for {0}" -f $Date)
  $injTool = Join-Path $RepoRoot 'tools/snapshot_injuries.py'
  if (Test-Path $injTool) {
    & $Python $injTool --date $Date 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  } else {
    Write-Log 'snapshot_injuries.py missing; skipping injuries cache'
  }
} catch { Write-Log ("Injuries snapshot failed (non-fatal): {0}" -f $_.Exception.Message) }

# 2.6) Build unified league_status for today (roster + injuries; consumed by predictions)
try {
  Write-Log "Building league_status for today's slate"
  $rcLS = Invoke-PyMod -plist @('-m','nba_betting.cli','build-league-status','--date', $Date)
  Write-Log ("build-league-status exit code: {0}" -f $rcLS)
  $lsPath = Join-Path $RepoRoot ("data/processed/league_status_{0}.csv" -f $Date)
  if (Test-Path $lsPath) {
    try {
      $rows = (Import-Csv -Path $lsPath | Measure-Object).Count
      Write-Log ("league_status rows: {0}" -f $rows)   } catch { }
  } else {
    Write-Log "league_status file missing after build; predictions will still run but may be less accurate"
  }
} catch { Write-Log ("build-league-status failed (non-fatal): {0}" -f $_.Exception.Message) }

# 3) Props predictions for today (calibrated) to CSV
# NOTE: --use-pure-onnx flag enables pure ONNX with NPU acceleration (NO sklearn required!)
# IMPORTANT: Restrict predictions to today's slate only (do NOT generate for all rostered players)
$rc3a = Invoke-PyMod -plist @(
  '-m','nba_betting.cli','predict-props',
  '--date', $Date,
  '--slate-only',
  '--calibrate','--calib-window','7',
  '--calibrate-player','--player-calib-window','30',
  '--use-pure-onnx'
)
Write-Log ("props-predictions exit code: {0}" -f $rc3a)

# 3.1) Post-process props_predictions to drop OUT players (ensures downstream CSVs have no injured players at all)
try {
  $ppPath = Join-Path $RepoRoot ("data/processed/props_predictions_{0}.csv" -f $Date)
  $injExclPath = Join-Path $RepoRoot ("data/processed/injuries_excluded_{0}.csv" -f $Date)
  if (Test-Path $ppPath) {
  Write-Log "Filtering props_predictions to remove OUT players based on injuries_excluded list"
  $pyFilt = @'
import os, pandas as pd
from pathlib import Path

preds_path = Path(os.environ.get("PP"))
inj_path = Path(os.environ.get("INJ"))
if not preds_path.exists():
  print("NO_PREDICTIONS"); raise SystemExit(0)
pdf = pd.read_csv(preds_path)
before = len(pdf)
name_keys = set(); short_keys = set()
def _norm_player_name(s: str) -> str:
  if s is None: return ""
  t = str(s)
  if "(" in t:
    t = t.split("(", 1)[0]
  t = (t.replace("-"," ").replace(".", "").replace("'", "").replace(","," ").strip())
  for suf in (" JR"," SR"," II"," III"," IV"):
    if t.upper().endswith(suf):
      t = t[: -len(suf)]
  try: t = t.encode("ascii","ignore").decode("ascii")
  except Exception: pass
  return t.upper().strip()
def _short_player_key(s: str) -> str:
  s2 = _norm_player_name(s)
  parts = [p for p in s2.replace("-"," ").split() if p]
  if not parts: return s2
  last = parts[-1]; first_initial = parts[0][0] if parts and parts[0] else ""
  return f"{last}{first_initial}"
if inj_path.exists():
  idf = pd.read_csv(inj_path)
  if not idf.empty and "player" in idf.columns:
    s = idf["player"].dropna().astype(str)
    name_keys = set(s.map(_norm_player_name).tolist())
    short_keys = set(s.map(_short_player_key).tolist())
if name_keys or short_keys:
  pdf["_name_key"] = pdf.get("player_name").astype(str).map(_norm_player_name)
  pdf["_short_key"] = pdf.get("player_name").astype(str).map(_short_player_key)
  mask = (~pdf["_name_key"].isin(name_keys)) & (~pdf["_short_key"].isin(short_keys))
  pdf = pdf[mask].drop(columns=["_name_key","_short_key"], errors="ignore")
after = len(pdf)
pdf.to_csv(preds_path, index=False)
print(f"FILTERED:{before}->{after}")
'@
  $env:PP = $ppPath
  $env:INJ = $injExclPath
  $tmpPyF = Join-Path $LogPath ("props_predictions_filter_{0}.py" -f $Stamp)
  Set-Content -Path $tmpPyF -Value $pyFilt -Encoding UTF8
  $outFilt = & $Python $tmpPyF 2>&1 | Tee-Object -FilePath $LogFile -Append
  Write-Log ("Props predictions filter result: {0}" -f $outFilt)
  } else {
  Write-Log "No props_predictions file found to filter; skipping"
  }
} catch { Write-Log ("Props predictions filter failed (non-fatal): {0}" -f $_.Exception.Message) }

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
        # Use python to extract rows for that date (one-liner to avoid here-string parsing issues)
        $pycode = (
          "import pandas as pd; parq=r'" + $parq + "'; date='" + $yesterday + "'; out=r'" + $snapPath + "'; " +
          "df=pd.read_parquet(parq); " +
          "\nimport pandas as pd; " +
          "\nimport numpy as np; " +
          "\nimport sys; " +
          "\n" +
          "\n" +
          "\nif not df.empty:\n" +
          "    df['date']=pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d');\n" +
          "    day=df[df['date']==date];\n" +
          "    (day.to_csv(out, index=False) if not day.empty else None)"
        )
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
# 6a) Game recommendations from predictions + odds
$rc5 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-recommendations','--date', $Date)
Write-Log ("export-recommendations exit code: {0}" -f $rc5)
# 6b) High-confidence picks (blended scoring) for games
try {
  Write-Log ("Generating high-confidence picks for {0}" -f $Date)
  $rc5b = Invoke-PyMod -plist @('-m','nba_betting.cli','recommend-picks','--date', $Date, '--topN','10','--minScore','0.15')
  Write-Log ("recommend-picks exit code: {0}" -f $rc5b)
} catch {
  Write-Log ("recommend-picks failed (non-fatal): {0}" -f $_.Exception.Message)
}
# 6c) Props recommendations
$rc6 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-props-recommendations','--date', $Date)
Write-Log ("export-props-recommendations exit code: {0}" -f $rc6)

# 6c.1) Post-process props_recommendations to prefer regular-priced plays and add explainability
try {
  Write-Log ("Post-processing props_recommendations for {0}: prefer regular-priced plays, add reasons" -f $Date)
  $propsCsv = Join-Path $RepoRoot ("data/processed/props_recommendations_{0}.csv" -f $Date)
  if (Test-Path $propsCsv) {
  $tmpPy3 = Join-Path $LogPath ("props_recs_regular_patch_{0}.py" -f $Stamp)
  $pycode3 = @'
import json, ast, math
import pandas as pd
from pathlib import Path

date_str = "{DATE_PLACEHOLDER}"
repo_root = Path(r"{REPO_PLACEHOLDER}")
props_csv = repo_root / f"data/processed/props_recommendations_{date_str}.csv"
preds_csv = repo_root / f"data/processed/props_predictions_{date_str}.csv"

def _parse_obj(val):
  if isinstance(val, (list, dict)):
    return val
  s = str(val)
  if not s or s.strip() in {"", "None", "nan"}:
    return None
  try:
    return json.loads(s)
  except Exception:
    try:
      return ast.literal_eval(s)
    except Exception:
      return None

  # 2.4d) Build opponent splits cache (allowed pts/reb/ast/threes, ranks) over recent window
  try {
    Write-Log ("Building opponent splits cache (window=21d) cutoff {0}" -f $Date)
    $oppTool = Join-Path $RepoRoot 'tools/build_opponent_splits.py'
    if (Test-Path $oppTool) {
      & $Python $oppTool --date $Date --days 21 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    } else {
      Write-Log 'build_opponent_splits.py missing; skipping opponent splits cache'
    }
  } catch { Write-Log ("Opponent splits build failed (non-fatal): {0}" -f $_.Exception.Message) }

def _regular_price(pr):
  try:
    if pr is None or (isinstance(pr, float) and math.isnan(pr)):
      return False
    v = float(pr)
    return (-150.0 <= v <= 125.0)
  except Exception:
    return False

def _choose_top_play(plays):
  if not isinstance(plays, list) or not plays:
    return None
  # Prefer regular-priced plays, fallback to any
  regular = [p for p in plays if _regular_price(p.get("price"))]
  cand = regular if regular else plays
  def score(p):
    evp = p.get("ev_pct")
    if evp is not None:
      try:
        return float(evp)
      except Exception:
        pass
    ev = p.get("ev")
    try:
      return float(ev) * 100.0 if ev is not None else 0.0
    except Exception:
      return 0.0
  cand = sorted(cand, key=score, reverse=True)
  p = cand[0]
  return {
    "market": p.get("market"),
    "side": p.get("side"),
    "line": p.get("line"),
    "price": p.get("price"),
    "ev": p.get("ev"),
    "ev_pct": p.get("ev_pct"),
    "book": p.get("book"),
  }

def _build_model_map(preds_df):
  m = {}
  if isinstance(preds_df, pd.DataFrame) and not preds_df.empty:
    tmp = preds_df.copy()
    for c in ("player_name","team"):
      if c not in tmp.columns:
        tmp[c] = None
    def _stat_map(row):
      out = {}
      for col, key in [("pred_pts","pts"),("pred_reb","reb"),("pred_ast","ast"),("pred_threes","threes"),("pred_pra","pra")]:
        if col in tmp.columns:
          try:
            v = float(row.get(col))
            if not math.isnan(v):
              out[key] = v
          except Exception:
            pass
      return out
    tmp["_stat_map"] = tmp.apply(_stat_map, axis=1)
    for _, r in tmp.iterrows():
      k = (str(r.get("player_name") or "").strip().lower(), str(r.get("team") or "").strip().upper())
      m[k] = r.get("_stat_map") or {}
  return m

def _explain_baseline(row, model_map):
  tp = row.get("top_play")
  if not isinstance(tp, dict) or not tp:
    return ""
  mkt = str(tp.get("market") or "").lower()
  line = tp.get("line")
  player = str(row.get("player") or row.get("player_name") or "").strip().lower()
  team = str(row.get("team") or "").strip().upper()
  stats = model_map.get((player, team)) or {}
  base = stats.get(mkt)
  if base is not None and line is not None:
    try:
      delta = float(base) - float(line)
      sign = "+" if delta >= 0 else ""
      return f"model {float(base):.1f} vs line {float(line):.1f} ({sign}{float(delta):.1f})"
    except Exception:
      return ""
  return ""

def _consensus_line_adv(row):
  tp = row.get("top_play") or {}
  plays = row.get("_plays_list") or []
  reasons = []
  cons_norm = 0.0
  line_adv_norm = 0.0
  # EV reason
  evp = tp.get("ev_pct")
  ev = tp.get("ev")
  if evp is not None:
    try:
      reasons.append(f"EV {float(evp):.1f}%")
    except Exception:
      pass
  elif ev is not None:
    try:
      reasons.append(f"EV +{float(ev):.2f}")
    except Exception:
      pass
  # Price friendliness and regular tag
  pr = tp.get("price")
  if pr is not None:
    try:
      if abs(float(pr) + 110.0) <= 10.0:
        reasons.append("Friendly price (~-110)")
      if _regular_price(pr):
        reasons.append("Regular price range")
    except Exception:
      pass
  # Consensus: same market/side across books (prefer regular-priced set)
  mk = str(tp.get("market") or "").lower()
  side = str(tp.get("side") or "").upper()
  same_all = [p for p in (plays or []) if str(p.get("market") or "").lower() == mk and str(p.get("side") or "").upper() == side]
  same_regular = [p for p in same_all if _regular_price(p.get("price"))]
  same = same_regular if same_regular else same_all
  distinct_books = sorted(list({str(p.get("book") or "").lower() for p in same if p.get("book") is not None})) )
  n_books = len(distinct_books)
  if n_books >= 3:
    reasons.append(f"Consensus: {n_books} books aligned")
  cons_norm = max(0.0, min(1.0, (n_books - 1) / 4.0))
  # Line advantage
  try:
    lines = [float(p.get("line")) for p in same if p.get("line") is not None]
    tpl = float(tp.get("line")) if tp.get("line") is not None else None
    if lines and tpl is not None:
      if side == "OVER":
        best = min(lines)
        if tpl <= best + 1e-6:
          reasons.append("Best line available")
          line_adv_norm = 1.0
      elif side == "UNDER":
        best = max(lines)
        if tpl >= best - 1e-6:
          reasons.append("Best line available")
          line_adv_norm = 1.0
  except Exception:
    pass
  return reasons, cons_norm, line_adv_norm

df = pd.read_csv(props_csv)
preds_df = pd.read_csv(preds_csv) if preds_csv.exists() else pd.DataFrame()
model_map = _build_model_map(preds_df)

# Parse plays and compute top_play
df = df.copy()
df["_plays_list"] = df.apply(lambda r: _parse_obj(r.get("plays")), axis=1)
df["top_play"] = df["_plays_list"].map(_choose_top_play)

# Explain baseline
df["top_play_explain"] = df.apply(lambda r: _explain_baseline(r, model_map), axis=1)

# Baseline raw value for scoring
def _baseline_val(row):
  tp = row.get("top_play")
  if not isinstance(tp, dict) or not tp:
    return None
  player = str(row.get("player") or row.get("player_name") or "").strip().lower()
  team = str(row.get("team") or "").strip().upper()
  stats = model_map.get((player, team)) or {}
  m = str(tp.get("market") or "").lower()
  return stats.get(m)
df["top_play_baseline"] = df.apply(_baseline_val, axis=1)

# Consensus, line-advantage, reasons
res = df.apply(lambda r: pd.Series({
  "_reasons_cons_line": _consensus_line_adv(r)
}), axis=1)
df["top_play_reasons"] = res["_reasons_cons_line"].map(lambda x: (x[0] if isinstance(x, tuple) else []))
df["top_play_consensus"] = res["_reasons_cons_line"].map(lambda x: (x[1] if isinstance(x, tuple) else 0.0))
df["top_play_line_adv"] = res["_reasons_cons_line"].map(lambda x: (x[2] if isinstance(x, tuple) else 0.0))
df.drop(columns=["_reasons_cons_line"], inplace=True, errors="ignore")

# Write back with enriched columns; preserve existing columns
df.to_csv(props_csv, index=False)
print("OK")
'@
  $pycode3 = $pycode3.Replace('{DATE_PLACEHOLDER}', $Date).Replace('{REPO_PLACEHOLDER}', $RepoRoot)
  Set-Content -Path $tmpPy3 -Value $pycode3 -Encoding UTF8
  $out3 = & $Python $tmpPy3 2>&1 | Tee-Object -FilePath $LogFile -Append
  if ($out3 -match 'OK') { Write-Log 'Props recommendations patched with regular-priced preference and reasons' } else { Write-Log ("Props recommendations patch returned: {0}" -f $out3) }
  } else {
  Write-Log 'No props_recommendations CSV found; skipping post-process'
  }
} catch {
  Write-Log ("Props recommendations post-process failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 6c.2) Props reliability (60d) and probability calibration JSON (local-only)
try {
  Write-Log "Computing props reliability bins (60d)"
  $relScript = Join-Path $RepoRoot 'tools/compute_props_reliability.py'
  if (Test-Path $relScript) {
    & $Python $relScript 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    $calScript = Join-Path $RepoRoot 'tools/calibrate_props_probability.py'
    if (Test-Path $calScript) {
      Write-Log "Generating props probability calibration JSON"
      & $Python $calScript 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    } else { Write-Log 'Calibration script missing; skipping props_prob_calibration.json' }
  } else { Write-Log 'Props reliability script missing; skipping' }
} catch { Write-Log ("Props reliability/calibration block failed (non-fatal): {0}" -f $_.Exception.Message) }

# 7) PBP-derived markets for today's slate (tip winner, first basket, early threes)
try {
  Write-Log ("Predicting PBP-derived markets for {0}" -f $Date)
  $rcPbp = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-pbp-markets','--date', $Date)
  Write-Log ("predict-pbp-markets exit code: {0}" -f $rcPbp)
} catch {
  Write-Log ("predict-pbp-markets failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 7.1a) First-basket recommendations for today's slate
try {
  Write-Log ("Exporting first-basket recommendations for {0}" -f $Date)
  $rcFbRecs = Invoke-PyMod -plist @('-m','nba_betting.cli','first-basket-recs','--date', $Date)
  Write-Log ("first-basket-recs exit code: {0}" -f $rcFbRecs)
} catch {
  Write-Log ("first-basket-recs failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 7.1) Export compact game cards for frontend
try {
  Write-Log ("Exporting game cards for {0}" -f $Date)
  $rcCards = Invoke-PyMod -plist @('-m','nba_betting.cli','export-game-cards','--date', $Date)
  Write-Log ("export-game-cards exit code: {0}" -f $rcCards)
} catch {
  Write-Log ("export-game-cards failed (non-fatal): {0}" -f $_.Exception.Message)
}

# Simple retention: keep last 21 local_daily_update_* logs
Get-ChildItem -Path $LogPath -Filter 'local_daily_update_*.log' | Sort-Object LastWriteTime -Descending | Select-Object -Skip 21 | ForEach-Object { Remove-Item $_.FullName -ErrorAction SilentlyContinue }

# Ensure push to Git is the final action of the script when enabled
if (-not $GitPush) {
  Write-Log 'Local daily update complete (no Git push requested).'
} else {
  # Use standardized commit script to stage and push only date-scoped processed artifacts
  try {
    Write-Log 'Git: committing processed artifacts via scripts/commit_processed.ps1 (yesterday then today)'
    $commitScript = Join-Path $RepoRoot 'scripts/commit_processed.ps1'
    if (Test-Path $commitScript) {
      Remove-StaleGitLock
      # First, commit yesterday's finals/reconcile outputs without push
      & powershell -NoProfile -ExecutionPolicy Bypass -File $commitScript -Date $yesterday -IncludeJson -DryRun | Tee-Object -FilePath $LogFile -Append | Out-Null
      & powershell -NoProfile -ExecutionPolicy Bypass -File $commitScript -Date $yesterday -IncludeJson | Tee-Object -FilePath $LogFile -Append | Out-Null
      # Then, commit and push today's predictions/edges/odds
      Remove-StaleGitLock
      & powershell -NoProfile -ExecutionPolicy Bypass -File $commitScript -Date $Date -IncludeJson -Push | Tee-Object -FilePath $LogFile -Append | Out-Null

      # Additionally stage and push yearly PBP metrics CSV if present/changed
      try {
        $metricsYear = ($yesterday.Substring(0,4))
        $metricsPath = Join-Path $RepoRoot ("data/processed/pbp_metrics_daily_{0}.csv" -f $metricsYear)
        if (Test-Path $metricsPath) {
          & git add -- $metricsPath 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
          $mchanged = & git diff --cached --name-only -- $metricsPath
          if ($mchanged) {
            $msg2 = "data(processed): update pbp metrics daily ($yesterday)"
            Remove-StaleGitLock
            & git commit -m $msg2 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
            & git push 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
            Write-Log 'Git: pushed pbp_metrics_daily update'
          } else {
            Write-Log 'Git: no changes in pbp_metrics_daily to push'
          }
        }
      } catch { Write-Log ("Git: push pbp_metrics_daily failed: {0}" -f $_.Exception.Message) }
    } else {
      Write-Log 'Commit script missing; falling back to broad staging (data/processed)'
      Remove-StaleGitLock
      & git add -- data data\processed 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    # Legacy root predictions.csv intentionally not staged (Render UI reads date-scoped processed files)
      $cached = & git diff --cached --name-only
      if ($cached) {
        $msg = "local daily: $Date (predictions/odds/props)"
        Remove-StaleGitLock
        & git commit -m $msg 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
        & git pull --rebase 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
        & git push 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
      } else {
        Write-Log 'Git: no staged changes; skipping push'
      }
    }
  } catch {
    Write-Log ("Git push failed: {0}" -f $_.Exception.Message)
  }
}
