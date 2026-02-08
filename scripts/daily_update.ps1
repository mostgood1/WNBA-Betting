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

# Avoid benign warnings on stderr (e.g., pandas DtypeWarning) being treated as errors
# by PowerShell in some environments/tasks.
$env:PYTHONWARNINGS = 'ignore'

# Reduce noisy ONNXRuntime/CPU info warnings (especially on Windows ARM).
# Safe to set even if ignored by the runtime.
$env:ONNXRUNTIME_LOG_SEVERITY_LEVEL = '3'
$env:ORT_DISABLE_CPUINFO = '1'

# PowerShell 7+: avoid treating native stderr as error records.
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

Write-Log "Starting NBA local daily update for date=$Date"
Write-Log "Python: $Python"

# Early exit: skip full run on days with no NBA games
try {
  $jf = Join-Path $RepoRoot 'data\processed\schedule_2025_26.json'
  if (Test-Path $jf) {
    $raw = Get-Content -Path $jf -Raw
    $sched = $raw | ConvertFrom-Json
    $games = 0
    try {
      $games = @($sched | Where-Object {
        try {
          $d = $_.date_utc
          if ($null -eq $d) { return $false }
          $ds = ([datetime]::Parse($d)).ToString('yyyy-MM-dd')
          return $ds -eq $Date
        } catch {
          return ($_.date_utc -eq $Date)
        }
      }).Count
    } catch {
      $games = 0
    }
    if ($games -le 0) {
      Write-Log "No NBA games scheduled today; exiting early"
      return
    }
    Write-Log ("Slate size: {0} games" -f $games)
  } else {
    Write-Log "Schedule file not found; skipping schedule gating"
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

# Helper to run a python module with a hard timeout (prevents hangs on network calls)
function Invoke-PyModWithTimeout {
  param(
    [string[]]$plist,
    [int]$TimeoutSeconds = 180,
    [string]$Label = 'python'
  )
  $cmd = @($Python) + $plist
  Write-Log ("Run (timeout={0}s): {1}" -f $TimeoutSeconds, ($cmd -join ' '))

  $tmpBase = Join-Path $LogPath ("py_{0}_{1}_{2}" -f $Label, $Stamp, ([guid]::NewGuid().ToString('N')))
  $outStd = "$tmpBase.out"
  $outErr = "$tmpBase.err"

  try {
    $p = Start-Process -FilePath $Python -ArgumentList $plist -NoNewWindow -PassThru -RedirectStandardOutput $outStd -RedirectStandardError $outErr
  } catch {
    Write-Log ("Start-Process failed: {0}" -f $_.Exception.Message)
    return 1
  }

  $finished = $false
  try {
    $finished = Wait-Process -Id $p.Id -Timeout $TimeoutSeconds -ErrorAction SilentlyContinue
  } catch { $finished = $false }

  if (-not $finished) {
    try { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue } catch {}
    Write-Log ("TIMEOUT: killed process after {0}s (pid={1})" -f $TimeoutSeconds, $p.Id)
    try {
      if (Test-Path $outStd) { Get-Content -Path $outStd -Raw | Out-File -FilePath $LogFile -Append -Encoding UTF8 }
      if (Test-Path $outErr) { Get-Content -Path $outErr -Raw | Out-File -FilePath $LogFile -Append -Encoding UTF8 }
    } catch {}
    return 124
  }

  try {
    if (Test-Path $outStd) { Get-Content -Path $outStd -Raw | Out-File -FilePath $LogFile -Append -Encoding UTF8 }
    if (Test-Path $outErr) { Get-Content -Path $outErr -Raw | Out-File -FilePath $LogFile -Append -Encoding UTF8 }
  } catch {}

  $exitCode = 0
  try { $exitCode = $p.ExitCode } catch { $exitCode = 0 }
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
  $seasonStr = "{0}-{1}" -f $seasonYear, ("{0:d2}" -f (($seasonYear + 1) % 100))
  Write-Log ("Fetching team rosters for season {0}" -f $seasonStr)
  $rc0 = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-rosters','--season', $seasonStr)
  Write-Log ("fetch-rosters exit code: {0}" -f $rc0)
} catch {
  Write-Log ("fetch-rosters error (non-fatal): {0}" -f $_.Exception.Message)
}
# 0.5) Fetch current-season player logs (used for roster sanity checks and calibration)
try {
  Write-Log ("Fetching player logs for season {0}" -f $seasonStr)
  $rcLogs = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-player-logs','--seasons', $seasonStr)
  Write-Log ("fetch-player-logs exit code: {0}" -f $rcLogs)
} catch {
  Write-Log ("fetch-player-logs error (non-fatal): {0}" -f $_.Exception.Message)
}

# 0.6) Trade-deadline hardening: fetch injuries + build league_status + validate expected dressed players
# This must run BEFORE any predictions/sims so the player pool is up-to-date.
try {
  Write-Log "Fetching injuries from ESPN (availability gate)"
  $rcInjEarly = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-injuries')
  Write-Log ("fetch-injuries exit code: {0}" -f $rcInjEarly)
} catch { Write-Log ("fetch-injuries error (non-fatal): {0}" -f $_.Exception.Message) }

try {
  Write-Log "Building league_status for today's slate (availability gate)"
  $rcLSEarly = Invoke-PyMod -plist @('-m','nba_betting.cli','build-league-status','--date', $Date)
  Write-Log ("build-league-status exit code: {0}" -f $rcLSEarly)
} catch { Write-Log ("build-league-status failed (non-fatal): {0}" -f $_.Exception.Message) }

# 0.65) Roster sanity check: validates slate-team roster depth + duplicates + basic team mapping.
try {
  Write-Log "Roster sanity check (fail-fast)"
  $rcRS = Invoke-PyMod -plist @('-m','nba_betting.cli','roster-sanity','--date', $Date)
  Write-Log ("roster-sanity exit code: {0}" -f $rcRS)
  if ($rcRS -ne 0) { throw "roster-sanity failed (exit=$rcRS)" }
} catch {
  Write-Log ("Roster sanity gate failed: {0}" -f $_.Exception.Message)
  throw
}

try {
  Write-Log "Checking expected dressed players (fail-fast)"
  $rcDress = Invoke-PyMod -plist @('-m','nba_betting.cli','check-dressed','--date', $Date)
  Write-Log ("check-dressed exit code: {0}" -f $rcDress)
  if ($rcDress -ne 0) { throw "check-dressed failed (exit=$rcDress)" }
} catch {
  Write-Log ("Dressed-to-play gate failed: {0}" -f $_.Exception.Message)
  throw
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
  $skipOddsSnap = $env:DAILY_SKIP_ODDS_SNAPSHOTS
  if ($null -ne $skipOddsSnap -and $skipOddsSnap -match '^(1|true|yes)$') {
    Write-Log 'Skipping odds-snapshots (DAILY_SKIP_ODDS_SNAPSHOTS=1)'
  } else {
    $to = $env:DAILY_ODDS_SNAPSHOTS_TIMEOUT_SEC
    if ($null -eq $to -or $to -eq '') { $to = '180' }
    try { $toInt = [int]$to } catch { $toInt = 180 }
    if ($toInt -lt 30) { $toInt = 30 }
    if ($toInt -gt 900) { $toInt = 900 }
    $rcOdds = Invoke-PyModWithTimeout -plist @('-m','nba_betting.cli','odds-snapshots','--date', $Date) -TimeoutSeconds $toInt -Label 'odds_snapshots'
    Write-Log ("odds-snapshots exit code: {0}" -f $rcOdds)
  }
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

# 1.7) Analytical simulations for ML/ATS/TOTAL using factor adjustments
try {
  Write-Log ("Running games simulations for {0}" -f $Date)
  $rcSim = Invoke-PyMod -plist @('-m','nba_betting.cli','simulate-games','--date', $Date)
  Write-Log ("simulate-games exit code: {0}" -f $rcSim)
} catch {
  Write-Log ("simulate-games failed (non-fatal): {0}" -f $_.Exception.Message)
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

# 1.6b++) Connected sim realism (player boxscore) evaluation
# This is for accuracy/regression monitoring. It is enabled by default; set DAILY_SKIP_CONNECTED_REALISM=1 to skip.
try {
  $skipConn = $env:DAILY_SKIP_CONNECTED_REALISM
  if ($null -ne $skipConn -and $skipConn -match '^(1|true|yes)$') {
    Write-Log 'Skipping connected realism (DAILY_SKIP_CONNECTED_REALISM=1)'
  } else {
    $connDays = $env:DAILY_CONNECTED_REALISM_DAYS
    if ($null -eq $connDays -or $connDays -notmatch '^\d+$') { $connDays = '1' }
    $connTopK = $env:DAILY_CONNECTED_REALISM_TOPK
    if ($null -eq $connTopK -or $connTopK -notmatch '^\d+$') { $connTopK = '8' }
    $connSkipOt = $env:DAILY_CONNECTED_REALISM_SKIP_OT
    if ($null -eq $connSkipOt -or $connSkipOt -eq '') { $connSkipOt = '1' }
    $connQS = $env:DAILY_CONNECTED_REALISM_QSAMPLES
    if ($null -eq $connQS -or $connQS -notmatch '^\d+$') { $connQS = '' }
    $connCS = $env:DAILY_CONNECTED_REALISM_CSAMPLES
    if ($null -eq $connCS -or $connCS -notmatch '^\d+$') { $connCS = '' }

    Write-Log ("Running connected realism (days={0}, topK={1}, skipOT={2})" -f $connDays, $connTopK, $connSkipOt)
    $plist = @('-m','nba_betting.cli','evaluate-connected-realism','--days', $connDays, '--top-k', $connTopK)
    if ($connSkipOt -match '^(1|true|yes)$') { $plist += '--skip-ot' }
    if ($connQS -ne '') { $plist += @('--n-quarter-samples', $connQS) }
    if ($connCS -ne '') { $plist += @('--n-connected-samples', $connCS) }

    $elapsed = Measure-Command { $rcConn = Invoke-PyMod -plist $plist }
    Write-Log ("evaluate-connected-realism exit code: {0} (elapsed={1:n2}s)" -f $rcConn, $elapsed.TotalSeconds)
  }
} catch { Write-Log ("Connected realism failed (non-fatal): {0}" -f $_.Exception.Message) }

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

# 2.4b) Append yesterday's boxscores into durable history (best-effort)
try {
  Write-Log ("Updating boxscores history for {0}" -f $yesterday)
  $rc_bsh = Invoke-PyMod -plist @('-m','nba_betting.cli','update-boxscores-history','--date', $yesterday, '--finals-only')
  Write-Log ("update-boxscores-history exit code: {0}" -f $rc_bsh)
} catch {
  Write-Log ("update-boxscores-history error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4c) Append yesterday's ESPN PBP into durable history (enables rotation priors)
try {
  Write-Log ("Updating ESPN PBP history for {0}" -f $yesterday)
  $rc_pbp_espn = Invoke-PyMod -plist @('-m','nba_betting.cli','update-pbp-espn-history','--date', $yesterday, '--finals-only')
  Write-Log ("update-pbp-espn-history exit code: {0}" -f $rc_pbp_espn)
} catch {
  Write-Log ("update-pbp-espn-history error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4d) Refresh rotation priors from substitution events (team-level)
try {
  Write-Log 'Writing rotation priors (first bench sub-in timing)'
  $rc_rot = Invoke-PyMod -plist @('-m','nba_betting.cli','write-rotation-priors','--lookback-days', '60', '--min-games', '5')
  Write-Log ("write-rotation-priors exit code: {0}" -f $rc_rot)
} catch {
  Write-Log ("write-rotation-priors error (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4e) Build ESPN rotation stints/pairs for yesterday (writes data/processed/rotations_espn/* and history files)
try {
  $skipRot = $env:DAILY_SKIP_ROTATIONS_ESPN
  if ($null -eq $skipRot -or $skipRot -notmatch '^(1|true|yes)$') {
    Write-Log ("Updating ESPN rotations history for {0} (stints/pairs/play_context)" -f $yesterday)
    $rc_rot_hist = Invoke-PyMod -plist @('-m','nba_betting.cli','update-rotations-espn-history','--date', $yesterday, '--rate-delay', '0.25')
    Write-Log ("update-rotations-espn-history exit code: {0}" -f $rc_rot_hist)

    # Gap scan: if any stints are missing for yesterday (ESPN flakiness), retry once or twice.
    try {
      $rotRetryMax = $env:DAILY_ROTATIONS_ESPN_RETRY_MAX
      if ($null -eq $rotRetryMax -or $rotRetryMax -eq '') { $rotRetryMax = '1' }
      try { $rotRetryMax = [int]$rotRetryMax } catch { $rotRetryMax = 1 }
      if ($rotRetryMax -lt 0) { $rotRetryMax = 0 }

      $tmpPyRotScan = Join-Path $LogPath ("rotations_gap_scan_{0}.py" -f $Stamp)
      $pyRotScan = @'
import os
from pathlib import Path

repo_root = Path(os.environ.get('REPO_ROOT', '.')).resolve()
date_str = os.environ.get('DATE_STR')

rot_dir = repo_root / 'data' / 'processed' / 'rotations_espn'

def _exists_nonempty(p: Path) -> bool:
    try:
        return p.exists() and p.stat().st_size > 0
    except Exception:
        return bool(p.exists())

missing = []
try:
    from nba_betting.boxscores import _nba_gid_to_tricodes
    gid_map = _nba_gid_to_tricodes(str(date_str)) or {}
    for gid in gid_map.keys():
        gid = str(gid).strip()
        if not gid:
            continue
        hp = rot_dir / f'stints_home_{gid}.csv'
        ap = rot_dir / f'stints_away_{gid}.csv'
        if not (_exists_nonempty(hp) and _exists_nonempty(ap)):
            missing.append(gid)
except Exception:
    missing = []

print('MISSING_COUNT:' + str(len(missing)))
print('MISSING_GIDS:' + ','.join(missing))
'@

      Set-Content -Path $tmpPyRotScan -Value $pyRotScan -Encoding UTF8
      $env:REPO_ROOT = $RepoRoot
      $env:DATE_STR = $yesterday

      for ($attempt = 0; $attempt -le $rotRetryMax; $attempt++) {
        $scanOut = & $Python $tmpPyRotScan 2>&1 | Tee-Object -FilePath $LogFile -Append
        $missingCount = 0
        $missingGids = ''
        if ($scanOut -match 'MISSING_COUNT:(\d+)') {
          $missingCount = [int]([regex]::Match($scanOut, 'MISSING_COUNT:(\d+)').Groups[1].Value)
        }
        if ($scanOut -match 'MISSING_GIDS:([^\r\n]*)') {
          $missingGids = [regex]::Match($scanOut, 'MISSING_GIDS:([^\r\n]*)').Groups[1].Value
        }

        if ($missingCount -le 0) {
          if ($attempt -gt 0) { Write-Log 'Rotations gap scan: OK after retry' }
          break
        }
        if ($attempt -ge $rotRetryMax) {
          Write-Log ("Rotations gap scan: still missing after retries ({0}): {1}" -f $missingCount, $missingGids)
          break
        }

        Write-Log ("Rotations gap scan: missing {0} games; retrying update-rotations-espn-history (attempt {1}/{2}) gids={3}" -f $missingCount, ($attempt + 1), $rotRetryMax, $missingGids)
        $rc_rot_retry = Invoke-PyMod -plist @('-m','nba_betting.cli','update-rotations-espn-history','--date', $yesterday, '--rate-delay', '0.5')
        Write-Log ("update-rotations-espn-history retry exit code: {0}" -f $rc_rot_retry)
      }
    } catch {
      Write-Log ("Rotations gap scan failed (non-fatal): {0}" -f $_.Exception.Message)
    }
  } else {
    Write-Log 'Skipping update-rotations-espn-history (DAILY_SKIP_ROTATIONS_ESPN=1)'
  }
} catch {
  Write-Log ("update-rotations-espn-history error (non-fatal): {0}" -f $_.Exception.Message)
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

# 2.4d.b) Build quarter calibration artifact (used by SmartSim per-quarter realism)
# Controlled by env DAILY_SKIP_QUARTERS_CALIB=1; otherwise rebuild if missing, older than 7 days,
# or recon_quarters_* has been updated more recently than the calibration artifact.
try {
  $skipQCal = $env:DAILY_SKIP_QUARTERS_CALIB
  if ($null -ne $skipQCal -and $skipQCal -match '^(1|true|yes)$') {
    Write-Log 'Skipping quarters calibration build (DAILY_SKIP_QUARTERS_CALIB=1)'
  } else {
    $qcalPath = Join-Path $RepoRoot 'data/processed/quarters_calibration.json'
    $needBuild = $true
    if (Test-Path $qcalPath) {
      $needBuild = $false
      try {
        $qcalItem = Get-Item $qcalPath
        $ageDays = ((Get-Date) - $qcalItem.LastWriteTime).TotalDays
        if ($ageDays -ge 7) { $needBuild = $true }

        $reconGlob = Join-Path $RepoRoot 'data/processed/recon_quarters_*.csv'
        $latestRecon = Get-ChildItem -Path $reconGlob -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($null -ne $latestRecon -and $latestRecon.LastWriteTime -gt $qcalItem.LastWriteTime) {
          $needBuild = $true
        }
      } catch { }
    }
    if ($needBuild) {
      Write-Log 'Building quarters calibration -> data/processed/quarters_calibration.json'
      $rc_qcal = Invoke-PyMod -plist @('tools/build_quarters_calibration.py','--workspace', $RepoRoot, '--out', 'data/processed/quarters_calibration.json')
      Write-Log ("build_quarters_calibration exit code: {0}" -f $rc_qcal)
    } else {
      Write-Log 'Quarters calibration: up-to-date (skipping rebuild)'
    }
  }
} catch {
  Write-Log ("Quarters calibration build failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4e) Calibrate game totals (global + team) using rolling window anchored at yesterday
try {
  Write-Log ("Calibrating game totals (window=14) anchored at {0}" -f $yesterday)
  $rc_cal_tot = Invoke-PyMod -plist @('-m','nba_betting.cli','calibrate-totals','--anchor', $yesterday, '--window', '14')
  Write-Log ("calibrate-totals exit code: {0}" -f $rc_cal_tot)
} catch {
  Write-Log ("calibrate-totals failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4e1) Build/refresh smart_sim_quarter_eval for recent window (non-fatal)
# This is used to calibrate period probabilities (over + cover) and to feed evaluation.
# Controlled by env DAILY_SKIP_SMART_SIM_EVAL_BUILD=1
try {
  $skipSSEval = $env:DAILY_SKIP_SMART_SIM_EVAL_BUILD
  if ($null -ne $skipSSEval -and $skipSSEval -match '^(1|true|yes)$') {
    Write-Log 'Skipping smart_sim_quarter_eval build (DAILY_SKIP_SMART_SIM_EVAL_BUILD=1)'
  } else {
    # Build/refresh PBP-derived period actuals so smart_sim_quarter_eval can join actual quarter/half scores
    # even when games_nba_api.csv is missing recent seasons.
    # Controlled by env DAILY_SKIP_PBP_PERIOD_ACTUALS=1
    try {
      $skipPbpAct = $env:DAILY_SKIP_PBP_PERIOD_ACTUALS
      if ($null -ne $skipPbpAct -and $skipPbpAct -match '^(1|true|yes)$') {
        Write-Log 'Skipping PBP period actuals build (DAILY_SKIP_PBP_PERIOD_ACTUALS=1)'
      } else {
        $pbpDays = $env:DAILY_PBP_PERIOD_ACTUALS_DAYS
        if ($null -eq $pbpDays -or $pbpDays -eq '') { $pbpDays = '7' }
        try { $pbpDaysInt = [int]$pbpDays } catch { $pbpDaysInt = 7 }
        if ($pbpDaysInt -lt 1) { $pbpDaysInt = 1 }
        if ($pbpDaysInt -gt 21) { $pbpDaysInt = 21 }
        $pbpStart = ([datetime]::ParseExact($yesterday, 'yyyy-MM-dd', $null)).AddDays(-($pbpDaysInt - 1)).ToString('yyyy-MM-dd')
        Write-Log ("Building PBP period actuals (start={0}, end={1})" -f $pbpStart, $yesterday)
        $rc_pbp_act = Invoke-PyMod -plist @('tools/build_period_actuals_from_pbp_espn.py','--start', $pbpStart, '--end', $yesterday)
        Write-Log ("build_period_actuals_from_pbp_espn exit code: {0}" -f $rc_pbp_act)
      }
    } catch {
      Write-Log ("PBP period actuals build failed (non-fatal): {0}" -f $_.Exception.Message)
    }

    Write-Log ("Building smart_sim_quarter_eval (end={0}, days=60)" -f $yesterday)
    $rc_ss_eval = Invoke-PyMod -plist @('tools/build_smart_sim_quarter_eval.py','--end', $yesterday, '--days', '60')
    Write-Log ("build_smart_sim_quarter_eval exit code: {0}" -f $rc_ss_eval)
  }
} catch {
  Write-Log ("smart_sim_quarter_eval build failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2.4e2) Calibrate period (quarters/halves) over probabilities using smart_sim_quarter_eval (non-fatal)
# Controlled by env DAILY_SKIP_PERIOD_PROBS_CALIB=1
try {
  $skipPProb = $env:DAILY_SKIP_PERIOD_PROBS_CALIB
  if ($null -ne $skipPProb -and $skipPProb -match '^(1|true|yes)$') {
    Write-Log 'Skipping period probability calibration build (DAILY_SKIP_PERIOD_PROBS_CALIB=1)'
  } else {
    Write-Log ("Calibrating period over probabilities (window=30) anchored at {0}" -f $yesterday)
    $rc_cal_pprob = Invoke-PyMod -plist @('-m','nba_betting.cli','calibrate-period-probs','--anchor', $yesterday, '--window', '30', '--bins', '12', '--alpha', '1.0')
    Write-Log ("calibrate-period-probs exit code: {0}" -f $rc_cal_pprob)
  }
} catch {
  Write-Log ("calibrate-period-probs failed (non-fatal): {0}" -f $_.Exception.Message)
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

# 2.6) league_status + injuries were built earlier as a fail-fast availability gate.

# 2.6.1) Roster correctness audit for today (fail loudly)
try {
  $skipRosterAud = $env:DAILY_SKIP_ROSTER_AUDIT
  if ($null -eq $skipRosterAud -or $skipRosterAud -notmatch '^(1|true|yes)$') {
    Write-Log ("Auditing rosters/team assignments for {0}" -f $Date)
    $rcRoster = Invoke-PyMod -plist @('tools/audit_rosters_today.py','--date', $Date, '--fail-if-stale', '--max-mismatches', '0')
    Write-Log ("audit_rosters_today exit code: {0}" -f $rcRoster)
    if ($rcRoster -ne 0) { throw "Roster correctness audit failed (exit=$rcRoster)" }
  } else {
    Write-Log 'Skipping roster correctness audit (DAILY_SKIP_ROSTER_AUDIT=1)'
  }
} catch {
  Write-Log ("Roster correctness audit failed: {0}" -f $_.Exception.Message)
  throw
}

# 2.6a) Snapshot injuries counts (team-level + excluded players) for explainability caches
# NOTE: run after league_status so the snapshot can stay consistent with the player pool used downstream.
try {
  Write-Log ("Snapshot injuries counts cache for {0}" -f $Date)
  $injTool = Join-Path $RepoRoot 'tools/snapshot_injuries.py'
  if (Test-Path $injTool) {
    & $Python $injTool --date $Date 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  } else {
    Write-Log 'snapshot_injuries.py missing; skipping injuries cache'
  }
} catch { Write-Log ("Injuries snapshot failed (non-fatal): {0}" -f $_.Exception.Message) }

# 3) Props predictions for today (calibrated) to CSV
# NOTE: --use-pure-onnx flag enables pure ONNX with NPU acceleration (NO sklearn required!)
# IMPORTANT: Restrict predictions to today's slate only (do NOT generate for all rostered players)
$rc3a = Invoke-PyMod -plist @(
  '-m','nba_betting.cli','predict-props',
  '--date', $Date,
  '--slate-only',
  '--calibrate','--calib-window','7',
  '--calibrate-player','--player-calib-window','30',
  '--use-pure-onnx',
  '--use-smart-sim',
  '--smart-sim-n-sims','150',
  '--smart-sim-pbp',
  '--smart-sim-overwrite'
)
Write-Log ("props-predictions exit code: {0}" -f $rc3a)

# 3.0) Export SmartSim player quarter + scenario distributions for today's slate (non-fatal)
# Controlled by env DAILY_SKIP_SMARTSIM_PLAYER_SPLITS=1
try {
  $skipSplits = $env:DAILY_SKIP_SMARTSIM_PLAYER_SPLITS
  if ($null -ne $skipSplits -and $skipSplits -match '^(1|true|yes)$') {
    Write-Log 'Skipping SmartSim player splits export (DAILY_SKIP_SMARTSIM_PLAYER_SPLITS=1)'
  } else {
    $outQ = Join-Path $RepoRoot ("data/processed/smartsim_player_quarters_{0}.csv" -f $Date)
    $outS = Join-Path $RepoRoot ("data/processed/smartsim_player_scenarios_{0}.csv" -f $Date)
    Write-Log ("Exporting SmartSim player splits for {0}" -f $Date)
    $rcSplits = Invoke-PyMod -plist @(
      'tools/extract_smartsim_player_splits.py',
      '--start', $Date,
      '--end', $Date,
      '--out-quarters', $outQ,
      '--out-scenarios', $outS
    )
    Write-Log ("extract_smartsim_player_splits exit code: {0}" -f $rcSplits)
  }
} catch {
  Write-Log ("SmartSim player splits export failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 3.1) Post-process props_predictions to drop OUT players (ensures downstream CSVs have no injured players at all)
try {
  $ppPath = Join-Path $RepoRoot ("data/processed/props_predictions_{0}.csv" -f $Date)
  $injExclPath = Join-Path $RepoRoot ("data/processed/injuries_excluded_{0}.csv" -f $Date)
  if (Test-Path $ppPath) {
  Write-Log "Filtering props_predictions to remove OUT players based on injuries_excluded list"

  # Repair injuries_excluded team assignments using processed rosters (prevents cross-team exclusions)
  try {
    $injRepair = Join-Path $RepoRoot 'tools/repair_injuries_excluded.py'
    if ((Test-Path $injRepair) -and (Test-Path $injExclPath)) {
      Write-Log "Repairing injuries_excluded team assignments via rosters"
      & $Python $injRepair --date $Date 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    }
  } catch { Write-Log ("repair_injuries_excluded failed (non-fatal): {0}" -f $_.Exception.Message) }

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
ban_pairs = set(); ban_short_pairs = set(); ban_names = set(); ban_short_names = set()
def _norm_player_name(s: str) -> str:
  if s is None: return ""
  t = str(s)
  if "(" in t:
    t = t.split("(", 1)[0]
  t = (t.replace("-"," ").replace(".", "").replace("'", "").replace(","," ").strip())
  for suf in (" JR"," SR"," II"," III"," IV"):
    if t.upper().endswith(suf):
      t = t[: -len(suf)]
  try:
    import unicodedata as ud
    t = ud.normalize("NFKD", t)
    t = t.encode("ascii","ignore").decode("ascii")
  except Exception: pass
  return t.upper().strip()
def _short_player_key(s: str) -> str:
  s2 = _norm_player_name(s)
  parts = [p for p in s2.replace("-"," ").split() if p]
  if not parts: return s2
  last = parts[-1]; first_initial = parts[0][0] if parts and parts[0] else ""
  return f"{last}{first_initial}"
def _tri(x: object) -> str:
  try:
    s = str(x or "").strip().upper()
  except Exception:
    return ""
  # Trust canonical tricodes.
  if len(s) == 3 and s.isalpha():
    return s
  return ""
if inj_path.exists():
  idf = pd.read_csv(inj_path)
  if not idf.empty and "player" in idf.columns:
    # Treat injuries_excluded as a per-run snapshot that can include older injury row dates.
    # Apply a recency window to avoid stale OUT labels persisting forever.
    cutoff = pd.to_datetime(os.environ.get("SLATE_DATE") or "", errors="coerce")
    cutoff = cutoff.date() if not pd.isna(cutoff) else None
    EXC = {"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}
    def _excluded_status(u: str) -> bool:
      try:
        u = str(u or "").upper().strip()
      except Exception:
        return False
      if not u: return False
      if u in EXC: return True
      if ("OUT" in u and ("SEASON" in u or "INDEFINITE" in u)) or ("SEASON-ENDING" in u):
        return True
      return False
    try:
      if "status" in idf.columns:
        idf = idf[idf["status"].map(_excluded_status)].copy()
    except Exception:
      pass
    try:
      if cutoff is not None and "date" in idf.columns:
        idf["date"] = pd.to_datetime(idf["date"], errors="coerce").dt.date
        idf = idf[idf["date"].notna() & (idf["date"] <= cutoff)].copy()
        from datetime import timedelta
        recency_days = 30
        fresh_cutoff = (cutoff - timedelta(days=int(recency_days)))
        s_norm = idf.get("status", "").astype(str).str.upper().str.strip()
        is_season = s_norm.astype(str).str.contains("SEASON", na=False) | s_norm.astype(str).str.contains("INDEFINITE", na=False) | s_norm.astype(str).str.contains("SEASON-ENDING", na=False)
        idf = idf[(idf["date"] >= fresh_cutoff) | is_season].copy()
    except Exception:
      pass
    # Build team-aware bans whenever possible.
    idf = idf.copy()
    idf["_pkey"] = idf["player"].astype(str).map(_norm_player_name)
    idf["_skey"] = idf["player"].astype(str).map(_short_player_key)
    if "team_tri" in idf.columns:
      idf["_tri"] = idf["team_tri"].map(_tri)
    elif "team" in idf.columns:
      idf["_tri"] = idf["team"].map(_tri)
    else:
      idf["_tri"] = ""

    for _, r in idf.iterrows():
      pk = str(r.get("_pkey") or "").strip().upper()
      sk = str(r.get("_skey") or "").strip().upper()
      tri = str(r.get("_tri") or "").strip().upper()
      if not pk and not sk:
        continue
      if tri:
        if pk:
          ban_pairs.add((pk, tri))
        if sk:
          ban_short_pairs.add((sk, tri))
      else:
        # If the injury row has no team, fall back to name-only.
        if pk:
          ban_names.add(pk)
        if sk:
          ban_short_names.add(sk)

if ban_pairs or ban_short_pairs or ban_names or ban_short_names:
  pdf = pdf.copy()
  pdf["_pkey"] = pdf.get("player_name").astype(str).map(_norm_player_name)
  pdf["_skey"] = pdf.get("player_name").astype(str).map(_short_player_key)
  # props_predictions 'team' is expected to be a 3-letter tricode.
  pdf["_tri"] = pdf.get("team").astype(str).map(_tri)
  # Pair-wise bans (team-aware)
  p_pairs = list(zip(pdf["_pkey"].astype(str).tolist(), pdf["_tri"].astype(str).tolist()))
  s_pairs = list(zip(pdf["_skey"].astype(str).tolist(), pdf["_tri"].astype(str).tolist()))
  bad_pair = pd.Series([p in ban_pairs for p in p_pairs], index=pdf.index)
  bad_short_pair = pd.Series([p in ban_short_pairs for p in s_pairs], index=pdf.index)
  bad_name_only = pdf["_pkey"].isin(ban_names) | pdf["_skey"].isin(ban_short_names)
  mask = ~(bad_pair | bad_short_pair | bad_name_only)
  pdf = pdf[mask].drop(columns=["_pkey","_skey","_tri"], errors="ignore")
after = len(pdf)
pdf.to_csv(preds_path, index=False)
print(f"FILTERED:{before}->{after}")
'@
  $env:PP = $ppPath
  $env:INJ = $injExclPath
  $env:SLATE_DATE = $Date
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
# Apply probability calibration (uses last saved curve; loader validates sanity)
$rc4a = Invoke-PyMod -plist @('-m','nba_betting.cli','props-edges','--date', $Date, '--source','oddsapi','--mode','current','--file-only','--calibrate-prob')
Write-Log ("props-edges (oddsapi, mode=current) exit code: {0}" -f $rc4a)

# 6) Export recommendations CSVs for site consumption
# 6a) Game recommendations from predictions + odds
$rc5 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-recommendations','--date', $Date)
Write-Log ("export-recommendations exit code: {0}" -f $rc5)
# 6b) High-confidence picks (blended scoring) for games
try {
  Write-Log ("Generating high-confidence picks for {0}" -f $Date)
  $rc5b = Invoke-PyMod -plist @(
    '-m','nba_betting.cli','recommend-picks',
    '--date', $Date,
    '--topN','10',
    '--minScore','0.15',
    '--minAtsEdge','0.05',
    '--minAtsEV','0.00',
    '--atsBlend','0.25',
    '--minTotalEdge','0.02',
    '--minTotalEV','0.00',
    '--totalsBlend','0.10'
  )
  Write-Log ("recommend-picks exit code: {0}" -f $rc5b)
} catch {
  Write-Log ("recommend-picks failed (non-fatal): {0}" -f $_.Exception.Message)
}
# 6c) Props recommendations
$rc6 = Invoke-PyMod -plist @('-m','nba_betting.cli','export-props-recommendations','--date', $Date)
Write-Log ("export-props-recommendations exit code: {0}" -f $rc6)

# 6d) Export authoritative best-edges snapshots (games + props) for tracking/UI
try {
  Write-Log ("Exporting best-edges snapshots for {0}" -f $Date)
  $rc6d = Invoke-PyMod -plist @('-m','nba_betting.cli','export-best-edges','--date', $Date, '--overwrite')
  Write-Log ("export-best-edges exit code: {0}" -f $rc6d)
} catch {
  Write-Log ("export-best-edges failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 6e) Purge cached summary so endpoints cannot serve stale results
try {
  $sumCache = Join-Path $RepoRoot 'data/processed/recommendations_summary.json'
  if (Test-Path $sumCache) {
    Remove-Item $sumCache -Force -ErrorAction SilentlyContinue
    Write-Log 'Purged recommendations_summary.json cache'
  } else {
    Write-Log 'No recommendations_summary.json cache to purge'
  }
} catch {
  Write-Log ("Summary cache purge failed (non-fatal): {0}" -f $_.Exception.Message)
}

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

def _regular_price(pr):
  try:
    if pr is None or (isinstance(pr, float) and math.isnan(pr)):
      return False
    v = float(pr)
    return (-150.0 <= v <= 150.0)
  except Exception:
    return False

def _choose_top_play(plays):
  if not isinstance(plays, list) or not plays:
    return None
  # Prefer regular-priced plays, fallback to any
  regular = [p for p in plays if _regular_price(p.get("price"))]
  cand = regular if regular else plays

  def eligible(p):
    mkt = str(p.get("market") or "").lower()
    if mkt in {"pts", "pra"}:
      edge = p.get("edge")
      try:
        return edge is not None and abs(float(edge)) >= 0.15
      except Exception:
        return False
    return True

  # Prefer core markets when available (avoid pts/pra unless very strong)
  core = {"reb", "ra", "ast"}
  core_cand = [p for p in cand if str(p.get("market") or "").lower() in core and eligible(p)]
  elig = [p for p in cand if eligible(p)]
  cand = core_cand if core_cand else (elig if elig else cand)

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
        reasons.append("Regular price range (-150 to +150)")
    except Exception:
      pass
  # Consensus: same market/side across books (prefer regular-priced set)
  mk = str(tp.get("market") or "").lower()
  side = str(tp.get("side") or "").upper()
  same_all = [p for p in (plays or []) if str(p.get("market") or "").lower() == mk and str(p.get("side") or "").upper() == side]
  same_regular = [p for p in same_all if _regular_price(p.get("price"))]
  same = same_regular if same_regular else same_all
  distinct_books = sorted(list({str(p.get("book") or "").lower() for p in same if p.get("book") is not None}))
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

# 7.15) Optional refinement: build interval band calibration (p10/p90 widening) from recent finals.
# Runs BEFORE SmartSim so today's interval ladders can use the latest calibration.
try {
  $doIntervalBandCalib = $env:DAILY_INTERVAL_BAND_CALIB
  if ($null -eq $doIntervalBandCalib -or $doIntervalBandCalib -eq '') { $doIntervalBandCalib = '0' }
  if ($doIntervalBandCalib -match '^(1|true|yes)$') {
    $endEval = (Get-Date $Date).AddDays(-1).ToString('yyyy-MM-dd')
    $startEval = (Get-Date $Date).AddDays(-7).ToString('yyyy-MM-dd')
    Write-Log ("Building interval actuals + evaluation + band calibration (start={0}, end={1})" -f $startEval, $endEval)

    $rcAct = Invoke-PyMod -plist @('tools/build_interval_actuals_from_pbp_espn.py','--start', $startEval, '--end', $endEval)
    Write-Log ("build_interval_actuals_from_pbp_espn exit code: {0}" -f $rcAct)

    $rcEval = Invoke-PyMod -plist @('tools/evaluate_intervals.py','--start', $startEval, '--end', $endEval, '--use-pbp-only')
    Write-Log ("evaluate_intervals exit code: {0}" -f $rcEval)

    $rcCal = Invoke-PyMod -plist @('tools/build_intervals_band_calibration.py','--start', $startEval, '--end', $endEval)
    Write-Log ("build_intervals_band_calibration exit code: {0}" -f $rcCal)
  } else {
    Write-Log 'Skipping interval band calibration (set DAILY_INTERVAL_BAND_CALIB=1 to enable)'
  }
} catch {
  Write-Log ("Interval band calibration step failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 7.2) SmartSim distributions for today's slate (writes per-game smart_sim_<date>_<HOME>_<AWAY>.json)
try {
  $skipSmart = $env:DAILY_SKIP_SMARTSIM
  if ($null -eq $skipSmart -or $skipSmart -notmatch '^(1|true|yes)$') {
    # Generate team advanced stats priors (pace/ratings) as-of today to avoid any future leakage.
    try {
      $dts = [datetime]::Parse($Date)
      $seasonY = if ($dts.Month -ge 7) { $dts.Year + 1 } else { $dts.Year }
      Write-Log ("Updating team advanced stats priors (season={0}, as_of={1})" -f $seasonY, $Date)
      $rcAdv = Invoke-PyMod -plist @('-m','nba_betting.cli','fetch-advanced-stats','--season', $seasonY, '--as-of', $Date)
      Write-Log ("fetch-advanced-stats exit code: {0}" -f $rcAdv)
    } catch {
      Write-Log ("fetch-advanced-stats failed (non-fatal): {0}" -f $_.Exception.Message)
    }

    $nSmart = $env:DAILY_SMARTSIM_NSIMS
    if ($null -eq $nSmart -or $nSmart -eq '') { $nSmart = '300' }
    $maxSmart = $env:DAILY_SMARTSIM_MAX_GAMES
    $plist = @('-m','nba_betting.cli','smart-sim-date','--date', $Date, '--n-sims', $nSmart, '--overwrite')
    if ($null -ne $maxSmart -and $maxSmart -ne '') { $plist += @('--max-games', $maxSmart) }
    Write-Log ("Running SmartSim slate for {0} (n_sims={1})" -f $Date, $nSmart)
    $rcSmart = Invoke-PyMod -plist $plist
    Write-Log ("smart-sim-date exit code: {0}" -f $rcSmart)
  } else {
    Write-Log 'Skipping smart-sim-date (DAILY_SKIP_SMARTSIM=1)'
  }
} catch {
  Write-Log ("smart-sim-date failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 8) End-to-end artifact validation (writes data/processed/daily_artifacts_<date>.json)
try {
  $fail = $env:DAILY_FAIL_ON_MISSING_ARTIFACTS
  if ($null -eq $fail -or $fail -eq '') { $fail = '1' }
  $reqOdds = $env:DAILY_REQUIRE_ODDS
  if ($null -eq $reqOdds -or $reqOdds -eq '') { $reqOdds = '1' }
  $reqSmart = $env:DAILY_REQUIRE_SMARTSIM
  if ($null -eq $reqSmart -or $reqSmart -eq '') {
    # If SmartSim was intentionally skipped, don't require SmartSim artifacts by default.
    $skipSmartNow = $env:DAILY_SKIP_SMARTSIM
    if ($null -ne $skipSmartNow -and $skipSmartNow -match '^(1|true|yes)$') { $reqSmart = '0' } else { $reqSmart = '1' }
  }
  $reqRot = $env:DAILY_REQUIRE_ROTATIONS_ESPN
  if ($null -eq $reqRot -or $reqRot -eq '') { $reqRot = '1' }

    $env:REPO_ROOT = $RepoRoot
    $env:FAIL_ON_MISSING = $fail
    $env:REQUIRE_ODDS = $reqOdds
    $env:REQUIRE_SMARTSIM = $reqSmart
    $env:REQUIRE_ROTATIONS = $reqRot
    if ($null -eq $env:ROTATIONS_MIN_COVERAGE -or $env:ROTATIONS_MIN_COVERAGE -eq '') { $env:ROTATIONS_MIN_COVERAGE = '0.70' }
    Write-Log 'Validating daily artifacts (predictions/props/odds/smart_sim)'
    $outV = Invoke-PyMod -plist @(
    'tools/validate_daily_artifacts.py',
    '--repo-root', $RepoRoot,
    '--date', $Date,
    '--yesterday', $yesterday,
    '--rotations-min-coverage', $env:ROTATIONS_MIN_COVERAGE
    )
  if ($LASTEXITCODE -ne 0) {
    Write-Log ("Daily artifact validation failed (exit={0})" -f $LASTEXITCODE)
    if ($fail -match '^(1|true|yes)$') { throw "daily artifacts missing" }
  } else {
    Write-Log 'Daily artifact validation OK'
  }
} catch {
  Write-Log ("Daily artifact validation block failed: {0}" -f $_.Exception.Message)
}

# 8.1) Player availability audits (fail loudly)
# - Ensures SmartSim JSON includes all expected props_predictions players
# - Ensures no stale injury exclusions conflict with playing_today
try {
  $skipAud = $env:DAILY_SKIP_PLAYER_AUDITS
  if ($null -eq $skipAud -or $skipAud -notmatch '^(1|true|yes)$') {
    Write-Log ("Running player audits for {0}" -f $Date)
    $rcCov = Invoke-PyMod -plist @('tools/audit_smart_sim_player_coverage.py','--date', $Date)
    Write-Log ("audit_smart_sim_player_coverage exit code: {0}" -f $rcCov)
    if ($rcCov -ne 0) { throw "SmartSim player coverage audit failed (exit=$rcCov)" }

    $rcMin = Invoke-PyMod -plist @('tools/audit_smart_sim_minutes.py','--date', $Date)
    Write-Log ("audit_smart_sim_minutes exit code: {0}" -f $rcMin)
    if ($rcMin -ne 0) { throw "SmartSim minutes audit failed (exit=$rcMin)" }

    $rcStale = Invoke-PyMod -plist @('tools/audit_stale_exclusions_today.py','--date', $Date)
    Write-Log ("audit_stale_exclusions_today exit code: {0}" -f $rcStale)
    if ($rcStale -ne 0) { throw "Stale exclusions audit failed (exit=$rcStale)" }

    $rcInjC = Invoke-PyMod -plist @('tools/audit_injuries_counts_consistency.py','--date', $Date)
    Write-Log ("audit_injuries_counts_consistency exit code: {0}" -f $rcInjC)
    if ($rcInjC -ne 0) { throw "injuries_counts consistency audit failed (exit=$rcInjC)" }
  } else {
    Write-Log 'Skipping player audits (DAILY_SKIP_PLAYER_AUDITS=1)'
  }
} catch {
  Write-Log ("Player audits failed: {0}" -f $_.Exception.Message)
  throw
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
