Param(
  [string]$Start,
  [string]$End,
  [int]$Days = 21,
  [switch]$FinalsOnly,
  [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

# Resolve repo root and Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Python = if (Test-Path $VenvPy) { $VenvPy } else { 'python' }

function Write-Log {
  param([string]$Msg)
  if (-not $Quiet) { Write-Host ("[weekly] " + $Msg) }
}

# Determine date range
try {
  if (-not $End) { $End = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
  if (-not $Start) {
    $EndDt = [datetime]::ParseExact($End, 'yyyy-MM-dd', $null)
    $Start = $EndDt.AddDays(-1 * [math]::Max(1,$Days)).ToString('yyyy-MM-dd')
  }
} catch {
  Write-Error "Invalid Start/End date format. Use YYYY-MM-DD."; exit 1
}

Write-Log ("Weekly backfill range: {0}..{1} (finals-only={2})" -f $Start, $End, [bool]$FinalsOnly)

function Invoke-PyMod {
  param([string[]]$plist)
  & $Python @plist
  return $LASTEXITCODE
}

# 1) PBP backfill for range
$plist = @('-m','nba_betting.cli','backfill-pbp','--start', $Start, '--end', $End)
if ($FinalsOnly) { $plist += '--finals-only' }
Write-Log ("Run: python {0}" -f ($plist -join ' '))
$rc1 = Invoke-PyMod -plist $plist
Write-Log ("backfill-pbp exit code: {0}" -f $rc1)

# 1b) Build ESPN PBP period actuals (quarters/halves) for range (non-fatal)
try {
  $skipPbpAct = $env:WEEKLY_SKIP_PBP_PERIOD_ACTUALS
  if ($null -ne $skipPbpAct -and $skipPbpAct -match '^(1|true|yes)$') {
    Write-Log 'Skipping PBP period actuals build (WEEKLY_SKIP_PBP_PERIOD_ACTUALS=1)'
  } else {
    $plistAct = @('tools/build_period_actuals_from_pbp_espn.py','--start', $Start, '--end', $End)
    Write-Log ("Run: python {0}" -f ($plistAct -join ' '))
    $rcAct = Invoke-PyMod -plist $plistAct
    Write-Log ("build_period_actuals_from_pbp_espn exit code: {0}" -f $rcAct)
  }
} catch {
  Write-Log ("PBP period actuals build failed (non-fatal): {0}" -f $_.Exception.Message)
}

# 2) recon_quarters for range
$plist2 = @('-m','nba_betting.cli','reconcile-quarters-range','--start', $Start, '--end', $End)
Write-Log ("Run: python {0}" -f ($plist2 -join ' '))
$rc2 = Invoke-PyMod -plist $plist2
Write-Log ("reconcile-quarters-range exit code: {0}" -f $rc2)

# 3) smart_sim_quarter_eval + period probs calibration (non-fatal)
# These steps require SmartSim outputs + period lines to exist in data/processed.
try {
  $skipSSEval = $env:WEEKLY_SKIP_SMART_SIM_EVAL_BUILD
  if ($null -ne $skipSSEval -and $skipSSEval -match '^(1|true|yes)$') {
    Write-Log 'Skipping smart_sim_quarter_eval build (WEEKLY_SKIP_SMART_SIM_EVAL_BUILD=1)'
  } else {
    $evalDays = $env:WEEKLY_SMART_SIM_EVAL_DAYS
    if ($null -eq $evalDays -or $evalDays -eq '') { $evalDays = '60' }
    try { $evalDaysInt = [int]$evalDays } catch { $evalDaysInt = 60 }
    if ($evalDaysInt -lt 7) { $evalDaysInt = 7 }
    if ($evalDaysInt -gt 120) { $evalDaysInt = 120 }
    Write-Log ("Building smart_sim_quarter_eval (end={0}, days={1})" -f $End, $evalDaysInt)
    $plistEval = @('tools/build_smart_sim_quarter_eval.py','--end', $End, '--days', [string]$evalDaysInt)
    Write-Log ("Run: python {0}" -f ($plistEval -join ' '))
    $rcEval = Invoke-PyMod -plist $plistEval
    Write-Log ("build_smart_sim_quarter_eval exit code: {0}" -f $rcEval)
  }
} catch {
  Write-Log ("smart_sim_quarter_eval build failed (non-fatal): {0}" -f $_.Exception.Message)
}

try {
  $skipPProb = $env:WEEKLY_SKIP_PERIOD_PROBS_CALIB
  if ($null -ne $skipPProb -and $skipPProb -match '^(1|true|yes)$') {
    Write-Log 'Skipping period probability calibration (WEEKLY_SKIP_PERIOD_PROBS_CALIB=1)'
  } else {
    # Use the same anchor convention as daily_update: anchor = yesterday/end date.
    Write-Log ("Calibrating period probabilities (window=30) anchored at {0}" -f $End)
    $plistCal = @('-m','nba_betting.cli','calibrate-period-probs','--anchor', $End, '--window', '30', '--bins', '12', '--alpha', '1.0')
    Write-Log ("Run: python {0}" -f ($plistCal -join ' '))
    $rcCal = Invoke-PyMod -plist $plistCal
    Write-Log ("calibrate-period-probs exit code: {0}" -f $rcCal)
  }
} catch {
  Write-Log ("calibrate-period-probs failed (non-fatal): {0}" -f $_.Exception.Message)
}

Write-Log "Weekly backfill completed."
