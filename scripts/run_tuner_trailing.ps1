param(
  [int]$Days = 14,
  [switch]$GitPush,
  [switch]$NoSlateOnly,
  [string]$Criterion = 'mae'
)

$ErrorActionPreference = 'Stop'

# Resolve repo root and Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }
$env:PYTHONPATH = Join-Path $RepoRoot 'src'

# Back up existing calibration config (if any) before running tuner
try {
  $cfgPath = Join-Path $RepoRoot 'data/processed/props_player_calibration_config.json'
  if (Test-Path $cfgPath) {
    $stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
    $bak = Join-Path $RepoRoot ("data/processed/_props_player_calibration_config_backup_{0}.json" -f $stamp)
    Copy-Item -Path $cfgPath -Destination $bak -Force
    Write-Host ("Backed up current calibration config -> {0}" -f $bak)
  }
} catch { Write-Host ("Config backup skipped: {0}" -f $_.Exception.Message) -ForegroundColor Yellow }

# Run tuner over trailing days ending yesterday
$opts = @('-m','nba_betting.cli','tune-props-player-calibration','--days', [string]$Days, '--criterion', $Criterion)
if ($NoSlateOnly) { $opts += '--no-slate-only' }

Write-Host ("Running tuner: trailing {0} days (criterion={1})" -f $Days, $Criterion) -ForegroundColor Cyan
& $Python @($opts) 2>&1 | Tee-Object -FilePath (Join-Path $RepoRoot "logs\tuner_$((Get-Date).ToString('yyyyMMdd_HHmmss')).log") -Append | Out-Null
$code = $LASTEXITCODE
Write-Host ("Tuner exit: {0}" -f $code)

# Optionally push updated config JSON
if ($GitPush) {
  $commit = Join-Path $RepoRoot 'scripts\commit_processed.ps1'
  if (Test-Path $commit) {
    $yday = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
    & powershell -NoProfile -ExecutionPolicy Bypass -File $commit -Date $yday -IncludeCalibConfig -Push 2>&1 | Out-Host
  }
}
