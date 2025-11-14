param(
  [int]$Days = 14,
  [switch]$GitPush,
  [switch]$NoSlateOnly
)

$ErrorActionPreference = 'Stop'

# Resolve repo root and Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }
$env:PYTHONPATH = Join-Path $RepoRoot 'src'

# Compute date range: trailing Days ending yesterday
$end = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
$start = (Get-Date).AddDays(-$Days).ToString('yyyy-MM-dd')
Write-Host "Running props calibration compare: $start .. $end" -ForegroundColor Cyan

# Build CLI args
$opts = @('-m','nba_betting.cli','evaluate-props-calibration-compare','--start', $start,'--end',$end)
if ($NoSlateOnly) { $opts += '--no-slate-only' }

# Run evaluator
& $Python @($opts) 2>&1 | Tee-Object -FilePath (Join-Path $RepoRoot "logs\eval_compare_$((Get-Date).ToString('yyyyMMdd_HHmmss')).log") -Append | Out-Null
$code = $LASTEXITCODE
Write-Host ("Evaluator exit: {0}" -f $code)

# Optionally push artifacts: include eval files
if ($GitPush) {
  $commit = Join-Path $RepoRoot 'scripts\commit_processed.ps1'
  if (Test-Path $commit) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $commit -Date $end -Push -IncludeEval 2>&1 | Out-Host
  }
}
