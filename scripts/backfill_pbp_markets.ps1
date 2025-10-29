Param(
  [string]$Start,
  [string]$End,
  [switch]$GitPush
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# Python resolution
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }
$env:PYTHONPATH = (Join-Path $RepoRoot 'src')

# Determine defaults if not provided
$today = Get-Date
if (-not $End) { $End = $today.ToString('yyyy-MM-dd') }
if (-not $Start) {
  $yr = $today.Year
  if ($today.Month -lt 7) { $yr = $yr - 1 }
  $Start = (Get-Date -Year $yr -Month 10 -Day 1).ToString('yyyy-MM-dd')
}

Write-Host "Backfill PBP markets: $Start .. $End"
$dates = @()
try {
  $s = Get-Date $Start
  $e = Get-Date $End
  if ($e -lt $s) { throw "End must be >= Start" }
  for ($d=$s; $d -le $e; $d=$d.AddDays(1)) { $dates += $d.ToString('yyyy-MM-dd') }
} catch { Write-Error $_.Exception.Message; exit 1 }

$fail = @(); $ok = 0
foreach ($d in $dates) {
  try {
    Write-Host "Predicting PBP markets for $d"
    & $Python -m nba_betting.cli predict-pbp-markets --date $d
    if ($LASTEXITCODE -ne 0) { throw "ExitCode=$LASTEXITCODE" }
    $ok++
  } catch {
    Write-Warning ("Failed for {0}: {1}" -f $d, $_.Exception.Message); $fail += $d
  }
}

Write-Host "Done. Days attempted=$($dates.Count) ok=$ok fail=$($fail.Count)"
if ($fail.Count -gt 0) { Write-Host ("Failures: " + ($fail -join ', ')) }

if ($GitPush) {
  try {
    $commitScript = Join-Path $RepoRoot 'scripts/commit_processed.ps1'
    if (Test-Path $commitScript) {
      foreach ($d in $dates) {
        & powershell -NoProfile -ExecutionPolicy Bypass -File $commitScript -Date $d -IncludeJson | Out-Null
      }
      # push once at the end for efficiency
      & git pull --rebase
      & git push
    } else {
      & git add -- data data\processed
      $msg = "backfill pbp markets: $Start..$End"
      & git commit -m $msg
      & git pull --rebase
      & git push
    }
  } catch { Write-Warning ("Git push failed: {0}" -f $_.Exception.Message) }
}
