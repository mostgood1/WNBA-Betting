param(
  [string]$Start = '2025-10-21',
  [string]$End = '',
  [switch]$GitPush
)

# Resolve repo root as the directory of this script's parent
$ScriptDir = Split-Path -Parent -Path $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent -Path $ScriptDir
Set-Location -Path $RepoRoot

# Resolve Python interpreter similar to app/_resolve_python
function Resolve-Python {
  try {
    if ($env:PYTHON -and (Test-Path $env:PYTHON)) { return $env:PYTHON }
    if ($PSVersionTable.PSEdition -eq 'Desktop') {
      # Prefer local venv on Windows
      $pyWin = Join-Path $RepoRoot '.venv/Scripts/python.exe'
      if (Test-Path $pyWin) { return $pyWin }
    }
    # Fallback to system python
    return 'python'
  } catch { return 'python' }
}

$Python = Resolve-Python

# Compute default End date = today (ET) - 0 days (inclusive), but safer to use yesterday for finals
if (-not $End -or $End -eq '') {
  try {
    $nowUtc = Get-Date
    # EST/EDT conversion isn't straightforward in PS5 without .NET tz; accept local date as reasonable default
    $End = $nowUtc.AddDays(-1).ToString('yyyy-MM-dd')
  } catch {
    $End = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
  }
}

Write-Host ("Reconciling range: {0} .. {1}" -f $Start, $End)

# Build date list inclusive
$dates = @()
try {
  $d0 = [datetime]::ParseExact($Start, 'yyyy-MM-dd', $null)
  $d1 = [datetime]::ParseExact($End, 'yyyy-MM-dd', $null)
  if ($d1 -lt $d0) { throw "End before Start" }
  $cur = $d0
  while ($cur -le $d1) {
    $dates += $cur.ToString('yyyy-MM-dd')
    $cur = $cur.AddDays(1)
  }
} catch {
  Write-Host ("Invalid date range: {0}" -f $_.Exception.Message) -ForegroundColor Red
  exit 1
}

# Run reconcile-date for each day
$ok = 0; $fail = 0
foreach ($d in $dates) {
  Write-Host ("-- reconcile {0}" -f $d)
  try {
    & $Python -m nba_betting.cli reconcile-date --date $d 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw "exit $LASTEXITCODE" }
    $ok++
  } catch {
    Write-Host ("reconcile-date failed for {0}: {1}" -f $d, $_.Exception.Message) -ForegroundColor Yellow
    $fail++
  }
}

Write-Host ("Completed. ok={0} fail={1}" -f $ok, $fail)

# Optionally stage and push in a single commit
if ($GitPush) {
  try {
    # Stage only recon files within the date range
    foreach ($d in $dates) {
      $p = Join-Path $RepoRoot ("data/processed/recon_games_{0}.csv" -f $d)
      if (Test-Path $p) { git add -- $p | Out-Null }
    }
    $changed = git diff --cached --name-only
    if ($changed) {
      $msg = "backfill: recon games $Start..$End"
      git commit -m $msg | Out-Null
      git pull --rebase | Out-Null
      git push | Out-Null
      Write-Host "Pushed: $msg"
    } else {
      Write-Host 'No recon files changed to push.'
    }
  } catch {
    Write-Host ("Git push failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
  }
}
