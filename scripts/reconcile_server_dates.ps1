param(
  [string[]]$Dates
)

$ErrorActionPreference = 'Stop'
if (-not $Dates -or $Dates.Length -eq 0) {
  Write-Host 'Provide -Dates list'; exit 1
}

# Resolve repo root and local Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }

foreach($d in $Dates) {
  try {
    Write-Host ("Reconcile games locally for date=" + $d)
    & $Python -m nba_betting.cli reconcile-date --date $d 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Write-Host ("OK " + $d) } else { Write-Host ("ERR " + $d + " exit=" + $LASTEXITCODE) }
  } catch {
    Write-Host ('ERR ' + $d + ': ' + $_.Exception.Message)
  }
}
