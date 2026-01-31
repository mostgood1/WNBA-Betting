param(
  [string]$BaseUrl = "http://127.0.0.1:5051",
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$SkipWarmCaches
)

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }

$env:NBA_API_BASE_URL = $BaseUrl
$env:NBA_DATE = $Date

Write-Host ("[cache-warmer] date={0} base_url={1}" -f $Date, $BaseUrl)

if (-not $SkipWarmCaches) {
  & $Python (Join-Path $RepoRoot 'tools\warm_caches.py')
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} else {
  Write-Host "[cache-warmer] SkipWarmCaches=1; not calling server"
}

# Only run audits when the underlying artifacts exist. Cache warming can run before the daily update.
$propsPred = Join-Path $RepoRoot ("data\processed\props_predictions_{0}.csv" -f $Date)
if (-not (Test-Path $propsPred)) {
  Write-Host ("[cache-warmer] {0} missing; skipping audits" -f $propsPred)
  exit 0
}

& $Python (Join-Path $RepoRoot 'tools\audit_smart_sim_player_coverage.py') --date $Date
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

& $Python (Join-Path $RepoRoot 'tools\audit_stale_exclusions_today.py') --date $Date
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "[cache-warmer] audits OK"
