param(
  [string]$TaskName = 'NBA-Betting - Intraday Props Refresh',
  [string]$StartTime = '10:00',
  [string]$EndTime = '23:40',
  [int]$IntervalMinutes = 20
)

$ErrorActionPreference = 'Stop'

if ($IntervalMinutes -lt 10 -or $IntervalMinutes -gt 180) {
  throw "Invalid -IntervalMinutes '$IntervalMinutes' (expected 10..180)"
}

$startDt = [DateTime]::Parse($StartTime)
$endDt = [DateTime]::Parse($EndTime)
if ($endDt -le $startDt) {
  throw "Invalid window: EndTime must be later than StartTime"
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$TargetScript = Join-Path $ScriptDir 'run_intraday_props_refresh.ps1'
$Runner = Join-Path $ScriptDir '_run_intraday_props_refresh.ps1'

if (-not (Test-Path $TargetScript)) {
  throw "run_intraday_props_refresh.ps1 not found at $TargetScript"
}

$runnerContent = @"
param()
Push-Location "$RepoRoot"
try {
  & "$TargetScript"
}
finally {
  Pop-Location
}
"@
Set-Content -Path $Runner -Encoding UTF8 -Value $runnerContent

$start24 = $startDt.ToString('HH:mm')
$end24 = $endDt.ToString('HH:mm')
$psCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$Runner`""

$out = & schtasks.exe /Create /TN "$TaskName" /SC MINUTE /MO $IntervalMinutes /ST $start24 /ET $end24 /TR "$psCmd" /F 2>&1
if ($LASTEXITCODE -ne 0) {
  Write-Host "Failed to register intraday props refresh task" -ForegroundColor Red
  if ($out) { Write-Host ($out | Out-String) }
  exit $LASTEXITCODE
}

Write-Host "Registered intraday props refresh via schtasks: $TaskName"
Write-Host "Window: $start24 -> $end24 every $IntervalMinutes minutes"