param(
  [string]$TaskName = 'NBA-Betting - Intraday Props Refresh'
)

$ErrorActionPreference = 'Stop'

try {
  schtasks.exe /Delete /TN "$TaskName" /F | Out-Null
  Write-Host "Unregistered intraday props refresh: $TaskName"
} catch {
  Write-Host "Intraday props refresh task not found or already removed: $TaskName"
}

try {
  $ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
  $Runner = Join-Path $ScriptDir '_run_intraday_props_refresh.ps1'
  if (Test-Path $Runner) {
    Remove-Item $Runner -Force -ErrorAction SilentlyContinue
  }
} catch {
}