<#
  Local-only cron runner: executes the daily update pipeline without any tokens
  or server calls. This script delegates to scripts/daily_update.ps1.
#>
param(
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$Push
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$DailyUpdate = Join-Path $ScriptDir 'daily_update.ps1'

if (-not (Test-Path $DailyUpdate)) {
  Write-Error "daily_update.ps1 not found at $DailyUpdate"
  exit 1
}

$gitPushSwitch = $Push.IsPresent
Write-Host "Running local daily update for date=$Date (GitPush=$gitPushSwitch)"
try {
  & $DailyUpdate -Date $Date -GitPush:$gitPushSwitch
} catch {
  Write-Error $_
  exit 1
}
