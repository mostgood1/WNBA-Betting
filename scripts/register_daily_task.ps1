param(
  [string]$TaskName = 'WNBA-Betting Daily Update',
  [string]$Time = '10:00',   # local time HH:mm
  [switch]$GitPush,
  # If set, registers task to run even when user is logged off (S4U). Requires 'Log on as a batch job' right.
  [switch]$RunWhenLoggedOff
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $root '..')).Path
$scriptPath = Join-Path $root 'daily_update.ps1'
if (-not (Test-Path $scriptPath)) { throw "daily_update.ps1 not found at $scriptPath" }

# Build the action to invoke PowerShell with our script
$psExe = (Get-Command powershell.exe).Source
$psArgs = @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$scriptPath`"")
$psArgs += '-GitSyncFirst'
if ($GitPush) { $psArgs += '-GitPush' }
$argLine = $psArgs -join ' '

$action = New-ScheduledTaskAction -Execute $psExe -Argument $argLine -WorkingDirectory $repoRoot

# Daily trigger at specified time
try {
  $hh,$mm = $Time.Split(':')
  $hour = [int]$hh; $min = [int]$mm
} catch { throw "Invalid -Time '$Time' (expected HH:mm)" }

# Create a daily trigger at the given time (local). Note: first run occurs at the next occurrence of this time.
$trigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::Today.AddHours($hour).AddMinutes($min))

# Principal: by default runs only when user is logged on. If -RunWhenLoggedOff is provided, use S4U to allow headless runs.
if ($RunWhenLoggedOff) {
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U
} else {
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
}
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings

# Register or update
try {
  if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false | Out-Null
  }
} catch {}

Register-ScheduledTask -TaskName $TaskName -InputObject $task | Out-Null
Write-Host "Registered scheduled task '$TaskName' to run daily at $Time (GitPush=$($GitPush.IsPresent), RunWhenLoggedOff=$($RunWhenLoggedOff.IsPresent))"
Write-Host "Action: $psExe $argLine"
Write-Host "WorkingDirectory: $repoRoot"
