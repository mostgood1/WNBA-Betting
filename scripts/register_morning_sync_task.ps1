param(
  [string]$TaskName = 'WNBA-Betting Morning Sync',
  [string]$Time = '09:45',
  [switch]$RunWhenLoggedOff
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $root '..')).Path
$scriptPath = Join-Path $root 'run_morning_sync.ps1'
if (-not (Test-Path $scriptPath)) { throw "run_morning_sync.ps1 not found at $scriptPath" }

$psExe = (Get-Command powershell.exe).Source
$psArgs = @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$scriptPath`"")
$argLine = $psArgs -join ' '

$action = New-ScheduledTaskAction -Execute $psExe -Argument $argLine -WorkingDirectory $repoRoot

try {
  $hh,$mm = $Time.Split(':')
  $hour = [int]$hh; $min = [int]$mm
} catch { throw "Invalid -Time '$Time' (expected HH:mm)" }

$trigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::Today.AddHours($hour).AddMinutes($min))

if ($RunWhenLoggedOff) {
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U
} else {
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
}
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings

try {
  if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false | Out-Null
  }
} catch {}

Register-ScheduledTask -TaskName $TaskName -InputObject $task | Out-Null
Write-Host "Registered scheduled task '$TaskName' to run daily at $Time (RunWhenLoggedOff=$($RunWhenLoggedOff.IsPresent))"
Write-Host "Action: $psExe $argLine"
Write-Host "WorkingDirectory: $repoRoot"
