Param(
  [string]$TaskName = 'NBA-Betting Weekly Backfill',
  [string]$DayOfWeek = 'Monday',
  [string]$Time = '09:30',
  [int]$Days = 21,
  [switch]$FinalsOnly,
  [switch]$Quiet,
  [switch]$RunWhenLoggedOff
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $root '..')).Path
$scriptPath = Join-Path $root 'weekly_backfill.ps1'
if (-not (Test-Path $scriptPath)) { throw "weekly_backfill.ps1 not found at $scriptPath" }

$psExe = (Get-Command powershell.exe).Source
$psArgs = @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$scriptPath`"",'-Days', $Days)
if ($FinalsOnly) { $psArgs += '-FinalsOnly' }
if ($Quiet) { $psArgs += '-Quiet' }
$argLine = $psArgs -join ' '

$action = New-ScheduledTaskAction -Execute $psExe -Argument $argLine -WorkingDirectory $repoRoot

# Parse day of week (case-insensitive)
try { $dow = [System.Enum]::Parse([System.DayOfWeek], [string]$DayOfWeek, $true) }
catch { throw "Invalid -DayOfWeek '$DayOfWeek' (e.g., Monday)" }

# Parse time
try { $hh,$mm = $Time.Split(':'); $hour = [int]$hh; $min = [int]$mm }
catch { throw "Invalid -Time '$Time' (expected HH:mm)" }

# First trigger time: next occurrence of the requested DayOfWeek at the given time
$startTime = [datetime]::Today.AddHours($hour).AddMinutes($min)
while ($startTime.DayOfWeek -ne $dow) { $startTime = $startTime.AddDays(1) }
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $dow -At $startTime

# Principal
if ($RunWhenLoggedOff) { $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U }
else { $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive }
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -RunOnlyIfNetworkAvailable
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings

try { if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false | Out-Null } } catch {}
Register-ScheduledTask -TaskName $TaskName -InputObject $task | Out-Null
Write-Host "Registered weekly backfill task '$TaskName' for $DayOfWeek at $Time (FinalsOnly=$($FinalsOnly.IsPresent), RunWhenLoggedOff=$($RunWhenLoggedOff.IsPresent))"
Write-Host "Action: $psExe $argLine"
Write-Host "WorkingDirectory: $repoRoot"