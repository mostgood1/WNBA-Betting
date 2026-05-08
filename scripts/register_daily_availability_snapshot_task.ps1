param(
    [Parameter(Mandatory=$false)][string]$Time = "06:30",
    [Parameter(Mandatory=$false)][switch]$Overwrite
)

# Registers a Windows Scheduled Task to build league_status_<date>.csv and injuries_counts_<date>.json daily.
# This prevents stale data/raw/injuries.csv from creating persistent false OUT labels.

$TaskName = "WNBA-Betting - Daily Availability Snapshot"
$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"
$ScriptPath = Join-Path $Workspace "tools\daily_availability_snapshot.py"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $ScriptPath)) { throw "Script missing: $ScriptPath" }

# Build for 'today' at runtime (script defaults to today). Optional overwrite.
$ArgStr = "$ScriptPath" + ($(if($Overwrite){" --overwrite"}else{""}))

$Action = New-ScheduledTaskAction -Execute $PyExe -Argument $ArgStr -WorkingDirectory $Workspace
$Trigger = New-ScheduledTaskTrigger -Daily -At ([DateTime]::Parse($Time))
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

try {
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal -Force | Out-Null
    Write-Host "✅ Registered task: $TaskName at $Time"
} catch {
    Write-Warning ("Register-ScheduledTask failed: " + $_.Exception.Message)
    # Fallback: schtasks
    try {
        $t24 = ([DateTime]::Parse($Time)).ToString('HH:mm')
        $runner = Join-Path $Workspace "scripts\_run_daily_availability_snapshot.ps1"
        $scriptContent = "param`nPush-Location `"$Workspace`"`n& `"$PyExe`" $ArgStr`nPop-Location"
        Set-Content -Path $runner -Encoding UTF8 -Value $scriptContent
        $psCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$runner`""
        schtasks.exe /Create /TN "$TaskName" /SC DAILY /ST $t24 /TR "$psCmd" /F | Out-Null
        Write-Host "✅ Registered via schtasks: $TaskName at $t24"
    } catch {
        Write-Error ("schtasks registration failed: " + $_.Exception.Message)
        throw
    }
}
