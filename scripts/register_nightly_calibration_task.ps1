param(
    [Parameter(Mandatory=$false)][string]$Time = "23:15",
    [switch]$GitPush
)

# Registers a Windows Scheduled Task to run multi-window props calibration nightly.
# Produces data/processed/props_prob_calibration_{30,60,90}.json and props_prob_calibration_windows.json.

$TaskName = "NBA-Betting - Nightly Props Calibration"
$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"
$ScriptPath = Join-Path $Workspace "tools\calibrate_props_probability_multi.py"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $ScriptPath)) { throw "Calibration script missing: $ScriptPath" }

$Action = New-ScheduledTaskAction -Execute $PyExe -Argument $ScriptPath -WorkingDirectory $Workspace
$Trigger = New-ScheduledTaskTrigger -Daily -At ([DateTime]::Parse($Time))
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

try {
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal -Force | Out-Null
    Write-Host "✅ Registered task: $TaskName at $Time"
} catch {
    Write-Warning ("Register-ScheduledTask failed: " + $_.Exception.Message)
    # Fallback: attempt registration via schtasks for current user (no elevation)
    try {
        $t24 = ([DateTime]::Parse($Time)).ToString('HH:mm')
        # Build a simple PowerShell command string; avoid semicolons that schtasks may misparse.
        # Use -File to avoid quoting complexities
        $runner = Join-Path $Workspace "scripts\_run_calibration.ps1"
        $scriptContent = "param`nPush-Location `"$Workspace`"`n& `"$PyExe`" `"$ScriptPath`"`nPop-Location"
        Set-Content -Path $runner -Encoding UTF8 -Value $scriptContent
        $psCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$runner`""
        schtasks.exe /Create /TN "$TaskName" /SC DAILY /ST $t24 /TR "$psCmd" /F | Out-Null
        Write-Host "✅ Registered via schtasks: $TaskName at $t24"
    } catch {
        Write-Error ("schtasks registration failed: " + $_.Exception.Message)
        throw
    }
}

if ($GitPush) {
    try {
        # Optional: push processed artifacts after calibration run completes
        $ps = Join-Path $Workspace "scripts\commit_processed.ps1"
        if (Test-Path $ps) {
            Write-Host "Info: You can add a separate task to run commit_processed.ps1 nightly after calibration."
        }
    } catch {}
}
