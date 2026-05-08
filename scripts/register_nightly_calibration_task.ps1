param(
    [Parameter(Mandatory=$false)][string]$Time = "23:15",
    [int]$Days = 60,
    [int]$MinBetsPerStat = 250,
    [double]$Alpha = 0.35,
    [switch]$GitPush
)

# Registers a Windows Scheduled Task to run props probability calibration nightly.
# Produces:
#   - data/processed/props_prob_calibration_{30,60,90}.json and props_prob_calibration_windows.json
#   - data/processed/props_prob_calibration_by_stat.json

$TaskName = "WNBA-Betting - Nightly Props Calibration"
$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"
$RunnerPath = Join-Path $Workspace "scripts\run_props_calibration.ps1"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $RunnerPath)) { throw "Calibration runner missing: $RunnerPath" }

$psCmd = "-NoProfile -ExecutionPolicy Bypass -File `"$RunnerPath`" -Days $Days -MinBetsPerStat $MinBetsPerStat -Alpha $Alpha"
$Action = New-ScheduledTaskAction -Execute "powershell" -Argument $psCmd -WorkingDirectory $Workspace
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
        $psCmd2 = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$RunnerPath`" -Days $Days -MinBetsPerStat $MinBetsPerStat -Alpha $Alpha"
        schtasks.exe /Create /TN "$TaskName" /SC DAILY /ST $t24 /TR "$psCmd2" /F | Out-Null
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
