param(
    [Parameter(Mandatory=$false)][string]$Time = "23:35"
)

# Registers a Windows Scheduled Task to run nightly metrics (alignment, validation, drift, sensitivity).

$TaskName = "NBA-Betting - Nightly Metrics"
$Workspace = (Get-Location).Path
$Runner = Join-Path $Workspace "scripts\run_nightly_metrics.ps1"
$PsExe = (Get-Command powershell).Source

if (-not (Test-Path $Runner)) { throw "Runner script missing: $Runner" }

$Action = New-ScheduledTaskAction -Execute $PsExe -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`"" -WorkingDirectory $Workspace
$Trigger = New-ScheduledTaskTrigger -Daily -At ([DateTime]::Parse($Time))
$Principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Highest

try {
    Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Principal $Principal -Force | Out-Null
    Write-Host "✅ Registered task: $TaskName at $Time"
} catch {
    Write-Warning ("Register-ScheduledTask failed: " + $_.Exception.Message)
    try {
        $t24 = ([DateTime]::Parse($Time)).ToString('HH:mm')
        $psCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
        schtasks.exe /Create /TN "$TaskName" /SC DAILY /ST $t24 /TR "$psCmd" /F | Out-Null
        Write-Host "✅ Registered via schtasks: $TaskName at $t24"
    } catch {
        Write-Error ("schtasks registration failed: " + $_.Exception.Message)
        throw
    }
}
