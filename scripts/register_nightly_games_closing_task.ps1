param(
    [Parameter(Mandatory=$false)][string]$Time = "01:50"
)

# Registers a Windows Scheduled Task to capture closing-ish game lines nightly.
# Produces data/processed/games_closing_lines_<date>.csv.

$TaskName = "WNBA-Betting - Nightly Games Closing Lines"
$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"
$ScriptPath = Join-Path $Workspace "tools\capture_games_closing_lines.py"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $ScriptPath)) { throw "Capture script missing: $ScriptPath" }

$Action = New-ScheduledTaskAction -Execute $PyExe -Argument $ScriptPath -WorkingDirectory $Workspace
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
        $runner = Join-Path $Workspace "scripts\_run_games_closing.ps1"
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
