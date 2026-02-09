param(
    [Parameter(Mandatory=$false)][string]$Time = "04:30",
    [Parameter(Mandatory=$false)][int]$LookbackDays = 7,
    [Parameter(Mandatory=$false)][double]$BetThreshold = 6.0,
    [Parameter(Mandatory=$false)][int]$MinBets = 25
)

# Registers a Windows Scheduled Task to run the Live Lens tuning optimizer nightly
# after games have finished. Writes data/processed/live_lens_tuning_override.json.

$TaskName = "NBA-Betting - Nightly Live Lens Tune"
$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"
$ScriptPath = Join-Path $Workspace "tools\daily_live_lens_tune.py"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $ScriptPath)) { throw "Tune script missing: $ScriptPath" }

$ArgStr = "$ScriptPath --lookback-days $LookbackDays --bet-threshold $BetThreshold --min-bets $MinBets --write-override"

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
        $runner = Join-Path $Workspace "scripts\_run_live_lens_tune.ps1"
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
