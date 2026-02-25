param(
    [int]$Days = 60,
    [int]$MinBetsPerStat = 250,
    [double]$Alpha = 0.35
)

$ErrorActionPreference = 'Stop'

$Workspace = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$PyExe = Join-Path $Workspace ".venv\Scripts\python.exe"

$MultiScript = Join-Path $Workspace "tools\calibrate_props_probability_multi.py"
$ByStatScript = Join-Path $Workspace "tools\calibrate_props_probability_by_stat.py"

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }
if (-not (Test-Path $MultiScript)) { throw "Calibration script missing: $MultiScript" }
if (-not (Test-Path $ByStatScript)) { throw "Calibration script missing: $ByStatScript" }

Push-Location $Workspace
try {
    & $PyExe $MultiScript
    & $PyExe $ByStatScript --days $Days --min-bets-per-stat $MinBetsPerStat --alpha $Alpha
} finally {
    Pop-Location
}
