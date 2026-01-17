param(
    [Parameter(Mandatory=$false)][string]$Date = (Get-Date -Format 'yyyy-MM-dd')
)

$ErrorActionPreference = 'Stop'

$Workspace = (Get-Location).Path
$PyExe = Join-Path $Workspace '.venv\Scripts\python.exe'

if (-not (Test-Path $PyExe)) { throw "Python venv not found: $PyExe" }

Write-Host ("[nightly-metrics] Running metrics for {0}" -f $Date)

# Ensure output directory exists
$OutDir = Join-Path $Workspace 'data\processed\metrics'
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }

# 1) Alignment report
try {
  & $PyExe (Join-Path $Workspace 'tools\simulation_alignment_report.py') --date $Date --windows '30,60,90' --outdir $OutDir
} catch { Write-Warning ("alignment_report failed: {0}" -f $_.Exception.Message) }

# 2) Data validation
try {
  & $PyExe (Join-Path $Workspace 'tools\validate_simulated_data.py') --outdir $OutDir
} catch { Write-Warning ("validate_simulated_data failed: {0}" -f $_.Exception.Message) }

# 3) Drift monitor (games)
try {
  & $PyExe (Join-Path $Workspace 'tools\drift_monitor.py') --date $Date --ref-days 30 --cur-days 7
} catch { Write-Warning ("drift_monitor failed: {0}" -f $_.Exception.Message) }

# 4) Sensitivity sweep (requires API alive)
try {
  & $PyExe (Join-Path $Workspace 'tools\sensitivity_sweep_corr.py') --base-url 'http://127.0.0.1:5051' --date $Date --scales '0.0,0.5,1.0,1.5,2.0' --outdir $OutDir
} catch { Write-Warning ("sensitivity_sweep failed (likely server offline): {0}" -f $_.Exception.Message) }

Write-Host '[nightly-metrics] Complete'
