param(
  [int]$Days = 14,
  [switch]$GitPush,
  [switch]$NoSlateOnly
)

$ErrorActionPreference = 'Stop'

# Resolve repo root and Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot
$Python = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $Python)) { $Python = 'python' }
$env:PYTHONPATH = Join-Path $RepoRoot 'src'

# Compute date range: trailing Days ending yesterday
$end = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
$start = (Get-Date).AddDays(-$Days).ToString('yyyy-MM-dd')
Write-Host "Running props calibration compare: $start .. $end" -ForegroundColor Cyan

# Build CLI args
$opts = @('-m','nba_betting.cli','evaluate-props-calibration-compare','--start', $start,'--end',$end)
if ($NoSlateOnly) { $opts += '--no-slate-only' }

# Run evaluator (tolerate stderr warnings from native tools like cpuinfo/onnxruntime)
$logPath = Join-Path $RepoRoot "logs\eval_compare_$((Get-Date).ToString('yyyyMMdd_HHmmss')).log"
$prevErr = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& $Python @($opts) 2>&1 | Tee-Object -FilePath $logPath -Append | Out-Null
$code = $LASTEXITCODE
$ErrorActionPreference = $prevErr
Write-Host ("Evaluator exit: {0} (log: {1})" -f $code, $logPath)

# Post-process: build a simple health summary JSON (improvements vs regressions)
try {
  $summaryPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_summary_{0}_{1}.csv" -f $start, $end)
  if (Test-Path $summaryPath) {
    $rows = Import-Csv -Path $summaryPath
    $improved = @(); $worsened = @(); $flat = @()
    $byStat = @{}
    foreach ($r in $rows) {
      $stat = $r.stat
      $d_mae = [double]$r.delta_mae
      $d_rmse = [double]$r.delta_rmse
      $entry = @{ stat=$stat; delta_mae=$d_mae; delta_rmse=$d_rmse }
      if ($d_mae -lt 0 -or $d_rmse -lt 0) { $improved += $stat }
      elseif ($d_mae -gt 0 -or $d_rmse -gt 0) { $worsened += $stat }
      else { $flat += $stat }
      $byStat[$stat] = $entry
    }
    $health = [ordered]@{
      start = $start
      end = $end
      improved = $improved
      worsened = $worsened
      flat = $flat
      by_stat = $byStat
      note = 'Negative deltas indicate improvement (player < global)'
    }
    $healthPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_health_{0}_{1}.json" -f $start, $end)
    ($health | ConvertTo-Json -Depth 5) | Out-File -FilePath $healthPath -Encoding UTF8
    Write-Host ("Wrote health summary -> {0}" -f $healthPath)
  } else {
    Write-Host "No summary CSV found; skipping health summary" -ForegroundColor Yellow
  }
} catch {
  Write-Host ("Health summary build failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
}

# Build a simple alert JSON if any stats worsen beyond thresholds (defaults)
try {
  & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $RepoRoot 'scripts\build_eval_alert.ps1') -Start $start -End $end 2>&1 | Out-Host
} catch {
  Write-Host ("Alert build failed: {0}" -f $_.Exception.Message) -ForegroundColor Yellow
}

# Optionally push artifacts: include eval files
if ($GitPush) {
  $commit = Join-Path $RepoRoot 'scripts\commit_processed.ps1'
  if (Test-Path $commit) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File $commit -Date $end -Push -IncludeEval 2>&1 | Out-Host
  }
}
