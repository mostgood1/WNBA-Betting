param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End
)
$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$healthPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_health_{0}_{1}.json" -f $Start, $End)
$alertPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_alert_{0}_{1}.json" -f $Start, $End)
if (-not (Test-Path $healthPath)) { Write-Host "HEALTH_NOT_FOUND:$healthPath"; exit 1 }
try { $health = Get-Content -Path $healthPath -Raw | ConvertFrom-Json } catch { Write-Host "INVALID_HEALTH_JSON"; exit 1 }
$alert = $null
if (Test-Path $alertPath) {
  try { $alert = Get-Content -Path $alertPath -Raw | ConvertFrom-Json } catch { $alert = $null }
}

$improved = $health.improved -join ', '
$worsened = $health.worsened -join ', '
$flat = $health.flat -join ', '

$lines = @()
$lines += "# Weekly Props Calibration Health"
$lines += ""
$lines += ("Range: {0} .. {1}" -f $Start, $End)
$lines += ""
$lines += "## Summary"
if ($improved -and $improved.Trim().Length -gt 0) { $lines += ("- Improved: {0}" -f $improved) } else { $lines += "- Improved: (none)" }
if ($worsened -and $worsened.Trim().Length -gt 0) { $lines += ("- Worsened: {0}" -f $worsened) } else { $lines += "- Worsened: (none)" }
if ($flat -and $flat.Trim().Length -gt 0) { $lines += ("- Flat: {0}" -f $flat) } else { $lines += "- Flat: (none)" }
$lines += ""
$lines += "## Per-stat deltas (negative = improvement)"
$lines += ""
foreach ($kv in $health.by_stat.PSObject.Properties) {
  $stat = $kv.Name
  $d_mae = [double]$kv.Value.delta_mae
  $d_rmse = [double]$kv.Value.delta_rmse
  $lines += ("- {0}: delta_mae={1:N3}, delta_rmse={2:N3}" -f $stat, $d_mae, $d_rmse)
}
if ($alert) {
  $lines += ""
  $lines += "## Alerts"
  if ($alert.ok) {
    $lines += "- OK: No stats worsened beyond thresholds."
  } else {
    $lines += ("- Thresholds: MAE>{0}, RMSE>{1}" -f $alert.mae_threshold, $alert.rmse_threshold)
    foreach ($w in $alert.worsened_over_threshold) {
      $lines += ("  - {0}: delta_mae={1:N3}, delta_rmse={2:N3}" -f $w.stat, [double]$w.delta_mae, [double]$w.delta_rmse)
    }
  }
}

$out = Join-Path $RepoRoot ("data/processed/props_eval_compare_health_{0}_{1}.md" -f $Start, $End)
Set-Content -Path $out -Value ($lines -join "`r`n") -Encoding UTF8
Write-Host ("WROTE: {0}" -f $out)
