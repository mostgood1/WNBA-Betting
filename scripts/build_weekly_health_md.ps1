param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End
)
$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$healthPath = Join-Path $RepoRoot "data/processed/props_eval_compare_health_${Start}_${End}.json"
$alertPath = Join-Path $RepoRoot "data/processed/props_eval_compare_alert_${Start}_${End}.json"
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
$lines += "Range: $Start .. $End"
$lines += ""
$lines += "## Summary"
if ($improved -and $improved.Trim() -and $improved.Trim().Length -gt 0) {
  $lines += "- Improved: $improved"
} else {
  $lines += "- Improved: (none)"
}
if ($worsened -and $worsened.Trim() -and $worsened.Trim().Length -gt 0) {
  $lines += "- Worsened: $worsened"
} else {
  $lines += "- Worsened: (none)"
}
if ($flat -and $flat.Trim() -and $flat.Trim().Length -gt 0) {
  $lines += "- Flat: $flat"
} else {
  $lines += "- Flat: (none)"
}
$lines += ""
$lines += "## Per-stat deltas (negative = improvement)"
$lines += ""
foreach ($kv in $health.by_stat.PSObject.Properties) {
  $stat = $kv.Name
  $d_mae = [double]$kv.Value.delta_mae
  $d_rmse = [double]$kv.Value.delta_rmse
  $d_mae_str = ([double]$d_mae).ToString("N3")
  $d_rmse_str = ([double]$d_rmse).ToString("N3")
  $lines += "- $($stat): delta_mae=$d_mae_str, delta_rmse=$d_rmse_str"
}
if ($alert) {
  $lines += ""
  $lines += "## Alerts"
  if ($alert.ok) {
    $lines += "- OK: No stats worsened beyond thresholds."
  } else {
    $lines += "- Thresholds: MAE>$($alert.mae_threshold), RMSE>$($alert.rmse_threshold)"
    foreach ($w in $alert.worsened_over_threshold) {
  $w_mae = ([double]$w.delta_mae).ToString("N3")
  $w_rmse = ([double]$w.delta_rmse).ToString("N3")
      $lines += "  - $($w.stat): delta_mae=$w_mae, delta_rmse=$w_rmse"
    }
  }
}

$out = Join-Path $RepoRoot "data/processed/props_eval_compare_health_${Start}_${End}.md"
Set-Content -Path $out -Value ($lines -join "`r`n") -Encoding UTF8
Write-Host "WROTE: $out"
