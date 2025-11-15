param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End,
  [double]$MaeThreshold = 0.25,
  [double]$RmseThreshold = 0.25
)
$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$healthPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_health_{0}_{1}.json" -f $Start, $End)
if (-not (Test-Path $healthPath)) { Write-Host "HEALTH_NOT_FOUND:$healthPath"; exit 1 }
try { $health = Get-Content -Path $healthPath -Raw | ConvertFrom-Json } catch { Write-Host "INVALID_HEALTH_JSON"; exit 1 }
$worse = @()
foreach ($kv in $health.by_stat.PSObject.Properties) {
  $stat = $kv.Name
  $d_mae = [double]$kv.Value.delta_mae
  $d_rmse = [double]$kv.Value.delta_rmse
  if (($d_mae -gt $MaeThreshold) -or ($d_rmse -gt $RmseThreshold)) {
    $worse += @{ stat=$stat; delta_mae=$d_mae; delta_rmse=$d_rmse }
  }
}
$alert = [ordered]@{
  start = $Start
  end = $End
  worsened_over_threshold = $worse
  mae_threshold = $MaeThreshold
  rmse_threshold = $RmseThreshold
  ok = ($worse.Count -eq 0)
}
$out = Join-Path $RepoRoot ("data/processed/props_eval_compare_alert_{0}_{1}.json" -f $Start, $End)
($alert | ConvertTo-Json -Depth 5) | Out-File -FilePath $out -Encoding UTF8
Write-Host ("WROTE: {0}" -f $out)
