param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End
)
$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
$summaryPath = Join-Path $RepoRoot ("data/processed/props_eval_compare_summary_{0}_{1}.csv" -f $Start, $End)
if (-not (Test-Path $summaryPath)) { Write-Host "SUMMARY_NOT_FOUND:$summaryPath"; exit 1 }
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
  start = $Start
  end = $End
  improved = $improved
  worsened = $worsened
  flat = $flat
  by_stat = $byStat
  note = 'Negative deltas indicate improvement (player < global)'
}
$out = Join-Path $RepoRoot ("data/processed/props_eval_compare_health_{0}_{1}.json" -f $Start, $End)
($health | ConvertTo-Json -Depth 5) | Out-File -FilePath $out -Encoding UTF8
Write-Host ("WROTE: {0}" -f $out)
