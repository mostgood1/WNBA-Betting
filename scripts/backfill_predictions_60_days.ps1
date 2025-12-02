Param(
  [string]$Start = (Get-Date).AddDays(-60).ToString('yyyy-MM-dd'),
  [string]$End = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd')
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path $PSScriptRoot -Parent
$Python = Join-Path $RepoRoot '.venv/Scripts/python.exe'

function Invoke-PyMod([string[]]$plist) {
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $Python
  $psi.Arguments = ($plist -join ' ')
  $psi.WorkingDirectory = $RepoRoot
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError = $true
  $psi.UseShellExecute = $false
  $p = New-Object System.Diagnostics.Process
  $p.StartInfo = $psi
  $p.Start() | Out-Null
  $p.WaitForExit()
  return $p.ExitCode
}

Write-Host "Backfilling predictions and recon from $Start to $End"
$sd = [DateTime]::ParseExact($Start, 'yyyy-MM-dd', $null)
$ed = [DateTime]::ParseExact($End, 'yyyy-MM-dd', $null)

for ($d = $sd; $d -le $ed; $d = $d.AddDays(1)) {
  $ds = $d.ToString('yyyy-MM-dd')
  Write-Host "[Predict] $ds"
  $rcPred = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-games-npu','--date', $ds)
  Write-Host "  rc=$rcPred"
  Write-Host "[Reconcile] $ds"
  $rcRec = Invoke-PyMod -plist @('-m','nba_betting.cli','reconcile-date','--date', $ds)
  Write-Host "  rc=$rcRec"
}

Write-Host 'Done backfill.'
