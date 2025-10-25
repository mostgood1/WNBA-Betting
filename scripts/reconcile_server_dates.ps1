param(
  [string[]]$Dates,
  [string]$BaseUrl = 'https://nba-betting-5qgf.onrender.com',
  [string]$Token
)

$ErrorActionPreference = 'Stop'
if (-not $Dates -or $Dates.Length -eq 0) {
  Write-Host 'Provide -Dates list'; exit 1
}
foreach($d in $Dates) {
  $qs = 'date=' + $d
  if ($Token) { $qs += '&token=' + $Token }
  $qs += '&push=1'
  $u = $BaseUrl + '/api/cron/reconcile-games?' + $qs
  try {
    $r = Invoke-WebRequest -UseBasicParsing -Uri $u -TimeoutSec 180
    Write-Host ('Reconcile ' + $d + ' -> ' + $r.StatusCode)
  } catch {
    Write-Host ('ERR ' + $d + ': ' + $_.Exception.Message)
  }
}
