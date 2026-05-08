param(
    [int]$Days = 30
)

$ErrorActionPreference = 'Stop'

$python = 'C:\Users\mostg\OneDrive\Coding\WNBA-Betting\.venv\Scripts\python.exe'

Write-Host "Backfilling odds snapshots for last $Days days..."
for($i = 1; $i -le $Days; $i++){
    $d = (Get-Date).AddDays(-$i).ToString('yyyy-MM-dd')
    Write-Host "[Odds] $d"
    & $python -m nba_betting.cli odds-snapshots --date $d
}
Write-Host "Done backfilling odds snapshots."