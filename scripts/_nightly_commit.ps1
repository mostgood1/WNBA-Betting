param()
Push-Location "C:\Users\mostg\OneDrive\Coding\WNBA-Betting"
$Date = (Get-Date -Format 'yyyy-MM-dd')
& "C:\Users\mostg\OneDrive\Coding\WNBA-Betting\scripts\commit_processed.ps1" -Date $Date -IncludeJson -Push
Pop-Location
