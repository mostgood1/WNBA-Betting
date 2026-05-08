# Props Recommendations Debug Script
Write-Host "Starting Flask app..." -ForegroundColor Cyan

# Start Flask in background
$env:PORT = "5051"
$env:PYTHONPATH = "C:\Users\mostg\OneDrive\Coding\WNBA-Betting\src"
$flaskJob = Start-Job -ScriptBlock {
    param($pythonPath, $appPath, $port, $pypath)
    $env:PORT = $port
    $env:PYTHONPATH = $pypath
    & $pythonPath $appPath 2>&1
} -ArgumentList "C:\Users\mostg\OneDrive\Coding\NBA NPU\.venv-arm64\Scripts\python.exe", "C:\Users\mostg\OneDrive\Coding\WNBA-Betting\app.py", "5051", "C:\Users\mostg\OneDrive\Coding\WNBA-Betting\src"

Write-Host "Waiting for Flask to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 3

# Test the API
Write-Host "`nTesting API endpoint..." -ForegroundColor Cyan
try {
    $response = Invoke-WebRequest -Uri "http://127.0.0.1:5051/api/props-recommendations?date=2025-10-17" -UseBasicParsing
    $data = $response.Content | ConvertFrom-Json
    
    Write-Host "✅ Status: $($response.StatusCode)" -ForegroundColor Green
    Write-Host "✅ Date: $($data.date)" -ForegroundColor Green
    Write-Host "✅ Total Rows: $($data.rows)" -ForegroundColor Green
    Write-Host "✅ Player Cards: $($data.data.Count)" -ForegroundColor Green
    Write-Host "✅ Games: $($data.games.Count)" -ForegroundColor Green
    
    if ($data.data.Count -gt 0) {
        Write-Host "`nFirst 3 players:" -ForegroundColor Cyan
        $data.data[0..2] | ForEach-Object {
            Write-Host "  - $($_.player) ($($_.team)) - $($_.plays.Count) props" -ForegroundColor White
        }
    } else {
        Write-Host "`n❌ NO PLAYER CARDS RETURNED!" -ForegroundColor Red
        Write-Host "Full response:" -ForegroundColor Yellow
        Write-Host ($data | ConvertTo-Json -Depth 3)
    }
} catch {
    Write-Host "❌ Error: $_" -ForegroundColor Red
}

# Cleanup
Write-Host "`nStopping Flask..." -ForegroundColor Yellow
Stop-Job -Job $flaskJob
Remove-Job -Job $flaskJob
Write-Host "Done!" -ForegroundColor Green
