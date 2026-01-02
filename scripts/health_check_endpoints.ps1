param(
    [string]$BaseUrl = 'http://127.0.0.1:5051',
    [int]$TimeoutSec = 10
)

$ErrorActionPreference = 'Stop'

function Invoke-HealthCheck {
    param(
        [string]$Url
    )
    $result = [ordered]@{
        url     = $Url
        status  = $null
        ok      = $false
        elapsed_ms = $null
        content_len = $null
        error   = $null
    }
    try {
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $r = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSec -Method GET
        $sw.Stop()
        $result.status = [int]$r.StatusCode
        $result.ok = ($r.StatusCode -eq 200)
        $result.elapsed_ms = [int]$sw.Elapsed.TotalMilliseconds
        $result.content_len = ($r.Content | Select-Object -ExpandProperty Length)
    }
    catch {
        $sw.Stop()
        $result.status = 0
        $result.ok = $false
        $result.elapsed_ms = [int]$sw.Elapsed.TotalMilliseconds
        $result.error = $_.Exception.Message
    }
    return $result
}

$today = Get-Date -Format 'yyyy-MM-dd'
$endpoints = @(
    "$BaseUrl/api/recommendations/summary",
    "$BaseUrl/api/recommendations/summary/props",
    "$BaseUrl/api/recommendations/all?date=$today&compact=1&regular_only=1",
    "$BaseUrl/api/recommendations/summary/props_calibration"
)

$results = @()
foreach ($u in $endpoints) {
    $results += (Invoke-HealthCheck -Url $u)
}

$root = "c:/Users/mostg/OneDrive/Coding/NBA-Betting"
$outDir = Join-Path $root 'data/processed/health_checks'
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$outJson = Join-Path $outDir ("health_" + $stamp + ".json")

$payload = [ordered]@{
    time = (Get-Date).ToString('s') + 'Z'
    base_url = $BaseUrl
    results = $results
}

# Extract calibration summary details if available
try {
    $calUrl = "$BaseUrl/api/recommendations/summary/props_calibration"
    $calRes = Invoke-HealthCheck -Url $calUrl
    if ($calRes.ok -and $calRes.status -eq 200) {
        $json = Invoke-WebRequest -UseBasicParsing -Uri $calUrl -TimeoutSec $TimeoutSec -Method GET | Select-Object -ExpandProperty Content | ConvertFrom-Json
        $wins = $json.windows
        if ($wins) {
            # choose best rmse among available windows
            $available = $wins | Where-Object { $_.available -eq $true -and $_.rmse_calibration -ne $null }
            if ($available) {
                $best = $available | Sort-Object -Property rmse_calibration | Select-Object -First 1
                $payload.calibration = [ordered]@{
                    version = $json.version
                    best_window_days = $best.days
                    best_rmse = $best.rmse_calibration
                    total_bets = $best.total_bets
                }
            }
        }
    }
} catch {}

# Extract applied calibration meta from unified recommendations
try {
    $recoUrl = "$BaseUrl/api/recommendations/all?date=$today&compact=1&regular_only=1"
    $recoRes = Invoke-WebRequest -UseBasicParsing -Uri $recoUrl -TimeoutSec $TimeoutSec -Method GET
    if ($recoRes.StatusCode -eq 200 -and $recoRes.Content) {
        $rj = $recoRes.Content | ConvertFrom-Json
        if ($rj.meta -and $rj.meta.props_calibration) {
            $pc = $rj.meta.props_calibration
            $payload.calibration_applied = [ordered]@{
                source = $pc.props_prob_calibration_source
                selected_via = $pc.selected_via
                window_days = $pc.window_days
            }
        }
    }
} catch {}

$payload | ConvertTo-Json -Depth 5 | Set-Content -Path $outJson -Encoding UTF8
Write-Host "Wrote health check -> $outJson"
