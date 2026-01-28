param(
    [string]$BaseUrl = 'http://127.0.0.1:5051',
    [string]$Date = $(Get-Date -Format 'yyyy-MM-dd'),
    [string]$AuthToken = '',
    [int]$TimeoutSec = 10
)

$ErrorActionPreference = 'Stop'

function Invoke-HealthCheck {
    param(
        [string]$Url,
        [hashtable]$Headers
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
        if ($Headers) {
            $r = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSec -Method GET -Headers $Headers
        } else {
            $r = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec $TimeoutSec -Method GET
        }
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

$headers = $null
if ($AuthToken -and $AuthToken.Trim().Length -gt 0) {
    $headers = @{ Authorization = "Bearer $AuthToken" }
}

$endpoints = @(
    "$BaseUrl/health",
    "$BaseUrl/recommendations?format=json&view=summary",
    "$BaseUrl/recommendations?format=json&view=summary-props",
    "$BaseUrl/recommendations?format=json&view=summary-props-calibration",
    "$BaseUrl/recommendations?format=json&view=all&date=$Date&compact=1&regular_only=1",
    "$BaseUrl/api/debug/recommendations/status?date=$Date",
    "$BaseUrl/api/predictions?date=$Date",
    "$BaseUrl/api/processed/recon_games?date=$Date"
)

# Cron endpoints (optional, may require AuthToken)
$cronEndpoints = @(
    "$BaseUrl/api/cron/refresh-bovada?date=$Date",
    "$BaseUrl/api/cron/reconcile-games?date=$Date",
    "$BaseUrl/api/cron/run-all?push=0"
)

$results = @()
foreach ($u in $endpoints) {
    $results += (Invoke-HealthCheck -Url $u -Headers $headers)
}
foreach ($u in $cronEndpoints) {
    $results += (Invoke-HealthCheck -Url $u -Headers $headers)
}

$root = "c:/Users/mostg/OneDrive/Coding/NBA-Betting"
$outDir = Join-Path $root 'data/processed/health_checks'
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$outJson = Join-Path $outDir ("health_" + $stamp + ".json")

$payload = [ordered]@{
    time = (Get-Date).ToString('s') + 'Z'
    base_url = $BaseUrl
    date = $Date
    results = $results
}

# Extract calibration summary details if available
try {
    $calUrl = "$BaseUrl/recommendations?format=json&view=summary-props-calibration"
    $calRes = Invoke-HealthCheck -Url $calUrl -Headers $headers
    if ($calRes.ok -and $calRes.status -eq 200) {
        $json = Invoke-WebRequest -UseBasicParsing -Uri $calUrl -TimeoutSec $TimeoutSec -Method GET -Headers $headers | Select-Object -ExpandProperty Content | ConvertFrom-Json
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
    $recoUrl = "$BaseUrl/recommendations?format=json&view=all&date=$Date&compact=1&regular_only=1"
    $recoRes = Invoke-WebRequest -UseBasicParsing -Uri $recoUrl -TimeoutSec $TimeoutSec -Method GET -Headers $headers
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
        # Include category counts and data_dates for quick verification
        try {
            $gCount = 0; $pCount = 0; $fbCount = 0; $tCount = 0
            if ($rj.games) { $gCount = [int]$rj.games.Count }
            if ($rj.props) { $pCount = [int]$rj.props.Count }
            if ($rj.first_basket) { $fbCount = [int]$rj.first_basket.Count }
            if ($rj.early_threes) { $tCount = [int]$rj.early_threes.Count }
            $dd = $null
            if ($rj.meta -and $rj.meta.data_dates) { $dd = $rj.meta.data_dates }
            $payload.recommendations = [ordered]@{
                counts = [ordered]@{
                    games = $gCount
                    props = $pCount
                    first_basket = $fbCount
                    early_threes = $tCount
                }
                data_dates = $dd
            }
        } catch {}
    }
} catch {}

$payload | ConvertTo-Json -Depth 5 | Set-Content -Path $outJson -Encoding UTF8
Write-Host "Wrote health check -> $outJson"
