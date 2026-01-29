param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [switch]$Push,
    [switch]$IncludeJson,
    [switch]$DryRun,
    # Include evaluation artifacts that are not tied to a single date (props_eval_compare_*.csv)
    [switch]$IncludeEval,
    # Include per-player calibration config JSON (range-based, no date in filename)
    [switch]$IncludeCalibConfig
)

# Ensure we run from repo root (script is in scripts/)
$repoRoot = Split-Path -Path $PSScriptRoot -Parent
Set-Location $repoRoot

$processedDir = Join-Path $repoRoot 'data/processed'
if (-not (Test-Path $processedDir)) {
    Write-Error "Processed directory not found: $processedDir"
    exit 1
}

# Build target globs for the date (CSV only by default)
$patterns = @("*_$Date.csv")
if ($IncludeJson) {
    $patterns += "*_$Date.json"
    # SmartSim uses smart_sim_<date>_<HOME>_<AWAY>.json (date not at end)
    $patterns += "smart_sim_${Date}_*.json"
    # Daily validation summary
    $patterns += "daily_artifacts_${Date}.json"
}

# Collect files matching patterns under data/processed only
$files = @()
foreach ($pat in $patterns) {
    $files += Get-ChildItem -Path $processedDir -Filter $pat -File -ErrorAction SilentlyContinue
}

# Deduplicate in case multiple patterns match the same file
if ($files -and $files.Count -gt 0) {
    $files = $files | Sort-Object FullName -Unique
}

# Include undated + latest rolling calibration artifacts when JSON is enabled.
# Note: these files often have a different date than -Date (e.g., anchored at yesterday).
if ($IncludeJson) {
    try {
        $qc = Join-Path $processedDir 'quarters_calibration.json'
        if (Test-Path $qc) {
            $files += Get-Item -Path $qc -ErrorAction SilentlyContinue
        }
    } catch { }
    try {
        $latestTot = Get-ChildItem -Path $processedDir -Filter 'calibration_totals_*.json' -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($null -ne $latestTot) { $files += $latestTot }
    } catch { }
    try {
        $latestPProb = Get-ChildItem -Path $processedDir -Filter 'calibration_period_probs_*.json' -File -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($null -ne $latestPProb) { $files += $latestPProb }
    } catch { }

    # Re-dedupe after adding extra files
    try {
        if ($files -and $files.Count -gt 0) {
            $files = $files | Sort-Object FullName -Unique
        }
    } catch { }
}

# Optionally include evaluation compare CSVs (range-based filenames)
if ($IncludeEval) {
    try {
        $files += Get-ChildItem -Path $processedDir -Filter 'props_eval_compare_*.csv' -File -ErrorAction SilentlyContinue
        $files += Get-ChildItem -Path $processedDir -Filter 'props_eval_compare_*.json' -File -ErrorAction SilentlyContinue
        $files += Get-ChildItem -Path $processedDir -Filter 'props_eval_compare_*.md' -File -ErrorAction SilentlyContinue
    } catch { }
}

# Optionally include per-player calibration config JSON (no date pattern)
if ($IncludeCalibConfig) {
    try {
        $cfgPath = Join-Path $processedDir 'props_player_calibration_config.json'
        if (Test-Path $cfgPath) {
            $files += Get-Item -Path $cfgPath -ErrorAction SilentlyContinue
        }
    } catch { }
}

# Whitelist only Render/UI-facing artifacts to keep pushes minimal
$allowedPrefixes = @(
    "predictions_",
    "recommendations_",
    "recon_games_",
    "recon_props_",
    # PBP-derived and reconciliation artifacts we want available to the site (and for debugging)
    "pbp_reconcile_",
    "tip_winner_probs_",
    "first_basket_probs_",
    "first_basket_recs_",
    "early_threes_",
    "pbp_metrics_daily_",
    "finals_",
    "closing_lines_",
    "market_",
    "odds_",
    "game_odds_",
    "period_lines_",
    "game_cards_",
    # Frontend-consumed season/game predictions snapshot
    "games_predictions_npu_",
    # Props artifacts surfaced in the UI
    "props_edges_",
    "props_predictions_",
    "props_recommendations_",
    # SmartSim per-game distributions (JSON) for UI/diagnostics
    "smart_sim_",
    # Daily pipeline artifact summary
    "daily_artifacts_",
    # Rolling totals calibration used to tune predictions/sims
    "calibration_totals_",
    # Rolling period probability calibration for quarter/half over markets
    "calibration_period_probs_",
    # Undated quarters calibration used by SmartSim
    "quarters_calibration",
    # New: per-player calibration artifact for diagnostics/analysis
    # New: evaluation compare outputs (daily + summary)
    "props_eval_compare_"
)

# Filter files to allowed prefixes (robust boolean predicate)
$files = $files | Where-Object {
    $name = $_.Name
    $match = $false
    foreach ($p in $allowedPrefixes) {
        if ($name.StartsWith($p)) { $match = $true; break }
    }
    return $match
}

# Debug: list filtered candidates
Write-Host "Filtered artifacts (allowed prefixes):" -ForegroundColor DarkCyan
$files | ForEach-Object { Write-Host " - "$_.Name }

if (-not $files -or $files.Count -eq 0) {
    Write-Host "No processed artifacts found for date $Date. Nothing to commit."
    exit 0
}

# Sort for stable output
$files = $files | Sort-Object FullName

Write-Host "Artifacts to commit for $($Date):" -ForegroundColor Cyan
$files | ForEach-Object { Write-Host " - " (Resolve-Path -Relative $_.FullName) }

if ($DryRun) {
    Write-Host "Dry run requested; skipping git add/commit/push." -ForegroundColor Yellow
    exit 0
}

# Stage files
foreach ($f in $files) {
    $rel = Resolve-Path -Relative $f.FullName
    if ($f.Name.StartsWith('calibration_totals_') -or $f.Name.StartsWith('calibration_period_probs_') -or $f.Name -eq 'quarters_calibration.json' -or $f.Name.StartsWith('smart_sim_')) {
        git add -f -- $rel
    } else {
        git add -- $rel
    }
}

# Commit with a concise message listing top artifacts
$names = ($files | Select-Object -First 5 | ForEach-Object { $_.Name }) -join ", "
$more = if ($files.Count -gt 5) { ", +$($files.Count-5) more" } else { "" }
$msg = "data(processed): $($Date) artifacts: $names$more"

git commit -m $msg | Out-Host

if ($LASTEXITCODE -ne 0) {
    Write-Error "git commit failed."
    exit 1
}

if ($Push) {
    Write-Host "Pushing to origin..." -ForegroundColor Cyan
    git push | Out-Host
}

Write-Host "Done." -ForegroundColor Green
