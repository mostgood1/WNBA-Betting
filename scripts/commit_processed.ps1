param(
    [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
    [switch]$Push,
    [switch]$IncludeJson,
    [switch]$DryRun
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
if ($IncludeJson) { $patterns += "*_$Date.json" }

# Collect files matching patterns under data/processed only
$files = @()
foreach ($pat in $patterns) {
    $files += Get-ChildItem -Path $processedDir -Filter $pat -File -ErrorAction SilentlyContinue
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
    "early_threes_",
    "pbp_metrics_daily_",
    "finals_",
    "closing_lines_",
    "market_",
    "odds_",
    "game_odds_",
    "game_cards_",
    # Frontend-consumed season/game predictions snapshot
    "games_predictions_npu_",
    # Props artifacts surfaced in the UI
    "props_edges_",
    "props_predictions_",
    "props_recommendations_"
)

# Filter files to allowed prefixes
$files = $files | Where-Object { $name = $_.Name; $allowedPrefixes | ForEach-Object { if ($name.StartsWith($_)) { $true; break } } }

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
    git add -- $rel
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
