Param(
    [string]$Start = "2023-10-01",
    [string]$End = "2025-10-26",
    [float]$RateDelay = 0.4,
    [switch]$IncludeLive
)

$ErrorActionPreference = "Continue"

# Resolve repo root and Python venv (repo root is parent of scripts folder)
$ScriptDir = Split-Path -Parent $PSCommandPath
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Write-Host "Python venv not found at $Python" -ForegroundColor Red
    exit 1
}

function Get-MonthRanges([DateTime]$s, [DateTime]$e) {
    $cur = Get-Date -Date (Get-Date $s -Format "yyyy-MM-01")
    $out = @()
    while ($cur -le $e) {
        $mStart = Get-Date -Date (Get-Date $cur -Format "yyyy-MM-01")
        $mEnd = (Get-Date $mStart).AddMonths(1).AddDays(-1)
        if ($mEnd -gt $e) { $mEnd = $e }
        if ($mEnd -lt $mStart) { break }
        $out += [PSCustomObject]@{ Start = $mStart; End = $mEnd }
        $cur = (Get-Date $mStart).AddMonths(1)
    }
    return $out
}

$s = [DateTime]::Parse($Start)
$e = [DateTime]::Parse($End)
$ranges = Get-MonthRanges -s $s -e $e

Write-Host ("Backfill two years: {0} to {1} | months={2}" -f ($s.ToString('yyyy-MM-dd')), ($e.ToString('yyyy-MM-dd')), $ranges.Count) -ForegroundColor Cyan

foreach ($pair in $ranges) {
    $ms = ($pair.Start).ToString('yyyy-MM-dd')
    $me = ($pair.End).ToString('yyyy-MM-dd')
    Write-Host ("\n=== Month: $ms .. $me ===") -ForegroundColor Yellow
    $flag = if ($IncludeLive) { "--include-live" } else { "--finals-only" }

    # Boxscores
    & $Python -m nba_betting.cli backfill-boxscores --start $ms --end $me $flag --rate-delay $RateDelay
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Boxscores backfill failed for $ms..$me (exit=$LASTEXITCODE)" -ForegroundColor Red
        $hadErrors = $true
        $LASTEXITCODE = 0
    }

    # PBP
    & $Python -m nba_betting.cli backfill-pbp --start $ms --end $me $flag --rate-delay $RateDelay
    if ($LASTEXITCODE -ne 0) {
        Write-Host "PBP backfill failed for $ms..$me (exit=$LASTEXITCODE)" -ForegroundColor Red
        $hadErrors = $true
        $LASTEXITCODE = 0
    }
}

if ($hadErrors) {
    Write-Host "\nBackfill completed with some errors. See logs above." -ForegroundColor Yellow
    exit 2
}
else {
    Write-Host "\nBackfill complete." -ForegroundColor Green
    exit 0
}
