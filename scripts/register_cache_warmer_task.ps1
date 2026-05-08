param(
  [string]$Time = "01:55",
  [string]$BaseUrl = "http://127.0.0.1:5051"
)
$ErrorActionPreference = 'Stop'
$taskName = "WNBA-Betting - Warm Caches"
$workDir = (Get-Location).Path
$script = Join-Path $workDir "scripts\run_cache_warmer.ps1"

# Build schtasks command (fallback for non-admin contexts)
$schArgs = @(
  "/Create",
  "/SC", "DAILY",
  "/TN", $taskName,
  "/TR", "powershell -NoProfile -ExecutionPolicy Bypass -File `"$script`" -BaseUrl `"$BaseUrl`"",
  "/ST", $Time,
  "/F"
)

# Try with schtasks.exe to avoid elevated Register-ScheduledTask requirement.
$out = & schtasks.exe @schArgs 2>&1
if ($LASTEXITCODE -ne 0) {
  Write-Host "❌ Failed to register with schtasks" -ForegroundColor Red
  if ($out) { Write-Host ($out | Out-String) }
  exit $LASTEXITCODE
}

Write-Host "✅ Registered cache warmer via schtasks: $taskName at $Time"

