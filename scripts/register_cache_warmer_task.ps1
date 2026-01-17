param(
  [string]$Time = "01:55",
  [string]$BaseUrl = "http://127.0.0.1:5051"
)
$ErrorActionPreference = 'Stop'
$taskName = "NBA-Betting - Warm Caches"
$workDir = (Get-Location).Path
$python = Join-Path $workDir ".venv\Scripts\python.exe"
$script = Join-Path $workDir "tools\warm_caches.py"

# Build schtasks command (fallback for non-admin contexts)
$schArgs = @(
  "/Create",
  "/SC", "DAILY",
  "/TN", $taskName,
  "/TR", "`"$python`" `"$script`"",
  "/ST", $Time
)
try {
  # Try with schtasks.exe to avoid elevated Register-ScheduledTask requirement
  schtasks.exe @schArgs | Out-Null
  Write-Host "✅ Registered cache warmer via schtasks: $taskName at $Time"
} catch {
  Write-Host "❌ Failed to register with schtasks: $($_.Exception.Message)" -ForegroundColor Red
  exit 1
}
