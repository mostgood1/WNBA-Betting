$ErrorActionPreference = 'Stop'
$taskName = "NBA-Betting - Warm Caches"
try {
  schtasks.exe /Delete /TN "$taskName" /F | Out-Null
  Write-Host "Unregistered cache warmer: $taskName"
} catch {
  Write-Host "Cache warmer task not found or already removed: $taskName"
}
