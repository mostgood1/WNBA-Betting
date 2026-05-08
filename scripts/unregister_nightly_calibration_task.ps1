param(
    [string]$TaskName = "WNBA-Betting - Nightly Props Calibration"
)

# Unregister the nightly props calibration task using schtasks (works without elevation for current user tasks).
try {
    schtasks.exe /Query /TN "$TaskName" | Out-Null
    schtasks.exe /Delete /TN "$TaskName" /F | Out-Null
    Write-Host "✅ Unregistered task: $TaskName"
} catch {
    Write-Warning ("Failed to unregister task: " + $_.Exception.Message)
}
