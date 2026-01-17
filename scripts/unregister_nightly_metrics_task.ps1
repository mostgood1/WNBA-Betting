$TaskName = "NBA-Betting - Nightly Metrics"
try {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop | Out-Null
  Write-Host "✅ Unregistered task: $TaskName"
} catch {
  Write-Warning ("Unregister-ScheduledTask failed: " + $_.Exception.Message)
  try {
    schtasks.exe /Delete /TN "$TaskName" /F | Out-Null
    Write-Host "✅ Unregistered via schtasks: $TaskName"
  } catch {
    Write-Warning ("schtasks delete failed: " + $_.Exception.Message)
  }
}
