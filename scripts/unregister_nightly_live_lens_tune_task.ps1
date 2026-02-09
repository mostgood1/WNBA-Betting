param()

$TaskName = "NBA-Betting - Nightly Live Lens Tune"

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop | Out-Null
    Write-Host "✅ Unregistered task: $TaskName"
} catch {
    Write-Warning ("Unregister-ScheduledTask failed or task not found: " + $_.Exception.Message)
    try {
        schtasks.exe /Delete /TN "$TaskName" /F | Out-Null
        Write-Host "✅ Removed via schtasks: $TaskName"
    } catch {
        Write-Warning ("schtasks removal failed: " + $_.Exception.Message)
    }
}
