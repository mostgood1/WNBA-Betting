param(
    [Parameter(Mandatory=$false)][switch]$Quiet
)

$TaskName = "NBA-Betting - Daily Availability Snapshot"

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop | Out-Null
    if (-not $Quiet) { Write-Host "✅ Unregistered task: $TaskName" }
    exit 0
} catch {
    try {
        schtasks.exe /Delete /TN "$TaskName" /F | Out-Null
        if (-not $Quiet) { Write-Host "✅ Unregistered via schtasks: $TaskName" }
        exit 0
    } catch {
        if (-not $Quiet) { Write-Warning ("Failed to unregister task: " + $_.Exception.Message) }
        exit 1
    }
}
