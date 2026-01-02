param(
  [string]$Since = '2025-10-21',
  [string]$Until = '2025-10-23'
)

$ErrorActionPreference = 'Stop'
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

# Local-only notice: remote finals export disabled. Provide local alternative or manual export.
Write-Host '[INFO] Remote finals export disabled; this script is local-only.'
Write-Host ('[INFO] Range requested: since={0} until={1}' -f $Since, $Until)
Write-Host '[INFO] No action taken. Use local CLI or tools to generate finals artifacts.'
exit 0
