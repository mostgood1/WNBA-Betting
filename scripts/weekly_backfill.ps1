Param(
  [string]$Start,
  [string]$End,
  [int]$Days = 21,
  [switch]$FinalsOnly,
  [switch]$Quiet
)

$ErrorActionPreference = 'Stop'

# Resolve repo root and Python
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Python = if (Test-Path $VenvPy) { $VenvPy } else { 'python' }

function Write-Log {
  param([string]$Msg)
  if (-not $Quiet) { Write-Host ("[weekly] " + $Msg) }
}

# Determine date range
try {
  if (-not $End) { $End = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') }
  if (-not $Start) {
    $EndDt = [datetime]::ParseExact($End, 'yyyy-MM-dd', $null)
    $Start = $EndDt.AddDays(-1 * [math]::Max(1,$Days)).ToString('yyyy-MM-dd')
  }
} catch {
  Write-Error "Invalid Start/End date format. Use YYYY-MM-DD."; exit 1
}

Write-Log ("Weekly backfill range: {0}..{1} (finals-only={2})" -f $Start, $End, [bool]$FinalsOnly)

function Invoke-PyMod {
  param([string[]]$plist)
  & $Python @plist
  return $LASTEXITCODE
}

# 1) PBP backfill for range
$plist = @('-m','nba_betting.cli','backfill-pbp','--start', $Start, '--end', $End)
if ($FinalsOnly) { $plist += '--finals-only' }
Write-Log ("Run: python {0}" -f ($plist -join ' '))
$rc1 = Invoke-PyMod -plist $plist
Write-Log ("backfill-pbp exit code: {0}" -f $rc1)

# 2) recon_quarters for range
$plist2 = @('-m','nba_betting.cli','reconcile-quarters-range','--start', $Start, '--end', $End)
Write-Log ("Run: python {0}" -f ($plist2 -join ' '))
$rc2 = Invoke-PyMod -plist $plist2
Write-Log ("reconcile-quarters-range exit code: {0}" -f $rc2)

Write-Log "Weekly backfill completed."
