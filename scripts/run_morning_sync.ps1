param(
  [string]$Branch = 'main',
  [string]$Remote = 'origin',
  [switch]$AllowDirty,
  [switch]$FetchOnly,
  [string]$LogDir = 'logs'
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $root '..')).Path
Set-Location -Path $repoRoot

$logPath = Join-Path $repoRoot $LogDir
if (-not (Test-Path $logPath)) { New-Item -ItemType Directory -Path $logPath | Out-Null }
$stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
$logFile = Join-Path $logPath ("morning_sync_{0}.log" -f $stamp)

function Write-Log {
  param([string]$Msg)
  $ts = (Get-Date).ToString('u')
  $line = "[$ts] $Msg"
  $line | Out-File -FilePath $logFile -Append -Encoding UTF8
  Write-Host $line
}

function Invoke-GitCapture {
  param([string[]]$ArgList)
  $prevEap = $ErrorActionPreference
  $hasNativePref = $false
  $prevNativePref = $null
  try {
    if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
      $hasNativePref = $true
      $prevNativePref = $PSNativeCommandUseErrorActionPreference
      $PSNativeCommandUseErrorActionPreference = $false
    }
    $ErrorActionPreference = 'Continue'
    $output = & git @ArgList 2>&1
    $rc = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $prevEap
    if ($hasNativePref) {
      $PSNativeCommandUseErrorActionPreference = $prevNativePref
    }
  }
  if ($null -ne $output) {
    ($output | Out-String).TrimEnd() | Out-File -FilePath $logFile -Append -Encoding UTF8
  }
  return [pscustomobject]@{ Output = $output; ExitCode = $rc }
}

Write-Log ("Morning sync starting (repo={0}, branch={1}, remote={2})" -f $repoRoot, $Branch, $Remote)

$inside = Invoke-GitCapture -ArgList @('rev-parse', '--is-inside-work-tree')
if ($inside.ExitCode -ne 0 -or -not (($inside.Output | Out-String).Trim() -eq 'true')) {
  throw "Not inside a git work tree: $repoRoot"
}

$branchInfo = Invoke-GitCapture -ArgList @('branch', '--show-current')
if ($branchInfo.ExitCode -ne 0) {
  throw 'Unable to determine current branch'
}
$currentBranch = ($branchInfo.Output | Out-String).Trim()
if ($currentBranch -ne $Branch) {
  Write-Log ("Current branch is '{0}', not '{1}'. Skipping sync." -f $currentBranch, $Branch)
  exit 2
}

$status = Invoke-GitCapture -ArgList @('status', '--porcelain')
if ($status.ExitCode -ne 0) {
  throw 'Unable to inspect git status'
}
$hasLocalChanges = -not [string]::IsNullOrWhiteSpace(($status.Output | Out-String).Trim())
if ($hasLocalChanges -and -not $AllowDirty) {
  Write-Log 'Working tree has local changes; refusing automatic sync. Re-run with -AllowDirty to fetch only / inspect state.'
  exit 2
}

Write-Log ("Fetching {0}/{1}" -f $Remote, $Branch)
$fetch = Invoke-GitCapture -ArgList @('fetch', $Remote, $Branch)
if ($fetch.ExitCode -ne 0) {
  throw ("git fetch failed for {0}/{1}" -f $Remote, $Branch)
}

$counts = Invoke-GitCapture -ArgList @('rev-list', '--left-right', '--count', ("HEAD...{0}/{1}" -f $Remote, $Branch))
if ($counts.ExitCode -ne 0) {
  throw 'Unable to compare local and remote history'
}
$parts = (($counts.Output | Out-String).Trim() -split '\s+')
if ($parts.Count -lt 2) {
  throw 'Unexpected rev-list output while comparing local and remote history'
}

$ahead = 0
$behind = 0
try {
  $ahead = [int]$parts[0]
  $behind = [int]$parts[1]
} catch {
  throw 'Unable to parse ahead/behind counts'
}

Write-Log ("Branch state relative to {0}/{1}: ahead={2} behind={3}" -f $Remote, $Branch, $ahead, $behind)

if ($FetchOnly) {
  Write-Log 'Fetch-only mode requested; stopping after remote refresh.'
  exit 0
}

if ($ahead -gt 0 -and $behind -gt 0) {
  Write-Log ("Local branch has diverged from {0}/{1}; skipping automatic sync." -f $Remote, $Branch)
  exit 3
}

if ($ahead -gt 0) {
  Write-Log 'Local branch is ahead of remote; skipping automatic sync.'
  exit 3
}

if ($behind -eq 0) {
  Write-Log 'Local repo is already up to date.'
  exit 0
}

if ($hasLocalChanges) {
  Write-Log 'Working tree is dirty; fetch succeeded but pull was skipped for safety.'
  exit 2
}

Write-Log ("Fast-forwarding from {0}/{1}" -f $Remote, $Branch)
$pull = Invoke-GitCapture -ArgList @('pull', '--ff-only', $Remote, $Branch)
if ($pull.ExitCode -ne 0) {
  throw 'git pull --ff-only failed'
}

Write-Log 'Morning sync complete.'
