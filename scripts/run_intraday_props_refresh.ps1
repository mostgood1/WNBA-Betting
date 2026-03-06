Param(
  [string]$Date = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$Quiet,
  [string]$LogDir = 'logs'
)

$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..')).Path
Set-Location -Path $RepoRoot

$VenvPy = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$Python = if (Test-Path $VenvPy) { $VenvPy } else { 'python' }

$env:PYTHONPATH = Join-Path $RepoRoot 'src'
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONWARNINGS = 'ignore'
$env:ONNXRUNTIME_LOG_SEVERITY_LEVEL = '3'
$env:ORT_DISABLE_CPUINFO = '1'

if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
  $PSNativeCommandUseErrorActionPreference = $false
}

$LogPath = Join-Path $RepoRoot $LogDir
if (-not (Test-Path $LogPath)) { New-Item -ItemType Directory -Path $LogPath | Out-Null }
$Stamp = (Get-Date).ToString('yyyyMMdd_HHmmss')
$LogFile = Join-Path $LogPath ("intraday_props_refresh_{0}.log" -f $Stamp)

function Write-Log {
  param([string]$Msg)
  $ts = (Get-Date).ToString('u')
  $line = "[$ts] $Msg"
  $line | Out-File -FilePath $LogFile -Append -Encoding UTF8
  if (-not $Quiet) { Write-Host $line }
}

function Import-DotEnv {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  try {
    Get-Content -Path $Path -Encoding UTF8 | ForEach-Object {
      $line = $_.Trim()
      if (-not $line) { return }
      if ($line.StartsWith('#')) { return }
      $idx = $line.IndexOf('=')
      if ($idx -lt 1) { return }
      $key = $line.Substring(0, $idx).Trim()
      $val = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
      if ($key) { Set-Item -Path "Env:$key" -Value $val }
    }
    Write-Log 'Loaded environment from .env'
  } catch {
    Write-Log (".env load failed (non-fatal): {0}" -f $_.Exception.Message)
  }
}

function Invoke-PyMod {
  param([string[]]$plist)
  $cmd = @($Python) + $plist
  Write-Log ("Run: {0}" -f ($cmd -join ' '))
  $ErrorActionPreference = 'Continue'
  & $Python @plist 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
  $exitCode = $LASTEXITCODE
  $ErrorActionPreference = 'Stop'
  return $exitCode
}

Import-DotEnv -Path (Join-Path $RepoRoot '.env')

Write-Log "Starting intraday props refresh for date=$Date"
Write-Log "Python: $Python"

try {
  $predPath = Join-Path $RepoRoot ("data\processed\props_predictions_{0}.csv" -f $Date)
  if (-not (Test-Path $predPath)) {
    Write-Log 'props_predictions missing; generating before edges'
    $rcPred = Invoke-PyMod -plist @('-m','nba_betting.cli','predict-props','--date', $Date)
    Write-Log ("predict-props exit code: {0}" -f $rcPred)
    if ($rcPred -ne 0) {
      throw "predict-props failed with exit code $rcPred"
    }
  }

  $rcSnap = Invoke-PyMod -plist @('-m','nba_betting.cli','odds-snapshots-props','--date', $Date)
  Write-Log ("odds-snapshots-props exit code: {0}" -f $rcSnap)
  if ($rcSnap -ne 0) {
    throw "odds-snapshots-props failed with exit code $rcSnap"
  }

  $rcEdges = Invoke-PyMod -plist @(
    '-m','nba_betting.cli','props-edges',
    '--date', $Date,
    '--source','oddsapi',
    '--mode','current',
    '--file-only',
    '--calibrate-prob',
    '--calibrate-sigma'
  )
  Write-Log ("props-edges (oddsapi, mode=current) exit code: {0}" -f $rcEdges)
  if ($rcEdges -ne 0) {
    throw "props-edges failed with exit code $rcEdges"
  }

  $rcExport = Invoke-PyMod -plist @('-m','nba_betting.cli','export-props-recommendations','--date', $Date)
  Write-Log ("export-props-recommendations exit code: {0}" -f $rcExport)
  if ($rcExport -ne 0) {
    throw "export-props-recommendations failed with exit code $rcExport"
  }

  Write-Log 'Intraday props refresh completed successfully'
  exit 0
} catch {
  Write-Log ("Intraday props refresh failed: {0}" -f $_.Exception.Message)
  exit 1
}