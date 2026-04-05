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

function Invoke-InlinePython {
  param(
    [string]$Label,
    [string]$ScriptBody
  )

  $tmpPy = Join-Path $LogPath ("{0}_{1}.py" -f $Label, $Stamp)
  Set-Content -Path $tmpPy -Value $ScriptBody -Encoding UTF8
  try {
    $ErrorActionPreference = 'Continue'
    & $Python $tmpPy 2>&1 | Tee-Object -FilePath $LogFile -Append | Out-Null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    return $exitCode
  } finally {
    try { Remove-Item $tmpPy -Force -ErrorAction SilentlyContinue } catch { }
  }
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

  $perGameLimitInt = 3
  $slateLimitInt = 25
  $slatePerMarketLimitInt = 4
  $mkts = 'pts,reb,ast,threes,blk,stl,pra,pr,pa,ra,dd,td'
  $topByGameOut = Join-Path $RepoRoot ("data/processed/props_recommendations_top_by_game_{0}.json" -f $Date)
  Write-Log ("Refreshing props top-by-game snapshot for {0} -> {1}" -f $Date, $topByGameOut)
  $pyTopByGame = @"
import json
import sys
from pathlib import Path

repo_root = Path(r"{REPO_PLACEHOLDER}")
if str(repo_root) not in sys.path:
  sys.path.insert(0, str(repo_root))

import app

date_str = r"{DATE_PLACEHOLDER}"
out_path = Path(r"{OUT_PLACEHOLDER}")
per_game_limit = int(r"{PGL_PLACEHOLDER}")
slate_limit = int(r"{SL_PLACEHOLDER}")
slate_per_market_limit = int(r"{SPML_PLACEHOLDER}")
markets = r"{MKTS_PLACEHOLDER}".strip()

query = f"/api/props/recommendations?date={date_str}&compact=1&portfolio_only=1&use_snapshot=0&limit={slate_limit}&per_game_limit={per_game_limit}&per_market=1&slate_per_market_limit={slate_per_market_limit}"
if markets:
  query += "&markets=" + markets

client = app.app.test_client()
resp = client.get(query)
try:
  payload = resp.get_json() if resp is not None else None
except Exception:
  payload = None

if not isinstance(payload, dict):
  payload = {"error": "no_json", "status": int(getattr(resp, 'status_code', 0) or 0)}

out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print("OK")
"@
  $pyTopByGame = $pyTopByGame.Replace('{REPO_PLACEHOLDER}', $RepoRoot)
  $pyTopByGame = $pyTopByGame.Replace('{DATE_PLACEHOLDER}', $Date)
  $pyTopByGame = $pyTopByGame.Replace('{OUT_PLACEHOLDER}', $topByGameOut)
  $pyTopByGame = $pyTopByGame.Replace('{PGL_PLACEHOLDER}', [string]$perGameLimitInt)
  $pyTopByGame = $pyTopByGame.Replace('{SL_PLACEHOLDER}', [string]$slateLimitInt)
  $pyTopByGame = $pyTopByGame.Replace('{SPML_PLACEHOLDER}', [string]$slatePerMarketLimitInt)
  $pyTopByGame = $pyTopByGame.Replace('{MKTS_PLACEHOLDER}', $mkts)
  $rcTopByGame = Invoke-InlinePython -Label 'intraday_props_top_by_game' -ScriptBody $pyTopByGame
  Write-Log ("props_recommendations_top_by_game refresh exit code: {0}" -f $rcTopByGame)
  if ($rcTopByGame -ne 0) {
    throw "props_recommendations_top_by_game refresh failed with exit code $rcTopByGame"
  }

  $cardsPropsSource = 'auto'
  $cardsPropsOut = Join-Path $RepoRoot ("data/processed/cards_props_snapshot_{0}.json" -f $Date)
  try { if (Test-Path $cardsPropsOut) { Remove-Item $cardsPropsOut -Force -ErrorAction SilentlyContinue } } catch { }
  Write-Log ("Refreshing cards props snapshot for {0} -> {1} (props_source={2})" -f $Date, $cardsPropsOut, $cardsPropsSource)
  $pyCardsProps = @"
import json
import sys
from pathlib import Path

repo_root = Path(r"{REPO_PLACEHOLDER}")
if str(repo_root) not in sys.path:
  sys.path.insert(0, str(repo_root))

import app

date_str = r"{DATE_PLACEHOLDER}"
out_path = Path(r"{OUT_PLACEHOLDER}")
props_source = r"{PROPS_SOURCE_PLACEHOLDER}"

client = app.app.test_client()
resp = client.get(f"/api/cards?date={date_str}&props_source={props_source}")
try:
  payload = resp.get_json() if resp is not None else None
except Exception:
  payload = None

games_out = []
if isinstance(payload, dict):
  for game in (payload.get("games") or []):
    if not isinstance(game, dict):
      continue
    prop_recommendations = game.get("prop_recommendations") if isinstance(game.get("prop_recommendations"), dict) else {}
    games_out.append({
      "home_tri": game.get("home_tri"),
      "away_tri": game.get("away_tri"),
      "prop_recommendations": {
        "home": [row for row in (prop_recommendations.get("home") or []) if isinstance(row, dict)],
        "away": [row for row in (prop_recommendations.get("away") or []) if isinstance(row, dict)],
      },
    })

out = {"date": date_str, "games": games_out}
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
print("OK")
"@
  $pyCardsProps = $pyCardsProps.Replace('{REPO_PLACEHOLDER}', $RepoRoot)
  $pyCardsProps = $pyCardsProps.Replace('{DATE_PLACEHOLDER}', $Date)
  $pyCardsProps = $pyCardsProps.Replace('{OUT_PLACEHOLDER}', $cardsPropsOut)
  $pyCardsProps = $pyCardsProps.Replace('{PROPS_SOURCE_PLACEHOLDER}', $cardsPropsSource)
  $rcCardsProps = Invoke-InlinePython -Label 'intraday_cards_props_snapshot' -ScriptBody $pyCardsProps
  Write-Log ("cards props snapshot refresh exit code: {0}" -f $rcCardsProps)
  if ($rcCardsProps -ne 0) {
    throw "cards props snapshot refresh failed with exit code $rcCardsProps"
  }

  $cardsSimDetailOut = Join-Path $RepoRoot ("data/processed/cards_sim_detail_{0}.json" -f $Date)
  try { if (Test-Path $cardsSimDetailOut) { Remove-Item $cardsSimDetailOut -Force -ErrorAction SilentlyContinue } } catch { }
  Write-Log ("Refreshing cards sim detail snapshot for {0} -> {1} (props_source={2})" -f $Date, $cardsSimDetailOut, $cardsPropsSource)
  $pyCardsSim = @"
import json
import sys
from pathlib import Path

repo_root = Path(r"{REPO_PLACEHOLDER}")
if str(repo_root) not in sys.path:
  sys.path.insert(0, str(repo_root))

import app

date_str = r"{DATE_PLACEHOLDER}"
out_path = Path(r"{OUT_PLACEHOLDER}")
props_source = r"{PROPS_SOURCE_PLACEHOLDER}"

client = app.app.test_client()
resp = client.get(f"/api/cards?date={date_str}&include_players=1&props_source={props_source}")
try:
  payload = resp.get_json() if resp is not None else None
except Exception:
  payload = None

games_out = []
if isinstance(payload, dict):
  for game in (payload.get("games") or []):
    if not isinstance(game, dict):
      continue
    sim = game.get("sim") if isinstance(game.get("sim"), dict) else {}
    players = sim.get("players") if isinstance(sim.get("players"), dict) else {}
    missing = sim.get("missing_prop_players") if isinstance(sim.get("missing_prop_players"), dict) else {}
    injuries = sim.get("injuries") if isinstance(sim.get("injuries"), dict) else {}
    summary = sim.get("players_summary") if isinstance(sim.get("players_summary"), dict) else {
      "home": len(players.get("home") or []),
      "away": len(players.get("away") or []),
      "missing_home": len(missing.get("home") or []),
      "missing_away": len(missing.get("away") or []),
      "injured_home": len(injuries.get("home") or []),
      "injured_away": len(injuries.get("away") or []),
    }
    games_out.append({
      "home_tri": game.get("home_tri"),
      "away_tri": game.get("away_tri"),
      "sim": {
        "players": {
          "home": [row for row in (players.get("home") or []) if isinstance(row, dict)],
          "away": [row for row in (players.get("away") or []) if isinstance(row, dict)],
        },
        "missing_prop_players": {
          "home": [row for row in (missing.get("home") or []) if isinstance(row, dict)],
          "away": [row for row in (missing.get("away") or []) if isinstance(row, dict)],
        },
        "injuries": {
          "home": [row for row in (injuries.get("home") or []) if isinstance(row, dict)],
          "away": [row for row in (injuries.get("away") or []) if isinstance(row, dict)],
        },
        "players_summary": summary,
      },
    })

out = {"date": date_str, "games": games_out}
out_path.parent.mkdir(parents=True, exist_ok=True)
out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
print("OK")
"@
  $pyCardsSim = $pyCardsSim.Replace('{REPO_PLACEHOLDER}', $RepoRoot)
  $pyCardsSim = $pyCardsSim.Replace('{DATE_PLACEHOLDER}', $Date)
  $pyCardsSim = $pyCardsSim.Replace('{OUT_PLACEHOLDER}', $cardsSimDetailOut)
  $pyCardsSim = $pyCardsSim.Replace('{PROPS_SOURCE_PLACEHOLDER}', $cardsPropsSource)
  $rcCardsSim = Invoke-InlinePython -Label 'intraday_cards_sim_detail' -ScriptBody $pyCardsSim
  Write-Log ("cards sim detail refresh exit code: {0}" -f $rcCardsSim)
  if ($rcCardsSim -ne 0) {
    throw "cards sim detail refresh failed with exit code $rcCardsSim"
  }

  Write-Log 'Intraday props refresh completed successfully'
  exit 0
} catch {
  Write-Log ("Intraday props refresh failed: {0}" -f $_.Exception.Message)
  exit 1
}