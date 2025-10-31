from __future__ import annotations

import click
import os
import pandas as pd
import numpy as np
import subprocess
from rich.console import Console
import re
from rich.progress import track

from .config import paths
# from .scrape_bref import scrape_games  # deprecated
from .features import build_features
# from .train import train_models  # MOVED TO CONDITIONAL IMPORT - requires sklearn
import joblib
from .elo import Elo
from .schedule import compute_rest_for_matchups, fetch_schedule_2025_26
from .rosters import fetch_rosters
from .league_status import build_league_status
from .roster_audit import audit_roster_for_date
from .player_logs import fetch_player_logs
from .teams import normalize_team
from .scrape_nba_api import fetch_games_nba_api, enrich_periods_existing, backfill_scoreboard
from .odds_api import backfill_historical_odds, OddsApiConfig, consensus_lines_at_close, backfill_player_props, fetch_player_props_current
from .pbp_markets import train_all_pbp_markets, predict_tip_for_date, predict_first_basket_for_date, predict_early_threes_for_date
from .odds_api import fetch_game_odds_current
from .odds_bovada import fetch_bovada_odds_current
from .props_actuals import fetch_prop_actuals_via_nbastatr, upsert_props_actuals
from .props_actuals import fetch_prop_actuals_via_nba_cdn, fetch_prop_actuals_via_nbaapi
from .props_features import build_props_features, build_features_for_date
from .finals import fetch_finals, write_finals_csv
from .pbp import fetch_pbp_for_date, backfill_pbp
from .boxscores import fetch_boxscores_for_date, backfill_boxscores
# from .props_train import train_props_models, predict_props  # MOVED TO CONDITIONAL - requires sklearn
from .props_edges import compute_props_edges, SigmaConfig, calibrate_sigma_for_date
from .props_linear import train_linear_props_models, export_linear_to_onnx
from .props_backtest import backtest_linear_props
from nba_api.stats.endpoints import scoreboardv2
from nba_api.stats.endpoints import boxscoretraditionalv3
from nba_api.stats.library import http as nba_http
from nba_api.stats.static import teams as static_teams
import subprocess
from pathlib import Path
import sys
import time
from typing import Optional
from datetime import date as _date

from .pbp_markets import _game_ids_for_date as _pbp_game_ids_for_date  # reuse for backtest
from .pbp_markets import _first_fg_event as _pbp_first_fg_event
from .pbp_markets import _jump_ball_event as _pbp_jump_ball_event
from .pbp_markets import _desc_cols as _pbp_desc_cols
from .pbp_markets import build_early_threes_dataset as _build_early_threes_dataset

console = Console()


@click.group()
def cli():
    """NBA Betting command-line interface."""
    pass


def _load_dotenv_key(name: str) -> str | None:
    """Lightweight .env reader: looks for KEY=VALUE in a .env at repo root."""
    try:
        env_path = paths.root / ".env"
        if not env_path.exists():
            return None
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip() == name:
                return v.strip()
        return None
    except Exception:
        return None


@cli.command("backfill-injuries-season")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", "end_date", type=str, required=True, help="End date YYYY-MM-DD")
def backfill_injuries_season_cmd(start_date: str, end_date: str):
    """Backfill injuries_overrides_<date>.csv for each date in a range using season-long/indefinite OUT flags from injuries.csv.

    Reads data/raw/injuries.csv and writes per-day overrides with columns [team, player, status] and status='OUT'.
    """
    console.rule("Backfill Injury Overrides (season-long/indefinite)")
    try:
        s = pd.to_datetime(start_date).date()
        e = pd.to_datetime(end_date).date()
    except Exception:
        console.print("Invalid --start/--end (YYYY-MM-DD)", style="red"); return
    if e < s:
        console.print("--end must be >= --start", style="red"); return
    inj_path = paths.data_raw / "injuries.csv"
    if not inj_path.exists():
        console.print(f"Injuries DB not found: {inj_path}", style="red"); return
    df = pd.read_csv(inj_path)
    if df is None or df.empty:
        console.print("Injuries DB is empty", style="red"); return
    # Normalize
    for c in ("team","player","status","injury"):
        if c in df.columns:
            df[c] = df[c].astype(str)
    df["status_norm"] = df.get("status", "").astype(str).str.upper()
    df["injury_norm"] = df.get("injury", "").astype(str).str.upper()
    # Season-long or indefinite classifier
    def _season_long(u: str, j: str) -> bool:
        u = str(u or "").upper(); j = str(j or "").upper()
        if ("OUT" in u) and ("SEASON" in u or "INDEFINITE" in u):
            return True
        if ("SEASON-ENDING" in u):
            return True
        if ("OUT FOR SEASON" in j) or ("SEASON-ENDING" in j) or ("OUT INDEFINITELY" in j):
            return True
        return False
    df["season_block"] = df.apply(lambda r: _season_long(r.get("status_norm"), r.get("injury_norm")), axis=1)
    block = df[df["season_block"]].copy()
    if block.empty:
        console.print("No season-long/indefinite injuries found in DB; nothing to backfill", style="yellow"); return
    from .teams import to_tricode as _to_tri
    block["team_tri"] = block["team"].map(lambda x: _to_tri(str(x)))
    block = block[block["team_tri"].astype(str).str.len() == 3]
    uniq = block.drop_duplicates(subset=["player","team_tri"]).copy()
    dates = pd.date_range(s, e, freq="D").date
    total_rows = 0
    for d in dates:
        out = paths.data_raw / f"injuries_overrides_{d}.csv"
        rows = uniq[["team_tri","player"]].rename(columns={"team_tri":"team"}).copy()
        rows["status"] = "OUT"
        rows.to_csv(out, index=False)
        total_rows += len(rows)
        console.print({"date": str(d), "rows": int(len(rows)), "output": str(out)})
    console.print({"dates": f"{start_date}..{end_date}", "files": int(len(dates)), "rows": int(total_rows)})


@cli.command("backfill-injuries-from-excluded")
@click.option("--src-date", type=str, required=True, help="Source date of injuries_excluded_YYYY-MM-DD.csv")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", "end_date", type=str, required=True, help="End date YYYY-MM-DD")
def backfill_injuries_from_excluded_cmd(src_date: str, start_date: str, end_date: str):
    """Generate per-day injuries_overrides_<date>.csv from an injuries_excluded_<src>.csv snapshot.

    This is a pragmatic backfill for short ranges: we take the excluded list (status OUT) from src-date diagnostics
    and apply it to each date in [start..end].
    """
    console.rule("Backfill Injury Overrides (from diagnostics)")
    try:
        s = pd.to_datetime(start_date).date(); e = pd.to_datetime(end_date).date(); sd = pd.to_datetime(src_date).date()
    except Exception:
        console.print("Invalid --src-date/--start/--end (YYYY-MM-DD)", style="red"); return
    if e < s:
        console.print("--end must be >= --start", style="red"); return
    src = paths.data_processed / f"injuries_excluded_{sd}.csv"
    if not src.exists():
        console.print(f"Diagnostics not found: {src}", style="red"); return
    df = pd.read_csv(src)
    if df is None or df.empty:
        console.print("Diagnostics file is empty", style="red"); return
    cols = set(c.lower() for c in df.columns)
    # Heuristics for column names
    team_col = next((c for c in df.columns if c.lower() in ("team","team_tri","team_abbr","teamabbr")), None)
    player_col = next((c for c in df.columns if c.lower() in ("player","player_name")), None)
    status_col = next((c for c in df.columns if c.lower() == "status"), None)
    if not (team_col and player_col and status_col):
        console.print("Diagnostics missing team/player/status columns", style="red"); return
    from .teams import to_tricode as _to_tri
    tmp = df.copy()
    tmp["team"] = tmp[team_col].astype(str).map(lambda x: _to_tri(str(x)))
    tmp["player"] = tmp[player_col].astype(str)
    tmp["status"] = tmp[status_col].astype(str).str.upper()
    tmp = tmp[tmp["status"] == "OUT"]
    tmp = tmp[tmp["team"].astype(str).str.len() == 3]
    tmp = tmp.drop_duplicates(subset=["team","player"])
    if tmp.empty:
        console.print("No OUT rows in diagnostics", style="yellow"); return
    dates = pd.date_range(s, e, freq="D").date
    total_rows = 0
    for d in dates:
        out = paths.data_raw / f"injuries_overrides_{d}.csv"
        rows = tmp[["team","player","status"]].copy()
        rows.to_csv(out, index=False)
        total_rows += len(rows)
        console.print({"date": str(d), "rows": int(len(rows)), "output": str(out)})
    console.print({"dates": f"{start_date}..{end_date}", "files": int(len(dates)), "rows": int(total_rows)})


@cli.command()
@click.option("--season", type=str, default="2025-26", show_default=True, help="NBA season string, e.g., 2025-26")
def fetch_rosters_cmd(season: str):
    """Fetch all team rosters for a season and save under data/processed/rosters_*.{csv,parquet}."""
    console.rule("Fetch Rosters")
    try:
        df = fetch_rosters(season=season)
        console.print({"rows": 0 if df is None else int(len(df)), "season": season})
    except Exception as e:
        console.print(f"Failed to fetch rosters: {e}", style="red")

@cli.command()
@click.option("--years", default=10, help="Number of past seasons to fetch")
@click.option("--with-periods/--no-periods", default=True, help="Fetch quarter/OT line scores (slower)")
@click.option("--verbose", is_flag=True, default=False, help="Print progress while fetching")
@click.option("--rate-delay", type=float, default=0.6, help="Delay between requests in seconds")
@click.option("--max-workers", type=int, default=1, help="Concurrent workers for period fetch (1 = serial)")
def fetch(years: int, with_periods: bool, verbose: bool, rate_delay: float, max_workers: int):
    """Fetch last N seasons from NBA Stats API"""
    console.rule("Fetch data")
    df = fetch_games_nba_api(last_n=years, with_periods=with_periods, verbose=verbose, rate_delay=rate_delay, max_workers=max_workers)
    console.print(f"Saved raw to {paths.data_raw}")
    console.print(df.head(3))


@cli.command("build-features")
def build_features_cmd():
    """Build features and save processed dataset"""
    console.rule("Build features")
    # Prefer CSV (engine-free) then parquet; NBA API outputs
    candidates = [
        paths.data_raw / "games_nba_api.csv",
        paths.data_raw / "games_nba_api.parquet",
    ]
    raw = next((p for p in candidates if p.exists()), None)
    if raw is None:
        console.print("No raw games file found. Run fetch first.", style="red")
        return
    # Read raw with fallback if parquet engine is missing
    if raw.suffix == ".parquet":
        try:
            df = pd.read_parquet(raw)
        except Exception:
            alt = paths.data_raw / "games_nba_api.csv"
            if alt.exists():
                df = pd.read_csv(alt)
            else:
                raise
    else:
        df = pd.read_csv(raw)
    feats = build_features(df)
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    # Always write CSV for maximum compatibility (no extra deps)
    out_csv = out_dir / "features.csv"
    feats.to_csv(out_csv, index=False)
    wrote_parquet = False
    try:
        out_pq = out_dir / "features.parquet"
        feats.to_parquet(out_pq, index=False)
        wrote_parquet = True
        console.print(f"Saved features to {out_pq}")
    except Exception as e:
        console.print(f"Parquet write skipped (engine missing): {e}", style="yellow")
        console.print(f"Saved features to {out_csv}")


@cli.command()
def train():
    """Train baseline models"""
    from .train import train_models  # Import here to avoid sklearn dependency at module level
    console.rule("Train models")
    feats_path = paths.data_processed / "features.parquet"
    df = pd.read_parquet(feats_path)
    metrics = train_models(df)
    console.print(metrics)


@cli.command("fetch-schedule")
@click.option("--season", "season", type=str, default="2025-26", show_default=True, help="Season string to export (currently only 2025-26 supported)")
def fetch_schedule_cmd(season: str):
    """Fetch the NBA schedule for 2025-26 from the public CDN and export JSON/CSV for the frontend."""
    console.rule("Fetch Schedule")
    if season != "2025-26":
        console.print("Only 2025-26 is supported right now; ignoring provided season.", style="yellow")
    try:
        df = fetch_schedule_2025_26()
    except Exception as e:
        console.print(f"Failed to fetch schedule: {e}", style="red"); return
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "schedule_2025_26.json"
    csv_path = out_dir / "schedule_2025_26.csv"
    try:
        # Save compact JSON list
        df.to_json(json_path, orient="records", date_format="iso")
        # Save CSV
        df.to_csv(csv_path, index=False)
        console.print(f"Saved schedule to {json_path} and {csv_path}")
        # Quick stats for sanity
        total_games = len(df)
        dates = df['date_utc'].dropna().unique()
        console.print({"rows": int(total_games), "unique_dates": int(len(dates))})
    except Exception as e:
        console.print(f"Failed to write schedule files: {e}", style="red")


@cli.command("fetch-rosters")
@click.option("--season", type=str, default="2025-26", show_default=True, help="Season string like 2025-26")
def fetch_rosters_cmd(season: str):
    """Fetch team rosters for the given season and save processed CSV/Parquet."""
    console.rule("Fetch Rosters")
    try:
        df = fetch_rosters(season=season)
    except Exception as e:
        console.print(f"Failed to fetch rosters: {e}", style="red"); return
    if df.empty:
        console.print("No roster data returned.", style="yellow"); return
    console.print({"rows": int(len(df)), "teams": int(df['TEAM_ID'].nunique())})


@cli.command("fetch-player-logs")
@click.option("--seasons", type=str, required=True, help="Comma-separated seasons like 2023-24,2024-25,2025-26")
def fetch_player_logs_cmd(seasons: str):
    """Fetch player game logs for the given seasons and save to processed folder."""
    console.rule("Fetch Player Logs")
    season_list = [s.strip() for s in seasons.split(',') if s.strip()]
    df = fetch_player_logs(season_list)
    if df.empty:
        console.print("No player logs returned.", style="yellow"); return
    console.print({"rows": int(len(df)), "players": int(df['PLAYER_ID'].nunique()), "games": int(df['GAME_ID'].nunique())})


@cli.command("fetch-pbp")
@click.option("--date", "date_str", type=str, required=True, help="Date YYYY-MM-DD (US/Eastern slate)")
@click.option("--include-live/--finals-only", "include_live", default=False, show_default=True, help="Include in-progress games as well (may be partial logs)")
@click.option("--rate-delay", type=float, default=0.35, show_default=True, help="Delay between game fetches (seconds)")
def fetch_pbp_cmd(date_str: str, include_live: bool, rate_delay: float):
    """Fetch PlayByPlay logs for all games on a date and write CSVs under data/processed.

    - Per-game files under data/processed/pbp/pbp_<gameId>.csv
    - Combined file under data/processed/pbp_<date>.csv
    """
    console.rule("Fetch PBP Logs")
    try:
        df, gids = fetch_pbp_for_date(date_str, only_final=(not include_live), rate_delay=rate_delay)
        console.print({"date": date_str, "games": len(gids), "rows": 0 if df is None else int(len(df))})
    except Exception as e:
        console.print(f"Failed to fetch PBP: {e}", style="red")

@cli.command("fetch-pbp-r")
@click.option("--date", "date_str", type=str, required=False, help="Target date YYYY-MM-DD")
@click.option("--start", type=str, required=False, help="Start date YYYY-MM-DD")
@click.option("--end", type=str, required=False, help="End date YYYY-MM-DD")
def fetch_pbp_r_cmd(date_str: str | None, start: str | None, end: str | None):
    """Fetch PBP using an R helper (hoopR preferred, nbastatR fallback).

    Requires R installed locally and either hoopR or nbastatR available in R.
    Writes per-game CSVs to data/processed/pbp and combined per-date CSVs.
    """
    console.rule("Fetch PBP via R")
    import subprocess, sys, shutil
    from pathlib import Path
    script = str(paths.root / "scripts" / "pbp_fetch_nbastatr.R")
    # Prefer Rscript on PATH; otherwise probe common install locations
    rscript = shutil.which("Rscript")
    if not rscript:
        candidates = []
        for base in (Path("C:/Program Files/R"), Path("C:/Program Files (x86)/R")):
            try:
                for p in base.glob("R-*/bin/Rscript.exe"):
                    candidates.append(str(p))
            except Exception:
                pass
        rscript = candidates[0] if candidates else None
    if not rscript:
        console.print("Rscript not found. Please install R or add Rscript.exe to PATH.", style="red")
        return
    args = [rscript, script]
    if date_str:
        args += ["--date", date_str]
    if start and end and not date_str:
        args += ["--start", start, "--end", end]
    try:
        r = subprocess.run(args, cwd=str(paths.root), capture_output=True, text=True, check=False)
        console.print({"cmd": " ".join(args), "returncode": r.returncode})
        if r.stdout:
            console.print(r.stdout)
        if r.stderr:
            console.print(r.stderr, style="yellow")
    except FileNotFoundError:
        console.print("Rscript not found. Please install R and ensure Rscript is on PATH.", style="red")
    except Exception as e:
        console.print(f"Failed to run R script: {e}", style="red")


@cli.command("fetch-boxscores")
@click.option("--date", "date_str", type=str, required=True, help="Date YYYY-MM-DD (US/Eastern slate)")
@click.option("--include-live/--finals-only", "include_live", default=False, show_default=True, help="Include in-progress games as well (may be partial)")
@click.option("--rate-delay", type=float, default=0.35, show_default=True, help="Delay between game fetches (seconds)")
def fetch_boxscores_cmd(date_str: str, include_live: bool, rate_delay: float):
    """Fetch BoxScoreTraditionalV3 player stats for all games on a date.

    - Per-game files under data/processed/boxscores/boxscore_<gameId>.csv
    - Combined file under data/processed/boxscores_<date>.csv
    """
    console.rule("Fetch Boxscores")
    try:
        df, gids = fetch_boxscores_for_date(date_str, only_final=(not include_live), rate_delay=rate_delay)
        console.print({"date": date_str, "games": len(gids), "rows": 0 if df is None else int(len(df))})
    except Exception as e:
        console.print(f"Failed to fetch boxscores: {e}", style="red")


@cli.command("backfill-pbp")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", "end_date", type=str, required=True, help="End date YYYY-MM-DD")
@click.option("--finals-only/--include-live", "finals_only", default=True, show_default=True, help="Fetch only final games (recommended)")
@click.option("--rate-delay", type=float, default=0.35, show_default=True, help="Delay between requests")
def backfill_pbp_cmd(start_date: str, end_date: str, finals_only: bool, rate_delay: float):
    """Backfill PBP logs over a date range, skipping dates that already exist."""
    console.rule("Backfill PBP")
    try:
        df = backfill_pbp(start_date, end_date, only_final=finals_only, rate_delay=rate_delay)
        rows = 0 if df is None else int(len(df))
        console.print({"start": start_date, "end": end_date, "rows": rows})
    except Exception as e:
        console.print(f"Failed to backfill PBP: {e}", style="red")


@cli.command("backfill-boxscores")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", "end_date", type=str, required=True, help="End date YYYY-MM-DD")
@click.option("--finals-only/--include-live", "finals_only", default=True, show_default=True, help="Fetch only final games (recommended)")
@click.option("--rate-delay", type=float, default=0.35, show_default=True, help="Delay between requests")
def backfill_boxscores_cmd(start_date: str, end_date: str, finals_only: bool, rate_delay: float):
    """Backfill boxscores over a date range, skipping dates that already exist."""
    console.rule("Backfill Boxscores")
    try:
        df = backfill_boxscores(start_date, end_date, only_final=finals_only, rate_delay=rate_delay)
        rows = 0 if df is None else int(len(df))
        console.print({"start": start_date, "end": end_date, "rows": rows})
    except Exception as e:
        console.print(f"Failed to backfill boxscores: {e}", style="red")


@cli.command("backfill-scoreboard")
@click.option("--seasons", type=str, required=True, help="Comma-separated season end years (e.g., 2018,2019,2020)")
@click.option("--rate-delay", type=float, default=0.8, help="Delay between day requests in seconds")
@click.option("--day-limit", type=int, default=None, help="Process at most this many days per season (for smoke tests)")
@click.option("--resume-file", type=click.Path(dir_okay=False, writable=True), default=str(paths.data_raw / "_scoreboard_resume.json"), help="Path to JSON resume file")
@click.option("--verbose", is_flag=True, default=False, help="Print progress while backfilling")
def backfill_scoreboard_cmd(seasons: str, rate_delay: float, day_limit: int | None, resume_file: str, verbose: bool):
    """Backfill games (with per-periods) via ScoreboardV2 day-by-day with resume support."""
    console.rule("Backfill (ScoreboardV2)")
    try:
        season_list = [int(s.strip()) for s in seasons.split(',') if s.strip()]
    except Exception:
        console.print("Invalid seasons list", style="red"); return
    df = backfill_scoreboard(seasons=season_list, rate_delay=rate_delay, verbose=verbose, day_limit=day_limit, resume_file=resume_file)
    console.print(f"Backfill complete. Raw rows={len(df)}")


@cli.command("finals-export")
@click.option("--date", "date_str", type=str, required=False, help="Single date YYYY-MM-DD")
@click.option("--since", "since_str", type=str, required=False, help="Start date YYYY-MM-DD (inclusive)")
@click.option("--until", "until_str", type=str, required=False, help="End date YYYY-MM-DD (inclusive; defaults to today)")
def finals_export_cmd(date_str: str | None, since_str: str | None, until_str: str | None):
    """Export final scores to data/processed/finals_<date>.csv for a date or date range.

    Source order: nba_api ScoreboardV2 -> NBA CDN -> ESPN, with +/- 1 day fallback.
    """
    console.rule("Export Finals")
    dates: list[str] = []
    if date_str:
        dates = [date_str]
    else:
        try:
            from datetime import datetime as _dt, timedelta as _td
            if not since_str:
                console.print("Provide --date or --since/--until", style="red"); return
            if not until_str:
                until_str = _dt.utcnow().date().isoformat()
            ds = _dt.strptime(since_str, "%Y-%m-%d").date()
            de = _dt.strptime(until_str, "%Y-%m-%d").date()
            if de < ds:
                ds, de = de, ds
            cur = ds
            while cur <= de:
                dates.append(cur.isoformat())
                cur += _td(days=1)
        except Exception as e:
            console.print(f"Invalid dates: {e}", style="red"); return
    out = []
    for d in dates:
        try:
            path, n = write_finals_csv(d)
            out.append({"date": d, "path": str(path), "rows": int(n)})
        except Exception as e:
            out.append({"date": d, "error": str(e)})
    console.print({"count": len(out), "items": out})


@cli.command("build-league-status")
@click.option("--date", "date_str", type=str, required=True, help="Date YYYY-MM-DD to build league_status_<date>.csv")
def build_league_status_cmd(date_str: str):
    """Build unified league roster+injury status for a date and write data/processed/league_status_<date>.csv."""
    console.rule("Build League Status")
    try:
        df = build_league_status(date_str)
        rows = 0 if df is None else int(len(df))
        out_path = (paths.data_processed / f"league_status_{date_str}.csv")
        exists = out_path.exists()
        console.print({"date": date_str, "rows": rows, "path": str(out_path), "wrote_file": bool(exists)})
    except Exception as e:
        console.print(f"Failed to build league status: {e}", style="red")


@cli.command("audit-rosters")
@click.option("--date", "date_str", type=str, required=True, help="Date YYYY-MM-DD to audit league_status vs boxscores")
def audit_rosters_cmd(date_str: str):
    """Audit roster team assignment accuracy by comparing league_status_<date>.csv vs boxscores_<date>.csv.

    Writes data/processed/roster_audit_<date>.csv and prints summary metrics.
    """
    console.rule("Audit Rosters")
    try:
        audit, summary = audit_roster_for_date(date_str)
        if audit is None or audit.empty:
            console.print({"date": date_str, "rows": 0, "summary": summary}, style="yellow")
            return
        out_path = paths.data_processed / f"roster_audit_{date_str}.csv"
        console.print({"date": date_str, "rows": int(len(audit)), "output": str(out_path), **summary})
    except Exception as e:
        console.print(f"Failed to audit rosters: {e}", style="red")


@cli.command("backfill-player-props")
@click.option("--date", type=str, required=True, help="Snapshot date YYYY-MM-DD (UTC) for player props")
@click.option("--markets", type=str, required=False, help="Comma-separated OddsAPI player markets (default common set)")
@click.option("--mode", type=click.Choice(["auto","historical","current"]), default="auto", show_default=True, help="Fetch mode: historical uses snapshots by timestamp; current pulls current event odds for the day.")
@click.option("--api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key (or set env ODDS_API_KEY)")
def backfill_player_props_cmd(date: str, markets: str | None, mode: str, api_key: str | None):
    """Fetch OddsAPI player props snapshot for a date and save to data/raw.

    Requires env var ODDS_API_KEY or configure via code.
    """
    console.rule("Backfill Player Props (OddsAPI)")
    if not api_key:
        # Fallback to .env file
        api_key = _load_dotenv_key("ODDS_API_KEY")
    if not api_key:
        console.print("Provide --api-key, set ODDS_API_KEY env, or add to .env at repo root.", style="red"); return
    cfg = OddsApiConfig(api_key=api_key)
    import datetime as _dt
    try:
        d = _dt.datetime.strptime(date, "%Y-%m-%d")
    except Exception:
        console.print("Invalid date; expected YYYY-MM-DD", style="red"); return
    mkts = [m.strip() for m in markets.split(',')] if markets else None
    # Modes: historical, current, or auto (try historical then current)
    from pathlib import Path as _P
    out_parq = paths.data_raw / "odds_nba_player_props.parquet"
    out_csv = paths.data_raw / "odds_nba_player_props.csv"
    def save(df):
        if df is None or df.empty:
            return 0
        df.to_csv(out_csv, index=False)
        try:
            df.to_parquet(out_parq, index=False)
        except Exception:
            pass
        return len(df)
    rows = 0
    if mode in ("historical","auto"):
        dfh = backfill_player_props(cfg, date=d, markets=mkts, verbose=True)
        rows += save(dfh)
    if rows == 0 and mode in ("current","auto"):
        # Use current event odds for the same calendar date
        dfc = fetch_player_props_current(cfg, date=d, markets=mkts, verbose=True)
        rows += save(dfc)
    console.print({"rows": int(rows)})


@cli.command("fetch-prop-actuals")
@click.option("--date", "date_str", type=str, required=False, help="Single date YYYY-MM-DD")
@click.option("--start", "start_str", type=str, required=False, help="Start date YYYY-MM-DD")
@click.option("--end", "end_str", type=str, required=False, help="End date YYYY-MM-DD")
def fetch_prop_actuals_cmd(date_str: str | None, start_str: str | None, end_str: str | None):
    """Fetch player prop actuals (PTS, REB, AST, 3PM, PRA) via nbastatR and upsert to processed files.

    Requires R and the nbastatR package installed. On Windows, ensure Rscript.exe is on PATH.
    """
    console.rule("Fetch Prop Actuals (nbastatR)")
    # First attempt nbastatR via Rscript; if R not available, fall back to nba_api
    try:
        if date_str:
            df = fetch_prop_actuals_via_nbastatr(date=date_str)
        else:
            if not start_str or not end_str:
                console.print("Provide --date or both --start and --end", style="red"); return
            df = fetch_prop_actuals_via_nbastatr(start=start_str, end=end_str)
    except (FileNotFoundError, RuntimeError):
        # Fallbacks (single-date only): try NBA CDN first, then nba_api
        if not date_str:
            console.print("R not available and fallback supports only --date.", style="red"); return
        console.print(f"nbastatR unavailable, trying NBA liveData CDN for {date_str}...", style="yellow")
        try:
            df = fetch_prop_actuals_via_nba_cdn(date_str)
        except Exception as e_cdn:
            console.print(f"NBA CDN fallback failed: {e_cdn}; trying nba_api...", style="yellow")
            try:
                df = fetch_prop_actuals_via_nbaapi(date_str)
            except Exception as ee:
                console.print(f"nba_api fallback failed: {ee}", style="red"); return
    if df is None or df.empty:
        console.print("No rows returned.", style="yellow"); return
    out_path = upsert_props_actuals(df)
    console.print({"rows": int(len(df)), "output": str(out_path)})
    # Also write simple recon_props_{date}.csv for the frontend if single date
    if date_str:
        try:
            dd = pd.to_datetime(date_str).date()
            small = df.copy()
            small["date"] = pd.to_datetime(small["date"]).dt.date
            small = small[small["date"] == dd]
            keep = [c for c in ["date","game_id","player_id","player_name","team_abbr","pts","reb","ast","threes","pra"] if c in small.columns]
            out_csv = paths.data_processed / f"recon_props_{date_str}.csv"
            small[keep].to_csv(out_csv, index=False)
            console.print({"recon_props": str(out_csv), "rows": int(len(small))})
        except Exception:
            pass


@cli.command("build-props-features")
def build_props_features_cmd():
    """Build per-player props features (rolling windows) from player logs and save to processed folder."""
    console.rule("Build Props Features")
    try:
        df = build_props_features()
    except Exception as e:
        console.print(f"Failed to build props features: {e}", style="red"); return
    console.print({"rows": int(len(df))})


@cli.command("train-props")
@click.option("--alpha", type=float, default=1.0, show_default=True, help="Ridge regularization strength")
def train_props_cmd(alpha: float):
    """Train props regression models for PTS/REB/AST/3PM/PRA and save to models folder."""
    from .props_train import train_props_models  # Import here to avoid sklearn dependency
    console.rule("Train Props Models")
    try:
        train_props_models(alpha=alpha)
        console.print("Saved props models and feature columns.")
    except FileNotFoundError as e:
        console.print(str(e), style="red")
    except Exception as e:
        console.print(f"Failed to train props models: {e}", style="red")


@cli.command("train-props-pure")
@click.option("--targets", type=str, default="t_stl,t_blk,t_tov", show_default=True, help="Comma-separated targets to train with pure linear fallback")
@click.option("--alpha", type=float, default=1.0, show_default=True, help="Ridge regularization strength")
def train_props_pure_cmd(targets: str, alpha: float):
    """Train pure linear (numpy ridge) fallback models for selected targets (no sklearn)."""
    console.rule("Train Props (Pure Linear)")
    try:
        tgt_list = [t.strip() for t in targets.split(',') if t.strip()]
        out = train_linear_props_models(targets=tgt_list, alpha=alpha)
        console.print({"saved": str(out), "targets": tgt_list})
    except Exception as e:
        console.print(f"Failed to train pure linear models: {e}", style="red")


@cli.command("export-props-onnx")
@click.option("--targets", type=str, default="t_stl,t_blk,t_tov", show_default=True, help="Comma-separated targets to export from pure linear models")
def export_props_onnx_cmd(targets: str):
    """Export pure-linear models (MatMul + Add) to ONNX for NPU path."""
    console.rule("Export Props ONNX (from pure-linear)")
    try:
        tgt_list = [t.strip() for t in targets.split(',') if t.strip()]
        out = export_linear_to_onnx(tgt_list)
        console.print({"exported": {k: str(v) for k, v in out.items()}})
    except Exception as e:
        console.print(f"Failed to export ONNX: {e}", style="red")


@cli.command("props-backtest")
@click.option("--targets", type=str, default="t_stl,t_blk,t_tov", show_default=True, help="Comma-separated targets to backtest (use t_* names)")
@click.option("--start", type=str, required=False, help="Start date YYYY-MM-DD (if available in features)")
@click.option("--end", type=str, required=False, help="End date YYYY-MM-DD (if available in features)")
def props_backtest_cmd(targets: str, start: str | None, end: str | None):
    """Quick backtest of pure-linear props models on historical features with basic metrics."""
    console.rule("Props Backtest (pure-linear)")
    tgt_list = [t.strip() for t in targets.split(',') if t.strip()]
    try:
        df = backtest_linear_props(tgt_list, start=start, end=end)
        rows = 0 if df is None else int(len(df))
        console.print({"rows": rows})
        if rows:
            console.print(df)
    except Exception as e:
        console.print(f"Failed to run backtest: {e}", style="red")


@cli.command("predict-props")
@click.option("--date", "date_str", type=str, required=True, help="Prediction date YYYY-MM-DD (features built up to the day before)")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path (default props_predictions_YYYY-MM-DD.csv)")
@click.option("--slate-only/--no-slate-only", default=True, show_default=True, help="Filter predictions to teams on the scoreboard slate and add opponent/home flags")
@click.option("--calibrate/--no-calibrate", default=True, show_default=True, help="Apply rolling bias calibration from recent recon vs predictions")
@click.option("--calib-window", type=int, default=7, show_default=True, help="Lookback days for calibration window (excludes today)")
@click.option("--use-pure-onnx/--no-use-pure-onnx", default=True, show_default=True, help="Use pure ONNX models with NPU acceleration (no sklearn dependency)")
def predict_props_cmd(date_str: str, out_path: str | None, slate_only: bool, calibrate: bool, calib_window: int, use_pure_onnx: bool):
    """Predict player props for a slate date using rolling-history models.

    Note: This version builds features from history only and returns predictions for all players seen in logs. A later enhancement can filter to the actual slate roster for the date and merge odds.
    """
    console.rule("Predict Props")
    
    # Build features (no sklearn required). If a pure builder exists, use it; else use standard builder.
    try:
        if use_pure_onnx:
            try:
                from .props_features_pure import build_features_for_date_pure
                console.print("Building features with pure method (no sklearn)...", style="cyan")
                feats = build_features_for_date_pure(date_str)
            except ModuleNotFoundError:
                # Fall back to standard feature builder (still sklearn-free)
                feats = build_features_for_date(date_str)
            except Exception as e:
                # Do not fall back to sklearn; fail fast in ONNX-only mode
                console.print(f"Failed to build features in pure mode: {e}", style="red"); return
        else:
            feats = build_features_for_date(date_str)
    except Exception as e:
        console.print(f"Failed to build features for {date_str}: {e}", style="red"); return
    # League-wide team cleanup (before slate filter): fix feats['team'] using latest roster, overrides, NBA API fallback; also integrate league_status
    try:
        import re as _re
        from .config import paths as _paths
        from .teams import to_tricode as _to_tri
        import pandas as _pd
        from pathlib import Path as _Path
        # Load latest roster file (avoid cross-season contamination)
        def _load_latest_roster_df():
            try:
                proc = _paths.data_processed
                files = sorted(proc.glob("rosters_*.csv"))
                if not files:
                    return _pd.DataFrame(columns=["PLAYER","PLAYER_ID","TEAM_ABBREVIATION"])
                p = files[-1]
                df = _pd.read_csv(p)
                if df is None or df.empty:
                    return _pd.DataFrame(columns=["PLAYER","PLAYER_ID","TEAM_ABBREVIATION"])
                cols = {c.upper(): c for c in df.columns}
                if "TEAM_ABBREVIATION" not in cols:
                    df["TEAM_ABBREVIATION"] = None
                return df
            except Exception:
                return _pd.DataFrame(columns=["PLAYER","PLAYER_ID","TEAM_ABBREVIATION"])
        # Normalize name
        _SUFFIXES = {"jr","sr","ii","iii","iv","v"}
        def _norm_name_key(s: str) -> str:
            s = (s or "").strip().lower()
            s = _re.sub(r"[^a-z0-9\s]", "", s)
            s = _re.sub(r"\s+", " ", s).strip()
            toks = [t for t in s.split(" ") if t and t not in _SUFFIXES]
            return " ".join(toks)
        # Build pid->team and name->team from latest roster
        pid_to_tri, name_to_tri = {}, {}
        rost = _load_latest_roster_df()
        if not rost.empty:
            rcols = {c.upper(): c for c in rost.columns}
            pid_col = rcols.get("PLAYER_ID"); name_col = rcols.get("PLAYER"); tri_col = rcols.get("TEAM_ABBREVIATION")
            if pid_col and name_col and tri_col:
                tmp = rost[[pid_col, name_col, tri_col]].copy()
                tmp[tri_col] = tmp[tri_col].astype(str).map(lambda x: (_to_tri(str(x)) or str(x).strip().upper()))
                for _, r in tmp.iterrows():
                    try:
                        tri = str(r[tri_col]).strip().upper()
                        if not tri:
                            continue
                        try:
                            pid = int(_pd.to_numeric(r[pid_col], errors="coerce"))
                            pid_to_tri[pid] = tri
                        except Exception:
                            pass
                        try:
                            nkey = _norm_name_key(str(r[name_col]))
                            if nkey:
                                name_to_tri[nkey] = tri
                        except Exception:
                            pass
                    except Exception:
                        continue
        # Apply global roster overrides if present
        try:
            ov = _paths.root / "data" / "overrides" / "roster_overrides.csv"
            if ov.exists():
                odf = _pd.read_csv(ov)
                if odf is not None and not odf.empty:
                    ocols = {c.upper(): c for c in odf.columns}
                    opid = ocols.get("PLAYER_ID"); oname = ocols.get("PLAYER"); otri = ocols.get("TEAM_ABBREVIATION")
                    if otri and (opid or oname):
                        tmpo = odf[[c for c in [opid, oname, otri] if c]].copy()
                        tmpo[otri] = tmpo[otri].astype(str).map(lambda x: (_to_tri(str(x)) or str(x).strip().upper()))
                        for _, r in tmpo.iterrows():
                            try:
                                tri = str(r[otri]).strip().upper()
                                if not tri:
                                    continue
                                if opid and _pd.notna(r.get(opid)):
                                    try:
                                        pid = int(_pd.to_numeric(r[opid], errors="coerce"))
                                        pid_to_tri[pid] = tri
                                    except Exception:
                                        pass
                                if oname and _pd.notna(r.get(oname)):
                                    try:
                                        nkey = _norm_name_key(str(r[oname]))
                                        if nkey:
                                            name_to_tri[nkey] = tri
                                    except Exception:
                                        pass
                            except Exception:
                                continue
        except Exception:
            pass
        # Load cache and resolver using nba_api CommonPlayerInfo
        cache_p = _paths.data_processed / "player_team_cache.csv"
        cache = {}
        if cache_p.exists():
            try:
                cdf = _pd.read_csv(cache_p)
                if cdf is not None and not cdf.empty and {"player_id","team"}.issubset(set(cdf.columns)):
                    for _, r in cdf.iterrows():
                        try:
                            cache[int(_pd.to_numeric(r["player_id"], errors="coerce"))] = str(r["team"]).strip().upper()
                        except Exception:
                            continue
            except Exception:
                pass
        def _resolve_pid_team(pid: int) -> str | None:
            if pid in pid_to_tri:
                return pid_to_tri[pid]
            if pid in cache:
                return cache[pid]
            # NBA API call with small timeout and graceful failure
            try:
                from nba_api.stats.endpoints import commonplayerinfo as _cpi
                resp = _cpi.CommonPlayerInfo(player_id=int(pid), timeout=10)
                nd = resp.get_normalized_dict()
                rows = nd.get("CommonPlayerInfo", [])
                if rows:
                    tri = str(rows[0].get("TEAM_ABBREVIATION") or "").strip().upper()
                    if tri:
                        cache[pid] = tri
                        return tri
            except Exception:
                return None
            return None
        # Apply mapping to feats
        if not feats.empty and ("team" in feats.columns):
            try:
                feats = feats.copy()
                # Prefer pid, then name
                if "player_id" in feats.columns:
                    feats["player_id"] = _pd.to_numeric(feats["player_id"], errors="coerce")
                if "player_name" in feats.columns:
                    feats["_name_key"] = feats["player_name"].astype(str).map(_norm_name_key)
                # Resolve team
                def _team_row(row):
                    # roster override via pid
                    pid = row.get("player_id") if "player_id" in row else None
                    tri = None
                    if pid is not None and _pd.notna(pid):
                        tri = _resolve_pid_team(int(pid))
                    if not tri and "_name_key" in row and row.get("_name_key"):
                        tri = name_to_tri.get(row.get("_name_key"))
                    if not tri:
                        # keep existing but normalize
                        tri = _to_tri(str(row.get("team"))) or str(row.get("team") or "").strip().upper()
                    return tri
                feats["team"] = feats.apply(_team_row, axis=1)
                # Clean up
                feats.drop(columns=["_name_key"], inplace=True, errors="ignore")
            except Exception:
                pass
        # Persist cache
        try:
            if cache:
                rows = [(k, v) for k, v in cache.items()]
                _pd.DataFrame(rows, columns=["player_id","team"]).to_csv(cache_p, index=False)
        except Exception:
            pass
        # Integrate league_status (ensures FA/non-playing dropped and team corrected)
        try:
            ls = build_league_status(date_str)
            if ls is not None and not ls.empty and {"player_id","team","injury_status","team_on_slate","playing_today"}.issubset(set(ls.columns)) and ("player_id" in feats.columns):
                feats = feats.copy()
                feats["player_id"] = _pd.to_numeric(feats["player_id"], errors="coerce")
                ls2 = ls[["player_id","team","injury_status","team_on_slate","playing_today"]].copy()
                ls2["player_id"] = _pd.to_numeric(ls2["player_id"], errors="coerce")
                feats = feats.merge(ls2, on="player_id", how="left", suffixes=("","_ls"))
                feats["team"] = feats["team"].where(feats["team"].astype(str).str.len()>0, feats["team_ls"]).fillna(feats["team_ls"])
                def _ok(row):
                    t = str(row.get("team") or "").strip().upper()
                    if not t:
                        return False
                    pt = row.get("playing_today")
                    if _pd.notna(pt) and (pt is False):
                        return False
                    return True
                feats = feats[feats.apply(_ok, axis=1)].drop(columns=[c for c in feats.columns if c.endswith("_ls")], errors="ignore")
        except Exception as _ee:
            pass
    except Exception:
        pass

    # Optional slate filter using ScoreboardV2, with fallback to OddsAPI game odds
    if slate_only:
        slate_applied = False
        # Primary: NBA ScoreboardV2
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
            nd = sb.get_normalized_dict()
            gh = pd.DataFrame(nd.get("GameHeader", []))
            ls = pd.DataFrame(nd.get("LineScore", []))
            if not gh.empty and not ls.empty:
                ls_cols = {c.upper(): c for c in ls.columns}
                if {"TEAM_ID","TEAM_ABBREVIATION"}.issubset(ls_cols.keys()):
                    team_map = {}
                    for _, r in ls.iterrows():
                        try:
                            team_map[int(r[ls_cols["TEAM_ID"]])] = str(r[ls_cols["TEAM_ABBREVIATION"]]).upper()
                        except Exception:
                            continue
                    gh_cols = {c.upper(): c for c in gh.columns}
                    if {"HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh_cols.keys()):
                        games = []
                        for _, g in gh.iterrows():
                            try:
                                hid = int(g[gh_cols["HOME_TEAM_ID"]]); vid = int(g[gh_cols["VISITOR_TEAM_ID"]])
                                h = team_map.get(hid); v = team_map.get(vid)
                                if h and v:
                                    games.append({"team": h, "opponent": v, "home": True})
                                    games.append({"team": v, "opponent": h, "home": False})
                            except Exception:
                                continue
                        slate = pd.DataFrame(games)
                        if not slate.empty and "team" in feats.columns:
                            feats["team"] = feats["team"].astype(str).str.upper()
                            feats = feats.merge(slate, on="team", how="inner")
                            slate_applied = True
        except Exception:
            pass
        # Fallback: use standardized OddsAPI game odds CSV written earlier in the pipeline
        if not slate_applied:
            try:
                from .config import paths as _paths
                from .teams import to_tricode as _to_tri
                go_path = _paths.data_processed / f"game_odds_{date_str}.csv"
                if go_path.exists():
                    go = pd.read_csv(go_path)
                    if not go.empty:
                        games = []
                        # Support both visitor_team and away_team column names
                        home_col = "home_team" if "home_team" in go.columns else None
                        away_col = "visitor_team" if "visitor_team" in go.columns else ("away_team" if "away_team" in go.columns else None)
                        if home_col and away_col:
                            for _, r in go.iterrows():
                                try:
                                    h_raw = str(r.get(home_col) or "").strip()
                                    a_raw = str(r.get(away_col) or "").strip()
                                    h = _to_tri(h_raw)
                                    a = _to_tri(a_raw)
                                    if h and a:
                                        games.append({"team": h, "opponent": a, "home": True})
                                        games.append({"team": a, "opponent": h, "home": False})
                                except Exception:
                                    continue
                        slate = pd.DataFrame(games)
                        if not slate.empty and "team" in feats.columns:
                            feats["team"] = feats["team"].astype(str).str.upper()
                            feats = feats.merge(slate, on="team", how="inner")
                            slate_applied = True
            except Exception:
                pass
    
    # Optional: exclude clearly inactive players based on ESPN injuries DB (OUT)
    try:
        import pandas as _pd
        from .teams import to_tricode as _to_tri
        inj_path = paths.data_raw / "injuries.csv"
        if not feats.empty:
            # Consistent name normalization for matching injuries -> features
            import re as _re
            _SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
            def _norm_name_key(s: str) -> str:
                s = (s or "").strip().lower()
                # remove punctuation
                s = _re.sub(r"[^a-z0-9\s]", "", s)
                # collapse whitespace
                s = _re.sub(r"\s+", " ", s).strip()
                # drop common suffix tokens
                toks = [t for t in s.split(" ") if t and t not in _SUFFIXES]
                return " ".join(toks)
            day_out = _pd.DataFrame()
            # Primary source: scraped injuries DB
            if inj_path.exists():
                inj = _pd.read_csv(inj_path)
                if not inj.empty and {"team","player","status","date"}.issubset(set(inj.columns)):
                    # Consider the latest status up to the target date, not just exact date rows
                    inj["date"] = _pd.to_datetime(inj["date"], errors="coerce").dt.date
                    cutoff = _pd.to_datetime(date_str).date()
                    inj = inj[inj["date"].notna()]
                    inj = inj[inj["date"] <= cutoff].copy()
                    if not inj.empty:
                        # take latest per player+team
                        inj = inj.sort_values(["date"])  # ascending so tail(1) gives latest
                        grp_cols = [c for c in ["player","team"] if c in inj.columns]
                        if not grp_cols:
                            grp_cols = ["player"]
                        latest = inj.groupby(grp_cols, as_index=False).tail(1)
                        # Ensure a standalone copy before column assignments to avoid SettingWithCopyWarning
                        latest = latest.copy()
                        latest["team_tri"] = latest["team"].astype(str).map(lambda x: _to_tri(str(x)))
                        latest["player_key"] = latest["player"].astype(str).map(_norm_name_key)
                        latest["status_norm"] = latest["status"].astype(str).str.upper()
                        # Exclusion logic: exact statuses plus season-long/indefinite phrasing
                        def _excluded_status(u: str) -> bool:
                            try:
                                u = str(u).upper()
                            except Exception:
                                return False
                            if u in {"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}:
                                return True
                            if ("OUT" in u and ("SEASON" in u or "INDEFINITE" in u)) or ("SEASON-ENDING" in u):
                                return True
                            return False
                        day_out = latest[latest["status_norm"].map(_excluded_status)].copy()
            # Manual override per-day file support
            try:
                override_path = paths.data_raw / f"injuries_overrides_{date_str}.csv"
                if override_path.exists():
                    ovr = _pd.read_csv(override_path)
                    if not ovr.empty and {"team","player","status"}.issubset(set(ovr.columns)):
                        ovr = ovr.copy()
                        ovr["team_tri"] = ovr["team"].astype(str).map(lambda x: _to_tri(str(x)))
                        ovr["player_key"] = ovr["player"].astype(str).str.strip().str.lower()
                        # Apply same exclusion logic to overrides
                        ovr_status = ovr["status"].astype(str).str.upper()
                        ovr_mask = ovr_status.isin({"OUT","DOUBTFUL","SUSPENDED","INACTIVE","REST"}) | \
                                   (ovr_status.str.contains("OUT", na=False) & (ovr_status.str.contains("SEASON", na=False) | ovr_status.str.contains("INDEFINITE", na=False))) | \
                                   (ovr_status.str.contains("SEASON-ENDING", na=False))
                        ovr_out = ovr[ovr_mask]
                        day_out = _pd.concat([day_out, ovr_out], ignore_index=True) if not day_out.empty else ovr_out
            except Exception:
                pass

            if not day_out.empty and {"team","player_name"}.issubset(set(feats.columns)):
                tmp = feats.copy()
                tmp["team_tri"] = tmp["team"].astype(str).map(lambda x: _to_tri(str(x)))
                tmp["player_key"] = tmp["player_name"].astype(str).map(_norm_name_key)
                ban = set((str(r.get("player_key")), str(r.get("team_tri"))) for _, r in day_out.iterrows())
                before = len(tmp)
                tmp = tmp[~tmp.apply(lambda r: (str(r.get("player_key")), str(r.get("team_tri"))) in ban, axis=1)]
                if len(tmp) < before:
                    console.print(f"Filtered OUT players by injuries/overrides: removed {before-len(tmp)} rows", style="yellow")
                # Persist a small diagnostics file to aid debugging
                try:
                    diag_cols = [c for c in ["date","team","team_tri","player","status"] if c in day_out.columns]
                    diag = day_out[diag_cols].copy() if diag_cols else day_out.copy()
                    out_diag = paths.data_processed / f"injuries_excluded_{date_str}.csv"
                    diag.to_csv(out_diag, index=False)
                except Exception:
                    pass
                feats = tmp.drop(columns=["team_tri","player_key"], errors="ignore")
    except Exception as _e:
        console.print(f"Injury filter skipped: {_e}", style="yellow")

    # Enforce slate participants via OddsAPI player props (today-only hard filter to remove misassigned players)
    try:
        from .config import paths as _paths
        import pandas as _pd
        import re as _re
        raw_props = _paths.data_raw / f"odds_nba_player_props_{date_str}.csv"
        if raw_props.exists() and not feats.empty and ("player_name" in feats.columns):
            props = _pd.read_csv(raw_props)
            if props is not None and not props.empty:
                name_col = next((c for c in props.columns if c.lower() in ("player","player_name","name")), None)
                if name_col:
                    def _norm(s: str) -> str:
                        s = (s or "").strip().lower()
                        s = _re.sub(r"[^a-z0-9\s]", "", s)
                        s = _re.sub(r"\s+", " ", s).strip()
                        toks = [t for t in s.split(" ") if t not in {"jr","sr","ii","iii","iv","v"}]
                        return " ".join(toks)
                    pset = set(props[name_col].astype(str).map(_norm).tolist())
                    feats["_pkey"] = feats["player_name"].astype(str).map(_norm)
                    before = len(feats)
                    feats = feats[feats["_pkey"].isin(pset)].drop(columns=["_pkey"], errors="ignore")
                    removed = before - len(feats)
                    if removed > 0:
                        console.print(f"Pruned non-participants via OddsAPI props: removed {removed} rows", style="yellow")
    except Exception as _e:
        console.print(f"Participants filter skipped: {_e}", style="yellow")

    # Sanity filter using latest team from player logs (and name->id fallback):
    # drop rows whose last known team is neither their current team nor one of today's slate teams
    try:
        from .config import paths as _paths
        from .teams import to_tricode as _to_tri
        import pandas as _pd
        if not feats.empty and ("player_id" in feats.columns) and ("team" in feats.columns):
            logs_p = _paths.data_processed / "player_logs.csv"
            if logs_p.exists():
                lg = _pd.read_csv(logs_p)
                if lg is not None and not lg.empty:
                    c = {c.upper(): c for c in lg.columns}
                    pid_c = c.get("PLAYER_ID"); team_c = c.get("TEAM_ABBREVIATION"); date_c = c.get("GAME_DATE") or c.get("GAME_DATE_EST")
                    if pid_c and team_c:
                        tmp = lg.copy()
                        if date_c:
                            tmp[date_c] = _pd.to_datetime(tmp[date_c], errors="coerce")
                            tmp = tmp.sort_values([pid_c, date_c])
                            last = tmp.groupby(pid_c, as_index=False).tail(1)
                        else:
                            last = tmp.drop_duplicates(subset=[pid_c], keep="last")
                        last = last[[pid_c, team_c]].copy().rename(columns={pid_c:"player_id", team_c:"last_team"})
                        last["player_id"] = _pd.to_numeric(last["player_id"], errors="coerce")
                        last["last_team"] = last["last_team"].astype(str).map(lambda x: (_to_tri(str(x)) or str(x).strip().upper()))
                        feats = feats.merge(last, on="player_id", how="left")
                        # Fallback: resolve missing last_team by mapping name -> player_id via nba_api static players
                        if "last_team" in feats.columns:
                            missing = feats["last_team"].isna()
                        else:
                            feats["last_team"] = _pd.NA
                            missing = feats["last_team"].isna()
                        if missing.any():
                            try:
                                from nba_api.stats.static import players as _static_players
                                plist = _static_players.get_players()
                                if plist:
                                    pdf = _pd.DataFrame(plist)
                                    cpl = {c.lower(): c for c in pdf.columns}
                                    if cpl.get("id") and cpl.get("full_name"):
                                        pdf = pdf.rename(columns={cpl["id"]: "pid", cpl["full_name"]: "pname"})
                                        def _norm(s: str) -> str:
                                            import re as __re
                                            s = (s or "").strip().lower()
                                            s = __re.sub(r"[^a-z0-9\s]", "", s)
                                            s = __re.sub(r"\s+", " ", s).strip()
                                            toks = [t for t in s.split(" ") if t not in {"jr","sr","ii","iii","iv","v"}]
                                            return " ".join(toks)
                                        pdf["_key"] = pdf["pname"].astype(str).map(_norm)
                                        feats["_key"] = feats["player_name"].astype(str).map(_norm)
                                        kmap = pdf[["_key","pid"]].drop_duplicates()
                                        feats = feats.merge(kmap, on="_key", how="left")
                                        # fill last_team using logs by name-resolved pid
                                        last2 = last.rename(columns={"player_id":"pid"})
                                        feats = feats.merge(last2, on="pid", how="left", suffixes=("","_byname"))
                                        feats["last_team"] = feats["last_team"].fillna(feats.get("last_team_byname"))
                                        feats = feats.drop(columns=[c for c in ["_key","pid","last_team_byname"] if c in feats.columns], errors="ignore")
                            except Exception:
                                pass
                        # Build today's event team set
                        slate_teams = set()
                        try:
                            go_path = _paths.data_processed / f"game_odds_{date_str}.csv"
                            go = _pd.read_csv(go_path) if go_path.exists() else _pd.DataFrame()
                            if not go.empty:
                                hcol = "home_team" if "home_team" in go.columns else None
                                acol = "visitor_team" if "visitor_team" in go.columns else ("away_team" if "away_team" in go.columns else None)
                                if hcol and acol:
                                    for _, r in go.iterrows():
                                        h = _to_tri(str(r.get(hcol) or "")); a = _to_tri(str(r.get(acol) or ""))
                                        if h: slate_teams.add(h)
                                        if a: slate_teams.add(a)
                        except Exception:
                            pass
                        def _keep(row):
                            t = str(row.get("team") or "").strip().upper()
                            lt = str(row.get("last_team") or "").strip().upper()
                            if not lt:
                                return True
                            if lt == t:
                                return True
                            if slate_teams and (lt in slate_teams):
                                # allow if last team is one of today's teams (recent trade edge case)
                                return True
                            return False
                        before = len(feats)
                        feats = feats[feats.apply(_keep, axis=1)]
                        dropped = before - len(feats)
                        if dropped > 0:
                            console.print(f"Dropped {dropped} rows by last-team sanity filter", style="yellow")
                        feats = feats.drop(columns=["last_team"], errors="ignore")
    except Exception as _e:
        console.print(f"Logs-based sanity filter skipped: {_e}", style="yellow")

    # Predict using pure ONNX only
    # Predict using pure ONNX only
    try:
        from .props_onnx_pure import predict_props_pure_onnx
        console.print("Using pure ONNX models with NPU acceleration...", style="cyan")
        preds = predict_props_pure_onnx(feats)
        console.print(f"Pure ONNX predictions generated for {len(preds)} players", style="green")
    except Exception as e:
        console.print(f"Failed to run pure ONNX predictions: {e}", style="red"); return
    # Optional light calibration (rolling intercept per stat)
    if calibrate:
        try:
            from .props_calibration import compute_biases, apply_biases, save_calibration
            biases = compute_biases(anchor_date=date_str, window_days=int(calib_window))
            preds = apply_biases(preds, biases)
            save_calibration(biases, anchor_date=date_str, window_days=int(calib_window))
            console.print({"calibration": biases})
        except Exception as _e:
            console.print(f"Calibration skipped due to error: {_e}", style="yellow")
    if not out_path:
        out_path = str(paths.data_processed / f"props_predictions_{date_str}.csv")
    preds.to_csv(out_path, index=False)
    console.print(f"Saved props predictions to {out_path} (rows={len(preds)}; calibrated={calibrate})")


@cli.command("evaluate-props")
@click.option("--start", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", type=str, required=True, help="End date YYYY-MM-DD")
@click.option("--slate-only/--no-slate-only", default=True, show_default=True, help="Filter predictions to scoreboard slate for each date")
def evaluate_props_cmd(start: str, end: str, slate_only: bool):
    """Evaluate props predictions vs nbastatR actuals over a date range.

    Builds features (no leakage) per date, predicts, joins to actuals (if present), and computes RMSE/MAE/R2 per stat.
    """
    console.rule("Evaluate Props Models")
    # Parse date inputs early for use in all branches
    import datetime as _dt
    try:
        start_d = _dt.datetime.strptime(start, "%Y-%m-%d").date()
        end_d = _dt.datetime.strptime(end, "%Y-%m-%d").date()
    except Exception:
        console.print("Invalid --start/--end date. Use YYYY-MM-DD.", style="red"); return
    # Load actuals store
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score  # type: ignore
    import numpy as np
    act_p = paths.data_processed / "props_actuals.parquet"
    actuals = None
    if act_p.exists():
        actuals = pd.read_parquet(act_p)
    else:
        # Fallback: combine any daily props_actuals_*.csv files in processed
        try:
            daily = list(paths.data_processed.glob("props_actuals_*.csv"))
            if daily:
                frames = []
                for p in daily:
                    try:
                        frames.append(pd.read_csv(p))
                    except Exception:
                        pass
                if frames:
                    actuals = pd.concat(frames, ignore_index=True)
        except Exception:
            pass
    if actuals is None or actuals.empty:
        # Try per-day nbastatR fetch
        console.print("No consolidated actuals found; attempting to fetch per-day via nbastatR...", style="yellow")
        tmp_frames = []
        try:
            for d in pd.date_range(start_d, end_d, freq="D").date:
                try:
                    df_day = fetch_prop_actuals_via_nbastatr(date=str(d))
                    if df_day is not None and not df_day.empty:
                        tmp_frames.append(df_day)
                except Exception:
                    continue
        except Exception:
            pass
        if tmp_frames:
            actuals = pd.concat(tmp_frames, ignore_index=True)
        else:
            # Final fallback: derive actuals from player_logs (Python only)
            console.print("Falling back to player_logs for actuals.", style="yellow")
            try:
                p = paths.data_processed / "player_logs.parquet"; c = paths.data_processed / "player_logs.csv"
                logs = pd.read_parquet(p) if p.exists() else (pd.read_csv(c) if c.exists() else None)
                if logs is None or logs.empty:
                    console.print("player_logs not found. Run fetch-player-logs.", style="red"); return
                # Identify columns
                def pick(df, cands):
                    m = {x.lower(): x for x in df.columns}
                    for k in cands:
                        if k.lower() in m:
                            return m[k.lower()]
                    return None
                dcol = pick(logs, ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT"]) 
                pid = pick(logs, ["PLAYER_ID", "player_id", "idPlayer"]) 
                pts = pick(logs, ["PTS","pts"]) ; reb = pick(logs, ["REB","reb","TREB","treb"]) ; ast = pick(logs, ["AST","ast"]) ; fg3m = pick(logs, ["FG3M","fg3m"]) 
                if not all([dcol, pid, pts, reb, ast, fg3m]):
                    console.print("player_logs missing required columns for actuals.", style="red"); return
                logs[dcol] = pd.to_datetime(logs[dcol]).dt.date
                mask = (logs[dcol] >= start_d) & (logs[dcol] <= end_d)
                part = logs.loc[mask, [dcol, pid, pts, reb, ast, fg3m]].copy()
                part.rename(columns={dcol: "date", pid: "player_id", pts: "pts", reb: "reb", ast: "ast", fg3m: "threes"}, inplace=True)
                for ccc in ["pts","reb","ast","threes"]:
                    part[ccc] = pd.to_numeric(part[ccc], errors="coerce")
                part["pra"] = part[["pts","reb","ast"]].sum(axis=1, skipna=True)
                actuals = part
            except Exception as _e:
                console.print(f"Failed to derive actuals from player_logs: {_e}", style="red"); return
    if actuals is not None and not actuals.empty:
        actuals["date"] = pd.to_datetime(actuals["date"]).dt.date
    dates = pd.date_range(start_d, end_d, freq="D").date
    rows = []
    for d in dates:
        try:
            feats = build_features_for_date(d)
            if slate_only:
                try:
                    sb = scoreboardv2.ScoreboardV2(game_date=str(d), day_offset=0, timeout=30)
                    nd = sb.get_normalized_dict(); gh = pd.DataFrame(nd.get("GameHeader", [])); ls = pd.DataFrame(nd.get("LineScore", []))
                    if not gh.empty and not ls.empty:
                        ls_cols = {c.upper(): c for c in ls.columns}
                        team_map = {}
                        if {"TEAM_ID","TEAM_ABBREVIATION"}.issubset(ls_cols.keys()):
                            for _, r in ls.iterrows():
                                try:
                                    team_map[int(r[ls_cols["TEAM_ID"]])] = str(r[ls_cols["TEAM_ABBREVIATION"]]).upper()
                                except Exception:
                                    pass
                        gh_cols = {c.upper(): c for c in gh.columns}
                        games = []
                        if {"HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh_cols.keys()):
                            for _, g in gh.iterrows():
                                try:
                                    hid = int(g[gh_cols["HOME_TEAM_ID"]]); vid = int(g[gh_cols["VISITOR_TEAM_ID"]])
                                    h = team_map.get(hid); v = team_map.get(vid)
                                    if h and v:
                                        games.append({"team": h}); games.append({"team": v})
                                except Exception:
                                    pass
                        slate = pd.DataFrame(games)
                        if not slate.empty and "team" in feats.columns:
                            feats["team"] = feats["team"].astype(str).str.upper()
                            feats = feats.merge(slate, on="team", how="inner")
                except Exception:
                    pass
            from .props_train import predict_props  # Import here to avoid sklearn dependency
            preds = predict_props(feats)
            preds["date"] = d
            # join to actuals by (date, player_id)
            part_act = actuals[actuals["date"] == d].copy()
            merged = preds.merge(part_act, on=["date","player_id"], how="inner", suffixes=("","_act"))
            if not merged.empty:
                rows.append(merged)
        except Exception:
            continue
    if not rows:
        console.print("No overlapping predictions and actuals in the range.", style="yellow"); return
    df = pd.concat(rows, ignore_index=True)
    # Compute metrics
    metrics = []
    for target, pred_col in [("pts","pred_pts"),("reb","pred_reb"),("ast","pred_ast"),("threes","pred_threes"),("pra","pred_pra")]:
        if target in df.columns and pred_col in df.columns:
            y = pd.to_numeric(df[target], errors="coerce")
            p = pd.to_numeric(df[pred_col], errors="coerce")
            mask = y.notna() & p.notna()
            if mask.any():
                rmse = float(np.sqrt(mean_squared_error(y[mask], p[mask])))
                mae = float(mean_absolute_error(y[mask], p[mask]))
                r2 = float(r2_score(y[mask], p[mask]))
                metrics.append({"stat": target, "rmse": rmse, "mae": mae, "r2": r2, "n": int(mask.sum())})
    out = pd.DataFrame(metrics)
    console.print(out)
    out_path = paths.data_processed / f"props_eval_{start}_{end}.csv"
    out.to_csv(out_path, index=False)
    console.print(f"Saved props eval metrics to {out_path}")


@cli.command("train-props-npu")
@click.option("--alpha", type=float, default=1.0, show_default=True, help="Ridge regularization strength")
def train_props_npu_cmd(alpha: float):
    """Train props regression models and convert to ONNX for NPU acceleration."""
    console.rule("Train Props Models (NPU)")
    try:
        from .props_npu import train_props_models_npu
        train_props_models_npu(alpha=alpha)
        console.print("Saved props models, ONNX models, and feature columns for NPU.")
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red")
    except FileNotFoundError as e:
        console.print(str(e), style="red")
    except Exception as e:
        console.print(f"Failed to train NPU props models: {e}", style="red")


@cli.command("predict-props-npu")
@click.option("--date", "date_str", type=str, required=True, help="Prediction date YYYY-MM-DD (features built up to the day before)")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path (default props_predictions_npu_YYYY-MM-DD.csv)")
@click.option("--slate-only/--no-slate-only", default=True, show_default=True, help="Filter predictions to teams on the scoreboard slate and add opponent/home flags")
@click.option("--calibrate/--no-calibrate", default=True, show_default=True, help="Apply rolling bias calibration from recent recon vs predictions")
@click.option("--calib-window", type=int, default=7, show_default=True, help="Lookback days for calibration window (excludes today)")
def predict_props_npu_cmd(date_str: str, out_path: str | None, slate_only: bool, calibrate: bool, calib_window: int):
    """Predict player props using NPU-accelerated ONNX models for ultra-fast inference."""
    console.rule("Predict Props (NPU)")
    try:
        feats = build_features_for_date(date_str)
    except Exception as e:
        console.print(f"Failed to build features for {date_str}: {e}", style="red"); return
    
    # Optional slate filter using ScoreboardV2
    if slate_only:
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
            nd = sb.get_normalized_dict()
            gh = pd.DataFrame(nd.get("GameHeader", []))
            ls = pd.DataFrame(nd.get("LineScore", []))
            if not gh.empty and not ls.empty:
                ls_cols = {c.upper(): c for c in ls.columns}
                if {"TEAM_ID","TEAM_ABBREVIATION"}.issubset(ls_cols.keys()):
                    team_map = {}
                    for _, r in ls.iterrows():
                        try:
                            team_map[int(r[ls_cols["TEAM_ID"]])] = str(r[ls_cols["TEAM_ABBREVIATION"]]).upper()
                        except Exception:
                            continue
                    gh_cols = {c.upper(): c for c in gh.columns}
                    if {"HOME_TEAM_ID","VISITOR_TEAM_ID"}.issubset(gh_cols.keys()):
                        games = []
                        for _, g in gh.iterrows():
                            try:
                                hid = int(g[gh_cols["HOME_TEAM_ID"]]); vid = int(g[gh_cols["VISITOR_TEAM_ID"]])
                                h = team_map.get(hid); v = team_map.get(vid)
                                if h and v:
                                    games.append({"team": h, "opponent": v, "home": True})
                                    games.append({"team": v, "opponent": h, "home": False})
                            except Exception:
                                continue
                        slate = pd.DataFrame(games)
                        if not slate.empty and "team" in feats.columns:
                            feats["team"] = feats["team"].astype(str).str.upper()
                            feats = feats.merge(slate, on="team", how="inner")
        except Exception:
            # If scoreboard fails, proceed without filtering
            pass
    
    try:
        from .props_npu import predict_props_npu
        preds = predict_props_npu(feats)
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red"); return
    except FileNotFoundError:
        console.print("NPU props models not found. Run train-props-npu first.", style="red"); return
    except Exception as e:
        console.print(f"Failed to predict props with NPU: {e}", style="red"); return
    
    # Optional light calibration (rolling intercept per stat)
    if calibrate:
        try:
            from .props_calibration import compute_biases, apply_biases, save_calibration
            biases = compute_biases(anchor_date=date_str, window_days=int(calib_window))
            preds = apply_biases(preds, biases)
            save_calibration(biases, anchor_date=date_str, window_days=int(calib_window))
            console.print({"calibration": biases})
        except Exception as _e:
            console.print(f"Calibration skipped due to error: {_e}", style="yellow")
    
    if not out_path:
        out_path = str(paths.data_processed / f"props_predictions_npu_{date_str}.csv")
    preds.to_csv(out_path, index=False)
    console.print(f"Saved NPU props predictions to {out_path} (rows={len(preds)}; calibrated={calibrate})")


@cli.command("benchmark-npu")
@click.option("--runs", type=int, default=100, show_default=True, help="Number of benchmark runs")
@click.option("--players", type=int, default=500, show_default=True, help="Number of players to simulate")
def benchmark_npu_cmd(runs: int, players: int):
    """Benchmark NPU vs CPU performance for props prediction."""
    console.rule("NPU Benchmark")
    try:
        from .props_npu import benchmark_npu_performance
        results = benchmark_npu_performance(num_runs=runs, num_players=players)
        console.print(results)
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red")
    except Exception as e:
        console.print(f"Benchmark failed: {e}", style="red")


@cli.command("train-games-npu")
@click.option("--retrain/--no-retrain", default=True, show_default=True, help="Retrain models with latest data before converting to ONNX")
def train_games_npu_cmd(retrain: bool):
    """Train game models (win probability, spread, totals) and convert to ONNX for NPU acceleration."""
    console.rule("Train Game Models (NPU)")
    try:
        from .games_npu import train_game_models_npu
        train_game_models_npu(retrain=retrain)
        console.print("Saved game models and ONNX models for NPU acceleration.")
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red")
    except FileNotFoundError as e:
        console.print(str(e), style="red")
    except Exception as e:
        console.print(f"Failed to train NPU game models: {e}", style="red")


@cli.command("train-games-enhanced-onnx")
def train_games_enhanced_onnx_cmd():
    """Train enhanced (45-feature) game + period models on all available data, then convert to ONNX.

    Outputs enhanced sklearn models (*.joblib with _enhanced suffix) and ONNX models (*.onnx with _enhanced suffix) under models/.
    """
    console.rule("Train Enhanced Models + Convert to ONNX")
    try:
        games_file = paths.data_raw / "games_nba_api.csv"
        if not games_file.exists():
            console.print(f"Games file not found: {games_file}. Run 'nba-betting fetch' first.", style="red"); return
        console.print(f"Loading games from {games_file}...")
        games = pd.read_csv(games_file)
        # Normalize common column names from NBA API output
        rename_map = {}
        if 'home_team_tri' in games.columns:
            rename_map.update({
                'home_team_tri': 'home_team',
                'visitor_team_tri': 'visitor_team',
            })
        if 'date_est' in games.columns:
            rename_map['date_est'] = 'date'
        if 'home_score' in games.columns:
            rename_map['home_score'] = 'home_pts'
        if 'visitor_score' in games.columns:
            rename_map['visitor_score'] = 'visitor_pts'
        if rename_map:
            games = games.rename(columns=rename_map)

        from .features_enhanced import build_features_enhanced
        console.print("Building enhanced features (45 features incl. injuries)...", style="cyan")
        df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)

        from .train_enhanced import train_models_enhanced
        console.print("Training enhanced models (win/spread/total + halves/quarters)...", style="cyan")
        metrics = train_models_enhanced(df, use_enhanced_features=True)
        console.print({k: (None if v is None else (round(v,4) if isinstance(v, float) else v)) for k, v in metrics.items()})

        # Convert to ONNX
        console.print("Converting enhanced models to ONNX...", style="cyan")
        try:
            from convert_enhanced_to_onnx import convert_enhanced_models
            convert_enhanced_models()
        except ImportError as e:
            console.print(f"skl2onnx not installed; install to convert to ONNX: {e}", style="yellow")
            console.print("You can still use sklearn CPU models; NPU/ONNX requires conversion.", style="yellow")
        console.print("Enhanced training + ONNX conversion complete.")
    except Exception as e:
        console.print(f"Failed to train/convert enhanced models: {e}", style="red")

@cli.command("predict-games-npu")
@click.option("--date", "date_str", type=str, required=True, help="Prediction date YYYY-MM-DD")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path (default games_predictions_npu_YYYY-MM-DD.csv)")
@click.option("--periods/--no-periods", default=True, show_default=True, help="Include halves and quarters predictions")
@click.option("--calibrate-periods/--no-calibrate-periods", default=True, show_default=True, help="Calibrate halves/quarters to team shares and enforce sum constraints")
def predict_games_npu_cmd(date_str: str, out_path: str | None, periods: bool, calibrate_periods: bool):
    """Predict game outcomes using NPU-accelerated models for ultra-fast inference."""
    console.rule("Predict Games (NPU)")
    try:
        # Load features for the date (prefer parquet; fallback to CSV to avoid parquet engine dependency)
        features_path = paths.data_processed / "features.parquet"
        csv_fallback = paths.data_processed / "features.csv"
        if not features_path.exists() and not csv_fallback.exists():
            console.print("Features not found. Run build-features first.", style="red")
            return

        try:
            if features_path.exists():
                features_df = pd.read_parquet(features_path)
            else:
                raise FileNotFoundError
        except Exception:
            # Parquet engine likely missing; try CSV fallback
            if csv_fallback.exists():
                features_df = pd.read_csv(csv_fallback)
            else:
                console.print(
                    "Failed to read features.parquet (pyarrow/fastparquet missing) and CSV fallback not found.",
                    style="red",
                )
                return


        # Filter to the specific date if provided
        if 'date' in features_df.columns:
            features_df['date'] = pd.to_datetime(features_df['date']).dt.date
            target_date = pd.to_datetime(date_str).date()
            features_df = features_df[features_df['date'] == target_date]
        
        if features_df.empty:
            # Fallbacks to build enhanced slate features even without parquet engine or prewritten odds:
            # 1) If standardized game odds CSV exists, use it to derive slate
            # 2) Else, fall back to processed season schedule to derive slate (no market lines, but teams/pairs)
            try:
                odds_path = paths.data_processed / f"game_odds_{date_str}.csv"
                raw_games_csv = paths.data_raw / "games_nba_api.csv"
                slate_df = None

                if odds_path.exists():
                    slate = pd.read_csv(odds_path)
                    # normalize team columns
                    home_col = "home_team" if "home_team" in slate.columns else ("home" if "home" in slate.columns else None)
                    away_col = "visitor_team" if "visitor_team" in slate.columns else ("away" if "away" in slate.columns else None)
                    if home_col and away_col:
                        slate_df = pd.DataFrame({
                            "date": pd.to_datetime(date_str),
                            "home_team": slate[home_col].astype(str),
                            "visitor_team": slate[away_col].astype(str),
                            "home_pts": np.nan,
                            "visitor_pts": np.nan,
                        })

                # Fallback to schedule if odds are unavailable
                if slate_df is None:
                    try:
                        # Infer schedule filename for current season (e.g., schedule_2025_26.csv)
                        d = pd.to_datetime(date_str)
                        season_end = d.year if d.month >= 7 else d.year  # 2025-26 season still ends in 2026 but filename includes 2025_26
                        season_str = f"{season_end}_26" if str(season_end).endswith("25") else "2025_26"  # conservative default
                        sched_path = paths.data_processed / f"schedule_{season_str}.csv"
                        if not sched_path.exists():
                            # Try known current schedule file if present
                            sched_path = paths.data_processed / "schedule_2025_26.csv"
                        if sched_path.exists():
                            sch = pd.read_csv(sched_path)
                            # Normalize columns
                            cols = {c.lower(): c for c in sch.columns}
                            if all(k in cols for k in ["date_utc","home_tricode","away_tricode"]) or all(k in cols for k in ["date_utc","home_team_id","away_team_id"]):
                                sch[cols.get("date_utc")] = pd.to_datetime(sch[cols.get("date_utc")], errors="coerce").dt.date
                                td = pd.to_datetime(date_str).date()
                                day = sch[sch[cols.get("date_utc")] == td].copy()
                                if not day.empty:
                                    # Prefer tricodes if available; otherwise attempt to map IDs later via normalization
                                    if "home_tricode" in (c.lower() for c in sch.columns) and "away_tricode" in (c.lower() for c in sch.columns):
                                        # Build from tricodes
                                        hc = cols.get("home_tricode"); ac = cols.get("away_tricode")
                                        slate_df = pd.DataFrame({
                                            "date": pd.to_datetime(date_str),
                                            "home_team": day[hc].astype(str),
                                            "visitor_team": day[ac].astype(str),
                                            "home_pts": np.nan,
                                            "visitor_pts": np.nan,
                                        })
                                    elif all(k in cols for k in ["home_team_id","away_team_id"]):
                                        # If only IDs present, we will later rely on normalization utilities
                                        hc = cols.get("home_team_id"); ac = cols.get("away_team_id")
                                        slate_df = pd.DataFrame({
                                            "date": pd.to_datetime(date_str),
                                            "home_team": day[hc].astype(str),
                                            "visitor_team": day[ac].astype(str),
                                            "home_pts": np.nan,
                                            "visitor_pts": np.nan,
                                        })
                    except Exception:
                        pass

                if slate_df is not None and not slate_df.empty and raw_games_csv.exists():
                    raw_games = pd.read_csv(raw_games_csv)
                    # Ensure columns exist
                    for col in ["date","home_team","visitor_team","home_pts","visitor_pts"]:
                        if col not in raw_games.columns:
                            raw_games[col] = np.nan
                    # Normalize date type
                    raw_games["date"] = pd.to_datetime(raw_games["date"], errors="coerce")
                    games = pd.concat([raw_games, slate_df], ignore_index=True, sort=False)
                    # Build enhanced features (adds pace/ratings + injuries)
                    from .features_enhanced import build_features_enhanced
                    feats2 = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True, season=2025)
                    # Filter to target date
                    feats2["date"] = pd.to_datetime(feats2["date"]).dt.date
                    target_date = pd.to_datetime(date_str).date()
                    features_df = feats2[feats2["date"] == target_date]

                if features_df.empty:
                    console.print(f"No games found for {date_str}", style="yellow")
                    return
            except Exception:
                console.print(f"No games found for {date_str}", style="yellow")
                return
        
        from .games_npu import predict_games_npu
        preds = predict_games_npu(features_df, include_periods=periods, calibrate_periods=calibrate_periods)
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red"); return
    except FileNotFoundError:
        console.print("NPU game models not found. Run train-games-npu first.", style="red"); return
    except Exception as e:
        console.print(f"Failed to predict games with NPU: {e}", style="red"); return
    
    if not out_path:
        out_path = str(paths.data_processed / f"games_predictions_npu_{date_str}.csv")
    preds.to_csv(out_path, index=False)
    console.print(f"Saved NPU game predictions to {out_path} (rows={len(preds)}; periods={periods}; calibrated={calibrate_periods})")


@cli.command("benchmark-games-npu")
@click.option("--runs", type=int, default=100, show_default=True, help="Number of benchmark runs")
@click.option("--games", type=int, default=100, show_default=True, help="Number of games to simulate")
def benchmark_games_npu_cmd(runs: int, games: int):
    """Benchmark NPU vs CPU performance for game predictions."""
    console.rule("Game NPU Benchmark")
    try:
        from .games_npu import benchmark_game_npu_performance
        results = benchmark_game_npu_performance(num_runs=runs, num_games=games)
        console.print(results)
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red")
    except Exception as e:
        console.print(f"Game benchmark failed: {e}", style="red")


@cli.command("props-edges")
@click.option("--date", "date_str", type=str, required=True, help="Slate date YYYY-MM-DD")
@click.option("--use-saved/--no-use-saved", default=True, show_default=True, help="Prefer odds in data/raw if present before fetching")
@click.option("--mode", type=click.Choice(["auto","historical","current"]), default="auto", show_default=True, help="If fetching via OddsAPI, whether to use historical snapshots, current event odds, or auto")
@click.option("--source", type=click.Choice(["auto","oddsapi","bovada"]), default="auto", show_default=True, help="Odds source for player props: oddsapi, bovada, or auto")
@click.option("--api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key (or set env ODDS_API_KEY)")
@click.option("--sigma-pts", type=float, default=7.5, show_default=True)
@click.option("--sigma-reb", type=float, default=3.0, show_default=True)
@click.option("--sigma-ast", type=float, default=2.5, show_default=True)
@click.option("--sigma-threes", type=float, default=1.3, show_default=True)
@click.option("--sigma-pra", type=float, default=9.0, show_default=True)
@click.option("--slate-only/--no-slate-only", default=True, show_default=True, help="Filter to teams on the scoreboard slate")
@click.option("--min-edge", type=float, default=0.02, show_default=True, help="Minimum model edge (probability diff)")
@click.option("--min-ev", type=float, default=0.0, show_default=True, help="Minimum EV per 1u")
@click.option("--top", type=int, default=1000, show_default=False, help="Limit to top N edges after filtering")
@click.option("--bookmakers", type=str, default=None, help="Comma-separated bookmaker keys to include (e.g., draftkings,fanduel,pinnacle)")
@click.option("--calibrate-sigma/--no-calibrate-sigma", default=False, show_default=True, help="Estimate sigma per stat from recent residuals")
@click.option("--predictions-csv", type=click.Path(exists=False, dir_okay=False), required=False, help="Use precomputed props_predictions_YYYY-MM-DD.csv from this path; defaults to data/processed")
@click.option("--file-only/--no-file-only", default=False, show_default=True, help="Do not run props models; require predictions CSV to exist")
def props_edges_cmd(date_str: str, use_saved: bool, mode: str, source: str, api_key: str | None, sigma_pts: float, sigma_reb: float, sigma_ast: float, sigma_threes: float, sigma_pra: float, slate_only: bool, min_edge: float, min_ev: float, top: int, bookmakers: str | None, calibrate_sigma: bool, predictions_csv: str | None, file_only: bool):
    """Compute player props edges (EV) by merging model predictions with OddsAPI lines for a date.

    Writes data/processed/props_edges_YYYY-MM-DD.csv
    """
    console.rule("Props Edges")
    try:
        pd.to_datetime(date_str)  # validate
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return
    if not api_key:
        api_key = _load_dotenv_key("ODDS_API_KEY")
    sigma = SigmaConfig(pts=sigma_pts, reb=sigma_reb, ast=sigma_ast, threes=sigma_threes, pra=sigma_pra)
    if calibrate_sigma:
        try:
            sigma = calibrate_sigma_for_date(date_str, window_days=30, min_rows=200, defaults=sigma)
            console.print({"sigma": sigma.__dict__})
        except Exception:
            pass
    try:
        edges = compute_props_edges(
            date=date_str,
            sigma=sigma,
            use_saved=use_saved,
            mode=mode,
            api_key=api_key,
            source=source,
            predictions_path=predictions_csv,
            from_file_only=file_only,
        )
    except FileNotFoundError as e:
        console.print(str(e), style="red"); return
    except Exception as e:
        console.print(f"Failed to compute edges: {e}", style="red"); return
    if edges is None or edges.empty:
        console.print("No edges computed (missing odds or predictions).", style="yellow"); return
    # Optional slate filter
    if slate_only:
        try:
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
            nd = sb.get_normalized_dict(); ls = pd.DataFrame(nd.get("LineScore", []))
            teams = []
            if not ls.empty:
                c = {x.upper(): x for x in ls.columns}
                if "TEAM_ABBREVIATION" in c:
                    teams = list(ls[c["TEAM_ABBREVIATION"]].astype(str).str.upper().unique())
            if teams:
                edges["team"] = edges["team"].astype(str).str.upper()
                edges = edges[edges["team"].isin(teams)].copy()
        except Exception:
            pass
    # Bookmaker filter
    if bookmakers:
        keep = [x.strip().lower() for x in bookmakers.split(',') if x.strip()]
        if keep:
            edges = edges[edges["bookmaker"].astype(str).str.lower().isin(keep)].copy()
    # Thresholds and top-N
    edges = edges[(edges["edge"] >= min_edge) & (edges["ev"] >= min_ev)].copy()
    edges.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
    if top and len(edges) > top:
        edges = edges.groupby("stat", group_keys=False).head(max(1, top // max(1, edges["stat"].nunique())))
    out = paths.data_processed / f"props_edges_{date_str}.csv"
    edges.to_csv(out, index=False)
    console.print({"rows": int(len(edges)), "output": str(out)})


@cli.command("export-recommendations")
@click.option("--date", "date_str", type=str, required=True, help="Slate date YYYY-MM-DD")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path; defaults to data/processed/recommendations_YYYY-MM-DD.csv")
def export_recommendations_cmd(date_str: str, out_path: str | None):
    """Export game recommendations (ML/ATS/TOTAL) to CSV from predictions + odds."""
    import pandas as pd
    from .config import paths
    from .teams import to_tricode as _tri
    try:
        d = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return
    # Prefer NPU game predictions if available, else fall back to baseline predictions
    pred_npu = paths.data_processed / f"games_predictions_npu_{date_str}.csv"
    pred = paths.data_processed / f"predictions_{date_str}.csv"
    use_path = pred_npu if pred_npu.exists() else pred
    if not use_path.exists():
        console.print(f"Predictions not found: {use_path}", style="red"); return
    df = pd.read_csv(use_path)
    # Optional: try merge standardized game_odds CSV
    odds_csv = paths.data_processed / f"game_odds_{date_str}.csv"
    if odds_csv.exists():
        try:
            o = pd.read_csv(odds_csv)
            if "date" in o.columns:
                o["date"] = pd.to_datetime(o["date"], errors="coerce").dt.date
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            on = ["date","home_team","visitor_team"]
            if all(c in df.columns for c in on) and all(c in o.columns for c in on):
                df = df.merge(o, on=on, how="left", suffixes=("","_odds"))
        except Exception:
            pass
    # Build recs
    recs: list[dict] = []
    def _num(x):
        try:
            return float(x)
        except Exception:
            return None
    def _ev(prob, american):
        try:
            p = float(prob)
        except Exception:
            return None
        try:
            a = float(american)
        except Exception:
            return None
        if a > 0:
            return p * (a/100.0) - (1-p) * 1.0
        else:
            return p * (100.0/(-a)) - (1-p) * 1.0
    def _implied(american):
        try:
            a = float(american)
        except Exception:
            return None
        if a == 0:
            return None
        if a > 0:
            return 100.0 / (a + 100.0)
        return (-a) / ((-a) + 100.0)
    def _tier(market: str, ev: float | None, edge: float | None) -> str:
        try:
            m = (market or '').upper()
            if m == 'ML' and ev is not None:
                if ev >= 0.04:
                    return 'High'
                if ev >= 0.02:
                    return 'Medium'
                return 'Low'
            if m == 'ATS' and edge is not None:
                v = abs(edge)
                if v >= 3.0:
                    return 'High'
                if v >= 1.5:
                    return 'Medium'
                return 'Low'
            if m == 'TOTAL' and edge is not None:
                v = abs(edge)
                if v >= 4.0:
                    return 'High'
                if v >= 2.0:
                    return 'Medium'
                return 'Low'
        except Exception:
            pass
        return 'Low'
    for _, r in df.iterrows():
        try:
            home = r.get("home_team"); away = r.get("visitor_team")
            # ML
            # Prefer explicit home_win_prob; fall back to NPU names
            p_home = _num(r.get("home_win_prob"))
            if p_home is None:
                p_home = _num(r.get("win_prob"))
            if p_home is None:
                p_home = _num(r.get("win_prob_from_spread"))
            ev_h = _ev(p_home, r.get("home_ml")) if p_home is not None else None
            ev_a = _ev((1-p_home) if p_home is not None else None, r.get("away_ml"))
            if ev_h is not None or ev_a is not None:
                side_ml = home if (ev_h or -1) >= (ev_a or -1) else away
                ev_ml = ev_h if side_ml == home else ev_a
                if ev_ml is not None and ev_ml > 0:
                    price = _num(r.get("home_ml")) if side_ml == home else _num(r.get("away_ml"))
                    recs.append({
                        "market":"ML",
                        "side": side_ml,
                        "home": home,
                        "away": away,
                        "date": str(d),
                        "ev": float(ev_ml),
                        "price": price,
                        "implied_prob": (_implied(price) if price is not None else None),
                        "tier": _tier('ML', float(ev_ml), None),
                    })
            # ATS
            # Use model margin from baseline or NPU column
            pm = _num(r.get("pred_margin"))
            if pm is None:
                pm = _num(r.get("spread_margin"))
            hs = _num(r.get("home_spread"))
            if pm is not None and hs is not None:
                edge_spread = pm - (-hs)
                if abs(edge_spread) >= 1.0:
                    side_ats = home if edge_spread>0 else away
                    line = hs if side_ats == home else (-hs if hs is not None else None)
                    recs.append({
                        "market":"ATS",
                        "side": side_ats,
                        "home": home,
                        "away": away,
                        "date": str(d),
                        "edge": float(edge_spread),
                        "line": line,
                        "pred_margin": pm,
                        "market_home_margin": -hs,
                        "tier": _tier('ATS', None, float(edge_spread)),
                    })
            # TOTAL
            # Use model total from baseline or NPU column
            pt = _num(r.get("pred_total"))
            if pt is None:
                pt = _num(r.get("totals"))
            tot = _num(r.get("total"))
            if pt is not None and tot is not None:
                edge_total = pt - tot
                if abs(edge_total) >= 1.5:
                    recs.append({
                        "market":"TOTAL",
                        "side": ("Over" if edge_total>0 else "Under"),
                        "home": home,
                        "away": away,
                        "date": str(d),
                        "edge": float(edge_total),
                        "line": tot,
                        "pred_total": pt,
                        "tier": _tier('TOTAL', None, float(edge_total)),
                    })
        except Exception:
            continue
    out = paths.data_processed / f"recommendations_{date_str}.csv" if not out_path else Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Enforce canonical column order
    cols = [
        "market","side","home","away","date",
        "ev","price","implied_prob",
        "edge","line","pred_margin","market_home_margin","pred_total",
        "tier",
    ]
    df_out = pd.DataFrame(recs)
    for c in cols:
        if c not in df_out.columns:
            df_out[c] = None
    df_out = df_out[cols]
    df_out.to_csv(out, index=False)
    console.print({"rows": int(len(recs)), "output": str(out)})


@cli.command("export-props-recommendations")
@click.option("--date", "date_str", type=str, required=True, help="Slate date YYYY-MM-DD")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path; defaults to data/processed/props_recommendations_YYYY-MM-DD.csv")
def export_props_recommendations_cmd(date_str: str, out_path: str | None):
    """Export props recommendation cards to CSV from edges (or model-only if edges missing)."""
    import pandas as pd
    from .config import paths
    try:
        d = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return
    edges_p = paths.data_processed / f"props_edges_{date_str}.csv"
    preds_p = paths.data_processed / f"props_predictions_{date_str}.csv"
    games_p = paths.data_processed / f"predictions_{date_str}.csv"
    df = pd.read_csv(edges_p) if edges_p.exists() else pd.DataFrame()
    pp = pd.read_csv(preds_p) if preds_p.exists() else pd.DataFrame()
    games_df = pd.read_csv(games_p) if games_p.exists() else pd.DataFrame()
    cards: list[dict] = []
    if df is None or df.empty:
        # Model-only cards
        if not pp.empty:
            for (player, team), grp in pp.groupby(["player_name","team"], dropna=False):
                model = {}
                for col, key in [("pred_pts","pts"),("pred_reb","reb"),("pred_ast","ast"),("pred_threes","threes"),("pred_pra","pra")]:
                    if col in grp.columns:
                        try:
                            v = pd.to_numeric(grp[col], errors="coerce").dropna()
                            if not v.empty:
                                model[key] = float(v.iloc[0])
                        except Exception:
                            pass
                cards.append({"player": player, "team": team, "plays": [], "ladders": [], "model": model})
    else:
        # Build plays per player/team
        def _num(x):
            try:
                return float(x)
            except Exception:
                return None
        for keys, grp in df.groupby([c for c in ["player_name","team"] if c in df.columns], dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)
            player = keys[0] if len(keys)>0 else None
            team = keys[1] if len(keys)>1 else None
            g2 = grp.copy()
            g2["ev_pct"] = pd.to_numeric(g2.get("ev"), errors="coerce") * 100.0 if "ev" in g2.columns else None
            plays = []
            for _, r in g2.iterrows():
                plays.append({
                    "market": r.get("stat"),
                    "side": r.get("side"),
                    "line": _num(r.get("line")),
                    "price": _num(r.get("price")),
                    "edge": _num(r.get("edge")),
                    "ev": _num(r.get("ev")),
                    "ev_pct": _num(r.get("ev"))*100.0 if _num(r.get("ev")) is not None else None,
                    "book": r.get("bookmaker"),
                })
            cards.append({"player": player, "team": team, "plays": plays, "ladders": []})
    out = paths.data_processed / f"props_recommendations_{date_str}.csv" if not out_path else Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cards).to_csv(out, index=False)
    console.print({"rows": int(len(cards)), "output": str(out)})


@cli.command("odds-refresh")
@click.option("--date", "date_str", type=str, required=False, help="Target date YYYY-MM-DD; defaults to today (UTC)")
@click.option("--api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key (or set env ODDS_API_KEY)")
@click.option("--min-prop-edge", type=float, default=0.02, show_default=True, help="Minimum edge filter for props edges")
@click.option("--min-prop-ev", type=float, default=0.0, show_default=True, help="Minimum EV filter for props edges")
def odds_refresh_cmd(date_str: str | None, api_key: str | None, min_prop_edge: float, min_prop_ev: float):
    """Refresh odds and recompute odds-related edges only.

    - Fetch current game odds (OddsAPI via ODDS_API_KEY) and write game_odds_<date>.csv
    - Compute props edges for the date using current odds (source=auto), saving props_edges_<date>.csv
    - Do not retrain or rebuild predictions beyond what's needed for edges
    """
    console.rule("Odds Refresh (odds + props-edges)")
    import datetime as _dt
    try:
        target_date = (_dt.date.today() if not date_str else _dt.datetime.strptime(date_str, "%Y-%m-%d").date())
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return

    # Load API key from env or .env
    if not api_key:
        api_key = _load_dotenv_key("ODDS_API_KEY")

    # 1) Fetch current game odds and save standardized CSV for frontend/merges
    try:
        if api_key:
            console.print("Fetching current game odds (OddsAPI)...", style="cyan")
            cfg = OddsApiConfig(api_key=api_key)
            go = fetch_game_odds_current(cfg, pd.to_datetime(target_date))
            if go is not None and not go.empty:
                out_csv = paths.data_processed / f"game_odds_{target_date}.csv"
                out_csv.parent.mkdir(parents=True, exist_ok=True)
                go.to_csv(out_csv, index=False)
                console.print({"game_odds_rows": int(len(go)), "output": str(out_csv)})
            else:
                console.print("No game odds returned (OddsAPI)", style="yellow")
        else:
            console.print("ODDS_API_KEY not set; skipping game odds fetch", style="yellow")
    except Exception as e:
        console.print(f"Game odds fetch failed: {e}", style="yellow")

    # 2) Compute props edges using current odds (source auto, mode current)
    try:
        console.print("Computing props edges from current odds...", style="cyan")
        # Calibrate sigma best-effort; fall back to defaults if it fails
        try:
            sigma = calibrate_sigma_for_date(str(target_date), window_days=30, min_rows=200, defaults=SigmaConfig())
        except Exception:
            sigma = SigmaConfig()
        edges = compute_props_edges(
            date=str(target_date),
            sigma=sigma,
            use_saved=False,
            mode="current",
            api_key=api_key,
            source="auto",
            predictions_path=None,
            from_file_only=False,
        )
        if edges is None or edges.empty:
            console.print("No props edges computed (missing odds or predictions)", style="yellow"); return
        # Filter and save
        edges = edges[(edges["edge"] >= float(min_prop_edge)) & (edges["ev"] >= float(min_prop_ev))].copy()
        edges.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
        out = paths.data_processed / f"props_edges_{target_date}.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        edges.to_csv(out, index=False)
        console.print({"props_edges_rows": int(len(edges)), "output": str(out)})
    except Exception as e:
        console.print(f"Props edges computation failed: {e}", style="yellow")


@cli.command("odds-snapshots")
@click.option("--date", "date_str", type=str, required=False, help="Target date YYYY-MM-DD; defaults to today (UTC)")
@click.option("--api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key (or set env ODDS_API_KEY)")
def odds_snapshots_cmd(date_str: str | None, api_key: str | None):
    """Write standardized OddsAPI snapshots for a given date.

    Produces CSVs under data/processed:
    - oddsapi_events_<date>.csv: events on the US/Eastern calendar day with has_odds flag
    - game_odds_<date>.csv: consensus moneyline/spread/total from current event odds

    Notes:
    - Requires ODDS_API_KEY
    - CSV-first; no parquet dependency
    """
    console.rule("Odds Snapshots (events + consensus lines)")
    import datetime as _dt
    import requests as _requests
    from .odds_api import ODDS_HOST, NBA_SPORT_KEY, OddsApiConfig, fetch_game_odds_current, consensus_lines_at_close

    try:
        target_date = (_dt.date.today() if not date_str else _dt.datetime.strptime(date_str, "%Y-%m-%d").date())
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return

    if not api_key:
        api_key = _load_dotenv_key("ODDS_API_KEY")
    if not api_key:
        console.print("Provide --api-key, set ODDS_API_KEY env, or add to .env at repo root.", style="red"); return

    # 1) Fetch events list and filter to US/Eastern day; write oddsapi_events_<date>.csv
    events_out = paths.data_processed / f"oddsapi_events_{target_date}.csv"
    # Also write an allowlisted alias for the site (tracked by !data/processed/odds_*.csv)
    events_out_alias = paths.data_processed / f"odds_events_{target_date}.csv"
    try:
        ev_resp = _requests.get(f"{ODDS_HOST}/v4/sports/{NBA_SPORT_KEY}/events", params={"apiKey": api_key}, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=45)
        ev_resp.raise_for_status()
        events = ev_resp.json() or []
    except Exception as e:
        console.print(f"Events fetch failed: {e}", style="yellow")
        events = []

    def _et_date(iso_str: str):
        try:
            ct_raw = pd.to_datetime(iso_str, utc=True)
        except Exception:
            return None
        for tzname in ("America/New_York", "US/Eastern"):
            try:
                return ct_raw.tz_convert(tzname).date()
            except Exception:
                continue
        try:
            month = int(ct_raw.month)
            offset_hours = 4 if 3 <= month <= 11 else 5
            return (ct_raw - pd.Timedelta(hours=offset_hours)).date()
        except Exception:
            return ct_raw.date()

    ev_rows: list[dict] = []
    # If we also fetch game odds later, capture event_ids with odds
    have_ids: set[str] = set()

    # 2) Fetch current game odds and write consensus game_odds_<date>.csv
    game_odds_out = paths.data_processed / f"game_odds_{target_date}.csv"
    try:
        cfg = OddsApiConfig(api_key=api_key)
        long_df = fetch_game_odds_current(cfg, pd.to_datetime(target_date))
        if long_df is not None and not long_df.empty:
            try:
                have_ids = set([str(x) for x in long_df["event_id"].dropna().astype(str).unique()])
            except Exception:
                have_ids = set()
            wide = consensus_lines_at_close(long_df)
            if wide is not None and not wide.empty:
                tmp = wide.copy()
                tmp["date"] = pd.to_datetime(tmp["commence_time"], utc=True).dt.tz_convert("US/Eastern").dt.strftime("%Y-%m-%d")
                tmp = tmp.rename(columns={"away_team":"visitor_team"})
                if "spread_point" in tmp.columns:
                    tmp["home_spread"] = tmp["spread_point"]
                    tmp["away_spread"] = tmp["home_spread"].apply(lambda x: -x if pd.notna(x) else pd.NA)
                if "total_point" in tmp.columns:
                    tmp["total"] = tmp["total_point"]
                cols = [c for c in ["date","commence_time","home_team","visitor_team","home_ml","away_ml","home_spread","away_spread","total"] if c in tmp.columns]
                out_df = tmp[cols].copy()
                out_df["bookmaker"] = "oddsapi_consensus"
                out_df.to_csv(game_odds_out, index=False)
                console.print({"game_odds_rows": int(len(out_df)), "output": str(game_odds_out)})
            else:
                console.print("No consensus rows from current game odds", style="yellow")
        else:
            console.print("No game odds returned (OddsAPI)", style="yellow")
    except Exception as e:
        console.print(f"Game odds fetch failed: {e}", style="yellow")

    try:
        tgt = pd.to_datetime(target_date).date()
        for ev in events:
            try:
                ct_et = _et_date(ev.get("commence_time"))
            except Exception:
                ct_et = None
            if ct_et == tgt:
                eid = ev.get("id")
                ev_rows.append({
                    "event_id": eid,
                    "commence_time": ev.get("commence_time"),
                    "home_team": ev.get("home_team"),
                    "away_team": ev.get("away_team"),
                    "has_odds": (str(eid) in have_ids) if eid else False,
                })
        if ev_rows:
            df_ev = pd.DataFrame(ev_rows)
            df_ev.to_csv(events_out, index=False)
            # Write alias for site allowlist
            try:
                df_ev.to_csv(events_out_alias, index=False)
            except Exception:
                pass
            console.print({"events_rows": int(len(ev_rows)), "output": str(events_out), "alias": str(events_out_alias)})
        else:
            console.print("No events found for target date (ET)", style="yellow")
    except Exception as e:
        console.print(f"Events snapshot write failed: {e}", style="yellow")

@cli.command("train-pbp-markets")
@click.option("--start", type=str, required=False, help="Optional start date YYYY-MM-DD to fetch PBP if needed")
@click.option("--end", type=str, required=False, help="Optional end date YYYY-MM-DD to fetch PBP if needed")
def train_pbp_markets_cmd(start: str | None, end: str | None):
    """Train ONNX-exportable models for:
    - Tip winner (home_won_tip probability)
    - First basket scorer (per-player scoring model; normalized at inference)
    - Total 3s made in first 3 minutes (regression + Poisson approx)

    Uses existing processed PBP/boxscores when available. If --start/--end are provided,
    will attempt to backfill missing PBP for that range before training.
    """
    console.rule("Train PBP-derived Markets")
    # Best-effort backfill if requested
    if start and end:
        try:
            from .pbp import backfill_pbp
            _ = backfill_pbp(start, end, only_final=True, rate_delay=0.35)
        except Exception as e:
            console.print(f"PBP backfill failed: {e}", style="yellow")
    try:
        arts = train_all_pbp_markets()
        out = {
            k: {"model": str(v.model_path), "onnx": (str(v.onnx_path) if v.onnx_path else None)}
            for k, v in arts.items()
        }
        console.print(out)
    except Exception as e:
        console.print(f"Training failed: {e}", style="red")

@cli.command("predict-pbp-markets")
@click.option("--date", "date_str", type=str, required=True, help="Target date YYYY-MM-DD (games on this day)")
def predict_pbp_markets_cmd(date_str: str):
    """Generate predictions for PBP-derived markets for a given date.

    Outputs under data/processed:
    - tip_winner_probs_<date>.csv
    - first_basket_probs_<date>.csv
    - early_threes_<date>.csv
    """
    console.rule("Predict PBP-derived Markets")
    try:
        tip = predict_tip_for_date(date_str)
        fb = predict_first_basket_for_date(date_str)
        thr = predict_early_threes_for_date(date_str)
        console.print({
            "tip_rows": int(len(tip) if tip is not None else 0),
            "first_basket_rows": int(len(fb) if fb is not None else 0),
            "early_threes_rows": int(len(thr) if thr is not None else 0),
        })
    except Exception as e:
        console.print(f"Prediction failed: {e}", style="red")


@cli.command("backfill-pbp-markets")
@click.option("--start", "start_date", type=str, required=False, help="Start date YYYY-MM-DD; default = season start (Oct 1) of current season")
@click.option("--end", "end_date", type=str, required=False, help="End date YYYY-MM-DD; default = today (local)")
@click.option("--with-pbp", "with_pbp", is_flag=True, default=False, help="Also backfill PBP logs for the range (finals-only)")
def backfill_pbp_markets_cmd(start_date: str | None, end_date: str | None, with_pbp: bool):
    """Backfill PBP-derived market predictions over a date range.

    Writes per-day CSVs under data/processed for:
    - tip_winner_probs_<date>.csv
    - first_basket_probs_<date>.csv
    - early_threes_<date>.csv
    """
    console.rule("Backfill PBP-derived Markets")
    import datetime as _dt
    # Defaults
    today = _dt.date.today()
    if end_date is None:
        e = today
    else:
        try:
            e = pd.to_datetime(end_date).date()
        except Exception:
            console.print("Invalid --end (YYYY-MM-DD)", style="red"); return
    if start_date is None:
        # Compute season start: Oct 1 of season year (season year = year if >= Jul else year-1)
        yr = today.year
        if today.month < 7:
            yr -= 1
        s = _dt.date(yr, 10, 1)
    else:
        try:
            s = pd.to_datetime(start_date).date()
        except Exception:
            console.print("Invalid --start (YYYY-MM-DD)", style="red"); return
    if e < s:
        console.print("--end must be >= --start", style="red"); return
    # Ensure boxscores exist for the date range to build candidates
    try:
        _ = backfill_boxscores(str(s), str(e), only_final=True, rate_delay=0.35)
    except Exception as ex:
        console.print({"warning": f"boxscores backfill failed: {ex}"}, style="yellow")
    if with_pbp:
        try:
            _ = backfill_pbp(str(s), str(e), only_final=True, rate_delay=0.35)
        except Exception as ex:
            console.print({"warning": f"pbp backfill failed: {ex}"}, style="yellow")
    dates = list(pd.date_range(s, e, freq="D").date)
    total = {"tip": 0, "first_basket": 0, "early_threes": 0}
    for d in track(dates, description="Backfilling PBP markets"):
        ds = str(d)
        try:
            tip = predict_tip_for_date(ds)
            fb = predict_first_basket_for_date(ds)
            thr = predict_early_threes_for_date(ds)
            total["tip"] += 0 if tip is None else len(tip)
            total["first_basket"] += 0 if fb is None else len(fb)
            total["early_threes"] += 0 if thr is None else len(thr)
        except Exception as ex:
            console.print({"date": ds, "error": str(ex)}, style="yellow")
    console.print({
        "start": str(s),
        "end": str(e),
        "days": len(dates),
        "rows_tip": int(total["tip"]),
        "rows_first_basket": int(total["first_basket"]),
        "rows_early_threes": int(total["early_threes"]),
    })


@cli.command("export-game-cards")
@click.option("--date", "date_str", type=str, required=True, help="Target date YYYY-MM-DD")
def export_game_cards_cmd(date_str: str):
    """Export compact per-game cards for a date by merging PBP markets and odds.

    Writes data/processed/game_cards_<date>.csv with columns:
    - date, game_id, home_team, visitor_team, commence_time
    - prob_home_tip
    - early_threes_expected, early_threes_prob_ge_1
    - first_basket_top5 ("TEAM: Player (p%)"; semicolon-separated)
    """
    console.rule("Export Game Cards")
    from .teams import to_tricode as _to_tri
    out_path = paths.data_processed / f"game_cards_{date_str}.csv"

    # Load odds for matchup framing (home/visitor and time)
    odds_path = paths.data_processed / f"game_odds_{date_str}.csv"
    if odds_path.exists():
        go = pd.read_csv(odds_path)
    else:
        go = pd.DataFrame(columns=["home_team","visitor_team","commence_time"])  # fallback blank

    # Build gameId mapping from boxscores_<date>
    gid_map = {}
    box_path = paths.data_processed / f"boxscores_{date_str}.csv"
    if box_path.exists():
        bs = pd.read_csv(box_path)
        if not bs.empty:
            # For each gameId, collect team tricodes
            grp = bs.groupby("gameId")["teamTricode"].unique().reset_index()
            for _, r in grp.iterrows():
                game_id = str(r["gameId"]) if pd.notna(r["gameId"]) else None
                arr = r["teamTricode"]
                vals = arr.tolist() if hasattr(arr, "tolist") else (list(arr) if isinstance(arr, (list, tuple)) else [arr])
                teams = set(str(x) for x in vals if isinstance(x, str) and x)
                if game_id and len(teams) == 2:
                    key = tuple(sorted(list(teams)))
                    gid_map[key] = game_id

    # Fallback (pregame): derive (HOME,AWAY)->game_id map from NBA CDN (public)
    if not gid_map:
        try:
            import requests  # type: ignore
            from datetime import datetime as _dt, date as _date
            target = _dt.strptime(date_str, "%Y-%m-%d").date()
            today = _date.today()
            # a) Today's live scoreboard
            if target == today:
                u = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"
                r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=10)
                if r.ok:
                    j = r.json() or {}
                    games = (j.get("scoreboard") or {}).get("games") or []
                    for g in games:
                        gid = str(g.get("gameId") or "").strip()
                        home = str((g.get("homeTeam") or {}).get("teamTricode") or "").upper()
                        away = str((g.get("awayTeam") or {}).get("teamTricode") or "").upper()
                        if gid and home and away:
                            key = tuple(sorted([home, away]))
                            gid_map[key] = gid
            # b) Season schedule (broad scan; avoid strict string-date equals in case of UTC/ET drift)
            if not gid_map:
                u = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json"
                r = requests.get(u, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=15)
                if r.ok:
                    j = r.json() or {}
                    league = j.get("leagueSchedule") or {}
                    game_dates = league.get("gameDates") or []
                    for gd in game_dates:
                        for g in (gd.get("games") or []):
                            gid = str(g.get("gameId") or "").strip()
                            home = str(((g.get("homeTeam") or {}).get("teamTricode")) or "").upper()
                            away = str(((g.get("awayTeam") or {}).get("teamTricode")) or "").upper()
                            if not (gid and home and away):
                                continue
                            # Filter by ET calendar day using startTimeUTC if present; else fall back to gd.gameDate
                            try:
                                from datetime import datetime as _dt
                                from zoneinfo import ZoneInfo as _ZI
                                et = _ZI("US/Eastern")
                            except Exception:
                                et = None
                            ok_day = False
                            st = g.get("gameDateTimeUTC") or g.get("startTimeUTC") or g.get("startTimeUTCFormatted")
                            if st:
                                try:
                                    dt = pd.to_datetime(st, utc=True)
                                    ds = dt.tz_convert("US/Eastern").strftime("%Y-%m-%d") if et else dt.strftime("%Y-%m-%d")
                                    ok_day = (ds == date_str)
                                except Exception:
                                    ok_day = False
                            if not ok_day:
                                gd_str = str(gd.get("gameDate") or "").split("T")[0]
                                ok_day = (gd_str == date_str)
                            if ok_day:
                                key = tuple(sorted([home, away]))
                                gid_map[key] = gid
            # c) Legacy data.nba.net scoreboard for the specific date (robust for historical + today)
            if not gid_map:
                try:
                    ymd = date_str.replace("-", "")
                    u2 = f"https://data.nba.net/prod/v1/{ymd}/scoreboard.json"
                    r2 = requests.get(u2, headers={"Accept":"application/json","User-Agent":"nba-betting/1.0"}, timeout=15)
                    if r2.ok:
                        js = r2.json() or {}
                        games = (js.get("games") or [])
                        for g in games:
                            gid = str(g.get("gameId") or "").strip()
                            home = str(((g.get("hTeam") or {}).get("triCode")) or "").upper()
                            away = str(((g.get("vTeam") or {}).get("triCode")) or "").upper()
                            if gid and home and away:
                                key = tuple(sorted([home, away]))
                                gid_map[key] = gid
                except Exception:
                    pass
            # d) Local schedule helper as a final fallback (stable schema + ET dates)
            if not gid_map:
                try:
                    from .schedule import fetch_schedule_2025_26 as _fetch_sched
                    df = _fetch_sched()
                    if df is not None and not df.empty:
                        # date_est is a date; compare to date_str
                        ds = pd.to_datetime(date_str).date()
                        day = df[pd.to_datetime(df.get("date_est"), errors="coerce").dt.date == ds].copy()
                        for _, r in day.iterrows():
                            gid = str(r.get("game_id") or "").strip()
                            home = str(r.get("home_tricode") or "").upper()
                            away = str(r.get("away_tricode") or "").upper()
                            if gid and home and away:
                                key = tuple(sorted([home, away]))
                                gid_map[key] = gid
                except Exception:
                    pass
        except Exception:
            pass

    # Load PBP market outputs
    tip = None; fb = None; thr = None
    tip_path = paths.data_processed / f"tip_winner_probs_{date_str}.csv"
    if tip_path.exists():
        try:
            tip = pd.read_csv(tip_path)
            # Normalize game_id to zero-padded 10-char strings
            if tip is not None and "game_id" in tip.columns:
                tip["game_id"] = tip["game_id"].astype(str).str.replace(".0$", "", regex=True).str.replace("^nan$","", regex=True).str.replace("^None$","", regex=True).str.replace("^\s+$","", regex=True).str.zfill(10)
            if tip is not None and tip.empty:
                tip = None
        except Exception:
            tip = None
    fb_path = paths.data_processed / f"first_basket_probs_{date_str}.csv"
    if fb_path.exists():
        try:
            fb = pd.read_csv(fb_path)
            if fb is not None and "game_id" in fb.columns:
                fb["game_id"] = fb["game_id"].astype(str).str.replace(".0$", "", regex=True).str.replace("^nan$","", regex=True).str.replace("^None$","", regex=True).str.replace("^\s+$","", regex=True).str.zfill(10)
            if fb is not None and fb.empty:
                fb = None
        except Exception:
            fb = None
    thr_path = paths.data_processed / f"early_threes_{date_str}.csv"
    if thr_path.exists():
        try:
            thr = pd.read_csv(thr_path)
            if thr is not None and "game_id" in thr.columns:
                thr["game_id"] = thr["game_id"].astype(str).str.replace(".0$", "", regex=True).str.replace("^nan$","", regex=True).str.replace("^None$","", regex=True).str.replace("^\s+$","", regex=True).str.zfill(10)
            if thr is not None and thr.empty:
                thr = None
        except Exception:
            thr = None

    rows: list[dict] = []
    # If odds available, iterate matchups; else iterate game IDs from tip/thr/fb
    def _build_row(game_id: Optional[str], home: Optional[str], away: Optional[str], ctime: Optional[str]) -> dict:
        # Normalize game_id to 10-digit zero-padded string for consistent frontend merges
        gid_norm = None
        if game_id is not None and str(game_id).strip():
            gid_norm = str(game_id).strip()
            # Some sources may already be zero-padded; enforce 10-digit
            gid_norm = gid_norm.replace('.0', '') if gid_norm.endswith('.0') else gid_norm
            if gid_norm.isdigit():
                gid_norm = gid_norm.zfill(10)
        row = {"date": date_str, "game_id": gid_norm, "home_team": home, "visitor_team": away, "commence_time": ctime}
        # Attach tip
        if tip is not None and game_id is not None:
            gid_norm = str(game_id).strip()
            gid_norm = gid_norm.zfill(10)
            trow = tip[tip["game_id"].astype(str).str.zfill(10) == gid_norm]
            if not trow.empty and "prob_home_tip" in trow.columns:
                row["prob_home_tip"] = float(trow.iloc[0]["prob_home_tip"])
        # Attach early threes
        if thr is not None and game_id is not None:
            gid_norm = str(game_id).strip().zfill(10)
            h = thr[thr["game_id"].astype(str).str.zfill(10) == gid_norm]
            if not h.empty:
                row["early_threes_expected"] = float(h.iloc[0].get("expected_threes_0_3", h.iloc[0].get("threes_0_3_pred", np.nan)))
                row["early_threes_prob_ge_1"] = float(h.iloc[0].get("prob_ge_1", np.nan))
        # Attach first basket top5
        if fb is not None and game_id is not None:
            gid_norm = str(game_id).strip().zfill(10)
            sub = fb[fb["game_id"].astype(str).str.zfill(10) == gid_norm].copy()
            if not sub.empty:
                sub = sub.sort_values("prob_first_basket", ascending=False).head(5)
                parts = []
                for _, r in sub.iterrows():
                    t = str(r.get("team","")); p = str(r.get("player_name","")); pr = float(r.get("prob_first_basket", 0))*100.0
                    parts.append(f"{t}: {p} ({pr:.1f}%)")
                row["first_basket_top5"] = "; ".join(parts)
        return row

    # Preload schedule day for a last-resort gid lookup by tricodes
    _sched_day = None
    try:
        from .schedule import fetch_schedule_2025_26 as _fetch_sched
        _df_sched = _fetch_sched()
        if _df_sched is not None and not _df_sched.empty:
            ds = pd.to_datetime(date_str).date()
            _sched_day = _df_sched[pd.to_datetime(_df_sched.get("date_est"), errors="coerce").dt.date == ds].copy()
    except Exception:
        _sched_day = None

    if go is not None and not go.empty and {"home_team","visitor_team"}.issubset(go.columns):
        # Normalize to tricodes for matching
        def _tri(x):
            try:
                return _to_tri(str(x))
            except Exception:
                return str(x)
        go = go.copy()
        go["home_tri"] = go["home_team"].map(_tri)
        go["away_tri"] = go["visitor_team"].map(_tri)
        for _, r in go.iterrows():
            home = r.get("home_team"); away = r.get("visitor_team"); ctime = r.get("commence_time")
            key = tuple(sorted([str(r.get("home_tri")), str(r.get("away_tri"))]))
            gid = gid_map.get(key)
            if (not gid) and (_sched_day is not None and not _sched_day.empty):
                try:
                    tri_home = str(r.get("home_tri") or "").upper()
                    tri_away = str(r.get("away_tri") or "").upper()
                    cand = _sched_day[((_sched_day["home_tricode"].astype(str).str.upper()==tri_home) & (_sched_day["away_tricode"].astype(str).str.upper()==tri_away))
                                      | ((_sched_day["home_tricode"].astype(str).str.upper()==tri_away) & (_sched_day["away_tricode"].astype(str).str.upper()==tri_home))]
                    if not cand.empty:
                        gid = str(cand.iloc[0].get("game_id") or "").strip()
                except Exception:
                    pass
            rows.append(_build_row(gid, home, away, ctime))
    else:
        # Fallback: derive from tip/first basket files (game_id only)
        gid_set = set()
        for df in (tip, thr, fb):
            if df is not None and not df.empty and "game_id" in df.columns:
                gid_set.update(str(x) for x in df["game_id"].dropna().unique().tolist())
        for gid in sorted(gid_set):
            rows.append(_build_row(gid, None, None, None))

    cards = pd.DataFrame(rows)
    # Post-attach PBP fields via merges to ensure no row misses due to per-row attach edge cases
    try:
        if not cards.empty:
            cards["gid10"] = cards.get("game_id").astype(str).str.replace(".0$","", regex=True).str.replace("^nan$","", regex=True).str.replace("^None$","", regex=True).str.replace("^\\s+$","", regex=True).str.zfill(10)
            # Tip merge
            if tip is not None and not tip.empty and {"game_id","prob_home_tip"}.issubset(set(tip.columns)):
                tp = tip.copy()
                tp["gid10"] = tp["game_id"].astype(str).str.replace(".0$","", regex=True).str.zfill(10)
                cards = cards.merge(tp[["gid10","prob_home_tip"]], on="gid10", how="left")
            # Early threes merge
            if thr is not None and not thr.empty and "game_id" in thr.columns:
                th = thr.copy()
                th["gid10"] = th["game_id"].astype(str).str.replace(".0$","", regex=True).str.zfill(10)
                # Prefer expected_threes_0_3 if present; else threes_0_3_pred
                exp_col = "expected_threes_0_3" if "expected_threes_0_3" in th.columns else ("threes_0_3_pred" if "threes_0_3_pred" in th.columns else None)
                if exp_col is not None:
                    th = th[["gid10", exp_col, "prob_ge_1"]].rename(columns={exp_col:"early_threes_expected", "prob_ge_1":"early_threes_prob_ge_1"})
                    cards = cards.merge(th, on="gid10", how="left")
            # First basket top5 aggregation
            if fb is not None and not fb.empty and {"game_id","team","player_name","prob_first_basket"}.issubset(set(fb.columns)):
                f = fb.copy()
                f["gid10"] = f["game_id"].astype(str).str.replace(".0$","", regex=True).str.zfill(10)
                f = f.sort_values(["gid10","prob_first_basket"], ascending=[True, False])
                top5 = f.groupby("gid10").head(5).copy()
                top5["part"] = top5.apply(lambda r: f"{str(r.get('team',''))}: {str(r.get('player_name',''))} ({float(r.get('prob_first_basket',0))*100:.1f}%)", axis=1)
                agg = top5.groupby("gid10")["part"].apply(lambda s: "; ".join(s.tolist())).reset_index().rename(columns={"part":"first_basket_top5"})
                cards = cards.merge(agg, on="gid10", how="left")
            # Coalesce into clean canonical columns
            for to_col in ("prob_home_tip","early_threes_expected","early_threes_prob_ge_1","first_basket_top5"):
                cols = []
                if f"{to_col}_x" in cards.columns: cols.append(f"{to_col}_x")
                if f"{to_col}_y" in cards.columns: cols.append(f"{to_col}_y")
                if to_col in cards.columns: cols.append(to_col)
                if cols:
                    ser = None
                    for c in cols:
                        s = cards.get(c)
                        if ser is None:
                            ser = s
                        else:
                            try:
                                ser = ser.where(ser.notna(), s)
                            except Exception:
                                pass
                    if ser is not None:
                        cards[to_col] = ser
            # Drop suffix variants after coalescing
            drop_cols = [c for c in cards.columns if c.endswith("_x") or c.endswith("_y")]
            if drop_cols:
                try:
                    cards = cards.drop(columns=drop_cols)
                except Exception:
                    for c in drop_cols:
                        try:
                            cards = cards.drop(columns=[c])
                        except Exception:
                            pass
            # Drop helper key
            cards = cards.drop(columns=["gid10"]) 
    except Exception as ex:
        console.print({"warning": f"post-merge of PBP fields failed: {ex}"}, style="yellow")

    cards.to_csv(out_path, index=False)
    console.print({"rows": int(len(cards)), "output": str(out_path)})


@cli.command("reconcile-pbp-markets")
@click.option("--date", "date_str", type=str, required=True, help="Target date YYYY-MM-DD to reconcile PBP-derived markets")
def reconcile_pbp_markets_cmd(date_str: str):
    """Reconcile PBP-derived markets (tip, first-basket, early-threes) for a date.

    Writes per-game reconciliation to data/processed/pbp_reconcile_<date>.csv and prints summary metrics.
    """
    console.rule("Reconcile PBP-derived Markets")
    try:
        _ = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return

    # Load predictions (safe reader to handle blank files)
    def _read_csv_safe(path: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    tip_path = paths.data_processed / f"tip_winner_probs_{date_str}.csv"
    fb_path = paths.data_processed / f"first_basket_probs_{date_str}.csv"
    thr_path = paths.data_processed / f"early_threes_{date_str}.csv"
    tip = _read_csv_safe(tip_path) if tip_path.exists() else pd.DataFrame()
    fb = _read_csv_safe(fb_path) if fb_path.exists() else pd.DataFrame()
    thr = _read_csv_safe(thr_path) if thr_path.exists() else pd.DataFrame()

    # Build PBP map by game_id using combined file if available; else per-game files by discovered IDs
    pbp_map: dict[str, pd.DataFrame] = {}
    pbp_comb = paths.data_processed / f"pbp_{date_str}.csv"
    if pbp_comb.exists():
        try:
            df = pd.read_csv(pbp_comb)
            if "game_id" in df.columns and not df.empty:
                for gid, grp in df.groupby("game_id"):
                    key_raw = str(gid).strip()
                    # Accept only numeric gameIds to avoid 'NA' and placeholders
                    if not key_raw or not key_raw.isdigit():
                        continue
                    pbp_map[key_raw.zfill(10)] = grp.copy()
        except Exception:
            pbp_map = {}
    if not pbp_map:
        # Discover game IDs from any available predictions or CDN schedule
        gids = set()
        for df in (tip, fb, thr):
            if not df.empty and "game_id" in df.columns:
                gids.update(str(x) for x in df["game_id"].dropna().astype(str).tolist())
        if not gids:
            try:
                from .pbp_markets import _game_ids_for_date as _ids
                gids = set(_ids(date_str))
            except Exception:
                gids = set()
        dpg = paths.data_processed / "pbp"
        for gid in gids:
            # Only consider numeric gids
            gid_s = str(gid).strip()
            if not gid_s.isdigit():
                continue
            gzp = gid_s.zfill(10)
            for name in (f"pbp_{gid_s}.csv", f"pbp_{gzp}.csv"):
                fpath = dpg / name
                if fpath.exists():
                    try:
                        pbp_map[gzp] = pd.read_csv(fpath)
                        break
                    except Exception:
                        continue

    # Home/away map for this date (for tip winner mapping)
    _gid_to_homeaway: dict[str, tuple[str,str]] = {}
    try:
        from .pbp_markets import _gid_team_map_for_date as _map
        m = _map(date_str) or {}
        _gid_to_homeaway = {str(k): (v[0], v[1]) for k, v in m.items()}
    except Exception:
        _gid_to_homeaway = {}

    # Helpers
    def _count_early_threes_q1(gdf: pd.DataFrame) -> int:
        if gdf is None or gdf.empty:
            return 0
        desc_cols = _pbp_desc_cols(gdf)
        c_per = "PERIOD" if "PERIOD" in gdf.columns else ("period" if "period" in gdf.columns else None)
        tmp = gdf.copy()
        if c_per:
            try:
                tmp = tmp[tmp[c_per] == 1]
            except Exception:
                pass
        # Prefer actionNumber ordering; else compute elapsed via shared parser
        if "actionNumber" in gdf.columns:
            try:
                tmp = tmp.sort_values("actionNumber", ascending=True)
            except Exception:
                pass
        # else: leave original order; we'll guard with per-row elapsed checks
        cnt = 0
        for _, r in tmp.iterrows():
            # Compute elapsed using shared helper
            try:
                from .pbp_markets import _to_sec_left as _sec
            except Exception:
                _sec = None
            t = r.get("PCTIMESTRING") or r.get("clock") or r.get("time")
            sec_left = _sec(t) if _sec else None
            if sec_left is None:
                continue
            elapsed = 12*60 - sec_left
            if elapsed is None or elapsed > 180:
                continue
            text = " ".join([str(r.get(c, "")) for c in desc_cols]).lower()
            # Handle both textual and structured formats
            made_three_text = ("3pt" in text) and ("made" in text or "makes" in text or "jump shot" in text)
            made_three_struct = False
            try:
                if (str(r.get("shotResult")).lower() == "made") and int(r.get("shotValue") or 0) == 3:
                    made_three_struct = True
            except Exception:
                made_three_struct = False
            if made_three_text or made_three_struct:
                cnt += 1
        return int(cnt)

    # Build reconciliation rows
    rows: list[dict] = []
    # Build union of game_ids from predictions and pbp_map
    gid_set = set()
    for df in (tip, fb, thr):
        if not df.empty and "game_id" in df.columns:
            gid_set.update(str(x) for x in df["game_id"].dropna().astype(str).tolist())
    gid_set.update(pbp_map.keys())

    for gid in sorted(gid_set):
        gid_key = str(gid)
        gid_norm = gid_key.zfill(10) if gid_key.isdigit() else gid_key
        rec: dict = {"date": date_str, "game_id": gid_norm}
        # Team tricodes if available
        home, away = _gid_to_homeaway.get(gid_key) or _gid_to_homeaway.get(gid_norm) or (None, None)
        # If not available, attempt to derive from the game's PBP (location h/v)
        gdf = None
        for _k in (gid_key, gid_norm):
            if _k in pbp_map:
                gdf = pbp_map[_k]
                break
        if (home is None or away is None) and gdf is not None and not gdf.empty:
            try:
                if {"teamTricode","location"}.issubset(set(gdf.columns)):
                    tri_h = gdf[gdf["location"].astype(str).str.lower()=="h"].get("teamTricode").dropna()
                    tri_v = gdf[gdf["location"].astype(str).str.lower()=="v"].get("teamTricode").dropna()
                    h0 = str(tri_h.iloc[0]).upper() if len(tri_h)>0 else None
                    v0 = str(tri_v.iloc[0]).upper() if len(tri_v)>0 else None
                    if h0 and v0:
                        home, away = h0, v0
            except Exception:
                pass
        rec.update({"home_team": home, "visitor_team": away})

        # Tip reconciliation
        p_home = None
        if not tip.empty:
            m = tip[tip["game_id"].astype(str).str.zfill(10) == gid_norm]
            if not m.empty and "prob_home_tip" in m.columns:
                try:
                    p_home = float(m.iloc[0]["prob_home_tip"])
                except Exception:
                    p_home = None
        rec["tip_prob_home"] = p_home
        outcome = None
        if gdf is not None and not gdf.empty:
            ev = _pbp_jump_ball_event(gdf)
            if ev:
                winner_text = (ev.get("winner_text") or "").strip()
                if winner_text:
                    # Try to infer the winner's team directly from PBP player names
                    try:
                        name_cols = [c for c in ("playerName","PLAYER1_NAME","player1_name") if c in gdf.columns]
                        team_cols = [c for c in ("teamTricode","PLAYER1_TEAM_ABBREVIATION","team_abbr") if c in gdf.columns]
                        t_winner = None
                        if name_cols and team_cols:
                            nl = winner_text.lower()
                            for _, rr in gdf.iterrows():
                                nm = None
                                for c in name_cols:
                                    nv = rr.get(c)
                                    if isinstance(nv, str) and nv.strip():
                                        nm = nv; break
                                if not nm:
                                    continue
                                if nl in str(nm).lower():
                                    tv = None
                                    for tc in team_cols:
                                        tv = rr.get(tc)
                                        if isinstance(tv, str) and tv.strip():
                                            break
                                    if tv:
                                        t_winner = str(tv).upper()
                                        break
                        if t_winner and home and away:
                            if t_winner == str(home).upper():
                                outcome = 1.0
                            elif t_winner == str(away).upper():
                                outcome = 0.0
                    except Exception:
                        pass
        rec["tip_outcome_home"] = outcome
        if p_home is not None and outcome is not None:
            try:
                brier = (float(p_home) - float(outcome))**2
                import math
                logloss = -(float(outcome)*math.log(max(1e-9, float(p_home))) + (1-float(outcome))*math.log(max(1e-9, 1-float(p_home))))
            except Exception:
                brier = None; logloss = None
        else:
            brier = None; logloss = None
        rec["tip_brier"] = brier
        rec["tip_logloss"] = logloss

        # First basket reconciliation
        fb_sub = fb[fb["game_id"].astype(str).str.zfill(10) == gid_norm] if not fb.empty else pd.DataFrame()
        fb_top1_name = None; fb_top1_prob = None; fb_actual_name = None; fb_hit_top1 = None; fb_hit_top5 = None; fb_prob_actual = None
        if gdf is not None and not gdf.empty:
            ev = _pbp_first_fg_event(gdf)
            if ev:
                fb_actual_name = (ev.get("player_name") or "").strip()
        if not fb_sub.empty:
            sub = fb_sub.copy().sort_values("prob_first_basket", ascending=False)
            try:
                fb_top1_name = str(sub.iloc[0].get("player_name"))
                fb_top1_prob = float(sub.iloc[0].get("prob_first_basket"))
            except Exception:
                pass
            if fb_actual_name:
                name_l = fb_actual_name.lower()
                # Top-1
                fb_hit_top1 = name_l in str(fb_top1_name or "").lower()
                # Top-5
                hit5 = False; p_act = None
                for _, r in sub.head(5).iterrows():
                    if name_l in str(r.get("player_name","")) .lower():
                        hit5 = True; p_act = float(r.get("prob_first_basket", np.nan)); break
                fb_hit_top5 = hit5
                if p_act is None:
                    m2 = sub[sub["player_name"].astype(str).str.lower().str.contains(name_l)].head(1)
                    if not m2.empty:
                        try:
                            p_act = float(m2.iloc[0].get("prob_first_basket"))
                        except Exception:
                            p_act = None
                fb_prob_actual = p_act
        rec.update({
            "first_basket_top1_name": fb_top1_name,
            "first_basket_top1_prob": fb_top1_prob,
            "first_basket_actual_name": fb_actual_name,
            "first_basket_hit_top1": fb_hit_top1,
            "first_basket_hit_top5": fb_hit_top5,
            "first_basket_prob_actual": fb_prob_actual,
        })

        # Early threes reconciliation
        yhat = None; p_ge1 = None
        if not thr.empty:
            m = thr[thr["game_id"].astype(str).str.zfill(10) == gid_norm]
            if not m.empty:
                try:
                    yhat = float(m.iloc[0].get("expected_threes_0_3", m.iloc[0].get("threes_0_3_pred", np.nan)))
                except Exception:
                    yhat = None
                try:
                    p_ge1 = float(m.iloc[0].get("prob_ge_1", np.nan))
                except Exception:
                    p_ge1 = None
        actual_thr = None; err_thr = None; brier_ge1 = None
        if gdf is not None and not gdf.empty:
            actual_thr = _count_early_threes_q1(gdf)
            if yhat is not None:
                try:
                    err_thr = float(actual_thr) - float(yhat)
                except Exception:
                    err_thr = None
            if p_ge1 is None and yhat is not None:
                p_ge1 = 1.0 - float(np.exp(-max(0.0, float(yhat))))
            if p_ge1 is not None and actual_thr is not None:
                brier_ge1 = (float(p_ge1) - (1.0 if int(actual_thr) >= 1 else 0.0))**2
        rec.update({
            "early_threes_expected": yhat,
            "early_threes_prob_ge_1": p_ge1,
            "early_threes_actual": actual_thr,
            "early_threes_error": err_thr,
            "early_threes_brier_ge1": brier_ge1,
        })

        rows.append(rec)

    out = paths.data_processed / f"pbp_reconcile_{date_str}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)

    # Print summary metrics
    df = pd.DataFrame(rows)
    tip_brier = float(pd.to_numeric(df.get("tip_brier"), errors="coerce").dropna().mean()) if not df.empty else float("nan")
    tip_acc = float(np.mean([(float(p)>=0.5)==(float(y)==1.0) for p, y in zip(pd.to_numeric(df.get("tip_prob_home"), errors="coerce").dropna(), pd.to_numeric(df.get("tip_outcome_home"), errors="coerce").dropna())])) if ("tip_prob_home" in df.columns and "tip_outcome_home" in df.columns) else float("nan")
    if {"first_basket_hit_top1","first_basket_hit_top5"}.issubset(set(df.columns)) and not df.empty:
        fb_df = df.dropna(subset=["first_basket_hit_top1","first_basket_hit_top5"], how="all")
        fb_top1_acc = float(pd.to_numeric(fb_df.get("first_basket_hit_top1"), errors="coerce").dropna().mean()) if not fb_df.empty else float("nan")
        fb_top5_cov = float(pd.to_numeric(fb_df.get("first_basket_hit_top5"), errors="coerce").dropna().mean()) if not fb_df.empty else float("nan")
    else:
        fb_top1_acc = float("nan"); fb_top5_cov = float("nan")
    _thr_col = df.get("early_threes_error")
    if isinstance(_thr_col, pd.Series):
        thr_errs = pd.to_numeric(_thr_col, errors="coerce").dropna()
    else:
        thr_errs = pd.Series(dtype=float)
    thr_mae = float(thr_errs.abs().mean()) if len(thr_errs) > 0 else float("nan")
    thr_rmse = float(np.sqrt((thr_errs**2).mean())) if len(thr_errs) > 0 else float("nan")
    thr_brier = float(pd.to_numeric(df.get("early_threes_brier_ge1"), errors="coerce").dropna().mean()) if not df.empty else float("nan")
    console.print({
        "date": date_str,
        "output": str(out),
        "tip": {"brier": tip_brier, "acc@0.5": tip_acc},
        "first_basket": {"top1_acc": fb_top1_acc, "top5_cov": fb_top5_cov},
        "early_threes": {"mae": thr_mae, "rmse": thr_rmse, "brier_ge1": thr_brier},
    })

@cli.command("reconcile-quarters")
@click.option("--date", "date_str", type=str, required=True, help="Target date YYYY-MM-DD to reconcile quarters/halves vs predictions")
def reconcile_quarters_cmd(date_str: str):
    """Reconcile per-quarter and half totals for a date by joining predictions with raw line scores.

    Writes data/processed/recon_quarters_<date>.csv with columns including:
    - date, home_team, visitor_team, home_tri, away_tri
    - actual_q{1..4}_total, actual_h1_total, actual_h2_total, actual_game_total
    - pred_q{1..4}_total, pred_h1_total, pred_h2_total, pred_game_total
    - err_q{1..4}_total, err_h1_total, err_h2_total, err_game_total (actual - pred)
    """
    console.rule("Reconcile Quarters")
    try:
        _ = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return

    from .teams import to_tricode as _to_tri, normalize_team as _norm
    # Locate predictions for the date (prefer NPU periods if available)
    pred_candidates = [
        paths.data_processed / f"games_predictions_npu_{date_str}.csv",
        paths.data_processed / f"predictions_{date_str}.csv",
        paths.root / f"predictions_{date_str}.csv",
    ]
    pred_path = next((p for p in pred_candidates if p.exists()), None)
    if pred_path is None:
        console.print(f"No predictions found for {date_str}", style="red"); return
    try:
        preds = pd.read_csv(pred_path)
    except Exception as e:
        console.print(f"Failed to read predictions: {e}", style="red"); return
    if preds is None or preds.empty:
        console.print("Predictions file is empty", style="red"); return

    # Normalize prediction team names and tricodes
    preds = preds.copy()
    for c in ("home_team","visitor_team"):
        if c in preds.columns:
            preds[c] = preds[c].apply(_norm)
        else:
            preds[c] = pd.NA
    preds["home_tri"] = preds["home_team"].astype(str).map(_to_tri)
    preds["away_tri"] = preds["visitor_team"].astype(str).map(_to_tri)

    # Identify prediction columns
    def _first_col(df: pd.DataFrame, names: list[str]) -> str | None:
        for n in names:
            if n in df.columns:
                return n
        return None
    # Game total and per-period predicted totals columns (model naming from games_npu.predict_game)
    game_pred_col = _first_col(preds, ["totals","pred_total","model_total","total"])
    # Period columns usually: quarters_q{i}_total and halves_h{1,2}_total
    have_quarters = any((f"quarters_q{i}_total" in preds.columns) for i in range(1,5))
    have_halves = any((f"halves_h{i}_total" in preds.columns) for i in range(1,3))

    # Load raw games with line scores and filter to date
    raw_csv = paths.data_raw / "games_nba_api.csv"
    raw_parq = paths.data_raw / "games_nba_api.parquet"
    raw = None
    if raw_parq.exists():
        try:
            raw = pd.read_parquet(raw_parq)
        except Exception:
            raw = None
    if raw is None and raw_csv.exists():
        try:
            raw = pd.read_csv(raw_csv)
        except Exception:
            raw = None
    if raw is None or raw.empty:
        console.print("Raw games with line scores not found. Run fetch or enrich-periods.", style="red"); return
    # Filter to date
    try:
        raw_day = raw.copy()
        raw_day["date"] = pd.to_datetime(raw_day["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        raw_day = raw_day[raw_day["date"] == date_str]
    except Exception:
        raw_day = raw[raw.get("date").astype(str) == date_str].copy()
    if raw_day is None or raw_day.empty:
        console.print(f"No raw games found on {date_str}", style="yellow"); return
    # Compute tricodes
    raw_day = raw_day.copy()
    raw_day["home_tri"] = raw_day["home_team"].astype(str).map(_to_tri)
    raw_day["away_tri"] = raw_day["visitor_team"].astype(str).map(_to_tri)

    # Build reconciliation rows by matchup key (home_tri,away_tri)
    rows: list[dict] = []
    for _, g in raw_day.iterrows():
        htri = str(g.get("home_tri") or "").upper(); atri = str(g.get("away_tri") or "").upper()
        if not htri or not atri:
            continue
        # Match prediction by tricode pair
        m = preds[(preds["home_tri"].astype(str).str.upper() == htri) & (preds["away_tri"].astype(str).str.upper() == atri)]
        if m.empty:
            # Try reversed (in case of naming mismatch)
            m = preds[(preds["home_tri"].astype(str).str.upper() == htri) & (preds["visitor_team"].astype(str).map(_to_tri).str.upper() == atri)]
        pred_row = m.iloc[0] if not m.empty else None

        rec: dict = {
            "date": date_str,
            "home_team": g.get("home_team"),
            "visitor_team": g.get("visitor_team"),
            "home_tri": htri,
            "away_tri": atri,
        }
        # Actuals from raw line scores
        aq = {}
        for i in range(1,5):
            hq = g.get(f"home_q{i}"); vq = g.get(f"visitor_q{i}")
            try:
                aq[f"q{i}"] = (float(hq) if pd.notna(hq) else 0.0) + (float(vq) if pd.notna(vq) else 0.0)
            except Exception:
                aq[f"q{i}"] = pd.NA
        # Halves
        def _sum2(a,b):
            try:
                return (float(a) if pd.notna(a) else 0.0) + (float(b) if pd.notna(b) else 0.0)
            except Exception:
                return pd.NA
        ah1 = _sum2(g.get("home_q1"), g.get("visitor_q1"))
        ah1 = (ah1 + _sum2(g.get("home_q2"), g.get("visitor_q2"))) if pd.notna(ah1) else pd.NA
        ah2 = _sum2(g.get("home_q3"), g.get("visitor_q3"))
        ah2 = (ah2 + _sum2(g.get("home_q4"), g.get("visitor_q4"))) if pd.notna(ah2) else pd.NA
        atot = None
        try:
            atot = (float(g.get("home_pts")) if pd.notna(g.get("home_pts")) else 0.0) + (float(g.get("visitor_pts")) if pd.notna(g.get("visitor_pts")) else 0.0)
        except Exception:
            atot = pd.NA

        for i in range(1,5):
            rec[f"actual_q{i}_total"] = aq.get(f"q{i}")
        rec["actual_h1_total"] = ah1
        rec["actual_h2_total"] = ah2
        rec["actual_game_total"] = atot

        # Predictions
        if pred_row is not None:
            # Quarter preds
            for i in range(1,5):
                pk = f"quarters_q{i}_total"
                rec[f"pred_q{i}_total"] = float(pred_row.get(pk)) if pk in pred_row.index and pd.notna(pred_row.get(pk)) else pd.NA
            # Halves from columns or derive from quarters if missing
            for hi, parts in [(1, (1,2)), (2, (3,4))]:
                hk = f"halves_h{hi}_total"
                val = float(pred_row.get(hk)) if hk in pred_row.index and pd.notna(pred_row.get(hk)) else pd.NA
                if (pd.isna(val)) and all(pd.notna(rec.get(f"pred_q{j}_total")) for j in parts):
                    val = float(rec.get(f"pred_q{parts[0]}_total", 0.0)) + float(rec.get(f"pred_q{parts[1]}_total", 0.0))
                rec[f"pred_h{hi}_total"] = val
            # Game total prediction
            if game_pred_col is not None:
                try:
                    rec["pred_game_total"] = float(pred_row.get(game_pred_col))
                except Exception:
                    rec["pred_game_total"] = pd.NA
            else:
                # Sum quarters if available
                if all(pd.notna(rec.get(f"pred_q{i}_total")) for i in range(1,5)):
                    rec["pred_game_total"] = sum(float(rec.get(f"pred_q{i}_total", 0.0)) for i in range(1,5))
                else:
                    rec["pred_game_total"] = pd.NA
        else:
            # No prediction available; leave pred_* as NA
            for i in range(1,5):
                rec[f"pred_q{i}_total"] = pd.NA
            rec["pred_h1_total"] = pd.NA
            rec["pred_h2_total"] = pd.NA
            rec["pred_game_total"] = pd.NA

        # Errors (actual - pred)
        for i in range(1,5):
            a = rec.get(f"actual_q{i}_total"); p = rec.get(f"pred_q{i}_total")
            rec[f"err_q{i}_total"] = (float(a) - float(p)) if (pd.notna(a) and pd.notna(p)) else pd.NA
        for hi, ak in [(1, "actual_h1_total"), (2, "actual_h2_total")]:
            a = rec.get(ak); p = rec.get(f"pred_h{hi}_total")
            rec[f"err_h{hi}_total"] = (float(a) - float(p)) if (pd.notna(a) and pd.notna(p)) else pd.NA
        a = rec.get("actual_game_total"); p = rec.get("pred_game_total")
        rec["err_game_total"] = (float(a) - float(p)) if (pd.notna(a) and pd.notna(p)) else pd.NA

        rows.append(rec)

    out = paths.data_processed / f"recon_quarters_{date_str}.csv"
    pd.DataFrame(rows).to_csv(out, index=False)
    console.print({"date": date_str, "rows": int(len(rows)), "output": str(out)})

@cli.command("calibrate-totals")
@click.option("--anchor", "anchor_date", type=str, required=True, help="Anchor date YYYY-MM-DD (typically yesterday)")
@click.option("--window", type=int, default=14, show_default=True, help="Lookback window in days (excludes anchor)")
@click.option("--prior-games", type=float, default=20.0, show_default=True, help="Empirical-Bayes prior strength for per-team shrinkage")
def calibrate_totals_cmd(anchor_date: str, window: int, prior_games: float):
    """Compute rolling bias calibration for game and quarter totals.

    Produces data/processed/calibration_totals_<anchor>.json with keys:
    - global: game_total_bias, q{1..4}_bias
    - team: per-team overrides with shrinkage toward global
    """
    console.rule("Calibrate Totals (rolling)")
    try:
        a = pd.to_datetime(anchor_date).date()
    except Exception:
        console.print("Invalid --anchor (YYYY-MM-DD)", style="red"); return
    if window <= 0:
        console.print("--window must be > 0", style="red"); return
    # Build date range excluding anchor
    dates = list(pd.date_range(a - pd.Timedelta(days=window), a - pd.Timedelta(days=1), freq="D").date)
    if not dates:
        console.print("Empty window; nothing to calibrate", style="yellow"); return

    # Helper to winsorize errors (robustness against outliers)
    def _winsorize(s: pd.Series, lo: float = 0.05, hi: float = 0.95) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return s
        ql, qh = s.quantile(lo), s.quantile(hi)
        return s.clip(lower=ql, upper=qh)

    # Accumulate errors across dates
    all_rows: list[pd.DataFrame] = []
    for d in dates:
        ds = d.strftime("%Y-%m-%d")
        # 1) Prefer recon_quarters if exists (already has pred/actual totals)
        rq = paths.data_processed / f"recon_quarters_{ds}.csv"
        if rq.exists():
            try:
                part = pd.read_csv(rq)
                part["date"] = ds
                all_rows.append(part)
                continue
            except Exception:
                pass
        # 2) Fallback: recon_games for the day (game-level totals)
        rg = paths.data_processed / f"recon_games_{ds}.csv"
        if rg.exists():
            try:
                gdf = pd.read_csv(rg)
                cols = {c.lower(): c for c in gdf.columns}
                # recon_games has actual totals but may lack pred_total; if so, merge with predictions
                have_actual = {"home_tri","away_tri","total_actual"}.issubset(set(cols.keys()))
                if have_actual:
                    gsel = gdf[[cols["home_tri"], cols["away_tri"], cols["total_actual"]]].copy()
                    gsel.columns = ["home_tri","away_tri","actual_game_total"]
                    # Attach predictions for pred_game_total
                    from .teams import to_tricode as _tri, normalize_team as _norm
                    pred_candidates = [
                        paths.data_processed / f"games_predictions_npu_{ds}.csv",
                        paths.data_processed / f"predictions_{ds}.csv",
                        paths.root / f"predictions_{ds}.csv",
                    ]
                    pred_path = next((p for p in pred_candidates if p.exists()), None)
                    if pred_path is None:
                        # If recon has pred_total column populated, use it; else skip
                        if "pred_total" in cols:
                            tmp = gdf[[cols["home_tri"], cols["away_tri"], cols["pred_total"], cols["total_actual"]]].copy()
                            tmp.columns = ["home_tri","away_tri","pred_game_total","actual_game_total"]
                            tmp["date"] = ds
                            all_rows.append(tmp)
                            continue
                    else:
                        pr = pd.read_csv(pred_path)
                        pr = pr.copy(); pr["home_tri"] = pr.get("home_team","").astype(str).map(_norm).map(_tri); pr["away_tri"] = pr.get("visitor_team","").astype(str).map(_norm).map(_tri)
                        def _pick_col(df, names):
                            for n in names:
                                if n in df.columns:
                                    return n
                            return None
                        pcol = _pick_col(pr, ["totals","pred_total","model_total","total"])
                        if pcol is not None:
                            pr2 = pr[["home_tri","away_tri", pcol]].copy().rename(columns={pcol: "pred_game_total"})
                            merged = gsel.merge(pr2, on=["home_tri","away_tri"], how="inner")
                            if not merged.empty:
                                merged["date"] = ds
                                all_rows.append(merged[["date","home_tri","away_tri","pred_game_total","actual_game_total"]])
                                continue
            except Exception:
                pass
        # 3) Fallback: join finals_<date>.csv with predictions for that date
        try:
            from .teams import to_tricode as _tri, normalize_team as _norm
            pred_candidates = [
                paths.data_processed / f"games_predictions_npu_{ds}.csv",
                paths.data_processed / f"predictions_{ds}.csv",
                paths.root / f"predictions_{ds}.csv",
            ]
            pred_path = next((p for p in pred_candidates if p.exists()), None)
            finals_path = paths.data_processed / f"finals_{ds}.csv"
            if pred_path is None or (not finals_path.exists()):
                # 4) Last resort: raw line scores if finals missing
                raw_csv = paths.data_raw / "games_nba_api.csv"
                if pred_path is None or (not raw_csv.exists()):
                    continue
                pr = pd.read_csv(pred_path)
                pr = pr.copy(); pr["home_tri"] = pr.get("home_team","").astype(str).map(_norm).map(_tri); pr["away_tri"] = pr.get("visitor_team","").astype(str).map(_norm).map(_tri)
                rw = pd.read_csv(raw_csv)
                rw = rw.copy(); rw["date"] = pd.to_datetime(rw["date"], errors="coerce").dt.strftime("%Y-%m-%d"); rw = rw[rw["date"] == ds]
                rw["home_tri"] = rw["home_team"].astype(str).map(_tri); rw["away_tri"] = rw["visitor_team"].astype(str).map(_tri)
                if pr.empty or rw.empty:
                    continue
                rows_local = []
                for _, rgrow in rw.iterrows():
                    h = str(rgrow.get("home_tri") or "").upper(); a2 = str(rgrow.get("away_tri") or "").upper()
                    m = pr[(pr["home_tri"].astype(str).str.upper()==h) & (pr["away_tri"].astype(str).str.upper()==a2)]
                    if m.empty:
                        continue
                    r0 = m.iloc[0]
                    def _pick(dfrow, names):
                        for n in names:
                            if n in dfrow.index and pd.notna(dfrow.get(n)):
                                return float(dfrow.get(n))
                        return None
                    pg = _pick(r0, ["totals","pred_total","model_total","total"]) or float("nan")
                    at = float(rgrow.get("home_pts") or 0.0) + float(rgrow.get("visitor_pts") or 0.0)
                    rows_local.append({"date": ds, "home_tri": h, "away_tri": a2, "pred_game_total": pg, "actual_game_total": at})
                if rows_local:
                    all_rows.append(pd.DataFrame(rows_local))
            else:
                pr = pd.read_csv(pred_path)
                pr = pr.copy(); pr["home_tri"] = pr.get("home_team","").astype(str).map(_norm).map(_tri); pr["away_tri"] = pr.get("visitor_team","").astype(str).map(_norm).map(_tri)
                fn = pd.read_csv(finals_path)
                # Normalize finals columns
                fcols = {c.lower(): c for c in fn.columns}
                # We need home_tri, away_tri, home_pts, visitor_pts
                needf = {"home_tri","away_tri","home_pts","visitor_pts"}
                if not needf.issubset(set(fcols.keys())):
                    continue
                fn2 = fn[[fcols["home_tri"], fcols["away_tri"], fcols["home_pts"], fcols["visitor_pts"]]].copy()
                fn2.columns = ["home_tri","away_tri","home_pts","visitor_pts"]
                # Select prediction total column
                def _pick_col(df, names):
                    for n in names:
                        if n in df.columns:
                            return n
                    return None
                pred_col = _pick_col(pr, ["totals","pred_total","model_total","total"])
                if pred_col is None:
                    continue
                pr2 = pr[["home_tri","away_tri", pred_col]].copy().rename(columns={pred_col: "pred_game_total"})
                merged = pr2.merge(fn2, on=["home_tri","away_tri"], how="inner")
                if merged.empty:
                    continue
                merged["actual_game_total"] = pd.to_numeric(merged.get("home_pts"), errors="coerce") + pd.to_numeric(merged.get("visitor_pts"), errors="coerce")
                merged["date"] = ds
                out_part = merged[["date","home_tri","away_tri","pred_game_total","actual_game_total"]].copy()
                out_part = out_part.dropna(subset=["pred_game_total","actual_game_total"], how="any")
                if not out_part.empty:
                    all_rows.append(out_part)
        except Exception:
            continue

    if not all_rows:
        console.print("No data found in window; skipping calibration", style="yellow"); return
    df = pd.concat(all_rows, ignore_index=True)
    # Ensure numeric
    df["pred_game_total"] = pd.to_numeric(df.get("pred_game_total", pd.NA), errors="coerce")
    df["actual_game_total"] = pd.to_numeric(df.get("actual_game_total", pd.NA), errors="coerce")
    df = df.dropna(subset=["pred_game_total","actual_game_total"], how="any")
    if df.empty:
        console.print("No comparable rows after filtering", style="yellow"); return
    df["err_game_total"] = df["actual_game_total"] - df["pred_game_total"]

    # Global bias (winsorized mean)
    g_err = _winsorize(df["err_game_total"], 0.05, 0.95)
    global_bias = float(g_err.mean()) if len(g_err) > 0 else 0.0

    # Per-team bias with shrinkage toward global
    # Approximate team predicted points by splitting total using predicted margin if available
    # home_pred_pts = (total + margin)/2; away_pred_pts = total - home_pred_pts
    # If margin unavailable, split evenly
    # Collect per-team rows
    team_rows = []
    # Try to attach margin from predictions if available (best-effort merge by date+matchup)
    # We won't refetch; simply compute team error from finals and implied split
    for _, r in df.iterrows():
        try:
            h = str(r.get("home_tri") or r.get("home_team") or "").upper()
            a2 = str(r.get("away_tri") or r.get("visitor_team") or "").upper()
        except Exception:
            h = None; a2 = None
        if not h or not a2:
            continue
        # Without per-row margin, assume even split for calibration baseline (robust)
        tot = float(r.get("pred_game_total"))
        hp = tot/2.0; ap = tot/2.0
        # Actual team totals require finals; try to load per-date finals quickly
        ds = str(r.get("date"))
        try:
            fin = pd.read_csv(paths.data_processed / f"finals_{ds}.csv")
        except Exception:
            fin = pd.DataFrame()
        if not fin.empty:
            fin["home_tri"] = fin.get("home_tri").astype(str).str.upper(); fin["away_tri"] = fin.get("away_tri").astype(str).str.upper()
            mm = fin[(fin["home_tri"]==h) & (fin["away_tri"]==a2)]
            if not mm.empty:
                hh = pd.to_numeric(mm.iloc[0].get("home_pts"), errors="coerce")
                vv = pd.to_numeric(mm.iloc[0].get("visitor_pts"), errors="coerce")
                if pd.notna(hh):
                    team_rows.append({"team": h, "err": float(hh) - float(hp)})
                if pd.notna(vv):
                    team_rows.append({"team": a2, "err": float(vv) - float(ap)})
    team_df = pd.DataFrame(team_rows)
    team_bias: dict[str, float] = {}
    if not team_df.empty:
        for team, grp in team_df.groupby("team"):
            errs = _winsorize(pd.to_numeric(grp["err"], errors="coerce"))
            n = len(errs)
            if n == 0:
                continue
            mu = float(errs.mean())
            w = float(n) / (float(n) + float(prior_games))
            team_bias[team] = float(w * mu + (1.0 - w) * global_bias)

    calib = {
        "anchor": str(a),
        "window_days": int(window),
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "global": {"game_total_bias": float(global_bias)},
        "team": team_bias,
    }
    out_json = paths.data_processed / f"calibration_totals_{anchor_date}.json"
    try:
        import json
        with open(out_json, "w", encoding="utf-8") as f:
            json.dump(calib, f, indent=2)
        console.print({"output": str(out_json), "global_game_total_bias": float(global_bias), "teams": int(len(team_bias))})
    except Exception as e:
        console.print(f"Failed to write calibration JSON: {e}", style="red")


@cli.command("apply-totals-calibration")
@click.option("--date", "date_str", type=str, required=True, help="Target slate date YYYY-MM-DD to adjust predictions for")
@click.option("--calib-date", type=str, required=False, help="Calibration anchor date; defaults to yesterday")
@click.option("--in", "in_path", type=click.Path(dir_okay=False), required=False, help="Input predictions CSV (defaults to data/processed/predictions_<date>.csv if present, else games_predictions_npu_<date>.csv)")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV; default overwrites input")
def apply_totals_calibration_cmd(date_str: str, calib_date: str | None, in_path: str | None, out_path: str | None):
    """Apply saved totals calibration JSON to a predictions CSV.

    Adjusts:
    - Game total: totals/pred_total columns
    - Halves/quarters totals if present (evenly distributes delta)
    """
    console.rule("Apply Totals Calibration")
    try:
        target = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return
    if calib_date:
        try:
            _ = pd.to_datetime(calib_date).date()
        except Exception:
            console.print("Invalid --calib-date (YYYY-MM-DD)", style="red"); return
    else:
        # Default to yesterday
        from datetime import date as _d, timedelta as _td
        calib_date = (_d.today() - _td(days=1)).isoformat()

    calib_path = paths.data_processed / f"calibration_totals_{calib_date}.json"
    if not calib_path.exists():
        console.print(f"Calibration not found: {calib_path}", style="yellow"); return
    try:
        import json
        with open(calib_path, "r", encoding="utf-8") as f:
            calib = json.load(f)
    except Exception as e:
        console.print(f"Failed to read calibration JSON: {e}", style="red"); return

    # Select input predictions
    in_file: Path
    if in_path:
        in_file = Path(in_path)
    else:
        cands = [
            paths.data_processed / f"predictions_{date_str}.csv",
            paths.data_processed / f"games_predictions_npu_{date_str}.csv",
            paths.root / f"predictions_{date_str}.csv",
        ]
        in_file = next((p for p in cands if p.exists()), None)  # type: ignore
        if in_file is None:
            console.print(f"Predictions not found for {date_str}", style="red"); return
    try:
        df = pd.read_csv(in_file)
    except Exception as e:
        console.print(f"Failed to read predictions CSV: {e}", style="red"); return
    if df is None or df.empty:
        console.print("Predictions CSV is empty; nothing to adjust", style="yellow"); return

    from .teams import to_tricode as _to_tri, normalize_team as _norm
    df = df.copy()
    for c in ("home_team","visitor_team"):
        if c in df.columns:
            df[c] = df[c].apply(_norm)
        else:
            df[c] = pd.NA
    df["home_tri"] = df["home_team"].astype(str).map(_to_tri)
    df["away_tri"] = df["visitor_team"].astype(str).map(_to_tri)

    # Fetch calibration pieces
    g_bias = float(calib.get("global", {}).get("game_total_bias", 0.0) or 0.0)
    team_bias: dict = calib.get("team", {}) or {}

    # Column helpers
    def _pick_col(names: list[str]) -> str | None:
        for n in names:
            if n in df.columns:
                return n
        return None
    game_col = _pick_col(["totals","pred_total","model_total","total"])  # adjust this
    # Period columns presence
    q_cols = [c for c in [f"quarters_q{i}_total" for i in range(1,5)] if c in df.columns]
    h_cols = [c for c in [f"halves_h1_total", "halves_h2_total"] if c in df.columns]

    # Apply per-row
    def _row_adjust(r: pd.Series) -> pd.Series:
        try:
            h = str(r.get("home_tri") or "").upper(); a2 = str(r.get("away_tri") or "").upper()
        except Exception:
            h = ""; a2 = ""
        tb_h = float(team_bias.get(h, 0.0) or 0.0) if h else 0.0
        tb_a = float(team_bias.get(a2, 0.0) or 0.0) if a2 else 0.0
        delta = float(g_bias + 0.5 * (tb_h + tb_a))
        # Game total
        if game_col and pd.notna(r.get(game_col)):
            try:
                r[game_col] = float(r.get(game_col)) + delta
            except Exception:
                pass
        # Distribute to halves/quarters if present
        if q_cols:
            add = delta / 4.0
            for qc in q_cols:
                if pd.notna(r.get(qc)):
                    try:
                        r[qc] = float(r.get(qc)) + add
                    except Exception:
                        pass
        if h_cols:
            addh = delta / 2.0
            for hc in h_cols:
                if pd.notna(r.get(hc)):
                    try:
                        r[hc] = float(r.get(hc)) + addh
                    except Exception:
                        pass
        return r

    df = df.apply(_row_adjust, axis=1)

    # Output
    if out_path:
        out_file = Path(out_path)
    else:
        out_file = in_file
    df.to_csv(out_file, index=False)
    console.print({"input": str(in_file), "output": str(out_file), "calibration": str(calib_path)})


@cli.command("first-basket-recs")
@click.option("--date", "date_str", type=str, required=True, help="Target date YYYY-MM-DD")
@click.option("--topk", type=int, default=3, show_default=True, help="Max picks per game")
@click.option("--min-prob", "min_prob", type=float, default=0.08, show_default=True, help="Minimum probability threshold to include a pick")
@click.option("--cum-target", "cum_target", type=float, default=0.45, show_default=True, help="Stop adding picks when cumulative coverage reaches this threshold")
def first_basket_recs_cmd(date_str: str, topk: int, min_prob: float, cum_target: float):
    """Export first-basket scorer recommendations per game using model probabilities.

    Produces data/processed/first_basket_recs_<date>.csv with columns:
    - date, game_id, team, player_id, player_name, prob_first_basket
    - fair_decimal, fair_american, rank, cum_prob
    """
    console.rule("First Basket: Recommendations")
    probs_path = paths.data_processed / f"first_basket_probs_{date_str}.csv"
    if not probs_path.exists():
        console.print(f"Missing probabilities: {probs_path}", style="red"); return
    try:
        df = pd.read_csv(probs_path)
    except Exception as e:
        console.print(f"Failed to read probabilities: {e}", style="red"); return
    required = {"game_id","team","player_id","player_name","prob_first_basket"}
    if not required.issubset(set(df.columns)):
        console.print(f"Missing required columns in {probs_path.name}: {required - set(df.columns)}", style="red"); return
    # Normalize gid
    df = df.copy()
    # Normalize game_id robustly: coerce to integer (drops any stray decimals), then zero-pad to 10
    try:
        _gid_num = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
        df["gid10"] = _gid_num.astype(str).str.replace("<NA>", "", regex=False).str.zfill(10)
    except Exception:
        df["gid10"] = df["game_id"].astype(str).str.replace(".0$","", regex=True).str.replace("^nan$","", regex=True).str.replace("^None$","", regex=True).str.replace("^\\s+$","", regex=True).str.zfill(10)
    # Per-game selections
    rows: list[dict] = []
    for gid, grp in df.groupby("gid10"):
        sub = grp.sort_values("prob_first_basket", ascending=False).copy()
        cum = 0.0; picked = 0; rank = 0
        for _, r in sub.iterrows():
            p = float(r.get("prob_first_basket", 0.0) or 0.0)
            if p < float(min_prob):
                continue
            rank += 1
            # Fair odds
            fair_dec = float("inf") if p <= 0 else (1.0/float(p))
            if p >= 0.5:
                # negative american
                fair_am = -round((p/(1.0-p))*100)
            else:
                fair_am = round(((1.0-p)/p)*100)
            rows.append({
                "date": date_str,
                "game_id": gid,
                "team": r.get("team"),
                "player_id": r.get("player_id"),
                "player_name": r.get("player_name"),
                "prob_first_basket": p,
                "fair_decimal": fair_dec,
                "fair_american": fair_am,
                "rank": rank,
                "cum_prob": cum + p,
            })
            picked += 1
            cum += p
            if picked >= int(topk) or cum >= float(cum_target):
                break
    out = pd.DataFrame(rows)
    out_path = paths.data_processed / f"first_basket_recs_{date_str}.csv"
    out.to_csv(out_path, index=False)
    console.print({"date": date_str, "games": int(out["game_id"].nunique() if not out.empty else 0), "rows": int(len(out)), "output": str(out_path)})


@cli.command("backtest-pbp-markets")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD (season start)")
@click.option("--end", "end_date", type=str, required=False, help="End date YYYY-MM-DD; default=today")
@click.option("--ensure-preds", is_flag=True, default=True, show_default=True, help="Generate predictions for any missing days in range")
@click.option("--ensure-pbp", is_flag=True, default=False, show_default=True, help="Fetch PBP logs if missing for games in range (finals only)")
def backtest_pbp_markets_cmd(start_date: str, end_date: str | None, ensure_preds: bool, ensure_pbp: bool):
    """Backtest PBP-derived markets (tip, first-basket, early-threes) over a date range.

    Metrics reported:
    - tip: Brier score, log loss (if outcome available), accuracy at 0.5 threshold, n
    - first-basket: top-1 accuracy, top-5 coverage, mean prob(actual), n
    - early-threes: MAE, RMSE for expected_threes_0_3, and Brier for prob_ge_1 vs indicator, n
    """
    console.rule("Backtest PBP-derived Markets")
    try:
        s = pd.to_datetime(start_date).date()
    except Exception:
        console.print("Invalid --start (YYYY-MM-DD)", style="red"); return
    if end_date:
        try:
            e = pd.to_datetime(end_date).date()
        except Exception:
            console.print("Invalid --end (YYYY-MM-DD)", style="red"); return
    else:
        e = _date.today()
    if e < s:
        console.print("--end must be >= --start", style="red"); return

    # Optionally ensure PBP exists (finals only)
    if ensure_pbp:
        try:
            _ = backfill_pbp(str(s), str(e), only_final=True, rate_delay=0.35)
        except Exception as ex:
            console.print({"warning": f"PBP fetch failed/skipped: {ex}"}, style="yellow")

    # Iterate days and evaluate
    dates = list(pd.date_range(s, e, freq="D").date)
    # Accumulators
    tip_probs = []; tip_outcomes = []
    fb_top1 = 0; fb_top5 = 0; fb_total = 0; fb_probs_actual = []
    thr_errs = []; thr_ge1_probs = []; thr_ge1_outcomes = []
    # Lightweight cache for home/away maps
    _gid_to_homeaway: dict[str, tuple[str,str]] = {}

    def _map_from_schedule(ds: str) -> dict[str, tuple[str,str]]:
        # Prefer local processed schedule to avoid network
        import glob
        from pathlib import Path as _Path
        root = paths.data_processed
        out: dict[str, tuple[str,str]] = {}
        try:
            # Try season schedule first
            cand = list(root.glob("schedule_*.csv"))
            if not cand:
                return {}
            p = cand[0]
            # Prefer 2025_26 if present
            for c in cand:
                if "2025_26" in c.name:
                    p = c; break
            df = pd.read_csv(p)
            cols = {c.lower(): c for c in df.columns}
            if not {"game_id","date_utc","home_tricode","away_tricode"}.issubset(set(cols)):
                return {}
            df[cols["date_utc"]] = pd.to_datetime(df[cols["date_utc"]], errors="coerce").dt.strftime("%Y-%m-%d")
            day = df[df[cols["date_utc"]] == ds].copy()
            if day.empty:
                return {}
            for _, r in day.iterrows():
                gid = str(r[cols["game_id"]]).strip()
                home = str(r[cols["home_tricode"]]).upper().strip()
                away = str(r[cols["away_tricode"]]).upper().strip()
                if gid and home and away:
                    out[gid] = (home, away)
            return out
        except Exception:
            return {}

    def _cdn_map_for_date(ds: str) -> dict[str, tuple[str,str]]:
        # Fallback to CDN if schedule missing
        from .pbp_markets import _gid_team_map_for_date as _map
        try:
            m = _map(ds) or {}
            return {str(k): (v[0], v[1]) for k, v in m.items()}
        except Exception:
            return {}

    def _read_csv_safe(path: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()

    def _norm_gid(x) -> str:
        try:
            sx = str(x).strip()
            # Map CDN schedule ids like '2250xxxx' to official '002250xxxx'
            if len(sx) == 8 and sx.startswith("2250"):
                sx = "00" + sx
            return sx.zfill(10) if sx.isdigit() else sx
        except Exception:
            return str(x)

    for d in track(dates, description="Backtesting"):
        ds = str(d)
        # Load or generate predictions
        tip_path = paths.data_processed / f"tip_winner_probs_{ds}.csv"
        fb_path = paths.data_processed / f"first_basket_probs_{ds}.csv"
        thr_path = paths.data_processed / f"early_threes_{ds}.csv"
        if ensure_preds:
            try:
                if not tip_path.exists() or not fb_path.exists() or not thr_path.exists():
                    _ = predict_tip_for_date(ds)
                    _ = predict_first_basket_for_date(ds)
                    _ = predict_early_threes_for_date(ds)
            except Exception:
                pass
        # Read available predictions
        tip = _read_csv_safe(tip_path) if tip_path.exists() else pd.DataFrame()
        fb = _read_csv_safe(fb_path) if fb_path.exists() else pd.DataFrame()
        thr = _read_csv_safe(thr_path) if thr_path.exists() else pd.DataFrame()
        if tip.empty and fb.empty and thr.empty:
            continue

        # Load PBP combined for outcomes (if exists)
        pbp_comb = paths.data_processed / f"pbp_{ds}.csv"
        pbp_map: dict[str, pd.DataFrame] = {}
        if pbp_comb.exists():
            df = pd.read_csv(pbp_comb)
            if "game_id" in df.columns:
                for gid, grp in df.groupby("game_id"):
                    key = _norm_gid(gid)
                    # Only accept well-formed numeric game ids after normalization
                    if key.isdigit() and len(key) == 10:
                        pbp_map[key] = grp.copy()
        # Fallback to per-game files if combined missing or yielded no valid keys
        if not pbp_map:
            dpg = paths.data_processed / "pbp"
            if dpg.exists():
                for f in dpg.glob("pbp_*.csv"):
                    try:
                        gid = f.stem.replace("pbp_", "").strip()
                        key = _norm_gid(gid)
                        if key.isdigit() and len(key) == 10:
                            pbp_map[key] = pd.read_csv(f)
                    except Exception:
                        continue

        # Early threes outcomes and errors
        if not thr.empty:
            # Build actuals for games present in thr
            actuals = {}
            if pbp_map:
                for gid, gdf in pbp_map.items():
                    # count threes in first 180 sec using existing helper
                    cnt = 0
                    desc_cols = _pbp_desc_cols(gdf)
                    c_time = "PCTIMESTRING" if "PCTIMESTRING" in gdf.columns else ("clock" if "clock" in gdf.columns else None)
                    c_per = "PERIOD" if "PERIOD" in gdf.columns else ("period" if "period" in gdf.columns else None)
                    tmp = gdf.copy()
                    if c_per: tmp = tmp[tmp[c_per] == 1]
                    if c_time: tmp = tmp.sort_values(c_time, ascending=False)
                    for _, r in tmp.iterrows():
                        t = r.get("PCTIMESTRING") or r.get("clock") or r.get("time")
                        sec_left = None
                        if isinstance(t, str) and ":" in t:
                            try:
                                m, s2 = t.split(":"); sec_left = int(m)*60+int(s2)
                            except Exception:
                                sec_left = None
                        if sec_left is None: continue
                        elapsed = 12*60 - sec_left
                        if elapsed is None or elapsed > 180:
                            continue
                        text = " ".join([str(r.get(c, "")) for c in desc_cols]).lower()
                        if ("3pt" in text) and ("makes" in text or "made" in text):
                            cnt += 1
                    actuals[str(gid)] = int(cnt)
            # Join predictions with actuals
            for _, row in thr.iterrows():
                gid = _norm_gid(row.get("game_id"))
                if not gid:
                    continue
                yhat = float(row.get("expected_threes_0_3", row.get("threes_0_3_pred", 0.0)) or 0.0)
                a = actuals.get(gid)
                if a is None:
                    continue
                thr_errs.append(float(a - yhat))
                p_ge1 = float(row.get("prob_ge_1", 1.0 - float(np.exp(-max(0.0, yhat)))))
                thr_ge1_probs.append(p_ge1)
                thr_ge1_outcomes.append(1.0 if a >= 1 else 0.0)

        # Optionally read reconciliation for outcomes (fast path)
        rec_path = paths.data_processed / f"pbp_reconcile_{ds}.csv"
        rec_df = None
        if rec_path.exists():
            try:
                rec_df = pd.read_csv(rec_path)
                if not rec_df.empty and "game_id" in rec_df.columns:
                    rec_df["game_id_norm"] = rec_df["game_id"].astype(str).str.strip().apply(_norm_gid)
            except Exception:
                rec_df = None

        # Early threes via reconciliation (preferred if available and none computed yet)
        if (not thr.empty) and (len(thr_errs) == 0) and (rec_df is not None) and {"game_id_norm","early_threes_actual"}.issubset(set(rec_df.columns)):
            try:
                thr2 = thr.copy()
                thr2["game_id_norm"] = thr2["game_id"].apply(_norm_gid)
                merged = thr2.merge(rec_df[["game_id_norm","early_threes_actual"]], on="game_id_norm", how="inner")
                merged = merged.dropna(subset=["early_threes_actual"]).copy()
                for _, rr in merged.iterrows():
                    yhat = float(rr.get("expected_threes_0_3", rr.get("threes_0_3_pred", 0.0)) or 0.0)
                    a = float(rr.get("early_threes_actual"))
                    thr_errs.append(float(a - yhat))
                    p_ge1 = float(rr.get("prob_ge_1", 1.0 - float(np.exp(-max(0.0, yhat)))))
                    thr_ge1_probs.append(p_ge1)
                    thr_ge1_outcomes.append(1.0 if a >= 1.0 else 0.0)
            except Exception:
                pass

        # First basket outcomes
        if not fb.empty and pbp_map:
            # Map actual first scorer per game
            actual_first: dict[str, dict] = {}
            for gid, gdf in pbp_map.items():
                ev = _pbp_first_fg_event(gdf)
                if ev:
                    actual_first[str(gid)] = ev
            for gid, grp in fb.groupby("game_id"):
                gid_n = _norm_gid(gid)
                ev = actual_first.get(gid_n)
                if not ev:
                    continue
                fb_total += 1
                pname_first = (ev.get("player_name") or "").strip().lower()
                # Top-1
                sub = grp.copy().sort_values("prob_first_basket", ascending=False)
                top = sub.iloc[0]
                top_hit = pname_first in str(top.get("player_name","")) .lower()
                if top_hit:
                    fb_top1 += 1
                # Top-5 coverage and prob(actual)
                hit5 = False; prob_actual = None
                for _, r in sub.head(5).iterrows():
                    if pname_first in str(r.get("player_name","")) .lower():
                        hit5 = True; prob_actual = float(r.get("prob_first_basket", np.nan)); break
                if hit5:
                    fb_top5 += 1
                if prob_actual is None:
                    m = sub[sub["player_name"].astype(str).str.lower().str.contains(pname_first)].head(1)
                    if not m.empty:
                        prob_actual = float(m.iloc[0].get("prob_first_basket", np.nan))
                if prob_actual is not None and not np.isnan(prob_actual):
                    fb_probs_actual.append(float(prob_actual))

        # Tip outcomes
        if not tip.empty and (pbp_map or (rec_df is not None)):
            # If reconciliation file exists, prefer its tip outcomes for simplicity
            if rec_df is not None and {"game_id_norm","tip_outcome_home"}.issubset(set(rec_df.columns)):
                tip2 = tip.copy()
                tip2["game_id_norm"] = tip2["game_id"].apply(_norm_gid)
                merged = tip2.merge(rec_df[["game_id_norm","tip_outcome_home"]], on="game_id_norm", how="inner")
                merged = merged.dropna(subset=["tip_outcome_home"]).copy()
                for _, rr in merged.iterrows():
                    p_home = float(rr.get("prob_home_tip", 0.5))
                    outcome = float(rr.get("tip_outcome_home"))
                    tip_probs.append(p_home)
                    tip_outcomes.append(outcome)
                # Skip the PBP path if reconcile provided outcomes for this date
                continue
            # Build home/away map for this date
            if not _gid_to_homeaway:
                # Prefer local schedule mapping; fallback to CDN
                _gid_to_homeaway = _map_from_schedule(ds) or _cdn_map_for_date(ds)
            for _, r in tip.iterrows():
                gid_n = _norm_gid(r.get("game_id"))
                if gid_n not in pbp_map:
                    continue
                pbp = pbp_map[gid_n]
                ev = _pbp_jump_ball_event(pbp)
                if not ev:
                    continue
                winner_text = (ev.get("winner_text") or "").strip().lower()
                if not winner_text:
                    continue
                # Try both zero-padded and raw keys when mapping home/away
                raw_gid = gid_n.lstrip("0") or gid_n
                home, away = (
                    _gid_to_homeaway.get(gid_n)
                    or _gid_to_homeaway.get(raw_gid)
                    or _gid_to_homeaway.get(gid_n.zfill(10))
                    or (None, None)
                )
                if not (home and away):
                    continue
                outcome = None
                try:
                    from .pbp_markets import _load_rosters_latest as _load_rost
                    rost = _load_rost()
                    if not rost.empty:
                        def _has_name(team):
                            tri_col = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in rost.columns else ("teamTricode" if "teamTricode" in rost.columns else None)
                            sub = rost[rost[tri_col].astype(str).str.upper() == str(team).upper()].copy() if tri_col else rost
                            names = sub.get("PLAYER") or sub.get("PLAYER_NAME") or pd.Series(dtype=str)
                            names = names.astype(str).str.lower()
                            # More robust: match last token and any tokenized subset
                            toks = [t for t in re.split(r"\s+", winner_text) if t]
                            if not toks:
                                return False
                            pat = r"|".join([re.escape(t) for t in toks[-2:]])  # last 1-2 tokens
                            return names.str.contains(pat, regex=True).any()
                        if _has_name(home): outcome = 1.0
                        elif _has_name(away): outcome = 0.0
                except Exception:
                    outcome = None

                # Fallback: infer winner team from PBP playerName occurrences if roster match failed
                if outcome is None:
                    try:
                        cols = set(pbp.columns)
                        name_series = None
                        for c in ("playerName","PLAYER1_NAME","player1_name"):
                            if c in cols:
                                name_series = pbp[c].astype(str).str.lower()
                                break
                        team_series = None
                        for c in ("teamTricode","PLAYER1_TEAM_ABBREVIATION","team_abbr"):
                            if c in cols:
                                team_series = pbp[c].astype(str).str.upper()
                                break
                        if name_series is not None and team_series is not None:
                            toks = [t for t in re.split(r"\s+", winner_text) if t]
                            if toks:
                                pat = r"|".join([re.escape(t) for t in toks[-2:]])
                                mask = name_series.str.contains(pat, regex=True)
                                teams = team_series[mask].value_counts()
                                if not teams.empty:
                                    winner_tri = teams.index[0]
                                    if str(winner_tri).upper() == str(home).upper():
                                        outcome = 1.0
                                    elif str(winner_tri).upper() == str(away).upper():
                                        outcome = 0.0
                    except Exception:
                        pass
                p_home = float(r.get("prob_home_tip", 0.5))
                if outcome is not None:
                    tip_probs.append(p_home)
                    tip_outcomes.append(outcome)

    # Aggregate metrics
    def _brier(p, y):
        return float(np.mean([(pi-yi)**2 for pi, yi in zip(p, y)])) if p and y else np.nan
    def _logloss(p, y, eps=1e-9):
        import math
        if not p or not y:
            return np.nan
        return float(np.mean([-(yi*math.log(max(eps, pi)) + (1-yi)*math.log(max(eps, 1-pi))) for pi, yi in zip(p, y)]))
    tip_brier = _brier(tip_probs, tip_outcomes)
    tip_logloss = _logloss(tip_probs, tip_outcomes)
    tip_acc = float(np.mean([int((pi>=0.5)==(yi==1.0)) for pi, yi in zip(tip_probs, tip_outcomes)])) if tip_probs else np.nan
    fb_top1_acc = (fb_top1 / fb_total) if fb_total else np.nan
    fb_top5_cov = (fb_top5 / fb_total) if fb_total else np.nan
    fb_mean_prob_actual = float(np.mean(fb_probs_actual)) if fb_probs_actual else np.nan
    thr_mae = float(np.mean([abs(e) for e in thr_errs])) if thr_errs else np.nan
    thr_rmse = float(np.sqrt(np.mean([e*e for e in thr_errs]))) if thr_errs else np.nan
    thr_brier = _brier(thr_ge1_probs, thr_ge1_outcomes)

    console.print({
        "range": f"{start_date}..{str(e)}",
        "tip": {"n": len(tip_outcomes), "brier": tip_brier, "logloss": tip_logloss, "acc@0.5": tip_acc},
        "first_basket": {"n": fb_total, "top1_acc": fb_top1_acc, "top5_cov": fb_top5_cov, "mean_prob_actual": fb_mean_prob_actual},
        "early_threes": {"n": len(thr_errs), "mae": thr_mae, "rmse": thr_rmse, "brier_ge1": thr_brier},
    })


@cli.command("calibrate-pbp-markets")
@click.option("--anchor", "anchor_date", type=str, required=False, help="Anchor date YYYY-MM-DD (default=yesterday)")
@click.option("--window", "window_days", type=int, default=7, show_default=True, help="Lookback window in days for calibration")
def calibrate_pbp_markets_cmd(anchor_date: str | None, window_days: int):
    """Compute lightweight calibration for PBP markets using recent reconciliation files.

    Outputs to data/processed/pbp_calibration.csv (appends a row):
    - thr_bias: additive bias for expected_threes_0_3 (early threes)
    - tip_logit_bias: intercept shift in log-odds space for tip prob_home
    - fb_temp: temperature scaling for first-basket candidate score normalization
    """
    console.rule("Calibrate PBP-derived Markets")
    import datetime as _dt
    if anchor_date:
        try:
            anchor = pd.to_datetime(anchor_date).date()
        except Exception:
            console.print("Invalid --anchor (YYYY-MM-DD)", style="red"); return
    else:
        anchor = (_dt.date.today() - _dt.timedelta(days=1))
    start = anchor - _dt.timedelta(days=max(0, int(window_days)-1))
    dates = list(pd.date_range(start, anchor, freq="D").date)
    rows = []
    for d in dates:
        p = paths.data_processed / f"pbp_reconcile_{d}.csv"
        if p.exists():
            try:
                df = pd.read_csv(p)
                df["date"] = str(d)
                rows.append(df)
            except Exception:
                continue
    if not rows:
        console.print("No reconciliation files in window; skipping calibration", style="yellow"); return
    all_df = pd.concat(rows, ignore_index=True)
    # Early threes intercept bias = mean(actual - expected)
    thr_err = pd.to_numeric(all_df.get("early_threes_error"), errors="coerce").dropna()
    thr_bias = float(thr_err.mean()) if len(thr_err)>0 else 0.0

    # Tip: fit a logit intercept b such that mean(sigmoid(logit(p)+b)) ~= mean(y)
    tip_b = 0.0
    try:
        p = pd.to_numeric(all_df.get("tip_prob_home"), errors="coerce").dropna().astype(float).tolist()
        y = pd.to_numeric(all_df.get("tip_outcome_home"), errors="coerce").dropna().astype(float).tolist()
        # Align lengths by inner join on rows with both
        if "tip_prob_home" in all_df.columns and "tip_outcome_home" in all_df.columns:
            tmp = all_df.dropna(subset=["tip_prob_home","tip_outcome_home"]).copy()
            pp = pd.to_numeric(tmp["tip_prob_home"], errors="coerce").astype(float).tolist()
            yy = pd.to_numeric(tmp["tip_outcome_home"], errors="coerce").astype(float).tolist()
            if len(pp) >= 5:
                import math
                probs = [min(max(float(t), 1e-6), 1-1e-6) for t in pp]
                ys = [1.0 if float(t)>=0.5 else 0.0 for t in yy]  # ensure 0/1
                ybar = float(np.mean(ys)) if ys else None
                if ybar is not None:
                    logits = [math.log(pv/(1-pv)) for pv in probs]
                    # Bisection to find b s.t. mean(sigmoid(logit+b)) = ybar
                    def _mean_after(b):
                        vals = [1.0/(1.0+math.exp(-(lv + b))) for lv in logits]
                        return float(np.mean(vals))
                    lo, hi = -5.0, 5.0
                    m_lo, m_hi = _mean_after(lo), _mean_after(hi)
                    # If ybar out of achievable range, pick closest bound
                    if ybar <= m_lo:
                        tip_b = lo
                    elif ybar >= m_hi:
                        tip_b = hi
                    else:
                        for _ in range(40):
                            mid = 0.5*(lo+hi)
                            m_mid = _mean_after(mid)
                            if m_mid < ybar:
                                lo = mid
                            else:
                                hi = mid
                        tip_b = 0.5*(lo+hi)
    except Exception:
        tip_b = 0.0

    # First-basket: choose temperature that maximizes mean probability assigned to actual first scorer
    fb_temp = 1.0
    try:
        # Build per-game actual names from reconciliation in window
        recon = all_df.dropna(subset=["game_id","first_basket_actual_name"]).copy()
        if not recon.empty:
            recon["game_id"] = recon["game_id"].astype(str)
            # Load candidate audits per day in window
            candidate_frames = []
            for d in dates:
                f = paths.data_processed / f"first_basket_candidates_{d}.csv"
                if f.exists():
                    try:
                        cdf = pd.read_csv(f)
                        cdf["game_id"] = cdf["game_id"].astype(str)
                        candidate_frames.append(cdf)
                    except Exception:
                        continue
            if candidate_frames:
                all_cand = pd.concat(candidate_frames, ignore_index=True)
                # For stability add epsilon to raw_score and clip >= 0
                all_cand["raw_score"] = pd.to_numeric(all_cand.get("raw_score"), errors="coerce").fillna(0.0).clip(lower=0.0) + 1e-9
                # Grid search tau in [0.6, 1.5]
                taus = [round(x,2) for x in np.linspace(0.6, 1.5, 19)]
                best_tau, best_mean = 1.0, -1.0
                eval_counts = 0
                for tau in taus:
                    probs_actual: list[float] = []
                    # Iterate games that have both recon actual and candidates
                    for gid, sub in all_cand.groupby("game_id"):
                        row = recon[recon["game_id"].astype(str).str.zfill(10) == str(gid).zfill(10)]
                        if row.empty:
                            continue
                        actual = str(row.iloc[0].get("first_basket_actual_name") or "").strip().lower()
                        if not actual:
                            continue
                        sc = sub.copy()
                        sc["player_name_l"] = sc["player_name"].astype(str).str.lower()
                        denom = float(np.power(sc["raw_score"].values, 1.0/float(tau)).sum())
                        if denom <= 0:
                            continue
                        sc["prob_tau"] = np.power(sc["raw_score"].values, 1.0/float(tau)) / denom
                        # find actual row
                        m = sc[sc["player_name_l"].str.contains(actual, na=False)]
                        if not m.empty:
                            probs_actual.append(float(m.iloc[0]["prob_tau"]))
                    if probs_actual:
                        eval_counts += 1
                        avg = float(np.mean(probs_actual))
                        # Maximize avg; tie-breaker by closeness to 1.0
                        if (avg > best_mean) or (abs(avg - best_mean) <= 1e-9 and abs(tau-1.0) < abs(best_tau-1.0)):
                            best_mean = avg; best_tau = float(tau)
                if eval_counts >= 3:  # require at least 3 games evaluated
                    fb_temp = float(best_tau)
    except Exception:
        fb_temp = 1.0
    cal_path = paths.data_processed / "pbp_calibration.csv"
    cal_path.parent.mkdir(parents=True, exist_ok=True)
    new_row = pd.DataFrame([{ "date": str(anchor), "window_days": int(window_days), "thr_bias": thr_bias, "tip_logit_bias": tip_b, "fb_temp": fb_temp }])
    try:
        if cal_path.exists():
            prev = pd.read_csv(cal_path)
            out = pd.concat([prev, new_row], ignore_index=True)
        else:
            out = new_row
        out.to_csv(cal_path, index=False)
    except Exception as e:
        console.print(f"Failed to write calibration: {e}", style="red"); return
    console.print({"anchor": str(anchor), "window_days": int(window_days), "thr_bias": thr_bias, "tip_logit_bias": tip_b, "fb_temp": fb_temp, "output": str(cal_path)})


@cli.command()
@click.option("--input", "input_csv", required=True, type=click.Path(exists=True), help="CSV with columns: date,home_team,visitor_team")
def predict(input_csv: str):
    """Predict using trained models for upcoming games"""
    console.rule("Predict")
    inp = pd.read_csv(input_csv)
    inp["home_team"] = inp["home_team"].apply(normalize_team)
    inp["visitor_team"] = inp["visitor_team"].apply(normalize_team)
    res = _predict_from_matchups(inp)
    out = paths.root / "predictions.csv"
    res.to_csv(out, index=False)
    console.print(f"Saved predictions to {out}")


def _predict_from_matchups(inp: pd.DataFrame) -> pd.DataFrame:
    """Core prediction routine used by predict() and predict-date().

    Expects columns: date, home_team, visitor_team (teams normalized already).
    Returns a DataFrame with predictions for full game and periods.
    """
    # Load features history to bootstrap Elo and recent form
    # Try CSV first (ARM64 compatible), fallback to parquet
    feats_csv = paths.data_processed / "features.csv"
    feats_parquet = paths.data_processed / "features.parquet"
    
    if feats_csv.exists():
        hist = pd.read_csv(feats_csv).sort_values("date")
    elif feats_parquet.exists():
        hist = pd.read_parquet(feats_parquet).sort_values("date")
    else:
        console.print("Features not found. Run build-features first.", style="red")
        raise SystemExit(1)
    elo = Elo()
    # Roll through history to update Elo
    for _, row in track(hist.iterrows(), total=len(hist), description="Updating Elo"):
        if pd.notna(row.get("home_pts")) and pd.notna(row.get("visitor_pts")):
            try:
                elo.update_game(row["home_team"], row["visitor_team"], int(row["home_pts"]), int(row["visitor_pts"]))
            except Exception:
                pass

    # Compute schedule-aware rest for the matchups using history
    rest_df = compute_rest_for_matchups(inp, hist)
    # Build features from Elo + rest
    feat_rows = []
    for _, r in rest_df.iterrows():
        feat_rows.append({
            "elo_diff": elo.get(r["home_team"]) - elo.get(r["visitor_team"]),
            "home_rest_days": r.get("home_rest_days", 2) if pd.notna(r.get("home_rest_days")) else 2,
            "visitor_rest_days": r.get("visitor_rest_days", 2) if pd.notna(r.get("visitor_rest_days")) else 2,
            "home_b2b": r.get("home_b2b", 0) if pd.notna(r.get("home_b2b")) else 0,
            "visitor_b2b": r.get("visitor_b2b", 0) if pd.notna(r.get("visitor_b2b")) else 0,
            "home_team": r["home_team"],
            "visitor_team": r["visitor_team"],
            "date": r.get("date"),
        })
    # Training feature columns
    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
    except FileNotFoundError:
        feat_cols = ["elo_diff", "home_rest_days", "visitor_rest_days", "home_b2b", "visitor_b2b"]

    # Use enhanced feature building for 45 features
    import numpy as np
    from .features_enhanced import build_features_enhanced
    
    console.print("[Building enhanced features (45 features)...]", style="cyan")
    
    # Combine historical games with new matchups for feature generation
    upcoming = pd.DataFrame(feat_rows)
    upcoming["home_pts"] = np.nan  # No scores yet (future games)
    upcoming["visitor_pts"] = np.nan
    
    # Ensure date columns have same type
    hist["date"] = pd.to_datetime(hist["date"])
    upcoming["date"] = pd.to_datetime(upcoming["date"])
    
    # Append upcoming to history
    combined = pd.concat([hist, upcoming], ignore_index=True).sort_values("date")
    
    # Build all 45 enhanced features
    features_df = build_features_enhanced(
        combined, 
        include_advanced_stats=True,
        include_injuries=True
    )
    
    # Extract only the upcoming games (last N rows)
    enriched = features_df.tail(len(feat_rows))
    
    # Get feature matrix
    X = enriched[feat_cols].fillna(0)

    # Load models - Use NPU-accelerated predictions for ALL models (game + periods)
    try:
        # Import NPU game predictor with period support
        from .games_npu import NPUGamePredictor
        
        # Create NPU predictor (win, spread, total, quarters, halves with NPU acceleration)
        console.print("[NPU] Using NPU-accelerated predictions (ONNX + QNN)", style="green")
        npu_predictor = NPUGamePredictor()
        
        # Convert features to numpy array for NPU inference
        X_np = X.values.astype(np.float32)
        
        # Run NPU predictions for all games with period breakdowns
        # Create result dataframe with original matchup info
        res = pd.DataFrame(feat_rows)[["date", "home_team", "visitor_team"]].copy()
        npu_results = npu_predictor.predict_batch(X_np, include_periods=True)
        
        # Extract main game predictions
        res["home_win_prob"] = [r["win_prob"] for r in npu_results]
        res["pred_margin"] = [r["spread_margin"] for r in npu_results]
        res["pred_total"] = [r["totals"] for r in npu_results]
        
        # Include calibration columns if available
        if npu_results and "win_prob_raw" in npu_results[0]:
            res["home_win_prob_raw"] = [r.get("win_prob_raw", r["win_prob"]) for r in npu_results]
            res["home_win_prob_from_spread"] = [r.get("win_prob_from_spread", r["win_prob"]) for r in npu_results]
        
        # Extract period predictions (halves and quarters)
        for i, r in enumerate(npu_results):
            if "halves" in r:
                for half in ("h1", "h2"):
                    if half in r["halves"]:
                        res.loc[i, f"halves_{half}_win"] = r["halves"][half].get("win", 0.5)
                        res.loc[i, f"halves_{half}_margin"] = r["halves"][half].get("margin", 0.0)
                        res.loc[i, f"halves_{half}_total"] = r["halves"][half].get("total", 100.0)
            
            if "quarters" in r:
                for q in ("q1", "q2", "q3", "q4"):
                    if q in r["quarters"]:
                        res.loc[i, f"quarters_{q}_win"] = r["quarters"][q].get("win", 0.5)
                        res.loc[i, f"quarters_{q}_margin"] = r["quarters"][q].get("margin", 0.0)
                        res.loc[i, f"quarters_{q}_total"] = r["quarters"][q].get("total", 50.0)

        # Apply optional period calibration if all required fields present
        try:
            from .period_calibration import calibrate_period_predictions, CalibrationConfig
            cfg = CalibrationConfig()
            if {"home_team", "visitor_team", "pred_total", "pred_margin"}.issubset(set(res.columns)) or {"home_team", "visitor_team", "pred_total", "spread_margin"}.issubset(set(res.columns)):
                # Ensure unified column names expected by calibrator
                if "pred_total" in res.columns and "totals" not in res.columns:
                    res.rename(columns={"pred_total": "totals"}, inplace=True)
                if "pred_margin" in res.columns and "spread_margin" not in res.columns:
                    res.rename(columns={"pred_margin": "spread_margin"}, inplace=True)
                res = calibrate_period_predictions(res, cfg)
        except Exception:
            pass
        
    except (FileNotFoundError, ImportError) as e:
        console.print(f"⚠️  NPU predictor not available: {e}", style="yellow")
        console.print("Falling back to sklearn models (requires sklearn installed)...", style="yellow")
        # Fallback to sklearn models
        win_model = joblib.load(paths.models / "win_prob.joblib")
        spread_model = joblib.load(paths.models / "spread_margin.joblib")
        total_model = joblib.load(paths.models / "totals.joblib")
        
        res = pd.DataFrame(feat_rows)
        res["home_win_prob"] = win_model.predict_proba(X)[:, 1]
        res["pred_margin"] = spread_model.predict(X)
        res["pred_total"] = total_model.predict(X)
        
        # Load period models (halves/quarters) for fallback
        try:
            halves = joblib.load(paths.models / "halves_models.joblib")
            quarters = joblib.load(paths.models / "quarters_models.joblib")
        except Exception:
            console.print("⚠️  Period models not available in fallback", style="yellow")
            halves = {}
            quarters = {}
        
        for half in ("h1", "h2"):
            if half in halves:
                res[f"halves_{half}_win"] = halves[half]["win"].predict_proba(X)[:, 1]
                res[f"halves_{half}_margin"] = halves[half]["margin"].predict(X)
                res[f"halves_{half}_total"] = halves[half]["total"].predict(X)
        for q in ("q1", "q2", "q3", "q4"):
            if q in quarters:
                res[f"quarters_{q}_win"] = quarters[q]["win"].predict_proba(X)[:, 1]
                res[f"quarters_{q}_margin"] = quarters[q]["margin"].predict(X)
                res[f"quarters_{q}_total"] = quarters[q]["total"].predict(X)
    return res

@cli.command("daily-update")
@click.option("--date", "date_str", type=str, required=False, help="Target date YYYY-MM-DD; defaults to today")
@click.option("--season", type=str, default="2025-26", help="Roster season string (e.g., 2025-26)")
@click.option("--odds-api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key for fetching current odds")
@click.option("--git-push/--no-git-push", default=False, show_default=True, help="Commit and push changes to git at end")
@click.option("--props-books", type=str, default=None, help="Comma-separated bookmaker keys to include in edges")
@click.option("--min-prop-edge", type=float, default=0.03, show_default=True)
@click.option("--use-npu/--no-npu", default=True, show_default=True, help="Use NPU acceleration for training and predictions")
@click.option("--reconcile-days", type=int, default=7, show_default=True, help="Days back to reconcile actuals")
@click.option("--retrain-games/--no-retrain-games", default=False, show_default=True, help="Retrain game models (enhanced models recommended). Default is no retrain to preserve calibrated enhanced models.")
def daily_update_cmd(date_str: str | None, season: str, odds_api_key: str | None, git_push: bool, props_books: str | None, min_prop_edge: float, use_npu: bool, reconcile_days: int, retrain_games: bool):
    """Enhanced end-to-end daily updater with NPU acceleration, actuals reconciliation, and comprehensive odds fetching."""
    console.rule("Enhanced Daily Update")
    import datetime as _dt
    import subprocess
    target_date = _dt.date.today() if not date_str else _dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    
    console.print(f"[NPU] Running enhanced daily update for {target_date}")
    console.print(f"[INFO] NPU Acceleration: {'Enabled' if use_npu else 'Disabled'}")
    console.print(f"🔄 Reconciliation window: {reconcile_days} days")

    # 1) Schedule (once per season; idempotent to run daily)
    try:
        console.print("📅 Updating schedule...")
        df_sched = fetch_schedule_2025_26()
        console.print({"schedule_rows": int(len(df_sched))})
    except Exception as e:
        console.print(f"Schedule update failed: {e}", style="yellow")

    # 2) Rosters
    try:
        console.print("👥 Updating rosters...")
        df_rosters = fetch_rosters(season=season)
        console.print({"roster_rows": int(len(df_rosters))})
    except Exception as e:
        console.print(f"Rosters update failed: {e}", style="yellow")

    # 3) Player logs (season(s) up to current)
    try:
        console.print("[INFO] Updating player logs...")
        # Infer seasons around target date; for now, update current season only
        fetch_player_logs([season])
        console.print("Player logs refreshed")
    except Exception as e:
        console.print(f"Player logs update failed: {e}", style="yellow")

    # 4) Reconcile historic game actuals (last N days) → writes recon_games_<date>.csv
    try:
        console.print(f"[SEARCH] Reconciling game actuals for last {reconcile_days} days...")
        for days_back in range(1, reconcile_days + 1):
            past_date = target_date - _dt.timedelta(days=days_back)
            try:
                # Invoke reconcile-date via module to avoid Click context issues
                log = paths.data_processed / f".logs/reconcile_games_{past_date}.log"
                log.parent.mkdir(parents=True, exist_ok=True)
                cmd = [sys.executable or "python", "-m", "nba_betting.cli", "reconcile-date", "--date", str(past_date)]
                with open(log, "w", encoding="utf-8", errors="ignore") as fh:
                    rc = subprocess.run(cmd, cwd=paths.root, stdout=fh, stderr=subprocess.STDOUT, check=False)
                console.print(f"  [OK] Game actuals for {past_date} (rc={rc.returncode})")
            except Exception as e:
                console.print(f"  ⚠️  Game actuals for {past_date}: {e}")
    except Exception as e:
        console.print(f"Game actuals reconciliation failed: {e}", style="yellow")

    # 5) Reconcile historic prop actuals (last N days) and write recon_props_<date>.csv
    try:
        console.print(f"[ACTION] Reconciling prop actuals for last {reconcile_days} days...")
        for days_back in range(1, reconcile_days + 1):
            past_date = target_date - _dt.timedelta(days=days_back)
            try:
                # Call the function directly rather than the CLI command
                from .props_actuals import fetch_prop_actuals_via_nbastatr
                try:
                    df_actuals = fetch_prop_actuals_via_nbastatr(date=str(past_date))
                    if df_actuals is not None and not df_actuals.empty:
                        from .props_actuals import upsert_props_actuals
                        upsert_props_actuals(df_actuals)
                        # Also write a recon CSV for transparency
                        try:
                            outp = paths.data_processed / f"recon_props_{past_date}.csv"
                            outp.parent.mkdir(parents=True, exist_ok=True)
                            df_actuals.to_csv(outp, index=False)
                        except Exception:
                            pass
                        console.print(f"  [OK] Prop actuals for {past_date}")
                except Exception:
                    # Fallback to nba_api
                    from .props_actuals import fetch_prop_actuals_via_nbaapi
                    df_actuals = fetch_prop_actuals_via_nbaapi(str(past_date))
                    if df_actuals is not None and not df_actuals.empty:
                        from .props_actuals import upsert_props_actuals
                        upsert_props_actuals(df_actuals)
                        try:
                            outp = paths.data_processed / f"recon_props_{past_date}.csv"
                            outp.parent.mkdir(parents=True, exist_ok=True)
                            df_actuals.to_csv(outp, index=False)
                        except Exception:
                            pass
                        console.print(f"  [OK] Prop actuals for {past_date}")
            except Exception as e:
                console.print(f"  ⚠️  Prop actuals for {past_date}: {e}")
    except Exception as e:
        console.print(f"Props actuals reconciliation failed: {e}", style="yellow")

    # 6) Rebuild base features (for diagnostics) and optionally retrain game models
    try:
        console.print("🏗️  Rebuilding game features (baseline for diagnostics)...")
        # Build game features uses data/raw/games_nba_api; assume already fetched historically; skip if missing
        feats_raw = paths.data_raw / "games_nba_api.parquet"
        if feats_raw.exists():
            # Build features directly - replicating build_features_cmd logic
            df = pd.read_parquet(feats_raw)
            from .features import build_features
            feats = build_features(df)
            out = paths.data_processed / "features.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            feats.to_parquet(out, index=False)
            console.print(f"Features built and saved to {out}")
            # Optional retrain: default False to preserve enhanced, injury-aware model artifacts
            if retrain_games:
                if use_npu:
                    console.print("[NPU] Retraining game models with latest data...")
                    from .games_npu import train_game_models_npu
                    train_game_models_npu(retrain=True)
                else:
                    console.print("🖥️  Retraining game models (CPU)...")
                    feats_path = paths.data_processed / "features.parquet"
                    df = pd.read_parquet(feats_path)
                    from .train import train_models
                    _ = train_models(df)
                    console.print("Game models retrained (CPU)")
            else:
                console.print("⏭️  Skipping game model retrain (use --retrain-games to enable)")
        else:
            console.print("Raw games not found; skipping full-game retrain.", style="yellow")
    except Exception as e:
        console.print(f"Game model retrain failed: {e}", style="yellow")

    # 7) Rebuild props features and retrain props models
    try:
        console.print("[ACTION] Rebuilding props features...")
        # Build props features directly
        from .props_features import build_props_features
        df = build_props_features()
        console.print(f"Props features built: {len(df)} rows")
        
        if use_npu:
            console.print("[NPU] Training props models with NPU acceleration...")
            from .props_npu import train_props_models_npu
            train_props_models_npu(alpha=1.0)
        else:
            console.print("🖥️  Training props models (CPU)...")
            from .props_train import train_props_models
            train_props_models(alpha=1.0)
            console.print("Props models trained (CPU)")
    except Exception as e:
        console.print(f"Props model retrain failed: {e}", style="yellow")

    # 8) Fetch current game odds from OddsAPI and write to CSV
    try:
        if odds_api_key:
            console.print("💰 Fetching current game odds...")
            cfg = OddsApiConfig(api_key=odds_api_key)
            go = fetch_game_odds_current(cfg, pd.to_datetime(target_date))
            if go is not None and not go.empty:
                out_csv = paths.data_raw / f"odds_nba_current_{target_date}.csv"
                go.to_csv(out_csv, index=False)
                console.print({"game_odds_rows": int(len(go)), "output": str(out_csv)})
                
                # Also write to processed for frontend
                proc_csv = paths.data_processed / f"game_odds_{target_date}.csv"
                go.to_csv(proc_csv, index=False)
        else:
            console.print("No OddsAPI key provided; skipping game odds fetch", style="yellow")
    except Exception as e:
        console.print(f"Game odds fetch failed: {e}", style="yellow")

    # 9) Predict today's slate (games) with enhanced, injury-aware features and write predictions_<date>.csv
    try:
        console.print("🎲 Generating game predictions (enhanced + injuries)...")
        # Always use the enhanced pipeline which already builds injury-aware features
        # and leverages NPU (NPUGamePredictor) internally when available.
        predict_date_cmd(date_str=str(target_date), merge_odds_csv=None, out_path=None)
    except Exception as e:
        console.print(f"Game predictions failed: {e}", style="yellow")

    # 10) Fetch current prop odds from OddsAPI and write to CSV
    try:
        if odds_api_key:
            console.print("[ACTION] Fetching current prop odds...")
            cfg = OddsApiConfig(api_key=odds_api_key)
            # Fetch player props for today
            from .odds_api import fetch_player_props_current
            props_odds = fetch_player_props_current(cfg, date=target_date, markets=None, verbose=True)
            if props_odds is not None and not props_odds.empty:
                out_csv = paths.data_raw / f"odds_nba_player_props_{target_date}.csv"
                props_odds.to_csv(out_csv, index=False)
                console.print({"prop_odds_rows": int(len(props_odds)), "output": str(out_csv)})
        else:
            console.print("No OddsAPI key provided; skipping prop odds fetch", style="yellow")
    except Exception as e:
        console.print(f"Prop odds fetch failed: {e}", style="yellow")

    # 11) Props predictions and edges for target date
    try:
        console.print("[ACTION] Generating prop predictions and edges...")
        # Predict props (builds as-of features internally)
        if use_npu:
            console.print("[NPU] Using NPU acceleration for prop predictions...")
            from .props_npu import predict_props_npu
            from .props_features import build_features_for_date
            try:
                feats = build_features_for_date(str(target_date))
                if not feats.empty:
                    preds = predict_props_npu(feats)
                    out_path = paths.data_processed / f"props_predictions_npu_{target_date}.csv"
                    preds.to_csv(out_path, index=False)
                    console.print(f"[OK] NPU prop predictions saved to {out_path}")
            except Exception as e:
                console.print(f"NPU prop predictions failed, falling back to CPU: {e}", style="yellow")
                predict_props_cmd(date_str=str(target_date), out_path=None, slate_only=True, calibrate=True, calib_window=7)
        else:
            predict_props_cmd(date_str=str(target_date), out_path=None, slate_only=True, calibrate=True, calib_window=7)
        
        # Fetch current props odds and compute edges
        if odds_api_key:
            try:
                sigma = calibrate_sigma_for_date(str(target_date), window_days=30, min_rows=200, defaults=SigmaConfig())
                edges = compute_props_edges(
                    date=str(target_date),
                    sigma=sigma,
                    use_saved=False,
                    mode="current",
                    api_key=odds_api_key,
                    source="auto",
                    predictions_path=None,
                    from_file_only=False
                )
                if edges is not None and not edges.empty:
                    # Apply edge and EV filters
                    edges = edges[(edges["edge"] >= min_prop_edge) & (edges["ev"] >= 0.0)].copy()
                    edges.sort_values(["stat", "edge"], ascending=[True, False], inplace=True)
                    if len(edges) > 200:
                        edges = edges.groupby("stat", group_keys=False).head(max(1, 200 // max(1, edges["stat"].nunique())))
                    out = paths.data_processed / f"props_edges_{target_date}.csv"
                    edges.to_csv(out, index=False)
                    console.print(f"[OK] Props edges saved to {out}")
            except Exception as e:
                console.print(f"Props edges computation failed: {e}", style="yellow")
        else:
            console.print("No OddsAPI key provided; skipping props edges", style="yellow")
    except Exception as e:
        console.print(f"Props edges failed: {e}", style="yellow")

    # 12) Generate frontend-ready recommendation files
    try:
        console.print("📱 Generating frontend recommendations...")
        # Generate game recommendations - simplified version to avoid click context
        try:
            pred = paths.data_processed / f"predictions_{target_date}.csv"
            if pred.exists():
                df = pd.read_csv(pred)
                d = target_date
                recs = []
                def _num(x):
                    try:
                        return float(x)
                    except Exception:
                        return None
                for _, r in df.iterrows():
                    try:
                        home = r.get("home_team"); away = r.get("visitor_team")
                        # ATS
                        pm = _num(r.get("pred_margin")); hs = _num(r.get("home_spread"))
                        if pm is not None and hs is not None:
                            edge_spread = pm - (-hs)
                            if abs(edge_spread) >= 1.0:
                                recs.append({"market":"ATS","side": home if edge_spread>0 else away, "home": home, "away": away, "edge": float(edge_spread), "date": str(d)})
                        # TOTAL
                        pt = _num(r.get("pred_total")); tot = _num(r.get("total"))
                        if pt is not None and tot is not None:
                            edge_total = pt - tot
                            if abs(edge_total) >= 1.5:
                                recs.append({"market":"TOTAL","side": ("Over" if edge_total>0 else "Under"), "home": home, "away": away, "edge": float(edge_total), "date": str(d)})
                    except Exception:
                        continue
                out = paths.data_processed / f"recommendations_{target_date}.csv"
                pd.DataFrame(recs).to_csv(out, index=False)
                console.print(f"[OK] Game recommendations saved to {out}")
        except Exception as e:
            console.print(f"Game recommendations failed: {e}", style="yellow")
        
        # Generate prop recommendations - simplified version
        try:
            edges_csv = paths.data_processed / f"props_edges_{target_date}.csv"
            if edges_csv.exists():
                edges = pd.read_csv(edges_csv)
                if not edges.empty:
                    # Simple filtering for top recommendations
                    top_recs = edges[edges["ev"] >= 0.05].copy()
                    top_recs = top_recs.sort_values("ev", ascending=False).head(50)
                    out = paths.data_processed / f"props_recommendations_{target_date}.csv"
                    top_recs.to_csv(out, index=False)
                    console.print(f"[OK] Prop recommendations saved to {out}")
        except Exception as e:
            console.print(f"Prop recommendations failed: {e}", style="yellow")
        
        console.print("[OK] Frontend recommendation files generated")
    except Exception as e:
        console.print(f"Frontend recommendations failed: {e}", style="yellow")

    # 13) Git commit and push changes (optional)
    if git_push:
        try:
            console.print("📤 Committing and pushing to git...")
            subprocess.run(["git", "add", "-A"], check=False, cwd=paths.root)
            msg = f"daily update {target_date} {'(NPU)' if use_npu else '(CPU)'}"
            result = subprocess.run(["git", "commit", "-m", msg], check=False, cwd=paths.root, capture_output=True, text=True)
            if result.returncode == 0:
                push_result = subprocess.run(["git", "push"], check=False, cwd=paths.root, capture_output=True, text=True)
                if push_result.returncode == 0:
                    console.print("[OK] Git push complete")
                else:
                    console.print(f"Git push failed: {push_result.stderr}", style="yellow")
            else:
                console.print("No changes to commit", style="blue")
        except Exception as e:
            console.print(f"Git operations failed: {e}", style="yellow")
    
    # 14) Summary report
    console.print("\n🎉 Daily update complete!")
    console.print(f"📅 Date: {target_date}")
    console.print(f"[NPU] NPU: {'Enabled' if use_npu else 'Disabled'}")
    console.print(f"💰 Odds API: {'Used' if odds_api_key else 'Skipped'}")
    console.print(f"📤 Git Push: {'Yes' if git_push else 'No'}")
    console.print("\n📁 Check these files for today's data:")
    console.print(f"  - predictions_{target_date}.csv")
    console.print(f"  - props_predictions{'_npu' if use_npu else ''}_{target_date}.csv")
    console.print(f"  - props_edges_{target_date}.csv") 
    console.print(f"  - recommendations_{target_date}.csv")
    console.print(f"  - props_recommendations_{target_date}.csv")


@cli.command("sync-frontend")
@click.option("--date", "date_str", type=str, required=False, help="Target date YYYY-MM-DD; defaults to today")
@click.option("--cleanup-days", type=int, default=30, show_default=True, help="Clean up files older than N days")
def sync_frontend_cmd(date_str: str | None, cleanup_days: int):
    """Validate and sync all data files for frontend consumption."""
    console.rule("Frontend Data Sync")
    import datetime as _dt
    target_date = _dt.date.today() if not date_str else _dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    
    try:
        from .frontend_sync import validate_and_sync_frontend_data, cleanup_old_files, get_frontend_data_status
        
        console.print(f"🔄 Syncing frontend data for {target_date}...")
        results = validate_and_sync_frontend_data(str(target_date))
        
        console.print("\n[INFO] Validation Results:")
        for data_type, validation in results["validation_results"].items():
            console.print(f"  {data_type}: {validation['rows']} rows")
        
        console.print(f"\n📁 Files Created: {len(results['files_created'])}")
        for file_path in results["files_created"]:
            console.print(f"  [OK] {file_path}")
        
        if results["errors"]:
            console.print(f"\n⚠️  Errors: {len(results['errors'])}")
            for error in results["errors"]:
                console.print(f"  [ERROR] {error}")
        
        # Cleanup old files
        if cleanup_days > 0:
            console.print(f"\n🧹 Cleaning up files older than {cleanup_days} days...")
            cleanup_stats = cleanup_old_files(keep_days=cleanup_days)
            console.print(f"  Removed {cleanup_stats['files_removed']} files")
            console.print(f"  Freed {cleanup_stats['bytes_freed']:,} bytes")
        
        # Show frontend status
        console.print(f"\n📱 Frontend Data Status:")
        status = get_frontend_data_status()
        for file_name, file_info in status["latest_files"].items():
            if file_info["exists"]:
                console.print(f"  [OK] {file_name}")
            else:
                console.print(f"  [ERROR] {file_name} (missing)")
        
        console.print(f"\n🎉 Frontend sync complete!")
        
    except Exception as e:
        console.print(f"Frontend sync failed: {e}", style="red")


@cli.command("frontend-status")
def frontend_status_cmd():
    """Check status of frontend data files."""
    console.rule("Frontend Data Status")
    
    try:
        from .frontend_sync import get_frontend_data_status
        
        status = get_frontend_data_status()
        
        console.print("📁 Latest Files:")
        for file_name, file_info in status["latest_files"].items():
            if file_info["exists"]:
                size_mb = file_info["size_bytes"] / (1024 * 1024)
                console.print(f"  [OK] {file_name} ({size_mb:.2f} MB)")
                console.print(f"     Last modified: {file_info['last_modified']}")
            else:
                console.print(f"  [ERROR] {file_name} (missing)")
        
        console.print(f"\n📅 Recent Files (last 7 days):")
        for date_str, files in status["dated_files"].items():
            available_count = sum(1 for exists in files.values() if exists)
            total_count = len(files)
            console.print(f"  {date_str}: {available_count}/{total_count} files available")
        
        if status["missing_files"]:
            console.print(f"\n⚠️  Missing Files:")
            for file_name in status["missing_files"]:
                console.print(f"  [ERROR] {file_name}")
        
    except Exception as e:
        console.print(f"Status check failed: {e}", style="red")


@cli.command("predict-date")
@click.option("--date", "date_str", type=str, required=False, help="Slate date YYYY-MM-DD; defaults to today")
@click.option("--merge-odds", "merge_odds_csv", type=click.Path(exists=True), required=False, help="Optional CSV of odds to merge. Columns supported: date,home_team,visitor_team,home_ml,away_ml,home_spread,total as well as period markets: h1_spread,h1_total,h2_spread,h2_total,q1_spread,q1_total,q2_spread,q2_total,q3_spread,q3_total,q4_spread,q4_total")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path; default predictions_YYYY-MM-DD.csv in repo root")
def predict_date_cmd(date_str: str | None, merge_odds_csv: str | None, out_path: str | None):
    """Predict today's (or specified date's) slate using ScoreboardV2, with optional odds merge."""
    console.rule("Predict (Date)")
    # Harden headers to reduce blocks
    try:
        nba_http.STATS_HEADERS.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://www.nba.com',
            'Referer': 'https://www.nba.com/stats/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
            'Connection': 'keep-alive',
        })
    except Exception:
        pass
    import datetime as _dt
    if not date_str:
        date_str = _dt.date.today().strftime("%Y-%m-%d")

    # Helper: build slate from historical features if NBA API fails
    def _build_slate_from_history(date_str_local: str) -> pd.DataFrame | None:
        feats_path = paths.data_processed / "features.parquet"
        if not feats_path.exists():
            return None
        try:
            dfh = pd.read_parquet(feats_path)
        except ImportError:
            # pyarrow not available (e.g., on ARM64 Windows)
            import sys
            print("Warning: pyarrow not available, skipping parquet fallback", file=sys.stderr)
            return None
        dfh = dfh.copy()
        dfh["date"] = pd.to_datetime(dfh["date"]).dt.date
        try:
            target_d = pd.to_datetime(date_str_local).date()
        except Exception:
            return None
        part = dfh[dfh["date"] == target_d][["date","home_team","visitor_team"]].dropna()
        part["home_team"] = part["home_team"].apply(normalize_team)
        part["visitor_team"] = part["visitor_team"].apply(normalize_team)
        part = part.drop_duplicates()
        return part if not part.empty else None

    slate = None
    # Fallback: build slate from processed schedule JSON (preseason/regular) when API/history fail
    def _build_slate_from_schedule(date_str_local: str) -> pd.DataFrame | None:
        try:
            sched_path = paths.data_processed / "schedule_2025_26.json"
            if not sched_path.exists():
                return None
            sdf = pd.read_json(sched_path)
            # Normalize date to YYYY-MM-DD
            if "date_utc" in sdf.columns:
                sdf["date_utc"] = pd.to_datetime(sdf["date_utc"], errors="coerce").dt.date
            target_d = pd.to_datetime(date_str_local).date()
            day = sdf[sdf["date_utc"] == target_d].copy()
            if day.empty:
                return None
            # Build full team names from City + Name to feed normalize_team
            def full_name(city, name):
                city_s = str(city or "").strip()
                name_s = str(name or "").strip()
                return f"{city_s} {name_s}".strip()
            rows = []
            for _, g in day.iterrows():
                home_full = full_name(g.get("home_city"), g.get("home_name"))
                away_full = full_name(g.get("away_city"), g.get("away_name"))
                home = normalize_team(home_full)
                away = normalize_team(away_full)
                rows.append({
                    "date": target_d,
                    "home_team": home,
                    "visitor_team": away,
                })
            df = pd.DataFrame(rows)
            return df if not df.empty else None
        except Exception:
            return None
    try:
        # Fetch slate from ScoreboardV2
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=30)
        nd = sb.get_normalized_dict()
        gh = pd.DataFrame(nd.get("GameHeader", []))
        ls = pd.DataFrame(nd.get("LineScore", []))
        if gh.empty or ls.empty:
            raise RuntimeError("Scoreboard returned empty tables")
        gh_cols = {c.upper(): c for c in gh.columns}
        ls_cols = {c.upper(): c for c in ls.columns}
        required = ["GAME_ID", "HOME_TEAM_ID", "VISITOR_TEAM_ID", "GAME_DATE_EST"]
        if "GAME_DATE_EST" not in gh_cols and "GAME_DATE" in gh_cols:
            gh_cols["GAME_DATE_EST"] = gh_cols["GAME_DATE"]
        if not all(k in gh_cols for k in required) or not {"GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION"}.issubset(ls_cols.keys()):
            raise RuntimeError("Scoreboard missing required columns")

        # Map TEAM_ID -> ABBR for this date
        team_abbr_map = {}
        for _, r in ls.iterrows():
            try:
                team_abbr_map[int(r[ls_cols["TEAM_ID"]])] = str(r[ls_cols["TEAM_ABBREVIATION"]])
            except Exception:
                continue
        # Build matchups
        team_list = static_teams.get_teams(); abbr_to_full = {t['abbreviation']: t['full_name'] for t in team_list}
        rows = []
        for _, g in gh.iterrows():
            try:
                home_id = int(g[gh_cols["HOME_TEAM_ID"]]); vis_id = int(g[gh_cols["VISITOR_TEAM_ID"]])
                habbr = team_abbr_map.get(home_id); vabbr = team_abbr_map.get(vis_id)
                if not habbr or not vabbr:
                    continue
                home = normalize_team(abbr_to_full.get(habbr, habbr))
                away = normalize_team(abbr_to_full.get(vabbr, vabbr))
                rows.append({
                    "date": pd.to_datetime(g[gh_cols["GAME_DATE_EST"]]).date(),
                    "home_team": home,
                    "visitor_team": away,
                })
            except Exception:
                continue
        if rows:
            slate = pd.DataFrame(rows)
        else:
            slate = None
    except Exception as e:
        console.print(f"Scoreboard fetch failed ({e}); trying fallbacks for {date_str}.", style="yellow")
        slate = _build_slate_from_history(date_str)
        if slate is None or slate.empty:
            slate = _build_slate_from_schedule(date_str)

    if slate is None or slate.empty:
        console.print(f"No games found on {date_str} (API down and no history/schedule fallback).", style="yellow"); return

    # Predict
    res = _predict_from_matchups(slate)

    # Auto-odds: If a CSV path was provided, merge it; otherwise try OddsAPI then Bovada, save standardized game_odds_{date}.csv, and merge
    def _merge_odds_df(odds_df: pd.DataFrame):
        nonlocal res
        if odds_df is None or odds_df.empty:
            return
        o = odds_df.copy()
        # Normalize 'date' to python date on both frames to avoid dtype mismatches
        try:
            if 'date' in o.columns:
                o['date'] = pd.to_datetime(o['date'], errors='coerce').dt.date
        except Exception:
            pass
        try:
            if 'date' in res.columns:
                res['date'] = pd.to_datetime(res['date'], errors='coerce').dt.date
        except Exception:
            pass
        # Normalize names
        if 'home_team' in o.columns:
            o['home_team'] = o['home_team'].apply(normalize_team)
        if 'visitor_team' in o.columns:
            o['visitor_team'] = o['visitor_team'].apply(normalize_team)
        res = res.merge(o, on=['date','home_team','visitor_team'], how='left', suffixes=('', '_odds'))
        # Compute implied probs and edges
        def implied_prob_american(odds):
            try:
                o = float(odds)
            except Exception:
                return pd.NA
            if pd.isna(o):
                return pd.NA
            if o < 0:
                return (-o) / ((-o) + 100.0)
            return 100.0 / (o + 100.0)
        if 'home_ml' in res.columns:
            res['home_implied_prob'] = res['home_ml'].apply(implied_prob_american)
            res['edge_win'] = res['home_win_prob'] - res['home_implied_prob']
        if 'home_spread' in res.columns:
            res['market_home_margin'] = -res['home_spread']
            # Use whichever prediction column exists: prefer 'pred_margin', else 'spread_margin'
            pred_col = 'pred_margin' if 'pred_margin' in res.columns else ('spread_margin' if 'spread_margin' in res.columns else None)
            if pred_col is not None:
                res['edge_spread'] = res[pred_col] - res['market_home_margin']
        if 'total' in res.columns:
            # Use available total prediction column
            total_pred_col = 'pred_total' if 'pred_total' in res.columns else ('totals' if 'totals' in res.columns else None)
            if total_pred_col is not None:
                res['edge_total'] = res[total_pred_col] - res['total']
        # Period edges if columns present
        for half in ("h1","h2"):
            sp_col = f"{half}_spread"; tot_col = f"{half}_total"
            if sp_col in res.columns:
                # Prefer calibrated pred margin if available, else use generic naming if present
                half_pred_col = f"{half}_pred_margin" if f"{half}_pred_margin" in res.columns else None
                if half_pred_col is not None:
                    res[f"edge_{half}_spread"] = res[half_pred_col] - (-res[sp_col])
            if tot_col in res.columns and f"{half}_pred_total" in res.columns:
                res[f"edge_{half}_total"] = res[f"{half}_pred_total"] - res[tot_col]
        for q in ("q1","q2","q3","q4"):
            sp_col = f"{q}_spread"; tot_col = f"{q}_total"
            if sp_col in res.columns:
                q_pred_col = f"{q}_pred_margin" if f"{q}_pred_margin" in res.columns else None
                if q_pred_col is not None:
                    res[f"edge_{q}_spread"] = res[q_pred_col] - (-res[sp_col])
            if tot_col in res.columns and f"{q}_pred_total" in res.columns:
                res[f"edge_{q}_total"] = res[f"{q}_pred_total"] - res[tot_col]

    # 1) If merge-odds CSV provided, use it
    if merge_odds_csv:
        try:
            odds_csv_df = pd.read_csv(merge_odds_csv)
            _merge_odds_df(odds_csv_df)
        except Exception as e:
            console.print(f"Failed to merge odds from CSV: {e}", style="yellow")
    else:
        # 2) Try OddsAPI current; if empty or no key, fall back to Bovada
        import datetime as _dt
        target_date = _dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        game_odds_out = paths.data_processed / f"game_odds_{date_str}.csv"
        odds_out_df = pd.DataFrame()
        # Try OddsAPI first
        api_key = os.environ.get("ODDS_API_KEY") or _load_dotenv_key("ODDS_API_KEY")
        if api_key:
            try:
                cfg = OddsApiConfig(api_key=api_key)
                long_df = fetch_game_odds_current(cfg, pd.to_datetime(target_date))
                if long_df is not None and not long_df.empty:
                    wide = consensus_lines_at_close(long_df)
                    if wide is not None and not wide.empty:
                        # Map to standardized per-game odds row
                        tmp = wide.copy()
                        tmp["date"] = pd.to_datetime(tmp["commence_time"]).dt.date
                        tmp.rename(columns={"away_team": "visitor_team"}, inplace=True)
                        tmp["home_spread"] = tmp.get("spread_point")
                        tmp["away_spread"] = tmp["home_spread"].apply(lambda x: -x if pd.notna(x) else pd.NA)
                        tmp["total"] = tmp.get("total_point")
                        cols = [
                            "date","commence_time","home_team","visitor_team",
                            "home_ml","away_ml","home_spread","away_spread","total"
                        ]
                        keep = [c for c in cols if c in tmp.columns]
                        odds_out_df = tmp[keep].copy()
                        odds_out_df["bookmaker"] = "oddsapi_consensus"
            except Exception as e:
                console.print(f"OddsAPI current odds failed: {e}", style="yellow")
        # Fallback to Bovada if still empty
        if odds_out_df is None or odds_out_df.empty:
            try:
                odds_out_df = fetch_bovada_odds_current(pd.to_datetime(target_date))
            except Exception as e:
                console.print(f"Bovada odds fetch failed: {e}", style="yellow")
        # Save standardized odds and merge
        if odds_out_df is not None and not odds_out_df.empty:
            try:
                game_odds_out.parent.mkdir(parents=True, exist_ok=True)
                odds_out_df.to_csv(game_odds_out, index=False)
                console.print({"game_odds_rows": int(len(odds_out_df)), "output": str(game_odds_out)})
            except Exception as e:
                console.print(f"Failed to save game odds CSV: {e}", style="yellow")
            _merge_odds_df(odds_out_df)

    # Save
    if not out_path:
        out_path = str(paths.data_processed / f"predictions_{date_str}.csv")
    res.to_csv(out_path, index=False)
    console.print(f"Saved predictions to {out_path}")


@cli.command()
@click.option("--holdout-season", type=int, required=False, help="Evaluate on a single season (e.g., 2024)")
def evaluate(holdout_season: int | None):
    """Evaluate saved models on a holdout season or the latest season."""
    console.rule("Evaluate")
    feats_path = paths.data_processed / "features.parquet"
    if not feats_path.exists():
        console.print("Features not found. Run build-features first.", style="red")
        return
    df = pd.read_parquet(feats_path)
    if holdout_season is None:
        holdout_season = int(df["season"].max())
    test = df[df["season"] == holdout_season].dropna(subset=["target_home_win", "target_margin", "target_total"])  
    if test.empty:
        console.print(f"No data for season {holdout_season}", style="yellow")
        return
    # Use the same features used during training
    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
    except FileNotFoundError:
        feat_cols = ["elo_diff", "home_rest_days", "visitor_rest_days", "home_b2b", "visitor_b2b"]
    X = test[feat_cols].fillna(0)
    y_win = test["target_home_win"].astype(int)
    y_margin = test["target_margin"].astype(float)
    y_total = test["target_total"].astype(float)

    import joblib
    from sklearn.metrics import log_loss, mean_squared_error  # type: ignore
    import numpy as np

    try:
        win_model = joblib.load(paths.models / "win_prob.joblib")
        spread_model = joblib.load(paths.models / "spread_margin.joblib")
        total_model = joblib.load(paths.models / "totals.joblib")
    except FileNotFoundError:
        console.print("Models not found. Run train first.", style="red")
        return

    p = win_model.predict_proba(X)[:, 1]
    ll = log_loss(y_win, p, labels=[0, 1])
    rmse_m = float(np.sqrt(mean_squared_error(y_margin, spread_model.predict(X))))
    rmse_t = float(np.sqrt(mean_squared_error(y_total, total_model.predict(X))))
    console.print({
        "season": holdout_season,
        "win_logloss": float(ll),
        "spread_rmse": float(rmse_m),
        "total_rmse": float(rmse_t),
    })


@cli.command()
@click.option("--start", type=int, required=False, help="Start season (e.g., 2018)")
@click.option("--end", type=int, required=False, help="End season inclusive (e.g., 2024)")
@click.option("--last-n", type=int, required=False, help="Evaluate last N seasons if start/end not provided")
def backtest(start: int | None, end: int | None, last_n: int | None):
    """Evaluate models across multiple seasons and summarize metrics."""
    console.rule("Backtest")
    feats_path = paths.data_processed / "features.parquet"
    if not feats_path.exists():
        console.print("Features not found. Run build-features first.", style="red")
        return
    df = pd.read_parquet(feats_path)
    all_seasons = sorted(df["season"].dropna().unique().tolist())
    if start is None and end is None:
        if last_n is None:
            last_n = 5
        seasons = all_seasons[-last_n:]
    else:
        if start is None:
            start = all_seasons[0]
        if end is None:
            end = all_seasons[-1]
        seasons = [s for s in all_seasons if start <= s <= end]
    if not seasons:
        console.print("No seasons selected for backtest.", style="yellow")
        return

    # Load models once
    try:
        win_model = joblib.load(paths.models / "win_prob.joblib")
        spread_model = joblib.load(paths.models / "spread_margin.joblib")
        total_model = joblib.load(paths.models / "totals.joblib")
    except FileNotFoundError:
        console.print("Models not found. Run train first.", style="red")
        return

    from sklearn.metrics import log_loss, mean_squared_error  # type: ignore
    import numpy as np

    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
    except FileNotFoundError:
        feat_cols = ["elo_diff", "home_rest_days", "visitor_rest_days", "home_b2b", "visitor_b2b"]
    rows = []
    for s in seasons:
        part = df[df["season"] == s].dropna(subset=["target_home_win", "target_margin", "target_total"]).copy()
        if part.empty:
            continue
        X = part[feat_cols].fillna(0)
        y_win = part["target_home_win"].astype(int)
        y_margin = part["target_margin"].astype(float)
        y_total = part["target_total"].astype(float)
        p = win_model.predict_proba(X)[:, 1]
        ll = log_loss(y_win, p, labels=[0, 1])
        rmse_m = float(np.sqrt(mean_squared_error(y_margin, spread_model.predict(X))))
        rmse_t = float(np.sqrt(mean_squared_error(y_total, total_model.predict(X))))
        rows.append({"season": int(s), "win_logloss": float(ll), "spread_rmse": rmse_m, "total_rmse": rmse_t, "n_games": int(len(part))})

    if not rows:
        console.print("No data available for the requested seasons.", style="yellow")
        return

    res_df = pd.DataFrame(rows).sort_values("season")
    console.print(res_df)
    agg = {
        "seasons": f"{res_df['season'].min()}-{res_df['season'].max()}",
        "avg_win_logloss": float(res_df["win_logloss"].mean()),
        "avg_spread_rmse": float(res_df["spread_rmse"].mean()),
        "avg_total_rmse": float(res_df["total_rmse"].mean()),
        "total_games": int(res_df["n_games"].sum()),
    }
    console.print(agg)

    # Save CSV
    out = paths.data_processed / "backtest_metrics.csv"
    res_df.to_csv(out, index=False)
    console.print(f"Saved per-season metrics to {out}")


@cli.command("backtest-periods")
@click.option("--start", type=int, required=False, help="Start season (e.g., 2018)")
@click.option("--end", type=int, required=False, help="End season inclusive (e.g., 2024)")
@click.option("--last-n", type=int, required=False, help="Evaluate last N seasons if start/end not provided")
def backtest_periods(start: int | None, end: int | None, last_n: int | None):
    """Backtest halves and quarters models across seasons."""
    console.rule("Backtest (Periods)")
    feats_path = paths.data_processed / "features.parquet"
    if not feats_path.exists():
        console.print("Features not found. Run build-features first.", style="red")
        return
    df = pd.read_parquet(feats_path)
    all_seasons = sorted(df["season"].dropna().unique().tolist())
    if start is None and end is None:
        if last_n is None:
            last_n = 5
        seasons = all_seasons[-last_n:]
    else:
        if start is None:
            start = all_seasons[0]
        if end is None:
            end = all_seasons[-1]
        seasons = [s for s in all_seasons if start <= s <= end]
    if not seasons:
        console.print("No seasons selected for backtest.", style="yellow")
        return

    # Load models
    try:
        halves = joblib.load(paths.models / "halves_models.joblib")
        quarters = joblib.load(paths.models / "quarters_models.joblib")
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
    except FileNotFoundError:
        console.print("Models not found. Run train first.", style="red")
        return

    from sklearn.metrics import log_loss, mean_squared_error  # type: ignore
    import numpy as np

    results = []
    # Evaluate halves
    for half in ("h1", "h2"):
        if half not in halves:
            continue
        rows = []
        for s in seasons:
            part = df[df["season"] == s].dropna(subset=[f"target_{half}_home_win", f"target_{half}_margin", f"target_{half}_total"]).copy()
            if part.empty:
                continue
            X = part[feat_cols].fillna(0)
            y_win = part[f"target_{half}_home_win"].astype(int)
            y_margin = part[f"target_{half}_margin"].astype(float)
            y_total = part[f"target_{half}_total"].astype(float)
            p = halves[half]["win"].predict_proba(X)[:, 1]
            ll = log_loss(y_win, p, labels=[0, 1])
            rmse_m = float(np.sqrt(mean_squared_error(y_margin, halves[half]["margin"].predict(X))))
            rmse_t = float(np.sqrt(mean_squared_error(y_total, halves[half]["total"].predict(X))))
            rows.append({"season": int(s), "period": half, "win_logloss": float(ll), "spread_rmse": rmse_m, "total_rmse": rmse_t, "n": int(len(part))})
        if rows:
            results.extend(rows)

    # Evaluate quarters
    for q in ("q1", "q2", "q3", "q4"):
        if q not in quarters:
            continue
        rows = []
        for s in seasons:
            part = df[df["season"] == s].dropna(subset=[f"target_{q}_home_win", f"target_{q}_margin", f"target_{q}_total"]).copy()
            if part.empty:
                continue
            X = part[feat_cols].fillna(0)
            y_win = part[f"target_{q}_home_win"].astype(int)
            y_margin = part[f"target_{q}_margin"].astype(float)
            y_total = part[f"target_{q}_total"].astype(float)
            p = quarters[q]["win"].predict_proba(X)[:, 1]
            ll = log_loss(y_win, p, labels=[0, 1])
            rmse_m = float(np.sqrt(mean_squared_error(y_margin, quarters[q]["margin"].predict(X))))
            rmse_t = float(np.sqrt(mean_squared_error(y_total, quarters[q]["total"].predict(X))))
            rows.append({"season": int(s), "period": q, "win_logloss": float(ll), "spread_rmse": rmse_m, "total_rmse": rmse_t, "n": int(len(part))})
        if rows:
            results.extend(rows)

    if not results:
        console.print("No period data available to backtest.", style="yellow")
        return

    out_df = pd.DataFrame(results).sort_values(["period", "season"])    
    console.print(out_df)
    out_path = paths.data_processed / "backtest_periods_metrics.csv"
    out_df.to_csv(out_path, index=False)
    console.print(f"Saved period backtest metrics to {out_path}")


@cli.command("enrich-periods")
@click.option("--rate-delay", type=float, default=0.6, help="Delay between requests in seconds")
@click.option("--max-workers", type=int, default=4, help="Concurrent workers for period fetch")
@click.option("--limit", type=int, default=None, help="Limit number of games to enrich (for quick tests)")
@click.option("--seasons", type=str, default=None, help="Comma-separated season end years to enrich (e.g., 2023,2024)")
@click.option("--verbose", is_flag=True, default=False, help="Print progress while fetching")
def enrich_periods_cmd(rate_delay: float, max_workers: int, limit: int | None, seasons: str | None, verbose: bool):
    """Enrich existing raw games with quarter/OT line scores.

    Useful when LeagueGameLog is timing out or blocked; only calls BoxScoreSummaryV2
    per missing game. Writes progress to data/raw/_period_fetch_progress.txt.
    """
    console.rule("Enrich Periods")
    season_list = None
    if seasons:
        try:
            season_list = [int(s.strip()) for s in seasons.split(',') if s.strip()]
        except Exception:
            season_list = None
    df = enrich_periods_existing(rate_delay=rate_delay, verbose=verbose, max_workers=max_workers, limit=limit, seasons=season_list)
    console.print(f"Updated raw with periods; rows={len(df)}")


@cli.command("backfill-odds")
@click.option("--api-key", envvar="ODDS_API_KEY", type=str, required=False, help="OddsAPI key (or set env ODDS_API_KEY)")
@click.option("--start", type=str, required=True, help="Start date ISO (e.g., 2016-10-01T00:00:00Z)")
@click.option("--end", type=str, required=True, help="End date ISO (e.g., 2025-06-30T23:59:59Z)")
@click.option("--step-days", type=int, default=5, help="Days between historical snapshots (cost control)")
@click.option("--regions", type=str, default="us", help="OddsAPI regions (e.g., us,us2,uk)")
@click.option("--markets", type=str, default="h2h,spreads,totals", help="Markets to fetch")
@click.option("--verbose", is_flag=True, default=False)
def backfill_odds_cmd(api_key: str | None, start: str, end: str, step_days: int, regions: str, markets: str, verbose: bool):
    """Backfill NBA historical odds to data/raw/odds_nba.(parquet|csv)."""
    console.rule("Backfill (OddsAPI historical)")
    import datetime as _dt
    if not api_key:
        console.print("Provide --api-key or set ODDS_API_KEY env.", style="red"); return
    try:
        start_dt = _dt.datetime.fromisoformat(start.replace("Z","+00:00")).replace(tzinfo=None)
        end_dt = _dt.datetime.fromisoformat(end.replace("Z","+00:00")).replace(tzinfo=None)
    except Exception:
        console.print("Invalid start/end ISO datetimes.", style="red"); return
    cfg = OddsApiConfig(api_key=api_key, regions=regions, markets=markets, odds_format="american")
    df = backfill_historical_odds(cfg, start_dt, end_dt, step_days=step_days, verbose=verbose)
    console.print(f"Odds rows now: {0 if df is None else len(df)}")


@cli.command("make-closing-lines")
def make_closing_lines_cmd():
    """Compute consensus closing lines per event from data/raw/odds_nba and save to data/processed/closing_lines.parquet."""
    console.rule("Build closing lines")
    odds_parq = paths.data_raw / "odds_nba.parquet"
    odds_csv = paths.data_raw / "odds_nba.csv"
    if odds_parq.exists():
        odds_df = pd.read_parquet(odds_parq)
    elif odds_csv.exists():
        odds_df = pd.read_csv(odds_csv)
    else:
        console.print("No odds data found. Run backfill-odds.", style="red"); return
    wide = consensus_lines_at_close(odds_df)
    out = paths.data_processed / "closing_lines.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    wide.to_parquet(out, index=False)
    console.print(f"Saved {len(wide)} rows to {out}")


@cli.command("export-closing-lines-csv")
@click.option("--date", "date_str", type=str, required=True, help="Game date (YYYY-MM-DD) to export consensus closing lines for")
@click.option("--out", "out_path", type=click.Path(), required=False, help="Optional output CSV path (defaults to data/processed/closing_lines_YYYY-MM-DD.csv)")
def export_closing_lines_csv_cmd(date_str: str, out_path: str | None):
    """Export per-date consensus closing lines to a CSV the frontend can consume.

    Looks for data/processed/closing_lines.parquet. If missing, falls back to
    data/raw/odds_nba.(parquet|csv) and computes consensus via consensus_lines_at_close.

    Output columns: date,home_team,visitor_team,home_ml,away_ml,home_spread,total,bookmaker
    (plus spread/total price columns if available).
    """
    console.rule("Export closing lines CSV")
    try:
        target_date = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date. Use YYYY-MM-DD.", style="red"); return

    # Load precomputed closing lines if available
    clos_parq = paths.data_processed / "closing_lines.parquet"
    wide = None
    if clos_parq.exists():
        try:
            wide = pd.read_parquet(clos_parq)
        except Exception as e:
            # No parquet engine available on ARM64? Fallback to computing from raw odds files
            console.print(f"Unable to read {clos_parq} ({e}); falling back to raw odds.", style="yellow")
            wide = None
    else:
        # Fallback: compute from raw odds
        odds_parq = paths.data_raw / "odds_nba.parquet"
        odds_csv = paths.data_raw / "odds_nba.csv"
        if odds_parq.exists():
            try:
                odds_df = pd.read_parquet(odds_parq)
            except Exception as e:
                console.print(f"Unable to read {odds_parq} ({e}); trying CSV fallback...", style="yellow")
                odds_df = None
        elif odds_csv.exists():
            odds_df = pd.read_csv(odds_csv)
        else:
            console.print("No odds data found. Run backfill-odds or make-closing-lines first.", style="red"); return
        if odds_df is None and odds_csv.exists():
            odds_df = pd.read_csv(odds_csv)
        if odds_df is None or odds_df.empty:
            console.print("No odds data available to compute closings.", style="yellow"); return
        wide = consensus_lines_at_close(odds_df)

    if wide is None or wide.empty:
        # Last-resort fallback: use per-date game_odds_{date}.csv if present
        game_odds_csv = paths.data_processed / f"game_odds_{target_date}.csv"
        if game_odds_csv.exists():
            console.print(f"No precomputed closing lines; using {game_odds_csv} as fallback.", style="yellow")
            try:
                wide = pd.read_csv(game_odds_csv)
            except Exception as e:
                console.print(f"Failed to read {game_odds_csv}: {e}", style="red"); return
        else:
            console.print("No closing lines available.", style="yellow"); return

    # Filter to the requested date by US/Eastern calendar day (handles international/UTC offsets)
    df = wide.copy()
    if "commence_time" in df.columns:
        try:
            df["date"] = pd.to_datetime(df["commence_time"], utc=True).dt.tz_convert("US/Eastern").dt.date
        except Exception:
            # Fallback: naive date (UTC)
            df["date"] = pd.to_datetime(df["commence_time"]).dt.date
    elif "date" in df.columns:
        # Already present in fallback game_odds
        try:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        except Exception:
            pass
    df = df[df["date"] == target_date]
    if df.empty:
        console.print(f"No events found on {target_date}.", style="yellow"); return

    # Normalize and map to export schema
    if "home_team" in df.columns:
        df["home_team"] = df["home_team"].apply(normalize_team)
    # Visitor team = away team normalized
    if "visitor_team" not in df.columns and "away_team" in df.columns:
        df["visitor_team"] = df["away_team"].apply(normalize_team)
    # Map numeric columns
    if "home_spread" not in df.columns:
        df["home_spread"] = df.get("spread_point")
    if "total" not in df.columns:
        df["total"] = df.get("total_point")
    if "bookmaker" not in df.columns:
        df["bookmaker"] = "consensus"

    keep = [
        "date","home_team","visitor_team",
        "home_ml","away_ml","home_spread","total","bookmaker",
    ]
    # Include prices if present
    opt = [
        "home_spread_price","away_spread_price","total_over_price","total_under_price"
    ]
    for c in opt:
        if c in df.columns:
            keep.append(c)

    export = df[keep].sort_values(["home_team","visitor_team"]).copy()
    # Ensure date is ISO string
    export["date"] = export["date"].astype(str)

    # Decide output path
    if out_path is None:
        out_path = str(paths.data_processed / f"closing_lines_{target_date}.csv")
    outp = click.Path()(out_path)
    # Ensure folder
    paths.data_processed.mkdir(parents=True, exist_ok=True)
    export.to_csv(out_path, index=False)
    console.print({"rows": int(len(export)), "output": out_path})

@cli.command("attach-closing-lines")
def attach_closing_lines_cmd():
    """Merge consensus closing lines onto features and save as features_with_market.parquet."""
    console.rule("Attach closing lines to features")
    feats_path = paths.data_processed / "features.parquet"
    clos_path = paths.data_processed / "closing_lines.parquet"
    if not feats_path.exists():
        console.print("Features not found. Run build-features first.", style="red"); return
    if not clos_path.exists():
        console.print("Closing lines not found. Run make-closing-lines first.", style="red"); return
    df = pd.read_parquet(feats_path)
    cl = pd.read_parquet(clos_path)
    # Prepare keys: normalize team names and align dates
    tmp = df.copy()
    tmp["date"] = pd.to_datetime(tmp["date"]).dt.date
    tmp["home_team_norm"] = tmp["home_team"].apply(normalize_team)
    tmp["visitor_team_norm"] = tmp["visitor_team"].apply(normalize_team)
    cl = cl.copy()
    cl["date"] = pd.to_datetime(cl["commence_time"]).dt.date
    cl["home_team_norm"] = cl["home_team"].apply(normalize_team)
    cl["visitor_team_norm"] = cl["away_team"].apply(normalize_team)
    keep = [
        "event_id","date","home_team_norm","visitor_team_norm",
        "home_ml","away_ml","spread_point","home_spread_price","away_spread_price",
        "total_point","total_over_price","total_under_price"
    ]
    cl_small = cl[keep]
    merged = tmp.merge(
        cl_small,
        on=["date","home_team_norm","visitor_team_norm"],
        how="left",
        suffixes=("","_cl")
    )
    out = paths.data_processed / "features_with_market.parquet"
    merged.to_parquet(out, index=False)
    console.print(f"Saved merged dataset with market columns to {out} (rows={len(merged)})")


@cli.command("calibrate-win")
@click.option("--season", type=int, required=False, help="Only compute for a single season")
@click.option("--bins", type=int, default=10, help="Number of probability bins (deciles by default)")
def calibrate_win_cmd(season: int|None, bins: int):
    """Compute calibration for win probabilities (decile reliability and log-loss)."""
    console.rule("Calibrate Win Probabilities")
    feats_path = paths.data_processed / "features.parquet"
    if not feats_path.exists():
        console.print("Features not found. Run build-features first.", style="red"); return
    df = pd.read_parquet(feats_path)
    # Load win model and features
    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
        win_model = joblib.load(paths.models / "win_prob.joblib")
    except FileNotFoundError:
        console.print("Models not found. Run train first.", style="red"); return
    data = df.dropna(subset=["target_home_win"]).copy()
    if season is not None:
        data = data[data["season"] == season]
        if data.empty:
            console.print(f"No data for season {season}", style="yellow"); return
    X = data[feat_cols].fillna(0)
    y = data["target_home_win"].astype(int)
    p = win_model.predict_proba(X)[:, 1]

    # Bin probabilities into equal-width intervals [0,1]
    data_cal = pd.DataFrame({"p": p, "y": y})
    data_cal["bin"] = pd.cut(data_cal["p"], bins=bins, include_lowest=True)
    summary = data_cal.groupby("bin").agg(
        n=("y", "size"),
        p_mean=("p", "mean"),
        y_rate=("y", "mean")
    ).reset_index()

    # Log-loss
    from sklearn.metrics import log_loss  # type: ignore
    ll = float(log_loss(y, p, labels=[0, 1]))
    summary["log_loss_overall"] = ll

    out = paths.data_processed / (f"calibration_win_{season}.csv" if season is not None else "calibration_win_all.csv")
    summary.to_csv(out, index=False)
    console.print(summary)
    console.print(f"Saved calibration to {out}")

@cli.command("backtest-vs-market")
@click.option("--start", type=int, required=False, help="Start season (e.g., 2018)")
@click.option("--end", type=int, required=False, help="End season inclusive (e.g., 2025)")
@click.option("--last-n", type=int, required=False, help="Evaluate last N seasons if start/end not provided")
@click.option("--win-edge", type=float, default=0.03, help="Min edge to bet moneyline (model - market implied)")
@click.option("--spread-edge", type=float, default=1.0, help="Min absolute edge (points) to bet spread")
@click.option("--total-edge", type=float, default=1.5, help="Min absolute edge (points) to bet total")
@click.option("--default-spread-price", type=int, default=-110, help="Assumed price for ATS when side price unavailable")
@click.option("--default-total-price", type=int, default=-110, help="Assumed price for totals when side price unavailable")
@click.option("--stake-mode", type=click.Choice(["flat","kelly","half-kelly"]), default="flat", help="Staking method for bankroll simulation")
@click.option("--bankroll-start", type=float, default=100.0, help="Starting bankroll for simulation")
@click.option("--kelly-cap", type=float, default=0.05, help="Max fraction of bankroll per bet for Kelly staking")
@click.option("--sigma-margin", type=float, default=13.5, help="Sigma (points) for ATS probability model")
@click.option("--sigma-total", type=float, default=19.5, help="Sigma (points) for Totals probability model")
def backtest_vs_market_cmd(start: int|None, end: int|None, last_n: int|None, win_edge: float, spread_edge: float, total_edge: float, default_spread_price: int, default_total_price: int, stake_mode: str, bankroll_start: float, kelly_cap: float, sigma_margin: float, sigma_total: float):
    """Simulate ROI vs consensus closing lines using model predictions.

    Bets:
    - Moneyline: bet home or away if model implied edge >= win-edge.
    - Spread: bet ATS on the side of model margin if |pred - market| >= spread-edge.
    - Total: bet Over/Under if |pred - total| >= total-edge.

    Prices: uses closing consensus prices when available, otherwise assumed -110 for ATS/O/U. One bet per market per game.
    """
    console.rule("Backtest vs Market")
    feats_path = paths.data_processed / "features_with_market.parquet"
    if not feats_path.exists():
        console.print("features_with_market.parquet not found. Run attach-closing-lines first.", style="red"); return

    df = pd.read_parquet(feats_path)
    all_seasons = sorted(df["season"].dropna().unique().tolist())
    if start is None and end is None:
        if last_n is None:
            last_n = 5
        seasons = all_seasons[-last_n:]
    else:
        if start is None:
            start = all_seasons[0]
        if end is None:
            end = all_seasons[-1]
        seasons = [s for s in all_seasons if start <= s <= end]
    if not seasons:
        console.print("No seasons selected for backtest.", style="yellow"); return

    # Load models and feature columns
    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
        win_model = joblib.load(paths.models / "win_prob.joblib")
        spread_model = joblib.load(paths.models / "spread_margin.joblib")
        total_model = joblib.load(paths.models / "totals.joblib")
    except FileNotFoundError:
        console.print("Models not found. Run train first.", style="red"); return

    def implied_prob_american(o: float|int|None):
        if o is None or pd.isna(o):
            return None
        o = float(o)
        if o < 0:
            return (-o) / ((-o) + 100.0)
        return 100.0 / (o + 100.0)

    def profit_from_american(price: float|int, stake: float = 1.0) -> float:
        price = float(price)
        if price > 0:
            return stake * (price / 100.0)
        else:
            return stake * (100.0 / abs(price))

    def american_to_b(price: float|int) -> float:
        price = float(price)
        return (price / 100.0) if price > 0 else (100.0 / abs(price))

    import math
    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    season_rows = []
    summary_bets = []
    for s in seasons:
        part = df[(df["season"] == s)].copy()
        if part.empty:
            continue
        X = part[feat_cols].fillna(0)
        part["pred_home_win_prob"] = win_model.predict_proba(X)[:, 1]
        part["pred_margin"] = spread_model.predict(X)
        part["pred_total"] = total_model.predict(X)
        # Market values
        part["market_home_margin"] = -part["spread_point"]
        part["market_total"] = part["total_point"]

        # Moneyline bets
        ml_bets = []
        for _, r in part.dropna(subset=["home_ml","away_ml"]).iterrows():
            ph = r["pred_home_win_prob"]
            ih = implied_prob_american(r["home_ml"]) or 0
            ia = implied_prob_american(r["away_ml"]) or 0
            home_edge = ph - ih
            away_edge = (1 - ph) - ia
            if home_edge >= win_edge and home_edge >= away_edge:
                outcome = 1 if (r.get("home_pts") > r.get("visitor_pts")) else 0 if pd.notna(r.get("home_pts")) else None
                price = r["home_ml"]
                ml_bets.append({"season": s, "market": "ML", "side": "home", "edge": home_edge, "price": price, "won": outcome, "date": pd.to_datetime(r.get("date")).date(), "p_model": ph})
            elif away_edge >= win_edge:
                outcome = 1 if (r.get("home_pts") < r.get("visitor_pts")) else 0 if pd.notna(r.get("home_pts")) else None
                price = r["away_ml"]
                ml_bets.append({"season": s, "market": "ML", "side": "away", "edge": away_edge, "price": price, "won": outcome, "date": pd.to_datetime(r.get("date")).date(), "p_model": (1 - ph)})

    # Spread bets (use both-side prices if available; assume -110 if missing)
        ats_bets = []
        for _, r in part.dropna(subset=["market_home_margin"]).iterrows():
            edge = r["pred_margin"] - r["market_home_margin"]
            if abs(edge) >= spread_edge:
                if edge > 0:
                    # Bet home ATS
                    won = None
                    if pd.notna(r.get("home_pts")):
                        margin = r.get("home_pts") - r.get("visitor_pts")
                        spread = r["market_home_margin"]
                        won = 1 if margin > spread else 0 if margin < spread else 0  # push counts as 0 profit
                    price = r.get("home_spread_price") if pd.notna(r.get("home_spread_price")) else default_spread_price
                    # probability of cover via normal assumption
                    mu = r["pred_margin"]; T = r["market_home_margin"]
                    p_cov = 1 - norm_cdf((T - mu) / max(1e-6, sigma_margin))
                    ats_bets.append({"season": s, "market": "ATS", "side": "home", "edge": edge, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_cov})
                else:
                    # Bet away ATS
                    won = None
                    if pd.notna(r.get("home_pts")):
                        margin = r.get("home_pts") - r.get("visitor_pts")
                        spread = r["market_home_margin"]
                        # Away +X wins if margin < spread
                        won = 1 if margin < spread else 0 if margin > spread else 0
                    price = r.get("away_spread_price") if pd.notna(r.get("away_spread_price")) else default_spread_price
                    mu = r["pred_margin"]; T = r["market_home_margin"]
                    p_cov = norm_cdf((T - mu) / max(1e-6, sigma_margin))
                    ats_bets.append({"season": s, "market": "ATS", "side": "away", "edge": -edge, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_cov})

    # Totals bets (use Over/Under prices if available)
        tot_bets = []
        for _, r in part.dropna(subset=["market_total"]).iterrows():
            diff = r["pred_total"] - r["market_total"]
            if abs(diff) >= total_edge:
                if diff > 0:
                    # Over
                    won = None
                    if pd.notna(r.get("home_pts")):
                        total = r.get("home_pts") + r.get("visitor_pts")
                        won = 1 if total > r["market_total"] else 0 if total < r["market_total"] else 0
                    price = r.get("total_over_price") if pd.notna(r.get("total_over_price")) else default_total_price
                    mu = r["pred_total"]; T = r["market_total"]
                    p_over = 1 - norm_cdf((T - mu) / max(1e-6, sigma_total))
                    tot_bets.append({"season": s, "market": "TOTAL", "side": "over", "edge": diff, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_over})
                else:
                    # Under
                    won = None
                    if pd.notna(r.get("home_pts")):
                        total = r.get("home_pts") + r.get("visitor_pts")
                        won = 1 if total < r["market_total"] else 0 if total > r["market_total"] else 0
                    price = r.get("total_under_price") if pd.notna(r.get("total_under_price")) else default_total_price
                    mu = r["pred_total"]; T = r["market_total"]
                    p_under = norm_cdf((T - mu) / max(1e-6, sigma_total))
                    tot_bets.append({"season": s, "market": "TOTAL", "side": "under", "edge": -diff, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_under})

        # Aggregate ROI
        def summarize(bets: list[dict], label: str) -> dict:
            if not bets:
                return {"season": s, "market": label, "n_bets": 0, "roi": 0.0, "hit_rate": 0.0}
            settled = [b for b in bets if b["won"] is not None]
            units = 0.0
            hits = 0
            edges = []
            evs = []
            for b in settled:
                if b["won"] == 1:
                    units += profit_from_american(b["price"], 1.0)
                    hits += 1
                elif b["won"] == 0:
                    units -= 1.0
                # Expected value per unit (using model probability if present)
                p_mod = b.get("p_model")
                if p_mod is not None and not pd.isna(p_mod):
                    b_odds = american_to_b(b["price"])  # decimal net odds
                    ev = p_mod * b_odds - (1 - p_mod) * 1.0
                    evs.append(ev)
                e = b.get("edge")
                if e is not None:
                    edges.append(float(e))
            n = len(settled)
            roi = units / n if n > 0 else 0.0
            hr = hits / n if n > 0 else 0.0
            return {
                "season": s,
                "market": label,
                "n_bets": n,
                "roi": float(roi),
                "hit_rate": float(hr),
                "avg_edge": float(pd.Series(edges).mean()) if edges else None,
                "avg_ev": float(pd.Series(evs).mean()) if evs else None,
            }

        ml_sum = summarize(ml_bets, "ML")
        ats_sum = summarize(ats_bets, "ATS")
        tot_sum = summarize(tot_bets, "TOTAL")
        season_rows.extend([ml_sum, ats_sum, tot_sum])
        # Compute implied prob and Kelly fraction for ledger
        def enrich(b):
            out = []
            for bet in b:
                price = bet["price"]
                p = bet.get("p_model")
                imp = implied_prob_american(price)
                kelly = None
                if p is not None and not pd.isna(p):
                    bdec = american_to_b(price)
                    q = 1 - p
                    kelly = max(0.0, (bdec * p - q) / bdec)
                bet["implied_prob"] = float(imp) if imp is not None else None
                bet["kelly_fraction"] = float(kelly) if kelly is not None else None
                # expected value per unit
                if p is not None and not pd.isna(p):
                    ev = p * american_to_b(price) - (1 - p)
                    bet["ev_unit"] = float(ev)
                out.append(bet)
            return out
        ml_bets = enrich(ml_bets)
        ats_bets = enrich(ats_bets)
        tot_bets = enrich(tot_bets)
        summary_bets.extend(ml_bets + ats_bets + tot_bets)

    # Results (unit-based)
    res_df = pd.DataFrame(season_rows).sort_values(["market","season"]) if season_rows else pd.DataFrame()
    console.print(res_df)
    out_season = paths.data_processed / "backtest_vs_market.csv"
    res_df.to_csv(out_season, index=False)
    # Save raw bet ledger
    ledger = pd.DataFrame(summary_bets)
    out_ledger = paths.data_processed / "backtest_vs_market_ledger.csv"
    ledger.to_csv(out_ledger, index=False)
    console.print(f"Saved summaries to {out_season} and ledger to {out_ledger}")

    # Bankroll simulation per season and market
    if not ledger.empty:
        ledger["date"] = pd.to_datetime(ledger["date"]).dt.date
        bank_summaries = []
        curves = []
        for (s, m), group in ledger.groupby(["season","market"], sort=True):
            g = group.sort_values("date").copy()
            bankroll = float(bankroll_start)
            for idx, row in g.iterrows():
                price = row["price"]
                b = american_to_b(price)
                p = row.get("p_model")
                stake = 1.0  # default flat
                if stake_mode in ("kelly","half-kelly") and p is not None and not pd.isna(p):
                    q = 1.0 - float(p)
                    f_star = max(0.0, (b * float(p) - q) / b)
                    if stake_mode == "half-kelly":
                        f_star *= 0.5
                    f_star = min(f_star, float(kelly_cap))
                    stake = bankroll * f_star
                pnl = 0.0
                if row["won"] == 1:
                    pnl = profit_from_american(price, stake)
                elif row["won"] == 0:
                    pnl = -stake
                bankroll += pnl
                curves.append({"season": s, "market": m, "date": row["date"], "stake": float(stake), "price": float(price), "p_model": float(p) if p is not None else None, "pnl": float(pnl), "bankroll": float(bankroll)})
            roi = (bankroll / bankroll_start) - 1.0
            bank_summaries.append({"season": s, "market": m, "start": float(bankroll_start), "end": float(bankroll), "roi_bankroll": float(roi)})
        bank_df = pd.DataFrame(bank_summaries).sort_values(["market","season"]) if bank_summaries else pd.DataFrame()
        curve_df = pd.DataFrame(curves).sort_values(["market","season","date"]) if curves else pd.DataFrame()
        out_bank = paths.data_processed / "backtest_vs_market_bankroll.csv"
        out_curve = paths.data_processed / "backtest_vs_market_bankroll_curve.csv"
        bank_df.to_csv(out_bank, index=False)
        curve_df.to_csv(out_curve, index=False)
        console.print(f"Saved bankroll summaries to {out_bank} and curves to {out_curve}")


@cli.command("make-synthetic-period-lines")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), default=str(paths.data_processed / "period_lines_synthetic.csv"), help="Output CSV path for synthetic period lines")
def make_synthetic_period_lines_cmd(out_path: str):
    """Create synthetic period (halves/quarters) lines from full-game closers using empirical ratios.

    - Totals: scale full-game total by average scoring share per period (computed from raw games with period data).
    - Spreads: scale full-game home spread by time fraction (0.5 for halves, 0.25 per quarter).
    Produces a CSV compatible with backtest-periods-vs-market.
    """
    console.rule("Make synthetic period lines")
    # Load raw with periods to estimate scoring shares
    raw_parq = paths.data_raw / "games_nba_api.parquet"
    raw_csv = paths.data_raw / "games_nba_api.csv"
    if raw_parq.exists():
        raw = pd.read_parquet(raw_parq)
    elif raw_csv.exists():
        raw = pd.read_csv(raw_csv)
    else:
        console.print("No raw games file found.", style="red"); return
    # Ensure totals available
    if "total_points" not in raw.columns:
        if {"home_pts","visitor_pts"}.issubset(raw.columns):
            raw["total_points"] = raw[["home_pts","visitor_pts"]].sum(axis=1)
        else:
            console.print("Raw missing total points.", style="red"); return
    # Compute shares where available (robust to NaNs)
    shares = {}
    # Halves
    for half in ("h1","h2"):
        hcol = f"home_{half}"; vcol = f"visitor_{half}"
        if hcol in raw.columns and vcol in raw.columns:
            hp = pd.to_numeric(raw[hcol], errors="coerce")
            vp = pd.to_numeric(raw[vcol], errors="coerce")
            tp = pd.to_numeric(raw["total_points"], errors="coerce")
            mask = hp.notna() & vp.notna() & tp.notna() & (tp > 0)
            ratio = (hp[mask] + vp[mask]) / tp[mask]
            shares[half] = float(ratio.mean()) if not ratio.empty else None
        else:
            shares[half] = None
    # Quarters
    for q in ("q1","q2","q3","q4"):
        hcol = f"home_{q}"; vcol = f"visitor_{q}"
        if hcol in raw.columns and vcol in raw.columns:
            hp = pd.to_numeric(raw[hcol], errors="coerce")
            vp = pd.to_numeric(raw[vcol], errors="coerce")
            tp = pd.to_numeric(raw["total_points"], errors="coerce")
            mask = hp.notna() & vp.notna() & tp.notna() & (tp > 0)
            ratio = (hp[mask] + vp[mask]) / tp[mask]
            shares[q] = float(ratio.mean()) if not ratio.empty else None
        else:
            shares[q] = None
    # Fallbacks if missing: use time fractions
    shares.setdefault("h1", 0.5); shares.setdefault("h2", 0.5)
    for q in ("q1","q2","q3","q4"):
        shares.setdefault(q, 0.25)

    # Load features_with_market to get full-game spread/total per matchup
    fwm = paths.data_processed / "features_with_market.parquet"
    feats = paths.data_processed / "features.parquet"
    if fwm.exists():
        base = pd.read_parquet(fwm)
    elif feats.exists():
        base = pd.read_parquet(feats)
    else:
        console.print("No features found. Run build-features first.", style="red"); return
    base = base.copy()
    base["date"] = pd.to_datetime(base["date"]).dt.date
    # Ensure we have market columns (spread_point, total_point). If missing, drop.
    if "spread_point" not in base.columns and "total_point" not in base.columns:
        console.print("No closing lines attached. Run make-closing-lines and attach-closing-lines first.", style="red"); return

    rows = []
    for _, r in base.iterrows():
        total = r.get("total_point")
        spread = r.get("spread_point")  # home spread (negative if favored)
        if pd.isna(total) and pd.isna(spread):
            continue
        out = {
            "date": r.get("date"),
            "home_team": r.get("home_team"),
            "visitor_team": r.get("visitor_team"),
        }
        # Totals
        if pd.notna(total):
            out["h1_total"] = float(total) * shares["h1"]
            out["h2_total"] = float(total) * shares["h2"]
            for q in ("q1","q2","q3","q4"):
                out[f"{q}_total"] = float(total) * shares[q]
        # Spreads (scale by time fraction)
        if pd.notna(spread):
            out["h1_spread"] = float(spread) * 0.5
            out["h2_spread"] = float(spread) * 0.5
            for q in ("q1","q2","q3","q4"):
                out[f"{q}_spread"] = float(spread) * 0.25
        if any(k.endswith('_total') or k.endswith('_spread') for k in out.keys()):
            rows.append(out)
    out_df = pd.DataFrame(rows)
    from pathlib import Path
    out_p = Path(out_path) if out_path else (paths.data_processed / "period_lines_synthetic.csv")
    # If relative, write under data/processed
    if not out_p.is_absolute():
        out_p = paths.data_processed / out_p
    out_p.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_p, index=False)
    console.print(f"Saved synthetic period lines to {out_p} (rows={len(out_df)})")

@cli.command("backtest-periods-vs-market")
@click.option("--odds-csv", type=click.Path(exists=True), required=True, help="CSV containing period odds by game: date,home_team,visitor_team and any of: h1_spread,h1_total,h2_spread,h2_total,q1_spread,q1_total,q2_spread,q2_total,q3_spread,q3_total,q4_spread,q4_total. Optional price columns: *_home_spread_price,*_away_spread_price,*_total_over_price,*_total_under_price.")
@click.option("--start", type=int, required=False, help="Start season (e.g., 2018)")
@click.option("--end", type=int, required=False, help="End season inclusive (e.g., 2025)")
@click.option("--last-n", type=int, required=False, help="Evaluate last N seasons if start/end not provided")
@click.option("--spread-edge", type=float, default=1.0, help="Min absolute edge (points) to bet period spreads")
@click.option("--total-edge", type=float, default=1.5, help="Min absolute edge (points) to bet period totals")
@click.option("--default-spread-price", type=int, default=-110, help="Assumed price for ATS when side price unavailable")
@click.option("--default-total-price", type=int, default=-110, help="Assumed price for totals when side price unavailable")
@click.option("--stake-mode", type=click.Choice(["flat","kelly","half-kelly"]), default="flat", help="Staking method for bankroll simulation")
@click.option("--bankroll-start", type=float, default=100.0, help="Starting bankroll for simulation")
@click.option("--kelly-cap", type=float, default=0.05, help="Max fraction of bankroll per bet for Kelly staking")
@click.option("--sigma-margin-half", type=float, default=9.6, help="Sigma (points) for Half ATS probability model; ~13.5/sqrt(2)")
@click.option("--sigma-total-half", type=float, default=13.8, help="Sigma (points) for Half Totals probability model; ~19.5/sqrt(2)")
@click.option("--sigma-margin-quarter", type=float, default=6.8, help="Sigma (points) for Quarter ATS probability model; ~13.5/2")
@click.option("--sigma-total-quarter", type=float, default=9.8, help="Sigma (points) for Quarter Totals probability model; ~19.5/2")
def backtest_periods_vs_market_cmd(odds_csv: str, start: int|None, end: int|None, last_n: int|None, spread_edge: float, total_edge: float, default_spread_price: int, default_total_price: int, stake_mode: str, bankroll_start: float, kelly_cap: float, sigma_margin_half: float, sigma_total_half: float, sigma_margin_quarter: float, sigma_total_quarter: float):
    """Simulate ROI vs period (halves/quarters) odds using model period predictions.

    Requires a CSV with period markets per game. Bets placed when model edges exceed thresholds; bankroll simulated by season and market.
    """
    console.rule("Backtest Periods vs Market")
    feats_path = paths.data_processed / "features.parquet"
    if not feats_path.exists():
        console.print("features.parquet not found. Run build-features first.", style="red"); return

    df = pd.read_parquet(feats_path)
    all_seasons = sorted(df["season"].dropna().unique().tolist())
    if start is None and end is None:
        if last_n is None:
            last_n = 5
        seasons = all_seasons[-last_n:]
    else:
        if start is None:
            start = all_seasons[0]
        if end is None:
            end = all_seasons[-1]
        seasons = [s for s in all_seasons if start <= s <= end]
    if not seasons:
        console.print("No seasons selected for backtest.", style="yellow"); return

    # Load models and feature columns
    try:
        feat_cols = joblib.load(paths.models / "feature_columns.joblib")
        halves = joblib.load(paths.models / "halves_models.joblib")
        quarters = joblib.load(paths.models / "quarters_models.joblib")
    except FileNotFoundError:
        console.print("Period models not found. Run train first.", style="red"); return

    # Read period odds CSV
    odds = pd.read_csv(odds_csv)
    # Normalize keys
    if 'date' in odds.columns:
        odds['date'] = pd.to_datetime(odds['date']).dt.date
    odds['home_team'] = odds['home_team'].apply(normalize_team)
    odds['visitor_team'] = odds['visitor_team'].apply(normalize_team)

    # Prepare feature base with period predictions
    base = df[df['season'].isin(seasons)].copy()
    base['date'] = pd.to_datetime(base['date']).dt.date
    base['home_team'] = base['home_team'].apply(normalize_team)
    base['visitor_team'] = base['visitor_team'].apply(normalize_team)
    X = base[feat_cols].fillna(0)
    # Halves predictions
    for half in ("h1","h2"):
        if half in halves:
            base[f"{half}_pred_margin"] = halves[half]["margin"].predict(X)
            base[f"{half}_pred_total"] = halves[half]["total"].predict(X)
    # Quarters predictions
    for q in ("q1","q2","q3","q4"):
        if q in quarters:
            base[f"{q}_pred_margin"] = quarters[q]["margin"].predict(X)
            base[f"{q}_pred_total"] = quarters[q]["total"].predict(X)

    # Merge odds
    merged = base.merge(odds, on=['date','home_team','visitor_team'], how='left', suffixes=("","_odds"))

    import math
    def norm_cdf(x: float) -> float:
        return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

    def american_to_b(price: float|int) -> float:
        price = float(price)
        return (price / 100.0) if price > 0 else (100.0 / abs(price))

    def profit_from_american(price: float|int, stake: float = 1.0) -> float:
        price = float(price)
        return stake * (price / 100.0) if price > 0 else stake * (100.0 / abs(price))

    period_specs = [
        ("h1", sigma_margin_half, sigma_total_half),
        ("h2", sigma_margin_half, sigma_total_half),
        ("q1", sigma_margin_quarter, sigma_total_quarter),
        ("q2", sigma_margin_quarter, sigma_total_quarter),
        ("q3", sigma_margin_quarter, sigma_total_quarter),
        ("q4", sigma_margin_quarter, sigma_total_quarter),
    ]

    season_rows = []
    ledger_rows = []
    for s in seasons:
        part = merged[merged['season'] == s].copy()
        if part.empty:
            continue
        # For each period, form bets from available odds columns
        for period, sig_m, sig_t in period_specs:
            # Spread bets
            sp_col = f"{period}_spread"
            sp_hp = f"{period}_home_spread_price"; sp_ap = f"{period}_away_spread_price"
            pred_m = f"{period}_pred_margin"
            tgt_m = f"target_{period}_margin"
            if sp_col in part.columns and pred_m in part.columns:
                for _, r in part.dropna(subset=[sp_col, pred_m]).iterrows():
                    edge = r[pred_m] - (-r[sp_col])
                    if abs(edge) >= spread_edge:
                        if edge > 0:
                            won = None
                            if tgt_m in part.columns and pd.notna(r.get(tgt_m)):
                                spread = -r[sp_col]
                                margin = float(r.get(tgt_m))
                                won = 1 if margin > spread else 0 if margin < spread else 0
                            price = r.get(sp_hp) if pd.notna(r.get(sp_hp)) else default_spread_price
                            mu = r[pred_m]; T = -r[sp_col]
                            p_cov = 1 - norm_cdf((T - mu) / max(1e-6, sig_m))
                            ledger_rows.append({"season": s, "market": f"{period}_ATS", "side": "home", "edge": edge, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_cov})
                        else:
                            won = None
                            if tgt_m in part.columns and pd.notna(r.get(tgt_m)):
                                spread = -r[sp_col]
                                margin = float(r.get(tgt_m))
                                won = 1 if margin < spread else 0 if margin > spread else 0
                            price = r.get(sp_ap) if pd.notna(r.get(sp_ap)) else default_spread_price
                            mu = r[pred_m]; T = -r[sp_col]
                            p_cov = norm_cdf((T - mu) / max(1e-6, sig_m))
                            ledger_rows.append({"season": s, "market": f"{period}_ATS", "side": "away", "edge": -edge, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_cov})

            # Totals bets
            tot_col = f"{period}_total"
            over_p = f"{period}_total_over_price"; under_p = f"{period}_total_under_price"
            pred_t = f"{period}_pred_total"
            tgt_t = f"target_{period}_total"
            if tot_col in part.columns and pred_t in part.columns:
                for _, r in part.dropna(subset=[tot_col, pred_t]).iterrows():
                    diff = r[pred_t] - r[tot_col]
                    if abs(diff) >= total_edge:
                        if diff > 0:
                            won = None
                            if tgt_t in part.columns and pd.notna(r.get(tgt_t)):
                                total = float(r.get(tgt_t))
                                won = 1 if total > r[tot_col] else 0 if total < r[tot_col] else 0
                            price = r.get(over_p) if pd.notna(r.get(over_p)) else default_total_price
                            mu = r[pred_t]; T = r[tot_col]
                            p_over = 1 - norm_cdf((T - mu) / max(1e-6, sig_t))
                            ledger_rows.append({"season": s, "market": f"{period}_TOTAL", "side": "over", "edge": diff, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_over})
                        else:
                            won = None
                            if tgt_t in part.columns and pd.notna(r.get(tgt_t)):
                                total = float(r.get(tgt_t))
                                won = 1 if total < r[tot_col] else 0 if total > r[tot_col] else 0
                            price = r.get(under_p) if pd.notna(r.get(under_p)) else default_total_price
                            mu = r[pred_t]; T = r[tot_col]
                            p_under = norm_cdf((T - mu) / max(1e-6, sig_t))
                            ledger_rows.append({"season": s, "market": f"{period}_TOTAL", "side": "under", "edge": -diff, "price": price, "won": won, "date": pd.to_datetime(r.get("date")).date(), "p_model": p_under})

        # Summarize per season by market
    def summarize(ledger: pd.DataFrame, label_prefix: str) -> list[dict]:
        out = []
        for (s, m), grp in ledger.groupby(["season","market"], sort=True):
            units = 0.0; hits = 0; n = 0; edges=[]; evs=[]
            for _, b in grp.dropna(subset=["won"]).iterrows():
                n += 1
                if b["won"] == 1:
                    units += profit_from_american(b["price"], 1.0)
                    hits += 1
                elif b["won"] == 0:
                    units -= 1.0
                if pd.notna(b.get("edge")):
                    edges.append(float(b["edge"]))
                p = b.get("p_model")
                if p is not None and not pd.isna(p):
                    ev = float(p) * american_to_b(b["price"]) - (1 - float(p))
                    evs.append(ev)
            roi = (units / n) if n > 0 else 0.0
            hr = (hits / n) if n > 0 else 0.0
            out.append({"season": int(s), "market": m, "n_bets": n, "roi": float(roi), "hit_rate": float(hr), "avg_edge": float(pd.Series(edges).mean()) if edges else None, "avg_ev": float(pd.Series(evs).mean()) if evs else None})
        return out

    # Build ledger DataFrame
    ledger = pd.DataFrame(ledger_rows)
    res_df = pd.DataFrame(summarize(ledger, "period")) if not ledger.empty else pd.DataFrame()
    console.print(res_df)
    out_season = paths.data_processed / "backtest_periods_vs_market.csv"
    res_df.to_csv(out_season, index=False)
    out_ledger = paths.data_processed / "backtest_periods_vs_market_ledger.csv"
    ledger.to_csv(out_ledger, index=False)
    console.print(f"Saved summaries to {out_season} and ledger to {out_ledger}")

    # Bankroll simulation by season and market
    if not ledger.empty:
        ledger["date"] = pd.to_datetime(ledger["date"]).dt.date
        bank_summaries = []
        curves = []
        for (s, m), group in ledger.groupby(["season","market"], sort=True):
            g = group.sort_values("date").copy()
            bankroll = float(bankroll_start)
            for _, row in g.iterrows():
                price = row["price"]
                b = american_to_b(price)
                p = row.get("p_model")
                stake = 1.0
                if stake_mode in ("kelly","half-kelly") and p is not None and not pd.isna(p):
                    q = 1.0 - float(p)
                    f_star = max(0.0, (b * float(p) - q) / b)
                    if stake_mode == "half-kelly":
                        f_star *= 0.5
                    f_star = min(f_star, float(kelly_cap))
                    stake = bankroll * f_star
                pnl = 0.0
                if row["won"] == 1:
                    pnl = profit_from_american(price, stake)
                elif row["won"] == 0:
                    pnl = -stake
                bankroll += pnl
                curves.append({"season": s, "market": m, "date": row["date"], "stake": float(stake), "price": float(price), "p_model": float(p) if p is not None else None, "pnl": float(pnl), "bankroll": float(bankroll)})
            roi = (bankroll / bankroll_start) - 1.0
            bank_summaries.append({"season": s, "market": m, "start": float(bankroll_start), "end": float(bankroll), "roi_bankroll": float(roi)})
        bank_df = pd.DataFrame(bank_summaries).sort_values(["market","season"]) if bank_summaries else pd.DataFrame()
        curve_df = pd.DataFrame(curves).sort_values(["market","season","date"]) if curves else pd.DataFrame()
        out_bank = paths.data_processed / "backtest_periods_vs_market_bankroll.csv"
        out_curve = paths.data_processed / "backtest_periods_vs_market_bankroll_curve.csv"
        bank_df.to_csv(out_bank, index=False)
        curve_df.to_csv(out_curve, index=False)
        console.print(f"Saved bankroll summaries to {out_bank} and curves to {out_curve}")

@cli.command("reconcile-date")
@click.option("--date", "date_str", type=str, required=True, help="Game date YYYY-MM-DD to reconcile")
@click.option("--predictions", "pred_path", type=click.Path(exists=False, dir_okay=False), required=False, help="Optional predictions CSV path; defaults to data/processed/predictions_YYYY-MM-DD.csv then repo-root predictions_YYYY-MM-DD.csv")
def reconcile_date_cmd(date_str: str, pred_path: str | None):
    """Build reconciliation CSV for a date by joining predictions with NBA final scores.

    Writes data/processed/recon_games_YYYY-MM-DD.csv
    Columns: date, home_team, visitor_team, home_tri, away_tri, home_pts, visitor_pts, pred_margin, pred_total, actual_margin, total_actual, margin_error, total_error
    """
    console.rule("Reconcile (Date)")
    # Locate predictions file
    from .config import paths as _paths
    try:
        target_date = pd.to_datetime(date_str).date()
    except Exception:
        console.print("Invalid --date (YYYY-MM-DD)", style="red"); return
    default_proc = _paths.data_processed / f"predictions_{target_date}.csv"
    default_root = _paths.root / f"predictions_{target_date}.csv"
    pred_file = None
    if pred_path:
        from pathlib import Path as _P
        p = _P(pred_path)
        pred_file = p if p.is_absolute() else (_paths.root / pred_path)
        if not pred_file.exists():
            pred_file = None
    if pred_file is None:
        pred_file = default_proc if default_proc.exists() else (default_root if default_root.exists() else None)
    if pred_file is None:
        console.print("Predictions CSV not found for date.", style="yellow"); return
    try:
        preds = pd.read_csv(pred_file)
    except Exception as e:
        console.print(f"Failed to read predictions: {e}", style="red"); return
    # Normalize to tricodes using nba_api static map
    try:
        team_list = static_teams.get_teams()
        full_to_abbr = {str(t.get('full_name')).upper(): str(t.get('abbreviation')).upper() for t in team_list}
        alt = {
            "LOS ANGELES CLIPPERS": "LAC",
            "LA CLIPPERS": "LAC",
            "PHOENIX SUNS": "PHX",
            "GOLDEN STATE WARRIORS": "GSW",
            "SAN ANTONIO SPURS": "SAS",
            "NEW YORK KNICKS": "NYK",
            "BROOKLYN NETS": "BKN",
            "UTAH JAZZ": "UTA",
        }
        def to_tri(name: str) -> str:
            if name is None:
                return ""
            s = str(name).strip().upper()
            if s in full_to_abbr:
                return full_to_abbr[s]
            if s in alt:
                return alt[s]
            if len(s) <= 4:
                return s
            return s
        preds = preds.copy()
        preds["home_tri"] = preds.get("home_team").apply(to_tri)
        preds["away_tri"] = preds.get("visitor_team").apply(to_tri)
    except Exception:
        preds = preds.copy()
        preds["home_tri"] = preds.get("home_team").astype(str).str.upper()
        preds["away_tri"] = preds.get("visitor_team").astype(str).str.upper()
    # Prefer processed finals CSV if available; else fetch via APIs with fallbacks
    try:
        finals = None
        # 0) Use previously exported finals if present
        try:
            from .config import paths as _paths2
            fpath = _paths2.data_processed / f"finals_{target_date}.csv"
            if fpath.exists():
                fdf = pd.read_csv(fpath)
                # Normalize column names
                cols = {c.lower(): c for c in fdf.columns}
                need = {"home_tri","away_tri","home_pts","visitor_pts"}
                if need.issubset(set(cols.keys())):
                    finals = fdf[[cols["home_tri"], cols["away_tri"], cols["home_pts"], cols["visitor_pts"]]].copy()
                    finals.columns = ["home_tri","away_tri","home_pts","visitor_pts"]
        except Exception:
            finals = None

        # 1) If no processed finals, fetch via ScoreboardV2
        try:
            nba_http.STATS_HEADERS.update({
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://www.nba.com',
                'Referer': 'https://www.nba.com/stats/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                'Connection': 'keep-alive',
            })
        except Exception:
            pass
        def fetch_finals_for(game_date: str) -> pd.DataFrame:
            tries = 0
            finals_local = pd.DataFrame()
            while tries < 2 and (finals_local is None or finals_local.empty):
                try:
                    sb = scoreboardv2.ScoreboardV2(game_date=game_date, day_offset=0, timeout=45)
                    nd = sb.get_normalized_dict()
                    gh = pd.DataFrame(nd.get("GameHeader", []))
                    ls = pd.DataFrame(nd.get("LineScore", []))
                    if not gh.empty and not ls.empty:
                        cgh = {c.upper(): c for c in gh.columns}
                        cls = {c.upper(): c for c in ls.columns}
                        team_rows = {}
                        for _, r in ls.iterrows():
                            try:
                                tid = int(r[cls["TEAM_ID"]])
                                tri = str(r[cls["TEAM_ABBREVIATION"]]).upper()
                                pts = None
                                if "PTS" in cls:
                                    try:
                                        pts = int(r[cls["PTS"]])
                                    except Exception:
                                        pts = None
                                team_rows[tid] = {"tri": tri, "pts": pts}
                            except Exception:
                                continue
                        out_rows = []
                        for _, g in gh.iterrows():
                            try:
                                hid = int(g[cgh["HOME_TEAM_ID"]]); vid = int(g[cgh["VISITOR_TEAM_ID"]])
                                gid = str(g.get(cgh.get("GAME_ID", "GAME_ID"), "")).strip()
                                h = team_rows.get(hid, {}); v = team_rows.get(vid, {})
                                out_rows.append({
                                    "game_id": gid,
                                    "home_tri": str(h.get("tri") or "").upper(),
                                    "away_tri": str(v.get("tri") or "").upper(),
                                    "home_pts": h.get("pts"),
                                    "visitor_pts": v.get("pts"),
                                })
                            except Exception:
                                continue
                        finals_local = pd.DataFrame(out_rows)
                    break
                except Exception:
                    tries += 1
                    time.sleep(3)
            return finals_local

        if finals is None or finals.empty:
            finals = fetch_finals_for(str(target_date))
        if finals is None or finals.empty:
            # Try +1 day then -1 day to handle timezone/date slippage
            from datetime import timedelta as _td
            finals = fetch_finals_for(str(target_date + _td(days=1)))
            if finals is None or finals.empty:
                finals = fetch_finals_for(str(target_date - _td(days=1)))

        # Preseason often lacks PTS in nba_api; fallback to NBA CDN daily scoreboard
        def fetch_finals_via_nba_cdn(dt) -> pd.DataFrame:
            try:
                import requests as _req
                ymd = pd.to_datetime(dt).strftime('%Y%m%d')
                url = f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json"
                headers = {
                    'User-Agent': 'Mozilla/5.0',
                    'Accept': 'application/json, text/plain, */*',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/'
                }
                resp = _req.get(url, headers=headers, timeout=30)
                if resp.status_code != 200:
                    return pd.DataFrame()
                js = resp.json()
                games = (js or {}).get('scoreboard', {}).get('games', [])
                rows = []
                for g in games:
                    try:
                        home = g.get('homeTeam', {})
                        away = g.get('awayTeam', {})
                        htri = str(home.get('triCode') or '').upper()
                        atri = str(away.get('triCode') or '').upper()
                        # Scores can be strings; coerce to int if possible
                        def _to_int(x):
                            try:
                                return int(x)
                            except Exception:
                                try:
                                    return int(float(x))
                                except Exception:
                                    return None
                        hpts = _to_int(home.get('score'))
                        apts = _to_int(away.get('score'))
                        rows.append({
                            'home_tri': htri,
                            'away_tri': atri,
                            'home_pts': hpts,
                            'visitor_pts': apts,
                        })
                    except Exception:
                        continue
                return pd.DataFrame(rows)
            except Exception:
                return pd.DataFrame()

        # Use CDN results if nba_api finals missing or all pts are NaN/None
        if finals is None or finals.empty or not (pd.to_numeric(finals.get('home_pts'), errors='coerce').notna().any() and pd.to_numeric(finals.get('visitor_pts'), errors='coerce').notna().any()):
            cdn_df = fetch_finals_via_nba_cdn(target_date)
            if cdn_df is not None and not cdn_df.empty:
                finals = cdn_df

        # If still missing points but we have game_ids from ScoreboardV2, try BoxScoreTraditionalV3 to compute team points
        def fill_pts_via_boxscore(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty or 'game_id' not in df.columns:
                return df
            rows = []
            for _, row in df.iterrows():
                try:
                    gid = str(row.get('game_id') or '').strip()
                    if not gid:
                        rows.append(row)
                        continue
                    bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=gid, timeout=45)
                    nd = bs.get_normalized_dict()
                    tstats = pd.DataFrame(nd.get('TeamStats', []))
                    htri = str(row.get('home_tri') or '').upper(); atri = str(row.get('away_tri') or '').upper()
                    hpts = row.get('home_pts'); apts = row.get('visitor_pts')
                    if not tstats.empty:
                        c = {c.upper(): c for c in tstats.columns}
                        for _, tr in tstats.iterrows():
                            tri = str(tr.get(c.get('TEAM_ABBREVIATION','TEAM_ABBREVIATION'), '')).upper()
                            try:
                                pts = int(tr.get(c.get('PTS','PTS')))
                            except Exception:
                                pts = None
                            if tri == htri:
                                hpts = pts
                            elif tri == atri:
                                apts = pts
                    new_row = row.copy()
                    new_row['home_pts'] = hpts
                    new_row['visitor_pts'] = apts
                    rows.append(new_row)
                except Exception:
                    rows.append(row)
            return pd.DataFrame(rows)

        if finals is not None and not finals.empty and not (pd.to_numeric(finals.get('home_pts'), errors='coerce').notna().any() and pd.to_numeric(finals.get('visitor_pts'), errors='coerce').notna().any()):
            finals = fill_pts_via_boxscore(finals)

        # Last resort: derive team totals from recon_props CSV if available (sum of player PTS often equals team score in preseason box files)
        if finals is not None and not finals.empty and (finals['home_pts'].isna().all() or finals['visitor_pts'].isna().all()):
            try:
                # Try processed folder by default
                from .config import paths as _pp
                rppath = _pp.data_processed / f"recon_props_{target_date}.csv"
                if rppath.exists():
                    dfp = pd.read_csv(rppath)
                    # Build {team_abbr: total_pts}
                    pts_by_team = dfp.groupby('team_abbr', dropna=False)['pts'].sum(min_count=1).to_dict()
                    def fill_row(row):
                        htri = str(row.get('home_tri') or '').upper(); atri = str(row.get('away_tri') or '').upper()
                        row = row.copy()
                        if pd.isna(row.get('home_pts')) and htri in pts_by_team:
                            row['home_pts'] = float(pts_by_team[htri])
                        if pd.isna(row.get('visitor_pts')) and atri in pts_by_team:
                            row['visitor_pts'] = float(pts_by_team[atri])
                        return row
                    finals = finals.apply(fill_row, axis=1)
            except Exception:
                pass
    except Exception as e:
        console.print(f"Scoreboard fetch failed: {e}", style="red"); return
    if finals is None or finals.empty:
        console.print("No finals found for date; writing reconciliation with empty finals.", style="yellow")
        merged = preds.copy()
        # Ensure score columns exist with NaN
        merged["home_pts"] = pd.NA
        merged["visitor_pts"] = pd.NA
    else:
        merged = preds.merge(finals, on=["home_tri","away_tri"], how="left")
    # Compute errors
    merged["pred_margin"] = pd.to_numeric(merged.get("pred_margin"), errors="coerce")
    merged["pred_total"] = pd.to_numeric(merged.get("pred_total"), errors="coerce")
    merged["home_pts"] = pd.to_numeric(merged.get("home_pts"), errors="coerce")
    merged["visitor_pts"] = pd.to_numeric(merged.get("visitor_pts"), errors="coerce")
    merged["actual_margin"] = merged["home_pts"] - merged["visitor_pts"]
    merged["total_actual"] = merged[["home_pts","visitor_pts"]].sum(axis=1)
    merged["margin_error"] = merged["pred_margin"] - merged["actual_margin"]
    merged["total_error"] = merged["pred_total"] - merged["total_actual"]
    keep = [
        "date","home_team","visitor_team","home_tri","away_tri",
        "home_pts","visitor_pts","pred_margin","pred_total",
        "actual_margin","total_actual","margin_error","total_error"
    ]
    if "date" not in merged.columns:
        merged["date"] = str(target_date)
    out_df = merged[keep]
    out = _paths.data_processed / f"recon_games_{target_date}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out, index=False)
    console.print({"date": str(target_date), "rows": int(len(out_df)), "output": str(out)})


# ============================================================================
# IMPROVEMENT COMMANDS
# ============================================================================

@cli.command()
@click.option("--season", type=int, default=2025, help="NBA season year (e.g., 2025 for 2024-25)")
def fetch_advanced_stats(season: int):
    """Fetch pace, efficiency, and Four Factors from Basketball Reference."""
    console.rule("Fetch Advanced Stats")
    try:
        from .scrapers import BasketballReferenceScraper
        
        scraper = BasketballReferenceScraper()
        
        console.print(f"Fetching team stats for {season} season...")
        stats = scraper.get_team_stats(season)
        
        if stats.empty:
            console.print("No stats fetched", style="yellow")
            return
        
        # Save to processed directory
        output_path = paths.data_processed / f"team_advanced_stats_{season}.csv"
        stats.to_csv(output_path, index=False)
        
        console.print(f"[OK] Saved {len(stats)} teams to {output_path}", style="green")
        console.print(stats.head(10))
        
    except Exception as e:
        console.print(f"Error fetching advanced stats: {e}", style="red")


@cli.command()
def fetch_injuries():
    """Fetch current injury reports from ESPN."""
    console.rule("Fetch Injury Reports")
    try:
        from .scrapers import NBAInjuryDatabase
        
        db = NBAInjuryDatabase()
        console.print("Fetching injury reports from ESPN...")
        
        injuries = db.update_injuries()
        
        if injuries.empty:
            console.print("No injuries fetched", style="yellow")
            return
        
        console.print(f"[OK] Saved {len(injuries)} injury records", style="green")
        
        # Show summary by team
        summary = injuries.groupby(['team', 'status']).size().reset_index(name='count')
        console.print("\nInjury Summary:")
        console.print(summary)
        
    except Exception as e:
        console.print(f"Error fetching injuries: {e}", style="red")


@cli.command()
@click.option("--days", type=int, default=30, help="Number of days to analyze")
def performance_report(days: int):
    """Generate performance report for model predictions."""
    console.rule(f"Performance Report (Last {days} Days)")
    try:
        from .performance import PerformanceTracker
        
        tracker = PerformanceTracker()
        report = tracker.generate_performance_report(days_back=days)
        
        tracker.print_performance_summary(report)
        
    except Exception as e:
        console.print(f"Error generating performance report: {e}", style="red")


@cli.command()
@click.option("--confidence", type=float, default=0.55, help="Minimum confidence threshold (0.5-1.0)")
@click.option("--days", type=int, default=30, help="Number of days to analyze")
def calculate_roi(confidence: float, days: int):
    """Calculate ROI for betting strategy."""
    console.rule(f"ROI Analysis (Last {days} Days, Confidence ≥ {confidence})")
    try:
        from .performance import PerformanceTracker
        
        tracker = PerformanceTracker()
        df = tracker.load_predictions_and_results()
        
        if df.empty:
            console.print("No data available for ROI calculation", style="yellow")
            return
        
        # Calculate ROI for different bet types
        for bet_type in ['moneyline']:
            console.print(f"\n{bet_type.upper()} ROI:")
            roi = tracker.calculate_roi(df, bet_type=bet_type, confidence_threshold=confidence)
            
            console.print(f"  Total Bets: {roi['total_bets']}")
            console.print(f"  Win Rate: {roi['win_rate']:.1f}%")
            console.print(f"  Total Profit: ${roi['total_profit']:.2f}")
            console.print(f"  ROI: {roi['roi']:.1f}%", style="green" if roi['roi'] > 0 else "red")
        
    except Exception as e:
        console.print(f"Error calculating ROI: {e}", style="red")


@cli.command()
def run_all_improvements():
    """Run all improvement tasks: fetch stats, injuries, and generate performance report."""
    console.rule("[NPU] Running All Improvements")
    
    # 1. Fetch advanced stats
    console.print("\n[1/3] Fetching advanced statistics...", style="cyan")
    try:
        from .scrapers import BasketballReferenceScraper
        scraper = BasketballReferenceScraper()
        stats = scraper.get_team_stats(2025)
        if not stats.empty:
            output_path = paths.data_processed / "team_advanced_stats_2025.csv"
            stats.to_csv(output_path, index=False)
            console.print(f"[OK] Saved {len(stats)} teams to {output_path}", style="green")
        else:
            console.print("⚠️  No stats fetched", style="yellow")
    except Exception as e:
        console.print(f"[ERROR] Error fetching stats: {e}", style="red")
    
    # 2. Fetch injury data
    console.print("\n[2/3] Fetching injury reports...", style="cyan")
    try:
        from .scrapers import NBAInjuryDatabase
        db = NBAInjuryDatabase()
        injuries = db.update_injuries()
        if not injuries.empty:
            console.print(f"[OK] Saved {len(injuries)} injury records", style="green")
            summary = injuries.groupby(['team', 'status']).size().reset_index(name='count')
            console.print(summary)
        else:
            console.print("⚠️  No injuries fetched", style="yellow")
    except Exception as e:
        console.print(f"[ERROR] Error fetching injuries: {e}", style="red")
    
    # 3. Generate performance report
    console.print("\n[3/3] Generating performance report...", style="cyan")
    try:
        from .performance import PerformanceTracker
        tracker = PerformanceTracker()
        report = tracker.generate_performance_report(days_back=30)
        tracker.print_performance_summary(report)
    except Exception as e:
        console.print(f"[ERROR] Error generating report: {e}", style="red")
    
    console.print("\n[OK] All improvements completed!", style="green bold")


@cli.command("backtest-period-calibration")
@click.option("--start", "start_date", type=str, required=True, help="Start date YYYY-MM-DD")
@click.option("--end", "end_date", type=str, required=True, help="End date YYYY-MM-DD")
@click.option("--weights", type=str, default="0.0,0.3,0.6,0.8,1.0", show_default=True, help="Comma-separated blend weights to test (0=model only, 1=shares only)")
def backtest_period_calibration_cmd(start_date: str, end_date: str, weights: str):
    """Backtest quarter/half calibration over a date range and report MAE for totals and margins.

    For each date:
    - Generates predictions with periods using NPU models (uncalibrated first)
    - Applies calibration across a list of blend weights without re-running models
    - Compares to actual line scores from data/raw/games_nba_api.csv
    - Reports aggregate MAE per weight for: Q1..Q4 totals, H1/H2 totals, game total, and quarter margins
    """
    console.rule("Backtest Period Calibration")
    try:
        s = pd.to_datetime(start_date).date(); e = pd.to_datetime(end_date).date()
    except Exception:
        console.print("Invalid --start/--end (YYYY-MM-DD)", style="red"); return
    if e < s:
        console.print("--end must be >= --start", style="red"); return
    # Parse weight list
    try:
        w_list = [float(x.strip()) for x in weights.split(',') if x.strip()]
        w_list = [w for w in w_list if 0.0 <= w <= 1.0]
        assert len(w_list) > 0
    except Exception:
        console.print("--weights must be comma-separated floats in [0,1]", style="red"); return

    # Load raw games for actuals
    raw_csv = paths.data_raw / "games_nba_api.csv"
    if not raw_csv.exists():
        console.print(f"Raw games not found: {raw_csv}. Run fetch first.", style="red"); return
    raw = pd.read_csv(raw_csv)
    # Normalize columns
    rename_map = {}
    if 'date_est' in raw.columns:
        rename_map['date_est'] = 'date'
    if 'home_team_tri' in raw.columns:
        rename_map.update({'home_team_tri': 'home_team', 'visitor_team_tri': 'visitor_team'})
    if 'home_score' in raw.columns:
        rename_map['home_score'] = 'home_pts'
    if 'visitor_score' in raw.columns:
        rename_map['visitor_score'] = 'visitor_pts'
    if rename_map:
        raw = raw.rename(columns=rename_map)
    # Restrict to date range and required cols
    need_cols = [
        'date','home_team','visitor_team','home_pts','visitor_pts',
        'home_q1','home_q2','home_q3','home_q4','visitor_q1','visitor_q2','visitor_q3','visitor_q4'
    ]
    for c in need_cols:
        if c not in raw.columns:
            console.print(f"Raw games missing column: {c}", style="red"); return
    raw['date'] = pd.to_datetime(raw['date'], errors='coerce').dt.date
    raw = raw[(raw['date'] >= s) & (raw['date'] <= e)].copy()
    if raw.empty:
        console.print("No raw games in date range", style="yellow"); return

    # Normalize team names to tricodes for robust merging
    try:
        from .teams import to_tricode as _to_tri
        raw['home_tri'] = raw['home_team'].astype(str).map(lambda x: _to_tri(x))
        raw['visitor_tri'] = raw['visitor_team'].astype(str).map(lambda x: _to_tri(x))
    except Exception:
        raw['home_tri'] = raw['home_team'].astype(str).str.upper()
        raw['visitor_tri'] = raw['visitor_team'].astype(str).str.upper()

    # Build date list
    dates = sorted(raw['date'].unique())

    # Accumulators per weight
    metrics = {w: {
        'sum_abs_q_total': 0.0,
        'cnt_q_total': 0,
        'sum_abs_h_total': 0.0,
        'cnt_h_total': 0,
        'sum_abs_game_total': 0.0,
        'cnt_game_total': 0,
        'sum_abs_q_margin': 0.0,
        'cnt_q_margin': 0,
    } for w in w_list}

    from .games_npu import predict_games_npu
    from .period_calibration import calibrate_period_predictions, CalibrationConfig, load_or_build_team_period_shares

    # Baseline aggregators
    metrics_equal = {
        'sum_abs_q_total': 0.0,
        'cnt_q_total': 0,
        'sum_abs_h_total': 0.0,
        'cnt_h_total': 0,
        'sum_abs_game_total': 0.0,
        'cnt_game_total': 0,
        'sum_abs_q_margin': 0.0,
        'cnt_q_margin': 0,
    }
    metrics_league = {k: 0 if isinstance(v, int) else 0.0 for k, v in metrics_equal.items()}
    # Load league shares once for league-only baseline
    try:
        _team_shares_df, _league_share_vec = load_or_build_team_period_shares(force_recompute=False)
    except Exception:
        _team_shares_df, _league_share_vec = None, np.array([0.25,0.25,0.25,0.25], dtype=float)

    for d in track(dates, description="Backtesting", total=len(dates)):
        ds = pd.Timestamp(d).strftime('%Y-%m-%d')
        # Build features for this date using the same path as predict-games-npu
        features_path = paths.data_processed / "features.parquet"
        csv_fallback = paths.data_processed / "features.csv"
        features_df = None
        try:
            if features_path.exists():
                df_fe = pd.read_parquet(features_path)
                df_fe['date'] = pd.to_datetime(df_fe['date']).dt.date
                features_df = df_fe[df_fe['date'] == d]
            elif csv_fallback.exists():
                df_fe = pd.read_csv(csv_fallback)
                if 'date' in df_fe.columns:
                    df_fe['date'] = pd.to_datetime(df_fe['date']).dt.date
                    features_df = df_fe[df_fe['date'] == d]
        except Exception:
            features_df = None
        if features_df is None or features_df.empty:
            # Fallback: construct slate directly from raw for this date
            try:
                day_raw = raw[raw['date'] == d][['date','home_team','visitor_team']].dropna()
                if not day_raw.empty:
                    slate_df = day_raw.copy()
                    slate_df['home_pts'] = np.nan
                    slate_df['visitor_pts'] = np.nan
                    # Combine with raw history to let feature builder compute form/adv stats
                    games = pd.concat([raw.copy(), slate_df], ignore_index=True, sort=False)
                    games['date'] = pd.to_datetime(games['date'])
                    from .features_enhanced import build_features_enhanced
                    feats2 = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True, season=2025)
                    feats2['date'] = pd.to_datetime(feats2['date']).dt.date
                    features_df = feats2[feats2['date'] == d]
            except Exception:
                pass
        if features_df is None or features_df.empty:
            continue

        # Predict once (uncalibrated) to get period columns
        try:
            base_preds = predict_games_npu(features_df, include_periods=True, calibrate_periods=False)
        except Exception:
            continue

        # Join with actuals for that date
        act = raw[raw['date'] == d].copy()
        # Compute actual totals and margins per quarter and halves
        act["q1_total"] = act["home_q1"] + act["visitor_q1"]
        act["q2_total"] = act["home_q2"] + act["visitor_q2"]
        act["q3_total"] = act["home_q3"] + act["visitor_q3"]
        act["q4_total"] = act["home_q4"] + act["visitor_q4"]
        act["h1_total"] = act["q1_total"] + act["q2_total"]
        act["h2_total"] = act["q3_total"] + act["q4_total"]
        act["game_total"] = act["home_pts"] + act["visitor_pts"]
        act["q1_margin"] = act["home_q1"] - act["visitor_q1"]
        act["q2_margin"] = act["home_q2"] - act["visitor_q2"]
        act["q3_margin"] = act["home_q3"] - act["visitor_q3"]
        act["q4_margin"] = act["home_q4"] - act["visitor_q4"]

        # Merge keys (tricodes preferred)
        try:
            from .teams import to_tricode as _to_tri
            base_preds['home_tri'] = base_preds['home_team'].astype(str).map(lambda x: _to_tri(x))
            base_preds['visitor_tri'] = base_preds['visitor_team'].astype(str).map(lambda x: _to_tri(x))
            act['home_tri'] = act['home_team'].astype(str).map(lambda x: _to_tri(x))
            act['visitor_tri'] = act['visitor_team'].astype(str).map(lambda x: _to_tri(x))
        except Exception:
            base_preds['home_tri'] = base_preds['home_team'].astype(str).str.upper()
            base_preds['visitor_tri'] = base_preds['visitor_team'].astype(str).str.upper()
            act['home_tri'] = act['home_team'].astype(str).str.upper()
            act['visitor_tri'] = act['visitor_team'].astype(str).str.upper()
        merged_base = pd.merge(
            base_preds,
            act[[
                'home_tri','visitor_tri','q1_total','q2_total','q3_total','q4_total',
                'h1_total','h2_total','game_total','q1_margin','q2_margin','q3_margin','q4_margin']
            ],
            left_on=['home_tri','visitor_tri'], right_on=['home_tri','visitor_tri'], how='inner'
        )
        if merged_base.empty:
            continue

        # Evaluate each weight by applying calibration
        import numpy as _np
        for w in w_list:
            cfg = CalibrationConfig(totals_blend_weight=w)
            try:
                preds = calibrate_period_predictions(merged_base.copy(), cfg)
            except Exception:
                continue
            # Collect absolute errors with NaN-safe handling and proper counts
            # Quarter totals (stack all quarters)
            q_abs = []
            for i in (1,2,3,4):
                diff = _np.abs(preds.get(f"quarters_q{i}_total", _np.nan) - preds.get(f"q{i}_total", _np.nan))
                q_abs.append(diff.values)
            q_abs = _np.concatenate(q_abs, axis=0)
            if q_abs.size:
                q_mask = _np.isfinite(q_abs)
                metrics[w]['sum_abs_q_total'] += float(_np.nansum(q_abs[q_mask]))
                metrics[w]['cnt_q_total'] += int(q_mask.sum())

            # Halves totals
            h1 = _np.abs(preds.get("halves_h1_total", _np.nan) - preds.get("h1_total", _np.nan)).values
            h2 = _np.abs(preds.get("halves_h2_total", _np.nan) - preds.get("h2_total", _np.nan)).values
            for h_arr in (h1, h2):
                h_mask = _np.isfinite(h_arr)
                metrics[w]['sum_abs_h_total'] += float(_np.nansum(h_arr[h_mask]))
                metrics[w]['cnt_h_total'] += int(h_mask.sum())

            # Game totals (prefer 'totals' else 'pred_total')
            g_col = 'totals' if 'totals' in preds.columns else ('pred_total' if 'pred_total' in preds.columns else None)
            if g_col is not None:
                g_abs = _np.abs(preds[g_col].values - preds["game_total"].values)
                g_mask = _np.isfinite(g_abs)
                metrics[w]['sum_abs_game_total'] += float(_np.nansum(g_abs[g_mask]))
                metrics[w]['cnt_game_total'] += int(g_mask.sum())

            # Quarter margins
            qm_abs = []
            for i in (1,2,3,4):
                diff = _np.abs(preds.get(f"quarters_q{i}_margin", _np.nan) - preds.get(f"q{i}_margin", _np.nan))
                qm_abs.append(diff.values)
            qm_abs = _np.concatenate(qm_abs, axis=0)
            if qm_abs.size:
                qm_mask = _np.isfinite(qm_abs)
                metrics[w]['sum_abs_q_margin'] += float(_np.nansum(qm_abs[qm_mask]))
                metrics[w]['cnt_q_margin'] += int(qm_mask.sum())

        # Baseline A: Equal split of predicted game total (and uniform margin)
        try:
            preds_eq = merged_base.copy()
            g_col = 'totals' if 'totals' in preds_eq.columns else ('pred_total' if 'pred_total' in preds_eq.columns else None)
            if g_col is not None:
                g = preds_eq[g_col].astype(float)
                sp = preds_eq.get('spread_margin', 0.0).astype(float)
                for i in (1,2,3,4):
                    preds_eq[f'quarters_q{i}_total'] = g / 4.0
                    preds_eq[f'quarters_q{i}_margin'] = sp / 4.0
                preds_eq['halves_h1_total'] = preds_eq['quarters_q1_total'] + preds_eq['quarters_q2_total']
                preds_eq['halves_h2_total'] = preds_eq['quarters_q3_total'] + preds_eq['quarters_q4_total']
                preds_eq['halves_h1_margin'] = preds_eq['quarters_q1_margin'] + preds_eq['quarters_q2_margin']
                preds_eq['halves_h2_margin'] = preds_eq['quarters_q3_margin'] + preds_eq['quarters_q4_margin']
                # Accumulate
                q_abs = []
                for i in (1,2,3,4):
                    diff = _np.abs(preds_eq.get(f"quarters_q{i}_total", _np.nan) - preds_eq.get(f"q{i}_total", _np.nan))
                    q_abs.append(diff.values)
                q_abs = _np.concatenate(q_abs, axis=0)
                if q_abs.size:
                    q_mask = _np.isfinite(q_abs)
                    metrics_equal['sum_abs_q_total'] += float(_np.nansum(q_abs[q_mask]))
                    metrics_equal['cnt_q_total'] += int(q_mask.sum())
                for h_pred, h_act in (("halves_h1_total","h1_total"),("halves_h2_total","h2_total")):
                    arr = _np.abs(preds_eq.get(h_pred, _np.nan) - preds_eq.get(h_act, _np.nan)).values
                    m = _np.isfinite(arr)
                    metrics_equal['sum_abs_h_total'] += float(_np.nansum(arr[m]))
                    metrics_equal['cnt_h_total'] += int(m.sum())
                if g_col is not None and 'game_total' in preds_eq.columns:
                    g_abs = _np.abs(preds_eq[g_col].values - preds_eq['game_total'].values)
                    g_mask = _np.isfinite(g_abs)
                    metrics_equal['sum_abs_game_total'] += float(_np.nansum(g_abs[g_mask]))
                    metrics_equal['cnt_game_total'] += int(g_mask.sum())
                qm_abs = []
                for i in (1,2,3,4):
                    diff = _np.abs(preds_eq.get(f"quarters_q{i}_margin", _np.nan) - preds_eq.get(f"q{i}_margin", _np.nan))
                    qm_abs.append(diff.values)
                qm_abs = _np.concatenate(qm_abs, axis=0)
                if qm_abs.size:
                    qm_mask = _np.isfinite(qm_abs)
                    metrics_equal['sum_abs_q_margin'] += float(_np.nansum(qm_abs[qm_mask]))
                    metrics_equal['cnt_q_margin'] += int(qm_mask.sum())
        except Exception:
            pass

        # Baseline B: League-average shares split (and uniform margin)
        try:
            preds_league = merged_base.copy()
            g_col = 'totals' if 'totals' in preds_league.columns else ('pred_total' if 'pred_total' in preds_league.columns else None)
            if g_col is not None:
                g = preds_league[g_col].astype(float)
                sp = preds_league.get('spread_margin', 0.0).astype(float)
                # Apply constant league shares to every row
                for i, share in enumerate(_league_share_vec, start=1):
                    preds_league[f'quarters_q{i}_total'] = g * float(share)
                    preds_league[f'quarters_q{i}_margin'] = sp / 4.0
                preds_league['halves_h1_total'] = preds_league['quarters_q1_total'] + preds_league['quarters_q2_total']
                preds_league['halves_h2_total'] = preds_league['quarters_q3_total'] + preds_league['quarters_q4_total']
                preds_league['halves_h1_margin'] = preds_league['quarters_q1_margin'] + preds_league['quarters_q2_margin']
                preds_league['halves_h2_margin'] = preds_league['quarters_q3_margin'] + preds_league['quarters_q4_margin']
                # Accumulate
                q_abs = []
                for i in (1,2,3,4):
                    diff = _np.abs(preds_league.get(f"quarters_q{i}_total", _np.nan) - preds_league.get(f"q{i}_total", _np.nan))
                    q_abs.append(diff.values)
                q_abs = _np.concatenate(q_abs, axis=0)
                if q_abs.size:
                    q_mask = _np.isfinite(q_abs)
                    metrics_league['sum_abs_q_total'] += float(_np.nansum(q_abs[q_mask]))
                    metrics_league['cnt_q_total'] += int(q_mask.sum())
                for h_pred, h_act in (("halves_h1_total","h1_total"),("halves_h2_total","h2_total")):
                    arr = _np.abs(preds_league.get(h_pred, _np.nan) - preds_league.get(h_act, _np.nan)).values
                    m = _np.isfinite(arr)
                    metrics_league['sum_abs_h_total'] += float(_np.nansum(arr[m]))
                    metrics_league['cnt_h_total'] += int(m.sum())
                if g_col is not None and 'game_total' in preds_league.columns:
                    g_abs = _np.abs(preds_league[g_col].values - preds_league['game_total'].values)
                    g_mask = _np.isfinite(g_abs)
                    metrics_league['sum_abs_game_total'] += float(_np.nansum(g_abs[g_mask]))
                    metrics_league['cnt_game_total'] += int(g_mask.sum())
                qm_abs = []
                for i in (1,2,3,4):
                    diff = _np.abs(preds_league.get(f"quarters_q{i}_margin", _np.nan) - preds_league.get(f"q{i}_margin", _np.nan))
                    qm_abs.append(diff.values)
                qm_abs = _np.concatenate(qm_abs, axis=0)
                if qm_abs.size:
                    qm_mask = _np.isfinite(qm_abs)
                    metrics_league['sum_abs_q_margin'] += float(_np.nansum(qm_abs[qm_mask]))
                    metrics_league['cnt_q_margin'] += int(qm_mask.sum())
        except Exception:
            pass

    # Summarize
    rows = []
    for w in w_list:
        rows.append({
            'method': 'calibrated',
            'weight': w,
            'games': metrics[w]['cnt_game_total'],
            'MAE_quarters_total': round((metrics[w]['sum_abs_q_total'] / metrics[w]['cnt_q_total']) if metrics[w]['cnt_q_total'] else float('nan'), 3),
            'MAE_halves_total': round((metrics[w]['sum_abs_h_total'] / metrics[w]['cnt_h_total']) if metrics[w]['cnt_h_total'] else float('nan'), 3),
            'MAE_game_total': round((metrics[w]['sum_abs_game_total'] / metrics[w]['cnt_game_total']) if metrics[w]['cnt_game_total'] else float('nan'), 3),
            'MAE_quarters_margin': round((metrics[w]['sum_abs_q_margin'] / metrics[w]['cnt_q_margin']) if metrics[w]['cnt_q_margin'] else float('nan'), 3),
        })
    # Append baselines
    rows.append({
        'method': 'baseline_equal',
        'weight': None,
        'games': metrics_equal['cnt_game_total'],
        'MAE_quarters_total': round((metrics_equal['sum_abs_q_total'] / metrics_equal['cnt_q_total']) if metrics_equal['cnt_q_total'] else float('nan'), 3),
        'MAE_halves_total': round((metrics_equal['sum_abs_h_total'] / metrics_equal['cnt_h_total']) if metrics_equal['cnt_h_total'] else float('nan'), 3),
        'MAE_game_total': round((metrics_equal['sum_abs_game_total'] / metrics_equal['cnt_game_total']) if metrics_equal['cnt_game_total'] else float('nan'), 3),
        'MAE_quarters_margin': round((metrics_equal['sum_abs_q_margin'] / metrics_equal['cnt_q_margin']) if metrics_equal['cnt_q_margin'] else float('nan'), 3),
    })
    rows.append({
        'method': 'baseline_league',
        'weight': None,
        'games': metrics_league['cnt_game_total'],
        'MAE_quarters_total': round((metrics_league['sum_abs_q_total'] / metrics_league['cnt_q_total']) if metrics_league['cnt_q_total'] else float('nan'), 3),
        'MAE_halves_total': round((metrics_league['sum_abs_h_total'] / metrics_league['cnt_h_total']) if metrics_league['cnt_h_total'] else float('nan'), 3),
        'MAE_game_total': round((metrics_league['sum_abs_game_total'] / metrics_league['cnt_game_total']) if metrics_league['cnt_game_total'] else float('nan'), 3),
        'MAE_quarters_margin': round((metrics_league['sum_abs_q_margin'] / metrics_league['cnt_q_margin']) if metrics_league['cnt_q_margin'] else float('nan'), 3),
    })
    out_df = pd.DataFrame(rows).sort_values('MAE_quarters_total')
    console.print(out_df)
    # Save optional CSV summary
    try:
        p = paths.data_processed / f"backtest_period_calibration_{start_date}_to_{end_date}.csv"
        out_df.to_csv(p, index=False)
        console.print({"summary": str(p)})
    except Exception:
        pass


if __name__ == "__main__":
    cli()
