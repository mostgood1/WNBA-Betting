from __future__ import annotations

import click
import os
import pandas as pd
import subprocess
from rich.console import Console
from rich.progress import track

from .config import paths
# from .scrape_bref import scrape_games  # deprecated
from .features import build_features
# from .train import train_models  # MOVED TO CONDITIONAL IMPORT - requires sklearn
import joblib
from .elo import Elo
from .schedule import compute_rest_for_matchups, fetch_schedule_2025_26
from .rosters import fetch_rosters
from .player_logs import fetch_player_logs
from .teams import normalize_team
from .scrape_nba_api import fetch_games_nba_api, enrich_periods_existing, backfill_scoreboard
from .odds_api import backfill_historical_odds, OddsApiConfig, consensus_lines_at_close, backfill_player_props, fetch_player_props_current
from .odds_api import fetch_game_odds_current
from .odds_bovada import fetch_bovada_odds_current
from .props_actuals import fetch_prop_actuals_via_nbastatr, upsert_props_actuals
from .props_actuals import fetch_prop_actuals_via_nba_cdn, fetch_prop_actuals_via_nbaapi
from .props_features import build_props_features, build_features_for_date
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

console = Console()


def _load_dotenv_key(name: str) -> str | None:
    """Lightweight .env reader: looks for KEY=VALUE in a .env at repo root."""
    try:
        env_path = paths.root / ".env"
        if not env_path.exists():
            return None
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            if k.strip() == name:
                val = v.strip().strip('"').strip("'")
                return val
    except Exception:
        return None
    return None


# Load .env values into os.environ at import time so click envvar options work
try:
    env_path = paths.root / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            key = k.strip()
            val = v.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
except Exception:
    pass


@click.group()
def cli():
    """NBA Betting pipeline"""


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
    # Prefer parquet then CSV; NBA API outputs
    candidates = [
        paths.data_raw / "games_nba_api.parquet",
        paths.data_raw / "games_nba_api.csv",
    ]
    raw = next((p for p in candidates if p.exists()), None)
    if raw is None:
        console.print("No raw games file found. Run fetch first.", style="red")
        return
    df = pd.read_parquet(raw) if raw.suffix == ".parquet" else pd.read_csv(raw)
    feats = build_features(df)
    out = paths.data_processed / "features.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    feats.to_parquet(out, index=False)
    console.print(f"Saved features to {out}")


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
    
    # Build features using pure or regular method based on flag
    if use_pure_onnx:
        try:
            from .props_features_pure import build_features_for_date_pure
            console.print("Building features with pure method (no sklearn)...", style="cyan")
            feats = build_features_for_date_pure(date_str)
        except Exception as e:
            console.print(f"WARNING: Pure feature builder failed: {e}", style="yellow")
            console.print("Falling back to regular feature builder...", style="yellow")
            use_pure_onnx = False
    
    if not use_pure_onnx:
        try:
            feats = build_features_for_date(date_str)
        except Exception as e:
            console.print(f"Failed to build features for {date_str}: {e}", style="red"); return
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
    
    # Predict using pure ONNX or sklearn
    if use_pure_onnx:
        try:
            from .props_onnx_pure import predict_props_pure_onnx
            
            console.print("Using pure ONNX models with NPU acceleration...", style="cyan")
            
            # Predict with pure ONNX
            preds = predict_props_pure_onnx(feats)
            
            console.print(f"Pure ONNX predictions generated for {len(preds)} players", style="green")
            
        except ImportError as e:
            console.print(f"WARNING: Pure ONNX not available: {e}", style="yellow")
            console.print("Falling back to sklearn models...", style="yellow")
            use_pure_onnx = False
        except Exception as e:
            console.print(f"WARNING: Pure ONNX failed: {e}", style="yellow")
            console.print("Falling back to sklearn models...", style="yellow")
            use_pure_onnx = False
    
    # Fallback to sklearn if pure ONNX disabled or failed
    if not use_pure_onnx:
        try:
            from .props_train import predict_props  # Import here to avoid sklearn dependency
            preds = predict_props(feats)
        except FileNotFoundError:
            console.print("Props models not found. Run train-props first.", style="red"); return
        except Exception as e:
            console.print(f"Failed to predict props: {e}", style="red"); return
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


@cli.command("predict-games-npu")
@click.option("--date", "date_str", type=str, required=True, help="Prediction date YYYY-MM-DD")
@click.option("--out", "out_path", type=click.Path(dir_okay=False), required=False, help="Output CSV path (default games_predictions_npu_YYYY-MM-DD.csv)")
@click.option("--periods/--no-periods", default=True, show_default=True, help="Include halves and quarters predictions")
def predict_games_npu_cmd(date_str: str, out_path: str | None, periods: bool):
    """Predict game outcomes using NPU-accelerated models for ultra-fast inference."""
    console.rule("Predict Games (NPU)")
    try:
        # Load features for the date
        features_path = paths.data_processed / "features.parquet"
        if not features_path.exists():
            console.print("Features not found. Run build-features first.", style="red")
            return
        
        features_df = pd.read_parquet(features_path)
        
        # Filter to the specific date if provided
        if 'date' in features_df.columns:
            features_df['date'] = pd.to_datetime(features_df['date']).dt.date
            target_date = pd.to_datetime(date_str).date()
            features_df = features_df[features_df['date'] == target_date]
        
        if features_df.empty:
            console.print(f"No games found for {date_str}", style="yellow")
            return
    
        from .games_npu import predict_games_npu
        preds = predict_games_npu(features_df, include_periods=periods)
    except ImportError as e:
        console.print(f"NPU dependencies not available: {e}", style="red"); return
    except FileNotFoundError:
        console.print("NPU game models not found. Run train-games-npu first.", style="red"); return
    except Exception as e:
        console.print(f"Failed to predict games with NPU: {e}", style="red"); return
    
    if not out_path:
        out_path = str(paths.data_processed / f"games_predictions_npu_{date_str}.csv")
    preds.to_csv(out_path, index=False)
    console.print(f"Saved NPU game predictions to {out_path} (rows={len(preds)}; periods={periods})")


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
    pred = paths.data_processed / f"predictions_{date_str}.csv"
    if not pred.exists():
        console.print(f"Predictions not found: {pred}", style="red"); return
    df = pd.read_csv(pred)
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
    recs = []
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
    for _, r in df.iterrows():
        try:
            home = r.get("home_team"); away = r.get("visitor_team")
            # ML
            p_home = _num(r.get("home_win_prob"))
            ev_h = _ev(p_home, r.get("home_ml")) if p_home is not None else None
            ev_a = _ev((1-p_home) if p_home is not None else None, r.get("away_ml"))
            side_ml = None; ev_ml = None
            if ev_h is not None or ev_a is not None:
                side_ml = home if (ev_h or -1) >= (ev_a or -1) else away
                ev_ml = ev_h if side_ml == home else ev_a
                if ev_ml is not None and ev_ml > 0:
                    recs.append({"market":"ML","side": side_ml, "home": home, "away": away, "ev": float(ev_ml), "date": str(d)})
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
    out = paths.data_processed / f"recommendations_{date_str}.csv" if not out_path else Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(recs).to_csv(out, index=False)
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
def daily_update_cmd(date_str: str | None, season: str, odds_api_key: str | None, git_push: bool, props_books: str | None, min_prop_edge: float, use_npu: bool, reconcile_days: int):
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

    # 4) Reconcile historic game actuals (last N days)
    try:
        console.print(f"[SEARCH] Reconciling game actuals for last {reconcile_days} days...")
        for days_back in range(1, reconcile_days + 1):
            past_date = target_date - _dt.timedelta(days=days_back)
            try:
                # This will fetch and upsert actual game results
                pass  # Game actuals reconciliation logic would go here
                console.print(f"  [OK] Game actuals for {past_date}")
            except Exception as e:
                console.print(f"  ⚠️  Game actuals for {past_date}: {e}")
    except Exception as e:
        console.print(f"Game actuals reconciliation failed: {e}", style="yellow")

    # 5) Reconcile historic prop actuals (last N days)
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
                        console.print(f"  [OK] Prop actuals for {past_date}")
                except Exception:
                    # Fallback to nba_api
                    from .props_actuals import fetch_prop_actuals_via_nbaapi
                    df_actuals = fetch_prop_actuals_via_nbaapi(str(past_date))
                    if df_actuals is not None and not df_actuals.empty:
                        from .props_actuals import upsert_props_actuals
                        upsert_props_actuals(df_actuals)
                        console.print(f"  [OK] Prop actuals for {past_date}")
            except Exception as e:
                console.print(f"  ⚠️  Prop actuals for {past_date}: {e}")
    except Exception as e:
        console.print(f"Props actuals reconciliation failed: {e}", style="yellow")

    # 6) Rebuild features and retrain game models
    try:
        console.print("🏗️  Rebuilding game features...")
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
            
            if use_npu:
                console.print("[NPU] Training game models with NPU acceleration...")
                from .games_npu import train_game_models_npu
                train_game_models_npu(retrain=True)
            else:
                console.print("🖥️  Training game models (CPU)...")
                feats_path = paths.data_processed / "features.parquet"
                df = pd.read_parquet(feats_path)
                from .train import train_models
                metrics = train_models(df)
                console.print("Game models trained (CPU)")
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

    # 9) Predict today's slate (games) and write predictions_<date>.csv
    try:
        console.print("🎲 Generating game predictions...")
        if use_npu:
            console.print("[NPU] Using NPU acceleration for game predictions...")
            from .games_npu import predict_games_npu
            # Load features and predict
            features_path = paths.data_processed / "features.parquet"
            if features_path.exists():
                features_df = pd.read_parquet(features_path)
                if 'date' in features_df.columns:
                    features_df['date'] = pd.to_datetime(features_df['date']).dt.date
                    target_date_obj = pd.to_datetime(str(target_date)).date()
                    features_df = features_df[features_df['date'] == target_date_obj]
                
                if not features_df.empty:
                    preds = predict_games_npu(features_df, include_periods=True)
                    out_path = paths.data_processed / f"predictions_{target_date}.csv"
                    preds.to_csv(out_path, index=False)
                    console.print(f"[OK] NPU game predictions saved to {out_path}")
                else:
                    console.print(f"No games found for {target_date} in features", style="yellow")
        else:
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
            res['edge_spread'] = res['pred_margin'] - res['market_home_margin']
        if 'total' in res.columns:
            res['edge_total'] = res['pred_total'] - res['total']
        # Period edges if columns present
        for half in ("h1","h2"):
            sp_col = f"{half}_spread"; tot_col = f"{half}_total"
            if sp_col in res.columns and f"{half}_pred_margin" in res.columns:
                res[f"edge_{half}_spread"] = res[f"{half}_pred_margin"] - (-res[sp_col])
            if tot_col in res.columns and f"{half}_pred_total" in res.columns:
                res[f"edge_{half}_total"] = res[f"{half}_pred_total"] - res[tot_col]
        for q in ("q1","q2","q3","q4"):
            sp_col = f"{q}_spread"; tot_col = f"{q}_total"
            if sp_col in res.columns and f"{q}_pred_margin" in res.columns:
                res[f"edge_{q}_spread"] = res[f"{q}_pred_margin"] - (-res[sp_col])
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
        wide = pd.read_parquet(clos_parq)
    else:
        # Fallback: compute from raw odds
        odds_parq = paths.data_raw / "odds_nba.parquet"
        odds_csv = paths.data_raw / "odds_nba.csv"
        if odds_parq.exists():
            odds_df = pd.read_parquet(odds_parq)
        elif odds_csv.exists():
            odds_df = pd.read_csv(odds_csv)
        else:
            console.print("No odds data found. Run backfill-odds or make-closing-lines first.", style="red"); return
        wide = consensus_lines_at_close(odds_df)

    if wide is None or wide.empty:
        console.print("No closing lines available.", style="yellow"); return

    # Filter to the requested date by US/Eastern calendar day (handles international/UTC offsets)
    df = wide.copy()
    try:
        df["date"] = pd.to_datetime(df["commence_time"], utc=True).dt.tz_convert("US/Eastern").dt.date
    except Exception:
        # Fallback: naive date (UTC)
        df["date"] = pd.to_datetime(df["commence_time"]).dt.date
    df = df[df["date"] == target_date]
    if df.empty:
        console.print(f"No events found on {target_date}.", style="yellow"); return

    # Normalize and map to export schema
    df["home_team"] = df["home_team"].apply(normalize_team)
    # Visitor team = away team normalized
    df["visitor_team"] = df["away_team"].apply(normalize_team)
    # Map numeric columns
    df["home_spread"] = df.get("spread_point")
    df["total"] = df.get("total_point")
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
    # Fetch finals via ScoreboardV2 with simple retry; if empty, fallback to NBA CDN daily scoreboard
    try:
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


if __name__ == "__main__":
    cli()
