from __future__ import annotations

import subprocess
import sys
import os
import shutil
from pathlib import Path
import pandas as pd
from .config import paths
from datetime import datetime as _dt
import time as _time
import requests

from .boxscores import _boxscore_from_espn, _espn_scoreboard, _espn_to_tri


def fetch_prop_actuals_via_nba_cdn(date: str) -> pd.DataFrame:
    """Fetch player actuals using ESPN's WNBA scoreboard and summary boxscores."""
    try:
        _ = _dt.strptime(date, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("Invalid date; expected YYYY-MM-DD")
    scoreboard = _espn_scoreboard(date)
    events = scoreboard.get("events") if isinstance(scoreboard, dict) else None
    if not isinstance(events, list) or not events:
        return pd.DataFrame()

    rows: list[dict] = []
    for event in events:
        comp = (((event or {}).get("competitions") or [None])[0]) or {}
        competitors = comp.get("competitors") or []
        home = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "home"), None)
        away = next((team for team in competitors if str((team or {}).get("homeAway") or "").strip().lower() == "away"), None)
        if not home or not away:
            continue
        gid = str((event or {}).get("id") or "").strip()
        home_tri = _espn_to_tri(str((((home or {}).get("team") or {}).get("abbreviation")) or "").strip())
        away_tri = _espn_to_tri(str((((away or {}).get("team") or {}).get("abbreviation")) or "").strip())
        box = _boxscore_from_espn(date_str=date, game_id=gid, home_tri=home_tri, away_tri=away_tri)
        if box is None or box.empty:
            continue
        for _, row in box.iterrows():
            pts = pd.to_numeric(row.get("PTS"), errors="coerce")
            reb = pd.to_numeric(row.get("REB"), errors="coerce")
            ast = pd.to_numeric(row.get("AST"), errors="coerce")
            threes = pd.to_numeric(row.get("FG3M"), errors="coerce")
            stl = pd.to_numeric(row.get("STL"), errors="coerce")
            blk = pd.to_numeric(row.get("BLK"), errors="coerce")
            tov = pd.to_numeric(row.get("TOV"), errors="coerce")
            vals = [pts, reb, ast, threes, stl, blk, tov]
            if all(pd.isna(v) or float(v) == 0.0 for v in vals):
                continue
            rows.append(
                {
                    "date": date,
                    "game_id": gid,
                    "player_id": pd.to_numeric(row.get("PLAYER_ID"), errors="coerce"),
                    "player_name": row.get("PLAYER_NAME"),
                    "team_abbr": row.get("TEAM_ABBREVIATION"),
                    "pts": None if pd.isna(pts) else float(pts),
                    "reb": None if pd.isna(reb) else float(reb),
                    "ast": None if pd.isna(ast) else float(ast),
                    "threes": None if pd.isna(threes) else float(threes),
                    "stl": None if pd.isna(stl) else float(stl),
                    "blk": None if pd.isna(blk) else float(blk),
                    "tov": None if pd.isna(tov) else float(tov),
                    "pra": float((0 if pd.isna(pts) else pts) + (0 if pd.isna(reb) else reb) + (0 if pd.isna(ast) else ast)),
                }
            )
    return pd.DataFrame(rows)


def fetch_prop_actuals_via_nbaapi(date: str) -> pd.DataFrame:
    """Compatibility wrapper for callers that still use the old nba_api name."""
    return fetch_prop_actuals_via_nba_cdn(date)


def ensure_rscript() -> str:
    """Return path to Rscript executable or raise a helpful error.

    Checks RSCRIPT_PATH env, then PATH via shutil.which for Rscript/Rscript.exe.
    """
    # Env override
    env_path = os.environ.get("RSCRIPT_PATH") or os.environ.get("RSCRIPT")
    if env_path and Path(env_path).exists():
        return str(env_path)
    # PATH lookup
    cand = shutil.which("Rscript.exe" if sys.platform == "win32" else "Rscript")
    if cand:
        return cand
    raise FileNotFoundError(
        "Rscript not found. Install R and ensure Rscript is on PATH, or set RSCRIPT_PATH env to the full path to Rscript.exe.\n"
        "Windows example path: C\\\Program Files\\R\\R-4.x.x\\bin\\Rscript.exe"
    )


def fetch_prop_actuals_via_nbastatr(date: str | None = None, start: str | None = None, end: str | None = None, out: Path | None = None, verbose: bool = True) -> pd.DataFrame:
    """Call the R script to fetch player actuals and return a DataFrame.

    Either provide date (YYYY-MM-DD) or start/end.
    """
    if (date is None) == (start is None or end is None):
        raise ValueError("Provide either date or both start and end")
    script = paths.root / "scripts" / "nbastatr_fetch_prop_actuals.R"
    if not script.exists():
        raise FileNotFoundError(f"R script not found at {script}")
    exe = ensure_rscript()
    args = [exe, str(script)]
    if date:
        args += ["--date", date]
    else:
        args += ["--start", start, "--end", end]
    tmp_out = out if out else (paths.data_processed / (f"props_actuals_{date}.csv" if date else f"props_actuals_{start}_{end}.csv"))
    args += ["--out", str(tmp_out)]
    # Run
    proc = subprocess.run(args, capture_output=True, text=True)
    if proc.returncode != 0:
        # If nbastatR missing, the script returns status 2 with message
        raise RuntimeError(f"Rscript failed (code {proc.returncode}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")
    # Load CSV if created
    if not Path(tmp_out).exists():
        return pd.DataFrame()
    df = pd.read_csv(tmp_out)
    if "team_abbr" not in df.columns and "team" in df.columns:
        df = df.rename(columns={"team": "team_abbr"})
    return df


def upsert_props_actuals(df: pd.DataFrame) -> Path:
    """Append/dedupe into a consolidated Parquet store and per-day CSV snapshots.

    Changes:
    - Stop writing a single props_actuals.csv (overwritten daily) to avoid git churn.
    - Write dated per-day CSVs: props_actuals_YYYY-MM-DD.csv (deduped by key per file).
    - Maintain props_actuals.parquet as a consolidated local store for analytics.
    """
    out_parq = paths.data_processed / "props_actuals.parquet"
    key_cols = ["date", "game_id", "player_id"]
    for c in key_cols:
        if c not in df.columns:
            raise ValueError(f"Missing required column {c}")
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    existing = None
    if out_parq.exists():
        try:
            existing = pd.read_parquet(out_parq)
        except Exception:
            existing = None
    if existing is not None and not existing.empty:
        # Deduplicate by key, prefer new rows
        existing["_key"] = existing[key_cols].astype(str).agg("|".join, axis=1)
        df["_key"] = df[key_cols].astype(str).agg("|".join, axis=1)
        keep_existing = existing[~existing["_key"].isin(df["_key"])]
        out = pd.concat([keep_existing.drop(columns=["_key"]), df.drop(columns=["_key"])], ignore_index=True)
    else:
        out = df
    # Update consolidated parquet (local analytics store)
    try:
        out.to_parquet(out_parq, index=False)
    except Exception:
        pass
    # Write per-day CSV snapshots (deduped)
    try:
        for d, g in out.groupby("date"):
            day_path = paths.data_processed / f"props_actuals_{d}.csv"
            if day_path.exists():
                try:
                    old = pd.read_csv(day_path)
                    old["date"] = pd.to_datetime(old["date"]).dt.date
                except Exception:
                    old = pd.DataFrame(columns=out.columns)
                # Merge and dedupe
                gg = pd.concat([old, g], ignore_index=True)
                gg["_key"] = gg[key_cols].astype(str).agg("|".join, axis=1)
                gg = gg.drop_duplicates(subset=["_key"]).drop(columns=["_key"]) if "_key" in gg.columns else gg
                gg.to_csv(day_path, index=False)
            else:
                g.to_csv(day_path, index=False)
    except Exception:
        # non-fatal
        pass
    return out_parq
