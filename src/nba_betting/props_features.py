from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Iterable, Dict, List

from .config import paths


NUM_COL_MAP = {
    # Core stats (existing)
    "PTS": ["PTS", "pts"],
    "REB": ["REB", "reb", "TREB", "treb"],
    "AST": ["AST", "ast"],
    "FG3M": ["FG3M", "fg3m", "FG3M_A"],
    "MIN": ["MIN", "min"],
    # Defensive stats
    "STL": ["STL", "stl"],
    "BLK": ["BLK", "blk"],
    "TOV": ["TOV", "tov"],
    # Shooting stats
    "FGM": ["FGM", "fgm"],
    "FGA": ["FGA", "fga"],
    "FG_PCT": ["FG_PCT", "fg_pct"],
    "FTM": ["FTM", "ftm"],
    "FTA": ["FTA", "fta"],
    "FT_PCT": ["FT_PCT", "ft_pct"],
    # Rebound breakdown
    "OREB": ["OREB", "oreb"],
    "DREB": ["DREB", "dreb"],
    # Other
    "PF": ["PF", "pf"],
    "PLUS_MINUS": ["PLUS_MINUS", "plus_minus"],
}

DATE_COLS = ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT"]
PLAYER_ID_COLS = ["PLAYER_ID", "player_id", "idPlayer"]
PLAYER_NAME_COLS = ["PLAYER_NAME", "player_name", "namePlayer"]
GAME_ID_COLS = ["GAME_ID", "game_id", "idGame"]
TEAM_COLS = ["TEAM_ABBREVIATION", "team", "slugTeam"]
MATCHUP_COL = "MATCHUP"


def _find_col(df: pd.DataFrame, candidates: Iterable[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols:
            return cols[k.lower()]
    return None


def _to_minutes(v) -> float | None:
    if pd.isna(v):
        return np.nan
    s = str(v)
    if s.isdigit():
        return float(s)
    if ":" in s:
        try:
            mm, ss = s.split(":", 1)
            return float(int(mm) + int(ss) / 60.0)
        except Exception:
            return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan


def load_player_logs() -> pd.DataFrame:
    # Prefer parquet then CSV
    p = paths.data_processed / "player_logs.parquet"
    c = paths.data_processed / "player_logs.csv"
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception as e:
            # Fallback to CSV if parquet engine isn't available
            if c.exists():
                return pd.read_csv(c)
            raise RuntimeError(f"Failed to read {p} and CSV fallback missing. Install pyarrow/fastparquet or provide player_logs.csv. Original error: {e}")
    if c.exists():
        return pd.read_csv(c)
    raise FileNotFoundError("player_logs not found; run fetch-player-logs")


def build_props_features(windows: List[int] = [3, 5, 10]) -> pd.DataFrame:
    """Compute per-player rolling features and targets from player logs.

    Outputs a row per player's game with lagged rolling stats up to previous game.
    Targets: pts, reb, ast, threes, pra, stl, blk, tov, fgm, fga, fg_pct, ftm, fta, ft_pct, oreb, dreb, pf, plus_minus
    """
    logs = load_player_logs().copy()
    # Identify columns
    pid = _find_col(logs, PLAYER_ID_COLS)
    pname = _find_col(logs, PLAYER_NAME_COLS)
    gid = _find_col(logs, GAME_ID_COLS)
    tcol = _find_col(logs, TEAM_COLS)
    dcol = _find_col(logs, DATE_COLS)
    
    # Core stats
    pts = _find_col(logs, NUM_COL_MAP["PTS"])
    reb = _find_col(logs, NUM_COL_MAP["REB"])
    ast = _find_col(logs, NUM_COL_MAP["AST"])
    fg3m = _find_col(logs, NUM_COL_MAP["FG3M"])
    minc = _find_col(logs, NUM_COL_MAP["MIN"])
    
    # Defensive stats
    stl = _find_col(logs, NUM_COL_MAP["STL"])
    blk = _find_col(logs, NUM_COL_MAP["BLK"])
    tov = _find_col(logs, NUM_COL_MAP["TOV"])
    
    # Shooting stats
    fgm = _find_col(logs, NUM_COL_MAP["FGM"])
    fga = _find_col(logs, NUM_COL_MAP["FGA"])
    fg_pct = _find_col(logs, NUM_COL_MAP["FG_PCT"])
    ftm = _find_col(logs, NUM_COL_MAP["FTM"])
    fta = _find_col(logs, NUM_COL_MAP["FTA"])
    ft_pct = _find_col(logs, NUM_COL_MAP["FT_PCT"])
    
    # Rebound breakdown
    oreb = _find_col(logs, NUM_COL_MAP["OREB"])
    dreb = _find_col(logs, NUM_COL_MAP["DREB"])
    
    # Other
    pf = _find_col(logs, NUM_COL_MAP["PF"])
    plus_minus = _find_col(logs, NUM_COL_MAP["PLUS_MINUS"])
    
    for col in [pid, pname, gid, tcol, dcol, pts, reb, ast, fg3m]:
        if col is None:
            raise ValueError("Missing required columns in player_logs")
    logs[dcol] = pd.to_datetime(logs[dcol])
    logs.sort_values([pid, dcol], inplace=True)
    
    # Numeric conversions for all stat columns
    stat_cols = [pts, reb, ast, fg3m, stl, blk, tov, fgm, fga, fg_pct, ftm, fta, ft_pct, oreb, dreb, pf, plus_minus]
    for c in stat_cols:
        if c is not None and c in logs.columns:
            logs[c] = pd.to_numeric(logs[c], errors="coerce")
    
    if minc is not None and minc in logs.columns:
        logs[minc] = logs[minc].apply(_to_minutes)
    else:
        logs[minc] = np.nan

    # Create combo stats
    logs["_PRA"] = logs[[pts, reb, ast]].sum(axis=1, skipna=True)
    if stl and blk:
        logs["_STOCKS"] = logs[[stl, blk]].sum(axis=1, skipna=True)  # Steals + Blocks
    if pts and reb:
        logs["_PR"] = logs[[pts, reb]].sum(axis=1, skipna=True)  # Points + Rebounds
    if pts and ast:
        logs["_PA"] = logs[[pts, ast]].sum(axis=1, skipna=True)  # Points + Assists
    if reb and ast:
        logs["_RA"] = logs[[reb, ast]].sum(axis=1, skipna=True)  # Rebounds + Assists

    feats = []
    # Group by player and compute rolling windows; then shift by 1 to avoid leakage
    grp = logs.groupby(pid, sort=False)
    base_cols = {"player_id": pid, "player_name": pname, "game_id": gid, "team": tcol, "date": dcol}
    
    # Define all stats to create rolling features for
    stat_map = {
        "pts": pts, "reb": reb, "ast": ast, "threes": fg3m,
        "stl": stl, "blk": blk, "tov": tov,
        "fgm": fgm, "fga": fga, "fg_pct": fg_pct,
        "ftm": ftm, "fta": fta, "ft_pct": ft_pct,
        "oreb": oreb, "dreb": dreb,
        "pf": pf, "plus_minus": plus_minus
    }
    
    for p, g in grp:
        g = g.copy()
        g["minutes"] = g[minc]
        
        # Rolling features for all stats
        for w in windows:
            g[f"roll{w}_min"] = g["minutes"].rolling(w, min_periods=1).mean().shift(1)
            for stat_name, stat_col in stat_map.items():
                if stat_col is not None and stat_col in g.columns:
                    g[f"roll{w}_{stat_name}"] = g[stat_col].rolling(w, min_periods=1).mean().shift(1)
        
        # Lag1 features for all stats
        g["lag1_min"] = g["minutes"].shift(1)
        for stat_name, stat_col in stat_map.items():
            if stat_col is not None and stat_col in g.columns:
                g[f"lag1_{stat_name}"] = g[stat_col].shift(1)
        
        # b2b indicator: played previous day
        g["b2b"] = (g[dcol].diff().dt.days == 1).shift(0).astype(float)
        
        # Targets for this game
        g["t_pts"] = g[pts]
        g["t_reb"] = g[reb]
        g["t_ast"] = g[ast]
        g["t_threes"] = g[fg3m]
        g["t_pra"] = g[[pts, reb, ast]].sum(axis=1, skipna=True)
        
        # Additional targets
        if stl and stl in g.columns:
            g["t_stl"] = g[stl]
        if blk and blk in g.columns:
            g["t_blk"] = g[blk]
        if tov and tov in g.columns:
            g["t_tov"] = g[tov]
        if fgm and fgm in g.columns:
            g["t_fgm"] = g[fgm]
        if fga and fga in g.columns:
            g["t_fga"] = g[fga]
        if fg_pct and fg_pct in g.columns:
            g["t_fg_pct"] = g[fg_pct]
        if ftm and ftm in g.columns:
            g["t_ftm"] = g[ftm]
        if fta and fta in g.columns:
            g["t_fta"] = g[fta]
        if ft_pct and ft_pct in g.columns:
            g["t_ft_pct"] = g[ft_pct]
        if oreb and oreb in g.columns:
            g["t_oreb"] = g[oreb]
        if dreb and dreb in g.columns:
            g["t_dreb"] = g[dreb]
        if pf and pf in g.columns:
            g["t_pf"] = g[pf]
        if plus_minus and plus_minus in g.columns:
            g["t_plus_minus"] = g[plus_minus]
        
        # Combo stat targets
        if "_STOCKS" in g.columns:
            g["t_stocks"] = g["_STOCKS"]
        if "_PR" in g.columns:
            g["t_pr"] = g["_PR"]
        if "_PA" in g.columns:
            g["t_pa"] = g["_PA"]
        if "_RA" in g.columns:
            g["t_ra"] = g["_RA"]
        
        # Build keep list dynamically
        keep = list(base_cols.values()) + ["b2b"]
        # Add all lag1 and rolling features that exist
        for col in g.columns:
            if col.startswith("lag1_") or col.startswith("roll"):
                keep.append(col)
        # Add all target columns
        for col in g.columns:
            if col.startswith("t_"):
                keep.append(col)
        
        keep = [c for c in keep if c in g.columns]  # Filter to existing columns
        feats.append(g[keep].rename(columns={
            base_cols["player_id"]: "player_id",
            base_cols["player_name"]: "player_name",
            base_cols["game_id"]: "game_id",
            base_cols["team"]: "team",
            base_cols["date"]: "date",
        }))
    out = pd.concat(feats, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.date
    # Save
    out_path = paths.data_processed / "props_features.parquet"
    try:
        out.to_parquet(out_path, index=False)
    except Exception as e:
        # Parquet engine missing on some ARM64 setups; continue with CSV snapshot only
        out_path = None
        # Best-effort: ensure there's at least a CSV artifact
        pass
    # Keep a dated CSV snapshot for reproducibility
    try:
        first_date = pd.to_datetime(out["date"].min()).date()
        last_date = pd.to_datetime(out["date"].max()).date()
        day_tag = f"{first_date}_{last_date}" if first_date != last_date else f"{first_date}"
        out_csv_dated = paths.data_processed / f"props_features_{day_tag}.csv"
        out.to_csv(out_csv_dated, index=False)
    except Exception:
        pass
    # Do not write a rolling convenience CSV to avoid overwrites
    return out


def build_features_for_date(date: str | pd.Timestamp, windows: List[int] = [3,5,10], players: List[int] | None = None) -> pd.DataFrame:
    """Build per-player features up to the day before the given date (no leakage).

    If players provided, filter to those player_ids; otherwise return all seen players.
    """
    logs = load_player_logs().copy()
    dcol = _find_col(logs, DATE_COLS)
    pid = _find_col(logs, PLAYER_ID_COLS)
    pname = _find_col(logs, PLAYER_NAME_COLS)
    tcol = _find_col(logs, TEAM_COLS)
    
    # Core stats
    pts = _find_col(logs, NUM_COL_MAP["PTS"])
    reb = _find_col(logs, NUM_COL_MAP["REB"])
    ast = _find_col(logs, NUM_COL_MAP["AST"])
    fg3m = _find_col(logs, NUM_COL_MAP["FG3M"])
    minc = _find_col(logs, NUM_COL_MAP["MIN"])
    
    # Defensive stats
    stl = _find_col(logs, NUM_COL_MAP["STL"])
    blk = _find_col(logs, NUM_COL_MAP["BLK"])
    tov = _find_col(logs, NUM_COL_MAP["TOV"])
    
    # Shooting stats
    fgm = _find_col(logs, NUM_COL_MAP["FGM"])
    fga = _find_col(logs, NUM_COL_MAP["FGA"])
    fg_pct = _find_col(logs, NUM_COL_MAP["FG_PCT"])
    ftm = _find_col(logs, NUM_COL_MAP["FTM"])
    fta = _find_col(logs, NUM_COL_MAP["FTA"])
    ft_pct = _find_col(logs, NUM_COL_MAP["FT_PCT"])
    
    # Rebound breakdown
    oreb = _find_col(logs, NUM_COL_MAP["OREB"])
    dreb = _find_col(logs, NUM_COL_MAP["DREB"])
    
    # Other
    pf = _find_col(logs, NUM_COL_MAP["PF"])
    plus_minus = _find_col(logs, NUM_COL_MAP["PLUS_MINUS"])
    
    for col in [pid, pname, tcol, dcol, pts, reb, ast, fg3m]:
        if col is None:
            raise ValueError("Missing required columns in player_logs")
    logs[dcol] = pd.to_datetime(logs[dcol])
    # Only prior to date
    target_date = pd.to_datetime(date)
    hist = logs[logs[dcol] < target_date].copy()
    if players is not None:
        hist = hist[hist[pid].isin(players)].copy()
    
    # Convert numerics for all stats
    stat_cols = [pts, reb, ast, fg3m, stl, blk, tov, fgm, fga, fg_pct, ftm, fta, ft_pct, oreb, dreb, pf, plus_minus]
    for c in stat_cols:
        if c is not None and c in hist.columns:
            hist[c] = pd.to_numeric(hist[c], errors="coerce")
    
    if minc is not None and minc in hist.columns:
        hist[minc] = hist[minc].apply(_to_minutes)
    else:
        hist[minc] = np.nan
    hist.sort_values([pid, dcol], inplace=True)
    
    # Define all stats to create features for
    stat_map = {
        "pts": pts, "reb": reb, "ast": ast, "threes": fg3m,
        "stl": stl, "blk": blk, "tov": tov,
        "fgm": fgm, "fga": fga, "fg_pct": fg_pct,
        "ftm": ftm, "fta": fta, "ft_pct": ft_pct,
        "oreb": oreb, "dreb": dreb,
        "pf": pf, "plus_minus": plus_minus
    }
    
    rows = []
    grp = hist.groupby(pid, sort=False)
    for p, g in grp:
        g = g.copy()
        g["minutes"] = g[minc]
        rec = {
            "player_id": p,
            "player_name": g.iloc[-1][_find_col(hist, PLAYER_NAME_COLS)] if _find_col(hist, PLAYER_NAME_COLS) else None,
            "team": g.iloc[-1][_find_col(hist, TEAM_COLS)] if _find_col(hist, TEAM_COLS) else None,
            "asof_date": target_date.date(),
        }
        
        # Lag1 features for all stats
        for stat_name, stat_col in stat_map.items():
            if stat_col is not None and stat_col in g.columns and len(g) > 0:
                rec[f"lag1_{stat_name}"] = g[stat_col].iloc[-1]
            else:
                rec[f"lag1_{stat_name}"] = np.nan
        rec["lag1_min"] = g["minutes"].iloc[-1] if len(g) > 0 else np.nan
        
        # b2b based on last two games
        if len(g) >= 2:
            d1 = g[dcol].iloc[-1]; d0 = g[dcol].iloc[-2]
            rec["b2b"] = float((d1 - d0).days == 1)
        else:
            rec["b2b"] = 0.0
        
        # Rolling features for all stats
        for w in windows:
            rec[f"roll{w}_min"] = g["minutes"].rolling(w, min_periods=1).mean().iloc[-1]
            for stat_name, stat_col in stat_map.items():
                if stat_col is not None and stat_col in g.columns:
                    rec[f"roll{w}_{stat_name}"] = g[stat_col].rolling(w, min_periods=1).mean().iloc[-1]
                else:
                    rec[f"roll{w}_{stat_name}"] = np.nan
        rows.append(rec)
    return pd.DataFrame(rows)
