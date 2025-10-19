"""
Lightweight feature builder WITHOUT sklearn dependencies
Uses only pandas and numpy for feature engineering
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List

from .config import paths


def build_features_for_date_pure(date: str, player_logs_path: Path | None = None) -> pd.DataFrame:
    """
    Build per-player features up to the day BEFORE the given date (no leakage),
    WITHOUT sklearn dependencies and WITHOUT requiring same-day logs.

    Mirrors props_features.build_features_for_date behavior so ONNX predictions
    work for any future/past date even if player_logs lack rows on that day.

    Args:
        date: 'YYYY-MM-DD' target slate date
        player_logs_path: Optional explicit path to player_logs (.csv preferred)

    Returns:
        DataFrame with one row per player_id containing:
          - b2b
          - lag1_[pts|reb|ast|threes|min]
          - roll{3,5,10}_[pts|reb|ast|threes|min]
        Plus metadata columns: player_id, player_name (if available), team (if available), asof_date
    """
    # Prefer CSV to avoid parquet engines on ARM64
    if player_logs_path is None:
        csv_path = paths.data_processed / "player_logs.csv"
        pq_path = paths.data_processed / "player_logs.parquet"
        if csv_path.exists():
            player_logs_path = csv_path
        elif pq_path.exists():
            player_logs_path = pq_path
        else:
            raise FileNotFoundError("player_logs not found (tried .csv and .parquet)")

    # Load logs
    try:
        if str(player_logs_path).endswith('.parquet'):
            logs = pd.read_parquet(player_logs_path)
        else:
            logs = pd.read_csv(player_logs_path)
    except Exception as e:
        raise RuntimeError(f"Failed to read player_logs at {player_logs_path}: {e}")

    # Identify columns
    date_col = _find_col(logs, ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT", "date"])
    player_id_col = _find_col(logs, ["PLAYER_ID", "player_id", "idPlayer"])
    player_name_col = _find_col(logs, ["PLAYER_NAME", "player_name", "namePlayer"])
    team_col = _find_col(logs, ["TEAM_ABBREVIATION", "team", "slugTeam"])

    if date_col is None or player_id_col is None:
        raise ValueError("player_logs missing required date/player_id columns")

    # Normalize dates
    logs[date_col] = pd.to_datetime(logs[date_col])
    target_date = pd.to_datetime(date)

    # Use only history strictly before target_date
    hist = logs[logs[date_col] < target_date].copy()
    if hist.empty:
        # No history at all => return empty features (no predictions possible)
        return pd.DataFrame(columns=[
            'player_id', 'b2b',
            'lag1_pts','lag1_reb','lag1_ast','lag1_threes','lag1_min',
            'roll3_pts','roll3_reb','roll3_ast','roll3_threes','roll3_min',
            'roll5_pts','roll5_reb','roll5_ast','roll5_threes','roll5_min',
            'roll10_pts','roll10_reb','roll10_ast','roll10_threes','roll10_min',
            'player_name','team','asof_date'
        ])

    # Stat column detection
    def _min_to_float(v):
        try:
            if pd.isna(v):
                return 0.0
            s = str(v)
            if ":" in s:
                mm, ss = s.split(":", 1)
                return float(int(mm) + int(ss)/60.0)
            return float(s)
        except Exception:
            return 0.0

    col_pts = _find_col(hist, ['PTS','pts'])
    col_reb = _find_col(hist, ['REB','reb','TREB','treb'])
    col_ast = _find_col(hist, ['AST','ast'])
    col_3m  = _find_col(hist, ['FG3M','fg3m','FG3M_A'])
    col_min = _find_col(hist, ['MIN','min'])

    # Coerce numerics; convert minutes
    for col in [col_pts, col_reb, col_ast, col_3m]:
        if col and col in hist.columns:
            hist[col] = pd.to_numeric(hist[col], errors='coerce').fillna(0.0)
    if col_min and col_min in hist.columns:
        hist[col_min] = hist[col_min].apply(_min_to_float)
    else:
        hist['__min_fallback__'] = 0.0
        col_min = '__min_fallback__'

    # Sort for rolling
    hist.sort_values([player_id_col, date_col], inplace=True)

    # Build feature rows per player
    rows = []
    for pid, g in hist.groupby(player_id_col):
        g = g.copy()
        # Metadata from last row
        pname = g.iloc[-1][player_name_col] if player_name_col else None
        team = g.iloc[-1][team_col] if team_col else None

        # Back-to-back: compare last two games
        if len(g) >= 2:
            d1 = g[date_col].iloc[-1]
            d0 = g[date_col].iloc[-2]
            b2b = 1.0 if (d1 - d0).days == 1 else 0.0
        else:
            b2b = 0.0

        rec = {
            'player_id': pid,
            'player_name': pname,
            'team': team,
            'asof_date': target_date.date(),
            'b2b': b2b,
        }

        # Helper to pull lag1 and rolling
        def add_stats(stat_key: str, col_name: str):
            vals = g[col_name].values if col_name in g.columns else np.array([])
            rec[f'lag1_{stat_key}'] = float(vals[-1]) if len(vals) > 0 else 0.0
            for w in (3,5,10):
                window_vals = vals[-w:] if len(vals) >= w else vals
                rec[f'roll{w}_{stat_key}'] = float(np.mean(window_vals)) if len(window_vals) > 0 else 0.0

        # Add each stat
        add_stats('pts', col_pts) if col_pts else rec.update({f'lag1_pts':0.0, 'roll3_pts':0.0, 'roll5_pts':0.0, 'roll10_pts':0.0})
        add_stats('reb', col_reb) if col_reb else rec.update({f'lag1_reb':0.0, 'roll3_reb':0.0, 'roll5_reb':0.0, 'roll10_reb':0.0})
        add_stats('ast', col_ast) if col_ast else rec.update({f'lag1_ast':0.0, 'roll3_ast':0.0, 'roll5_ast':0.0, 'roll10_ast':0.0})
        add_stats('threes', col_3m) if col_3m else rec.update({f'lag1_threes':0.0, 'roll3_threes':0.0, 'roll5_threes':0.0, 'roll10_threes':0.0})
        add_stats('min', col_min) if col_min else rec.update({f'lag1_min':0.0, 'roll3_min':0.0, 'roll5_min':0.0, 'roll10_min':0.0})

        rows.append(rec)

    features = pd.DataFrame(rows)

    # Ensure expected ONNX feature columns exist
    expected = [
        'b2b',
        'lag1_pts','lag1_reb','lag1_ast','lag1_threes','lag1_min',
        'roll3_pts','roll3_reb','roll3_ast','roll3_threes','roll3_min',
        'roll5_pts','roll5_reb','roll5_ast','roll5_threes','roll5_min',
        'roll10_pts','roll10_reb','roll10_ast','roll10_threes','roll10_min'
    ]
    for col in expected:
        if col not in features.columns:
            features[col] = 0.0

    return features


def _find_col(df: pd.DataFrame, candidates: List[str]) -> str | None:
    """Find first matching column name (case-insensitive)"""
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols:
            return cols[k.lower()]
    return None


def _build_rolling_features(
    logs: pd.DataFrame,
    target_games: pd.DataFrame,
    date_col: str,
    player_id_col: str,
    windows: List[int] = [3, 5, 10]
) -> pd.DataFrame:
    # Deprecated: The pure builder now computes features from history without requiring target_games.
    # Keep function to avoid breaking imports if any, but delegate to new path.
    raise NotImplementedError("_build_rolling_features is deprecated; use build_features_for_date_pure")


def validate_features(features_df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that features DataFrame has all required columns
    
    Args:
        features_df: Features DataFrame to validate
        required_columns: List of required column names
    
    Returns:
        True if valid, raises ValueError if not
    """
    missing = [col for col in required_columns if col not in features_df.columns]
    
    if missing:
        raise ValueError(f"Missing {len(missing)} required columns: {missing[:10]}...")
    
    return True


if __name__ == "__main__":
    # Test the pure feature builder
    print("\n" + "="*60)
    print("Testing Pure Feature Builder (No sklearn)")
    print("="*60 + "\n")
    
    # Try to build features for today
    from datetime import date
    today = date.today().strftime('%Y-%m-%d')
    
    try:
        features = build_features_for_date_pure(today)
        
        if not features.empty:
            print(f"\n✅ Built features for {len(features)} players")
            print(f"Feature columns: {len([c for c in features.columns if '_L' in c])}")
            print(f"\nSample features:")
            print(features.head())
        else:
            print(f"⚠️  No games today ({today})")
            
    except FileNotFoundError as e:
        print(f"⚠️  {e}")
        print("Run 'fetch-player-logs' first to populate player_logs")
