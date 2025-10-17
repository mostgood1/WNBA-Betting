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
    Build features for a specific date WITHOUT sklearn dependencies
    
    This replicates props_features.build_features_for_date but without any sklearn imports
    
    Args:
        date: Date string in format 'YYYY-MM-DD'
        player_logs_path: Optional path to player_logs file
    
    Returns:
        DataFrame with features ready for ONNX inference
    """
    if player_logs_path is None:
        # Try CSV first (works without pyarrow on ARM64 Windows)
        player_logs_path = paths.data_processed / "player_logs.csv"
        if not player_logs_path.exists():
            # Fallback to parquet
            player_logs_path = paths.data_processed / "player_logs.parquet"
            if not player_logs_path.exists():
                raise FileNotFoundError(f"player_logs not found (tried .csv and .parquet)")
    
    # Load player logs
    if str(player_logs_path).endswith('.parquet'):
        logs = pd.read_parquet(player_logs_path)
    else:
        logs = pd.read_csv(player_logs_path)
    
    # Identify columns (flexible column names)
    date_col = _find_col(logs, ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT", "date"])
    player_id_col = _find_col(logs, ["PLAYER_ID", "player_id", "idPlayer"])
    player_name_col = _find_col(logs, ["PLAYER_NAME", "player_name", "namePlayer"])
    team_col = _find_col(logs, ["TEAM_ABBREVIATION", "team", "slugTeam"])
    
    # Ensure date column exists
    if date_col is None:
        raise ValueError("No date column found in player logs")
    
    # Convert date column to datetime
    logs[date_col] = pd.to_datetime(logs[date_col])
    target_date = pd.to_datetime(date)
    
    # Get games for the target date
    games_today = logs[logs[date_col] == target_date].copy()
    
    if games_today.empty:
        print(f"⚠️  No games found for {date}")
        return pd.DataFrame()
    
    print(f"📅 Found {len(games_today)} player entries for {date}")
    
    # Build rolling features for these players
    features = _build_rolling_features(logs, games_today, date_col, player_id_col)
    
    # Add metadata columns
    if player_name_col:
        features['player_name'] = games_today[player_name_col].values
    if team_col:
        features['team'] = games_today[team_col].values
    
    features['date'] = date
    
    print(f"✅ Built features for {len(features)} players")
    
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
    """
    Build rolling average features WITHOUT sklearn - matches exact format expected by ONNX models
    
    Expected features (21 total):
    - b2b: back-to-back games flag
    - lag1_pts, lag1_reb, lag1_ast, lag1_threes, lag1_min: last game stats
    - roll3_pts, roll3_reb, roll3_ast, roll3_threes, roll3_min: 3-game averages
    - roll5_pts, roll5_reb, roll5_ast, roll5_threes, roll5_min: 5-game averages
    - roll10_pts, roll10_reb, roll10_ast, roll10_threes, roll10_min: 10-game averages
    """
    # Stat columns to aggregate
    stat_cols = {
        'pts': _find_col(logs, ['PTS', 'pts']),
        'reb': _find_col(logs, ['REB', 'reb', 'TREB', 'treb']),
        'ast': _find_col(logs, ['AST', 'ast']),
        'threes': _find_col(logs, ['FG3M', 'fg3m', 'FG3M_A']),
        'min': _find_col(logs, ['MIN', 'min']),
    }
    
    # Filter out None values
    stat_cols = {k: v for k, v in stat_cols.items() if v is not None}
    
    if not stat_cols:
        raise ValueError("No stat columns found in player logs")
    
    # Sort logs by player and date
    logs_sorted = logs.sort_values([player_id_col, date_col])
    
    features_list = []
    
    # Process each player in target games
    for idx, row in target_games.iterrows():
        player_id = row[player_id_col]
        game_date = row[date_col]
        
        # Get player's historical games before this date
        player_history = logs_sorted[
            (logs_sorted[player_id_col] == player_id) & 
            (logs_sorted[date_col] < game_date)
        ].copy()
        
        feature_dict = {'player_id': player_id}
        
        if player_history.empty:
            # No history - use zeros
            feature_dict['b2b'] = 0
            for stat_name in stat_cols.keys():
                feature_dict[f'lag1_{stat_name}'] = 0.0
                for window in windows:
                    feature_dict[f'roll{window}_{stat_name}'] = 0.0
            features_list.append(feature_dict)
            continue
        
        # Check for back-to-back games
        if len(player_history) > 0:
            last_game_date = player_history[date_col].iloc[-1]
            days_since = (game_date - last_game_date).days
            feature_dict['b2b'] = 1 if days_since == 1 else 0
        else:
            feature_dict['b2b'] = 0
        
        # Calculate lag1 (last game stats) and rolling averages
        for stat_name, col_name in stat_cols.items():
            stat_values = player_history[col_name].fillna(0).values
            
            # Lag1: last game
            if len(stat_values) > 0:
                feature_dict[f'lag1_{stat_name}'] = float(stat_values[-1])
            else:
                feature_dict[f'lag1_{stat_name}'] = 0.0
            
            # Rolling windows
            for window in windows:
                # Get last N games
                recent = stat_values[-window:] if len(stat_values) >= window else stat_values
                avg = np.mean(recent) if len(recent) > 0 else 0.0
                feature_dict[f'roll{window}_{stat_name}'] = avg
        
        features_list.append(feature_dict)
    
    features_df = pd.DataFrame(features_list)
    
    # Ensure all expected columns exist in correct order
    expected_features = [
        'b2b',
        'lag1_pts', 'lag1_reb', 'lag1_ast', 'lag1_threes', 'lag1_min',
        'roll3_pts', 'roll3_reb', 'roll3_ast', 'roll3_threes', 'roll3_min',
        'roll5_pts', 'roll5_reb', 'roll5_ast', 'roll5_threes', 'roll5_min',
        'roll10_pts', 'roll10_reb', 'roll10_ast', 'roll10_threes', 'roll10_min'
    ]
    
    for col in expected_features:
        if col not in features_df.columns:
            features_df[col] = 0.0
    
    return features_df


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
