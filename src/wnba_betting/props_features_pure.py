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
    from .props_features import build_features_for_date

    return build_features_for_date(date)


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
