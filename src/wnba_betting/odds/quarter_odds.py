from __future__ import annotations

from typing import Dict, Optional
from pathlib import Path

import pandas as pd


def load_quarter_odds(date: str, processed_dir: Path) -> Optional[pd.DataFrame]:
    """Load quarter odds if available from processed artifacts.

    Expects a file like game_quarter_odds_<date>.csv with columns:
      home_team, visitor_team, q, home_ml, away_ml, home_spread, total, prices...
    Returns a DataFrame or None when missing.
    """
    try:
        fp = processed_dir / f"game_quarter_odds_{date}.csv"
        if fp.exists():
            return pd.read_csv(fp)
    except Exception:
        return None
    return None
