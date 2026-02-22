"""Smoke test for enhanced game predictions.

Historically this file was used as a runnable script; pytest will import any
`test_*.py` file, so we keep it import-safe and convert it into a real test.
"""

from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest


sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_betting.features_enhanced import build_features_enhanced
from nba_betting.games_npu import NPUGamePredictor


def test_enhanced_predictions_smoke() -> None:
    games_path = Path("data/raw/games_nba_api.csv")
    if not games_path.exists():
        pytest.skip(f"Missing raw games file: {games_path}")

    games = pd.read_csv(games_path).tail(5)
    if games.empty:
        pytest.skip("No games available in games_nba_api.csv")

    # Build features using the enhanced pipeline.
    df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)
    if df.empty:
        pytest.skip("Enhanced feature builder returned empty dataframe")

    # Predictor loads feature columns from models dir; skip if models aren't present.
    try:
        predictor = NPUGamePredictor()
    except FileNotFoundError as exc:
        pytest.skip(f"Game models/feature columns not available: {exc}")

    feature_cols = predictor.feature_columns
    assert feature_cols, "Expected predictor.feature_columns to be populated"

    # Some rows may be missing due to joins; take the first valid row.
    X = df[feature_cols].values.astype(np.float32)
    assert X.shape[0] >= 1

    pred = predictor.predict_game(X[0:1], include_periods=True)
    assert isinstance(pred, dict)
    assert "win_prob" in pred
    assert "spread_margin" in pred
    assert ("totals" in pred) or ("total" in pred)
