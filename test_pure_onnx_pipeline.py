"""Test the complete Pure ONNX pipeline (features + inference).

This is intended as a smoke test. It skips (instead of failing) when
the local processed data/models required for the chosen date aren't present.
"""

from __future__ import annotations

import sys

import pytest


sys.path.insert(0, "src")

from nba_betting.props_features_pure import build_features_for_date_pure
from nba_betting.props_onnx_pure import PureONNXPredictor


def test_pure_onnx_pipeline_smoke() -> None:
    # Historical date (regular season end). If the local data store doesn't
    # contain this day, we skip rather than aborting the whole test run.
    test_date = "2025-04-13"

    features = build_features_for_date_pure(test_date)
    if getattr(features, "empty", True):
        pytest.skip(f"No games found for {test_date} in local dataset")

    try:
        predictor = PureONNXPredictor()
    except FileNotFoundError as exc:
        pytest.skip(f"Pure ONNX model files not available: {exc}")

    predictions = predictor.predict(features)
    assert len(predictions) == len(features)

    pred_cols = [c for c in predictions.columns if c.startswith("pred_")]
    assert pred_cols, "Expected at least one prediction column (pred_*)"
