"""
Integration test for pure ONNX game predictions
Tests the complete pipeline: features -> ONNX inference -> predictions
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import numpy as np
import pandas as pd
from nba_betting.games_onnx_pure import PureONNXGamePredictor


def _make_feature_row(feature_columns: list[str], overrides: dict) -> dict:
    row = {c: 0.0 for c in feature_columns}
    for k, v in overrides.items():
        if k in row:
            row[k] = float(v)
    return row

def test_pure_onnx_games():
    """Test pure ONNX game predictor with sample data."""
    print("=" * 70)
    print("PURE ONNX GAME PREDICTIONS - INTEGRATION TEST")
    print("=" * 70)
    
    # Initialize predictor
    models_dir = Path(__file__).parent / "models"
    print(f"\n📦 Loading ONNX models from: {models_dir}")
    try:
        predictor = PureONNXGamePredictor(models_dir)
    except (FileNotFoundError, ImportError) as exc:
        import pytest

        pytest.skip(f"Pure ONNX game models not available: {exc}")
    
    # Get model info
    info = predictor.get_model_info()
    print(f"\n✅ Models loaded:")
    print(f"   - Win probability: {info['win_model']['providers']}")
    print(f"   - Spread margin: {info['spread_model']['providers']}")
    print(f"   - Total points: {info['total_model']['providers']}")
    print(f"   - Features required: {info['num_features']}")
    
    # Create sample game features matching the model's expected feature columns.
    # Simulating opening night games:
    # Game 1: Thunder (home) vs Rockets (visitor)
    # Game 2: Lakers (home) vs Warriors (visitor)
    
    cols = list(getattr(predictor, "feature_columns", []) or [])
    if not cols:
        import pytest

        pytest.skip("Predictor did not expose feature_columns")

    sample_features = pd.DataFrame([
        _make_feature_row(
            cols,
            {
                "elo_diff": 45.0,
                "home_rest_days": 5.0,
                "visitor_rest_days": 5.0,
                "home_form_off_5": 116.5,
                "home_form_def_5": 108.2,
                "visitor_form_off_5": 113.8,
                "visitor_form_def_5": 111.4,
            },
        ),
        _make_feature_row(
            cols,
            {
                "elo_diff": -25.0,
                "home_rest_days": 5.0,
                "visitor_rest_days": 5.0,
                "home_form_off_5": 115.2,
                "home_form_def_5": 109.8,
                "visitor_form_off_5": 118.6,
                "visitor_form_def_5": 107.4,
            },
        ),
    ])
    
    games = [
        "Thunder vs Rockets (Opening Night)",
        "Lakers vs Warriors (Opening Night)"
    ]
    
    print(f"\n🏀 Testing with {len(sample_features)} games:")
    for i, game in enumerate(games):
        print(f"   Game {i+1}: {game}")
    
    # Run predictions
    print(f"\n⚡ Running ONNX inference...")
    import time
    start = time.perf_counter()
    predictions = predictor.predict(sample_features)
    end = time.perf_counter()
    
    inference_time_ms = (end - start) * 1000
    avg_time_ms = inference_time_ms / len(sample_features)
    
    print(f"✅ Inference complete!")
    print(f"   Total time: {inference_time_ms:.2f}ms")
    print(f"   Average per game: {avg_time_ms:.2f}ms")
    
    # Display predictions
    print(f"\n📊 PREDICTIONS:")
    print("=" * 70)
    
    for i, game in enumerate(games):
        pred = predictions.iloc[i]
        print(f"\n{game}")
        print(f"  Home Win Probability: {pred['home_win_prob']:.1%}")
        
        if pred['pred_margin'] > 0:
            print(f"  Predicted Spread: Home by {pred['pred_margin']:.1f}")
        else:
            print(f"  Predicted Spread: Visitor by {abs(pred['pred_margin']):.1f}")
        
        print(f"  Predicted Total: {pred['pred_total']:.1f} points")
        
        # Calculate implied line
        home_prob = pred['home_win_prob']
        if home_prob > 0.5:
            moneyline_home = -100 * home_prob / (1 - home_prob)
            moneyline_visitor = 100 * (1 - home_prob) / home_prob
        else:
            moneyline_home = 100 * (1 - home_prob) / home_prob
            moneyline_visitor = -100 * home_prob / (1 - home_prob)
        
        print(f"  Implied Moneyline: Home {moneyline_home:+.0f} / Visitor {moneyline_visitor:+.0f}")
    
    # Verify NPU usage
    print(f"\n🖥️  NPU Status:")
    if 'QNNExecutionProvider' in info['win_model']['providers']:
        print("   ✅ QNN ExecutionProvider ACTIVE (using Qualcomm NPU)")
    else:
        print("   ⚠️  CPU ExecutionProvider (NPU not active)")
    
    print(f"\n✅ Pure ONNX game prediction test PASSED!")
    print("=" * 70)
    
    assert len(predictions) == len(sample_features)


if __name__ == "__main__":
    test_pure_onnx_games()
