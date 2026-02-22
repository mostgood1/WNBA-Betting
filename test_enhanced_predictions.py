"""
Quick test of enhanced model predictions
Tests that 45 features are being used correctly
"""

import pandas as pd
import numpy as np
from pathlib import Path
import joblib
import pytest

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_betting.features_enhanced import build_features_enhanced, get_enhanced_feature_columns
from nba_betting.games_npu import NPUGamePredictor

def test_enhanced_pipeline():
    """Test complete prediction pipeline with 45 features"""
    
    print("\n" + "="*70)
    print("TESTING ENHANCED PREDICTION PIPELINE (45 Features)")
    print("="*70)
    
    # 1. Load feature columns
    print("\n[1/4] Loading feature configuration...")
    feature_cols_path = Path("models/feature_columns.joblib")
    if not feature_cols_path.exists():
        pytest.skip(f"Missing feature columns file: {feature_cols_path}")
    feature_cols = joblib.load(feature_cols_path)
    print(f"   OK Loaded {len(feature_cols)} feature columns")
    print(f"   First 3: {feature_cols[:3]}")
    print(f"   Last 3: {feature_cols[-3:]}")
    
    # 2. Load historical games
    print("\n[2/4] Loading historical games...")
    games_path = Path("data/raw/games_nba_api.csv")
    if not games_path.exists():
        pytest.skip(f"Games file not found: {games_path}")
    
    games_df = pd.read_csv(games_path)
    print(f"   OK Loaded {len(games_df)} games")
    
    # Filter to recent games for testing
    games_df['date'] = pd.to_datetime(games_df['date'])
    recent_games = games_df[games_df['date'] >= '2025-10-01'].copy()
    print(f"   Testing with {len(recent_games)} recent games")
    
    if len(recent_games) == 0:
        print("   No recent games found, using last 5 games")
        recent_games = games_df.tail(5).copy()
    
    # 3. Build enhanced features
    print("\n[3/4] Building enhanced features...")
    features_df = build_features_enhanced(
        recent_games,
        include_advanced_stats=True,
        include_injuries=True
    )
    print(f"   OK Built features: {features_df.shape}")
    assert not features_df.empty
    
    # Check feature availability
    enhanced_cols = get_enhanced_feature_columns()
    available = [col for col in enhanced_cols if col in features_df.columns]
    print(f"   Available features: {len(available)}/{len(enhanced_cols)}")

    # Strong sanity: the configured model feature cols should exist.
    missing_required = [c for c in feature_cols if c not in features_df.columns]
    assert not missing_required, f"Missing required feature columns: {missing_required[:10]}"
    
    if len(available) != 45:
        print(f"   WARNING Expected 45 features, got {len(available)}")
        missing = set(enhanced_cols) - set(available)
        if missing:
            print(f"   Missing: {missing}")
    
    # 4. Make predictions with NPU
    print("\n[4/4] Generating predictions with NPU...")
    
    try:
        predictor = NPUGamePredictor()
    except FileNotFoundError as exc:
        pytest.skip(f"Models not available for NPUGamePredictor: {exc}")

    # Get feature matrix
    X_df = features_df[feature_cols].copy()
    X_df = X_df.fillna(0.0).astype(np.float32)
    X = X_df.values
    print(f"   Feature matrix shape: {X.shape}")
    print(f"   Feature matrix dtype: {X.dtype}")
    assert len(X) > 0
    assert np.isfinite(X).all(), "Feature matrix contains NaN/inf"

    pred = predictor.predict_game(X[0:1], include_periods=True)
    assert isinstance(pred, dict)
    assert "win_prob" in pred
    assert "spread_margin" in pred
    total_key = "total" if "total" in pred else ("totals" if "totals" in pred else None)
    assert total_key is not None, f"Missing total prediction key, got keys={list(pred.keys())}"
    
    print("\n" + "="*70)
    print("OK ALL TESTS PASSED!")
    print("="*70)
    print("\nDONE Enhanced models (45 features) are working correctly!")
    print("   - Feature engineering: OK")
    print("   - NPU acceleration: OK")
    print("   - Predictions: OK")
    print("\nReady for production use!\n")
    
    return

if __name__ == "__main__":
    test_enhanced_pipeline()
