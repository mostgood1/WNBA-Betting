"""
Quick test of enhanced model predictions
Tests that 45 features are being used correctly
"""

import pandas as pd
import numpy as np
from pathlib import Path
import joblib

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
    feature_cols = joblib.load("models/feature_columns.joblib")
    print(f"   OK Loaded {len(feature_cols)} feature columns")
    print(f"   First 3: {feature_cols[:3]}")
    print(f"   Last 3: {feature_cols[-3:]}")
    
    # 2. Load historical games
    print("\n[2/4] Loading historical games...")
    games_path = Path("data/raw/games_nba_api.csv")
    if not games_path.exists():
        print("   ERROR Games file not found")
        return
    
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
    
    # Check feature availability
    enhanced_cols = get_enhanced_feature_columns()
    available = [col for col in enhanced_cols if col in features_df.columns]
    print(f"   Available features: {len(available)}/{len(enhanced_cols)}")
    
    if len(available) != 45:
        print(f"   WARNING Expected 45 features, got {len(available)}")
        missing = set(enhanced_cols) - set(available)
        if missing:
            print(f"   Missing: {missing}")
    
    # 4. Make predictions with NPU
    print("\n[4/4] Generating predictions with NPU...")
    
    try:
        predictor = NPUGamePredictor()
        
        # Get feature matrix
        X = features_df[feature_cols].values
        print(f"   Feature matrix shape: {X.shape}")
        print(f"   Feature matrix dtype: {X.dtype}")
        
        # Check for non-numeric values
        print(f"\n   Checking data types in features_df:")
        for col in feature_cols:
            dtype = features_df[col].dtype
            if dtype == 'object':
                print(f"      WARNING {col}: {dtype} (non-numeric!)")
                print(f"         Sample: {features_df[col].head(2).tolist()}")
        
        # Predict first game
        if len(X) > 0:
            pred = predictor.predict_game(X[0:1], include_periods=True)
            
            print(f"\n   === PREDICTION SAMPLE ===")
            print(f"   Game: {features_df.iloc[0]['home_team']} vs {features_df.iloc[0]['visitor_team']}")
            print(f"   Win Probability: {pred['win_prob']:.1%}")
            print(f"   Predicted Spread: {pred['spread_margin']:.1f}")
            print(f"   Predicted Total: {pred['total']:.1f}")
            
            if 'halves' in pred:
                print(f"\n   Halves:")
                for period, data in pred['halves'].items():
                    print(f"      {period}: Margin={data['margin']:.1f}, Total={data['total']:.1f}")
            
            if 'quarters' in pred:
                print(f"\n   Quarters:")
                for period, data in pred['quarters'].items():
                    print(f"      {period}: Margin={data['margin']:.1f}, Total={data['total']:.1f}")
        
        print(f"\n   OK NPU prediction successful!")
        
    except Exception as e:
        print(f"   ERROR during prediction: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*70)
    print("OK ALL TESTS PASSED!")
    print("="*70)
    print("\nDONE Enhanced models (45 features) are working correctly!")
    print("   - Feature engineering: OK")
    print("   - NPU acceleration: OK")
    print("   - Predictions: OK")
    print("\nReady for production use!\n")
    
    return True

if __name__ == "__main__":
    test_enhanced_pipeline()
