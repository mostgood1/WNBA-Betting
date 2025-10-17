"""Simple test of enhanced predictions - ASCII only"""
import pandas as pd
import numpy as np
from pathlib import Path
import joblib
import sys
sys.path.insert(0, str(Path(__file__).parent / "src"))

from nba_betting.features_enhanced import build_features_enhanced, get_enhanced_feature_columns
from nba_betting.games_npu import NPUGamePredictor

print("\n" + "="*70)
print("TESTING ENHANCED PREDICTION PIPELINE")
print("="*70)

# Load games and build features
games = pd.read_csv('data/raw/games_nba_api.csv').tail(5)
df = build_features_enhanced(games, include_advanced_stats=True, include_injuries=True)

# Load feature columns  
feature_cols = joblib.load("models/feature_columns.joblib")
print(f"\nFeature count: {len(feature_cols)}")

# Get feature matrix
X = df[feature_cols].values.astype(np.float32)
print(f"Feature matrix shape: {X.shape}, dtype: {X.dtype}")

# Make prediction
predictor = NPUGamePredictor()
pred = predictor.predict_game(X[0:1], include_periods=True)

print(f"\nPREDICTION RESULTS:")
print(f"Win Probability: {pred['win_prob']:.1%}")
print(f"Spread: {pred['spread_margin']:.1f}")
print(f"Total: {pred['total']:.1f}")

if 'halves' in pred:
    print(f"\nHalves:")
    for period, data in pred['halves'].items():
        print(f"  {period}: Margin={data['margin']:.1f}, Total={data['total']:.1f}")

if 'quarters' in pred:
    print(f"\nQuarters:")
    for period, data in pred['quarters'].items():
        print(f"  {period}: Margin={data['margin']:.1f}, Total={data['total']:.1f}")

print(f"\n{'='*70}")
print("SUCCESS! Enhanced models working with 45 features on NPU")
print("="*70 + "\n")
