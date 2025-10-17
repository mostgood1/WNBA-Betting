"""
Debug script to trace win probability predictions and find why we get 0% or 100%
"""
import pandas as pd
import numpy as np
import onnxruntime as ort
from pathlib import Path

# Load the ONNX model
model_path = Path("models/win_prob.onnx")
session = ort.InferenceSession(str(model_path), providers=['QNNExecutionProvider', 'CPUExecutionProvider'])

print("=" * 70)
print("DEBUGGING WIN PROBABILITY MODEL")
print("=" * 70)

# Load feature columns
import joblib
feature_cols = joblib.load("models/feature_columns.joblib")
print(f"\n✅ Loaded {len(feature_cols)} features")
print(f"Features: {feature_cols[:10]}...")

# Load historical data with features
hist_file = Path("data/processed/all_games_features_enhanced.csv")
if hist_file.exists():
    hist = pd.read_csv(hist_file)
    print(f"\n✅ Loaded {len(hist)} historical games")
    
    # Get recent games (last 10)
    recent = hist.tail(10).copy()
    
    # Extract features
    X = recent[feature_cols].fillna(0).astype(np.float32).values
    
    print(f"\nFeature matrix shape: {X.shape}")
    print(f"Feature value ranges:")
    print(f"  Min: {X.min():.2f}")
    print(f"  Max: {X.max():.2f}")
    print(f"  Mean: {X.mean():.2f}")
    print(f"  Std: {X.std():.2f}")
    
    # Check for extreme values
    print(f"\nExtreme values:")
    print(f"  Values > 100: {(X > 100).sum()}")
    print(f"  Values < -100: {(X < -100).sum()}")
    print(f"  NaN values: {np.isnan(X).sum()}")
    
    # Run through model
    input_name = session.get_inputs()[0].name
    outputs = session.run(None, {input_name: X})
    
    labels = outputs[0]
    probs = outputs[1]
    
    print(f"\n" + "=" * 70)
    print("MODEL PREDICTIONS ON RECENT GAMES")
    print("=" * 70)
    
    for i in range(len(recent)):
        row = recent.iloc[i]
        prob_home = probs[i][1]
        
        print(f"\n{row['date']}: {row['home_team']} vs {row['visitor_team']}")
        print(f"  Predicted: {prob_home:.4f} ({prob_home*100:.1f}%)")
        if 'home_pts' in row and 'visitor_pts' in row and pd.notna(row['home_pts']):
            actual_win = 1 if row['home_pts'] > row['visitor_pts'] else 0
            print(f"  Actual: {'HOME' if actual_win else 'AWAY'} ({row['home_pts']:.0f}-{row['visitor_pts']:.0f})")
        
        # Show top features for this game
        features_dict = {feature_cols[j]: X[i, j] for j in range(len(feature_cols))}
        sorted_features = sorted(features_dict.items(), key=lambda x: abs(x[1]), reverse=True)
        print(f"  Top 5 features by magnitude:")
        for feat, val in sorted_features[:5]:
            print(f"    {feat}: {val:.3f}")

else:
    print(f"\n❌ File not found: {hist_file}")
    print("Run 'python -m nba_betting.cli run-all-improvements' first")

print("\n" + "=" * 70)
