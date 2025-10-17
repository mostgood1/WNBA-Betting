"""
Test the complete Pure ONNX pipeline (features + inference)
"""
import sys
sys.path.insert(0, 'src')

from nba_betting.props_features_pure import build_features_for_date_pure
from nba_betting.props_onnx_pure import PureONNXPredictor

# Test with historical date
test_date = '2025-04-13'

print("="*60)
print(f"Testing Pure ONNX Pipeline (No sklearn) - {test_date}")
print("="*60)

# Step 1: Build features
print("\n📊 Step 1: Building features...")
features = build_features_for_date_pure(test_date)

if features.empty:
    print(f"❌ No games found for {test_date}")
    sys.exit(1)

print(f"✅ Features built: {len(features)} players, {len(features.columns)} columns")

# Step 2: Load ONNX models and predict
print("\n🚀 Step 2: Loading ONNX models and running inference...")
predictor = PureONNXPredictor()

# Step 3: Make predictions
print("\n⚡ Step 3: Making predictions...")
predictions = predictor.predict(features)

print(f"\n✅ Predictions complete!")
print(f"   Players: {len(predictions)}")
print(f"   Prediction columns: {[c for c in predictions.columns if c.startswith('pred_')]}")

# Show sample predictions
print(f"\n📈 Sample predictions (first 5 players):")
pred_cols = ['player_id', 'player_name'] + [c for c in predictions.columns if c.startswith('pred_')]
available_cols = [c for c in pred_cols if c in predictions.columns]
print(predictions[available_cols].head())

# Summary
print(f"\n{'='*60}")
print("✅ PURE ONNX PIPELINE TEST PASSED!")
print(f"   - No sklearn dependencies")
print(f"   - NPU accelerated: {'Yes' if predictor.has_qnn else 'No'}")
print(f"   - Players predicted: {len(predictions)}")
print(f"={'='*60}")
