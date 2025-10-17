"""
Convert quarters and halves sklearn models to ONNX format for NPU acceleration.

This script converts the period models (halves_models.joblib and quarters_models.joblib)
from sklearn format to ONNX, enabling pure NPU inference without sklearn dependency.
"""

import sys
from pathlib import Path
import joblib
import numpy as np

# Add src to path
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from nba_betting.config import paths

def convert_period_models_to_onnx():
    """Convert halves and quarters models to ONNX format"""
    
    print("🔄 Converting Period Models to ONNX for NPU Acceleration\n")
    
    try:
        import skl2onnx
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
    except ImportError:
        print("❌ ERROR: skl2onnx not installed")
        print("   Install with: pip install skl2onnx")
        print("   Note: This requires sklearn, but only for the conversion process")
        print("   After conversion, sklearn is NOT needed for inference!")
        return False
    
    models_dir = paths.models
    
    # Load feature columns to get input dimensions
    feature_cols_path = models_dir / "feature_columns.joblib"
    if not feature_cols_path.exists():
        print(f"❌ ERROR: Feature columns not found: {feature_cols_path}")
        return False
    
    feature_columns = joblib.load(feature_cols_path)
    n_features = len(feature_columns)
    print(f"✅ Loaded feature columns: {n_features} features\n")
    
    # Define initial type for ONNX conversion
    initial_type = [('float_input', FloatTensorType([None, n_features]))]
    
    success_count = 0
    total_count = 0
    
    # Convert halves models
    halves_path = models_dir / "halves_models.joblib"
    if halves_path.exists():
        print("📦 Loading halves models...")
        halves_models = joblib.load(halves_path)
        
        for period in ["h1", "h2"]:
            if period not in halves_models:
                print(f"   ⚠️  Period {period} not found in halves models")
                continue
            
            period_models = halves_models[period]
            
            for model_type in ["win", "margin", "total"]:
                if model_type not in period_models:
                    print(f"   ⚠️  Model type {model_type} not found for {period}")
                    continue
                
                total_count += 1
                model = period_models[model_type]
                output_path = models_dir / f"halves_{period}_{model_type}.onnx"
                
                try:
                    # Convert sklearn model to ONNX
                    onx = convert_sklearn(
                        model,
                        initial_types=initial_type,
                        target_opset=13,
                        options={'zipmap': False} if model_type == 'win' else None
                    )
                    
                    # Save ONNX model
                    with open(output_path, "wb") as f:
                        f.write(onx.SerializeToString())
                    
                    print(f"   ✅ {period.upper()} {model_type:6s} → {output_path.name}")
                    success_count += 1
                    
                except Exception as e:
                    print(f"   ❌ {period.upper()} {model_type:6s} FAILED: {e}")
    else:
        print(f"⚠️  Halves models not found: {halves_path}")
    
    print()  # Blank line
    
    # Convert quarters models
    quarters_path = models_dir / "quarters_models.joblib"
    if quarters_path.exists():
        print("📦 Loading quarters models...")
        quarters_models = joblib.load(quarters_path)
        
        for period in ["q1", "q2", "q3", "q4"]:
            if period not in quarters_models:
                print(f"   ⚠️  Period {period} not found in quarters models")
                continue
            
            period_models = quarters_models[period]
            
            for model_type in ["win", "margin", "total"]:
                if model_type not in period_models:
                    print(f"   ⚠️  Model type {model_type} not found for {period}")
                    continue
                
                total_count += 1
                model = period_models[model_type]
                output_path = models_dir / f"quarters_{period}_{model_type}.onnx"
                
                try:
                    # Convert sklearn model to ONNX
                    onx = convert_sklearn(
                        model,
                        initial_types=initial_type,
                        target_opset=13,
                        options={'zipmap': False} if model_type == 'win' else None
                    )
                    
                    # Save ONNX model
                    with open(output_path, "wb") as f:
                        f.write(onx.SerializeToString())
                    
                    print(f"   ✅ {period.upper()} {model_type:6s} → {output_path.name}")
                    success_count += 1
                    
                except Exception as e:
                    print(f"   ❌ {period.upper()} {model_type:6s} FAILED: {e}")
    else:
        print(f"⚠️  Quarters models not found: {quarters_path}")
    
    print(f"\n{'='*60}")
    print(f"✨ Conversion Complete: {success_count}/{total_count} models converted")
    print(f"{'='*60}")
    
    if success_count == total_count:
        print("\n✅ All period models successfully converted to ONNX!")
        print("   You can now run predictions WITHOUT sklearn installed.")
        print("   The system will use pure NPU acceleration.")
        return True
    elif success_count > 0:
        print(f"\n⚠️  Partial success: {success_count}/{total_count} models converted")
        return False
    else:
        print("\n❌ Conversion failed. Check errors above.")
        return False


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║  NBA Betting - Period Models ONNX Converter                 ║
║  Converts halves & quarters models for NPU acceleration     ║
╚══════════════════════════════════════════════════════════════╝
""")
    
    success = convert_period_models_to_onnx()
    
    if success:
        print("\n🚀 Next steps:")
        print("   1. The ONNX period models are now in models/")
        print("   2. Update games_npu.py to load these ONNX models")
        print("   3. Run predictions with pure NPU (no sklearn needed!)")
    
    sys.exit(0 if success else 1)
