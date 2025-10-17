"""
Convert enhanced sklearn models (45 features) to ONNX format.
Updates all 26 models with enhanced feature set.
"""

import numpy as np
from pathlib import Path
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
import joblib
import onnx


def convert_enhanced_models():
    """Convert all enhanced sklearn models to ONNX format."""
    
    models_dir = Path("models")
    
    print("\n" + "="*70)
    print("CONVERTING ENHANCED MODELS TO ONNX")
    print("="*70)
    
    # Check if enhanced models exist
    enhanced_files = [
        "win_prob_enhanced.joblib",
        "spread_margin_enhanced.joblib",
        "totals_enhanced.joblib",
        "halves_models_enhanced.joblib",
        "quarters_models_enhanced.joblib",
        "feature_columns_enhanced.joblib",
    ]
    
    missing = [f for f in enhanced_files if not (models_dir / f).exists()]
    if missing:
        print(f"\nError: Missing enhanced model files:")
        for f in missing:
            print(f"   - {f}")
        print("\nRun training first: python -m nba_betting.train_enhanced")
        return
    
    # Load feature count
    feature_cols = joblib.load(models_dir / "feature_columns_enhanced.joblib")
    n_features = len(feature_cols)
    
    print(f"\nFeature count: {n_features}")
    print(f"First few features: {feature_cols[:5]}")
    
    # Define initial type for ONNX conversion
    initial_type = [('float_input', FloatTensorType([None, n_features]))]
    
    converted_count = 0
    
    # ========================================================================
    # 1. MAIN GAME MODELS
    # ========================================================================
    print("\n[1/4] Converting Main Game Models...")
    
    main_models = [
        ("win_prob_enhanced.joblib", "win_prob_enhanced.onnx"),
        ("spread_margin_enhanced.joblib", "spread_margin_enhanced.onnx"),
        ("totals_enhanced.joblib", "totals_enhanced.onnx"),
    ]
    
    for joblib_file, onnx_file in main_models:
        try:
            model = joblib.load(models_dir / joblib_file)
            onx = convert_sklearn(model, initial_types=initial_type, target_opset=13)
            
            output_path = models_dir / onnx_file
            with open(output_path, "wb") as f:
                f.write(onx.SerializeToString())
            
            file_size = output_path.stat().st_size
            print(f"   {onnx_file}: {file_size} bytes")
            converted_count += 1
            
        except Exception as e:
            print(f"   {onnx_file}: FAILED - {e}")
    
    # ========================================================================
    # 2. HALVES MODELS
    # ========================================================================
    print("\n[2/4] Converting Halves Models...")
    
    halves_models = joblib.load(models_dir / "halves_models_enhanced.joblib")
    
    for half, models in halves_models.items():
        for model_type, model in models.items():
            onnx_filename = f"halves_{half}_{model_type}_enhanced.onnx"
            try:
                onx = convert_sklearn(model, initial_types=initial_type, target_opset=13)
                
                output_path = models_dir / onnx_filename
                with open(output_path, "wb") as f:
                    f.write(onx.SerializeToString())
                
                file_size = output_path.stat().st_size
                print(f"   {onnx_filename}: {file_size} bytes")
                converted_count += 1
                
            except Exception as e:
                print(f"   {onnx_filename}: FAILED - {e}")
    
    # ========================================================================
    # 3. QUARTERS MODELS
    # ========================================================================
    print("\n[3/4] Converting Quarters Models...")
    
    quarters_models = joblib.load(models_dir / "quarters_models_enhanced.joblib")
    
    for quarter, models in quarters_models.items():
        for model_type, model in models.items():
            onnx_filename = f"quarters_{quarter}_{model_type}_enhanced.onnx"
            try:
                onx = convert_sklearn(model, initial_types=initial_type, target_opset=13)
                
                output_path = models_dir / onnx_filename
                with open(output_path, "wb") as f:
                    f.write(onx.SerializeToString())
                
                file_size = output_path.stat().st_size
                print(f"   {onnx_filename}: {file_size} bytes")
                converted_count += 1
                
            except Exception as e:
                print(f"   {onnx_filename}: FAILED - {e}")
    
    # ========================================================================
    # 4. COPY FEATURE COLUMNS
    # ========================================================================
    print("\n[4/4] Copying Feature Columns...")
    feature_cols_output = models_dir / "feature_columns_enhanced.joblib"
    print(f"   {feature_cols_output}: {len(feature_cols)} features")
    
    # ========================================================================
    # SUMMARY
    # ========================================================================
    print("\n" + "="*70)
    print("CONVERSION COMPLETE")
    print("="*70)
    print(f"\nSuccessfully converted: {converted_count}/21 models")
    print(f"   Main models: 3")
    print(f"   Halves models: 6 (H1-H2 × 3 types)")
    print(f"   Quarters models: 12 (Q1-Q4 × 3 types)")
    print(f"\nModels saved to: {models_dir.absolute()}")
    print("="*70 + "\n")


if __name__ == "__main__":
    convert_enhanced_models()
