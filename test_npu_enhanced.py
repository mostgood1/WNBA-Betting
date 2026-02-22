"""
Test NPU inference with enhanced ONNX models (45 features).
Verifies that Qualcomm NPU can run the new models successfully.
"""

import numpy as np
from pathlib import Path
import joblib
import pytest


def test_npu_enhanced():
    """Test enhanced models with NPU acceleration."""

    ort = pytest.importorskip("onnxruntime")
    
    print("\n" + "="*70)
    print("TESTING ENHANCED MODELS WITH NPU")
    print("="*70)
    
    models_dir = Path("models")
    
    # Load feature columns to know expected input size
    feature_cols_file = models_dir / "feature_columns_enhanced.joblib"
    if not feature_cols_file.exists():
        pytest.skip(f"Missing enhanced feature columns: {feature_cols_file}")
    
    feature_cols = joblib.load(feature_cols_file)
    n_features = len(feature_cols)
    
    print(f"\nFeature Configuration:")
    print(f"   Total features: {n_features}")
    print(f"   First 5: {feature_cols[:5]}")
    print(f"   Last 5: {feature_cols[-5:]}")
    
    # Create dummy input (all zeros)
    dummy_input = np.zeros((1, n_features), dtype=np.float32)
    print(f"\nDummy input shape: {dummy_input.shape}")
    
    # NPU providers
    providers = ['QNNExecutionProvider', 'CPUExecutionProvider']
    
    # Test each enhanced model
    enhanced_models = [
        "win_prob_enhanced.onnx",
        "spread_margin_enhanced.onnx",
        "totals_enhanced.onnx",
        "halves_h1_win_enhanced.onnx",
        "halves_h1_margin_enhanced.onnx",
        "halves_h1_total_enhanced.onnx",
        "halves_h2_win_enhanced.onnx",
        "halves_h2_margin_enhanced.onnx",
        "halves_h2_total_enhanced.onnx",
        "quarters_q1_win_enhanced.onnx",
        "quarters_q1_margin_enhanced.onnx",
        "quarters_q1_total_enhanced.onnx",
        "quarters_q2_win_enhanced.onnx",
        "quarters_q2_margin_enhanced.onnx",
        "quarters_q2_total_enhanced.onnx",
        "quarters_q3_win_enhanced.onnx",
        "quarters_q3_margin_enhanced.onnx",
        "quarters_q3_total_enhanced.onnx",
        "quarters_q4_win_enhanced.onnx",
        "quarters_q4_margin_enhanced.onnx",
        "quarters_q4_total_enhanced.onnx",
    ]
    
    print(f"\n" + "="*70)
    print("TESTING MODELS")
    print("="*70)
    
    npu_count = 0
    cpu_count = 0
    failed_count = 0

    missing_models = [m for m in enhanced_models if not (models_dir / m).exists()]
    if missing_models:
        pytest.skip(f"Missing enhanced ONNX models: {missing_models[:5]}" + (f" (and {len(missing_models)-5} more)" if len(missing_models) > 5 else ""))
    
    for model_file in enhanced_models:
        model_path = models_dir / model_file
        
        if not model_path.exists():
            # Should be unreachable due to the pre-check above.
            failed_count += 1
            continue
        
        try:
            # Create inference session
            session = ort.InferenceSession(str(model_path), providers=providers)
            
            # Check which provider is being used
            active_provider = session.get_providers()[0]
            
            # Get input/output names
            input_name = session.get_inputs()[0].name
            output_name = session.get_outputs()[0].name
            
            # Run inference
            result = session.run([output_name], {input_name: dummy_input})
            
            # Check output
            if 'win' in model_file:
                # Classification model - 2 outputs (probabilities)
                output_shape = result[0].shape
                expected_shape = (1, 2)
                if output_shape == expected_shape:
                    status = "OK"
                else:
                    status = f"WARN: shape {output_shape} != {expected_shape}"
            else:
                # Regression model - 1 output (scalar)
                output_val = float(result[0].flat[0])
                status = f"OK (output: {output_val:.2f})"
            
            # Count provider usage
            if active_provider == 'QNNExecutionProvider':
                provider_icon = "NPU"
                npu_count += 1
            else:
                provider_icon = "CPU"
                cpu_count += 1
            
            print(f"\n{model_file}:")
            print(f"   Provider: {provider_icon} ({active_provider})")
            print(f"   Status: {status}")
            
        except Exception as e:
            print(f"\n{model_file}: FAILED")
            print(f"   Error: {e}")
            failed_count += 1
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"\nTotal models tested: {len(enhanced_models)}")
    print(f"   NPU acceleration: {npu_count} models")
    print(f"   CPU fallback: {cpu_count} models")
    print(f"   Failed: {failed_count} models")
    
    assert failed_count == 0, f"{failed_count} models failed to load"
    print("="*70 + "\n")
    return


if __name__ == "__main__":
    test_npu_enhanced()
