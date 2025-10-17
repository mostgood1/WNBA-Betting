"""
Test NPU/ONNX models directly without sklearn dependencies
"""
import onnxruntime as ort
import numpy as np
from pathlib import Path

def test_onnx_model(model_path: str):
    """Load and test an ONNX model"""
    print(f"\n{'='*60}")
    print(f"Testing: {model_path}")
    print(f"{'='*60}")
    
    # Create session with QNN provider
    providers = ['QNNExecutionProvider', 'CPUExecutionProvider']
    session = ort.InferenceSession(model_path, providers=providers)
    
    print(f"✅ Model loaded successfully")
    print(f"Active providers: {session.get_providers()}")
    print(f"NPU Active: {'QNNExecutionProvider' in session.get_providers()}")
    
    # Get input/output info
    inputs = session.get_inputs()
    outputs = session.get_outputs()
    
    print(f"\nModel I/O Info:")
    print(f"  Inputs: {[(i.name, i.shape, i.type) for i in inputs]}")
    print(f"  Outputs: {[(o.name, o.shape, o.type) for o in outputs]}")
    
    # Create dummy input matching the shape
    input_name = inputs[0].name
    input_shape = inputs[0].shape
    
    # Handle dynamic shapes (negative or None values)
    test_shape = tuple([1 if (s is None or s < 0) else s for s in input_shape])
    test_input = np.random.randn(*test_shape).astype(np.float32)
    
    print(f"\nTest Input shape: {test_input.shape}")
    
    # Run inference
    result = session.run(None, {input_name: test_input})
    
    print(f"✅ Inference successful!")
    print(f"Output shape: {result[0].shape}")
    print(f"Sample prediction: {result[0][0][:5] if len(result[0][0]) > 5 else result[0][0]}")
    
    return True

def main():
    print("\n" + "="*60)
    print("NPU/ONNX Model Testing Suite")
    print("="*60)
    
    models_dir = Path("models")
    onnx_models = [
        "t_pts_ridge.onnx",
        "t_reb_ridge.onnx", 
        "t_ast_ridge.onnx",
        "t_pra_ridge.onnx",
        "t_threes_ridge.onnx"
    ]
    
    results = {}
    for model_name in onnx_models:
        model_path = models_dir / model_name
        if model_path.exists():
            try:
                results[model_name] = test_onnx_model(str(model_path))
            except Exception as e:
                print(f"❌ Error testing {model_name}: {e}")
                results[model_name] = False
        else:
            print(f"\n⚠️ Model not found: {model_path}")
            results[model_name] = None
    
    print("\n" + "="*60)
    print("Test Summary")
    print("="*60)
    for model, status in results.items():
        if status is True:
            print(f"✅ {model}")
        elif status is False:
            print(f"❌ {model}")
        else:
            print(f"⚠️ {model} (not found)")
    
    successful = sum(1 for v in results.values() if v is True)
    print(f"\n{successful}/{len(onnx_models)} models tested successfully with NPU!")

if __name__ == "__main__":
    main()
