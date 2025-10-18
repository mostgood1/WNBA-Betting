"""
Pure ONNX inference module - NO SKLEARN DEPENDENCIES
This module provides props predictions using only ONNX models and numpy.
Feature engineering is handled separately.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional
import time

try:
    import onnxruntime as ort
    ONNX_AVAILABLE = True
except ImportError:
    ONNX_AVAILABLE = False
    print("⚠️  onnxruntime not available")

from .config import paths

# Props targets
TARGETS = ["t_pts", "t_reb", "t_ast", "t_pra", "t_threes"]


class PureONNXPredictor:
    """Pure ONNX predictor with NO sklearn dependencies"""
    
    def __init__(self, models_dir: Path | str | None = None):
        if not ONNX_AVAILABLE:
            raise RuntimeError("onnxruntime not available - install onnxruntime-qnn")
        
        self.models_dir = Path(models_dir) if models_dir else paths.models
        self.sessions: Dict[str, ort.InferenceSession] = {}
        self.feature_columns: list[str] = []
        self.npu_available = True
        
        # Check for QNN provider
        available_providers = ort.get_available_providers()
        self.has_qnn = 'QNNExecutionProvider' in available_providers
        
        print(f"[ONNX Runtime initialized]")
        print(f"   Providers: {available_providers}")
        print(f"   NPU/QNN: {'[Available]' if self.has_qnn else '[Not available]'}")
        
        self._load_feature_columns()
        self._load_onnx_models()
    
    def _load_feature_columns(self):
        """Load feature columns WITHOUT sklearn - just read the pickled list"""
        import pickle
        
        feature_cols_path = self.models_dir / "props_feature_columns.joblib"
        
        if not feature_cols_path.exists():
            raise FileNotFoundError(f"Feature columns not found: {feature_cols_path}")
        
        # joblib files are just pickle files - read directly
        with open(feature_cols_path, 'rb') as f:
            self.feature_columns = pickle.load(f)
        
        print(f"[OK] Loaded {len(self.feature_columns)} feature columns")
    
    def _create_npu_session(self, model_path: Path) -> ort.InferenceSession:
        """Create ONNX inference session with NPU acceleration"""
        
        # Try QNN provider first, fall back to CPU
        providers = []
        if self.has_qnn:
            providers.append('QNNExecutionProvider')
        providers.append('CPUExecutionProvider')
        
        sess_options = ort.SessionOptions()
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        
        session = ort.InferenceSession(
            str(model_path),
            sess_options=sess_options,
            providers=providers
        )
        
        return session
    
    def _load_onnx_models(self):
        """Load all ONNX models"""
        print("[Loading ONNX models...]")
        
        loaded = 0
        for target in TARGETS:
            target_clean = target.replace("t_", "")
            onnx_file = self.models_dir / f"{target}_ridge.onnx"
            
            if not onnx_file.exists():
                raise FileNotFoundError(f"ONNX model not found: {onnx_file}")
            
            try:
                self.sessions[target] = self._create_npu_session(onnx_file)
                active_provider = self.sessions[target].get_providers()[0]
                npu_status = "[NPU]" if active_provider == "QNNExecutionProvider" else "[CPU]"
                print(f"[OK] {target_clean.upper():7s} model loaded ({npu_status})")
                loaded += 1
            except Exception as e:
                raise RuntimeError(f"Failed to load {target}: {e}")
        
        if loaded != len(TARGETS):
            raise RuntimeError(f"Only loaded {loaded}/{len(TARGETS)} models")
        
        print(f"[READY] All {loaded} ONNX models ready!")
    
    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """
        Make predictions using ONNX models
        
        Args:
            features_df: DataFrame with feature columns already computed
        
        Returns:
            DataFrame with prediction columns added (pred_pts, pred_reb, etc.)
        """
        if features_df.empty:
            return features_df.copy()
        
        # Validate feature columns
        missing_cols = [col for col in self.feature_columns if col not in features_df.columns]
        if missing_cols:
            raise ValueError(f"Missing feature columns: {missing_cols[:5]}... ({len(missing_cols)} total)")
        
        # Prepare features array
        X = features_df[self.feature_columns].fillna(0.0).values.astype(np.float32)
        
        # Copy input dataframe for results
        result_df = features_df.copy()
        
        total_inference_time = 0
        predictions_made = 0
        
        # Run inference for each target
        for target in TARGETS:
            pred_col = target.replace("t_", "pred_")
            
            try:
                session = self.sessions[target]
                input_name = session.get_inputs()[0].name
                
                start_time = time.perf_counter()
                predictions = session.run(None, {input_name: X})[0]
                inference_time = (time.perf_counter() - start_time) * 1000
                
                result_df[pred_col] = predictions.flatten()
                total_inference_time += inference_time
                predictions_made += 1
                
            except Exception as e:
                raise RuntimeError(f"Prediction failed for {target}: {e}")
        
        if predictions_made > 0:
            avg_time = total_inference_time / predictions_made
            provider = "NPU" if self.has_qnn else "CPU"
            print(f"[PERF] {provider} inference: {total_inference_time:.2f}ms total, {avg_time:.2f}ms avg per model")
        
        return result_df


def predict_props_pure_onnx(features_df: pd.DataFrame, models_dir: Path | None = None) -> pd.DataFrame:
    """
    Convenience function for pure ONNX predictions
    
    Args:
        features_df: DataFrame with features already built
        models_dir: Optional path to models directory
    
    Returns:
        DataFrame with predictions added
    """
    predictor = PureONNXPredictor(models_dir=models_dir)
    return predictor.predict(features_df)


if __name__ == "__main__":
    # Test the pure ONNX predictor
    print("\n" + "="*60)
    print("Testing Pure ONNX Predictor (No sklearn)")
    print("="*60 + "\n")
    
    # Create dummy features for testing
    predictor = PureONNXPredictor()
    
    # Create test data with correct number of features
    n_features = len(predictor.feature_columns)
    test_df = pd.DataFrame(
        np.random.randn(10, n_features),
        columns=predictor.feature_columns
    )
    
    print(f"\nTest input: {len(test_df)} rows x {len(predictor.feature_columns)} features")
    
    # Run prediction
    result = predictor.predict(test_df)
    
    print(f"\nResult columns: {[c for c in result.columns if c.startswith('pred_')]}")
    print(f"\nSample predictions:")
    pred_cols = [c for c in result.columns if c.startswith('pred_')]
    print(result[pred_cols].head())
    
    print("\n[OK] Pure ONNX predictor working!")
