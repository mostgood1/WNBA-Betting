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
import os
import time


def _is_windows_arm() -> bool:
    try:
        import platform
        return os.name == "nt" and ("arm" in platform.machine().lower() or "aarch64" in platform.machine().lower())
    except Exception:
        return False


class _SuppressStderrFD:
    def __enter__(self):
        if not _is_windows_arm():
            self._active = False
            return self
        import sys
        self._active = True
        self._fd = sys.stderr.fileno()
        self._saved = os.dup(self._fd)
        self._devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(self._devnull, self._fd)
        return self

    def __exit__(self, exc_type, exc, tb):
        if not getattr(self, "_active", False):
            return False
        try:
            os.dup2(self._saved, self._fd)
        finally:
            try:
                os.close(self._devnull)
            except Exception:
                pass
            try:
                os.close(self._saved)
            except Exception:
                pass
        return False

try:
    with _SuppressStderrFD():
        import onnxruntime as ort
    ONNX_AVAILABLE = True
except ImportError:
    ONNX_AVAILABLE = False
    print("⚠️  onnxruntime not available")

from .config import paths
from .props_linear import load_linear_props_models, predict_with_linear_models, train_linear_props_models

# Props targets
PRIMARY_ONNX_TARGETS = ["t_pts", "t_reb", "t_ast", "t_pra", "t_threes"]
# Additional targets we want predictions for; try ONNX else fall back to sklearn joblib
EXTRA_TARGETS = ["t_stl", "t_blk", "t_tov"]


class PureONNXPredictor:
    """Pure ONNX predictor with NO sklearn dependencies"""
    
    def __init__(self, models_dir: Path | str | None = None):
        if not ONNX_AVAILABLE:
            raise RuntimeError("onnxruntime not available - install onnxruntime-qnn")
        
        self.models_dir = Path(models_dir) if models_dir else paths.models
        self.sessions: Dict[str, ort.InferenceSession] = {}
        self.extra_models: Dict[str, object] = {}
        self.feature_columns: list[str] = []
        self.npu_available = True
        
        # Check for QNN provider
        with _SuppressStderrFD():
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
    
    def _setup_qnn_paths(self) -> None:
        """Add QNN SDK lib directories to DLL search path if present (Windows)."""
        qnn_roots = [
            os.environ.get("QNN_SDK"),
            os.environ.get("QNN_SDK_ROOT"),
            "C:/Qualcomm/QNN_SDK",
        ]
        subdirs = [
            "lib/aarch64-windows-msvc",
            "lib/arm64x-windows-msvc",
            "lib/x86_64-windows-msvc",
        ]
        for root in [p for p in qnn_roots if p]:
            for sd in subdirs:
                p = os.path.join(root, sd)
                if os.path.isdir(p):
                    try:
                        os.add_dll_directory(p)
                    except Exception:
                        pass

    def _resolve_qnn_backend(self) -> Optional[str]:
        """Return best-effort path to QNN backend DLL if available."""
        candidates = [
            os.environ.get("QNN_BACKEND_PATH"),
            "C:/Qualcomm/QNN_SDK/lib/aarch64-windows-msvc/QnnHtp.dll",
            "C:/Qualcomm/QNN_SDK/lib/arm64x-windows-msvc/QnnHtp.dll",
            "C:/Qualcomm/QNN_SDK/lib/x86_64-windows-msvc/QnnHtp.dll",
        ]
        for c in candidates:
            if c and os.path.exists(c):
                return c
        return None

    def _create_npu_session(self, model_path: Path) -> ort.InferenceSession:
        """Create ONNX inference session with NPU acceleration"""
        # Try QNN provider first, fall back to CPU
        providers = []
        provider_options = []
        if self.has_qnn:
            # Prepare QNN search paths and backend
            self._setup_qnn_paths()
            backend_path = self._resolve_qnn_backend()
            qnn_opts = {
                "target_device": "xelite",
                "runtime": "htp",
            }
            if backend_path:
                qnn_opts["backend_path"] = backend_path
            providers.append("QNNExecutionProvider")
            provider_options.append(qnn_opts)
        # Always include CPU fallback
        providers.append('CPUExecutionProvider')
        provider_options.append({})
        
        sess_options = ort.SessionOptions()
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        
        # Prefer provider_options when available (ORT supports pairing list of providers and options)
        try:
            with _SuppressStderrFD():
                session = ort.InferenceSession(
                str(model_path),
                sess_options=sess_options,
                providers=providers,
                provider_options=provider_options,
            )
        except TypeError:
            # Older ORT may not support provider_options kwarg; fall back
            with _SuppressStderrFD():
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
        # Load primary ONNX models (required)
        for target in PRIMARY_ONNX_TARGETS:
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
        
        if loaded != len(PRIMARY_ONNX_TARGETS):
            raise RuntimeError(f"Only loaded {loaded}/{len(PRIMARY_ONNX_TARGETS)} primary models")

        # Try to load extra targets as ONNX if available; otherwise prepare sklearn fallbacks
        import joblib as _joblib
        models_path = self.models_dir / "props_models.joblib"
        models_store = None
        if models_path.exists():
            try:
                models_store = _joblib.load(models_path)
            except Exception:
                models_store = None
        for target in EXTRA_TARGETS:
            onnx_file = self.models_dir / f"{target}_ridge.onnx"
            if onnx_file.exists():
                try:
                    self.sessions[target] = self._create_npu_session(onnx_file)
                    active_provider = self.sessions[target].get_providers()[0]
                    npu_status = "[NPU]" if active_provider == "QNNExecutionProvider" else "[CPU]"
                    print(f"[OK] {target.replace('t_','').upper():7s} model loaded ({npu_status})")
                    continue
                except Exception as e:
                    print(f"[WARN] ONNX load failed for {target}: {e}; will try sklearn fallback")
            # Fallback: sklearn model from joblib store
            if models_store is not None and target in models_store:
                self.extra_models[target] = models_store[target]
                print(f"[OK] {target.replace('t_','').upper():7s} model ready (sklearn fallback)")
            else:
                print(f"[WARN] No model available for {target}; predictions will be missing (will try pure-linear fallback)")

        # Attempt pure-linear fallback models if any extras still missing
        missing_extras = [t for t in EXTRA_TARGETS if t not in self.sessions and t not in self.extra_models]
        if missing_extras:
            # Try to load previously trained linear models; if missing, train now
            try:
                lin = load_linear_props_models()
            except FileNotFoundError:
                print("[INFO] Training pure-linear fallback models for extras...")
                try:
                    train_linear_props_models(targets=missing_extras, alpha=1.0)
                    lin = load_linear_props_models()
                except Exception as e:
                    lin = None
                    print(f"[WARN] Failed to train/load pure-linear models: {e}")
            if lin:
                # Store full linear model dict including feature_cols for detection at predict-time
                self.extra_models.update(lin)
        
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
        
        # Run inference for each primary ONNX target
        for target in PRIMARY_ONNX_TARGETS:
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
        
        # Run extra targets (ONNX if session present, else sklearn fallback, else pure-linear if available)
        for target in EXTRA_TARGETS:
            pred_col = target.replace("t_", "pred_")
            try:
                if target in self.sessions:
                    session = self.sessions[target]
                    input_name = session.get_inputs()[0].name
                    start_time = time.perf_counter()
                    predictions = session.run(None, {input_name: X})[0]
                    inference_time = (time.perf_counter() - start_time) * 1000
                    result_df[pred_col] = predictions.flatten()
                    total_inference_time += inference_time
                    predictions_made += 1
                elif target in self.extra_models and hasattr(self.extra_models[target], 'predict'):
                    model = self.extra_models[target]
                    # sklearn Ridge predict
                    result_df[pred_col] = model.predict(X)
                    predictions_made += 1
                elif target in self.extra_models:
                    # pure-linear fallback models are stored in a dict; run once per group after loop
                    pass
                else:
                    # leave missing
                    continue
            except Exception as e:
                raise RuntimeError(f"Prediction failed for {target}: {e}")

        # If pure-linear fallback exists, run for all in one shot
        # Detect by presence of 'feature_cols' key
        if 'feature_cols' in self.extra_models:
            try:
                result_df = predict_with_linear_models(result_df, self.extra_models)
                predictions_made += len([t for t in EXTRA_TARGETS if f"pred_{t[2:]}" in result_df.columns])
            except Exception as e:
                print(f"[WARN] Pure-linear fallback prediction failed: {e}")

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
