"""
🏀 NPU-Accelerated Game Projection Module for NBA-Betting
Optimizes win probability, spread, totals, halves, and quarters models with Qualcomm NPU
"""

from __future__ import annotations

import os
import time
import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
import warnings
warnings.filterwarnings('ignore')

try:
    import onnxruntime as ort
    ONNX_AVAILABLE = True
except ImportError:
    ONNX_AVAILABLE = False
    ort = None

from .config import paths
from .train import train_models


class NPUGamePredictor:
    """NPU-accelerated game predictor for win probability, spread, totals, halves, quarters"""
    
    def __init__(self, models_dir: Optional[Path] = None):
        """Initialize NPU game predictor with models from NBA-Betting pipeline"""
        self.models_dir = models_dir or paths.models
        self.npu_sessions = {}
        self.fallback_models = {}
        self.feature_columns = None
        self.npu_available = ONNX_AVAILABLE
        
        # Model types we'll convert to NPU
        self.model_types = {
            "win_prob": "classification",
            "spread_margin": "regression", 
            "totals": "regression"
        }
        
        # Halves and quarters models
        self.period_models = {}
        
        # Setup NPU if available
        if self.npu_available:
            self._setup_qnn_paths()
        
        # Load models and features
        self._load_models()
    
    def _setup_qnn_paths(self):
        """Setup QNN paths for NPU acceleration"""
        qnn_paths = [
            "C:/Qualcomm/QNN_SDK/lib/aarch64-windows-msvc",
            "C:/Qualcomm/QNN_SDK/lib/arm64x-windows-msvc", 
            "C:/Qualcomm/QNN_SDK/lib/x86_64-windows-msvc"
        ]
        
        for path in qnn_paths:
            if os.path.exists(path):
                try:
                    os.add_dll_directory(path)
                except OSError:
                    pass  # Directory may already be added
    
    def _create_npu_session(self, model_path: str):
        """Create optimized ONNX Runtime session for NPU"""
        
        # QNN Provider configuration for maximum NPU utilization
        qnn_options = {
            "backend_path": "C:/Qualcomm/QNN_SDK/lib/aarch64-windows-msvc/QnnHtp.dll",
            "target_device": "xelite",
            "runtime": "htp",
            "enable_htp_fp16_precision": "1",
            "htp_performance_mode": "sustained_high_performance",
            "htp_graph_finalization_optimization_mode": "3"
        }
        
        providers = [
            ("QNNExecutionProvider", qnn_options),
            "CPUExecutionProvider"  # Fallback
        ]
        
        # Session options for better performance  
        sess_options = ort.SessionOptions()
        sess_options.enable_profiling = False
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        sess_options.enable_mem_pattern = True
        sess_options.enable_cpu_mem_arena = True
        
        return ort.InferenceSession(model_path, sess_options=sess_options, providers=providers)
    
    def _load_models(self):
        """Load ONNX models and fallback sklearn models"""
        print("🚀 Loading NBA Game Models with NPU acceleration...")
        
        # Load feature columns
        feature_path = self.models_dir / "feature_columns.joblib"
        if feature_path.exists():
            self.feature_columns = joblib.load(feature_path)
            print(f"✅ Loaded {len(self.feature_columns)} game features")
        else:
            raise FileNotFoundError(f"Feature columns not found: {feature_path}")
        
        # Load main game models (win_prob, spread_margin, totals)
        for model_name, model_type in self.model_types.items():
            # Try to load ONNX version first
            onnx_path = self.models_dir / f"{model_name}.onnx"
            if onnx_path.exists() and self.npu_available:
                try:
                    session = self._create_npu_session(str(onnx_path))
                    self.npu_sessions[model_name] = session
                    print(f"✅ {model_name.upper()} loaded with NPU acceleration")
                except Exception as e:
                    print(f"⚠️  {model_name.upper()} NPU failed, using CPU: {e}")
            
            # Load sklearn fallback
            sklearn_path = self.models_dir / f"{model_name}.joblib"
            if sklearn_path.exists():
                self.fallback_models[model_name] = joblib.load(sklearn_path)
                if model_name not in self.npu_sessions:
                    print(f"✅ {model_name.upper()} loaded (CPU fallback)")
        
        # Load halves models
        halves_path = self.models_dir / "halves_models.joblib"
        if halves_path.exists():
            self.period_models["halves"] = joblib.load(halves_path)
            print(f"✅ Loaded halves models (h1, h2)")
        
        # Load quarters models  
        quarters_path = self.models_dir / "quarters_models.joblib"
        if quarters_path.exists():
            self.period_models["quarters"] = joblib.load(quarters_path)
            print(f"✅ Loaded quarters models (q1-q4)")
        
        total_npu = len(self.npu_sessions)
        total_cpu = len(self.fallback_models)
        print(f"🎯 Ready with {total_npu + total_cpu} models ({total_npu} NPU-accelerated)")
    
    def predict_game(self, features: np.ndarray, include_periods: bool = True) -> Dict[str, Any]:
        """Predict full game outcomes using NPU where available"""
        predictions = {}
        inference_times = {}
        
        # Main game predictions
        for model_name in self.model_types.keys():
            start_time = time.perf_counter()
            
            if model_name in self.npu_sessions:
                # NPU prediction
                session = self.npu_sessions[model_name]
                input_name = session.get_inputs()[0].name
                result = session.run(None, {input_name: features})[0]
                
                if model_name == "win_prob":
                    # Classification - handle different output shapes
                    if result.shape == (1,):
                        # Single output value
                        predictions[model_name] = float(result[0])
                    elif len(result.shape) > 1 and result.shape[1] > 1:
                        # Probability array - get probability of home team win
                        predictions[model_name] = float(result[0][1])
                    else:
                        # Single probability in 2D array
                        predictions[model_name] = float(result[0][0])
                else:
                    # Regression - handle 2D output
                    if len(result.shape) > 1:
                        predictions[model_name] = float(result[0][0])
                    else:
                        predictions[model_name] = float(result[0])
                    
            elif model_name in self.fallback_models:
                # CPU fallback
                model = self.fallback_models[model_name]
                result = model.predict(features)
                
                if model_name == "win_prob":
                    # Get probability from sklearn classifier
                    proba = model.predict_proba(features)
                    predictions[model_name] = float(proba[0][1])
                else:
                    predictions[model_name] = float(result[0])
            
            inference_times[model_name] = (time.perf_counter() - start_time) * 1000
        
        # Period predictions (halves/quarters) using CPU for now
        if include_periods:
            for period_type, models_dict in self.period_models.items():
                predictions[period_type] = {}
                for period_name, period_models in models_dict.items():
                    predictions[period_type][period_name] = {}
                    for pred_type, model in period_models.items():
                        if pred_type == "win":
                            proba = model.predict_proba(features)
                            predictions[period_type][period_name][pred_type] = float(proba[0][1])
                        else:
                            result = model.predict(features)
                            predictions[period_type][period_name][pred_type] = float(result[0])
        
        predictions["_inference_times_ms"] = inference_times
        return predictions
    
    def predict_batch(self, features_batch: np.ndarray, include_periods: bool = True) -> List[Dict[str, Any]]:
        """Batch prediction for multiple games"""
        results = []
        
        for i in range(features_batch.shape[0]):
            game_features = features_batch[i:i+1]  # Keep batch dimension
            prediction = self.predict_game(game_features, include_periods)
            results.append(prediction)
        
        return results
    
    def benchmark_npu_performance(self, num_predictions: int = 1000) -> Dict[str, Any]:
        """Benchmark NPU performance vs sklearn fallback for game models"""
        
        if not self.feature_columns:
            raise RuntimeError("Feature columns not loaded")
        
        print(f"🔥 Benchmarking Game Models NPU vs CPU performance ({num_predictions} predictions)...")
        
        # Create test features
        test_features = np.random.rand(1, len(self.feature_columns)).astype(np.float32)
        
        benchmark_results = {
            'npu_times': {},
            'cpu_times': {},
            'speedup': {},
            'npu_available_models': len(self.npu_sessions),
            'total_models': len(self.model_types)
        }
        
        for model_name in self.model_types.keys():
            
            # Benchmark NPU if available
            if model_name in self.npu_sessions:
                session = self.npu_sessions[model_name]
                input_name = session.get_inputs()[0].name
                
                # Warmup
                for _ in range(10):
                    session.run(None, {input_name: test_features})
                
                # Benchmark
                times = []
                for _ in range(num_predictions):
                    start_time = time.perf_counter()
                    session.run(None, {input_name: test_features})
                    end_time = time.perf_counter()
                    times.append((end_time - start_time) * 1000)
                
                benchmark_results['npu_times'][model_name] = {
                    'avg_ms': np.mean(times),
                    'min_ms': np.min(times),
                    'max_ms': np.max(times),
                    'throughput_per_sec': 1000 / np.mean(times)
                }
            
            # Benchmark CPU fallback if available
            if model_name in self.fallback_models:
                model = self.fallback_models[model_name]
                
                # Warmup
                for _ in range(10):
                    if model_name == "win_prob":
                        model.predict_proba(test_features)
                    else:
                        model.predict(test_features)
                
                # Benchmark
                times = []
                for _ in range(num_predictions):
                    start_time = time.perf_counter()
                    if model_name == "win_prob":
                        model.predict_proba(test_features)
                    else:
                        model.predict(test_features)
                    end_time = time.perf_counter()
                    times.append((end_time - start_time) * 1000)
                
                benchmark_results['cpu_times'][model_name] = {
                    'avg_ms': np.mean(times),
                    'min_ms': np.min(times),
                    'max_ms': np.max(times),
                    'throughput_per_sec': 1000 / np.mean(times)
                }
            
            # Calculate speedup
            if model_name in benchmark_results['npu_times'] and model_name in benchmark_results['cpu_times']:
                npu_time = benchmark_results['npu_times'][model_name]['avg_ms']
                cpu_time = benchmark_results['cpu_times'][model_name]['avg_ms']
                speedup_factor = cpu_time / npu_time
                benchmark_results['speedup'][model_name] = {
                    'speedup_factor': speedup_factor,
                    'npu_faster': speedup_factor > 1.0
                }
        
        return benchmark_results


def train_game_models_npu(retrain: bool = True):
    """Train game models and convert to ONNX for NPU acceleration"""
    if not ONNX_AVAILABLE:
        raise ImportError("ONNX Runtime not available. Install with: pip install onnxruntime")
    
    try:
        import skl2onnx
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
    except ImportError:
        raise ImportError("skl2onnx not available. Install with: pip install skl2onnx")
    
    print("🏗️  Training game models for NPU conversion...")
    
    if retrain:
        # Load features and retrain models
        features_path = paths.data_processed / "features.parquet"
        if not features_path.exists():
            raise FileNotFoundError(f"Features not found: {features_path}")
        
        df = pd.read_parquet(features_path)
        print("📊 Retraining game models with latest data...")
        train_models(df)
    
    # Load trained models and convert to ONNX
    feature_columns_path = paths.models / "feature_columns.joblib"
    if not feature_columns_path.exists():
        raise FileNotFoundError(f"Feature columns not found: {feature_columns_path}")
    
    feature_columns = joblib.load(feature_columns_path)
    
    # Models to convert
    models_to_convert = {
        "win_prob": paths.models / "win_prob.joblib",
        "spread_margin": paths.models / "spread_margin.joblib", 
        "totals": paths.models / "totals.joblib"
    }
    
    print(f"🔄 Converting {len(models_to_convert)} game models to ONNX...")
    
    # Convert each model to ONNX
    for model_name, model_path in models_to_convert.items():
        if not model_path.exists():
            print(f"⚠️  Skipping {model_name} - model file not found")
            continue
            
        print(f"Converting {model_name} model...")
        
        model = joblib.load(model_path)
        
        # Define the input type
        initial_type = [('float_input', FloatTensorType([None, len(feature_columns)]))]
        
        # Convert to ONNX
        try:
            onx = convert_sklearn(model, initial_types=initial_type)
            
            # Save ONNX model
            onnx_path = paths.models / f"{model_name}.onnx"
            with open(onnx_path, "wb") as f:
                f.write(onx.SerializeToString())
            
            print(f"✅ Saved {model_name} ONNX model to {onnx_path}")
            
        except Exception as e:
            print(f"❌ Failed to convert {model_name}: {e}")
    
    print("🎯 NPU game model conversion complete!")


def predict_games_npu(features_df: pd.DataFrame, include_periods: bool = True) -> pd.DataFrame:
    """Predict game outcomes using NPU-accelerated models"""
    predictor = NPUGamePredictor()
    
    # Prepare features
    X = features_df[predictor.feature_columns].values.astype(np.float32)
    
    # Get predictions for all games
    predictions = []
    total_inference_time = 0
    
    for i in range(X.shape[0]):
        game_features = X[i:i+1]
        pred_dict = predictor.predict_game(game_features, include_periods)
        
        # Extract main predictions and timing
        row_pred = {}
        for model_name in predictor.model_types.keys():
            if model_name in pred_dict:
                row_pred[model_name] = pred_dict[model_name]
        
        # Add period predictions if requested
        if include_periods:
            for period_type, period_data in pred_dict.items():
                if period_type.startswith("_") or period_type in predictor.model_types:
                    continue
                for period_name, period_preds in period_data.items():
                    for pred_type, value in period_preds.items():
                        col_name = f"{period_type}_{period_name}_{pred_type}"
                        row_pred[col_name] = value
        
        # Track inference times
        if "_inference_times_ms" in pred_dict:
            total_inference_time += sum(pred_dict["_inference_times_ms"].values())
        
        predictions.append(row_pred)
    
    # Convert to DataFrame
    pred_df = pd.DataFrame(predictions)
    
    # Add game info columns
    info_cols = [col for col in features_df.columns if col not in predictor.feature_columns]
    for col in info_cols:
        pred_df[col] = features_df[col].values
    
    # Reorder columns
    main_cols = list(predictor.model_types.keys())
    period_cols = [col for col in pred_df.columns if col not in main_cols and col not in info_cols]
    cols = info_cols + main_cols + period_cols
    pred_df = pred_df[cols]
    
    print(f"⚡ NPU game predictions complete: {len(features_df)} games in {total_inference_time:.1f}ms")
    print(f"🚀 Average per game: {total_inference_time/len(features_df):.2f}ms")
    
    return pred_df


def benchmark_game_npu_performance(num_runs: int = 100, num_games: int = 100) -> Dict:
    """Benchmark NPU vs CPU performance for game predictions"""
    print(f"🔥 Benchmarking Game NPU performance with {num_runs} runs, {num_games} games each...")
    
    try:
        predictor = NPUGamePredictor()
        
        # Use the existing benchmark method from the class
        results = predictor.benchmark_npu_performance(num_predictions=num_runs)
        
        # Add some additional info
        results["benchmark_config"] = {
            "num_runs": num_runs,
            "num_games": num_games,
            "npu_models": results.get('npu_available_models', 0),
            "total_models": results.get('total_models', 0)
        }
        
        return results
        
    except Exception as e:
        return {"error": str(e)}