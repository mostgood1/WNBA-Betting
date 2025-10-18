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

# Only import train when needed (requires sklearn)
try:
    from .train import train_models
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    train_models = None


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
        print("[INFO][LOADING]🚀 Loading NBA Game Models with NPU acceleration...")
        
        # Load feature columns
        feature_path = self.models_dir / "feature_columns.joblib"
        if feature_path.exists():
            self.feature_columns = joblib.load(feature_path)
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Loaded {len(self.feature_columns)} game features")
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
                    print(f"[WARN][ERROR][PERF][READY][OK]✅ {model_name.upper()} loaded with NPU acceleration")
                except Exception as e:
                    print(f"[WARN][ERROR][PERF][READY][OK]⚠️  {model_name.upper()} NPU failed, using CPU: {e}")
            
            # Load sklearn fallback (only if sklearn available)
            sklearn_path = self.models_dir / f"{model_name}.joblib"
            if sklearn_path.exists() and SKLEARN_AVAILABLE:
                try:
                    self.fallback_models[model_name] = joblib.load(sklearn_path)
                    if model_name not in self.npu_sessions:
                        print(f"[WARN][ERROR][PERF][READY][OK]✅ {model_name.upper()} loaded (CPU fallback)")
                except (ImportError, ModuleNotFoundError):
                    # Skip sklearn fallback if sklearn not available
                    pass
        
        # Load halves models (ONNX for NPU, fallback to joblib if needed)
        halves_onnx_count = 0
        halves_cpu_count = 0
        self.period_models["halves"] = {}
        for half in ["h1", "h2"]:
            self.period_models["halves"][half] = {}
            for model_type in ["win", "margin", "total"]:
                # Try ONNX first
                onnx_path = self.models_dir / f"halves_{half}_{model_type}.onnx"
                if onnx_path.exists() and self.npu_available:
                    try:
                        session = self._create_npu_session(str(onnx_path))
                        self.period_models["halves"][half][model_type] = ("onnx", session)
                        halves_onnx_count += 1
                    except Exception as e:
                        print(f"[WARN][ERROR][PERF][READY][OK]⚠️  {half}_{model_type} ONNX failed, trying sklearn: {e}")
                
                # Fallback to sklearn joblib if ONNX not available
                if model_type not in self.period_models["halves"][half]:
                    halves_joblib_path = self.models_dir / "halves_models.joblib"
                    if halves_joblib_path.exists() and SKLEARN_AVAILABLE:
                        try:
                            if "halves_joblib" not in self.__dict__:
                                self.halves_joblib = joblib.load(halves_joblib_path)
                            if half in self.halves_joblib and model_type in self.halves_joblib[half]:
                                self.period_models["halves"][half][model_type] = ("sklearn", self.halves_joblib[half][model_type])
                                halves_cpu_count += 1
                        except (ImportError, ModuleNotFoundError):
                            pass
        
        if halves_onnx_count > 0:
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Loaded halves models: {halves_onnx_count} NPU, {halves_cpu_count} CPU")
        elif halves_cpu_count > 0:
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Loaded halves models: {halves_cpu_count} CPU (ONNX not available)")
        
        # Load quarters models (ONNX for NPU, fallback to joblib if needed)
        quarters_onnx_count = 0
        quarters_cpu_count = 0
        self.period_models["quarters"] = {}
        for quarter in ["q1", "q2", "q3", "q4"]:
            self.period_models["quarters"][quarter] = {}
            for model_type in ["win", "margin", "total"]:
                # Try ONNX first
                onnx_path = self.models_dir / f"quarters_{quarter}_{model_type}.onnx"
                if onnx_path.exists() and self.npu_available:
                    try:
                        session = self._create_npu_session(str(onnx_path))
                        self.period_models["quarters"][quarter][model_type] = ("onnx", session)
                        quarters_onnx_count += 1
                    except Exception as e:
                        print(f"[WARN][ERROR][PERF][READY][OK]⚠️  {quarter}_{model_type} ONNX failed, trying sklearn: {e}")
                
                # Fallback to sklearn joblib if ONNX not available
                if model_type not in self.period_models["quarters"][quarter]:
                    quarters_joblib_path = self.models_dir / "quarters_models.joblib"
                    if quarters_joblib_path.exists() and SKLEARN_AVAILABLE:
                        try:
                            if "quarters_joblib" not in self.__dict__:
                                self.quarters_joblib = joblib.load(quarters_joblib_path)
                            if quarter in self.quarters_joblib and model_type in self.quarters_joblib[quarter]:
                                self.period_models["quarters"][quarter][model_type] = ("sklearn", self.quarters_joblib[quarter][model_type])
                                quarters_cpu_count += 1
                        except (ImportError, ModuleNotFoundError):
                            pass
        
        if quarters_onnx_count > 0:
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Loaded quarters models: {quarters_onnx_count} NPU, {quarters_cpu_count} CPU")
        elif quarters_cpu_count > 0:
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Loaded quarters models: {quarters_cpu_count} CPU (ONNX not available)")
        
        total_npu = len(self.npu_sessions) + halves_onnx_count + quarters_onnx_count
        total_cpu = len(self.fallback_models) + halves_cpu_count + quarters_cpu_count
        print(f"[WARN][ERROR][PERF][READY][OK]🎯 Ready with {total_npu + total_cpu} models ({total_npu} NPU-accelerated)")
    
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
                        
                    # Store raw win probability for analysis
                    predictions["win_prob_raw"] = predictions[model_name]
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
                    predictions["win_prob_raw"] = predictions[model_name]
                else:
                    predictions[model_name] = float(result[0])
            
            inference_times[model_name] = (time.perf_counter() - start_time) * 1000
        
        # ========================================================================
        # CALIBRATED WIN PROBABILITY: Use spread predictions via sigmoid
        # ========================================================================
        # The spread model is well-calibrated, but the win_prob model is overconfident.
        # Convert spread → win probability using logistic function:
        #   P(home wins) = 1 / (1 + exp(-spread / σ))
        # where σ ≈ 12 points (NBA historical spread standard deviation)
        #
        # This leverages the NN's spread predictions directly rather than
        # relying on the overconfident binary classifier.
        if "spread_margin" in predictions:
            sigma = 12.0  # Standard deviation of NBA point spreads
            spread_based_prob = 1.0 / (1.0 + np.exp(-predictions["spread_margin"] / sigma))
            predictions["win_prob_from_spread"] = float(spread_based_prob)
            
            # Blend: 80% spread-based (calibrated), 20% direct model
            # This preserves any signal from the win model while fixing overconfidence
            if "win_prob" in predictions:
                predictions["win_prob"] = 0.8 * spread_based_prob + 0.2 * predictions["win_prob"]
        
        # Period predictions (halves/quarters) using NPU ONNX or CPU fallback
        if include_periods:
            for period_type, models_dict in self.period_models.items():
                predictions[period_type] = {}
                for period_name, period_models in models_dict.items():
                    predictions[period_type][period_name] = {}
                    for pred_type, model_tuple in period_models.items():
                        model_type, model = model_tuple  # ("onnx", session) or ("sklearn", model)
                        
                        if model_type == "onnx":
                            # NPU ONNX prediction
                            input_name = model.get_inputs()[0].name
                            result = model.run(None, {input_name: features})[0]
                            
                            if pred_type == "win":
                                # Classification - get probability for class 1
                                result = np.array(result)  # Ensure numpy array
                                if result.ndim > 1 and result.shape[1] > 1:
                                    predictions[period_type][period_name][pred_type] = float(result[0][1])
                                elif result.ndim > 1:
                                    predictions[period_type][period_name][pred_type] = float(result[0][0])
                                else:
                                    predictions[period_type][period_name][pred_type] = float(result[0])
                            else:
                                # Regression - extract single value
                                result = np.array(result)  # Ensure numpy array
                                predictions[period_type][period_name][pred_type] = float(result.flat[0])
                        
                        elif model_type == "sklearn":
                            # sklearn fallback
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
        
        print(f"[WARN][ERROR][PERF][READY][OK]🔥 Benchmarking Game Models NPU vs CPU performance ({num_predictions} predictions)...")
        
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
    
    print("[INFO][LOADING]🏗️  Training game models for NPU conversion...")
    
    if retrain:
        # Load features and retrain models
        features_path = paths.data_processed / "features.parquet"
        if not features_path.exists():
            raise FileNotFoundError(f"Features not found: {features_path}")
        
        df = pd.read_parquet(features_path)
        print("[INFO][LOADING]📊 Retraining game models with latest data...")
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
    
    print(f"[WARN][ERROR][PERF][READY][OK]🔄 Converting {len(models_to_convert)} game models to ONNX...")
    
    # Convert each model to ONNX
    for model_name, model_path in models_to_convert.items():
        if not model_path.exists():
            print(f"[WARN][ERROR][PERF][READY][OK]⚠️  Skipping {model_name} - model file not found")
            continue
            
        print(f"[WARN][ERROR][PERF][READY][OK]Converting {model_name} model...")
        
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
            
            print(f"[WARN][ERROR][PERF][READY][OK]✅ Saved {model_name} ONNX model to {onnx_path}")
            
        except Exception as e:
            print(f"[WARN][ERROR][PERF][READY][OK]❌ Failed to convert {model_name}: {e}")
    
    print("[INFO][LOADING]🎯 NPU game model conversion complete!")


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
    
    print(f"[WARN][ERROR][PERF][READY][OK]⚡ NPU game predictions complete: {len(features_df)} games in {total_inference_time:.1f}ms")
    print(f"[WARN][ERROR][PERF][READY][OK]🚀 Average per game: {total_inference_time/len(features_df):.2f}ms")
    
    return pred_df


def benchmark_game_npu_performance(num_runs: int = 100, num_games: int = 100) -> Dict:
    """Benchmark NPU vs CPU performance for game predictions"""
    print(f"[WARN][ERROR][PERF][READY][OK]🔥 Benchmarking Game NPU performance with {num_runs} runs, {num_games} games each...")
    
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