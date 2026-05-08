"""
NPU-Accelerated Game Projection Module for WNBA-Betting
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
    ort = None

from .config import paths
from .league import LEAGUE

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
        """Initialize NPU game predictor with models from WNBA-Betting pipeline"""
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
        
        with _SuppressStderrFD():
            return ort.InferenceSession(model_path, sess_options=sess_options, providers=providers)
    
    def _load_models(self):
        """Load ONNX models and fallback sklearn models"""
        print("[INFO][LOADING] Loading NBA Game Models with NPU acceleration...")
        
        # Load feature columns (prefer enhanced)
        feature_paths = [
            self.models_dir / "feature_columns_enhanced.joblib",
            self.models_dir / "feature_columns.joblib",
        ]
        for feature_path in feature_paths:
            if feature_path.exists():
                self.feature_columns = joblib.load(feature_path)
                print(f"[OK] Loaded {len(self.feature_columns)} game features from {feature_path.name}")
                break
        if self.feature_columns is None:
            raise FileNotFoundError("Feature columns not found: feature_columns_enhanced.joblib or feature_columns.joblib")
        
        # Load main game models (win_prob, spread_margin, totals)
        for model_name, model_type in self.model_types.items():
            # Try to load ONNX version first
            onnx_candidates = [
                self.models_dir / f"{model_name}_enhanced.onnx",
                self.models_dir / f"{model_name}.onnx",
            ]
            for onnx_path in onnx_candidates:
                if onnx_path.exists() and self.npu_available:
                    try:
                        session = self._create_npu_session(str(onnx_path))
                        self.npu_sessions[model_name] = session
                        print(f"[OK] {model_name.upper()} loaded with NPU acceleration ({onnx_path.name})")
                        break
                    except Exception as e:
                        print(f"[WARN] {model_name.upper()} NPU failed for {onnx_path.name}: {e}")
            
            # Load sklearn fallback (only if sklearn available)
            sklearn_candidates = [
                self.models_dir / f"{model_name}_enhanced.joblib",
                self.models_dir / f"{model_name}.joblib",
            ]
            for sklearn_path in sklearn_candidates:
                if sklearn_path.exists() and SKLEARN_AVAILABLE and model_name not in self.fallback_models:
                    try:
                        self.fallback_models[model_name] = joblib.load(sklearn_path)
                        if model_name not in self.npu_sessions:
                            print(f"[OK] {model_name.upper()} loaded (CPU fallback from {sklearn_path.name})")
                        break
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
                onnx_candidates = [
                    self.models_dir / f"halves_{half}_{model_type}_enhanced.onnx",
                    self.models_dir / f"halves_{half}_{model_type}.onnx",
                ]
                for onnx_path in onnx_candidates:
                    if onnx_path.exists() and self.npu_available:
                        try:
                            session = self._create_npu_session(str(onnx_path))
                            self.period_models["halves"][half][model_type] = ("onnx", session)
                            halves_onnx_count += 1
                            break
                        except Exception as e:
                            print(f"[WARN] {half}_{model_type} ONNX failed for {onnx_path.name}, trying others: {e}")
                
                # Fallback to sklearn joblib if ONNX not available
                if model_type not in self.period_models["halves"][half]:
                    for halves_joblib_path in [self.models_dir / "halves_models_enhanced.joblib", self.models_dir / "halves_models.joblib"]:
                        if halves_joblib_path.exists() and SKLEARN_AVAILABLE:
                            try:
                                if "halves_joblib" not in self.__dict__ or getattr(self, "_halves_loaded_from", None) != str(halves_joblib_path):
                                    self.halves_joblib = joblib.load(halves_joblib_path)
                                    self._halves_loaded_from = str(halves_joblib_path)
                                if half in self.halves_joblib and model_type in self.halves_joblib[half]:
                                    self.period_models["halves"][half][model_type] = ("sklearn", self.halves_joblib[half][model_type])
                                    halves_cpu_count += 1
                                    break
                            except (ImportError, ModuleNotFoundError):
                                pass
        
        if halves_onnx_count > 0:
            print(f"[OK] Loaded halves models: {halves_onnx_count} NPU, {halves_cpu_count} CPU")
        elif halves_cpu_count > 0:
            print(f"[OK] Loaded halves models: {halves_cpu_count} CPU (ONNX not available)")
        
        # Load quarters models (ONNX for NPU, fallback to joblib if needed)
        quarters_onnx_count = 0
        quarters_cpu_count = 0
        self.period_models["quarters"] = {}
        for quarter in ["q1", "q2", "q3", "q4"]:
            self.period_models["quarters"][quarter] = {}
            for model_type in ["win", "margin", "total"]:
                # Try ONNX first
                onnx_candidates = [
                    self.models_dir / f"quarters_{quarter}_{model_type}_enhanced.onnx",
                    self.models_dir / f"quarters_{quarter}_{model_type}.onnx",
                ]
                for onnx_path in onnx_candidates:
                    if onnx_path.exists() and self.npu_available:
                        try:
                            session = self._create_npu_session(str(onnx_path))
                            self.period_models["quarters"][quarter][model_type] = ("onnx", session)
                            quarters_onnx_count += 1
                            break
                        except Exception as e:
                            print(f"[WARN] {quarter}_{model_type} ONNX failed for {onnx_path.name}, trying others: {e}")
                
                # Fallback to sklearn joblib if ONNX not available
                if model_type not in self.period_models["quarters"][quarter]:
                    for quarters_joblib_path in [self.models_dir / "quarters_models_enhanced.joblib", self.models_dir / "quarters_models.joblib"]:
                        if quarters_joblib_path.exists() and SKLEARN_AVAILABLE:
                            try:
                                if "quarters_joblib" not in self.__dict__ or getattr(self, "_quarters_loaded_from", None) != str(quarters_joblib_path):
                                    self.quarters_joblib = joblib.load(quarters_joblib_path)
                                    self._quarters_loaded_from = str(quarters_joblib_path)
                                if quarter in self.quarters_joblib and model_type in self.quarters_joblib[quarter]:
                                    self.period_models["quarters"][quarter][model_type] = ("sklearn", self.quarters_joblib[quarter][model_type])
                                    quarters_cpu_count += 1
                                    break
                            except (ImportError, ModuleNotFoundError):
                                pass
        
        if quarters_onnx_count > 0:
            print(f"[OK] Loaded quarters models: {quarters_onnx_count} NPU, {quarters_cpu_count} CPU")
        elif quarters_cpu_count > 0:
            print(f"[OK] Loaded quarters models: {quarters_cpu_count} CPU (ONNX not available)")
        
        total_npu = len(self.npu_sessions) + halves_onnx_count + quarters_onnx_count
        total_cpu = len(self.fallback_models) + halves_cpu_count + quarters_cpu_count
        print(f"[READY] Ready with {total_npu + total_cpu} models ({total_npu} NPU-accelerated)")
    
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
                onnx_outputs = session.run(None, {input_name: features})
                result = onnx_outputs[0] if onnx_outputs else None
                
                if model_name == "win_prob":
                    # Classification: ensure we extract probabilities, not labels
                    predictions[model_name] = float(self._extract_class1_probability(list(onnx_outputs or [])))
                        
                    # Store raw win probability for analysis
                    predictions["win_prob_raw"] = predictions[model_name]
                else:
                    # Regression - handle 2D output
                    if result is None:
                        predictions[model_name] = float("nan")
                    else:
                        result = np.asarray(result)
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
        # where σ is league-configured so the spread calibration follows the active league.
        #
        # This leverages the NN's spread predictions directly rather than
        # relying on the overconfident binary classifier.
        if "spread_margin" in predictions:
            sigma = float(LEAGUE.spread_winprob_sigma)
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
                            onnx_outputs = model.run(None, {input_name: features})
                            result = onnx_outputs[0] if onnx_outputs else None
                            
                            if pred_type == "win":
                                # Classification: ensure we extract probabilities, not labels
                                predictions[period_type][period_name][pred_type] = float(self._extract_class1_probability(list(onnx_outputs or [])))
                            else:
                                # Regression - extract single value
                                if result is None:
                                    predictions[period_type][period_name][pred_type] = float("nan")
                                else:
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
        
        print(f"[PERF] Benchmarking Game Models NPU vs CPU performance ({num_predictions} predictions)...")
        
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

    @staticmethod
    def _extract_class1_probability(onnx_outputs: list[Any]) -> float:
        """Extract P(class=1) from common ONNX classifier output conventions.

        sklearn->onnx commonly returns two outputs: (label, probabilities). If we naively
        read the first output, we end up with hard 0/1 labels which destroys logloss.
        """
        arrs: list[np.ndarray] = []
        for out in onnx_outputs:
            try:
                arrs.append(np.asarray(out))
            except Exception:
                continue

        # 1) Handle ZipMap-like outputs: object array containing a single dict
        for a in arrs:
            try:
                if a.dtype == object and a.size == 1 and isinstance(a.flat[0], dict):
                    d = a.flat[0]
                    if 1 in d:
                        return float(d[1])
                    if "1" in d:
                        return float(d["1"])
                    # Fallback: choose the second key in sorted order
                    keys = sorted(d.keys())
                    if len(keys) >= 2:
                        return float(d[keys[1]])
            except Exception:
                pass

        # 2) Prefer a floating probability matrix with 2+ columns
        for a in arrs:
            if a.dtype.kind in ("f", "c") and a.ndim == 2 and a.shape[0] >= 1 and a.shape[1] >= 2:
                return float(a[0, 1])

        # 3) Next: a floating vector of class probabilities
        for a in arrs:
            if a.dtype.kind in ("f", "c") and a.ndim == 1 and a.shape[0] >= 2:
                return float(a[1])

        # 4) Last resort: a single floating probability
        for a in arrs:
            if a.dtype.kind in ("f", "c"):
                return float(a.flat[0])

        # 5) Give up and coerce first output
        if arrs:
            return float(arrs[0].flat[0])
        return 0.5


def train_game_models_npu(retrain: bool = True):
    """Train game models and convert to ONNX for NPU acceleration"""
    if not ONNX_AVAILABLE:
        raise ImportError("ONNX Runtime not available. Install with: pip install onnxruntime")
    
    try:
        import importlib
        skl2onnx = importlib.import_module("skl2onnx")
        convert_sklearn = getattr(skl2onnx, "convert_sklearn")
        dt_mod = importlib.import_module("skl2onnx.common.data_types")
        FloatTensorType = getattr(dt_mod, "FloatTensorType")
    except Exception:
        raise ImportError("skl2onnx not available. Install with: pip install skl2onnx")
    
    print("[INFO][LOADING] Training game models for NPU conversion...")
    
    if retrain:
        # Load features and retrain models
        features_path = paths.data_processed / "features.parquet"
        if not features_path.exists():
            raise FileNotFoundError(f"Features not found: {features_path}")
        
        df = pd.read_parquet(features_path)
        print("[INFO][LOADING] Retraining game models with latest data...")
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
    
    print(f"[INFO] Converting {len(models_to_convert)} game models to ONNX...")
    
    # Convert each model to ONNX
    for model_name, model_path in models_to_convert.items():
        if not model_path.exists():
            print(f"[WARN] Skipping {model_name} - model file not found")
            continue
            
        print(f"[INFO] Converting {model_name} model...")
        
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
            
            print(f"[OK] Saved {model_name} ONNX model to {onnx_path}")
            
        except Exception as e:
            print(f"[ERROR] Failed to convert {model_name}: {e}")
    
    print("[INFO][LOADING] NPU game model conversion complete!")


def predict_games_npu(features_df: pd.DataFrame, include_periods: bool = True, calibrate_periods: bool = True) -> pd.DataFrame:
    """Predict game outcomes using NPU-accelerated models"""
    predictor = NPUGamePredictor()
    
    # Prepare features (fill missing for inference stability)
    X = (
        features_df[predictor.feature_columns]
        .copy()
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0.0)
        .values
        .astype(np.float32)
    )
    
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
                # Skip internal keys, main models, and any non-dict entries (e.g., blended floats)
                if period_type.startswith("_") or period_type in predictor.model_types or not isinstance(period_data, dict):
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
    
    print(f"[PERF] NPU game predictions complete: {len(features_df)} games in {total_inference_time:.1f}ms")
    print(f"[PERF] Average per game: {total_inference_time/len(features_df):.2f}ms")

    # Optional calibration of period predictions to enforce constraints and add team tendencies
    if include_periods and calibrate_periods:
        try:
            from .period_calibration import calibrate_period_predictions, CalibrationConfig
            cfg = CalibrationConfig()
            pred_df = calibrate_period_predictions(pred_df, cfg)
            print("[CAL] Applied period calibration (totals/margins constrained and blended)")
        except Exception as e:
            print(f"[CAL][WARN] Period calibration skipped: {e}")

    # Add compatibility aliases so downstream tools can consume either artifact style.
    if "win_prob" in pred_df.columns and "home_win_prob" not in pred_df.columns:
        pred_df["home_win_prob"] = pred_df["win_prob"]
    if "win_prob_raw" in pred_df.columns and "home_win_prob_raw" not in pred_df.columns:
        pred_df["home_win_prob_raw"] = pred_df["win_prob_raw"]
    if "win_prob_from_spread" in pred_df.columns and "home_win_prob_from_spread" not in pred_df.columns:
        pred_df["home_win_prob_from_spread"] = pred_df["win_prob_from_spread"]
    if "spread_margin" in pred_df.columns and "pred_margin" not in pred_df.columns:
        pred_df["pred_margin"] = pred_df["spread_margin"]
    if "totals" in pred_df.columns and "pred_total" not in pred_df.columns:
        pred_df["pred_total"] = pred_df["totals"]

    return pred_df


def benchmark_game_npu_performance(num_runs: int = 100, num_games: int = 100) -> Dict:
    """Benchmark NPU vs CPU performance for game predictions"""
    print(f"[PERF] Benchmarking Game NPU performance with {num_runs} runs, {num_games} games each...")
    
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