"""
🏀 NPU-Accelerated Props Prediction Module for NBA-Betting
Integrates Qualcomm Snapdragon X Elite NPU with existing NBA-Betting pipeline
"""

from __future__ import annotations

import os
import time
import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import warnings
warnings.filterwarnings('ignore')

try:
    import onnxruntime as ort
    ONNX_AVAILABLE = True
except ImportError:
    ONNX_AVAILABLE = False
    ort = None

from .config import paths
from .props_train import TARGETS, _load_features
from .props_features import build_features_for_date


class NPUPropsPredictor:
    """NPU-accelerated props predictor for NBA-Betting integration"""
    
    def __init__(self, models_dir: Optional[Path] = None):
        """Initialize NPU predictor with models from NBA-Betting pipeline"""
        self.models_dir = models_dir or paths.models
        self.sessions = {}
        self.feature_columns = None
        self.fallback_models = None
        self.npu_available = ONNX_AVAILABLE
        
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
                except Exception:
                    continue
    
    def _create_npu_session(self, model_path: Path) -> ort.InferenceSession:
        """Create optimized NPU session for ONNX model"""
        if not self.npu_available:
            raise RuntimeError("ONNX Runtime not available")
            
        # QNN Provider configuration for NPU
        qnn_options = {
            "backend_path": "C:/Qualcomm/QNN_SDK/lib/aarch64-windows-msvc/QnnHtp.dll",
            "target_device": "xelite",
            "runtime": "htp",
        }
        
        providers = [
            ("QNNExecutionProvider", qnn_options),
            "CPUExecutionProvider"  # Fallback
        ]
        
        # Optimized session options
        sess_options = ort.SessionOptions()
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        sess_options.enable_mem_pattern = True
        sess_options.enable_cpu_mem_arena = True
        
        return ort.InferenceSession(str(model_path), sess_options=sess_options, providers=providers)
    
    def _load_models(self):
        """Load ONNX models for NPU or fallback to sklearn models"""
        
        # Load feature columns from NBA-Betting pipeline
        feature_cols_path = self.models_dir / "props_feature_columns.joblib"
        if feature_cols_path.exists():
            self.feature_columns = joblib.load(feature_cols_path)
            print(f"✅ Loaded {len(self.feature_columns)} feature columns from NBA-Betting")
        else:
            raise FileNotFoundError(f"Feature columns not found: {feature_cols_path}")
        
        # Try to load ONNX models for NPU acceleration
        onnx_loaded = 0
        if self.npu_available:
            print("🚀 Loading NPU-accelerated ONNX models...")
            
            for target in TARGETS:
                target_clean = target.replace("t_", "")  # Remove t_ prefix for file names
                onnx_file = self.models_dir / f"{target}_ridge.onnx"
                
                if onnx_file.exists():
                    try:
                        self.sessions[target] = self._create_npu_session(onnx_file)
                        print(f"✅ {target_clean.upper()} model loaded with NPU acceleration")
                        onnx_loaded += 1
                    except Exception as e:
                        print(f"⚠️  {target_clean.upper()} NPU failed, will use CPU fallback: {e}")
                else:
                    # Don't error on missing combo stats, they'll be calculated
                    if target not in ["t_stocks", "t_pr", "t_pa", "t_ra"]:
                        print(f"⚠️  ONNX model not found: {onnx_file}")
        
        # Load sklearn models as fallback
        sklearn_models_path = self.models_dir / "props_models.joblib"
        if sklearn_models_path.exists():
            self.fallback_models = joblib.load(sklearn_models_path)
            fallback_count = len(self.fallback_models)
            print(f"✅ Loaded {fallback_count} sklearn fallback models")
        else:
            print("⚠️  No sklearn models found for fallback")
        
        total_models = onnx_loaded + (len(self.fallback_models) if self.fallback_models else 0)
        print(f"🎯 Ready with {total_models} models ({onnx_loaded} NPU-accelerated)")
    
    def predict_props_for_features(self, features_df: pd.DataFrame, use_npu: bool = True) -> pd.DataFrame:
        """Predict props using NPU acceleration when available"""
        
        if features_df.empty:
            return features_df.copy()
        
        # Prepare features array
        X = features_df[self.feature_columns].fillna(0.0).values.astype(np.float32)
        
        # Copy input dataframe for results
        result_df = features_df.copy()
        
        total_inference_time = 0
        predictions_made = 0
        
        for target in TARGETS:
            pred_col = target.replace("t_", "pred_")
            
            # Try NPU first if available and requested
            if use_npu and target in self.sessions:
                try:
                    session = self.sessions[target]
                    input_name = session.get_inputs()[0].name
                    
                    start_time = time.perf_counter()
                    predictions = session.run(None, {input_name: X})[0]
                    inference_time = (time.perf_counter() - start_time) * 1000
                    
                    result_df[pred_col] = predictions.flatten()
                    total_inference_time += inference_time
                    predictions_made += 1
                    continue
                    
                except Exception as e:
                    print(f"NPU prediction failed for {target}, falling back to sklearn: {e}")
            
            # Fallback to sklearn model
            if self.fallback_models and target in self.fallback_models:
                model = self.fallback_models[target]
                predictions = model.predict(X)
                result_df[pred_col] = predictions
                predictions_made += 1
            else:
                print(f"⚠️  No model available for {target}")
        
        if total_inference_time > 0:
            print(f"⚡ NPU inference: {total_inference_time:.3f}ms for {predictions_made} props")
        
        # Calculate combo stats from predictions
        if "pred_stl" in result_df.columns and "pred_blk" in result_df.columns:
            result_df["pred_stocks"] = result_df["pred_stl"] + result_df["pred_blk"]
        if "pred_pts" in result_df.columns and "pred_reb" in result_df.columns:
            result_df["pred_pr"] = result_df["pred_pts"] + result_df["pred_reb"]
        if "pred_pts" in result_df.columns and "pred_ast" in result_df.columns:
            result_df["pred_pa"] = result_df["pred_pts"] + result_df["pred_ast"]
        if "pred_reb" in result_df.columns and "pred_ast" in result_df.columns:
            result_df["pred_ra"] = result_df["pred_reb"] + result_df["pred_ast"]
        
        return result_df
    
    def predict_props_for_date(self, date: str, use_npu: bool = True) -> pd.DataFrame:
        """Build features for date and predict props with NPU acceleration"""
        
        print(f"🏀 Predicting props for {date} with NPU acceleration...")
        
        # Use NBA-Betting's feature building pipeline
        try:
            features_df = build_features_for_date(date)
            if features_df.empty:
                print(f"No features available for {date}")
                return pd.DataFrame()
            
            # Make predictions
            predictions_df = self.predict_props_for_features(features_df, use_npu=use_npu)
            
            return predictions_df
            
        except Exception as e:
            print(f"Error predicting props for {date}: {e}")
            return pd.DataFrame()
    
    def benchmark_npu_performance(self, num_predictions: int = 1000) -> Dict[str, Any]:
        """Benchmark NPU performance vs sklearn fallback"""
        
        if not self.feature_columns:
            raise RuntimeError("Feature columns not loaded")
        
        print(f"🔥 Benchmarking NPU vs CPU performance ({num_predictions} predictions)...")
        
        # Create test features
        test_features = np.random.rand(1, len(self.feature_columns)).astype(np.float32)
        
        benchmark_results = {
            'npu_times': {},
            'cpu_times': {},
            'speedup': {},
            'npu_available_models': len(self.sessions),
            'total_models': len(TARGETS)
        }
        
        for target in TARGETS:
            target_clean = target.replace("t_", "")
            
            # Benchmark NPU if available
            if target in self.sessions:
                session = self.sessions[target]
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
                
                benchmark_results['npu_times'][target_clean] = {
                    'avg_ms': np.mean(times),
                    'min_ms': np.min(times),
                    'max_ms': np.max(times),
                    'throughput_per_sec': 1000 / np.mean(times)
                }
            
            # Benchmark CPU fallback if available
            if self.fallback_models and target in self.fallback_models:
                model = self.fallback_models[target]
                
                times = []
                for _ in range(num_predictions):
                    start_time = time.perf_counter()
                    model.predict(test_features)
                    end_time = time.perf_counter()
                    times.append((end_time - start_time) * 1000)
                
                benchmark_results['cpu_times'][target_clean] = {
                    'avg_ms': np.mean(times),
                    'min_ms': np.min(times),
                    'max_ms': np.max(times),
                    'throughput_per_sec': 1000 / np.mean(times)
                }
            
            # Calculate speedup
            if (target_clean in benchmark_results['npu_times'] and 
                target_clean in benchmark_results['cpu_times']):
                
                npu_time = benchmark_results['npu_times'][target_clean]['avg_ms']
                cpu_time = benchmark_results['cpu_times'][target_clean]['avg_ms']
                speedup = cpu_time / npu_time
                
                benchmark_results['speedup'][target_clean] = {
                    'speedup_factor': speedup,
                    'npu_faster': speedup > 1.0
                }
        
        return benchmark_results


def integrate_npu_with_nba_betting(date: str, output_csv: bool = True) -> pd.DataFrame:
    """
    Main integration function for NBA-Betting pipeline
    
    Args:
        date: Date string (YYYY-MM-DD) for predictions
        output_csv: Whether to save results to CSV
    
    Returns:
        DataFrame with NPU-accelerated predictions
    """
    
    print(f"🏀 NBA-Betting NPU Integration for {date}")
    print("=" * 60)
    
    try:
        # Initialize NPU predictor
        predictor = NPUPropsPredictor()
        
        # Make predictions for date
        predictions_df = predictor.predict_props_for_date(date, use_npu=True)
        
        if predictions_df.empty:
            print("No predictions generated")
            return pd.DataFrame()
        
        # Save to NBA-Betting standard location
        if output_csv:
            output_path = paths.data_processed / f"npu_props_predictions_{date}.csv"
            predictions_df.to_csv(output_path, index=False)
            print(f"💾 Saved NPU predictions to: {output_path}")
        
        # Display summary
        pred_cols = [col for col in predictions_df.columns if col.startswith('pred_')]
        print(f"\n📊 Generated {len(pred_cols)} prop predictions for {len(predictions_df)} player-games")
        
        # Show sample predictions
        if not predictions_df.empty and pred_cols:
            print(f"\n📈 Sample predictions:")
            sample_cols = ['player_name', 'team_abbr'] + pred_cols[:3]
            available_cols = [col for col in sample_cols if col in predictions_df.columns]
            if available_cols:
                print(predictions_df[available_cols].head(3).to_string(index=False))
        
        return predictions_df
        
    except Exception as e:
        print(f"❌ Error in NPU integration: {e}")
        return pd.DataFrame()


def benchmark_nba_betting_npu() -> None:
    """Benchmark NPU performance for NBA-Betting integration"""
    
    print("🚀 NBA-Betting NPU Performance Benchmark")
    print("=" * 50)
    
    try:
        predictor = NPUPropsPredictor()
        results = predictor.benchmark_npu_performance(1000)
        
        print(f"\n📊 Performance Results:")
        print(f"NPU Models: {results['npu_available_models']}/{results['total_models']}")
        
        if results['npu_times']:
            print(f"\n⚡ NPU Performance:")
            for prop, metrics in results['npu_times'].items():
                print(f"  {prop.upper():8} | {metrics['avg_ms']:.3f}ms avg | {metrics['throughput_per_sec']:.0f} pred/sec")
        
        if results['cpu_times']:
            print(f"\n🖥️  CPU Performance:")
            for prop, metrics in results['cpu_times'].items():
                print(f"  {prop.upper():8} | {metrics['avg_ms']:.3f}ms avg | {metrics['throughput_per_sec']:.0f} pred/sec")
        
        if results['speedup']:
            print(f"\n🏁 Speedup Analysis:")
            for prop, speedup_data in results['speedup'].items():
                factor = speedup_data['speedup_factor']
                faster = "✅ NPU Faster" if speedup_data['npu_faster'] else "❌ CPU Faster"
                print(f"  {prop.upper():8} | {factor:.1f}x | {faster}")
        
    except Exception as e:
        print(f"❌ Benchmark failed: {e}")


if __name__ == "__main__":
    # Demo integration
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    
    print("🏀 NBA-Betting NPU Integration Demo")
    
    # Run benchmark
    benchmark_nba_betting_npu()
    
    # Try prediction for today (will work if data available)
    print(f"\n🎯 Attempting prediction for {today}...")
    results = integrate_npu_with_nba_betting(today)
    
    if not results.empty:
        print("✅ NPU integration successful!")
    else:
        print("ℹ️  No data available for today - integration ready for live data")


def train_props_models_npu(alpha: float = 1.0):
    """Train props models and convert to ONNX for NPU acceleration"""
    from .props_train import train_props_models
    from sklearn.linear_model import Ridge
    
    if not ONNX_AVAILABLE:
        raise ImportError("ONNX Runtime not available. Install with: pip install onnxruntime")
    
    try:
        import skl2onnx
        from skl2onnx import convert_sklearn
        from skl2onnx.common.data_types import FloatTensorType
    except ImportError:
        raise ImportError("skl2onnx not available. Install with: pip install skl2onnx")
    
    print("🏗️  Training props models for NPU conversion...")
    
    # Train regular models first
    train_props_models(alpha=alpha)
    
    # Load trained models and convert to ONNX
    models_path = paths.models / "props_models.joblib"
    feature_columns_path = paths.models / "props_feature_columns.joblib"
    
    if not models_path.exists():
        raise FileNotFoundError(f"Props models not found: {models_path}")
    if not feature_columns_path.exists():
        raise FileNotFoundError(f"Feature columns not found: {feature_columns_path}")
    
    models = joblib.load(models_path)
    feature_columns = joblib.load(feature_columns_path)
    
    print(f"🔄 Converting {len(models)} models to ONNX...")
    
    # Convert each model to ONNX
    for target, model in models.items():
        print(f"Converting {target} model...")
        
        # Define the input type
        initial_type = [('float_input', FloatTensorType([None, len(feature_columns)]))]
        
        # Convert to ONNX
        onx = convert_sklearn(model, initial_types=initial_type)
        
        # Save ONNX model
        onnx_path = paths.models / f"{target}_ridge.onnx"
        with open(onnx_path, "wb") as f:
            f.write(onx.SerializeToString())
        
        print(f"✅ Saved {target} ONNX model to {onnx_path}")
    
    print("🎯 NPU model conversion complete!")
    return models


def predict_props_npu(features_df: pd.DataFrame) -> pd.DataFrame:
    """Predict props using NPU-accelerated models"""
    predictor = NPUPropsPredictor()
    
    # Use the existing predict_props_for_features method
    result_df = predictor.predict_props_for_features(features_df, use_npu=True)
    
    print(f"⚡ NPU predictions complete: {len(features_df)} players")
    
    return result_df
    
    return pred_df


def benchmark_npu_performance(num_runs: int = 100, num_players: int = 500) -> Dict:
    """Benchmark NPU vs CPU performance for props prediction"""
    print(f"🔥 Benchmarking NPU performance with {num_runs} runs, {num_players} players each...")
    
    try:
        predictor = NPUPropsPredictor()
        
        # Use the existing benchmark method from the class
        results = predictor.benchmark_npu_performance(num_predictions=num_runs)
        
        # Add some additional info
        results["benchmark_config"] = {
            "num_runs": num_runs,
            "num_players": num_players,
            "npu_models": results.get('npu_available_models', 0),
            "total_models": results.get('total_models', 0)
        }
        
        return results
        
    except Exception as e:
        return {"error": str(e)}