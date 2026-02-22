"""
Pure ONNX Game Predictor - NO sklearn dependency
Uses ONNX models with Qualcomm NPU acceleration for game predictions.

This module provides game predictions (win probability, spread, total) using
only ONNX models without any sklearn dependencies, allowing it to work on
ARM64 Windows where sklearn compilation fails.

Models:
- win_prob.onnx: Predicts home team win probability
- spread_margin.onnx: Predicts point spread (margin)
- totals.onnx: Predicts total points scored

Author: GitHub Copilot
Date: October 17, 2025
"""

from __future__ import annotations
import os
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd

try:
    import onnxruntime as ort
    ONNX_AVAILABLE = True
    if TYPE_CHECKING:
        InferenceSession = ort.InferenceSession
except ImportError:
    ONNX_AVAILABLE = False
    ort = None


class PureONNXGamePredictor:
    """
    Pure ONNX game predictor using Qualcomm NPU acceleration.
    
    This predictor:
    - Uses ONNX models exclusively (no sklearn)
    - Leverages QNN ExecutionProvider for NPU acceleration (when available)
    - Requires N input features per game (driven by the model + feature columns file)
    - Provides win probability, spread, and total predictions
    """
    
    # Legacy/default expected features (17 total)
    EXPECTED_FEATURES = [
        'elo_diff', 'home_rest_days', 'visitor_rest_days', 'home_b2b', 'visitor_b2b',
        'home_form_off_5', 'home_form_def_5', 'visitor_form_off_5', 'visitor_form_def_5',
        'home_games_last3', 'visitor_games_last3', 'home_games_last5', 'visitor_games_last5',
        'home_3in4', 'visitor_3in4', 'home_4in6', 'visitor_4in6'
    ]
    
    def __init__(self, models_dir: Path):
        """
        Initialize the pure ONNX game predictor.
        
        Args:
            models_dir: Path to directory containing ONNX model files
        """
        if not ONNX_AVAILABLE:
            raise ImportError(
                "onnxruntime not available. Install with: "
                "pip install onnxruntime-qnn"
            )
        
        self.models_dir = Path(models_dir)
        self.feature_columns = self._load_feature_columns()
        
        # Setup QNN (Qualcomm NPU) paths
        self._setup_qnn_paths()
        
        # Initialize ONNX sessions with NPU
        self.win_session = self._create_npu_session("win_prob.onnx")
        self.spread_session = self._create_npu_session("spread_margin.onnx")
        self.total_session = self._create_npu_session("totals.onnx")
        
        print(f"[OK]✅ Pure ONNX Game Predictor initialized")
        print(f"[OK]   Win model providers: {self.win_session.get_providers()}")
        print(f"[OK]   Spread model providers: {self.spread_session.get_providers()}")
        print(f"[OK]   Total model providers: {self.total_session.get_providers()}")
        print(f"[OK]   Features: {len(self.feature_columns)}")
    
    def _load_feature_columns(self) -> List[str]:
        """Load feature column names from pickle file (no sklearn)."""
        feature_path = self.models_dir / "feature_columns.joblib"
        
        if not feature_path.exists():
            print(f"[OK]⚠️  Feature columns not found at {feature_path}")
            print(f"[OK]   Using default 17 features")
            return self.EXPECTED_FEATURES
        
        try:
            with open(feature_path, "rb") as f:
                columns = pickle.load(f)

            if not isinstance(columns, (list, tuple)) or not columns:
                print(f"[OK]⚠️  Feature columns file invalid: {feature_path}")
                print(f"[OK]   Using default {len(self.EXPECTED_FEATURES)} features")
                return self.EXPECTED_FEATURES

            columns = list(columns)
            if len(columns) != 17:
                # Models may evolve (e.g., enhanced 45-feature models). Use whatever
                # the pipeline wrote and validate against the ONNX input at runtime.
                print(f"[OK]⚠️  Feature columns count is {len(columns)} (legacy default is 17)")
            return columns
        except Exception as e:
            print(f"[OK]⚠️  Error loading feature columns: {e}")
            print(f"[OK]   Using default features")
            return self.EXPECTED_FEATURES
    
    def _setup_qnn_paths(self):
        """Setup Qualcomm QNN SDK paths for NPU acceleration."""
        qnn_sdk_path = r"C:/Qualcomm/QNN_SDK/lib/aarch64-windows-msvc"
        
        if os.path.exists(qnn_sdk_path):
            # Add QNN SDK to PATH if not already there
            current_path = os.environ.get('PATH', '')
            if qnn_sdk_path not in current_path:
                os.environ['PATH'] = f"{qnn_sdk_path};{current_path}"
            print(f"[OK]✅ QNN SDK path configured: {qnn_sdk_path}")
        else:
            print(f"[OK]⚠️  QNN SDK not found at {qnn_sdk_path}")
            print(f"[OK]   Will fall back to CPU execution")
    
    def _create_npu_session(self, model_filename: str) -> "ort.InferenceSession":  # type: ignore
        """
        Create ONNX Runtime session with QNN (NPU) provider.
        
        Args:
            model_filename: Name of the ONNX model file
            
        Returns:
            Configured InferenceSession with QNN and CPU providers
        """
        model_path = str(self.models_dir / model_filename)
        
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        # QNN options for Qualcomm NPU
        qnn_options = {
            'backend_path': 'QnnHtp.dll',  # Hexagon Tensor Processor
            'qnn_context_priority': 'high',
            'htp_performance_mode': 'burst',
            'qnn_saver_path': str(self.models_dir / 'qnn_context'),
            'enable_htp_fp16_precision': 'true'
        }
        
        # Session options
        sess_options = ort.SessionOptions()
        sess_options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        
        try:
            # Try QNN (NPU) first, fall back to CPU
            providers = [
                ('QNNExecutionProvider', qnn_options),
                'CPUExecutionProvider'
            ]
            
            session = ort.InferenceSession(
                model_path,
                sess_options=sess_options,
                providers=providers
            )
            
            return session
            
        except Exception as e:
            print(f"[OK]⚠️  QNN provider failed for {model_filename}: {e}")
            print(f"[OK]   Falling back to CPU")
            
            # Fallback to CPU only
            return ort.InferenceSession(
                model_path,
                sess_options=sess_options,
                providers=['CPUExecutionProvider']
            )
    
    def predict(self, features_df: pd.DataFrame) -> pd.DataFrame:
        """
        Predict game outcomes using pure ONNX models.
        
        Args:
            features_df: DataFrame with 17 game features per matchup
            
        Returns:
            DataFrame with predictions:
                - home_win_prob: Home team win probability (0-1)
                - pred_margin: Predicted point spread (+ favors home)
                - pred_total: Predicted total points scored
        """
        # Validate required columns are present (allow extra columns).
        missing = [c for c in self.feature_columns if c not in features_df.columns]
        if missing:
            raise ValueError(
                f"Missing required feature columns: {missing[:10]}"
                + (f" (and {len(missing) - 10} more)" if len(missing) > 10 else "")
            )
        
        # Prepare input for ONNX (float32)
        X = features_df[self.feature_columns].values.astype(np.float32)
        
        # Get input names from models
        win_input_name = self.win_session.get_inputs()[0].name
        spread_input_name = self.spread_session.get_inputs()[0].name
        total_input_name = self.total_session.get_inputs()[0].name
        
        # Run inference on NPU
        # Spread margin (regression) - PRIMARY prediction
        spread_outputs = self.spread_session.run(None, {spread_input_name: X})
        spreads = spread_outputs[0].flatten()
        
        # Total points (regression)
        total_outputs = self.total_session.run(None, {total_input_name: X})
        totals = total_outputs[0].flatten()
        
        # Win probability (binary classification - get probability of class 1)
        win_outputs = self.win_session.run(None, {win_input_name: X})
        # Output format: [labels, probabilities] where probabilities is list of dicts {0: p0, 1: p1}
        win_probs_raw = np.array([p[1] for p in win_outputs[1]], dtype=np.float32)
        
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
        sigma = 12.0  # Standard deviation of NBA point spreads
        win_probs_calibrated = 1.0 / (1.0 + np.exp(-spreads / sigma))
        
        # Blend: 80% spread-based (calibrated), 20% direct model
        # This preserves any signal from the win model while fixing overconfidence
        win_probs = 0.8 * win_probs_calibrated + 0.2 * win_probs_raw
        
        # Build results DataFrame
        results = pd.DataFrame({
            'home_win_prob': win_probs,
            'home_win_prob_raw': win_probs_raw,  # Keep raw for analysis
            'home_win_prob_from_spread': win_probs_calibrated,  # Keep spread-based for comparison
            'pred_margin': spreads,
            'pred_total': totals
        })
        
        return results
    
    def predict_single(self, features: Dict[str, float]) -> Dict[str, float]:
        """
        Predict a single game outcome.
        
        Args:
            features: Dictionary with 17 game features
            
        Returns:
            Dictionary with predictions:
                - home_win_prob: Home team win probability
                - pred_margin: Predicted point spread
                - pred_total: Predicted total points
        """
        # Convert to DataFrame
        df = pd.DataFrame([features])
        
        # Predict
        result = self.predict(df)
        
        # Return as dictionary
        return {
            'home_win_prob': float(result['home_win_prob'].iloc[0]),
            'pred_margin': float(result['pred_margin'].iloc[0]),
            'pred_total': float(result['pred_total'].iloc[0])
        }
    
    def get_model_info(self) -> Dict[str, any]:
        """Get information about loaded models."""
        return {
            'win_model': {
                'providers': self.win_session.get_providers(),
                'inputs': [(i.name, i.shape) for i in self.win_session.get_inputs()],
                'outputs': [(o.name, o.shape) for o in self.win_session.get_outputs()]
            },
            'spread_model': {
                'providers': self.spread_session.get_providers(),
                'inputs': [(i.name, i.shape) for i in self.spread_session.get_inputs()],
                'outputs': [(o.name, o.shape) for o in self.spread_session.get_outputs()]
            },
            'total_model': {
                'providers': self.total_session.get_providers(),
                'inputs': [(i.name, i.shape) for i in self.total_session.get_inputs()],
                'outputs': [(o.name, o.shape) for o in self.total_session.get_outputs()]
            },
            'features': self.feature_columns,
            'num_features': len(self.feature_columns)
        }


def create_pure_game_predictor(models_dir: Optional[Path] = None) -> PureONNXGamePredictor:
    """
    Factory function to create a pure ONNX game predictor.
    
    Args:
        models_dir: Path to models directory (defaults to standard location)
        
    Returns:
        Initialized PureONNXGamePredictor
    """
    if models_dir is None:
        # Assume standard project structure
        from .config import paths
        models_dir = paths.models
    
    return PureONNXGamePredictor(models_dir)


if __name__ == "__main__":
    # Test the predictor
    print("Testing Pure ONNX Game Predictor...")
    print("=" * 60)
    
    # Create predictor with explicit path for testing
    models_path = Path(__file__).parent.parent.parent / "models"
    predictor = PureONNXGamePredictor(models_path)
    
    # Show model info
    print("\n[OK]Model Information:")
    info = predictor.get_model_info()
    for model_name, model_info in info.items():
        if model_name in ('features', 'num_features'):
            continue
        print(f"[OK]\n{model_name}:")
        print(f"[OK]  Providers: {model_info['providers']}")
        print(f"[OK]  Input: {model_info['inputs']}")
        print(f"[OK]  Output: {model_info['outputs']}")
    
    # Test with dummy data
    print("\n[OK]Testing with dummy game data...")
    dummy_features = {
        'elo_diff': 50.0,
        'home_rest_days': 1.0,
        'visitor_rest_days': 2.0,
        'home_b2b': 0.0,
        'visitor_b2b': 1.0,
        'home_form_off_5': 115.0,
        'home_form_def_5': 108.0,
        'visitor_form_off_5': 110.0,
        'visitor_form_def_5': 112.0,
        'home_games_last3': 3.0,
        'visitor_games_last3': 3.0,
        'home_games_last5': 5.0,
        'visitor_games_last5': 5.0,
        'home_3in4': 0.0,
        'visitor_3in4': 1.0,
        'home_4in6': 0.0,
        'visitor_4in6': 1.0
    }
    
    prediction = predictor.predict_single(dummy_features)
    print(f"[OK]\nPrediction:")
    print(f"[OK]  Home Win Probability: {prediction['home_win_prob']:.1%}")
    print(f"[OK]  Predicted Margin: {prediction['pred_margin']:+.1f}")
    print(f"[OK]  Predicted Total: {prediction['pred_total']:.1f}")
    
    print("\n[OK]✅ Pure ONNX Game Predictor test complete!")
