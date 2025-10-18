"""
Win Probability Calibration Module

Fixes overconfident 0%/100% predictions using multiple calibration methods.
"""
import numpy as np
import pandas as pd
from pathlib import Path

# Optional imports for advanced calibration (not needed for quick Platt fix)
try:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.isotonic import IsotonicRegression
    import joblib
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False


def platt_scaling_transform(prob: float, alpha: float = 0.15) -> float:
    """
    Quick Platt-like scaling to compress extreme probabilities.
    
    Maps:
    - 0.0 → alpha (e.g., 0.15)
    - 1.0 → 1 - alpha (e.g., 0.85)
    - Preserves relative ordering
    
    Args:
        prob: Raw probability from model (0.0 to 1.0)
        alpha: Compression parameter (typical: 0.10 to 0.20)
        
    Returns:
        Calibrated probability
    """
    # Map [0, 1] → [alpha, 1-alpha]
    return alpha + prob * (1 - 2 * alpha)


if SKLEARN_AVAILABLE:
    def isotonic_calibration(y_true: np.ndarray, y_pred: np.ndarray) -> IsotonicRegression:
        """
        Fit isotonic regression calibrator on historical predictions vs actuals.
        
        Args:
            y_true: Actual outcomes (0 or 1)
            y_pred: Predicted probabilities (0.0 to 1.0)
            
        Returns:
            Fitted IsotonicRegression model
        """
        iso = IsotonicRegression(out_of_bounds='clip')
        iso.fit(y_pred, y_true)
        return iso


def empirical_calibration(y_true: np.ndarray, y_pred: np.ndarray, n_bins: int = 10) -> dict:
    """
    Create empirical calibration map from historical data.
    
    Bins predictions and computes actual win rate per bin.
    
    Args:
        y_true: Actual outcomes (0 or 1)
        y_pred: Predicted probabilities
        n_bins: Number of probability bins
        
    Returns:
        Dict mapping bin centers to calibrated probabilities
    """
    bins = np.linspace(0, 1, n_bins + 1)
    bin_indices = np.digitize(y_pred, bins) - 1
    bin_indices = np.clip(bin_indices, 0, n_bins - 1)
    
    calibration_map = {}
    for i in range(n_bins):
        mask = bin_indices == i
        if mask.sum() > 0:
            actual_rate = y_true[mask].mean()
            bin_center = (bins[i] + bins[i + 1]) / 2
            calibration_map[bin_center] = actual_rate
    
    return calibration_map


def apply_platt_scaling(predictions_df: pd.DataFrame, alpha: float = 0.15) -> pd.DataFrame:
    """
    Apply Platt scaling to win probabilities in predictions dataframe.
    
    Args:
        predictions_df: DataFrame with 'home_win_prob' column
        alpha: Compression parameter
        
    Returns:
        Modified DataFrame with calibrated probabilities
    """
    df = predictions_df.copy()
    
    if 'home_win_prob' in df.columns:
        original = df['home_win_prob'].copy()
        df['home_win_prob_raw'] = original  # Keep original
        df['home_win_prob'] = original.apply(lambda p: platt_scaling_transform(p, alpha))
        
        print(f"[OK]✅ Applied Platt scaling (alpha={alpha})")
        print(f"[OK]   Before: {original.min():.3f} to {original.max():.3f}")
        print(f"[OK]   After:  {df['home_win_prob'].min():.3f} to {df['home_win_prob'].max():.3f}")
    
    # Also calibrate halves/quarters if present
    for col in df.columns:
        if 'win' in col and col not in ['home_win_prob', 'home_win_prob_raw']:
            if df[col].dtype in ['float64', 'float32']:
                df[f"{col}_raw"] = df[col].copy()
                df[col] = df[col].apply(lambda p: platt_scaling_transform(p, alpha))
    
    return df


def calibrate_predictions_file(
    predictions_path: str | Path,
    output_path: str | Path | None = None,
    method: str = 'platt',
    alpha: float = 0.15
) -> Path:
    """
    Calibrate win probabilities in a predictions CSV file.
    
    Args:
        predictions_path: Path to predictions CSV
        output_path: Path for calibrated output (default: adds '_calibrated' suffix)
        method: Calibration method ('platt', 'isotonic', 'empirical')
        alpha: Compression parameter for Platt scaling
        
    Returns:
        Path to calibrated predictions file
    """
    df = pd.read_csv(predictions_path)
    
    if method == 'platt':
        df = apply_platt_scaling(df, alpha=alpha)
    else:
        raise ValueError(f"Method '{method}' requires historical data. Use 'platt' for quick fix.")
    
    if output_path is None:
        p = Path(predictions_path)
        output_path = p.parent / f"{p.stem}_calibrated{p.suffix}"
    
    df.to_csv(output_path, index=False)
    print(f"[OK]✅ Saved calibrated predictions to: {output_path}")
    
    return Path(output_path)


# ============================================================================
# TRAINING-TIME CALIBRATION (for future model retraining)
# ============================================================================

if SKLEARN_AVAILABLE:
    def train_with_calibration(
        X_train: np.ndarray,
        y_train: np.ndarray,
        base_model,
        method: str = 'isotonic',
        cv: int = 5
    ) -> CalibratedClassifierCV:
        """
        Wrap model with sklearn's CalibratedClassifierCV during training.
        
        Use this when retraining models to get properly calibrated probabilities.
        
        Args:
            X_train: Training features
            y_train: Training labels (0/1)
            base_model: Unfitted sklearn model (e.g., LogisticRegression)
            method: 'sigmoid' (Platt) or 'isotonic'
            cv: Number of cross-validation folds
            
        Returns:
            Fitted calibrated model
        """
        calibrated = CalibratedClassifierCV(
            base_model,
            method=method,
            cv=cv,
            ensemble=True  # Keep all CV models for better calibration
        )
        calibrated.fit(X_train, y_train)
        
        print(f"[OK]✅ Trained model with {method} calibration ({cv}-fold CV)")
        
        return calibrated


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python calibrate_win_prob.py <predictions_file> [alpha]")
        print("Example: python calibrate_win_prob.py data/processed/predictions_2025-10-17.csv 0.15")
        sys.exit(1)
    
    predictions_file = sys.argv[1]
    alpha = float(sys.argv[2]) if len(sys.argv) > 2 else 0.15
    
    print("=" * 70)
    print("WIN PROBABILITY CALIBRATION")
    print("=" * 70)
    
    output = calibrate_predictions_file(predictions_file, alpha=alpha)
    
    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)
