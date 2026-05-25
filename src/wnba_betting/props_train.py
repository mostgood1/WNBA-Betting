from __future__ import annotations

import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import TimeSeriesSplit
import joblib
from typing import Dict, List

from .config import paths

TARGETS = [
    # Core stats (existing)
    "t_pts", "t_reb", "t_ast", "t_threes", "t_pra",
    # Defensive stats
    "t_stl", "t_blk", "t_tov",
    # Shooting stats
    "t_fgm", "t_fga", "t_fg_pct",
    "t_ftm", "t_fta", "t_ft_pct",
    # Rebound breakdown
    "t_oreb", "t_dreb",
    # Other
    "t_pf", "t_plus_minus",
    # Combo stats
    "t_stocks",  # STL + BLK
    "t_pr",      # PTS + REB
    "t_pa",      # PTS + AST
    "t_ra",      # REB + AST
]


def _load_features() -> pd.DataFrame:
    p = paths.data_processed / "props_features.parquet"
    c = paths.data_processed / "props_features.csv"
    if p.exists():
        try:
            return pd.read_parquet(p)
        except Exception:
            if c.exists():
                return pd.read_csv(c)
            raise
    if c.exists():
        return pd.read_csv(c)
    raise FileNotFoundError("props_features not found; run build-props-features")


def _feature_columns(df: pd.DataFrame) -> list[str]:
    exclude = {
        "player_id",
        "player_name",
        "game_id",
        "team",
        "date",
        "opp_team",
    }
    exclude.update(TARGETS)
    cols: list[str] = []
    for col in df.columns:
        if col in exclude:
            continue
        if pd.api.types.is_numeric_dtype(df[col]):
            cols.append(col)
    return cols


def _recency_sample_weights(df: pd.DataFrame, half_life_days: float = 30.0) -> np.ndarray:
    if "date" not in df.columns or df.empty:
        return np.ones(len(df), dtype=float)
    dates = pd.to_datetime(df["date"], errors="coerce")
    if dates.notna().sum() <= 1:
        return np.ones(len(df), dtype=float)
    max_date = dates.max()
    age_days = (max_date - dates).dt.days.fillna(0).astype(float)
    weights = np.power(0.5, age_days / max(float(half_life_days), 1.0))
    return np.clip(weights.to_numpy(dtype=float), 0.35, 1.0)


def _best_alpha(X: np.ndarray, y: np.ndarray, sample_weights: np.ndarray, alphas: list[float]) -> tuple[float, float]:
    n_rows = len(y)
    if n_rows < 30:
        alpha = float(alphas[0]) if alphas else 1.0
        return alpha, float("nan")

    n_splits = min(5, max(2, n_rows // 40))
    splitter = TimeSeriesSplit(n_splits=n_splits)
    losses: dict[float, list[float]] = {float(alpha): [] for alpha in alphas}
    for tr, te in splitter.split(X):
        Xtr, Xte = X[tr], X[te]
        ytr, yte = y[tr], y[te]
        wtr = sample_weights[tr]
        for alpha in alphas:
            model = Ridge(alpha=float(alpha), random_state=42)
            model.fit(Xtr, ytr, sample_weight=wtr)
            pred = model.predict(Xte)
            losses[float(alpha)].append(float(np.sqrt(mean_squared_error(yte, pred))))

    mean_losses = {
        alpha: float(np.mean(vals))
        for alpha, vals in losses.items()
        if vals
    }
    if not mean_losses:
        alpha = float(alphas[0]) if alphas else 1.0
        return alpha, float("nan")
    best_alpha = min(mean_losses, key=mean_losses.get)
    return float(best_alpha), float(mean_losses[best_alpha])


def train_props_models(alpha: float = 1.0) -> Dict[str, object]:
    df = _load_features()
    available_targets = [target for target in TARGETS if target in df.columns]
    if not available_targets:
        raise ValueError("No props training targets available in props_features")

    df = df.sort_values("date") if "date" in df.columns else df.copy()
    feat_cols = _feature_columns(df)
    if not feat_cols:
        raise ValueError("No numeric props feature columns available for training")

    X = df[feat_cols].fillna(0.0).values
    sample_weights = _recency_sample_weights(df)
    alpha_grid = sorted({float(alpha), 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0})
    models: Dict[str, Ridge] = {}
    metrics: Dict[str, dict[str, float]] = {}
    for tgt in available_targets:
        target_df = df.dropna(subset=[tgt]).copy()
        if target_df.empty:
            continue
        X_t = target_df[feat_cols].fillna(0.0).values
        y = target_df[tgt].astype(float).values
        w_t = _recency_sample_weights(target_df)
        best_alpha, cv_rmse = _best_alpha(X_t, y, w_t, alpha_grid)
        m = Ridge(alpha=best_alpha, random_state=42)
        m.fit(X_t, y, sample_weight=w_t)
        models[tgt] = m
        metrics[tgt] = {"alpha": float(best_alpha), "cv_rmse": float(cv_rmse) if np.isfinite(cv_rmse) else float("nan")}
    # Save models and feature columns
    paths.models.mkdir(parents=True, exist_ok=True)
    joblib.dump(feat_cols, paths.models / "props_feature_columns.joblib")
    joblib.dump(models, paths.models / "props_models.joblib")
    joblib.dump(metrics, paths.models / "props_model_metrics.joblib")
    return models


def predict_props(features: pd.DataFrame) -> pd.DataFrame:
    feat_cols = joblib.load(paths.models / "props_feature_columns.joblib")
    models: Dict[str, Ridge] = joblib.load(paths.models / "props_models.joblib")
    X = features[feat_cols].fillna(0.0).values
    out = features.copy()
    for tgt, m in models.items():
        pred_col = tgt.replace("t_", "pred_")
        out[pred_col] = m.predict(X)
    return out
