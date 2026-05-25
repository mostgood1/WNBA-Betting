from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional

from .config import paths
from .props_linear import _load_training_features, load_linear_props_models, predict_with_linear_models


def _filter_by_date(df: pd.DataFrame, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
    if start:
        df = df[df.get("date") >= start] if "date" in df.columns else df
    if end:
        df = df[df.get("date") <= end] if "date" in df.columns else df
    return df


def backtest_linear_props(targets: List[str], start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """Compute quick backtest metrics for linear props models on historical features.

    Metrics per target:
    - count, mae, rmse, r2, corr, bias (mean error)
    """
    feats = _load_training_features()
    feats = feats.copy()
    # Ensure a comparable date column if present
    if "date" in feats.columns:
        try:
            feats["date"] = pd.to_datetime(feats["date"]).dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    feats = _filter_by_date(feats, start, end)

    lin = load_linear_props_models()
    preds = predict_with_linear_models(feats, lin)

    rows: List[Dict[str, object]] = []
    for tgt in targets:
        if tgt not in feats.columns:
            continue
        pred_col = tgt.replace("t_","pred_")
        if pred_col not in preds.columns:
            continue
        y = pd.to_numeric(feats[tgt], errors="coerce")
        yhat = pd.to_numeric(preds[pred_col], errors="coerce")
        mask = y.notna() & yhat.notna()
        if not mask.any():
            continue
        y = y[mask].astype(float).values
        yhat = yhat[mask].astype(float).values
        err = yhat - y
        mae = float(np.mean(np.abs(err)))
        rmse = float(np.sqrt(np.mean(err**2)))
        bias = float(np.mean(err))
        # R2 and corr guarded
        y_mean = np.mean(y)
        ss_tot = float(np.sum((y - y_mean)**2))
        ss_res = float(np.sum((y - yhat)**2))
        r2 = float(1 - ss_res/ss_tot) if ss_tot > 0 else float("nan")
        corr = float(np.corrcoef(y, yhat)[0,1]) if len(y) > 1 else float("nan")
        rows.append({
            "target": tgt,
            "count": int(len(y)),
            "mae": mae,
            "rmse": rmse,
            "bias": bias,
            "r2": r2,
            "corr": corr,
            "start": start,
            "end": end,
        })

    out = pd.DataFrame(rows)
    out_path = paths.data_processed / "backtest_props_metrics.csv"
    try:
        out.to_csv(out_path, index=False)
    except Exception:
        pass
    return out
