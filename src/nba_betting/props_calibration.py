from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np

from .config import paths


STAT_KEYS = ["pts", "reb", "ast", "threes", "pra"]
PRED_COLS = {
    "pts": "pred_pts",
    "reb": "pred_reb",
    "ast": "pred_ast",
    "threes": "pred_threes",
    "pra": "pred_pra",
}


def _list_recent_dates(anchor_date: str, days: int) -> List[str]:
    d = pd.to_datetime(anchor_date).date()
    out: List[str] = []
    for i in range(1, days + 1):  # lookback excludes anchor (today)
        out.append(str(d - pd.Timedelta(days=i)))
    return out


def _read_predictions_for_date(date_str: str) -> Optional[pd.DataFrame]:
    p = paths.data_processed / f"props_predictions_{date_str}.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _read_recon_for_date(date_str: str) -> Optional[pd.DataFrame]:
    p = paths.data_processed / f"recon_props_{date_str}.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def compute_biases(anchor_date: str, window_days: int = 7, min_pairs: int = 50) -> Dict[str, float]:
    """Compute per-stat intercept bias using last N days of (actual - predicted).

    - anchor_date: YYYY-MM-DD (today) – we look back 1..N days
    - window_days: lookback horizon
    - min_pairs: require at least this many matched rows overall to apply calibration

    Returns dict like {"pts": +0.3, "reb": -0.1, ...}. Missing stats default to 0.0.
    """
    dates = _list_recent_dates(anchor_date, window_days)
    errs: Dict[str, List[float]] = {k: [] for k in STAT_KEYS}
    total_pairs = 0
    for ds in dates:
        pred = _read_predictions_for_date(ds)
        recon = _read_recon_for_date(ds)
        if pred is None or recon is None or pred.empty or recon.empty:
            continue
        # Prefer joining by player_id if available; else by player_name (+ team if present)
        join_keys: List[Tuple[str, str]] = []
        if "player_id" in pred.columns and "player_id" in recon.columns:
            join_keys = [("player_id", "player_id")]
        elif "player_name" in pred.columns and "player_name" in recon.columns:
            join_keys = [("player_name", "player_name")]
            if "team" in pred.columns and "team_abbr" in recon.columns:
                join_keys.append(("team", "team_abbr"))
        else:
            continue
        left_on = [a for a, _ in join_keys]
        right_on = [b for _, b in join_keys]
        try:
            merged = pred.merge(recon, left_on=left_on, right_on=right_on, how="inner", suffixes=("_pred", "_act"))
        except Exception:
            continue
        if merged.empty:
            continue
        # Collect errors actual - predicted per stat
        for stat in STAT_KEYS:
            pc = PRED_COLS.get(stat)
            if pc in merged.columns and stat in merged.columns:
                try:
                    a = pd.to_numeric(merged[stat], errors="coerce")
                    p = pd.to_numeric(merged[pc], errors="coerce")
                    diff = (a - p).astype(float)
                    # Drop extreme outliers to keep it stable
                    q1, q99 = np.nanpercentile(diff.dropna(), [1, 99]) if diff.notna().sum() > 10 else (np.nan, np.nan)
                    if not np.isnan(q1) and not np.isnan(q99):
                        mask = (diff >= q1) & (diff <= q99)
                        vals = diff[mask].tolist()
                    else:
                        vals = diff.tolist()
                    errs[stat].extend([x for x in vals if np.isfinite(x)])
                except Exception:
                    continue
        total_pairs += int(len(merged))
    # If not enough data, return zeros
    if total_pairs < min_pairs:
        return {k: 0.0 for k in STAT_KEYS}
    # Use median error (robust) as intercept
    biases: Dict[str, float] = {}
    for stat in STAT_KEYS:
        arr = np.array(errs.get(stat, []), dtype=float)
        if arr.size == 0:
            biases[stat] = 0.0
            continue
        b = float(np.nanmedian(arr))
        # Light safety caps by stat
        caps = {"pts": 2.5, "reb": 1.5, "ast": 1.5, "threes": 0.8, "pra": 3.5}
        cap = caps.get(stat, 3.0)
        if b > cap:
            b = cap
        if b < -cap:
            b = -cap
        biases[stat] = b
    return biases


def apply_biases(preds: pd.DataFrame, biases: Dict[str, float]) -> pd.DataFrame:
    out = preds.copy()
    for stat, pc in PRED_COLS.items():
        if pc in out.columns and stat in biases:
            try:
                out[pc] = pd.to_numeric(out[pc], errors="coerce") + float(biases[stat])
            except Exception:
                continue
    return out


def save_calibration(biases: Dict[str, float], anchor_date: str, window_days: int) -> Path:
    obj = {
        "date": anchor_date,
        "window_days": int(window_days),
        "biases": {k: float(v) for k, v in biases.items()},
    }
    # Write dated calibration for reproducibility
    out = paths.data_processed / f"props_calibration_{anchor_date}.json"
    # Also write/update a latest pointer file for convenience
    latest = paths.data_processed / "props_calibration.json"
    try:
        out.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        try:
            latest.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        except Exception:
            pass
    except Exception:
        pass
    return out
