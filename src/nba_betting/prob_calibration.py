from __future__ import annotations

import json
from typing import Any, Optional

import numpy as np
import pandas as pd

from .config import paths


_PROB_CALIBRATION_INDEX: Optional[list[tuple[pd.Timestamp, Any]]] = None
_PROB_CALIBRATION_CACHE: dict[str, Optional[dict[str, Any]]] = {}


def _calibration_period_prob_paths() -> list[Any]:
    """Return calibration artifacts with repo copies preferred over active data root."""
    chosen: dict[pd.Timestamp, Any] = {}
    roots = [paths.repo_data_processed, paths.data_processed]
    for root in roots:
        try:
            for fp in root.glob("calibration_period_probs_*.json"):
                ds = fp.name.replace("calibration_period_probs_", "").replace(".json", "")
                try:
                    dt = pd.to_datetime(ds).normalize()
                except Exception:
                    continue
                if dt not in chosen:
                    chosen[dt] = fp
        except Exception:
            continue
    return [chosen[dt] for dt in sorted(chosen.keys())]


def _clamp01(x: Any, default: float = 0.5) -> float:
    try:
        v = float(x)
        if not np.isfinite(v):
            return float(default)
        return float(max(0.0, min(1.0, v)))
    except Exception:
        return float(default)


def _load_prob_calibration_for_date(date_str: str) -> Optional[dict[str, Any]]:
    """Load the most recent period probability calibration JSON on/before (date-1).

    Looks for data/processed/calibration_period_probs_YYYY-MM-DD.json.

    Returns the JSON object (dict) or None.
    """
    try:
        target = pd.to_datetime(date_str).normalize()
    except Exception:
        return None

    key = str(target.date())
    if key in _PROB_CALIBRATION_CACHE:
        return _PROB_CALIBRATION_CACHE[key]

    cutoff = target - pd.Timedelta(days=1)

    global _PROB_CALIBRATION_INDEX
    if _PROB_CALIBRATION_INDEX is None:
        idx: list[tuple[pd.Timestamp, Any]] = []
        try:
            for fp in _calibration_period_prob_paths():
                ds = fp.name.replace("calibration_period_probs_", "").replace(".json", "")
                try:
                    dt = pd.to_datetime(ds).normalize()
                except Exception:
                    continue
                idx.append((dt, fp))
        except Exception:
            idx = []
        _PROB_CALIBRATION_INDEX = sorted(idx, key=lambda t: t[0])

    best_fp = None
    try:
        for dt, fp in _PROB_CALIBRATION_INDEX or []:
            if dt <= cutoff:
                best_fp = fp
            else:
                break
    except Exception:
        best_fp = None

    if best_fp is None:
        _PROB_CALIBRATION_CACHE[key] = None
        return None

    try:
        obj = json.loads(best_fp.read_text(encoding="utf-8"))
        _PROB_CALIBRATION_CACHE[key] = obj if isinstance(obj, dict) else None
        return _PROB_CALIBRATION_CACHE[key]
    except Exception:
        _PROB_CALIBRATION_CACHE[key] = None
        return None


def calibrate_prob(date_str: str, market: str, p: float) -> float:
    """Calibrate a raw probability using the latest available calibration artifact.

    Args:
        date_str: slate date (YYYY-MM-DD). Calibration uses <= date-1.
        market: market key, e.g. "q1_over", "h1_over".
        p: raw probability in [0,1].

    Returns:
        Calibrated probability in [0,1]. Falls back to raw if no calibration.
    """
    p = _clamp01(p, default=0.5)
    cal = _load_prob_calibration_for_date(date_str) or {}
    markets = cal.get("markets") if isinstance(cal, dict) else None
    if not isinstance(markets, dict):
        return p

    m = markets.get(str(market))
    if not isinstance(m, dict):
        return p

    edges = m.get("bin_edges")
    vals = m.get("p_cal")
    n_bin = m.get("n_bin")
    if not isinstance(edges, list) or not isinstance(vals, list) or len(edges) < 2 or len(vals) < 1:
        return p

    try:
        e = np.asarray(edges, dtype=float)
        v = np.asarray(vals, dtype=float)
        nb = np.asarray(n_bin, dtype=float) if isinstance(n_bin, list) else None
        e = e[np.isfinite(e)]
        v = v[np.isfinite(v)]
        if e.size < 2 or v.size < 1:
            return p

        # bins are [e[i], e[i+1]) except last which includes 1.0
        idx = int(np.digitize([p], e[1:-1], right=False)[0])
        idx = max(0, min(idx, int(v.size - 1)))
        p_cal = _clamp01(float(v[idx]), default=p)

        # Blend calibrated probability toward raw when sample size is small.
        # This limits overfitting, especially in narrow windows.
        # Weight grows with per-bin sample size.
        try:
            if nb is None or nb.size == 0:
                return p_cal
            n_here = float(nb[min(idx, int(nb.size - 1))])
            prior = 25.0
            w = float(n_here / (n_here + prior)) if (n_here >= 0.0) else 0.0
            w = float(max(0.0, min(1.0, w)))
            return _clamp01(((1.0 - w) * p) + (w * p_cal), default=p)
        except Exception:
            return p_cal
    except Exception:
        return p
