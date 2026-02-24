import json
import os
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
PROC_DIR = BASE_DIR / "data" / "processed"
OUT_JSON = PROC_DIR / "props_prob_calibration.json"


def load_reliability_bins(path: Path) -> Tuple[List[float], List[float]]:
    df = pd.read_csv(path)
    if df.empty:
        raise RuntimeError("reliability_props.csv is empty")
    # Use avg_model_prob (x) -> hit_rate (y)
    xs = df["avg_model_prob"].astype(float).tolist()
    ys = df["hit_rate"].astype(float).tolist()
    # Ensure sorted by x
    arr = sorted(zip(xs, ys), key=lambda t: t[0])
    xs = [a for a, _ in arr]
    ys = [b for _, b in arr]
    return xs, ys


def fit_isotonic(xs: List[float], ys: List[float]) -> Tuple[np.ndarray, np.ndarray]:
    from sklearn.isotonic import IsotonicRegression

    # Bound x to [0,1]
    xs_arr = np.clip(np.asarray(xs, dtype=float), 0.0, 1.0)
    ys_arr = np.clip(np.asarray(ys, dtype=float), 0.0, 1.0)
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
    iso.fit(xs_arr, ys_arr)
    # Sample a grid for lightweight JSON calibration
    grid = np.linspace(0.0, 1.0, 51)
    preds = iso.transform(grid)
    return grid, preds


def main() -> None:
    rel_path = PROC_DIR / "reliability_props.csv"
    if not rel_path.exists():
        raise FileNotFoundError(f"Missing reliability bins: {rel_path}")
    xs, ys = load_reliability_bins(rel_path)
    try:
        grid, preds = fit_isotonic(xs, ys)
    except Exception:
        # Fallback: monotone envelope without sklearn
        arr = sorted(zip(xs, ys), key=lambda t: t[0])
        xs2 = np.asarray([a for a, _ in arr], dtype=float)
        ys2 = np.asarray([b for _, b in arr], dtype=float)
        # Cummax to enforce monotonic non-decreasing
        ys_mono = np.maximum.accumulate(ys2)
        # Interpolate to grid
        grid = np.linspace(0.0, 1.0, 51)
        preds = np.interp(grid, xs2, ys_mono)
        preds = np.clip(preds, 0.0, 1.0)

    # Guardrail: only emit a calibration curve if it passes the same sanity
    # checks enforced by the runtime loader in nba_betting.props_edges.
    # Otherwise, write an identity mapping so calibration is an explicit no-op
    # (instead of being silently ignored).
    note = None
    try:
        g = np.asarray(grid, dtype=float)
        p = np.asarray(preds, dtype=float)

        def _interp(xv: float) -> float:
            return float(np.interp(float(xv), g, p))

        y10 = _interp(0.10)
        y50 = _interp(0.50)
        y90 = _interp(0.90)

        sane = True
        if not (0.35 <= y50 <= 0.65):
            sane = False
        if (y90 - y10) < 0.25:
            sane = False
        if not (0.05 <= y10 <= 0.40):
            sane = False
        if not (0.60 <= y90 <= 0.95):
            sane = False

        if not sane:
            preds = grid
            note = f"identity_fallback_due_to_sanity(y10={y10:.3f},y50={y50:.3f},y90={y90:.3f})"
    except Exception:
        preds = grid
        note = "identity_fallback_due_to_exception"
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as fh:
        obj = {"x": [float(v) for v in grid], "y": [float(v) for v in preds], "source": rel_path.name}
        if note:
            obj["note"] = str(note)
        json.dump(obj, fh, indent=2)
    print(json.dumps({"ok": True, "json": str(OUT_JSON), "points": len(grid)}, indent=2))


if __name__ == "__main__":
    main()
