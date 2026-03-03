"""Build a simple *time-profile* calibration for SmartSim 3-minute interval segments.

Goal
----
Reduce systematic *cumulative* interval drift (e.g. early/late scoring shape) while
preserving each simulation's total points.

Method
------
We use the interval evaluation detail CSV produced by `tools/evaluate_intervals.py`
(or via `tools/interval_drift_report.py`). For each regulation segment `seg_idx`:
- compute mean actual segment points
- compute mean predicted segment mean (`pred_seg_mu`)
- set raw multiplier m_j = act_mean / pred_mean

Then we:
- clip multipliers to a conservative range (default 0.90..1.10)
- renormalize so the expected total is preserved:
    sum_j (m_j * pred_mean_j) == sum_j pred_mean_j

Output
------
Writes `data/processed/intervals_time_profile.json` by default.
SmartSim will automatically apply it when building 3-minute interval ladders.

Usage
-----
python tools/build_intervals_time_profile.py --start 2026-01-14 --end 2026-02-12
python tools/build_intervals_time_profile.py --detail data/processed/intervals_eval_detail_...csv
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROCESSED = DATA_ROOT / "processed"


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


def _infer_detail_path(start: str, end: str) -> Path:
    return PROCESSED / f"intervals_eval_detail_{start}_{end}.csv"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=False)
    ap.add_argument("--end", type=str, required=False)
    ap.add_argument("--detail", type=str, required=False, help="Path to intervals eval detail CSV")
    ap.add_argument("--out", type=str, required=False, help="Output JSON path")
    ap.add_argument("--clip-lo", type=float, default=0.90)
    ap.add_argument("--clip-hi", type=float, default=1.10)
    ap.add_argument("--min-rows-per-seg", type=int, default=50)
    args = ap.parse_args()

    if args.detail:
        detail_path = Path(args.detail)
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --detail OR --start and --end")
        detail_path = _infer_detail_path(args.start, args.end)

    if not detail_path.exists():
        raise SystemExit(f"Missing detail CSV: {detail_path}. Run tools/interval_drift_report.py first.")

    df = pd.read_csv(detail_path)
    if df is None or df.empty:
        raise SystemExit(f"Detail CSV is empty: {detail_path}")

    needed = ["seg_idx", "act_seg_total", "pred_seg_mu"]
    for c in needed:
        if c not in df.columns:
            raise SystemExit(f"Detail CSV missing column: {c}")

    df = df.copy()
    df["seg_idx"] = pd.to_numeric(df["seg_idx"], errors="coerce").astype("Int64")
    df["act_seg_total"] = pd.to_numeric(df["act_seg_total"], errors="coerce")
    df["pred_seg_mu"] = pd.to_numeric(df["pred_seg_mu"], errors="coerce")

    # Regulation segments only.
    df = df[df["seg_idx"].notna()].copy()
    df = df[(df["seg_idx"] >= 1) & (df["seg_idx"] <= 16)].copy()

    seg_mults: list[float] = []
    seg_diag: list[dict[str, Any]] = []

    clip_lo = float(args.clip_lo)
    clip_hi = float(args.clip_hi)
    if not (np.isfinite(clip_lo) and np.isfinite(clip_hi) and clip_lo > 0 and clip_hi > 0 and clip_lo <= clip_hi):
        raise SystemExit("Invalid clip bounds")

    # Build per-seg means.
    pred_means = []
    raw = []
    for si in range(1, 17):
        sdf = df[df["seg_idx"] == si]
        n = int(len(sdf))
        act_mean = float(sdf["act_seg_total"].mean()) if n else float("nan")
        pred_mean = float(sdf["pred_seg_mu"].mean()) if n else float("nan")

        if n < int(args.min_rows_per_seg) or not np.isfinite(act_mean) or not np.isfinite(pred_mean) or pred_mean <= 0:
            m = 1.0
            reason = "fallback"
        else:
            m = float(act_mean / pred_mean)
            reason = "ratio"

        m_clipped = float(np.clip(m, clip_lo, clip_hi))

        pred_means.append(pred_mean if np.isfinite(pred_mean) else float("nan"))
        raw.append(m_clipped)

        seg_diag.append(
            {
                "seg_idx": int(si),
                "n": n,
                "act_mean": act_mean,
                "pred_mean": pred_mean,
                "mult_raw": float(m),
                "mult_clipped": m_clipped,
                "reason": reason,
            }
        )

    pred_means_arr = np.asarray([_safe_float(x) for x in pred_means], dtype=float)
    raw_arr = np.asarray([_safe_float(x) for x in raw], dtype=float)

    # Renormalize to preserve expected total.
    # We want sum(raw*m_pred) == sum(m_pred). If pred means missing, fall back to no-op.
    ok = np.isfinite(pred_means_arr) & (pred_means_arr > 0) & np.isfinite(raw_arr) & (raw_arr > 0)
    if not bool(np.all(ok)):
        mults_final = np.ones(16, dtype=float)
        renorm = 1.0
        note = "missing_pred_means"
    else:
        denom = float(np.sum(raw_arr * pred_means_arr))
        numer = float(np.sum(pred_means_arr))
        renorm = float(numer / denom) if denom > 0 else 1.0
        if not np.isfinite(renorm) or renorm <= 0:
            renorm = 1.0
        mults_final = raw_arr * renorm
        # Safety: re-clip after renorm.
        mults_final = np.clip(mults_final, clip_lo, clip_hi)
        note = "ok"

    out_path = Path(args.out) if args.out else (PROCESSED / "intervals_time_profile.json")
    out = {
        "source": "intervals_time_profile",
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_detail": str(detail_path),
        "clip": [clip_lo, clip_hi],
        "note": note,
        "segment_multipliers": [float(x) for x in mults_final.tolist()],
        "diagnostics": {
            "renorm": renorm,
            "per_segment": seg_diag,
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
