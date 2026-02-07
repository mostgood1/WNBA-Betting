"""Build calibration scalers to widen (or shrink) SmartSim interval 10-90 bands.

Goal: improve empirical coverage of SmartSim's interval ladder quantiles vs ESPN PBP actuals.

We treat each (prediction, actual) as defining a required scale factor `s` such that,
when the band [p10,p90] is symmetrically widened around p50 by `s`, the actual is
inside the band.

For a given target coverage (default 0.80 for 10-90), we pick the scale as:
  s* = quantile(required_scales, target_coverage)

We compute:
- global scale for segment totals and cumulative totals
- per-segment (seg_idx=1..16) scales for segment totals and cumulative totals

Output JSON defaults to: data/processed/intervals_band_calibration.json

Usage:
  python tools/build_intervals_band_calibration.py --start 2026-01-30 --end 2026-02-05
  python tools/build_intervals_band_calibration.py --detail data/processed/intervals_eval_detail_...csv
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


def _required_scale(act: float, p10: float, p50: float, p90: float) -> float:
    """Minimum symmetric scale about p50 needed to include act in [p10,p90]."""
    if not (np.isfinite(act) and np.isfinite(p10) and np.isfinite(p50) and np.isfinite(p90)):
        return float("nan")
    # Ensure ordering is sane.
    if not (p10 < p50 < p90):
        return float("nan")

    if act <= p50:
        denom = p50 - p10
        if denom <= 0:
            return float("nan")
        return float((p50 - act) / denom)

    denom = p90 - p50
    if denom <= 0:
        return float("nan")
    return float((act - p50) / denom)


def _pick_scale(req: pd.Series, target: float, min_scale: float, max_scale: float) -> float:
    r = pd.to_numeric(req, errors="coerce").dropna().astype(float)
    r = r[np.isfinite(r)]
    if r.empty:
        return float("nan")
    s = float(np.quantile(r.to_numpy(), target))
    if np.isfinite(min_scale):
        s = max(float(min_scale), s)
    if np.isfinite(max_scale):
        s = min(float(max_scale), s)
    return float(s)


def _coverage_with_scale(act: pd.Series, p10: pd.Series, p50: pd.Series, p90: pd.Series, scale: float) -> float:
    a = pd.to_numeric(act, errors="coerce").astype(float)
    lo = pd.to_numeric(p10, errors="coerce").astype(float)
    mid = pd.to_numeric(p50, errors="coerce").astype(float)
    hi = pd.to_numeric(p90, errors="coerce").astype(float)

    m = np.isfinite(a) & np.isfinite(lo) & np.isfinite(mid) & np.isfinite(hi) & (lo < mid) & (mid < hi)
    if not bool(np.any(m)):
        return float("nan")

    a = a[m]
    lo = lo[m]
    mid = mid[m]
    hi = hi[m]

    new_lo = mid - float(scale) * (mid - lo)
    new_hi = mid + float(scale) * (hi - mid)
    cov = ((a >= new_lo) & (a <= new_hi)).astype(float)
    return float(np.mean(cov)) if len(cov) else float("nan")


def _coverage_with_scales(act: pd.Series, p10: pd.Series, p50: pd.Series, p90: pd.Series, scales: pd.Series) -> float:
    a = pd.to_numeric(act, errors="coerce").astype(float)
    lo = pd.to_numeric(p10, errors="coerce").astype(float)
    mid = pd.to_numeric(p50, errors="coerce").astype(float)
    hi = pd.to_numeric(p90, errors="coerce").astype(float)
    s = pd.to_numeric(scales, errors="coerce").astype(float)

    m = np.isfinite(a) & np.isfinite(lo) & np.isfinite(mid) & np.isfinite(hi) & np.isfinite(s) & (s > 0) & (lo < mid) & (mid < hi)
    if not bool(np.any(m)):
        return float("nan")

    a = a[m]
    lo = lo[m]
    mid = mid[m]
    hi = hi[m]
    s = s[m]

    new_lo = mid - s * (mid - lo)
    new_hi = mid + s * (hi - mid)
    cov = ((a >= new_lo) & (a <= new_hi)).astype(float)
    return float(np.mean(cov)) if len(cov) else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=False)
    ap.add_argument("--end", type=str, required=False)
    ap.add_argument("--detail", type=str, required=False, help="Path to intervals eval detail CSV")
    ap.add_argument("--out", type=str, required=False)
    ap.add_argument("--target-coverage", type=float, default=0.80)
    ap.add_argument("--min-scale", type=float, default=1.0)
    ap.add_argument("--max-scale", type=float, default=2.0)
    args = ap.parse_args()

    if args.detail:
        detail_path = Path(args.detail)
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --detail OR --start and --end")
        detail_path = PROCESSED / f"intervals_eval_detail_{args.start}_{args.end}.csv"

    if not detail_path.exists():
        raise SystemExit(f"Missing detail CSV: {detail_path}. Run tools/evaluate_intervals.py first.")

    df = pd.read_csv(detail_path)
    if df is None or df.empty:
        raise SystemExit(f"Detail CSV is empty: {detail_path}")

    needed_cols = [
        "seg_idx",
        "act_seg_total",
        "pred_seg_p10",
        "pred_seg_p50",
        "pred_seg_p90",
        "act_cum_total",
        "pred_cum_p10",
        "pred_cum_p50",
        "pred_cum_p90",
    ]
    for c in needed_cols:
        if c not in df.columns:
            raise SystemExit(f"Detail CSV missing column: {c}")

    df["seg_idx"] = pd.to_numeric(df["seg_idx"], errors="coerce")

    # Compute required scales per row.
    req_seg = []
    req_cum = []
    for _, r in df.iterrows():
        req_seg.append(
            _required_scale(
                _safe_float(r.get("act_seg_total")),
                _safe_float(r.get("pred_seg_p10")),
                _safe_float(r.get("pred_seg_p50")),
                _safe_float(r.get("pred_seg_p90")),
            )
        )
        req_cum.append(
            _required_scale(
                _safe_float(r.get("act_cum_total")),
                _safe_float(r.get("pred_cum_p10")),
                _safe_float(r.get("pred_cum_p50")),
                _safe_float(r.get("pred_cum_p90")),
            )
        )

    df["req_scale_seg"] = pd.to_numeric(pd.Series(req_seg), errors="coerce")
    df["req_scale_cum"] = pd.to_numeric(pd.Series(req_cum), errors="coerce")

    target = float(args.target_coverage)
    min_scale = float(args.min_scale)
    max_scale = float(args.max_scale)

    global_seg = _pick_scale(df["req_scale_seg"], target, min_scale, max_scale)
    global_cum = _pick_scale(df["req_scale_cum"], target, min_scale, max_scale)

    per_segment: dict[str, dict[str, float]] = {}
    for si in range(1, 17):
        sdf = df[df["seg_idx"] == si]
        if sdf.empty:
            continue
        per_segment[str(si)] = {
            "seg": _pick_scale(sdf["req_scale_seg"], target, min_scale, max_scale),
            "cum": _pick_scale(sdf["req_scale_cum"], target, min_scale, max_scale),
        }

    # Report before/after coverage using the chosen scales.
    cov_seg_before = float(pd.to_numeric(df.get("cover_seg_10_90"), errors="coerce").mean()) if "cover_seg_10_90" in df.columns else float("nan")
    cov_cum_before = float(pd.to_numeric(df.get("cover_cum_10_90"), errors="coerce").mean()) if "cover_cum_10_90" in df.columns else float("nan")

    cov_seg_after_global = _coverage_with_scale(df["act_seg_total"], df["pred_seg_p10"], df["pred_seg_p50"], df["pred_seg_p90"], global_seg)
    cov_cum_after_global = _coverage_with_scale(df["act_cum_total"], df["pred_cum_p10"], df["pred_cum_p50"], df["pred_cum_p90"], global_cum)

    # Expected coverage after applying per-segment scales (fallback to global).
    seg_scale_map = {int(k): float((v or {}).get("seg")) for k, v in (per_segment or {}).items() if str(k).isdigit()}
    cum_scale_map = {int(k): float((v or {}).get("cum")) for k, v in (per_segment or {}).items() if str(k).isdigit()}
    seg_scales = pd.to_numeric(df["seg_idx"], errors="coerce").map(seg_scale_map).fillna(global_seg)
    cum_scales = pd.to_numeric(df["seg_idx"], errors="coerce").map(cum_scale_map).fillna(global_cum)
    cov_seg_after_per_segment = _coverage_with_scales(df["act_seg_total"], df["pred_seg_p10"], df["pred_seg_p50"], df["pred_seg_p90"], seg_scales)
    cov_cum_after_per_segment = _coverage_with_scales(df["act_cum_total"], df["pred_cum_p10"], df["pred_cum_p50"], df["pred_cum_p90"], cum_scales)

    # Also report per-segment before/after (per-seg scaling).
    per_segment_diag = []
    for si in range(1, 17):
        sdf = df[df["seg_idx"] == si]
        if sdf.empty:
            continue
        s_seg = float(seg_scale_map.get(si, global_seg))
        s_cum = float(cum_scale_map.get(si, global_cum))
        per_segment_diag.append(
            {
                "seg_idx": int(si),
                "n": int(len(sdf)),
                "scale_seg": s_seg,
                "scale_cum": s_cum,
                "cover_seg_before": float(pd.to_numeric(sdf.get("cover_seg_10_90"), errors="coerce").mean()) if "cover_seg_10_90" in sdf.columns else float("nan"),
                "cover_cum_before": float(pd.to_numeric(sdf.get("cover_cum_10_90"), errors="coerce").mean()) if "cover_cum_10_90" in sdf.columns else float("nan"),
                "cover_seg_after": _coverage_with_scale(sdf["act_seg_total"], sdf["pred_seg_p10"], sdf["pred_seg_p50"], sdf["pred_seg_p90"], s_seg),
                "cover_cum_after": _coverage_with_scale(sdf["act_cum_total"], sdf["pred_cum_p10"], sdf["pred_cum_p50"], sdf["pred_cum_p90"], s_cum),
            }
        )

    out = {
        "source": "intervals_band_calibration",
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_detail": str(detail_path),
        "target_coverage": target,
        "min_scale": min_scale,
        "max_scale": max_scale,
        "global": {
            "seg": global_seg,
            "cum": global_cum,
        },
        "per_segment": per_segment,
        "diagnostics": {
            "n_rows": int(len(df)),
            "coverage_before": {
                "seg_10_90": cov_seg_before,
                "cum_10_90": cov_cum_before,
            },
            "coverage_after_global": {
                "seg_10_90": cov_seg_after_global,
                "cum_10_90": cov_cum_after_global,
            },
            "coverage_after_per_segment": {
                "seg_10_90": cov_seg_after_per_segment,
                "cum_10_90": cov_cum_after_per_segment,
            },
            "per_segment": per_segment_diag,
        },
    }

    out_path = Path(args.out) if args.out else (PROCESSED / "intervals_band_calibration.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(
        {
            "ok": True,
            "out": str(out_path),
            "global_seg": global_seg,
            "global_cum": global_cum,
            "cov_before_seg": cov_seg_before,
            "cov_before_cum": cov_cum_before,
            "cov_after_seg_global": cov_seg_after_global,
            "cov_after_cum_global": cov_cum_after_global,
            "cov_after_seg_per_segment": cov_seg_after_per_segment,
            "cov_after_cum_per_segment": cov_cum_after_per_segment,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
