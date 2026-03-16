"""Build a time-profile calibration for SmartSim 3-minute interval segments.

Goal
----
Reduce systematic cumulative interval drift while preserving each simulation's total
points.

Method
------
We use the interval evaluation detail CSV produced by tools/evaluate_intervals.py.
For regulation segments 1..16 we build a target scoring shape as a hybrid of:
- actual mean segment share of regulation points
- per-game normalized cumulative share, differenced back into segment shares

This directly targets cumulative drift while still respecting the segment ladder.
The resulting per-segment multipliers are clipped and renormalized so expected game
totals remain unchanged.

Output
------
Writes data/processed/intervals_time_profile.json by default.
SmartSim automatically applies it when building 3-minute interval ladders.
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


def _as_monotone_share(arr: np.ndarray) -> np.ndarray:
    out = np.asarray(arr, dtype=float)
    if out.size <= 0:
        return out
    out = np.where(np.isfinite(out), out, np.nan)
    out = np.clip(out, 0.0, 1.0)
    last = 0.0
    for i in range(out.size):
        if not np.isfinite(out[i]):
            out[i] = last
        else:
            out[i] = max(last, float(out[i]))
        last = float(out[i])
    out[-1] = 1.0
    return np.clip(out, 0.0, 1.0)


def _segment_shares_from_cumulative(df: pd.DataFrame, seg_count: int) -> tuple[np.ndarray, np.ndarray]:
    key_cols = [c for c in ["date", "home_tri", "away_tri", "game_id", "file"] if c in df.columns]
    if not key_cols:
        return np.full(seg_count, np.nan, dtype=float), np.full(seg_count, np.nan, dtype=float)

    work = df.copy()
    work["_game_key"] = work[key_cols].astype(str).agg("|".join, axis=1)
    work = work.sort_values(["_game_key", "seg_idx"], kind="stable")

    finals = work.groupby("_game_key", as_index=False).tail(1)[["_game_key", "act_cum_total", "pred_cum_mu"]].copy()
    finals = finals.rename(columns={"act_cum_total": "act_reg_total", "pred_cum_mu": "pred_reg_total"})
    work = work.merge(finals, on="_game_key", how="left")

    work["act_reg_total"] = pd.to_numeric(work["act_reg_total"], errors="coerce")
    work["pred_reg_total"] = pd.to_numeric(work["pred_reg_total"], errors="coerce")
    work["act_cum_share"] = pd.to_numeric(work["act_cum_total"], errors="coerce") / work["act_reg_total"]
    work["pred_cum_share"] = pd.to_numeric(work["pred_cum_mu"], errors="coerce") / work["pred_reg_total"]

    act_cum = work.groupby("seg_idx", as_index=True)["act_cum_share"].mean().reindex(range(1, seg_count + 1))
    pred_cum = work.groupby("seg_idx", as_index=True)["pred_cum_share"].mean().reindex(range(1, seg_count + 1))

    act_cum_arr = _as_monotone_share(act_cum.to_numpy(dtype=float))
    pred_cum_arr = _as_monotone_share(pred_cum.to_numpy(dtype=float))
    act_seg = np.diff(np.concatenate([[0.0], act_cum_arr]))
    pred_seg = np.diff(np.concatenate([[0.0], pred_cum_arr]))
    return act_seg, pred_seg


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=False)
    ap.add_argument("--end", type=str, required=False)
    ap.add_argument("--detail", type=str, required=False, help="Path to intervals eval detail CSV")
    ap.add_argument("--out", type=str, required=False, help="Output JSON path")
    ap.add_argument("--clip-lo", type=float, default=0.85)
    ap.add_argument("--clip-hi", type=float, default=1.10)
    ap.add_argument("--min-rows-per-seg", type=int, default=50)
    ap.add_argument("--cum-weight", type=float, default=1.00, help="Blend weight on per-game cumulative-share calibration")
    args = ap.parse_args()

    if args.detail:
        detail_path = Path(args.detail)
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --detail OR --start and --end")
        detail_path = _infer_detail_path(args.start, args.end)

    if not detail_path.exists():
        raise SystemExit(f"Missing detail CSV: {detail_path}. Run tools/evaluate_intervals.py first.")

    df = pd.read_csv(detail_path)
    if df is None or df.empty:
        raise SystemExit(f"Detail CSV is empty: {detail_path}")

    needed = ["seg_idx", "act_seg_total", "pred_seg_mu", "act_cum_total", "pred_cum_mu"]
    for c in needed:
        if c not in df.columns:
            raise SystemExit(f"Detail CSV missing column: {c}")

    df = df.copy()
    df["seg_idx"] = pd.to_numeric(df["seg_idx"], errors="coerce").astype("Int64")
    df["act_seg_total"] = pd.to_numeric(df["act_seg_total"], errors="coerce")
    df["pred_seg_mu"] = pd.to_numeric(df["pred_seg_mu"], errors="coerce")
    df["act_cum_total"] = pd.to_numeric(df["act_cum_total"], errors="coerce")
    df["pred_cum_mu"] = pd.to_numeric(df["pred_cum_mu"], errors="coerce")

    df = df[df["seg_idx"].notna()].copy()
    df = df[(df["seg_idx"] >= 1) & (df["seg_idx"] <= 16)].copy()
    if df.empty:
        raise SystemExit("No regulation interval rows found in detail CSV")

    clip_lo = float(args.clip_lo)
    clip_hi = float(args.clip_hi)
    if not (np.isfinite(clip_lo) and np.isfinite(clip_hi) and clip_lo > 0 and clip_hi > 0 and clip_lo <= clip_hi):
        raise SystemExit("Invalid clip bounds")

    seg_diag: list[dict[str, Any]] = []
    pred_means = []
    act_means = []
    counts = []
    for si in range(1, 17):
        sdf = df[df["seg_idx"] == si]
        counts.append(int(len(sdf)))
        act_means.append(float(sdf["act_seg_total"].mean()) if len(sdf) else float("nan"))
        pred_means.append(float(sdf["pred_seg_mu"].mean()) if len(sdf) else float("nan"))

    pred_means_arr = np.asarray([_safe_float(x) for x in pred_means], dtype=float)
    act_means_arr = np.asarray([_safe_float(x) for x in act_means], dtype=float)

    pred_total = float(np.nansum(pred_means_arr))
    act_total = float(np.nansum(act_means_arr))
    if (not np.isfinite(pred_total)) or pred_total <= 0 or (not np.isfinite(act_total)) or act_total <= 0:
        raise SystemExit("Unable to build interval profile: bad regulation totals")

    pred_share_seg = pred_means_arr / pred_total
    act_share_seg = act_means_arr / act_total
    act_share_cum, pred_share_cum = _segment_shares_from_cumulative(df, seg_count=16)

    cum_weight = float(np.clip(float(args.cum_weight), 0.0, 1.0))
    target_share = np.copy(act_share_seg)
    raw_arr = np.ones(16, dtype=float)
    for idx in range(16):
        n = int(counts[idx])
        act_seg_share = float(act_share_seg[idx]) if np.isfinite(act_share_seg[idx]) else float("nan")
        pred_seg_share = float(pred_share_seg[idx]) if np.isfinite(pred_share_seg[idx]) else float("nan")
        act_cum_share = float(act_share_cum[idx]) if np.isfinite(act_share_cum[idx]) else float("nan")
        pred_cum_share = float(pred_share_cum[idx]) if np.isfinite(pred_share_cum[idx]) else float("nan")

        use_seg = n >= int(args.min_rows_per_seg) and np.isfinite(act_seg_share) and act_seg_share >= 0.0
        use_cum = n >= int(args.min_rows_per_seg) and np.isfinite(act_cum_share) and act_cum_share >= 0.0
        if use_seg and use_cum:
            target = ((1.0 - cum_weight) * act_seg_share) + (cum_weight * act_cum_share)
            reason = "hybrid_share"
        elif use_cum:
            target = act_cum_share
            reason = "cum_share"
        elif use_seg:
            target = act_seg_share
            reason = "seg_share"
        else:
            target = pred_seg_share if np.isfinite(pred_seg_share) and pred_seg_share > 0 else (1.0 / 16.0)
            reason = "fallback"

        target_share[idx] = float(target)
        if np.isfinite(pred_seg_share) and pred_seg_share > 0 and np.isfinite(target) and target > 0:
            raw_arr[idx] = float(target / pred_seg_share)
        else:
            raw_arr[idx] = 1.0

        seg_diag.append(
            {
                "seg_idx": int(idx + 1),
                "n": n,
                "act_mean": float(act_means_arr[idx]) if np.isfinite(act_means_arr[idx]) else float("nan"),
                "pred_mean": float(pred_means_arr[idx]) if np.isfinite(pred_means_arr[idx]) else float("nan"),
                "pred_share": pred_seg_share,
                "pred_share_cum": pred_cum_share,
                "act_share_seg": act_seg_share,
                "act_share_cum": act_cum_share,
                "target_share": float(target_share[idx]) if np.isfinite(target_share[idx]) else float("nan"),
                "mult_raw": float(raw_arr[idx]),
                "reason": reason,
            }
        )

    target_share = np.where(np.isfinite(target_share) & (target_share > 0), target_share, pred_share_seg)
    target_share = np.clip(target_share, 1e-6, None)
    target_share = target_share / float(np.sum(target_share))

    raw_arr = np.where(np.isfinite(raw_arr) & (raw_arr > 0), raw_arr, 1.0)
    raw_arr = np.clip(raw_arr, clip_lo, clip_hi)

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
        mults_final = np.clip(mults_final, clip_lo, clip_hi)
        note = "ok"

    for idx, row in enumerate(seg_diag):
        row["mult_clipped"] = float(raw_arr[idx])
        row["mult_final"] = float(mults_final[idx])

    out_path = Path(args.out) if args.out else (PROCESSED / "intervals_time_profile.json")
    out = {
        "source": "intervals_time_profile",
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_detail": str(detail_path),
        "clip": [clip_lo, clip_hi],
        "note": note,
        "cum_weight": cum_weight,
        "segment_multipliers": [float(x) for x in mults_final.tolist()],
        "diagnostics": {
            "renorm": renorm,
            "target_share": [float(x) for x in target_share.tolist()],
            "pred_share": [float(x) for x in pred_share_seg.tolist()],
            "per_segment": seg_diag,
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
