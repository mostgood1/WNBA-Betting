from __future__ import annotations

"""Drift monitoring for core games features.

Computes PSI (Population Stability Index) and KS statistic comparing a recent
"current" window vs a longer "reference" window. Intended for daily runtime
to catch feature distribution shifts that may degrade calibration.

Reference window: --ref-days prior to --date (exclusive of current day)
Current window:   --cur-days prior to --date (exclusive of current day)

Usage:
  python tools/drift_monitor.py --date 2025-12-01 --ref-days 30 --cur-days 7

Writes: data/processed/drift_games_<date>.csv
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import math
import sys
import pandas as pd
import numpy as np
try:
    from nba_betting.features import build_features
except Exception:
    build_features = None

PROCESSED = Path("data/processed")

FEATURES = [
    "elo_diff",
    "home_rest_days", "visitor_rest_days",
    "home_form_off_5", "home_form_def_5", "visitor_form_off_5", "visitor_form_def_5",
    "home_games_last3", "visitor_games_last3", "home_games_last5", "visitor_games_last5",
    "home_3in4", "visitor_3in4", "home_4in6", "visitor_4in6",
]

def _load_recon(date: datetime) -> pd.DataFrame:
    p = PROCESSED / f"recon_games_{date:%Y-%m-%d}.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()

def build_window(end_date: datetime, days: int) -> pd.DataFrame:
    rows = []
    for i in range(1, days + 1):  # look back excluding end_date
        d = end_date - timedelta(days=i)
        df = _load_recon(d)
        if not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def psi(ref: np.ndarray, cur: np.ndarray, bins: int = 10) -> float:
    ref = ref[~np.isnan(ref)]
    cur = cur[~np.isnan(cur)]
    if len(ref) == 0 or len(cur) == 0:
        return np.nan
    # Quantile-based bin edges from reference (ensure uniqueness)
    qs = np.linspace(0, 1, bins + 1)
    edges = np.unique(np.quantile(ref, qs))
    # Fallback to min/max bins if too few unique edges
    if len(edges) < 3:
        edges = np.array([ref.min(), ref.mean(), ref.max()])
    # Digitize (last bin inclusive)
    def proportions(x: np.ndarray) -> np.ndarray:
        if len(x) == 0:
            return np.zeros(len(edges) - 1)
        bins_idx = np.digitize(x, edges[1:-1], right=True)
        counts = np.bincount(bins_idx, minlength=len(edges) - 1)
        return counts / counts.sum() if counts.sum() > 0 else np.zeros(len(counts))
    pr = proportions(ref)
    pc = proportions(cur)
    eps = 1e-8
    return float(np.sum((pc - pr) * np.log((pc + eps) / (pr + eps))))

def ks_stat(ref: np.ndarray, cur: np.ndarray) -> float:
    ref = ref[~np.isnan(ref)]
    cur = cur[~np.isnan(cur)]
    if len(ref) == 0 or len(cur) == 0:
        return np.nan
    # Combined sorted values
    combined = np.sort(np.concatenate([ref, cur]))
    if len(combined) == 0:
        return np.nan
    def ecdf(x: np.ndarray, points: np.ndarray) -> np.ndarray:
        return np.searchsorted(np.sort(x), points, side="right") / len(x)
    cdf_ref = ecdf(ref, combined)
    cdf_cur = ecdf(cur, combined)
    return float(np.max(np.abs(cdf_ref - cdf_cur)))

def main():
    ap = argparse.ArgumentParser(description="Compute drift metrics (PSI, KS) for games features")
    ap.add_argument("--date", required=True, help="Anchor date (YYYY-MM-DD)")
    ap.add_argument("--ref-days", type=int, default=30, help="Reference window days")
    ap.add_argument("--cur-days", type=int, default=7, help="Current window days")
    ap.add_argument("--out", help="Override output path")
    args = ap.parse_args()

    try:
        anchor = datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("Invalid --date format; expected YYYY-MM-DD", file=sys.stderr)
        sys.exit(2)

    ref_df = build_window(anchor, args.ref_days + args.cur_days)  # include both periods
    if ref_df.empty:
        print("No data for reference+current window; drift skipped")
        return

    # If feature columns are missing, attempt deriving via build_features
    have_any = any(col in ref_df.columns for col in FEATURES)
    df_for_split = ref_df
    if not have_any and build_features is not None:
        base_cols = ["date", "home_team", "visitor_team", "home_pts", "visitor_pts"]
        missing = [c for c in base_cols if c not in ref_df.columns]
        if not missing:
            try:
                # Ensure date is datetime
                df_for_split = build_features(ref_df)
            except Exception:
                df_for_split = ref_df

    # Split into reference (older) and current (most recent cur_days)
    ref_cutoff = anchor - timedelta(days=args.cur_days)
    ref_dates = pd.to_datetime(df_for_split.get("date", ref_cutoff), errors="coerce")
    ref_part = df_for_split[ref_dates < ref_cutoff]
    cur_part = df_for_split[ref_dates >= ref_cutoff]

    if ref_part.empty or cur_part.empty:
        print("Insufficient data in one of the windows; drift skipped")
        return

    rows = []
    for feat in FEATURES:
        if feat not in ref_part.columns or feat not in cur_part.columns:
            continue
        rvals = pd.to_numeric(ref_part[feat], errors="coerce").to_numpy()
        cvals = pd.to_numeric(cur_part[feat], errors="coerce").to_numpy()
        ref_mean = float(np.nanmean(rvals)) if len(rvals) else np.nan
        cur_mean = float(np.nanmean(cvals)) if len(cvals) else np.nan
        ref_std = float(np.nanstd(rvals)) if len(rvals) else np.nan
        cur_std = float(np.nanstd(cvals)) if len(cvals) else np.nan
        psi_val = psi(rvals, cvals)
        ks_val = ks_stat(rvals, cvals)
        rows.append({
            "feature": feat,
            "ref_count": int(np.sum(~np.isnan(rvals))),
            "cur_count": int(np.sum(~np.isnan(cvals))),
            "ref_mean": ref_mean,
            "cur_mean": cur_mean,
            "ref_std": ref_std,
            "cur_std": cur_std,
            "psi": psi_val,
            "ks": ks_val,
            "psi_flag": psi_val > 0.2 if not math.isnan(psi_val) else False,
            "psi_severe": psi_val > 0.3 if not math.isnan(psi_val) else False,
            "ks_flag": ks_val > 0.1 if not math.isnan(ks_val) else False,
        })

    if not rows:
        print("No overlapping feature columns found; drift report skipped")
        return
    out_df = pd.DataFrame(rows).sort_values("psi", ascending=False)
    out_path = Path(args.out) if args.out else PROCESSED / f"drift_games_{anchor:%Y-%m-%d}.csv"
    out_df.to_csv(out_path, index=False)
    severe = out_df[out_df.psi_severe]
    flagged = out_df[out_df.psi_flag]
    print(f"Drift report written: {out_path}")
    if len(severe):
        print(f"SEVERE drift features (psi>0.3): {', '.join(severe.feature.tolist())}")
    elif len(flagged):
        print(f"Moderate drift features (psi>0.2): {', '.join(flagged.feature.tolist())}")
    else:
        print("No drift flags (psi <= 0.2)")

if __name__ == "__main__":
    main()
