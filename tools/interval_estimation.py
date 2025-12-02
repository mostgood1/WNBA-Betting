from __future__ import annotations

"""Predictive interval estimation for spreads and totals.

Computes empirical residual standard deviations over a reference window and
evaluates coverage if predictions are available. Produces interval parameters
and optional coverage summary.

Usage:
  python tools/interval_estimation.py --date 2025-12-01 --ref-days 30 --z 1.96

Outputs:
  - data/processed/interval_params_<date>.csv
  - data/processed/interval_coverage_<date>.csv (if predictions exist)
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

PROCESSED = Path("data/processed")

def _load_recon(date: datetime) -> pd.DataFrame:
    p = PROCESSED / f"recon_games_{date:%Y-%m-%d}.csv"
    if p.exists():
        try:
            return pd.read_csv(p)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def build_window(end_date: datetime, days: int) -> pd.DataFrame:
    rows = []
    for i in range(1, days + 1):
        d = end_date - timedelta(days=i)
        df = _load_recon(d)
        if not df.empty:
            rows.append(df)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser(description="Estimate predictive intervals for spread/total")
    ap.add_argument("--date", required=True, help="Anchor date (YYYY-MM-DD)")
    ap.add_argument("--ref-days", type=int, default=30, help="Reference window size")
    ap.add_argument("--z", type=float, default=1.96, help="Z-score for interval half-width")
    args = ap.parse_args()

    anchor = datetime.strptime(args.date, "%Y-%m-%d")
    ref_df = build_window(anchor, args.ref_days)
    if ref_df.empty:
        print("No reference data; interval estimation skipped")
        return

    # Compute residuals if predictions available in recon files
    have_spread_pred = "pred_margin" in ref_df.columns or "spread_margin" in ref_df.columns
    pred_col = "pred_margin" if "pred_margin" in ref_df.columns else ("spread_margin" if "spread_margin" in ref_df.columns else None)
    have_total_pred = "pred_total" in ref_df.columns

    params = []
    if have_spread_pred and "home_pts" in ref_df.columns and "visitor_pts" in ref_df.columns:
        true_margin = pd.to_numeric(ref_df["home_pts"], errors="coerce") - pd.to_numeric(ref_df["visitor_pts"], errors="coerce")
        pred_margin = pd.to_numeric(ref_df[pred_col], errors="coerce")
        resid_spread = (true_margin - pred_margin).dropna().to_numpy()
        sigma_spread = float(np.std(resid_spread)) if len(resid_spread) else np.nan
        params.append({"target": "spread", "sigma": sigma_spread, "z": args.z, "half_width": args.z * sigma_spread})
    else:
        params.append({"target": "spread", "sigma": np.nan, "z": args.z, "half_width": np.nan})

    if have_total_pred and "home_pts" in ref_df.columns and "visitor_pts" in ref_df.columns:
        true_total = pd.to_numeric(ref_df["home_pts"], errors="coerce") + pd.to_numeric(ref_df["visitor_pts"], errors="coerce")
        pred_total = pd.to_numeric(ref_df["pred_total"], errors="coerce")
        resid_total = (true_total - pred_total).dropna().to_numpy()
        sigma_total = float(np.std(resid_total)) if len(resid_total) else np.nan
        params.append({"target": "total", "sigma": sigma_total, "z": args.z, "half_width": args.z * sigma_total})
    else:
        params.append({"target": "total", "sigma": np.nan, "z": args.z, "half_width": np.nan})

    out_params = pd.DataFrame(params)
    out_p = PROCESSED / f"interval_params_{anchor:%Y-%m-%d}.csv"
    out_params.to_csv(out_p, index=False)
    print(f"Interval params written: {out_p}")

    # Coverage over reference window (if predictions exist)
    cov_rows = []
    if have_spread_pred:
        true_margin = pd.to_numeric(ref_df["home_pts"], errors="coerce") - pd.to_numeric(ref_df["visitor_pts"], errors="coerce")
        pred_margin = pd.to_numeric(ref_df[pred_col], errors="coerce")
        sigma_spread = float(out_params.loc[out_params.target == "spread", "sigma"].values[0])
        z = args.z
        low = pred_margin - z * sigma_spread
        high = pred_margin + z * sigma_spread
        covered = ((true_margin >= low) & (true_margin <= high)).astype(float)
        cov_rows.append({"target": "spread", "n": int(np.sum(~np.isnan(covered))), "coverage": float(np.nanmean(covered))})
    if have_total_pred:
        true_total = pd.to_numeric(ref_df["home_pts"], errors="coerce") + pd.to_numeric(ref_df["visitor_pts"], errors="coerce")
        pred_total = pd.to_numeric(ref_df["pred_total"], errors="coerce")
        sigma_total = float(out_params.loc[out_params.target == "total", "sigma"].values[0])
        z = args.z
        low = pred_total - z * sigma_total
        high = pred_total + z * sigma_total
        covered = ((true_total >= low) & (true_total <= high)).astype(float)
        cov_rows.append({"target": "total", "n": int(np.sum(~np.isnan(covered))), "coverage": float(np.nanmean(covered))})

    if cov_rows:
        out_cov = pd.DataFrame(cov_rows)
        out_c = PROCESSED / f"interval_coverage_{anchor:%Y-%m-%d}.csv"
        out_cov.to_csv(out_c, index=False)
        print(f"Interval coverage written: {out_c}")
    else:
        print("No predictions found in reference window; coverage skipped")

if __name__ == "__main__":
    main()
