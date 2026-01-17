import json
from datetime import date, timedelta
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

try:
    from sklearn.isotonic import IsotonicRegression
except Exception:
    IsotonicRegression = None

BASE_DIR = Path(__file__).resolve().parent.parent
PROC_DIR = BASE_DIR / "data" / "processed"
DEFAULT_OUT_JSON = PROC_DIR / "games_prob_calibration.json"
DEFAULT_OUT_BINS = PROC_DIR / "reliability_games.csv"


def collect_window(days: int) -> pd.DataFrame:
    today = date.today()
    rows = []
    for i in range(days):
        d = today - timedelta(days=i)
        preds = PROC_DIR / f"predictions_{d:%Y-%m-%d}.csv"
        recon = PROC_DIR / f"recon_games_{d:%Y-%m-%d}.csv"
        if not (preds.exists() and recon.exists()):
            continue
        try:
            p = pd.read_csv(preds)
            r = pd.read_csv(recon)
            # standardize columns
            p = p.rename(columns={"visitor_team": "away_team"})
            r = r.rename(columns={"visitor_team": "away_team"})
            m = pd.merge(
                p[["home_team", "away_team", "home_win_prob"]],
                r[["home_team", "away_team", "home_pts", "visitor_pts"]],
                on=["home_team", "away_team"],
                how="inner",
            )
            if not m.empty:
                rows.append(m.assign(date=f"{d:%Y-%m-%d}"))
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["date","home_team","away_team","home_win_prob","home_pts","visitor_pts"])
    return pd.concat(rows, ignore_index=True)


def reliability_bins(df: pd.DataFrame, bins: int = 10) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    # Compute actual home win indicator
    df = df.copy()
    df["home_win"] = (df["home_pts"].astype(float) > df["visitor_pts"].astype(float)).astype(int)
    # Bin by predicted prob
    df["bin"] = pd.qcut(df["home_win_prob"].astype(float), q=bins, duplicates="drop")
    grp = df.groupby("bin", observed=False)
    xs = grp["home_win_prob"].mean().values
    ys = grp["home_win"].mean().values
    ns = grp.size().values
    # sort by x
    order = np.argsort(xs)
    return xs[order], ys[order], ns[order]


def fit_isotonic(xs: np.ndarray, ys: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    xs_arr = np.clip(xs.astype(float), 0.0, 1.0)
    ys_arr = np.clip(ys.astype(float), 0.0, 1.0)
    if IsotonicRegression is not None:
        iso = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
        iso.fit(xs_arr, ys_arr)
        grid = np.linspace(0.0, 1.0, 51)
        preds = iso.transform(grid)
        return grid, preds
    # Fallback monotone envelope
    ys_mono = np.maximum.accumulate(ys_arr)
    grid = np.linspace(0.0, 1.0, 51)
    preds = np.interp(grid, xs_arr, ys_mono)
    preds = np.clip(preds, 0.0, 1.0)
    return grid, preds


def main(days: int = 60, bins: int = 10) -> None:
    df = collect_window(days)
    if df.empty:
        print(json.dumps({"ok": False, "reason": "no-data"}))
        return
    xs, ys, ns = reliability_bins(df, bins=bins)
    grid, preds = fit_isotonic(xs, ys)
    # Write bins CSV + JSON calibrator (windowed)
    out_bins = PROC_DIR / f"reliability_games_{days}.csv"
    out_json = PROC_DIR / f"games_prob_calibration_{days}.json"
    out_bins.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"avg_model_prob": xs, "hit_rate": ys, "n": ns}).to_csv(out_bins, index=False)
    with open(out_json, "w", encoding="utf-8") as fh:
        json.dump({"x": [float(v) for v in grid], "y": [float(v) for v in preds], "source": out_bins.name}, fh, indent=2)
    # Backward-compatible copies (non-windowed) for other codepaths
    try:
        DEFAULT_OUT_BINS.write_text(out_bins.read_text(encoding="utf-8"), encoding="utf-8")
        DEFAULT_OUT_JSON.write_text(out_json.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        pass
    print(json.dumps({
        "ok": True,
        "bins_csv": str(out_bins),
        "json": str(out_json),
        "bins_csv_default": str(DEFAULT_OUT_BINS),
        "json_default": str(DEFAULT_OUT_JSON),
    }, indent=2))


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--bins", type=int, default=10)
    args = ap.parse_args()
    main(days=args.days, bins=args.bins)
