import json
import os
from pathlib import Path
from typing import List, Tuple, Dict, Any

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
PROC_DIR = BASE_DIR / "data" / "processed"

PRICE_MIN = -150.0
PRICE_MAX = 125.0
BINS = 10
WINDOWS = [30, 60, 90]


def profit_per_unit(price: float) -> float:
    try:
        price = float(price)
    except Exception:
        return np.nan
    return (price/100.0) if price > 0 else (100.0/abs(price))


def reliability_for_window(days: int) -> pd.DataFrame:
    # Load actuals consolidated
    act_parq = PROC_DIR / "props_actuals.parquet"
    if act_parq.exists():
        try:
            actuals = pd.read_parquet(act_parq)
        except Exception:
            actuals = pd.DataFrame()
            for p in PROC_DIR.glob('props_actuals_*.csv'):
                try:
                    df = pd.read_csv(p)
                    actuals = pd.concat([actuals, df], ignore_index=True)
                except Exception:
                    pass
    else:
        actuals = pd.DataFrame()
        for p in PROC_DIR.glob('props_actuals_*.csv'):
            try:
                df = pd.read_csv(p)
                actuals = pd.concat([actuals, df], ignore_index=True)
            except Exception:
                pass
    if actuals is None or actuals.empty:
        raise RuntimeError("no-actuals")
    # Normalize types
    actuals['date'] = pd.to_datetime(actuals['date'], errors='coerce').dt.date
    if 'player_id' in actuals.columns:
        actuals['player_id'] = pd.to_numeric(actuals['player_id'], errors='coerce')
    # Date window
    today = pd.Timestamp.today().date()
    start = today - pd.Timedelta(days=days)
    mask = (actuals['date'] >= start) & (actuals['date'] <= today)
    actuals = actuals.loc[mask].copy()
    if actuals.empty:
        raise RuntimeError("no-actuals-in-window")
    rows = []
    dates = sorted(set(actuals['date']))
    for d in dates:
        ef = PROC_DIR / f'props_edges_{d}.csv'
        if not ef.exists():
            continue
        try:
            edges = pd.read_csv(ef)
        except Exception:
            continue
        if edges is None or edges.empty:
            continue
        if 'price' not in edges.columns:
            continue
        edges = edges[(edges['price'] >= PRICE_MIN) & (edges['price'] <= PRICE_MAX)].copy()
        if edges.empty:
            continue
        need = {'date','player_id','stat','side','line','price','model_prob'}
        if not need.issubset(set(edges.columns)):
            continue
        edges['date'] = pd.to_datetime(edges['date'], errors='coerce').dt.date
        edges['player_id'] = pd.to_numeric(edges['player_id'], errors='coerce')
        # Join on date+player_id
        a_day = actuals[actuals['date'] == d].copy()
        a_day['player_id'] = pd.to_numeric(a_day['player_id'], errors='coerce')
        merged = edges.merge(a_day, on=['date','player_id'], how='left', suffixes=('', '_act'))
        if merged is None or merged.empty:
            continue
        # Map actual stat column
        stat_map = {
            'pts': 'pts',
            'reb': 'reb',
            'ast': 'ast',
            'threes': 'threes',
            'pra': 'pra',
        }
        merged['actual_val'] = np.nan
        merged['stat'] = merged['stat'].astype(str).str.lower()
        for stat, col in stat_map.items():
            mask_s = merged['stat'] == stat
            if col in merged.columns:
                merged.loc[mask_s, 'actual_val'] = pd.to_numeric(merged.loc[mask_s, col], errors='coerce')
        merged['line'] = pd.to_numeric(merged['line'], errors='coerce')
        merged['side'] = merged['side'].astype(str).str.upper()
        merged['hit'] = np.where(
            (merged['side'] == 'OVER') & (merged['actual_val'] > merged['line']), 1,
            np.where((merged['side'] == 'UNDER') & (merged['actual_val'] < merged['line']), 1,
                     np.where((merged['actual_val'] == merged['line']), np.nan, 0)))
        merged = merged.dropna(subset=['hit'])
        if merged.empty:
            continue
        merged['unit_profit'] = merged['price'].map(profit_per_unit)
        merged['roi'] = np.where(merged['hit'] == 1, merged['unit_profit'], -1.0)
        cols = ['date','player_id','stat','side','line','price','implied_prob','model_prob','edge','ev','actual_val','hit','roi']
        cols = [c for c in cols if c in merged.columns]
        rows.append(merged[cols])
    if not rows:
        raise RuntimeError("no-rows-merged")
    all_df = pd.concat(rows, ignore_index=True)
    if all_df.empty:
        raise RuntimeError("empty-merged")
    # Bin by model_prob
    all_df['prob_bin'] = pd.cut(pd.to_numeric(all_df['model_prob'], errors='coerce'), bins=BINS, include_lowest=True)
    # Group and aggregate
    grp = all_df.groupby('prob_bin', dropna=True)
    out = grp.agg(
        n=('hit','size'),
        hit_rate=('hit','mean'),
        avg_model_prob=('model_prob','mean'),
        avg_implied_prob=('implied_prob','mean') if 'implied_prob' in all_df.columns else ('hit','mean'),
        avg_edge=('edge','mean') if 'edge' in all_df.columns else ('hit','mean'),
        avg_ev=('ev','mean') if 'ev' in all_df.columns else ('hit','mean'),
        roi=('roi','mean')
    ).reset_index()
    # Expand bin ranges
    out['bin_low'] = out['prob_bin'].apply(lambda x: float(str(x).split(',')[0].strip('[').strip('(')) if pd.notna(x) else np.nan)
    out['bin_high'] = out['prob_bin'].apply(lambda x: float(str(x).split(',')[1].strip(']').strip(')')) if pd.notna(x) else np.nan)
    return out


def fit_isotonic(xs: List[float], ys: List[float]) -> Tuple[np.ndarray, np.ndarray]:
    try:
        from sklearn.isotonic import IsotonicRegression
        xs_arr = np.clip(np.asarray(xs, dtype=float), 0.0, 1.0)
        ys_arr = np.clip(np.asarray(ys, dtype=float), 0.0, 1.0)
        iso = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
        iso.fit(xs_arr, ys_arr)
        grid = np.linspace(0.0, 1.0, 51)
        preds = iso.transform(grid)
        return grid, preds
    except Exception:
        arr = sorted(zip(xs, ys), key=lambda t: t[0])
        xs2 = np.asarray([a for a, _ in arr], dtype=float)
        ys2 = np.asarray([b for _, b in arr], dtype=float)
        ys_mono = np.maximum.accumulate(ys2)
        grid = np.linspace(0.0, 1.0, 51)
        preds = np.interp(grid, xs2, ys_mono)
        preds = np.clip(preds, 0.0, 1.0)
        return grid, preds


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)


def main() -> None:
    summary: Dict[str, Any] = {"windows": []}
    for days in WINDOWS:
        try:
            bins_df = reliability_for_window(days)
        except Exception as e:
            summary["windows"].append({"days": days, "error": str(e)})
            continue
        # Save reliability CSV per window
        rel_csv = PROC_DIR / f"reliability_props_{days}.csv"
        keep_cols = ['bin_low','bin_high','n','hit_rate','avg_model_prob','avg_implied_prob','avg_edge','avg_ev','roi']
        out_df = bins_df[[c for c in keep_cols if c in bins_df.columns]].copy()
        out_df.to_csv(rel_csv, index=False)
        # Fit isotonic calibration
        xs = out_df["avg_model_prob"].astype(float).tolist()
        ys = out_df["hit_rate"].astype(float).tolist()
        grid, preds = fit_isotonic(xs, ys)
        calib_json = PROC_DIR / f"props_prob_calibration_{days}.json"
        write_json(calib_json, {"x": [float(v) for v in grid], "y": [float(v) for v in preds], "source": rel_csv.name})
        # Compute RMSE of calibration vs observed hit_rate across bins
        try:
            import bisect
            def interp(p: float) -> float:
                xs2 = [float(v) for v in grid]
                ys2 = [float(v) for v in preds]
                p = max(0.0, min(1.0, float(p)))
                i = bisect.bisect_left(xs2, p)
                if i <= 0:
                    return float(ys2[0])
                if i >= len(xs2):
                    return float(ys2[-1])
                x0 = xs2[i-1]; x1 = xs2[i]; y0 = ys2[i-1]; y1 = ys2[i]
                t = (p - x0) / (x1 - x0) if (x1 > x0) else 0.0
                return float(y0 + t * (y1 - y0))
            w = out_df["n"].astype(float).fillna(0).values
            mp = out_df["avg_model_prob"].astype(float).fillna(0.0).values
            hr = out_df["hit_rate"].astype(float).fillna(0.0).values
            pred = np.array([interp(p) for p in mp], dtype=float)
            rmse = float(np.sqrt(np.average((pred - hr) ** 2, weights=w))) if float(w.sum()) > 0 else None
        except Exception:
            rmse = None
        summary["windows"].append({
            "days": days,
            "reliability_csv": rel_csv.name,
            "calibration_json": calib_json.name,
            "rmse_calibration": rmse,
            "total_bets": int(out_df["n"].sum()) if "n" in out_df.columns else None,
        })
    write_json(PROC_DIR / "props_prob_calibration_windows.json", summary)
    print(json.dumps({"ok": True, "windows": summary["windows"]}, indent=2))


if __name__ == "__main__":
    main()
