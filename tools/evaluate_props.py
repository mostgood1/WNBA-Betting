import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / 'data' / 'processed'

# Expected file pattern: props_predictions_<date>.csv with columns:
#  date, player, team, market, line, prob_over, prob_under, outcome (optional: 'over'/'under')
# This scaffold computes calibration and accuracy by market.

def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def load_day(ds: str):
    path = PROCESSED / f'props_predictions_{ds}.csv'
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def evaluate_props(start: datetime, end: datetime) -> pd.DataFrame:
    rows = []
    for d in daterange(start, end):
        ds = d.strftime('%Y-%m-%d')
        df = load_day(ds)
        if df is None or df.empty:
            continue
        # Normalize columns
        base_cols = set(df.columns)
        if 'market' not in base_cols or 'prob_over' not in base_cols or 'prob_under' not in base_cols:
            continue
        # Outcome encoding: 1 if over, 0 if under when outcome present
        if 'outcome' in df.columns:
            df['y_over'] = (df['outcome'].astype(str).str.lower() == 'over').astype(float)
        else:
            df['y_over'] = np.nan
        # Per market aggregate
        g = df.groupby('market')
        for market, grp in g:
            p_over = pd.to_numeric(grp['prob_over'], errors='coerce')
            y_over = pd.to_numeric(grp['y_over'], errors='coerce')
            m = (~p_over.isna()) & (~y_over.isna())
            if m.sum() < 5:
                continue
            brier = float(((p_over[m] - y_over[m])**2).mean())
            acc = float(np.mean(((p_over[m] >= 0.5).astype(int) == y_over[m].astype(int))))
            rows.append({
                'date': ds,
                'market': market,
                'n': int(m.sum()),
                'brier_over': brier,
                'acc_over': acc,
                'p_mean_over': float(p_over[m].mean()),
                'y_rate_over': float(y_over[m].mean()),
            })
    if not rows:
        return pd.DataFrame(columns=['date','market','n','brier_over','acc_over','p_mean_over','y_rate_over'])
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description='Evaluate props prediction calibration over a date range')
    ap.add_argument('--start', type=str)
    ap.add_argument('--end', type=str)
    ap.add_argument('--days', type=int, default=14)
    args = ap.parse_args()
    if args.start and args.end:
        start = datetime.strptime(args.start, '%Y-%m-%d'); end = datetime.strptime(args.end, '%Y-%m-%d')
    else:
        end = datetime.today() - timedelta(days=1)
        start = end - timedelta(days=max(1, args.days))
    out_df = evaluate_props(start, end)
    out_path = PROCESSED / 'props_eval.csv'
    out_df.to_csv(out_path, index=False)
    print({'rows': len(out_df), 'out': str(out_path)})
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
