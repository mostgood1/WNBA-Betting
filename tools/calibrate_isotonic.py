import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

try:
    from sklearn.isotonic import IsotonicRegression
except ImportError:
    IsotonicRegression = None

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / 'data' / 'processed'


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def collect_pairs(end_date: datetime, days: int, prob_col_priority) -> tuple[pd.Series, pd.Series]:
    start = end_date - timedelta(days=max(1, days))
    probs = []
    outs = []
    for d in daterange(start, end_date - timedelta(days=1)):  # exclude target date
        ds = d.strftime('%Y-%m-%d')
        pred_path = PROCESSED / f'predictions_{ds}.csv'
        recon_path = PROCESSED / f'recon_games_{ds}.csv'
        if not (pred_path.exists() and recon_path.exists()):
            continue
        try:
            p = pd.read_csv(pred_path); r = pd.read_csv(recon_path)
        except Exception:
            continue
        for c in ('home_team','visitor_team','date'):
            if c not in p.columns and c.upper() in p.columns: p[c] = p[c.upper()]
            if c not in r.columns and c.upper() in r.columns: r[c] = r[c.upper()]
        keys = [c for c in ('date','home_team','visitor_team') if c in p.columns and c in r.columns]
        if len(keys) < 2:
            continue
        m = p.merge(r, on=keys, suffixes=('_p','_r'))
        # choose probability column
        prob_col = None
        for col in prob_col_priority:
            if col in m.columns:
                prob_col = col; break
        if prob_col is None:
            continue
        if {'home_final','visitor_final'}.issubset(m.columns):
            y = (pd.to_numeric(m['home_final'], errors='coerce') > pd.to_numeric(m['visitor_final'], errors='coerce')).astype(float)
        elif {'home_pts','visitor_pts'}.issubset(m.columns):
            y = (pd.to_numeric(m['home_pts'], errors='coerce') > pd.to_numeric(m['visitor_pts'], errors='coerce')).astype(float)
        elif 'winner' in m.columns:
            y = (m['winner'].astype(str).str.upper() == m['home_team'].astype(str).str.upper()).astype(float)
        else:
            continue
        probs.append(pd.to_numeric(m[prob_col], errors='coerce'))
        outs.append(y)
    if not probs:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    return pd.concat(probs, ignore_index=True), pd.concat(outs, ignore_index=True)


def main():
    ap = argparse.ArgumentParser(description='Isotonic calibration for game win probabilities')
    ap.add_argument('--date', required=True, type=str, help='Target date (YYYY-MM-DD) predictions file to calibrate')
    ap.add_argument('--days', type=int, default=30, help='Lookback days (default 30)')
    ap.add_argument('--min-samples', type=int, default=200, help='Minimum samples required to fit isotonic')
    ap.add_argument('--prob-priority', type=str, default='home_win_prob_cal,home_win_prob,prob_home_win', help='Comma list priority order of probability columns to calibrate')
    args = ap.parse_args()
    if IsotonicRegression is None:
        print('SKIP: scikit-learn not available')
        return 0
    try:
        target_date = datetime.strptime(args.date, '%Y-%m-%d')
    except Exception:
        print('Invalid --date format'); return 1
    priority = [s.strip() for s in args.prob_priority.split(',') if s.strip()]
    probs, outs = collect_pairs(target_date, args.days, priority)
    n = len(probs)
    if n < args.min_samples:
        print(f'SKIP: insufficient samples ({n} < {args.min_samples})')
        return 0
    # Fit isotonic
    iso = IsotonicRegression(out_of_bounds='clip')
    try:
        iso.fit(probs, outs)
    except Exception as e:
        print(f'FAIL: isotonic fit error {e}')
        return 1
    # Load target predictions
    pred_path = PROCESSED / f'predictions_{args.date}.csv'
    if not pred_path.exists():
        print('Missing target predictions file'); return 1
    pred = pd.read_csv(pred_path)
    base_col = None
    for col in priority:
        if col in pred.columns:
            base_col = col; break
    if base_col is None:
        print('No priority probability column found in target predictions')
        return 1
    pred['home_win_prob_iso'] = iso.predict(pd.to_numeric(pred[base_col], errors='coerce').clip(0,1))
    pred.to_csv(pred_path, index=False)
    # Report training metrics
    train_brier_before = float(((probs - outs) ** 2).mean()) if n else float('nan')
    train_brier_after = float(((iso.predict(probs) - outs) ** 2).mean()) if n else float('nan')
    print({'samples': n, 'train_brier_before': train_brier_before, 'train_brier_after': train_brier_after, 'target_updated': str(pred_path)})
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
