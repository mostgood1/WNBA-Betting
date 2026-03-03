import argparse
from datetime import datetime, timedelta
import os
from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT_ENV = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(_DATA_ROOT_ENV).expanduser().resolve() if _DATA_ROOT_ENV else (BASE_DIR / "data")
PROCESSED = DATA_ROOT / "processed"

WIN_PROB_CANDIDATES = [
    ('home_win_prob', 'Model: blended'),
    ('home_win_prob_raw', 'Model: raw'),
    ('home_win_prob_from_spread', 'Baseline: market-spread'),
]

def _load_recon(d: datetime) -> pd.DataFrame:
    p = PROCESSED / f'recon_games_{d:%Y-%m-%d}.csv'
    if p.exists():
        return pd.read_csv(p)
    return pd.DataFrame()

def _load_preds(d: datetime) -> pd.DataFrame:
    p = PROCESSED / f'predictions_{d:%Y-%m-%d}.csv'
    if p.exists():
        return pd.read_csv(p)
    return pd.DataFrame()

def build_window(anchor: datetime, days: int):
    recs, preds = [], []
    for i in range(1, days+1):
        d = anchor - timedelta(days=i)
        rd = _load_recon(d)
        pd_ = _load_preds(d)
        if not rd.empty:
            recs.append(rd)
        if not pd_.empty:
            preds.append(pd_)
    return (pd.concat(recs, ignore_index=True) if recs else pd.DataFrame(),
            pd.concat(preds, ignore_index=True) if preds else pd.DataFrame())

def brier(p: pd.Series, y: pd.Series) -> float:
    return float(np.mean((np.clip(p,0,1) - y)**2))

def ece(p: pd.Series, y: pd.Series, bins: int = 10) -> float:
    p = np.clip(p,0,1)
    edges = np.linspace(0,1,bins+1)
    idx = np.digitize(p, edges, right=True)
    total = 0.0
    n = 0
    for b in range(1, bins+1):
        mask = idx == b
        if not np.any(mask):
            continue
        avg_p = float(np.mean(p[mask]))
        avg_y = float(np.mean(y[mask]))
        w = int(np.sum(mask))
        total += w * abs(avg_p - avg_y)
        n += w
    return float(total / n) if n else np.nan

def main():
    ap = argparse.ArgumentParser(description='Calibration comparison over recent window')
    ap.add_argument('--date', required=True, help='Anchor date YYYY-MM-DD')
    ap.add_argument('--days', type=int, default=60)
    args = ap.parse_args()
    anchor = datetime.strptime(args.date, '%Y-%m-%d')
    rec_df, pred_df = build_window(anchor, args.days)
    if rec_df.empty or pred_df.empty:
        print('Insufficient data for calibration comparison')
        return
    m = pd.merge(pred_df, rec_df[['home_team','visitor_team','home_pts','visitor_pts']], on=['home_team','visitor_team'], how='inner')
    if m.empty:
        print('No matches between predictions and recon')
        return
    y = (pd.to_numeric(m['home_pts'], errors='coerce') > pd.to_numeric(m['visitor_pts'], errors='coerce')).astype(float)
    rows = []
    for col, label in WIN_PROB_CANDIDATES:
        if col in m.columns:
            p = pd.to_numeric(m[col], errors='coerce')
            bv = brier(p, y)
            ev = ece(p, y, bins=10)
            rows.append({'model': label, 'column': col, 'n': int(np.sum(~np.isnan(p))), 'brier': bv, 'ece': ev})
    out = pd.DataFrame(rows)
    out_csv = PROCESSED / 'calibration_compare.csv'
    out.to_csv(out_csv, index=False)
    html = out.to_html(index=False, float_format=lambda x: f"{x:.4f}")
    with open(PROCESSED / 'calibration_compare.html', 'w', encoding='utf-8') as f:
        f.write('<html><head><meta charset="utf-8"><title>Calibration Comparison</title></head><body>')
        f.write('<h2>Calibration Comparison (last {} days)</h2>'.format(args.days))
        f.write(html)
        f.write('</body></html>')
    print(f'Wrote {out_csv} and calibration_compare.html')

if __name__ == '__main__':
    main()
