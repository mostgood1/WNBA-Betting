import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

# Config
DAYS = int(os.environ.get('PROPS_REL_DAYS', 60)) if 'PROPS_REL_DAYS' in os.environ else 60
BINS = int(os.environ.get('PROPS_REL_BINS', 10)) if 'PROPS_REL_BINS' in os.environ else 10
PRICE_MIN = float(os.environ.get('PROPS_REL_PRICE_MIN', -150)) if 'PROPS_REL_PRICE_MIN' in os.environ else -150.0
PRICE_MAX = float(os.environ.get('PROPS_REL_PRICE_MAX', 125)) if 'PROPS_REL_PRICE_MAX' in os.environ else 125.0
TODAY_OVERRIDE = os.environ.get('PROPS_REL_TODAY')

root = Path('c:/Users/mostg/OneDrive/Coding/NBA-Betting')
proc = root / 'data' / 'processed'

# Load actuals consolidated (parquet preferred, else stitch CSVs)
act_parq = proc / 'props_actuals.parquet'
if act_parq.exists():
    try:
        actuals = pd.read_parquet(act_parq)
    except Exception:
        # Fallback if parquet engine unavailable
        actuals = pd.DataFrame()
        for p in proc.glob('props_actuals_*.csv'):
            try:
                df = pd.read_csv(p)
                actuals = pd.concat([actuals, df], ignore_index=True)
            except Exception:
                pass
else:
    actuals = pd.DataFrame()
    for p in proc.glob('props_actuals_*.csv'):
        try:
            df = pd.read_csv(p)
            actuals = pd.concat([actuals, df], ignore_index=True)
        except Exception:
            pass

if actuals is None or actuals.empty:
    print({'status': 'no-actuals'})
    raise SystemExit(0)

# Normalize types
actuals['date'] = pd.to_datetime(actuals['date'], errors='coerce').dt.date
if 'player_id' in actuals.columns:
    actuals['player_id'] = pd.to_numeric(actuals['player_id'], errors='coerce')

# Date window
if TODAY_OVERRIDE:
    try:
        today = datetime.strptime(TODAY_OVERRIDE, '%Y-%m-%d').date()
    except Exception:
        today = datetime.today().date()
else:
    today = datetime.today().date()
start = today - timedelta(days=DAYS)
mask = (actuals['date'] >= start) & (actuals['date'] <= today)
actuals = actuals.loc[mask].copy()
if actuals.empty:
    print({'status': 'no-actuals-in-window'})
    raise SystemExit(0)

# Helper: ROI per $1 stake from American odds
# price > 0: profit = price/100; price < 0: profit = 100/abs(price)
def profit_per_unit(price: float) -> float:
    try:
        price = float(price)
    except Exception:
        return np.nan
    return (price/100.0) if price > 0 else (100.0/abs(price))

rows = []
# Iterate dates present in actuals; load matching edges
dates = sorted(set(actuals['date']))
for d in dates:
    ef = proc / f'props_edges_{d}.csv'
    if not ef.exists():
        continue
    try:
        edges = pd.read_csv(ef)
    except Exception:
        continue
    if edges is None or edges.empty:
        continue
    # Filter reasonable price range
    if 'price' not in edges.columns:
        continue
    edges = edges[(edges['price'] >= PRICE_MIN) & (edges['price'] <= PRICE_MAX)].copy()
    if edges.empty:
        continue
    # Required columns
    need = {'date','player_id','stat','side','line','price','model_prob'}
    if not need.issubset(set(edges.columns)):
        # Schema mismatch, skip this date
        continue
    # Exclude markets we cannot grade reliably (no numeric line / different semantics)
    edges['stat'] = edges['stat'].astype(str).str.lower()
    edges = edges[~edges['stat'].isin(['dd','td'])].copy()
    if edges.empty:
        continue
    # Normalize
    edges['date'] = pd.to_datetime(edges['date'], errors='coerce').dt.date
    edges['player_id'] = pd.to_numeric(edges['player_id'], errors='coerce')
    # Join on date+player_id
    a_day = actuals[actuals['date'] == d].copy()
    a_day['player_id'] = pd.to_numeric(a_day['player_id'], errors='coerce')
    merged = edges.merge(a_day, on=['date','player_id'], how='left', suffixes=('','_act'))
    if merged is None or merged.empty:
        continue
    # Map actual stat values. Some markets are derived combos.
    merged['actual_val'] = np.nan
    merged['stat'] = merged['stat'].astype(str).str.lower()

    base_cols = {
        'pts': 'pts',
        'reb': 'reb',
        'ast': 'ast',
        'threes': 'threes',
        'pra': 'pra',
        # Optional if present in actuals
        'stl': 'stl',
        'blk': 'blk',
        'tov': 'tov',
    }
    for stat, col in base_cols.items():
        if col in merged.columns:
            mask_s = merged['stat'] == stat
            if mask_s.any():
                merged.loc[mask_s, 'actual_val'] = pd.to_numeric(merged.loc[mask_s, col], errors='coerce')

    # Derived combos from base actuals
    pts = pd.to_numeric(merged.get('pts'), errors='coerce') if 'pts' in merged.columns else None
    reb = pd.to_numeric(merged.get('reb'), errors='coerce') if 'reb' in merged.columns else None
    ast = pd.to_numeric(merged.get('ast'), errors='coerce') if 'ast' in merged.columns else None
    if pts is not None and reb is not None:
        mask_s = merged['stat'] == 'pr'
        if mask_s.any():
            merged.loc[mask_s, 'actual_val'] = pts + reb
    if pts is not None and ast is not None:
        mask_s = merged['stat'] == 'pa'
        if mask_s.any():
            merged.loc[mask_s, 'actual_val'] = pts + ast
    if reb is not None and ast is not None:
        mask_s = merged['stat'] == 'ra'
        if mask_s.any():
            merged.loc[mask_s, 'actual_val'] = reb + ast

    # Drop rows we can't grade (missing actual)
    merged = merged.dropna(subset=['actual_val'])
    if merged.empty:
        continue
    # Outcome
    merged['line'] = pd.to_numeric(merged['line'], errors='coerce')
    merged['side'] = merged['side'].astype(str).str.upper()
    merged['hit'] = np.where(
        (merged['side'] == 'OVER') & (merged['actual_val'] > merged['line']), 1,
        np.where((merged['side'] == 'UNDER') & (merged['actual_val'] < merged['line']), 1,
                 np.where((merged['actual_val'] == merged['line']), np.nan, 0)))
    merged = merged.dropna(subset=['hit'])  # drop pushes
    if merged.empty:
        continue
    # ROI
    merged['unit_profit'] = merged['price'].map(profit_per_unit)
    merged['roi'] = np.where(merged['hit'] == 1, merged['unit_profit'], -1.0)
    # Collect selected columns if present
    cols = ['date','player_id','stat','side','line','price','implied_prob','model_prob','edge','ev','actual_val','hit','roi']
    cols = [c for c in cols if c in merged.columns]
    rows.append(merged[cols])

if not rows:
    print({'status': 'no-rows-merged'})
    raise SystemExit(0)
all_df = pd.concat(rows, ignore_index=True)
if all_df.empty:
    print({'status': 'empty-merged'})
    raise SystemExit(0)

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

# Expand bin ranges to numeric columns
out['bin_low'] = out['prob_bin'].apply(lambda x: float(str(x).split(',')[0].strip('[').strip('(')) if pd.notna(x) else np.nan)
out['bin_high'] = out['prob_bin'].apply(lambda x: float(str(x).split(',')[1].strip(']').strip(')')) if pd.notna(x) else np.nan)

# Order and write CSV
keep_cols = ['bin_low','bin_high','n','hit_rate','avg_model_prob','avg_implied_prob','avg_edge','avg_ev','roi']
out = out[[c for c in keep_cols if c in out.columns]]
csv_path = proc / 'reliability_props.csv'
out.to_csv(csv_path, index=False)
print({'rows': int(len(out)), 'output': str(csv_path), 'total_bets': int(len(all_df))})
