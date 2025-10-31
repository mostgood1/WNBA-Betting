import pandas as pd
import numpy as np
from pathlib import Path

def main():
    p = Path(__file__).resolve().parent.parent / 'data' / 'processed' / 'pbp_calibration.csv'
    try:
        df = pd.read_csv(p)
    except Exception:
        print('no calibration file found')
        return
    if df.empty:
        print('calibration file empty')
        return
    def clip(col, lo, hi):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').clip(lower=lo, upper=hi)
    clip('thr_bias', -1.0, 1.0)
    clip('tip_logit_bias', -1.5, 1.5)
    clip('fb_temp', 0.5, 2.0)
    clip('fb_tip_alpha', 0.0, 1.0)
    if 'window_days' in df.columns:
        df['window_days'] = pd.to_numeric(df['window_days'], errors='coerce').fillna(7).clip(lower=1, upper=30).astype(int)
    df.to_csv(p, index=False)
    print('sanitized', str(p), 'rows:', len(df))

if __name__ == '__main__':
    main()
