import os
import glob
import datetime as dt
import pandas as pd


def load_drift_csvs(days: int = 60, pattern: str = 'data/processed/drift_games_*.csv') -> pd.DataFrame:
    files = sorted(glob.glob(pattern))
    rows = []
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        date_str = os.path.splitext(os.path.basename(f))[0].split('_')[-1]
        try:
            d = dt.datetime.strptime(date_str, '%Y-%m-%d').date()
        except Exception:
            continue
        if (dt.date.today() - d).days <= days:
            df['date'] = pd.to_datetime(d)
            rows.append(df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def weekly_rollup(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df['week'] = df['date'].dt.to_period('W').dt.start_time
    agg = df.groupby(['week', 'feature'], as_index=False).agg({
        'psi': 'mean',
        'ks_stat': 'mean',
        'ref_count': 'sum',
        'cur_count': 'sum',
        'psi_flag': lambda s: (s == 'severe').sum(),
        'ks_flag': lambda s: (s == 'severe').sum(),
    })
    agg = agg.rename(columns={'psi_flag': 'severe_psi_count', 'ks_flag': 'severe_ks_count'})
    return agg


def render_html_trend(weekly: pd.DataFrame, out_path: str):
    if weekly.empty:
        html = '<html><body><h3>No drift data found for trend.</h3></body></html>'
    else:
        pivot_psi = weekly.pivot(index='week', columns='feature', values='psi').fillna(0)
        pivot_ks = weekly.pivot(index='week', columns='feature', values='ks_stat').fillna(0)
        html = '<html><head><meta charset="utf-8"><title>Weekly Drift Trend</title>' \
               '<style>table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px}</style></head><body>'
        html += '<h2>Weekly PSI (mean)</h2>'
        html += pivot_psi.to_html(classes='psi', float_format=lambda x: f"{x:.3f}")
        html += '<h2>Weekly KS (mean)</h2>'
        html += pivot_ks.to_html(classes='ks', float_format=lambda x: f"{x:.3f}")
        html += '</body></html>'
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(html)


def main():
    df = load_drift_csvs(days=60)
    weekly = weekly_rollup(df)
    out_dir = os.path.join('data','processed')
    os.makedirs(out_dir, exist_ok=True)
    out_csv = os.path.join(out_dir, 'drift_weekly.csv')
    out_html = os.path.join(out_dir, 'drift_weekly_trend.html')
    if not weekly.empty:
        weekly.to_csv(out_csv, index=False)
    render_html_trend(weekly, out_html)


if __name__ == '__main__':
    main()
