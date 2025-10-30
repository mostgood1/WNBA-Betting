import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parent.parent / 'data' / 'processed' / 'pbp_calibration.csv'
try:
    df = pd.read_csv(p)
    print(df.tail(10).to_string(index=False))
except Exception as e:
    print(f"failed to read {p}: {e}")
