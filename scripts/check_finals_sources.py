import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app import _finals_from_stats_all as s,_finals_from_cdn_all as c,_finals_from_espn_all as e
import pandas as pd
import json

for d in ['2025-10-21','2025-10-20','2025-10-22']:
    R = {}
    def count(df):
        return 0 if (df is None or (isinstance(df, pd.DataFrame) and df.empty)) else len(df)
    try:
        R['stats'] = count(s(d))
    except Exception as ex:
        R['stats'] = f"error: {ex}"
    try:
        R['cdn'] = count(c(d))
    except Exception as ex:
        R['cdn'] = f"error: {ex}"
    try:
        R['espn'] = count(e(d))
    except Exception as ex:
        R['espn'] = f"error: {ex}"
    print(d, json.dumps(R))
