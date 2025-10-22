import os
import sys
from datetime import datetime
import pandas as pd

# Ensure src is on path
ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, 'src')
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from nba_betting.odds_api import OddsApiConfig, fetch_player_props_current
from nba_betting.config import paths


def main(date_str: str | None = None):
    if not date_str:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')
    d = pd.to_datetime(date_str)
    api_key = os.environ.get('ODDS_API_KEY')
    if not api_key:
        # try .env file
        env_path = os.path.join(ROOT, '.env')
        if os.path.exists(env_path):
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    s = line.strip()
                    if not s or s.startswith('#') or '=' not in s:
                        continue
                    k, v = s.split('=', 1)
                    if k.strip() == 'ODDS_API_KEY':
                        api_key = v.strip().strip('"').strip("'")
                        break
    if not api_key:
        print('Missing ODDS_API_KEY in environment or .env')
        return 1
    cfg = OddsApiConfig(api_key=api_key)
    df = fetch_player_props_current(cfg, date=d, markets=None, verbose=True)
    out_csv = paths.data_raw / f"odds_nba_player_props_{d.date()}.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    if df is None or df.empty:
        print({'rows': 0, 'output': str(out_csv)})
        return 0
    df.to_csv(out_csv, index=False)
    print({'rows': int(len(df)), 'output': str(out_csv)})
    return 0


if __name__ == '__main__':
    import sys as _sys
    date_arg = _sys.argv[1] if len(_sys.argv) > 1 else None
    _sys.exit(main(date_arg))
