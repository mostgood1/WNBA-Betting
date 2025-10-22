import os
import sys
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, 'src')
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from nba_betting.props_edges import compute_props_edges, SigmaConfig
from nba_betting.config import paths


def main(date_str: str):
    d = pd.to_datetime(date_str)
    api_key = os.environ.get('ODDS_API_KEY')
    # Prefer current fetch over saved
    edges = compute_props_edges(
        date=date_str,
        sigma=SigmaConfig(),
        use_saved=False,
        mode='current',
        api_key=api_key,
        source='oddsapi',
        predictions_path=None,
        from_file_only=False,
    )
    out = paths.data_processed / f"props_edges_{d.date()}.csv"
    if edges is None or edges.empty:
        print({'rows': 0, 'output': str(out)})
        return 0
    edges.to_csv(out, index=False)
    print({'rows': int(len(edges)), 'output': str(out)})
    return 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python scripts/compute_edges_for_date.py YYYY-MM-DD')
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
