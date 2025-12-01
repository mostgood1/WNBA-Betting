import argparse
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def daterange(start, end):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def derive_for_date(d: datetime, overwrite: bool=False) -> bool:
    ds = d.strftime('%Y-%m-%d')
    recon = PROCESSED / f"recon_games_{ds}.csv"
    out = PROCESSED / f"finals_{ds}.csv"
    if out.exists() and not overwrite:
        return True
    if not recon.exists():
        return False
    try:
        df = pd.read_csv(recon)
    except Exception:
        return False
    needed = {"date","home_team","visitor_team","home_pts","visitor_pts"}
    if not needed.issubset(df.columns):
        return False
    finals = df[list(needed)].rename(columns={"home_pts":"home_score","visitor_pts":"visitor_score"})
    # Reorder columns to expected schema for evaluation (date, home_team, visitor_team, home_score, visitor_score)
    finals = finals[["date","home_team","visitor_team","home_score","visitor_score"]]
    finals.to_csv(out, index=False)
    return True


def main():
    ap = argparse.ArgumentParser(description="Derive finals_{date}.csv from recon_games_{date}.csv over a range")
    ap.add_argument('--start', required=True, type=str)
    ap.add_argument('--end', required=True, type=str)
    ap.add_argument('--overwrite', action='store_true')
    args = ap.parse_args()
    start = datetime.strptime(args.start, '%Y-%m-%d')
    end = datetime.strptime(args.end, '%Y-%m-%d')
    ok = 0; miss = 0
    for d in daterange(start, end):
        if derive_for_date(d, overwrite=args.overwrite):
            ok += 1
        else:
            miss += 1
    print({'written': ok, 'skipped_or_missing': miss})
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
