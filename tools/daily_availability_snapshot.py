from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import subprocess
import sys

import pandas as pd

# Ensure src/ is importable when running as a script.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from nba_betting.league_status import build_league_status  # type: ignore


def _default_date() -> str:
    return dt.date.today().isoformat()


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Build daily availability artifacts: league_status_<date>.csv and injuries_counts_<date>.json. "
            "These are the preferred sources for cards/availability and prevent stale raw injuries.csv from poisoning OUT flags."
        )
    )
    ap.add_argument("--date", default=_default_date(), help="Date YYYY-MM-DD (default: today)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite league_status if it already exists")
    ap.add_argument(
        "--skip-snapshot-injuries",
        action="store_true",
        help="Skip building injuries_counts_<date>.json (league_status only)",
    )
    args = ap.parse_args()

    date_str = str(args.date).strip()
    if not date_str:
        raise SystemExit("ERROR: missing --date")

    proc = ROOT / "data" / "processed"
    proc.mkdir(parents=True, exist_ok=True)

    ls_path = proc / f"league_status_{date_str}.csv"
    if ls_path.exists() and (not args.overwrite):
        try:
            existing = pd.read_csv(ls_path)
            if isinstance(existing, pd.DataFrame) and (not existing.empty):
                print(f"SKIP:league_status_exists:{ls_path}")
            else:
                raise ValueError("empty")
        except Exception:
            # File exists but is unreadable/empty; rebuild.
            pass
        else:
            # Still allow snapshot injuries even if league_status already exists.
            if args.skip_snapshot_injuries:
                print("OK:availability_daily")
                return

    df = build_league_status(date_str)
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        raise SystemExit(f"ERROR: build_league_status returned empty for {date_str}")

    df.to_csv(ls_path, index=False)
    print(f"OK:league_status:{ls_path}")

    if not args.skip_snapshot_injuries:
        cmd = [sys.executable, str(ROOT / "tools" / "snapshot_injuries.py"), "--date", date_str]
        rc = subprocess.call(cmd, cwd=str(ROOT))
        if rc != 0:
            raise SystemExit(f"ERROR: snapshot_injuries failed rc={rc}")

    print("OK:availability_daily")


if __name__ == "__main__":
    main()
