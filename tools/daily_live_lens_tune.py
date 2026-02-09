from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: date) -> str:
    return d.isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Daily Live Lens tuner: runs optimize_live_lens_adjustments.py over a rolling window and optionally writes the override."
        )
    )
    ap.add_argument(
        "--end",
        type=str,
        default=None,
        help="Window end date YYYY-MM-DD (default: yesterday, local time)",
    )
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Rolling window size (default: 7)",
    )
    ap.add_argument("--bet-threshold", type=float, default=6.0)
    ap.add_argument("--juice", type=float, default=110.0)
    ap.add_argument("--min-bets", type=int, default=25)
    ap.add_argument(
        "--write-override",
        action="store_true",
        help="Write data/processed/live_lens_tuning_override.json with best adjustments.game_total",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Run optimizer but do not write override",
    )

    args = ap.parse_args()

    if args.end:
        end = _parse_date(args.end)
    else:
        end = date.today() - timedelta(days=1)

    lookback = int(args.lookback_days)
    if lookback <= 0:
        raise SystemExit("--lookback-days must be >= 1")

    start = end - timedelta(days=lookback - 1)

    optimizer = ROOT / "tools" / "optimize_live_lens_adjustments.py"
    if not optimizer.exists():
        raise SystemExit(f"Missing optimizer: {optimizer}")

    out_path = PROCESSED / f"live_lens_adjustments_optimized_{_iso(start)}_{_iso(end)}.json"

    cmd = [
        sys.executable,
        str(optimizer),
        "--start",
        _iso(start),
        "--end",
        _iso(end),
        "--bet-threshold",
        str(float(args.bet_threshold)),
        "--juice",
        str(float(args.juice)),
        "--min-bets",
        str(int(args.min_bets)),
        "--out",
        str(out_path),
    ]

    if args.write_override and not args.dry_run:
        cmd.append("--write-override")

    print(
        f"Live Lens daily tune: window={_iso(start)}..{_iso(end)} lookback_days={lookback} write_override={bool(args.write_override and not args.dry_run)}"
    )

    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=False)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
