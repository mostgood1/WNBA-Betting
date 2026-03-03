from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LIVE_LENS_DIR = Path((os.getenv("NBA_LIVE_LENS_DIR") or os.getenv("LIVE_LENS_DIR") or "").strip() or str(PROCESSED))


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iso(d: date) -> str:
    return d.isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Daily Live Lens tuner: runs totals + player-prop optimizers over rolling windows and optionally writes live_lens_tuning_override.json."
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
    ap.add_argument(
        "--props-lookback-days",
        type=int,
        default=14,
        help="Rolling window size for player-prop tuning (default: 14)",
    )
    ap.add_argument("--bet-threshold", type=float, default=6.0)
    ap.add_argument("--juice", type=float, default=110.0)
    ap.add_argument("--min-bets", type=int, default=25)
    ap.add_argument("--props-min-bets", type=int, default=40)
    ap.add_argument(
        "--props-sigma",
        action="store_true",
        help="Also tune sigma-normalized prop thresholds (writes *_sigma keys into override when --write-override is set)",
    )
    ap.add_argument(
        "--props-sigma-per-stat",
        action="store_true",
        help="When tuning sigma thresholds, also tune per-stat sigma thresholds",
    )
    ap.add_argument(
        "--props-sigma-min-bets-per-stat",
        type=int,
        default=15,
        help="Minimum per-stat sample size for sigma per-stat tuning (default: 15)",
    )
    ap.add_argument(
        "--write-override",
        action="store_true",
        help="Merge best settings into live_lens_tuning_override.json",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Run optimizer but do not write override",
    )
    ap.add_argument(
        "--props-require-logged-bets",
        action="store_true",
        help="Tune prop bettability gating using only rows logged as klass=BET",
    )

    args = ap.parse_args()

    if args.end:
        end = _parse_date(args.end)
    else:
        end = date.today() - timedelta(days=1)

    lookback = int(args.lookback_days)
    if lookback <= 0:
        raise SystemExit("--lookback-days must be >= 1")

    props_lookback = int(args.props_lookback_days)
    if props_lookback <= 0:
        raise SystemExit("--props-lookback-days must be >= 1")

    start = end - timedelta(days=lookback - 1)
    props_start = end - timedelta(days=props_lookback - 1)

    # End-to-end wiring: default to sigma tuning unless explicitly disabled.
    # This is additive (writes extra keys) and does not remove legacy thresholds.
    props_sigma = bool(args.props_sigma) or str(os.environ.get("LIVE_LENS_TUNE_SIGMA", "1")).strip().lower() in {"1", "true", "yes"}
    props_sigma_per_stat = bool(args.props_sigma_per_stat) or str(os.environ.get("LIVE_LENS_TUNE_SIGMA_PER_STAT", "1")).strip().lower() in {"1", "true", "yes"}
    try:
        sigma_min_bets_per_stat = int(args.props_sigma_min_bets_per_stat)
    except Exception:
        sigma_min_bets_per_stat = 15
    sigma_min_bets_per_stat = int(max(5, min(200, sigma_min_bets_per_stat)))

    optimizer = ROOT / "tools" / "optimize_live_lens_adjustments.py"
    if not optimizer.exists():
        raise SystemExit(f"Missing optimizer: {optimizer}")

    out_path = LIVE_LENS_DIR / f"live_lens_adjustments_optimized_{_iso(start)}_{_iso(end)}.json"

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
        f"Live Lens daily tune: totals={_iso(start)}..{_iso(end)} props={_iso(props_start)}..{_iso(end)} write_override={bool(args.write_override and not args.dry_run)}"
    )

    write_override = bool(args.write_override and not args.dry_run)

    rc = 0
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=False)
    rc = max(rc, int(proc.returncode))

    # Player-prop watch/bet thresholds
    try:
        pp_thr = ROOT / "tools" / "optimize_live_lens_player_prop_thresholds.py"
        if pp_thr.exists():
            cmd2 = [
                sys.executable,
                str(pp_thr),
                "--start",
                _iso(props_start),
                "--end",
                _iso(end),
                "--min-bets",
                str(int(args.props_min_bets)),
                "--assumed-juice",
                str(float(args.juice)),
            ]
            if props_sigma:
                cmd2.append("--also-sigma")
                if props_sigma_per_stat:
                    cmd2.append("--sigma-per-stat")
                    cmd2.extend(["--sigma-min-bets-per-stat", str(int(sigma_min_bets_per_stat))])
            if write_override:
                cmd2.append("--write-override")
            proc2 = subprocess.run(cmd2, cwd=str(ROOT), capture_output=False)
            rc = max(rc, int(proc2.returncode))
        else:
            print(f"WARN: missing player-prop threshold optimizer: {pp_thr}")
    except Exception as e:
        print(f"WARN: player-prop threshold tune failed: {e}")

    # Player-prop bettability gating threshold
    try:
        pp_bett = ROOT / "tools" / "optimize_live_lens_player_prop_bettability.py"
        if pp_bett.exists():
            cmd3 = [
                sys.executable,
                str(pp_bett),
                "--start",
                _iso(props_start),
                "--end",
                _iso(end),
                "--min-bets",
                str(int(args.props_min_bets)),
                "--assumed-juice",
                str(float(args.juice)),
            ]
            if bool(args.props_require_logged_bets):
                cmd3.append("--require-logged-bets")
            if write_override:
                cmd3.append("--write-override")
            proc3 = subprocess.run(cmd3, cwd=str(ROOT), capture_output=False)
            rc = max(rc, int(proc3.returncode))
        else:
            print(f"WARN: missing player-prop bettability optimizer: {pp_bett}")
    except Exception as e:
        print(f"WARN: player-prop bettability tune failed: {e}")

    return int(rc)


if __name__ == "__main__":
    raise SystemExit(main())
