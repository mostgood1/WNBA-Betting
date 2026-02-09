"""Backfill props artifacts (edges -> cards -> best_edges snapshots) for a date range.

Goal: regenerate historical CSVs using the *current* props edge logic while staying
as offline/deterministic as possible.

It will only process a date if both of these exist:
  - data/raw/odds_nba_player_props_<date>.csv
  - data/processed/props_predictions_<date>.csv

Outputs (per processed date):
  - data/processed/props_edges_<date>.csv
  - data/processed/props_recommendations_<date>.csv
  - data/processed/best_edges_props_<date>.csv (and best_edges_games_<date>.csv if possible)

Typical use:
  .\.venv\Scripts\python.exe tools/backfill_props_artifacts.py --days 60

Notes:
- Uses saved odds only; if saved odds are missing/empty, the date is skipped.
- Uses file-only predictions; it will not run models.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Result:
    date_str: str
    status: str
    edges_rows: int = 0


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: _date, end: _date) -> list[str]:
    out: list[str] = []
    d = start
    while d <= end:
        out.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return out


def rebuild_one(
    date_str: str,
    overwrite: bool,
    calibrate_sigma: bool,
    calibrate_prob: bool,
    max_games: int,
    max_props: int,
) -> Result:
    from nba_betting.config import paths
    from nba_betting.props_edges import SigmaConfig, calibrate_sigma_for_date, compute_props_edges
    from nba_betting.cli import _export_best_edges_snapshot, _export_props_recommendations_cards

    raw_odds = paths.data_raw / f"odds_nba_player_props_{date_str}.csv"
    preds_p = paths.data_processed / f"props_predictions_{date_str}.csv"

    if not raw_odds.exists():
        return Result(date_str=date_str, status="skip_missing_saved_odds")
    if not preds_p.exists():
        return Result(date_str=date_str, status="skip_missing_predictions")

    edges_out = paths.data_processed / f"props_edges_{date_str}.csv"
    if edges_out.exists() and not overwrite:
        return Result(date_str=date_str, status="skip_edges_exists")

    sigma = SigmaConfig()
    if calibrate_sigma:
        try:
            sigma = calibrate_sigma_for_date(date_str, window_days=60, min_rows=200, defaults=sigma)
        except Exception:
            pass

    try:
        edges = compute_props_edges(
            date=date_str,
            sigma=sigma,
            use_saved=True,
            mode="historical",
            api_key=None,
            source="oddsapi",
            predictions_path=str(preds_p),
            from_file_only=True,
            calibrate_prob=calibrate_prob,
        )
    except Exception as e:
        return Result(date_str=date_str, status=f"error_edges:{type(e).__name__}")

    if edges is None or edges.empty:
        return Result(date_str=date_str, status="skip_no_edges")

    edges_out.parent.mkdir(parents=True, exist_ok=True)
    edges.to_csv(edges_out, index=False)

    # Cards + best-edges snapshots
    try:
        # In cli.py, export-props-recommendations is registered via Click.
        # Importing the symbol may yield a click.Command, not a plain function.
        # Calling it directly would trigger Click argv parsing.
        cmd = _export_props_recommendations_cards
        cb = getattr(cmd, "callback", None)
        if callable(cb):
            cb(date_str, None)
        else:
            cmd(date_str, None)
    except Exception:
        # Keep going; best-edges has a fallback path from cards.
        pass
    try:
        _export_best_edges_snapshot(
            date_str=date_str,
            max_games=int(max_games),
            max_props=int(max_props),
            overwrite=True,
        )
    except Exception:
        pass

    return Result(date_str=date_str, status="ok", edges_rows=int(len(edges)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default=None, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None, help="End date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--days", type=int, default=60, help="Backfill window length when --start not provided")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite props_edges_<date>.csv if it exists")
    ap.add_argument("--no-calibrate-sigma", action="store_true", help="Disable sigma calibration")
    ap.add_argument("--no-calibrate-prob", action="store_true", help="Disable probability calibration")
    ap.add_argument("--max-games", type=int, default=10, help="Max games rows in best_edges snapshot")
    ap.add_argument("--max-props", type=int, default=25, help="Max props rows in best_edges snapshot")
    args = ap.parse_args()

    end_d = _parse_date(args.end) if args.end else (datetime.today().date() - timedelta(days=1))
    if args.start:
        start_d = _parse_date(args.start)
    else:
        start_d = end_d - timedelta(days=int(args.days))

    dates = _date_range(start_d, end_d)

    results: list[Result] = []
    for ds in dates:
        r = rebuild_one(
            date_str=ds,
            overwrite=bool(args.overwrite),
            calibrate_sigma=not bool(args.no_calibrate_sigma),
            calibrate_prob=not bool(args.no_calibrate_prob),
            max_games=int(args.max_games),
            max_props=int(args.max_props),
        )
        results.append(r)
        if r.status == "ok":
            print({"date": r.date_str, "status": r.status, "edges_rows": r.edges_rows})

    # Summary
    ok = [r for r in results if r.status == "ok"]
    skipped = [r for r in results if r.status.startswith("skip_")]
    errors = [r for r in results if r.status.startswith("error_") or r.status.startswith("error")]
    print(
        {
            "start": start_d.strftime("%Y-%m-%d"),
            "end": end_d.strftime("%Y-%m-%d"),
            "processed_ok": len(ok),
            "skipped": len(skipped),
            "errors": len(errors),
        }
    )

    if errors:
        # Print a short error list to make it easy to spot patterns
        print({"errors": [{"date": r.date_str, "status": r.status} for r in errors[:20]]})
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
