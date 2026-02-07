"""Batch rebuild daily recommendation artifacts over a window.

Primary intent: refresh exported CSVs after logic/scoring fixes.

This tool rebuilds:
- recommendations_YYYY-MM-DD.csv (games)
- props_recommendations_YYYY-MM-DD.csv (cards; optional)
- best_edges_games_YYYY-MM-DD.csv + best_edges_props_YYYY-MM-DD.csv (authoritative snapshots)

It is designed to be resumable and to skip dates that are missing prerequisite inputs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, asdict
from datetime import date, timedelta
import json
from pathlib import Path
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


@dataclass
class DayResult:
    date: str
    games_recs: str
    props_cards: str
    snapshots: str
    seconds: float
    notes: list[str]


def _iter_days(end: date, days: int) -> list[str]:
    days = int(days)
    if days <= 0:
        return []
    start = end - timedelta(days=days - 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default=date.today().isoformat(), help="End date YYYY-MM-DD (inclusive)")
    ap.add_argument("--days", type=int, default=30, help="Number of days ending at --end")
    ap.add_argument("--max-games", type=int, default=10)
    ap.add_argument("--max-props", type=int, default=25)
    ap.add_argument("--skip-props-cards", action="store_true", help="Skip props_recommendations_<date>.csv export")
    ap.add_argument("--snapshots-only", action="store_true", help="Only rebuild best_edges snapshots (no rec exports)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--out",
        default=str(PROCESSED / f"rebuild_recommendations_window_{date.today().isoformat()}.json"),
        help="Summary JSON output path",
    )
    args = ap.parse_args()

    try:
        end_d = date.fromisoformat(str(args.end))
    except Exception:
        raise SystemExit("Invalid --end (expected YYYY-MM-DD)")

    PROCESSED.mkdir(parents=True, exist_ok=True)

    days = _iter_days(end_d, int(args.days))
    results: list[DayResult] = []

    cli_base = [sys.executable, "-m", "nba_betting.cli"]

    def _run(args_list: list[str]) -> tuple[bool, str]:
        try:
            cp = subprocess.run(
                cli_base + args_list,
                cwd=str(ROOT),
                capture_output=True,
                text=True,
            )
            if cp.returncode == 0:
                return True, (cp.stdout or "").strip()
            msg = ((cp.stdout or "") + "\n" + (cp.stderr or "")).strip()
            msg = msg[-800:] if len(msg) > 800 else msg
            return False, msg
        except Exception as e:
            return False, str(e)

    for ds in days:
        t0 = time.time()
        notes: list[str] = []

        # Basic prerequisites
        pred = PROCESSED / f"predictions_{ds}.csv"
        props_edges = PROCESSED / f"props_edges_{ds}.csv"

        if not pred.exists() and not args.snapshots_only:
            notes.append("missing predictions")

        if not props_edges.exists():
            # Snapshots can still be written (will likely be empty for props)
            notes.append("missing props_edges")

        if args.dry_run:
            results.append(
                DayResult(
                    date=ds,
                    games_recs="skipped(dry_run)",
                    props_cards="skipped(dry_run)",
                    snapshots="skipped(dry_run)",
                    seconds=round(time.time() - t0, 3),
                    notes=notes,
                )
            )
            continue

        games_recs_status = "skipped"
        props_cards_status = "skipped"
        snapshots_status = "skipped"

        try:
            if not args.snapshots_only:
                ok, msg = _run(["export-recommendations", "--date", ds])
                games_recs_status = "ok" if ok else f"error: {msg}"
        except Exception as e:
            games_recs_status = f"error: {e}"

        try:
            if (not args.snapshots_only) and (not bool(args.skip_props_cards)):
                ok, msg = _run(["export-props-recommendations", "--date", ds])
                props_cards_status = "ok" if ok else f"error: {msg}"
        except Exception as e:
            props_cards_status = f"error: {e}"

        try:
            ok, msg = _run(
                [
                    "export-best-edges",
                    "--date",
                    ds,
                    "--max-games",
                    str(int(args.max_games)),
                    "--max-props",
                    str(int(args.max_props)),
                    "--overwrite",
                ]
            )
            snapshots_status = "ok" if ok else f"error: {msg}"
        except Exception as e:
            snapshots_status = f"error: {e}"

        results.append(
            DayResult(
                date=ds,
                games_recs=games_recs_status,
                props_cards=props_cards_status,
                snapshots=snapshots_status,
                seconds=round(time.time() - t0, 3),
                notes=notes,
            )
        )

    # Write summary
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "window": {"end": end_d.isoformat(), "days": int(args.days), "dates": days},
        "options": {
            "max_games": int(args.max_games),
            "max_props": int(args.max_props),
            "skip_props_cards": bool(args.skip_props_cards),
            "snapshots_only": bool(args.snapshots_only),
            "dry_run": bool(args.dry_run),
        },
        "results": [asdict(r) for r in results],
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    # Concise console summary
    ok_snap = sum(1 for r in results if r.snapshots == "ok")
    ok_rec = sum(1 for r in results if r.games_recs == "ok")
    print(f"Rebuilt window: {days[0] if days else '-'} to {days[-1] if days else '-'}")
    print(f"games recs ok: {ok_rec}/{len(results)}")
    print(f"snapshots ok: {ok_snap}/{len(results)}")
    print(f"Wrote summary: {out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
