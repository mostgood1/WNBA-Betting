from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from audit_smart_sim_player_coverage import _load_expected_players, _load_smartsim_names  # type: ignore


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit SmartSim coverage across a date range.")
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--max-issues", type=int, default=200)
    args = ap.parse_args()

    start = _parse_date(args.start)
    end = _parse_date(args.end)

    _DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
    data_root = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (REPO_ROOT / "data")
    processed = data_root / "processed"
    issues: list[dict] = []
    days_scanned = 0
    games_scanned = 0

    for d in _daterange(start, end):
        ds = d.isoformat()
        props_path = processed / f"props_predictions_{ds}.csv"
        if not props_path.exists():
            continue
        smarts = sorted(processed.glob(f"smart_sim_{ds}_*.json"))
        if not smarts:
            continue
        days_scanned += 1

        for fp in smarts:
            games_scanned += 1
            parts = fp.stem.split("_")
            if len(parts) < 5:
                continue
            home_tri = parts[-2].strip().upper()
            away_tri = parts[-1].strip().upper()

            try:
                home_names, away_names = _load_smartsim_names(fp)
            except Exception as e:
                issues.append({"date": ds, "file": str(fp), "error": repr(e)})
                continue

            exp_home = _load_expected_players(props_path, team_tri=home_tri, opp_tri=away_tri)
            exp_away = _load_expected_players(props_path, team_tri=away_tri, opp_tri=home_tri)

            miss_home = sorted(list(exp_home - home_names))
            miss_away = sorted(list(exp_away - away_names))

            if miss_home or miss_away:
                issues.append(
                    {
                        "date": ds,
                        "file": str(fp),
                        "home": home_tri,
                        "away": away_tri,
                        "missing_home": miss_home[:25],
                        "missing_away": miss_away[:25],
                    }
                )
                if len(issues) >= int(args.max_issues):
                    break
        if len(issues) >= int(args.max_issues):
            break

    print(
        json.dumps(
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "days_scanned": days_scanned,
                "games_scanned": games_scanned,
                "issues": issues,
                "issues_n": len(issues),
                "ran_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
