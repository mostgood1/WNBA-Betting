"""Audit SmartSim JSON minutes sanity.

Purpose:
- Catch pathological minutes allocations (e.g., >44 regulation cap) and missing min_mean exports.
- Verify minutes totals per team sum to ~200 for WNBA pregame sims.

Designed to run in local/CI daily pipelines.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parents[1]
_DATA_ROOT_ENV = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(_DATA_ROOT_ENV).expanduser().resolve() if _DATA_ROOT_ENV else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
        return v
    except Exception:
        return None


def _team_minutes_from_obj(obj: dict[str, Any], side: str) -> list[float]:
    players = (obj.get("players") or {})
    if not isinstance(players, dict):
        return []
    arr = players.get(side) or []
    if not isinstance(arr, list):
        return []
    mins: list[float] = []
    for p in arr:
        if not isinstance(p, dict):
            continue
        v = _safe_float(p.get("min_mean"))
        mins.append(float(v) if v is not None else 0.0)
    return mins


def _count_cards_sim_detail_games(path: Path) -> int:
    try:
        if not path.exists() or path.stat().st_size <= 0:
            return 0
        payload = json.loads(path.read_text(encoding="utf-8"))
        games = payload.get("games") if isinstance(payload, dict) else None
        if not isinstance(games, list):
            return 0
        count = 0
        for game in games:
            if not isinstance(game, dict):
                continue
            home_tri = str(game.get("home_tri") or "").strip().upper()
            away_tri = str(game.get("away_tri") or "").strip().upper()
            if home_tri and away_tri:
                count += 1
        return count
    except Exception:
        return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    ap.add_argument("--processed", default=str(PROC_DIR), help="Processed dir (default data/processed)")
    ap.add_argument("--cap", type=float, default=44.0, help="Max allowed min_mean per player (default 44.0)")
    ap.add_argument(
        "--sum-tol",
        type=float,
        default=0.75,
        help="Allowed deviation from 200 minutes team sum (default 0.75)",
    )
    ap.add_argument(
        "--fail-on-error-json",
        action="store_true",
        help="Fail if any smart_sim_<date>_*.json contains an 'error' field",
    )
    args = ap.parse_args()

    date = str(args.date).strip()
    proc = Path(str(args.processed))
    fps = sorted(proc.glob(f"smart_sim_{date}_*.json"))
    if not fps:
        cards_sim_detail = proc / f"cards_sim_detail_{date}.json"
        cards_sim_detail_games = _count_cards_sim_detail_games(cards_sim_detail)
        if cards_sim_detail_games > 0:
            print(
                f"No raw SmartSim files found for {date} in {proc}; "
                f"cards_sim_detail_{date}.json covers {cards_sim_detail_games} game(s), skipping raw minutes audit"
            )
            return 0
        print(f"No SmartSim files found for {date} in {proc}")
        return 2

    cap = float(args.cap)
    sum_tol = float(args.sum_tol)

    bad: list[tuple[str, str, str]] = []
    warn: list[tuple[str, str]] = []

    for fp in fps:
        try:
            obj = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            bad.append((fp.name, "file", f"invalid_json: {e}"))
            continue

        if not isinstance(obj, dict):
            bad.append((fp.name, "file", "not_a_dict"))
            continue

        if obj.get("error") is not None:
            msg = f"error={obj.get('error')}"
            if args.fail_on_error_json:
                bad.append((fp.name, "file", msg))
            else:
                warn.append((fp.name, msg))
            continue

        for side in ("home", "away"):
            mins = _team_minutes_from_obj(obj, side)
            if not mins:
                bad.append((fp.name, side, "empty_players_or_minutes"))
                continue

            s = sum(mins)
            mx = max(mins)
            if abs(s - 200.0) > sum_tol:
                bad.append((fp.name, side, f"sum_minutes={s:.3f}"))
            if mx > cap + 1e-6:
                bad.append((fp.name, side, f"max_min_mean={mx:.3f} > cap={cap:.1f}"))

    if warn:
        print("WARN: SmartSim JSON contains errors (skipped):")
        for fn, msg in warn:
            print(f"  - {fn}: {msg}")

    if bad:
        print("FAIL: SmartSim minutes audit failed:")
        for fn, side, msg in bad:
            print(f"  - {fn} [{side}]: {msg}")
        return 1

    print(f"OK: SmartSim minutes audit passed for {date} (files={len(fps)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
