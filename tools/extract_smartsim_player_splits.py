"""Extract SmartSim player quarter + scenario distributions into flat CSVs.

Reads:
- data/processed/smart_sim_<date>_<HOME>_<AWAY>.json

Writes (defaults):
- data/processed/smartsim_player_quarters_<start>_<end>.csv
- data/processed/smartsim_player_scenarios_<start>_<end>.csv

This is intended for modeling/analysis of quarter-level props and game-script (close/medium/blowout)
conditioned props.

Usage:
  python tools/extract_smartsim_player_splits.py --start 2026-01-17 --end 2026-01-23
  python tools/extract_smartsim_player_splits.py --date 2026-01-24
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


_QKEY_RE = re.compile(r"^p(\d+)$")


def _iter_dates(start: str, end: str) -> Iterable[str]:
    for d in pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="D"):
        yield d.strftime("%Y-%m-%d")


def _safe_float(x: Any) -> float | None:
    try:
        v = float(x)
        if np.isfinite(v):
            return float(v)
        return None
    except Exception:
        return None


def _sorted_quantile_keys(d: dict[str, Any]) -> list[str]:
    keys = [str(k) for k in d.keys()]

    def key_fn(k: str):
        m = _QKEY_RE.match(k)
        if not m:
            return (1, k)
        return (0, int(m.group(1)))

    return sorted(keys, key=key_fn)


def _flatten_quantiles(prefix: str, q: Any, row: dict[str, Any]) -> None:
    if not isinstance(q, dict):
        return
    for k in _sorted_quantile_keys(q):
        row[f"{prefix}_{k}"] = _safe_float(q.get(k))


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=str, help="Single date YYYY-MM-DD")
    g.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD (required when using --start)")

    ap.add_argument("--out-quarters", type=str, default=None)
    ap.add_argument("--out-scenarios", type=str, default=None)
    ap.add_argument("--include-empty-scenarios", action="store_true", help="Include scenario rows even when n=0")

    args = ap.parse_args()

    if args.start and not args.end:
        raise SystemExit("--end is required when using --start")

    if args.date:
        dates = [str(args.date).strip()]
        start = end = dates[0]
    else:
        start = str(args.start).strip()
        end = str(args.end).strip()
        dates = list(_iter_dates(start, end))

    out_q = Path(args.out_quarters) if args.out_quarters else (PROCESSED / f"smartsim_player_quarters_{start}_{end}.csv")
    out_s = Path(args.out_scenarios) if args.out_scenarios else (PROCESSED / f"smartsim_player_scenarios_{start}_{end}.csv")

    q_rows: list[dict[str, Any]] = []
    s_rows: list[dict[str, Any]] = []

    for ds in dates:
        sim_files = sorted(PROCESSED.glob(f"smart_sim_{ds}_*.json"))
        for fp in sim_files:
            obj = _load_json(fp)
            if not isinstance(obj, dict):
                continue

            game_id = obj.get("game_id")
            try:
                game_id = int(float(game_id)) if game_id is not None and str(game_id).lower() != "nan" else None
            except Exception:
                game_id = None

            home = str(obj.get("home") or "").upper().strip()
            away = str(obj.get("away") or "").upper().strip()
            n_sims = obj.get("n_sims")
            try:
                n_sims = int(float(n_sims)) if n_sims is not None and str(n_sims).lower() != "nan" else None
            except Exception:
                n_sims = None

            players = obj.get("players") if isinstance(obj.get("players"), dict) else {}

            for side in ("home", "away"):
                team = home if side == "home" else away
                arr = players.get(side) if isinstance(players.get(side), list) else []
                for pr in arr:
                    if not isinstance(pr, dict):
                        continue
                    base = {
                        "date": ds,
                        "game_id": game_id,
                        "home": home,
                        "away": away,
                        "side": side,
                        "team": team,
                        "n_sims": n_sims,
                        "player_id": pr.get("player_id"),
                        "player_name": pr.get("player_name"),
                    }

                    # Quarter rows
                    quarters = pr.get("quarters") if isinstance(pr.get("quarters"), dict) else None
                    if quarters:
                        for qk in ("q1", "q2", "q3", "q4"):
                            qobj = quarters.get(qk) if isinstance(quarters.get(qk), dict) else None
                            if not qobj:
                                continue
                            r = dict(base)
                            r["split_type"] = "quarter"
                            r["split"] = qk
                            _flatten_quantiles("pts", qobj.get("pts_q"), r)
                            _flatten_quantiles("reb", qobj.get("reb_q"), r)
                            _flatten_quantiles("ast", qobj.get("ast_q"), r)
                            _flatten_quantiles("threes", qobj.get("threes_q"), r)
                            q_rows.append(r)

                    # Scenario rows
                    scenarios = pr.get("scenarios") if isinstance(pr.get("scenarios"), dict) else None
                    if scenarios:
                        for sk in ("close", "medium", "blowout"):
                            sobj = scenarios.get(sk) if isinstance(scenarios.get(sk), dict) else None
                            if not sobj:
                                continue
                            n = sobj.get("n")
                            try:
                                n = int(float(n)) if n is not None and str(n).lower() != "nan" else 0
                            except Exception:
                                n = 0
                            if n <= 0 and not bool(args.include_empty_scenarios):
                                continue
                            r = dict(base)
                            r["split_type"] = "scenario"
                            r["split"] = sk
                            r["n"] = n
                            _flatten_quantiles("pts", sobj.get("pts_q"), r)
                            _flatten_quantiles("reb", sobj.get("reb_q"), r)
                            _flatten_quantiles("ast", sobj.get("ast_q"), r)
                            _flatten_quantiles("threes", sobj.get("threes_q"), r)
                            _flatten_quantiles("pra", sobj.get("pra_q"), r)
                            s_rows.append(r)

    out_q.parent.mkdir(parents=True, exist_ok=True)
    out_s.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(q_rows).to_csv(out_q, index=False)
    pd.DataFrame(s_rows).to_csv(out_s, index=False)

    print({
        "dates": f"{start}..{end}",
        "quarters_rows": int(len(q_rows)),
        "scenarios_rows": int(len(s_rows)),
        "out_quarters": str(out_q),
        "out_scenarios": str(out_s),
    })

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
