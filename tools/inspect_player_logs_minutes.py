from __future__ import annotations

import argparse
from typing import Any

import numpy as np
import pandas as pd


def _find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for k in candidates:
        if k.lower() in cols:
            return cols[k.lower()]
    return None


def _to_min(v: Any) -> float:
    try:
        if isinstance(v, str) and ":" in v:
            mm, ss = v.split(":", 1)
            return float(mm) + float(ss) / 60.0
        x = pd.to_numeric(v, errors="coerce")
        return float(x) if pd.notna(x) else 0.0
    except Exception:
        return 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--player", action="append", required=True, help="Exact PLAYER_NAME to filter; can repeat")
    ap.add_argument("--cutoff", required=True, help="YYYY-MM-DD cutoff (strictly before)")
    ap.add_argument("--n", type=int, default=12)
    args = ap.parse_args()

    logs = pd.read_csv("data/processed/player_logs.csv")

    date_col = _find_col(logs, ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT", "date"])
    name_col = _find_col(logs, ["PLAYER_NAME", "player_name", "namePlayer"])
    team_col = _find_col(logs, ["TEAM_ABBREVIATION", "team", "slugTeam"])
    min_col = _find_col(logs, ["MIN", "min"])

    if not date_col or not name_col or not min_col:
        raise SystemExit(f"Missing required columns: date={date_col} name={name_col} min={min_col}")

    logs[date_col] = pd.to_datetime(logs[date_col], errors="coerce")
    cutoff = pd.to_datetime(args.cutoff)
    logs["_min_f"] = logs[min_col].map(_to_min)

    for player in args.player:
        g = logs[(logs[name_col].astype(str) == str(player)) & (logs[date_col] < cutoff)].copy()
        g = g.sort_values(date_col)
        print(f"\n=== {player} before {args.cutoff} ===")
        if g.empty:
            print("NO ROWS")
            continue
        cols = [date_col]
        if team_col:
            cols.append(team_col)
        cols.append("_min_f")
        tail = g.tail(int(args.n))[cols]
        print(tail.to_string(index=False))
        last10 = g.tail(10)["_min_f"].to_numpy(float)
        if last10.size:
            print(f"last10 mean={float(np.mean(last10)):.3f} median={float(np.median(last10)):.3f} std={float(np.std(last10)):.3f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
