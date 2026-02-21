"""Audit SmartSim roster augmentation sources.

Why this exists
---------------
SmartSim can augment the props-based roster with processed boxscores (post-game)
when coverage is sparse. That is great for historical realism, but it is a form
of lookahead if you are trying to validate *pregame* prop-selection performance.

This tool quantifies how often SmartSim outputs appear augmented beyond the
props pool, and flags cases that were *likely* augmented from processed
boxscores (because a same-day boxscores_<date>.csv exists and the props pool was
below the 8-player guardrail).

Usage
-----
  python tools/audit_smartsim_roster_sources.py --start 2025-12-25 --end 2026-02-12

Outputs
-------
Prints a summary and optionally writes a CSV of per-game stats.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _date_range(start: str, end: str) -> list[str]:
    s = pd.to_datetime(start, errors="coerce")
    e = pd.to_datetime(end, errors="coerce")
    if pd.isna(s) or pd.isna(e):
        raise SystemExit("Bad --start/--end date")
    if e < s:
        s, e = e, s
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(s, e, freq="D")]


def _safe_set(series: pd.Series) -> set[str]:
    if series is None:
        return set()
    try:
        return set(str(x).strip() for x in series.dropna().astype(str).tolist() if str(x).strip())
    except Exception:
        return set()


def _props_player_set(props_df: pd.DataFrame, team_tri: str) -> set[str]:
    if props_df is None or props_df.empty:
        return set()
    df = props_df
    if "team" not in df.columns or "player" not in df.columns:
        # props_predictions schema varies; try common alternatives.
        team_col = "team" if "team" in df.columns else ("TEAM" if "TEAM" in df.columns else None)
        name_col = "player" if "player" in df.columns else (
            "player_name" if "player_name" in df.columns else ("PLAYER_NAME" if "PLAYER_NAME" in df.columns else None)
        )
        if not team_col or not name_col:
            return set()
        df = df.rename(columns={team_col: "team", name_col: "player"})

    t = str(team_tri).strip().upper()
    tmp = df.copy()
    tmp["team"] = tmp["team"].astype(str).str.upper().str.strip()
    tmp["player"] = tmp["player"].astype(str).str.strip()
    tmp = tmp[(tmp["team"] == t) & (tmp["player"].ne(""))].copy()
    return _safe_set(tmp["player"])


def _sim_player_set(sim_obj: dict, side: str) -> set[str]:
    try:
        players = ((sim_obj.get("players") or {}).get(side) or [])
        out = set()
        for p in players:
            if not isinstance(p, dict):
                continue
            nm = str(p.get("player_name") or "").strip()
            if nm:
                out.add(nm)
        return out
    except Exception:
        return set()


@dataclass
class GameAuditRow:
    date: str
    file: str
    home: str
    away: str
    props_home_n: int
    props_away_n: int
    sim_home_n: int
    sim_away_n: int
    sim_home_augmented: bool
    sim_away_augmented: bool
    boxscores_exists: bool
    likely_postgame_home: bool
    likely_postgame_away: bool


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit SmartSim roster augmentation sources")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--out", default="", help="Optional CSV output path")
    args = ap.parse_args()

    rows: list[GameAuditRow] = []

    for ds in _date_range(args.start, args.end):
        props_path = PROCESSED / f"props_predictions_{ds}.csv"
        if not props_path.exists():
            continue
        try:
            props_df = pd.read_csv(props_path)
        except Exception:
            continue

        boxscores_path = PROCESSED / f"boxscores_{ds}.csv"
        boxscores_exists = bool(boxscores_path.exists())

        sim_files = sorted(PROCESSED.glob(f"smart_sim_{ds}_*.json"))
        for fp in sim_files:
            try:
                sim_obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue

            home = str(sim_obj.get("home") or "").strip().upper()
            away = str(sim_obj.get("away") or "").strip().upper()
            if not home or not away:
                # fallback: try parse filename smart_sim_<date>_<HOME>_<AWAY>.json
                parts = fp.stem.split("_")
                if len(parts) >= 5:
                    home = home or parts[-2].strip().upper()
                    away = away or parts[-1].strip().upper()

            props_home = _props_player_set(props_df, home)
            props_away = _props_player_set(props_df, away)
            sim_home = _sim_player_set(sim_obj, "home")
            sim_away = _sim_player_set(sim_obj, "away")

            sim_home_aug = len(sim_home) > len(props_home)
            sim_away_aug = len(sim_away) > len(props_away)

            likely_post_home = bool(boxscores_exists and (len(props_home) < 8) and sim_home_aug)
            likely_post_away = bool(boxscores_exists and (len(props_away) < 8) and sim_away_aug)

            rows.append(
                GameAuditRow(
                    date=ds,
                    file=fp.name,
                    home=home,
                    away=away,
                    props_home_n=len(props_home),
                    props_away_n=len(props_away),
                    sim_home_n=len(sim_home),
                    sim_away_n=len(sim_away),
                    sim_home_augmented=sim_home_aug,
                    sim_away_augmented=sim_away_aug,
                    boxscores_exists=boxscores_exists,
                    likely_postgame_home=likely_post_home,
                    likely_postgame_away=likely_post_away,
                )
            )

    if not rows:
        print("No rows; missing props_predictions and/or smart_sim JSONs in range")
        return 2

    df = pd.DataFrame([r.__dict__ for r in rows])

    sides = 2 * len(df)
    aug_sides = int(df["sim_home_augmented"].sum() + df["sim_away_augmented"].sum())
    likely_post_sides = int(df["likely_postgame_home"].sum() + df["likely_postgame_away"].sum())
    boxscores_game_rate = float(df["boxscores_exists"].mean())

    print("=== SmartSim roster audit ===")
    print(f"Games: {len(df)}")
    print(f"Sides: {sides}")
    print(f"Boxscores present (game rate): {boxscores_game_rate:.3f}")
    print(f"Augmented vs props pool (side rate): {aug_sides}/{sides} = {aug_sides / max(1, sides):.3f}")
    print(
        f"Likely postgame roster augmentation (boxscores exists & props<8 & augmented): "
        f"{likely_post_sides}/{sides} = {likely_post_sides / max(1, sides):.3f}"
    )

    # Show the worst offenders (largest augmentation gaps)
    df["home_gap"] = df["sim_home_n"] - df["props_home_n"]
    df["away_gap"] = df["sim_away_n"] - df["props_away_n"]
    df["max_gap"] = df[["home_gap", "away_gap"]].max(axis=1)

    top = df.sort_values(["max_gap", "date"], ascending=[False, True]).head(15)
    print("\nTop 15 by max roster gap (sim_n - props_n):")
    cols = [
        "date",
        "home",
        "away",
        "props_home_n",
        "sim_home_n",
        "props_away_n",
        "sim_away_n",
        "boxscores_exists",
        "likely_postgame_home",
        "likely_postgame_away",
        "file",
    ]
    print(top[cols].to_string(index=False))

    if args.out:
        outp = Path(args.out)
        if not outp.is_absolute():
            outp = ROOT / outp
        outp.parent.mkdir(parents=True, exist_ok=True)
        df.drop(columns=["home_gap", "away_gap", "max_gap"], errors="ignore").to_csv(outp, index=False)
        print(f"\nWrote: {outp}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
