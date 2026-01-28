"""Build period (quarters/halves) actuals from ESPN play-by-play.

Outputs a CSV under data/processed/ that contains, per game:
- date, home_tri, away_tri, game_id
- q1_home_act, q1_away_act, ..., q4_home_act, q4_away_act
- h1_home_act, h1_away_act, h2_home_act, h2_away_act

This is intended as an actuals source for tools/build_smart_sim_quarter_eval.py
when NBA API line score data is missing/out-of-date.

Usage:
  python tools/build_period_actuals_from_pbp_espn.py --date 2026-01-21
  python tools/build_period_actuals_from_pbp_espn.py --start 2026-01-21 --end 2026-01-24
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PROC = REPO_ROOT / "data" / "processed"


def _num(x: Any) -> float | None:
    try:
        v = float(x)
        if np.isfinite(v):
            return float(v)
        return None
    except Exception:
        return None


def _scores_from_row(r: pd.Series) -> tuple[float | None, float | None]:
    for hc, ac in [
        ("home_score", "away_score"),
        ("scoreHome", "scoreAway"),
        ("homeScore", "awayScore"),
        ("home", "away"),
    ]:
        if hc in r.index and ac in r.index:
            h = _num(r.get(hc))
            a = _num(r.get(ac))
            if h is not None and a is not None:
                return h, a
    return None, None


def _quarter_end_scores(gdf: pd.DataFrame) -> dict[int, tuple[float, float]]:
    """Return {period: (home_end, away_end)} for periods 1..4 when present."""
    tmp = gdf.copy()

    # clock ordering: take the earliest clock remaining in each period (i.e., end of period)
    if "clock_sec_remaining" in tmp.columns:
        tmp["_clock_sec"] = pd.to_numeric(tmp["clock_sec_remaining"], errors="coerce")
    elif "clock" in tmp.columns:
        # Fallback: not ideal; will likely be NaN
        tmp["_clock_sec"] = pd.to_numeric(tmp["clock"], errors="coerce")
    else:
        tmp["_clock_sec"] = np.nan

    if "sequence" in tmp.columns:
        tmp["_action_num"] = pd.to_numeric(tmp["sequence"], errors="coerce")
    elif "actionNumber" in tmp.columns:
        tmp["_action_num"] = pd.to_numeric(tmp["actionNumber"], errors="coerce")
    else:
        tmp["_action_num"] = np.nan

    per_end: dict[int, tuple[float, float]] = {}
    for p in (1, 2, 3, 4):
        sub = tmp[tmp.get("period") == p]
        if sub is None or sub.empty:
            continue
        sub2 = sub.sort_values(["_clock_sec", "_action_num"], ascending=[True, False], na_position="last")
        hs = as_ = None
        for _, rr in sub2.iterrows():
            hs, as_ = _scores_from_row(rr)
            if hs is not None and as_ is not None:
                break
        if hs is None or as_ is None:
            continue
        per_end[p] = (float(hs), float(as_))
    return per_end


def _quarter_points_from_ends(per_end: dict[int, tuple[float, float]]) -> dict[str, float]:
    """Convert cumulative end-of-quarter scores into per-quarter points."""
    out: dict[str, float] = {}

    prev_h = 0.0
    prev_a = 0.0
    for q in (1, 2, 3, 4):
        if q not in per_end:
            return {}
        h_end, a_end = per_end[q]
        out[f"q{q}_home_act"] = float(h_end - prev_h)
        out[f"q{q}_away_act"] = float(a_end - prev_a)
        prev_h, prev_a = float(h_end), float(a_end)

    out["h1_home_act"] = float(out["q1_home_act"] + out["q2_home_act"])
    out["h1_away_act"] = float(out["q1_away_act"] + out["q2_away_act"])
    out["h2_home_act"] = float(out["q3_home_act"] + out["q4_home_act"])
    out["h2_away_act"] = float(out["q3_away_act"] + out["q4_away_act"])
    return out


def build_for_date(date_str: str, rate_delay: float = 0.15) -> pd.DataFrame:
    pbp_path = PROC / f"pbp_espn_{date_str}.csv"
    dfe: pd.DataFrame | None = None

    if pbp_path.exists():
        try:
            dfe = pd.read_csv(pbp_path)
        except Exception:
            dfe = None

    # Fetch if missing or unusable
    if dfe is None or dfe.empty or ("home_score" not in dfe.columns) or ("away_score" not in dfe.columns):
        from nba_betting.pbp_espn import fetch_pbp_espn_for_date

        dfe, _ = fetch_pbp_espn_for_date(date_str, only_final=True, rate_delay=rate_delay)

    if dfe is None or dfe.empty:
        return pd.DataFrame()

    key_col = "game_id" if ("game_id" in dfe.columns and dfe["game_id"].notna().any()) else ("event_id" if "event_id" in dfe.columns else None)
    if key_col is None:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for gid, gdf in dfe.groupby(key_col):
        gdf = gdf.copy()
        htri = str(gdf.get("home_tri").dropna().iloc[0]).upper().strip() if ("home_tri" in gdf.columns and gdf["home_tri"].notna().any()) else ""
        atri = str(gdf.get("away_tri").dropna().iloc[0]).upper().strip() if ("away_tri" in gdf.columns and gdf["away_tri"].notna().any()) else ""
        if not htri or not atri:
            continue

        per_end = _quarter_end_scores(gdf)
        pts = _quarter_points_from_ends(per_end)
        if not pts:
            continue

        rec: dict[str, Any] = {
            "date": date_str,
            "home_tri": htri,
            "away_tri": atri,
            "game_id": str(gid).strip(),
        }
        rec.update(pts)
        rows.append(rec)

    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, required=False, help="Single date YYYY-MM-DD")
    ap.add_argument("--start", type=str, required=False, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, required=False, help="End date YYYY-MM-DD")
    ap.add_argument("--out", type=str, required=False, help="Output path")
    ap.add_argument("--rate-delay", type=float, default=0.15, help="Delay between ESPN requests")
    args = ap.parse_args()

    if args.date:
        start = end = pd.to_datetime(args.date).strftime("%Y-%m-%d")
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --date or --start and --end")
        start = pd.to_datetime(args.start).strftime("%Y-%m-%d")
        end = pd.to_datetime(args.end).strftime("%Y-%m-%d")

    dfs: list[pd.DataFrame] = []
    for ds in pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="D").strftime("%Y-%m-%d"):
        df = build_for_date(ds, rate_delay=float(args.rate_delay))
        if df is not None and not df.empty:
            dfs.append(df)

    out_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    out_path = Path(args.out) if args.out else (PROC / f"smart_sim_quarter_eval_{start}_{end}_pbp_espn_actuals.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print({"ok": True, "rows": int(len(out_df)), "out": str(out_path), "start": start, "end": end})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
