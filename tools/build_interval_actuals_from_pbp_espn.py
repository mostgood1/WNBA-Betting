"""Build 3-minute segment actuals (regulation) from ESPN play-by-play.

This is the "ground truth" counterpart to SmartSim's 3-minute `intervals` ladder.

Outputs a wide CSV under data/processed/ that contains, per game:
- date, home_tri, away_tri, game_id
- seg1_total_act .. seg16_total_act  (3-minute segment totals, both teams combined)
- cum1_total_act .. cum16_total_act  (cumulative total through that segment)

Segments are anchored to 12-minute quarters, 4 segments per quarter:
Q1 12-9, 9-6, 6-3, 3-0, then repeat for Q2..Q4.

Usage:
  python tools/build_interval_actuals_from_pbp_espn.py --date 2026-02-05
  python tools/build_interval_actuals_from_pbp_espn.py --start 2026-02-01 --end 2026-02-05
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PROC = REPO_ROOT / "data" / "processed"

# Quarter has 12 minutes. Segment boundaries are expressed as seconds remaining.
# End of each 3-minute segment: 9:00, 6:00, 3:00, 0:00 remaining.
_SEG_BOUNDARIES_SEC_REMAIN = (9 * 60, 6 * 60, 3 * 60, 0)

# NBA overtime is 5 minutes; treat each OT period as a single 5-minute interval.
_MAX_OT_PERIODS = 6


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


def _prep_clock_and_seq(gdf: pd.DataFrame) -> pd.DataFrame:
    tmp = gdf.copy()

    if "clock_sec_remaining" in tmp.columns:
        tmp["_clock_sec"] = pd.to_numeric(tmp["clock_sec_remaining"], errors="coerce")
    elif "clock" in tmp.columns:
        # Fallback: may already be seconds or mm:ss strings; best-effort numeric parse only.
        tmp["_clock_sec"] = pd.to_numeric(tmp["clock"], errors="coerce")
    else:
        tmp["_clock_sec"] = np.nan

    if "sequence" in tmp.columns:
        tmp["_action_num"] = pd.to_numeric(tmp["sequence"], errors="coerce")
    elif "actionNumber" in tmp.columns:
        tmp["_action_num"] = pd.to_numeric(tmp["actionNumber"], errors="coerce")
    else:
        tmp["_action_num"] = np.nan

    return tmp


def _score_at_boundary(sub: pd.DataFrame, boundary_sec_rem: int) -> tuple[float | None, float | None]:
    """Return score at the boundary time (in seconds remaining) for a period.

    We use the last known score before the clock drops *below* the boundary.
    In practice, this is the play with the smallest clock_sec_remaining that is
    still >= boundary.
    """
    if sub is None or sub.empty:
        return None, None

    s = sub.copy()
    s = s[pd.to_numeric(s.get("_clock_sec"), errors="coerce").notna()].copy()
    if s.empty:
        return None, None

    s = s[s["_clock_sec"] >= float(boundary_sec_rem)].copy()
    if s.empty:
        return None, None

    s = s.sort_values(["_clock_sec", "_action_num"], ascending=[True, False], na_position="last")
    for _, rr in s.iterrows():
        hs, as_ = _scores_from_row(rr)
        if hs is not None and as_ is not None:
            return float(hs), float(as_)
    return None, None


def _segment_actuals_from_game_pbp(gdf: pd.DataFrame) -> dict[str, float] | None:
    """Compute seg totals and cum totals for regulation + (optional) OTs.

    Regulation: 16 segments (4 quarters x 4 segments).
    Overtime: up to _MAX_OT_PERIODS additional segments, one per OT period.
    """
    tmp = _prep_clock_and_seq(gdf)

    out: dict[str, float] = {}

    prev_h = 0.0
    prev_a = 0.0
    seg_idx = 0

    for q in (1, 2, 3, 4):
        sub = tmp[tmp.get("period") == q]
        if sub is None or sub.empty:
            return None

        # At start of quarter, cumulative is previous quarter end.
        q_start_h, q_start_a = prev_h, prev_a

        # Scores at each segment boundary within the quarter.
        boundary_scores: list[tuple[float, float]] = []
        for b in _SEG_BOUNDARIES_SEC_REMAIN:
            hs, as_ = _score_at_boundary(sub, int(b))
            if hs is None or as_ is None:
                return None
            boundary_scores.append((float(hs), float(as_)))

        # Segment 1 is (start -> 9:00), 2 is (9:00 -> 6:00), etc.
        prev_boundary_h, prev_boundary_a = q_start_h, q_start_a
        for (bh, ba) in boundary_scores:
            seg_idx += 1
            seg_home = float(bh - prev_boundary_h)
            seg_away = float(ba - prev_boundary_a)
            seg_total = float(seg_home + seg_away)

            out[f"seg{seg_idx}_total_act"] = seg_total
            out[f"cum{seg_idx}_total_act"] = float(bh + ba)

            prev_boundary_h, prev_boundary_a = float(bh), float(ba)

        prev_h, prev_a = prev_boundary_h, prev_boundary_a

    # Overtime periods, if present (period 5+)
    try:
        max_period = int(pd.to_numeric(tmp.get("period"), errors="coerce").dropna().max())
    except Exception:
        max_period = 4

    ot_idx = 0
    if max_period and int(max_period) > 4:
        for p in range(5, int(max_period) + 1):
            if ot_idx >= int(_MAX_OT_PERIODS):
                break
            sub = tmp[tmp.get("period") == p]
            if sub is None or sub.empty:
                break

            # Treat whole OT as one segment; grab score at 0:00 of the OT.
            hs, as_ = _score_at_boundary(sub, 0)
            if hs is None or as_ is None:
                break

            ot_idx += 1
            seg_idx += 1
            seg_total = float((float(hs) - float(prev_h)) + (float(as_) - float(prev_a)))
            out[f"seg{seg_idx}_total_act"] = seg_total
            out[f"cum{seg_idx}_total_act"] = float(float(hs) + float(as_))

            prev_h, prev_a = float(hs), float(as_)

    if seg_idx < 16:
        return None
    return out


def build_for_date(date_str: str, rate_delay: float = 0.15) -> pd.DataFrame:
    pbp_path = PROC / f"pbp_espn_{date_str}.csv"
    dfe: pd.DataFrame | None = None

    if pbp_path.exists():
        try:
            dfe = pd.read_csv(pbp_path)
        except Exception:
            dfe = None

    if dfe is None or dfe.empty or ("home_score" not in dfe.columns) or ("away_score" not in dfe.columns):
        from nba_betting.pbp_espn import fetch_pbp_espn_for_date

        dfe, _ = fetch_pbp_espn_for_date(date_str, only_final=True, rate_delay=rate_delay)

    if dfe is None or dfe.empty:
        return pd.DataFrame()

    key_col = (
        "game_id"
        if ("game_id" in dfe.columns and dfe["game_id"].notna().any())
        else ("event_id" if "event_id" in dfe.columns else None)
    )
    if key_col is None:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for gid, gdf in dfe.groupby(key_col):
        gdf = gdf.copy()
        htri = (
            str(gdf.get("home_tri").dropna().iloc[0]).upper().strip()
            if ("home_tri" in gdf.columns and gdf["home_tri"].notna().any())
            else ""
        )
        atri = (
            str(gdf.get("away_tri").dropna().iloc[0]).upper().strip()
            if ("away_tri" in gdf.columns and gdf["away_tri"].notna().any())
            else ""
        )
        if not htri or not atri or htri == "NAN" or atri == "NAN":
            continue

        segs = _segment_actuals_from_game_pbp(gdf)
        if not segs:
            continue

        rec: dict[str, Any] = {
            "date": date_str,
            "home_tri": htri,
            "away_tri": atri,
            "game_id": str(gid).strip(),
        }
        try:
            # Best-effort OT count (actual game)
            mp = int(pd.to_numeric(gdf.get("period"), errors="coerce").dropna().max())
            rec["n_ot"] = int(max(0, mp - 4)) if mp else 0
        except Exception:
            rec["n_ot"] = 0
        rec.update(segs)
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

    # Always write a CSV with headers so downstream evaluators don't crash on empty files.
    expected_cols = ["date", "home_tri", "away_tri", "game_id", "n_ot"]
    expected_cols += [f"seg{i}_total_act" for i in range(1, 16 + _MAX_OT_PERIODS + 1)]
    expected_cols += [f"cum{i}_total_act" for i in range(1, 16 + _MAX_OT_PERIODS + 1)]
    if out_df is None or out_df.empty:
        out_df = pd.DataFrame(columns=expected_cols)
    else:
        # Keep stable column ordering when possible.
        for c in expected_cols:
            if c not in out_df.columns:
                out_df[c] = np.nan
        out_df = out_df[expected_cols]

    out_path = (
        Path(args.out)
        if args.out
        else (PROC / f"smart_sim_intervals_actuals_{start}_{end}_pbp_espn.csv")
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(out_path, index=False)
    print({"ok": True, "rows": int(len(out_df)), "out": str(out_path), "start": start, "end": end})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
