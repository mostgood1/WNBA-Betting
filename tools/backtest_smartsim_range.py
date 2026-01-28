"""Backtest SmartSim outputs across a date range.

This is intended for daily tuning / regression detection, not model selection.

Compares:
- smart_sim_<date>_<HOME>_<AWAY>.json score means/probabilities vs finals_<date>.csv (when present)
- player mean stats vs boxscores_<date>.csv (when present)

Writes:
- data/processed/audits/smartsim_backtest_<start>_<end>.csv (per-game)
- data/processed/audits/smartsim_backtest_players_<start>_<end>.csv (top player outliers)

Usage:
  python tools/backtest_smartsim_range.py --start 2026-01-17 --end 2026-01-23

Exit code:
  0: ran (even if some dates missing actuals)
  1: ran but found structural issues (missing game_id, no sims found, etc.)
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
PROC = REPO_ROOT / "data" / "processed"
AUDITS = PROC / "audits"


@dataclass(frozen=True)
class GameRow:
    date: str
    game_id: int | None
    home: str
    away: str
    n_sims: int | None

    pred_home: float | None
    pred_away: float | None
    pred_total: float | None
    pred_margin: float | None

    p_home_win: float | None
    p_home_cover: float | None
    p_total_over: float | None

    act_home: float | None
    act_away: float | None
    act_total: float | None
    act_margin: float | None

    err_home: float | None
    err_away: float | None
    err_total: float | None
    err_margin: float | None

    brier_home_win: float | None
    brier_home_cover: float | None
    brier_total_over: float | None


def _num(x) -> float | None:
    try:
        v = float(x)
        if np.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _load_finals(date_str: str) -> dict[tuple[str, str], tuple[float, float]]:
    p = PROC / f"finals_{date_str}.csv"
    if not p.exists():
        return {}
    df = pd.read_csv(p)
    if df is None or df.empty:
        return {}
    df["home_tri"] = df["home_tri"].astype(str).str.upper().str.strip()
    df["away_tri"] = df["away_tri"].astype(str).str.upper().str.strip()
    out = {}
    for r in df.itertuples(index=False):
        try:
            out[(str(r.home_tri), str(r.away_tri))] = (float(r.home_pts), float(r.visitor_pts))
        except Exception:
            pass
    return out


def _load_boxscores(date_str: str) -> pd.DataFrame | None:
    p = PROC / f"boxscores_{date_str}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    if df is None or df.empty:
        return None
    df = df.copy()
    df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce")
    df["MIN"] = pd.to_numeric(df["MIN"], errors="coerce").fillna(0.0)
    for c in ["PTS", "REB", "AST"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    df["TEAM_ABBREVIATION"] = df["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()
    return df


def _iter_dates(start: str, end: str):
    for d in pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="D"):
        yield d.strftime("%Y-%m-%d")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--min-minutes", type=float, default=10.0)
    ap.add_argument("--top-player-outliers", type=int, default=50)
    args = ap.parse_args()

    start = args.start.strip()
    end = args.end.strip()
    min_minutes = float(args.min_minutes)
    top_n = int(args.top_player_outliers)

    AUDITS.mkdir(parents=True, exist_ok=True)

    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []

    any_structural_issue = False
    any_sims = False

    for ds in _iter_dates(start, end):
        sim_files = sorted(PROC.glob(f"smart_sim_{ds}_*.json"))
        if not sim_files:
            continue
        any_sims = True

        finals = _load_finals(ds)
        box = _load_boxscores(ds)

        for fp in sim_files:
            try:
                obj = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                any_structural_issue = True
                continue

            home = str(obj.get("home") or "").upper().strip()
            away = str(obj.get("away") or "").upper().strip()
            gid = obj.get("game_id")
            try:
                gid_i = int(float(gid)) if gid is not None and str(gid).lower() != "nan" else None
            except Exception:
                gid_i = None
            if gid_i is None:
                any_structural_issue = True

            score = obj.get("score", {}) or {}
            pred_home = _num(score.get("home_mean"))
            pred_away = _num(score.get("away_mean"))
            pred_total = _num(score.get("total_mean"))
            pred_margin = _num(score.get("margin_mean"))
            p_home_win = _num(score.get("p_home_win"))
            p_home_cover = _num(score.get("p_home_cover"))
            p_total_over = _num(score.get("p_total_over"))

            act_home = act_away = None
            if (home, away) in finals:
                act_home, act_away = finals[(home, away)]
            act_total = (act_home + act_away) if (act_home is not None and act_away is not None) else None
            act_margin = (act_home - act_away) if (act_home is not None and act_away is not None) else None

            err_home = (pred_home - act_home) if (pred_home is not None and act_home is not None) else None
            err_away = (pred_away - act_away) if (pred_away is not None and act_away is not None) else None
            err_total = (pred_total - act_total) if (pred_total is not None and act_total is not None) else None
            err_margin = (pred_margin - act_margin) if (pred_margin is not None and act_margin is not None) else None

            # Brier scores when actuals exist
            brier_home_win = None
            if p_home_win is not None and act_margin is not None:
                y = 1.0 if act_margin > 0 else 0.0
                brier_home_win = (p_home_win - y) ** 2
            brier_home_cover = None
            if p_home_cover is not None and act_margin is not None and score.get("home_cover_line") is not None:
                # We don't have the line in JSON consistently; skip unless present.
                pass
            brier_total_over = None
            if p_total_over is not None and act_total is not None and score.get("total_line") is not None:
                pass

            game_rows.append(
                GameRow(
                    date=ds,
                    game_id=gid_i,
                    home=home,
                    away=away,
                    n_sims=int(obj.get("n_sims") or 0) if obj.get("n_sims") is not None else None,
                    pred_home=pred_home,
                    pred_away=pred_away,
                    pred_total=pred_total,
                    pred_margin=pred_margin,
                    p_home_win=p_home_win,
                    p_home_cover=p_home_cover,
                    p_total_over=p_total_over,
                    act_home=act_home,
                    act_away=act_away,
                    act_total=act_total,
                    act_margin=act_margin,
                    err_home=err_home,
                    err_away=err_away,
                    err_total=err_total,
                    err_margin=err_margin,
                    brier_home_win=brier_home_win,
                    brier_home_cover=brier_home_cover,
                    brier_total_over=brier_total_over,
                ).__dict__
            )

            # Player-level backtest when boxscores exist
            if box is not None and gid_i is not None:
                b = box[box["game_id"] == gid_i].copy()
                if not b.empty:
                    b = b[b["MIN"] >= min_minutes].copy()
                    if not b.empty:
                        sim_players = obj.get("players", {}) if isinstance(obj.get("players", {}), dict) else {}
                        for side in ["home", "away"]:
                            for pr in sim_players.get(side, []) or []:
                                try:
                                    pid = int(pr.get("player_id"))
                                except Exception:
                                    continue
                                act = b[b["PLAYER_ID"] == pid]
                                if act.empty:
                                    continue
                                act_row = act.iloc[0]
                                pred_pts = _num(pr.get("pts_mean"))
                                pred_reb = _num(pr.get("reb_mean"))
                                pred_ast = _num(pr.get("ast_mean"))
                                pred_pra = _num(pr.get("pra_mean"))
                                act_pts = float(act_row["PTS"])
                                act_reb = float(act_row["REB"])
                                act_ast = float(act_row["AST"])
                                act_pra = float(act_row["PRA"])
                                if pred_pts is None or pred_reb is None or pred_ast is None:
                                    continue
                                player_rows.append(
                                    {
                                        "date": ds,
                                        "game_id": gid_i,
                                        "home": home,
                                        "away": away,
                                        "player_id": pid,
                                        "player_name": pr.get("player_name"),
                                        "min": float(act_row["MIN"]),
                                        "pred_pts": pred_pts,
                                        "act_pts": act_pts,
                                        "err_pts": pred_pts - act_pts,
                                        "pred_reb": pred_reb,
                                        "act_reb": act_reb,
                                        "err_reb": pred_reb - act_reb,
                                        "pred_ast": pred_ast,
                                        "act_ast": act_ast,
                                        "err_ast": pred_ast - act_ast,
                                        "pred_pra": pred_pra,
                                        "act_pra": act_pra,
                                        "err_pra": (pred_pra - act_pra) if pred_pra is not None else None,
                                    }
                                )

    if not any_sims:
        print(f"No smart_sim files found in range {start}..{end}")
        return 1

    games_df = pd.DataFrame(game_rows)
    out_games = AUDITS / f"smartsim_backtest_{start}_{end}.csv"
    games_df.to_csv(out_games, index=False)

    if player_rows:
        players_df = pd.DataFrame(player_rows)
        # Keep top outliers by absolute points error
        players_df["abs_err_pts"] = players_df["err_pts"].abs()
        outliers = players_df.sort_values("abs_err_pts", ascending=False).head(top_n)
        out_players = AUDITS / f"smartsim_backtest_players_{start}_{end}.csv"
        outliers.to_csv(out_players, index=False)
    else:
        out_players = None

    # Print concise summary
    has_actual = games_df["act_total"].notna()
    n_actual = int(has_actual.sum())
    if n_actual:
        mae_total = float((games_df.loc[has_actual, "err_total"].abs()).mean())
        mae_margin = float((games_df.loc[has_actual, "err_margin"].abs()).mean())
        mae_home = float((games_df.loc[has_actual, "err_home"].abs()).mean())
        mae_away = float((games_df.loc[has_actual, "err_away"].abs()).mean())
        brier = float((games_df.loc[games_df["brier_home_win"].notna(), "brier_home_win"]).mean()) if games_df["brier_home_win"].notna().any() else float("nan")
        print(f"Backtest {start}..{end}: games_with_finals={n_actual}/{len(games_df)} MAE(total)={mae_total:.2f} MAE(margin)={mae_margin:.2f} MAE(home)={mae_home:.2f} MAE(away)={mae_away:.2f} Brier(home_win)={brier:.3f}")
    else:
        print(f"Backtest {start}..{end}: no finals available yet; wrote {out_games}")

    if out_players is not None:
        print(f"Top player outliers written to {out_players}")

    return 1 if any_structural_issue else 0


if __name__ == "__main__":
    raise SystemExit(main())
