"""Train team quarter-share splits from smart_sim_quarter_eval and (optionally) write quarters_calibration.json.

Goal
- Improve quarter *team points* accuracy by learning better per-team quarter share splits.

Method
- Use actual quarter team points (q1..q4) from smart_sim_quarter_eval_*.csv.
- Learn league-level mean split and per-team mean split.
- Apply Bayesian shrinkage (team -> league) based on sample size.
- Evaluate on a holdout window by redistributing *existing* predicted game team totals
  (sum of q*_home_pred / q*_away_pred) across quarters using learned splits.
  This isolates split quality without changing game total accuracy.

Writes
- data/processed/quarters_calibration.json with keys:
  - league_split
  - team_split_by_tri
  - meta

Safety
- By default, does NOT write. Use --write-if-better or --force-write.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _pick_latest_eval() -> Path | None:
    cands = list(PROCESSED.glob("smart_sim_quarter_eval_*.csv"))
    if not cands:
        return None
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _clamp_split(arr: Iterable[float]) -> list[float] | None:
    try:
        a = np.asarray(list(arr), dtype=float)
        a = np.where(np.isfinite(a) & (a > 0), a, 0.0)
        if a.size != 4:
            return None
        s = float(a.sum())
        if s <= 0:
            return None
        a = a / s
        return [float(x) for x in a.tolist()]
    except Exception:
        return None


@dataclass(frozen=True)
class ErrSummary:
    n: int
    mae: float
    rmse: float


def _summarize_errors(err: np.ndarray) -> ErrSummary:
    e = np.asarray(err, dtype=float)
    e = e[np.isfinite(e)]
    if e.size == 0:
        return ErrSummary(n=0, mae=float("nan"), rmse=float("nan"))
    return ErrSummary(n=int(e.size), mae=float(np.mean(np.abs(e))), rmse=float(np.sqrt(np.mean(e * e))))


def _load_eval(path: Path, start: _date | None = None, end: _date | None = None) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df[dt.notna()].copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date

    # Filter to actuals present for at least one quarter
    keep = pd.Series(False, index=df.index)
    for q in (1, 2, 3, 4):
        keep |= pd.to_numeric(df.get(f"q{q}_total_act"), errors="coerce").notna()
    df = df[keep].copy()

    # Prefer high-quality rows
    if "use_pbp" in df.columns:
        m = df["use_pbp"].astype(str).str.lower().isin(["true", "1", "yes"])
        if m.any():
            df = df[m].copy()

    if start is not None and end is not None and "date" in df.columns:
        d = df["date"]
        df = df[(d >= start) & (d <= end)].copy()

    # normalize team tris
    for c in ("home_tri", "away_tri"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper().str.strip()

    return df


def _build_team_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return long-form rows for (team, quarter, share_act, q_pred, q_act, game_pred, game_act)."""

    out: list[dict[str, Any]] = []

    for _, r in df.iterrows():
        date = r.get("date")
        home = str(r.get("home_tri") or "").strip().upper()
        away = str(r.get("away_tri") or "").strip().upper()

        # actual totals
        home_act_q = []
        away_act_q = []
        home_pred_q = []
        away_pred_q = []
        for q in (1, 2, 3, 4):
            ha = pd.to_numeric(pd.Series([r.get(f"q{q}_home_act")]), errors="coerce").iloc[0]
            aa = pd.to_numeric(pd.Series([r.get(f"q{q}_away_act")]), errors="coerce").iloc[0]
            hp = pd.to_numeric(pd.Series([r.get(f"q{q}_home_pred")]), errors="coerce").iloc[0]
            ap = pd.to_numeric(pd.Series([r.get(f"q{q}_away_pred")]), errors="coerce").iloc[0]
            home_act_q.append(float(ha) if pd.notna(ha) else float("nan"))
            away_act_q.append(float(aa) if pd.notna(aa) else float("nan"))
            home_pred_q.append(float(hp) if pd.notna(hp) else float("nan"))
            away_pred_q.append(float(ap) if pd.notna(ap) else float("nan"))

        if not np.isfinite(home_act_q).all() or not np.isfinite(away_act_q).all():
            continue
        if not np.isfinite(home_pred_q).all() or not np.isfinite(away_pred_q).all():
            continue

        home_game_act = float(np.sum(home_act_q))
        away_game_act = float(np.sum(away_act_q))
        home_game_pred = float(np.sum(home_pred_q))
        away_game_pred = float(np.sum(away_pred_q))

        if home_game_act <= 0 or away_game_act <= 0 or home_game_pred <= 0 or away_game_pred <= 0:
            continue

        for i, q in enumerate((1, 2, 3, 4)):
            out.append(
                {
                    "date": date,
                    "team": home,
                    "opp": away,
                    "is_home": 1,
                    "q": q,
                    "share_act": float(home_act_q[i] / home_game_act),
                    "q_act": float(home_act_q[i]),
                    "q_pred": float(home_pred_q[i]),
                    "game_act": home_game_act,
                    "game_pred": home_game_pred,
                }
            )
            out.append(
                {
                    "date": date,
                    "team": away,
                    "opp": home,
                    "is_home": 0,
                    "q": q,
                    "share_act": float(away_act_q[i] / away_game_act),
                    "q_act": float(away_act_q[i]),
                    "q_pred": float(away_pred_q[i]),
                    "game_act": away_game_act,
                    "game_pred": away_game_pred,
                }
            )

    return pd.DataFrame(out)


def _fit_splits(
    long_df: pd.DataFrame,
    min_games: int = 8,
    shrink_k: float = 10.0,
) -> tuple[list[float], dict[str, list[float]], dict[str, Any]]:
    """Fit league split and per-team splits with shrinkage."""

    # league split: mean of share_act by quarter
    league = (
        long_df.groupby("q")["share_act"].mean().reindex([1, 2, 3, 4]).to_numpy(dtype=float)
    )
    league_split = _clamp_split(league.tolist()) or [0.245, 0.245, 0.255, 0.255]

    # per team: average of share_act by quarter
    team_split_by_tri: dict[str, list[float]] = {}
    team_games = long_df[["date", "team", "opp"]].drop_duplicates().groupby("team").size().to_dict()

    for team, n_games in team_games.items():
        if int(n_games) < int(min_games):
            continue
        sub = long_df[long_df["team"] == team]
        mu = sub.groupby("q")["share_act"].mean().reindex([1, 2, 3, 4]).to_numpy(dtype=float)
        base = _clamp_split(mu.tolist())
        if base is None:
            continue
        # shrink to league
        w = float(n_games) / float(n_games + shrink_k)
        split = [
            float(w * base[i] + (1.0 - w) * league_split[i])
            for i in range(4)
        ]
        split = _clamp_split(split) or base
        team_split_by_tri[str(team)] = split

    meta = {
        "min_games": int(min_games),
        "shrink_k": float(shrink_k),
        "teams": int(len(team_split_by_tri)),
        "league_split": league_split,
    }

    return league_split, team_split_by_tri, meta


def _fit_splits_home_away(
    long_df: pd.DataFrame,
    min_games: int = 8,
    shrink_k: float = 10.0,
) -> tuple[dict[str, list[float]], dict[str, dict[str, list[float]]], dict[str, Any]]:
    """Fit separate splits for home vs away (pregame-known feature)."""

    def _fit_for_side(side_val: int) -> tuple[list[float], dict[str, list[float]]]:
        df = long_df[long_df["is_home"] == int(side_val)].copy()
        league = df.groupby("q")["share_act"].mean().reindex([1, 2, 3, 4]).to_numpy(dtype=float)
        league_split = _clamp_split(league.tolist()) or [0.245, 0.245, 0.255, 0.255]

        team_map: dict[str, list[float]] = {}
        team_games = df[["date", "team", "opp"]].drop_duplicates().groupby("team").size().to_dict()
        for team, n_games in team_games.items():
            if int(n_games) < int(min_games):
                continue
            sub = df[df["team"] == team]
            mu = sub.groupby("q")["share_act"].mean().reindex([1, 2, 3, 4]).to_numpy(dtype=float)
            base = _clamp_split(mu.tolist())
            if base is None:
                continue
            w = float(n_games) / float(n_games + shrink_k)
            split = [float(w * base[i] + (1.0 - w) * league_split[i]) for i in range(4)]
            split = _clamp_split(split) or base
            team_map[str(team)] = split

        return league_split, team_map

    league_home, team_home = _fit_for_side(1)
    league_away, team_away = _fit_for_side(0)

    meta = {
        "min_games": int(min_games),
        "shrink_k": float(shrink_k),
        "teams_home": int(len(team_home)),
        "teams_away": int(len(team_away)),
        "league_split_home": league_home,
        "league_split_away": league_away,
    }

    return {"home": league_home, "away": league_away}, {"home": team_home, "away": team_away}, meta


def _predict_split(team: str, league_split: list[float], team_split_by_tri: dict[str, list[float]]) -> list[float]:
    t = str(team or "").strip().upper()
    s = team_split_by_tri.get(t)
    return s if s is not None else league_split


def _predict_split_home_away(
    team: str,
    is_home: int,
    league: dict[str, list[float]],
    team_map: dict[str, dict[str, list[float]]],
) -> list[float]:
    side = "home" if int(is_home) == 1 else "away"
    t = str(team or "").strip().upper()
    s = (team_map.get(side) or {}).get(t)
    return s if s is not None else (league.get(side) or [0.245, 0.245, 0.255, 0.255])


def _evaluate_redistribution(
    long_df: pd.DataFrame,
    league_split: list[float],
    team_split_by_tri: dict[str, list[float]],
) -> dict[str, Any]:
    """Evaluate baseline quarter team points vs redistributed quarter team points."""

    # Baseline: q_pred is the existing smart_sim predicted quarter points.
    # New: q_pred_new = game_pred * split[q]. This keeps game_pred fixed.

    rows = long_df.copy()
    # map splits
    def _split_for_row(r):
        s = _predict_split(str(r["team"]), league_split, team_split_by_tri)
        return float(s[int(r["q"]) - 1])

    rows["split"] = rows.apply(_split_for_row, axis=1)
    rows["q_pred_new"] = rows["game_pred"].astype(float) * rows["split"].astype(float)

    out: dict[str, Any] = {}

    # overall
    base_err = (rows["q_act"].astype(float) - rows["q_pred"].astype(float)).to_numpy(dtype=float)
    new_err = (rows["q_act"].astype(float) - rows["q_pred_new"].astype(float)).to_numpy(dtype=float)
    out["overall"] = {
        "baseline": _summarize_errors(base_err).__dict__,
        "redistributed": _summarize_errors(new_err).__dict__,
    }

    # by quarter
    by_q: dict[str, Any] = {}
    for q in (1, 2, 3, 4):
        sub = rows[rows["q"] == q]
        be = (sub["q_act"].astype(float) - sub["q_pred"].astype(float)).to_numpy(dtype=float)
        ne = (sub["q_act"].astype(float) - sub["q_pred_new"].astype(float)).to_numpy(dtype=float)
        by_q[f"q{q}"] = {
            "baseline": _summarize_errors(be).__dict__,
            "redistributed": _summarize_errors(ne).__dict__,
        }
    out["by_quarter"] = by_q

    return out


def _evaluate_redistribution_home_away(
    long_df: pd.DataFrame,
    league: dict[str, list[float]],
    team_map: dict[str, dict[str, list[float]]],
) -> dict[str, Any]:
    rows = long_df.copy()

    def _split_for_row(r):
        s = _predict_split_home_away(str(r["team"]), int(r["is_home"]), league, team_map)
        return float(s[int(r["q"]) - 1])

    rows["split"] = rows.apply(_split_for_row, axis=1)
    rows["q_pred_new"] = rows["game_pred"].astype(float) * rows["split"].astype(float)

    out: dict[str, Any] = {}
    base_err = (rows["q_act"].astype(float) - rows["q_pred"].astype(float)).to_numpy(dtype=float)
    new_err = (rows["q_act"].astype(float) - rows["q_pred_new"].astype(float)).to_numpy(dtype=float)
    out["overall"] = {
        "baseline": _summarize_errors(base_err).__dict__,
        "redistributed": _summarize_errors(new_err).__dict__,
    }
    by_q: dict[str, Any] = {}
    for q in (1, 2, 3, 4):
        sub = rows[rows["q"] == q]
        be = (sub["q_act"].astype(float) - sub["q_pred"].astype(float)).to_numpy(dtype=float)
        ne = (sub["q_act"].astype(float) - sub["q_pred_new"].astype(float)).to_numpy(dtype=float)
        by_q[f"q{q}"] = {
            "baseline": _summarize_errors(be).__dict__,
            "redistributed": _summarize_errors(ne).__dict__,
        }
    out["by_quarter"] = by_q
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Train team quarter-share splits from smart_sim_quarter_eval")
    ap.add_argument("--eval-path", type=str, default=None, help="Path to smart_sim_quarter_eval_*.csv (defaults to latest)")
    ap.add_argument("--days", type=int, default=60, help="Lookback days ending at max date in eval file")
    ap.add_argument("--holdout-days", type=int, default=6, help="Holdout window size in days")
    ap.add_argument("--min-games", type=int, default=8, help="Minimum games for team-specific split")
    ap.add_argument("--shrink-k", type=float, default=10.0, help="Shrinkage strength toward league")
    ap.add_argument("--home-away", action="store_true", help="Fit separate home/away splits (recommended)")
    ap.add_argument("--write-if-better", action="store_true", help="Write quarters_calibration.json only if holdout improves overall MAE and RMSE")
    ap.add_argument("--force-write", action="store_true", help="Write quarters_calibration.json even if not better")
    ap.add_argument("--out", type=str, default=None, help="Output path (defaults to data/processed/quarters_calibration.json)")
    args = ap.parse_args()

    if args.eval_path:
        p = Path(args.eval_path)
        if not p.is_absolute():
            p = (ROOT / p).resolve()
        eval_path = p
    else:
        eval_path = _pick_latest_eval()

    if eval_path is None or not eval_path.exists():
        raise SystemExit("No smart_sim_quarter_eval_*.csv found")

    df0 = pd.read_csv(eval_path)
    if "date" not in df0.columns:
        raise SystemExit("Eval file missing date")
    max_d = pd.to_datetime(df0["date"], errors="coerce").dt.date.max()
    if max_d is None or pd.isna(max_d):
        raise SystemExit("Could not parse max date")
    end = max_d
    start = end - timedelta(days=int(args.days) - 1)

    df = _load_eval(eval_path, start=start, end=end)
    if df.empty:
        raise SystemExit("No usable rows after filtering")

    long_df = _build_team_rows(df)
    if long_df.empty:
        raise SystemExit("No usable team-quarter rows")

    # Split train/test by date
    max_date = pd.to_datetime(long_df["date"], errors="coerce").dt.date.max()
    if max_date is None:
        raise SystemExit("Missing max date in long_df")
    cut = max_date - timedelta(days=int(args.holdout_days))

    train = long_df[pd.to_datetime(long_df["date"]).dt.date <= cut].copy()
    test = long_df[pd.to_datetime(long_df["date"]).dt.date > cut].copy()

    if train.empty or test.empty:
        raise SystemExit(f"Train/test split empty (train={len(train)} test={len(test)} cut={cut})")

    if bool(args.home_away):
        league_ha, team_ha, meta = _fit_splits_home_away(train, min_games=int(args.min_games), shrink_k=float(args.shrink_k))
        eval_train = _evaluate_redistribution_home_away(train, league_ha, team_ha)
        eval_test = _evaluate_redistribution_home_away(test, league_ha, team_ha)
        league_split = None
        team_split_by_tri = None
    else:
        league_split, team_split_by_tri, meta = _fit_splits(train, min_games=int(args.min_games), shrink_k=float(args.shrink_k))
        eval_train = _evaluate_redistribution(train, league_split, team_split_by_tri)
        eval_test = _evaluate_redistribution(test, league_split, team_split_by_tri)

    out = {
        "eval_path": str(eval_path),
        "window": {"start": str(start), "end": str(end)},
        "holdout": {"cut": str(cut), "holdout_days": int(args.holdout_days)},
        "fit": meta,
        "metrics": {"train": eval_train, "test": eval_test},
        "mode": "home_away" if bool(args.home_away) else "single",
    }

    print(json.dumps(out, indent=2))

    # Decide whether to write
    def _get(m: dict, which: str, key: str) -> float:
        try:
            return float(m["metrics"][which]["overall"][key]["mae"]), float(m["metrics"][which]["overall"][key]["rmse"])
        except Exception:
            return float("nan"), float("nan")

    base_mae, base_rmse = _get(out, "test", "baseline")
    new_mae, new_rmse = _get(out, "test", "redistributed")

    improved = (new_mae < base_mae) and (new_rmse < base_rmse)

    should_write = False
    if bool(args.force_write):
        should_write = True
    elif bool(args.write_if_better) and improved:
        should_write = True

    if should_write:
        out_path = Path(args.out) if args.out else (PROCESSED / "quarters_calibration.json")
        if not out_path.is_absolute():
            out_path = (ROOT / out_path).resolve()
        obj: dict[str, Any] = {
            "meta": {
                **meta,
                "trained_from": str(eval_path),
                "trained_at": datetime.now().isoformat(timespec="seconds"),
                "window": {"start": str(start), "end": str(end)},
                "holdout": {"cut": str(cut), "holdout_days": int(args.holdout_days)},
                "test_baseline": {"mae": base_mae, "rmse": base_rmse},
                "test_redistributed": {"mae": new_mae, "rmse": new_rmse},
                "write_condition": "force" if bool(args.force_write) else "write-if-better",
                "mode": "home_away" if bool(args.home_away) else "single",
            },
        }
        if bool(args.home_away):
            obj["league_split_home"] = (league_ha or {}).get("home")  # type: ignore[name-defined]
            obj["league_split_away"] = (league_ha or {}).get("away")  # type: ignore[name-defined]
            obj["team_split_home_by_tri"] = (team_ha or {}).get("home")  # type: ignore[name-defined]
            obj["team_split_away_by_tri"] = (team_ha or {}).get("away")  # type: ignore[name-defined]
        else:
            obj["league_split"] = league_split
            obj["team_split_by_tri"] = team_split_by_tri
        out_path.write_text(json.dumps(obj, indent=2), encoding="utf-8")
        print(f"Wrote splits: {out_path}")
        # Clear any in-process cache by touching a simple marker file (optional)
    else:
        print(
            f"Did not write quarters_calibration.json (improved={improved}; "
            f"baseline_mae={base_mae:.4f} baseline_rmse={base_rmse:.4f} "
            f"new_mae={new_mae:.4f} new_rmse={new_rmse:.4f})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
