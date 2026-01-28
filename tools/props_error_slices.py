"""Analyze props prediction errors and produce actionable slices.

Usage:
  python tools/props_error_slices.py --start 2026-01-17 --end 2026-01-22

Outputs (data/processed):
  - props_error_slices_<start>_<end>.csv
  - props_error_worst_rows_<start>_<end>.csv

This script uses already-saved daily predictions (props_predictions_YYYY-MM-DD.csv)
plus actuals (props_actuals_YYYY-MM-DD.csv if present, else player_logs.* fallback).
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROC = ROOT / "data" / "processed"


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def _load_player_logs() -> pd.DataFrame | None:
    p = PROC / "player_logs.parquet"
    c = PROC / "player_logs.csv"
    try:
        if p.exists():
            try:
                return pd.read_parquet(p)
            except Exception:
                return None
        if c.exists():
            return pd.read_csv(c)
    except Exception:
        return None
    return None


def _load_actuals_for_date(d: dt.date, logs: pd.DataFrame | None) -> pd.DataFrame | None:
    daily = PROC / f"props_actuals_{d:%Y-%m-%d}.csv"
    if daily.exists():
        try:
            df = pd.read_csv(daily)
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            df = df[df["date"] == d]
            if "player_id" in df.columns:
                df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
                df = df.sort_values(["date", "player_id"]).drop_duplicates(subset=["date", "player_id"], keep="last")
            return df
        except Exception:
            pass

    if logs is None or logs.empty:
        return None

    dcol = _pick_col(logs, ["GAME_DATE", "GAME_DATE_EST", "dateGame", "GAME_DATE_PT", "date"])
    pid = _pick_col(logs, ["PLAYER_ID", "player_id", "idPlayer"])
    pts = _pick_col(logs, ["PTS", "pts"])
    reb = _pick_col(logs, ["REB", "reb", "TREB", "treb"])
    ast = _pick_col(logs, ["AST", "ast"])
    fg3m = _pick_col(logs, ["FG3M", "fg3m"])
    minutes = _pick_col(logs, ["MIN", "min", "minutes"])
    team = _pick_col(logs, ["TEAM_ABBREVIATION", "team", "TEAM", "team_abbr"])

    if not all([dcol, pid, pts, reb, ast, fg3m]):
        return None

    df = logs.copy()
    df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date
    df = df[df[dcol] == d]
    if df.empty:
        return None

    keep = [dcol, pid, pts, reb, ast, fg3m]
    if minutes:
        keep.append(minutes)
    if team:
        keep.append(team)

    df = df[keep].copy()
    df = df.rename(
        columns={
            dcol: "date",
            pid: "player_id",
            pts: "pts",
            reb: "reb",
            ast: "ast",
            fg3m: "threes",
            **({minutes: "min"} if minutes else {}),
            **({team: "team"} if team else {}),
        }
    )

    for c in ["pts", "reb", "ast", "threes", "min"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["pra"] = df[["pts", "reb", "ast"]].sum(axis=1, skipna=False)
    df = df.sort_values(["date", "player_id"]).drop_duplicates(subset=["date", "player_id"], keep="last")
    return df


def _load_preds_for_date(d: dt.date) -> pd.DataFrame | None:
    p = PROC / f"props_predictions_{d:%Y-%m-%d}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        return None

    # Normalize date column
    date_col = "asof_date" if "asof_date" in df.columns else ("date" if "date" in df.columns else None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df = df[df[date_col] == d]
        df = df.rename(columns={date_col: "date"})
    else:
        df["date"] = d

    # Ensure ids are numeric
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")

    # De-dupe (some pipelines can emit duplicates after merges)
    if {"date", "player_id"}.issubset(df.columns):
        df = df.sort_values(["date", "player_id"]).drop_duplicates(subset=["date", "player_id"], keep="last")

    return df


def _rmse(err: pd.Series) -> float:
    x = pd.to_numeric(err, errors="coerce")
    x = x.dropna()
    if x.empty:
        return float("nan")
    return float(np.sqrt(np.mean(x * x)))


def _slice_summary(joined: pd.DataFrame, slice_col: str, slice_name: str, *, min_n: int = 25) -> pd.DataFrame:
    out = []
    stats = [
        ("pts", "pred_pts"),
        ("reb", "pred_reb"),
        ("ast", "pred_ast"),
        ("threes", "pred_threes"),
        ("pra", "pred_pra"),
    ]

    if slice_col not in joined.columns:
        return pd.DataFrame()

    for stat, pred_col in stats:
        if stat not in joined.columns or pred_col not in joined.columns:
            continue
        err = joined[pred_col] - joined[stat]
        tmp = joined[[slice_col]].copy()
        tmp["err"] = err
        tmp["abs_err"] = err.abs()
        for key, g in tmp.groupby(slice_col):
            n = int(g["err"].notna().sum())
            if n < min_n:
                continue
            out.append(
                {
                    "slice_type": slice_name,
                    "slice": str(key),
                    "stat": stat,
                    "n": n,
                    "mae": float(g["abs_err"].mean()),
                    "rmse": _rmse(g["err"]),
                    "bias": float(g["err"].mean()),
                    "p50_abs_err": float(g["abs_err"].quantile(0.50)),
                    "p90_abs_err": float(g["abs_err"].quantile(0.90)),
                }
            )

    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.sort_values(["stat", "mae"], ascending=[True, False])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    start = dt.datetime.strptime(args.start, "%Y-%m-%d").date()
    end = dt.datetime.strptime(args.end, "%Y-%m-%d").date()

    logs = _load_player_logs()

    joined_parts = []
    missing_preds = []
    missing_actuals = []
    for d in pd.date_range(start, end, freq="D").date:
        preds = _load_preds_for_date(d)
        if preds is None or preds.empty:
            missing_preds.append(str(d))
            continue
        act = _load_actuals_for_date(d, logs)
        if act is None or act.empty:
            missing_actuals.append(str(d))
            continue

        # Join keys
        if "player_id" not in preds.columns or "player_id" not in act.columns:
            continue

        # Harmonize types
        act["player_id"] = pd.to_numeric(act["player_id"], errors="coerce")
        preds["player_id"] = pd.to_numeric(preds["player_id"], errors="coerce")

        merged = preds.merge(act, on=["date", "player_id"], how="inner", suffixes=("", "_act"))
        if merged.empty:
            continue
        joined_parts.append(merged)

    if not joined_parts:
        print("No joined rows; missing preds:", missing_preds)
        print("No joined rows; missing actuals:", missing_actuals)
        return 2

    df = pd.concat(joined_parts, ignore_index=True)

    # Minutes bins if available
    if "min" in df.columns:
        bins = [-1, 0, 10, 20, 30, 40, 60]
        labels = ["DNP/0", "1-10", "11-20", "21-30", "31-40", "41+"]
        df["min_bin"] = pd.cut(df["min"].fillna(-1), bins=bins, labels=labels)

    # Identify a usable team column (preds or actuals)
    if "team" not in df.columns:
        team_pred = _pick_col(df, ["TEAM", "team", "team_abbr", "TEAM_ABBREVIATION", "team_tri"])
        if team_pred and team_pred != "team":
            df = df.rename(columns={team_pred: "team"})

    # Worst rows table
    worst_rows = []
    for stat, pred_col in [("pts", "pred_pts"), ("reb", "pred_reb"), ("ast", "pred_ast"), ("threes", "pred_threes"), ("pra", "pred_pra")]:
        if stat not in df.columns or pred_col not in df.columns:
            continue
        tmp = df.copy()
        tmp["stat"] = stat
        tmp["pred"] = pd.to_numeric(tmp[pred_col], errors="coerce")
        tmp["actual"] = pd.to_numeric(tmp[stat], errors="coerce")
        tmp["err"] = tmp["pred"] - tmp["actual"]
        tmp["abs_err"] = tmp["err"].abs()
        tmp = tmp[tmp["abs_err"].notna()].sort_values("abs_err", ascending=False).head(200)

        cols = [c for c in ["date", "player_id", "player_name", "team", "min", "min_bin"] if c in tmp.columns]
        cols += ["stat", "pred", "actual", "err", "abs_err"]
        worst_rows.append(tmp[cols])

    worst_rows_df = pd.concat(worst_rows, ignore_index=True) if worst_rows else pd.DataFrame()

    # Slice summary tables
    slices = []
    if "team" in df.columns:
        slices.append(_slice_summary(df, "team", "team", min_n=25))
    if "min_bin" in df.columns:
        slices.append(_slice_summary(df, "min_bin", "min_bin", min_n=25))

    # Player slices (requires player_name or player_id)
    if "player_name" in df.columns:
        slices.append(_slice_summary(df, "player_name", "player_name", min_n=4))
    else:
        slices.append(_slice_summary(df, "player_id", "player_id", min_n=4))

    slices_df = pd.concat([s for s in slices if s is not None and not s.empty], ignore_index=True) if slices else pd.DataFrame()

    out_slices = PROC / f"props_error_slices_{start:%Y-%m-%d}_{end:%Y-%m-%d}.csv"
    out_worst = PROC / f"props_error_worst_rows_{start:%Y-%m-%d}_{end:%Y-%m-%d}.csv"

    slices_df.to_csv(out_slices, index=False)
    worst_rows_df.to_csv(out_worst, index=False)

    print(f"Joined rows: {len(df):,}  (dates={start}..{end})")
    if missing_preds:
        print("Missing preds for:", ", ".join(missing_preds))
    if missing_actuals:
        print("Missing actuals for:", ", ".join(missing_actuals))

    # Print quick highlights
    if not slices_df.empty:
        for stat in ["pra", "pts"]:
            top_team = slices_df[(slices_df["slice_type"] == "team") & (slices_df["stat"] == stat)].sort_values("mae", ascending=False).head(5)
            if not top_team.empty:
                print(f"\nTop team MAE for {stat}:")
                print(top_team[["slice", "n", "mae", "rmse", "bias"]].to_string(index=False))

    print(f"Wrote {out_slices}")
    print(f"Wrote {out_worst}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
