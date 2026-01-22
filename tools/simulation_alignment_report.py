import argparse
import datetime as dt
import json
import os
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import numpy as np
import pandas as pd


def find_probability_columns(df: pd.DataFrame) -> List[str]:
    candidates = [
        "prob", "probability", "p", "p_over", "p_under",
        "prob_over", "prob_under", "win_prob", "over_prob", "under_prob",
        "model_prob", "implied_prob"
    ]
    cols = [c for c in candidates if c in df.columns]
    # Filter to [0,1]
    cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c]) and df[c].between(0, 1, inclusive="both").any()]
    return cols or [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) and df[c].between(0, 1, inclusive="both").all()]


def find_outcome_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "hit", "won", "is_hit", "success", "result", "outcome", "actual"
    ]
    for c in candidates:
        if c in df.columns:
            # Normalize to boolean/0-1
            series = df[c]
            if series.dtype == bool:
                return c
            if pd.api.types.is_numeric_dtype(series) and set(series.dropna().unique()).issubset({0, 1}):
                return c
            if series.dtype == object:
                lowered = series.dropna().astype(str).str.lower()
                if set(lowered.unique()).issubset({"0", "1", "true", "false", "hit", "miss", "won", "lost"}):
                    return c
    # Fallback: look for a column named like "over_hit"/"under_hit"
    for c in df.columns:
        if c.endswith("_hit") and pd.api.types.is_numeric_dtype(df[c]):
            return c
    return None


def normalize_outcome(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series.astype(int)
    if pd.api.types.is_numeric_dtype(series):
        # Ensure 0/1
        return series.clip(0, 1).astype(int)
    lowered = series.astype(str).str.lower()
    mapping = {"true": 1, "false": 0, "hit": 1, "miss": 0, "won": 1, "lost": 0, "1": 1, "0": 0}
    return lowered.map(mapping).fillna(0).astype(int)


def brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    return float(np.mean((y_prob - y_true) ** 2))


def log_loss_safe(y_true: np.ndarray, y_prob: np.ndarray, eps: float = 1e-7) -> float:
    p = np.clip(y_prob, eps, 1 - eps)
    return float(np.mean(-(y_true * np.log(p) + (1 - y_true) * np.log(1 - p))))


def calibration_curve(y_true: np.ndarray, y_prob: np.ndarray, bins: int = 10) -> pd.DataFrame:
    df = pd.DataFrame({"y": y_true, "p": y_prob})
    df["bin"] = pd.cut(df["p"], bins=bins, labels=False, include_lowest=True)
    grouped = df.groupby("bin").agg(count=("y", "size"), mean_p=("p", "mean"), frac_hit=("y", "mean")).reset_index()
    grouped["bin"] = grouped["bin"].astype(int)
    return grouped


def collect_files_by_window(base_dir: Path, pattern: str, end_date: dt.date, days: int) -> List[Path]:
    files = []
    for d in range(days):
        date_str = (end_date - dt.timedelta(days=d)).strftime("%Y-%m-%d")
        candidate = base_dir / f"{pattern}_{date_str}.csv"
        if candidate.exists():
            files.append(candidate)
    return files


def load_concat(files: List[Path]) -> pd.DataFrame:
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f))
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, axis=0, ignore_index=True)


def evaluate_window(end_date: dt.date, days: int, base_dir: Path) -> Dict:
    # Prefer recon_props for outcomes; props_predictions for probabilities
    recon_files = collect_files_by_window(base_dir, "recon_props", end_date, days)
    pred_files = collect_files_by_window(base_dir, "props_predictions", end_date, days)

    # Props calibration is best evaluated at the prop-line level.
    # `props_edges_*` contains probabilities for each prop line, and `props_actuals_*` contains realized boxscore stats.
    edge_files = collect_files_by_window(base_dir, "props_edges", end_date, days)
    actual_files = collect_files_by_window(base_dir, "props_actuals", end_date, days)

    recon = load_concat(recon_files)
    preds = load_concat(pred_files)
    edges = load_concat(edge_files)
    actuals = load_concat(actual_files)

    result: Dict = {
        "days": days,
        "rows": int(max(len(recon), len(preds), len(edges), len(actuals))),
        "metrics": {},
        "notes": [],
    }

    if recon.empty and preds.empty and edges.empty and actuals.empty:
        result["notes"].append("No data available for window")
        return result

    # If we have prop edges, compute outcomes by joining to actuals/recon.
    # This enables true probability calibration metrics (Brier/logloss) for props.
    df_edges: Optional[pd.DataFrame] = None
    if not edges.empty:
        # Fall back to recon_props if props_actuals is missing for a day.
        actual_source = actuals if not actuals.empty else recon
        if actual_source.empty:
            result["notes"].append("Found props_edges but no props_actuals/recon_props to compute outcomes")
        else:
            edges_norm = edges.copy()
            actual_norm = actual_source.copy()

            # Normalize join dtypes
            for frame in (edges_norm, actual_norm):
                if "player_id" in frame.columns:
                    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
                if "date" in frame.columns:
                    frame["date"] = frame["date"].astype(str)

            stat_cols = [c for c in ["pts", "reb", "ast", "threes", "pra", "stl", "blk", "tov"] if c in actual_norm.columns]
            if not stat_cols:
                result["notes"].append("No stat columns found in props_actuals/recon_props")
            else:
                act_long = actual_norm.melt(
                    id_vars=[c for c in ["date", "player_id"] if c in actual_norm.columns],
                    value_vars=stat_cols,
                    var_name="stat",
                    value_name="actual_value",
                )
                act_long = act_long.dropna(subset=["date", "player_id", "stat", "actual_value"])

                needed_edge_cols = ["date", "player_id", "stat", "side", "line"]
                missing = [c for c in needed_edge_cols if c not in edges_norm.columns]
                if missing:
                    result["notes"].append(f"props_edges missing required columns: {missing}")
                else:
                    df_edges = pd.merge(edges_norm, act_long, on=["date", "player_id", "stat"], how="left")
                    # Compute binary hit outcome; treat pushes (actual == line) as NA and exclude.
                    df_edges["line"] = pd.to_numeric(df_edges["line"], errors="coerce")
                    df_edges["actual_value"] = pd.to_numeric(df_edges["actual_value"], errors="coerce")
                    side = df_edges["side"].astype(str).str.upper()
                    av = df_edges["actual_value"]
                    line = df_edges["line"]

                    push = av.eq(line)
                    hit_over = av.gt(line)
                    hit_under = av.lt(line)
                    hit = np.where(side.eq("OVER"), hit_over, np.where(side.eq("UNDER"), hit_under, np.nan))
                    hit = np.where(push, np.nan, hit)

                    df_edges["hit"] = hit
                    df_edges = df_edges.dropna(subset=["hit"])
                    df_edges["hit"] = df_edges["hit"].astype(int)

    df = None

    # Highest priority: edges + actuals -> line-level probability calibration.
    if df_edges is not None and not df_edges.empty:
        df = df_edges
    else:
        # Attempt to align/join on common keys if available
        join_keys = [k for k in ["date", "game_id", "player", "market", "stat", "side"] if k in recon.columns and k in preds.columns]
        if join_keys:
            try:
                df = pd.merge(preds, recon, on=join_keys, suffixes=("_pred", "_recon"))
            except Exception:
                df = None
        if df is None:
            # Fallback: use whichever has both prob and outcome
            df = recon.copy() if not recon.empty else preds.copy()

    result["rows"] = int(len(df))

    # Probability columns
    prob_cols = find_probability_columns(df)
    outcome_col = find_outcome_column(df)

    if not prob_cols:
        result["notes"].append("No probability-like columns found")
        return result

    if outcome_col is None:
        result["notes"].append("No outcome column found; computing sharpness only")
        # Sharpness: variance of probabilities
        sharp = float(np.nanmean([np.var(df[c].dropna().values) for c in prob_cols]))
        result["metrics"]["sharpness_var"] = sharp
        return result

    y = normalize_outcome(df[outcome_col].dropna())
    metrics: Dict[str, float] = {}

    # Evaluate for each prob column and also combined if multiple
    for c in prob_cols:
        p = df[c].loc[y.index].astype(float).values
        yt = y.values
        if len(p) == 0 or len(yt) == 0:
            continue
        metrics[f"brier_{c}"] = brier_score(yt, p)
        metrics[f"logloss_{c}"] = log_loss_safe(yt, p)
        # Calibration curve bins=10
        calib = calibration_curve(yt, p, bins=10)
        result.setdefault("calibration", {})[c] = calib.to_dict(orient="records")
        # Calibration slope via linear fit of y on p
        try:
            slope, intercept = np.polyfit(p, yt, 1)
            metrics[f"calibration_slope_{c}"] = float(slope)
            metrics[f"calibration_intercept_{c}"] = float(intercept)
        except Exception:
            pass
    # Sharpness overall
    sharp = float(np.nanmean([np.var(df[c].dropna().values) for c in prob_cols]))
    metrics["sharpness_var"] = sharp

    result["metrics"] = metrics
    return result


def main():
    parser = argparse.ArgumentParser(description="Simulation alignment report across windows")
    parser.add_argument("--date", type=str, default=dt.date.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)")
    parser.add_argument("--windows", type=str, default="30,60,90", help="Comma-separated windows (days)")
    parser.add_argument("--outdir", type=str, default=str(Path("data/processed/metrics")), help="Output directory for reports")
    args = parser.parse_args()

    end_date = dt.datetime.strptime(args.date, "%Y-%m-%d").date()
    windows = [int(x) for x in args.windows.split(",") if x.strip()]
    base_dir = Path("data/processed")

    os.makedirs(args.outdir, exist_ok=True)

    reports = []
    for w in windows:
        rep = evaluate_window(end_date, w, base_dir)
        reports.append(rep)

    # Aggregate summary
    summary = {
        "date": end_date.strftime("%Y-%m-%d"),
        "windows": windows,
        "reports": reports,
    }

    out_json = Path(args.outdir) / f"sim_alignment_{end_date.strftime('%Y-%m-%d')}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Also write a CSV summary of key metrics per window
    rows: List[Tuple[int, str, float]] = []
    for rep in reports:
        w = rep.get("days")
        for k, v in rep.get("metrics", {}).items():
            rows.append((w, k, v))
    if rows:
        df = pd.DataFrame(rows, columns=["window_days", "metric", "value"])
        out_csv = Path(args.outdir) / f"sim_alignment_metrics_{end_date.strftime('%Y-%m-%d')}.csv"
        df.to_csv(out_csv, index=False)

    print(f"Wrote alignment reports to {out_json}")


if __name__ == "__main__":
    main()
