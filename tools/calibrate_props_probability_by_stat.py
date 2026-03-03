import argparse
import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
_DATA_ROOT = os.environ.get("NBA_BETTING_DATA_ROOT")
DATA_ROOT = Path(_DATA_ROOT).expanduser() if _DATA_ROOT else (BASE_DIR / "data")
PROC_DIR = DATA_ROOT / "processed"

# Wider than production card filters: calibration needs a broader implied-prob range
# to learn a meaningful monotone mapping. The production guardrails (max_plus_odds etc.)
# are enforced downstream when selecting plays.
PRICE_MIN = -400.0
PRICE_MAX = 400.0
BINS_DEFAULT = 10
GRID_N = 51

SUPPORTED_STATS = [
    "pts",
    "reb",
    "ast",
    "threes",
    "pra",
    "pr",
    "pa",
    "ra",
]


def _load_actuals() -> pd.DataFrame:
    act_parq = PROC_DIR / "props_actuals.parquet"
    if act_parq.exists():
        try:
            df = pd.read_parquet(act_parq)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
        except Exception:
            pass
    frames: list[pd.DataFrame] = []
    for p in sorted(PROC_DIR.glob("props_actuals_*.csv")):
        try:
            df = pd.read_csv(p)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception:
            continue
    if frames:
        return pd.concat(frames, ignore_index=True)

    # Fallback: daily reconciliation outputs (same columns as props_actuals snapshots)
    for p in sorted(PROC_DIR.glob("recon_props_*.csv")):
        try:
            df = pd.read_csv(p)
            if isinstance(df, pd.DataFrame) and not df.empty:
                frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _profit_per_unit(price: float) -> float:
    price = float(price)
    return (price / 100.0) if price > 0 else (100.0 / abs(price))


def _iter_window_dates(days: int, end: date) -> list[date]:
    start = end - timedelta(days=days - 1)
    out: list[date] = []
    d = start
    while d <= end:
        out.append(d)
        d = d + timedelta(days=1)
    return out


def _merge_edges_actuals(*, edges: pd.DataFrame, actuals_day: pd.DataFrame) -> pd.DataFrame:
    if edges is None or edges.empty:
        return pd.DataFrame()
    need_base = {"date", "player_id", "stat", "side", "line", "price"}
    if not need_base.issubset(set(edges.columns)):
        return pd.DataFrame()
    prob_col = "model_prob_raw" if "model_prob_raw" in edges.columns else "model_prob"
    if prob_col not in edges.columns:
        return pd.DataFrame()
    e = edges.copy()
    e["date"] = pd.to_datetime(e["date"], errors="coerce").dt.date
    e["player_id"] = pd.to_numeric(e["player_id"], errors="coerce")
    e["stat"] = e["stat"].astype(str).str.lower().str.strip()
    e["side"] = e["side"].astype(str).str.upper().str.strip()
    e["line"] = pd.to_numeric(e["line"], errors="coerce")
    e["price"] = pd.to_numeric(e["price"], errors="coerce")
    # Fit calibration off the *raw* model probability when available to avoid double-calibration.
    e["model_prob"] = pd.to_numeric(e[prob_col], errors="coerce")

    e = e[(e["price"] >= PRICE_MIN) & (e["price"] <= PRICE_MAX)].copy()
    e = e[e["stat"].isin(SUPPORTED_STATS)].copy()
    e = e.dropna(subset=["date", "player_id", "stat", "side", "line", "price", "model_prob"]).copy()
    if e.empty:
        return pd.DataFrame()

    a = actuals_day.copy()
    a["date"] = pd.to_datetime(a["date"], errors="coerce").dt.date
    if "player_id" in a.columns:
        a["player_id"] = pd.to_numeric(a["player_id"], errors="coerce")

    merged = e.merge(a, on=["date", "player_id"], how="left", suffixes=("", "_act"))
    if merged is None or merged.empty:
        return pd.DataFrame()

    stat_map = {
        "pts": "pts",
        "reb": "reb",
        "ast": "ast",
        "threes": "threes",
        "pra": "pra",
    }

    merged["actual_val"] = np.nan
    for st, col in stat_map.items():
        if col in merged.columns:
            mask = merged["stat"] == st
            if mask.any():
                merged.loc[mask, "actual_val"] = pd.to_numeric(merged.loc[mask, col], errors="coerce")

    # Composite markets: compute from components if present
    try:
        if {"pts", "reb"}.issubset(set(merged.columns)):
            mask = merged["stat"] == "pr"
            if mask.any():
                merged.loc[mask, "actual_val"] = (
                    pd.to_numeric(merged.loc[mask, "pts"], errors="coerce")
                    + pd.to_numeric(merged.loc[mask, "reb"], errors="coerce")
                )
    except Exception:
        pass
    try:
        if {"pts", "ast"}.issubset(set(merged.columns)):
            mask = merged["stat"] == "pa"
            if mask.any():
                merged.loc[mask, "actual_val"] = (
                    pd.to_numeric(merged.loc[mask, "pts"], errors="coerce")
                    + pd.to_numeric(merged.loc[mask, "ast"], errors="coerce")
                )
    except Exception:
        pass
    try:
        if {"reb", "ast"}.issubset(set(merged.columns)):
            mask = merged["stat"] == "ra"
            if mask.any():
                merged.loc[mask, "actual_val"] = (
                    pd.to_numeric(merged.loc[mask, "reb"], errors="coerce")
                    + pd.to_numeric(merged.loc[mask, "ast"], errors="coerce")
                )
    except Exception:
        pass

    merged = merged.dropna(subset=["actual_val"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["hit"] = np.where(
        (merged["side"] == "OVER") & (merged["actual_val"] > merged["line"]),
        1,
        np.where(
            (merged["side"] == "UNDER") & (merged["actual_val"] < merged["line"]),
            1,
            np.where((merged["actual_val"] == merged["line"]), np.nan, 0),
        ),
    )
    merged = merged.dropna(subset=["hit"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["unit_profit"] = merged["price"].astype(float).map(_profit_per_unit)
    merged["roi"] = np.where(merged["hit"] == 1, merged["unit_profit"], -1.0)

    keep = [
        "date",
        "player_id",
        "stat",
        "side",
        "line",
        "price",
        "model_prob",
        "hit",
        "roi",
    ]
    keep = [c for c in keep if c in merged.columns]
    return merged[keep].copy()


def _fit_monotone_curve(xs: np.ndarray, ys: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    xs = np.clip(np.asarray(xs, dtype=float), 0.0, 1.0)
    ys = np.clip(np.asarray(ys, dtype=float), 0.0, 1.0)

    # Try isotonic regression if available; otherwise use monotone envelope.
    try:
        from sklearn.isotonic import IsotonicRegression  # type: ignore

        iso = IsotonicRegression(y_min=0.0, y_max=1.0, increasing=True, out_of_bounds="clip")
        iso.fit(xs, ys)
        grid = np.linspace(0.0, 1.0, GRID_N)
        preds = iso.transform(grid)
        return grid, np.clip(preds, 0.0, 1.0)
    except Exception:
        # Enforce monotonicity via cummax and interpolate onto a fixed grid.
        order = np.argsort(xs)
        xs2 = xs[order]
        ys2 = ys[order]
        ys_mono = np.maximum.accumulate(ys2)
        grid = np.linspace(0.0, 1.0, GRID_N)
        preds = np.interp(grid, xs2, ys_mono)
        return grid, np.clip(preds, 0.0, 1.0)


def _reliability_bins(df: pd.DataFrame, bins: int) -> pd.DataFrame:
    tmp = df.copy()
    tmp["model_prob"] = pd.to_numeric(tmp["model_prob"], errors="coerce").clip(lower=0.0, upper=1.0)
    tmp["hit"] = pd.to_numeric(tmp["hit"], errors="coerce")
    tmp = tmp.dropna(subset=["model_prob", "hit"]).copy()
    if tmp.empty:
        return pd.DataFrame(columns=["bin_low", "bin_high", "n", "avg_model_prob", "hit_rate", "roi"])

    edges = np.linspace(0.0, 1.0, int(bins) + 1)
    tmp["prob_bin"] = pd.cut(tmp["model_prob"], bins=edges, include_lowest=True)
    grp = tmp.groupby("prob_bin", dropna=True, observed=False)
    out = grp.agg(
        n=("hit", "size"),
        hit_rate=("hit", "mean"),
        avg_model_prob=("model_prob", "mean"),
        roi=("roi", "mean"),
    ).reset_index()

    out["bin_low"] = out["prob_bin"].apply(
        lambda x: float(str(x).split(",")[0].strip("[").strip("(")) if pd.notna(x) else np.nan
    )
    out["bin_high"] = out["prob_bin"].apply(
        lambda x: float(str(x).split(",")[1].strip("]").strip(")")) if pd.notna(x) else np.nan
    )
    return out[["bin_low", "bin_high", "n", "avg_model_prob", "hit_rate", "roi"]].copy()


def _curve_payload(*, bins_df: pd.DataFrame, alpha: float) -> dict[str, Any] | None:
    if bins_df is None or bins_df.empty:
        return None
    b = bins_df.copy()
    b["avg_model_prob"] = pd.to_numeric(b["avg_model_prob"], errors="coerce")
    b["hit_rate"] = pd.to_numeric(b["hit_rate"], errors="coerce")
    b["n"] = pd.to_numeric(b["n"], errors="coerce")
    b = b.dropna(subset=["avg_model_prob", "hit_rate", "n"]).copy()
    if b.empty:
        return None

    xs = b["avg_model_prob"].astype(float).to_numpy()
    ys = b["hit_rate"].astype(float).to_numpy()
    grid, preds = _fit_monotone_curve(xs, ys)

    try:
        a = float(alpha)
    except Exception:
        a = 1.0
    a = float(np.clip(a, 0.0, 1.0))
    # Blend toward identity to avoid over-shrinking probabilities.
    # alpha=1 -> full calibration; alpha=0 -> identity.
    preds = (1.0 - a) * grid + a * preds
    return {
        "x": [float(v) for v in grid],
        "y": [float(v) for v in preds],
        "n": int(b["n"].sum()),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--bins", type=int, default=BINS_DEFAULT)
    ap.add_argument("--min-bets-per-stat", type=int, default=250)
    ap.add_argument(
        "--alpha",
        type=float,
        default=0.35,
        help="Calibration strength in [0,1]. 1=full mapping; 0=identity (no calibration).",
    )
    ap.add_argument("--out", type=str, default=str(PROC_DIR / "props_prob_calibration_by_stat.json"))
    ap.add_argument("--end", type=str, default="")
    args = ap.parse_args()

    if str(args.end).strip():
        end = pd.Timestamp(args.end).date()
    else:
        # Default to yesterday to avoid including same-day partial actuals.
        end = date.today() - timedelta(days=1)

    actuals = _load_actuals()
    if actuals is None or actuals.empty:
        raise SystemExit("no-actuals")

    actuals["date"] = pd.to_datetime(actuals["date"], errors="coerce").dt.date
    if "player_id" in actuals.columns:
        actuals["player_id"] = pd.to_numeric(actuals["player_id"], errors="coerce")

    rows: list[pd.DataFrame] = []
    dates = _iter_window_dates(int(args.days), end=end)
    for d in dates:
        ef = PROC_DIR / f"props_edges_{d}.csv"
        if not ef.exists():
            continue
        try:
            edges = pd.read_csv(ef)
        except Exception:
            continue
        a_day = actuals[actuals["date"] == d].copy()
        if a_day.empty:
            continue
        m = _merge_edges_actuals(edges=edges, actuals_day=a_day)
        if m is not None and not m.empty:
            rows.append(m)

    if not rows:
        raise SystemExit("no-rows-merged")

    all_df = pd.concat(rows, ignore_index=True)
    all_df["stat"] = all_df["stat"].astype(str).str.lower().str.strip()

    # Global curve
    global_bins = _reliability_bins(all_df, bins=int(args.bins))
    global_curve = _curve_payload(bins_df=global_bins, alpha=float(args.alpha))

    # Per-stat curves
    per_stat: dict[str, dict[str, Any]] = {}
    for st, grp in all_df.groupby("stat"):
        try:
            st_s = str(st).strip().lower()
            if st_s not in SUPPORTED_STATS:
                continue
            bins_df = _reliability_bins(grp, bins=int(args.bins))
            curve = _curve_payload(bins_df=bins_df, alpha=float(args.alpha))
            n = int(curve.get("n") or 0) if isinstance(curve, dict) else 0
            if curve is None or n < int(args.min_bets_per_stat):
                continue
            per_stat[st_s] = curve
        except Exception:
            continue

    out = {
        "updated_at": pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": int(args.days),
        "bins": int(args.bins),
        "price_min": float(PRICE_MIN),
        "price_max": float(PRICE_MAX),
        "global": global_curve,
        "per_stat": per_stat,
        "note": "Calibration curves map sim-engine model_prob -> empirical hit_rate (monotone). Per-stat curves require sufficient sample; otherwise global is used.",
    }

    out_path = Path(str(args.out))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")

    print(json.dumps({"ok": True, "out": str(out_path), "per_stat": sorted(list(per_stat.keys()))}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
