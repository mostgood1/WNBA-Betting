"""Assess whether micro time bins (10/15/30s) carry usable signal for live pace.

Motivation:
- We sometimes see very quick shifts in scoring pace.
- This tool checks if short-window scoring rate actually predicts near-future scoring
  better than a simple baseline (game-to-date average).

Data source:
- Uses cached ESPN PBP CSVs: data/processed/pbp_espn_<date>.csv

Method (regulation only):
- Build a per-second total-points timeline from PBP scores.
- For each time t, compare next-horizon points (default 180s) to predictions based on:
  A) last-W-seconds points scaled to horizon
  B) game-to-date average rate scaled to horizon

Output:
- Markdown report under data/processed/

Usage:
  python tools/assess_micro_intervals.py --start 2026-02-01 --end 2026-02-11
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

REG_SECONDS = 48 * 60


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


def _load_pbp_for_dates(start: str, end: str) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for ds in pd.date_range(pd.to_datetime(start), pd.to_datetime(end), freq="D").strftime("%Y-%m-%d"):
        p = PROCESSED / f"pbp_espn_{ds}.csv"
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        df = df.copy()
        df["date"] = ds
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _infer_game_id_col(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    for c in ["game_id", "event_id"]:
        if c in df.columns and df[c].notna().any():
            return c
    return None


def _build_reg_points_timeline(gdf: pd.DataFrame) -> np.ndarray | None:
    """Return per-second total points timeline length REG_SECONDS+1."""
    if gdf is None or gdf.empty:
        return None
    if "period" not in gdf.columns:
        return None

    tmp = gdf.copy()

    # Must have scores.
    if "home_score" in tmp.columns and "away_score" in tmp.columns:
        hs = pd.to_numeric(tmp["home_score"], errors="coerce")
        aw = pd.to_numeric(tmp["away_score"], errors="coerce")
    elif "scoreHome" in tmp.columns and "scoreAway" in tmp.columns:
        hs = pd.to_numeric(tmp["scoreHome"], errors="coerce")
        aw = pd.to_numeric(tmp["scoreAway"], errors="coerce")
    else:
        return None

    tmp["_hs"] = hs
    tmp["_as"] = aw

    tmp["period"] = pd.to_numeric(tmp["period"], errors="coerce")
    tmp = tmp[tmp["period"].notna()].copy()
    tmp = tmp[(tmp["period"] >= 1) & (tmp["period"] <= 4)].copy()
    if tmp.empty:
        return None

    if "clock_sec_remaining" in tmp.columns:
        csr = pd.to_numeric(tmp["clock_sec_remaining"], errors="coerce")
    elif "clock" in tmp.columns:
        csr = pd.to_numeric(tmp["clock"], errors="coerce")
    else:
        return None

    tmp["_csr"] = csr
    tmp = tmp[tmp["_csr"].notna()].copy()
    if tmp.empty:
        return None

    # Elapsed seconds in regulation.
    tmp["_elapsed"] = (tmp["period"].astype(int) - 1) * 12 * 60 + (12 * 60 - tmp["_csr"].astype(float))
    tmp["_elapsed"] = pd.to_numeric(tmp["_elapsed"], errors="coerce")
    tmp = tmp[tmp["_elapsed"].notna()].copy()
    if tmp.empty:
        return None

    tmp["_elapsed"] = tmp["_elapsed"].clip(lower=0, upper=REG_SECONDS).round().astype(int)
    tmp["_tot"] = pd.to_numeric(tmp["_hs"], errors="coerce") + pd.to_numeric(tmp["_as"], errors="coerce")
    tmp = tmp[tmp["_tot"].notna()].copy()
    if tmp.empty:
        return None

    # Stable last score per elapsed second.
    if "sequence" in tmp.columns:
        tmp["_seq"] = pd.to_numeric(tmp["sequence"], errors="coerce")
    elif "actionNumber" in tmp.columns:
        tmp["_seq"] = pd.to_numeric(tmp["actionNumber"], errors="coerce")
    else:
        tmp["_seq"] = np.arange(len(tmp), dtype=float)

    tmp = tmp.sort_values(["_elapsed", "_seq"], ascending=[True, True], na_position="last")
    last_by_sec = tmp.groupby("_elapsed", as_index=True)["_tot"].last()

    s = last_by_sec.reindex(range(REG_SECONDS + 1)).ffill().fillna(0.0)
    arr = s.to_numpy(dtype=float)
    # Basic sanity: total should be nondecreasing.
    arr = np.maximum.accumulate(arr)
    return arr


@dataclass(frozen=True)
class WindowResult:
    window_sec: int
    horizon_sec: int
    n_samples: int
    n_games: int
    mae_pred_lastw: float
    rmse_pred_lastw: float
    corr_pred_lastw: float
    mae_pred_avg: float
    rmse_pred_avg: float
    corr_pred_avg: float


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 3:
        return float("nan")
    if not np.isfinite(a).all() or not np.isfinite(b).all():
        m = np.isfinite(a) & np.isfinite(b)
        a = a[m]
        b = b[m]
    if a.size < 3:
        return float("nan")
    try:
        return float(np.corrcoef(a, b)[0, 1])
    except Exception:
        return float("nan")


def evaluate_windows(df: pd.DataFrame, windows_sec: list[int], horizon_sec: int) -> list[WindowResult]:
    gid_col = _infer_game_id_col(df)
    if gid_col is None:
        return []

    results: list[WindowResult] = []

    for w in windows_sec:
        preds_lastw: list[float] = []
        preds_avg: list[float] = []
        actuals: list[float] = []
        n_games = 0

        for _, gdf in df.groupby(gid_col):
            tl = _build_reg_points_timeline(gdf)
            if tl is None:
                continue
            n_games += 1

            # Sample at 1-second granularity.
            # Avoid very early times (rate unstable) by starting at max(w, 60s).
            start_t = int(max(w, 60))
            end_t = int(REG_SECONDS - horizon_sec - 1)
            if end_t <= start_t:
                continue

            t_idx = np.arange(start_t, end_t + 1, dtype=int)

            lastw_pts = tl[t_idx] - tl[t_idx - int(w)]
            next_pts = tl[t_idx + int(horizon_sec)] - tl[t_idx]
            # Predictor A: last-W rate scaled.
            pred_a = lastw_pts * (float(horizon_sec) / float(w))
            # Predictor B: game-to-date avg rate scaled.
            elapsed = np.maximum(1.0, t_idx.astype(float))
            pred_b = (tl[t_idx] / elapsed) * float(horizon_sec)

            preds_lastw.append(pred_a)
            preds_avg.append(pred_b)
            actuals.append(next_pts)

        if not actuals:
            continue

        a = np.concatenate(actuals).astype(float)
        p1 = np.concatenate(preds_lastw).astype(float)
        p2 = np.concatenate(preds_avg).astype(float)

        err1 = a - p1
        err2 = a - p2

        mae1 = float(np.mean(np.abs(err1)))
        rmse1 = float(np.sqrt(np.mean(np.square(err1))))
        mae2 = float(np.mean(np.abs(err2)))
        rmse2 = float(np.sqrt(np.mean(np.square(err2))))

        results.append(
            WindowResult(
                window_sec=int(w),
                horizon_sec=int(horizon_sec),
                n_samples=int(a.size),
                n_games=int(n_games),
                mae_pred_lastw=mae1,
                rmse_pred_lastw=rmse1,
                corr_pred_lastw=_corr(p1, a),
                mae_pred_avg=mae2,
                rmse_pred_avg=rmse2,
                corr_pred_avg=_corr(p2, a),
            )
        )

    return results


def render_md(start: str, end: str, horizon_sec: int, results: list[WindowResult], notes: list[str]) -> str:
    lines: list[str] = []
    lines.append(f"# Micro-Interval Signal Report ({start}..{end})")
    lines.append("")
    lines.append(f"Horizon: next {horizon_sec}s points (regulation)")
    lines.append("")

    if not results:
        lines.append("No results (missing PBP or required columns).")
        return "\n".join(lines) + "\n"

    lines.append("## Results")
    lines.append("")
    lines.append("| window (s) | games | samples | MAE(last-W→h) | RMSE(last-W→h) | corr(last-W, next) | MAE(avg→h) | RMSE(avg→h) | corr(avg, next) |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in sorted(results, key=lambda x: x.window_sec):
        lines.append(
            "| "
            + " | ".join(
                [
                    str(r.window_sec),
                    str(r.n_games),
                    str(r.n_samples),
                    f"{r.mae_pred_lastw:.2f}",
                    f"{r.rmse_pred_lastw:.2f}",
                    f"{r.corr_pred_lastw:.3f}",
                    f"{r.mae_pred_avg:.2f}",
                    f"{r.rmse_pred_avg:.2f}",
                    f"{r.corr_pred_avg:.3f}",
                ]
            )
            + " |"
        )

    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("- If `MAE(last-W→h)` is not better than `MAE(avg→h)`, micro bins are mostly noise for predicting near-future scoring.")
    lines.append("- Even if correlation improves slightly, very short windows can be unstable (single made 3 swings the rate).")

    if notes:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        for n in notes:
            lines.append(f"- {n}")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--windows", type=str, default="10,15,30")
    ap.add_argument("--horizon-sec", type=int, default=180)
    ap.add_argument("--out", type=str, required=False)
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").strftime("%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d").strftime("%Y-%m-%d")

    windows_sec = []
    for tok in str(args.windows).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = int(float(tok))
        except Exception:
            continue
        if v > 0:
            windows_sec.append(v)
    windows_sec = sorted(set(windows_sec))

    df = _load_pbp_for_dates(start, end)
    notes: list[str] = []
    if df.empty:
        notes.append("No cached PBP CSVs found in data/processed for the date range.")

    results = evaluate_windows(df, windows_sec=windows_sec, horizon_sec=int(args.horizon_sec))

    out = Path(args.out) if args.out else (PROCESSED / f"micro_interval_signal_{start}_{end}_h{int(args.horizon_sec)}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_md(start, end, int(args.horizon_sec), results, notes), encoding="utf-8")

    print({"ok": True, "out": str(out), "start": start, "end": end, "horizon_sec": int(args.horizon_sec), "windows": windows_sec})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
