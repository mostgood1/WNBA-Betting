"""Evaluate SmartSim 3-minute `intervals` accuracy vs ESPN PBP actuals.

This is NOT a betting backtest; it measures interval-ladder accuracy.

Inputs:
- SmartSim JSON files under data/processed/: smart_sim_<date>_<HOME>_<AWAY>.json
- Actuals CSV produced by tools/build_interval_actuals_from_pbp_espn.py

Outputs:
- Detail CSV (one row per game x segment)
- Summary JSON with aggregate error and coverage metrics

Usage:
  python tools/evaluate_intervals.py --start 2026-02-01 --end 2026-02-05
  python tools/evaluate_intervals.py --date 2026-02-05
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: _date, end: _date) -> list[_date]:
    if end < start:
        return []
    days = (end - start).days
    return [start + timedelta(days=i) for i in range(days + 1)]


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


def _q_get(d: Any, key: str) -> float:
    if isinstance(d, dict):
        return _safe_float(d.get(key))
    return float("nan")


@dataclass(frozen=True)
class MetricRow:
    metric: str
    n: int
    mean_err: float
    mae: float
    rmse: float
    p50_abs: float
    p90_abs: float


def _summarize_errors(name: str, err: pd.Series) -> MetricRow:
    e = pd.to_numeric(err, errors="coerce").dropna().astype(float)
    if e.empty:
        return MetricRow(metric=name, n=0, mean_err=float("nan"), mae=float("nan"), rmse=float("nan"), p50_abs=float("nan"), p90_abs=float("nan"))
    ae = e.abs()
    return MetricRow(
        metric=name,
        n=int(len(e)),
        mean_err=float(e.mean()),
        mae=float(ae.mean()),
        rmse=float(np.sqrt(np.mean(np.square(e)))),
        p50_abs=float(np.quantile(ae, 0.50)),
        p90_abs=float(np.quantile(ae, 0.90)),
    )


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _iter_smart_sim_files(start: _date, end: _date, smart_sim_dir: Path) -> list[Path]:
    out: list[Path] = []
    for d in _date_range(start, end):
        out.extend(sorted(smart_sim_dir.glob(f"smart_sim_{d.isoformat()}_*.json")))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", type=str, required=False)
    ap.add_argument("--start", type=str, required=False)
    ap.add_argument("--end", type=str, required=False)
    ap.add_argument("--smart-sim-dir", type=str, default=str(PROCESSED))
    ap.add_argument("--actuals", type=str, required=False, help="Path to intervals actuals CSV")
    ap.add_argument("--out-detail", type=str, required=False)
    ap.add_argument("--out-summary", type=str, required=False)
    ap.add_argument("--use-pbp-only", action="store_true", help="Keep only SmartSim JSONs with mode.use_pbp=True")
    args = ap.parse_args()

    if args.date:
        start = end = _parse_date(args.date)
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --date or --start and --end")
        start = _parse_date(args.start)
        end = _parse_date(args.end)

    actuals_path = Path(args.actuals) if args.actuals else (PROCESSED / f"smart_sim_intervals_actuals_{start}_{end}_pbp_espn.csv")
    if not actuals_path.exists():
        raise SystemExit(
            f"Missing actuals CSV: {actuals_path}. Build it with tools/build_interval_actuals_from_pbp_espn.py"
        )

    try:
        adf = pd.read_csv(actuals_path)
    except pd.errors.EmptyDataError:
        adf = pd.DataFrame()

    # If the actuals are empty, still produce empty outputs (so this tool is pipeline-friendly).
    if adf is None or adf.empty:
        summary: dict[str, Any] = {
            "source": "smart_sim_intervals_eval",
            "start": str(start),
            "end": str(end),
            "actuals": str(actuals_path),
            "rows": 0,
            "games": 0,
            "metrics": [],
            "note": "Actuals CSV is empty; nothing to evaluate.",
        }
        out_detail = Path(args.out_detail) if args.out_detail else (PROCESSED / f"intervals_eval_detail_{start}_{end}.csv")
        out_summary = Path(args.out_summary) if args.out_summary else (PROCESSED / f"intervals_eval_summary_{start}_{end}.json")
        out_detail.parent.mkdir(parents=True, exist_ok=True)
        out_summary.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame().to_csv(out_detail, index=False)
        out_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print({"ok": True, "rows": 0, "games": 0, "out_detail": str(out_detail), "out_summary": str(out_summary)})
        return 0

    for c in ["date", "home_tri", "away_tri"]:
        if c not in adf.columns:
            raise SystemExit(f"Actuals CSV missing required column: {c}")

    if "n_ot" not in adf.columns:
        adf["n_ot"] = 0

    adf["date"] = adf["date"].astype(str)
    adf["home_tri"] = adf["home_tri"].astype(str).str.upper().str.strip()
    adf["away_tri"] = adf["away_tri"].astype(str).str.upper().str.strip()

    akey = { (r["date"], r["home_tri"], r["away_tri"]): r for _, r in adf.iterrows() }

    smart_sim_dir = Path(args.smart_sim_dir)
    files = _iter_smart_sim_files(start, end, smart_sim_dir)

    detail_rows: list[dict[str, Any]] = []

    for fp in files:
        obj = _load_json(fp)
        if not isinstance(obj, dict):
            continue

        date_str = str(obj.get("date") or "")
        htri = str(obj.get("home") or "").upper().strip()
        atri = str(obj.get("away") or "").upper().strip()
        if not date_str or not htri or not atri:
            continue
        if htri == "NAN" or atri == "NAN":
            continue

        if args.use_pbp_only:
            try:
                if not bool(((obj.get("mode") or {}).get("use_pbp"))):
                    continue
            except Exception:
                continue

        arow = akey.get((date_str, htri, atri))
        if arow is None:
            continue

        # How many segments are present in actuals for this game?
        try:
            n_ot = int(float(arow.get("n_ot") or 0))
            n_ot = max(0, min(6, n_ot))
        except Exception:
            n_ot = 0
        n_act_segments = int(16 + n_ot)

        intervals = obj.get("intervals")
        segs = (intervals or {}).get("segments") if isinstance(intervals, dict) else None
        if not isinstance(segs, list) or len(segs) < 16:
            continue

        # ensure stable order
        try:
            segs_sorted = sorted(segs, key=lambda s: int((s or {}).get("idx") or 0))
        except Exception:
            segs_sorted = segs

        n_pred_segments = int(len(segs_sorted))
        n_eval = int(min(n_pred_segments, n_act_segments))

        for j in range(n_eval):
            s = segs_sorted[j] if j < len(segs_sorted) else None
            if not isinstance(s, dict):
                continue

            idx = int(s.get("idx") or j)
            quarter = int(s.get("quarter") or ((idx // 4) + 1))
            segn = int(s.get("seg") or ((idx % 4) + 1))

            act_seg = _safe_float(arow.get(f"seg{j+1}_total_act"))
            act_cum = _safe_float(arow.get(f"cum{j+1}_total_act"))

            pred_seg_mu = _safe_float(s.get("mu"))
            pred_seg_p10 = _q_get(s.get("q"), "p10")
            pred_seg_p50 = _q_get(s.get("q"), "p50")
            pred_seg_p90 = _q_get(s.get("q"), "p90")

            pred_cum_mu = _safe_float(s.get("cum_mu"))
            pred_cum_p10 = _q_get(s.get("cum_q"), "p10")
            pred_cum_p50 = _q_get(s.get("cum_q"), "p50")
            pred_cum_p90 = _q_get(s.get("cum_q"), "p90")

            if not np.isfinite(act_seg) or not np.isfinite(act_cum):
                continue

            detail_rows.append(
                {
                    "date": date_str,
                    "home_tri": htri,
                    "away_tri": atri,
                    "game_id": obj.get("game_id"),
                    "file": fp.name,
                    "seg_idx": int(j + 1),
                    "quarter": quarter,
                    "seg": segn,
                    "label": str(s.get("label") or ""),
                    "act_seg_total": float(act_seg),
                    "pred_seg_mu": float(pred_seg_mu),
                    "pred_seg_p10": float(pred_seg_p10),
                    "pred_seg_p50": float(pred_seg_p50),
                    "pred_seg_p90": float(pred_seg_p90),
                    "act_cum_total": float(act_cum),
                    "pred_cum_mu": float(pred_cum_mu),
                    "pred_cum_p10": float(pred_cum_p10),
                    "pred_cum_p50": float(pred_cum_p50),
                    "pred_cum_p90": float(pred_cum_p90),
                    "err_seg_mu": float(act_seg - pred_seg_mu) if np.isfinite(pred_seg_mu) else float("nan"),
                    "err_seg_p50": float(act_seg - pred_seg_p50) if np.isfinite(pred_seg_p50) else float("nan"),
                    "err_cum_mu": float(act_cum - pred_cum_mu) if np.isfinite(pred_cum_mu) else float("nan"),
                    "err_cum_p50": float(act_cum - pred_cum_p50) if np.isfinite(pred_cum_p50) else float("nan"),
                    "cover_seg_10_90": bool(np.isfinite(pred_seg_p10) and np.isfinite(pred_seg_p90) and (act_seg >= pred_seg_p10) and (act_seg <= pred_seg_p90)),
                    "cover_cum_10_90": bool(np.isfinite(pred_cum_p10) and np.isfinite(pred_cum_p90) and (act_cum >= pred_cum_p10) and (act_cum <= pred_cum_p90)),
                }
            )

    detail_df = pd.DataFrame(detail_rows)

    # Summary
    summary: dict[str, Any] = {
        "source": "smart_sim_intervals_eval",
        "start": str(start),
        "end": str(end),
        "actuals": str(actuals_path),
        "rows": int(len(detail_df)),
        "games": int(detail_df[["date", "home_tri", "away_tri"]].drop_duplicates().shape[0]) if not detail_df.empty else 0,
    }

    metrics: list[MetricRow] = []
    if not detail_df.empty:
        metrics.extend(
            [
                _summarize_errors("seg_total_mu", detail_df["err_seg_mu"]),
                _summarize_errors("seg_total_p50", detail_df["err_seg_p50"]),
                _summarize_errors("cum_total_mu", detail_df["err_cum_mu"]),
                _summarize_errors("cum_total_p50", detail_df["err_cum_p50"]),
            ]
        )
        summary["coverage_seg_10_90"] = float(detail_df["cover_seg_10_90"].mean()) if "cover_seg_10_90" in detail_df.columns else float("nan")
        summary["coverage_cum_10_90"] = float(detail_df["cover_cum_10_90"].mean()) if "cover_cum_10_90" in detail_df.columns else float("nan")

        # Per-segment coverage + MAE (helps see where it breaks)
        per_seg = []
        for si in range(1, 17):
            sdf = detail_df[detail_df["seg_idx"] == si]
            if sdf.empty:
                continue
            per_seg.append(
                {
                    "seg_idx": int(si),
                    "n": int(len(sdf)),
                    "mae_seg_mu": float(pd.to_numeric(sdf["err_seg_mu"], errors="coerce").abs().mean()),
                    "mae_cum_mu": float(pd.to_numeric(sdf["err_cum_mu"], errors="coerce").abs().mean()),
                    "cover_seg_10_90": float(sdf["cover_seg_10_90"].mean()),
                    "cover_cum_10_90": float(sdf["cover_cum_10_90"].mean()),
                }
            )
        summary["per_segment"] = per_seg

    summary["metrics"] = [asdict(m) for m in metrics]

    out_detail = Path(args.out_detail) if args.out_detail else (PROCESSED / f"intervals_eval_detail_{start}_{end}.csv")
    out_summary = Path(args.out_summary) if args.out_summary else (PROCESSED / f"intervals_eval_summary_{start}_{end}.json")

    out_detail.parent.mkdir(parents=True, exist_ok=True)
    out_summary.parent.mkdir(parents=True, exist_ok=True)

    detail_df.to_csv(out_detail, index=False)
    out_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print({"ok": True, "rows": summary["rows"], "games": summary["games"], "out_detail": str(out_detail), "out_summary": str(out_summary)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
