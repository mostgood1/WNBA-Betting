"""Build + evaluate SmartSim 3-minute interval ladders and emit a drift report.

This is a thin orchestrator around:
- tools/build_interval_actuals_from_pbp_espn.py
- tools/evaluate_intervals.py
- (optional) tools/build_intervals_band_calibration.py

It exists to make "interval drift" checks easy to run for a date range and
produce a human-readable report you can use to decide whether calibration or
retraining is needed.

Usage:
  python tools/interval_drift_report.py --start 2026-02-01 --end 2026-02-11

Notes:
- Requires SmartSim JSONs in data/processed/: smart_sim_<date>_<HOME>_<AWAY>.json
- Uses cached data/processed/pbp_espn_<date>.csv when available; otherwise
  the actuals builder may fetch from ESPN.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + "  "
            + " ".join(cmd)
            + "\n\nSTDOUT:\n"
            + (p.stdout or "")
            + "\n\nSTDERR:\n"
            + (p.stderr or "")
        )


def _safe_float(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


@dataclass(frozen=True)
class SegStats:
    seg_idx: int
    n: int
    mean_err_seg_p50: float
    mae_seg_p50: float
    mean_err_cum_p50: float
    mae_cum_p50: float
    cover_seg_10_90: float
    cover_cum_10_90: float


def _segment_stats(detail: pd.DataFrame) -> list[SegStats]:
    if detail is None or detail.empty:
        return []

    out: list[SegStats] = []
    for si in range(1, 17):
        sdf = detail[detail["seg_idx"] == si]
        if sdf.empty:
            continue

        err_seg = pd.to_numeric(sdf.get("err_seg_p50"), errors="coerce").astype(float)
        err_cum = pd.to_numeric(sdf.get("err_cum_p50"), errors="coerce").astype(float)

        out.append(
            SegStats(
                seg_idx=int(si),
                n=int(len(sdf)),
                mean_err_seg_p50=float(err_seg.mean()),
                mae_seg_p50=float(err_seg.abs().mean()),
                mean_err_cum_p50=float(err_cum.mean()),
                mae_cum_p50=float(err_cum.abs().mean()),
                cover_seg_10_90=float(pd.to_numeric(sdf.get("cover_seg_10_90"), errors="coerce").mean()),
                cover_cum_10_90=float(pd.to_numeric(sdf.get("cover_cum_10_90"), errors="coerce").mean()),
            )
        )
    return out


def _render_report_md(
    *,
    start: str,
    end: str,
    summary: dict[str, Any],
    seg_stats: list[SegStats],
    out_detail: Path,
    out_summary: Path,
    out_actuals: Path,
    warnings: list[str],
) -> str:
    lines: list[str] = []
    lines.append(f"# Interval Drift Report ({start}..{end})")
    lines.append("")
    lines.append("## Artifacts")
    lines.append("")
    lines.append(f"- actuals: `{out_actuals.as_posix()}`")
    lines.append(f"- eval detail: `{out_detail.as_posix()}`")
    lines.append(f"- eval summary: `{out_summary.as_posix()}`")

    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- games: {summary.get('games')}")
    lines.append(f"- rows: {summary.get('rows')}")
    lines.append(f"- coverage seg 10-90: {_safe_float(summary.get('coverage_seg_10_90')):.3f}")
    lines.append(f"- coverage cum 10-90: {_safe_float(summary.get('coverage_cum_10_90')):.3f}")

    metrics = summary.get("metrics") or []
    if metrics:
        lines.append("")
        lines.append("## Metrics")
        lines.append("")
        for m in metrics:
            metric = str((m or {}).get("metric") or "")
            lines.append(
                f"- {metric}: mean_err={_safe_float((m or {}).get('mean_err')):.3f}, "
                f"mae={_safe_float((m or {}).get('mae')):.3f}, "
                f"rmse={_safe_float((m or {}).get('rmse')):.3f}"
            )

    if seg_stats:
        lines.append("")
        lines.append("## Per-segment (p50 drift)")
        lines.append("")
        lines.append("| seg | n | mean err seg p50 | MAE seg p50 | mean err cum p50 | MAE cum p50 | cov seg 10-90 | cov cum 10-90 |")
        lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
        for s in seg_stats:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(s.seg_idx),
                        str(s.n),
                        f"{s.mean_err_seg_p50:.2f}",
                        f"{s.mae_seg_p50:.2f}",
                        f"{s.mean_err_cum_p50:.2f}",
                        f"{s.mae_cum_p50:.2f}",
                        f"{s.cover_seg_10_90:.3f}",
                        f"{s.cover_cum_10_90:.3f}",
                    ]
                )
                + " |"
            )

        # Heuristic flags
        flags = []
        for s in seg_stats:
            if s.n < 10:
                continue
            if abs(s.mean_err_seg_p50) >= 2.0 or s.mae_seg_p50 >= 8.0:
                flags.append(
                    f"seg {s.seg_idx}: mean_err_seg_p50={s.mean_err_seg_p50:.2f}, mae_seg_p50={s.mae_seg_p50:.2f}"
                )
        if flags:
            lines.append("")
            lines.append("### Flags")
            lines.append("")
            for f in flags[:12]:
                lines.append(f"- {f}")

    if warnings:
        lines.append("")
        lines.append("## Notes")
        lines.append("")
        for w in warnings:
            lines.append(f"- {w}")

    lines.append("")
    lines.append("## Suggested next steps")
    lines.append("")
    lines.append("- If coverage is low (<0.80) and bands are too tight, run `tools/build_intervals_band_calibration.py` on this window.")
    lines.append("- If early segments show systematic p50 bias, consider retraining or refreshing SmartSim interval priors (see `tools/interval_estimation.py`).")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, required=True)
    ap.add_argument("--end", type=str, required=True)
    ap.add_argument("--smart-sim-dir", type=str, default=str(PROCESSED))
    ap.add_argument("--use-pbp-only", action="store_true")
    ap.add_argument("--rebuild-actuals", action="store_true")
    ap.add_argument("--rate-delay", type=float, default=0.15)
    ap.add_argument("--out-md", type=str, required=False)
    ap.add_argument("--build-band-calibration", action="store_true")
    ap.add_argument("--band-calibration-out", type=str, required=False)
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").strftime("%Y-%m-%d")
    end = datetime.strptime(args.end, "%Y-%m-%d").strftime("%Y-%m-%d")

    out_actuals = PROCESSED / f"smart_sim_intervals_actuals_{start}_{end}_pbp_espn.csv"
    out_detail = PROCESSED / f"intervals_eval_detail_{start}_{end}.csv"
    out_summary = PROCESSED / f"intervals_eval_summary_{start}_{end}.json"
    out_md = Path(args.out_md) if args.out_md else (PROCESSED / f"intervals_drift_report_{start}_{end}.md")

    warnings: list[str] = []

    # 1) Build actuals
    if args.rebuild_actuals or (not out_actuals.exists()):
        _run(
            [
                sys.executable,
                str(ROOT / "tools" / "build_interval_actuals_from_pbp_espn.py"),
                "--start",
                start,
                "--end",
                end,
                "--out",
                str(out_actuals),
                "--rate-delay",
                str(float(args.rate_delay)),
            ]
        )

    if not out_actuals.exists():
        raise SystemExit(f"Missing actuals CSV even after build: {out_actuals}")

    # 2) Evaluate
    eval_cmd = [
        sys.executable,
        str(ROOT / "tools" / "evaluate_intervals.py"),
        "--start",
        start,
        "--end",
        end,
        "--smart-sim-dir",
        str(Path(args.smart_sim_dir)),
        "--actuals",
        str(out_actuals),
        "--out-detail",
        str(out_detail),
        "--out-summary",
        str(out_summary),
    ]
    if args.use_pbp_only:
        eval_cmd.append("--use-pbp-only")

    _run(eval_cmd)

    # 3) Load and report
    summary = json.loads(out_summary.read_text(encoding="utf-8")) if out_summary.exists() else {}

    try:
        detail = pd.read_csv(out_detail)
    except Exception:
        detail = pd.DataFrame()

    if detail.empty:
        warnings.append("Detail CSV is empty; this usually means missing SmartSim JSONs or missing actuals matches.")

    seg_stats = _segment_stats(detail)

    report = _render_report_md(
        start=start,
        end=end,
        summary=summary,
        seg_stats=seg_stats,
        out_detail=out_detail,
        out_summary=out_summary,
        out_actuals=out_actuals,
        warnings=warnings,
    )

    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(report, encoding="utf-8")

    # 4) Optional: build band calibration
    if args.build_band_calibration:
        cal_out = Path(args.band_calibration_out) if args.band_calibration_out else (PROCESSED / "intervals_band_calibration.json")
        _run(
            [
                sys.executable,
                str(ROOT / "tools" / "build_intervals_band_calibration.py"),
                "--detail",
                str(out_detail),
                "--out",
                str(cal_out),
            ]
        )

    print(
        json.dumps(
            {
                "ok": True,
                "start": start,
                "end": end,
                "actuals": str(out_actuals),
                "out_detail": str(out_detail),
                "out_summary": str(out_summary),
                "out_md": str(out_md),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
