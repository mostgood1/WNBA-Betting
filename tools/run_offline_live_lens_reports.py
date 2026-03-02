#!/usr/bin/env python3
"""Run offline Live Lens signals + audit/ROI for arbitrary dates.

Why this exists
--------------
The standard Live Lens audit/ROI scripts consume `live_lens_signals_<date>.jsonl`,
which is normally produced by the UI/server POST logging pipeline.

For historical evaluation when logs are missing, we can synthesize *totals* signals
from `game_cards_<date>.csv` + `_predictions_backup_<date>.csv` using
`tools/generate_offline_live_lens_signals.py`.

This runner:
- Generates signals into an isolated folder (won't overwrite real logs).
- Runs audit + ROI with `NBA_LIVE_LENS_DIR` pointing to that folder.
- Snapshots report outputs to per-date/per-variant filenames.

Supported markets
-----------------
- totals only (market="total")
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date as _date
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
REPORTS = PROCESSED / "reports"


def _parse_date(s: str) -> _date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _date_range(start: _date, end: _date) -> list[_date]:
    if end < start:
        start, end = end, start
    out: list[_date] = []
    d = start
    while d <= end:
        out.append(d)
        d = d + timedelta(days=1)
    return out


@dataclass(frozen=True)
class Variant:
    tag: str
    bias_points: float | None


def _run(
    cmd: list[str],
    *,
    env: dict[str, str] | None = None,
    ok_exit_codes: set[int] | None = None,
    nonfatal_stdout_substrings: tuple[str, ...] = (),
) -> subprocess.CompletedProcess[str]:
    ok = ok_exit_codes or {0}
    p = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False, capture_output=True, text=True)
    if p.returncode in ok:
        if p.returncode != 0 and nonfatal_stdout_substrings:
            if not any(s in (p.stdout or "") for s in nonfatal_stdout_substrings):
                raise RuntimeError(
                    "Command failed\n"
                    f"cmd: {' '.join(cmd)}\n"
                    f"exit: {p.returncode}\n"
                    f"stdout:\n{p.stdout}\n"
                    f"stderr:\n{p.stderr}\n"
                )
        return p

    raise RuntimeError(
        "Command failed\n"
        f"cmd: {' '.join(cmd)}\n"
        f"exit: {p.returncode}\n"
        f"stdout:\n{p.stdout}\n"
        f"stderr:\n{p.stderr}\n"
    )


def _copy_if_new(src: Path, dst: Path, *, min_mtime: float) -> bool:
    if not src.exists():
        return False
    try:
        if src.stat().st_mtime < float(min_mtime):
            return False
    except Exception:
        return False
    shutil.copyfile(src, dst)
    return True


def _snapshot_audit(ds: str, tag: str, *, min_mtime: float) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    src_a_md = REPORTS / f"live_lens_audit_{ds}.md"
    src_a_csv = REPORTS / f"live_lens_scored_{ds}.csv"

    dst_a_md = REPORTS / f"live_lens_audit_{ds}_{tag}.md"
    dst_a_csv = REPORTS / f"live_lens_scored_{ds}_{tag}.csv"

    _copy_if_new(src_a_md, dst_a_md, min_mtime=min_mtime)
    _copy_if_new(src_a_csv, dst_a_csv, min_mtime=min_mtime)


def _snapshot_roi(ds: str, tag: str, *, min_mtime: float) -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    src_r_md = REPORTS / f"live_lens_roi_{ds}.md"
    src_r_csv = REPORTS / f"live_lens_roi_scored_{ds}.csv"

    dst_r_md = REPORTS / f"live_lens_roi_{ds}_{tag}.md"
    dst_r_csv = REPORTS / f"live_lens_roi_scored_{ds}_{tag}.csv"

    _copy_if_new(src_r_md, dst_r_md, min_mtime=min_mtime)
    _copy_if_new(src_r_csv, dst_r_csv, min_mtime=min_mtime)


def main() -> int:
    ap = argparse.ArgumentParser(description="Offline Live Lens: generate signals + run audit/ROI")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="Single date YYYY-MM-DD")
    g.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")

    ap.add_argument("--end", help="End date YYYY-MM-DD (inclusive; default: start)")
    ap.add_argument("--min-left", type=float, default=24.0, help="Assumed minutes remaining (default: 24)")
    ap.add_argument("--watch", type=float, default=3.0, help="Totals WATCH threshold (default: 3)")
    ap.add_argument("--bet", type=float, default=6.0, help="Totals BET threshold (default: 6)")
    ap.add_argument(
        "--variants",
        default="bias_current,bias0",
        help="Comma-separated variants: bias_current,bias0 or bias:<points> (default: bias_current,bias0)",
    )
    ap.add_argument(
        "--include-watch",
        action="store_true",
        help="Include WATCH signals in ROI (default: BET only)",
    )
    ap.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip dates missing required inputs instead of erroring",
    )
    args = ap.parse_args()

    if args.date:
        dates = [_parse_date(args.date)]
    else:
        start = _parse_date(args.start)
        end = _parse_date(args.end) if args.end else start
        dates = _date_range(start, end)

    variants: list[Variant] = []
    for raw in str(args.variants).split(","):
        v = raw.strip().lower()
        if not v:
            continue
        if v in {"bias_current", "current"}:
            variants.append(Variant(tag="bias_current", bias_points=None))
        elif v in {"bias0", "zero"}:
            variants.append(Variant(tag="bias0", bias_points=0.0))
        elif v.startswith("bias:"):
            bp = float(v.split(":", 1)[1].strip())
            tag = f"bias{str(bp).replace('.', 'p').replace('-', 'm')}"
            variants.append(Variant(tag=tag, bias_points=bp))
        else:
            raise SystemExit(f"Unknown variant: {raw}")

    for d in dates:
        ds = d.isoformat()
        cards_path = PROCESSED / f"game_cards_{ds}.csv"
        preds_path = PROCESSED / f"_predictions_backup_{ds}.csv"
        rg_path = PROCESSED / f"recon_games_{ds}.csv"
        rq_path = PROCESSED / f"recon_quarters_{ds}.csv"
        rp_path = PROCESSED / f"recon_props_{ds}.csv"

        required = [cards_path, preds_path, rg_path, rq_path, rp_path]
        missing = [p for p in required if not p.exists()]
        if missing:
            msg = f"{ds}: missing {len(missing)} inputs (e.g. {missing[0].name})"
            if args.skip_missing:
                print(msg + " -> SKIP")
                continue
            raise SystemExit(msg)

        for v in variants:
            # Generate signals into an isolated folder so we never overwrite real logs.
            out_dir = PROCESSED / "_offline_live_lens" / v.tag / ds
            out_dir.mkdir(parents=True, exist_ok=True)
            out_jsonl = out_dir / f"live_lens_signals_{ds}.jsonl"

            gen_cmd = [
                sys.executable,
                "tools/generate_offline_live_lens_signals.py",
                "--date",
                ds,
                "--min-left",
                str(float(args.min_left)),
                "--watch",
                str(float(args.watch)),
                "--bet",
                str(float(args.bet)),
                "--out",
                str(out_jsonl),
            ]
            if v.bias_points is not None:
                gen_cmd.extend(["--bias-points", str(float(v.bias_points))])

            _run(gen_cmd)

            env = os.environ.copy()
            env["NBA_LIVE_LENS_DIR"] = str(out_dir)

            audit_cmd = [sys.executable, "tools/daily_live_lens_audit.py", "--date", ds]
            roi_cmd = [sys.executable, "tools/daily_live_lens_roi.py", "--date", ds]
            if args.include_watch:
                roi_cmd.append("--include-watch")

            t_a = time.time()
            _run(audit_cmd, env=env)
            _snapshot_audit(ds, v.tag, min_mtime=t_a)

            t_r = time.time()
            p_roi = _run(
                roi_cmd,
                env=env,
                ok_exit_codes={0, 2},
                nonfatal_stdout_substrings=("No scored rows for",),
            )
            if p_roi.returncode == 0:
                _snapshot_roi(ds, v.tag, min_mtime=t_r)
                print(f"{ds} {v.tag}: OK -> {out_jsonl}")
            else:
                print(f"{ds} {v.tag}: SKIP ROI (no scored rows) -> {out_jsonl}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
