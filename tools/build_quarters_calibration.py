"""Build a calibration artifact for quarter scoring splits/variance.

Outputs: data/processed/quarters_calibration.json

Inputs (best-effort):
- data/processed/team_period_shares.csv (team-specific Q1..Q4 scoring shares)
- data/processed/recon_quarters_*.csv (actual quarter total variability; Q1 often missing)

This is intentionally lightweight: it produces a league+team split profile and
per-quarter target SD for *total points* in a quarter.

Note: This file is part of the supported daily pipeline (see scripts/daily_update.ps1).
"""

from __future__ import annotations

import argparse
import glob
import json
from dataclasses import asdict, dataclass
import os
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass
class QuartersCalibration:
    version: int
    league_split: list[float]
    team_split_by_tri: dict[str, list[float]]
    quarter_total_sd: dict[str, float]
    sources: dict[str, object]


def _safe_norm_split(x: list[float]) -> list[float]:
    arr = np.array(x, dtype=float)
    arr = np.where(np.isfinite(arr) & (arr > 0), arr, 0.0)
    s = float(arr.sum())
    if s <= 0:
        return [0.25, 0.25, 0.25, 0.25]
    arr = arr / s
    return [float(v) for v in arr.tolist()]


def build(workspace: Path) -> QuartersCalibration:
    data_root_env = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
    data_root = Path(data_root_env).expanduser().resolve() if data_root_env else (workspace / "data")
    processed = data_root / "processed"

    # Team splits
    team_split_by_tri: dict[str, list[float]] = {}
    tps_fp = processed / "team_period_shares.csv"
    if tps_fp.exists():
        df = pd.read_csv(tps_fp)
        for _, r in df.iterrows():
            name = str(r.get("team") or "").strip()
            tri = name
            try:
                from nba_betting.teams import to_tricode

                tri = to_tricode(name)
            except Exception:
                tri = name[:3].upper()
            split = _safe_norm_split([
                float(r.get("q1")) if pd.notna(r.get("q1")) else 0.0,
                float(r.get("q2")) if pd.notna(r.get("q2")) else 0.0,
                float(r.get("q3")) if pd.notna(r.get("q3")) else 0.0,
                float(r.get("q4")) if pd.notna(r.get("q4")) else 0.0,
            ])
            if len(str(tri)) == 3:
                team_split_by_tri[str(tri).upper()] = split

    # League split as average of team splits (fallback to uniform)
    if team_split_by_tri:
        league_split = np.mean(np.array(list(team_split_by_tri.values()), dtype=float), axis=0)
        league_split = _safe_norm_split([float(x) for x in league_split.tolist()])
    else:
        league_split = [0.25, 0.25, 0.25, 0.25]

    # Quarter total SD targets from recon_quarters files (Q1 often missing => infer from Q2)
    recon_fps = sorted(glob.glob(str(processed / "recon_quarters_*.csv")))
    vals: dict[int, list[float]] = {1: [], 2: [], 3: [], 4: []}
    dates: list[str] = []
    for fp in recon_fps:
        try:
            df = pd.read_csv(fp)
        except Exception:
            continue
        try:
            d = str(df.get("date").iloc[0])
            dates.append(d)
        except Exception:
            pass
        for q in (1, 2, 3, 4):
            col = f"actual_q{q}_total"
            if col not in df.columns:
                continue
            x = pd.to_numeric(df[col], errors="coerce")
            x = x[np.isfinite(x) & (x > 0)]
            if not x.empty:
                vals[q].extend([float(v) for v in x.tolist()])

    q_sd: dict[str, float] = {}
    for q in (1, 2, 3, 4):
        arr = np.asarray(vals[q], dtype=float)
        if arr.size >= 5:
            q_sd[f"q{q}"] = float(arr.std(ddof=1))

    # If Q1 missing, set it to Q2 SD (or a conservative default)
    if "q1" not in q_sd:
        q_sd["q1"] = float(q_sd.get("q2") or 8.0)

    # Ensure other quarters have something reasonable
    for q in (2, 3, 4):
        key = f"q{q}"
        if key not in q_sd:
            q_sd[key] = float(q_sd.get("q1") or 8.0)

    sources: dict[str, object] = {
        "team_period_shares": str(tps_fp) if tps_fp.exists() else None,
        "recon_quarters_glob": "data/processed/recon_quarters_*.csv",
        "recon_quarters_files": len(recon_fps),
        "recon_dates": sorted({d for d in dates if d and d != "nan"})[:10],
    }

    return QuartersCalibration(
        version=1,
        league_split=league_split,
        team_split_by_tri=team_split_by_tri,
        quarter_total_sd=q_sd,
        sources=sources,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workspace", default=".", help="Repo root")
    ap.add_argument("--out", default="data/processed/quarters_calibration.json")
    args = ap.parse_args()

    ws = Path(args.workspace).resolve()
    cal = build(ws)

    data_root_env = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
    data_root = Path(data_root_env).expanduser().resolve() if data_root_env else (ws / "data")
    processed = data_root / "processed"
    if str(args.out).replace("\\", "/") == "data/processed/quarters_calibration.json":
        out_fp = (processed / "quarters_calibration.json").resolve()
    else:
        out_fp = (ws / args.out).resolve()
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    out_fp.write_text(json.dumps(asdict(cal), indent=2, sort_keys=True), encoding="utf-8")
    print(f"Wrote {out_fp}")
    print(f"league_split={cal.league_split}")
    print(f"quarter_total_sd={cal.quarter_total_sd}")
    print(f"n_teams={len(cal.team_split_by_tri)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
