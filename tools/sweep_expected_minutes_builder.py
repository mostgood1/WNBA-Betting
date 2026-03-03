from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv" / "Scripts" / "python.exe"

_DATA_ROOT_ENV = (os.environ.get("NBA_BETTING_DATA_ROOT") or "").strip()
DATA_ROOT = Path(_DATA_ROOT_ENV).expanduser().resolve() if _DATA_ROOT_ENV else (ROOT / "data")
PROC_DIR = DATA_ROOT / "processed"


@dataclass(frozen=True)
class Combo:
    half_life: float
    alpha: float


def _run(cmd: list[str]) -> None:
    p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(
            "Command failed:\n"
            + " ".join(cmd)
            + "\n\nstdout:\n"
            + (p.stdout or "")
            + "\n\nstderr:\n"
            + (p.stderr or "")
        )


def _load_means(fp: Path) -> dict:
    d = json.loads(fp.read_text(encoding="utf-8"))
    return d.get("means", {})


def main() -> int:
    ap = argparse.ArgumentParser(description="Sweep rotations expected-minutes builder params and evaluate realism")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--lookback-days", type=int, default=60)
    ap.add_argument("--half-life", default="7,10,12,14")
    ap.add_argument("--alpha", default="0.3,0.5,0.7,1.0")
    ap.add_argument("--n-connected", type=int, default=50)
    ap.add_argument("--n-quarter", type=int, default=25)
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--out-csv", default=str(PROC_DIR / "sweep_expected_minutes.csv"))
    args = ap.parse_args()

    if not PY.exists():
        raise RuntimeError(f"Python not found at {PY}")

    half_lives = [float(x.strip()) for x in str(args.half_life).split(",") if x.strip()]
    alphas = [float(x.strip()) for x in str(args.alpha).split(",") if x.strip()]
    combos = [Combo(half_life=h, alpha=a) for h in half_lives for a in alphas]

    rows: list[dict] = []
    for c in combos:
        tag = f"hl{c.half_life:g}_a{c.alpha:g}".replace(".", "p")
        out_json = PROC_DIR / f"connected_realism_{args.start}_{args.end}_rotmin_{tag}.json"

        # 1) Build expected minutes
        _run(
            [
                str(PY),
                "tools/build_pregame_expected_minutes_range.py",
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--source",
                "rotations",
                "--rotations-lookback-days",
                str(int(args.lookback_days)),
                "--rotations-half-life-days",
                str(float(c.half_life)),
                "--rotations-blend-alpha",
                str(float(c.alpha)),
                "--overwrite",
            ]
        )

        # 2) Evaluate
        _run(
            [
                str(PY),
                "tools/evaluate_connected_realism.py",
                "--start",
                str(args.start),
                "--end",
                str(args.end),
                "--n-connected-samples",
                str(int(args.n_connected)),
                "--n-quarter-samples",
                str(int(args.n_quarter)),
                "--seed",
                str(int(args.seed)),
                "--out-json",
                str(out_json.as_posix()),
            ]
        )

        m = _load_means(out_json)
        row = {
            "half_life_days": c.half_life,
            "blend_alpha": c.alpha,
            "home_min_mae_topk": m.get("home_min_mae_topk"),
            "away_min_mae_topk": m.get("away_min_mae_topk"),
            "home_min_corr_topk": m.get("home_min_corr_topk"),
            "away_min_corr_topk": m.get("away_min_corr_topk"),
            "home_pts_mae_topk": m.get("home_pts_mae_topk"),
            "away_pts_mae_topk": m.get("away_pts_mae_topk"),
            "sim_pathology_30min_zerostat": m.get("sim_pathology_30min_zerostat"),
            "eval_json": str(out_json.as_posix()),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Simple composite score: minutes MAE priority, light penalty for points MAE.
    df["score"] = (
        df["home_min_mae_topk"].astype(float)
        + df["away_min_mae_topk"].astype(float)
        + 0.15 * df["home_pts_mae_topk"].astype(float)
        + 0.15 * df["away_pts_mae_topk"].astype(float)
    )

    out_csv = ROOT / str(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values(["score"], ascending=True).to_csv(out_csv, index=False)

    print(f"Wrote: {out_csv}")
    print(df.sort_values(["score"], ascending=True).head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
