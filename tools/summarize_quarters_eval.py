from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _fmt(x) -> str:
    try:
        if x is None:
            return ""
        if pd.isna(x):
            return ""
        return f"{float(x):0.3f}"
    except Exception:
        return str(x)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    processed = root / "data" / "processed"

    recon = processed / "quarters_eval_recon_2025-12-25_2026-01-23.csv"
    team = processed / "quarters_eval_team_2025-12-25_2026-01-23.csv"
    summary = processed / "quarters_eval_summary_2025-12-25_2026-01-23.json"

    print("Quarter eval window: 2025-12-25 -> 2026-01-23 (last 30 days ending at latest recon)")
    print("Files:")
    print(" -", recon)
    print(" -", team)
    print(" -", summary)

    df_recon = pd.read_csv(recon)
    df_team = pd.read_csv(team)

    for df in (df_recon, df_team):
        for c in df.columns:
            if c != "metric":
                df[c] = pd.to_numeric(df[c], errors="coerce")

    print("\n== Recon quarter totals (actual - pred) ==")
    cols = ["metric", "n", "mean_err", "mae", "rmse", "within_3", "within_5"]
    print(
        df_recon[cols]
        .sort_values("metric")
        .to_string(index=False, float_format=lambda x: f"{x:0.3f}")
    )

    # Team eval aggregates
    q_totals = df_team[df_team["metric"].str.match(r"q[1-4]_total$")]
    q_home = df_team[df_team["metric"].str.match(r"q[1-4]_home$")]
    q_away = df_team[df_team["metric"].str.match(r"q[1-4]_away$")]
    h_totals = df_team[df_team["metric"].str.match(r"h[12]_total$")]

    print("\n== Team/Quarter aggregates (means across Q1-Q4) ==")
    for label, d in [("q_totals", q_totals), ("q_home", q_home), ("q_away", q_away)]:
        print(
            f"{label:8s}  mean_MAE={_fmt(d['mae'].mean())}  mean_abs_bias={_fmt(d['mean_err'].abs().mean())}  mean_within3={_fmt(d['within_3'].mean())}"
        )

    print("\n== Half totals (means) ==")
    print(
        f"h_totals  mean_MAE={_fmt(h_totals['mae'].mean())}  mean_abs_bias={_fmt(h_totals['mean_err'].abs().mean())}  mean_within5={_fmt(h_totals['within_5'].mean())}"
    )

    # Worst bias metrics
    d = df_team.dropna(subset=["mean_err"]).copy()
    d["abs_bias"] = d["mean_err"].abs()
    worst = d.sort_values("abs_bias", ascending=False).head(10)

    print("\n== Largest absolute bias metrics (team eval) ==")
    print(
        worst[["metric", "n", "mean_err", "mae", "rmse"]]
        .to_string(index=False, float_format=lambda x: f"{x:0.3f}")
    )

    if summary.exists():
        j = json.loads(summary.read_text(encoding="utf-8"))
        calib = (j.get("smart_sim_quarter_eval") or {}).get("calibration_means") or {}
        if calib:
            print("\n== Calibration means (lower is better) ==")
            for k in sorted(calib.keys()):
                print(f"{k:14s} {calib[k]:0.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
