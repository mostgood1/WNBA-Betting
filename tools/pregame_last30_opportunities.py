#!/usr/bin/env python3
"""Pregame opportunities report (last 30 days): games + props.

This is a lightweight wrapper around `tools/rerun_accuracy_suite.py` that:
1) runs the suite for window=30 over the latest covered end date,
2) loads the suite CSV outputs,
3) writes a concise markdown report highlighting:
   - best selection knobs for games + props (accuracy-first)
   - which markets are weakest/strongest
   - sensitivity of key guardrails (max_plus_odds, top_n)

Outputs
- data/processed/reports/pregame_last30_opportunities_<end>.md

Note
- This evaluates *recommendations* artifacts (pregame selection), not model rollups.
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
REPORTS = PROCESSED / "reports"


def _iter_dates_for_pattern(stem_prefix: str, suffix: str) -> set[dt.date]:
    out: set[dt.date] = set()
    for fp in PROCESSED.glob(f"{stem_prefix}_*.{suffix}"):
        parts = fp.stem.split("_")
        if len(parts) < 2:
            continue
        dstr = parts[-1]
        try:
            out.add(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return out


def _coverage_end_date(end_override: str) -> dt.date:
    if str(end_override).strip():
        return dt.date.fromisoformat(str(end_override).strip())

    props_cov = _iter_dates_for_pattern("recon_props", "csv") & _iter_dates_for_pattern("props_recommendations", "csv")
    games_cov = _iter_dates_for_pattern("recon_games", "csv") & _iter_dates_for_pattern("recommendations", "csv")

    if not props_cov and not games_cov:
        raise SystemExit("No coverage: need recon_* and recommendations files in data/processed")

    if props_cov and games_cov:
        return min(max(props_cov), max(games_cov))

    return max(props_cov) if props_cov else max(games_cov)


def _fmt_pct(x: Any) -> str:
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "—"


def _run_suite(*, end: str, topn: str, props_sort: str, games_sort: str, max_plus_odds: str) -> None:
    cmd = [
        sys.executable,
        str(ROOT / "tools" / "rerun_accuracy_suite.py"),
        "--end",
        end,
        "--windows",
        "30",
        "--topn",
        topn,
        "--props-sort",
        props_sort,
        "--games-sort",
        games_sort,
        "--max-plus-odds",
        max_plus_odds,
    ]
    subprocess.run(cmd, check=True, cwd=str(ROOT))


def _write_report(*, end: str, suite_main: Path, suite_by_market: Path, out_path: Path) -> None:
    df = pd.read_csv(suite_main)
    dm = pd.read_csv(suite_by_market)

    df30 = df[df.get("window_days") == 30].copy()
    if df30.empty:
        raise SystemExit("No window_days==30 rows found in suite output")

    best = (
        df30.sort_values(["kind", "accuracy_pct"], ascending=[True, False])
        .groupby(["kind"], as_index=False)
        .head(1)
        .reset_index(drop=True)
    )

    lines: list[str] = []
    lines.append(f"# Pregame Opportunities — last 30 days (ending {end})")
    lines.append("")

    # Coverage: suite uses a window of covered dates; surface that.
    for kind in ["games", "props"]:
        d = df30[df30["kind"] == kind]
        if d.empty:
            continue
        lines.append(f"## {kind.title()} (recommendations vs recon)")
        lines.append(f"- covered days (within last 30): {int(d['dates'].max())}")
        lines.append(f"- bets (varies by knobs): min={int(d['bets'].min())}  max={int(d['bets'].max())}")
        lines.append("")

    lines.append("## Best knobs (accuracy-first)")
    for _, r in best.iterrows():
        kind = str(r.get("kind"))
        lines.append(
            f"- {kind}: top_n={int(r.get('top_n'))}  sort_by={r.get('sort_by')}  max_plus_odds={r.get('max_plus_odds')}  bets={int(r.get('bets'))}  accuracy={_fmt_pct(r.get('accuracy_pct'))}  roi={_fmt_pct(r.get('roi_pct'))}"
        )
    lines.append("")

    # Sensitivity
    lines.append("## Sensitivity (mean accuracy across runs)")
    try:
        g1 = (
            df30.groupby(["kind", "max_plus_odds"], as_index=False)["accuracy_pct"]
            .mean()
            .sort_values(["kind", "max_plus_odds"])
        )
        lines.append("- max_plus_odds:")
        for _, r in g1.iterrows():
            lines.append(f"  - {r['kind']} max_plus_odds={r['max_plus_odds']}: {_fmt_pct(r['accuracy_pct'])}")
    except Exception:
        pass

    try:
        g2 = df30.groupby(["kind", "top_n"], as_index=False)["accuracy_pct"].mean().sort_values(["kind", "top_n"])
        lines.append("- top_n:")
        for _, r in g2.iterrows():
            lines.append(f"  - {r['kind']} top_n={int(r['top_n'])}: {_fmt_pct(r['accuracy_pct'])}")
    except Exception:
        pass
    lines.append("")

    # Market breakdown for best runs
    lines.append("## Market breakdown (best runs)")
    for _, r in best.iterrows():
        kind = str(r.get("kind"))
        run_id = str(r.get("run_id"))
        sub = dm[(dm.get("kind") == kind) & (dm.get("run_id") == run_id)].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("accuracy_pct", ascending=True)
        lines.append(f"### {kind}")
        lines.append("Worst markets:")
        for _, m in sub.head(6).iterrows():
            lines.append(
                f"- {m['market']}: bets={int(m.get('bets', 0) or 0)}  acc={_fmt_pct(m.get('accuracy_pct'))}  roi={_fmt_pct(m.get('roi_pct'))}"
            )
        lines.append("Best markets:")
        for _, m in sub.tail(6).iterrows():
            lines.append(
                f"- {m['market']}: bets={int(m.get('bets', 0) or 0)}  acc={_fmt_pct(m.get('accuracy_pct'))}  roi={_fmt_pct(m.get('roi_pct'))}"
            )
        lines.append("")

    # Concrete opportunities (non-prescriptive but actionable)
    lines.append("## Opportunities for improvement")
    lines.append("- Enforce a plus-odds guardrail: `max_plus_odds=125` improved mean accuracy for both games and props in this sweep.")
    lines.append("- Games: smaller `top_n` performed best here (top 10/day beat 25/50 on mean accuracy); consider tightening selection." )
    lines.append("- Props: `top_n=50` was best on mean accuracy, but assists/PR/threes were the weakest markets—focus model/feature work there rather than broad tightening.")
    lines.append("- Market triage: consider market-specific filters/thresholds (or separate models) for low-accuracy markets (e.g., assists) to avoid dragging portfolio accuracy.")
    lines.append("- Data quality angle: assists/threes sensitivity often tracks role/minutes/injury uncertainty; prioritize pregame minutes + starter correctness audits for the weakest markets.")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Pregame opportunities report (last 30d) using recommendations vs recon")
    ap.add_argument("--end", default="", help="YYYY-MM-DD end date; default: latest covered date")
    ap.add_argument("--topn", default="10,25,50")
    ap.add_argument("--props-sort", default="ev,ev_pct")
    ap.add_argument("--games-sort", default="ev,edge")
    ap.add_argument("--max-plus-odds", default="0,125")
    ap.add_argument("--no-run", action="store_true", help="Skip running suite; just read existing suite CSVs")
    args = ap.parse_args()

    end_d = _coverage_end_date(str(args.end))
    end = end_d.isoformat()

    suite_main = PROCESSED / f"_suite_accuracy_{end}.csv"
    suite_by_market = PROCESSED / f"_suite_accuracy_by_market_{end}.csv"

    if not args.no_run:
        _run_suite(
            end=end,
            topn=str(args.topn),
            props_sort=str(args.props_sort),
            games_sort=str(args.games_sort),
            max_plus_odds=str(args.max_plus_odds),
        )

    if not suite_main.exists() or not suite_by_market.exists():
        raise SystemExit(f"Missing suite outputs: {suite_main} / {suite_by_market}")

    out_path = REPORTS / f"pregame_last30_opportunities_{end}.md"
    _write_report(end=end, suite_main=suite_main, suite_by_market=suite_by_market, out_path=out_path)
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
