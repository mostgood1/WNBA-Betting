"""Accuracy-focused sweep across exported Props + Games recommendations.

Goal
- Compute accuracy for:
  - Props markets supported by recon settling: pts, reb, ast, threes/3pt/3pm, pra, pr, pa, ra
  - Games markets: TOTAL, ML, ATS
- Sweep simple selection knobs to find best accuracy (not ROI):
  - top_n (per day)
  - sort_by (props: ev/ev_pct, games: ev/edge)
  - max_plus_odds guardrail

This uses existing production artifacts:
- Props: data/processed/props_recommendations_YYYY-MM-DD.csv + recon_props_YYYY-MM-DD.csv
- Games: data/processed/recommendations_YYYY-MM-DD.csv + recon_games_YYYY-MM-DD.csv (+ optional game_odds_YYYY-MM-DD.csv)

It calls tools/backtest_top_recommendations.py functions directly so the settling logic stays identical.

Outputs (default under data/processed):
- _suite_accuracy_END.csv (one row per run)
- _suite_accuracy_by_market_END.csv (one row per run+market)
- per-run JSON + ledger CSV for easy drilldown

Examples
  python tools/rerun_accuracy_suite.py
  python tools/rerun_accuracy_suite.py --end 2026-02-12 --windows 30,60 --topn 10,25,50 --props-sort ev,ev_pct --games-sort ev,edge --max-plus-odds 0,125
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
TOOLS = ROOT / "tools"


def _iter_dates_for_pattern(stem_prefix: str, suffix: str) -> list[dt.date]:
    dates: list[dt.date] = []
    for fp in PROCESSED.glob(f"{stem_prefix}_*.{suffix}"):
        stem = fp.stem
        parts = stem.split("_")
        if len(parts) < 2:
            continue
        dstr = parts[-1]
        try:
            dates.append(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return sorted(set(dates))


def _iter_recon_props_dates() -> set[dt.date]:
    return set(_iter_dates_for_pattern("recon_props", "csv"))


def _iter_recon_games_dates() -> set[dt.date]:
    return set(_iter_dates_for_pattern("recon_games", "csv"))


def _iter_props_recs_dates() -> set[dt.date]:
    return set(_iter_dates_for_pattern("props_recommendations", "csv"))


def _iter_games_recs_dates() -> set[dt.date]:
    return set(_iter_dates_for_pattern("recommendations", "csv"))


def _coverage_end_date(end_override: str) -> dt.date:
    if str(end_override).strip():
        return dt.date.fromisoformat(str(end_override).strip())

    props_cov = _iter_recon_props_dates() & _iter_props_recs_dates()
    games_cov = _iter_recon_games_dates() & _iter_games_recs_dates()

    if not props_cov and not games_cov:
        raise SystemExit("No coverage: need recon_* and recommendations files in data/processed")

    # If both exist, pick a date that supports both so combined comparisons are meaningful.
    if props_cov and games_cov:
        return min(max(props_cov), max(games_cov))

    # Otherwise fall back to whichever exists.
    return max(props_cov) if props_cov else max(games_cov)


def _parse_int_list(s: str) -> list[int]:
    out: list[int] = []
    for tok in str(s or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = int(tok)
        except Exception:
            continue
        if v > 0:
            out.append(v)
    return sorted(set(out))


def _parse_float_list(s: str) -> list[float]:
    out: list[float] = []
    for tok in str(s or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            v = float(tok)
        except Exception:
            continue
        if v >= 0:
            out.append(v)
    return sorted(set(out))


def _parse_str_list(s: str) -> list[str]:
    out = [x.strip() for x in str(s or "").split(",") if x.strip()]
    # preserve order but dedupe
    seen: set[str] = set()
    res: list[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        res.append(x)
    return res


def _fmt_pct(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return round(100.0 * float(x), 4)
    except Exception:
        return None


def _load_backtester():
    import sys

    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    import backtest_top_recommendations as btr  # type: ignore

    return btr


def _write_run_artifacts(
    *,
    out_dir: Path,
    kind: str,
    start: dt.date,
    end: dt.date,
    params: dict[str, Any],
    summary: dict[str, Any],
    ledger: pd.DataFrame,
) -> tuple[str, str]:
    param_slug = "_".join(
        [
            f"top{int(params.get('top_n'))}",
            f"sort-{params.get('sort_by')}",
            f"mxplus-{str(params.get('max_plus_odds')).replace('.', 'p')}",
        ]
    )
    run_id = f"{kind}_{start.isoformat()}_{end.isoformat()}_{param_slug}"

    out_json = out_dir / f"_backtest_{run_id}.json"
    out_ledger = out_dir / f"_backtest_{run_id}.ledger.csv"

    payload = {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "selection": {"kind": kind, **params},
        kind: summary,
    }

    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if ledger is not None:
        ledger.to_csv(out_ledger, index=False)

    return str(out_json), str(out_ledger)


def _explode_by_market(kind: str, run_id: str, start: str, end: str, summary: dict[str, Any]) -> list[dict[str, Any]]:
    bm = summary.get("by_market") or {}
    if not isinstance(bm, dict):
        return []
    rows: list[dict[str, Any]] = []
    for market, ms in bm.items():
        if not isinstance(ms, dict):
            continue
        rows.append(
            {
                "kind": kind,
                "run_id": run_id,
                "start": start,
                "end": end,
                "market": str(market),
                "bets": ms.get("bets"),
                "wins": ms.get("wins"),
                "losses": ms.get("losses"),
                "pushes": ms.get("pushes"),
                "accuracy": ms.get("accuracy"),
                "accuracy_pct": _fmt_pct(ms.get("accuracy")),
                "roi": ms.get("roi"),
                "roi_pct": _fmt_pct(ms.get("roi")),
                "profit_sum": ms.get("profit_sum"),
            }
        )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: latest covered date)")
    ap.add_argument("--windows", default="30,60", help="Comma-separated window sizes in days")

    ap.add_argument("--topn", default="10,25,50", help="Comma-separated top N per day")
    ap.add_argument("--props-sort", default="ev,ev_pct", help="Comma-separated props sort_by values")
    ap.add_argument("--games-sort", default="ev,edge", help="Comma-separated games sort_by values")
    ap.add_argument("--max-plus-odds", default="0,125", help="Comma-separated max_plus_odds values (0 means disabled)")

    ap.add_argument("--out-dir", default=str(PROCESSED))
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-by-market-csv", default="")

    args = ap.parse_args()

    end = _coverage_end_date(str(args.end))

    windows = _parse_int_list(str(args.windows))
    if not windows:
        raise SystemExit("No valid --windows")

    topn_list = _parse_int_list(str(args.topn))
    if not topn_list:
        raise SystemExit("No valid --topn")

    props_sorts = _parse_str_list(str(args.props_sort))
    games_sorts = _parse_str_list(str(args.games_sort))
    if not props_sorts or not games_sorts:
        raise SystemExit("No valid sort lists")

    max_plus_odds_list = _parse_float_list(str(args.max_plus_odds))
    if not max_plus_odds_list:
        raise SystemExit("No valid --max-plus-odds")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    btr = _load_backtester()

    all_rows: list[dict[str, Any]] = []
    by_market_rows: list[dict[str, Any]] = []

    for w in windows:
        start = end - dt.timedelta(days=int(w) - 1)

        # Props runs
        for top_n in topn_list:
            for sort_by in props_sorts:
                for mx in max_plus_odds_list:
                    summary, ledger = btr.backtest_props_recommendations(
                        start=start,
                        end=end,
                        top_n=int(top_n),
                        sort_by=str(sort_by),
                        max_plus_odds=float(mx),
                    )
                    params = {"top_n": int(top_n), "sort_by": str(sort_by), "max_plus_odds": float(mx)}
                    out_json, out_ledger = _write_run_artifacts(
                        out_dir=out_dir,
                        kind="props",
                        start=start,
                        end=end,
                        params=params,
                        summary=summary,
                        ledger=ledger,
                    )
                    run_id = Path(out_json).stem.replace("_backtest_", "")
                    row = {
                        "kind": "props",
                        "run_id": run_id,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "window_days": int((end - start).days + 1),
                        **params,
                        "dates": summary.get("dates"),
                        "bets": summary.get("bets"),
                        "wins": summary.get("wins"),
                        "losses": summary.get("losses"),
                        "pushes": summary.get("pushes"),
                        "accuracy": summary.get("accuracy"),
                        "accuracy_pct": _fmt_pct(summary.get("accuracy")),
                        "roi": summary.get("roi"),
                        "roi_pct": _fmt_pct(summary.get("roi")),
                        "profit_sum": summary.get("profit_sum"),
                        "out_json": out_json,
                        "out_ledger": out_ledger,
                    }
                    all_rows.append(row)
                    by_market_rows.extend(_explode_by_market("props", run_id, start.isoformat(), end.isoformat(), summary))

        # Games runs
        for top_n in topn_list:
            for sort_by in games_sorts:
                for mx in max_plus_odds_list:
                    summary, ledger = btr.backtest_games_recommendations(
                        start=start,
                        end=end,
                        top_n=int(top_n),
                        sort_by=str(sort_by),
                        max_plus_odds=float(mx),
                    )
                    params = {"top_n": int(top_n), "sort_by": str(sort_by), "max_plus_odds": float(mx)}
                    out_json, out_ledger = _write_run_artifacts(
                        out_dir=out_dir,
                        kind="games",
                        start=start,
                        end=end,
                        params=params,
                        summary=summary,
                        ledger=ledger,
                    )
                    run_id = Path(out_json).stem.replace("_backtest_", "")
                    row = {
                        "kind": "games",
                        "run_id": run_id,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "window_days": int((end - start).days + 1),
                        **params,
                        "dates": summary.get("dates"),
                        "bets": summary.get("bets"),
                        "wins": summary.get("wins"),
                        "losses": summary.get("losses"),
                        "pushes": summary.get("pushes"),
                        "accuracy": summary.get("accuracy"),
                        "accuracy_pct": _fmt_pct(summary.get("accuracy")),
                        "roi": summary.get("roi"),
                        "roi_pct": _fmt_pct(summary.get("roi")),
                        "profit_sum": summary.get("profit_sum"),
                        "out_json": out_json,
                        "out_ledger": out_ledger,
                    }
                    all_rows.append(row)
                    by_market_rows.extend(_explode_by_market("games", run_id, start.isoformat(), end.isoformat(), summary))

    df = pd.DataFrame(all_rows)
    if df is None or df.empty:
        raise SystemExit("No runs produced")

    # Choose best per kind/window by accuracy, then by bets.
    def _pick_best(dd: pd.DataFrame) -> dict[str, Any] | None:
        if dd is None or dd.empty:
            return None
        x = dd.copy()
        x["accuracy"] = pd.to_numeric(x.get("accuracy"), errors="coerce")
        x["bets"] = pd.to_numeric(x.get("bets"), errors="coerce")
        x = x[x["accuracy"].notna()].copy()
        if x.empty:
            return None
        x = x.sort_values(["accuracy", "bets"], ascending=[False, False])
        return dict(x.iloc[0].to_dict())

    best_rows: list[dict[str, Any]] = []
    for (kind, window_days), g in df.groupby(["kind", "window_days"], dropna=False):
        b = _pick_best(g)
        if b:
            best_rows.append({"kind": kind, "window_days": int(window_days), **b})

    out_csv = Path(args.out_csv) if str(args.out_csv).strip() else (out_dir / f"_suite_accuracy_{end.isoformat()}.csv")
    out_by_market_csv = (
        Path(args.out_by_market_csv)
        if str(args.out_by_market_csv).strip()
        else (out_dir / f"_suite_accuracy_by_market_{end.isoformat()}.csv")
    )

    df.to_csv(out_csv, index=False)
    if by_market_rows:
        pd.DataFrame(by_market_rows).to_csv(out_by_market_csv, index=False)

    # Print compact best-of summary
    if best_rows:
        best_df = pd.DataFrame(best_rows)
        cols = ["kind", "window_days", "top_n", "sort_by", "max_plus_odds", "dates", "bets", "accuracy_pct", "roi_pct", "profit_sum"]
        cols = [c for c in cols if c in best_df.columns]
        print("\nBest by kind/window (accuracy-first):")
        print(best_df[cols].to_string(index=False))

    print(f"\nWrote: {out_csv}")
    if by_market_rows:
        print(f"Wrote: {out_by_market_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
