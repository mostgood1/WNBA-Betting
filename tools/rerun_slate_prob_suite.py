"""Rerun the production-aligned slate_prob Top-N backtests in a reproducible sweep.

This is the "rerun suite" entrypoint for diagnosing pregame performance.

What it does:
- Auto-selects an end date (latest recon_props_YYYY-MM-DD.csv) unless provided.
- For each window size (e.g. 30, 60) and each SmartSim prefix (e.g. smart_sim_pregame),
  runs the *exact* Flask `/recommendations?view=slate` codepath via the existing
  backtester (`tools/backtest_top_recommendations.py --kind slate_prob`).
- Writes per-run JSON + ledger CSV under data/processed.
- Optionally runs the audit (calibration + random baseline) per run.
- Produces a consolidated summary CSV/JSON for quick comparison.

Examples:
  python tools/rerun_slate_prob_suite.py
  python tools/rerun_slate_prob_suite.py --end 2026-02-12 --windows 30,60 --prefixes smart_sim_pregame,smart_sim_pregame_pem
  python tools/rerun_slate_prob_suite.py --audit --audit-trials 100
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


def _iter_recon_props_dates() -> list[dt.date]:
    dates: list[dt.date] = []
    for fp in PROCESSED.glob("recon_props_*.csv"):
        stem = fp.stem  # recon_props_YYYY-MM-DD
        parts = stem.split("_")
        if len(parts) < 3:
            continue
        dstr = parts[-1]
        try:
            dates.append(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return sorted(set(dates))


def _iter_props_recommendations_dates() -> list[dt.date]:
    dates: list[dt.date] = []
    for fp in PROCESSED.glob("props_recommendations_*.csv"):
        stem = fp.stem  # props_recommendations_YYYY-MM-DD
        parts = stem.split("_")
        if len(parts) < 3:
            continue
        dstr = parts[-1]
        try:
            dates.append(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return sorted(set(dates))


def _iter_smartsim_dates(prefix: str) -> list[dt.date]:
    pref = str(prefix or "").strip()
    if not pref:
        return []
    dates: list[dt.date] = []
    # files like: {prefix}_YYYY-MM-DD_AWAY_HOME.json
    for fp in PROCESSED.glob(f"{pref}_????-??-??_*.json"):
        name = fp.name
        try:
            dstr = name[len(pref) + 1 : len(pref) + 11]
            dates.append(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return sorted(set(dates))


def _available_dates_for_prefix(prefix: str) -> list[dt.date]:
    recon = set(_iter_recon_props_dates())
    props = set(_iter_props_recommendations_dates())
    sim = set(_iter_smartsim_dates(prefix))
    return sorted(recon & props & sim)


def _fmt_pct(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return round(100.0 * float(x), 4)
    except Exception:
        return None


def _run_one(
    *,
    start: dt.date,
    end: dt.date,
    smart_sim_prefix: str,
    n_game: int,
    n_market: int,
    scope: str,
    max_plus_odds: float,
    slate_rank: str,
    p_shrink: float,
    max_per_player: int,
    min_prob: float | None,
    min_ev: float | None,
    w_ev: float | None,
    w_prob: float | None,
    w_z: float | None,
    w_unc: float | None,
    w_ctx: float | None,
    w_pace: float | None,
    w_inj: float | None,
    w_blowout: float | None,
    out_dir: Path,
    do_audit: bool,
    audit_trials: int,
    audit_seed: int,
    audit_bins: int,
) -> dict[str, Any]:
    import sys

    # Import the backtester as a module (tools is not a package).
    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    import backtest_top_recommendations as btr  # type: ignore

    summary, ledger = btr.backtest_slate_prob(
        start=start,
        end=end,
        n_game=int(n_game),
        n_market=int(n_market),
        scope=str(scope or "union"),
        max_plus_odds=float(max_plus_odds or 0.0),
        smart_sim_prefix=str(smart_sim_prefix or "smart_sim_pregame"),
        rank=str(slate_rank or "ev"),
        p_shrink=float(p_shrink),
        max_per_player=int(max_per_player),
        min_prob=min_prob,
        min_ev=min_ev,
        w_ev=w_ev,
        w_prob=w_prob,
        w_z=w_z,
        w_unc=w_unc,
        w_ctx=w_ctx,
        w_pace=w_pace,
        w_inj=w_inj,
        w_blowout=w_blowout,
    )

    weight_parts: list[str] = []
    for k, v in (
        ("wev", w_ev),
        ("wprob", w_prob),
        ("wz", w_z),
        ("wunc", w_unc),
        ("wctx", w_ctx),
        ("wpace", w_pace),
        ("winj", w_inj),
        ("wblow", w_blowout),
    ):
        if v is None:
            continue
        try:
            weight_parts.append(f"{k}-{float(v):.3f}")
        except Exception:
            continue
    weights_tag = ("" if not weight_parts else ("_" + "_".join(weight_parts)))

    run_id = (
        f"slate_prob_{smart_sim_prefix}_"
        f"{start.isoformat()}_{end.isoformat()}_"
        f"scope-{scope}_ng-{int(n_game)}_nm-{int(n_market)}_"
        f"rank-{str(slate_rank or 'ev')}_ps-{float(p_shrink):.2f}_mpp-{int(max_per_player)}_"
        f"minp-{('' if min_prob is None else f'{float(min_prob):.3f}')}_minev-{('' if min_ev is None else f'{float(min_ev):.3f}')}{weights_tag}"
    )

    out_json = out_dir / f"_backtest_{run_id}.json"
    out_ledger = out_dir / f"_backtest_{run_id}.ledger.csv"

    payload = {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "selection": {
            "kind": "slate_prob",
            "n_game": int(n_game),
            "n_market": int(n_market),
            "scope": str(scope),
            "smart_sim_prefix": str(smart_sim_prefix),
            "max_plus_odds": float(max_plus_odds or 0.0),
            "slate_rank": str(slate_rank or "ev"),
            "p_shrink": float(p_shrink),
            "max_per_player": int(max_per_player),
            "min_prob": (None if min_prob is None else float(min_prob)),
            "min_ev": (None if min_ev is None else float(min_ev)),
            "combo_weights": {
                "w_ev": w_ev,
                "w_prob": w_prob,
                "w_z": w_z,
                "w_unc": w_unc,
                "w_ctx": w_ctx,
                "w_pace": w_pace,
                "w_inj": w_inj,
                "w_blowout": w_blowout,
            },
        },
        "slate_prob": summary,
    }

    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if ledger is not None:
        ledger.to_csv(out_ledger, index=False)

    audit_payload: dict[str, Any] | None = None
    out_audit = None
    if do_audit and ledger is not None and (not ledger.empty):
        import audit_slate_prob_backtest as audit  # type: ignore

        out_audit = out_dir / f"_audit_{run_id}.json"
        audit_payload = audit.audit_ledger_df(
            ledger,
            smart_sim_prefix=str(smart_sim_prefix),
            seed=int(audit_seed),
            trials=int(audit_trials),
            bins=int(audit_bins),
        )
        audit_payload["window"] = {"start": start.isoformat(), "end": end.isoformat()}
        out_audit.write_text(json.dumps(audit_payload, indent=2), encoding="utf-8")

    # One-row summary
    s = summary or {}
    row = {
        "run_id": run_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "window_days": int((end - start).days + 1),
        "smart_sim_prefix": str(smart_sim_prefix),
        "scope": str(scope),
        "n_game": int(n_game),
        "n_market": int(n_market),
        "max_plus_odds": float(max_plus_odds or 0.0),
        "slate_rank": str(slate_rank or "ev"),
        "p_shrink": float(p_shrink),
        "max_per_player": int(max_per_player),
        "min_prob": (None if min_prob is None else float(min_prob)),
        "min_ev": (None if min_ev is None else float(min_ev)),
        "w_ev": (None if w_ev is None else float(w_ev)),
        "w_prob": (None if w_prob is None else float(w_prob)),
        "w_z": (None if w_z is None else float(w_z)),
        "w_unc": (None if w_unc is None else float(w_unc)),
        "w_ctx": (None if w_ctx is None else float(w_ctx)),
        "w_pace": (None if w_pace is None else float(w_pace)),
        "w_inj": (None if w_inj is None else float(w_inj)),
        "w_blowout": (None if w_blowout is None else float(w_blowout)),
        "dates": s.get("dates"),
        "bets": s.get("bets"),
        "wins": s.get("wins"),
        "losses": s.get("losses"),
        "pushes": s.get("pushes"),
        "accuracy": s.get("accuracy"),
        "accuracy_pct": _fmt_pct(s.get("accuracy")),
        "roi": s.get("roi"),
        "roi_pct": _fmt_pct(s.get("roi")),
        "profit_sum": s.get("profit_sum"),
        "out_json": str(out_json),
        "out_ledger": str(out_ledger),
        "out_audit": None if out_audit is None else str(out_audit),
    }

    if audit_payload:
        head = audit_payload.get("headline") or {}
        base = audit_payload.get("random_baseline") or {}
        row.update(
            {
                "audit_brier": audit_payload.get("brier"),
                "audit_roi_pct": _fmt_pct(head.get("roi")),
                "audit_acc_pct": _fmt_pct(head.get("accuracy")),
                "baseline_roi_mean_pct": _fmt_pct(base.get("roi_mean")),
                "baseline_acc_mean_pct": _fmt_pct(base.get("acc_mean")),
            }
        )

    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: latest recon_props date)")
    ap.add_argument("--windows", default="30,60", help="Comma-separated window sizes in days (default: 30,60)")
    ap.add_argument(
        "--prefixes",
        default="smart_sim_pregame,smart_sim_pregame_pem",
        help="Comma-separated SmartSim prefixes to sweep",
    )
    ap.add_argument("--n-game", type=int, default=3)
    ap.add_argument("--n-market", type=int, default=4)
    ap.add_argument("--scope", choices=["union", "per_game", "per_market", "both"], default="union")
    ap.add_argument("--max-plus-odds", type=float, default=0.0)
    ap.add_argument("--slate-rank", choices=["ev", "prob", "z", "combo"], default="ev")
    ap.add_argument("--p-shrink", type=float, default=1.0)
    ap.add_argument("--max-per-player", type=int, default=1)
    ap.add_argument("--min-prob", type=float, default=None)
    ap.add_argument("--min-ev", type=float, default=None)

    ap.add_argument("--w-ev", type=float, default=None)
    ap.add_argument("--w-prob", type=float, default=None)
    ap.add_argument("--w-z", type=float, default=None)
    ap.add_argument("--w-unc", type=float, default=None)
    ap.add_argument("--w-ctx", type=float, default=None)
    ap.add_argument("--w-pace", type=float, default=None)
    ap.add_argument("--w-inj", type=float, default=None)
    ap.add_argument("--w-blowout", type=float, default=None)

    ap.add_argument("--out-dir", default=str(PROCESSED))

    ap.add_argument("--audit", action="store_true", help="Run audit_slate_prob_backtest for each run")
    ap.add_argument("--audit-trials", type=int, default=100)
    ap.add_argument("--audit-seed", type=int, default=7)
    ap.add_argument("--audit-bins", type=int, default=10)

    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")

    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    prefixes = [p.strip() for p in str(args.prefixes).split(",") if p.strip()]
    if not prefixes:
        raise SystemExit("No prefixes provided")

    if str(args.end).strip():
        end = dt.date.fromisoformat(str(args.end).strip())
    else:
        # Pick an end date that actually has coverage for each requested prefix.
        # If some prefixes have zero coverage, drop them with a warning.
        coverage: dict[str, list[dt.date]] = {p: _available_dates_for_prefix(p) for p in prefixes}
        dropped = [p for p, ds in coverage.items() if not ds]
        if dropped:
            print(f"WARNING: dropping prefixes with no coverage: {dropped}")
            prefixes = [p for p in prefixes if p not in set(dropped)]
        if not prefixes:
            raise SystemExit(
                "No prefixes have coverage (need recon_props_*.csv + props_recommendations_*.csv + {prefix}_YYYY-MM-DD_*.json)"
            )

        latest_per_prefix = [max(coverage[p]) for p in prefixes]
        end = min(latest_per_prefix)

    windows: list[int] = []
    for tok in str(args.windows).split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            w = int(tok)
        except Exception:
            continue
        if w > 0:
            windows.append(w)
    windows = sorted(set(windows))
    if not windows:
        raise SystemExit("No valid --windows provided")

    rows: list[dict[str, Any]] = []
    for w in windows:
        start = end - dt.timedelta(days=int(w) - 1)
        for pref in prefixes:
            rows.append(
                _run_one(
                    start=start,
                    end=end,
                    smart_sim_prefix=pref,
                    n_game=int(args.n_game),
                    n_market=int(args.n_market),
                    scope=str(args.scope),
                    max_plus_odds=float(args.max_plus_odds or 0.0),
                    slate_rank=str(args.slate_rank or "ev"),
                    p_shrink=float(args.p_shrink),
                    max_per_player=int(args.max_per_player),
                    min_prob=args.min_prob,
                    min_ev=args.min_ev,
                    w_ev=args.w_ev,
                    w_prob=args.w_prob,
                    w_z=args.w_z,
                    w_unc=args.w_unc,
                    w_ctx=args.w_ctx,
                    w_pace=args.w_pace,
                    w_inj=args.w_inj,
                    w_blowout=args.w_blowout,
                    out_dir=out_dir,
                    do_audit=bool(args.audit),
                    audit_trials=int(args.audit_trials),
                    audit_seed=int(args.audit_seed),
                    audit_bins=int(args.audit_bins),
                )
            )

    df = pd.DataFrame(rows)
    if df is None or df.empty:
        raise SystemExit("No runs produced")

    # Sort: largest windows first, then prefix.
    df["window_days"] = pd.to_numeric(df.get("window_days"), errors="coerce")
    df = df.sort_values(["window_days", "smart_sim_prefix"], ascending=[False, True])

    # Avoid collisions across different tuning settings (rank/thresholds/weights).
    suite_parts: list[str] = [
        f"scope-{str(args.scope)}",
        f"ng-{int(args.n_game)}",
        f"nm-{int(args.n_market)}",
        f"maxodds-{float(args.max_plus_odds or 0.0):.0f}",
        f"rank-{str(args.slate_rank or 'ev')}",
    ]
    suite_parts.append(f"ps-{float(args.p_shrink):.2f}")
    suite_parts.append(f"mpp-{int(args.max_per_player)}")
    suite_parts.append(
        f"minp-{('' if args.min_prob is None else f'{float(args.min_prob):.3f}')}_minev-{('' if args.min_ev is None else f'{float(args.min_ev):.3f}')}"
    )
    for k, v in (
        ("wev", args.w_ev),
        ("wprob", args.w_prob),
        ("wz", args.w_z),
        ("wunc", args.w_unc),
        ("wctx", args.w_ctx),
        ("wpace", args.w_pace),
        ("winj", args.w_inj),
        ("wblow", args.w_blowout),
    ):
        if v is None:
            continue
        try:
            suite_parts.append(f"{k}-{float(v):.3f}")
        except Exception:
            continue
    suite_tag = "_" + "_".join(suite_parts)

    out_csv = (
        Path(args.out_csv)
        if str(args.out_csv).strip()
        else (out_dir / f"_suite_slate_prob_{end.isoformat()}{suite_tag}.csv")
    )
    out_json = (
        Path(args.out_json)
        if str(args.out_json).strip()
        else (out_dir / f"_suite_slate_prob_{end.isoformat()}{suite_tag}.json")
    )

    df.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps({"end": end.isoformat(), "runs": rows}, indent=2), encoding="utf-8")

    # Compact console summary
    cols = ["window_days", "smart_sim_prefix", "dates", "bets", "accuracy_pct", "roi_pct", "profit_sum"]
    existing = [c for c in cols if c in df.columns]
    print(df[existing].to_string(index=False))
    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
