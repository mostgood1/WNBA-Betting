"""Tune the sim-rooted slate_prob selector knobs for profit/accuracy.

This script sweeps selector parameters for the curated
`/recommendations?view=slate` codepath (slate_prob) using the
production-aligned backtester (`tools/backtest_top_recommendations.py`).

It is explicitly *rooted to the sim engine* because the selector uses SmartSim
mean/sd to compute win probability, then ranks/filters based on that.

Example:
  python tools/tune_slate_prob_selector.py --window 60 --prefix smart_sim_pregame \
    --rank ev,prob --p-shrink 0.6,0.8,1.0 --max-plus-odds 0,125 \
    --min-prob "",0.52,0.55 --min-ev "",0.02,0.05

Combo weights (rank=combo):
    python tools/tune_slate_prob_selector.py --window 60 --prefix smart_sim_pregame \
        --rank combo --p-shrink 1.0 --max-plus-odds 125 \
        --w-ev "",1.0,1.2 --w-z "",0.05,0.10 --w-unc "",0.03,0.05
"""

from __future__ import annotations

import argparse
import datetime as dt
import itertools
import random
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
TOOLS = ROOT / "tools"


def _iter_recon_props_dates() -> list[dt.date]:
    dates: list[dt.date] = []
    for fp in PROCESSED.glob("recon_props_*.csv"):
        parts = fp.stem.split("_")
        if len(parts) < 3:
            continue
        dstr = parts[-1]
        try:
            dates.append(dt.date.fromisoformat(dstr))
        except Exception:
            continue
    return sorted(set(dates))


def _parse_csv_list(s: str) -> list[str]:
    return [t.strip() for t in str(s or "").split(",") if t.strip() or t == ""]


def _parse_float_or_none(tok: str) -> float | None:
    t = str(tok)
    if t.strip() == "":
        return None
    try:
        return float(t)
    except Exception:
        return None


def _parse_range_or_none(tok: str) -> tuple[float, float] | None:
    t = str(tok or "").strip()
    if not t:
        return None
    # Accept "lo,hi" or "lo:hi"
    sep = ":" if ":" in t else ","
    parts = [p.strip() for p in t.split(sep) if p.strip()]
    if len(parts) != 2:
        return None
    try:
        lo = float(parts[0])
        hi = float(parts[1])
    except Exception:
        return None
    if hi < lo:
        lo, hi = hi, lo
    return (lo, hi)


def _sample_weight(
    rng: random.Random,
    *,
    fixed_choices: list[float | None],
    value_range: tuple[float, float] | None,
) -> float | None:
    if value_range is not None:
        lo, hi = value_range
        return float(rng.uniform(lo, hi))
    if not fixed_choices:
        return None
    if len(fixed_choices) == 1:
        return fixed_choices[0]
    return fixed_choices[int(rng.randrange(0, len(fixed_choices)))]


def _stable_int_seed(*parts: object, base: int) -> int:
    s = "|".join([str(p) for p in parts])
    h = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
    return int(base) + int(h, 16)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--end", default="", help="YYYY-MM-DD (default: latest recon_props date)")
    ap.add_argument("--window", type=int, default=60, help="Window size in days")
    ap.add_argument("--prefix", default="smart_sim_pregame", help="SmartSim prefix")

    ap.add_argument("--n-game", type=int, default=3)
    ap.add_argument("--n-market", type=int, default=4)
    ap.add_argument("--max-per-player", type=int, default=1)

    ap.add_argument("--rank", default="ev,prob", help="Comma-separated: ev,prob,combo,z")
    ap.add_argument("--p-shrink", default="0.6,0.8,1.0", help="Comma-separated floats")
    ap.add_argument("--max-plus-odds", default="0,125", help="Comma-separated floats")

    ap.add_argument(
        "--w-ev",
        default="",  # allow "" token to mean None
        help='Comma-separated combo weights (rank=combo); use empty token for None, e.g. ",1.0,1.2"',
    )
    ap.add_argument("--w-prob", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-z", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-unc", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-ctx", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-pace", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-inj", default="", help='Comma-separated combo weights; use empty token for None')
    ap.add_argument("--w-blowout", default="", help='Comma-separated combo weights; use empty token for None')

    ap.add_argument(
        "--combo-search",
        choices=["grid", "random"],
        default="grid",
        help="When rank includes combo: grid sweeps cartesian product of w_* lists; random samples weights from ranges/lists.",
    )
    ap.add_argument("--combo-samples", type=int, default=100, help="Number of random weight sets to sample (combo-search=random)")
    ap.add_argument("--combo-seed", type=int, default=7, help="RNG seed (combo-search=random)")

    ap.add_argument("--w-ev-range", default="", help='Uniform range for w_ev, e.g. "0.8,1.3" or "0.8:1.3"')
    ap.add_argument("--w-prob-range", default="", help='Uniform range for w_prob')
    ap.add_argument("--w-z-range", default="", help='Uniform range for w_z')
    ap.add_argument("--w-unc-range", default="", help='Uniform range for w_unc')
    ap.add_argument("--w-ctx-range", default="", help='Uniform range for w_ctx')
    ap.add_argument("--w-pace-range", default="", help='Uniform range for w_pace')
    ap.add_argument("--w-inj-range", default="", help='Uniform range for w_inj')
    ap.add_argument("--w-blowout-range", default="", help='Uniform range for w_blowout')

    ap.add_argument(
        "--min-prob",
        default="",  # allow "" token to mean None
        help='Comma-separated min_prob values; use empty token for None, e.g. ",0.52,0.55"',
    )
    ap.add_argument(
        "--min-ev",
        default="",  # allow "" token to mean None
        help='Comma-separated min_ev values; use empty token for None, e.g. ",0.02,0.05"',
    )

    ap.add_argument("--min-bets", type=int, default=100, help="Skip configs with fewer graded bets")
    ap.add_argument("--out-csv", default="")

    args = ap.parse_args()

    if str(args.end).strip():
        end = dt.date.fromisoformat(str(args.end).strip())
    else:
        ds = _iter_recon_props_dates()
        if not ds:
            raise SystemExit("No recon_props_*.csv found")
        end = ds[-1]

    window = int(args.window)
    if window <= 0:
        raise SystemExit("--window must be > 0")

    start = end - dt.timedelta(days=window - 1)

    ranks = [r.strip().lower() for r in _parse_csv_list(args.rank) if r.strip()]
    p_shrinks = [_parse_float_or_none(t) for t in _parse_csv_list(args.p_shrink)]
    p_shrinks = [p for p in p_shrinks if p is not None]
    max_plus_odds = [_parse_float_or_none(t) for t in _parse_csv_list(args.max_plus_odds)]
    max_plus_odds = [m for m in max_plus_odds if m is not None]

    min_probs = [_parse_float_or_none(t) for t in _parse_csv_list(args.min_prob)]
    min_evs = [_parse_float_or_none(t) for t in _parse_csv_list(args.min_ev)]

    w_evs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_ev)]
    w_probs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_prob)]
    w_zs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_z)]
    w_uncs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_unc)]
    w_ctxs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_ctx)]
    w_paces = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_pace)]
    w_injs = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_inj)]
    w_blowouts = [_parse_float_or_none(t) for t in _parse_csv_list(args.w_blowout)]

    w_ev_range = _parse_range_or_none(args.w_ev_range)
    w_prob_range = _parse_range_or_none(args.w_prob_range)
    w_z_range = _parse_range_or_none(args.w_z_range)
    w_unc_range = _parse_range_or_none(args.w_unc_range)
    w_ctx_range = _parse_range_or_none(args.w_ctx_range)
    w_pace_range = _parse_range_or_none(args.w_pace_range)
    w_inj_range = _parse_range_or_none(args.w_inj_range)
    w_blowout_range = _parse_range_or_none(args.w_blowout_range)

    if not ranks:
        raise SystemExit("No --rank values")
    if not p_shrinks:
        raise SystemExit("No --p-shrink values")
    if not max_plus_odds:
        raise SystemExit("No --max-plus-odds values")
    if not min_probs:
        min_probs = [None]
    if not min_evs:
        min_evs = [None]

    if not w_evs:
        w_evs = [None]
    if not w_probs:
        w_probs = [None]
    if not w_zs:
        w_zs = [None]
    if not w_uncs:
        w_uncs = [None]
    if not w_ctxs:
        w_ctxs = [None]
    if not w_paces:
        w_paces = [None]
    if not w_injs:
        w_injs = [None]
    if not w_blowouts:
        w_blowouts = [None]

    import sys

    if str(TOOLS) not in sys.path:
        sys.path.insert(0, str(TOOLS))
    import backtest_top_recommendations as btr  # type: ignore

    rows: list[dict[str, Any]] = []

    for rk in ranks:
        for ps in p_shrinks:
            for mx in max_plus_odds:
                for mp in min_probs:
                    for me in min_evs:
                        if rk == "combo":
                            if str(args.combo_search) == "random":
                                n_samples = int(args.combo_samples)
                                if n_samples <= 0:
                                    raise SystemExit("--combo-samples must be > 0")
                                seed = _stable_int_seed(
                                    rk,
                                    float(ps),
                                    float(mx),
                                    ("" if mp is None else float(mp)),
                                    ("" if me is None else float(me)),
                                    base=int(args.combo_seed),
                                )
                                rng = random.Random(int(seed))
                                weight_grid = (
                                    (
                                        _sample_weight(rng, fixed_choices=w_evs, value_range=w_ev_range),
                                        _sample_weight(rng, fixed_choices=w_probs, value_range=w_prob_range),
                                        _sample_weight(rng, fixed_choices=w_zs, value_range=w_z_range),
                                        _sample_weight(rng, fixed_choices=w_uncs, value_range=w_unc_range),
                                        _sample_weight(rng, fixed_choices=w_ctxs, value_range=w_ctx_range),
                                        _sample_weight(rng, fixed_choices=w_paces, value_range=w_pace_range),
                                        _sample_weight(rng, fixed_choices=w_injs, value_range=w_inj_range),
                                        _sample_weight(rng, fixed_choices=w_blowouts, value_range=w_blowout_range),
                                    )
                                    for _ in range(n_samples)
                                )
                            else:
                                weight_grid = itertools.product(
                                    w_evs,
                                    w_probs,
                                    w_zs,
                                    w_uncs,
                                    w_ctxs,
                                    w_paces,
                                    w_injs,
                                    w_blowouts,
                                )
                        else:
                            weight_grid = [(None, None, None, None, None, None, None, None)]

                        for (
                            w_ev,
                            w_prob,
                            w_z,
                            w_unc,
                            w_ctx,
                            w_pace,
                            w_inj,
                            w_blowout,
                        ) in weight_grid:
                            summary, _ledger = btr.backtest_slate_prob(
                                start=start,
                                end=end,
                                n_game=int(args.n_game),
                                n_market=int(args.n_market),
                                scope="union",
                                max_plus_odds=float(mx),
                                smart_sim_prefix=str(args.prefix),
                                rank=str(rk),
                                p_shrink=float(ps),
                                max_per_player=int(args.max_per_player),
                                min_prob=mp,
                                min_ev=me,
                                w_ev=w_ev,
                                w_prob=w_prob,
                                w_z=w_z,
                                w_unc=w_unc,
                                w_ctx=w_ctx,
                                w_pace=w_pace,
                                w_inj=w_inj,
                                w_blowout=w_blowout,
                            )
                            s = summary or {}
                            bets = int(s.get("bets") or 0)
                            if bets < int(args.min_bets):
                                continue
                            rows.append(
                                {
                                    "start": start.isoformat(),
                                    "end": end.isoformat(),
                                    "window_days": int((end - start).days + 1),
                                    "prefix": str(args.prefix),
                                    "n_game": int(args.n_game),
                                    "n_market": int(args.n_market),
                                    "max_per_player": int(args.max_per_player),
                                    "rank": str(rk),
                                    "p_shrink": float(ps),
                                    "max_plus_odds": float(mx),
                                    "min_prob": mp,
                                    "min_ev": me,
                                    "w_ev": w_ev,
                                    "w_prob": w_prob,
                                    "w_z": w_z,
                                    "w_unc": w_unc,
                                    "w_ctx": w_ctx,
                                    "w_pace": w_pace,
                                    "w_inj": w_inj,
                                    "w_blowout": w_blowout,
                                    "bets": bets,
                                    "accuracy": s.get("accuracy"),
                                    "roi": s.get("roi"),
                                    "profit_sum": s.get("profit_sum"),
                                }
                            )

    df = pd.DataFrame(rows)
    if df is None or df.empty:
        raise SystemExit("No configs met min-bets threshold")

    df["roi"] = pd.to_numeric(df.get("roi"), errors="coerce")
    df["accuracy"] = pd.to_numeric(df.get("accuracy"), errors="coerce")
    df["profit_sum"] = pd.to_numeric(df.get("profit_sum"), errors="coerce")

    df = df.sort_values(["roi", "profit_sum", "accuracy", "bets"], ascending=[False, False, False, False])

    out_csv = Path(args.out_csv) if str(args.out_csv).strip() else (PROCESSED / f"_tune_slate_prob_{end.isoformat()}.csv")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    cols = [
        "rank",
        "p_shrink",
        "max_plus_odds",
        "min_prob",
        "min_ev",
        "w_ev",
        "w_prob",
        "w_z",
        "w_unc",
        "w_ctx",
        "w_pace",
        "w_inj",
        "w_blowout",
        "bets",
        "accuracy",
        "roi",
        "profit_sum",
    ]
    existing = [c for c in cols if c in df.columns]
    print(df[existing].head(15).to_string(index=False))
    print(f"Wrote: {out_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
