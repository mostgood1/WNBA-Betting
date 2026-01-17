import os
import json
import argparse
import datetime as dt
from itertools import product
from typing import Any, Dict, List, Tuple

import pandas as pd

import backtest_portfolio as bp


def run_sweep(
    start: dt.date,
    end: dt.date,
    limits: List[int],
    corr_scales: List[float],
    cap_teams: List[int],
    cap_markets: List[int],
    regular_only: bool,
    include_baseline: bool,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    # optional baseline (no optimizer)
    if include_baseline:
        res = bp.backtest(start, end, max(limits), False, 0.0, 0, 0, regular_only)
        s = res.get("summary", {})
        rows.append({
            "optimize": False,
            "limit": max(limits),
            "corr_penalty_scale": 0.0,
            "cap_team": 0,
            "cap_market": 0,
            "bets": s.get("bets", 0),
            "resolved": s.get("resolved", 0),
            "wins": s.get("wins", 0),
            "losses": s.get("losses", 0),
            "roi": s.get("roi", 0.0),
            "hit_rate": s.get("hit_rate", 0.0),
            "brier_mean": s.get("brier_mean"),
            "logloss_mean": s.get("logloss_mean"),
        })

    for limit, scale, ct, cm in product(limits, corr_scales, cap_teams, cap_markets):
        res = bp.backtest(start, end, limit, True, scale, ct, cm, regular_only)
        s = res.get("summary", {})
        rows.append({
            "optimize": True,
            "limit": limit,
            "corr_penalty_scale": scale,
            "cap_team": ct,
            "cap_market": cm,
            "bets": s.get("bets", 0),
            "resolved": s.get("resolved", 0),
            "wins": s.get("wins", 0),
            "losses": s.get("losses", 0),
            "roi": s.get("roi", 0.0),
            "hit_rate": s.get("hit_rate", 0.0),
            "brier_mean": s.get("brier_mean"),
            "logloss_mean": s.get("logloss_mean"),
        })

    df = pd.DataFrame(rows)
    df_sorted = df.sort_values(by=["roi", "hit_rate"], ascending=[False, False]).reset_index(drop=True)
    summary = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "n_configs": len(rows),
        "top": df_sorted.head(10).to_dict(orient="records"),
    }
    return df_sorted, summary


def parse_float_list(s: str) -> List[float]:
    return [float(x) for x in s.split(",") if x.strip() != ""]


def parse_int_list(s: str) -> List[int]:
    return [int(x) for x in s.split(",") if x.strip() != ""]


def main():
    ap = argparse.ArgumentParser(description="Sweep optimizer parameters for props portfolio backtests")
    ap.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limits", type=str, default="10,12")
    ap.add_argument("--corr-scales", type=str, default="0.0,0.8,1.2")
    ap.add_argument("--cap-teams", type=str, default="1,2")
    ap.add_argument("--cap-markets", type=str, default="3")
    ap.add_argument("--regular-only", action="store_true")
    ap.add_argument("--no-baseline", action="store_true")
    ap.add_argument("--outdir", type=str, default=os.path.join(bp.PROCESSED_DIR, "backtests"))
    args = ap.parse_args()

    if args.start and args.end:
        start = dt.date.fromisoformat(args.start)
        end = dt.date.fromisoformat(args.end)
    else:
        end = dt.date.today()
        start = end - dt.timedelta(days=args.days)

    limits = parse_int_list(args.limits)
    corr_scales = parse_float_list(args.corr_scales)
    cap_teams = parse_int_list(args.cap_teams)
    cap_markets = parse_int_list(args.cap_markets)

    df, summ = run_sweep(
        start,
        end,
        limits,
        corr_scales,
        cap_teams,
        cap_markets,
        regular_only=args.regular_only,
        include_baseline=(not args.no_baseline),
    )

    os.makedirs(args.outdir, exist_ok=True)
    stamp = f"{start.isoformat()}_{end.isoformat()}"
    csv_path = os.path.join(args.outdir, f"optimizer_sweep_{stamp}.csv")
    json_path = os.path.join(args.outdir, f"optimizer_sweep_{stamp}.json")
    df.to_csv(csv_path, index=False)
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(summ, fh, indent=2)

    # Print quick summary
    print(json.dumps(summ, indent=2))


if __name__ == "__main__":
    main()
