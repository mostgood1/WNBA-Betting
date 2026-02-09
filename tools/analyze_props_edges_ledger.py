from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def _agg(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    g = (
        df.groupby(keys, dropna=False)
        .agg(
            graded=("result", lambda s: s.notna().sum()),
            wins=("result", lambda s: (s == "W").sum()),
            losses=("result", lambda s: (s == "L").sum()),
            pushes=("result", lambda s: (s == "P").sum()),
            profit=("profit", "sum"),
            avg_ev=("ev", "mean") if "ev" in df.columns else ("profit", "mean"),
            avg_edge=("edge", "mean") if "edge" in df.columns else ("profit", "mean"),
            avg_price=("price", "mean") if "price" in df.columns else ("profit", "mean"),
        )
        .reset_index()
    )
    g["hit_rate"] = g.apply(lambda r: (r.wins / (r.wins + r.losses)) if (r.wins + r.losses) else np.nan, axis=1)
    g["roi_per_bet"] = g["profit"] / g["graded"].replace({0: np.nan})
    return g


def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze props_edges_backtest ledger")
    ap.add_argument("--path", type=str, required=True, help="Path to props_edges_backtest_*.csv")
    ap.add_argument("--min-graded", type=int, default=5, help="Min graded bets for group breakdown")
    ap.add_argument("--top", type=int, default=12, help="Rows to print per breakdown")
    args = ap.parse_args()

    p = Path(args.path)
    if not p.exists():
        raise SystemExit(f"Missing file: {p}")

    df = pd.read_csv(p)
    print({"path": str(p), "rows": int(len(df))})

    if "stat" in df.columns:
        print("by_stat_counts", df["stat"].value_counts(dropna=False).to_dict())

    if "profit" in df.columns and "ev" in df.columns:
        x = pd.to_numeric(df["ev"], errors="coerce")
        y = pd.to_numeric(df["profit"], errors="coerce")
        m = x.notna() & y.notna()
        if int(m.sum()) > 10:
            print("corr_profit_ev", float(x[m].corr(y[m])))

    if "profit" in df.columns and "edge" in df.columns:
        x = pd.to_numeric(df["edge"], errors="coerce")
        y = pd.to_numeric(df["profit"], errors="coerce")
        m = x.notna() & y.notna()
        if int(m.sum()) > 10:
            print("corr_profit_edge", float(x[m].corr(y[m])))

    if "stat" in df.columns:
        g = _agg(df, ["stat"]).sort_values("roi_per_bet")
        print("\nROI by stat (worst):")
        print(g.head(int(args.top)).to_string(index=False))

    if all(c in df.columns for c in ("stat", "bookmaker")):
        g = _agg(df, ["stat", "bookmaker"])
        g = g[g["graded"] >= int(args.min_graded)].sort_values("roi_per_bet")
        print("\nROI by stat+bookmaker (worst, min graded):")
        print(g.head(int(args.top)).to_string(index=False))

    if "bookmaker" in df.columns:
        g = _agg(df, ["bookmaker"]).sort_values("roi_per_bet")
        print("\nROI by bookmaker (worst):")
        print(g.head(int(args.top)).to_string(index=False))

    if "price" in df.columns:
        s = pd.to_numeric(df["price"], errors="coerce")
        buckets = pd.cut(s, bins=[-1000, -300, -200, -150, -120, -110, -105, 105, 110, 120, 150, 200, 300, 1000])
        g = df.assign(price_bucket=buckets)
        g2 = _agg(g, ["price_bucket"]).sort_values("roi_per_bet")
        g2 = g2[g2["graded"] >= int(args.min_graded)]
        print("\nROI by price bucket (worst, min graded):")
        print(g2.head(int(args.top)).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
