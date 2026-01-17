import argparse
import datetime as dt
import json
import os
from typing import Dict, List, Tuple

import pandas as pd
import requests


def american_profit(american_odds: float, win: bool, stake: float = 1.0) -> float:
    if not win:
        return -stake
    a = float(american_odds)
    if a > 0:
        return stake * (a / 100.0)
    elif a < 0:
        return stake * (100.0 / abs(a))
    return 0.0


def fetch_portfolio(date: str, method: str, limit: int, port: int) -> List[Dict]:
    base = f"http://127.0.0.1:{port}"
    url = (
        f"{base}/api/props/recommendations?date={date}&compact=1&regular_only=1"
        f"&limit={limit}&penalize_correlation=1&optimize=1&opt_alpha=0.15&portfolio_only=1"
        f"&cap_team=2&cap_market=3&corr_method={method}"
    )
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    j = r.json()
    return j.get("data", [])


def load_recon_props(date: str, base_dir: str) -> pd.DataFrame:
    p = os.path.join(base_dir, "data", "processed", f"recon_props_{date}.csv")
    if not os.path.exists(p):
        return pd.DataFrame()
    return pd.read_csv(p)


def eval_day(date: str, method: str, limit: int, port: int, base_dir: str) -> Tuple[int, int, float, float]:
    portfolio = fetch_portfolio(date, method, limit, port)
    df = load_recon_props(date, base_dir)
    if df is None or df.empty:
        return 0, 0, 0.0, 0.0
    # Normalize df
    cols = {c.lower(): c for c in df.columns}
    mcol = cols.get("market") or cols.get("stat")
    scol = cols.get("side")
    tcol = cols.get("team")
    pcol = cols.get("player")
    hcol = cols.get("hit") or cols.get("won") or cols.get("outcome")
    if not (mcol and scol and tcol and pcol and hcol):
        return 0, 0, 0.0, 0.0
    dd = df[[mcol, scol, tcol, pcol, hcol]].copy()
    dd[mcol] = dd[mcol].astype(str).str.strip().str.upper()
    dd[scol] = dd[scol].astype(str).str.strip().str.upper()
    dd[tcol] = dd[tcol].astype(str).str.strip().str.upper()
    dd[pcol] = dd[pcol].astype(str).str.strip()
    dd[hcol] = pd.to_numeric(dd[hcol], errors="coerce").fillna(0.0)
    # Build lookup
    key = lambda row: (
        str(row.get("top_play", {}).get("market") or "").strip().upper(),
        str(row.get("top_play", {}).get("side") or "").strip().upper(),
        str(row.get("team") or "").strip().upper(),
        str(row.get("player") or "").strip(),
    )
    total = 0
    wins = 0
    roi = 0.0
    for row in portfolio:
        k = key(row)
        q = dd[
            (dd[mcol] == k[0])
            & (dd[scol] == k[1])
            & (dd[tcol] == k[2])
            & (dd[pcol] == k[3])
        ]
        if q.empty:
            continue
        total += 1
        win = bool(q[hcol].iloc[0] > 0)
        price = float(row.get("top_play", {}).get("price") or 0.0)
        roi += american_profit(price, win)
        if win:
            wins += 1
    win_rate = (wins / total) if total > 0 else 0.0
    return total, wins, win_rate, roi


def main():
    ap = argparse.ArgumentParser(description="Evaluate correlation methods via recon props")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--port", type=int, default=5051)
    ap.add_argument("--end-date", type=str, default=None, help="YYYY-MM-DD (defaults to today)")
    args = ap.parse_args()
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    end_date = args.end_date or dt.date.today().isoformat()
    end = dt.date.fromisoformat(end_date)
    start = end - dt.timedelta(days=args.days - 1)
    methods = ["phi", "proxy", "off"]
    summary = {m: {"total": 0, "wins": 0, "win_rate": 0.0, "roi": 0.0, "days": 0} for m in methods}
    for m in methods:
        for i in range(args.days):
            d = (start + dt.timedelta(days=i)).isoformat()
            try:
                total, wins, win_rate, roi = eval_day(d, m, args.limit, args.port, base_dir)
                if total > 0:
                    s = summary[m]
                    s["total"] += total
                    s["wins"] += wins
                    s["roi"] += roi
                    s["days"] += 1
            except Exception as e:
                # ignore day if server or file missing
                continue
        s = summary[m]
        s["win_rate"] = (float(s["wins"]) / float(s["total"])) if s["total"] > 0 else 0.0
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
