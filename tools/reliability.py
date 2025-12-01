import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def reliability_curve(probs: pd.Series, outcomes: pd.Series, bins: int = 10) -> pd.DataFrame:
    p = pd.to_numeric(probs, errors="coerce")
    y = pd.to_numeric(outcomes, errors="coerce")
    m = (~p.isna()) & (~y.isna())
    p = p[m].clip(0.001, 0.999)
    y = y[m]
    if len(p) == 0:
        return pd.DataFrame(columns=["bin","p_mean","y_rate","count","brier_mean"])
    q = pd.qcut(p, q=bins, duplicates="drop")
    df = pd.DataFrame({"p": p, "y": y, "bin": q})
    grp = df.groupby("bin")
    out = grp.agg(p_mean=("p", "mean"), y_rate=("y", "mean"), count=("p", "size")).reset_index(drop=True)
    # compute Brier score per bin: mean((p - y)^2)
    brier = grp.apply(lambda g: ((g["p"] - g["y"]) ** 2).mean())
    out["brier_mean"] = brier.to_numpy()
    out.insert(0, "bin", np.arange(1, len(out) + 1))
    return out


def collect_games(start: datetime, end: datetime):
    rows = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        pred = PROCESSED / f"predictions_{ds}.csv"
        rec = PROCESSED / f"recon_games_{ds}.csv"
        if not (pred.exists() and rec.exists()):
            continue
        try:
            p = pd.read_csv(pred); r = pd.read_csv(rec)
        except Exception:
            continue
        # normalize columns
        for df in (p, r):
            for c in ("home_team","visitor_team","date"):
                if c not in df.columns and c.upper() in df.columns:
                    df[c] = df[c.upper()]
        keys = [c for c in ("date","home_team","visitor_team") if c in p.columns and c in r.columns]
        if len(keys) < 2:
            continue
        m = p.merge(r, on=keys, suffixes=("_p","_r"))
        # determine model probability column
        pcol = None
        for c in ("home_win_prob_cal","home_win_prob","prob_home_win"):
            if c in m.columns:
                pcol = c; break
        if pcol is None:
            continue
        # derive game outcome (home win = 1.0) from available columns
        if {"home_final","visitor_final"}.issubset(m.columns):
            y = (pd.to_numeric(m["home_final"], errors="coerce") > pd.to_numeric(m["visitor_final"], errors="coerce")).astype(float)
        elif {"home_pts","visitor_pts"}.issubset(m.columns):
            y = (pd.to_numeric(m["home_pts"], errors="coerce") > pd.to_numeric(m["visitor_pts"], errors="coerce")).astype(float)
        elif "winner" in m.columns:
            y = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
        else:
            continue
        rows.append(pd.DataFrame({"p": pd.to_numeric(m[pcol], errors="coerce"), "y": y}))
    if not rows:
        return None
    return pd.concat(rows, ignore_index=True)


def main():
    ap = argparse.ArgumentParser(description="Compute reliability curves for games over a date range")
    ap.add_argument("--start", type=str)
    ap.add_argument("--end", type=str)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--bins", type=int, default=10)
    args = ap.parse_args()
    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d"); end = datetime.strptime(args.end, "%Y-%m-%d")
    else:
        end = datetime.today() - timedelta(days=1)
        start = end - timedelta(days=max(1, args.days))
    games = collect_games(start, end)
    if games is None or games.empty:
        print("NO_DATA"); return 0
    curve = reliability_curve(games["p"], games["y"], bins=args.bins)
    curve["segment"] = "games"
    curve["start"] = start.date().isoformat(); curve["end"] = end.date().isoformat()
    out = PROCESSED / "reliability_games.csv"
    curve.to_csv(out, index=False)
    print(f"OK:{out}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
