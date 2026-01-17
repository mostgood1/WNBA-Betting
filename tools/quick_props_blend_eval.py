import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def _logloss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.asarray(p, dtype=float)
    y = np.asarray(y, dtype=float)
    m = np.isfinite(p) & np.isfinite(y)
    if int(m.sum()) == 0:
        return float("nan")
    pp = np.clip(p[m], 1e-6, 1.0 - 1e-6)
    yy = y[m]
    return float(np.mean(-(yy * np.log(pp) + (1.0 - yy) * np.log(1.0 - pp))))


def _iter_dates(start: datetime, end: datetime):
    n = int((end.date() - start.date()).days)
    for i in range(n + 1):
        yield (start + timedelta(days=i)).date().isoformat()


def _actual_val(r: pd.Series) -> float:
    stat = str(r.get("stat") or "").lower().strip()
    pts = r.get("pts")
    reb = r.get("reb")
    ast = r.get("ast")
    threes = r.get("threes")
    pra = r.get("pra")
    if stat == "pts":
        return pts
    if stat == "reb":
        return reb
    if stat == "ast":
        return ast
    if stat in ("threes", "3pm", "3pt"):
        return threes
    if stat == "pra":
        return pra
    if stat == "pr":
        return (pts if pd.notna(pts) else np.nan) + (reb if pd.notna(reb) else np.nan)
    if stat == "pa":
        return (pts if pd.notna(pts) else np.nan) + (ast if pd.notna(ast) else np.nan)
    if stat == "ra":
        return (reb if pd.notna(reb) else np.nan) + (ast if pd.notna(ast) else np.nan)
    return np.nan


def main() -> int:
    ap = argparse.ArgumentParser(description="Quick backtest for props blend: model_prob vs market implied_prob")
    ap.add_argument("--end", type=str, default="2026-01-16")
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--step", type=float, default=0.05)
    ap.add_argument("--by-stat", action="store_true", help="Print logloss by stat")
    args = ap.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d")
    start = end - timedelta(days=int(args.days))

    parts = []
    used_days = 0

    for ds in _iter_dates(start, end):
        pe = f"data/processed/props_edges_{ds}.csv"
        pa = f"data/processed/props_actuals_{ds}.csv"
        if not (os.path.exists(pe) and os.path.exists(pa)):
            continue
        try:
            edges = pd.read_csv(pe)
            act = pd.read_csv(pa)
        except Exception:
            continue
        if edges.empty or act.empty:
            continue

        edges = edges.copy()
        act = act.copy()
        edges["player_id"] = pd.to_numeric(edges.get("player_id"), errors="coerce")
        act["player_id"] = pd.to_numeric(act.get("player_id"), errors="coerce")
        m = edges.merge(act, on=["player_id"], how="inner")
        if m.empty:
            continue

        m["actual"] = m.apply(_actual_val, axis=1)
        m["line"] = pd.to_numeric(m.get("line"), errors="coerce")
        m["model_prob"] = pd.to_numeric(m.get("model_prob"), errors="coerce")
        m["implied_prob"] = pd.to_numeric(m.get("implied_prob"), errors="coerce")

        side = m.get("side").astype(str).str.upper().str.strip()
        over_win = m["actual"] > m["line"]
        under_win = m["actual"] < m["line"]
        push = m["actual"] == m["line"]
        y = np.where(side == "OVER", over_win, np.where(side == "UNDER", under_win, np.nan))
        m["y"] = y

        keep = (
            np.isfinite(m["model_prob"].to_numpy(float))
            & np.isfinite(m["implied_prob"].to_numpy(float))
            & np.isfinite(m["line"].to_numpy(float))
            & np.isfinite(m["actual"].to_numpy(float))
            & (~push)
            & pd.notna(m["y"])
        )
        m = m[keep]
        if m.empty:
            continue

        parts.append(m[["stat", "model_prob", "implied_prob", "y"]])
        used_days += 1

    if not parts:
        print("NO_ROWS")
        return 0

    df = pd.concat(parts, ignore_index=True)
    y = df["y"].astype(float).to_numpy(dtype=float)
    pm = df["model_prob"].to_numpy(dtype=float)
    pi = df["implied_prob"].to_numpy(dtype=float)

    print(f"rows={len(df)} used_days={used_days} window_days={int(args.days)} end={end.date().isoformat()}")
    print(f"logloss_model={_logloss(pm, y):.6f} logloss_implied={_logloss(pi, y):.6f}")

    if bool(args.by_stat):
        tmp = df.copy()
        tmp["stat"] = tmp["stat"].astype(str).str.lower().str.strip()
        for stat, g in tmp.groupby("stat", dropna=False):
            if not stat:
                continue
            yy = g["y"].astype(float).to_numpy(dtype=float)
            ll_m = _logloss(g["model_prob"].to_numpy(dtype=float), yy)
            ll_i = _logloss(g["implied_prob"].to_numpy(dtype=float), yy)
            n = int(len(g))
            if n < 500:
                continue
            print(f"stat={stat:8s} n={n:6d} ll_model={ll_m:.4f} ll_implied={ll_i:.4f}")

    alphas = np.round(np.arange(0.0, 1.0 + 1e-9, float(args.step)), 4)
    best_a = None
    best_ll = None
    for a in alphas:
        p = a * pm + (1.0 - a) * pi
        ll = _logloss(p, y)
        if best_ll is None or ll < best_ll:
            best_ll = ll
            best_a = float(a)
    print(f"best_alpha(model_vs_market)={best_a:.3f} best_logloss={float(best_ll):.6f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
