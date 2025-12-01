import argparse
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)


def daterange(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def brier_score(probs: pd.Series, outcomes: pd.Series) -> float:
    p = pd.to_numeric(probs, errors="coerce")
    y = pd.to_numeric(outcomes, errors="coerce")
    m = (~p.isna()) & (~y.isna())
    if m.sum() == 0:
        return float("nan")
    return float(((p[m] - y[m]) ** 2).mean())


def log_loss(probs: pd.Series, outcomes: pd.Series, eps: float = 1e-6) -> float:
    p = pd.to_numeric(probs, errors="coerce").clip(eps, 1 - eps)
    y = pd.to_numeric(outcomes, errors="coerce")
    m = (~p.isna()) & (~y.isna())
    if m.sum() == 0:
        return float("nan")
    return float(-(y[m] * np.log(p[m]) + (1 - y[m]) * np.log(1 - p[m])).mean())


def mae(a: pd.Series, b: pd.Series) -> float:
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    m = (~x.isna()) & (~y.isna())
    if m.sum() == 0:
        return float("nan")
    return float((x[m] - y[m]).abs().mean())


def rmse(a: pd.Series, b: pd.Series) -> float:
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    m = (~x.isna()) & (~y.isna())
    if m.sum() == 0:
        return float("nan")
    return float(np.sqrt(((x[m] - y[m]) ** 2).mean()))


def _load_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def evaluate_games(start: datetime, end: datetime) -> dict:
    rows = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        pred = _load_csv(PROCESSED / f"predictions_{ds}.csv")
        rec = _load_csv(PROCESSED / f"recon_games_{ds}.csv")
        if pred is None or pred.empty or rec is None or rec.empty:
            continue
        # Merge on home/visitor team names when possible
        cols = set(pred.columns)
        # Prob that home wins (from predictions), actual winner from recon
        p = pred.get("home_win_prob") if "home_win_prob" in cols else pred.get("prob_home_win")
        if p is None:
            continue
        # Build outcomes (1 if home won else 0)
        # Try robust merge on matchup key when available
        try:
            pred_k = pred.copy()
            for c in ("home_team", "visitor_team", "date"):
                if c not in pred_k.columns and c.upper() in pred_k.columns:
                    pred_k[c] = pred_k[c.upper()]
            rec_k = rec.copy()
            for c in ("home_team", "visitor_team", "date"):
                if c not in rec_k.columns and c.upper() in rec_k.columns:
                    rec_k[c] = rec_k[c.upper()]
            keys = [c for c in ("date","home_team","visitor_team") if c in pred_k.columns and c in rec_k.columns]
            if len(keys) >= 2:
                m = pred_k.merge(rec_k, on=keys, suffixes=("_p","_r"))
                if "home_final" in m.columns and "visitor_final" in m.columns:
                    y = (pd.to_numeric(m["home_final"], errors="coerce") > pd.to_numeric(m["visitor_final"], errors="coerce")).astype(float)
                elif "winner" in m.columns:
                    y = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
                else:
                    continue
                rows.append({
                    "date": ds,
                    "brier": brier_score(m[p.name], y),
                    "logloss": log_loss(m[p.name], y)
                })
        except Exception:
            continue
    if not rows:
        return {"games": {"n_days": 0}}
    df = pd.DataFrame(rows)
    return {
        "games": {
            "n_days": int(df["date"].nunique()),
            "brier_mean": float(df["brier"].mean()),
            "logloss_mean": float(df["logloss"].mean()),
        }
    }


def evaluate_totals(start: datetime, end: datetime) -> dict:
    rows = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        pred = _load_csv(PROCESSED / f"predictions_{ds}.csv")
        finals = _load_csv(PROCESSED / f"finals_{ds}.csv")
        if pred is None or pred.empty or finals is None or finals.empty:
            continue
        try:
            pp = pred.copy(); ff = finals.copy()
            for c in ("home_team","visitor_team","date"):
                if c not in pp.columns and c.upper() in pp.columns:
                    pp[c] = pp[c.upper()]
                if c not in ff.columns and c.upper() in ff.columns:
                    ff[c] = ff[c.upper()]
            keys = [c for c in ("date","home_team","visitor_team") if c in pp.columns and c in ff.columns]
            if len(keys) >= 2 and "totals" in pp.columns:
                m = pp.merge(ff, on=keys, suffixes=("_p","_f"))
                if {"home_score","visitor_score"}.issubset(set(m.columns)):
                    actual_total = pd.to_numeric(m["home_score"], errors="coerce") + pd.to_numeric(m["visitor_score"], errors="coerce")
                    rows.append({
                        "date": ds,
                        "mae": mae(m["totals"], actual_total),
                        "rmse": rmse(m["totals"], actual_total),
                    })
        except Exception:
            continue
    if not rows:
        return {"totals": {"n_days": 0}}
    df = pd.DataFrame(rows)
    return {
        "totals": {
            "n_days": int(df["date"].nunique()),
            "mae_mean": float(df["mae"].mean()),
            "rmse_mean": float(df["rmse"].mean()),
        }
    }


def evaluate_pbp_markets(start: datetime, end: datetime) -> dict:
    # Uses pbp_reconcile_<date>.csv metrics when available
    rows = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        rec = _load_csv(PROCESSED / f"pbp_reconcile_{ds}.csv")
        if rec is None or rec.empty:
            continue
        r = {}
        for col in [
            "tip_brier","tip_logloss","first_basket_hit_top1","first_basket_hit_top5","early_threes_error","early_threes_brier_ge1"
        ]:
            if col in rec.columns:
                s = pd.to_numeric(rec[col], errors="coerce").dropna()
                if len(s) > 0:
                    r[col] = float(s.mean())
        if r:
            r["date"] = ds
            rows.append(r)
    if not rows:
        return {"pbp": {"n_days": 0}}
    df = pd.DataFrame(rows)
    out = {"pbp": {"n_days": int(df["date"].nunique())}}
    if "tip_brier" in df.columns:
        out["pbp"]["tip_brier_mean"] = float(df["tip_brier"].mean())
    if "tip_logloss" in df.columns:
        out["pbp"]["tip_logloss_mean"] = float(df["tip_logloss"].mean())
    if "first_basket_hit_top1" in df.columns:
        out["pbp"]["first_basket_top1_mean"] = float(df["first_basket_hit_top1"].mean())
    if "first_basket_hit_top5" in df.columns:
        out["pbp"]["first_basket_top5_mean"] = float(df["first_basket_hit_top5"].mean())
    if "early_threes_error" in df.columns:
        out["pbp"]["early_threes_mae_mean"] = float(df["early_threes_error"].abs().mean())
    if "early_threes_brier_ge1" in df.columns:
        out["pbp"]["early_threes_brier_ge1_mean"] = float(df["early_threes_brier_ge1"].mean())
    return out


def main():
    ap = argparse.ArgumentParser(description="Evaluate models over a date range using processed files")
    ap.add_argument("--start", type=str, help="YYYY-MM-DD start date")
    ap.add_argument("--end", type=str, help="YYYY-MM-DD end date")
    ap.add_argument("--days", type=int, default=30, help="If start/end not provided, evaluate the last N days (default 30)")
    args = ap.parse_args()

    if args.start and args.end:
        try:
            start = datetime.strptime(args.start, "%Y-%m-%d")
            end = datetime.strptime(args.end, "%Y-%m-%d")
        except Exception:
            print("Invalid --start/--end format; expected YYYY-MM-DD"); return 1
    else:
        end = datetime.today() - timedelta(days=1)
        start = end - timedelta(days=max(1, args.days))

    res = {}
    res.update(evaluate_games(start, end))
    res.update(evaluate_totals(start, end))
    res.update(evaluate_pbp_markets(start, end))

    # Print and write a summary CSV
    print(res)
    try:
        out = ROOT / "data" / "processed" / "metrics_eval_rollup.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        # Flatten for CSV
        flat = []
        for k, v in res.items():
            row = {"segment": k}
            row.update(v)
            flat.append(row)
        pd.DataFrame(flat).to_csv(out, index=False)
        print(f"Wrote summary -> {out}")
    except Exception as e:
        print(f"Failed to write summary: {e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
