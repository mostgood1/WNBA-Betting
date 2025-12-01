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
        # Primary probability (prefer calibrated column if already replaced; avoid Series truth-value ambiguity)
        p_main = pred["home_win_prob"] if "home_win_prob" in cols else (pred["prob_home_win"] if "prob_home_win" in cols else None)
        if p_main is None:
            continue
        # Raw and calibrated variants if available
        p_raw = pred.get("home_win_prob_raw")
        p_cal = pred.get("home_win_prob_cal")
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
                elif {"home_pts","visitor_pts"}.issubset(set(m.columns)):
                    y = (pd.to_numeric(m["home_pts"], errors="coerce") > pd.to_numeric(m["visitor_pts"], errors="coerce")).astype(float)
                elif "winner" in m.columns:
                    y = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
                else:
                    continue
                row = {
                    "date": ds,
                    "brier_main": brier_score(m[p_main.name], y),
                    "logloss_main": log_loss(m[p_main.name], y)
                }
                if p_raw is not None and p_raw.name in m.columns:
                    row["brier_raw"] = brier_score(m[p_raw.name], y)
                    row["logloss_raw"] = log_loss(m[p_raw.name], y)
                if p_cal is not None and p_cal.name in m.columns:
                    row["brier_cal"] = brier_score(m[p_cal.name], y)
                    row["logloss_cal"] = log_loss(m[p_cal.name], y)
                rows.append(row)
        except Exception:
            continue
    if not rows:
        return {"games": {"n_days": 0}}
    df = pd.DataFrame(rows)
    # Sharpness (variance) & entropy helper
    def _entropy(p: pd.Series, eps: float = 1e-9) -> float:
        q = pd.to_numeric(p, errors="coerce").clip(eps, 1 - eps)
        if q.empty:
            return float("nan")
        return float((-q * np.log2(q) - (1 - q) * np.log2(1 - q)).mean())

    # Expected Calibration Error (ECE) using quantile bins
    def _ece(p: pd.Series, y: pd.Series, bins: int = 10) -> float:
        pv = pd.to_numeric(p, errors="coerce").clip(0, 1)
        yv = pd.to_numeric(y, errors="coerce")
        m = (~pv.isna()) & (~yv.isna())
        pv = pv[m]; yv = yv[m]
        if len(pv) == 0:
            return float("nan")
        try:
            q = pd.qcut(pv, q=bins, duplicates="drop")
        except Exception:
            return float("nan")
        df_e = pd.DataFrame({"p": pv, "y": yv, "bin": q})
        g = df_e.groupby("bin").agg(p_mean=("p", "mean"), y_rate=("y", "mean"), count=("p", "size"))
        total = g["count"].sum()
        ece = (g["count"] * (g["p_mean"] - g["y_rate"]).abs()).sum() / total if total > 0 else float("nan")
        return float(ece)

    out = {"games": {"n_days": int(df["date"].nunique())}}
    out_g = out["games"]
    out_g["brier_mean"] = float(df["brier_main"].mean())
    out_g["logloss_mean"] = float(df["logloss_main"].mean())
    # Reconstruct merged per-day probabilities/outcomes for ECE & entropy (approximate by averaging daily values)
    # We'll reload all rows where brier_main was computed; reuse earlier merge logic by collecting again for aggregate metrics
    # For efficiency we can approximate by concatenating daily merges already reduced; skip as sample size is small.
    # Instead simply build vectors from first pass (p_main vs y) by re-merging.
    # Collect vectors
    all_p_main = []; all_y = []
    all_p_raw = []; all_p_cal = []
    for d in daterange(start, end):
        ds = d.strftime("%Y-%m-%d")
        pred = _load_csv(PROCESSED / f"predictions_{ds}.csv")
        rec = _load_csv(PROCESSED / f"recon_games_{ds}.csv")
        if pred is None or pred.empty or rec is None or rec.empty:
            continue
        pred_k = pred.copy(); rec_k = rec.copy()
        for c in ("home_team","visitor_team","date"):
            if c not in pred_k.columns and c.upper() in pred_k.columns:
                pred_k[c] = pred_k[c.upper()]
            if c not in rec_k.columns and c.upper() in rec_k.columns:
                rec_k[c] = rec_k[c.upper()]
        keys = [c for c in ("date","home_team","visitor_team") if c in pred_k.columns and c in rec_k.columns]
        if len(keys) < 2:
            continue
        m = pred_k.merge(rec_k, on=keys, suffixes=("_p","_r"))
        p_main_col = "home_win_prob" if "home_win_prob" in pred_k.columns else ("prob_home_win" if "prob_home_win" in pred_k.columns else None)
        if p_main_col is None or p_main_col not in m.columns:
            continue
        if {"home_final","visitor_final"}.issubset(m.columns):
            yv = (pd.to_numeric(m["home_final"], errors="coerce") > pd.to_numeric(m["visitor_final"], errors="coerce")).astype(float)
        elif {"home_pts","visitor_pts"}.issubset(m.columns):
            yv = (pd.to_numeric(m["home_pts"], errors="coerce") > pd.to_numeric(m["visitor_pts"], errors="coerce")).astype(float)
        elif "winner" in m.columns:
            yv = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
        else:
            continue
        all_p_main.append(pd.to_numeric(m[p_main_col], errors="coerce"))
        all_y.append(yv)
        if "home_win_prob_raw" in m.columns:
            all_p_raw.append(pd.to_numeric(m["home_win_prob_raw"], errors="coerce"))
        if "home_win_prob_cal" in m.columns:
            all_p_cal.append(pd.to_numeric(m["home_win_prob_cal"], errors="coerce"))
    if all_p_main and all_y:
        p_main_concat = pd.concat(all_p_main, ignore_index=True)
        y_concat = pd.concat(all_y, ignore_index=True)
        out_g["ece_main"] = _ece(p_main_concat, y_concat)
        out_g["entropy_main"] = _entropy(p_main_concat)
        out_g["sharpness_var_main"] = float(pd.to_numeric(p_main_concat, errors="coerce").var())
        if all_p_raw:
            p_raw_concat = pd.concat(all_p_raw, ignore_index=True)
            out_g["ece_raw"] = _ece(p_raw_concat, y_concat)
            out_g["entropy_raw"] = _entropy(p_raw_concat)
        if all_p_cal:
            p_cal_concat = pd.concat(all_p_cal, ignore_index=True)
            out_g["ece_cal"] = _ece(p_cal_concat, y_concat)
            out_g["entropy_cal"] = _entropy(p_cal_concat)
    if "brier_raw" in df.columns:
        out_g["brier_raw_mean"] = float(df["brier_raw"].mean())
        out_g["logloss_raw_mean"] = float(df.get("logloss_raw").mean())
    if "brier_cal" in df.columns:
        out_g["brier_cal_mean"] = float(df["brier_cal"].mean())
        out_g["logloss_cal_mean"] = float(df.get("logloss_cal").mean())
    return out


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
