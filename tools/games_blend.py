import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def american_to_prob(ml: float) -> float | None:
    try:
        o = float(ml)
    except Exception:
        return None
    if np.isnan(o):
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    elif o < 0:
        return (-o) / ((-o) + 100.0)
    else:
        return None


def implied_probs(home_ml, away_ml):
    ph = american_to_prob(home_ml)
    pa = american_to_prob(away_ml)
    if ph is None or pa is None:
        return None, None, None
    s = ph + pa
    if s <= 0:
        return None, None, None
    # de-vig normalize
    return ph / s, pa / s, s


def choose_alpha_brier(p_model: np.ndarray, p_market: np.ndarray, y: np.ndarray) -> float:
    mask = (~np.isnan(p_model)) & (~np.isnan(p_market)) & (~np.isnan(y))
    if mask.sum() < 10:
        return 0.5
    pm = p_model[mask].clip(0.001, 0.999)
    mk = p_market[mask].clip(0.001, 0.999)
    yy = y[mask]
    best_a, best_loss = 0.5, np.inf
    for a in np.linspace(0.0, 1.0, 21):  # step 0.05
        pb = a * pm + (1 - a) * mk
        loss = np.mean((pb - yy) ** 2)
        if loss < best_loss:
            best_loss = loss
            best_a = a
    return float(best_a)


def collect_training(days: int, end_date: datetime):
    start_date = end_date - timedelta(days=days)
    rows = []
    for d in (start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)):
        ds = d.strftime("%Y-%m-%d")
        pred = PROCESSED / f"predictions_{ds}.csv"
        rec = PROCESSED / f"recon_games_{ds}.csv"
        odds = PROCESSED / f"game_odds_{ds}.csv"
        if not (pred.exists() and rec.exists() and odds.exists()):
            continue
        try:
            p = pd.read_csv(pred); r = pd.read_csv(rec); o = pd.read_csv(odds)
        except Exception:
            continue
        for df in (p, r, o):
            for c in ("home_team","visitor_team","date"):
                if c not in df.columns and c.upper() in df.columns:
                    df[c] = df[c].upper()
                elif c in df.columns:
                    df[c] = df[c].astype(str).str.upper()
        keys = [c for c in ("date","home_team","visitor_team") if c in p.columns and c in r.columns and c in o.columns]
        if len(keys) < 2:
            continue
        m = p.merge(r, on=keys, suffixes=("_p","_r")).merge(o, on=keys)
        pcol = None
        for c in ("home_win_prob","prob_home_win","home_win_prob_cal"):
            if c in m.columns:
                pcol = c; break
        if pcol is None:
            continue
        # outcome
        if {"home_score","visitor_score"}.issubset(m.columns):
            y = (pd.to_numeric(m["home_score"], errors="coerce") > pd.to_numeric(m["visitor_score"], errors="coerce")).astype(float)
        elif "winner" in m.columns:
            y = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
        else:
            continue
        # market probs
        if not {"home_ml","away_ml"}.issubset(m.columns):
            continue
        tmp = []
        for _, row in m.iterrows():
            ph, pa, _ = implied_probs(row.get("home_ml"), row.get("away_ml"))
            tmp.append(ph)
        mkt = pd.Series(tmp, index=m.index, dtype=float)
        rows.append(pd.DataFrame({"p_model": pd.to_numeric(m[pcol], errors="coerce"), "p_market": mkt, "y": y}))
    if not rows:
        return None
    return pd.concat(rows, ignore_index=True)


def apply_alpha_to_date(alpha: float, date_str: str, out_path: Path | None = None) -> Path | None:
    pred = PROCESSED / f"predictions_{date_str}.csv"
    odds = PROCESSED / f"game_odds_{date_str}.csv"
    if not (pred.exists() and odds.exists()):
        return None
    p = pd.read_csv(pred); o = pd.read_csv(odds)
    for df in (p, o):
        for c in ("home_team","visitor_team","date"):
            if c not in df.columns and c.upper() in df.columns:
                df[c] = df[c].upper()
            elif c in df.columns:
                df[c] = df[c].astype(str).str.upper()
    keys = [c for c in ("date","home_team","visitor_team") if c in p.columns and c in o.columns]
    if len(keys) < 2:
        return None
    m = p.merge(o, on=keys)
    pcol = None
    for c in ("home_win_prob","prob_home_win","home_win_prob_cal"):
        if c in m.columns:
            pcol = c; break
    if pcol is None:
        return None
    ph_list = []
    for _, row in m.iterrows():
        ph, pa, _ = implied_probs(row.get("home_ml"), row.get("away_ml"))
        ph_list.append(ph)
    m["p_market"] = pd.Series(ph_list, index=m.index, dtype=float)
    p_blend = (alpha * pd.to_numeric(m[pcol], errors="coerce").clip(0.001, 0.999) +
               (1 - alpha) * m["p_market"].clip(0.001, 0.999))
    m["home_win_prob_cal"] = p_blend.clip(0.001, 0.999)
    # Write back by joining to original predictions
    out = p.merge(m[keys + ["home_win_prob_cal"]], on=keys, how="left")
    out_path = out_path or pred
    out.to_csv(out_path, index=False)
    return out_path


def main():
    ap = argparse.ArgumentParser(description="Train market blend alpha and optionally apply to a date")
    ap.add_argument("--train-days", type=int, default=30, help="Days before end to train alpha")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--apply-date", type=str, help="Date YYYY-MM-DD to apply calibrated prob into predictions")
    ap.add_argument("--out", type=str, help="Optional output path when applying")
    args = ap.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d") if args.end else (datetime.today() - timedelta(days=1))
    train = collect_training(args.train_days, end)
    if train is None or train.empty:
        print("NO_TRAIN_DATA"); return 0
    alpha = choose_alpha_brier(train["p_model"].to_numpy(dtype=float), train["p_market"].to_numpy(dtype=float), train["y"].to_numpy(dtype=float))
    meta = {"alpha": float(alpha), "train_days": int(args.train_days), "end": end.date().isoformat(), "rows": int(len(train))}
    (PROCESSED / "games_blend_alpha.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    if args.apply_date:
        out_path = Path(args.out) if args.out else None
        res = apply_alpha_to_date(alpha, args.apply_date, out_path)
        if res is None:
            print(f"OK:alpha={alpha};APPLY_SKIPPED")
        else:
            print(f"OK:alpha={alpha};WROTE:{res}")
    else:
        print(f"OK:alpha={alpha}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
