import argparse
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _normalize_cols(df: pd.DataFrame | None) -> pd.DataFrame | None:
    if df is None:
        return None
    if df.empty:
        return df
    rename = {}
    for c in (
        "date",
        "home_team",
        "visitor_team",
        "home_ml",
        "away_ml",
        "home_spread",
        "away_spread",
        "total",
        "home_win_prob_from_spread",
    ):
        cu = c.upper()
        if cu in df.columns and c not in df.columns:
            rename[cu] = c
    if rename:
        df = df.rename(columns=rename)
    # Standardize merge keys casing
    for c in ("home_team", "visitor_team"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.upper()
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.upper()
    return df


def _build_market_prob(df: pd.DataFrame) -> pd.Series:
    """Market home-win probability.

    Prefer moneyline-implied probability; fall back to spread-based probability if present.
    """
    p_market = pd.Series(np.nan, index=df.index, dtype=float)
    if {"home_ml", "away_ml"}.issubset(df.columns):
        ph_list = []
        for _, row in df.iterrows():
            ph, _, _ = implied_probs(row.get("home_ml"), row.get("away_ml"))
            ph_list.append(ph)
        p_market = pd.Series(ph_list, index=df.index, dtype=float)

    for c in ("home_win_prob_from_spread", "win_prob_from_spread"):
        if c in df.columns:
            fallback = pd.to_numeric(df[c], errors="coerce")
            p_market = p_market.fillna(fallback)
            break

    return p_market


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
        # Require predictions + recon; odds are optional (can fallback to market columns embedded in predictions)
        if not (pred.exists() and rec.exists()):
            continue
        try:
            p = pd.read_csv(pred); r = pd.read_csv(rec)
            o = pd.read_csv(odds) if odds.exists() else None
        except Exception:
            continue
        p = _normalize_cols(p)
        r = _normalize_cols(r)
        o = _normalize_cols(o) if o is not None else None
        # Build merge keys and join available frames
        keys_base = [c for c in ("date","home_team","visitor_team") if c in p.columns and c in r.columns]
        if len(keys_base) < 2:
            continue
        m = p.merge(r, on=keys_base, suffixes=("_p","_r"))
        if o is not None:
            keys_with_odds = [c for c in keys_base if c in o.columns]
            if len(keys_with_odds) >= 2:
                m = m.merge(o, on=keys_with_odds, how="left")
        pcol = None
        for c in ("home_win_prob","prob_home_win","home_win_prob_cal"):
            if c in m.columns:
                pcol = c; break
        if pcol is None:
            continue
        # outcome
        if {"home_score","visitor_score"}.issubset(m.columns):
            y = (pd.to_numeric(m["home_score"], errors="coerce") > pd.to_numeric(m["visitor_score"], errors="coerce")).astype(float)
        elif {"home_pts","visitor_pts"}.issubset(m.columns):
            y = (pd.to_numeric(m["home_pts"], errors="coerce") > pd.to_numeric(m["visitor_pts"], errors="coerce")).astype(float)
        elif "actual_margin" in m.columns:
            y = (pd.to_numeric(m["actual_margin"], errors="coerce") > 0).astype(float)
        elif "winner" in m.columns:
            y = (m["winner"].astype(str).str.upper() == m["home_team"].astype(str).str.upper()).astype(float)
        else:
            continue
        # market probs: prefer moneyline-implied; fall back to spread-based probability if present
        if {"home_ml", "away_ml"}.issubset(p.columns) and not {"home_ml", "away_ml"}.issubset(m.columns):
            m = m.merge(p[[*keys_base, "home_ml", "away_ml"]], on=keys_base, how="left")
        p_market = _build_market_prob(m)
        if p_market.notna().sum() < 10:
            continue
        rows.append(pd.DataFrame({"p_model": pd.to_numeric(m[pcol], errors="coerce"), "p_market": p_market, "y": y}))
    if not rows:
        return None
    return pd.concat(rows, ignore_index=True)


def apply_alpha_to_date(alpha: float, date_str: str, out_path: Path | None = None) -> Path | None:
    pred = PROCESSED / f"predictions_{date_str}.csv"
    odds = PROCESSED / f"game_odds_{date_str}.csv"
    if not pred.exists():
        return None
    p = pd.read_csv(pred)
    p = _normalize_cols(p)
    o = pd.read_csv(odds) if odds.exists() else None
    o = _normalize_cols(o) if o is not None else None
    # Merge with odds if available; otherwise work with predictions-only
    keys = [c for c in ("date","home_team","visitor_team") if c in p.columns]
    if len(keys) < 2:
        return None
    if o is not None:
        keys_odds = [c for c in keys if c in o.columns]
        if len(keys_odds) >= 2:
            m = p.merge(o, on=keys_odds, how="left")
        else:
            m = p.copy()
    else:
        m = p.copy()
    pcol = None
    for c in ("home_win_prob","prob_home_win","home_win_prob_cal"):
        if c in m.columns:
            pcol = c; break
    if pcol is None:
        return None
    # Build market probs: prefer moneyline-implied; fall back to spread-based market prob if present
    if {"home_ml", "away_ml"}.issubset(p.columns) and not {"home_ml", "away_ml"}.issubset(m.columns):
        m = m.merge(p[[*keys, "home_ml", "away_ml"]], on=keys, how="left")
    m["p_market"] = _build_market_prob(m)
    if m["p_market"].notna().sum() == 0:
        return None
    p_blend = (alpha * pd.to_numeric(m[pcol], errors="coerce").clip(0.001, 0.999) +
               (1 - alpha) * m["p_market"].clip(0.001, 0.999))
    m["home_win_prob_cal"] = p_blend.clip(0.001, 0.999)
    # Write back by joining to original predictions
    out = p.copy()
    # Remove any prior calibration columns to avoid home_win_prob_cal_x/y accumulation
    drop_cols = [c for c in out.columns if c.startswith("home_win_prob_cal")]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    cal_df = m[keys + ["home_win_prob_cal"]].drop_duplicates(subset=keys)
    out = out.merge(cal_df, on=keys, how="left")
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
