import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

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


def reliability_adjust(p: float, curve_df: pd.DataFrame | None) -> float:
    """Map model probability to reliability-adjusted y_rate via nearest bin center.
    Expects columns: ['p_mean','y_rate'].
    """
    try:
        if curve_df is None or curve_df.empty:
            return float(p)
        # nearest by p_mean
        idx = (curve_df["p_mean"] - float(p)).abs().idxmin()
        yr = curve_df.loc[idx, "y_rate"]
        return float(yr)
    except Exception:
        return float(p)


def load_inputs(date_str: str):
    pred = PROCESSED / f"predictions_{date_str}.csv"
    odds = PROCESSED / f"game_odds_{date_str}.csv"
    clos = PROCESSED / f"closing_lines_{date_str}.csv"
    p = pd.read_csv(pred) if pred.exists() else None
    o = pd.read_csv(odds) if odds.exists() else (pd.read_csv(clos) if clos.exists() else None)
    rel = PROCESSED / "reliability_games.csv"
    r = pd.read_csv(rel) if rel.exists() else None
    return p, o, r


def normalize_join(p: pd.DataFrame, o: pd.DataFrame) -> pd.DataFrame:
    def _ensure_col(df: pd.DataFrame, col: str) -> None:
        if col not in df.columns and col.upper() in df.columns:
            df[col] = df[col.upper()]
        if col in df.columns:
            df[col] = df[col].astype(str)

    for df in (p, o):
        for c in ("home_team", "visitor_team", "date"):
            _ensure_col(df, c)

    # Normalize join keys to be resilient to casing/whitespace differences
    for df in (p, o):
        if "home_team" in df.columns:
            df["home_team_norm"] = (
                df["home_team"].astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
            )
        if "visitor_team" in df.columns:
            df["visitor_team_norm"] = (
                df["visitor_team"].astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
            )
        if "date" in df.columns:
            df["date_norm"] = df["date"].astype(str).str.strip()

    keys = [c for c in ("date_norm", "home_team_norm", "visitor_team_norm") if c in p.columns and c in o.columns]
    if len(keys) < 2:
        return pd.DataFrame()

    merged = p.merge(o, on=keys, how="inner")

    # Prefer the predictions-side team/date columns for downstream display
    for base in ("date", "home_team", "visitor_team"):
        if f"{base}_x" in merged.columns:
            merged[base] = merged[f"{base}_x"]
        elif base in merged.columns:
            merged[base] = merged[base]
    # Canonicalize duplicate columns that often occur on merge
    def _coalesce(df: pd.DataFrame, base: str) -> None:
        # Prefer odds-side value ("_y") then predictions-side ("_x") then bare
        cand = [f"{base}_y", f"{base}_x", base]
        for name in cand:
            if name in df.columns:
                df[base] = df[name]
                break
        # If both exist, drop the suffixed ones to avoid confusion
        for name in [f"{base}_y", f"{base}_x"]:
            if name in df.columns:
                try:
                    df.drop(columns=[name], inplace=True)
                except Exception:
                    pass
    for col in (
        "home_ml","away_ml","home_spread","away_spread","total",
        "spread_point","total_point",
        "home_spread_price","away_spread_price","total_over_price","total_under_price",
    ):
        _coalesce(merged, col)

    # Drop helper join columns
    merged.drop(
        columns=[
            c
            for c in (
                "date_norm",
                "home_team_norm",
                "visitor_team_norm",
                "date_x",
                "home_team_x",
                "visitor_team_x",
                "date_y",
                "home_team_y",
                "visitor_team_y",
            )
            if c in merged.columns
        ],
        inplace=True,
        errors="ignore",
    )
    return merged

def _get_num(row: pd.Series, names: list[str]) -> float | None:
    for n in names:
        if n in row.index:
            try:
                v = row.get(n)
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    continue
                return float(v)
            except Exception:
                continue
    return None



def score_moneyline(row: pd.Series, rel_curve: pd.DataFrame | None) -> dict | None:
    # Model prob
    pcol = None
    for c in ("home_win_prob_cal","home_win_prob","prob_home_win"):
        if c in row.index:
            pcol = c; break
    if pcol is None:
        return None
    p_model = float(row.get(pcol))
    # Market prob
    h_ml = _get_num(row, ["home_ml","home_ml_y","home_ml_x"])
    a_ml = _get_num(row, ["away_ml","away_ml_y","away_ml_x"])
    ph, pa, _ = implied_probs(h_ml, a_ml)
    if ph is None or pa is None:
        return None
    # Reliability adjust
    p_adj = reliability_adjust(p_model, rel_curve)
    # Agreement and edge
    edge = p_adj - ph
    # Confidence components: how far from coinflip and how much aligned edge vs market
    distance = abs(p_adj - 0.5)
    market_gap = abs(edge)
    score = 0.6 * distance + 0.4 * market_gap
    pick_side = "HOME" if p_adj >= 0.5 else "AWAY"
    implied_odds = h_ml if pick_side == "HOME" else a_ml
    return {
        "market": "moneyline",
        "pick": pick_side,
        "score": float(score),
        "edge": float(edge),
        "prob_adj": float(p_adj),
        "odds": float(implied_odds) if implied_odds is not None else np.nan,
    }


def _ats_confidence(pred_margin: float, spread_line: float) -> float:
    # Confidence by distance to line in points, normalized ~10 pts scale
    try:
        pm = float(pred_margin); ln = float(spread_line)
    except Exception:
        return float("nan")
    # Favorite vs dog adjustment: margin vs required cover threshold
    if ln < 0:
        gap = pm - abs(ln)
    else:
        gap = pm + ln
    # Normalize and cap
    return float(max(0.0, min(1.0, abs(gap) / 10.0)))


def score_ats(row: pd.Series) -> dict | None:
    if ("spread_point" not in row.index) and ("home_spread" not in row.index) and ("home_spread_x" not in row.index) and ("home_spread_y" not in row.index):
        return None
    line = _get_num(row, ["home_spread","home_spread_y","home_spread_x","spread_point","spread_point_y","spread_point_x"])
    pm_col = "spread_margin" if "spread_margin" in row.index else None
    if pm_col is None or line is None:
        return None
    pm = float(row.get(pm_col))
    ln = float(line)
    # Pick side by sign of ATS differential
    if ln < 0:
        diff = pm - abs(ln)
    else:
        diff = pm + ln
    pick_side = "HOME" if diff >= 0 else "AWAY"
    score = _ats_confidence(pm, ln)
    price_col = "home_spread_price" if pick_side == "HOME" else "away_spread_price"
    return {
        "market": "spread",
        "pick": pick_side,
        "score": float(score),
        "edge": float(diff),
        "odds": float(row.get(price_col)) if price_col in row.index else np.nan,
    }


def score_totals(row: pd.Series) -> dict | None:
    if ("total_point" not in row.index) and ("total" not in row.index) and ("total_x" not in row.index) and ("total_y" not in row.index):
        return None
    if "totals" not in row.index:
        return None
    ln_v = _get_num(row, ["total","total_y","total_x","total_point","total_point_y","total_point_x"])
    if ln_v is None:
        return None
    ln = float(ln_v)
    pt = float(row.get("totals"))
    diff = pt - ln
    pick = "OVER" if diff > 0 else "UNDER"
    # Confidence by distance in points normalized ~12 pts scale
    score = float(max(0.0, min(1.0, abs(diff) / 12.0)))
    price_col = "total_over_price" if pick == "OVER" else "total_under_price"
    return {
        "market": "total",
        "pick": pick,
        "score": score,
        "edge": float(diff),
        "odds": float(row.get(price_col)) if price_col in row.index else np.nan,
    }


def main():
    ap = argparse.ArgumentParser(description="Recommend daily high-confidence picks across ML/ATS/OU")
    ap.add_argument("--date", type=str, required=True, help="YYYY-MM-DD slate date")
    ap.add_argument("--topN", type=int, default=10, help="Max picks per market type")
    ap.add_argument("--minScore", type=float, default=0.15, help="Minimum confidence score threshold")
    ap.add_argument("--out", type=str, help="Optional output CSV path")
    args = ap.parse_args()

    try:
        dt = datetime.strptime(args.date, "%Y-%m-%d").date()
    except Exception:
        print("Invalid --date format; expected YYYY-MM-DD"); return 1

    p, o, r = load_inputs(dt.isoformat())
    if p is None or p.empty or o is None or o.empty:
        print("Missing predictions or odds for date"); return 0
    # Reliability curve only for games segment
    rel_curve = None
    if r is not None and not r.empty:
        rel_curve = r[r.get("segment") == "games"][["p_mean","y_rate"]] if "segment" in r.columns else r[["p_mean","y_rate"]]

    merged = normalize_join(p, o)
    if merged is None or merged.empty:
        print("No join between predictions and odds"); return 0

    picks = []
    for _, row in merged.iterrows():
        meta = {
            "date": row.get("date"),
            "home_team": row.get("home_team"),
            "visitor_team": row.get("visitor_team"),
        }
        # Moneyline
        ml = score_moneyline(row, rel_curve)
        if ml and ml["score"] >= args.minScore:
            picks.append({**meta, **ml})
        # ATS
        ats = score_ats(row)
        if ats and ats["score"] >= args.minScore:
            picks.append({**meta, **ats})
        # Totals
        ou = score_totals(row)
        if ou and ou["score"] >= args.minScore:
            picks.append({**meta, **ou})

    if not picks:
        print("NO_PICKS"); return 0
    df = pd.DataFrame(picks)
    # Rank within market
    df["rank"] = df.groupby("market")["score"].rank(ascending=False, method="first")
    # Keep topN per market
    keep = df[df["rank"] <= args.topN].copy()
    keep = keep.sort_values(["market","score"], ascending=[True, False])

    out_path = Path(args.out) if args.out else (PROCESSED / f"picks_{dt.isoformat()}.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    keep.to_csv(out_path, index=False)
    print({"rows": int(len(keep)), "output": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
