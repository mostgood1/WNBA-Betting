import argparse
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import math

from nba_betting.sim_games import SimConfig
from nba_betting.teams import to_tricode

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"

# Basic sanity bounds to ignore clearly-bad odds rows.
# NBA spreads almost never exceed ~20 points; totals typically sit in ~170-260.
MAX_ABS_SPREAD = 20.0
MIN_TOTAL_LINE = 170.0
MAX_TOTAL_LINE = 260.0

# ATS probability blending toward market no-vig probability.
# w=1.0 => pure model; w=0.0 => pure market.
ATS_BLEND_WEIGHT = 0.25

# Totals probability blending toward market no-vig probability.
TOTALS_BLEND_WEIGHT = 0.25


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

    # Prefer tri-code join if possible (handles "LA Clippers" vs "Los Angeles Clippers" variants).
    for df in (p, o):
        if "home_team" in df.columns:
            df["home_tri"] = df["home_team"].astype(str).map(to_tricode)
        if "visitor_team" in df.columns:
            df["away_tri"] = df["visitor_team"].astype(str).map(to_tricode)

    keys_tri = [c for c in ("date_norm", "home_tri", "away_tri") if c in p.columns and c in o.columns]
    if len(keys_tri) >= 3:
        merged = p.merge(o, on=keys_tri, how="inner")
    else:
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
    if abs(ln) > MAX_ABS_SPREAD:
        return None
    # Calibrated ATS cover probability
    cfg = SimConfig()
    threshold = -ln  # home covers if margin > -home_spread
    mu_ats = float(cfg.ats_scale) * float(pm) + float(cfg.ats_bias)
    z = (threshold - mu_ats) / max(1e-6, float(cfg.sd_margin_ats))
    p_home_cover = float(max(1e-6, min(1 - 1e-6, 1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))))

    # Market no-vig implied probability (fallback -110/-110)
    home_price = _get_num(row, ["home_spread_price", "home_spread_price_y", "home_spread_price_x"]) or -110.0
    away_price = _get_num(row, ["away_spread_price", "away_spread_price_y", "away_spread_price_x"]) or -110.0
    ph = american_to_prob(home_price)
    pa = american_to_prob(away_price)
    if ph is None or pa is None or (ph + pa) <= 0:
        return None
    p_mkt_home_nv = ph / (ph + pa)

    # Blend model probability toward market baseline to reduce overconfidence.
    w = float(max(0.0, min(1.0, ATS_BLEND_WEIGHT)))
    p_home_cover_blend = (w * p_home_cover) + ((1.0 - w) * p_mkt_home_nv)

    # Choose side by edge vs market; compute EV
    edge_home = p_home_cover_blend - p_mkt_home_nv
    edge_away = (1 - p_home_cover_blend) - (1 - p_mkt_home_nv)
    pick_side = "HOME" if edge_home >= edge_away else "AWAY"
    price = home_price if pick_side == "HOME" else away_price

    def _ev(prob: float, american: float) -> float:
        a = float(american)
        p = float(prob)
        if a > 0:
            return p * (a / 100.0) - (1 - p)
        return p * (100.0 / abs(a)) - (1 - p)

    p_pick = p_home_cover_blend if pick_side == "HOME" else (1 - p_home_cover_blend)
    ev = _ev(p_pick, price)

    edge_pick = float(edge_home) if pick_side == "HOME" else float(edge_away)

    # Keep point-edge for display but score via probability edge + confidence
    point_edge = pm - (-ln)
    # Score is a proxy for confidence; include positive EV signal.
    score = float(min(1.0, (max(0.0, ev) * 2.0) + (abs(edge_pick) * 6.0) + (abs(p_pick - 0.5) * 2.0)))

    return {
        "market": "spread",
        "pick": pick_side,
        "score": float(score),
        "edge": float(point_edge),
        "edge_prob": float(edge_pick),
        "prob_model": float(p_pick),
        "prob_mkt": float(p_mkt_home_nv if pick_side == "HOME" else (1 - p_mkt_home_nv)),
        "ev": float(ev),
        "odds": float(price),
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
    if (ln < MIN_TOTAL_LINE) or (ln > MAX_TOTAL_LINE):
        return None
    pt = float(row.get("totals"))
    diff = pt - ln

    # Model probability of OVER using Normal(total ~ N(mu=pt, sd=cfg.sd_total))
    cfg = SimConfig()
    z = (ln - pt) / max(1e-6, float(cfg.sd_total))
    p_over = float(max(1e-6, min(1 - 1e-6, 1.0 - 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))))
    p_under = 1.0 - p_over

    # Market no-vig implied probability (fallback -110/-110)
    over_price = _get_num(row, ["total_over_price", "total_over_price_y", "total_over_price_x"]) or -110.0
    under_price = _get_num(row, ["total_under_price", "total_under_price_y", "total_under_price_x"]) or -110.0
    po = american_to_prob(over_price)
    pu = american_to_prob(under_price)
    if po is None or pu is None or (po + pu) <= 0:
        return None
    p_mkt_over_nv = po / (po + pu)
    p_mkt_under_nv = 1.0 - p_mkt_over_nv

    # Blend model prob toward market baseline to reduce overconfidence.
    w = float(max(0.0, min(1.0, TOTALS_BLEND_WEIGHT)))
    p_over_blend = (w * p_over) + ((1.0 - w) * p_mkt_over_nv)
    p_under_blend = 1.0 - p_over_blend

    # Pick by probability edge vs market
    edge_over = p_over_blend - p_mkt_over_nv
    edge_under = p_under_blend - p_mkt_under_nv
    pick = "OVER" if edge_over >= edge_under else "UNDER"
    price = over_price if pick == "OVER" else under_price
    p_pick = p_over_blend if pick == "OVER" else p_under_blend
    edge_pick = float(edge_over) if pick == "OVER" else float(edge_under)

    def _ev(prob: float, american: float) -> float:
        a = float(american)
        p = float(prob)
        if a > 0:
            return p * (a / 100.0) - (1 - p)
        return p * (100.0 / abs(a)) - (1 - p)

    ev = _ev(p_pick, price)

    # Confidence: point distance + probability signal + positive EV signal.
    score = float(min(1.0, (max(0.0, ev) * 2.0) + (abs(edge_pick) * 6.0) + (abs(p_pick - 0.5) * 2.0)))

    return {
        "market": "total",
        "pick": pick,
        "score": score,
        "edge": float(diff),
        "edge_prob": float(edge_pick),
        "prob_model": float(p_pick),
        "prob_mkt": float(p_mkt_over_nv if pick == "OVER" else p_mkt_under_nv),
        "ev": float(ev),
        "odds": float(price),
    }


def main():
    ap = argparse.ArgumentParser(description="Recommend daily high-confidence picks across ML/ATS/OU")
    ap.add_argument("--date", type=str, required=True, help="YYYY-MM-DD slate date")
    ap.add_argument("--topN", type=int, default=10, help="Max picks per market type")
    ap.add_argument("--minScore", type=float, default=0.15, help="Minimum confidence score threshold")
    ap.add_argument("--minAtsEdge", type=float, default=0.05, help="Minimum ATS probability edge vs market (no-vig)")
    ap.add_argument("--minAtsEV", type=float, default=0.00, help="Minimum ATS expected value (ROI per $1)")
    ap.add_argument("--atsBlend", type=float, default=0.25, help="Blend weight for ATS prob: w*model + (1-w)*market")
    ap.add_argument("--minTotalEdge", type=float, default=0.02, help="Minimum total probability edge vs market (no-vig)")
    ap.add_argument("--minTotalEV", type=float, default=0.00, help="Minimum total expected value (ROI per $1)")
    ap.add_argument("--totalsBlend", type=float, default=0.10, help="Blend weight for totals prob: w*model + (1-w)*market")
    ap.add_argument("--out", type=str, help="Optional output CSV path")
    args = ap.parse_args()

    global ATS_BLEND_WEIGHT
    try:
        ATS_BLEND_WEIGHT = float(args.atsBlend)
    except Exception:
        ATS_BLEND_WEIGHT = 0.25

    global TOTALS_BLEND_WEIGHT
    try:
        TOTALS_BLEND_WEIGHT = float(args.totalsBlend)
    except Exception:
        TOTALS_BLEND_WEIGHT = 0.25

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
            try:
                edge_prob = float(ats.get("edge_prob") or 0.0)
            except Exception:
                edge_prob = 0.0
            try:
                ev = float(ats.get("ev") or 0.0)
            except Exception:
                ev = 0.0
            if edge_prob >= float(args.minAtsEdge) and ev >= float(args.minAtsEV):
                picks.append({**meta, **ats})
        # Totals
        ou = score_totals(row)
        if ou and ou["score"] >= args.minScore:
            try:
                edge_prob = float(ou.get("edge_prob") or 0.0)
            except Exception:
                edge_prob = 0.0
            try:
                ev = float(ou.get("ev") or 0.0)
            except Exception:
                ev = 0.0
            if edge_prob >= float(args.minTotalEdge) and ev >= float(args.minTotalEV):
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
