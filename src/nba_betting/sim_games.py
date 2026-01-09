from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
import math

import numpy as np
import pandas as pd

from .teams import normalize_team
from .config import paths

PROC = paths.data_processed


@dataclass
class SimConfig:
    sd_margin: float = 7.0            # Std dev for final margin
    sd_total: float = 12.0            # Std dev for final total
    home_adv_points: float = 2.0      # Baseline home advantage added to margin
    injury_margin_coef: float = 0.10  # Points of margin per unit injury impact diff
    injury_total_coef: float = 0.05   # Points of total per unit injury impact sum
    opp_rank_total_coef: float = -0.05  # Points of total per rank delta (better defense lowers total)
    threes_total_coef: float = 0.08   # Points added to total when both teams weak against 3s


def _read_odds(date_str: str) -> pd.DataFrame:
    p = PROC / f"game_odds_{date_str}.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p)
    for c in ("home_team","visitor_team"):
        if c in df.columns:
            df[c] = df[c].astype(str).map(lambda x: normalize_team(x))
    return df


def _read_injuries_impact(date_str: str) -> dict:
    p = PROC / f"injuries_counts_{date_str}.json"
    if not p.exists():
        return {}
    try:
        import json
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj.get("team_impact") or {}
    except Exception:
        return {}


def _read_opponent_ranks(date_str: str) -> dict:
    p = PROC / f"opponent_splits_{date_str}.json"
    if not p.exists():
        return {}
    try:
        import json
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj.get("ranks") or {}
    except Exception:
        return {}


def _norm_team(t: str) -> str:
    return normalize_team(t or "")


def _adjust_total_spread(row: pd.Series, inj_imp: dict, opp_ranks: dict, cfg: SimConfig) -> Tuple[float, float]:
    base_total = float(row.get("total") or np.nan)
    base_spread = float(row.get("home_spread") or np.nan)
    if not np.isfinite(base_total):
        # fallback: infer from ML if present or neutral default
        base_total = 225.0
    if not np.isfinite(base_spread):
        base_spread = 0.0
    home = _norm_team(row.get("home_team"))
    away = _norm_team(row.get("visitor_team"))

    # Home advantage baseline
    margin_adj = cfg.home_adv_points
    total_adj = 0.0

    # Injury impact: difference shifts margin, combined shifts total slightly
    hi = float(inj_imp.get(home, 0.0) or 0.0)
    ai = float(inj_imp.get(away, 0.0) or 0.0)
    margin_adj += cfg.injury_margin_coef * (ai - hi)  # opponent missing increases our margin
    total_adj += cfg.injury_total_coef * (hi + ai)

    # Opponent ranks: higher "pts allowed" and "threes allowed" increase total; better defense lowers
    hr = opp_ranks.get(home) or {}
    ar = opp_ranks.get(away) or {}
    # Use PTS ranks primarily
    try:
        # Lower rank number == stronger defense -> reduce total
        # Approximate delta around league-median 15
        r_home_pts = float(hr.get("pts", np.nan))
        r_away_pts = float(ar.get("pts", np.nan))
        if np.isfinite(r_home_pts) and np.isfinite(r_away_pts):
            delta_def = (15.0 - r_home_pts) + (15.0 - r_away_pts)
            total_adj += cfg.opp_rank_total_coef * delta_def
    except Exception:
        pass
    # Threes allowance synergy
    try:
        r_home_3 = float(hr.get("threes", np.nan))
        r_away_3 = float(ar.get("threes", np.nan))
        weak_3 = 0
        if np.isfinite(r_home_3) and r_home_3 >= 18:
            weak_3 += 1
        if np.isfinite(r_away_3) and r_away_3 >= 18:
            weak_3 += 1
        if weak_3 >= 2:
            total_adj += cfg.threes_total_coef * 10.0  # boost total when both weak against 3s
    except Exception:
        pass

    adj_total = base_total + total_adj
    adj_spread = base_spread + margin_adj
    return float(adj_total), float(adj_spread)


def _phi(x: float) -> float:
    # standard normal CDF
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _analytical_probs(total_line: float, spread_line: float, total_mu: float, margin_mu: float, cfg: SimConfig) -> dict:
    # Model total ~ N(total_mu, sd_total), margin ~ N(margin_mu, sd_margin)
    # Winners: P(margin > 0)
    p_home_win = 1.0 - _phi((0.0 - margin_mu) / cfg.sd_margin)
    # ATS: P(margin > spread_line)
    p_home_cover = 1.0 - _phi((spread_line - margin_mu) / cfg.sd_margin)
    # TOTAL: P(total > total_line)
    p_total_over = 1.0 - _phi((total_line - total_mu) / cfg.sd_total)
    # And unders/complements
    return {
        "p_home_win": float(p_home_win),
        "p_away_win": float(1.0 - p_home_win),
        "p_home_cover": float(p_home_cover),
        "p_away_cover": float(1.0 - p_home_cover),
        "p_total_over": float(p_total_over),
        "p_total_under": float(1.0 - p_total_over),
    }


def _implied_prob_from_american(ml: float | None) -> Optional[float]:
    if ml is None:
        return None
    try:
        o = float(ml)
        if np.isnan(o):
            return None
        if o > 0:
            return 100.0 / (o + 100.0)
        elif o < 0:
            return (-o) / ((-o) + 100.0)
        return None
    except Exception:
        return None


def _ev_from_prob(price: float | None, prob: float | None) -> Optional[float]:
    if price is None or prob is None:
        return None
    try:
        p = float(prob)
        a = float(price)
        if np.isnan(p) or np.isnan(a):
            return None
        # ROI per $1 stake: if hit -> profit; else -1
        profit = (a / 100.0) if a > 0 else (100.0 / abs(a))
        return float(p * profit - (1.0 - p))
    except Exception:
        return None


def simulate_games_for_date(date_str: str, cfg: Optional[SimConfig] = None) -> pd.DataFrame:
    cfg = cfg or SimConfig()
    odds = _read_odds(date_str)
    inj_imp = _read_injuries_impact(date_str)
    opp_ranks = _read_opponent_ranks(date_str)
    if odds is None or odds.empty:
        return pd.DataFrame()
    rows = []
    for _, row in odds.iterrows():
        total_line = float(row.get("total") or np.nan)
        spread_line = float(row.get("home_spread") or np.nan)
        adj_total, adj_spread = _adjust_total_spread(row, inj_imp, opp_ranks, cfg)
        # Means for margin/total
        margin_mu = adj_spread
        total_mu = adj_total
        probs = _analytical_probs(total_line, spread_line, total_mu, margin_mu, cfg)
        # EVs where prices available
        home_ml = row.get("home_ml")
        away_ml = row.get("away_ml")
        over_price = row.get("total_over_price")
        under_price = row.get("total_under_price")
        # ATS prices may exist: home_spread_price, away_spread_price
        hsp = row.get("home_spread_price")
        asp = row.get("away_spread_price")
        ev_home_ml = _ev_from_prob(home_ml, probs["p_home_win"]) if home_ml is not None else None
        ev_away_ml = _ev_from_prob(away_ml, probs["p_away_win"]) if away_ml is not None else None
        ev_total_over = _ev_from_prob(over_price, probs["p_total_over"]) if over_price is not None else None
        ev_total_under = _ev_from_prob(under_price, probs["p_total_under"]) if under_price is not None else None
        ev_home_cover = _ev_from_prob(hsp, probs["p_home_cover"]) if hsp is not None else None
        ev_away_cover = _ev_from_prob(asp, probs["p_away_cover"]) if asp is not None else None
        rows.append({
            "date": date_str,
            "home_team": _norm_team(row.get("home_team")),
            "visitor_team": _norm_team(row.get("visitor_team")),
            "total_line": float(total_line) if np.isfinite(total_line) else None,
            "spread_line": float(spread_line) if np.isfinite(spread_line) else None,
            "adj_total_mu": float(total_mu),
            "adj_margin_mu": float(margin_mu),
            **probs,
            "ev_home_ml": ev_home_ml,
            "ev_away_ml": ev_away_ml,
            "ev_total_over": ev_total_over,
            "ev_total_under": ev_total_under,
            "ev_home_cover": ev_home_cover,
            "ev_away_cover": ev_away_cover,
        })
    out = pd.DataFrame(rows)
    out_path = PROC / f"games_sim_{date_str}.csv"
    try:
        out.to_csv(out_path, index=False)
    except Exception:
        pass
    return out
