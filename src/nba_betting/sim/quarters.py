from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class TeamContext:
    team: str
    pace: float  # possessions per game
    off_rating: float  # points per 100 possessions
    def_rating: float  # points allowed per 100 possessions
    injuries_out: int = 0
    back_to_back: bool = False
    form_7: Optional[float] = None  # recent offense performance delta
    form_30: Optional[float] = None


@dataclass
class GameInputs:
    date: str
    home: TeamContext
    away: TeamContext
    market_total: Optional[float] = None
    market_home_spread: Optional[float] = None


@dataclass
class QuarterResult:
    q: int
    home_pts_mu: float
    home_pts_sigma: float
    away_pts_mu: float
    away_pts_sigma: float
    corr: float  # correlation between home/away scoring in the quarter


@dataclass
class QuarterSummary:
    quarters: List[QuarterResult]
    final_total_mu: float
    final_total_sigma: float
    final_margin_mu: float
    final_margin_sigma: float
    probs: Dict[str, float]
    evs: Dict[str, float]


def _safe_float(x, default=None):
    try:
        v = float(x)
        if np.isfinite(v):
            return v
        return default
    except Exception:
        return default


def _adjustments(ctx: TeamContext) -> float:
    """Compute a simple offense adjustment scalar from injuries/b2b/recent form."""
    adj = 0.0
    try:
        adj -= 0.5 * max(0, int(ctx.injuries_out or 0))  # half-point penalty per key out
        if ctx.back_to_back:
            adj -= 0.8
        if ctx.form_7 is not None:
            adj += 0.5 * _safe_float(ctx.form_7, 0.0)
        if ctx.form_30 is not None:
            adj += 0.25 * _safe_float(ctx.form_30, 0.0)
    except Exception:
        pass
    return adj


def _quarter_splits() -> List[float]:
    """Baseline fraction of total points per quarter (sums to 1)."""
    # Slightly higher early scoring variance, typical distribution.
    return [0.245, 0.245, 0.255, 0.255]


def _sigma_for_quarter(mu: float) -> float:
    """Heuristic dispersion: larger for early quarters, shrinks slightly later."""
    # Empirical: quarter points SD around 6-10; tie to mean gently.
    base = max(6.0, min(10.0, 0.9 * math.sqrt(max(1.0, mu))))
    return base


def simulate_quarters(inp: GameInputs, n_samples: int = 5000) -> QuarterSummary:
    """Simulate quarter scoring paths and compute ML/ATS/TOTAL probabilities/EVs.

    Relies on game-level pace and ratings to estimate total/margin, then splits by quarter.
    Market inputs optional; EVs computed when odds present; otherwise probabilities only.
    """
    home = inp.home
    away = inp.away

    # Estimate possessions and base total from pace and ratings
    pace = np.mean([_safe_float(home.pace, 98.0), _safe_float(away.pace, 98.0)])
    # Schedule- and lineup-sensitive pace tweak: mild drag on B2B and injuries
    try:
        b2b_drag = 0.0
        if bool(home.back_to_back):
            b2b_drag += 1.0
        if bool(away.back_to_back):
            b2b_drag += 1.0
        inj_drag = 0.3 * max(0, int(home.injuries_out or 0)) + 0.3 * max(0, int(away.injuries_out or 0))
        pace = max(90.0, pace - b2b_drag - inj_drag)
    except Exception:
        pass
    # Convert ratings to expected points per game.
    # NOTE: def_rating is points allowed per 100 possessions (lower is better defense).
    # We adjust offense by opponent defense relative to league-average; the prior
    # implementation subtracted full opponent defensive PPG and unintentionally
    # floored most team means to ~80.
    LEAGUE_AVG_RATING = 112.0

    def _clip_rating(x: float, lo: float = 95.0, hi: float = 130.0) -> float:
        try:
            return float(max(lo, min(hi, x)))
        except Exception:
            return float(x)

    home_off = _safe_float(home.off_rating, LEAGUE_AVG_RATING)
    away_off = _safe_float(away.off_rating, LEAGUE_AVG_RATING)
    home_def = _safe_float(home.def_rating, LEAGUE_AVG_RATING)
    away_def = _safe_float(away.def_rating, LEAGUE_AVG_RATING)

    # Expected offensive efficiency vs opponent defense
    home_eff = _clip_rating(home_off - (away_def - LEAGUE_AVG_RATING))
    away_eff = _clip_rating(away_off - (home_def - LEAGUE_AVG_RATING))

    # Adjustments
    home_adj = _adjustments(home)
    away_adj = _adjustments(away)

    # Baseline means
    home_mu = max(70.0, (home_eff / 100.0) * pace) + home_adj
    away_mu = max(70.0, (away_eff / 100.0) * pace) + away_adj

    # Align to market total/spread when provided (Bayesian blend)
    if inp.market_total is not None:
        mt = float(inp.market_total)
        # Blend: 70% market, 30% model for total
        cur_total_mu = home_mu + away_mu
        blend_total = 0.7 * mt + 0.3 * cur_total_mu
        scale = blend_total / max(1e-6, cur_total_mu)
        home_mu *= scale
        away_mu *= scale
    margin_mu = home_mu - away_mu
    if inp.market_home_spread is not None:
        ms = float(inp.market_home_spread)
        # Blend margin to market spread
        margin_mu = 0.7 * (-ms) + 0.3 * margin_mu  # home spread positive => market expects margin for away

    # Quarter splits for mean points
    splits = _quarter_splits()
    quarters: List[QuarterResult] = []
    for qi, frac in enumerate(splits, start=1):
        h_mu_q = frac * home_mu
        a_mu_q = frac * away_mu
        h_sig_q = _sigma_for_quarter(h_mu_q)
        a_sig_q = _sigma_for_quarter(a_mu_q)
        # Increase dispersion with schedule/injury stress and recent form volatility
        try:
            stress = 0.0
            stress += (0.1 if bool(home.back_to_back) else 0.0) + (0.1 * max(0, int(home.injuries_out or 0)))
            stress += (0.1 if bool(away.back_to_back) else 0.0) + (0.1 * max(0, int(away.injuries_out or 0)))
            # form-based variability: use magnitude to bump sigma slightly
            for f in [home.form_7, home.form_30, away.form_7, away.form_30]:
                try:
                    fv = abs(_safe_float(f, 0.0) or 0.0)
                    stress += 0.05 * min(3.0, fv)
                except Exception:
                    pass
            scale = 1.0 + min(0.35, stress)
            h_sig_q = float(h_sig_q) * scale
            a_sig_q = float(a_sig_q) * scale
        except Exception:
            pass
        # Correlation positive (game pace factors both teams)
        corr_q = 0.25
        try:
            # Slightly raise correlation for higher pace contexts
            if pace >= 100.0:
                corr_q = min(0.40, corr_q + 0.10)
        except Exception:
            pass
        quarters.append(QuarterResult(q=qi, home_pts_mu=h_mu_q, home_pts_sigma=h_sig_q,
                                      away_pts_mu=a_mu_q, away_pts_sigma=a_sig_q, corr=corr_q))

    # Monte Carlo for final totals/margin
    total_samples = []
    margin_samples = []
    for _ in range(min(5000, max(1000, n_samples))):
        h_sum = 0.0
        a_sum = 0.0
        for q in quarters:
            # Sample correlated normals via Cholesky; fallback to independent
            try:
                cov = np.array([[q.home_pts_sigma ** 2, q.corr * q.home_pts_sigma * q.away_pts_sigma],
                                [q.corr * q.home_pts_sigma * q.away_pts_sigma, q.away_pts_sigma ** 2]])
                L = np.linalg.cholesky(cov)
                z = np.random.normal(size=(2,))
                v = L @ z
                h = max(0.0, q.home_pts_mu + v[0])
                a = max(0.0, q.away_pts_mu + v[1])
            except Exception:
                h = np.random.normal(loc=q.home_pts_mu, scale=q.home_pts_sigma)
                a = np.random.normal(loc=q.away_pts_mu, scale=q.away_pts_sigma)
            h_sum += h
            a_sum += a
        total_samples.append(h_sum + a_sum)
        margin_samples.append(h_sum - a_sum)
    total_samples = np.array(total_samples)
    margin_samples = np.array(margin_samples)

    final_total_mu = float(np.mean(total_samples))
    final_total_sigma = float(np.std(total_samples))
    final_margin_mu = float(np.mean(margin_samples))
    final_margin_sigma = float(np.std(margin_samples))

    # Probabilities for ML/ATS/TOTAL using market lines if present
    probs: Dict[str, float] = {}
    try:
        # ML: home win probability
        probs["p_home_ml"] = float(np.mean(margin_samples > 0.0))
        # ATS: cover using market spread (home spread positive means home is underdog)
        if inp.market_home_spread is not None:
            hs = float(inp.market_home_spread)
            probs["p_home_cover"] = float(np.mean(margin_samples + hs > 0.0))
            probs["p_away_cover"] = float(np.mean(-margin_samples - hs > 0.0))
        # TOTAL Over/Under using market total
        if inp.market_total is not None:
            tot = float(inp.market_total)
            probs["p_total_over"] = float(np.mean(total_samples > tot))
            probs["p_total_under"] = float(np.mean(total_samples < tot))
    except Exception:
        pass

    # EVs when market prices are known (defaults for ATS/TOTAL if missing)
    def _ev(prob: float, amer: Optional[float]) -> Optional[float]:
        try:
            if amer is None:
                # Use -110 for spreads/totals by default
                dec = 1.909090909
            else:
                a = float(amer)
                dec = (1.0 + a / 100.0) if a > 0 else (1.0 + 100.0 / abs(a))
            return (prob * (dec - 1.0)) - ((1.0 - prob) * 1.0)
        except Exception:
            return None

    evs: Dict[str, float] = {}
    try:
        evs["ev_home_ml"] = _ev(probs.get("p_home_ml", 0.0), None)
        if inp.market_home_spread is not None:
            ph = probs.get("p_home_cover")
            pa = probs.get("p_away_cover")
            if ph is not None:
                evs["ev_home_cover"] = _ev(ph, -110.0)
            if pa is not None:
                evs["ev_away_cover"] = _ev(pa, -110.0)
        if inp.market_total is not None:
            po = probs.get("p_total_over")
            pu = probs.get("p_total_under")
            if po is not None:
                evs["ev_total_over"] = _ev(po, -110.0)
            if pu is not None:
                evs["ev_total_under"] = _ev(pu, -110.0)
    except Exception:
        pass

    return QuarterSummary(
        quarters=quarters,
        final_total_mu=final_total_mu,
        final_total_sigma=final_total_sigma,
        final_margin_mu=final_margin_mu,
        final_margin_sigma=final_margin_sigma,
        probs=probs,
        evs=evs,
    )


def sample_quarter_scores(
    quarters: List[QuarterResult],
    n_samples: int = 2000,
    rng: Optional[np.random.Generator] = None,
    round_to_int: bool = True,
) -> Tuple[np.ndarray, np.ndarray]:
    """Sample per-quarter home/away points from QuarterResult parameters.

    Returns:
      home_q, away_q: arrays of shape (n_samples, n_quarters)
    """
    if rng is None:
        rng = np.random.default_rng()
    n = int(max(1, n_samples))
    qs = list(quarters or [])
    k = len(qs)
    if k == 0:
        return np.zeros((n, 0)), np.zeros((n, 0))

    home_out = np.zeros((n, k), dtype=float)
    away_out = np.zeros((n, k), dtype=float)
    for j, q in enumerate(qs):
        mu = np.array([float(q.home_pts_mu), float(q.away_pts_mu)], dtype=float)
        sh = float(q.home_pts_sigma)
        sa = float(q.away_pts_sigma)
        corr = float(getattr(q, "corr", 0.0) or 0.0)
        corr = float(max(-0.75, min(0.75, corr)))
        cov = np.array(
            [[sh**2, corr * sh * sa], [corr * sh * sa, sa**2]],
            dtype=float,
        )
        try:
            L = np.linalg.cholesky(cov)
            z = rng.normal(size=(n, 2))
            v = z @ L.T
            x = mu[None, :] + v
        except Exception:
            x = rng.normal(loc=mu[None, :], scale=np.array([sh, sa])[None, :], size=(n, 2))
        x = np.maximum(0.0, x)
        home_out[:, j] = x[:, 0]
        away_out[:, j] = x[:, 1]

    if round_to_int:
        home_out = np.maximum(0, np.rint(home_out)).astype(int)
        away_out = np.maximum(0, np.rint(away_out)).astype(int)
    return home_out, away_out
