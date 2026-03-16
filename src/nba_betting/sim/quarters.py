from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ..config import paths


@dataclass
class TeamContext:
    team: str
    pace: float  # possessions per game
    off_rating: float  # points per 100 possessions
    def_rating: float  # points allowed per 100 possessions
    injuries_out: int = 0
    back_to_back: bool = False
    # Pregame-known schedule context. Convention: 0 means played yesterday (B2B),
    # 1 means played 2 days ago, 2 means played 3 days ago, 3 means 4+ days rest.
    rest_days: Optional[int] = None
    # Additional schedule density signal (pregame-known): number of games played in
    # the last 3 days (excluding today). Range typically 0..3.
    games_last_3d: Optional[int] = None
    form_7: Optional[float] = None  # recent offense performance delta
    form_30: Optional[float] = None


@dataclass
class GameInputs:
    date: str
    home: TeamContext
    away: TeamContext
    market_total: Optional[float] = None
    market_home_spread: Optional[float] = None
    # Optional tuned blend weights. If None, defaults can be loaded from
    # data/processed/quarters_blend_weights.json.
    blend_total_market_w: Optional[float] = None
    blend_margin_market_w: Optional[float] = None


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


_DEFAULT_BLEND_TOTAL_W = 0.7
_DEFAULT_BLEND_MARGIN_W = 0.95


_QUARTERS_CALIBRATION_CACHE: Optional[Dict[str, Any]] = None

_TOTALS_CALIBRATION_INDEX: Optional[list[tuple[pd.Timestamp, Any]]] = None
_TOTALS_CALIBRATION_CACHE: dict[str, Optional[Dict[str, Any]]] = {}


def _clamp(x: Any, lo: float, hi: float) -> float:
    try:
        v = float(x)
        if not np.isfinite(v):
            return 0.0
        return float(max(lo, min(hi, v)))
    except Exception:
        return 0.0


def _load_totals_calibration_for_date(date_str: str) -> Optional[Dict[str, Any]]:
    """Load the most recent totals calibration JSON on/before (date-1).

    Looks for data/processed/calibration_totals_YYYY-MM-DD.json.
    """
    try:
        target = pd.to_datetime(date_str).normalize()
    except Exception:
        return None
    key = str(target.date())
    if key in _TOTALS_CALIBRATION_CACHE:
        return _TOTALS_CALIBRATION_CACHE[key]

    # Calibrate using info known before games start: use <= (date - 1 day)
    cutoff = target - pd.Timedelta(days=1)

    global _TOTALS_CALIBRATION_INDEX
    if _TOTALS_CALIBRATION_INDEX is None:
        idx: list[tuple[pd.Timestamp, Any]] = []
        try:
            for fp in paths.data_processed.glob("calibration_totals_*.json"):
                name = fp.name
                # calibration_totals_YYYY-MM-DD.json
                ds = name.replace("calibration_totals_", "").replace(".json", "")
                try:
                    dt = pd.to_datetime(ds).normalize()
                except Exception:
                    continue
                idx.append((dt, fp))
        except Exception:
            idx = []
        _TOTALS_CALIBRATION_INDEX = sorted(idx, key=lambda t: t[0])

    best_fp = None
    try:
        for dt, fp in _TOTALS_CALIBRATION_INDEX or []:
            if dt <= cutoff:
                best_fp = fp
            else:
                break
    except Exception:
        best_fp = None

    if best_fp is None:
        _TOTALS_CALIBRATION_CACHE[key] = None
        return None

    try:
        import json

        obj = json.loads(best_fp.read_text(encoding="utf-8"))
        _TOTALS_CALIBRATION_CACHE[key] = obj if isinstance(obj, dict) else None
        return _TOTALS_CALIBRATION_CACHE[key]
    except Exception:
        _TOTALS_CALIBRATION_CACHE[key] = None
        return None


def _apply_totals_calibration(
    date_str: str,
    home_tri: str,
    away_tri: str,
    home_mu: float,
    away_mu: float,
) -> tuple[float, float, dict[str, float]]:
    """Apply rolling bias corrections learned from recent reconciliation.

    Returns (home_mu, away_mu, quarter_biases).
    Biases are additive in points.
    """
    cal = _load_totals_calibration_for_date(date_str) or {}

    # Quarter total biases (per-quarter additive corrections)
    q_biases: dict[str, float] = {}
    try:
        g = cal.get("global") if isinstance(cal, dict) else None
        if isinstance(g, dict):
            qb = g.get("quarters")
            if isinstance(qb, dict):
                for k, v in qb.items():
                    q_biases[str(k)] = _clamp(v, -6.0, 6.0)
            # Optional: smart-sim learned quarter biases (actual - pred) from smart_sim_quarter_eval
            sqb = g.get("sim_quarters")
            if isinstance(sqb, dict):
                for k, v in sqb.items():
                    kk = str(k)
                    combined = float(q_biases.get(kk, 0.0)) + _clamp(v, -6.0, 6.0)
                    q_biases[kk] = _clamp(combined, -6.0, 6.0)

            # Ensure quarter biases redistribute (sum ~ 0) rather than shifting the full-game total.
            # Full-game bias is handled by game_total_bias/sim_game_total_bias below.
            keys = [f"q{i}" for i in range(1, 5) if f"q{i}" in q_biases]
            if len(keys) == 4:
                mean_b = float(sum(float(q_biases[k]) for k in keys) / 4.0)
                for k in keys:
                    q_biases[k] = _clamp(float(q_biases[k]) - mean_b, -6.0, 6.0)
    except Exception:
        q_biases = {}

    # Team point biases (additive on each team's mean)
    try:
        tmap = cal.get("team") if isinstance(cal, dict) else None
        if isinstance(tmap, dict):
            if home_tri in tmap:
                home_mu += _clamp(tmap.get(home_tri), -4.0, 4.0)
            if away_tri in tmap:
                away_mu += _clamp(tmap.get(away_tri), -4.0, 4.0)
    except Exception:
        pass

    # Global game total bias (split across teams)
    try:
        g = cal.get("global") if isinstance(cal, dict) else None
        if isinstance(g, dict):
            gb = _clamp(g.get("game_total_bias", 0.0), -15.0, 15.0)
            # Optional: smart-sim global total bias (actual - pred) from smart_sim_quarter_eval
            gb += _clamp(g.get("sim_game_total_bias", 0.0), -15.0, 15.0)
            gb = _clamp(gb, -15.0, 15.0)
            home_mu += 0.5 * gb
            away_mu += 0.5 * gb
    except Exception:
        pass

    return float(home_mu), float(away_mu), q_biases


def _clamp01(x: float) -> float:
    try:
        return float(max(0.0, min(1.0, float(x))))
    except Exception:
        return 0.7


def _load_default_blend_weights() -> tuple[float, float]:
    """Return (total_w, margin_w) market blend weights.

    If data/processed/quarters_blend_weights.json exists and contains
    {"total_w": ..., "margin_w": ...}, those values are used.
    """
    global _DEFAULT_BLEND_TOTAL_W, _DEFAULT_BLEND_MARGIN_W
    try:
        fp = paths.data_processed / "quarters_blend_weights.json"
        if not fp.exists():
            return _DEFAULT_BLEND_TOTAL_W, _DEFAULT_BLEND_MARGIN_W
        import json

        obj = json.loads(fp.read_text(encoding="utf-8"))
        tw = obj.get("total_w")
        mw = obj.get("margin_w")
        if tw is not None:
            _DEFAULT_BLEND_TOTAL_W = _clamp01(float(tw))
        if mw is not None:
            _DEFAULT_BLEND_MARGIN_W = _clamp01(float(mw))
    except Exception:
        pass
    return _DEFAULT_BLEND_TOTAL_W, _DEFAULT_BLEND_MARGIN_W


def _blend_weights(inp: GameInputs) -> tuple[float, float]:
    tw, mw = _load_default_blend_weights()
    if inp.blend_total_market_w is not None:
        tw = _clamp01(float(inp.blend_total_market_w))
    if inp.blend_margin_market_w is not None:
        mw = _clamp01(float(inp.blend_margin_market_w))
    return tw, mw


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


def _load_quarters_calibration() -> Optional[Dict[str, Any]]:
    """Load optional quarters calibration artifact.

    File: data/processed/quarters_calibration.json
    Expected keys (best-effort):
      - league_split: [q1,q2,q3,q4]
      - team_split_by_tri: {"LAL": [..], ...}
      - quarter_total_sd: {"q1": float, "q2": float, ...}
    """
    global _QUARTERS_CALIBRATION_CACHE
    if _QUARTERS_CALIBRATION_CACHE is not None:
        return _QUARTERS_CALIBRATION_CACHE
    try:
        fp = paths.data_processed / "quarters_calibration.json"
        if not fp.exists():
            _QUARTERS_CALIBRATION_CACHE = None
            return None
        import json

        obj = json.loads(fp.read_text(encoding="utf-8"))
        _QUARTERS_CALIBRATION_CACHE = obj if isinstance(obj, dict) else None
        return _QUARTERS_CALIBRATION_CACHE
    except Exception:
        _QUARTERS_CALIBRATION_CACHE = None
        return None


def _norm_split(x: Any) -> Optional[List[float]]:
    try:
        arr = np.asarray(list(x), dtype=float)
        arr = np.where(np.isfinite(arr) & (arr > 0), arr, 0.0)
        s = float(arr.sum())
        if s <= 0:
            return None
        arr = arr / s
        out = [float(v) for v in arr.tolist()]
        if len(out) != 4:
            return None
        return out
    except Exception:
        return None


def _quarter_splits_for_team(team_tri: str, is_home: Optional[bool] = None) -> List[float]:
    cal = _load_quarters_calibration() or {}
    t = str(team_tri or "").strip().upper()

    # Optional: home/away-specific splits (pregame-known feature)
    try:
        if is_home is not None and isinstance(cal, dict):
            if bool(is_home):
                team_map = cal.get("team_split_home_by_tri")
                league = cal.get("league_split_home")
            else:
                team_map = cal.get("team_split_away_by_tri")
                league = cal.get("league_split_away")
            if isinstance(team_map, dict) and t in team_map:
                split = _norm_split(team_map.get(t))
                if split is not None:
                    return split
            split = _norm_split(league)
            if split is not None:
                return split
    except Exception:
        pass

    try:
        team_map = cal.get("team_split_by_tri") if isinstance(cal, dict) else None
        if isinstance(team_map, dict) and t in team_map:
            split = _norm_split(team_map.get(t))
            if split is not None:
                return split
    except Exception:
        pass
    try:
        league = cal.get("league_split") if isinstance(cal, dict) else None
        split = _norm_split(league)
        if split is not None:
            return split
    except Exception:
        pass
    return _quarter_splits()


def _target_quarter_total_sd(q: int) -> Optional[float]:
    cal = _load_quarters_calibration() or {}
    try:
        qsd = cal.get("quarter_total_sd") if isinstance(cal, dict) else None
        if not isinstance(qsd, dict):
            return None
        v = qsd.get(f"q{int(q)}")
        v = float(v)
        return v if np.isfinite(v) and v > 0 else None
    except Exception:
        return None


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

    # Optional rolling calibration from recent reconciliation
    try:
        home_mu, away_mu, q_biases = _apply_totals_calibration(inp.date, str(home.team).upper(), str(away.team).upper(), home_mu, away_mu)
    except Exception:
        q_biases = {}

    w_total, w_margin = _blend_weights(inp)

    # Align to market total/spread when provided (Bayesian blend)
    if inp.market_total is not None:
        mt = float(inp.market_total)
        # Blend: w_total market, (1-w_total) model for total
        cur_total_mu = home_mu + away_mu
        blend_total = w_total * mt + (1.0 - w_total) * cur_total_mu
        scale = blend_total / max(1e-6, cur_total_mu)
        home_mu *= scale
        away_mu *= scale
    cur_total_mu = home_mu + away_mu
    margin_mu = home_mu - away_mu
    if inp.market_home_spread is not None:
        ms = float(inp.market_home_spread)
        # Blend margin to market spread
        # home_spread convention: negative means home is favorite; market expects home margin = -home_spread
        target_margin_mu = w_margin * (-ms) + (1.0 - w_margin) * margin_mu
        # IMPORTANT: apply the target margin to team means while preserving the total.
        # Otherwise the simulation stays near a coin-flip even for large market spreads.
        home_mu = 0.5 * (cur_total_mu + target_margin_mu)
        away_mu = 0.5 * (cur_total_mu - target_margin_mu)

        # Keep team means in a sane range and re-balance to preserve the total.
        MIN_TEAM_PTS = 60.0
        if home_mu < MIN_TEAM_PTS:
            home_mu = MIN_TEAM_PTS
            away_mu = cur_total_mu - home_mu
        if away_mu < MIN_TEAM_PTS:
            away_mu = MIN_TEAM_PTS
            home_mu = cur_total_mu - away_mu
        margin_mu = home_mu - away_mu

    # Quarter splits for mean points (team-specific when calibrated)
    home_splits = _quarter_splits_for_team(home.team, is_home=True)
    away_splits = _quarter_splits_for_team(away.team, is_home=False)

    # First pass: build quarter means, optionally apply quarter-total bias, then rescale
    cur_total_mu = float(home_mu + away_mu)
    q_means: list[tuple[float, float]] = []
    for qi in range(1, 5):
        h_frac = float(home_splits[qi - 1])
        a_frac = float(away_splits[qi - 1])
        h_mu_q = float(h_frac * home_mu)
        a_mu_q = float(a_frac * away_mu)
        try:
            b = q_biases.get(f"q{qi}") if isinstance(q_biases, dict) else None
            if b is not None and np.isfinite(float(b)):
                tot_q = float(h_mu_q + a_mu_q)
                if tot_q > 1e-6:
                    share_h = float(h_mu_q / tot_q)
                    h_mu_q += float(b) * share_h
                    a_mu_q += float(b) * (1.0 - share_h)
        except Exception:
            pass
        q_means.append((h_mu_q, a_mu_q))

    try:
        sum_mu = float(sum((h + a) for (h, a) in q_means))
        if sum_mu > 1e-6:
            sf = float(cur_total_mu / sum_mu)
            # keep scaling mild; market alignment is more important
            sf = float(max(0.95, min(1.05, sf)))
            q_means = [(float(h * sf), float(a * sf)) for (h, a) in q_means]
    except Exception:
        pass

    quarters: List[QuarterResult] = []
    for qi in range(1, 5):
        h_mu_q, a_mu_q = q_means[qi - 1]
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
        # Optional: scale per-team sigmas so implied quarter-total SD matches observed.
        try:
            tgt = _target_quarter_total_sd(qi)
            if tgt is not None:
                sh = float(h_sig_q)
                sa = float(a_sig_q)
                base_total_sd = float(math.sqrt(max(1e-6, (sh * sh) + (sa * sa) + 2.0 * corr_q * sh * sa)))
                if base_total_sd > 1e-6:
                    scale = float(tgt / base_total_sd)
                    scale = float(max(0.70, min(1.35, scale)))
                    h_sig_q = float(h_sig_q) * scale
                    a_sig_q = float(a_sig_q) * scale
        except Exception:
            pass

        quarters.append(
            QuarterResult(
                q=qi,
                home_pts_mu=h_mu_q,
                home_pts_sigma=h_sig_q,
                away_pts_mu=a_mu_q,
                away_pts_sigma=a_sig_q,
                corr=corr_q,
            )
        )

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


def simulate_quarters_analytic(inp: GameInputs) -> QuarterSummary:
    """Deterministic quarter scoring summary (no Monte Carlo).

    Uses the same mean/quarter-sigma model as simulate_quarters, but computes
    final total/margin variances analytically and returns normal-CDF probs.
    """
    home = inp.home
    away = inp.away

    pace = np.mean([_safe_float(home.pace, 98.0), _safe_float(away.pace, 98.0)])
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

    home_eff = _clip_rating(home_off - (away_def - LEAGUE_AVG_RATING))
    away_eff = _clip_rating(away_off - (home_def - LEAGUE_AVG_RATING))

    home_adj = _adjustments(home)
    away_adj = _adjustments(away)

    home_mu = max(70.0, (home_eff / 100.0) * pace) + home_adj
    away_mu = max(70.0, (away_eff / 100.0) * pace) + away_adj

    w_total, w_margin = _blend_weights(inp)

    if inp.market_total is not None:
        mt = float(inp.market_total)
        cur_total_mu = home_mu + away_mu
        blend_total = w_total * mt + (1.0 - w_total) * cur_total_mu
        scale = blend_total / max(1e-6, cur_total_mu)
        home_mu *= scale
        away_mu *= scale

    cur_total_mu = home_mu + away_mu
    margin_mu = home_mu - away_mu
    if inp.market_home_spread is not None:
        ms = float(inp.market_home_spread)
        target_margin_mu = w_margin * (-ms) + (1.0 - w_margin) * margin_mu
        home_mu = 0.5 * (cur_total_mu + target_margin_mu)
        away_mu = 0.5 * (cur_total_mu - target_margin_mu)

        MIN_TEAM_PTS = 60.0
        if home_mu < MIN_TEAM_PTS:
            home_mu = MIN_TEAM_PTS
            away_mu = cur_total_mu - home_mu
        if away_mu < MIN_TEAM_PTS:
            away_mu = MIN_TEAM_PTS
            home_mu = cur_total_mu - away_mu

        margin_mu = home_mu - away_mu

    home_splits = _quarter_splits_for_team(home.team, is_home=True)
    away_splits = _quarter_splits_for_team(away.team, is_home=False)
    quarters: List[QuarterResult] = []
    corr_q = 0.25
    try:
        if pace >= 100.0:
            corr_q = min(0.40, corr_q + 0.10)
    except Exception:
        pass

    # Build quarter means, optionally apply quarter-total bias, then rescale to preserve total mean
    cur_total_mu = float(home_mu + away_mu)
    q_means: list[tuple[float, float]] = []
    for qi in range(1, 5):
        h_frac = float(home_splits[qi - 1])
        a_frac = float(away_splits[qi - 1])
        h_mu_q = float(h_frac * home_mu)
        a_mu_q = float(a_frac * away_mu)
        try:
            b = q_biases.get(f"q{qi}") if isinstance(q_biases, dict) else None
            if b is not None and np.isfinite(float(b)):
                tot_q = float(h_mu_q + a_mu_q)
                if tot_q > 1e-6:
                    share_h = float(h_mu_q / tot_q)
                    h_mu_q += float(b) * share_h
                    a_mu_q += float(b) * (1.0 - share_h)
        except Exception:
            pass
        q_means.append((h_mu_q, a_mu_q))

    try:
        sum_mu = float(sum((h + a) for (h, a) in q_means))
        if sum_mu > 1e-6:
            sf = float(cur_total_mu / sum_mu)
            sf = float(max(0.95, min(1.05, sf)))
            q_means = [(float(h * sf), float(a * sf)) for (h, a) in q_means]
    except Exception:
        pass

    for qi in range(1, 5):
        h_mu_q, a_mu_q = q_means[qi - 1]
        h_sig_q = _sigma_for_quarter(h_mu_q)
        a_sig_q = _sigma_for_quarter(a_mu_q)
        try:
            stress = 0.0
            stress += (0.1 if bool(home.back_to_back) else 0.0) + (0.1 * max(0, int(home.injuries_out or 0)))
            stress += (0.1 if bool(away.back_to_back) else 0.0) + (0.1 * max(0, int(away.injuries_out or 0)))
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

        try:
            tgt = _target_quarter_total_sd(qi)
            if tgt is not None:
                sh = float(h_sig_q)
                sa = float(a_sig_q)
                base_total_sd = float(math.sqrt(max(1e-6, (sh * sh) + (sa * sa) + 2.0 * corr_q * sh * sa)))
                if base_total_sd > 1e-6:
                    scale = float(tgt / base_total_sd)
                    scale = float(max(0.70, min(1.35, scale)))
                    h_sig_q = float(h_sig_q) * scale
                    a_sig_q = float(a_sig_q) * scale
        except Exception:
            pass

        quarters.append(
            QuarterResult(
                q=qi,
                home_pts_mu=h_mu_q,
                home_pts_sigma=h_sig_q,
                away_pts_mu=a_mu_q,
                away_pts_sigma=a_sig_q,
                corr=corr_q,
            )
        )

    var_total = 0.0
    var_margin = 0.0
    for q in quarters:
        cov = float((q.corr or 0.0) * q.home_pts_sigma * q.away_pts_sigma)
        var_h = float(q.home_pts_sigma ** 2)
        var_a = float(q.away_pts_sigma ** 2)
        var_total += var_h + var_a + 2.0 * cov
        var_margin += var_h + var_a - 2.0 * cov

    final_total_mu = float(home_mu + away_mu)
    final_margin_mu = float(home_mu - away_mu)
    final_total_sigma = float(math.sqrt(max(1e-6, var_total)))
    final_margin_sigma = float(math.sqrt(max(1e-6, var_margin)))

    def _phi(z: float) -> float:
        return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))

    probs: Dict[str, float] = {}
    try:
        probs["p_home_ml"] = float(1.0 - _phi((0.0 - final_margin_mu) / max(1e-6, final_margin_sigma)))
        if inp.market_home_spread is not None:
            hs = float(inp.market_home_spread)
            probs["p_home_cover"] = float(1.0 - _phi(((-hs) - final_margin_mu) / max(1e-6, final_margin_sigma)))
            probs["p_away_cover"] = float(1.0 - probs["p_home_cover"])
        if inp.market_total is not None:
            tot = float(inp.market_total)
            probs["p_total_over"] = float(1.0 - _phi((tot - final_total_mu) / max(1e-6, final_total_sigma)))
            probs["p_total_under"] = float(1.0 - probs["p_total_over"])
    except Exception:
        pass

    evs: Dict[str, float] = {}
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
