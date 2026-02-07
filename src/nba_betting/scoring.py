from __future__ import annotations

from dataclasses import dataclass
import json
import math
from typing import Any, Optional


def _num(x: Any) -> Optional[float]:
    try:
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else (hi if x > hi else x)


def _sigmoid(x: float) -> float:
    # Numerically stable sigmoid
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


@dataclass(frozen=True)
class GameScoreConfig:
    # Weights should sum to 1.0 per market group.
    w_ev: float = 0.7982
    w_edge_pts: float = 0.1465
    # For ATS/TOTAL we include EV with a smaller weight than edge.
    w_ev_non_ml: float = 0.8171
    w_price: float = 0.0364

    # Sigmoid centers/scales (tuned to typical ranges)
    # NOTE: Empirical EV/edge are much larger than early-season assumptions.
    # Widen scales to avoid sigmoid saturation (scores pinning at 97-100).
    ev_scale: float = 0.60  # EV per unit stake (median ~0.32 over last 30d)
    edge_center: float = 6.0  # points (median |edge| ~6 over last 30d)
    edge_scale: float = 6.0


@dataclass(frozen=True)
class PropScoreConfig:
    # Weights should sum to 1.0
    w_ev: float = 0.3726
    w_prob_edge: float = 0.5047
    w_price: float = 0.1227

    # Sigmoid centers/scales
    # Props EV can be extremely heavy-tailed; widen scale to reduce saturation.
    ev_scale: float = 1.00
    prob_edge_center: float = 0.05
    prob_edge_scale: float = 0.10


DEFAULT_GAME_CFG = GameScoreConfig()
DEFAULT_PROP_CFG = PropScoreConfig()


def score_game_pick_0_100(
    market: str,
    ev: Any = None,
    edge: Any = None,
    price: Any = None,
    cfg: GameScoreConfig = DEFAULT_GAME_CFG,
) -> tuple[int, dict[str, Any], str]:
    """Return (score_0_100_int, components, explain).

    Interpretation:
      - ML: score is mostly EV-driven.
      - ATS/TOTAL: score is mostly abs(edge) (points) driven.
      - price is a small modifier (regular odds score slightly higher).

    The score is deterministic and intentionally simple so it can be optimized.
    """
    m = str(market or "").upper().strip()
    evv = _num(ev)
    edgev = _num(edge)
    pricev = _num(price)

    # Price quality: 1.0 at -110, gently lower towards +/-150.
    price_quality = 0.5
    if pricev is not None and pricev != 0:
        ap = abs(float(pricev))
        # clamp range to avoid extreme penalties when missing/bad data leaks in
        ap = _clamp(ap, 90.0, 250.0)
        # 110 -> 1.0 ; 150 -> ~0.8 ; 250 -> ~0.3
        price_quality = _clamp(1.0 - ((ap - 110.0) / 200.0), 0.0, 1.0)

    # Components in [0,1]
    ev_comp = 0.5
    if evv is not None:
        ev_comp = _sigmoid(float(evv) / max(1e-9, float(cfg.ev_scale)))

    edge_comp = 0.5
    if edgev is not None:
        eabs = abs(float(edgev))
        edge_comp = _sigmoid((eabs - float(cfg.edge_center)) / max(1e-9, float(cfg.edge_scale)))

    if m == "ML":
        combined = (float(cfg.w_ev) * ev_comp) + (float(cfg.w_price) * price_quality)
        weights_sum = float(cfg.w_ev + cfg.w_price)
        combined = combined / max(1e-9, weights_sum)
        explain = (
            f"Score = 100 * ("
            f"{cfg.w_ev:.2f}*sigmoid(EV/{cfg.ev_scale:.2f}) + {cfg.w_price:.2f}*PriceQuality"
            f")"
        )
        components = {
            "market": m,
            "ev": evv,
            "edge": edgev,
            "price": pricev,
            "ev_component": ev_comp,
            "edge_component": None,
            "price_quality": price_quality,
            "combined": combined,
        }
    else:
        combined = (
            (float(cfg.w_edge_pts) * edge_comp)
            + (float(cfg.w_ev_non_ml) * ev_comp)
            + (float(cfg.w_price) * price_quality)
        )
        weights_sum = float(cfg.w_edge_pts + cfg.w_ev_non_ml + cfg.w_price)
        combined = combined / max(1e-9, weights_sum)
        explain = (
            f"Score = 100 * ("
            f"{cfg.w_edge_pts:.2f}*sigmoid((|edge_pts|-{cfg.edge_center:.1f})/{cfg.edge_scale:.1f}) + "
            f"{cfg.w_ev_non_ml:.2f}*sigmoid(EV/{cfg.ev_scale:.2f}) + {cfg.w_price:.2f}*PriceQuality"
            f")"
        )
        components = {
            "market": m,
            "ev": evv,
            "edge": edgev,
            "price": pricev,
            "ev_component": ev_comp,
            "edge_component": edge_comp,
            "price_quality": price_quality,
            "combined": combined,
        }

    score = int(round(_clamp(100.0 * float(combined), 0.0, 100.0)))
    return score, components, explain


def score_prop_pick_0_100(
    ev: Any = None,
    edge: Any = None,
    model_prob: Any = None,
    implied_prob: Any = None,
    price: Any = None,
    cfg: PropScoreConfig = DEFAULT_PROP_CFG,
) -> tuple[int, dict[str, Any], str]:
    """Return (score_0_100_int, components, explain).

    Uses three signals:
      - EV (expected return per unit stake)
      - Probability edge: model_prob - implied_prob (or `edge` if already that)
      - Price quality: slightly favors standard-ish lines (e.g. -110)

    All signals are passed through sigmoids to reduce sensitivity to outliers.
    """
    evv = _num(ev)
    edgev = _num(edge)
    mp = _num(model_prob)
    ip = _num(implied_prob)
    pricev = _num(price)

    prob_edge = None
    if edgev is not None:
        prob_edge = float(edgev)
    elif (mp is not None) and (ip is not None):
        prob_edge = float(mp) - float(ip)

    ev_comp = 0.5
    if evv is not None:
        ev_comp = _sigmoid(float(evv) / max(1e-9, float(cfg.ev_scale)))

    prob_comp = 0.5
    if prob_edge is not None:
        prob_comp = _sigmoid((float(prob_edge) - float(cfg.prob_edge_center)) / max(1e-9, float(cfg.prob_edge_scale)))

    price_quality = 0.5
    if pricev is not None and pricev != 0:
        ap = abs(float(pricev))
        ap = _clamp(ap, 90.0, 250.0)
        price_quality = _clamp(1.0 - ((ap - 110.0) / 200.0), 0.0, 1.0)

    combined = (
        float(cfg.w_ev) * ev_comp
        + float(cfg.w_prob_edge) * prob_comp
        + float(cfg.w_price) * price_quality
    )
    weights_sum = float(cfg.w_ev + cfg.w_prob_edge + cfg.w_price)
    combined = combined / max(1e-9, weights_sum)

    score = int(round(_clamp(100.0 * float(combined), 0.0, 100.0)))
    components = {
        "ev": evv,
        "edge": edgev,
        "model_prob": mp,
        "implied_prob": ip,
        "prob_edge": prob_edge,
        "ev_component": ev_comp,
        "prob_component": prob_comp,
        "price": pricev,
        "price_quality": price_quality,
        "combined": combined,
    }
    explain = (
        f"Score = 100 * ("
        f"{cfg.w_ev:.2f}*sigmoid(EV/{cfg.ev_scale:.2f}) + "
        f"{cfg.w_prob_edge:.2f}*sigmoid((prob_edge-{cfg.prob_edge_center:.2f})/{cfg.prob_edge_scale:.2f}) + "
        f"{cfg.w_price:.2f}*PriceQuality"
        f")"
    )
    return score, components, explain


def dump_components_json(components: dict[str, Any]) -> str:
    try:
        return json.dumps(components, sort_keys=True)
    except Exception:
        return "{}"
