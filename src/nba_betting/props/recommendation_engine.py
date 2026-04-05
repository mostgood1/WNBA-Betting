from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _coalesce(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in {None, ""}:
            return mapping.get(key)
    return None


def canonical_market(value: Any) -> str:
    market = str(value or "").strip().lower()
    aliases = {
        "points": "pts",
        "rebounds": "reb",
        "assists": "ast",
        "3pm": "threes",
        "3ptm": "threes",
        "fg3m": "threes",
        "steals": "stl",
        "blocks": "blk",
        "turnovers": "tov",
        "turnover": "tov",
    }
    return aliases.get(market, market)


def canonical_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"O", "OVER"}:
        return "OVER"
    if side in {"U", "UNDER"}:
        return "UNDER"
    return side


@dataclass(slots=True)
class CanonicalPropCandidate:
    date: str | None
    game_key: str | None
    player_name: str | None
    team: str | None
    opponent: str | None
    team_side: str | None
    market: str
    side: str
    line: float | None
    price: float | None
    implied_prob: float | None
    model_prob: float | None
    push_prob: float | None
    ev_pct: float | None
    sim_mean: float | None
    sim_sd: float | None
    minutes: float | None
    source: str | None
    wrapper_source: str | None

    @property
    def sleeve_key(self) -> str:
        return f"{self.market}:{self.side.lower()}"


def build_canonical_prop_candidate(
    row: Mapping[str, Any],
    *,
    date: str | None = None,
    game_key: str | None = None,
    team: str | None = None,
    opponent: str | None = None,
    team_side: str | None = None,
) -> CanonicalPropCandidate:
    best = row.get("best") if isinstance(row.get("best"), Mapping) else None
    top_play = row.get("top_play") if isinstance(row.get("top_play"), Mapping) else None
    effective = best or top_play or row

    market = canonical_market(_coalesce(effective, "market", "stat", "prop_type") or _coalesce(row, "market", "stat", "prop_type"))
    side = canonical_side(_coalesce(effective, "side", "selection") or _coalesce(row, "side", "selection"))
    line = _safe_float(_coalesce(effective, "line", "market_line") or _coalesce(row, "line", "market_line"))
    price = _safe_float(_coalesce(effective, "price", "odds") or _coalesce(row, "price", "odds"))

    return CanonicalPropCandidate(
        date=date,
        game_key=game_key,
        player_name=str(_coalesce(row, "player", "player_name") or _coalesce(effective, "player", "player_name") or "").strip() or None,
        team=str(team or _coalesce(row, "team", "team_tricode") or _coalesce(effective, "team", "team_tricode") or "").strip().upper() or None,
        opponent=str(opponent or _coalesce(row, "opponent", "opp") or _coalesce(effective, "opponent", "opp") or "").strip().upper() or None,
        team_side=str(team_side or row.get("team_side") or "").strip().lower() or None,
        market=market,
        side=side,
        line=line,
        price=price,
        implied_prob=_safe_float(_coalesce(effective, "implied_prob") or _coalesce(row, "implied_prob")),
        model_prob=_safe_float(_coalesce(effective, "p_win", "model_prob", "probability", "prob_calib") or _coalesce(row, "p_win", "model_prob", "probability", "prob_calib")),
        push_prob=_safe_float(_coalesce(effective, "p_push") or _coalesce(row, "p_push")),
        ev_pct=_safe_float(_coalesce(effective, "ev_pct") or _coalesce(row, "ev_pct")),
        sim_mean=_safe_float(_coalesce(effective, "sim_mu", "mean") or _coalesce(row, "sim_mu", "mean")),
        sim_sd=_safe_float(_coalesce(effective, "sim_sd", "sd") or _coalesce(row, "sim_sd", "sd")),
        minutes=_safe_float(_coalesce(effective, "expected_minutes", "minutes_proj") or _coalesce(row, "expected_minutes", "minutes_proj")),
        source=str(_coalesce(effective, "source") or "").strip() or None,
        wrapper_source=str(_coalesce(row, "source") or "").strip() or None,
    )