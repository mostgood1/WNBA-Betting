from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Sequence

import numpy as np

from .player_names import normalize_player_name_key
from .teams import to_tricode


_PROP_MARKET_ALIASES = {
    "points": "pts",
    "point": "pts",
    "pts": "pts",
    "rebounds": "reb",
    "rebound": "reb",
    "reb": "reb",
    "assists": "ast",
    "assist": "ast",
    "ast": "ast",
    "3pm": "threes",
    "3pt": "threes",
    "3-pt": "threes",
    "three": "threes",
    "threes": "threes",
    "steals": "stl",
    "steal": "stl",
    "stl": "stl",
    "blocks": "blk",
    "block": "blk",
    "blk": "blk",
    "turnovers": "tov",
    "turnover": "tov",
    "tov": "tov",
    "pra": "pra",
    "pr": "pr",
    "pa": "pa",
    "ra": "ra",
    "dd": "dd",
    "td": "td",
}

_PROP_MARKET_LABELS = {
    "pts": "Points",
    "reb": "Rebounds",
    "ast": "Assists",
    "threes": "3PM",
    "stl": "Steals",
    "blk": "Blocks",
    "tov": "Turnovers",
    "pra": "PRA",
    "pr": "PR",
    "pa": "PA",
    "ra": "RA",
    "dd": "Double Double",
    "td": "Triple Double",
}


def normalize_prop_market_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return _PROP_MARKET_ALIASES.get(raw, raw)


def prop_market_label(value: Any) -> str:
    key = normalize_prop_market_key(value)
    return _PROP_MARKET_LABELS.get(key, str(value or "").strip().upper())


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(float(value))
    except Exception:
        return None


def _summary_quantile_value(value: Any) -> float | None:
    if not isinstance(value, dict):
        return None
    for key in ("p95", "p90", "p85", "p75", "p50"):
        quant = _safe_float(value.get(key))
        if quant is not None and np.isfinite(quant):
            return float(quant)
    return None


def _team_key(value: Any) -> str:
    tri = to_tricode(value)
    if tri:
        return str(tri).strip().upper()
    return str(value or "").strip().upper()


def _rounded_count_map(values: Sequence[Any]) -> dict[int, int]:
    arr = np.asarray(values, dtype=float)
    if not arr.size:
        return {}
    arr = arr[np.isfinite(arr)]
    if not arr.size:
        return {}

    rounded = np.rint(arr).astype(int)
    totals, counts = np.unique(rounded, return_counts=True)
    return {int(total): int(count) for total, count in zip(totals.tolist(), counts.tolist())}


def _distribution_count_map(raw_distribution: Any) -> dict[int, int]:
    if not isinstance(raw_distribution, dict):
        return {}

    count_map: dict[int, int] = {}
    for raw_total, raw_count in raw_distribution.items():
        total = _safe_int(raw_total)
        count = _safe_int(raw_count)
        if total is None or count is None or count <= 0:
            continue
        count_map[int(total)] = int(count)
    return count_map


def build_exact_distribution_payload(values: Sequence[Any]) -> dict[str, int] | None:
    count_map = _rounded_count_map(values)
    if not count_map:
        return None
    return {str(total): int(count) for total, count in sorted(count_map.items())}


def build_exact_ladder_payload_from_distribution(
    raw_distribution: Any,
    *,
    mean_value: Any = None,
) -> dict[str, Any] | None:
    count_map = _distribution_count_map(raw_distribution)
    if not count_map:
        return None

    sim_count = int(sum(count_map.values()))
    max_count = max(count_map.values()) if count_map else 0
    mode_candidates = sorted(total for total, count in count_map.items() if count == max_count)
    mode_total = mode_candidates[len(mode_candidates) // 2] if mode_candidates else None

    mean_num = _safe_float(mean_value)
    if mean_num is None or not np.isfinite(mean_num):
        weighted_sum = float(sum(float(total) * float(count) for total, count in count_map.items()))
        mean_num = float(weighted_sum / float(max(1, sim_count)))

    running = 0
    rows_desc: list[dict[str, Any]] = []
    for total in sorted(count_map.keys(), reverse=True):
        exact_count = int(count_map.get(total, 0))
        running += exact_count
        rows_desc.append(
            {
                "total": int(total),
                "hitCount": int(running),
                "hitProb": float(running) / float(max(1, sim_count)),
                "exactCount": int(exact_count),
                "exactProb": float(exact_count) / float(max(1, sim_count)),
            }
        )
    rows_desc.reverse()

    return {
        "simCount": int(sim_count),
        "mean": float(mean_num),
        "mode": int(mode_total) if mode_total is not None else None,
        "modeProb": (float(max_count) / float(max(1, sim_count))) if max_count else None,
        "minTotal": int(min(count_map.keys())),
        "maxTotal": int(max(count_map.keys())),
        "distribution": {str(total): int(count) for total, count in sorted(count_map.items())},
        "ladder": rows_desc,
        "ladderShape": "exact",
    }


def build_exact_ladder_payload(values: Sequence[Any]) -> dict[str, Any] | None:
    if values is None:
        return None
    arr = np.asarray(values, dtype=float)
    if not arr.size:
        return None
    arr = arr[np.isfinite(arr)]
    if not arr.size:
        return None

    return build_exact_ladder_payload_from_distribution(
        build_exact_distribution_payload(arr),
        mean_value=float(np.mean(arr)),
    )


def build_summary_estimated_ladder_payload(
    mean_value: Any,
    sd_value: Any,
    *,
    sim_count: Any = None,
    quantiles: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    mean_num = _safe_float(mean_value)
    if mean_num is None or not np.isfinite(mean_num) or mean_num < 0:
        return None
    if float(mean_num) <= 1e-9:
        sim_n = _safe_int(sim_count) or 500
        return {
            "simCount": int(sim_n),
            "mean": 0.0,
            "mode": 0,
            "modeProb": 1.0,
            "minTotal": 0,
            "maxTotal": 0,
            "ladder": [
                {
                    "total": 0,
                    "hitCount": int(sim_n),
                    "hitProb": 1.0,
                    "exactCount": int(sim_n),
                    "exactProb": 1.0,
                }
            ],
            "ladderShape": "estimated",
        }

    sd_num = _safe_float(sd_value)
    if sd_num is not None and np.isfinite(sd_num) and sd_num > 0:
        variance_num = float(sd_num) ** 2.0
    else:
        variance_num = float(max(mean_num, 1e-6))

    quant_guess = _summary_quantile_value(quantiles)
    max_total = max(
        8,
        min(
            int(
                math.ceil(
                    max(
                        float(mean_num) + (6.0 * max(float(sd_num or 0.0), 1.0)),
                        float(quant_guess or mean_num) + 8.0,
                        12.0,
                    )
                )
            ),
            120,
        ),
    )

    probs: dict[int, float] = {}
    if variance_num <= float(mean_num) + 1e-9:
        if float(mean_num) <= 0:
            probs = {0: 1.0}
        else:
            prob = math.exp(-float(mean_num))
            probs[0] = prob
            for total in range(1, max_total + 1):
                prob *= float(mean_num) / float(total)
                probs[total] = prob
    else:
        shape = (float(mean_num) ** 2.0) / max(float(variance_num) - float(mean_num), 1e-9)
        prob_success = float(shape) / float(shape + float(mean_num))
        log_prob_success = math.log(max(prob_success, 1e-12))
        log_prob_failure = math.log(max(1.0 - prob_success, 1e-12))
        for total in range(0, max_total + 1):
            log_pmf = (
                math.lgamma(float(total) + float(shape))
                - math.lgamma(float(shape))
                - math.lgamma(float(total) + 1.0)
                + (float(shape) * log_prob_success)
                + (float(total) * log_prob_failure)
            )
            probs[total] = math.exp(log_pmf)

    total_prob = float(sum(probs.values()))
    if not probs or total_prob <= 0:
        return None

    normalized = {int(total): float(prob) / total_prob for total, prob in probs.items()}
    sim_n = _safe_int(sim_count) or 500
    mode_total, mode_prob = max(normalized.items(), key=lambda item: item[1])
    running_prob = 0.0
    rows_desc: list[dict[str, Any]] = []
    for total in sorted(normalized.keys(), reverse=True):
        exact_prob = float(normalized.get(total) or 0.0)
        running_prob += exact_prob
        rows_desc.append(
            {
                "total": int(total),
                "hitCount": int(round(running_prob * float(sim_n))),
                "hitProb": float(running_prob),
                "exactCount": int(round(exact_prob * float(sim_n))),
                "exactProb": float(exact_prob),
            }
        )
    rows_desc.reverse()

    return {
        "simCount": int(sim_n),
        "mean": float(mean_num),
        "mode": int(mode_total),
        "modeProb": float(mode_prob),
        "minTotal": int(min(normalized.keys())),
        "maxTotal": int(max(normalized.keys())),
        "ladder": rows_desc,
        "ladderShape": "estimated",
    }


def _merge_market_line_side(
    line_bucket: dict[str, Any],
    side: str,
    *,
    odds_value: Any,
    prob_value: Any = None,
    ev_pct_value: Any = None,
) -> None:
    side_key = side.lower()
    next_odds = _safe_float(odds_value)
    next_prob = _safe_float(prob_value)
    next_ev = _safe_float(ev_pct_value)
    current_ev = _safe_float(line_bucket.get(f"{side_key}EvPct"))
    current_odds = _safe_float(line_bucket.get(f"{side_key}Odds"))

    should_replace = current_odds is None
    if next_ev is not None and (current_ev is None or next_ev >= current_ev):
        should_replace = True

    if not should_replace:
        return

    line_bucket[f"{side_key}Odds"] = next_odds
    if next_prob is not None:
        line_bucket[f"{side_key}Prob"] = next_prob
    if next_ev is not None or current_ev is None:
        line_bucket[f"{side_key}EvPct"] = next_ev


def build_market_lines_by_stat(
    plays: Sequence[dict[str, Any]] | None,
    market_ladders: Sequence[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[float, dict[str, Any]]] = {}

    def _line_bucket(market_key: str, line_value: float) -> dict[str, Any]:
        market_bucket = grouped.setdefault(market_key, {})
        return market_bucket.setdefault(
            float(line_value),
            {
                "stat": market_key,
                "label": prop_market_label(market_key),
                "line": float(line_value),
                "overOdds": None,
                "underOdds": None,
                "overProb": None,
                "underProb": None,
                "overEvPct": None,
                "underEvPct": None,
            },
        )

    for play in plays or []:
        if not isinstance(play, dict):
            continue
        market_key = normalize_prop_market_key(play.get("market"))
        side = str(play.get("side") or "").strip().upper()
        line_value = _safe_float(play.get("line"))
        if not market_key or side not in {"OVER", "UNDER"} or line_value is None:
            continue
        line_bucket = _line_bucket(market_key, float(line_value))
        _merge_market_line_side(
            line_bucket,
            side,
            odds_value=play.get("price"),
            prob_value=_safe_float(play.get("prob")) or _safe_float(play.get("model_prob")),
            ev_pct_value=play.get("ev_pct"),
        )

    for ladder in market_ladders or []:
        if not isinstance(ladder, dict):
            continue
        market_key = normalize_prop_market_key(ladder.get("market"))
        side = str(ladder.get("side") or "").strip().upper()
        if not market_key or side not in {"OVER", "UNDER"}:
            continue
        base_obj = ladder.get("base") if isinstance(ladder.get("base"), dict) else None
        ladder_rows = []
        if isinstance(base_obj, dict) and base_obj:
            ladder_rows.append(base_obj)
        entries = ladder.get("entries") if isinstance(ladder.get("entries"), list) else []
        ladder_rows.extend(entry for entry in entries if isinstance(entry, dict))
        for entry in ladder_rows:
            line_value = _safe_float(entry.get("line"))
            if line_value is None:
                continue
            line_bucket = _line_bucket(market_key, float(line_value))
            _merge_market_line_side(
                line_bucket,
                side,
                odds_value=entry.get("price"),
                prob_value=entry.get("prob") or entry.get("model_prob") or entry.get("implied_prob"),
                ev_pct_value=entry.get("ev_pct"),
            )

    out: dict[str, list[dict[str, Any]]] = {}
    for market_key, line_map in grouped.items():
        entries = list(line_map.values())

        def _line_priority(entry: dict[str, Any]) -> tuple[float, float]:
            deltas = []
            for odds_key in ("overOdds", "underOdds"):
                odds_value = _safe_float(entry.get(odds_key))
                if odds_value is not None:
                    deltas.append(abs(odds_value + 110.0))
            return (min(deltas) if deltas else 1e9, abs(float(entry.get("line") or 0.0)))

        entries.sort(key=_line_priority)
        normalized_entries: list[dict[str, Any]] = []
        for index, entry in enumerate(entries[:12]):
            normalized = dict(entry)
            normalized["isPrimary"] = index == 0
            normalized_entries.append(normalized)
        out[market_key] = normalized_entries
    return out


def _build_summary_prop_ladder_markets(row: dict[str, Any], sim_count: Any) -> dict[str, dict[str, Any]]:
    inferred: dict[str, dict[str, Any]] = {}
    for market_key in ("pts", "reb", "ast", "threes", "stl", "blk", "tov", "pra"):
        payload = build_summary_estimated_ladder_payload(
            row.get(f"{market_key}_mean"),
            row.get(f"{market_key}_sd"),
            sim_count=sim_count,
            quantiles=row.get(f"{market_key}_q") if isinstance(row.get(f"{market_key}_q"), dict) else None,
        )
        if not isinstance(payload, dict):
            continue
        inferred[market_key] = {
            "market": market_key,
            "label": prop_market_label(market_key),
            "simCount": _safe_int(payload.get("simCount")),
            "mean": _safe_float(payload.get("mean")),
            "mode": _safe_int(payload.get("mode")),
            "modeProb": _safe_float(payload.get("modeProb")),
            "minTotal": _safe_int(payload.get("minTotal")),
            "maxTotal": _safe_int(payload.get("maxTotal")),
            "ladder": payload.get("ladder") if isinstance(payload.get("ladder"), list) else [],
            "ladderShape": str(payload.get("ladderShape") or "estimated"),
        }
    return inferred


def load_smart_sim_prop_ladder_lookup(
    processed_dir: Path,
    date_str: str,
    *,
    prefix: str = "smart_sim",
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    prefix_s = str(prefix or "smart_sim").strip() or "smart_sim"

    def _extract_exact_markets(raw_markets: Any) -> dict[str, dict[str, Any]]:
        markets: dict[str, dict[str, Any]] = {}
        if not isinstance(raw_markets, dict):
            return markets

        for market_name, payload in raw_markets.items():
            market_key = normalize_prop_market_key(market_name)
            if not market_key or not isinstance(payload, dict):
                continue

            exact_payload = payload
            ladder_rows = payload.get("ladder") if isinstance(payload.get("ladder"), list) else []
            if not ladder_rows:
                rebuilt = build_exact_ladder_payload_from_distribution(
                    payload.get("distribution") if isinstance(payload.get("distribution"), dict) else None,
                    mean_value=payload.get("mean"),
                )
                if not isinstance(rebuilt, dict):
                    continue
                exact_payload = rebuilt
                ladder_rows = rebuilt.get("ladder") if isinstance(rebuilt.get("ladder"), list) else []
            if not ladder_rows:
                continue

            markets[market_key] = {
                "market": market_key,
                "label": prop_market_label(market_key),
                "simCount": _safe_int(exact_payload.get("simCount")),
                "mean": _safe_float(exact_payload.get("mean")),
                "mode": _safe_int(exact_payload.get("mode")),
                "modeProb": _safe_float(exact_payload.get("modeProb")),
                "minTotal": _safe_int(exact_payload.get("minTotal")),
                "maxTotal": _safe_int(exact_payload.get("maxTotal")),
                "ladder": ladder_rows,
                "ladderShape": str(exact_payload.get("ladderShape") or "exact"),
            }
        return markets

    for sim_path in sorted(Path(processed_dir).glob(f"{prefix_s}_{date_str}_*.json")):
        try:
            raw = json.loads(sim_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        players_obj = raw.get("players") if isinstance(raw, dict) else None
        if not isinstance(players_obj, dict):
            continue
        for side in ("home", "away"):
            team_tri = _team_key(raw.get(side))
            opponent_tri = _team_key(raw.get("away" if side == "home" else "home"))
            sim_count = _safe_int(raw.get("n_sims"))
            for row in players_obj.get(side) or []:
                if not isinstance(row, dict):
                    continue
                player_name = str(row.get("player_name") or "").strip()
                player_key = normalize_player_name_key(player_name, case="lower")
                if not player_key or not team_tri:
                    continue
                prop_ladders = row.get("prop_ladders") if isinstance(row.get("prop_ladders"), dict) else {}
                markets = _extract_exact_markets(prop_ladders)
                if not markets:
                    markets = _extract_exact_markets(
                        row.get("prop_distributions") if isinstance(row.get("prop_distributions"), dict) else None
                    )
                if not markets:
                    markets = _build_summary_prop_ladder_markets(row, sim_count)
                if not markets:
                    continue
                lookup[(player_key, team_tri)] = {
                    "player": player_name,
                    "player_id": _safe_int(row.get("player_id")),
                    "team": team_tri,
                    "opponent": opponent_tri,
                    "game_id": raw.get("game_id"),
                    "sourceFile": sim_path.name,
                    "markets": markets,
                }
    return lookup


def build_card_sim_ladders(
    player_name: Any,
    team: Any,
    plays: Sequence[dict[str, Any]] | None,
    market_ladders: Sequence[dict[str, Any]] | None,
    lookup: dict[tuple[str, str], dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    if not lookup:
        return []
    player_key = normalize_player_name_key(player_name, case="lower")
    team_key = _team_key(team)
    match = lookup.get((player_key, team_key))
    if match is None and player_key:
        candidates = [
            payload
            for (candidate_player, candidate_team), payload in lookup.items()
            if candidate_player == player_key and (not team_key or candidate_team == team_key)
        ]
        if len(candidates) == 1:
            match = candidates[0]
    if not isinstance(match, dict):
        return []

    market_lines = build_market_lines_by_stat(plays, market_ladders)
    out: list[dict[str, Any]] = []
    for market_key, payload in sorted((match.get("markets") or {}).items(), key=lambda item: (prop_market_label(item[0]), item[0])):
        if not isinstance(payload, dict):
            continue
        row = dict(payload)
        line_entries = market_lines.get(market_key, [])
        row["marketLinesByStat"] = line_entries
        primary_line = next((entry.get("line") for entry in line_entries if entry.get("isPrimary")), None)
        row["marketLine"] = _safe_float(primary_line)
        row["overLineCount"] = None
        row["overLineProb"] = None
        if row.get("marketLine") is not None:
            target_total = int(np.floor(float(row["marketLine"]))) + 1
            line_row = next(
                (
                    ladder_row
                    for ladder_row in row.get("ladder") or []
                    if _safe_int(ladder_row.get("total")) == int(target_total)
                ),
                None,
            )
            if isinstance(line_row, dict):
                row["overLineCount"] = _safe_int(line_row.get("hitCount"))
                row["overLineProb"] = _safe_float(line_row.get("hitProb"))
        row["player_id"] = match.get("player_id")
        row["opponent"] = match.get("opponent")
        row["game_id"] = match.get("game_id")
        row["sourceFile"] = match.get("sourceFile")
        out.append(row)
    return out