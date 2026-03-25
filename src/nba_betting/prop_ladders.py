from __future__ import annotations

import json
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


def _team_key(value: Any) -> str:
    tri = to_tricode(value)
    if tri:
        return str(tri).strip().upper()
    return str(value or "").strip().upper()


def build_exact_ladder_payload(values: Sequence[Any]) -> dict[str, Any] | None:
    if values is None:
        return None
    arr = np.asarray(values, dtype=float)
    if not arr.size:
        return None
    arr = arr[np.isfinite(arr)]
    if not arr.size:
        return None

    rounded = np.rint(arr).astype(int)
    totals, counts = np.unique(rounded, return_counts=True)
    if not totals.size:
        return None

    sim_count = int(rounded.size)
    count_map = {int(total): int(count) for total, count in zip(totals.tolist(), counts.tolist())}
    max_count = max(count_map.values()) if count_map else 0
    mode_candidates = sorted(total for total, count in count_map.items() if count == max_count)
    mode_total = mode_candidates[len(mode_candidates) // 2] if mode_candidates else None

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
        "mean": float(np.mean(arr)),
        "mode": int(mode_total) if mode_total is not None else None,
        "modeProb": (float(max_count) / float(max(1, sim_count))) if max_count else None,
        "minTotal": int(min(count_map.keys())),
        "maxTotal": int(max(count_map.keys())),
        "ladder": rows_desc,
        "ladderShape": "exact",
    }


def build_market_lines_by_stat(plays: Sequence[dict[str, Any]] | None) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, dict[float, dict[str, Any]]] = {}
    for play in plays or []:
        if not isinstance(play, dict):
            continue
        market_key = normalize_prop_market_key(play.get("market"))
        side = str(play.get("side") or "").strip().upper()
        line_value = _safe_float(play.get("line"))
        if not market_key or side not in {"OVER", "UNDER"} or line_value is None:
            continue
        market_bucket = grouped.setdefault(market_key, {})
        line_bucket = market_bucket.setdefault(
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
        next_ev = _safe_float(play.get("ev_pct"))
        current_ev = _safe_float(line_bucket.get(f"{side.lower()}EvPct"))
        if current_ev is None or (next_ev is not None and next_ev > current_ev):
            line_bucket[f"{side.lower()}Odds"] = _safe_float(play.get("price"))
            line_bucket[f"{side.lower()}Prob"] = _safe_float(play.get("prob")) or _safe_float(play.get("model_prob"))
            line_bucket[f"{side.lower()}EvPct"] = next_ev

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
        for index, entry in enumerate(entries[:8]):
            normalized = dict(entry)
            normalized["isPrimary"] = index == 0
            normalized_entries.append(normalized)
        out[market_key] = normalized_entries
    return out


def load_smart_sim_prop_ladder_lookup(
    processed_dir: Path,
    date_str: str,
    *,
    prefix: str = "smart_sim",
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    prefix_s = str(prefix or "smart_sim").strip() or "smart_sim"
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
            for row in players_obj.get(side) or []:
                if not isinstance(row, dict):
                    continue
                player_name = str(row.get("player_name") or "").strip()
                player_key = normalize_player_name_key(player_name, case="lower")
                if not player_key or not team_tri:
                    continue
                prop_ladders = row.get("prop_ladders") if isinstance(row.get("prop_ladders"), dict) else {}
                markets: dict[str, dict[str, Any]] = {}
                for market_name, payload in prop_ladders.items():
                    market_key = normalize_prop_market_key(market_name)
                    if not market_key or not isinstance(payload, dict):
                        continue
                    ladder_rows = payload.get("ladder") if isinstance(payload.get("ladder"), list) else []
                    if not ladder_rows:
                        continue
                    markets[market_key] = {
                        "market": market_key,
                        "label": prop_market_label(market_key),
                        "simCount": _safe_int(payload.get("simCount")),
                        "mean": _safe_float(payload.get("mean")),
                        "mode": _safe_int(payload.get("mode")),
                        "modeProb": _safe_float(payload.get("modeProb")),
                        "minTotal": _safe_int(payload.get("minTotal")),
                        "maxTotal": _safe_int(payload.get("maxTotal")),
                        "ladder": ladder_rows,
                        "ladderShape": str(payload.get("ladderShape") or "exact"),
                    }
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

    market_lines = build_market_lines_by_stat(plays)
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