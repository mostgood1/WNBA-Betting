from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402


_SUFFIX_TOKENS = frozenset({"JR", "SR", "II", "III", "IV", "V"})


def _default_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _name_tokens(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    if "(" in text:
        text = text.split("(", 1)[0]
    text = text.replace("-", " ")
    text = text.replace(".", "").replace("'", "").replace(",", " ")
    text = " ".join(text.split())
    if not text:
        return []
    try:
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    return [token for token in text.upper().split() if token and token not in _SUFFIX_TOKENS]


def _candidate_aliases(missing_name: str, sim_names: list[str], *, max_candidates: int = 3) -> list[dict[str, Any]]:
    missing_tokens = _name_tokens(missing_name)
    if len(missing_tokens) < 2:
        return []

    missing_first = missing_tokens[0]
    missing_last = missing_tokens[-1]
    missing_full = " ".join(missing_tokens)
    missing_short = app._short_player_key(missing_name)

    candidates: list[dict[str, Any]] = []
    for sim_name in sim_names:
        sim_tokens = _name_tokens(sim_name)
        if len(sim_tokens) < 2:
            continue

        sim_first = sim_tokens[0]
        sim_last = sim_tokens[-1]
        sim_full = " ".join(sim_tokens)
        sim_short = app._short_player_key(sim_name)

        same_last = missing_last == sim_last
        same_short = bool(missing_short and sim_short and missing_short == sim_short)
        if not same_last and not same_short:
            continue

        first_similarity = SequenceMatcher(None, missing_first, sim_first).ratio()
        full_similarity = SequenceMatcher(None, missing_full, sim_full).ratio()
        prefix_match = missing_first.startswith(sim_first) or sim_first.startswith(missing_first)
        same_first_initial = bool(missing_first and sim_first and missing_first[:1] == sim_first[:1])

        score = 0
        signals: list[str] = []
        if same_last:
            score += 45
            signals.append("same_last_name")
        if same_short:
            score += 25
            signals.append("same_short_key")
        if same_first_initial:
            score += 10
            signals.append("same_first_initial")
        if prefix_match:
            score += 15
            signals.append("first_name_prefix")
        if first_similarity >= 0.7:
            score += 15
            signals.append("first_name_similarity")
        elif first_similarity >= 0.5:
            score += 8
        if full_similarity >= 0.85:
            score += 10
            signals.append("full_name_similarity")
        elif full_similarity >= 0.7:
            score += 5

        if score < 60:
            continue

        candidates.append(
            {
                "player_name": str(sim_name or "").strip(),
                "score": int(score),
                "signals": signals,
            }
        )

    candidates.sort(key=lambda row: (-int(row.get("score") or 0), str(row.get("player_name") or "")))
    return candidates[:max_candidates]


def _load_cards_payload(date_str: str) -> dict[str, Any]:
    with app.app.test_request_context(f"/api/cards?date={date_str}"):
        response = app.api_cards()
    payload = response.get_json() if hasattr(response, "get_json") else None
    if not isinstance(payload, dict):
        raise RuntimeError("api_cards returned a non-dict payload")
    return payload


def audit_cards_payload(payload: dict[str, Any], *, date_str: str, max_candidates: int = 3) -> dict[str, Any]:
    games = payload.get("games") if isinstance(payload, dict) else []
    games = games if isinstance(games, list) else []

    issues_n = 0
    likely_aliases_n = 0
    unresolved_n = 0
    game_rows: list[dict[str, Any]] = []

    for game in games:
        if not isinstance(game, dict):
            continue

        sim = game.get("sim") if isinstance(game.get("sim"), dict) else {}
        players = sim.get("players") if isinstance(sim.get("players"), dict) else {}
        missing = sim.get("missing_prop_players") if isinstance(sim.get("missing_prop_players"), dict) else {}

        side_rows: dict[str, list[dict[str, Any]]] = {}
        for side in ("home", "away"):
            sim_rows = players.get(side) if isinstance(players.get(side), list) else []
            sim_names = [str(row.get("player_name") or "").strip() for row in sim_rows if isinstance(row, dict) and str(row.get("player_name") or "").strip()]
            missing_rows = missing.get(side) if isinstance(missing.get(side), list) else []

            out_rows: list[dict[str, Any]] = []
            for row in missing_rows:
                if not isinstance(row, dict):
                    continue
                player_name = str(row.get("player_name") or "").strip()
                if not player_name:
                    continue
                candidate_aliases = _candidate_aliases(player_name, sim_names, max_candidates=max_candidates)
                issues_n += 1
                if candidate_aliases:
                    likely_aliases_n += 1
                else:
                    unresolved_n += 1
                out_rows.append(
                    {
                        "player_name": player_name,
                        "prop_lines": row.get("prop_lines") if isinstance(row.get("prop_lines"), dict) else {},
                        "candidate_aliases": candidate_aliases,
                    }
                )

            if out_rows:
                side_rows[side] = out_rows

        if not side_rows:
            continue

        warnings = game.get("warnings") if isinstance(game.get("warnings"), list) else []
        missing_warnings = [
            str(warning)
            for warning in warnings
            if isinstance(warning, str) and warning.startswith("Players with prop lines missing from SmartSim boxscore:")
        ]
        game_rows.append(
            {
                "matchup": f"{str(game.get('away_tri') or '').strip().upper()}@{str(game.get('home_tri') or '').strip().upper()}",
                "home": str(game.get("home_tri") or "").strip().upper(),
                "away": str(game.get("away_tri") or "").strip().upper(),
                "warnings": missing_warnings,
                "issues": side_rows,
            }
        )

    return {
        "date": str(date_str or "").strip(),
        "ran_at_local": datetime.now().isoformat(timespec="seconds"),
        "games_n": len(games),
        "games_with_issues_n": len(game_rows),
        "issues_n": issues_n,
        "likely_aliases_n": likely_aliases_n,
        "unresolved_n": unresolved_n,
        "games": game_rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit api_cards prop-player misses and suggest likely SmartSim aliases")
    ap.add_argument("--date", default=_default_date(), help="YYYY-MM-DD (default: today, local time)")
    ap.add_argument("--max-candidates", type=int, default=3, help="Maximum alias suggestions per missing player")
    args = ap.parse_args()

    payload = _load_cards_payload(str(args.date).strip())
    report = audit_cards_payload(payload, date_str=str(args.date).strip(), max_candidates=max(1, int(args.max_candidates or 3)))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 2 if int(report.get("issues_n") or 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())