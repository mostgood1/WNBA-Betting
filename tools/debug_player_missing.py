from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure repo root (where app.py lives) is importable.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import app  # noqa: E402


def present(roster, name: str) -> bool:
    if not isinstance(roster, (set, list, tuple)):
        return False
    tgt = name.strip().lower()
    return any(str(x).strip().lower() == tgt for x in roster)


def main() -> None:
    date_str = "2026-01-29"
    team = "PHI"
    player = "Tyrese Maxey"

    nk = app._norm_player_name(player)
    sk = app._short_player_key(player)

    try:
        roster_processed = app._roster_players_for_date(date_str).get(team, set())
    except Exception as e:
        roster_processed = f"ERR: {e!r}"

    try:
        roster_live = app._team_roster_names_via_nba_api(date_str, team)
    except Exception as e:
        roster_live = f"ERR: {e!r}"

    try:
        name_keys_team, short_keys_team = app._injury_name_sets_for_teams(date_str, {team})
    except Exception as e:
        name_keys_team, short_keys_team = f"ERR: {e!r}", set()

    try:
        name_keys_all, short_keys_all = app._injury_name_sets_for_date(date_str)
    except Exception as e:
        name_keys_all, short_keys_all = f"ERR: {e!r}", set()

    out: dict[str, object] = {
        "date": date_str,
        "team": team,
        "player": player,
        "player_norm": nk,
        "player_short": sk,
        "processed_roster_type": type(roster_processed).__name__,
        "processed_roster_count": (len(roster_processed) if isinstance(roster_processed, set) else None),
        "processed_has_player": present(roster_processed, player),
        "live_roster_type": type(roster_live).__name__,
        "live_roster_count": (len(roster_live) if isinstance(roster_live, list) else None),
        "live_has_player": present(roster_live, player),
        "excluded_teamaware": (nk in name_keys_team) if isinstance(name_keys_team, set) else str(name_keys_team),
        "excluded_global": (nk in name_keys_all) if isinstance(name_keys_all, set) else str(name_keys_all),
    }

    if isinstance(roster_processed, set) and isinstance(name_keys_team, set):
        filtered_proc = [
            p
            for p in roster_processed
            if not (app._norm_player_name(p) in name_keys_team or app._short_player_key(p) in short_keys_team)
        ]
        out["filtered_processed_count"] = len(filtered_proc)
        out["filtered_processed_has_player"] = present(filtered_proc, player)

    if isinstance(roster_live, list) and isinstance(name_keys_team, set):
        filtered_live = [
            p
            for p in roster_live
            if not (app._norm_player_name(p) in name_keys_team or app._short_player_key(p) in short_keys_team)
        ]
        out["filtered_live_count"] = len(filtered_live)
        out["filtered_live_has_player"] = present(filtered_live, player)

    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
