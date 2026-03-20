from __future__ import annotations

import importlib.util
from pathlib import Path


_TOOL_PATH = Path(__file__).resolve().parents[1] / "tools" / "audit_prop_player_aliases.py"
_SPEC = importlib.util.spec_from_file_location("audit_prop_player_aliases", _TOOL_PATH)
assert _SPEC and _SPEC.loader
audit_tool = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(audit_tool)


def test_audit_prop_player_aliases_reports_likely_alias_candidate():
    payload = {
        "games": [
            {
                "home_tri": "ATL",
                "away_tri": "BKN",
                "warnings": [
                    "Players with prop lines missing from SmartSim boxscore: BKN: Nicolas Claxton"
                ],
                "sim": {
                    "players": {
                        "home": [{"player_name": "Trae Young"}],
                        "away": [{"player_name": "Nic Claxton"}, {"player_name": "Cam Thomas"}],
                    },
                    "missing_prop_players": {
                        "home": [],
                        "away": [{"player_name": "Nicolas Claxton", "prop_lines": {"reb": 6.5}}],
                    },
                },
            }
        ]
    }

    report = audit_tool.audit_cards_payload(payload, date_str="2026-03-19")

    assert report["issues_n"] == 1
    assert report["likely_aliases_n"] == 1
    assert report["unresolved_n"] == 0

    issue = report["games"][0]["issues"]["away"][0]
    candidate = issue["candidate_aliases"][0]

    assert candidate["player_name"] == "Nic Claxton"
    assert "same_last_name" in candidate["signals"]
    assert "same_short_key" in candidate["signals"]


def test_audit_prop_player_aliases_leaves_true_missing_players_unresolved():
    payload = {
        "games": [
            {
                "home_tri": "ATL",
                "away_tri": "BKN",
                "warnings": [
                    "Players with prop lines missing from SmartSim boxscore: BKN: Trendon Watford"
                ],
                "sim": {
                    "players": {
                        "home": [{"player_name": "Trae Young"}],
                        "away": [{"player_name": "Nic Claxton"}, {"player_name": "Cam Thomas"}],
                    },
                    "missing_prop_players": {
                        "home": [],
                        "away": [{"player_name": "Trendon Watford", "prop_lines": {"reb": 4.5}}],
                    },
                },
            }
        ]
    }

    report = audit_tool.audit_cards_payload(payload, date_str="2026-03-19")

    assert report["issues_n"] == 1
    assert report["likely_aliases_n"] == 0
    assert report["unresolved_n"] == 1
    assert report["games"][0]["issues"]["away"][0]["candidate_aliases"] == []