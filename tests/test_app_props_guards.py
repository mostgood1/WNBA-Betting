from __future__ import annotations

import json

import pandas as pd

import app as app_module


def test_prune_invalid_props_recommendations_artifact_removes_model_only_cards(tmp_path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    rec_path = processed / "props_recommendations_2026-03-13.csv"
    pd.DataFrame(
        [
            {
                "player": "Cade Cunningham",
                "team": "DET",
                "plays": "[]",
                "ladders": "[]",
                "model": "{'pts': 25.8}",
            }
        ]
    ).to_csv(rec_path, index=False)

    messages: list[str] = []
    removed = app_module._prune_invalid_props_recommendations_artifact(
        processed / "props_edges_2026-03-13.csv",
        rec_path,
        log_cb=messages.append,
    )

    assert removed is True
    assert not rec_path.exists()
    assert any("Removed same-day props recommendations" in msg for msg in messages)


def test_prune_invalid_props_recommendations_artifact_preserves_line_bearing_cards(tmp_path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    rec_path = processed / "props_recommendations_2026-03-13.csv"
    pd.DataFrame(
        [
            {
                "player": "Cade Cunningham",
                "team": "DET",
                "plays": "[{'market': 'pts', 'side': 'OVER', 'line': 25.5, 'price': -110}]",
                "ladders": "[]",
                "model": "{'pts': 25.8}",
            }
        ]
    ).to_csv(rec_path, index=False)

    messages: list[str] = []
    removed = app_module._prune_invalid_props_recommendations_artifact(
        processed / "props_edges_2026-03-13.csv",
        rec_path,
        log_cb=messages.append,
    )

    assert removed is False
    assert rec_path.exists()
    assert any("Preserving existing same-day line-bearing props recommendations" in msg for msg in messages)


def test_api_cards_skips_missing_prop_players_warning_when_smartsim_errors(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)
    smart_sim_path = processed / "smart_sim_2026-03-13_MEM_DET.json"
    smart_sim_path.write_text(
        json.dumps(
            {
                "error": "missing_players",
                "home": "MEM",
                "away": "DET",
                "home_players": 0,
                "away_players": 0,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_load_game_odds_map", lambda _date: {("MEM", "DET"): {"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}})
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_load_props_edges_index", lambda _date: {})
    monkeypatch.setattr(
        app_module,
        "_load_props_recommendations_by_team",
        lambda _date: {
            "MEM": [
                {
                    "player": "Cam Spencer",
                    "team": "MEM",
                    "plays": [{"market": "pts", "side": "over", "line": 10.5, "price": -110, "book": "fanduel"}],
                    "top_play": None,
                    "top_play_reasons": [],
                }
            ],
            "DET": [
                {
                    "player": "Cade Cunningham",
                    "team": "DET",
                    "plays": [{"market": "ast", "side": "over", "line": 8.5, "price": -110, "book": "fanduel"}],
                    "top_play": None,
                    "top_play_reasons": [],
                }
            ],
        },
    )
    monkeypatch.setattr(app_module, "_load_injury_context_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {})
    monkeypatch.setattr(app_module, "_matchup_writeup", lambda _game: "")

    with app_module.app.test_request_context("/api/cards?date=2026-03-13"):
        response = app_module.api_cards()

    payload = response.get_json()
    warnings = payload["games"][0].get("warnings") or []

    assert f"SmartSim error for MEM-DET: missing_players" in warnings
    assert all("Players with prop lines missing from SmartSim boxscore" not in warning for warning in warnings)