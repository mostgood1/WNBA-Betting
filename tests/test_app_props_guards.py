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


def test_api_cards_surfaces_snapshot_prop_line_options_and_marks_recommendations(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    smart_sim_path = processed / "smart_sim_2026-03-13_MEM_DET.json"
    smart_sim_path.write_text(
        json.dumps(
            {
                "home": "MEM",
                "away": "DET",
                "n_sims": 100,
                "score": {},
                "periods": [],
                "players": {
                    "home": [
                        {
                            "player_name": "Ja Morant",
                            "player_id": 1,
                            "pts_mean": 26.2,
                            "reb_mean": 5.1,
                            "ast_mean": 8.3,
                            "threes_mean": 1.9,
                            "stl_mean": 1.2,
                            "blk_mean": 0.3,
                            "tov_mean": 3.7,
                            "pra_mean": 39.6,
                        }
                    ],
                    "away": [
                        {
                            "player_name": "Cade Cunningham",
                            "player_id": 2,
                            "pts_mean": 25.4,
                            "reb_mean": 6.0,
                            "ast_mean": 8.8,
                            "threes_mean": 2.1,
                            "stl_mean": 1.1,
                            "blk_mean": 0.6,
                            "tov_mean": 4.0,
                            "pra_mean": 40.2,
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    pd.DataFrame(
        [
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_points",
                "outcome_name": "Over",
                "player_name": "Ja Morant",
                "point": 24.5,
                "price": -115,
                "last_update": "2026-03-13T17:01:00Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "draftkings",
                "bookmaker_title": "DraftKings",
                "market": "player_points",
                "outcome_name": "Over",
                "player_name": "Ja Morant",
                "point": 24.5,
                "price": -110,
                "last_update": "2026-03-13T17:01:30Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_points",
                "outcome_name": "Under",
                "player_name": "Ja Morant",
                "point": 24.5,
                "price": -105,
                "last_update": "2026-03-13T17:01:00Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "betmgm",
                "bookmaker_title": "BetMGM",
                "market": "player_points",
                "outcome_name": "Over",
                "player_name": "Ja Morant",
                "point": 25.5,
                "price": 105,
                "last_update": "2026-03-13T17:02:00Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_threes",
                "outcome_name": "Over",
                "player_name": "Cam Spencer",
                "point": 1.5,
                "price": -120,
                "last_update": "2026-03-13T17:03:00Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
            {
                "snapshot_ts": "2026-03-13T17:00:00Z",
                "event_id": "evt-1",
                "commence_time": "2026-03-13T23:00:00Z",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_threes",
                "outcome_name": "Under",
                "player_name": "Cam Spencer",
                "point": 1.5,
                "price": -110,
                "last_update": "2026-03-13T17:03:00Z",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
            },
        ]
    ).to_csv(processed / "oddsapi_player_props_2026-03-13.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_load_smart_sim_files_for_date", lambda _date: [smart_sim_path])
    monkeypatch.setattr(app_module, "_load_game_odds_map", lambda _date: {("MEM", "DET"): {"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}})
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_load_props_edges_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_props_recommendations_by_team", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_injury_context_map", lambda _date: {})
    monkeypatch.setattr(
        app_module,
        "_roster_players_for_date",
        lambda _date: {
            "MEM": {
                app_module._norm_player_name_for_keys("Ja Morant"),
                app_module._norm_player_name_for_keys("Cam Spencer"),
            },
            "DET": {app_module._norm_player_name_for_keys("Cade Cunningham")},
        },
    )
    monkeypatch.setattr(app_module, "_matchup_writeup", lambda _game: "")
    monkeypatch.setattr(
        app_module,
        "_sim_vs_line_prop_recommendations",
        lambda players_out, props_recs_by_team, **kwargs: {
            "home": [
                {
                    "team": "MEM",
                    "player": "Ja Morant",
                    "best": {
                        "market": "pts",
                        "side": "OVER",
                        "line": 24.5,
                        "book": "fanduel",
                        "price": -115,
                        "ev_pct": 4.2,
                        "guidance": {
                            "action": "play",
                            "action_code": "PLAY",
                            "play_to_line": 25.5,
                            "summary": "Model clears the opener.",
                        },
                    },
                    "picks": [
                        {
                            "market": "pts",
                            "side": "OVER",
                            "line": 24.5,
                            "book": "fanduel",
                            "price": -115,
                            "ev_pct": 4.2,
                            "guidance": {
                                "action": "play",
                                "action_code": "PLAY",
                                "play_to_line": 25.5,
                                "summary": "Model clears the opener.",
                            },
                        }
                    ],
                }
            ],
            "away": [],
        },
    )

    with app_module.app.test_request_context("/api/cards?date=2026-03-13"):
        response = app_module.api_cards()

    payload = response.get_json()
    game = payload["games"][0]
    ja_row = next(row for row in game["sim"]["players"]["home"] if row["player_name"] == "Ja Morant")
    pts_options = ja_row["prop_line_options"]["pts"]

    assert any(option["side"] == "OVER" and option["line"] == 24.5 and option["book_count"] == 2 for option in pts_options)
    recommended_option = next(option for option in pts_options if option["side"] == "OVER" and option["line"] == 24.5)
    assert recommended_option["recommended"] is True
    assert recommended_option["recommended_primary"] is True
    assert recommended_option["recommendation_action"] == "play"
    assert recommended_option["recommendation_play_to_line"] == 25.5

    missing_home = game["sim"]["missing_prop_players"]["home"]
    cam_row = next(row for row in missing_home if row["player_name"] == "Cam Spencer")

    assert "threes" in cam_row["prop_line_options"]
    assert any(option["side"] == "OVER" and option["line"] == 1.5 for option in cam_row["prop_line_options"]["threes"])