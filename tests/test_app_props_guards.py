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
    monkeypatch.setattr(app_module, "_live_find_processed_csv", lambda _stem, _date: None)
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
    raw = tmp_path / "data" / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

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
    ).to_csv(raw / "odds_nba_player_props_2026-03-13.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "DATA_RAW_DIR", raw)
    monkeypatch.setattr(app_module, "_load_smart_sim_files_for_date", lambda _date: [smart_sim_path])
    monkeypatch.setattr(app_module, "_load_game_odds_map", lambda _date: {("MEM", "DET"): {"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}})
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_find_processed_csv", lambda _stem, _date: None)
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


def test_load_props_movement_callouts_exposes_player_id_photo_fallback(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player": "Jayson Tatum",
                "team": "Boston Celtics",
                "team_tricode": "BOS",
                "market": "pts",
                "side": "OVER",
                "line": 28.5,
                "price": -110,
                "open_line": 27.5,
                "open_price": -115,
                "line_move": 1.0,
                "implied_move": 0.03,
                "ev_pct": 2.4,
                "movement_tier": "fast",
            }
        ]
    ).to_csv(processed / "props_movement_signals_2026-03-15.csv", index=False)

    pd.DataFrame(
        [
            {
                "home_team": "Boston Celtics",
                "visitor_team": "Detroit Pistons",
            }
        ]
    ).to_csv(processed / "predictions_2026-03-15.csv", index=False)

    pd.DataFrame(
        [
            {
                "player_name": "Jayson Tatum",
                "team": "BOS",
                "player_id": 1628369,
                "pred_pts": 29.1,
            }
        ]
    ).to_csv(processed / "props_predictions_2026-03-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)

    items = app_module._load_props_movement_callouts(
        "2026-03-15",
        markets={"pts"},
        min_ev_pct=1.0,
        only_ev=True,
    )

    assert len(items) == 1
    assert items[0]["player"] == "Jayson Tatum"
    assert items[0]["player_id"] == 1628369
    assert items[0]["photo"] == "https://cdn.nba.com/headshots/nba/latest/1040x760/1628369.png"


def test_load_props_movement_callouts_rewrites_source_id_to_resolved_nba_headshot(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player": "Shai Gilgeous-Alexander",
                "team": "Oklahoma City Thunder",
                "team_tricode": "OKC",
                "market": "threes",
                "side": "UNDER",
                "line": 1.5,
                "price": 102,
                "open_line": 1.5,
                "open_price": -123,
                "line_move": 0.0,
                "implied_move": -0.05,
                "ev_pct": 4.8,
                "movement_tier": "fast",
                "player_id": 4278073,
                "photo": "https://cdn.nba.com/headshots/nba/latest/1040x760/4278073.png",
            }
        ]
    ).to_csv(processed / "props_movement_signals_2026-03-15.csv", index=False)

    pd.DataFrame(
        [
            {
                "home_team": "Oklahoma City Thunder",
                "visitor_team": "Minnesota Timberwolves",
            }
        ]
    ).to_csv(processed / "predictions_2026-03-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(app_module, "_resolve_player_id", lambda player, team=None: 1628983 if player == "Shai Gilgeous-Alexander" else None)

    items = app_module._load_props_movement_callouts(
        "2026-03-15",
        markets={"threes"},
        min_ev_pct=1.0,
        only_ev=True,
    )

    assert len(items) == 1
    assert items[0]["player_id"] == 1628983
    assert items[0]["photo"] == "https://cdn.nba.com/headshots/nba/latest/1040x760/1628983.png"


def test_load_props_movement_callouts_falls_back_to_espn_headshot_for_source_id(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player": "Donovan Clingan",
                "team": "Portland Trail Blazers",
                "team_tricode": "POR",
                "market": "blk",
                "side": "UNDER",
                "line": 1.5,
                "price": 118,
                "open_line": 1.5,
                "open_price": -108,
                "line_move": 0.0,
                "implied_move": -0.06,
                "ev_pct": 3.2,
                "movement_tier": "fast",
                "player_id": 5105565,
            }
        ]
    ).to_csv(processed / "props_movement_signals_2026-03-15.csv", index=False)

    pd.DataFrame(
        [
            {
                "home_team": "Philadelphia 76ers",
                "visitor_team": "Portland Trail Blazers",
            }
        ]
    ).to_csv(processed / "predictions_2026-03-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "_maybe_fetch_remote_processed", lambda _name: None)
    monkeypatch.setattr(app_module, "_resolve_player_id", lambda player, team=None: None)

    items = app_module._load_props_movement_callouts(
        "2026-03-15",
        markets={"blk"},
        min_ev_pct=1.0,
        only_ev=True,
    )

    assert len(items) == 1
    assert items[0]["player_id"] == 5105565
    assert items[0]["photo"] == "https://a.espncdn.com/i/headshots/nba/players/full/5105565.png"