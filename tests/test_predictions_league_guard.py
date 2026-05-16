from __future__ import annotations

import pandas as pd

import app as app_module


def test_load_predictions_rows_map_rejects_stale_nba_predictions_in_wnba_mode(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "home_team": "Cleveland Cavaliers",
                "visitor_team": "Detroit Pistons",
                "home_win_prob": 0.56,
                "spread_margin": 2.5,
                "totals": 218.5,
            }
        ]
    ).to_csv(processed / "predictions_2026-05-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)

    assert app_module._load_predictions_rows_map("2026-05-15") == {}


def test_load_predictions_rows_map_accepts_wnba_predictions_in_wnba_mode(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "home_team": "Indiana Fever",
                "visitor_team": "Washington Mystics",
                "home_win_prob": 0.63,
                "spread_margin": 5.5,
                "totals": 169.5,
            }
        ]
    ).to_csv(processed / "predictions_2026-05-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)

    rows = app_module._load_predictions_rows_map("2026-05-15")

    assert ("IND", "WSH") in rows
    assert rows[("IND", "WSH")]["home_team"] == "Indiana Fever"


def test_load_best_bets_game_context_rejects_stale_nba_predictions_in_wnba_mode(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "home_team": "Minnesota Timberwolves",
                "visitor_team": "San Antonio Spurs",
                "home_win_prob": 0.57,
                "spread_margin": 3.0,
                "totals": 221.0,
            }
        ]
    ).to_csv(processed / "predictions_2026-05-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", processed)
    monkeypatch.setattr(app_module, "BASE_DIR", tmp_path)

    context = app_module._load_best_bets_game_context("2026-05-15")

    assert context == {"by_pair": {}, "by_team": {}, "slate_total_median": None}


def test_find_game_odds_for_date_uses_repo_processed_fallback(tmp_path, monkeypatch):
    active_processed = tmp_path / "active" / "data" / "processed"
    repo_processed = tmp_path / "repo" / "data" / "processed"
    active_processed.mkdir(parents=True)
    repo_processed.mkdir(parents=True)

    odds_path = repo_processed / "game_odds_2026-05-15.csv"
    pd.DataFrame(
        [
            {
                "home_team": "Indiana Fever",
                "visitor_team": "Washington Mystics",
                "home_spread": -8.5,
                "total": 170.0,
            }
        ]
    ).to_csv(odds_path, index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", active_processed)
    monkeypatch.setattr(app_module, "REPO_DATA_PROCESSED_DIR", repo_processed)

    assert app_module._find_game_odds_for_date("2026-05-15") == odds_path


def test_api_cards_returns_odds_only_fallback_from_repo_game_odds(tmp_path, monkeypatch):
    active_processed = tmp_path / "active" / "data" / "processed"
    repo_processed = tmp_path / "repo" / "data" / "processed"
    active_processed.mkdir(parents=True)
    repo_processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "date": "2026-05-15",
                "commence_time": "2026-05-15T23:30:00Z",
                "home_team": "Indiana Fever",
                "visitor_team": "Washington Mystics",
                "home_ml": -323.5,
                "away_ml": 323.5,
                "home_spread": -8.5,
                "away_spread": 8.5,
                "total": 170.0,
            }
        ]
    ).to_csv(repo_processed / "game_odds_2026-05-15.csv", index=False)

    monkeypatch.setattr(app_module, "DATA_PROCESSED_DIR", active_processed)
    monkeypatch.setattr(app_module, "REPO_DATA_PROCESSED_DIR", repo_processed)
    monkeypatch.setattr(app_module, "_load_props_predictions_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_compute_player_minutes_priors", lambda _date, days_back=21: {})
    monkeypatch.setattr(app_module, "_live_load_props_edges_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_props_recommendations_by_team", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_injury_context_map", lambda _date: {})
    monkeypatch.setattr(app_module, "_roster_players_for_date", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_best_bets_game_context", lambda _date: {"by_pair": {}, "by_team": {}, "slate_total_median": None})
    monkeypatch.setattr(app_module, "_load_best_bets_props_prediction_lookup", lambda _date: {})
    monkeypatch.setattr(app_module, "_best_bets_load_injury_snapshot", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_game_recommendations_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_snapshot_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_cards_prop_recommendations_index", lambda _date: {})
    monkeypatch.setattr(app_module, "_load_finals_lookup", lambda _date: {})
    monkeypatch.setattr(app_module, "_matchup_writeup", lambda _game: "")

    with app_module.app.test_request_context("/api/cards?date=2026-05-15"):
        response = app_module.api_cards()

    payload = response.get_json()

    assert payload["date"] == "2026-05-15"
    assert len(payload["games"]) == 1
    game = payload["games"][0]
    assert game["home_tri"] == "IND"
    assert game["away_tri"] == "WSH"
    assert game["sim"]["mode"] == "odds_fallback"
    assert game["sim"]["score"]["total_mean"] == 170.0
    assert game["sim"]["score"]["margin_mean"] == 8.5