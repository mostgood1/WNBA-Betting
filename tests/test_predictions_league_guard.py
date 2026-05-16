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