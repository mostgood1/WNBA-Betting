import pandas as pd

from nba_betting.refresh_oddsapi_props_job import (
    _collect_snapshot_coverage_gaps,
    _merge_props_prediction_frames,
)
from nba_betting.player_names import normalize_player_name_key


def test_merge_props_prediction_frames_appends_fallback_only_rows():
    preferred = pd.DataFrame(
        [
            {
                "player_id": 1,
                "team": "BOS",
                "player_name": "Jayson Tatum",
                "pred_pts": 28.5,
            }
        ]
    )
    fallback = pd.DataFrame(
        [
            {
                "player_id": 1,
                "team": "BOS",
                "player_name": "Jayson Tatum",
                "pred_pts": 27.8,
            },
            {
                "player_id": 2,
                "team": "BOS",
                "player_name": "Derrick White",
                "pred_pts": 15.1,
            },
        ]
    )

    merged, stats = _merge_props_prediction_frames(preferred, fallback)

    assert len(merged) == 2
    assert stats["fallback_only_rows"] == 1
    assert set(merged["player_name"].tolist()) == {"Jayson Tatum", "Derrick White"}


def test_collect_snapshot_coverage_gaps_flags_missing_prediction_and_sim():
    snapshot = pd.DataFrame(
        [
            {
                "home_team": "Boston Celtics",
                "away_team": "New York Knicks",
                "player_name": "Jayson Tatum",
                "market": "player_points",
            },
            {
                "home_team": "Boston Celtics",
                "away_team": "New York Knicks",
                "player_name": "Derrick White",
                "market": "player_points",
            },
            {
                "home_team": "Boston Celtics",
                "away_team": "New York Knicks",
                "player_name": "Karl-Anthony Towns",
                "market": "player_points",
            },
        ]
    )
    predictions = pd.DataFrame(
        [
            {
                "player_id": 1,
                "team": "BOS",
                "opponent": "NYK",
                "player_name": "Jayson Tatum",
            },
            {
                "player_id": 2,
                "team": "BOS",
                "opponent": "NYK",
                "player_name": "Derrick White",
            },
        ]
    )
    smart_sim = {("BOS", "NYK"): {"jayson tatum"}}

    gaps = _collect_snapshot_coverage_gaps(snapshot, predictions, smart_sim)

    by_name = {row["player_name"]: row for row in gaps}
    assert "Jayson Tatum" not in by_name
    assert by_name["Derrick White"]["missing_prediction"] is False
    assert by_name["Derrick White"]["missing_sim"] is True
    assert by_name["Karl-Anthony Towns"]["missing_prediction"] is True
    assert by_name["Karl-Anthony Towns"]["missing_sim"] is True


def test_normalize_player_name_key_collapses_known_aliases():
    assert normalize_player_name_key("Herb Jones") == "HERBERT JONES"
    assert normalize_player_name_key("Herbert Jones") == "HERBERT JONES"
    assert normalize_player_name_key("Moe Wagner") == "MORITZ WAGNER"
    assert normalize_player_name_key("Moritz Wagner") == "MORITZ WAGNER"


def test_merge_props_prediction_frames_treats_known_aliases_as_same_player():
    preferred = pd.DataFrame(
        [
            {
                "team": "NOP",
                "player_name": "Herbert Jones",
                "pred_pts": 10.1,
            }
        ]
    )
    fallback = pd.DataFrame(
        [
            {
                "team": "NOP",
                "player_name": "Herb Jones",
                "pred_pts": 9.8,
            }
        ]
    )

    merged, stats = _merge_props_prediction_frames(preferred, fallback)

    assert len(merged) == 1
    assert stats["fallback_only_rows"] == 0
    assert merged.iloc[0]["player_name"] == "Herbert Jones"


def test_collect_snapshot_coverage_gaps_collapses_known_aliases():
    snapshot = pd.DataFrame(
        [
            {
                "home_team": "Orlando Magic",
                "away_team": "Cleveland Cavaliers",
                "player_name": "Moe Wagner",
                "market": "player_points",
            },
            {
                "home_team": "New Orleans Pelicans",
                "away_team": "Toronto Raptors",
                "player_name": "Herb Jones",
                "market": "player_points",
            },
        ]
    )
    predictions = pd.DataFrame(
        [
            {
                "team": "ORL",
                "opponent": "CLE",
                "player_name": "Moritz Wagner",
            },
            {
                "team": "NOP",
                "opponent": "TOR",
                "player_name": "Herbert Jones",
            },
        ]
    )
    smart_sim = {
        ("ORL", "CLE"): {"moritz wagner"},
        ("NOP", "TOR"): {"herbert jones"},
    }

    gaps = _collect_snapshot_coverage_gaps(snapshot, predictions, smart_sim)

    assert gaps == []