from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


def _load_filter_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "filter_props_predictions_by_injuries.py"
    spec = importlib.util.spec_from_file_location("filter_props_predictions_by_injuries_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_filter_props_predictions_ignores_short_key_collisions_from_noisy_injuries(tmp_path):
    preds_path = tmp_path / "props_predictions_2026-04-02.csv"
    injuries_path = tmp_path / "injuries_excluded_2026-04-02.csv"
    league_status_path = tmp_path / "league_status_2026-04-02.csv"

    pd.DataFrame(
        [
            {"player_name": "Dillon Brooks", "team": "PHX", "playing_today": True},
            {"player_name": "Haywood Highsmith", "team": "PHX", "playing_today": True},
            {"player_name": "Mark Williams", "team": "PHX", "playing_today": True},
            {"player_name": "Devin Booker", "team": "PHX", "playing_today": True},
        ]
    ).to_csv(preds_path, index=False)

    pd.DataFrame(
        [
            {"team": "PHX", "team_tri": "PHX", "player": "Dillon Management Brooks", "status": "OUT"},
            {"team": "PHX", "team_tri": "PHX", "player": "Haywood Sprain Highsmith", "status": "OUT"},
            {"team": "PHX", "team_tri": "PHX", "player": "Mark Soreness Williams", "status": "OUT"},
        ]
    ).to_csv(injuries_path, index=False)

    pd.DataFrame(
        [
            {"player_name": "Dillon Brooks", "team": "PHX", "team_on_slate": True, "playing_today": True},
            {"player_name": "Haywood Highsmith", "team": "PHX", "team_on_slate": True, "playing_today": True},
            {"player_name": "Mark Williams", "team": "PHX", "team_on_slate": True, "playing_today": True},
            {"player_name": "Devin Booker", "team": "PHX", "team_on_slate": True, "playing_today": True},
        ]
    ).to_csv(league_status_path, index=False)

    module = _load_filter_module()
    result = module.filter_props_predictions_by_injuries(
        preds_path,
        injuries_path,
        league_status_path=league_status_path,
    )

    filtered = pd.read_csv(preds_path)
    assert result["before_rows"] == 4
    assert result["after_rows"] == 4
    assert result["removed_players"] == []
    assert sorted(filtered["player_name"].tolist()) == [
        "Devin Booker",
        "Dillon Brooks",
        "Haywood Highsmith",
        "Mark Williams",
    ]


def test_filter_props_predictions_removes_exact_team_match_when_not_playing_today(tmp_path):
    preds_path = tmp_path / "props_predictions_2026-04-02.csv"
    injuries_path = tmp_path / "injuries_excluded_2026-04-02.csv"

    pd.DataFrame(
        [
            {"player_name": "Amir Coffey", "team": "PHX", "playing_today": False},
            {"player_name": "Devin Booker", "team": "PHX", "playing_today": True},
        ]
    ).to_csv(preds_path, index=False)

    pd.DataFrame(
        [
            {"team": "PHX", "team_tri": "PHX", "player": "Amir Coffey", "status": "OUT"},
        ]
    ).to_csv(injuries_path, index=False)

    module = _load_filter_module()
    result = module.filter_props_predictions_by_injuries(preds_path, injuries_path)

    filtered = pd.read_csv(preds_path)
    assert result["before_rows"] == 2
    assert result["after_rows"] == 1
    assert result["removed_players"] == ["Amir Coffey"]
    assert filtered["player_name"].tolist() == ["Devin Booker"]