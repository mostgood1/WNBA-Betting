from __future__ import annotations

import numpy as np
import pandas as pd

import nba_betting.props_edges as props_edges_module
from nba_betting.props_edges import SigmaConfig, compute_props_edges


def test_compute_props_edges_keeps_unknown_availability_rows(tmp_path, monkeypatch):
    pred_path = tmp_path / "props_predictions_2026-03-14.csv"
    pd.DataFrame(
        [
            {
                "player_id": 1,
                "player_name": "Quentin Grimes",
                "team": "PHI",
                "pred_pts": 24.2,
                "pred_reb": 4.1,
                "pred_ast": 3.8,
                "pred_threes": 2.6,
                "pred_pra": 32.1,
                "pred_stl": 1.1,
                "pred_blk": 0.4,
                "pred_tov": 2.0,
                "team_on_slate": np.nan,
                "playing_today": np.nan,
            },
            {
                "player_id": 2,
                "player_name": "Cam Thomas",
                "team": "BKN",
                "pred_pts": 22.8,
                "pred_reb": 3.5,
                "pred_ast": 4.2,
                "pred_threes": 2.9,
                "pred_pra": 30.5,
                "pred_stl": 0.9,
                "pred_blk": 0.2,
                "pred_tov": 2.6,
                "team_on_slate": True,
                "playing_today": False,
            },
        ]
    ).to_csv(pred_path, index=False)

    odds = pd.DataFrame(
        [
            {
                "snapshot_ts": "2026-03-14T16:00:00Z",
                "event_id": "evt-1",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_points",
                "outcome_name": "Over",
                "player_name": "Quentin Grimes",
                "point": 19.5,
                "price": -110,
                "commence_time": "2026-03-14T23:00:00Z",
                "home_team": "Philadelphia 76ers",
                "away_team": "Brooklyn Nets",
            },
            {
                "snapshot_ts": "2026-03-14T16:00:00Z",
                "event_id": "evt-1",
                "bookmaker": "fanduel",
                "bookmaker_title": "FanDuel",
                "market": "player_points",
                "outcome_name": "Over",
                "player_name": "Cam Thomas",
                "point": 21.5,
                "price": -110,
                "commence_time": "2026-03-14T23:00:00Z",
                "home_team": "Philadelphia 76ers",
                "away_team": "Brooklyn Nets",
            },
        ]
    )

    monkeypatch.setattr(props_edges_module, "_load_props_odds_from_path", lambda _date, _path: odds.copy())

    out = compute_props_edges(
        date="2026-03-14",
        sigma=SigmaConfig(),
        source="oddsapi",
        predictions_path=str(pred_path),
        from_file_only=True,
        exclude_injured=False,
        odds_path=str(tmp_path / "unused.csv"),
        attach_opening_snapshot=False,
        resolve_roster=False,
    )

    valid = out[out["model_prob"].notna()].copy()

    assert len(valid) == 1
    assert valid.iloc[0]["player_name"] == "Quentin Grimes"
    assert valid.iloc[0]["team"] == "PHI"
    assert valid.iloc[0]["model_prob"] > valid.iloc[0]["implied_prob"]