from types import SimpleNamespace

import pandas as pd

from nba_betting.sim import smart_sim


def test_merge_pregame_expected_minutes_handles_missing_player_name_column(monkeypatch):
    pem = pd.DataFrame(
        {
            "date": ["2026-03-31"],
            "team_tri": ["PHX"],
            "player_id": [1],
            "player_name": ["Devin Booker"],
            "exp_min_mean": [34.0],
        }
    )
    monkeypatch.setattr(smart_sim, "_load_pregame_expected_minutes", lambda _date: pem)

    team_df = pd.DataFrame({"player_id": [1], "exp_min_mean": [pd.NA]})

    out, diag = smart_sim._merge_pregame_expected_minutes_for_team(
        team_df,
        date_str="2026-03-31",
        team_tri="PHX",
    )

    assert len(out) == 1
    assert list(out.columns) == ["player_id", "exp_min_mean"]
    assert diag["attempted"] is True


def test_apply_player_priors_handles_missing_roll_minutes_columns():
    team_df = pd.DataFrame(
        {
            "player_name": ["Devin Booker"],
            "pred_pts": [26.0],
            "pred_reb": [4.0],
            "pred_ast": [6.0],
            "pred_threes": [2.5],
            "pred_stl": [1.0],
            "pred_blk": [0.4],
            "pred_tov": [2.8],
        }
    )
    priors = SimpleNamespace(rates={("PHX", "DEVIN BOOKER"): {}})

    out = smart_sim._apply_player_priors(
        team_df,
        priors,
        team_tri="PHX",
        sim_minutes=pd.Series([34.0]),
        date_str=None,
    )

    assert out.loc[0, "_pkey"] == "DEVIN BOOKER"
    assert float(out.loc[0, "_prior_pts_pm"]) > 0.0
    assert float(out.loc[0, "_prior_fga_pm"]) > 0.0