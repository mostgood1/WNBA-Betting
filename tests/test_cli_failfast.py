from __future__ import annotations

import pandas as pd
from click.testing import CliRunner

from nba_betting import cli as cli_module
from nba_betting import config as config_module
from nba_betting import props_onnx_pure as props_onnx_pure_module


def test_predict_props_exits_nonzero_when_feature_build_fails(monkeypatch):
    def _boom(_date_str: str):
        raise RuntimeError("feature build blew up")

    monkeypatch.setattr(cli_module, "build_features_for_date", _boom)

    import nba_betting.props_features_pure as props_features_pure

    monkeypatch.setattr(props_features_pure, "build_features_for_date_pure", _boom)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["predict-props", "--date", "2026-03-12"])

    assert result.exit_code != 0
    assert "Failed to build features" in result.output


def test_predict_props_keeps_league_status_active_players_despite_stale_injuries(tmp_path, monkeypatch):
    date_str = "2026-03-17"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    raw = data_root / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player_id": 1626164,
                "player_name": "Devin Booker",
                "team": "PHX",
                "team_on_slate": True,
                "playing_today": True,
            }
        ]
    ).to_csv(processed / f"league_status_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "team": "PHI",
                "player": "Devin Booker",
                "status": "OUT",
                "injury": "Out",
                "date": "2026-02-26",
            }
        ]
    ).to_csv(raw / "injuries.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(cli_module, "paths", test_paths)

    import nba_betting.props_features_pure as props_features_pure_module

    monkeypatch.setattr(
        props_features_pure_module,
        "build_features_for_date_pure",
        lambda _date_str: pd.DataFrame(
            [
                {
                    "player_id": 1626164,
                    "player_name": "Devin Booker",
                    "team": "PHX",
                }
            ]
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "build_features_for_date",
        lambda _date_str: pd.DataFrame(
            [
                {
                    "player_id": 1626164,
                    "player_name": "Devin Booker",
                    "team": "PHX",
                }
            ]
        ),
    )

    monkeypatch.setattr(
        props_onnx_pure_module,
        "predict_props_pure_onnx",
        lambda feats: pd.DataFrame(
            [
                {
                    "player_id": int(feats.iloc[0]["player_id"]),
                    "player_name": feats.iloc[0]["player_name"],
                    "team": feats.iloc[0]["team"],
                    "asof_date": date_str,
                    "opponent": "MIN",
                    "home": False,
                    "pred_pts": 27.5,
                    "pred_reb": 4.5,
                    "pred_ast": 6.5,
                    "pred_pra": 38.5,
                    "sd_pts": 7.5,
                    "sd_reb": 3.0,
                    "sd_ast": 2.5,
                    "sd_pra": 9.0,
                }
            ]
        ),
    )

    out_path = processed / f"props_predictions_{date_str}.csv"
    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "predict-props",
            "--date",
            date_str,
            "--out",
            str(out_path),
            "--no-slate-only",
            "--no-calibrate",
            "--no-use-smart-sim",
        ],
    )

    assert result.exit_code == 0
    written = pd.read_csv(out_path)
    assert written.shape[0] == 1
    assert written.iloc[0]["player_name"] == "Devin Booker"
    assert written.iloc[0]["team"] == "PHX"


def test_predict_games_npu_uses_odds_events_fallback(tmp_path, monkeypatch):
    date_str = "2026-03-12"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    raw = data_root / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "date": "2026-03-11",
                "home_team": "Boston Celtics",
                "visitor_team": "Los Angeles Lakers",
                "feature_stub": 1.0,
            }
        ]
    ).to_csv(processed / "features.csv", index=False)

    pd.DataFrame(
        [
            {
                "home_team": "Detroit Pistons",
                "away_team": "Philadelphia 76ers",
                "commence_time": "2026-03-12T23:10:00Z",
            }
        ]
    ).to_csv(processed / f"odds_events_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "date": "2026-03-11",
                "home_team": "Boston Celtics",
                "visitor_team": "Los Angeles Lakers",
                "home_pts": 110,
                "visitor_pts": 108,
            }
        ]
    ).to_csv(raw / "games_nba_api.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(cli_module, "paths", test_paths)

    import nba_betting.features_enhanced as features_enhanced_module
    import nba_betting.games_npu as games_npu_module

    def _fake_build_features(games, include_advanced_stats=True, include_injuries=True, season=2025):
        assert ((games["home_team"] == "Detroit Pistons") & (games["visitor_team"] == "Philadelphia 76ers")).any()
        return pd.DataFrame(
            [
                {
                    "date": date_str,
                    "home_team": "Detroit Pistons",
                    "visitor_team": "Philadelphia 76ers",
                    "feature_stub": 1.0,
                }
            ]
        )

    def _fake_predict_games_npu(features_df, include_periods=True, calibrate_periods=True):
        assert not features_df.empty
        return pd.DataFrame(
            [
                {
                    "date": date_str,
                    "home_team": "Detroit Pistons",
                    "visitor_team": "Philadelphia 76ers",
                    "win_prob": 0.61,
                }
            ]
        )

    monkeypatch.setattr(features_enhanced_module, "build_features_enhanced", _fake_build_features)
    monkeypatch.setattr(games_npu_module, "predict_games_npu", _fake_predict_games_npu)
    monkeypatch.setattr(cli_module, "_ensure_game_models_available", lambda: None)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["predict-games-npu", "--date", date_str])

    assert result.exit_code == 0
    assert (processed / f"games_predictions_npu_{date_str}.csv").exists()


def test_predict_games_npu_uses_predictions_fallback(tmp_path, monkeypatch):
    date_str = "2026-03-12"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    raw = data_root / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "date": "2026-03-11",
                "home_team": "Boston Celtics",
                "visitor_team": "Los Angeles Lakers",
                "feature_stub": 1.0,
            }
        ]
    ).to_csv(processed / "features.csv", index=False)

    pd.DataFrame(
        [
            {
                "date": date_str,
                "home_team": "Detroit Pistons",
                "visitor_team": "Philadelphia 76ers",
                "home_win_prob": 0.61,
            }
        ]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {
                "date": "2026-03-11",
                "home_team": "Boston Celtics",
                "visitor_team": "Los Angeles Lakers",
                "home_pts": 110,
                "visitor_pts": 108,
            }
        ]
    ).to_csv(raw / "games_nba_api.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(cli_module, "paths", test_paths)

    import nba_betting.features_enhanced as features_enhanced_module
    import nba_betting.games_npu as games_npu_module

    def _fake_build_features(games, include_advanced_stats=True, include_injuries=True, season=2025):
        assert ((games["home_team"] == "Detroit Pistons") & (games["visitor_team"] == "Philadelphia 76ers")).any()
        return pd.DataFrame(
            [
                {
                    "date": date_str,
                    "home_team": "Detroit Pistons",
                    "visitor_team": "Philadelphia 76ers",
                    "feature_stub": 1.0,
                }
            ]
        )

    def _fake_predict_games_npu(features_df, include_periods=True, calibrate_periods=True):
        assert not features_df.empty
        return pd.DataFrame(
            [
                {
                    "date": date_str,
                    "home_team": "Detroit Pistons",
                    "visitor_team": "Philadelphia 76ers",
                    "win_prob": 0.61,
                }
            ]
        )

    monkeypatch.setattr(features_enhanced_module, "build_features_enhanced", _fake_build_features)
    monkeypatch.setattr(games_npu_module, "predict_games_npu", _fake_predict_games_npu)
    monkeypatch.setattr(cli_module, "_ensure_game_models_available", lambda: None)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["predict-games-npu", "--date", date_str])

    assert result.exit_code == 0
    assert (processed / f"games_predictions_npu_{date_str}.csv").exists()


def test_props_edges_file_only_exits_nonzero_when_no_edges(monkeypatch):
    def _empty_edges(**_kwargs):
        return pd.DataFrame()

    monkeypatch.setattr(cli_module, "compute_props_edges", _empty_edges)

    runner = CliRunner()
    result = runner.invoke(
        cli_module.cli,
        [
            "props-edges",
            "--date",
            "2026-03-12",
            "--file-only",
            "--source",
            "oddsapi",
            "--mode",
            "current",
        ],
    )

    assert result.exit_code != 0
    assert "No edges computed for 2026-03-12" in result.output


def test_fetch_player_logs_exits_nonzero_when_no_logs(monkeypatch):
    def _empty_logs(_seasons):
        return pd.DataFrame()

    monkeypatch.setattr(cli_module, "fetch_player_logs", _empty_logs)

    runner = CliRunner()
    result = runner.invoke(cli_module.cli, ["fetch-player-logs", "--seasons", "2025-26"])

    assert result.exit_code != 0
    assert "No player logs returned." in result.output


def test_predict_props_pure_onnx_falls_back_when_model_artifacts_missing(monkeypatch):
    class _MissingPredictor:
        def __init__(self, *args, **kwargs):
            raise FileNotFoundError("Feature columns not found")

    class _FakePriors:
        def rate(self, _team, _player_name, _key):
            return {
                "min_mu": 32.0,
                "pts_pm": 0.78,
                "reb_pm": 0.24,
                "ast_pm": 0.16,
                "threes_pm": 0.11,
                "stl_pm": 0.03,
                "blk_pm": 0.02,
                "tov_pm": 0.07,
            }

    monkeypatch.setattr(props_onnx_pure_module, "PureONNXPredictor", _MissingPredictor)
    monkeypatch.setattr(props_onnx_pure_module, "compute_player_priors", lambda *_args, **_kwargs: _FakePriors())

    feats = pd.DataFrame(
        [
            {
                "player_id": 1,
                "player_name": "Jayson Tatum",
                "team": "BOS",
                "asof_date": "2026-03-12",
                "b2b": 0.0,
                "lag1_pts": 28.0,
                "roll3_pts": 27.0,
                "roll5_pts": 26.0,
                "roll10_pts": 25.0,
                "lag1_reb": 9.0,
                "roll3_reb": 8.0,
                "roll5_reb": 8.5,
                "roll10_reb": 8.0,
                "lag1_ast": 7.0,
                "roll3_ast": 6.5,
                "roll5_ast": 6.0,
                "roll10_ast": 5.5,
                "lag1_threes": 3.0,
                "roll3_threes": 3.5,
                "roll5_threes": 3.2,
                "roll10_threes": 3.1,
                "lag1_min": 36.0,
                "roll3_min": 35.0,
                "roll5_min": 34.0,
                "roll10_min": 33.0,
            }
        ]
    )

    preds = props_onnx_pure_module.predict_props_pure_onnx(feats)

    assert preds.loc[0, "pred_pts"] > 0
    assert preds.loc[0, "pred_reb"] > 0
    assert preds.loc[0, "pred_ast"] > 0
    assert preds.loc[0, "pred_threes"] > 0
    assert preds.loc[0, "pred_stl"] > 0
    assert preds.loc[0, "pred_blk"] > 0
    assert preds.loc[0, "pred_tov"] > 0
    assert preds.loc[0, "pred_pra"] == preds.loc[0, "pred_pts"] + preds.loc[0, "pred_reb"] + preds.loc[0, "pred_ast"]


def test_export_props_recommendations_excludes_nonpositive_edges(tmp_path, monkeypatch):
    date_str = "2026-03-12"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player_name": "Jayson Tatum",
                "team": "BOS",
                "stat": "pts",
                "side": "OVER",
                "line": 29.5,
                "price": -110,
                "edge": -0.02,
                "ev": -0.01,
                "bookmaker": "draftkings",
            },
            {
                "player_name": "Jaylen Brown",
                "team": "BOS",
                "stat": "reb",
                "side": "OVER",
                "line": 6.5,
                "price": -105,
                "edge": 0.04,
                "ev": 0.02,
                "bookmaker": "fanduel",
            },
        ]
    ).to_csv(processed / f"props_edges_{date_str}.csv", index=False)

    monkeypatch.setattr(
        config_module,
        "paths",
        config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root),
    )

    rows, out = cli_module._export_props_recommendations_cards.callback(date_str, None, 125.0)
    exported = pd.read_csv(out)

    assert rows == 1
    assert exported["player"].tolist() == ["Jaylen Brown"]