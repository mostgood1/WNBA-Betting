from __future__ import annotations

import pandas as pd
from click.testing import CliRunner

from nba_betting import cli as cli_module
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