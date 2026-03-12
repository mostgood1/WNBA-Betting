from __future__ import annotations

import pandas as pd
from click.testing import CliRunner

from nba_betting import cli as cli_module


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