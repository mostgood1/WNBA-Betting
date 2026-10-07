"""fetch-rosters must never file today's roster under another season.

ESPN's roster endpoint serves only the CURRENT roster; fetch_rosters used its `season` argument only to label the
output. Both CLI commands (and app.py's cron route) defaulted to the NBA-style "2025-26", which wrote the current
rosters as rosters_2025-26.csv / SEASON 2025-26 -- a file pick_rosters_file then matches for 2025 dates.
"""

from __future__ import annotations

import datetime

import pandas as pd
from click.testing import CliRunner

from wnba_betting import cli
from wnba_betting import config as config_module
from wnba_betting import rosters as rosters_module
from wnba_betting.league import season_label_from_date

TEAMS = [{"id": "20", "display_name": "Atlanta Dream", "team_abbreviation": "ATL"}]
ATHLETES = [{"id": "3058901", "displayName": "Allisha Gray", "firstName": "Allisha", "lastName": "Gray"}]


def _setup(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)
    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(rosters_module, "paths", test_paths)
    monkeypatch.setattr(rosters_module, "_fetch_espn_teams", lambda: TEAMS)
    monkeypatch.setattr(rosters_module, "_fetch_espn_roster", lambda team_id: ATHLETES)
    monkeypatch.setenv("WNBA_ROSTERS_RATE_DELAY", "0")
    return processed


def _written(processed):
    files = sorted(p.name for p in processed.glob("rosters_*.csv"))
    seasons = sorted({str(x) for f in files for x in pd.read_csv(processed / f)["SEASON"]})
    return files, seasons


def test_an_nba_style_season_is_normalised_to_the_current_one(tmp_path, monkeypatch, capsys):
    processed = _setup(tmp_path, monkeypatch)
    current = str(season_label_from_date(datetime.date.today()))
    rosters_module.fetch_rosters(season="2025-26")
    assert _written(processed) == ([f"rosters_{current}.csv"], [current])
    assert "ROSTER_SEASON_NORMALISED requested=2025-26" in capsys.readouterr().out


def test_both_cli_commands_default_to_the_current_season(tmp_path, monkeypatch):
    current = str(season_label_from_date(datetime.date.today()))
    for command in ("fetch-rosters", "fetch-rosters-cmd"):
        processed = _setup(tmp_path / command, monkeypatch)
        result = CliRunner().invoke(cli.cli, [command])
        assert result.exit_code == 0, result.output
        assert _written(processed) == ([f"rosters_{current}.csv"], [current]), command
