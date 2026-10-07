"""league_status_<date>.csv must hold WNBA players and the WNBA slate only.

Measured 2026-10-06 on a production data root with no processed rosters_*.csv: every league_status file
(09-30..10-08) held the NBA roster (~600 rows, 30 NBA teams, no WNBA player). build_league_status fell through to
nba_api's static_teams (the NBA list) + CommonTeamRoster, and its ScoreboardV2 call had no league_id, so NBA
ATL/GSW/... were flagged on slate.
"""

from __future__ import annotations

import pandas as pd

from wnba_betting import config as config_module
from wnba_betting import league_status as league_status_module

DATE = "2026-10-07"


def _setup(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    raw = data_root / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {"GAME_DATE": "2026-09-24", "PLAYER_NAME": "Allisha Gray", "PLAYER_ID": 1628932, "TEAM_ABBREVIATION": "ATL"},
            {"GAME_DATE": "2026-09-24", "PLAYER_NAME": "Breanna Stewart", "PLAYER_ID": 1627668, "TEAM_ABBREVIATION": "NYL"},
            {"GAME_DATE": "2026-06-01", "PLAYER_NAME": "Kelsey Plum", "PLAYER_ID": 1628276, "TEAM_ABBREVIATION": "LAS"},
            {"GAME_DATE": "2026-09-20", "PLAYER_NAME": "Kelsey Plum", "PLAYER_ID": 1628276, "TEAM_ABBREVIATION": "LVA"},
            {"GAME_DATE": "2026-09-21", "PLAYER_NAME": "Kate Martin", "PLAYER_ID": 1642801, "TEAM_ABBREVIATION": "GSV"},
            {"GAME_DATE": "2026-09-21", "PLAYER_NAME": "Tina Charles", "PLAYER_ID": 201600, "TEAM_ABBREVIATION": "MIN"},
            {"GAME_DATE": "2026-05-10", "PLAYER_NAME": "Niger Guard", "PLAYER_ID": 900001, "TEAM_ABBREVIATION": "NIGER"},
            {"GAME_DATE": "2025-09-01", "PLAYER_NAME": "Retired Player", "PLAYER_ID": 900002, "TEAM_ABBREVIATION": "SEA"},
            {"GAME_DATE": "2026-10-20", "PLAYER_NAME": "Future Row", "PLAYER_ID": 900003, "TEAM_ABBREVIATION": "ATL"},
        ]
    ).to_csv(processed / "player_logs.csv", index=False)
    # Daily snapshots: Jewell Loyd was OUT on 10-05 and is off the 10-06 report (returned).
    pd.DataFrame(
        [
            {"team": "ATL", "player": "Allisha Gray", "status": "OUT", "injury": "Out", "date": "2026-10-05"},
            {"team": "LVA", "player": "Kelsey Plum", "status": "OUT", "injury": "Out", "date": "2026-10-05"},
            {"team": "ATL", "player": "Allisha Gray", "status": "OUT", "injury": "Out", "date": "2026-10-06"},
            {"team": "NYL", "player": "Breanna Stewart", "status": "DAY-TO-DAY", "injury": "Knee", "date": "2026-10-06"},
        ]
    ).to_csv(raw / "injuries.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(league_status_module, "paths", test_paths)

    import nba_api.stats.endpoints as endpoints_module

    slates = {"00": ["ATL", "MIN", "GSW", "LAL"], "10": ["ATL", "NYL", "GSV", "LVA"]}
    calls: list[str] = []

    class _FakeScoreboardModule:
        class ScoreboardV2:
            def __init__(self, *args, league_id="00", **kwargs):
                calls.append(league_id)
                self._tris = slates.get(league_id, [])

            def get_normalized_dict(self):
                return {"LineScore": [{"TEAM_ABBREVIATION": tri} for tri in self._tris]}

    class _NoNbaRosterModule:
        class CommonTeamRoster:
            def __init__(self, *args, **kwargs):
                raise AssertionError("the NBA team roster endpoint must not be called")

    monkeypatch.setattr(endpoints_module, "scoreboardv2", _FakeScoreboardModule)
    monkeypatch.setattr(endpoints_module, "commonteamroster", _NoNbaRosterModule)
    return processed, calls


def test_roster_is_wnba_only_from_the_seasons_player_logs(tmp_path, monkeypatch):
    processed, _ = _setup(tmp_path, monkeypatch)
    df = league_status_module.build_league_status(DATE)
    assert sorted(set(df["team"])) == ["ATL", "GSV", "LVA", "MIN", "NYL"]
    assert sorted(df["player_name"]) == ["Allisha Gray", "Breanna Stewart", "Kate Martin", "Kelsey Plum", "Tina Charles"]
    assert (processed / f"league_status_{DATE}.csv").exists()


def test_slate_comes_from_the_wnba_scoreboard(tmp_path, monkeypatch):
    _, calls = _setup(tmp_path, monkeypatch)
    df = league_status_module.build_league_status(DATE)
    assert calls == ["10"]
    assert sorted(set(df.loc[df["team_on_slate"].astype(bool), "team"])) == ["ATL", "GSV", "LVA", "NYL"]


def test_injury_status_comes_from_the_latest_snapshot(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    df = league_status_module.build_league_status(DATE).set_index("player_name")
    assert df.loc["Allisha Gray", "injury_status"] == "OUT"
    assert df.loc["Allisha Gray", "playing_today"] == False  # noqa: E712
    # Plum was OUT on the 10-05 snapshot and is absent from 10-06: back, and on the slate.
    assert df.loc["Kelsey Plum", "injury_status"] == ""
    assert df.loc["Kelsey Plum", "playing_today"] == True  # noqa: E712
    assert df.loc["Breanna Stewart", "playing_today"] == True  # noqa: E712
    # A WNBA team not on the slate is not playing.
    assert df.loc["Tina Charles", "playing_today"] == False  # noqa: E712
