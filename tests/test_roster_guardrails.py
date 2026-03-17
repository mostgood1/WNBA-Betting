from __future__ import annotations

import pandas as pd

from nba_betting import config as config_module
from nba_betting import roster_checks as roster_checks_module
from nba_betting import roster_files as roster_files_module
from nba_betting import rosters as rosters_module


def test_pick_roster_file_prefers_more_complete_candidate(tmp_path):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame([{"TEAM_ABBREVIATION": "MIN"}, {"TEAM_ABBREVIATION": "PHX"}]).to_csv(
        processed / "rosters_2025-26.csv",
        index=False,
    )
    pd.DataFrame(
        [
            {"TEAM_ABBREVIATION": "MIN"},
            {"TEAM_ABBREVIATION": "PHX"},
            {"TEAM_ABBREVIATION": "DET"},
        ]
    ).to_csv(processed / "rosters_2025.csv", index=False)

    picked = roster_files_module.pick_rosters_file(processed, season="2025-26")

    assert picked == processed / "rosters_2025.csv"


def test_roster_sanity_check_flags_missing_slate_team_in_rosters(tmp_path, monkeypatch):
    date_str = "2026-03-17"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "player_id": 1,
                "player_name": "Anthony Edwards",
                "team": "MIN",
                "team_on_slate": True,
                "playing_today": True,
            },
            {
                "player_id": 2,
                "player_name": "Devin Booker",
                "team": "PHX",
                "team_on_slate": True,
                "playing_today": True,
            },
        ]
    ).to_csv(processed / f"league_status_{date_str}.csv", index=False)

    pd.DataFrame(
        [
            {"PLAYER_ID": 1, "PLAYER": "Anthony Edwards", "TEAM_ABBREVIATION": "MIN"},
        ]
    ).to_csv(processed / "rosters_2025-26.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(roster_checks_module, "paths", test_paths)

    result = roster_checks_module.roster_sanity_check(
        date_str,
        min_total_roster_per_team=1,
        min_playing_today_per_team=0,
    )

    assert not result.ok
    assert any("season rosters missing slate teams" in issue for issue in result.issues)
    assert result.summary["season_roster_missing_slate_teams"] == ["PHX"]


def test_fetch_rosters_retries_missing_teams_after_partial_pass(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(rosters_module, "paths", test_paths)

    teams = [
        {"id": 1, "abbreviation": "ATL", "full_name": "Atlanta Hawks"},
        {"id": 2, "abbreviation": "PHX", "full_name": "Phoenix Suns"},
    ]
    monkeypatch.setattr(rosters_module.static_teams, "get_teams", lambda: teams)

    calls = {"ATL": 0, "PHX": 0}

    class _FakeCommonTeamRoster:
        def __init__(self, team_id: int, season: str, timeout: int):
            self.team_id = team_id

        def get_normalized_dict(self):
            tri = "ATL" if self.team_id == 1 else "PHX"
            calls[tri] += 1
            if tri == "PHX" and calls[tri] == 1:
                raise RuntimeError("temporary failure")
            return {
                "CommonTeamRoster": [
                    {
                        "PLAYER": f"{tri} Player",
                        "PLAYER_ID": self.team_id,
                    }
                ]
            }

    monkeypatch.setattr(rosters_module.commonteamroster, "CommonTeamRoster", _FakeCommonTeamRoster)

    out = rosters_module.fetch_rosters(
        season="2025-26",
        rate_delay=0.0,
        max_retries=1,
        request_timeout=5,
        persist_every=0,
    )

    assert set(out["TEAM_ABBREVIATION"].astype(str).str.upper()) == {"ATL", "PHX"}
    assert calls["PHX"] == 2