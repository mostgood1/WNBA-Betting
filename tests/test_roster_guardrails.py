from __future__ import annotations

import pandas as pd

from nba_betting import config as config_module
from nba_betting import availability as availability_module
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


def test_check_dressed_allows_explained_thin_team(tmp_path, monkeypatch):
    date_str = "2026-04-08"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    rows = []
    for idx in range(1, 7):
        rows.append(
            {
                "player_id": idx,
                "player_name": f"Active {idx}",
                "team": "MEM",
                "team_on_slate": True,
                "playing_today": True,
                "injury_status": "",
            }
        )
    for idx in range(7, 11):
        rows.append(
            {
                "player_id": idx,
                "player_name": f"Out {idx}",
                "team": "MEM",
                "team_on_slate": True,
                "playing_today": False,
                "injury_status": "OUT",
            }
        )

    pd.DataFrame(rows).to_csv(processed / f"league_status_{date_str}.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(availability_module, "paths", test_paths)

    result = availability_module.build_and_check_dressed_players(
        date_str,
        min_dressed_per_team=8,
        min_total_roster_per_team=10,
        fail_on_error=True,
    )

    assert result.ok is True
    assert result.summary["issues"] == []
    assert "team_dressed_thin_explained:MEM:6:4" in result.summary["warnings"]


def test_check_dressed_still_fails_unexplained_thin_team(tmp_path, monkeypatch):
    date_str = "2026-04-08"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    rows = []
    for idx in range(1, 7):
        rows.append(
            {
                "player_id": idx,
                "player_name": f"Active {idx}",
                "team": "MEM",
                "team_on_slate": True,
                "playing_today": True,
                "injury_status": "",
            }
        )
    for idx in range(7, 11):
        rows.append(
            {
                "player_id": idx,
                "player_name": f"Unknown {idx}",
                "team": "MEM",
                "team_on_slate": True,
                "playing_today": False,
                "injury_status": "",
            }
        )

    pd.DataFrame(rows).to_csv(processed / f"league_status_{date_str}.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(availability_module, "paths", test_paths)

    try:
        availability_module.build_and_check_dressed_players(
            date_str,
            min_dressed_per_team=8,
            min_total_roster_per_team=10,
            fail_on_error=True,
        )
    except RuntimeError as exc:
        assert "team_dressed_thin:MEM:6" in str(exc)
    else:
        raise AssertionError("expected build_and_check_dressed_players to fail")