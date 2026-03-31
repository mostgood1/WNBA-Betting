from __future__ import annotations

import pandas as pd

from nba_betting import config as config_module
from nba_betting.scrapers import injuries as injuries_module


def test_update_injuries_remaps_fallback_team_to_roster(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    raw_dir = data_root / "raw"
    processed_dir = data_root / "processed"
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    rosters = pd.DataFrame(
        [
            {"PLAYER": "Jalen Suggs", "TEAM_ABBREVIATION": "ORL"},
            {"PLAYER": "Devin Booker", "TEAM_ABBREVIATION": "PHX"},
        ]
    )
    rosters.to_csv(processed_dir / "rosters_2025-26.csv", index=False)

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(injuries_module, "paths", test_paths)

    class _FakeOfficial:
        def get_injuries_for_date(self, _date_str):
            return pd.DataFrame()

    class _FakeRotowire:
        def get_all_injuries(self):
            return pd.DataFrame()

    bad_fallback = pd.DataFrame(
        [
            {"team": "OKC", "player": "Jalen Suggs", "status": "OUT", "injury": "Ankle", "date": "2026-03-31"},
            {"team": "PHI", "player": "Devin Booker", "status": "OUT", "injury": "Hamstring", "date": "2026-03-31"},
        ]
    )

    monkeypatch.setattr(injuries_module, "NBAOfficialInjuryReportScraper", _FakeOfficial)
    monkeypatch.setattr(injuries_module, "RotowireInjuryScraper", _FakeRotowire)
    monkeypatch.setattr(injuries_module.ESPNInjuryScraper, "get_all_injuries", lambda self: bad_fallback.copy())

    db = injuries_module.NBAInjuryDatabase(filepath=str(raw_dir / "injuries.csv"))
    out = db.update_injuries(date_str="2026-03-31")

    pairs = {(str(row["player"]), str(row["team"])) for _, row in out.iterrows()}
    assert ("Jalen Suggs", "ORL") in pairs
    assert ("Devin Booker", "PHX") in pairs
    assert ("Jalen Suggs", "OKC") not in pairs
    assert ("Devin Booker", "PHI") not in pairs