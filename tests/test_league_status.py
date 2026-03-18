from __future__ import annotations

import pandas as pd

from nba_betting import config as config_module
from nba_betting import league_status as league_status_module


def test_today_slate_team_tricodes_ignores_stale_schedule_when_live_sources_exist(tmp_path, monkeypatch):
    date_str = "2026-03-18"
    data_root = tmp_path / "data"
    processed = data_root / "processed"
    processed.mkdir(parents=True)

    pd.DataFrame(
        [{"home_team": "Memphis Grizzlies", "visitor_team": "Denver Nuggets"}]
    ).to_csv(processed / f"game_odds_{date_str}.csv", index=False)
    pd.DataFrame(
        [
            {
                "game_id": "0022501003",
                "date_est": date_str,
                "date_utc": date_str,
                "home_tricode": "MEM",
                "away_tricode": "NYK",
            }
        ]
    ).to_json(processed / "schedule_2025_26.json", orient="records")

    test_paths = config_module.Paths(root=tmp_path, repo_data_root=data_root, data_root=data_root)
    monkeypatch.setattr(config_module, "paths", test_paths)
    monkeypatch.setattr(league_status_module, "paths", test_paths)

    import nba_api.stats.endpoints as endpoints_module

    class _BoomScoreboardModule:
        class ScoreboardV2:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("scoreboard unavailable")

    monkeypatch.setattr(endpoints_module, "scoreboardv2", _BoomScoreboardModule)

    tris = league_status_module._today_slate_team_tricodes(date_str)

    assert tris == {"DEN", "MEM"}