"""No ScoreboardV2 call in this package may read the NBA slate.

nba_api's ScoreboardV2 defaults to league_id "00", the NBA. Called without it from this package it returns NBA
games: predictions_<date>.csv held NBA preseason games (#9) and league_status_<date>.csv flagged NBA teams on slate
(#10). The AST guard covers the whole package; the other test checks the reachable callers end to end.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd

import nba_api.stats.endpoints.scoreboardv2 as scoreboard_module
from wnba_betting import boxscores, finals, pbp

PKG = Path(boxscores.__file__).resolve().parent


def test_no_scoreboard_call_in_the_package_omits_league_id():
    missing = []
    for path in sorted(PKG.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else (func.id if isinstance(func, ast.Name) else "")
            if name == "ScoreboardV2" and not any(kw.arg == "league_id" for kw in node.keywords):
                missing.append(f"{path.relative_to(PKG)}:{node.lineno}")
    assert missing == []


def test_reachable_callers_read_the_wnba_slate_only(monkeypatch):
    # What the real API returns: the NBA slate under "00" (the default), the WNBA one under "10".
    slates = {
        "00": [(1, 1610612737, "ATL", 1610612750, "MIN")],
        "10": [(3, 1611661330, "ATL", 1611661313, "NYL"), (4, 1611661317, "PHO", 1611661322, "WAS")],
    }
    calls = []

    class FakeScoreboard:
        def __init__(self, game_date=None, day_offset=0, league_id="00", timeout=None, **kw):
            calls.append(league_id)
            self.rows = slates.get(league_id, [])

        def get_normalized_dict(self):
            gh = [{"GAME_ID": g, "HOME_TEAM_ID": h, "VISITOR_TEAM_ID": v, "GAME_STATUS_ID": 3} for g, h, _, v, _ in self.rows]
            ls = [{"GAME_ID": g, "TEAM_ID": t, "TEAM_ABBREVIATION": a, "PTS": 80}
                  for g, h, ha, v, va in self.rows for t, a in ((h, ha), (v, va))]
            return {"GameHeader": gh, "LineScore": ls}

    def no_network(*args, **kwargs):
        raise AssertionError("finals must not read the NBA CDN for the WNBA")

    monkeypatch.setattr(scoreboard_module, "ScoreboardV2", FakeScoreboard)
    monkeypatch.setattr(finals, "_scoreboardv2", scoreboard_module)
    monkeypatch.setattr("requests.get", no_network)

    box_ids = sorted(int(x) for x in boxscores._scoreboard_games("2026-10-07")["GAME_ID"])
    pbp_ids = sorted(int(x) for x in pbp._scoreboard_games("2026-10-07")["GAME_ID"])
    fin = finals._finals_from_stats("2026-10-07")
    cdn = finals._finals_from_cdn("2026-10-07")

    assert calls == ["10", "10", "10"]
    assert box_ids == [3, 4]
    assert pbp_ids == [3, 4]
    # stats-API spellings PHO/WAS map to this package's PHX/WSH
    assert sorted(zip(fin["home_tri"], fin["away_tri"])) == [("ATL", "NYL"), ("PHX", "WSH")]
    assert isinstance(cdn, pd.DataFrame) and cdn.empty
