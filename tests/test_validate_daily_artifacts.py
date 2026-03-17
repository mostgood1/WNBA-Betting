from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


def _load_validate_daily_artifacts_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "validate_daily_artifacts.py"
    spec = importlib.util.spec_from_file_location("validate_daily_artifacts_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_daily_artifacts_requires_props_lines_only_when_snapshot_rows_exist(tmp_path, monkeypatch):
    repo_root = tmp_path
    processed = repo_root / "data" / "processed"
    raw = repo_root / "data" / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

    date_str = "2026-03-14"
    yesterday = "2026-03-13"

    pd.DataFrame(
        [{"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)
    pd.DataFrame(
        [{"player_name": "Ja Morant", "team": "MEM", "pred_pts": 26.2}]
    ).to_csv(processed / f"props_predictions_{date_str}.csv", index=False)

    validate_module = _load_validate_daily_artifacts_module()
    monkeypatch.setenv("FAIL_ON_MISSING", "1")
    monkeypatch.setenv("REQUIRE_ODDS", "0")
    monkeypatch.setenv("REQUIRE_SMARTSIM", "0")
    monkeypatch.setenv("REQUIRE_PROPS_LINES", "1")
    monkeypatch.setenv("REQUIRE_ROTATIONS", "0")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_daily_artifacts.py",
            "--repo-root",
            str(repo_root),
            "--date",
            date_str,
            "--yesterday",
            yesterday,
        ],
    )
    assert validate_module.main() == 0

    pd.DataFrame(
        [
            {
                "player_name": "Ja Morant",
                "home_team": "Memphis Grizzlies",
                "away_team": "Detroit Pistons",
                "market": "player_points",
                "point": 24.5,
                "outcome_name": "Over",
            }
        ]
    ).to_csv(raw / f"odds_nba_player_props_{date_str}.csv", index=False)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_daily_artifacts.py",
            "--repo-root",
            str(repo_root),
            "--date",
            date_str,
            "--yesterday",
            yesterday,
        ],
    )
    assert validate_module.main() == 3


def test_validate_daily_artifacts_flags_missing_props_slate_team(tmp_path, monkeypatch):
    repo_root = tmp_path
    processed = repo_root / "data" / "processed"
    raw = repo_root / "data" / "raw"
    processed.mkdir(parents=True)
    raw.mkdir(parents=True)

    date_str = "2026-03-14"
    yesterday = "2026-03-13"

    pd.DataFrame(
        [{"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}]
    ).to_csv(processed / f"predictions_{date_str}.csv", index=False)
    pd.DataFrame(
        [{"home_team": "Memphis Grizzlies", "visitor_team": "Detroit Pistons"}]
    ).to_csv(processed / f"game_odds_{date_str}.csv", index=False)
    pd.DataFrame(
        [{"player_name": "Ja Morant", "team": "MEM", "pred_pts": 26.2}]
    ).to_csv(processed / f"props_predictions_{date_str}.csv", index=False)

    validate_module = _load_validate_daily_artifacts_module()
    monkeypatch.setenv("FAIL_ON_MISSING", "1")
    monkeypatch.setenv("REQUIRE_ODDS", "0")
    monkeypatch.setenv("REQUIRE_SMARTSIM", "0")
    monkeypatch.setenv("REQUIRE_PROPS_LINES", "0")
    monkeypatch.setenv("REQUIRE_ROTATIONS", "0")

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_daily_artifacts.py",
            "--repo-root",
            str(repo_root),
            "--date",
            date_str,
            "--yesterday",
            yesterday,
        ],
    )

    assert validate_module.main() == 3

    report = json.loads((processed / f"daily_artifacts_{date_str}.json").read_text(encoding="utf-8"))
    assert report["props_missing_teams"] == ["DET"]