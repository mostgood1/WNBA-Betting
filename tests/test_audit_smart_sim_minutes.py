from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_audit_smart_sim_minutes_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "audit_smart_sim_minutes.py"
    spec = importlib.util.spec_from_file_location("audit_smart_sim_minutes_test", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_audit_smart_sim_minutes_skips_when_cards_sim_detail_exists(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    date_str = "2026-05-15"
    (processed / f"cards_sim_detail_{date_str}.json").write_text(
        json.dumps(
            {
                "date": date_str,
                "games": [
                    {
                        "home_tri": "IND",
                        "away_tri": "WSH",
                        "sim": {"players_summary": {"home": 8, "away": 8}},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    audit_module = _load_audit_smart_sim_minutes_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_smart_sim_minutes.py",
            "--date",
            date_str,
            "--processed",
            str(processed),
        ],
    )

    assert audit_module.main() == 0


def test_audit_smart_sim_minutes_fails_when_no_inputs_exist(tmp_path, monkeypatch):
    processed = tmp_path / "data" / "processed"
    processed.mkdir(parents=True)

    date_str = "2026-05-15"
    audit_module = _load_audit_smart_sim_minutes_module()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "audit_smart_sim_minutes.py",
            "--date",
            date_str,
            "--processed",
            str(processed),
        ],
    )

    assert audit_module.main() == 2
