from __future__ import annotations

import json

from nba_betting import prob_calibration
from nba_betting.sim import quarters


def test_prob_calibration_prefers_repo_copy_over_active_data_root(tmp_path, monkeypatch):
    repo_processed = tmp_path / "repo" / "data" / "processed"
    active_processed = tmp_path / "active" / "data" / "processed"
    repo_processed.mkdir(parents=True)
    active_processed.mkdir(parents=True)

    fname = "calibration_period_probs_2026-04-07.json"
    (repo_processed / fname).write_text(
        json.dumps({"markets": {"q1_over": {"bin_edges": [0.0, 1.0], "p_cal": [0.8], "n_bin": [100]}}}),
        encoding="utf-8",
    )
    (active_processed / fname).write_text(
        json.dumps({"markets": {"q1_over": {"bin_edges": [0.0, 1.0], "p_cal": [0.2], "n_bin": [100]}}}),
        encoding="utf-8",
    )

    old_repo_processed = prob_calibration.paths.repo_data_processed
    old_active_processed = prob_calibration.paths.data_processed
    object.__setattr__(prob_calibration.paths, "repo_data_processed", repo_processed)
    object.__setattr__(prob_calibration.paths, "data_processed", active_processed)
    monkeypatch.setattr(prob_calibration, "_PROB_CALIBRATION_INDEX", None)
    monkeypatch.setattr(prob_calibration, "_PROB_CALIBRATION_CACHE", {})

    try:
        out = prob_calibration.calibrate_prob("2026-04-08", "q1_over", 0.6)
    finally:
        object.__setattr__(prob_calibration.paths, "repo_data_processed", old_repo_processed)
        object.__setattr__(prob_calibration.paths, "data_processed", old_active_processed)

    assert abs(out - 0.76) < 1e-9


def test_quarters_blend_weights_prefer_repo_copy_over_active_data_root(tmp_path, monkeypatch):
    repo_processed = tmp_path / "repo" / "data" / "processed"
    active_processed = tmp_path / "active" / "data" / "processed"
    repo_processed.mkdir(parents=True)
    active_processed.mkdir(parents=True)

    (repo_processed / "quarters_blend_weights.json").write_text(
        json.dumps({"total_w": 0.55, "margin_w": 0.85}),
        encoding="utf-8",
    )
    (active_processed / "quarters_blend_weights.json").write_text(
        json.dumps({"total_w": 0.15, "margin_w": 0.25}),
        encoding="utf-8",
    )

    old_repo_processed = quarters.paths.repo_data_processed
    old_active_processed = quarters.paths.data_processed
    object.__setattr__(quarters.paths, "repo_data_processed", repo_processed)
    object.__setattr__(quarters.paths, "data_processed", active_processed)
    monkeypatch.setattr(quarters, "_DEFAULT_BLEND_TOTAL_W", 0.7)
    monkeypatch.setattr(quarters, "_DEFAULT_BLEND_MARGIN_W", 0.95)

    try:
        total_w, margin_w = quarters._load_default_blend_weights()
    finally:
        object.__setattr__(quarters.paths, "repo_data_processed", old_repo_processed)
        object.__setattr__(quarters.paths, "data_processed", old_active_processed)

    assert total_w == 0.55
    assert margin_w == 0.85