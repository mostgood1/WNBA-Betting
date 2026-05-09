from __future__ import annotations

import json

from nba_betting import config as config_module
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


def test_data_root_prefers_render_mount_when_env_missing(tmp_path, monkeypatch):
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True)
    render_root = tmp_path / "render-data"
    render_root.mkdir(parents=True)

    monkeypatch.delenv(config_module.LEAGUE.data_root_env, raising=False)
    monkeypatch.delenv(config_module.LEAGUE.legacy_data_root_env, raising=False)
    monkeypatch.setattr(config_module, "_RENDER_DEFAULT_DATA_ROOT", render_root)

    assert config_module._data_root(repo_root) == render_root


def test_reconcile_repo_data_to_active_detects_non_repo_root_without_env(tmp_path, monkeypatch):
    repo_data_root = tmp_path / "repo" / "data"
    active_data_root = tmp_path / "active" / "data"
    repo_processed = repo_data_root / "processed"
    active_processed = active_data_root / "processed"
    repo_raw = repo_data_root / "raw"
    active_raw = active_data_root / "raw"
    repo_overrides = repo_data_root / "overrides"
    active_overrides = active_data_root / "overrides"

    repo_processed.mkdir(parents=True)
    active_processed.mkdir(parents=True)
    repo_raw.mkdir(parents=True)
    active_raw.mkdir(parents=True)
    repo_overrides.mkdir(parents=True)
    active_overrides.mkdir(parents=True)

    source_file = repo_processed / "live_lens_projections_2026-05-09.jsonl"
    source_file.write_text('{"ok": true}\n', encoding="utf-8")

    old_values = {
        "repo_data_root": config_module.paths.repo_data_root,
        "data_root": config_module.paths.data_root,
        "repo_data_raw": config_module.paths.repo_data_raw,
        "data_raw": config_module.paths.data_raw,
        "repo_data_processed": config_module.paths.repo_data_processed,
        "data_processed": config_module.paths.data_processed,
        "repo_data_overrides": config_module.paths.repo_data_overrides,
        "data_overrides": config_module.paths.data_overrides,
    }
    object.__setattr__(config_module.paths, "repo_data_root", repo_data_root)
    object.__setattr__(config_module.paths, "data_root", active_data_root)
    object.__setattr__(config_module.paths, "repo_data_raw", repo_raw)
    object.__setattr__(config_module.paths, "data_raw", active_raw)
    object.__setattr__(config_module.paths, "repo_data_processed", repo_processed)
    object.__setattr__(config_module.paths, "data_processed", active_processed)
    object.__setattr__(config_module.paths, "repo_data_overrides", repo_overrides)
    object.__setattr__(config_module.paths, "data_overrides", active_overrides)
    monkeypatch.delenv(config_module.LEAGUE.data_root_env, raising=False)
    monkeypatch.delenv(config_module.LEAGUE.legacy_data_root_env, raising=False)

    try:
        result = config_module.reconcile_repo_data_to_active()
    finally:
        for name, value in old_values.items():
            object.__setattr__(config_module.paths, name, value)

    assert result["ok"] is True
    assert result["skipped"] is False
    assert result["files_copied"] == 1
    assert (active_processed / source_file.name).exists()