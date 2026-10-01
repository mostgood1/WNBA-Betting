"""PBP engine calibration: shooter free throws, fouled-miss accounting, landing on the points target.

Measured in the Syndicate deployment's component backtests (2026-10-01, actual minutes,
possessions and team points held fixed):
- free throws were drawn from a TEAM rate before the shooter was chosen, so top-2
  scorers drew 0.220 FTA/FGA vs 0.376 real;
- a fouled miss was counted as a missed FGA (sim FGA 1.25x actual);
- eff_mult missed its own target (+2.9 pts/team) and the team prior stacked on it.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from wnba_betting.sim import events

def _ft_team(star_fta_pm: float, n: int = 10) -> pd.DataFrame:
    fga = np.array([0.55, 0.45, 0.40, 0.35, 0.30, 0.25, 0.25, 0.20, 0.20, 0.15])[:n]
    fta = np.array([star_fta_pm] + [0.06] * (n - 1))
    mins = np.array([34, 32, 30, 28, 22, 18, 14, 10, 8, 4], dtype=float)[:n]
    return pd.DataFrame({
        "player_name": [f"P{i}" for i in range(n)], "_sim_min": mins,
        "_prior_fga_pm": fga, "_prior_fgm_pm": fga * 0.45, "_prior_threes_att_pm": fga * 0.3, "_prior_threes_pm": fga * 0.1,
        "_prior_fta_pm": fta, "_prior_ftm_pm": fta * 0.8, "_prior_pts_pm": fga * 1.1, "_prior_reb_pm": [0.15] * n,
        "_prior_ast_pm": [0.08] * n, "_prior_stl_pm": [0.03] * n, "_prior_blk_pm": [0.02] * n, "_prior_tov_pm": [0.05] * n,
        "_prior_pf_pm": [0.08] * n, "pred_pts": fga * 30,
    })


def test_multipliers_are_shot_weighted_mean_one_and_favour_foul_drawers():
    team = _ft_team(0.30)
    mult = events._ft_rate_multipliers(team, team["_sim_min"].to_numpy())
    w = team["_prior_fga_pm"].to_numpy() * team["_sim_min"].to_numpy()
    assert float((mult * w).sum() / w.sum()) == pytest.approx(1.0)
    # The star (5x the FTA rate of everyone else) gets by far the largest multiplier;
    # it is a per-SHOT ratio, so low-volume players with the same FTA rank above the mid-rotation.
    assert mult[0] == mult.max() and mult[0] > 1.8
    assert mult[1] < 1.0


def test_unknown_rates_get_a_neutral_multiplier():
    team = _ft_team(0.30)
    team.loc[3, "_prior_fga_pm"] = 0.0
    assert events._ft_rate_multipliers(team, team["_sim_min"].to_numpy())[3] == 1.0


def _star_ft_share(flag: bool) -> tuple[float, float]:
    events.SHOOTER_FT_RATE = flag
    try:
        rng = np.random.default_rng(3)
        star_fta = star_fga = team_fta = 0
        for _ in range(60):
            hb, _ab, _hq, _aq = events.simulate_pbp_game_boxscore(rng, _ft_team(0.30), _ft_team(0.06), target_home_points=85.0, target_away_points=85.0)
            p0 = hb["players"][0]
            star_fta += p0["fta"]
            star_fga += p0["fga"]
            team_fta += sum(p["fta"] for p in hb["players"])
        return star_fta / max(star_fga, 1), team_fta / 60
    finally:
        events.SHOOTER_FT_RATE = True


def test_reachability_the_switch_changes_who_shoots_free_throws():
    off_rate, off_team = _star_ft_share(False)
    on_rate, on_team = _star_ft_share(True)
    assert on_rate > off_rate * 1.5           # the star now draws fouls at her own rate
    assert on_team == pytest.approx(off_team, rel=0.25)  # the team's free throws are not inflated


FLAGS = ("FOULED_MISS_NOT_FGA", "EXACT_TARGET_CALIBRATION", "TEAM_PRIOR_STACKS_ON_TARGET")


@pytest.fixture(autouse=True)
def _restore_flags():
    saved = {f: getattr(events, f) for f in FLAGS}
    yield
    for f, v in saved.items():
        setattr(events, f, v)


def _team(n: int = 10) -> pd.DataFrame:
    fga = np.array([0.55, 0.45, 0.40, 0.35, 0.30, 0.25, 0.25, 0.20, 0.20, 0.15])[:n]
    tpa = fga * np.array([0.25, 0.5, 0.3, 0.45, 0.2, 0.4, 0.35, 0.3, 0.5, 0.2])[:n]
    mins = np.array([34, 32, 30, 28, 22, 18, 14, 10, 8, 4], dtype=float)[:n]
    fta = np.array([0.30] + [0.08] * (n - 1))
    return pd.DataFrame({
        "player_name": [f"P{i}" for i in range(n)], "_sim_min": mins,
        "_prior_fga_pm": fga, "_prior_threes_att_pm": tpa, "_prior_threes_pm": tpa * 0.34,
        "_prior_fgm_pm": (fga - tpa) * 0.52 + tpa * 0.34,
        "_prior_fta_pm": fta, "_prior_ftm_pm": fta * 0.8, "_prior_pts_pm": fga * 1.1, "_prior_reb_pm": [0.15] * n,
        "_prior_ast_pm": [0.08] * n, "_prior_stl_pm": [0.03] * n, "_prior_blk_pm": [0.02] * n, "_prior_tov_pm": [0.055] * n,
        "_prior_pf_pm": [0.08] * n, "pred_pts": fga * 30,
    })


def _run(n: int = 80, seed: int = 11, **kw) -> dict:
    rng = np.random.default_rng(seed)
    tot = {"pts": 0.0, "fga": 0.0, "fgm": 0.0, "fta": 0.0}
    for _ in range(n):
        hb, _ab, _hq, _aq = events.simulate_pbp_game_boxscore(rng, _team(), _team(), **kw)
        for k in tot:
            tot[k] += sum(float(p[k]) for p in hb["players"]) / n
    return tot


def test_loop_shot_share_is_a_distribution_flatter_than_raw_volume():
    team = _team()
    mins = team["_sim_min"].to_numpy()
    share = events._loop_shot_share(team, mins, "_prior_fga_pm")
    raw = team["_prior_fga_pm"].to_numpy() * mins
    raw = raw / raw.sum()
    assert share.sum() == pytest.approx(1.0)
    assert share[0] < raw[0]          # the loop compresses the top shooter's volume
    assert share[-1] > raw[-1]        # and floors the end of the bench


def test_solver_inverts_the_points_per_possession_model():
    args = dict(p_tov=0.15, p3=0.36, fg2=0.50, fg3=0.34, foul=0.20, ft=0.80, oreb=0.24)
    for target in (0.95, 1.05, 1.15):
        eff = events._solve_eff_mult(target, **args)
        assert events._loop_points_per_possession(eff=eff, **args) == pytest.approx(target, abs=1e-6)
    lo = events._loop_points_per_possession(eff=0.9, **args)
    assert events._loop_points_per_possession(eff=1.1, **args) > lo


def test_reachability_fouled_miss_is_not_an_fga_and_scores_identically():
    events.FOULED_MISS_NOT_FGA = False
    off = _run(target_home_points=85.0, target_away_points=85.0)
    events.FOULED_MISS_NOT_FGA = True
    on = _run(target_home_points=85.0, target_away_points=85.0)
    assert on["fga"] < off["fga"] * 0.97        # fouled misses leave the FGA column
    assert on["fgm"] == off["fgm"] and on["pts"] == off["pts"]  # same draws: accounting only


def test_reachability_exact_calibration_lands_on_the_target():
    for target in (78.0, 92.0):
        events.EXACT_TARGET_CALIBRATION = False
        off = _run(target_home_points=target, target_away_points=target)["pts"]
        events.EXACT_TARGET_CALIBRATION = True
        on = _run(target_home_points=target, target_away_points=target)["pts"]
        assert abs(on - target) < 2.5
        assert abs(on - target) < abs(off - target)


def test_team_prior_does_not_stack_on_a_target_but_still_applies_without_one():
    adj = {"eff_mult": 1.10}
    kw = dict(target_home_points=85.0, target_away_points=85.0, home_team_adj=adj, away_team_adj=adj)
    events.TEAM_PRIOR_STACKS_ON_TARGET = True
    stacked = _run(**kw)["pts"]
    events.TEAM_PRIOR_STACKS_ON_TARGET = False
    unstacked = _run(**kw)["pts"]
    assert stacked > unstacked + 4.0           # off != on
    assert abs(unstacked - 85.0) < 2.5
    # No target: the prior is the only efficiency signal and must still apply.
    plain = _run(n=60)["pts"]
    boosted = _run(n=60, home_team_adj=adj, away_team_adj=adj)["pts"]
    assert boosted > plain + 4.0
