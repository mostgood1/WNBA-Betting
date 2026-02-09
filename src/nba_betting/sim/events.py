from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class EventSimConfig:
    # Pace / possession controls
    possessions_per_game: float = 98.0
    possessions_jitter: float = 0.06  # stddev fraction applied per quarter

    # Outcome priors (fallbacks when player priors are missing)
    base_tov_per_poss: float = 0.125
    base_shooting_foul_per_fga: float = 0.095
    base_nonshooting_foul_per_poss: float = 0.05
    base_oreb_rate: float = 0.24

    # Defense event rates (fallbacks)
    base_steal_share_of_tov: float = 0.55
    base_block_rate_on_2pa: float = 0.05

    # Blowout / gameflow
    blowout_margin: int = 18
    blowout_q4_margin: int = 15
    garbage_time_pace_scale: float = 0.94
    garbage_time_eff_scale: float = 0.96
    bench_weight_boost: float = 1.35

    # Reconciliation strength
    reconcile_points: bool = True
    reconcile_max_changes_per_quarter: int = 12


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if df is None or df.empty or col not in df.columns:
        return pd.Series([0.0] * (0 if df is None else len(df)), dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _pick_weighted(rng: np.random.Generator, items: List[Any], weights: np.ndarray) -> Any:
    if not items:
        return None
    w = np.asarray(weights, dtype=float)
    if w.size != len(items):
        w = np.ones(len(items), dtype=float)
    w = np.maximum(0.0, w)
    s = float(np.sum(w))
    if not np.isfinite(s) or s <= 0:
        p = np.full(len(items), 1.0 / len(items))
    else:
        p = w / s
    idx = int(rng.choice(len(items), p=p))
    return items[idx]


def _sample_lineup(
    rng: np.random.Generator,
    players: pd.DataFrame,
    minutes_weights: np.ndarray,
    k: int = 5,
    blowout_boost_bench: bool = False,
    bench_boost: float = 1.35,
) -> List[int]:
    """Return indices into players of a sampled on-court 5-man unit."""
    n = int(len(players))
    if n <= 0:
        return []
    w = np.asarray(minutes_weights, dtype=float)
    if w.size != n:
        w = np.ones(n, dtype=float)
    w = np.maximum(0.0, w)

    # Bench boost: slightly de-emphasize top-minute guys
    if blowout_boost_bench and n >= 8:
        order = np.argsort(-w)
        top = order[:5]
        rest = order[5:]
        w[top] = w[top] / max(1.0, bench_boost)
        w[rest] = w[rest] * bench_boost

    # Sample without replacement, but be robust if weights degenerate
    s = float(np.sum(w))
    if not np.isfinite(s) or s <= 0:
        probs = np.full(n, 1.0 / n)
    else:
        probs = w / s

    k_eff = int(min(k, n))
    try:
        idx = rng.choice(n, size=k_eff, replace=False, p=probs)
        return [int(i) for i in idx]
    except Exception:
        # Fallback: take top-k by minutes
        order = np.argsort(-w)
        return [int(i) for i in order[:k_eff]]


def _team_rates_from_priors(players: pd.DataFrame, cfg: EventSimConfig) -> Dict[str, float]:
    """Derive team-level per-possession / per-FGA rates from player per-minute priors."""
    mins = _safe_series(players, "_sim_min").to_numpy(dtype=float)
    total_min = float(np.sum(np.maximum(0.0, mins)))
    if total_min <= 0:
        total_min = 240.0

    def per_game_from_pm(col_pm: str) -> float:
        pm = _safe_series(players, col_pm).to_numpy(dtype=float)
        return float(np.sum(np.maximum(0.0, pm) * np.maximum(0.0, mins)))

    fga = per_game_from_pm("_prior_fga_pm")
    fg3a = per_game_from_pm("_prior_threes_att_pm")
    fta = per_game_from_pm("_prior_fta_pm")
    tov = per_game_from_pm("_prior_tov_pm")
    pf = per_game_from_pm("_prior_pf_pm")

    poss = max(60.0, float(cfg.possessions_per_game))

    p_tov = float(np.clip(tov / poss, 0.05, 0.22)) if np.isfinite(tov) and tov > 0 else cfg.base_tov_per_poss
    p3 = float(np.clip(fg3a / max(1.0, fga), 0.18, 0.55)) if np.isfinite(fga) and fga > 0 else 0.36
    foul_per_fga = float(np.clip(fta / max(1.0, fga), 0.05, 0.20)) if np.isfinite(fga) and fga > 0 else cfg.base_shooting_foul_per_fga

    # PF per possession (includes non-shooting; rough)
    pf_per_poss = float(np.clip(pf / poss, 0.10, 0.30)) if np.isfinite(pf) and pf > 0 else 0.18

    return {
        "poss": poss,
        "p_tov": p_tov,
        "p3": p3,
        "foul_per_fga": foul_per_fga,
        "pf_per_poss": pf_per_poss,
    }


def _player_pct(players: pd.DataFrame, made_pm: str, att_pm: str, default: float, lo: float, hi: float) -> np.ndarray:
    made = _safe_series(players, made_pm).to_numpy(dtype=float)
    att = _safe_series(players, att_pm).to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        pct = np.where(att > 0, made / att, np.nan)
    pct = np.where(np.isfinite(pct), pct, default)
    return np.clip(pct, lo, hi)


def _player_usage_weights(players: pd.DataFrame, col_pm: str, lineup_idx: List[int]) -> np.ndarray:
    """Return selection weights for the current on-court lineup.

    Key realism guardrail:
    - We *blend* priors with minutes so missing/zero priors don't collapse usage onto a single player.
      This was a major cause of "inflated" statlines (one player with nonzero priors getting nearly
      all shots/assists/rebounds simply because others had 0 priors).
    """
    n = int(len(players))
    if n <= 0:
        return np.zeros(0, dtype=float)

    pm = _safe_series(players, col_pm).to_numpy(dtype=float)
    pm = np.maximum(0.0, np.where(np.isfinite(pm), pm, 0.0))
    # Compress outliers (robust even if a bad prior slips through).
    pm = np.log1p(pm)

    mins = _safe_series(players, "_sim_min").to_numpy(dtype=float)
    mins = np.maximum(0.0, np.where(np.isfinite(mins), mins, 0.0))

    w = np.zeros(n, dtype=float)
    idx = [int(i) for i in (lineup_idx or []) if 0 <= int(i) < n]
    if not idx:
        return w

    pm_line = pm[idx]
    mins_line = mins[idx]

    # Minutes provide a stable floor so everyone can accrue events.
    mins_floor = np.maximum(1.0, mins_line)
    s_m = float(mins_floor.sum())
    mins_norm = (mins_floor / s_m) if np.isfinite(s_m) and s_m > 0 else np.full(len(idx), 1.0 / len(idx))

    s_p = float(pm_line.sum())
    if (not np.isfinite(s_p)) or s_p <= 0:
        probs = mins_norm
    else:
        pm_norm = pm_line / s_p
        # Priors-heavy but never priors-only.
        pri_weight = 0.75
        probs = pri_weight * pm_norm + (1.0 - pri_weight) * mins_norm

    probs = np.maximum(0.0, probs)
    s = float(probs.sum())
    if not np.isfinite(s) or s <= 0:
        probs = np.full(len(idx), 1.0 / len(idx))
    else:
        probs = probs / s

    # Convert back into weights on the full player index space.
    for j, i in enumerate(idx):
        w[int(i)] = float(probs[j])
    return w


def simulate_event_level_boxscore(
    rng: np.random.Generator,
    home_players: pd.DataFrame,
    away_players: pd.DataFrame,
    home_q_pts: List[int],
    away_q_pts: List[int],
    cfg: Optional[EventSimConfig] = None,
    home_lineups: Optional[List[List[int]]] = None,
    home_lineup_weights: Optional[np.ndarray] = None,
    away_lineups: Optional[List[List[int]]] = None,
    away_lineup_weights: Optional[np.ndarray] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Simulate an event-driven representative boxscore.

    Returns (home_box, away_box). Each box includes a minimal event log under key 'events'.
    """
    cfg = cfg or EventSimConfig()

    # Precompute weights and rates
    h_mins = _safe_series(home_players, "_sim_min").to_numpy(dtype=float)
    a_mins = _safe_series(away_players, "_sim_min").to_numpy(dtype=float)
    h_rates = _team_rates_from_priors(home_players, cfg)
    a_rates = _team_rates_from_priors(away_players, cfg)

    # Player shooting pcts
    h_fg_pct = _player_pct(home_players, "_prior_fgm_pm", "_prior_fga_pm", default=0.46, lo=0.25, hi=0.75)
    a_fg_pct = _player_pct(away_players, "_prior_fgm_pm", "_prior_fga_pm", default=0.46, lo=0.25, hi=0.75)
    h_3p_pct = _player_pct(home_players, "_prior_threes_pm", "_prior_threes_att_pm", default=0.35, lo=0.20, hi=0.50)
    a_3p_pct = _player_pct(away_players, "_prior_threes_pm", "_prior_threes_att_pm", default=0.35, lo=0.20, hi=0.50)
    h_ft_pct = _player_pct(home_players, "_prior_ftm_pm", "_prior_fta_pm", default=0.76, lo=0.45, hi=0.95)
    a_ft_pct = _player_pct(away_players, "_prior_ftm_pm", "_prior_fta_pm", default=0.76, lo=0.45, hi=0.95)

    # Aggregation arrays
    def blank(players: pd.DataFrame) -> Dict[str, np.ndarray]:
        n = int(len(players))
        return {
            "pts": np.zeros(n, dtype=int),
            "fga": np.zeros(n, dtype=int),
            "fgm": np.zeros(n, dtype=int),
            "fg3a": np.zeros(n, dtype=int),
            "fg3m": np.zeros(n, dtype=int),
            "fta": np.zeros(n, dtype=int),
            "ftm": np.zeros(n, dtype=int),
            "reb": np.zeros(n, dtype=int),
            "ast": np.zeros(n, dtype=int),
            "stl": np.zeros(n, dtype=int),
            "blk": np.zeros(n, dtype=int),
            "tov": np.zeros(n, dtype=int),
            "pf": np.zeros(n, dtype=int),
        }

    h = blank(home_players)
    a = blank(away_players)
    events: List[Dict[str, Any]] = []

    home_score = 0
    away_score = 0

    def _pick_lineup_from_pool(
        pool: Optional[List[List[int]]],
        weights: Optional[np.ndarray],
        n_players: int,
    ) -> Optional[List[int]]:
        if not pool:
            return None
        # Validate each lineup is within bounds and has 5 unique players
        valid: List[List[int]] = []
        w: List[float] = []
        for i, lu in enumerate(pool):
            try:
                idx = [int(x) for x in (lu or [])]
            except Exception:
                continue
            idx = [x for x in idx if 0 <= int(x) < int(n_players)]
            idx_u = list(dict.fromkeys(idx))
            if len(idx_u) != 5:
                continue
            valid.append(idx_u)
            if weights is not None and i < int(np.asarray(weights).size):
                try:
                    w.append(float(np.asarray(weights, dtype=float)[i]))
                except Exception:
                    w.append(1.0)
            else:
                w.append(1.0)
        if not valid:
            return None
        ww = np.asarray(w, dtype=float)
        ww = np.maximum(0.0, ww)
        s = float(np.sum(ww))
        if (not np.isfinite(s)) or s <= 0:
            probs = np.full(len(valid), 1.0 / len(valid))
        else:
            probs = ww / s
        try:
            j = int(rng.choice(len(valid), p=probs))
            return valid[j]
        except Exception:
            return valid[int(rng.integers(0, len(valid)))]

    # Quarter loop
    for q in range(1, 5):
        tq_h = int(home_q_pts[q - 1]) if q - 1 < len(home_q_pts) else 0
        tq_a = int(away_q_pts[q - 1]) if q - 1 < len(away_q_pts) else 0

        # possessions per quarter with mild jitter
        # possessions_per_game is per-team; our loop simulates team-possessions.
        # So total simulated possessions per game is ~2x (home+away).
        base_poss = 2.0 * float(np.mean([h_rates["poss"], a_rates["poss"]])) / 4.0
        jitter = float(rng.normal(0.0, cfg.possessions_jitter))
        q_poss = int(max(18, round(base_poss * (1.0 + jitter))))

        # Determine garbage-time settings based on running margin
        margin = int(home_score - away_score)
        blowout = False
        if q >= 4 and abs(margin) >= int(cfg.blowout_q4_margin):
            blowout = True
        if q >= 3 and abs(margin) >= int(cfg.blowout_margin):
            blowout = True

        for pidx in range(q_poss):
            # alternate possession with some randomness
            if pidx == 0:
                offense_home = bool(rng.random() < 0.5)
            else:
                offense_home = not offense_home if bool(rng.random() < 0.85) else bool(rng.random() < 0.5)

            # Sample on-court lineups
            # When provided, prefer observed lineup pools (stints) to preserve realistic 5-man correlations.
            # In blowouts, fall back to minutes-weighted bench-boost sampling.
            h_line = None
            a_line = None
            if not blowout:
                h_line = _pick_lineup_from_pool(home_lineups, home_lineup_weights, n_players=len(home_players))
                a_line = _pick_lineup_from_pool(away_lineups, away_lineup_weights, n_players=len(away_players))
            if not h_line:
                h_line = _sample_lineup(
                    rng,
                    home_players,
                    h_mins,
                    k=5,
                    blowout_boost_bench=blowout,
                    bench_boost=cfg.bench_weight_boost,
                )
            if not a_line:
                a_line = _sample_lineup(
                    rng,
                    away_players,
                    a_mins,
                    k=5,
                    blowout_boost_bench=blowout,
                    bench_boost=cfg.bench_weight_boost,
                )

            # Effective rates with garbage-time scaling
            off_rates = h_rates if offense_home else a_rates
            def_rates = a_rates if offense_home else h_rates
            p_tov = float(off_rates["p_tov"]) * (cfg.garbage_time_pace_scale if blowout else 1.0)
            p3 = float(off_rates["p3"])

            # Determine possession outcome
            r = float(rng.random())
            if r < p_tov:
                # turnover
                if offense_home:
                    shooter_w = _player_usage_weights(home_players, "_prior_tov_pm", h_line)
                    t_idx = int(_pick_weighted(rng, list(range(len(home_players))), shooter_w) or 0)
                    h["tov"][t_idx] += 1

                    # steal attribution
                    if float(rng.random()) < cfg.base_steal_share_of_tov:
                        stl_w = _player_usage_weights(away_players, "_prior_stl_pm", a_line)
                        s_idx = int(_pick_weighted(rng, list(range(len(away_players))), stl_w) or 0)
                        a["stl"][s_idx] += 1
                    events.append({"q": q, "type": "TOV", "off": "H", "player_i": t_idx})
                else:
                    t_w = _player_usage_weights(away_players, "_prior_tov_pm", a_line)
                    t_idx = int(_pick_weighted(rng, list(range(len(away_players))), t_w) or 0)
                    a["tov"][t_idx] += 1
                    if float(rng.random()) < cfg.base_steal_share_of_tov:
                        stl_w = _player_usage_weights(home_players, "_prior_stl_pm", h_line)
                        s_idx = int(_pick_weighted(rng, list(range(len(home_players))), stl_w) or 0)
                        h["stl"][s_idx] += 1
                    events.append({"q": q, "type": "TOV", "off": "A", "player_i": t_idx})
                continue

            # Shot attempt
            shot_is_3 = bool(rng.random() < p3)
            points_if_make = 3 if shot_is_3 else 2

            if offense_home:
                # shooter
                if shot_is_3:
                    w = _player_usage_weights(home_players, "_prior_threes_att_pm", h_line)
                else:
                    w = _player_usage_weights(home_players, "_prior_fga_pm", h_line)
                sh = int(_pick_weighted(rng, list(range(len(home_players))), w) or 0)

                h["fga"][sh] += 1
                if shot_is_3:
                    h["fg3a"][sh] += 1

                # shooting foul chance (roughly proportional to foul_per_fga)
                foul = bool(rng.random() < float(off_rates["foul_per_fga"]))

                # make probability
                eff_scale = cfg.garbage_time_eff_scale if blowout else 1.0
                make_p = float(h_3p_pct[sh] if shot_is_3 else h_fg_pct[sh]) * eff_scale
                made = bool(rng.random() < make_p)

                blk = False
                if (not shot_is_3) and (rng.random() < cfg.base_block_rate_on_2pa):
                    blk = True
                    blk_w = _player_usage_weights(away_players, "_prior_blk_pm", a_line)
                    bidx = int(_pick_weighted(rng, list(range(len(away_players))), blk_w) or 0)
                    a["blk"][bidx] += 1

                if made:
                    h["fgm"][sh] += 1
                    if shot_is_3:
                        h["fg3m"][sh] += 1
                    h["pts"][sh] += points_if_make
                    home_score += points_if_make

                    # assist
                    if rng.random() < 0.58:
                        ast_w = _player_usage_weights(home_players, "_prior_ast_pm", [i for i in h_line if i != sh])
                        aidx = _pick_weighted(rng, list(range(len(home_players))), ast_w)
                        if aidx is not None:
                            h["ast"][int(aidx)] += 1

                    # and-1
                    if foul and rng.random() < 0.32:
                        h["fta"][sh] += 1
                        if rng.random() < float(h_ft_pct[sh]):
                            h["ftm"][sh] += 1
                            h["pts"][sh] += 1
                            home_score += 1
                        # defender PF
                        pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                        didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                        if didx is not None:
                            a["pf"][int(didx)] += 1
                    events.append({"q": q, "type": "FGM3" if shot_is_3 else "FGM2", "off": "H", "sh": sh, "pts": points_if_make})
                else:
                    # miss
                    if foul and rng.random() < 0.70:
                        # shooting FTs (2 or 3)
                        n_ft = 3 if shot_is_3 else 2
                        h["fta"][sh] += int(n_ft)
                        made_fts = int(rng.binomial(int(n_ft), float(h_ft_pct[sh])))
                        if made_fts > 0:
                            h["ftm"][sh] += made_fts
                            h["pts"][sh] += made_fts
                            home_score += made_fts
                        # defender PF
                        pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                        didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                        if didx is not None:
                            a["pf"][int(didx)] += 1
                        events.append({"q": q, "type": "FTA", "off": "H", "sh": sh, "fta": n_ft, "ftm": made_fts})
                    else:
                        # rebound
                        oreb = bool(rng.random() < cfg.base_oreb_rate)
                        if oreb:
                            reb_w = _player_usage_weights(home_players, "_prior_reb_pm", h_line)
                            ridx = _pick_weighted(rng, list(range(len(home_players))), reb_w)
                            if ridx is not None:
                                h["reb"][int(ridx)] += 1
                        else:
                            reb_w = _player_usage_weights(away_players, "_prior_reb_pm", a_line)
                            ridx = _pick_weighted(rng, list(range(len(away_players))), reb_w)
                            if ridx is not None:
                                a["reb"][int(ridx)] += 1
                        events.append({"q": q, "type": "MISS3" if shot_is_3 else "MISS2", "off": "H", "sh": sh, "blk": blk})

                # Non-shooting foul noise
                if rng.random() < cfg.base_nonshooting_foul_per_poss:
                    pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                    didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                    if didx is not None:
                        a["pf"][int(didx)] += 1

            else:
                # away offense
                if shot_is_3:
                    w = _player_usage_weights(away_players, "_prior_threes_att_pm", a_line)
                else:
                    w = _player_usage_weights(away_players, "_prior_fga_pm", a_line)
                sh = int(_pick_weighted(rng, list(range(len(away_players))), w) or 0)

                a["fga"][sh] += 1
                if shot_is_3:
                    a["fg3a"][sh] += 1

                foul = bool(rng.random() < float(off_rates["foul_per_fga"]))
                eff_scale = cfg.garbage_time_eff_scale if blowout else 1.0
                make_p = float(a_3p_pct[sh] if shot_is_3 else a_fg_pct[sh]) * eff_scale
                made = bool(rng.random() < make_p)

                blk = False
                if (not shot_is_3) and (rng.random() < cfg.base_block_rate_on_2pa):
                    blk = True
                    blk_w = _player_usage_weights(home_players, "_prior_blk_pm", h_line)
                    bidx = int(_pick_weighted(rng, list(range(len(home_players))), blk_w) or 0)
                    h["blk"][bidx] += 1

                if made:
                    a["fgm"][sh] += 1
                    if shot_is_3:
                        a["fg3m"][sh] += 1
                    a["pts"][sh] += points_if_make
                    away_score += points_if_make

                    if rng.random() < 0.58:
                        ast_w = _player_usage_weights(away_players, "_prior_ast_pm", [i for i in a_line if i != sh])
                        aidx = _pick_weighted(rng, list(range(len(away_players))), ast_w)
                        if aidx is not None:
                            a["ast"][int(aidx)] += 1

                    if foul and rng.random() < 0.32:
                        a["fta"][sh] += 1
                        if rng.random() < float(a_ft_pct[sh]):
                            a["ftm"][sh] += 1
                            a["pts"][sh] += 1
                            away_score += 1
                        pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                        didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                        if didx is not None:
                            h["pf"][int(didx)] += 1
                    events.append({"q": q, "type": "FGM3" if shot_is_3 else "FGM2", "off": "A", "sh": sh, "pts": points_if_make})
                else:
                    if foul and rng.random() < 0.70:
                        n_ft = 3 if shot_is_3 else 2
                        a["fta"][sh] += int(n_ft)
                        made_fts = int(rng.binomial(int(n_ft), float(a_ft_pct[sh])))
                        if made_fts > 0:
                            a["ftm"][sh] += made_fts
                            a["pts"][sh] += made_fts
                            away_score += made_fts
                        pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                        didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                        if didx is not None:
                            h["pf"][int(didx)] += 1
                        events.append({"q": q, "type": "FTA", "off": "A", "sh": sh, "fta": n_ft, "ftm": made_fts})
                    else:
                        oreb = bool(rng.random() < cfg.base_oreb_rate)
                        if oreb:
                            reb_w = _player_usage_weights(away_players, "_prior_reb_pm", a_line)
                            ridx = _pick_weighted(rng, list(range(len(away_players))), reb_w)
                            if ridx is not None:
                                a["reb"][int(ridx)] += 1
                        else:
                            reb_w = _player_usage_weights(home_players, "_prior_reb_pm", h_line)
                            ridx = _pick_weighted(rng, list(range(len(home_players))), reb_w)
                            if ridx is not None:
                                h["reb"][int(ridx)] += 1
                        events.append({"q": q, "type": "MISS3" if shot_is_3 else "MISS2", "off": "A", "sh": sh, "blk": blk})

                if rng.random() < cfg.base_nonshooting_foul_per_poss:
                    pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                    didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                    if didx is not None:
                        h["pf"][int(didx)] += 1

        # Reconcile quarter points (minimal editing of totals, not full event coherence)
        if cfg.reconcile_points:
            # compute points scored this quarter from aggregates since last quarter start
            # (we don't track per-quarter aggregates; reconcile at game-level approximation)
            cur_h = int(home_score)
            cur_a = int(away_score)
            # Desired cumulative totals through this quarter
            want_h = int(sum(int(x) for x in home_q_pts[:q]))
            want_a = int(sum(int(x) for x in away_q_pts[:q]))
            dh = int(want_h - cur_h)
            da = int(want_a - cur_a)

            def _add_points(side: str, delta: int):
                nonlocal home_score, away_score
                if delta == 0:
                    return
                changes = 0
                # Add via FT first for +/-1, else via 2pt makes
                while delta != 0 and changes < cfg.reconcile_max_changes_per_quarter:
                    changes += 1
                    if side == "H":
                        target = h
                        pts = "home"
                        players = home_players
                    else:
                        target = a
                        pts = "away"
                        players = away_players

                    if delta > 0:
                        if abs(delta) == 1:
                            # pick a random shooter and add a FT make/att
                            w = _safe_series(players, "_prior_fta_pm").to_numpy(dtype=float)
                            idx = int(_pick_weighted(rng, list(range(len(players))), w) or 0)
                            target["fta"][idx] += 1
                            target["ftm"][idx] += 1
                            target["pts"][idx] += 1
                            if side == "H":
                                home_score += 1
                            else:
                                away_score += 1
                            delta -= 1
                        else:
                            # add a 2pt make
                            w = _safe_series(players, "_prior_fga_pm").to_numpy(dtype=float)
                            idx = int(_pick_weighted(rng, list(range(len(players))), w) or 0)
                            target["fga"][idx] += 1
                            target["fgm"][idx] += 1
                            target["pts"][idx] += 2
                            if side == "H":
                                home_score += 2
                            else:
                                away_score += 2
                            delta -= 2
                    else:
                        # remove points conservatively from FT then FG
                        if abs(delta) == 1:
                            cand = np.where(target["ftm"] > 0)[0]
                            if cand.size > 0:
                                idx = int(rng.choice(cand))
                                target["ftm"][idx] -= 1
                                target["pts"][idx] -= 1
                                delta += 1
                                if side == "H":
                                    home_score -= 1
                                else:
                                    away_score -= 1
                                continue
                        # remove 2 pts from a made FG (prefer non-3)
                        cand = np.where((target["fgm"] > target["fg3m"]))[0]
                        if cand.size > 0:
                            idx = int(rng.choice(cand))
                            target["fgm"][idx] -= 1
                            target["pts"][idx] -= 2
                            delta += 2
                            if side == "H":
                                home_score -= 2
                            else:
                                away_score -= 2
                            continue
                        break

            if dh != 0:
                _add_points("H", dh)
            if da != 0:
                _add_points("A", da)

    def finalize(players: pd.DataFrame, agg: Dict[str, np.ndarray]) -> Dict[str, Any]:
        out_players: List[Dict[str, Any]] = []
        mins = _safe_series(players, "_sim_min").to_numpy(dtype=float)
        names = [str(x or "").strip() for x in players.get("player_name", pd.Series([""] * len(players)))].copy()
        for i in range(len(players)):
            out_players.append(
                {
                    "player_name": names[i],
                    "min": float(mins[i]) if np.isfinite(mins[i]) else None,
                    "pts": int(agg["pts"][i]),
                    "reb": int(agg["reb"][i]),
                    "ast": int(agg["ast"][i]),
                    "threes": int(agg["fg3m"][i]),
                    "fg3a": int(agg["fg3a"][i]),
                    "fg3m": int(agg["fg3m"][i]),
                    "fga": int(agg["fga"][i]),
                    "fgm": int(agg["fgm"][i]),
                    "fta": int(agg["fta"][i]),
                    "ftm": int(agg["ftm"][i]),
                    "pf": int(agg["pf"][i]),
                    "stl": int(agg["stl"][i]),
                    "blk": int(agg["blk"][i]),
                    "tov": int(agg["tov"][i]),
                }
            )

        out_players.sort(key=lambda r: ((r.get("min") or 0.0), r.get("pts") or 0), reverse=True)
        return {
            "players": out_players,
            "team_total_pts": int(sum(int(p.get("pts") or 0) for p in out_players)),
            "team_total_reb": int(sum(int(p.get("reb") or 0) for p in out_players)),
            "team_total_ast": int(sum(int(p.get("ast") or 0) for p in out_players)),
            "team_total_threes": int(sum(int(p.get("threes") or 0) for p in out_players)),
            "team_total_fg3a": int(sum(int(p.get("fg3a") or 0) for p in out_players)),
            "team_total_fga": int(sum(int(p.get("fga") or 0) for p in out_players)),
            "team_total_fgm": int(sum(int(p.get("fgm") or 0) for p in out_players)),
            "team_total_fta": int(sum(int(p.get("fta") or 0) for p in out_players)),
            "team_total_ftm": int(sum(int(p.get("ftm") or 0) for p in out_players)),
            "team_total_pf": int(sum(int(p.get("pf") or 0) for p in out_players)),
            "team_total_tov": int(sum(int(p.get("tov") or 0) for p in out_players)),
            "team_total_stl": int(sum(int(p.get("stl") or 0) for p in out_players)),
            "team_total_blk": int(sum(int(p.get("blk") or 0) for p in out_players)),
        }

    home_box = finalize(home_players, h)
    away_box = finalize(away_players, a)

    # Keep event payload small-ish for API
    home_box["events"] = events[:500]
    away_box["events"] = []

    return home_box, away_box


def _expected_points_per_possession(
    p_tov: float,
    p3: float,
    fg2_pct: float,
    fg3_pct: float,
    foul_per_fga: float,
    ft_pct: float,
    oreb_rate: float = 0.24,
) -> float:
    """Very rough expected points per possession from rate inputs.

    Used only to compute a small efficiency multiplier to match a target PPP.
    """
    try:
        p_tov = float(np.clip(p_tov, 0.01, 0.35))
        p3 = float(np.clip(p3, 0.05, 0.75))
        fg2_pct = float(np.clip(fg2_pct, 0.20, 0.80))
        fg3_pct = float(np.clip(fg3_pct, 0.15, 0.60))
        foul_per_fga = float(np.clip(foul_per_fga, 0.02, 0.30))
        ft_pct = float(np.clip(ft_pct, 0.45, 0.95))
    except Exception:
        return 1.05

    # Assume each non-TOV possession produces ~1 FGA.
    # A fraction of FGAs become shooting fouls; those yield FTs instead of a shot result.
    p_shot_poss = 1.0 - p_tov
    p_shooting_foul = float(np.clip(foul_per_fga * 0.70, 0.0, 0.35))
    p_live_shot = max(0.0, 1.0 - p_shooting_foul)

    exp_from_fg = p_live_shot * ((1.0 - p3) * (2.0 * fg2_pct) + p3 * (3.0 * fg3_pct))
    # FTs: mix 2/3 shots by p3
    exp_fts = p_shooting_foul * (((1.0 - p3) * 2.0) + (p3 * 3.0)) * ft_pct

    base = p_shot_poss * (exp_from_fg + exp_fts)

    # Offensive rebounds extend the same possession and create additional shot chances.
    # Approximate with a geometric continuation factor.
    try:
        oreb_rate = float(np.clip(float(oreb_rate), 0.05, 0.45))
        p_make_live = float(np.clip((1.0 - p3) * fg2_pct + p3 * fg3_pct, 0.05, 0.95))
        p_miss_live = 1.0 - p_make_live
        # Only live-shot misses can lead to OREBs.
        p_cont = float(np.clip(p_shot_poss * p_live_shot * p_miss_live * oreb_rate, 0.0, 0.35))
        mult = 1.0 / max(1e-6, (1.0 - p_cont))
        return float(base * mult)
    except Exception:
        return float(base)


def simulate_pbp_game_boxscore(
    rng: np.random.Generator,
    home_players: pd.DataFrame,
    away_players: pd.DataFrame,
    cfg: Optional[EventSimConfig] = None,
    home_lineups: Optional[List[List[int]]] = None,
    home_lineup_weights: Optional[np.ndarray] = None,
    away_lineups: Optional[List[List[int]]] = None,
    away_lineup_weights: Optional[np.ndarray] = None,
    target_home_points: Optional[float] = None,
    target_away_points: Optional[float] = None,
    quarters: Optional[List[Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[int], List[int]]:
    """Simulate a possession/play-by-play style game.

    This produces quarter points and player props from one shared possession stream,
    so correlations emerge naturally (pace/efficiency/usage/lineups affect everything).

    Returns (home_box, away_box, home_q_pts, away_q_pts).
    """
    cfg = cfg or EventSimConfig()

    # Precompute weights and rates
    h_mins = _safe_series(home_players, "_sim_min").to_numpy(dtype=float)
    a_mins = _safe_series(away_players, "_sim_min").to_numpy(dtype=float)
    h_rates = _team_rates_from_priors(home_players, cfg)
    a_rates = _team_rates_from_priors(away_players, cfg)

    # Player shooting pcts
    h_fg_pct = _player_pct(home_players, "_prior_fgm_pm", "_prior_fga_pm", default=0.46, lo=0.25, hi=0.75)
    a_fg_pct = _player_pct(away_players, "_prior_fgm_pm", "_prior_fga_pm", default=0.46, lo=0.25, hi=0.75)
    h_3p_pct = _player_pct(home_players, "_prior_threes_pm", "_prior_threes_att_pm", default=0.35, lo=0.20, hi=0.50)
    a_3p_pct = _player_pct(away_players, "_prior_threes_pm", "_prior_threes_att_pm", default=0.35, lo=0.20, hi=0.50)
    h_ft_pct = _player_pct(home_players, "_prior_ftm_pm", "_prior_fta_pm", default=0.76, lo=0.45, hi=0.95)
    a_ft_pct = _player_pct(away_players, "_prior_ftm_pm", "_prior_fta_pm", default=0.76, lo=0.45, hi=0.95)

    def _team_avg(arr: np.ndarray, w: np.ndarray, default: float) -> float:
        try:
            ww = np.maximum(0.0, np.asarray(w, dtype=float))
            aa = np.asarray(arr, dtype=float)
            if ww.size != aa.size or ww.size == 0:
                return float(default)
            s = float(np.sum(ww))
            if not np.isfinite(s) or s <= 0:
                return float(np.nanmean(aa)) if np.isfinite(np.nanmean(aa)) else float(default)
            return float(np.sum(aa * ww) / s)
        except Exception:
            return float(default)

    # Small efficiency calibration toward target points (keeps market/model alignment)
    poss = float(np.mean([h_rates.get("poss", cfg.possessions_per_game), a_rates.get("poss", cfg.possessions_per_game)]))
    poss = float(max(70.0, poss))
    tpp_h = float(target_home_points) / poss if target_home_points is not None and np.isfinite(float(target_home_points)) else None
    tpp_a = float(target_away_points) / poss if target_away_points is not None and np.isfinite(float(target_away_points)) else None

    base_ppp_h = _expected_points_per_possession(
        p_tov=float(h_rates["p_tov"]),
        p3=float(h_rates["p3"]),
        fg2_pct=_team_avg(h_fg_pct, h_mins, 0.46),
        fg3_pct=_team_avg(h_3p_pct, h_mins, 0.35),
        foul_per_fga=float(h_rates["foul_per_fga"]),
        ft_pct=_team_avg(h_ft_pct, h_mins, 0.76),
        oreb_rate=float(cfg.base_oreb_rate),
    )
    base_ppp_a = _expected_points_per_possession(
        p_tov=float(a_rates["p_tov"]),
        p3=float(a_rates["p3"]),
        fg2_pct=_team_avg(a_fg_pct, a_mins, 0.46),
        fg3_pct=_team_avg(a_3p_pct, a_mins, 0.35),
        foul_per_fga=float(a_rates["foul_per_fga"]),
        ft_pct=_team_avg(a_ft_pct, a_mins, 0.76),
        oreb_rate=float(cfg.base_oreb_rate),
    )

    eff_mult_h = 1.0
    eff_mult_a = 1.0
    try:
        if tpp_h is not None and base_ppp_h > 1e-6:
            eff_mult_h = float(np.clip(tpp_h / base_ppp_h, 0.85, 1.15))
        if tpp_a is not None and base_ppp_a > 1e-6:
            eff_mult_a = float(np.clip(tpp_a / base_ppp_a, 0.85, 1.15))
    except Exception:
        eff_mult_h = 1.0
        eff_mult_a = 1.0

    # Aggregation arrays
    def blank(players: pd.DataFrame) -> Dict[str, np.ndarray]:
        n = int(len(players))
        return {
            "pts": np.zeros(n, dtype=int),
            "fga": np.zeros(n, dtype=int),
            "fgm": np.zeros(n, dtype=int),
            "fg3a": np.zeros(n, dtype=int),
            "fg3m": np.zeros(n, dtype=int),
            "fta": np.zeros(n, dtype=int),
            "ftm": np.zeros(n, dtype=int),
            "reb": np.zeros(n, dtype=int),
            "ast": np.zeros(n, dtype=int),
            "stl": np.zeros(n, dtype=int),
            "blk": np.zeros(n, dtype=int),
            "tov": np.zeros(n, dtype=int),
            "pf": np.zeros(n, dtype=int),
        }

    def blank_q(players: pd.DataFrame) -> Dict[str, np.ndarray]:
        n = int(len(players))
        # Only track the stats we expose as quarter-level props (keeps output size sane).
        return {
            "pts": np.zeros((4, n), dtype=int),
            "reb": np.zeros((4, n), dtype=int),
            "ast": np.zeros((4, n), dtype=int),
            "threes": np.zeros((4, n), dtype=int),
        }

    h = blank(home_players)
    a = blank(away_players)
    hq = blank_q(home_players)
    aq = blank_q(away_players)
    events: List[Dict[str, Any]] = []

    home_score = 0
    away_score = 0
    home_q_pts: List[int] = []
    away_q_pts: List[int] = []

    # Segment buckets for live-lens interval ladders.
    # Keep the legacy 3-minute buckets (4 per quarter) and ALSO track 1-minute buckets (12 per quarter).
    quarter_seconds = 12 * 60
    segment_seconds = 3 * 60
    n_segments = 4
    minute_seconds = 60
    n_minutes = 12
    home_q_seg_pts = np.zeros((4, n_segments), dtype=int)
    away_q_seg_pts = np.zeros((4, n_segments), dtype=int)
    home_q_min_pts = np.zeros((4, n_minutes), dtype=int)
    away_q_min_pts = np.zeros((4, n_minutes), dtype=int)

    # Overtime settings (NBA: 5 minutes)
    ot_seconds = 5 * 60
    max_overtimes = 6
    home_ot_pts: List[int] = []
    away_ot_pts: List[int] = []

    def _add_q_stat(qarr: np.ndarray, q: int, idx: int, val: int) -> None:
        """Add to quarter-level arrays (only for regulation Q1-Q4)."""
        try:
            if 1 <= int(q) <= 4:
                qarr[int(q) - 1, int(idx)] += int(val)
        except Exception:
            pass

    def _add_q_seg(segarr: np.ndarray, q: int, seg: int, val: int) -> None:
        """Add to 3-minute segment buckets (only for regulation Q1-Q4)."""
        try:
            if 1 <= int(q) <= 4:
                segarr[int(q) - 1, int(seg)] += int(val)
        except Exception:
            pass

    def _add_q_min(minarr: np.ndarray, q: int, minute_idx: int, val: int) -> None:
        """Add to 1-minute segment buckets (only for regulation Q1-Q4)."""
        try:
            if 1 <= int(q) <= 4:
                minarr[int(q) - 1, int(minute_idx)] += int(val)
        except Exception:
            pass

    def _pick_lineup_from_pool(
        pool: Optional[List[List[int]]],
        weights: Optional[np.ndarray],
        n_players: int,
    ) -> Optional[List[int]]:
        if not pool:
            return None
        valid: List[List[int]] = []
        w: List[float] = []
        for i, lu in enumerate(pool):
            try:
                idx = [int(x) for x in (lu or [])]
            except Exception:
                continue
            idx = [x for x in idx if 0 <= int(x) < int(n_players)]
            idx_u = list(dict.fromkeys(idx))
            if len(idx_u) != 5:
                continue
            valid.append(idx_u)
            if weights is not None and i < int(np.asarray(weights).size):
                try:
                    w.append(float(np.asarray(weights, dtype=float)[i]))
                except Exception:
                    w.append(1.0)
            else:
                w.append(1.0)
        if not valid:
            return None
        ww = np.asarray(w, dtype=float)
        ww = np.maximum(0.0, ww)
        s = float(np.sum(ww))
        probs = (ww / s) if (np.isfinite(s) and s > 0) else np.full(len(valid), 1.0 / len(valid))
        try:
            j = int(rng.choice(len(valid), p=probs))
            return valid[j]
        except Exception:
            return valid[int(rng.integers(0, len(valid)))]

    # Quarter loop
    # Precompute game-level target total from quarter model (if provided)
    game_mu_total = None
    try:
        if quarters and len(quarters) >= 4:
            gm = float(sum(float(getattr(qr, "home_pts_mu", (qr or {}).get("home_pts_mu"))) + float(getattr(qr, "away_pts_mu", (qr or {}).get("away_pts_mu"))) for qr in quarters[:4]))
            if np.isfinite(gm) and gm > 0:
                game_mu_total = float(gm)
    except Exception:
        game_mu_total = None

    def _simulate_period(q: int, period_seconds: int, q_env_mult: float, q_pace_mult: float, blowout: bool) -> None:
        nonlocal home_score, away_score

        # Track an approximate game clock so we can bucket points into 3-minute intervals.
        # This is intentionally lightweight (we don't emit full timestamps per event).
        q_remaining = int(period_seconds)

        # possessions_per_game is per-team; our loop simulates team-possessions.
        base_poss = (2.0 * float(poss)) / 4.0
        # Scale possessions by period length relative to a 12-minute quarter.
        base_poss *= float(period_seconds) / float(quarter_seconds)
        jitter = float(rng.normal(0.0, cfg.possessions_jitter))
        q_poss = int(max(6, round(base_poss * q_pace_mult * (1.0 + jitter))))

        for pidx in range(q_poss):
            if pidx == 0:
                offense_home = bool(rng.random() < 0.5)
            else:
                offense_home = not offense_home if bool(rng.random() < 0.85) else bool(rng.random() < 0.5)

            h_line = None
            a_line = None
            if not blowout:
                h_line = _pick_lineup_from_pool(home_lineups, home_lineup_weights, n_players=len(home_players))
                a_line = _pick_lineup_from_pool(away_lineups, away_lineup_weights, n_players=len(away_players))
            if not h_line:
                h_line = _sample_lineup(
                    rng,
                    home_players,
                    h_mins,
                    k=5,
                    blowout_boost_bench=blowout,
                    bench_boost=cfg.bench_weight_boost,
                )
            if not a_line:
                a_line = _sample_lineup(
                    rng,
                    away_players,
                    a_mins,
                    k=5,
                    blowout_boost_bench=blowout,
                    bench_boost=cfg.bench_weight_boost,
                )

            # Possession may include multiple shot attempts on offensive rebounds.
            max_shots_this_poss = 5
            shot_n = 0

            while True:
                shot_n += 1

                # Approximate attempt duration (seconds). Keeps segment buckets plausible.
                try:
                    avg_sec = float(period_seconds) / float(max(1, q_poss))
                    dur = int(np.clip(rng.normal(avg_sec, 4.0), 4.0, 28.0))
                except Exception:
                    dur = 14

                # Segment indices at attempt end (approx) for scoring buckets.
                seg = 0
                minute_idx = 0
                q_remaining_after = int(q_remaining)
                try:
                    q_remaining_after = max(0, int(q_remaining - min(int(dur), int(q_remaining))))
                    q_elapsed_after = int(period_seconds - q_remaining_after)
                    seg = int(np.clip(q_elapsed_after // segment_seconds, 0, n_segments - 1))
                    minute_idx = int(np.clip(q_elapsed_after // minute_seconds, 0, n_minutes - 1))
                except Exception:
                    seg = 0
                    minute_idx = 0

                off_rates = h_rates if offense_home else a_rates
                p_tov = float(off_rates["p_tov"]) * (cfg.garbage_time_pace_scale if blowout else 1.0)
                p3 = float(off_rates["p3"])

                # Late-clock heuristics: 2-for-1 / trailing-team 3PA uptick.
                try:
                    is_reg = 1 <= int(q) <= 4
                    margin_now = int(home_score - away_score)
                    close_game = (not blowout) and (abs(margin_now) <= 8)
                    trailing_home = bool(margin_now < 0)
                    offense_trailing = bool(trailing_home) if offense_home else bool(margin_now > 0)
                    is_2for1_window = is_reg and (int(q_remaining) <= 45) and (int(q_remaining) >= 24)
                    is_clutch = is_reg and (int(q_remaining) <= 60) and close_game and (int(q) in (2, 4))

                    if is_2for1_window:
                        p3 = float(np.clip(p3 + 0.02, 0.05, 0.70))
                    if is_clutch and offense_trailing:
                        p3 = float(np.clip(p3 + 0.06, 0.05, 0.75))
                except Exception:
                    pass

                # Turnover ends the possession.
                if float(rng.random()) < p_tov:
                    if offense_home:
                        shooter_w = _player_usage_weights(home_players, "_prior_tov_pm", h_line)
                        t_idx = int(_pick_weighted(rng, list(range(len(home_players))), shooter_w) or 0)
                        h["tov"][t_idx] += 1
                        if float(rng.random()) < cfg.base_steal_share_of_tov:
                            stl_w = _player_usage_weights(away_players, "_prior_stl_pm", a_line)
                            s_idx = int(_pick_weighted(rng, list(range(len(away_players))), stl_w) or 0)
                            a["stl"][s_idx] += 1
                        events.append({"q": q, "type": "TOV", "off": "H", "player_i": t_idx})
                    else:
                        t_w = _player_usage_weights(away_players, "_prior_tov_pm", a_line)
                        t_idx = int(_pick_weighted(rng, list(range(len(away_players))), t_w) or 0)
                        a["tov"][t_idx] += 1
                        if float(rng.random()) < cfg.base_steal_share_of_tov:
                            stl_w = _player_usage_weights(home_players, "_prior_stl_pm", h_line)
                            s_idx = int(_pick_weighted(rng, list(range(len(home_players))), stl_w) or 0)
                            h["stl"][s_idx] += 1
                        events.append({"q": q, "type": "TOV", "off": "A", "player_i": t_idx})
                    try:
                        q_remaining = int(q_remaining_after)
                    except Exception:
                        pass
                    break

                shot_is_3 = bool(rng.random() < p3)
                points_if_make = 3 if shot_is_3 else 2

                foul_per_fga = float(off_rates["foul_per_fga"])
                nonshoot_pf = float(cfg.base_nonshooting_foul_per_poss)
                try:
                    is_reg = 1 <= int(q) <= 4
                    margin_now = int(home_score - away_score)
                    close_game = (not blowout) and (abs(margin_now) <= 6)
                    if is_reg and (int(q) in (2, 4)) and int(q_remaining) <= 45 and close_game:
                        # In close games late in Q2/Q4, the trailing defense is more likely to foul.
                        offense_leading = bool(margin_now > 0) if offense_home else bool(margin_now < 0)
                        if offense_leading:
                            foul_per_fga = float(np.clip(foul_per_fga * 1.25, 0.0, 0.60))
                            nonshoot_pf = float(np.clip(nonshoot_pf + 0.10, 0.0, 0.60))
                except Exception:
                    pass

                # Non-shooting foul noise (does not end the possession in this simplified model).
                do_nonshoot_pf = bool(rng.random() < nonshoot_pf)

                def _maybe_add_nonshoot_pf() -> None:
                    if not do_nonshoot_pf:
                        return
                    try:
                        if offense_home:
                            pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                            didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                            if didx is not None:
                                a["pf"][int(didx)] += 1
                        else:
                            pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                            didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                            if didx is not None:
                                h["pf"][int(didx)] += 1
                    except Exception:
                        return

                foul = bool(rng.random() < foul_per_fga)

                if offense_home:
                    if shot_is_3:
                        w = _player_usage_weights(home_players, "_prior_threes_att_pm", h_line)
                    else:
                        w = _player_usage_weights(home_players, "_prior_fga_pm", h_line)
                    sh = int(_pick_weighted(rng, list(range(len(home_players))), w) or 0)

                    h["fga"][sh] += 1
                    if shot_is_3:
                        h["fg3a"][sh] += 1

                    eff_scale = cfg.garbage_time_eff_scale if blowout else 1.0
                    base_p = float(h_3p_pct[sh] if shot_is_3 else h_fg_pct[sh])
                    make_p = float(np.clip(base_p * eff_scale * eff_mult_h * q_env_mult, 0.05, 0.95))
                    made = bool(rng.random() < make_p)

                    blk = False
                    if (not shot_is_3) and (rng.random() < cfg.base_block_rate_on_2pa):
                        blk = True
                        blk_w = _player_usage_weights(away_players, "_prior_blk_pm", a_line)
                        bidx = int(_pick_weighted(rng, list(range(len(away_players))), blk_w) or 0)
                        a["blk"][bidx] += 1

                    if made:
                        h["fgm"][sh] += 1
                        if shot_is_3:
                            h["fg3m"][sh] += 1
                            _add_q_stat(hq["threes"], q, sh, 1)
                        h["pts"][sh] += points_if_make
                        _add_q_stat(hq["pts"], q, sh, int(points_if_make))
                        home_score += points_if_make
                        _add_q_seg(home_q_seg_pts, q, seg, int(points_if_make))
                        _add_q_min(home_q_min_pts, q, minute_idx, int(points_if_make))

                        if rng.random() < 0.58:
                            ast_w = _player_usage_weights(home_players, "_prior_ast_pm", [i for i in h_line if i != sh])
                            aidx = _pick_weighted(rng, list(range(len(home_players))), ast_w)
                            if aidx is not None:
                                h["ast"][int(aidx)] += 1
                                _add_q_stat(hq["ast"], q, int(aidx), 1)

                        if foul and rng.random() < 0.32:
                            h["fta"][sh] += 1
                            ftp = float(np.clip(float(h_ft_pct[sh]) * eff_mult_h * q_env_mult, 0.45, 0.95))
                            if rng.random() < ftp:
                                h["ftm"][sh] += 1
                                h["pts"][sh] += 1
                                _add_q_stat(hq["pts"], q, sh, 1)
                                home_score += 1
                                _add_q_seg(home_q_seg_pts, q, seg, 1)
                                _add_q_min(home_q_min_pts, q, minute_idx, 1)
                            pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                            didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                            if didx is not None:
                                a["pf"][int(didx)] += 1
                        _maybe_add_nonshoot_pf()
                        events.append({"q": q, "type": "FGM3" if shot_is_3 else "FGM2", "off": "H", "sh": sh, "pts": points_if_make})
                        try:
                            q_remaining = int(q_remaining_after)
                        except Exception:
                            pass
                        break

                    if foul and rng.random() < 0.70:
                        n_ft = 3 if shot_is_3 else 2
                        h["fta"][sh] += int(n_ft)
                        ftp = float(np.clip(float(h_ft_pct[sh]) * eff_mult_h * q_env_mult, 0.45, 0.95))
                        made_fts = int(rng.binomial(int(n_ft), ftp))
                        if made_fts > 0:
                            h["ftm"][sh] += made_fts
                            h["pts"][sh] += made_fts
                            _add_q_stat(hq["pts"], q, sh, int(made_fts))
                            home_score += made_fts
                            _add_q_seg(home_q_seg_pts, q, seg, int(made_fts))
                            _add_q_min(home_q_min_pts, q, minute_idx, int(made_fts))
                        pf_w = _player_usage_weights(away_players, "_prior_pf_pm", a_line)
                        didx = _pick_weighted(rng, list(range(len(away_players))), pf_w)
                        if didx is not None:
                            a["pf"][int(didx)] += 1
                        _maybe_add_nonshoot_pf()
                        events.append({"q": q, "type": "FTA", "off": "H", "sh": sh, "fta": n_ft, "ftm": made_fts})
                        try:
                            q_remaining = int(q_remaining_after)
                        except Exception:
                            pass
                        break

                    oreb = bool(rng.random() < cfg.base_oreb_rate)
                    if oreb:
                        reb_w = _player_usage_weights(home_players, "_prior_reb_pm", h_line)
                        ridx = _pick_weighted(rng, list(range(len(home_players))), reb_w)
                        if ridx is not None:
                            h["reb"][int(ridx)] += 1
                            _add_q_stat(hq["reb"], q, int(ridx), 1)
                    else:
                        reb_w = _player_usage_weights(away_players, "_prior_reb_pm", a_line)
                        ridx = _pick_weighted(rng, list(range(len(away_players))), reb_w)
                        if ridx is not None:
                            a["reb"][int(ridx)] += 1
                            _add_q_stat(aq["reb"], q, int(ridx), 1)
                    events.append({"q": q, "type": "MISS3" if shot_is_3 else "MISS2", "off": "H", "sh": sh, "blk": blk})
                    try:
                        q_remaining = int(q_remaining_after)
                    except Exception:
                        pass
                    _maybe_add_nonshoot_pf()
                    if oreb and (shot_n < int(max_shots_this_poss)) and int(q_remaining) > 0:
                        continue
                    break

                else:
                    if shot_is_3:
                        w = _player_usage_weights(away_players, "_prior_threes_att_pm", a_line)
                    else:
                        w = _player_usage_weights(away_players, "_prior_fga_pm", a_line)
                    sh = int(_pick_weighted(rng, list(range(len(away_players))), w) or 0)

                    a["fga"][sh] += 1
                    if shot_is_3:
                        a["fg3a"][sh] += 1

                    eff_scale = cfg.garbage_time_eff_scale if blowout else 1.0
                    base_p = float(a_3p_pct[sh] if shot_is_3 else a_fg_pct[sh])
                    make_p = float(np.clip(base_p * eff_scale * eff_mult_a * q_env_mult, 0.05, 0.95))
                    made = bool(rng.random() < make_p)

                    blk = False
                    if (not shot_is_3) and (rng.random() < cfg.base_block_rate_on_2pa):
                        blk = True
                        blk_w = _player_usage_weights(home_players, "_prior_blk_pm", h_line)
                        bidx = int(_pick_weighted(rng, list(range(len(home_players))), blk_w) or 0)
                        h["blk"][bidx] += 1

                    if made:
                        a["fgm"][sh] += 1
                        if shot_is_3:
                            a["fg3m"][sh] += 1
                            _add_q_stat(aq["threes"], q, sh, 1)
                        a["pts"][sh] += points_if_make
                        _add_q_stat(aq["pts"], q, sh, int(points_if_make))
                        away_score += points_if_make
                        _add_q_seg(away_q_seg_pts, q, seg, int(points_if_make))
                        _add_q_min(away_q_min_pts, q, minute_idx, int(points_if_make))

                        if rng.random() < 0.58:
                            ast_w = _player_usage_weights(away_players, "_prior_ast_pm", [i for i in a_line if i != sh])
                            aidx = _pick_weighted(rng, list(range(len(away_players))), ast_w)
                            if aidx is not None:
                                a["ast"][int(aidx)] += 1
                                _add_q_stat(aq["ast"], q, int(aidx), 1)

                        if foul and rng.random() < 0.32:
                            a["fta"][sh] += 1
                            ftp = float(np.clip(float(a_ft_pct[sh]) * eff_mult_a * q_env_mult, 0.45, 0.95))
                            if rng.random() < ftp:
                                a["ftm"][sh] += 1
                                a["pts"][sh] += 1
                                _add_q_stat(aq["pts"], q, sh, 1)
                                away_score += 1
                                _add_q_seg(away_q_seg_pts, q, seg, 1)
                                _add_q_min(away_q_min_pts, q, minute_idx, 1)
                            pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                            didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                            if didx is not None:
                                h["pf"][int(didx)] += 1
                        _maybe_add_nonshoot_pf()
                        events.append({"q": q, "type": "FGM3" if shot_is_3 else "FGM2", "off": "A", "sh": sh, "pts": points_if_make})
                        try:
                            q_remaining = int(q_remaining_after)
                        except Exception:
                            pass
                        break

                    if foul and rng.random() < 0.70:
                        n_ft = 3 if shot_is_3 else 2
                        a["fta"][sh] += int(n_ft)
                        ftp = float(np.clip(float(a_ft_pct[sh]) * eff_mult_a * q_env_mult, 0.45, 0.95))
                        made_fts = int(rng.binomial(int(n_ft), ftp))
                        if made_fts > 0:
                            a["ftm"][sh] += made_fts
                            a["pts"][sh] += made_fts
                            _add_q_stat(aq["pts"], q, sh, int(made_fts))
                            away_score += made_fts
                            _add_q_seg(away_q_seg_pts, q, seg, int(made_fts))
                            _add_q_min(away_q_min_pts, q, minute_idx, int(made_fts))
                        pf_w = _player_usage_weights(home_players, "_prior_pf_pm", h_line)
                        didx = _pick_weighted(rng, list(range(len(home_players))), pf_w)
                        if didx is not None:
                            h["pf"][int(didx)] += 1
                        _maybe_add_nonshoot_pf()
                        events.append({"q": q, "type": "FTA", "off": "A", "sh": sh, "fta": n_ft, "ftm": made_fts})
                        try:
                            q_remaining = int(q_remaining_after)
                        except Exception:
                            pass
                        break

                    oreb = bool(rng.random() < cfg.base_oreb_rate)
                    if oreb:
                        reb_w = _player_usage_weights(away_players, "_prior_reb_pm", a_line)
                        ridx = _pick_weighted(rng, list(range(len(away_players))), reb_w)
                        if ridx is not None:
                            a["reb"][int(ridx)] += 1
                            _add_q_stat(aq["reb"], q, int(ridx), 1)
                    else:
                        reb_w = _player_usage_weights(home_players, "_prior_reb_pm", h_line)
                        ridx = _pick_weighted(rng, list(range(len(home_players))), reb_w)
                        if ridx is not None:
                            h["reb"][int(ridx)] += 1
                            _add_q_stat(hq["reb"], q, int(ridx), 1)
                    events.append({"q": q, "type": "MISS3" if shot_is_3 else "MISS2", "off": "A", "sh": sh, "blk": blk})
                    try:
                        q_remaining = int(q_remaining_after)
                    except Exception:
                        pass
                    _maybe_add_nonshoot_pf()
                    if oreb and (shot_n < int(max_shots_this_poss)) and int(q_remaining) > 0:
                        continue
                    break


    for q in range(1, 5):
        h_start = int(home_score)
        a_start = int(away_score)

        # Quarter-level environment multiplier derived from quarter-model mean/volatility.
        # - Mean shift nudges expected scoring toward the quarter model's mean split
        # - Random component adds quarter-to-quarter swings
        # This keeps the simulation fully possession-driven (no point reconciliation).
        q_env_mult = 1.0
        q_pace_mult = 1.0
        try:
            if quarters and len(quarters) >= q:
                qr = quarters[q - 1]
                hm = float(getattr(qr, "home_pts_mu", (qr or {}).get("home_pts_mu")))
                am = float(getattr(qr, "away_pts_mu", (qr or {}).get("away_pts_mu")))
                hs = float(getattr(qr, "home_pts_sigma", (qr or {}).get("home_pts_sigma")))
                as_ = float(getattr(qr, "away_pts_sigma", (qr or {}).get("away_pts_sigma")))
                corr = float(getattr(qr, "corr", (qr or {}).get("corr", 0.0)))
                tot_mu = max(1.0, float(hm + am))
                tot_sd = float(np.sqrt(max(0.0, hs * hs + as_ * as_ + 2.0 * corr * hs * as_)))
                sd_frac = float(np.clip(tot_sd / tot_mu, 0.02, 0.25))

                # Mean shift: align expected quarter total to quarter-model split.
                q_mu_mult = 1.0
                if game_mu_total is not None and game_mu_total > 0:
                    base_q = float(game_mu_total) / 4.0
                    if base_q > 0:
                        q_mu_mult = float(np.clip(tot_mu / base_q, 0.88, 1.12))

                # Split mean shift into pace + efficiency components.
                # (Points ~ possessions * PPP), so sqrt split is a decent neutral choice.
                q_pace_mult = float(np.clip(np.sqrt(q_mu_mult), 0.94, 1.06))
                q_eff_mu_mult = float(np.clip(np.sqrt(q_mu_mult), 0.94, 1.06))

                # Random env: scale down slightly to avoid over-dispersion.
                q_sd_mult = float(np.clip(1.0 + rng.normal(0.0, sd_frac * 0.65), 0.85, 1.20))
                q_env_mult = float(np.clip(q_eff_mu_mult * q_sd_mult, 0.82, 1.22))
        except Exception:
            q_env_mult = 1.0
            q_pace_mult = 1.0

        # For regulation periods, enforce a slightly higher minimum possessions.
        # (OT periods will use a lower min via _simulate_period())
        q_pace_mult = float(q_pace_mult)

        margin = int(home_score - away_score)
        blowout = False
        if q >= 4 and abs(margin) >= int(cfg.blowout_q4_margin):
            blowout = True
        if q >= 3 and abs(margin) >= int(cfg.blowout_margin):
            blowout = True

        _simulate_period(q=q, period_seconds=int(quarter_seconds), q_env_mult=float(q_env_mult), q_pace_mult=float(q_pace_mult), blowout=bool(blowout))

        home_q_pts.append(int(home_score - h_start))
        away_q_pts.append(int(away_score - a_start))

    # Overtime: simulate 5-minute periods until a winner (up to a safety cap)
    try:
        ot_n = 0
        while int(home_score) == int(away_score) and ot_n < int(max_overtimes):
            ot_n += 1
            h_start = int(home_score)
            a_start = int(away_score)

            # Reuse last-known environment; keep it stable for OT.
            _simulate_period(q=4 + int(ot_n), period_seconds=int(ot_seconds), q_env_mult=1.0, q_pace_mult=1.0, blowout=False)

            home_ot_pts.append(int(home_score - h_start))
            away_ot_pts.append(int(away_score - a_start))
    except Exception:
        pass

    def finalize(players: pd.DataFrame, agg: Dict[str, np.ndarray], qagg: Dict[str, np.ndarray]) -> Dict[str, Any]:
        out_players: List[Dict[str, Any]] = []
        mins = _safe_series(players, "_sim_min").to_numpy(dtype=float)
        names = [str(x or "").strip() for x in players.get("player_name", pd.Series([""] * len(players)))].copy()
        for i in range(len(players)):
            out_players.append(
                {
                    "player_name": names[i],
                    "min": float(mins[i]) if np.isfinite(mins[i]) else None,
                    "pts": int(agg["pts"][i]),
                    "q_pts": [int(qagg["pts"][0, i]), int(qagg["pts"][1, i]), int(qagg["pts"][2, i]), int(qagg["pts"][3, i])],
                    "reb": int(agg["reb"][i]),
                    "q_reb": [int(qagg["reb"][0, i]), int(qagg["reb"][1, i]), int(qagg["reb"][2, i]), int(qagg["reb"][3, i])],
                    "ast": int(agg["ast"][i]),
                    "q_ast": [int(qagg["ast"][0, i]), int(qagg["ast"][1, i]), int(qagg["ast"][2, i]), int(qagg["ast"][3, i])],
                    "threes": int(agg["fg3m"][i]),
                    "q_threes": [int(qagg["threes"][0, i]), int(qagg["threes"][1, i]), int(qagg["threes"][2, i]), int(qagg["threes"][3, i])],
                    "fg3a": int(agg["fg3a"][i]),
                    "fg3m": int(agg["fg3m"][i]),
                    "fga": int(agg["fga"][i]),
                    "fgm": int(agg["fgm"][i]),
                    "fta": int(agg["fta"][i]),
                    "ftm": int(agg["ftm"][i]),
                    "pf": int(agg["pf"][i]),
                    "stl": int(agg["stl"][i]),
                    "blk": int(agg["blk"][i]),
                    "tov": int(agg["tov"][i]),
                }
            )

        out_players.sort(key=lambda r: ((r.get("min") or 0.0), r.get("pts") or 0), reverse=True)
        return {
            "players": out_players,
            "team_total_pts": int(sum(int(p.get("pts") or 0) for p in out_players)),
            "team_total_reb": int(sum(int(p.get("reb") or 0) for p in out_players)),
            "team_total_ast": int(sum(int(p.get("ast") or 0) for p in out_players)),
            "team_total_threes": int(sum(int(p.get("threes") or 0) for p in out_players)),
            "team_total_fg3a": int(sum(int(p.get("fg3a") or 0) for p in out_players)),
            "team_total_fga": int(sum(int(p.get("fga") or 0) for p in out_players)),
            "team_total_fgm": int(sum(int(p.get("fgm") or 0) for p in out_players)),
            "team_total_fta": int(sum(int(p.get("fta") or 0) for p in out_players)),
            "team_total_ftm": int(sum(int(p.get("ftm") or 0) for p in out_players)),
            "team_total_pf": int(sum(int(p.get("pf") or 0) for p in out_players)),
            "team_total_tov": int(sum(int(p.get("tov") or 0) for p in out_players)),
            "team_total_stl": int(sum(int(p.get("stl") or 0) for p in out_players)),
            "team_total_blk": int(sum(int(p.get("blk") or 0) for p in out_players)),
        }

    home_box = finalize(home_players, h, hq)
    away_box = finalize(away_players, a, aq)
    home_box["events"] = events[:500]
    away_box["events"] = []

    # Segment points per quarter (4x4 and 4x12). Helpful for live-lens interval ladders.
    try:
        home_box["q_segment_pts"] = home_q_seg_pts.astype(int).tolist()
        away_box["q_segment_pts"] = away_q_seg_pts.astype(int).tolist()
        home_box["segment_seconds"] = int(segment_seconds)
        away_box["segment_seconds"] = int(segment_seconds)

        home_box["q_minute_pts"] = home_q_min_pts.astype(int).tolist()
        away_box["q_minute_pts"] = away_q_min_pts.astype(int).tolist()
        home_box["minute_seconds"] = int(minute_seconds)
        away_box["minute_seconds"] = int(minute_seconds)
    except Exception:
        pass

    # Overtime totals per OT period (per-team).
    try:
        if home_ot_pts or away_ot_pts:
            home_box["ot_pts"] = [int(x) for x in (home_ot_pts or [])]
            away_box["ot_pts"] = [int(x) for x in (away_ot_pts or [])]
            home_box["ot_seconds"] = int(ot_seconds)
            away_box["ot_seconds"] = int(ot_seconds)
    except Exception:
        pass

    return home_box, away_box, home_q_pts, away_q_pts
