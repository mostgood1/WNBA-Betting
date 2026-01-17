from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .quarters import QuarterResult, sample_quarter_scores


def _to_num(x: Any) -> Optional[float]:
    try:
        v = pd.to_numeric(x, errors="coerce")
        if np.isfinite(v):
            return float(v)
        return None
    except Exception:
        try:
            v = float(x)
            return v if np.isfinite(v) else None
        except Exception:
            return None


def _norm_name(x: Any) -> str:
    return " ".join(str(x or "").strip().split())


def _norm_player_key(x: Any) -> str:
    """Normalize player name for matching across data sources.

    Must be stable across:
      - roster lists (often already normalized)
      - props_df player_name (can contain punctuation/suffixes)
      - minutes priors keys (uppercased, punctuation stripped)
    """
    try:
        t = str(x or "").strip()
        if not t:
            return ""
        if "(" in t:
            t = t.split("(", 1)[0]
        t = t.replace("-", " ")
        t = t.replace(".", "").replace("'", "").replace(",", " ")
        t = " ".join(t.split())
        u = t.upper()
        for suf in (" JR", " SR", " II", " III", " IV"):
            if u.endswith(suf):
                u = u[: -len(suf)].strip()
                break
        try:
            u = u.encode("ascii", "ignore").decode("ascii")
        except Exception:
            pass
        return " ".join(u.split())
    except Exception:
        return ""


def _cap_probs(p: np.ndarray, cap: float = 0.38) -> np.ndarray:
    """Cap maximum probability to avoid pathological concentration."""
    x = np.asarray(p, dtype=float)
    if x.ndim != 1 or x.size == 0:
        return x
    x = np.maximum(0.0, np.where(np.isfinite(x), x, 0.0))
    s = float(np.sum(x))
    if not np.isfinite(s) or s <= 0:
        return np.full_like(x, 1.0 / x.size)
    x = x / s
    cap = float(cap)
    if not np.isfinite(cap) or cap <= 0:
        return x
    cap = min(cap, 0.95)

    # Iteratively cap the largest entries and renormalize the remainder.
    for _ in range(10):
        m = float(np.max(x))
        if m <= cap + 1e-12:
            break
        i = int(np.argmax(x))
        excess = float(x[i] - cap)
        x[i] = cap
        rem = float(np.sum(x) - cap)
        if rem <= 0:
            # all mass on one player; spread uniformly
            x = np.full_like(x, 1.0 / x.size)
            break
        scale = (1.0 - cap) / rem
        for j in range(x.size):
            if j == i:
                continue
            x[j] *= scale
    # final renorm
    s2 = float(np.sum(x))
    if s2 > 0 and np.isfinite(s2):
        x = x / s2
    return x


def _dirichlet_weights(
    players: pd.DataFrame,
    points_col: str = "pred_pts",
    minutes_cols: Tuple[str, ...] = ("_sim_min", "pred_min", "roll10_min", "roll5_min", "roll20_min", "roll30_min"),
    min_floor: float = 1.0,
) -> np.ndarray:
    if players is None or players.empty:
        return np.zeros(0, dtype=float)
    if points_col in players.columns:
        pts = pd.to_numeric(players[points_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    else:
        pts = np.zeros(int(len(players)), dtype=float)
    mins = None
    for c in minutes_cols:
        if c in players.columns:
            v = pd.to_numeric(players[c], errors="coerce").to_numpy(dtype=float)
            if mins is None:
                mins = v
            else:
                mins = np.where(np.isfinite(mins), mins, v)
    if mins is None:
        mins = np.full_like(pts, 24.0)
    mins = np.where(np.isfinite(mins), mins, 0.0)

    # More stable points allocation: minutes-driven with tempered scoring signal.
    pts_clean = np.where(np.isfinite(pts), np.maximum(0.0, pts), 0.0)
    mins_clean = np.maximum(min_floor, np.where(np.isfinite(mins), np.maximum(0.0, mins), 0.0))

    # Use points-per-minute as a usage proxy (clipped), but keep minutes primary.
    ppm = pts_clean / np.maximum(1.0, mins_clean)
    ppm = np.clip(ppm, 0.0, 1.2)  # ~43 pts / 36 min upper-ish
    usage = 0.9 + 0.9 * ppm  # [0.9, 1.98]

    # Temper predicted points so a single outlier doesn't dominate.
    w = mins_clean * usage * (1.0 + 0.15 * np.log1p(pts_clean))
    w = np.maximum(1e-3, w)
    p = w / float(np.sum(w))
    # Flatten slightly and cap max share.
    p = np.power(p, 0.90)
    p = p / float(np.sum(p))
    p = _cap_probs(p, cap=0.38)
    return p


def _normalize_team_minutes(
    mins_raw: np.ndarray,
    total_minutes: float = 240.0,
    cap_player_minutes: float = 44.0,
    floor_minutes: float = 0.0,
    max_iter: int = 20,
) -> np.ndarray:
    mins = np.asarray(mins_raw, dtype=float)
    mins = np.where(np.isfinite(mins), mins, 0.0)
    mins = np.maximum(floor_minutes, mins)
    n = int(mins.size)
    if n == 0:
        return mins

    # If all zeros, allocate evenly.
    if float(np.sum(mins)) <= 0:
        return np.full(n, total_minutes / n, dtype=float)

    remaining = np.ones(n, dtype=bool)
    fixed = np.zeros(n, dtype=float)
    for _ in range(max_iter):
        rem_total = float(total_minutes - float(np.sum(fixed)))
        if rem_total <= 0:
            break
        rem_sum = float(np.sum(mins[remaining]))
        if rem_sum <= 0:
            # distribute remaining equally across remaining slots
            fixed[remaining] = rem_total / max(1, int(np.sum(remaining)))
            remaining[:] = False
            break
        scaled = mins[remaining] * (rem_total / rem_sum)
        over = scaled > cap_player_minutes
        if not bool(np.any(over)):
            fixed[remaining] = scaled
            remaining[:] = False
            break
        # fix capped players, keep iterating for the rest
        idxs = np.flatnonzero(remaining)
        fixed[idxs[over]] = cap_player_minutes
        remaining[idxs[over]] = False

    # Final correction to hit exact total if there is headroom.
    s = float(np.sum(fixed))
    if s > 0 and np.isfinite(s):
        diff = float(total_minutes - s)
        for _ in range(10):
            if abs(diff) <= 1e-6:
                break
            if diff > 0:
                headroom = np.maximum(0.0, cap_player_minutes - fixed)
                if float(np.sum(headroom)) <= 0:
                    break
                add = headroom * (diff / float(np.sum(headroom)))
                fixed = np.minimum(cap_player_minutes, fixed + add)
            else:
                reducible = np.maximum(0.0, fixed)
                if float(np.sum(reducible)) <= 0:
                    break
                sub = reducible * ((-diff) / float(np.sum(reducible)))
                fixed = np.maximum(0.0, fixed - sub)
            diff = float(total_minutes - float(np.sum(fixed)))
    return fixed


def _weights_from_stat_and_minutes(
    players: pd.DataFrame,
    stat_col: str,
    min_col: str = "_sim_min",
    floor: float = 0.05,
) -> np.ndarray:
    if players is None or players.empty:
        return np.zeros(0, dtype=float)
    if stat_col in players.columns:
        base = pd.to_numeric(players[stat_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    else:
        base = np.zeros(int(len(players)), dtype=float)
    if min_col in players.columns:
        mins = pd.to_numeric(players[min_col], errors="coerce").fillna(0.0).to_numpy(dtype=float)
    else:
        mins = np.zeros(int(len(players)), dtype=float)
    base_pos = np.maximum(0.0, np.where(np.isfinite(base), base, 0.0))
    mins_pos = np.maximum(1.0, np.where(np.isfinite(mins), np.maximum(0.0, mins), 0.0))

    # Blend minutes-driven allocation with a tempered stat prior to avoid zeroing low-signal players.
    # log1p compresses large priors while still differentiating.
    w = mins_pos * (0.85 + 0.35 * np.log1p(base_pos))
    w = np.maximum(floor, w)

    s = float(np.sum(w))
    if not np.isfinite(s) or s <= 0:
        w = np.ones_like(w, dtype=float)
        s = float(np.sum(w))
    p = w / s
    p = _cap_probs(p, cap=0.55)
    return p


def _shannon_entropy(p: np.ndarray) -> float:
    try:
        x = np.asarray(p, dtype=float)
        x = x[np.isfinite(x) & (x > 0)]
        if x.size == 0:
            return 0.0
        x = x / float(np.sum(x))
        return float(-(x * np.log(x)).sum())
    except Exception:
        return 0.0


def _multinomial_allocate(rng: np.random.Generator, total: int, probs: np.ndarray) -> np.ndarray:
    if total <= 0:
        return np.zeros_like(probs, dtype=int)
    probs = np.asarray(probs, dtype=float)
    if probs.ndim != 1 or probs.size == 0:
        return np.zeros(0, dtype=int)
    probs = np.maximum(0.0, probs)
    s = float(np.sum(probs))
    if not np.isfinite(s) or s <= 0:
        probs = np.full_like(probs, 1.0 / probs.size)
    else:
        probs = probs / s
    return rng.multinomial(int(total), probs)


def simulate_connected_game(
    quarters: List[QuarterResult],
    home_tri: str,
    away_tri: str,
    props_df: pd.DataFrame,
    home_roster: Optional[List[str]] = None,
    away_roster: Optional[List[str]] = None,
    minutes_priors: Optional[Dict[Tuple[str, str], float]] = None,
    minutes_lookback_days: int = 21,
    n_samples: int = 1500,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """Connected simulation: quarter team points + player box scores share the same scoring totals.

    - Samples integer quarter scores from the quarter distribution.
    - Allocates each quarter's team points across players via a Dirichlet-multinomial driven by pred_pts/minutes.
    - Generates a representative single-game box score (median margin) and also returns means.
    """
    rng = np.random.default_rng(seed)

    home_q, away_q = sample_quarter_scores(quarters, n_samples=int(n_samples), rng=rng, round_to_int=True)
    n = int(home_q.shape[0])
    if n == 0:
        return {"error": "no samples"}

    home_final = home_q.sum(axis=1)
    away_final = away_q.sum(axis=1)
    margin = home_final - away_final

    total = home_final + away_final

    # Pick a representative sample: near-median margin AND near-median total.
    # Also avoid exact ties (NBA games cannot end tied without OT).
    try:
        med_m = float(np.median(margin))
        med_t = float(np.median(total))
        score = (margin - med_m) ** 2 + 0.25 * (total - med_t) ** 2
        order = np.argsort(score)
        idx = int(order[0])
        for j in order[: min(200, len(order))]:
            if int(margin[int(j)]) != 0:
                idx = int(j)
                break
    except Exception:
        idx = int(np.argsort(margin)[len(margin) // 2])

    def _team_players(team: str, opp: str, roster: Optional[List[str]]) -> pd.DataFrame:
        df = props_df.copy() if isinstance(props_df, pd.DataFrame) else pd.DataFrame()
        if df.empty:
            df = pd.DataFrame()
        # normalize columns
        if "team" in df.columns:
            df["team"] = df["team"].astype(str).str.upper().str.strip()
        if "opponent" in df.columns:
            df["opponent"] = df["opponent"].astype(str).str.upper().str.strip()
        # Primary filter: team+opponent (best specificity).
        if (not df.empty) and ("team" in df.columns) and ("opponent" in df.columns):
            out = df[(df["team"] == team) & (df["opponent"] == opp)]
        elif (not df.empty) and ("team" in df.columns):
            out = df[(df["team"] == team)]
        else:
            out = pd.DataFrame()
        # If too thin, fall back to team-only to avoid missing rotation players.
        if (out is None) or (not isinstance(out, pd.DataFrame)) or (out.empty) or (int(len(out)) < 6):
            out2 = df[(df["team"] == team)] if ((not df.empty) and ("team" in df.columns)) else pd.DataFrame()
            if isinstance(out2, pd.DataFrame) and (not out2.empty) and (int(len(out2)) >= int(len(out))):
                out = out2
        before_n = int(len(out))
        # If playing_today exists, filter out explicit false
        if "playing_today" in out.columns:
            try:
                pt = out["playing_today"].astype(str).str.lower().str.strip()
                out = out[~pt.isin(["false", "0", "no", "n"])]
            except Exception:
                pass
        # Require a name
        if "player_name" in out.columns:
            out = out[out["player_name"].astype(str).str.strip().ne("")]

        # If an explicit roster was provided, restrict to those players.
        # This prevents injured/non-rotation players from appearing due to stale rows in props_df.
        try:
            roster_names = [(_norm_player_key(x), _norm_name(x)) for x in (roster or []) if _norm_player_key(x)]
            if roster_names and ("player_name" in out.columns) and (not out.empty):
                allowed = set(k for k, _ in roster_names)
                out = out[out["player_name"].map(_norm_player_key).isin(allowed)].copy()
        except Exception:
            pass

        # Deduplicate: keep the most-relevant row per player (highest minutes signal, then pred_pts)
        if not out.empty and "player_name" in out.columns:
            try:
                out = out.copy()
                out["_player_norm"] = out["player_name"].map(_norm_player_key)
                # pick best minutes feature available
                mins_col = None
                for c in ("pred_min", "roll10_min", "roll5_min", "roll20_min", "roll30_min"):
                    if c in out.columns:
                        mins_col = c
                        break
                if mins_col:
                    out["_mins"] = pd.to_numeric(out[mins_col], errors="coerce")
                else:
                    out["_mins"] = np.nan
                if "pred_pts" in out.columns:
                    out["_pts"] = pd.to_numeric(out["pred_pts"], errors="coerce")
                else:
                    out["_pts"] = np.nan
                out = out.sort_values(["_mins", "_pts"], ascending=[False, False])
                out = out.drop_duplicates(subset=["_player_norm"], keep="first")
                out = out.drop(columns=[c for c in ["_player_norm", "_mins", "_pts"] if c in out.columns])
            except Exception:
                pass
        try:
            after_n = int(len(out))
            out.attrs["_dedup_removed"] = max(0, before_n - after_n)
        except Exception:
            pass
        out = out.reset_index(drop=True)

        # Expand with roster players (no placeholders). Only include roster players with some minutes prior,
        # unless needed to reach a minimal rotation size.
        try:
            pri = minutes_priors or {}
            team_u = str(team or "").strip().upper()
            roster_names = [(_norm_player_key(x), _norm_name(x)) for x in (roster or []) if _norm_player_key(x)]

            # If roster isn't available, derive a pseudo-roster from minutes priors for this team.
            # This prevents inflating a tiny player pool up to 240 minutes.
            if (not roster_names) and pri:
                try:
                    cand: list[tuple[str, float]] = []
                    for (t, nm), m in pri.items():
                        if str(t).strip().upper() != team_u:
                            continue
                        mm = _to_num(m)
                        if mm is None or mm <= 0:
                            continue
                        key = _norm_player_key(nm)
                        if not key:
                            continue
                        cand.append((key, float(mm)))
                    if cand:
                        cand.sort(key=lambda x: x[1], reverse=True)
                        roster_names = [(k, k) for k, _ in cand[:14]]
                except Exception:
                    pass

            # If roster exists but is undersized, augment it from priors.
            try:
                min_roster_target = 10
                if pri and roster_names and int(len(roster_names)) < min_roster_target:
                    have = set(k for k, _ in roster_names)
                    cand2: list[tuple[str, float]] = []
                    for (t, nm), m in pri.items():
                        if str(t).strip().upper() != team_u:
                            continue
                        key = _norm_player_key(nm)
                        if not key or key in have:
                            continue
                        mm = _to_num(m)
                        if mm is None or mm <= 0:
                            continue
                        cand2.append((key, float(mm)))
                    if cand2:
                        cand2.sort(key=lambda x: x[1], reverse=True)
                        for k, _ in cand2:
                            roster_names.append((k, k))
                            have.add(k)
                            if int(len(roster_names)) >= 14:
                                break
            except Exception:
                pass

            # Attach minutes priors for existing players to improve rotation realism when pred_min is missing.
            if (not out.empty) and ("player_name" in out.columns) and pri:
                try:
                    out = out.copy()
                    out["_prior_min"] = out["player_name"].map(lambda nm: pri.get((team_u, _norm_player_key(nm))))
                except Exception:
                    pass

            if roster_names:
                existing = set()
                if "player_name" in out.columns and not out.empty:
                    existing = set(out["player_name"].map(_norm_player_key).tolist())

                additions: list[dict[str, Any]] = []
                for key_norm, disp in roster_names:
                    if key_norm in existing:
                        continue
                    m = pri.get((team_u, key_norm))
                    if m is None:
                        continue
                    additions.append(
                        {
                            "player_name": disp,
                            "team": team_u,
                            # Provide a minutes signal so _attach_sim_minutes can normalize.
                            "pred_min": float(m),
                            # Very conservative stat priors so these players don't steal usage.
                            "pred_pts": 0.0,
                            "pred_reb": 0.0,
                            "pred_ast": 0.0,
                            "pred_threes": 0.0,
                            "pred_tov": 0.0,
                            "pred_stl": 0.0,
                            "pred_blk": 0.0,
                        }
                    )

                if additions:
                    out = pd.concat([out, pd.DataFrame(additions)], ignore_index=True)

                # Ensure at least an 8-man rotation by adding low-minute roster players (even without priors).
                # This avoids placeholders while keeping weights small.
                min_roster = 10
                if int(len(out)) < min_roster:
                    need = max(0, min_roster - int(len(out)))
                    more: list[dict[str, Any]] = []
                    for key_norm, disp in roster_names:
                        if need <= 0:
                            break
                        if key_norm in set(out.get("player_name", pd.Series([], dtype=str)).map(_norm_player_key).tolist()):
                            continue
                        more.append(
                            {
                                "player_name": disp,
                                "team": team_u,
                                "pred_min": float(pri.get((team_u, key_norm), 8.0) or 8.0),
                                "pred_pts": 0.0,
                                "pred_reb": 0.0,
                                "pred_ast": 0.0,
                                "pred_threes": 0.0,
                                "pred_tov": 0.0,
                                "pred_stl": 0.0,
                                "pred_blk": 0.0,
                            }
                        )
                        need -= 1
                    if more:
                        out = pd.concat([out, pd.DataFrame(more)], ignore_index=True)
        except Exception:
            pass

        return out.reset_index(drop=True)

    home_players = _team_players(home_tri, away_tri, home_roster)
    away_players = _team_players(away_tri, home_tri, away_roster)

    # Normalize minutes to a realistic team total.
    def _attach_sim_minutes(players: pd.DataFrame, team_label: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        diag: Dict[str, Any] = {
            "minutes_source": None,
            "minutes_total_raw": None,
            "minutes_total_sim": None,
            "minutes_cap": 40.0,
            "minutes_target": 240.0,
            "fillers_added": 0,
            "players": 0,
        }
        if players is None or players.empty:
            return players, diag
        players = players.copy()
        diag["players"] = int(len(players))

        # No placeholder players. If minutes signals are missing, we'll normalize whatever is available.

        mins_col = None
        for c in ("pred_min", "roll10_min", "roll5_min", "roll20_min", "roll30_min"):
            if c in players.columns:
                mins_col = c
                break
        diag["minutes_source"] = mins_col
        raw = pd.to_numeric(players.get(mins_col), errors="coerce").fillna(0.0).to_numpy(dtype=float) if mins_col else np.zeros(len(players), dtype=float)

        # If we have priors, fill missing/near-zero minutes from priors.
        try:
            if "_prior_min" in players.columns:
                pri = pd.to_numeric(players.get("_prior_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                use = (raw < 1.0) & (pri > 0.0)
                if bool(np.any(use)):
                    raw = np.where(use, pri, raw)
                    diag["minutes_source"] = f"{mins_col or 'none'}+prior"
        except Exception:
            pass
        # If all zeros, give a small default so we can still allocate a rotation.
        if float(np.sum(raw)) <= 0:
            raw = np.full(len(players), 24.0, dtype=float)
        diag["minutes_total_raw"] = float(np.sum(raw))
        sim_mins = _normalize_team_minutes(raw, total_minutes=240.0, cap_player_minutes=40.0, floor_minutes=0.0)
        diag["minutes_total_sim"] = float(np.sum(sim_mins))
        out = players.copy()
        out["_sim_min"] = sim_mins
        return out, diag

    home_players, home_min_diag = _attach_sim_minutes(home_players, team_label=str(home_tri))
    away_players, away_min_diag = _attach_sim_minutes(away_players, team_label=str(away_tri))

    def _allocate_points(team_q_points: np.ndarray, team_players: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
        if team_players is None or team_players.empty:
            return pd.DataFrame(), np.zeros((team_q_points.shape[0], 0), dtype=int)

        base_probs = _dirichlet_weights(team_players)
        concentration = 180.0  # higher => less noisy, closer to baseline weights
        alpha = np.maximum(0.05, base_probs * concentration)

        n_samp, n_q = team_q_points.shape
        alloc = np.zeros((n_samp, len(base_probs), n_q), dtype=int)
        for s in range(n_samp):
            # One Dirichlet draw per game (not per quarter) to avoid extreme quarter-to-quarter swings
            p_game = rng.dirichlet(alpha)
            for q in range(n_q):
                alloc[s, :, q] = _multinomial_allocate(rng, int(team_q_points[s, q]), p_game)
        # return per-player totals for representative sample
        return team_players, alloc

    hp, h_alloc = _allocate_points(home_q, home_players)
    ap, a_alloc = _allocate_points(away_q, away_players)

    def _build_box(team_players: pd.DataFrame, alloc: np.ndarray, team_q_points: np.ndarray) -> Dict[str, Any]:
        if team_players is None or team_players.empty or alloc.size == 0:
            return {"players": [], "team_total_pts": int(team_q_points[idx].sum())}

        pts_by_player = alloc[idx].sum(axis=1)  # shape (players,)
        # Scale other stats with team scoring vs predicted scoring, but enforce team totals.
        pred_team_pts = float(pd.to_numeric(team_players.get("pred_pts"), errors="coerce").fillna(0.0).sum())
        scale_pts = float(team_q_points[idx].sum() / max(1e-6, pred_team_pts)) if pred_team_pts > 0 else 1.0
        mins = pd.to_numeric(team_players.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)

        def team_total_from_pred(col: str, power: float) -> int:
            if col not in team_players.columns:
                return 0
            pred = float(pd.to_numeric(team_players.get(col), errors="coerce").fillna(0.0).sum())
            lam = max(0.0, pred * (scale_pts**power))
            # Keep variance reasonable; Poisson is fine for a first pass.
            return int(rng.poisson(lam=lam)) if lam > 0 else 0

        def alloc_team_total(total_value: int, col: str) -> List[int]:
            probs = _weights_from_stat_and_minutes(team_players, col)
            return list(_multinomial_allocate(rng, int(total_value), probs).astype(int))

        # Team totals (constrained)
        team_reb = team_total_from_pred("pred_reb", power=0.55)
        team_ast = team_total_from_pred("pred_ast", power=0.75)
        team_3pm = team_total_from_pred("pred_threes", power=0.75)
        team_tov = team_total_from_pred("pred_tov", power=0.70)
        team_stl = team_total_from_pred("pred_stl", power=0.60)
        team_blk = team_total_from_pred("pred_blk", power=0.60)

        reb = alloc_team_total(team_reb, "pred_reb")
        ast = alloc_team_total(team_ast, "pred_ast")
        threes = alloc_team_total(team_3pm, "pred_threes")
        tov = alloc_team_total(team_tov, "pred_tov")
        stl = alloc_team_total(team_stl, "pred_stl")
        blk = alloc_team_total(team_blk, "pred_blk")

        players_out = []
        for i in range(len(team_players)):
            p = team_players.iloc[i]
            players_out.append(
                {
                    "player_name": _norm_name(p.get("player_name")),
                    "min": float(mins[i]) if np.isfinite(mins[i]) else None,
                    "pts": int(pts_by_player[i]),
                    "reb": int(reb[i]),
                    "ast": int(ast[i]),
                    "threes": int(threes[i]),
                    "stl": int(stl[i]),
                    "blk": int(blk[i]),
                    "tov": int(tov[i]),
                }
            )

        # Sort by minutes then points
        players_out.sort(key=lambda r: ((r.get("min") or 0.0), r.get("pts") or 0), reverse=True)
        return {
            "players": players_out,
            "team_total_pts": int(team_q_points[idx].sum()),
            "team_total_reb": int(sum(reb)),
            "team_total_ast": int(sum(ast)),
            "team_total_threes": int(sum(threes)),
            "team_total_tov": int(sum(tov)),
            "team_total_stl": int(sum(stl)),
            "team_total_blk": int(sum(blk)),
        }

    home_box = _build_box(hp, h_alloc, home_q)
    away_box = _build_box(ap, a_alloc, away_q)

    def _q_line(h: np.ndarray, a: np.ndarray) -> List[Dict[str, int]]:
        out = []
        hcum = 0
        acum = 0
        for qi in range(h.shape[0]):
            hcum += int(h[qi])
            acum += int(a[qi])
            out.append({"q": qi + 1, "home": int(h[qi]), "away": int(a[qi]), "home_cum": hcum, "away_cum": acum})
        return out

    q_rep = _q_line(home_q[idx], away_q[idx])

    # Mean scores (for display)
    q_mean = [
        {
            "q": i + 1,
            "home": float(np.mean(home_q[:, i])),
            "away": float(np.mean(away_q[:, i])),
        }
        for i in range(home_q.shape[1])
    ]

    # Diagnostics / sanity checks
    warnings: List[str] = []
    def _warn(msg: str) -> None:
        if msg and msg not in warnings:
            warnings.append(msg)

    try:
        # Minutes sanity
        for side, df, md in [("home", hp, home_min_diag), ("away", ap, away_min_diag)]:
            if df is not None and not df.empty:
                mx = float(pd.to_numeric(df.get("_sim_min"), errors="coerce").fillna(0.0).max())
                if mx > 46.0:
                    _warn(f"{side}: max minutes unusually high ({mx:.1f}).")
                totm = float(pd.to_numeric(df.get("_sim_min"), errors="coerce").fillna(0.0).sum())
                if abs(totm - 240.0) > 0.75:
                    _warn(f"{side}: team minutes not ~240 (got {totm:.1f}).")
        # Points invariants
        if home_box and "players" in home_box:
            ps = int(sum(int(p.get("pts") or 0) for p in home_box.get("players") or []))
            if ps != int(home_box.get("team_total_pts") or 0):
                _warn("home: player points do not sum to team total.")
        if away_box and "players" in away_box:
            ps = int(sum(int(p.get("pts") or 0) for p in away_box.get("players") or []))
            if ps != int(away_box.get("team_total_pts") or 0):
                _warn("away: player points do not sum to team total.")
        # Top scorer plausibility
        top_pts = 0
        top_name = None
        for p in (home_box.get("players") or []) + (away_box.get("players") or []):
            v = int(p.get("pts") or 0)
            if v > top_pts:
                top_pts = v
                top_name = str(p.get("player_name") or "")
        if top_pts >= 60:
            _warn(f"top scorer very high: {top_name} {top_pts} pts.")
        # Share check
        try:
            h_tot = int(home_box.get("team_total_pts") or 0)
            a_tot = int(away_box.get("team_total_pts") or 0)
            if h_tot > 0:
                h_max = max([int(p.get("pts") or 0) for p in (home_box.get("players") or [])] + [0])
                if h_max / h_tot > 0.45:
                    _warn(f"home: top scorer share unusually high ({h_max}/{h_tot}).")
            if a_tot > 0:
                a_max = max([int(p.get("pts") or 0) for p in (away_box.get("players") or [])] + [0])
                if a_max / a_tot > 0.45:
                    _warn(f"away: top scorer share unusually high ({a_max}/{a_tot}).")
        except Exception:
            pass
    except Exception:
        pass

    diagnostics = {
        "home_minutes": home_min_diag,
        "away_minutes": away_min_diag,
        "home_dedup_removed": int(getattr(home_players, "attrs", {}).get("_dedup_removed", 0)) if isinstance(home_players, pd.DataFrame) else 0,
        "away_dedup_removed": int(getattr(away_players, "attrs", {}).get("_dedup_removed", 0)) if isinstance(away_players, pd.DataFrame) else 0,
        "home_points_entropy": float(_shannon_entropy(_dirichlet_weights(home_players))) if isinstance(home_players, pd.DataFrame) and not home_players.empty else 0.0,
        "away_points_entropy": float(_shannon_entropy(_dirichlet_weights(away_players))) if isinstance(away_players, pd.DataFrame) and not away_players.empty else 0.0,
        "warnings": warnings,
    }

    return {
        "home": home_tri,
        "away": away_tri,
        "rep": {
            "home_score": int(home_final[idx]),
            "away_score": int(away_final[idx]),
            "margin": int(home_final[idx] - away_final[idx]),
            "quarters": q_rep,
            "home_box": home_box,
            "away_box": away_box,
        },
        "means": {
            "home_score": float(np.mean(home_final)),
            "away_score": float(np.mean(away_final)),
            "margin": float(np.mean(margin)),
            "quarters": q_mean,
        },
        "diagnostics": diagnostics,
    }


def write_sportswriter_recap(
    sim: Dict[str, Any],
    market_total: Optional[float] = None,
    market_home_spread: Optional[float] = None,
    use_means: bool = True,
    quarters_override: Optional[List[Dict[str, Any]]] = None,
    home_score_override: Optional[float] = None,
    away_score_override: Optional[float] = None,
    probs: Optional[Dict[str, float]] = None,
) -> str:
    """Generate an original sportswriter-style recap.

    By default, this uses mean quarter/score outputs so the narrative matches the displayed
    quarter table and mean score. Set use_means=False to narrate the representative sample.

    If quarters_override is provided, the narrative will be driven from those quarters
    (with cumulative recomputed) and the score will be derived from the override unless
    home_score_override/away_score_override are set.
    """
    try:
        home = sim.get("home")
        away = sim.get("away")
        rep = sim.get("rep") or {}

        def _with_cum(q_in: Any) -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            hcum = 0.0
            acum = 0.0
            for row in (q_in if isinstance(q_in, list) else []):
                try:
                    hh = float((row or {}).get("home") or 0.0)
                    aa = float((row or {}).get("away") or 0.0)
                except Exception:
                    hh = 0.0
                    aa = 0.0
                hcum += hh
                acum += aa
                out.append(
                    {
                        "q": int((row or {}).get("q") or (len(out) + 1)),
                        "home": hh,
                        "away": aa,
                        "home_cum": hcum,
                        "away_cum": acum,
                    }
                )
            return out

        if quarters_override is not None:
            q = _with_cum(quarters_override)
            if home_score_override is not None:
                h = int(round(float(home_score_override)))
            else:
                h = int(round(float(sum(float(r.get("home") or 0.0) for r in q))))
            if away_score_override is not None:
                a = int(round(float(away_score_override)))
            else:
                a = int(round(float(sum(float(r.get("away") or 0.0) for r in q))))
        else:
            means = sim.get("means") or {}
            if use_means and isinstance(means, dict):
                h = int(round(float(means.get("home_score") or 0.0)))
                a = int(round(float(means.get("away_score") or 0.0)))
                q_raw = means.get("quarters") or []
                # means['quarters'] doesn't include cumulative; build it.
                q = _with_cum(q_raw)
            else:
                h = int(rep.get("home_score") or 0)
                a = int(rep.get("away_score") or 0)
                q = rep.get("quarters") or []

        if h == a:
            winner = None
            loser = None
            w_score = h
            l_score = a
            verb = "played"
        else:
            home_wins = h > a
            winner = home if home_wins else away
            loser = away if home_wins else home
            w_score = h if home_wins else a
            l_score = a if home_wins else h
            verb = "held off" if abs(h - a) <= 6 else ("pulled away from" if abs(h - a) >= 10 else "edged")

        # Key swing quarter
        swing_q = None
        swing_amt = 0
        prev = 0
        for row in q:
            cur = int(row.get("home_cum", 0) - row.get("away_cum", 0))
            d = cur - prev
            if abs(d) > abs(swing_amt):
                swing_amt = d
                swing_q = int(row.get("q", 0))
            prev = cur

        home_box = (rep.get("home_box") or {}).get("players") or []
        away_box = (rep.get("away_box") or {}).get("players") or []
        top = None
        for p in home_box + away_box:
            try:
                nm = str(p.get("player_name") or "").strip()
                if nm.lower().startswith("replacement"):
                    continue
            except Exception:
                pass
            if top is None or int(p.get("pts") or 0) > int(top.get("pts") or 0):
                top = p
        top_line = ""
        if top:
            top_line = f"In the representative box score, {top.get('player_name')} led the way with {int(top.get('pts') or 0)} points."

        mkt_line = ""
        try:
            tot = float(market_total) if market_total is not None else None
            spr = float(market_home_spread) if market_home_spread is not None else None
            if tot is not None:
                po = None
                if isinstance(probs, dict):
                    try:
                        po = float(probs.get("p_total_over")) if probs.get("p_total_over") is not None else None
                    except Exception:
                        po = None
                if po is not None and np.isfinite(po):
                    mkt_line += (
                        f" Expected total is about {int(round(h+a))} against a market total of {tot:.1f} "
                        f"(Over {po*100:.0f}%, Under {(1.0-po)*100:.0f}%)."
                    )
                else:
                    mkt_line += f" Expected total is about {int(round(h+a))} against a market total of {tot:.1f}."
            if spr is not None:
                ph = None
                if isinstance(probs, dict):
                    try:
                        ph = float(probs.get("p_home_cover")) if probs.get("p_home_cover") is not None else None
                    except Exception:
                        ph = None
                if ph is not None and np.isfinite(ph):
                    mkt_line += f" At {home} {spr:+.1f}, cover chances are {home} {ph*100:.0f}% and {away} {(1.0-ph)*100:.0f}%."
                else:
                    mkt_line += f" Spread line: {home} {spr:+.1f}."
        except Exception:
            pass

        q1 = q[0] if len(q) > 0 else {}
        q2 = q[1] if len(q) > 1 else {}
        q3 = q[2] if len(q) > 2 else {}
        q4 = q[3] if len(q) > 3 else {}

        lines = []
        if winner and loser:
            lines.append(f"{winner} {verb} {loser} {w_score}-{l_score} in a quarter-by-quarter grind.")
        else:
            lines.append(f"{home} and {away} played to a {h}-{a} draw through regulation.")
        if q1:
            lines.append(
                f"It started fast: {away} put up {int(round(float(q1.get('away',0) or 0)))} in the first, "
                f"but {home} answered with {int(round(float(q1.get('home',0) or 0)))}."
            )
        if q2:
            lines.append(
                f"By halftime it was {int(round(float(q2.get('home_cum',0) or 0)))}-"
                f"{int(round(float(q2.get('away_cum',0) or 0)))}, with both sides trading clean looks."
            )
        if q3 and swing_q == 3:
            lines.append(f"The third quarter swung the night — a {home if swing_amt>0 else away} burst flipped the tone.")
        elif q3:
            lines.append(f"The third brought the usual push, setting up a late finish.")
        if q4:
            lines.append(
                f"In the fourth, {home} scored {int(round(float(q4.get('home',0) or 0)))} "
                f"while {away} added {int(round(float(q4.get('away',0) or 0)))}, and that was enough to seal it."
            )
        if top_line:
            lines.append(top_line)
        if mkt_line:
            lines.append(mkt_line.strip())
        return " ".join([s for s in lines if s])
    except Exception:
        return ""
