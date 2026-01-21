from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .events import EventSimConfig, simulate_event_level_boxscore, simulate_pbp_game_boxscore
from .quarters import GameInputs, QuarterResult, TeamContext, simulate_quarters
from ..config import paths
from ..player_priors import PlayerPriorsConfig, compute_player_priors, _norm_player_key  # type: ignore
from ..teams import to_tricode


@dataclass
class SmartSimConfig:
    n_sims: int = 300
    seed: Optional[int] = None
    priors_days_back: int = 21

    # If true, run a unified possession-level sim that produces quarter/game scores
    # and player stats from one coherent event stream (no quarter targets + reconciliation).
    use_pbp: bool = True

    # Event-level controls
    event_cfg: EventSimConfig = field(default_factory=EventSimConfig)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if np.isfinite(v):
            return float(v)
        return float(default)
    except Exception:
        return float(default)


def _safe_str(x: Any) -> str:
    try:
        return str(x or "").strip()
    except Exception:
        return ""


def _team_players_from_props(props_df: pd.DataFrame, team_tri: str, opp_tri: str) -> pd.DataFrame:
    df = props_df.copy() if isinstance(props_df, pd.DataFrame) else pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    if "team" in df.columns:
        df["team"] = df["team"].astype(str).str.upper().str.strip()
    if "opponent" in df.columns:
        df["opponent"] = df["opponent"].astype(str).str.upper().str.strip()

    team_u = str(team_tri or "").upper().strip()
    opp_u = str(opp_tri or "").upper().strip()

    out = pd.DataFrame()
    if ("team" in df.columns) and ("opponent" in df.columns):
        out = df[(df["team"] == team_u) & (df["opponent"] == opp_u)].copy()
    if out.empty and ("team" in df.columns):
        out = df[df["team"] == team_u].copy()

    if out.empty:
        return out

    if "playing_today" in out.columns:
        try:
            pt = out["playing_today"].astype(str).str.lower().str.strip()
            out = out[~pt.isin(["false", "0", "no", "n"])].copy()
        except Exception:
            pass

    if "player_name" in out.columns:
        out = out[out["player_name"].astype(str).str.strip().ne("")].copy()

    return out


def _clean_id_str(x: Any) -> str:
    try:
        s = str(x or "").strip()
        if s.lower() in ("nan", "none"):
            return ""
        if s.endswith(".0") and s[:-2].isdigit():
            s = s[:-2]
        return s
    except Exception:
        return ""


def _infer_game_id(date_str: str, home_tri: str, away_tri: str) -> Optional[str]:
    """Infer NBA game_id (gid) for matchup using cached ESPN scoreboard helpers."""
    try:
        from ..boxscores import _nba_gid_to_tricodes  # type: ignore
    except Exception:
        return None
    try:
        gid_map = _nba_gid_to_tricodes(str(date_str))
        if not gid_map:
            return None
        h = str(home_tri or "").strip().upper()
        a = str(away_tri or "").strip().upper()
        for gid, (hh, aa) in gid_map.items():
            if str(hh).strip().upper() == h and str(aa).strip().upper() == a:
                return str(gid).strip()
        return None
    except Exception:
        return None


def _read_rotation_stints(game_id: str, side: str) -> pd.DataFrame:
    if not game_id:
        return pd.DataFrame()
    side_u = str(side or "").strip().lower()
    if side_u not in {"home", "away"}:
        return pd.DataFrame()
    fp = paths.data_processed / "rotations_espn" / f"stints_{side_u}_{str(game_id).strip()}.csv"
    if not fp.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()


def _build_player_minutes_from_stints(stints: pd.DataFrame) -> pd.DataFrame:
    """Return per-player minutes from stints (ESPN athlete IDs)."""
    if stints is None or stints.empty:
        return pd.DataFrame()
    need = {"team", "duration_sec", "lineup_player_ids"}
    if not need.issubset(set(stints.columns)):
        return pd.DataFrame()

    tmp = stints[["team", "duration_sec", "lineup_player_ids"]].copy()
    tmp["team"] = tmp["team"].astype(str).str.upper().str.strip()
    tmp["duration_sec"] = pd.to_numeric(tmp["duration_sec"], errors="coerce").fillna(0.0)
    tmp["player_id"] = tmp["lineup_player_ids"].astype(str).str.split(";")
    tmp = tmp.explode("player_id")
    tmp["player_id"] = tmp["player_id"].map(_clean_id_str)
    tmp = tmp[tmp["player_id"].astype(str).str.len() > 0]
    if tmp.empty:
        return pd.DataFrame()

    out = tmp.groupby(["team", "player_id"], as_index=False)["duration_sec"].sum()
    out["minutes"] = out["duration_sec"].astype(float) / 60.0
    return out


def _espn_name_to_id_map_for_game(
    date_str: str,
    home_tri: str,
    away_tri: str,
    event_id: Optional[str] = None,
) -> dict[tuple[str, str], str]:
    """Return mapping (team_tricode, normalized_player_key) -> espn_athlete_id.

    Prefer providing event_id (e.g., from rotation stints) to avoid relying on scoreboard lookup.
    """
    if not str(event_id or "").strip() and not str(date_str or "").strip():
        return {}
    try:
        from ..boxscores import _espn_event_id_for_matchup, _espn_summary, _espn_to_tri  # type: ignore
    except Exception:
        return {}

    try:
        eid = str(event_id or "").strip() or (
            _espn_event_id_for_matchup(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri)) or ""
        )
        if not eid:
            return {}
        summ = _espn_summary(eid)
        box = (summ or {}).get("boxscore") or {}
        teams = box.get("players") or []
        if not isinstance(teams, list) or not teams:
            return {}

        out: dict[tuple[str, str], str] = {}
        for tp in teams:
            team = (tp or {}).get("team") or {}
            team_ab = str(team.get("abbreviation") or "").strip().upper()
            team_tri = _espn_to_tri(team_ab) if team_ab else ""
            stats_groups = (tp or {}).get("statistics") or []
            if not isinstance(stats_groups, list) or not stats_groups:
                continue
            g0 = stats_groups[0] or {}
            athletes = g0.get("athletes") or []
            if not isinstance(athletes, list):
                continue

            for a in athletes:
                if not isinstance(a, dict):
                    continue
                athlete = a.get("athlete") or {}
                pid = _clean_id_str(athlete.get("id"))
                name = str(athlete.get("displayName") or "").strip()
                if not pid or not name or not team_tri:
                    continue
                key = _norm_player_key(name)
                if not key:
                    continue
                out[(str(team_tri).upper().strip(), str(key).upper().strip())] = pid
        return out
    except Exception:
        return {}


def _team_players_from_espn_boxscore(
    date_str: str,
    home_tri: str,
    away_tri: str,
    team_tri: str,
    event_id: Optional[str] = None,
) -> pd.DataFrame:
    """Fallback roster builder: use ESPN summary boxscore to produce team player list."""
    try:
        from ..boxscores import _espn_event_id_for_matchup, _espn_summary, _espn_to_tri  # type: ignore
    except Exception:
        return pd.DataFrame()

    try:
        eid = str(event_id or "").strip() or (
            _espn_event_id_for_matchup(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri)) or ""
        )
        if not eid:
            return pd.DataFrame()
        summ = _espn_summary(eid)
        box = (summ or {}).get("boxscore") or {}
        teams = box.get("players") or []
        if not isinstance(teams, list) or not teams:
            return pd.DataFrame()

        team_u = str(team_tri or "").strip().upper()
        opp_u = str(away_tri if team_u == str(home_tri).strip().upper() else home_tri).strip().upper()

        rows: list[dict[str, Any]] = []
        for tp in teams:
            team = (tp or {}).get("team") or {}
            team_ab = str(team.get("abbreviation") or "").strip().upper()
            tri = _espn_to_tri(team_ab) if team_ab else ""
            if str(tri).upper().strip() != team_u:
                continue
            stats_groups = (tp or {}).get("statistics") or []
            if not isinstance(stats_groups, list) or not stats_groups:
                continue
            g0 = stats_groups[0] or {}
            athletes = g0.get("athletes") or []
            if not isinstance(athletes, list):
                continue
            for a in athletes:
                if not isinstance(a, dict):
                    continue
                athlete = a.get("athlete") or {}
                name = str(athlete.get("displayName") or athlete.get("shortName") or "").strip()
                if not name:
                    continue
                rows.append({
                    "player_name": name,
                    "team": team_u,
                    "opponent": opp_u,
                    "playing_today": True,
                })

        out = pd.DataFrame(rows)
        if out is None or out.empty:
            return pd.DataFrame()
        out = out.drop_duplicates(subset=["player_name", "team"], keep="last")
        return out
    except Exception:
        return pd.DataFrame()


def _roll_minutes_unscaled(team_df: pd.DataFrame) -> pd.Series:
    if team_df is None or team_df.empty:
        return pd.Series(dtype=float)
    min_cols = [c for c in ("roll10_min", "roll5_min", "roll3_min", "lag1_min") if c in team_df.columns]
    if not min_cols:
        return pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
    mins = pd.to_numeric(team_df[min_cols[0]], errors="coerce").fillna(0.0).astype(float)
    return mins.clip(lower=0.0, upper=44.0)


def _rotation_sim_minutes_for_team(
    team_df: pd.DataFrame,
    date_str: str,
    home_tri: str,
    away_tri: str,
    team_tri: str,
    side: str,
    game_id: Optional[str],
) -> tuple[Optional[pd.Series], Optional[List[List[int]]], Optional[np.ndarray], dict[str, Any]]:
    diag: dict[str, Any] = {
        "attempted": True,
        "applied": False,
        "side": str(side).lower().strip(),
        "team": str(team_tri).upper().strip(),
        "game_id": str(game_id or "").strip(),
    }
    if team_df is None or team_df.empty:
        diag["reason"] = "empty_players"
        return None, None, None, diag

    gid = str(game_id or "").strip()
    if not gid:
        diag["reason"] = "missing_game_id"
        return None, None, None, diag

    stints = _read_rotation_stints(gid, side=side)
    if stints is None or stints.empty:
        diag["reason"] = "missing_stints_file"
        return None, None, None, diag

    eid = ""
    try:
        if "event_id" in stints.columns:
            eid = str(stints["event_id"].dropna().astype(str).head(1).iloc[0] or "").strip()
            if eid:
                diag["event_id"] = eid
    except Exception:
        eid = ""

    mins_df = _build_player_minutes_from_stints(stints)
    if mins_df.empty:
        diag["reason"] = "no_minutes_from_stints"
        return None, None, None, diag

    team_u = str(team_tri or "").strip().upper()
    mins_df = mins_df[mins_df["team"].astype(str).str.upper().str.strip() == team_u].copy()
    if mins_df.empty:
        diag["reason"] = "team_not_in_stints"
        return None, None, None, diag

    # Map our player names to ESPN athlete IDs for the matchup.
    name_to_id = _espn_name_to_id_map_for_game(
        str(date_str),
        home_tri=str(home_tri),
        away_tri=str(away_tri),
        event_id=eid or None,
    )
    if not name_to_id:
        diag["reason"] = "no_espn_name_map"
        return None, None, None, diag

    tmp = team_df.copy().reset_index(drop=True)
    tmp["_pkey"] = tmp.get("player_name", pd.Series(["" for _ in range(len(tmp))])).map(_norm_player_key)
    tmp["_espn_id"] = tmp["_pkey"].map(lambda k: name_to_id.get((team_u, str(k).upper().strip()), ""))
    tmp["_espn_id"] = tmp["_espn_id"].astype(str).replace({"nan": "", "None": ""}).str.strip()

    mins_df["player_id"] = mins_df["player_id"].astype(str).map(_clean_id_str)
    mins_df["minutes"] = pd.to_numeric(mins_df["minutes"], errors="coerce").fillna(0.0).astype(float)
    id_to_min = dict(zip(mins_df["player_id"].astype(str), mins_df["minutes"].astype(float)))

    total_target = float(mins_df["minutes"].sum())
    diag["rotation_total_minutes"] = total_target

    # Assign mapped minutes; handle duplicated ESPN IDs by splitting proportionally.
    base_w = _roll_minutes_unscaled(tmp)
    sim_min = pd.Series([0.0] * len(tmp), index=tmp.index, dtype=float)

    espn_ids = tmp["_espn_id"].astype(str)
    have = espn_ids.str.len() > 0

    mapped_players = 0
    mapped_minutes_sum = 0.0

    for pid in sorted(set(espn_ids[have].tolist())):
        m = float(id_to_min.get(str(pid), 0.0))
        if m <= 0:
            continue
        idx = tmp.index[espn_ids == pid]
        if len(idx) == 0:
            continue
        w = base_w.loc[idx].astype(float)
        ws = float(w.sum())
        if not np.isfinite(ws) or ws <= 0:
            # even split
            alloc = pd.Series([m / float(len(idx))] * len(idx), index=idx, dtype=float)
        else:
            alloc = (w / ws) * m
        sim_min.loc[idx] = alloc.astype(float)
        mapped_players += int(len(idx))
        mapped_minutes_sum += float(alloc.sum())

    diag["mapped_players"] = int(mapped_players)
    diag["mapped_minutes"] = float(mapped_minutes_sum)

    # Fill leftover minutes with a proportional fallback across unmapped players.
    leftover = float(total_target - mapped_minutes_sum)
    diag["leftover_minutes"] = float(leftover)

    if leftover > 1e-6:
        unm = sim_min <= 0
        w = base_w.loc[unm].astype(float)
        ws = float(w.sum())
        if (not np.isfinite(ws)) or ws <= 0:
            if int(unm.sum()) > 0:
                sim_min.loc[unm] = float(leftover) / float(int(unm.sum()))
        else:
            sim_min.loc[unm] = (w / ws) * float(leftover)

    # If we somehow over-allocated (rare), scale down gently.
    total_sim = float(sim_min.sum())
    if np.isfinite(total_target) and total_target > 0 and np.isfinite(total_sim) and total_sim > 0:
        sim_min = sim_min * (total_target / total_sim)

    # Build observed lineup pool from stints (5-man units) mapped to row indices.
    lineup_pool: List[List[int]] = []
    lineup_w: List[float] = []
    try:
        if {"lineup_player_ids", "duration_sec"}.issubset(set(stints.columns)):
            s2 = stints.copy()
            s2["team"] = s2.get("team", "").astype(str).str.upper().str.strip()
            s2 = s2[s2["team"] == team_u].copy()
            s2["duration_sec"] = pd.to_numeric(s2["duration_sec"], errors="coerce").fillna(0.0).astype(float)
            for _, r in s2.iterrows():
                lu = str(r.get("lineup_player_ids") or "").strip()
                if not lu:
                    continue
                pids = [p.strip() for p in lu.split(";") if p.strip()]
                if len(pids) < 5:
                    continue
                idxs: List[int] = []
                for pid in pids:
                    cand = tmp.index[tmp["_espn_id"].astype(str) == str(pid)].tolist()
                    if cand:
                        idxs.append(int(cand[0]))
                idxs_u = list(dict.fromkeys(idxs))
                if len(idxs_u) == 5:
                    lineup_pool.append([int(x) for x in idxs_u])
                    lineup_w.append(float(r.get("duration_sec") or 0.0))
    except Exception:
        lineup_pool = []
        lineup_w = []

    diag["lineup_pool_n"] = int(len(lineup_pool))

    diag["applied"] = True
    diag["sim_minutes_sum"] = float(sim_min.sum())
    return sim_min.astype(float), (lineup_pool if lineup_pool else None), (np.asarray(lineup_w, dtype=float) if lineup_w else None), diag


def _derive_sim_minutes(team_df: pd.DataFrame) -> pd.Series:
    if team_df is None or team_df.empty:
        return pd.Series(dtype=float)

    # Prefer roll10/roll5 minutes from props predictions.
    min_cols = [c for c in ("roll10_min", "roll5_min", "roll3_min", "lag1_min") if c in team_df.columns]
    if not min_cols:
        mins = pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
    else:
        mins = pd.to_numeric(team_df[min_cols[0]], errors="coerce").fillna(0.0).astype(float)

    mins = mins.clip(lower=0.0, upper=44.0)

    s = float(mins.sum())
    if not np.isfinite(s) or s <= 0:
        mins = pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
        s = float(mins.sum())

    # Scale to 240 team minutes.
    scale = 240.0 / max(1e-6, s)
    mins = (mins * scale).clip(lower=0.0, upper=44.0)

    # Rebalance if caps changed total.
    s2 = float(mins.sum())
    if np.isfinite(s2) and s2 > 0:
        mins = mins * (240.0 / s2)

    return mins


def _apply_player_priors(team_df: pd.DataFrame, priors, team_tri: str, sim_minutes: Optional[pd.Series] = None) -> pd.DataFrame:
    if team_df is None or team_df.empty:
        return pd.DataFrame()

    out = team_df.copy()
    out["_pkey"] = out.get("player_name", "").map(_norm_player_key)

    # Minutes (rotation-based when available)
    if sim_minutes is not None and len(sim_minutes) == len(out):
        out["_sim_min"] = pd.to_numeric(sim_minutes, errors="coerce").fillna(0.0).astype(float)
    else:
        out["_sim_min"] = _derive_sim_minutes(out)

    # Prediction-derived per-minute fallbacks
    pred_cols = {
        "pts": "pred_pts",
        "reb": "pred_reb",
        "ast": "pred_ast",
        "threes": "pred_threes",
        "stl": "pred_stl",
        "blk": "pred_blk",
        "tov": "pred_tov",
    }

    sim_min = pd.to_numeric(out["_sim_min"], errors="coerce").fillna(0.0).astype(float)
    sim_min_safe = sim_min.where(sim_min > 0.0, other=1.0)

    for stat, col in pred_cols.items():
        if col in out.columns:
            per_min = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype(float) / sim_min_safe
            out[f"_pred_{stat}_pm"] = per_min.clip(lower=0.0)
        else:
            out[f"_pred_{stat}_pm"] = 0.0

    # Priors mapping from compute_player_priors
    def _rate_row(r: pd.Series) -> Dict[str, float]:
        try:
            team_u = str(team_tri or "").strip().upper()
            key = str(r.get("_pkey") or "").strip().upper()
            return priors.rates.get((team_u, key), {})
        except Exception:
            return {}

    pri_map = out.apply(_rate_row, axis=1)

    def _get_rate(i: int, k: str) -> float:
        try:
            rr = pri_map.iloc[int(i)]
            return _safe_float(rr.get(k), 0.0)
        except Exception:
            return 0.0

    # Fill key per-minute rates.
    stat_pm_keys = [
        ("pts", "pts_pm"),
        ("reb", "reb_pm"),
        ("ast", "ast_pm"),
        ("stl", "stl_pm"),
        ("blk", "blk_pm"),
        ("tov", "tov_pm"),
        ("threes", "threes_pm"),
        ("threes_att", "threes_att_pm"),
        ("fga", "fga_pm"),
        ("fgm", "fgm_pm"),
        ("fta", "fta_pm"),
        ("ftm", "ftm_pm"),
        ("pf", "pf_pm"),
    ]

    for out_name, pri_key in stat_pm_keys:
        out[f"_prior_{out_name}_pm"] = [float(_get_rate(i, pri_key)) for i in range(len(out))]

    # Backfill missing priors with prediction-derived rates where available.
    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
        col = f"_prior_{stat}_pm"
        pred_col = f"_pred_{stat}_pm"
        if col in out.columns and pred_col in out.columns:
            out[col] = np.where(out[col] > 0.0, out[col], out[pred_col])

    # Defensive baseline if we have no point priors at all (e.g., missing props + missing priors).
    try:
        pts_pm = pd.to_numeric(out.get("_prior_pts_pm"), errors="coerce").fillna(0.0).astype(float)
        if float(pts_pm.sum()) <= 0:
            out["_prior_pts_pm"] = np.where(sim_min > 0, 0.55, 0.0)  # ~20 pts per 36
    except Exception:
        pass

    # For attempt/make rates: if missing, infer from points/3s and conservative defaults.
    fga = pd.to_numeric(out.get("_prior_fga_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(fga.sum()) <= 0:
        # roughly 0.55 FGA/min is a reasonable starter (~20 FGA in 36 min)
        out["_prior_fga_pm"] = np.where(sim_min > 0, 0.55, 0.0)
        fga = pd.to_numeric(out["_prior_fga_pm"], errors="coerce").fillna(0.0).astype(float)

    fg3a = pd.to_numeric(out.get("_prior_threes_att_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(fg3a.sum()) <= 0:
        out["_prior_threes_att_pm"] = 0.36 * fga

    fgm = pd.to_numeric(out.get("_prior_fgm_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(fgm.sum()) <= 0:
        out["_prior_fgm_pm"] = 0.46 * fga

    fg3m = pd.to_numeric(out.get("_prior_threes_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(fg3m.sum()) <= 0:
        out["_prior_threes_pm"] = 0.35 * pd.to_numeric(out["_prior_threes_att_pm"], errors="coerce").fillna(0.0).astype(float)

    fta = pd.to_numeric(out.get("_prior_fta_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(fta.sum()) <= 0:
        out["_prior_fta_pm"] = 0.18 * fga

    ftm = pd.to_numeric(out.get("_prior_ftm_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(ftm.sum()) <= 0:
        out["_prior_ftm_pm"] = 0.76 * pd.to_numeric(out["_prior_fta_pm"], errors="coerce").fillna(0.0).astype(float)

    pf = pd.to_numeric(out.get("_prior_pf_pm"), errors="coerce").fillna(0.0).astype(float)
    if float(pf.sum()) <= 0:
        out["_prior_pf_pm"] = 0.085  # ~3.0 PF per 36

    # Cleanup helper columns
    return out


def _quantiles(x: np.ndarray, qs: Tuple[float, ...] = (0.1, 0.5, 0.9)) -> Dict[str, float]:
    arr = np.asarray(x, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return {f"p{int(q*100)}": float("nan") for q in qs}
    out: Dict[str, float] = {}
    for q in qs:
        out[f"p{int(q*100)}"] = float(np.quantile(arr, q))
    return out


def _market_lines_from_processed_odds(date_str: str, home_tri: str, away_tri: str) -> tuple[Optional[float], Optional[float]]:
    """Best-effort market lines from data/processed/game_odds_<date>.csv."""
    fp = paths.data_processed / f"game_odds_{str(date_str).strip()}.csv"
    if not fp.exists():
        return None, None
    try:
        odf = pd.read_csv(fp)
        if odf is None or odf.empty:
            return None, None
        odf = odf.copy()
        odf["home_tri"] = odf.get("home_team", "").astype(str).map(to_tricode)
        odf["away_tri"] = odf.get("visitor_team", "").astype(str).map(to_tricode)
        m = odf[(odf["home_tri"] == str(home_tri).upper()) & (odf["away_tri"] == str(away_tri).upper())].head(1)
        if m.empty:
            return None, None
        r = m.iloc[0]
        total = pd.to_numeric(r.get("total"), errors="coerce")
        spread = pd.to_numeric(r.get("home_spread"), errors="coerce")
        total = float(total) if np.isfinite(total) else None
        spread = float(spread) if np.isfinite(spread) else None
        return total, spread
    except Exception:
        return None, None


def _period_lines_from_processed(date_str: str, home_tri: str, away_tri: str) -> Optional[dict[str, Any]]:
    """Best-effort period lines (Q1-Q4 + H1) from data/processed/period_lines_<date>.csv."""
    fp = paths.data_processed / f"period_lines_{str(date_str).strip()}.csv"
    if not fp.exists():
        return None
    try:
        df = pd.read_csv(fp)
        if df is None or df.empty:
            return None
        df = df.copy()
        df["home_tri"] = df.get("home_team", "").astype(str).map(to_tricode)
        df["away_tri"] = df.get("visitor_team", "").astype(str).map(to_tricode)
        m = df[(df["home_tri"] == str(home_tri).upper()) & (df["away_tri"] == str(away_tri).upper())].head(1)
        if m.empty:
            return None
        r = m.iloc[0].to_dict()
        out: dict[str, Any] = {}
        for k, v in r.items():
            if k in ("date", "home_team", "visitor_team", "home_tri", "away_tri"):
                continue
            try:
                vv = pd.to_numeric(v, errors="coerce")
                out[k] = float(vv) if np.isfinite(vv) else None
            except Exception:
                out[k] = None
        return out
    except Exception:
        return None


def simulate_smart_game(
    date_str: str,
    home_tri: str,
    away_tri: str,
    props_df: pd.DataFrame,
    quarters: Optional[List[QuarterResult]] = None,
    market_total: Optional[float] = None,
    market_home_spread: Optional[float] = None,
    game_id: Optional[str] = None,
    cfg: Optional[SmartSimConfig] = None,
) -> Dict[str, Any]:
    cfg = cfg or SmartSimConfig()
    rng = np.random.default_rng(cfg.seed)

    if market_total is None or market_home_spread is None:
        t2, s2 = _market_lines_from_processed_odds(date_str=date_str, home_tri=home_tri, away_tri=away_tri)
        if market_total is None:
            market_total = t2
        if market_home_spread is None:
            market_home_spread = s2

    # Quarter distribution
    if quarters is None:
        # Minimal fallback TeamContext from prediction-implied ratings. (Caller should prefer passing quarters.)
        home_ctx = TeamContext(team=home_tri, pace=98.0, off_rating=112.0, def_rating=112.0)
        away_ctx = TeamContext(team=away_tri, pace=98.0, off_rating=112.0, def_rating=112.0)
        inp = GameInputs(date=date_str, home=home_ctx, away=away_ctx, market_total=market_total, market_home_spread=market_home_spread)
        quarters = simulate_quarters(inp, n_samples=3000).quarters

    # Player priors
    pri_cfg = PlayerPriorsConfig(days_back=int(cfg.priors_days_back))
    pri = compute_player_priors(date_str, pri_cfg)

    home_raw = _team_players_from_props(props_df, home_tri, away_tri)
    away_raw = _team_players_from_props(props_df, away_tri, home_tri)

    # Fallback: if props predictions do not include this matchup, use ESPN boxscore roster.
    if home_raw is None or home_raw.empty:
        home_raw = _team_players_from_espn_boxscore(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=home_tri)
    if away_raw is None or away_raw.empty:
        away_raw = _team_players_from_espn_boxscore(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=away_tri)

    home_raw = home_raw.reset_index(drop=True) if isinstance(home_raw, pd.DataFrame) else pd.DataFrame()
    away_raw = away_raw.reset_index(drop=True) if isinstance(away_raw, pd.DataFrame) else pd.DataFrame()

    gid = str(game_id or "").strip() or (_infer_game_id(date_str, home_tri=home_tri, away_tri=away_tri) or "")
    rot_home_min, home_lineups, home_lineup_w, rot_home_diag = _rotation_sim_minutes_for_team(
        home_raw,
        date_str=date_str,
        home_tri=home_tri,
        away_tri=away_tri,
        team_tri=home_tri,
        side="home",
        game_id=gid,
    )
    rot_away_min, away_lineups, away_lineup_w, rot_away_diag = _rotation_sim_minutes_for_team(
        away_raw,
        date_str=date_str,
        home_tri=home_tri,
        away_tri=away_tri,
        team_tri=away_tri,
        side="away",
        game_id=gid,
    )

    home_players = _apply_player_priors(home_raw, pri, team_tri=home_tri, sim_minutes=rot_home_min)
    away_players = _apply_player_priors(away_raw, pri, team_tri=away_tri, sim_minutes=rot_away_min)

    # Optional: lineup-conditioned teammate effects (learned from historical play context + rotation pairs).
    lineup_effects_diag: dict[str, Any] = {"home": None, "away": None}
    try:
        from .connected_game import _apply_lineup_teammate_effects_to_priors  # type: ignore

        eid = str(rot_home_diag.get("event_id") or rot_away_diag.get("event_id") or "").strip() or None
        home_players = _apply_lineup_teammate_effects_to_priors(
            home_players,
            team_tri=str(home_tri),
            date_str=str(date_str),
            home_tri=str(home_tri),
            away_tri=str(away_tri),
            event_id=eid,
        )
        away_players = _apply_lineup_teammate_effects_to_priors(
            away_players,
            team_tri=str(away_tri),
            date_str=str(date_str),
            home_tri=str(home_tri),
            away_tri=str(away_tri),
            event_id=eid,
        )

        try:
            lineup_effects_diag["home"] = dict(getattr(home_players, "attrs", {}).get("_lineup_effects", {}) or {})
        except Exception:
            lineup_effects_diag["home"] = None
        try:
            lineup_effects_diag["away"] = dict(getattr(away_players, "attrs", {}).get("_lineup_effects", {}) or {})
        except Exception:
            lineup_effects_diag["away"] = None
    except Exception:
        lineup_effects_diag = {"home": None, "away": None}

    if home_players.empty or away_players.empty:
        return {
            "error": "missing_players",
            "home": home_tri,
            "away": away_tri,
            "home_players": int(len(home_players)),
            "away_players": int(len(away_players)),
        }

    n_sims = int(max(1, cfg.n_sims))

    # Target means used only to gently calibrate possession efficiency in PBP mode.
    # This keeps the event stream aligned to the model/market expectations without
    # forcibly reconciling quarter scores.
    try:
        target_home_points = float(sum(float(q.home_pts_mu) for q in (quarters or [])))
        target_away_points = float(sum(float(q.away_pts_mu) for q in (quarters or [])))
        if not np.isfinite(target_home_points):
            target_home_points = None  # type: ignore[assignment]
        if not np.isfinite(target_away_points):
            target_away_points = None  # type: ignore[assignment]
    except Exception:
        target_home_points = None
        target_away_points = None

    hq = None
    aq = None

    period_lines = _period_lines_from_processed(date_str=date_str, home_tri=home_tri, away_tri=away_tri) or {}

    def _period_quantiles(arr: np.ndarray) -> dict[str, float]:
        arr = np.asarray(arr, dtype=float)
        return _quantiles(arr)

    def _summarize_period(name: str, h: np.ndarray, a: np.ndarray, total_line: Optional[float], spread_line: Optional[float]) -> dict[str, Any]:
        h = np.asarray(h, dtype=float)
        a = np.asarray(a, dtype=float)
        margin = h - a
        total = h + a
        out: dict[str, Any] = {
            "home_mean": float(np.mean(h)),
            "away_mean": float(np.mean(a)),
            "margin_mean": float(np.mean(margin)),
            "total_mean": float(np.mean(total)),
            "home_q": _period_quantiles(h),
            "away_q": _period_quantiles(a),
            "margin_q": _period_quantiles(margin),
            "total_q": _period_quantiles(total),
            "p_home_win": float(np.mean(margin > 0.0)),
        }
        if spread_line is not None:
            try:
                out["market_home_spread"] = float(spread_line)
                out["p_home_cover"] = float(np.mean((margin + float(spread_line)) > 0.0))
            except Exception:
                out["market_home_spread"] = float(spread_line)
                out["p_home_cover"] = None
        else:
            out["market_home_spread"] = None
            out["p_home_cover"] = None
        if total_line is not None:
            try:
                out["market_total"] = float(total_line)
                out["p_total_over"] = float(np.mean(total > float(total_line)))
            except Exception:
                out["market_total"] = float(total_line)
                out["p_total_over"] = None
        else:
            out["market_total"] = None
            out["p_total_over"] = None
        out["name"] = name
        return out

    periods: dict[str, Any] = {}

    # Accumulators
    home_scores = np.zeros(n_sims, dtype=int)
    away_scores = np.zeros(n_sims, dtype=int)

    h_names = [str(x or "").strip() for x in home_players.get("player_name", pd.Series([], dtype=str)).tolist()]
    a_names = [str(x or "").strip() for x in away_players.get("player_name", pd.Series([], dtype=str)).tolist()]

    def _blank_player_store(names: List[str]) -> Dict[str, Dict[str, List[int]]]:
        return {
            n: {"pts": [], "reb": [], "ast": [], "threes": [], "stl": [], "blk": [], "tov": []}
            for n in names
            if n
        }

    home_store = _blank_player_store(h_names)
    away_store = _blank_player_store(a_names)

    # Quarter score arrays (filled either by quarter-sampling or by PBP sim)
    hq_sims = np.zeros((n_sims, 4), dtype=int)
    aq_sims = np.zeros((n_sims, 4), dtype=int)

    if cfg.use_pbp:
        for i in range(n_sims):
            h_box, a_box, hq_i, aq_i = simulate_pbp_game_boxscore(
                rng=rng,
                home_players=home_players,
                away_players=away_players,
                cfg=cfg.event_cfg,
                home_lineups=home_lineups,
                home_lineup_weights=home_lineup_w,
                away_lineups=away_lineups,
                away_lineup_weights=away_lineup_w,
                target_home_points=target_home_points,
                target_away_points=target_away_points,
                quarters=quarters,
            )

            hq_sims[i, :] = np.asarray(list(hq_i or [0, 0, 0, 0])[:4], dtype=int)
            aq_sims[i, :] = np.asarray(list(aq_i or [0, 0, 0, 0])[:4], dtype=int)
            home_scores[i] = int(np.sum(hq_sims[i, :]))
            away_scores[i] = int(np.sum(aq_sims[i, :]))

            for p in (h_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in home_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        home_store[name][stat].append(int((p or {}).get(stat) or 0))

            for p in (a_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in away_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        away_store[name][stat].append(int((p or {}).get(stat) or 0))
    else:
        # Legacy path: sample quarter totals first and reconcile event stream to match.
        from .quarters import sample_quarter_scores

        hq, aq = sample_quarter_scores(quarters, n_samples=n_sims, rng=rng, round_to_int=True)
        for i in range(n_sims):
            hq_i = [int(x) for x in hq[i, :].tolist()]
            aq_i = [int(x) for x in aq[i, :].tolist()]

            h_box, a_box = simulate_event_level_boxscore(
                rng=rng,
                home_players=home_players,
                away_players=away_players,
                home_q_pts=hq_i,
                away_q_pts=aq_i,
                cfg=cfg.event_cfg,
                home_lineups=home_lineups,
                home_lineup_weights=home_lineup_w,
                away_lineups=away_lineups,
                away_lineup_weights=away_lineup_w,
            )

            hq_sims[i, :] = np.asarray(list(hq_i or [0, 0, 0, 0])[:4], dtype=int)
            aq_sims[i, :] = np.asarray(list(aq_i or [0, 0, 0, 0])[:4], dtype=int)

            hs = int((h_box or {}).get("team_total_pts") or int(sum(hq_i)))
            aw = int((a_box or {}).get("team_total_pts") or int(sum(aq_i)))
            home_scores[i] = hs
            away_scores[i] = aw

            for p in (h_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in home_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        home_store[name][stat].append(int((p or {}).get(stat) or 0))

            for p in (a_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in away_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        away_store[name][stat].append(int((p or {}).get(stat) or 0))

    # Period summaries from simulated quarter paths (works for both modes)
    try:
        for i in range(4):
            qi = i + 1
            tline = period_lines.get(f"q{qi}_total")
            sline = period_lines.get(f"q{qi}_spread")
            tline = float(tline) if tline is not None and np.isfinite(tline) else None
            sline = float(sline) if sline is not None and np.isfinite(sline) else None
            periods[f"q{qi}"] = _summarize_period(f"q{qi}", hq_sims[:, i], aq_sims[:, i], tline, sline)

        h1_h = hq_sims[:, 0] + hq_sims[:, 1]
        h1_a = aq_sims[:, 0] + aq_sims[:, 1]
        tline = period_lines.get("h1_total")
        sline = period_lines.get("h1_spread")
        tline = float(tline) if tline is not None and np.isfinite(tline) else None
        sline = float(sline) if sline is not None and np.isfinite(sline) else None
        periods["h1"] = _summarize_period("h1", h1_h, h1_a, tline, sline)

        h2_h = hq_sims[:, 2] + hq_sims[:, 3]
        h2_a = aq_sims[:, 2] + aq_sims[:, 3]
        periods["h2"] = _summarize_period("h2", h2_h, h2_a, None, None)
    except Exception:
        periods = {}

    margin = home_scores - away_scores
    total = home_scores + away_scores

    def _team_player_summaries(store: Dict[str, Dict[str, List[int]]]) -> List[Dict[str, Any]]:
        out_rows: List[Dict[str, Any]] = []
        for name, stats in store.items():
            row: Dict[str, Any] = {"player_name": name}
            for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                arr = np.asarray(stats.get(stat) or [], dtype=float)
                row[f"{stat}_mean"] = float(np.mean(arr)) if arr.size else float("nan")
                row[f"{stat}_sd"] = float(np.std(arr)) if arr.size else float("nan")
                row[f"{stat}_q"] = _quantiles(arr)
            # Derived props
            pra = np.asarray(stats.get("pts") or [], dtype=float) + np.asarray(stats.get("reb") or [], dtype=float) + np.asarray(stats.get("ast") or [], dtype=float)
            row["pra_mean"] = float(np.mean(pra)) if pra.size else float("nan")
            row["pra_sd"] = float(np.std(pra)) if pra.size else float("nan")
            row["pra_q"] = _quantiles(pra)
            out_rows.append(row)
        out_rows.sort(key=lambda r: float(r.get("pts_mean") or 0.0), reverse=True)
        return out_rows

    p_home_win = float(np.mean(margin > 0))
    p_away_win = float(1.0 - p_home_win)

    p_home_cover = None
    p_total_over = None
    if market_home_spread is not None:
        try:
            line = float(market_home_spread)
            p_home_cover = float(np.mean((margin + line) > 0))
        except Exception:
            p_home_cover = None
    if market_total is not None:
        try:
            line = float(market_total)
            p_total_over = float(np.mean(total > line))
        except Exception:
            p_total_over = None

    return {
        "home": str(home_tri).upper(),
        "away": str(away_tri).upper(),
        "date": str(date_str),
        "game_id": str(gid) if gid else None,
        "market": {
            "market_total": float(market_total) if market_total is not None else None,
            "market_home_spread": float(market_home_spread) if market_home_spread is not None else None,
        },
        "rotation_minutes": {
            "home": rot_home_diag,
            "away": rot_away_diag,
        },
        "lineup_effects": lineup_effects_diag,
        "n_sims": int(n_sims),
        "mode": {
            "use_pbp": bool(cfg.use_pbp),
            "target_home_points": float(target_home_points) if target_home_points is not None else None,
            "target_away_points": float(target_away_points) if target_away_points is not None else None,
        },
        "periods": periods,
        "score": {
            "home_mean": float(np.mean(home_scores)),
            "away_mean": float(np.mean(away_scores)),
            "margin_mean": float(np.mean(margin)),
            "total_mean": float(np.mean(total)),
            "home_q": _quantiles(home_scores),
            "away_q": _quantiles(away_scores),
            "margin_q": _quantiles(margin),
            "total_q": _quantiles(total),
            "p_home_win": p_home_win,
            "p_away_win": p_away_win,
            "p_home_cover": p_home_cover,
            "p_total_over": p_total_over,
        },
        "players": {
            "home": _team_player_summaries(home_store),
            "away": _team_player_summaries(away_store),
        },
    }
