from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .events import EventSimConfig, simulate_event_level_boxscore, simulate_pbp_game_boxscore
from .quarters import GameInputs, QuarterResult, TeamContext, simulate_quarters
from ..config import paths
from ..player_priors import PlayerPriorsConfig, compute_player_priors, _norm_player_key  # type: ignore
from ..teams import to_tricode


@lru_cache(maxsize=64)
def _load_pregame_expected_minutes(date_str: str) -> pd.DataFrame:
    """Load pregame expected minutes artifact for a slate date.

    Expected path: data/processed/pregame_expected_minutes_<YYYY-MM-DD>.csv
    """
    ds = str(date_str).strip()
    fp_csv = paths.data_processed / f"pregame_expected_minutes_{ds}.csv"
    fp_parq = paths.data_processed / f"pregame_expected_minutes_{ds}.parquet"

    fp = fp_csv if fp_csv.exists() else fp_parq
    if fp is None or (not fp.exists()):
        return pd.DataFrame()

    try:
        if fp.suffix.lower() == ".parquet":
            try:
                df = pd.read_parquet(fp)
            except Exception:
                # parquet is optional; fall back to CSV if present
                if fp_csv.exists():
                    df = pd.read_csv(fp_csv)
                else:
                    return pd.DataFrame()
        else:
            df = pd.read_csv(fp)
    except Exception:
        return pd.DataFrame()

    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # Normalize schema
    if "team_tri" not in out.columns:
        if "team" in out.columns:
            out = out.rename(columns={"team": "team_tri"})
        elif "team_abbrev" in out.columns:
            out = out.rename(columns={"team_abbrev": "team_tri"})
    if "team_tri" in out.columns:
        out["team_tri"] = out["team_tri"].astype(str).str.upper().str.strip()
    if "player_name" in out.columns:
        out["player_name"] = out["player_name"].astype(str).str.strip()
    if "player_id" in out.columns:
        out["player_id"] = pd.to_numeric(out["player_id"], errors="coerce")

    if "exp_min_mean" in out.columns:
        out["exp_min_mean"] = pd.to_numeric(out["exp_min_mean"], errors="coerce")
    if "starter_prob" in out.columns:
        out["starter_prob"] = pd.to_numeric(out["starter_prob"], errors="coerce")
    for c in ["exp_min_sd", "exp_min_cap"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    if "is_starter" in out.columns:
        try:
            out["is_starter"] = out["is_starter"].astype(bool)
        except Exception:
            out["is_starter"] = out["is_starter"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})

    keep = [
        c
        for c in [
            "date",
            "team_tri",
            "player_id",
            "player_name",
            "exp_min_mean",
            "exp_min_source",
            "starter_prob",
            "is_starter",
            "exp_min_sd",
            "exp_min_cap",
            "exp_asof_ts",
        ]
        if c in out.columns
    ]
    out = out[keep].copy() if keep else pd.DataFrame()
    out = out.dropna(subset=["team_tri"]).copy() if (not out.empty and "team_tri" in out.columns) else out
    return out


def _merge_pregame_expected_minutes_for_team(team_df: pd.DataFrame, *, date_str: str, team_tri: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    diag: dict[str, Any] = {
        "attempted": True,
        "applied": False,
        "date": str(date_str),
        "team": str(team_tri).upper().strip(),
        "path": str(paths.data_processed / f"pregame_expected_minutes_{str(date_str).strip()}.csv"),
    }
    if team_df is None or team_df.empty:
        diag["reason"] = "empty_team_df"
        return (pd.DataFrame() if team_df is None else team_df), diag

    pem = _load_pregame_expected_minutes(str(date_str))
    if pem is None or pem.empty:
        diag["reason"] = "missing_pregame_expected_minutes"
        return team_df, diag

    t = str(team_tri or "").upper().strip()
    if not t:
        diag["reason"] = "missing_team"
        return team_df, diag
    if "team_tri" not in pem.columns:
        diag["reason"] = "bad_schema"
        return team_df, diag

    pem_t = pem[pem["team_tri"].astype(str).str.upper().str.strip() == t].copy()
    if pem_t.empty:
        diag["reason"] = "team_not_found"
        return team_df, diag

    out = team_df.copy()
    if "player_name" in out.columns:
        out["player_name"] = out["player_name"].astype(str).str.strip()
    out["_pkey"] = out.get("player_name", "").map(_norm_player_key)

    pem_t["player_name"] = pem_t.get("player_name", "").astype(str).str.strip()
    pem_t["_pkey"] = pem_t.get("player_name", "").map(_norm_player_key)

    # Dedupe so mapping is stable.
    try:
        if "player_id" in pem_t.columns and pem_t["player_id"].notna().any():
            pem_t = pem_t.sort_values(["player_id", "exp_min_mean"], ascending=[True, False], kind="stable")
            pem_t = pem_t.drop_duplicates(subset=["player_id"], keep="first")
        pem_t = pem_t.sort_values(["_pkey", "exp_min_mean"], ascending=[True, False], kind="stable")
        pem_t = pem_t.drop_duplicates(subset=["_pkey"], keep="first")
    except Exception:
        pass

    # Build mappings (prefer player_id match, fall back to normalized name key).
    cols = [c for c in pem_t.columns if c not in {"date", "team_tri", "player_name"}]
    pid_maps: dict[str, dict[int, Any]] = {}
    key_maps: dict[str, dict[str, Any]] = {}
    try:
        pid_ser = pd.to_numeric(pem_t.get("player_id"), errors="coerce").astype("Int64") if "player_id" in pem_t.columns else pd.Series([], dtype="Int64")
        for c in cols:
            if c == "player_id":
                continue
            if len(pid_ser) and pid_ser.notna().any():
                m = {}
                v = pem_t[c]
                for pid, vv in zip(pid_ser.tolist(), v.tolist()):
                    if pid is None or (isinstance(pid, float) and (not np.isfinite(pid))):
                        continue
                    try:
                        m[int(pid)] = vv
                    except Exception:
                        continue
                pid_maps[c] = m
            km = dict(zip(pem_t.get("_pkey", pd.Series([], dtype=str)).astype(str).tolist(), pem_t[c].tolist()))
            key_maps[c] = km
    except Exception:
        pid_maps = {}
        key_maps = {}

    for c in cols:
        if c == "_pkey":
            continue
        if c in out.columns:
            base = out[c]
        else:
            base = pd.Series([np.nan] * len(out), index=out.index)

        v_pid = pd.Series([np.nan] * len(out), index=out.index)
        if c in pid_maps and pid_maps[c]:
            try:
                v_pid = pid_out.map(pid_maps[c])
            except Exception:
                v_pid = pd.Series([np.nan] * len(out), index=out.index)

        v_key = pd.Series([np.nan] * len(out), index=out.index)
        if c in key_maps and key_maps[c]:
            try:
                v_key = out["_pkey"].astype(str).map(key_maps[c])
            except Exception:
                v_key = pd.Series([np.nan] * len(out), index=out.index)

        # Prefer pid-derived values; fill remaining by key; preserve any existing (non-null) values.
        filled = base.where(base.notna(), other=v_pid)
        filled = filled.where(filled.notna(), other=v_key)
        out[c] = filled

    try:
        pid_out = pd.to_numeric(out.get("player_id"), errors="coerce").astype("Int64") if "player_id" in out.columns else pd.Series([pd.NA] * len(out), index=out.index, dtype="Int64")
        if "exp_min_mean" in out.columns:
            exp = pd.to_numeric(out["exp_min_mean"], errors="coerce")
            diag["matched_exp_min"] = int(exp.notna().sum())

            v_pid = pd.Series([np.nan] * len(out), index=out.index)
            if pid_maps.get("exp_min_mean"):
                v_pid = pid_out.map(pid_maps["exp_min_mean"])
            v_key = pd.Series([np.nan] * len(out), index=out.index)
            if key_maps.get("exp_min_mean"):
                v_key = out["_pkey"].astype(str).map(key_maps["exp_min_mean"])

            diag["matched_pid_n"] = int(v_pid.notna().sum())
            diag["matched_key_n"] = int(v_key.notna().sum())
    except Exception:
        pass

    diag.setdefault("matched_pid_n", 0)
    diag.setdefault("matched_key_n", 0)
    diag["applied"] = True
    out = out.drop(columns=["_pkey"], errors="ignore")
    return out, diag


@lru_cache(maxsize=96)
def _compute_player_priors_cached(asof_date_str: str, days_back: int) -> Any:
    """Cached wrapper for compute_player_priors.

    compute_player_priors can be expensive (loads/aggregates history). During SmartSim
    range runs it was being recomputed once per game; caching makes it once per as-of date.
    """
    cfg = PlayerPriorsConfig(days_back=int(days_back))
    return compute_player_priors(str(asof_date_str), cfg)


def _season_from_date_str(date_str: str) -> int:
    """Infer NBA season-year from a YYYY-MM-DD date.

    Example: 2026-02-13 -> 2026 (2025-26 season).
    """
    try:
        ts = pd.to_datetime(str(date_str), errors="coerce")
        if ts is None or pd.isna(ts):
            raise ValueError("bad date")
        y = int(ts.year)
        m = int(ts.month)
        return int(y + 1) if m >= 7 else int(y)
    except Exception:
        # Conservative fallback
        try:
            return int(str(date_str)[:4])
        except Exception:
            return 0


def _norm_pct01(v: Any) -> float:
    """Normalize percent-like values to 0..1 when inputs are 0..100."""
    try:
        x = float(v)
        if not np.isfinite(x):
            return float("nan")
        if x > 1.5:
            x = x / 100.0
        return float(x)
    except Exception:
        return float("nan")


@lru_cache(maxsize=96)
def _load_team_advanced_stats_asof(season: int, as_of_date_str: str) -> pd.DataFrame:
    """Load team advanced stats (prefer as-of cache; fall back to season-level).

        Schema (expected):
            - Core: team, pace, off_rtg, def_rtg, efg_pct, tov_pct, orb_pct, ft_rate, games, source
            - Optional (if present): fg3a_rate, fg3_pct, ts_pct, ast_per_100
    """

    s = int(season)
    ds = str(as_of_date_str).strip()
    fp_asof = paths.data_processed / f"team_advanced_stats_{s}_asof_{ds}.csv"
    fp_season = paths.data_processed / f"team_advanced_stats_{s}.csv"

    fp = fp_asof if fp_asof.exists() else fp_season
    if not fp.exists():
        return pd.DataFrame()

    try:
        df = pd.read_csv(fp)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()

        # Some cached CSVs can have stray whitespace/newlines in headers (e.g. "ft_rate\r\n").
        df.columns = [str(c).strip() for c in df.columns]

        team_col = "team" if "team" in df.columns else ("team_tri" if "team_tri" in df.columns else None)
        if team_col is None:
            return pd.DataFrame()
        df[team_col] = df[team_col].astype(str).str.upper().str.strip()
        if team_col != "team":
            df = df.rename(columns={team_col: "team"})

        for c in [
            "pace",
            "off_rtg",
            "def_rtg",
            "efg_pct",
            "tov_pct",
            "orb_pct",
            "ft_rate",
            "fg3a_rate",
            "fg3_pct",
            "ts_pct",
            "ast_per_100",
            "games",
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Normalize percent-like columns when sourced from scrapes.
        for c in ["efg_pct", "tov_pct", "orb_pct", "fg3a_rate", "fg3_pct", "ts_pct"]:
            if c in df.columns:
                df[c] = df[c].map(_norm_pct01)
        if "ft_rate" in df.columns:
            # FT rate should already be ~0.15..0.35; this keeps 0.xx as-is and converts 20..35 to 0.20..0.35.
            df["ft_rate"] = df["ft_rate"].map(_norm_pct01)

        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna(subset=["team"]).reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame()


def _team_adv_row(df: pd.DataFrame, team_tri: str) -> dict[str, float] | None:
    if df is None or df.empty:
        return None
    t = str(team_tri or "").upper().strip()
    if not t:
        return None
    if "team" not in df.columns:
        return None
    m = df[df["team"].astype(str).str.upper().str.strip() == t]
    if m.empty:
        return None
    r = m.iloc[0].to_dict()
    out: dict[str, float] = {}
    for k in ["pace", "off_rtg", "def_rtg", "efg_pct", "tov_pct", "orb_pct", "ft_rate", "games"]:
        try:
            v = float(r.get(k))
            out[k] = float(v) if np.isfinite(v) else float("nan")
        except Exception:
            out[k] = float("nan")
    return out


def _league_means(df: pd.DataFrame) -> dict[str, float]:
    out: dict[str, float] = {}
    if df is None or df.empty:
        return {
            "pace": 98.0,
            "off_rtg": 110.0,
            "def_rtg": 110.0,
            "tov_pct": 0.135,
            "orb_pct": 0.240,
            "ft_rate": 0.220,
        }

    def mean_col(c: str, default: float) -> float:
        try:
            if c not in df.columns:
                return float(default)
            arr = pd.to_numeric(df[c], errors="coerce").to_numpy(dtype=float)
            m = float(np.nanmean(arr))
            return float(m) if np.isfinite(m) else float(default)
        except Exception:
            return float(default)

    out["pace"] = mean_col("pace", 98.0)
    out["off_rtg"] = mean_col("off_rtg", 110.0)
    out["def_rtg"] = mean_col("def_rtg", 110.0)
    out["tov_pct"] = mean_col("tov_pct", 0.135)
    out["orb_pct"] = mean_col("orb_pct", 0.240)
    out["ft_rate"] = mean_col("ft_rate", 0.220)
    return out


def _team_adj_from_advanced_stats(
    date_str: str,
    home_tri: str,
    away_tri: str,
) -> tuple[Optional[dict[str, float]], Optional[dict[str, float]], float, dict[str, Any]]:
    """Return (home_adj, away_adj, pace_mult, diag) from cached team advanced stats.

    - Uses as-of cache when present (no-leakage).
    - Does NOT use market lines.
    """
    diag: dict[str, Any] = {"attempted": True, "applied": False, "source": None, "as_of": str(date_str)}
    try:
        season = _season_from_date_str(date_str)
        if season <= 0:
            diag["reason"] = "bad_season"
            return None, None, 1.0, diag
        df = _load_team_advanced_stats_asof(int(season), str(date_str))
        if df is None or df.empty:
            diag["reason"] = "missing_team_advanced_stats"
            return None, None, 1.0, diag

        if isinstance(df, pd.DataFrame) and "source" in df.columns and not df["source"].empty:
            try:
                diag["source"] = str(df["source"].iloc[0])
            except Exception:
                diag["source"] = "cache"
        else:
            diag["source"] = "cache"
        h = _team_adv_row(df, home_tri)
        a = _team_adv_row(df, away_tri)
        lg = _league_means(df)
        diag["league"] = lg
        diag["home"] = h
        diag["away"] = a

        if not h or not a:
            diag["reason"] = "team_not_found"
            return None, None, 1.0, diag

        # Pace multiplier: based on average matchup pace vs league.
        pace_vals = [float(h.get("pace", float("nan"))), float(a.get("pace", float("nan")))]
        pace_vals = [x for x in pace_vals if np.isfinite(x) and x > 0]
        league_pace = float(lg.get("pace", 98.0))
        if pace_vals and np.isfinite(league_pace) and league_pace > 0:
            pace_match = float(np.mean(pace_vals))
            pace_mult = float(np.clip(pace_match / league_pace, 0.92, 1.08))
        else:
            pace_mult = 1.0

        league_off = float(lg.get("off_rtg", 110.0))
        league_def = float(lg.get("def_rtg", 110.0))

        def _ratio(x: float, base: float, lo: float, hi: float) -> float:
            try:
                x = float(x)
                base = float(base)
                if (not np.isfinite(x)) or (not np.isfinite(base)) or base <= 0:
                    return 1.0
                return float(np.clip(x / base, lo, hi))
            except Exception:
                return 1.0

        # Efficiency is opponent-aware: offense strength * opponent defense weakness.
        eff_h = _ratio(float(h.get("off_rtg", float("nan"))), league_off, 0.85, 1.15) * _ratio(float(a.get("def_rtg", float("nan"))), league_def, 0.90, 1.10)
        eff_a = _ratio(float(a.get("off_rtg", float("nan"))), league_off, 0.85, 1.15) * _ratio(float(h.get("def_rtg", float("nan"))), league_def, 0.90, 1.10)
        eff_h = float(np.clip(eff_h, 0.80, 1.20))
        eff_a = float(np.clip(eff_a, 0.80, 1.20))

        # Other four-factor style adjustments (offense-side only; kept bounded).
        league_tov = float(lg.get("tov_pct", 0.135))
        league_orb = float(lg.get("orb_pct", 0.240))
        league_ft = float(lg.get("ft_rate", 0.220))

        tov_h = _ratio(float(h.get("tov_pct", float("nan"))), league_tov, 0.85, 1.15)
        tov_a = _ratio(float(a.get("tov_pct", float("nan"))), league_tov, 0.85, 1.15)
        foul_h = _ratio(float(h.get("ft_rate", float("nan"))), league_ft, 0.80, 1.25)
        foul_a = _ratio(float(a.get("ft_rate", float("nan"))), league_ft, 0.80, 1.25)
        oreb_h = _ratio(float(h.get("orb_pct", float("nan"))), league_orb, 0.75, 1.35)
        oreb_a = _ratio(float(a.get("orb_pct", float("nan"))), league_orb, 0.75, 1.35)

        home_adj = {"eff_mult": eff_h, "tov_mult": tov_h, "foul_mult": foul_h, "oreb_mult": oreb_h}
        away_adj = {"eff_mult": eff_a, "tov_mult": tov_a, "foul_mult": foul_a, "oreb_mult": oreb_a}

        diag["applied"] = True
        diag["pace_mult"] = pace_mult
        diag["home_adj"] = home_adj
        diag["away_adj"] = away_adj
        return home_adj, away_adj, pace_mult, diag
    except Exception as e:
        diag["reason"] = str(e)
        return None, None, 1.0, diag


def _finite_float_or_nan(x: Any) -> float:
    try:
        v = float(x)
        return float(v) if np.isfinite(v) else float("nan")
    except Exception:
        return float("nan")


@lru_cache(maxsize=1)
def _load_intervals_band_calibration() -> dict[str, Any] | None:
    """Load optional calibration used to widen interval p10/p90 bands.

    If the file doesn't exist or is invalid, returns None and SmartSim outputs remain unchanged.
    """

    p = paths.data_processed / "intervals_band_calibration.json"
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


@lru_cache(maxsize=1)
def _load_intervals_time_profile() -> dict[str, Any] | None:
    """Load optional interval time-profile calibration.

    If present, this adjusts the *shape* of regulation 3-minute segments before
    we compute interval quantiles (p10/p50/p90), while preserving each simulation's
    total points by renormalizing per-sim totals.

    Expected JSON at data/processed/intervals_time_profile.json:
      {"segment_multipliers": [m1..m16], "clip": [lo, hi], ...}

    If missing/invalid, returns None and SmartSim outputs remain unchanged.
    """

    p = paths.data_processed / "intervals_time_profile.json"
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _apply_intervals_time_profile(
    reg_total: np.ndarray,
    profile: dict[str, Any] | None,
) -> np.ndarray:
    """Apply time-profile multipliers to reg_total (n_sims x n_reg_segs).

    Multiplies each segment by its multiplier, then rescales each sim row to keep
    the row sum unchanged.
    """

    if reg_total is None or not isinstance(reg_total, np.ndarray) or reg_total.size == 0:
        return reg_total
    if not profile or not isinstance(profile, dict):
        return reg_total

    mults = profile.get("segment_multipliers")
    if not isinstance(mults, list) or len(mults) != int(reg_total.shape[1]):
        return reg_total

    m = np.asarray([_finite_float_or_nan(x) for x in mults], dtype=float)
    if m.size != int(reg_total.shape[1]) or not np.isfinite(m).all():
        return reg_total

    # Optional clip guardrail (extra safety beyond what the builder should already do).
    try:
        clip = profile.get("clip")
        if isinstance(clip, (list, tuple)) and len(clip) == 2:
            lo = float(clip[0])
            hi = float(clip[1])
            if np.isfinite(lo) and np.isfinite(hi) and lo > 0 and hi > 0 and lo <= hi:
                m = np.clip(m, lo, hi)
    except Exception:
        pass

    # Apply per-segment multipliers.
    adj = reg_total.astype(float) * m.reshape(1, -1)

    # Renormalize per sim so total points stays identical.
    orig_sum = np.sum(reg_total.astype(float), axis=1)
    adj_sum = np.sum(adj, axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        scale = np.where(adj_sum > 0, orig_sum / adj_sum, 1.0)
    scale = np.where(np.isfinite(scale), scale, 1.0)
    return adj * scale.reshape(-1, 1)


@lru_cache(maxsize=1)
def _load_player_stat_calibration() -> dict[str, Any] | None:
    """Load optional per-player stat bias calibration.

    Expects a JSON artifact at data/processed/player_stat_calibration.json with shape:
      {"players": {"<player_id>": {"pts": bias, "reb": bias, ...}}, "global": {...}}

    If missing/invalid, returns None and SmartSim outputs remain unchanged.
    """

    p = paths.data_processed / "player_stat_calibration.json"
    if not p.exists():
        return None
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _apply_band_scale(q: Any, scale: float) -> Any:
    """Symmetrically widen [p10,p90] about p50 by `scale`.

    Keeps p50 fixed; returns the original object if inputs are invalid.
    """

    if not isinstance(q, dict):
        return q
    s = _finite_float_or_nan(scale)
    if not np.isfinite(s) or s <= 0:
        return q

    p10 = _finite_float_or_nan(q.get("p10"))
    p50 = _finite_float_or_nan(q.get("p50"))
    p90 = _finite_float_or_nan(q.get("p90"))
    if not (np.isfinite(p10) and np.isfinite(p50) and np.isfinite(p90)):
        return q
    if not (p10 < p50 < p90):
        return q

    new_p10 = float(p50 - s * (p50 - p10))
    new_p90 = float(p50 + s * (p90 - p50))
    return {"p10": new_p10, "p50": float(p50), "p90": new_p90}


def _interval_scale(cal: dict[str, Any] | None, seg_idx: int, kind: str) -> float:
    """Return a scale for kind in {'seg','cum'} for a 1-based seg_idx."""

    if not cal:
        return 1.0
    try:
        per = cal.get("per_segment") or {}
        if isinstance(per, dict):
            v = (per.get(str(int(seg_idx))) or {}).get(kind)
            s = _finite_float_or_nan(v)
            if np.isfinite(s) and s > 0:
                return float(s)
    except Exception:
        pass

    try:
        g = cal.get("global") or {}
        if isinstance(g, dict):
            s = _finite_float_or_nan(g.get(kind))
            if np.isfinite(s) and s > 0:
                return float(s)
    except Exception:
        pass

    return 1.0


def _read_hist_any(pq_path, csv_path) -> pd.DataFrame:
    try:
        if pq_path is not None and getattr(pq_path, "exists", lambda: False)():
            try:
                df = pd.read_parquet(pq_path)
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            except Exception:
                pass
        if csv_path is not None and getattr(csv_path, "exists", lambda: False)():
            try:
                df = pd.read_csv(csv_path)
                return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            except Exception:
                pass
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()


@lru_cache(maxsize=1)
def _load_player_logs_processed() -> pd.DataFrame:
    p = paths.data_processed / "player_logs.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(p)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _parse_min_to_float(v: Any) -> float:
    try:
        if v is None:
            return float("nan")
        if isinstance(v, str) and ":" in v:
            mm, ss = v.split(":", 1)
            return float(mm) + float(ss) / 60.0
        x = float(v)
        return float(x) if np.isfinite(x) else float("nan")
    except Exception:
        return float("nan")


@lru_cache(maxsize=512)
def _minutes_priors_from_player_logs(*, date_str: str, team_tri: str, lookback_days: int = 21) -> dict[str, float]:
    """Return {PLAYER_KEY -> avg_minutes} from recent games for a team."""

    try:
        ds = str(date_str)
        if not ds:
            return {}
        team = str(team_tri or "").strip().upper()
        if not team:
            return {}

        logs = _load_player_logs_processed()
        if logs is None or logs.empty:
            return {}

        if "GAME_DATE" not in logs.columns or "TEAM_ABBREVIATION" not in logs.columns:
            return {}

        x = logs.copy()
        x["GAME_DATE"] = pd.to_datetime(x["GAME_DATE"], errors="coerce")
        end = pd.to_datetime(ds, errors="coerce")
        if pd.isna(end):
            return {}

        start = end - pd.Timedelta(days=int(max(1, lookback_days)))
        x = x[(x["GAME_DATE"] >= start) & (x["GAME_DATE"] < end)].copy()
        if x.empty:
            return {}

        x["TEAM_ABBREVIATION"] = x["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()
        x = x[x["TEAM_ABBREVIATION"] == team].copy()
        if x.empty:
            return {}

        name_col = "PLAYER_NAME" if "PLAYER_NAME" in x.columns else ("player_name" if "player_name" in x.columns else None)
        if not name_col:
            return {}

        min_col = "MIN" if "MIN" in x.columns else ("min" if "min" in x.columns else None)
        if not min_col:
            return {}

        x["_pkey"] = x[name_col].map(_norm_player_key)
        x["_min"] = x[min_col].map(_parse_min_to_float)
        x = x[np.isfinite(x["_min"])].copy()
        x = x[x["_pkey"].astype(str).str.len() > 0].copy()
        if x.empty:
            return {}

        pri = x.groupby("_pkey", as_index=True)["_min"].mean().to_dict()
        return {str(k): float(v) for k, v in pri.items() if k and v is not None and np.isfinite(float(v))}
    except Exception:
        return {}


@dataclass
class SmartSimConfig:
    n_sims: int = 2000
    seed: Optional[int] = None
    priors_days_back: int = 21

    # Roster sourcing mode:
    # - "historical" (default): may use completed-game artifacts (processed boxscores, ESPN boxscore)
    #   to build a full roster when the props pool is sparse.
    # - "pregame": do not use postgame boxscore-derived roster sources; only use props pool and
    #   processed season rosters as fallback.
    roster_mode: str = "historical"

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
    team_only = pd.DataFrame()
    if "team" in df.columns:
        team_only = df[df["team"] == team_u].copy()

    # Prefer the matchup-specific subset when it has reasonable coverage.
    # If opponent tagging is missing/spotty, a strict (team, opponent) filter can yield only a couple
    # of rows, which makes SmartSim allocate essentially the entire team boxscore to one player.
    if ("team" in df.columns) and ("opponent" in df.columns):
        out = df[(df["team"] == team_u) & (df["opponent"] == opp_u)].copy()

    # Coverage guardrail: if strict matchup filter returns too few players, fall back to team-only.
    try:
        if (not team_only.empty) and (out is not None) and (len(out) < 8):
            out = team_only
    except Exception:
        pass

    if (out is None or out.empty) and (not team_only.empty):
        out = team_only

    if out.empty:
        return out

    if "playing_today" in out.columns:
        try:
            pt = out["playing_today"].astype(str).str.lower().str.strip()
            out = out[~pt.isin(["false", "0", "no", "n"])].copy()
        except Exception:
            pass

    if "player_name" in out.columns:
        out["player_name"] = out["player_name"].astype(str).str.strip()
        out = out[out["player_name"].ne("")].copy()

    # Props feeds can contain multiple rows per player (one per market/stat).
    # SmartSim expects one row per player; duplicates cause minutes/usage to be split
    # across multiple rows and then effectively disappear from per-player exports.
    try:
        if "player_name" in out.columns:
            if "team" in out.columns:
                out = out.drop_duplicates(subset=["player_name", "team"], keep="last")
            else:
                out = out.drop_duplicates(subset=["player_name"], keep="last")
    except Exception:
        pass

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


def _rotation_sim_minutes_from_history(
    team_df: pd.DataFrame,
    date_str: str,
    home_tri: str,
    away_tri: str,
    team_tri: str,
    lookback_days: int = 28,
) -> tuple[Optional[pd.Series], Optional[List[List[int]]], Optional[np.ndarray], dict[str, Any]]:
    diag: dict[str, Any] = {
        "attempted": True,
        "applied": False,
        "source": "history",
        "team": str(team_tri).upper().strip(),
        "lookback_days": int(lookback_days),
    }
    if team_df is None or team_df.empty:
        diag["reason"] = "empty_players"
        return None, None, None, diag

    team_u = str(team_tri or "").strip().upper()
    if not team_u:
        diag["reason"] = "missing_team"
        return None, None, None, diag

    st_hist = _read_hist_any(
        paths.data_processed / "rotation_stints_history.parquet",
        paths.data_processed / "rotation_stints_history.csv",
    )
    if st_hist is None or st_hist.empty:
        diag["reason"] = "no_rotation_stints_history"
        return None, None, None, diag

    need = {"team", "duration_sec", "lineup_player_ids"}
    if not need.issubset(set(st_hist.columns)):
        diag["reason"] = "history_missing_columns"
        diag["missing_cols"] = sorted(list(need - set(st_hist.columns)))
        return None, None, None, diag

    st = st_hist.copy()
    st["team"] = st["team"].astype(str).str.upper().str.strip()
    st = st[st["team"] == team_u].copy()
    if st.empty:
        diag["reason"] = "team_not_in_history"
        return None, None, None, diag

    # Filter to recent window if date is available.
    if "date" in st.columns:
        try:
            cutoff = pd.to_datetime(str(date_str), errors="coerce")
            if pd.notna(cutoff):
                start = cutoff - pd.Timedelta(days=int(lookback_days))
                st["date"] = pd.to_datetime(st["date"], errors="coerce")
                st = st[(st["date"].notna()) & (st["date"] >= start) & (st["date"] < cutoff)].copy()
        except Exception:
            pass

    if st.empty:
        diag["reason"] = "no_recent_history"
        return None, None, None, diag

    # Map our player names to ESPN athlete IDs for the matchup (pregame roster mapping).
    name_to_id = _espn_name_to_id_map_for_game(
        str(date_str),
        home_tri=str(home_tri),
        away_tri=str(away_tri),
        event_id=None,
    )
    if not name_to_id:
        diag["reason"] = "no_espn_name_map"
        return None, None, None, diag

    tmp = team_df.copy().reset_index(drop=True)
    tmp["_pkey"] = tmp.get("player_name", pd.Series(["" for _ in range(len(tmp))])).map(_norm_player_key)
    tmp["_espn_id"] = tmp["_pkey"].map(lambda k: name_to_id.get((team_u, str(k).upper().strip()), ""))
    tmp["_espn_id"] = tmp["_espn_id"].astype(str).replace({"nan": "", "None": ""}).str.strip()

    # Compute per-player minutes from historical stints.
    st2 = st[["team", "duration_sec", "lineup_player_ids"]].copy()
    st2["duration_sec"] = pd.to_numeric(st2["duration_sec"], errors="coerce").fillna(0.0)
    st2["player_id"] = st2["lineup_player_ids"].astype(str).str.split(";")
    st2 = st2.explode("player_id")
    st2["player_id"] = st2["player_id"].map(_clean_id_str)
    st2 = st2[st2["player_id"].astype(str).str.len() > 0]
    if st2.empty:
        diag["reason"] = "no_player_ids_in_history"
        return None, None, None, diag

    mins_df = st2.groupby(["team", "player_id"], as_index=False)["duration_sec"].sum()
    mins_df["minutes"] = mins_df["duration_sec"].astype(float) / 60.0
    mins_df = mins_df[mins_df["team"].astype(str).str.upper().str.strip() == team_u].copy()
    if mins_df.empty:
        diag["reason"] = "no_minutes_from_history"
        return None, None, None, diag

    # Target 240 minutes, using history minutes distribution.
    mins_df["minutes"] = pd.to_numeric(mins_df["minutes"], errors="coerce").fillna(0.0).astype(float)
    total_hist = float(mins_df["minutes"].sum())
    if not np.isfinite(total_hist) or total_hist <= 0:
        diag["reason"] = "bad_history_total_minutes"
        return None, None, None, diag
    mins_df["minutes_scaled"] = mins_df["minutes"] * (240.0 / total_hist)
    id_to_min = dict(zip(mins_df["player_id"].astype(str), mins_df["minutes_scaled"].astype(float)))

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
            alloc = pd.Series([m / float(len(idx))] * len(idx), index=idx, dtype=float)
        else:
            alloc = (w / ws) * m
        sim_min.loc[idx] = alloc.astype(float)
        mapped_players += int(len(idx))
        mapped_minutes_sum += float(alloc.sum())

    leftover = float(240.0 - mapped_minutes_sum)
    if leftover > 1e-6:
        # IMPORTANT: history stints include minutes for ESPN IDs that may not exist on the
        # current roster (trades, new players, missing mappings). Those "missing" minutes
        # should be reallocated across the *entire* current roster, not just the handful of
        # unmapped players (which can create absurd 45-55 minute projections).
        w = base_w.astype(float).clip(lower=0.0)
        ws = float(w.sum())
        if (not np.isfinite(ws)) or ws <= 0:
            sim_min = sim_min + (float(leftover) / float(max(1, len(sim_min))))
        else:
            sim_min = sim_min + ((w / ws) * float(leftover))

    # Normalize to 240.
    total_sim = float(sim_min.sum())
    if np.isfinite(total_sim) and total_sim > 0:
        sim_min = sim_min * (240.0 / total_sim)

    # Enforce a regulation-style cap. History-based allocation can otherwise assign
    # unrealistic minutes to a single player (e.g., 45-55) due to noisy mapping/weights.
    sim_min = _cap_and_redistribute_minutes(sim_min, total_target=240.0, cap=44.0, iters=12)

    # Lineup pool from historical stints; keep only full 5-man units mapped to our roster.
    lineup_pool: List[List[int]] = []
    lineup_w: List[float] = []
    try:
        if {"lineup_player_ids", "duration_sec"}.issubset(set(st.columns)):
            s2 = st.copy()
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
                    w = float(r.get("duration_sec") or 0.0)
                    if w > 0:
                        lineup_pool.append([int(x) for x in idxs_u])
                        lineup_w.append(w)
    except Exception:
        lineup_pool = []
        lineup_w = []

    diag["history_rows"] = int(len(st))
    diag["mapped_players"] = int(mapped_players)
    diag["mapped_minutes"] = float(mapped_minutes_sum)
    diag["leftover_minutes"] = float(max(0.0, leftover))
    diag["lineup_pool_n"] = int(len(lineup_pool))

    # Guardrail: if ESPN ID mapping is too sparse, rotation-based minutes become pathological
    # (e.g., assigning ~all minutes/scoring to a single mapped player). In that case, do not apply
    # rotation minutes; caller should fall back to roll-based minutes.
    try:
        total_target = 240.0
        mapped_ids = [pid for pid in sorted(set(espn_ids[have].tolist())) if float(id_to_min.get(str(pid), 0.0)) > 0.0]
        mapped_id_n = int(len(mapped_ids))
        frac = float(mapped_minutes_sum) / float(max(1e-6, total_target))
        diag["mapped_id_n"] = mapped_id_n
        diag["mapped_minutes_frac"] = frac
        if mapped_id_n < 5 or frac < 0.50 or int(len(lineup_pool)) < 5:
            diag["applied"] = False
            diag["reason"] = "rotation_mapping_too_sparse"
            return None, None, None, diag
    except Exception:
        # If diagnostics fail, be conservative and do not apply.
        diag["applied"] = False
        diag["reason"] = str(diag.get("reason") or "rotation_mapping_guard_failed")
        return None, None, None, diag

    diag["applied"] = True
    diag["sim_minutes_sum"] = float(sim_min.sum())
    return sim_min.astype(float), (lineup_pool if lineup_pool else None), (np.asarray(lineup_w, dtype=float) if lineup_w else None), diag


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

    def _from_pbp_history(lookback_days: int = 120) -> dict[tuple[str, str], str]:
        """Local fallback mapping from pbp_espn_history.csv substitution rows.

        This avoids relying on ESPN boxscore being populated pre-game.
        """
        try:
            fp = paths.data_processed / "pbp_espn_history.csv"
            if not fp.exists():
                return {}
            usecols = [
                "date",
                "team",
                "enter_player_id",
                "exit_player_id",
                "enter_player_name",
                "exit_player_name",
            ]
            hist = pd.read_csv(fp, usecols=usecols)
            if hist is None or hist.empty:
                return {}

            teams = {str(home_tri or "").upper().strip(), str(away_tri or "").upper().strip()}
            teams = {t for t in teams if t}
            if teams:
                hist["team"] = hist["team"].astype(str).str.upper().str.strip()
                hist = hist[hist["team"].isin(list(teams))].copy()
            if hist.empty:
                return {}

            try:
                cutoff = pd.to_datetime(str(date_str), errors="coerce")
                if pd.notna(cutoff):
                    start = cutoff - pd.Timedelta(days=int(lookback_days))
                    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
                    hist = hist[(hist["date"].notna()) & (hist["date"] >= start) & (hist["date"] <= cutoff)].copy()
            except Exception:
                pass
            if hist.empty:
                return {}

            def _rows(id_col: str, name_col: str) -> pd.DataFrame:
                try:
                    df = hist[["team", "date", id_col, name_col]].copy()
                    df["id"] = df[id_col].map(_clean_id_str)
                    df["key"] = df[name_col].astype(str).map(_norm_player_key)
                    df = df[(df["id"].astype(str).str.len() > 0) & (df["key"].astype(str).str.len() > 0)].copy()
                    df["team"] = df["team"].astype(str).str.upper().str.strip()
                    df["key"] = df["key"].astype(str).str.upper().str.strip()
                    return df[["team", "key", "id", "date"]]
                except Exception:
                    return pd.DataFrame(columns=["team", "key", "id", "date"])

            a = _rows("enter_player_id", "enter_player_name")
            b = _rows("exit_player_id", "exit_player_name")
            combo = pd.concat([a, b], ignore_index=True)
            if combo.empty:
                return {}

            try:
                combo = combo.sort_values(["date"])  # keep last seen
            except Exception:
                pass
            combo = combo.drop_duplicates(subset=["team", "key"], keep="last")
            out: dict[tuple[str, str], str] = {}
            for _, r in combo.iterrows():
                t = str(r.get("team") or "").upper().strip()
                k = str(r.get("key") or "").upper().strip()
                pid = _clean_id_str(r.get("id"))
                if t and k and pid:
                    out[(t, k)] = pid
            return out
        except Exception:
            return {}

    try:
        from ..boxscores import _espn_event_id_for_matchup, _espn_summary, _espn_to_tri  # type: ignore
    except Exception:
        return _from_pbp_history()

    try:
        eid = str(event_id or "").strip() or (
            _espn_event_id_for_matchup(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri)) or ""
        )
        if not eid:
            return _from_pbp_history()
        summ = _espn_summary(eid)
        box = (summ or {}).get("boxscore") or {}
        teams = box.get("players") or []
        if not isinstance(teams, list) or not teams:
            return _from_pbp_history()

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
        return out or _from_pbp_history()
    except Exception:
        return _from_pbp_history()


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
            for g in stats_groups:
                athletes = (g or {}).get("athletes") or []
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


def _team_players_from_processed_boxscores(
    date_str: str,
    home_tri: str,
    away_tri: str,
    team_tri: str,
    game_id: Optional[str] = None,
) -> pd.DataFrame:
    """Fallback roster builder using processed NBA boxscores.

    This is the most reliable source for historical/completed games and avoids ESPN lookup failures.
    Returns a minimal DataFrame with [player_name, team, opponent, playing_today].
    """
    try:
        fp = paths.data_processed / f"boxscores_{str(date_str).strip()}.csv"
        if not fp.exists():
            return pd.DataFrame()
        df = pd.read_csv(fp)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        # Normalize
        df["TEAM_ABBREVIATION"] = df.get("TEAM_ABBREVIATION", "").astype(str).str.upper().str.strip()
        df["PLAYER_NAME"] = df.get("PLAYER_NAME", "").astype(str).str.strip()
        df["game_id"] = pd.to_numeric(df.get("game_id"), errors="coerce")

        home_u = str(home_tri or "").strip().upper()
        away_u = str(away_tri or "").strip().upper()
        team_u = str(team_tri or "").strip().upper()
        opp_u = str(away_u if team_u == home_u else home_u)

        gid = None
        try:
            if game_id is not None and str(game_id).strip() and str(game_id).lower() != "nan":
                gid = int(float(game_id))
        except Exception:
            gid = None

        if gid is None:
            # Infer gid by finding a game_id that contains both teams.
            g = df[df["TEAM_ABBREVIATION"].isin([home_u, away_u])].dropna(subset=["game_id"]).copy()
            if not g.empty:
                by = g.groupby("game_id")["TEAM_ABBREVIATION"].nunique()
                cand = by[by >= 2].index.tolist()
                if cand:
                    gid = int(float(cand[0]))

        if gid is None:
            return pd.DataFrame()

        tdf = df[(df["game_id"] == gid) & (df["TEAM_ABBREVIATION"] == team_u)].copy()
        if tdf.empty:
            return pd.DataFrame()

        # Only keep players who actually appeared (MIN > 0 when present).
        if "MIN" in tdf.columns:
            try:
                tdf["MIN"] = pd.to_numeric(tdf["MIN"], errors="coerce").fillna(0.0)
                tdf = tdf[tdf["MIN"] > 0].copy()
            except Exception:
                pass
        if tdf.empty:
            return pd.DataFrame()

        out = pd.DataFrame({
            "player_name": tdf["PLAYER_NAME"],
            "team": team_u,
            "opponent": opp_u,
            "playing_today": True,
        })
        out = out.dropna(subset=["player_name"]).copy()
        out["player_name"] = out["player_name"].astype(str).str.strip()
        out = out[out["player_name"].ne("")].drop_duplicates(subset=["player_name", "team"], keep="last")
        return out
    except Exception:
        return pd.DataFrame()


def _team_players_from_processed_rosters(
    date_str: str,
    home_tri: str,
    away_tri: str,
    team_tri: str,
) -> pd.DataFrame:
    """Fallback roster builder using processed season rosters.

    This is a reliable *pregame* fallback when props_predictions is missing a team and
    ESPN boxscore isn't populated yet.
    Returns a minimal DataFrame with at least [player_id, player_name, team, opponent, playing_today].
    """
    try:
        team_u = str(team_tri or "").strip().upper()
        home_u = str(home_tri or "").strip().upper()
        away_u = str(away_tri or "").strip().upper()
        if not team_u:
            return pd.DataFrame()
        opp_u = str(away_u if team_u == home_u else home_u)

        proc = paths.data_processed
        # Prefer rosters_<season>.csv matching the slate date.
        roster_path = None
        try:
            d = pd.to_datetime(str(date_str), errors="coerce")
            if pd.notna(d):
                start_year = int(d.year) if int(d.month) >= 7 else int(d.year) - 1
                season = f"{start_year}-{str(start_year + 1)[-2:]}"
                cand = proc / f"rosters_{season}.csv"
                if cand.exists():
                    roster_path = cand
        except Exception:
            roster_path = None

        if roster_path is None:
            try:
                cands = list(proc.glob("rosters_*.csv"))
                if cands:
                    cands.sort(key=lambda p: p.stat().st_mtime if p.exists() else 0, reverse=True)
                    roster_path = cands[0]
            except Exception:
                roster_path = None

        if roster_path is None or (not roster_path.exists()):
            return pd.DataFrame()

        df = pd.read_csv(roster_path)
        if df is None or df.empty:
            return pd.DataFrame()

        cols = {c.upper(): c for c in df.columns}
        pid_c = cols.get("PLAYER_ID")
        name_c = cols.get("PLAYER") or cols.get("PLAYER_NAME")
        tri_c = cols.get("TEAM_ABBREVIATION")
        if not (name_c and tri_c):
            return pd.DataFrame()

        tmp = df[[c for c in [pid_c, name_c, tri_c] if c]].copy()
        tmp[tri_c] = tmp[tri_c].astype(str).str.upper().str.strip()
        tmp = tmp[tmp[tri_c] == team_u].copy()
        if tmp.empty:
            return pd.DataFrame()

        out = pd.DataFrame(
            {
                "player_id": (pd.to_numeric(tmp[pid_c], errors="coerce") if pid_c else np.nan),
                "player_name": tmp[name_c].astype(str).str.strip(),
                "team": team_u,
                "opponent": opp_u,
                "playing_today": True,
            }
        )
        out = out.dropna(subset=["player_name"]).copy()
        out = out[out["player_name"].astype(str).str.strip().ne("")].copy()
        out = out.drop_duplicates(subset=["player_name", "team"], keep="last")
        return out
    except Exception:
        return pd.DataFrame()


def _roll_minutes_unscaled(team_df: pd.DataFrame) -> pd.Series:
    if team_df is None or team_df.empty:
        return pd.Series(dtype=float)
    min_cols = [c for c in ("exp_min_mean", "roll10_min", "roll5_min", "roll3_min", "lag1_min") if c in team_df.columns]
    mins = pd.Series([np.nan] * len(team_df), index=team_df.index, dtype=float)

    for c in min_cols:
        alt = pd.to_numeric(team_df[c], errors="coerce").astype(float)
        alt = alt.where(alt > 0.0, np.nan)
        mins = mins.where(mins.notna(), other=alt)

    # Avoid collapse to a few rows when roster augmentation introduced missing features.
    pos_n = int((pd.to_numeric(mins, errors="coerce").fillna(0.0) > 0.0).sum())
    if len(team_df) >= 8 and pos_n < 8:
        try:
            med = float(pd.to_numeric(mins[mins.notna()], errors="coerce").median())
            fill_val = med if np.isfinite(med) and med > 0.0 else 18.0
        except Exception:
            fill_val = 18.0
        mins = mins.where(mins.notna(), other=float(fill_val))
    mins = mins.fillna(24.0)
    return pd.to_numeric(mins, errors="coerce").fillna(0.0).astype(float).clip(lower=0.0, upper=44.0)


def _cap_and_redistribute_minutes(
    mins: pd.Series,
    total_target: float = 240.0,
    cap: float = 44.0,
    iters: int = 12,
) -> pd.Series:
    """Enforce a per-player minutes cap while preserving team total minutes.

    This is used to keep pregame rotation/history-based minutes realistic (regulation).
    """
    if mins is None or len(mins) == 0:
        return pd.Series(dtype=float)
    try:
        out = pd.to_numeric(mins, errors="coerce").fillna(0.0).astype(float).copy()
    except Exception:
        out = pd.Series([0.0] * int(len(mins)), index=getattr(mins, "index", None), dtype=float)

    out = out.clip(lower=0.0, upper=float(cap))
    base_w = out.copy()

    for _ in range(int(iters)):
        total = float(out.sum())
        if not np.isfinite(total) or total <= 0:
            break

        gap = float(total_target) - total
        if abs(gap) < 1e-6:
            break

        if gap < 0:
            # Too many minutes: scale down then re-clip.
            out = (out * (float(total_target) / total)).clip(lower=0.0, upper=float(cap))
            continue

        free = out < (float(cap) - 1e-9)
        if int(free.sum()) <= 0:
            break

        w = base_w.loc[free].astype(float).clip(lower=0.0)
        ws = float(w.sum())
        if (not np.isfinite(ws)) or ws <= 0:
            out.loc[free] = out.loc[free] + (gap / float(int(free.sum())))
        else:
            out.loc[free] = out.loc[free] + ((w / ws) * gap)

        out = out.clip(lower=0.0, upper=float(cap))

    return out.astype(float)


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
        # Pregame (or lookup failure): fall back to recent history minutes.
        sim_min, lineups, lw, diag2 = _rotation_sim_minutes_from_history(
            team_df=team_df,
            date_str=date_str,
            home_tri=home_tri,
            away_tri=away_tri,
            team_tri=team_tri,
        )
        try:
            diag2["fallback_reason"] = "missing_game_id"
        except Exception:
            pass
        return sim_min, lineups, lw, diag2

    stints = _read_rotation_stints(gid, side=side)
    if stints is None or stints.empty:
        # Pregame: we won't have stints for this future game. Fall back to recent history.
        sim_min, lineups, lw, diag2 = _rotation_sim_minutes_from_history(
            team_df=team_df,
            date_str=date_str,
            home_tri=home_tri,
            away_tri=away_tri,
            team_tri=team_tri,
            lookback_days=28,
        )
        diag.update({k: v for k, v in (diag2 or {}).items() if k not in {"attempted", "team"}})
        if diag.get("applied"):
            return sim_min, lineups, lw, diag
        diag["reason"] = str(diag.get("reason") or "missing_stints_file")
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
    total_raw = float(mins_df["minutes"].sum())
    diag["rotation_total_minutes_raw"] = total_raw
    # Guardrail: rotation stints files can occasionally be incomplete/corrupt; treat them
    # as a distribution only if totals look plausible.
    if (not np.isfinite(total_raw)) or total_raw <= 0 or total_raw < 200.0 or total_raw > 340.0:
        sim_min, lineups, lw, diag2 = _rotation_sim_minutes_from_history(
            team_df=team_df,
            date_str=date_str,
            home_tri=home_tri,
            away_tri=away_tri,
            team_tri=team_tri,
            lookback_days=28,
        )
        diag.update({k: v for k, v in (diag2 or {}).items() if k not in {"attempted", "team"}})
        diag["applied"] = bool(diag.get("applied", False))
        diag["fallback_reason"] = "bad_rotation_total_minutes"
        diag["reason"] = str(diag.get("reason") or "bad_rotation_total_minutes")
        return sim_min, lineups, lw, diag

    # Treat stints minutes as a *distribution*; normalize to regulation team minutes.
    mins_df["minutes_scaled"] = mins_df["minutes"] * (240.0 / float(total_raw))
    id_to_min = dict(zip(mins_df["player_id"].astype(str), mins_df["minutes_scaled"].astype(float)))
    total_target = 240.0
    diag["rotation_total_minutes"] = float(total_target)

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
        # If ESPN mapping is incomplete, some stints minutes won't map to a current row.
        # Reallocate those minutes across the full roster (weighted by roll minutes)
        # rather than dumping them onto the small set of unmapped players.
        w = base_w.astype(float).clip(lower=0.0)
        ws = float(w.sum())
        if (not np.isfinite(ws)) or ws <= 0:
            sim_min = sim_min + (float(leftover) / float(max(1, len(sim_min))))
        else:
            sim_min = sim_min + ((w / ws) * float(leftover))

    # If we somehow over-allocated (rare), scale down gently.
    total_sim = float(sim_min.sum())
    if np.isfinite(total_target) and total_target > 0 and np.isfinite(total_sim) and total_sim > 0:
        sim_min = sim_min * (total_target / total_sim)

    # Enforce a regulation-style cap and preserve team minutes.
    sim_min = _cap_and_redistribute_minutes(sim_min, total_target=240.0, cap=44.0, iters=12)

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

    # Guardrail: if mapping is too sparse, do not apply rotation minutes.
    try:
        mapped_ids = [pid for pid in sorted(set(espn_ids[have].tolist())) if float(id_to_min.get(str(pid), 0.0)) > 0.0]
        mapped_id_n = int(len(mapped_ids))
        frac = float(mapped_minutes_sum) / float(max(1e-6, total_target))
        diag["mapped_id_n"] = mapped_id_n
        diag["mapped_minutes_frac"] = frac
        if mapped_id_n < 5 or frac < 0.50 or int(len(lineup_pool)) < 5:
            diag["applied"] = False
            diag["reason"] = "rotation_mapping_too_sparse"
            return None, None, None, diag
    except Exception:
        diag["applied"] = False
        diag["reason"] = str(diag.get("reason") or "rotation_mapping_guard_failed")
        return None, None, None, diag

    diag["applied"] = True
    diag["sim_minutes_sum"] = float(sim_min.sum())
    return sim_min.astype(float), (lineup_pool if lineup_pool else None), (np.asarray(lineup_w, dtype=float) if lineup_w else None), diag


def _derive_sim_minutes(team_df: pd.DataFrame, *, date_str: str | None = None, team_tri: str | None = None) -> pd.Series:
    if team_df is None or team_df.empty:
        return pd.Series(dtype=float)

    # Prefer pregame expected minutes when available; otherwise fall back to roll10/roll5 minutes.
    # NOTE: roster augmentation can introduce rows with missing minute features; if we
    # leave those at 0, SmartSim will allocate nearly all team stats to the remaining
    # few players with non-zero minutes.
    min_cols = [c for c in ("exp_min_mean", "roll10_min", "roll5_min", "roll3_min", "lag1_min") if c in team_df.columns]
    if not min_cols:
        mins = pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
    else:
        mins = pd.to_numeric(team_df[min_cols[0]], errors="coerce").fillna(0.0).astype(float)
        # If the preferred column is missing/zero for many rows, try fallbacks.
        for c in min_cols[1:]:
            if int((mins > 0.0).sum()) >= 8:
                break
            alt = pd.to_numeric(team_df[c], errors="coerce").fillna(0.0).astype(float)
            mins = mins.where(mins > 0.0, alt)

        # If minutes are sparse, try to fill from recent actual minutes (player_logs.csv).
        pos_n = int((mins > 0.0).sum())
        if len(team_df) >= 8 and pos_n < 8 and date_str and team_tri:
            pri = _minutes_priors_from_player_logs(date_str=str(date_str), team_tri=str(team_tri), lookback_days=21)
            if pri:
                try:
                    pkeys = team_df.get("player_name", "").map(_norm_player_key)
                    pri_m = pkeys.map(pri)
                    pri_m = pd.to_numeric(pri_m, errors="coerce").fillna(0.0).astype(float)
                    # Only use priors if we matched a reasonable number of players.
                    if int((pri_m > 0.0).sum()) >= 5:
                        mins = mins.where(mins > 0.0, other=pri_m)
                except Exception:
                    pass

        # If we still have too few players with minutes, assign a conservative
        # default to the remaining roster so the sim doesn't collapse to 1-3 players.
        pos_n = int((mins > 0.0).sum())
        if len(team_df) >= 8 and pos_n < 8:
            fill_val: float
            try:
                med = float(pd.to_numeric(mins[mins > 0.0], errors="coerce").median())
                fill_val = med if np.isfinite(med) and med > 0.0 else 18.0
            except Exception:
                fill_val = 18.0
            mins = mins.where(mins > 0.0, other=float(fill_val))

    mins = mins.clip(lower=0.0, upper=44.0)

    s = float(mins.sum())
    if not np.isfinite(s) or s <= 0:
        mins = pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
        s = float(mins.sum())

    # Start with a scaled minutes vector.
    mins = mins * (240.0 / max(1e-6, s))
    mins = mins.clip(lower=0.0, upper=44.0)

    # Iteratively redistribute any leftover minutes to players below the cap.
    # This prevents the final normalization step from breaking the 44-minute cap.
    base_w = mins.copy()
    for _ in range(12):
        total = float(mins.sum())
        if not np.isfinite(total) or total <= 0:
            mins = pd.Series([24.0] * len(team_df), index=team_df.index, dtype=float)
            mins = mins * (240.0 / float(mins.sum()))
            mins = mins.clip(lower=0.0, upper=44.0)
            continue
        gap = 240.0 - total
        if abs(gap) < 1e-6:
            break
        if gap < 0:
            # Too many minutes: scale down then re-clip.
            mins = (mins * (240.0 / total)).clip(lower=0.0, upper=44.0)
            continue

        free = mins < (44.0 - 1e-9)
        if int(free.sum()) <= 0:
            break
        w = base_w.loc[free].astype(float).clip(lower=0.0)
        ws = float(w.sum())
        if (not np.isfinite(ws)) or ws <= 0:
            mins.loc[free] = mins.loc[free] + (gap / float(int(free.sum())))
        else:
            mins.loc[free] = mins.loc[free] + (w / ws) * gap
        mins = mins.clip(lower=0.0, upper=44.0)

    # Final normalization (safe): if we're still off due to caps, only scale down (never up past cap).
    total = float(mins.sum())
    if np.isfinite(total) and total > 240.0 + 1e-6:
        mins = (mins * (240.0 / total)).clip(lower=0.0, upper=44.0)

    return mins


def _apply_player_priors(team_df: pd.DataFrame, priors, team_tri: str, sim_minutes: Optional[pd.Series] = None, *, date_str: str | None = None) -> pd.DataFrame:
    if team_df is None or team_df.empty:
        return pd.DataFrame()

    out = team_df.copy()
    out["_pkey"] = out.get("player_name", "").map(_norm_player_key)

    # Minutes (rotation-based when available)
    if sim_minutes is not None and len(sim_minutes) == len(out):
        out["_sim_min"] = pd.to_numeric(sim_minutes, errors="coerce").fillna(0.0).astype(float)
    else:
        out["_sim_min"] = _derive_sim_minutes(out, date_str=date_str, team_tri=team_tri)

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
    # Important realism fix:
    # New/traded players often have *no priors* under their new team, so only pts/reb/ast
    # get backfilled from predictions. If FGA/3PA/FTA remain 0 for those players, the
    # possession engine gives them near-zero shots and forces team scoring onto the few
    # players with nonzero attempt priors (inflated statlines / implausible distributions).
    active = sim_min > 0.5

    pts_pm = pd.to_numeric(out.get("_prior_pts_pm"), errors="coerce").fillna(0.0).astype(float)
    threes_pm = pd.to_numeric(out.get("_prior_threes_pm"), errors="coerce").fillna(0.0).astype(float)

    # Player-level attempt priors: infer from predicted points + threes with conservative assumptions.
    # These are *fallbacks* only for rows that have 0/NaN priors.
    try:
        # Assume a modest FT share and typical shooting efficiencies.
        p3 = 3.0 * threes_pm
        pft = 0.18 * pts_pm
        p2 = np.maximum(0.0, pts_pm - p3 - pft)
        fgm2_pm = p2 / 2.0
        fga2_pm = fgm2_pm / 0.50  # ~50% on 2PA
        fg3a_fallback = (threes_pm / 0.35).clip(lower=0.0, upper=0.65)  # ~35% 3P%
        fga_fallback = (fga2_pm + fg3a_fallback).clip(lower=0.05, upper=0.85)
        fta_fallback = (pft / 0.76).clip(lower=0.0, upper=0.35)  # FT% ~76%
    except Exception:
        fga_fallback = (pts_pm / 1.05).clip(lower=0.05, upper=0.85)
        fg3a_fallback = (threes_pm / 0.35).clip(lower=0.0, upper=0.65)
        fta_fallback = (0.18 * fga_fallback).clip(lower=0.0, upper=0.35)

    fga = pd.to_numeric(out.get("_prior_fga_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_fga_pm"] = np.where(active & (fga <= 0.0), fga_fallback, fga)

    fg3a = pd.to_numeric(out.get("_prior_threes_att_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_threes_att_pm"] = np.where(active & (fg3a <= 0.0), fg3a_fallback, fg3a)

    # Ensure 3PA is not an impossible share of total FGA.
    try:
        out["_prior_threes_att_pm"] = np.minimum(
            pd.to_numeric(out["_prior_threes_att_pm"], errors="coerce").fillna(0.0).astype(float),
            0.9 * pd.to_numeric(out["_prior_fga_pm"], errors="coerce").fillna(0.0).astype(float),
        )
    except Exception:
        pass

    fgm = pd.to_numeric(out.get("_prior_fgm_pm"), errors="coerce").fillna(0.0).astype(float)
    fga_now = pd.to_numeric(out.get("_prior_fga_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_fgm_pm"] = np.where(active & (fgm <= 0.0), 0.46 * fga_now, fgm)

    fg3m = pd.to_numeric(out.get("_prior_threes_pm"), errors="coerce").fillna(0.0).astype(float)
    fg3a_now = pd.to_numeric(out.get("_prior_threes_att_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_threes_pm"] = np.where(active & (fg3m <= 0.0), 0.35 * fg3a_now, fg3m)

    fta = pd.to_numeric(out.get("_prior_fta_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_fta_pm"] = np.where(active & (fta <= 0.0), fta_fallback, fta)

    ftm = pd.to_numeric(out.get("_prior_ftm_pm"), errors="coerce").fillna(0.0).astype(float)
    fta_now = pd.to_numeric(out.get("_prior_fta_pm"), errors="coerce").fillna(0.0).astype(float)
    out["_prior_ftm_pm"] = np.where(active & (ftm <= 0.0), 0.76 * fta_now, ftm)

    # Team-level safety net: if *everyone* is missing attempts, apply a conservative baseline.
    fga_final = pd.to_numeric(out.get("_prior_fga_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((fga_final * sim_min).sum()) <= 0:
        out["_prior_fga_pm"] = np.where(sim_min > 0, 0.55, 0.0)
        fga_final = pd.to_numeric(out["_prior_fga_pm"], errors="coerce").fillna(0.0).astype(float)

    fg3a_final = pd.to_numeric(out.get("_prior_threes_att_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((fg3a_final * sim_min).sum()) <= 0:
        out["_prior_threes_att_pm"] = 0.36 * fga_final

    fgm_final = pd.to_numeric(out.get("_prior_fgm_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((fgm_final * sim_min).sum()) <= 0:
        out["_prior_fgm_pm"] = 0.46 * fga_final

    fg3m_final = pd.to_numeric(out.get("_prior_threes_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((fg3m_final * sim_min).sum()) <= 0:
        out["_prior_threes_pm"] = 0.35 * pd.to_numeric(out["_prior_threes_att_pm"], errors="coerce").fillna(0.0).astype(float)

    fta_final = pd.to_numeric(out.get("_prior_fta_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((fta_final * sim_min).sum()) <= 0:
        out["_prior_fta_pm"] = 0.18 * fga_final

    ftm_final = pd.to_numeric(out.get("_prior_ftm_pm"), errors="coerce").fillna(0.0).astype(float)
    if float((ftm_final * sim_min).sum()) <= 0:
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


def _load_smartsim_total_calibration() -> dict[str, Any]:
    """Optional post-hoc calibration for total points.

    If present, expects data/processed/smart_sim_total_calibration.json:
      {"points_mult": 0.99, ...}
    """
    fp = paths.data_processed / "smart_sim_total_calibration.json"
    if not fp.exists():
        return {}
    try:
        import json

        with open(fp, "r", encoding="utf-8") as f:
            j = json.load(f)
        return j if isinstance(j, dict) else {}
    except Exception:
        return {}


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
    excluded_player_keys_by_team: Optional[dict[str, set[str]]] = None,
    pregame_context: Optional[dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg = cfg or SmartSimConfig()
    rng = np.random.default_rng(cfg.seed)

    market_total_source: Optional[str] = None
    market_home_spread_source: Optional[str] = None

    # Derive a per-game event config using pregame data features (pace, injuries, schedule).
    # This keeps the possession engine aligned to expected possession volume.
    event_cfg = cfg.event_cfg
    try:
        hp = None
        ap = None
        hb2b = False
        ab2b = False
        h_outs = 0
        a_outs = 0
        if isinstance(pregame_context, dict):
            hp = _safe_float(pregame_context.get("home_pace"), default=float("nan"))
            ap = _safe_float(pregame_context.get("away_pace"), default=float("nan"))
            hb2b = bool(pregame_context.get("home_b2b", False))
            ab2b = bool(pregame_context.get("away_b2b", False))
            h_outs = int(max(0, _safe_float(pregame_context.get("home_injuries_out"), default=0.0)))
            a_outs = int(max(0, _safe_float(pregame_context.get("away_injuries_out"), default=0.0)))

        pace_vals: list[float] = []
        for x in (hp, ap):
            try:
                xx = float(x)
                if np.isfinite(xx):
                    pace_vals.append(xx)
            except Exception:
                continue
        pace = float(np.mean(pace_vals)) if pace_vals else float("nan")
        if not np.isfinite(pace):
            pace = float(event_cfg.possessions_per_game)

        # Mild pace drag: consistent with quarters model.
        b2b_drag = (1.0 if hb2b else 0.0) + (1.0 if ab2b else 0.0)
        inj_drag = 0.3 * float(max(0, h_outs)) + 0.3 * float(max(0, a_outs))
        pace = float(max(90.0, pace - b2b_drag - inj_drag))

        if np.isfinite(pace) and pace > 0:
            event_cfg = replace(event_cfg, possessions_per_game=float(pace))
    except Exception:
        event_cfg = cfg.event_cfg

    roster_mode = str(getattr(cfg, "roster_mode", None) or "historical").strip().lower()
    pregame_safe = roster_mode in {"pregame", "pregame_safe", "pregame-safe", "safe_pregame", "no_boxscore", "no-boxscore"}

    # Strict as-of cutoff for pregame-safe backtests:
    # many cached artifacts (player_logs, boxscore-derived team stats, etc.) are keyed by game date.
    # When evaluating historical dates *as if pregame*, we should not include same-day games.
    asof_date_str = str(date_str)
    if pregame_safe:
        try:
            ts = pd.to_datetime(str(date_str), errors="coerce")
            if ts is not None and (not pd.isna(ts)):
                asof_date_str = (ts.normalize() - pd.Timedelta(days=1)).date().isoformat()
        except Exception:
            asof_date_str = str(date_str)

    # Opponent-aware team priors from cached advanced stats (no market inputs).
    home_team_adj: Optional[dict[str, float]] = None
    away_team_adj: Optional[dict[str, float]] = None
    team_adv_diag: dict[str, Any] = {"attempted": False, "applied": False}
    try:
        home_team_adj, away_team_adj, pace_mult, team_adv_diag = _team_adj_from_advanced_stats(
            date_str=str(asof_date_str),
            home_tri=str(home_tri),
            away_tri=str(away_tri),
        )
        pm = float(pace_mult) if np.isfinite(float(pace_mult)) else 1.0
        if np.isfinite(pm) and pm != 1.0:
            base_poss = float(getattr(event_cfg, "possessions_per_game", 98.0))
            # Keep bounded to preserve existing tuning + quarters model.
            new_poss = float(np.clip(base_poss * pm, 88.0, 112.0))
            event_cfg = replace(event_cfg, possessions_per_game=float(new_poss))
            team_adv_diag["possessions_per_game_before"] = float(base_poss)
            team_adv_diag["possessions_per_game_after"] = float(new_poss)
    except Exception:
        home_team_adj = None
        away_team_adj = None
        team_adv_diag = {"attempted": True, "applied": False, "reason": "exception"}

    # Optional: global total-points calibration (acts as a gentle PPP multiplier).
    # Applied via team efficiency multipliers so player stats remain coherent.
    try:
        cal = _load_smartsim_total_calibration()
        pm = float(cal.get("points_mult", 1.0))
        if not np.isfinite(pm):
            pm = 1.0
        pm = float(np.clip(pm, 0.97, 1.03))
        if abs(pm - 1.0) > 1e-9:
            def _apply(adj: Optional[dict[str, float]]) -> dict[str, float]:
                out = dict(adj) if isinstance(adj, dict) else {}
                out["eff_mult"] = float(out.get("eff_mult", 1.0)) * pm
                return out

            home_team_adj = _apply(home_team_adj)
            away_team_adj = _apply(away_team_adj)
            try:
                team_adv_diag["points_mult"] = float(pm)
                team_adv_diag["points_mult_source"] = "smart_sim_total_calibration.json"
            except Exception:
                pass
    except Exception:
        pass

    if market_total is None or market_home_spread is None:
        t2, s2 = _market_lines_from_processed_odds(date_str=date_str, home_tri=home_tri, away_tri=away_tri)
        if market_total is None and t2 is not None:
            market_total = t2
            market_total_source = "processed_game_odds"
        if market_home_spread is None and s2 is not None:
            market_home_spread = s2
            market_home_spread_source = "processed_game_odds"

    # Best-effort per-period lines (quarters/halves). Also serves as a fallback market anchor.
    period_lines = _period_lines_from_processed(date_str=date_str, home_tri=home_tri, away_tri=away_tri) or {}

    # Fallback: if full-game totals are missing but H1 total exists, approximate full-game total.
    # This is intentionally simple and is only used to avoid completely unanchored totals.
    if market_total is None and isinstance(period_lines, dict):
        try:
            h1_total = period_lines.get("h1_total")
            h1_total_f = float(h1_total) if h1_total is not None else float("nan")
            if np.isfinite(h1_total_f) and h1_total_f > 0:
                market_total = float(2.0 * h1_total_f)
                market_total_source = "period_lines_h1_total_x2"
        except Exception:
            pass

    # Quarter distribution
    if quarters is None:
        # Minimal fallback TeamContext from prediction-implied ratings. (Caller should prefer passing quarters.)
        home_ctx = TeamContext(team=home_tri, pace=98.0, off_rating=112.0, def_rating=112.0)
        away_ctx = TeamContext(team=away_tri, pace=98.0, off_rating=112.0, def_rating=112.0)
        inp = GameInputs(date=date_str, home=home_ctx, away=away_ctx, market_total=market_total, market_home_spread=market_home_spread)
        quarters = simulate_quarters(inp, n_samples=3000).quarters

    # Player priors (cached per as-of date)
    pri = _compute_player_priors_cached(str(asof_date_str), int(cfg.priors_days_back))

    excluded_map: dict[str, set[str]] = {}
    try:
        if isinstance(excluded_player_keys_by_team, dict):
            for k, v in excluded_player_keys_by_team.items():
                kk = str(k or "").strip().upper()
                if not kk:
                    continue
                vv = set(str(x or "").strip().upper() for x in (v or set()) if str(x or "").strip())
                if vv:
                    excluded_map[kk] = vv
    except Exception:
        excluded_map = {}

    def _drop_excluded(df: pd.DataFrame, team_tri: str) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df
        out = df.copy()

        # Always enforce non-empty player names. Some fallback pools (or partial joins)
        # can introduce blank names; if those rows receive minutes they will silently
        # vanish from exports/minutes summaries.
        if "player_name" in out.columns:
            out["player_name"] = out["player_name"].astype(str).str.strip()
            out = out[out["player_name"].ne("")].copy()
        if out.empty:
            return out

        t = str(team_tri or "").strip().upper()
        ban = excluded_map.get(t)
        if ban:
            out["_pkey"] = out.get("player_name", "").map(_norm_player_key)
            out = out[~out["_pkey"].astype(str).str.upper().isin(ban)].drop(columns=["_pkey"], errors="ignore")
        return out

    home_raw = _drop_excluded(_team_players_from_props(props_df, home_tri, away_tri), home_tri)
    away_raw = _drop_excluded(_team_players_from_props(props_df, away_tri, home_tri), away_tri)

    allow_processed_boxscores = (not pregame_safe)
    allow_espn_boxscore = (not pregame_safe)

    # Roster guardrail: SmartSim needs a reasonably-sized player pool.
    # If props-based pool is missing most of the roster, augment with (in order):
    # 1) processed boxscores roster (completed games only; disabled in pregame-safe mode)
    # 2) ESPN boxscore roster (often postgame; disabled in pregame-safe mode)
    # 3) processed season rosters (pregame-safe fallback)
    # letting props rows override on name/team.
    def _augment_team_players(team_raw: pd.DataFrame, team_tri: str, gid: Optional[str]) -> pd.DataFrame:
        try:
            base = team_raw if isinstance(team_raw, pd.DataFrame) else pd.DataFrame()
            if base is None:
                base = pd.DataFrame()
            if (not base.empty) and (len(base) >= 8):
                return _drop_excluded(base, team_tri)

            if allow_processed_boxscores:
                from_box = _team_players_from_processed_boxscores(
                    date_str=str(date_str),
                    home_tri=str(home_tri),
                    away_tri=str(away_tri),
                    team_tri=str(team_tri),
                    game_id=gid,
                )
                if from_box is not None and not from_box.empty:
                    comb = pd.concat([from_box, base], ignore_index=True, sort=False)
                    if "team" in comb.columns:
                        comb["team"] = comb["team"].astype(str).str.upper().str.strip()
                    if "player_name" in comb.columns:
                        comb["player_name"] = comb["player_name"].astype(str).str.strip()
                        comb = comb[comb["player_name"].ne("")].copy()
                        if "team" in comb.columns:
                            comb = comb.drop_duplicates(subset=["player_name", "team"], keep="last")
                        else:
                            comb = comb.drop_duplicates(subset=["player_name"], keep="last")
                    return _drop_excluded(comb, team_tri)

            espn = None
            if allow_espn_boxscore:
                espn = _team_players_from_espn_boxscore(
                    date_str,
                    home_tri=home_tri,
                    away_tri=away_tri,
                    team_tri=team_tri,
                )

            if espn is None or espn.empty:
                # Pregame-safe fallback: season rosters.
                rost = _team_players_from_processed_rosters(
                    date_str=str(date_str),
                    home_tri=str(home_tri),
                    away_tri=str(away_tri),
                    team_tri=str(team_tri),
                )
                if rost is None or rost.empty:
                    return _drop_excluded(base, team_tri)
                comb = pd.concat([rost, base], ignore_index=True, sort=False)
                if "team" in comb.columns:
                    comb["team"] = comb["team"].astype(str).str.upper().str.strip()
                if "player_name" in comb.columns:
                    comb["player_name"] = comb["player_name"].astype(str).str.strip()
                    comb = comb[comb["player_name"].ne("")].copy()
                    if "team" in comb.columns:
                        comb = comb.drop_duplicates(subset=["player_name", "team"], keep="last")
                    else:
                        comb = comb.drop_duplicates(subset=["player_name"], keep="last")
                return _drop_excluded(comb, team_tri)

            # Prefer props rows when present by concatenating ESPN first then props and keeping last.
            comb = pd.concat([espn, base], ignore_index=True, sort=False)
            if "team" in comb.columns:
                comb["team"] = comb["team"].astype(str).str.upper().str.strip()
            if "player_name" in comb.columns:
                comb["player_name"] = comb["player_name"].astype(str).str.strip()
                comb = comb[comb["player_name"].ne("")].copy()
                if "team" in comb.columns:
                    comb = comb.drop_duplicates(subset=["player_name", "team"], keep="last")
                else:
                    comb = comb.drop_duplicates(subset=["player_name"], keep="last")
            return _drop_excluded(comb, team_tri)
        except Exception:
            return team_raw if isinstance(team_raw, pd.DataFrame) else pd.DataFrame()

    # IMPORTANT: In pregame-safe mode, never use a concrete game_id.
    # A gid enables reading per-game rotation stints which are postgame artifacts in backfills.
    gid = "" if pregame_safe else str(game_id or "").strip()
    if (not gid) and (not pregame_safe):
        gid = str(_infer_game_id(date_str, home_tri=home_tri, away_tri=away_tri) or "").strip()
    gid = gid or None

    home_raw = _augment_team_players(home_raw, team_tri=home_tri, gid=gid)
    away_raw = _augment_team_players(away_raw, team_tri=away_tri, gid=gid)

    # Final fallback: if still empty, try processed boxscores/ESPN only when allowed.
    if allow_processed_boxscores:
        if home_raw is None or home_raw.empty:
            home_raw = _team_players_from_processed_boxscores(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=home_tri, game_id=gid)
        if away_raw is None or away_raw.empty:
            away_raw = _team_players_from_processed_boxscores(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=away_tri, game_id=gid)
    if allow_espn_boxscore:
        if home_raw is None or home_raw.empty:
            home_raw = _team_players_from_espn_boxscore(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=home_tri)
        if away_raw is None or away_raw.empty:
            away_raw = _team_players_from_espn_boxscore(date_str, home_tri=home_tri, away_tri=away_tri, team_tri=away_tri)
    # Pregame-safe final fallback: season rosters.
    if home_raw is None or home_raw.empty:
        home_raw = _team_players_from_processed_rosters(date_str=str(date_str), home_tri=str(home_tri), away_tri=str(away_tri), team_tri=str(home_tri))
    if away_raw is None or away_raw.empty:
        away_raw = _team_players_from_processed_rosters(date_str=str(date_str), home_tri=str(home_tri), away_tri=str(away_tri), team_tri=str(away_tri))

    home_raw = home_raw.reset_index(drop=True) if isinstance(home_raw, pd.DataFrame) else pd.DataFrame()
    away_raw = away_raw.reset_index(drop=True) if isinstance(away_raw, pd.DataFrame) else pd.DataFrame()

    # Merge pregame expected minutes into the roster rows (if available).
    pem_diag: dict[str, Any] = {"home": None, "away": None}
    try:
        home_raw, pem_diag_home = _merge_pregame_expected_minutes_for_team(home_raw, date_str=str(date_str), team_tri=str(home_tri))
        away_raw, pem_diag_away = _merge_pregame_expected_minutes_for_team(away_raw, date_str=str(date_str), team_tri=str(away_tri))
        pem_diag = {"home": pem_diag_home, "away": pem_diag_away}
    except Exception:
        pem_diag = {"home": None, "away": None}

    # Best-effort ESPN event id for this matchup (useful for lineup teammate effects even pregame).
    eid_matchup: Optional[str] = None
    try:
        from ..boxscores import _espn_event_id_for_matchup  # type: ignore

        eid_matchup = _espn_event_id_for_matchup(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri))
        eid_matchup = str(eid_matchup or "").strip() or None
    except Exception:
        eid_matchup = None

    rot_date_str = str(asof_date_str) if pregame_safe else str(date_str)

    rot_home_min, home_lineups, home_lineup_w, rot_home_diag = _rotation_sim_minutes_for_team(
        home_raw,
        date_str=rot_date_str,
        home_tri=home_tri,
        away_tri=away_tri,
        team_tri=home_tri,
        side="home",
        game_id=gid,
    )
    rot_away_min, away_lineups, away_lineup_w, rot_away_diag = _rotation_sim_minutes_for_team(
        away_raw,
        date_str=rot_date_str,
        home_tri=home_tri,
        away_tri=away_tri,
        team_tri=away_tri,
        side="away",
        game_id=gid,
    )

    home_players = _apply_player_priors(home_raw, pri, team_tri=home_tri, sim_minutes=rot_home_min, date_str=str(rot_date_str))
    away_players = _apply_player_priors(away_raw, pri, team_tri=away_tri, sim_minutes=rot_away_min, date_str=str(rot_date_str))

    # Optional: lineup-conditioned teammate effects (learned from historical play context + rotation pairs).
    lineup_effects_diag: dict[str, Any] = {"home": None, "away": None}
    try:
        from .connected_game import _apply_lineup_teammate_effects_to_priors  # type: ignore

        eid = str(eid_matchup or rot_home_diag.get("event_id") or rot_away_diag.get("event_id") or "").strip() or None
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
                p_raw = float(np.mean((margin + float(spread_line)) > 0.0))
                # Optional probability calibration learned from recent smart_sim_quarter_eval.
                # Applies only when a calibration artifact exists for (date-1).
                try:
                    from ..prob_calibration import calibrate_prob  # type: ignore

                    out["p_home_cover_raw"] = p_raw
                    out["p_home_cover"] = float(calibrate_prob(str(date_str), f"{name}_cover", p_raw))
                except Exception:
                    out["p_home_cover"] = p_raw
            except Exception:
                out["market_home_spread"] = float(spread_line)
                out["p_home_cover"] = None
        else:
            out["market_home_spread"] = None
            out["p_home_cover"] = None
        if total_line is not None:
            try:
                out["market_total"] = float(total_line)
                p_raw = float(np.mean(total > float(total_line)))
                # Optional probability calibration learned from recent smart_sim_quarter_eval.
                # Applies only when a calibration artifact exists for (date-1).
                try:
                    from ..prob_calibration import calibrate_prob  # type: ignore

                    out["p_total_over_raw"] = p_raw
                    out["p_total_over"] = float(calibrate_prob(str(date_str), f"{name}_over", p_raw))
                except Exception:
                    out["p_total_over"] = p_raw
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

    def _blank_player_q_store(names: List[str]) -> Dict[str, Dict[str, List[int]]]:
        return {
            n: {
                "q1_pts": [], "q2_pts": [], "q3_pts": [], "q4_pts": [],
                "q1_reb": [], "q2_reb": [], "q3_reb": [], "q4_reb": [],
                "q1_ast": [], "q2_ast": [], "q3_ast": [], "q4_ast": [],
                "q1_threes": [], "q2_threes": [], "q3_threes": [], "q4_threes": [],
            }
            for n in names
            if n
        }

    def _blank_player_store_light(names: List[str]) -> Dict[str, Dict[str, List[int]]]:
        return {n: {"pts": [], "reb": [], "ast": [], "threes": []} for n in names if n}

    home_store = _blank_player_store(h_names)
    away_store = _blank_player_store(a_names)
    home_store_q = _blank_player_q_store(h_names)
    away_store_q = _blank_player_q_store(a_names)

    scenario_keys = ("close", "medium", "blowout")
    home_store_s = {k: _blank_player_store_light(h_names) for k in scenario_keys}
    away_store_s = {k: _blank_player_store_light(a_names) for k in scenario_keys}

    def _scenario_from_margin(m: int) -> str:
        try:
            am = abs(int(m))
        except Exception:
            am = 0
        if am <= 6:
            return "close"
        if am <= 14:
            return "medium"
        return "blowout"

    # Quarter score arrays (filled either by quarter-sampling or by PBP sim)
    hq_sims = np.zeros((n_sims, 4), dtype=int)
    aq_sims = np.zeros((n_sims, 4), dtype=int)

    # Regulation segment buckets (for live-lens interval ladders)
    # - 3-minute segments (legacy): 4 per quarter
    # - 1-minute segments (native): 12 per quarter
    n_seg_per_q = 4
    n_min_per_q = 12
    hqseg_sims = np.zeros((n_sims, 4, n_seg_per_q), dtype=int)
    aqseg_sims = np.zeros((n_sims, 4, n_seg_per_q), dtype=int)
    hqmin_sims = np.zeros((n_sims, 4, n_min_per_q), dtype=int)
    aqmin_sims = np.zeros((n_sims, 4, n_min_per_q), dtype=int)
    seg_seconds = 3 * 60
    min_seconds = 60

    # Overtime (5-minute periods). Variable count across sims.
    ot_seconds = 5 * 60
    home_ot_sims: list[list[int]] = [[] for _ in range(n_sims)]
    away_ot_sims: list[list[int]] = [[] for _ in range(n_sims)]

    if cfg.use_pbp:
        for i in range(n_sims):
            h_box, a_box, hq_i, aq_i = simulate_pbp_game_boxscore(
                rng=rng,
                home_players=home_players,
                away_players=away_players,
                cfg=event_cfg,
                home_lineups=home_lineups,
                home_lineup_weights=home_lineup_w,
                away_lineups=away_lineups,
                away_lineup_weights=away_lineup_w,
                target_home_points=target_home_points,
                target_away_points=target_away_points,
                quarters=quarters,
                home_team_adj=home_team_adj,
                away_team_adj=away_team_adj,
            )

            hq_sims[i, :] = np.asarray(list(hq_i or [0, 0, 0, 0])[:4], dtype=int)
            aq_sims[i, :] = np.asarray(list(aq_i or [0, 0, 0, 0])[:4], dtype=int)
            home_scores[i] = int(np.sum(hq_sims[i, :]))
            away_scores[i] = int(np.sum(aq_sims[i, :]))

            # Segment buckets (best-effort; present when events.py provides q_segment_pts)
            try:
                hseg = np.asarray((h_box or {}).get("q_segment_pts") or [[0] * n_seg_per_q] * 4, dtype=int)
                aseg = np.asarray((a_box or {}).get("q_segment_pts") or [[0] * n_seg_per_q] * 4, dtype=int)
                if hseg.shape == (4, n_seg_per_q):
                    hqseg_sims[i, :, :] = hseg
                if aseg.shape == (4, n_seg_per_q):
                    aqseg_sims[i, :, :] = aseg
                ssec = (h_box or {}).get("segment_seconds")
                if ssec is not None and int(ssec) > 0:
                    seg_seconds = int(ssec)
            except Exception:
                pass

            # Minute buckets (best-effort; present when events.py provides q_minute_pts)
            try:
                hmin = np.asarray((h_box or {}).get("q_minute_pts") or [[0] * n_min_per_q] * 4, dtype=int)
                amin = np.asarray((a_box or {}).get("q_minute_pts") or [[0] * n_min_per_q] * 4, dtype=int)
                if hmin.shape == (4, n_min_per_q):
                    hqmin_sims[i, :, :] = hmin
                if amin.shape == (4, n_min_per_q):
                    aqmin_sims[i, :, :] = amin
                msec = (h_box or {}).get("minute_seconds")
                if msec is not None and int(msec) > 0:
                    min_seconds = int(msec)
            except Exception:
                pass

            # Overtime points (per OT period) for interval ladder.
            try:
                hot = (h_box or {}).get("ot_pts")
                aot = (a_box or {}).get("ot_pts")
                if isinstance(hot, list) and isinstance(aot, list):
                    home_ot_sims[i] = [int(x) for x in hot if x is not None]
                    away_ot_sims[i] = [int(x) for x in aot if x is not None]
                osec = (h_box or {}).get("ot_seconds")
                if osec is not None and int(osec) > 0:
                    ot_seconds = int(osec)
            except Exception:
                pass

            scen = _scenario_from_margin(int(home_scores[i] - away_scores[i]))

            for p in (h_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in home_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        home_store[name][stat].append(int((p or {}).get(stat) or 0))

                    # Quarter-level props (PBP mode)
                    try:
                        qpts = (p or {}).get("q_pts") or [0, 0, 0, 0]
                        qreb = (p or {}).get("q_reb") or [0, 0, 0, 0]
                        qast = (p or {}).get("q_ast") or [0, 0, 0, 0]
                        q3 = (p or {}).get("q_threes") or [0, 0, 0, 0]
                        home_store_q[name]["q1_pts"].append(int(qpts[0] or 0))
                        home_store_q[name]["q2_pts"].append(int(qpts[1] or 0))
                        home_store_q[name]["q3_pts"].append(int(qpts[2] or 0))
                        home_store_q[name]["q4_pts"].append(int(qpts[3] or 0))
                        home_store_q[name]["q1_reb"].append(int(qreb[0] or 0))
                        home_store_q[name]["q2_reb"].append(int(qreb[1] or 0))
                        home_store_q[name]["q3_reb"].append(int(qreb[2] or 0))
                        home_store_q[name]["q4_reb"].append(int(qreb[3] or 0))
                        home_store_q[name]["q1_ast"].append(int(qast[0] or 0))
                        home_store_q[name]["q2_ast"].append(int(qast[1] or 0))
                        home_store_q[name]["q3_ast"].append(int(qast[2] or 0))
                        home_store_q[name]["q4_ast"].append(int(qast[3] or 0))
                        home_store_q[name]["q1_threes"].append(int(q3[0] or 0))
                        home_store_q[name]["q2_threes"].append(int(q3[1] or 0))
                        home_store_q[name]["q3_threes"].append(int(q3[2] or 0))
                        home_store_q[name]["q4_threes"].append(int(q3[3] or 0))
                    except Exception:
                        pass

                    # Scenario-conditioned totals (game script)
                    try:
                        home_store_s[scen][name]["pts"].append(int((p or {}).get("pts") or 0))
                        home_store_s[scen][name]["reb"].append(int((p or {}).get("reb") or 0))
                        home_store_s[scen][name]["ast"].append(int((p or {}).get("ast") or 0))
                        home_store_s[scen][name]["threes"].append(int((p or {}).get("threes") or 0))
                    except Exception:
                        pass

            for p in (a_box or {}).get("players", []) or []:
                name = str((p or {}).get("player_name") or "").strip()
                if name in away_store:
                    for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                        away_store[name][stat].append(int((p or {}).get(stat) or 0))

                    try:
                        qpts = (p or {}).get("q_pts") or [0, 0, 0, 0]
                        qreb = (p or {}).get("q_reb") or [0, 0, 0, 0]
                        qast = (p or {}).get("q_ast") or [0, 0, 0, 0]
                        q3 = (p or {}).get("q_threes") or [0, 0, 0, 0]
                        away_store_q[name]["q1_pts"].append(int(qpts[0] or 0))
                        away_store_q[name]["q2_pts"].append(int(qpts[1] or 0))
                        away_store_q[name]["q3_pts"].append(int(qpts[2] or 0))
                        away_store_q[name]["q4_pts"].append(int(qpts[3] or 0))
                        away_store_q[name]["q1_reb"].append(int(qreb[0] or 0))
                        away_store_q[name]["q2_reb"].append(int(qreb[1] or 0))
                        away_store_q[name]["q3_reb"].append(int(qreb[2] or 0))
                        away_store_q[name]["q4_reb"].append(int(qreb[3] or 0))
                        away_store_q[name]["q1_ast"].append(int(qast[0] or 0))
                        away_store_q[name]["q2_ast"].append(int(qast[1] or 0))
                        away_store_q[name]["q3_ast"].append(int(qast[2] or 0))
                        away_store_q[name]["q4_ast"].append(int(qast[3] or 0))
                        away_store_q[name]["q1_threes"].append(int(q3[0] or 0))
                        away_store_q[name]["q2_threes"].append(int(q3[1] or 0))
                        away_store_q[name]["q3_threes"].append(int(q3[2] or 0))
                        away_store_q[name]["q4_threes"].append(int(q3[3] or 0))
                    except Exception:
                        pass

                    try:
                        away_store_s[scen][name]["pts"].append(int((p or {}).get("pts") or 0))
                        away_store_s[scen][name]["reb"].append(int((p or {}).get("reb") or 0))
                        away_store_s[scen][name]["ast"].append(int((p or {}).get("ast") or 0))
                        away_store_s[scen][name]["threes"].append(int((p or {}).get("threes") or 0))
                    except Exception:
                        pass
    else:
        # Legacy path: sample quarter totals first and reconcile event stream to match.
        from .quarters import sample_quarter_scores

        hq, aq = sample_quarter_scores(quarters, n_samples=n_sims, rng=rng, round_to_int=True)
        for i in range(n_sims):
            hq_i = [int(x) for x in hq[i, :].tolist()]
            aq_i = [int(x) for x in aq[i, :].tolist()]

            # Synthesize segment splits that sum to the quarter totals.
            try:
                for qi in range(4):
                    hqseg_sims[i, qi, :] = rng.multinomial(int(max(0, hq_i[qi])), [0.25, 0.25, 0.25, 0.25]).astype(int)
                    aqseg_sims[i, qi, :] = rng.multinomial(int(max(0, aq_i[qi])), [0.25, 0.25, 0.25, 0.25]).astype(int)

                    # 1-minute fallback buckets: uniform split across 12 minutes.
                    pm = [1.0 / float(n_min_per_q)] * int(n_min_per_q)
                    hqmin_sims[i, qi, :] = rng.multinomial(int(max(0, hq_i[qi])), pm).astype(int)
                    aqmin_sims[i, qi, :] = rng.multinomial(int(max(0, aq_i[qi])), pm).astype(int)
            except Exception:
                pass

            h_box, a_box = simulate_event_level_boxscore(
                rng=rng,
                home_players=home_players,
                away_players=away_players,
                home_q_pts=hq_i,
                away_q_pts=aq_i,
                cfg=event_cfg,
                home_lineups=home_lineups,
                home_lineup_weights=home_lineup_w,
                away_lineups=away_lineups,
                away_lineup_weights=away_lineup_w,
                home_team_adj=home_team_adj,
                away_team_adj=away_team_adj,
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

    home_name_to_id: dict[str, Any] = {}
    away_name_to_id: dict[str, Any] = {}
    try:
        if "player_id" in home_raw.columns and "player_name" in home_raw.columns:
            htmp = home_raw[["player_name", "player_id"]].copy()
            for _, rr in htmp.iterrows():
                nm = str(rr.get("player_name") or "").strip()
                pid = rr.get("player_id")
                if nm and pid is not None and str(pid) != "nan":
                    home_name_to_id[nm] = pid
        if "player_id" in away_raw.columns and "player_name" in away_raw.columns:
            atmp = away_raw[["player_name", "player_id"]].copy()
            for _, rr in atmp.iterrows():
                nm = str(rr.get("player_name") or "").strip()
                pid = rr.get("player_id")
                if nm and pid is not None and str(pid) != "nan":
                    away_name_to_id[nm] = pid
    except Exception:
        home_name_to_id = {}
        away_name_to_id = {}

    def _team_player_summaries(
        store: Dict[str, Dict[str, List[int]]],
        store_q: Dict[str, Dict[str, List[int]]],
        store_s: Dict[str, Dict[str, Dict[str, List[int]]]],
        name_to_id: dict[str, Any],
        minutes_by_name: Optional[dict[str, float]] = None,
    ) -> List[Dict[str, Any]]:
        minutes_by_name = minutes_by_name or {}
        out_rows: List[Dict[str, Any]] = []
        cal = _load_player_stat_calibration()
        cal_players: dict[str, Any] = {}
        try:
            if isinstance(cal, dict):
                cal_players = cal.get("players") or {}
        except Exception:
            cal_players = {}
        for name, stats in store.items():
            row: Dict[str, Any] = {"player_name": name}
            pid_key: str = ""
            try:
                pid = name_to_id.get(name)
                if pid is not None and str(pid) != "nan":
                    row["player_id"] = int(float(pid)) if str(pid).replace(".", "", 1).isdigit() else pid
                    pid_key = _clean_id_str(row.get("player_id"))
            except Exception:
                pass

            try:
                mm = minutes_by_name.get(name)
                if mm is not None:
                    row["min_mean"] = float(mm)
            except Exception:
                pass
            for stat in ("pts", "reb", "ast", "threes", "stl", "blk", "tov"):
                arr = np.asarray(stats.get(stat) or [], dtype=float)
                mu = float(np.mean(arr)) if arr.size else float("nan")
                # Optional: per-player bias correction from recent recon.
                try:
                    if pid_key:
                        pb = (cal_players.get(str(pid_key)) or {}).get(stat)
                    else:
                        pb = None
                    b = _finite_float_or_nan(pb)
                    if np.isfinite(b):
                        mu = float(mu + float(b))
                except Exception:
                    pass
                row[f"{stat}_mean"] = float(mu)
                row[f"{stat}_sd"] = float(np.std(arr)) if arr.size else float("nan")
                row[f"{stat}_q"] = _quantiles(arr)
            # Derived props
            pra = np.asarray(stats.get("pts") or [], dtype=float) + np.asarray(stats.get("reb") or [], dtype=float) + np.asarray(stats.get("ast") or [], dtype=float)
            pra_mu = float(np.mean(pra)) if pra.size else float("nan")
            try:
                if pid_key:
                    pb = (cal_players.get(str(pid_key)) or {}).get("pra")
                else:
                    pb = None
                b = _finite_float_or_nan(pb)
                if np.isfinite(b):
                    pra_mu = float(pra_mu + float(b))
            except Exception:
                pass
            row["pra_mean"] = float(pra_mu)
            row["pra_sd"] = float(np.std(pra)) if pra.size else float("nan")
            row["pra_q"] = _quantiles(pra)

            # Quarter-level summaries (points/rebounds/assists/threes)
            try:
                qd = store_q.get(name) or {}
                row["quarters"] = {
                    "q1": {
                        "pts_q": _quantiles(np.asarray(qd.get("q1_pts") or [], dtype=float)),
                        "reb_q": _quantiles(np.asarray(qd.get("q1_reb") or [], dtype=float)),
                        "ast_q": _quantiles(np.asarray(qd.get("q1_ast") or [], dtype=float)),
                        "threes_q": _quantiles(np.asarray(qd.get("q1_threes") or [], dtype=float)),
                    },
                    "q2": {
                        "pts_q": _quantiles(np.asarray(qd.get("q2_pts") or [], dtype=float)),
                        "reb_q": _quantiles(np.asarray(qd.get("q2_reb") or [], dtype=float)),
                        "ast_q": _quantiles(np.asarray(qd.get("q2_ast") or [], dtype=float)),
                        "threes_q": _quantiles(np.asarray(qd.get("q2_threes") or [], dtype=float)),
                    },
                    "q3": {
                        "pts_q": _quantiles(np.asarray(qd.get("q3_pts") or [], dtype=float)),
                        "reb_q": _quantiles(np.asarray(qd.get("q3_reb") or [], dtype=float)),
                        "ast_q": _quantiles(np.asarray(qd.get("q3_ast") or [], dtype=float)),
                        "threes_q": _quantiles(np.asarray(qd.get("q3_threes") or [], dtype=float)),
                    },
                    "q4": {
                        "pts_q": _quantiles(np.asarray(qd.get("q4_pts") or [], dtype=float)),
                        "reb_q": _quantiles(np.asarray(qd.get("q4_reb") or [], dtype=float)),
                        "ast_q": _quantiles(np.asarray(qd.get("q4_ast") or [], dtype=float)),
                        "threes_q": _quantiles(np.asarray(qd.get("q4_threes") or [], dtype=float)),
                    },
                }
            except Exception:
                pass

            # Scenario-conditioned summaries (game script: close/medium/blowout)
            try:
                scen_out: Dict[str, Any] = {}
                for sk in ("close", "medium", "blowout"):
                    ss = (store_s.get(sk) or {}).get(name) or {}
                    arr_pts = np.asarray(ss.get("pts") or [], dtype=float)
                    arr_reb = np.asarray(ss.get("reb") or [], dtype=float)
                    arr_ast = np.asarray(ss.get("ast") or [], dtype=float)
                    arr_thr = np.asarray(ss.get("threes") or [], dtype=float)
                    scen_out[sk] = {
                        "n": int(arr_pts.size),
                        "pts_q": _quantiles(arr_pts),
                        "reb_q": _quantiles(arr_reb),
                        "ast_q": _quantiles(arr_ast),
                        "threes_q": _quantiles(arr_thr),
                        "pra_q": _quantiles(arr_pts + arr_reb + arr_ast),
                    }
                row["scenarios"] = scen_out
            except Exception:
                pass
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

    ctx_out: dict[str, Any] = {}
    try:
        if isinstance(pregame_context, dict) and pregame_context:
            ctx_out = dict(pregame_context)
    except Exception:
        ctx_out = {}

    # Record which simulation path was used.
    try:
        ctx_out["pbp_used"] = bool(getattr(cfg, "use_pbp", False))
    except Exception:
        pass

    try:
        ctx_out["roster_mode"] = str(roster_mode)
        ctx_out["asof_date"] = str(asof_date_str)
    except Exception:
        pass

    try:
        if isinstance(pem_diag, dict) and pem_diag:
            ctx_out["pregame_expected_minutes"] = pem_diag
    except Exception:
        pass

    try:
        if isinstance(team_adv_diag, dict) and team_adv_diag:
            # Always overwrite to reflect the actual priors used by this simulation.
            ctx_out["team_advanced_priors"] = team_adv_diag
    except Exception:
        pass
    try:
        if excluded_map:
            ctx_out.setdefault(
                "excluded_players",
                {
                    str(k): sorted(list(v))
                    for k, v in excluded_map.items()
                },
            )
    except Exception:
        pass

    def _sim_minutes_by_name(team_raw: pd.DataFrame, sim_min: Any) -> dict[str, float]:
        if not isinstance(team_raw, pd.DataFrame) or team_raw.empty:
            return {}
        if "player_name" not in team_raw.columns:
            return {}
        try:
            s = sim_min
            if s is None:
                return {}
            if not isinstance(s, pd.Series):
                s = pd.Series(s)
            s = s.reindex(team_raw.index)
            df = team_raw[["player_name"]].copy()
            df["_sim_min"] = pd.to_numeric(s, errors="coerce").fillna(0.0).astype(float)
            df["player_name"] = df["player_name"].astype(str).str.strip()
            df = df[df["player_name"].ne("")].copy()
            out = df.groupby("player_name")["_sim_min"].max().to_dict()
            return {str(k): float(v) for k, v in out.items()}
        except Exception:
            return {}

    home_minutes_by_name = _sim_minutes_by_name(home_raw, rot_home_min)
    away_minutes_by_name = _sim_minutes_by_name(away_raw, rot_away_min)

    # Ensure minutes are always available for UI/exports even when rotation mapping is sparse.
    # (e.g., pregame: stints unavailable, ESPN mapping incomplete, or fallback rosters used.)
    try:
        if not home_minutes_by_name:
            home_minutes_by_name = _sim_minutes_by_name(home_raw, _derive_sim_minutes(home_raw, date_str=str(date_str), team_tri=str(home_tri)))
        if not away_minutes_by_name:
            away_minutes_by_name = _sim_minutes_by_name(away_raw, _derive_sim_minutes(away_raw, date_str=str(date_str), team_tri=str(away_tri)))
    except Exception:
        pass

    def _minutes_summary(m: dict[str, float]) -> dict[str, Any]:
        try:
            vals = [float(v) for v in (m or {}).values() if v is not None and np.isfinite(float(v))]
        except Exception:
            vals = []
        if not vals:
            return {"n": 0, "sum": 0.0, "min": 0.0, "max": 0.0}
        return {
            "n": int(len(vals)),
            "sum": float(np.sum(vals)),
            "min": float(np.min(vals)),
            "max": float(np.max(vals)),
        }

    # Interval ladders
    # - intervals: regulation 3-minute segments + (optional) 5-minute OT segments
    # - intervals_1m: regulation 1-minute segments + (optional) 5-minute OT segments
    intervals: Optional[dict[str, Any]] = None
    intervals_1m: Optional[dict[str, Any]] = None
    try:
        cal = _load_intervals_band_calibration()
        tp = _load_intervals_time_profile()

        n_reg_segs = int(4 * n_seg_per_q)
        reg_total = (hqseg_sims + aqseg_sims).reshape(int(n_sims), n_reg_segs).astype(float)
        reg_total = _apply_intervals_time_profile(reg_total, tp)
        reg_cum = np.cumsum(reg_total, axis=1)

        def _seg_label(qi: int, si: int, seconds_per_seg: int) -> str:
            # qi, si are 0-based
            q = qi + 1
            seg_min = float(seconds_per_seg) / 60.0
            start_min = 12.0 - (seg_min * float(si))
            end_min = 12.0 - (seg_min * float(si + 1))
            if abs(seg_min - round(seg_min)) < 1e-6:
                return f"Q{q} {int(start_min)}-{int(end_min)}"
            return f"Q{q} {start_min:.1f}-{end_min:.1f}"

        seg_rows: list[dict[str, Any]] = []
        for j in range(n_reg_segs):
            qi = j // int(n_seg_per_q)
            si = j % int(n_seg_per_q)
            seg_arr = reg_total[:, j]
            cum_arr = reg_cum[:, j]

            seg_q = _quantiles(seg_arr, qs=(0.1, 0.5, 0.9))
            cum_q = _quantiles(cum_arr, qs=(0.1, 0.5, 0.9))
            seg_q = _apply_band_scale(seg_q, _interval_scale(cal, j + 1, "seg"))
            cum_q = _apply_band_scale(cum_q, _interval_scale(cal, j + 1, "cum"))

            seg_rows.append(
                {
                    "idx": int(j + 1),
                    "quarter": int(qi + 1),
                    "seg": int(si + 1),
                    "label": _seg_label(qi, si, int(seg_seconds)),
                    "mu": float(np.mean(seg_arr)) if seg_arr.size else float("nan"),
                    "q": seg_q,
                    "cum_mu": float(np.mean(cum_arr)) if cum_arr.size else float("nan"),
                    "cum_q": cum_q,
                }
            )

        # Overtime segments: one 5-minute interval per OT period.
        try:
            max_ot = 0
            for i in range(int(n_sims)):
                max_ot = max(max_ot, len(home_ot_sims[i] or []), len(away_ot_sims[i] or []))

            reg_totals = np.sum(reg_total, axis=1).astype(float)

            def _ot_label(k0: int) -> str:
                return f"OT{k0 + 1} 5-0"

            # Build conditional distributions for each OT k: only sims that reach that OT.
            for k in range(int(max_ot)):
                ot_vals: list[float] = []
                ot_cum_vals: list[float] = []
                reach = 0
                for i in range(int(n_sims)):
                    hot = home_ot_sims[i] or []
                    aot = away_ot_sims[i] or []
                    if len(hot) > k and len(aot) > k:
                        reach += 1
                        v = float(int(hot[k]) + int(aot[k]))
                        ot_vals.append(v)
                        # cumulative through OT k (reg + prior OTs)
                        prior = float(sum(int(hot[j]) + int(aot[j]) for j in range(0, k + 1)))
                        ot_cum_vals.append(float(reg_totals[i] + prior))

                arr = np.asarray(ot_vals, dtype=float)
                carr = np.asarray(ot_cum_vals, dtype=float)
                seg_rows.append(
                    {
                        "idx": int(16 + k + 1),
                        "ot": int(k + 1),
                        "label": _ot_label(k),
                        "duration_seconds": int(ot_seconds),
                        "p_reach": float(reach) / float(max(1, int(n_sims))),
                        "n_reach": int(reach),
                        "mu": float(np.mean(arr)) if arr.size else float("nan"),
                        "q": _apply_band_scale(
                            (_quantiles(arr, qs=(0.1, 0.5, 0.9)) if arr.size else {"p10": float("nan"), "p50": float("nan"), "p90": float("nan")}),
                            _interval_scale(cal, 16, "seg"),
                        ),
                        "cum_mu": float(np.mean(carr)) if carr.size else float("nan"),
                        "cum_q": _apply_band_scale(
                            (_quantiles(carr, qs=(0.1, 0.5, 0.9)) if carr.size else {"p10": float("nan"), "p50": float("nan"), "p90": float("nan")}),
                            _interval_scale(cal, 16, "cum"),
                        ),
                    }
                )
        except Exception:
            pass

        intervals = {
            "segment_seconds": int(seg_seconds),
            "segments_per_quarter": int(n_seg_per_q),
            "ot_segment_seconds": int(ot_seconds),
            "segments": seg_rows,
        }
    except Exception:
        intervals = None

    try:
        cal = _load_intervals_band_calibration()

        n_reg_mins = int(4 * n_min_per_q)
        reg_total_m = (hqmin_sims + aqmin_sims).reshape(int(n_sims), n_reg_mins).astype(float)
        reg_cum_m = np.cumsum(reg_total_m, axis=1)

        seg_rows_m: list[dict[str, Any]] = []
        for j in range(n_reg_mins):
            qi = j // int(n_min_per_q)
            si = j % int(n_min_per_q)
            seg_arr = reg_total_m[:, j]
            cum_arr = reg_cum_m[:, j]

            seg_q = _quantiles(seg_arr, qs=(0.1, 0.5, 0.9))
            cum_q = _quantiles(cum_arr, qs=(0.1, 0.5, 0.9))
            seg_q = _apply_band_scale(seg_q, _interval_scale(cal, j + 1, "seg"))
            cum_q = _apply_band_scale(cum_q, _interval_scale(cal, j + 1, "cum"))

            seg_rows_m.append(
                {
                    "idx": int(j + 1),
                    "quarter": int(qi + 1),
                    "seg": int(si + 1),
                    "label": _seg_label(qi, si, int(min_seconds)),
                    "mu": float(np.mean(seg_arr)) if seg_arr.size else float("nan"),
                    "q": seg_q,
                    "cum_mu": float(np.mean(cum_arr)) if cum_arr.size else float("nan"),
                    "cum_q": cum_q,
                }
            )

        # Overtime segments: one 5-minute interval per OT period (conditional on reaching OT).
        try:
            max_ot = 0
            for i in range(int(n_sims)):
                max_ot = max(max_ot, len(home_ot_sims[i] or []), len(away_ot_sims[i] or []))

            reg_totals_m = np.sum(reg_total_m, axis=1).astype(float)

            def _ot_label(k0: int) -> str:
                return f"OT{k0 + 1} 5-0"

            for k in range(int(max_ot)):
                ot_vals: list[float] = []
                ot_cum_vals: list[float] = []
                reach = 0
                for i in range(int(n_sims)):
                    hot = home_ot_sims[i] or []
                    aot = away_ot_sims[i] or []
                    if len(hot) > k and len(aot) > k:
                        reach += 1
                        v = float(int(hot[k]) + int(aot[k]))
                        ot_vals.append(v)
                        prior = float(sum(int(hot[j]) + int(aot[j]) for j in range(0, k + 1)))
                        ot_cum_vals.append(float(reg_totals_m[i] + prior))

                arr = np.asarray(ot_vals, dtype=float)
                carr = np.asarray(ot_cum_vals, dtype=float)
                seg_rows_m.append(
                    {
                        "idx": int(n_reg_mins + k + 1),
                        "ot": int(k + 1),
                        "label": _ot_label(k),
                        "duration_seconds": int(ot_seconds),
                        "p_reach": float(reach) / float(max(1, int(n_sims))),
                        "n_reach": int(reach),
                        "mu": float(np.mean(arr)) if arr.size else float("nan"),
                        "q": _apply_band_scale(
                            (_quantiles(arr, qs=(0.1, 0.5, 0.9)) if arr.size else {"p10": float("nan"), "p50": float("nan"), "p90": float("nan")}),
                            _interval_scale(cal, n_reg_mins, "seg"),
                        ),
                        "cum_mu": float(np.mean(carr)) if carr.size else float("nan"),
                        "cum_q": _apply_band_scale(
                            (_quantiles(carr, qs=(0.1, 0.5, 0.9)) if carr.size else {"p10": float("nan"), "p50": float("nan"), "p90": float("nan")}),
                            _interval_scale(cal, n_reg_mins, "cum"),
                        ),
                    }
                )
        except Exception:
            pass

        intervals_1m = {
            "segment_seconds": int(min_seconds),
            "segments_per_quarter": int(n_min_per_q),
            "ot_segment_seconds": int(ot_seconds),
            "segments": seg_rows_m,
        }
    except Exception:
        intervals_1m = None

    return {
        "home": str(home_tri).upper(),
        "away": str(away_tri).upper(),
        "date": str(date_str),
        "game_id": str(gid) if gid else None,
        "context": (ctx_out if ctx_out else None),
        "market": {
            "market_total": float(market_total) if market_total is not None else None,
            "market_home_spread": float(market_home_spread) if market_home_spread is not None else None,
            "market_total_source": market_total_source,
            "market_home_spread_source": market_home_spread_source,
        },
        "rotation_minutes": {
            "home": rot_home_diag,
            "away": rot_away_diag,
        },
        "minutes_summary": {
            "home": _minutes_summary(home_minutes_by_name),
            "away": _minutes_summary(away_minutes_by_name),
        },
        "lineup_effects": lineup_effects_diag,
        "n_sims": int(n_sims),
        "mode": {
            "use_pbp": bool(cfg.use_pbp),
            "target_home_points": float(target_home_points) if target_home_points is not None else None,
            "target_away_points": float(target_away_points) if target_away_points is not None else None,
        },
        "intervals": intervals,
        "intervals_1m": intervals_1m,
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
            "home": _team_player_summaries(home_store, home_store_q, home_store_s, home_name_to_id, minutes_by_name=home_minutes_by_name),
            "away": _team_player_summaries(away_store, away_store_q, away_store_s, away_name_to_id, minutes_by_name=away_minutes_by_name),
        },
    }
