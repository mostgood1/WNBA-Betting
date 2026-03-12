from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .quarters import QuarterResult, sample_quarter_scores
from ..config import paths
from ..player_names import normalize_player_name_key


def _read_processed_any(parquet_path, csv_path) -> pd.DataFrame:
    try:
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
    except Exception:
        pass
    try:
        if csv_path.exists():
            return pd.read_csv(csv_path)
    except Exception:
        pass
    return pd.DataFrame()


def _merge_pregame_expected_minutes(props_df: Any, date_str: Optional[str]) -> Any:
    """Best-effort merge of pregame expected minutes + starters into props_df.

    Expected artifact (if present):
      data/processed/pregame_expected_minutes_YYYY-MM-DD.(csv|parquet)

    Canonical columns after merge:
      - exp_min_mean, exp_min_sd, exp_min_cap, is_starter, exp_asof_ts, exp_min_source

    This function is intentionally tolerant: if files/cols are missing, returns props_df unchanged.
    """
    if props_df is None or not isinstance(props_df, pd.DataFrame) or props_df.empty:
        return props_df
    ds = str(date_str or "").strip()
    if not ds:
        return props_df

    exp = pd.DataFrame()
    try:
        # Canonical location is data/processed/, but during development/backtests we may keep
        # expected-minutes artifacts in backup folders.
        candidate_dirs = [
            paths.data_processed,
            paths.data_processed / "_bak_expected_minutes_eval",
            paths.data_processed / "_bak_expected_minutes",
        ]
        for base in candidate_dirs:
            try:
                p_csv = base / f"pregame_expected_minutes_{ds}.csv"
                p_pq = base / f"pregame_expected_minutes_{ds}.parquet"
                exp = _read_processed_any(p_pq, p_csv)
                if isinstance(exp, pd.DataFrame) and not exp.empty:
                    break
            except Exception:
                continue
    except Exception:
        exp = pd.DataFrame()

    if exp is None or exp.empty:
        return props_df

    try:
        exp = exp.copy()
        # Normalize team
        team_col = None
        for c in ("team_tri", "team", "team_abbr", "team_tricode"):
            if c in exp.columns:
                team_col = c
                break
        if team_col:
            exp[team_col] = exp[team_col].astype(str).str.upper().str.strip()
        # Normalize player id
        pid_col = None
        for c in ("player_id", "PLAYER_ID"):
            if c in exp.columns:
                pid_col = c
                break
        if pid_col:
            exp[pid_col] = exp[pid_col].map(_clean_id_str)

        # Canonicalize minutes columns
        col_map = {}
        for src, dst in (
            ("exp_min_mean", "exp_min_mean"),
            ("expected_min", "exp_min_mean"),
            ("expected_minutes", "exp_min_mean"),
            ("proj_min", "exp_min_mean"),
            ("exp_min", "exp_min_mean"),
            ("exp_min_sd", "exp_min_sd"),
            ("expected_min_sd", "exp_min_sd"),
            ("exp_min_cap", "exp_min_cap"),
            ("expected_min_cap", "exp_min_cap"),
            ("is_starter", "is_starter"),
            ("starter", "is_starter"),
            ("asof_ts", "exp_asof_ts"),
            ("report_ts", "exp_asof_ts"),
            ("source", "exp_min_source"),
        ):
            if src in exp.columns and dst not in exp.columns:
                col_map[src] = dst
        if col_map:
            exp = exp.rename(columns=col_map)

        # Ensure canonical columns exist
        for c in ("exp_min_mean", "exp_min_sd", "exp_min_cap", "is_starter", "exp_asof_ts", "exp_min_source"):
            if c not in exp.columns:
                exp[c] = None

        # Only merge *trusted* expected-minutes into props_df.
        # Low-trust sources (history backfills / baseline fillers) are still loaded elsewhere
        # for diagnostics and optional soft usage, but merging them here can perturb props
        # pool dedup/selection even when we don't use exp minutes for simulation.
        try:
            s = exp.get("exp_min_source").astype(str).str.strip().str.lower().fillna("")
            is_baseline = s.str.startswith("baseline:")
            is_history = s.str.contains("rotations_espn_history", regex=False)
            exp = exp[(~is_baseline) & (~is_history)].copy()
        except Exception:
            pass

        if exp is None or exp.empty:
            return props_df

        # Merge keys: prefer (team, player_id), else (team, name_key)
        base = props_df.copy()
        if "team" in base.columns:
            base["team"] = base["team"].astype(str).str.upper().str.strip()

        use_team = "team" if ("team" in base.columns) else None
        exp_team = team_col if team_col else None

        # Prepare exp slice: keep latest asof_ts per (team, player)
        try:
            if "exp_asof_ts" in exp.columns:
                exp["_exp_asof_dt"] = pd.to_datetime(exp.get("exp_asof_ts"), errors="coerce")
            else:
                exp["_exp_asof_dt"] = pd.NaT
        except Exception:
            exp["_exp_asof_dt"] = pd.NaT

        if pid_col and "player_id" in base.columns:
            try:
                base["player_id"] = base["player_id"].map(_clean_id_str)
            except Exception:
                pass
            exp2 = exp.copy()
            try:
                exp2["player_id"] = exp2[pid_col].map(_clean_id_str)
            except Exception:
                exp2["player_id"] = exp2.get(pid_col)
            keys = [k for k in (use_team, "player_id") if k]
            if exp_team and exp_team != use_team and exp_team in exp2.columns and use_team:
                exp2[use_team] = exp2[exp_team].astype(str).str.upper().str.strip()
            if keys:
                try:
                    exp2 = exp2.sort_values(["_exp_asof_dt"], ascending=True)
                except Exception:
                    pass
                exp2 = exp2.drop_duplicates(subset=keys, keep="last")
                base = base.merge(
                    exp2[keys + ["exp_min_mean", "exp_min_sd", "exp_min_cap", "is_starter", "exp_asof_ts", "exp_min_source"]],
                    on=keys,
                    how="left",
                )
                return base

        # Fallback: merge on normalized player name key.
        if "player_name" in base.columns and ("player_name" in exp.columns or "name" in exp.columns or "PLAYER_NAME" in exp.columns):
            exp_name_col = "player_name" if "player_name" in exp.columns else ("name" if "name" in exp.columns else ("PLAYER_NAME" if "PLAYER_NAME" in exp.columns else None))
            if exp_name_col:
                exp2 = exp.copy()
                base["_pkey"] = base["player_name"].map(_norm_player_key)
                exp2["_pkey"] = exp2[exp_name_col].astype(str).map(_norm_player_key)
                if exp_team and exp_team in exp2.columns and use_team:
                    exp2[use_team] = exp2[exp_team].astype(str).str.upper().str.strip()
                keys = [k for k in (use_team, "_pkey") if k]
                try:
                    exp2 = exp2.sort_values(["_exp_asof_dt"], ascending=True)
                except Exception:
                    pass
                exp2 = exp2.drop_duplicates(subset=keys, keep="last")
                base = base.merge(
                    exp2[keys + ["exp_min_mean", "exp_min_sd", "exp_min_cap", "is_starter", "exp_asof_ts", "exp_min_source"]],
                    on=keys,
                    how="left",
                )
                base = base.drop(columns=["_pkey"], errors="ignore")
                return base
    except Exception:
        return props_df

    return props_df


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


def _espn_name_to_id_map_for_game(
    date_str: str,
    home_tri: str,
    away_tri: str,
    event_id: Optional[str] = None,
) -> dict[tuple[str, str], str]:
    """Return mapping (team_tricode, normalized_player_key) -> espn_athlete_id.

    Prefer providing event_id (e.g., from rotation stints) to avoid relying on scoreboard lookup.
    Uses ESPN summary boxscore and normalizes names via _norm_player_key.
    """
    if not str(event_id or "").strip() and not date_str:
        return {}

    def _from_pbp_history(lookback_days: int = 120) -> dict[tuple[str, str], str]:
        """Local fallback mapping from pbp_espn_history.csv substitution rows."""
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
                combo = combo.sort_values(["date"])
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
        eid = str(event_id or "").strip() or (_espn_event_id_for_matchup(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri)) or "")
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
                out[(team_tri, key)] = pid
        return out or _from_pbp_history()
    except Exception:
        return _from_pbp_history()


def _load_lineup_context_tables() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load (baselines_df, teammate_df) if available; else (empty, empty)."""
    base = _read_processed_any(paths.data_processed / "lineup_player_baselines.parquet", paths.data_processed / "lineup_player_baselines.csv")
    tm = _read_processed_any(paths.data_processed / "lineup_teammate_effects.parquet", paths.data_processed / "lineup_teammate_effects.csv")
    if base is None:
        base = pd.DataFrame()
    if tm is None:
        tm = pd.DataFrame()
    return base, tm


def _apply_lineup_teammate_effects_to_priors(
    team_players: pd.DataFrame,
    team_tri: str,
    date_str: Optional[str],
    home_tri: str,
    away_tri: str,
    event_id: Optional[str] = None,
) -> pd.DataFrame:
    """Apply minutes-weighted teammate effects as multiplicative adjustments to per-minute priors.

    This is a soft conditioning layer: it nudges rates but stays bounded.
    If required tables or ESPN IDs are unavailable, returns team_players unchanged.
    """
    def _set_diag(df: pd.DataFrame, **kw: Any) -> pd.DataFrame:
        try:
            if isinstance(df, pd.DataFrame):
                d = dict(getattr(df, "attrs", {}).get("_lineup_effects", {}) or {})
                d.update(kw)
                df.attrs["_lineup_effects"] = d
        except Exception:
            pass
        return df

    if team_players is None or team_players.empty:
        return _set_diag(team_players, attempted=True, applied=False, reason="empty_players")
    if not date_str:
        return _set_diag(team_players, attempted=True, applied=False, reason="missing_date")

    base, tm = _load_lineup_context_tables()
    if base.empty or tm.empty:
        return _set_diag(team_players, attempted=True, applied=False, reason="missing_tables")

    needed_cols = {"team", "player_id", "minutes"}
    if not {"team", "player_id"}.issubset(set(base.columns)):
        return _set_diag(team_players, attempted=True, applied=False, reason="bad_baselines_schema")
    if not {"team", "player_id", "teammate_id"}.issubset(set(tm.columns)):
        return _set_diag(team_players, attempted=True, applied=False, reason="bad_teammate_schema")

    # ESPN athlete ID mapping for this matchup
    name_to_id = _espn_name_to_id_map_for_game(str(date_str), home_tri=str(home_tri), away_tri=str(away_tri), event_id=event_id)
    if not name_to_id:
        return _set_diag(team_players, attempted=True, applied=False, reason="no_espn_id_map")

    out = team_players.copy()
    team_u = str(team_tri or "").strip().upper()
    out["_pkey"] = out.get("player_name", pd.Series(["" for _ in range(len(out))])).map(_norm_player_key)
    out["_espn_id"] = out["_pkey"].map(lambda k: name_to_id.get((team_u, k), ""))
    out["_espn_id"] = out["_espn_id"].astype(str).replace({"nan": "", "None": ""}).str.strip()

    mapped_n = int((out["_espn_id"].astype(str).str.len() > 0).sum())

    # Only apply when we have priors.
    prior_stats = [
        "pts",
        "reb",
        "ast",
        "threes",
        "threes_att",
        "tov",
        "stl",
        "blk",
        "fga",
        "fgm",
        "fta",
        "ftm",
        "pf",
    ]
    have_any = any(f"_prior_{s}_pm" in out.columns for s in prior_stats)
    if not have_any:
        return _set_diag(team_players, attempted=True, applied=False, reason="missing_prior_columns", mapped_players=mapped_n)

    # Normalize tables
    try:
        base = base.copy()
        tm = tm.copy()
        base["team"] = base["team"].astype(str).str.upper().str.strip()
        tm["team"] = tm["team"].astype(str).str.upper().str.strip()
        base["player_id"] = base["player_id"].astype(str).str.replace(r"^(\d+)\.0$", r"\1", regex=True).str.strip()
        tm["player_id"] = tm["player_id"].astype(str).str.replace(r"^(\d+)\.0$", r"\1", regex=True).str.strip()
        tm["teammate_id"] = tm["teammate_id"].astype(str).str.replace(r"^(\d+)\.0$", r"\1", regex=True).str.strip()
    except Exception:
        return _set_diag(team_players, attempted=True, applied=False, reason="table_normalization_failed", mapped_players=mapped_n)

    base_t = base[base["team"] == team_u]
    tm_t = tm[tm["team"] == team_u]
    if base_t.empty or tm_t.empty:
        return _set_diag(team_players, attempted=True, applied=False, reason="no_team_rows", mapped_players=mapped_n)

    # Minutes weights from the sim rotation
    mins = pd.to_numeric(out.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
    id_list = out["_espn_id"].astype(str).tolist()
    if not any(id_list):
        return _set_diag(team_players, attempted=True, applied=False, reason="no_players_mapped", mapped_players=mapped_n)
    id_to_min = {str(pid): float(mins[i]) for i, pid in enumerate(id_list) if str(pid)}

    # Map stat -> columns in tables
    stat_to_base_per36 = {
        "pts": "pts_per36",
        "reb": "reb_per36",
        "ast": "ast_per36",
        "tov": "tov_per36",
        "stl": "stl_per36",
        "blk": "blk_per36",
        "pf": "pf_per36",
        "fga": "fga_per36",
        "threes_att": "three_pa_per36",
    }
    stat_to_with_per36 = {
        "pts": "pts_per36_with",
        "reb": "reb_per36_with",
        "ast": "ast_per36_with",
        "tov": "tov_per36_with",
        "stl": "stl_per36_with",
        "blk": "blk_per36_with",
        "pf": "pf_per36_with",
        "fga": "fga_per36_with",
        "threes_att": "three_pa_per36_with",
    }
    # For makes, approximate with attempts multiplier.
    make_like = {"fgm": "fga", "ftm": "fta", "threes": "threes_att"}

    # Precompute baseline per36 for each player id.
    base_idx = base_t.set_index("player_id", drop=False)

    # Apply a bounded multiplier per player/stat.
    nudges = 0
    max_abs_mult_delta = 0.0
    for i in range(len(out)):
        pid = str(out.at[i, "_espn_id"] or "").strip()
        if not pid:
            continue
        # Teammates present in this sim rotation
        teammates = [(tid, m) for tid, m in id_to_min.items() if tid and tid != pid and m > 0]
        if not teammates:
            continue

        # Pre-filter teammate rows once
        trows = tm_t[tm_t["player_id"] == pid]
        if trows.empty:
            continue
        trows = trows[trows["teammate_id"].isin([tid for tid, _ in teammates])]
        if trows.empty:
            continue

        for stat in prior_stats:
            prior_col = f"_prior_{stat}_pm"
            if prior_col not in out.columns:
                continue
            # determine baseline/with columns
            if stat in make_like:
                proxy = make_like[stat]
                base_col = stat_to_base_per36.get(proxy)
                with_col = stat_to_with_per36.get(proxy)
            elif stat == "fta":
                # no explicit baseline column; infer via ftm/fta if available is too noisy, so skip
                base_col = None
                with_col = None
            else:
                base_col = stat_to_base_per36.get(stat)
                with_col = stat_to_with_per36.get(stat)

            if not base_col or not with_col:
                continue
            if base_col not in base_idx.columns or with_col not in trows.columns:
                continue

            try:
                base_val = float(pd.to_numeric(base_idx.at[pid, base_col], errors="coerce"))
            except Exception:
                base_val = float("nan")
            if not np.isfinite(base_val) or base_val <= 1e-9:
                continue

            # Weighted mean of (with/base) over expected teammates
            w_sum = 0.0
            r_sum = 0.0
            for tid, w in teammates:
                if w <= 0:
                    continue
                rr = trows[trows["teammate_id"] == tid]
                if rr.empty:
                    continue
                v = pd.to_numeric(rr.iloc[0].get(with_col), errors="coerce")
                v = float(v) if v is not None and np.isfinite(v) else float("nan")
                if not np.isfinite(v):
                    continue
                r = float(v) / float(base_val)
                if not np.isfinite(r):
                    continue
                w_sum += float(w)
                r_sum += float(w) * float(r)
            if w_sum <= 0:
                continue
            mult = float(r_sum / w_sum)
            # Tight caps: we want nudges, not rewrites.
            mult = float(np.clip(mult, 0.85, 1.15))
            if np.isfinite(mult):
                max_abs_mult_delta = max(float(max_abs_mult_delta), float(abs(mult - 1.0)))

            cur = pd.to_numeric(out.at[i, prior_col], errors="coerce")
            cur = float(cur) if cur is not None and np.isfinite(cur) else None
            if cur is None:
                continue
            new_v = float(max(0.0, cur * mult))
            if abs(new_v - cur) > 1e-12:
                nudges += 1
            out.at[i, prior_col] = new_v

    out = out.drop(columns=[c for c in ["_pkey"] if c in out.columns])
    return _set_diag(out, attempted=True, applied=(nudges > 0), mapped_players=mapped_n, nudges=int(nudges), max_abs_mult_delta=float(max_abs_mult_delta))


def _load_rotation_first_sub_priors() -> dict[str, dict[str, Any]]:
    """Load team-level first bench sub-in timing priors (seconds elapsed in Q1).

    File is written by nba_betting.rotation_priors.write_rotation_priors().

    Returns a dict keyed by team tricode. Values contain:
      - elapsed_sec_mean (float)
      - top_enter_player_name (str | None)
      - top_enter_share (float | None)
    """
    p = paths.data_processed / "rotation_priors_first_bench_sub_in.csv"
    if not p.exists():
        return {}
    try:
        df = pd.read_csv(p)
        if df is None or df.empty:
            return {}
        if "team" not in df.columns or "elapsed_sec_mean" not in df.columns:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for _, r in df.iterrows():
            team = str(r.get("team") or "").strip().upper()
            v = pd.to_numeric(r.get("elapsed_sec_mean"), errors="coerce")
            if not team or not np.isfinite(v):
                continue

            top_name = None
            try:
                tn = str(r.get("top_enter_player_name") or "").strip()
                if tn:
                    top_name = tn
            except Exception:
                top_name = None

            top_share = pd.to_numeric(r.get("top_enter_share"), errors="coerce")
            top_share_f = float(top_share) if top_share is not None and np.isfinite(top_share) else None

            out[team] = {
                "elapsed_sec_mean": float(v),
                "top_enter_player_name": top_name,
                "top_enter_share": top_share_f,
            }
        return out
    except Exception:
        return {}


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
    return normalize_player_name_key(x, case="upper")


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


def _pick_minutes_col(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    """Pick the best minutes column based on data availability.

    Many processed props files include a 'pred_min' column that can be entirely blank.
    Prefer the column with the most finite, positive values rather than the first one.
    """
    if df is None or df.empty:
        return None
    best = None
    best_n = -1
    best_sum = -1.0
    for c in candidates:
        if c not in df.columns:
            continue
        try:
            v = pd.to_numeric(df[c], errors="coerce")
            ok = v[np.isfinite(v) & (v > 0)]
            n = int(ok.shape[0])
            s = float(ok.sum()) if n > 0 else 0.0
        except Exception:
            n = 0
            s = 0.0
        if (n > best_n) or (n == best_n and s > best_sum):
            best = c
            best_n = n
            best_sum = s
    return best


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
    player_priors: Optional[Any] = None,
    minutes_lookback_days: int = 21,
    n_samples: int = 1500,
    seed: Optional[int] = None,
    target_quarters: Optional[List[Dict[str, Any]]] = None,
    target_home_score: Optional[float] = None,
    target_away_score: Optional[float] = None,
    rotation_priors: Optional[Dict[str, float]] = None,
    date_str: Optional[str] = None,
    use_lineup_teammate_effects: bool = True,
    use_event_level_sim: bool = False,
    hist_exp_blend_alpha: float = 0.0,
    hist_exp_blend_max_cov: float = 0.67,
    coach_rotation_alpha: float = 0.0,
    rotation_shock_alpha: float = 0.0,
    garbage_time_alpha: float = 0.0,
    correlated_scoring_alpha: float = 0.0,
    foul_trouble_alpha: float = 0.0,
    guardrail_priors: Optional[Dict[str, Any]] = None,
    guardrail_alpha: float = 0.0,
    guardrail_max_scale: float = 0.10,
) -> Dict[str, Any]:
    """Connected simulation: quarter team points + player box scores share the same scoring totals.

    - Samples integer quarter scores from the quarter distribution.
    - Allocates each quarter's team points across players via a Dirichlet-multinomial driven by pred_pts/minutes.
    - Generates a representative single-game box score (median margin) and also returns means.
    """
    rng = np.random.default_rng(seed)

    # Event-level sim is explicitly opt-in. Only a boolean True (or np.bool_) enables it;
    # this prevents accidental enablement via truthy strings/ints.
    use_event_level_sim = bool(use_event_level_sim) if isinstance(use_event_level_sim, (bool, np.bool_)) else False

    # Optional pregame expected minutes + starters. If present, it becomes the highest-priority
    # minutes signal (ahead of roll mins) for connected minutes allocation.
    try:
        props_df = _merge_pregame_expected_minutes(props_df, date_str=date_str)
    except Exception:
        pass

    # Also load the expected-minutes artifact for use when roster restriction/augmentation
    # rebuilds players not present in the props pool.
    expected_minutes_art = pd.DataFrame()
    try:
        ds = str(date_str or "").strip()
        if ds:
            candidate_dirs = [
                paths.data_processed,
                paths.data_processed / "_bak_expected_minutes_eval",
                paths.data_processed / "_bak_expected_minutes",
            ]
            for base in candidate_dirs:
                try:
                    p_csv = base / f"pregame_expected_minutes_{ds}.csv"
                    p_pq = base / f"pregame_expected_minutes_{ds}.parquet"
                    expected_minutes_art = _read_processed_any(p_pq, p_csv)
                    if isinstance(expected_minutes_art, pd.DataFrame) and not expected_minutes_art.empty:
                        break
                except Exception:
                    continue

        if isinstance(expected_minutes_art, pd.DataFrame) and not expected_minutes_art.empty:
            expected_minutes_art = expected_minutes_art.copy()
            # Normalize team
            team_col = None
            for c in ("team_tri", "team", "team_abbr", "team_tricode"):
                if c in expected_minutes_art.columns:
                    team_col = c
                    break
            if team_col:
                expected_minutes_art["team"] = expected_minutes_art[team_col].astype(str).str.upper().str.strip()
            else:
                expected_minutes_art["team"] = None

            # Normalize name key
            name_col = None
            for c in ("player_name", "name", "PLAYER_NAME"):
                if c in expected_minutes_art.columns:
                    name_col = c
                    break
            if name_col:
                expected_minutes_art["_pkey"] = expected_minutes_art[name_col].astype(str).map(_norm_player_key)
            else:
                expected_minutes_art["_pkey"] = ""

            # Canonicalize expected-minutes columns (best-effort)
            col_map = {}
            for src, dst in (
                ("exp_min_mean", "exp_min_mean"),
                ("expected_min", "exp_min_mean"),
                ("expected_minutes", "exp_min_mean"),
                ("proj_min", "exp_min_mean"),
                ("exp_min", "exp_min_mean"),
                ("exp_min_sd", "exp_min_sd"),
                ("expected_min_sd", "exp_min_sd"),
                ("exp_min_cap", "exp_min_cap"),
                ("expected_min_cap", "exp_min_cap"),
                ("is_starter", "is_starter"),
                ("starter", "is_starter"),
                ("asof_ts", "exp_asof_ts"),
                ("report_ts", "exp_asof_ts"),
                ("source", "exp_min_source"),
            ):
                if src in expected_minutes_art.columns and dst not in expected_minutes_art.columns:
                    col_map[src] = dst
            if col_map:
                expected_minutes_art = expected_minutes_art.rename(columns=col_map)
            for c in ("exp_min_mean", "exp_min_sd", "exp_min_cap", "is_starter", "exp_asof_ts", "exp_min_source"):
                if c not in expected_minutes_art.columns:
                    expected_minutes_art[c] = None

            try:
                expected_minutes_art["_asof_dt"] = pd.to_datetime(expected_minutes_art.get("exp_asof_ts"), errors="coerce")
            except Exception:
                expected_minutes_art["_asof_dt"] = pd.NaT

            # Keep latest per (team, player_key)
            try:
                expected_minutes_art = expected_minutes_art.sort_values(["_asof_dt"], ascending=True)
            except Exception:
                pass
            expected_minutes_art = expected_minutes_art.drop_duplicates(subset=["team", "_pkey"], keep="last")
    except Exception:
        expected_minutes_art = pd.DataFrame()

    rotation_first_sub = rotation_priors if isinstance(rotation_priors, dict) else None
    if rotation_first_sub is None:
        rotation_first_sub = _load_rotation_first_sub_priors()

    def _round_quarters_to_total(q_floats: List[float], target_total: Optional[int] = None) -> List[int]:
        vals = [max(0.0, float(x or 0.0)) for x in (q_floats or [])]
        if len(vals) < 4:
            vals = vals + [0.0] * (4 - len(vals))
        vals = vals[:4]
        total_target = int(round(float(np.sum(vals)))) if target_total is None else int(target_total)
        floors = [int(np.floor(v)) for v in vals]
        rema = [vals[i] - floors[i] for i in range(4)]
        out = floors[:]
        cur = int(sum(out))
        # Add points to highest remainders
        if cur < total_target:
            need = total_target - cur
            order = sorted(range(4), key=lambda i: rema[i], reverse=True)
            k = 0
            while need > 0:
                out[order[k % 4]] += 1
                need -= 1
                k += 1
        # Remove points from lowest remainders (but don't go negative)
        if cur > total_target:
            need = cur - total_target
            order = sorted(range(4), key=lambda i: rema[i])
            k = 0
            while need > 0 and k < 1000:
                i = order[k % 4]
                if out[i] > 0:
                    out[i] -= 1
                    need -= 1
                k += 1
        return [max(0, int(x)) for x in out]

    def _extract_target_quarters(rows: Optional[List[Dict[str, Any]]]) -> Tuple[Optional[List[float]], Optional[List[float]]]:
        if not rows:
            return None, None
        try:
            by_q = {int((r or {}).get("q") or 0): (r or {}) for r in rows if isinstance(r, dict)}
            h = []
            a = []
            for qi in (1, 2, 3, 4):
                rr = by_q.get(qi) or {}
                h.append(float(rr.get("home") or 0.0))
                a.append(float(rr.get("away") or 0.0))
            return h, a
        except Exception:
            return None, None

    t_home_qf, t_away_qf = _extract_target_quarters(target_quarters)
    t_home_total = None
    t_away_total = None
    if t_home_qf is not None and t_away_qf is not None:
        try:
            t_home_total = int(round(float(target_home_score))) if target_home_score is not None else int(round(float(np.sum(t_home_qf))))
            t_away_total = int(round(float(target_away_score))) if target_away_score is not None else int(round(float(np.sum(t_away_qf))))
        except Exception:
            t_home_total = None
            t_away_total = None

    home_q, away_q = sample_quarter_scores(quarters, n_samples=int(n_samples), rng=rng, round_to_int=True)

    # Optional: correlated scoring variance.
    # Add a per-sample latent factor that:
    #   - Moves both teams in the same direction (shared "environment": pace/whistle/shooting)
    #   - Moves teams in opposite directions (relative "strength": one side runs hot, the other cold)
    # This increases tails and induces realistic intra-game correlation while preserving the mean
    # (lognormal with mean 1.0).
    try:
        corr_alpha = float(correlated_scoring_alpha or 0.0)
    except Exception:
        corr_alpha = 0.0
    corr_alpha = float(np.clip(corr_alpha, 0.0, 1.0))

    scoring_corr_diag: Dict[str, Any] = {
        "enabled": bool(corr_alpha > 0.0),
        "alpha": float(corr_alpha),
        "env_sigma": 0.0,
        "rel_sigma": 0.0,
        "home_mult_mean": None,
        "away_mult_mean": None,
        "home_mult_std": None,
        "away_mult_std": None,
        "clipped_frac": None,
    }

    # Optional: model guardrails (soft anchoring of quarter samples to model priors).
    # Default is OFF (alpha=0.0). When enabled, this gently scales the sampled quarter
    # points so the aggregate mean (and optionally per-quarter means) do not drift far
    # from the model priors provided by the caller.
    guard_diag: Dict[str, Any] = {
        "enabled": False,
        "alpha": None,
        "max_scale": None,
        "mode": None,
        "priors": {},
        "pre": {},
        "post": {},
        "scales": {},
        "warnings": [],
    }
    try:
        gr_alpha = float(guardrail_alpha or 0.0)
    except Exception:
        gr_alpha = 0.0
    gr_alpha = float(np.clip(gr_alpha, 0.0, 1.0))
    try:
        gr_max_scale = float(guardrail_max_scale if guardrail_max_scale is not None else 0.10)
    except Exception:
        gr_max_scale = 0.10
    gr_max_scale = float(np.clip(gr_max_scale, 0.0, 0.50))
    priors = guardrail_priors if isinstance(guardrail_priors, dict) else None
    guard_diag["alpha"] = float(gr_alpha)
    guard_diag["max_scale"] = float(gr_max_scale)

    def _gr_warn(msg: str) -> None:
        try:
            if msg and msg not in (guard_diag.get("warnings") or []):
                (guard_diag["warnings"] if isinstance(guard_diag.get("warnings"), list) else []).append(msg)
        except Exception:
            pass

    def _pick_prior_num(d: Dict[str, Any], keys: Tuple[str, ...]) -> float | None:
        for k in keys:
            try:
                v = _to_num(d.get(k))
                if v is not None and np.isfinite(float(v)):
                    return float(v)
            except Exception:
                continue
        return None

    def _solve_home_away_targets(
        pre_home_mu: float,
        pre_away_mu: float,
        total_tgt: float | None,
        margin_tgt: float | None,
    ) -> Tuple[float | None, float | None]:
        # Convert (total, margin) → (home, away). If one is missing, preserve the
        # pre-sample split as much as possible.
        try:
            pre_total = float(pre_home_mu + pre_away_mu)
            if total_tgt is not None and np.isfinite(float(total_tgt)):
                t = float(total_tgt)
            else:
                t = float(pre_total)
            if margin_tgt is not None and np.isfinite(float(margin_tgt)):
                m = float(margin_tgt)
                h = 0.5 * (t + m)
                a = 0.5 * (t - m)
            else:
                # Preserve home share of total from the sampled distribution.
                h_share = float(pre_home_mu / max(1e-6, pre_total)) if pre_total > 0 else 0.5
                h_share = float(np.clip(h_share, 0.05, 0.95))
                h = float(t * h_share)
                a = float(max(0.0, t - h))
            if not np.isfinite(h) or not np.isfinite(a) or h < 0 or a < 0:
                return None, None
            return float(h), float(a)
        except Exception:
            return None, None

    def _clip_scale(ratio: float) -> float:
        try:
            r = float(ratio)
            if not np.isfinite(r) or r <= 0:
                return 1.0
            lo = 1.0 - float(gr_max_scale)
            hi = 1.0 + float(gr_max_scale)
            return float(np.clip(r, lo, hi))
        except Exception:
            return 1.0

    def _scale_quarters_int(q_int: np.ndarray, mult: np.ndarray) -> np.ndarray:
        q_int = np.asarray(q_int, dtype=int)
        mult = np.asarray(mult, dtype=float)
        if q_int.ndim != 2 or q_int.shape[0] == 0:
            return q_int
        n_samp = int(q_int.shape[0])
        n_q = int(q_int.shape[1])
        out = np.zeros_like(q_int, dtype=int)
        for i in range(n_samp):
            m = float(mult[i]) if i < int(mult.size) else 1.0
            if not np.isfinite(m) or m <= 0:
                out[i] = q_int[i]
                continue
            qf = np.maximum(0.0, q_int[i].astype(float) * m)
            tgt = int(round(float(np.sum(qf))))
            floors = np.floor(qf).astype(int)
            rema = qf - floors
            vals = floors.copy()
            cur = int(np.sum(vals))
            if cur < tgt:
                need = int(tgt - cur)
                order = np.argsort(-rema)
                k = 0
                while need > 0 and k < 2000:
                    vals[int(order[k % n_q])] += 1
                    need -= 1
                    k += 1
            elif cur > tgt:
                need = int(cur - tgt)
                order = np.argsort(rema)
                k = 0
                while need > 0 and k < 2000:
                    j = int(order[k % n_q])
                    if vals[j] > 0:
                        vals[j] -= 1
                        need -= 1
                    k += 1
            out[i] = np.maximum(0, vals)
        return out

    if corr_alpha > 0.0:
        try:
            n0 = int(home_q.shape[0])
            # Small sigmas: keep the feature safe by default.
            env_sigma = float(0.060 * corr_alpha)
            rel_sigma = float(0.045 * corr_alpha)
            scoring_corr_diag["env_sigma"] = float(env_sigma)
            scoring_corr_diag["rel_sigma"] = float(rel_sigma)

            z_env = rng.normal(0.0, 1.0, size=n0)
            z_rel = rng.normal(0.0, 1.0, size=n0)

            env = np.exp(env_sigma * z_env - 0.5 * (env_sigma**2))
            rel = np.exp(rel_sigma * z_rel - 0.5 * (rel_sigma**2))
            home_mult = env * rel
            away_mult = env / np.maximum(1e-9, rel)

            # Guardrails against pathological multipliers.
            lo, hi = 0.80, 1.25
            home_mult_c = np.clip(home_mult, lo, hi)
            away_mult_c = np.clip(away_mult, lo, hi)
            clipped_frac = float(
                np.mean((home_mult != home_mult_c) | (away_mult != away_mult_c))
            )

            home_q = _scale_quarters_int(home_q, home_mult_c)
            away_q = _scale_quarters_int(away_q, away_mult_c)

            scoring_corr_diag["home_mult_mean"] = float(np.mean(home_mult_c))
            scoring_corr_diag["away_mult_mean"] = float(np.mean(away_mult_c))
            scoring_corr_diag["home_mult_std"] = float(np.std(home_mult_c))
            scoring_corr_diag["away_mult_std"] = float(np.std(away_mult_c))
            scoring_corr_diag["clipped_frac"] = float(clipped_frac)
        except Exception:
            pass

    # Apply guardrails after correlated-scoring variance so we anchor the final
    # scoring environment (still before representative sample selection).
    if gr_alpha > 0.0 and priors and isinstance(home_q, np.ndarray) and isinstance(away_q, np.ndarray):
        try:
            guard_diag["enabled"] = True

            # Pre means
            pre_hq_mu = np.mean(home_q, axis=0).astype(float) if home_q.size else np.zeros(4, dtype=float)
            pre_aq_mu = np.mean(away_q, axis=0).astype(float) if away_q.size else np.zeros(4, dtype=float)
            pre_home_mu = float(np.sum(pre_hq_mu))
            pre_away_mu = float(np.sum(pre_aq_mu))
            guard_diag["pre"] = {
                "home_mu": float(pre_home_mu),
                "away_mu": float(pre_away_mu),
                "total_mu": float(pre_home_mu + pre_away_mu),
                "margin_mu": float(pre_home_mu - pre_away_mu),
                "home_q_mu": [float(x) for x in list(pre_hq_mu)],
                "away_q_mu": [float(x) for x in list(pre_aq_mu)],
            }

            # Extract priors
            pred_total = _pick_prior_num(priors, ("pred_total", "totals", "total_pred"))
            pred_margin = _pick_prior_num(priors, ("pred_margin", "spread_margin", "margin_pred"))
            q_totals = []
            q_margins = []
            for qi in (1, 2, 3, 4):
                q_totals.append(_pick_prior_num(priors, (f"quarters_q{qi}_total", f"q{qi}_total", f"quarters_{'q'+str(qi)}_total")))
                q_margins.append(_pick_prior_num(priors, (f"quarters_q{qi}_margin", f"q{qi}_margin", f"quarters_{'q'+str(qi)}_margin")))

            guard_diag["priors"] = {
                "pred_total": float(pred_total) if pred_total is not None else None,
                "pred_margin": float(pred_margin) if pred_margin is not None else None,
                "quarters_total": [float(x) if x is not None else None for x in q_totals],
                "quarters_margin": [float(x) if x is not None else None for x in q_margins],
            }

            have_any_q = any(x is not None for x in q_totals) or any(x is not None for x in q_margins)
            if have_any_q:
                guard_diag["mode"] = "quarters"
                scales_h = []
                scales_a = []
                for j in range(4):
                    pre_h = float(pre_hq_mu[j])
                    pre_a = float(pre_aq_mu[j])
                    h_tgt, a_tgt = _solve_home_away_targets(pre_h, pre_a, q_totals[j], q_margins[j])

                    # If only some quarter priors are present, optionally backfill totals/margins from full-game priors.
                    if (h_tgt is None or a_tgt is None) and (pred_total is not None or pred_margin is not None):
                        try:
                            pre_t = float(pre_h + pre_a)
                            pre_T = float(pre_home_mu + pre_away_mu)
                            share = float(pre_t / max(1e-6, pre_T)) if pre_T > 0 else 0.25
                            total_fb = float(pred_total * share) if pred_total is not None else None
                            # margin share can be negative; scale similarly by absolute share.
                            margin_fb = float(pred_margin * share) if pred_margin is not None else None
                            h_tgt, a_tgt = _solve_home_away_targets(pre_h, pre_a, total_fb, margin_fb)
                        except Exception:
                            h_tgt, a_tgt = None, None

                    if h_tgt is None or a_tgt is None:
                        scales_h.append(1.0)
                        scales_a.append(1.0)
                        continue

                    # Blend the target toward the prior.
                    h_blend = float((1.0 - gr_alpha) * pre_h + gr_alpha * float(h_tgt))
                    a_blend = float((1.0 - gr_alpha) * pre_a + gr_alpha * float(a_tgt))

                    sh = _clip_scale(h_blend / max(1e-6, pre_h)) if pre_h > 0 else 1.0
                    sa = _clip_scale(a_blend / max(1e-6, pre_a)) if pre_a > 0 else 1.0
                    scales_h.append(float(sh))
                    scales_a.append(float(sa))

                    home_q[:, j] = np.maximum(0, np.rint(home_q[:, j].astype(float) * float(sh))).astype(int)
                    away_q[:, j] = np.maximum(0, np.rint(away_q[:, j].astype(float) * float(sa))).astype(int)

                guard_diag["scales"] = {"home_q": [float(x) for x in scales_h], "away_q": [float(x) for x in scales_a]}
            elif pred_total is not None or pred_margin is not None:
                guard_diag["mode"] = "game"
                pre_home = float(pre_home_mu)
                pre_away = float(pre_away_mu)
                h_tgt, a_tgt = _solve_home_away_targets(pre_home, pre_away, pred_total, pred_margin)
                if h_tgt is not None and a_tgt is not None and pre_home > 0 and pre_away > 0:
                    h_blend = float((1.0 - gr_alpha) * pre_home + gr_alpha * float(h_tgt))
                    a_blend = float((1.0 - gr_alpha) * pre_away + gr_alpha * float(a_tgt))
                    sh = _clip_scale(h_blend / max(1e-6, pre_home))
                    sa = _clip_scale(a_blend / max(1e-6, pre_away))
                    home_q = _scale_quarters_int(home_q, np.full(int(home_q.shape[0]), float(sh)))
                    away_q = _scale_quarters_int(away_q, np.full(int(away_q.shape[0]), float(sa)))
                    guard_diag["scales"] = {"home": float(sh), "away": float(sa)}
                else:
                    _gr_warn("guardrails: could not compute valid home/away targets")
            else:
                guard_diag["mode"] = "none"

            # Post means
            post_hq_mu = np.mean(home_q, axis=0).astype(float) if home_q.size else np.zeros(4, dtype=float)
            post_aq_mu = np.mean(away_q, axis=0).astype(float) if away_q.size else np.zeros(4, dtype=float)
            post_home_mu = float(np.sum(post_hq_mu))
            post_away_mu = float(np.sum(post_aq_mu))
            guard_diag["post"] = {
                "home_mu": float(post_home_mu),
                "away_mu": float(post_away_mu),
                "total_mu": float(post_home_mu + post_away_mu),
                "margin_mu": float(post_home_mu - post_away_mu),
                "home_q_mu": [float(x) for x in list(post_hq_mu)],
                "away_q_mu": [float(x) for x in list(post_aq_mu)],
            }

        except Exception as e:
            guard_diag["enabled"] = False
            guard_diag["mode"] = "error"
            _gr_warn(f"guardrails error: {e}")
    n = int(home_q.shape[0])
    if n == 0:
        return {"error": "no samples"}

    home_final = home_q.sum(axis=1)
    away_final = away_q.sum(axis=1)
    margin = home_final - away_final

    total = home_final + away_final

    # Optional: garbage-time/blowout minutes behavior.
    # Use the sampled final-margin distribution to estimate blowout likelihood.
    try:
        gt_alpha = float(garbage_time_alpha or 0.0)
    except Exception:
        gt_alpha = 0.0
    gt_alpha = float(np.clip(gt_alpha, 0.0, 1.0))
    blow_thr = 18.0
    try:
        m = margin.astype(float)
        p_abs_blow = float(np.mean(np.abs(m) >= blow_thr)) if m.size else 0.0
        p_home_win_big = float(np.mean(m >= blow_thr)) if m.size else 0.0
        p_away_win_big = float(np.mean(m <= -blow_thr)) if m.size else 0.0
    except Exception:
        p_abs_blow = 0.0
        p_home_win_big = 0.0
        p_away_win_big = 0.0

    # Pick a representative sample: if a target line is provided, choose a sample close to
    # that target total/margin; otherwise use near-median margin AND near-median total.
    # Also avoid exact ties (NBA games cannot end tied without OT).
    try:
        if t_home_total is not None and t_away_total is not None:
            tgt_m = float(t_home_total - t_away_total)
            tgt_t = float(t_home_total + t_away_total)
            score = (margin - tgt_m) ** 2 + 0.25 * (total - tgt_t) ** 2
        else:
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
        # We prefer trusting the roster (especially in evaluator/backtests) over props_df,
        # because props pools can contain stale/traded players that would otherwise steal minutes.
        try:
            roster_names = [(_norm_player_key(x), _norm_name(x)) for x in (roster or []) if _norm_player_key(x)]
            if roster_names and ("player_name" in out.columns) and (not out.empty):
                allowed = set(k for k, _ in roster_names)
                before = int(len(out))
                filtered = out[out["player_name"].map(_norm_player_key).isin(allowed)].copy()
                after = int(len(filtered))
                # If we matched anyone, keep only matched roster players.
                # If we matched nobody but roster is non-trivial, drop the props pool entirely;
                # roster expansion below will rebuild a plausible rotation.
                if after > 0:
                    out = filtered
                elif int(len(allowed)) >= 8:
                    out = out.head(0).copy()
        except Exception:
            pass

        # Record basic roster/pool diagnostics (used later for UI warnings).
        try:
            roster_keys = [(_norm_player_key(x), _norm_name(x) or str(x)) for x in (roster or []) if _norm_player_key(x)]
            roster_allowed = set(k for k, _ in roster_keys)
            pool_keys = set(out["player_name"].map(_norm_player_key).tolist()) if ("player_name" in out.columns and not out.empty) else set()
            missing = [disp for k, disp in roster_keys if k not in pool_keys]
            out.attrs["_roster_n"] = int(len(roster_allowed))
            out.attrs["_pool_n"] = int(len(pool_keys))
            out.attrs["_roster_missing_n"] = int(len(missing))
            out.attrs["_roster_missing_sample"] = [str(x) for x in (missing[:12] if missing else [])]
        except Exception:
            pass

        # Deduplicate: keep the most-relevant row per player (highest minutes signal, then pred_pts)
        if not out.empty and "player_name" in out.columns:
            try:
                out = out.copy()
                out["_player_norm"] = out["player_name"].map(_norm_player_key)
                # Pick best minutes feature available (based on non-empty data).
                # Important: do NOT use exp_min_mean here, because expected-minutes columns
                # (especially low-trust backfills) can perturb which duplicate row we keep.
                mins_col = _pick_minutes_col(out, ("roll5_min", "roll10_min", "roll20_min", "roll30_min", "pred_min"))
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

        # Expand with roster players (no placeholders).
        # Important: roster players may be missing from team-keyed priors (e.g., recent trades).
        # In that case, fall back to a team-agnostic minutes prior by player key.
        try:
            pri = minutes_priors or {}
            team_u = str(team or "").strip().upper()
            explicit_roster = bool(roster)
            roster_names = [(_norm_player_key(x), _norm_name(x)) for x in (roster or []) if _norm_player_key(x)]

            # Build a team-agnostic minutes prior map for robustness (trades, team-code mismatches).
            # Use a mean across teams (not max) to avoid inflating newly-added roster players
            # from their previous team context.
            pri_any: dict[str, float] = {}
            try:
                pri_sum: dict[str, float] = {}
                pri_cnt: dict[str, int] = {}
                for (t, nm), m in pri.items():
                    key = _norm_player_key(nm)
                    mm = _to_num(m)
                    if not key or mm is None or mm <= 0:
                        continue
                    pri_sum[key] = float(pri_sum.get(key, 0.0)) + float(mm)
                    pri_cnt[key] = int(pri_cnt.get(key, 0)) + 1
                for k, s in pri_sum.items():
                    c = int(pri_cnt.get(k, 0))
                    if c > 0:
                        pri_any[k] = float(s) / float(c)
            except Exception:
                pri_any = {}

            # If roster isn't available, derive a pseudo-roster from minutes priors for this team.
            # This prevents inflating a tiny player pool up to 240 minutes.
            if (not roster_names) and pri:
                try:
                    cand: list[tuple[str, str, float]] = []
                    for (t, nm), m in pri.items():
                        if str(t).strip().upper() != team_u:
                            continue
                        mm = _to_num(m)
                        if mm is None or mm <= 0:
                            continue
                        key = _norm_player_key(nm)
                        if not key:
                            continue
                        cand.append((key, str(nm), float(mm)))
                    if cand:
                        cand.sort(key=lambda x: x[2], reverse=True)
                        roster_names = [(k, _norm_name(disp) or disp) for k, disp, _ in cand[:14]]
                except Exception:
                    pass

            # If roster exists but is undersized, augment it from priors.
            # IMPORTANT: only do this when an explicit roster was NOT provided.
            # In evaluator/backtests, explicit rosters come from actual logs and should be treated
            # as authoritative; augmenting from priors can introduce non-participating players
            # that steal minutes and break realism scoring.
            try:
                min_roster_target = 10
                if (not explicit_roster) and pri and roster_names and int(len(roster_names)) < min_roster_target:
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
                    out["_prior_min"] = out["player_name"].map(
                        lambda nm: (pri.get((team_u, _norm_player_key(nm))) if pri else None) or (pri_any.get(_norm_player_key(nm)) if pri_any else None)
                    )
                except Exception:
                    pass

            # Attach broader player priors (rates) if provided.
            try:
                pp = None
                if isinstance(player_priors, dict):
                    pp = player_priors
                else:
                    try:
                        # evaluator may pass PlayerPriors dataclass; use its `.rates` dict
                        pp = getattr(player_priors, "rates", None)
                    except Exception:
                        pp = None

                if (not out.empty) and ("player_name" in out.columns) and pp:
                    out = out.copy()
                    out["_pkey"] = out["player_name"].map(_norm_player_key)
                    def _p(team_key: str, player_key: str, k: str) -> Optional[float]:
                        try:
                            v = (pp.get((team_key, player_key)) or {}).get(k)
                            return float(v) if v is not None and np.isfinite(float(v)) else None
                        except Exception:
                            return None
                    # Store per-minute rates; totals are computed later using sim minutes.
                    for stat in (
                        "pts",
                        "reb",
                        "ast",
                        "threes",
                        "threes_att",
                        "tov",
                        "stl",
                        "blk",
                        "fga",
                        "fgm",
                        "fta",
                        "ftm",
                        "pf",
                    ):
                        out[f"_prior_{stat}_pm"] = out["_pkey"].map(lambda pk: _p(team_u, pk, f"{stat}_pm"))
                    out["_prior_min_mu"] = out["_pkey"].map(lambda pk: _p(team_u, pk, "min_mu"))
                    out = out.drop(columns=["_pkey"])
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
                    m = _to_num((pri.get((team_u, key_norm)) if pri else None) or (pri_any.get(key_norm) if pri_any else None))
                    # If we have no minutes prior at all, still include the player with a conservative
                    # minutes signal so they don't vanish from the pool.
                    if m is None or m <= 0:
                        m = 2.0
                    additions.append(
                        {
                            "player_name": disp,
                            "team": team_u,
                            # Provide a minutes signal so _attach_sim_minutes can normalize.
                            # Prefer roll-based minutes selection downstream, so seed roll mins too.
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

            # After roster augmentation, attach priors for any newly-added players.
            # (The earlier attachment only covered props-derived players.)
            try:
                if (not out.empty) and ("player_name" in out.columns) and pri:
                    out = out.copy()
                    if "_prior_min" not in out.columns:
                        out["_prior_min"] = None
                    mins_map = out["player_name"].map(
                        lambda nm: (pri.get((team_u, _norm_player_key(nm))) if pri else None) or (pri_any.get(_norm_player_key(nm)) if pri_any else None)
                    )
                    out["_prior_min"] = pd.to_numeric(out["_prior_min"], errors="coerce")
                    out["_prior_min"] = out["_prior_min"].where(out["_prior_min"].notna(), mins_map)
            except Exception:
                pass

            try:
                pp = None
                if isinstance(player_priors, dict):
                    pp = player_priors
                else:
                    try:
                        pp = getattr(player_priors, "rates", None)
                    except Exception:
                        pp = None

                if (not out.empty) and ("player_name" in out.columns) and pp:
                    out = out.copy()
                    out["_pkey"] = out["player_name"].map(_norm_player_key)

                    def _p(team_key: str, player_key: str, k: str) -> Optional[float]:
                        try:
                            v = (pp.get((team_key, player_key)) or {}).get(k)
                            return float(v) if v is not None and np.isfinite(float(v)) else None
                        except Exception:
                            return None

                    for stat in (
                        "pts",
                        "reb",
                        "ast",
                        "threes",
                        "threes_att",
                        "tov",
                        "stl",
                        "blk",
                        "fga",
                        "fgm",
                        "fta",
                        "ftm",
                        "pf",
                    ):
                        col = f"_prior_{stat}_pm"
                        if col not in out.columns:
                            out[col] = None
                        vals = out["_pkey"].map(lambda pk: _p(team_u, pk, f"{stat}_pm"))
                        cur = pd.to_numeric(out[col], errors="coerce")
                        out[col] = cur.where(cur.notna(), vals)

                    if "_prior_min_mu" not in out.columns:
                        out["_prior_min_mu"] = None
                    vals = out["_pkey"].map(lambda pk: _p(team_u, pk, "min_mu"))
                    cur = pd.to_numeric(out["_prior_min_mu"], errors="coerce")
                    out["_prior_min_mu"] = cur.where(cur.notna(), vals)

                    out = out.drop(columns=["_pkey"], errors="ignore")
            except Exception:
                pass

            # After roster augmentation, attach expected minutes for any players missing it.
            # This matters when the props pool is empty/incorrect for the roster and we rebuild
            # the rotation from actual logs/priors.
            try:
                expa = expected_minutes_art
                if isinstance(expa, pd.DataFrame) and (not expa.empty) and (not out.empty) and ("player_name" in out.columns) and ("team" in out.columns):
                    out = out.copy()
                    out["_pkey"] = out["player_name"].map(_norm_player_key)
                    exp_cols = [
                        "team",
                        "_pkey",
                        "exp_min_mean",
                        "exp_min_sd",
                        "exp_min_cap",
                        "is_starter",
                        "exp_asof_ts",
                        "exp_min_source",
                    ]
                    expx = expa[[c for c in exp_cols if c in expa.columns]].copy()
                    expx = expx[(expx.get("team").astype(str).str.len() > 0) & (expx.get("_pkey").astype(str).str.len() > 0)].copy()
                    out = out.merge(expx, on=["team", "_pkey"], how="left", suffixes=("", "_exp"))
                    for c in ("exp_min_mean", "exp_min_sd", "exp_min_cap", "is_starter", "exp_asof_ts", "exp_min_source"):
                        ce = f"{c}_exp"
                        if c not in out.columns and ce in out.columns:
                            out[c] = out[ce]
                        elif c in out.columns and ce in out.columns:
                            cur = out[c]
                            add = out[ce]
                            out[c] = cur.where(pd.to_numeric(cur, errors="coerce").notna(), add)
                    out = out.drop(columns=[c for c in out.columns if c.endswith("_exp")], errors="ignore")
                    out = out.drop(columns=["_pkey"], errors="ignore")
            except Exception:
                pass
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
            "minutes_prior_coverage": None,
            "minutes_prior_divergence": None,
            "minutes_expected_coverage": None,
            "minutes_expected_asof_max": None,
            "rotation_first_sub_elapsed_sec_mean": None,
            "rotation_first_sub_top_enter_name": None,
            "rotation_first_sub_top_enter_share": None,
            "rotation_bench_anchor_applied": False,
            "rotation_bench_anchor_mult": None,
            "rotation_bench_shape_applied": False,
            "rotation_bench_shape_p": None,
            "rotation_starter_share_target": None,
            "rotation_starter_share_before": None,
            "rotation_starter_share_after": None,
            "coach_rotation_alpha": None,
            "rotation_shock_detected": False,
            "rotation_shock_divergence": None,
            "rotation_shock_top5_overlap": None,
            "rotation_shock_alpha": None,
            "garbage_time_alpha": float(gt_alpha),
            "garbage_time_p_abs": float(p_abs_blow),
            "garbage_time_p_home_win_big": float(p_home_win_big),
            "garbage_time_p_away_win_big": float(p_away_win_big),
            "garbage_time_shift_minutes": 0.0,
            "foul_trouble_alpha": None,
            "foul_trouble_pf_coverage": None,
            "foul_trouble_p_ge5_max": None,
            "foul_trouble_shift_minutes": 0.0,
            "foul_trouble_whistle": None,
        }
        if players is None or players.empty:
            return players, diag
        players = players.copy()
        diag["players"] = int(len(players))

        # No placeholder players. If minutes signals are missing, we'll normalize whatever is available.

        # 1) Pregame expected minutes (if present) can be high-leverage, but some sources are
        #    low-trust (e.g. backfilled baselines / history-derived approximations). For those,
        #    only use it to fill players missing the primary minutes signal.
        # 2) Prefer roll-based minutes over model minutes (pred_min).
        mins_col = None
        exp_col = _pick_minutes_col(players, ("exp_min_mean", "expected_min", "expected_minutes", "proj_min", "exp_min"))
        exp_vals = None
        exp_ok_mask = None
        exp_use_mask = None
        exp_hard_mask = None
        exp_hist_mask = None
        try:
            if exp_col and exp_col in players.columns:
                exp_vals = pd.to_numeric(players.get(exp_col), errors="coerce")
                exp_ok_mask = np.isfinite(exp_vals.to_numpy(dtype=float)) & (exp_vals.to_numpy(dtype=float) > 0)
                diag["minutes_expected_coverage"] = float(int(exp_ok_mask.sum())) / max(1.0, float(len(players)))
        except Exception:
            exp_vals = None
            exp_ok_mask = None

        try:
            if "exp_asof_ts" in players.columns:
                dt = pd.to_datetime(players.get("exp_asof_ts"), errors="coerce")
                mx = dt.max()
                diag["minutes_expected_asof_max"] = str(mx) if pd.notna(mx) else None
        except Exception:
            pass

        # Choose fallback minutes column (roll-based preferred).
        roll_candidates = ("roll5_min", "roll10_min", "roll20_min", "roll30_min")
        mins_col = _pick_minutes_col(players, roll_candidates)
        if mins_col is None:
            mins_col = _pick_minutes_col(players, ("pred_min",))

        # Extra bias toward roll5_min when it has reasonable coverage.
        try:
            if "roll5_min" in players.columns:
                v5 = pd.to_numeric(players.get("roll5_min"), errors="coerce")
                ok5 = v5[np.isfinite(v5) & (v5 > 0)]
                if int(ok5.shape[0]) >= 8:
                    mins_col = "roll5_min"
        except Exception:
            pass

        fb_raw = (
            pd.to_numeric(players.get(mins_col), errors="coerce").fillna(0.0).to_numpy(dtype=float)
            if mins_col
            else np.zeros(len(players), dtype=float)
        )

        # Determine which expected-minutes rows are eligible to be used.
        # - Trusted sources: allowed to override per-player minutes signal.
        # - rotations_espn_history: treat as a soft signal (blend into roll mins).
        try:
            if exp_vals is not None and exp_ok_mask is not None:
                src = players.get("exp_min_source") if isinstance(players, pd.DataFrame) else None
                if src is not None:
                    s = src.astype(str).str.strip().str.lower().fillna("")
                    is_baseline = s.str.startswith("baseline:").to_numpy(dtype=bool)
                    is_history = s.str.contains("rotations_espn_history", regex=False).to_numpy(dtype=bool)
                    exp_hard_mask = exp_ok_mask & (~is_baseline) & (~is_history)
                    exp_hist_mask = exp_ok_mask & is_history
                else:
                    exp_hard_mask = exp_ok_mask
                    exp_hist_mask = np.zeros(int(len(players)), dtype=bool)

                exp_use_mask = exp_hard_mask
        except Exception:
            exp_use_mask = None
            exp_hard_mask = None
            exp_hist_mask = None

        # Hybrid: allow trusted sources to override per-player signals.
        # rotations_espn_history is treated as fill-only (only when the primary minutes signal
        # is missing for that player) to avoid overriding stable roll minutes.
        using_expected = bool(exp_vals is not None and exp_use_mask is not None and np.any(exp_use_mask))
        raw = fb_raw
        if exp_vals is not None and exp_ok_mask is not None:
            exp_raw = exp_vals.fillna(0.0).to_numpy(dtype=float)

            # Fill-only for rotations history: only for players missing fallback minutes.
            try:
                diag["minutes_source"] = mins_col
                if exp_hist_mask is not None and bool(np.any(exp_hist_mask)):
                    fb_ok = np.isfinite(fb_raw) & (fb_raw > 0)
                    hist_fill = exp_hist_mask & (~fb_ok)
                    if bool(np.any(hist_fill)):
                        raw = np.where(hist_fill, exp_raw, raw)
                        diag["minutes_source"] = f"{mins_col or 'none'}+{exp_col}[hist_fill]"

                    # Optional: softly blend rotations-history expected minutes into *bench* players
                    # when it meaningfully disagrees with recent-roll minutes.
                    # This is opt-in (alpha=0 by default) to avoid regressions.
                    try:
                        a = float(hist_exp_blend_alpha or 0.0)
                    except Exception:
                        a = 0.0
                    if a > 0.0:
                        a = float(np.clip(a, 0.0, 0.95))

                        # Only activate blending when expected-minutes coverage is not already high.
                        # (When coverage is high, roll mins are typically stable and blending can regress.)
                        try:
                            cov = float(diag.get("minutes_expected_coverage") or 0.0)
                        except Exception:
                            cov = 0.0
                        try:
                            max_cov = float(hist_exp_blend_max_cov if hist_exp_blend_max_cov is not None else 0.7)
                        except Exception:
                            max_cov = 0.67
                        if not np.isfinite(max_cov):
                            max_cov = 0.67
                        if not np.isfinite(cov):
                            cov = 0.0
                        if cov > max_cov:
                            # skip blend
                            pass
                        else:
                            # Only adjust players that look bench-ish by the primary minutes signal.
                            bench_cutoff = 24.0
                            # Only adjust when disagreement is material.
                            diff_cutoff = 6.0
                            exp_ok = np.isfinite(exp_raw) & (exp_raw > 0)
                            bench_like = np.isfinite(fb_raw) & (fb_raw > 0) & (fb_raw <= bench_cutoff)
                            material_diff = np.isfinite(fb_raw) & np.isfinite(exp_raw) & (np.abs(fb_raw - exp_raw) >= diff_cutoff)
                            hist_blend = exp_hist_mask & fb_ok & exp_ok & bench_like & material_diff
                            if bool(np.any(hist_blend)):
                                raw = np.where(hist_blend, (1.0 - a) * raw + a * exp_raw, raw)
                                tag = f"hist_blend_{a:.2f}".rstrip("0").rstrip(".")
                                diag["minutes_source"] = f"{diag.get('minutes_source') or (mins_col or 'none')}+{exp_col}[{tag}]"
                    
            except Exception:
                diag["minutes_source"] = mins_col

            # Hard override for trusted sources.
            if using_expected:
                raw = np.where(exp_use_mask, exp_raw, raw)
                diag["minutes_source"] = f"{exp_col}+{diag.get('minutes_source') or (mins_col or 'none')}"
        else:
            diag["minutes_source"] = mins_col

        
        # Track minutes signal coverage for fallback heuristics.
        try:
            if exp_vals is not None and exp_use_mask is not None and bool(np.any(exp_use_mask)):
                diag["minutes_signal_n"] = int(exp_use_mask.sum())
            elif mins_col and mins_col in players.columns:
                v = pd.to_numeric(players.get(mins_col), errors="coerce")
                ok = v[np.isfinite(v) & (v > 0)]
                diag["minutes_signal_n"] = int(ok.shape[0])
            else:
                diag["minutes_signal_n"] = 0
        except Exception:
            pass

        # If a team has multiple DAY-TO-DAY flags, minutes are often more uncertain.
        # Apply a very small flattening of minute shares (team-level, not per-player),
        # which nudges minutes toward the bench without hard-capping specific players.
        dtd_n = 0
        try:
            if "injury_status" in players.columns:
                st = players.get("injury_status").astype(str).str.upper().str.strip()
                dtd_n = int(st.eq("DAY-TO-DAY").sum())
        except Exception:
            dtd_n = 0

        # Build an effective prior minutes vector for this roster.
        # Prefer explicit minutes priors (_prior_min), then fill remaining gaps from broader min_mu priors.
        pri_eff = np.zeros(len(players), dtype=float)
        pri_tag = None
        try:
            if "_prior_min" in players.columns:
                pri_eff = pd.to_numeric(players.get("_prior_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                pri_tag = "prior"
        except Exception:
            pri_eff = np.zeros(len(players), dtype=float)
            pri_tag = None

        try:
            if "_prior_min_mu" in players.columns:
                pri2 = pd.to_numeric(players.get("_prior_min_mu"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                if pri_tag is None or float(np.sum(pri_eff)) <= 0.0:
                    pri_eff = pri2
                    pri_tag = "logs"
                else:
                    pri_eff = np.where(pri_eff > 0.0, pri_eff, pri2)
        except Exception:
            pass

        try:
            cov = float(np.mean((pri_eff > 0.0).astype(float))) if pri_eff.size > 0 else 0.0
            diag["minutes_prior_coverage"] = cov
        except Exception:
            pass

        # When expected-minutes coverage is sparse, we are often in a rotation-shock regime
        # (rest/injuries/blowout), where deep bench players can soak up meaningful minutes.
        # In that case, flatten minute shares a bit and boost players missing expected minutes.
        # Prefer priors (when available) for those missing players; otherwise apply a small floor.
        try:
            exp_cov = diag.get("minutes_expected_coverage")
            sig_n = int(diag.get("minutes_signal_n") or 0)
            if using_expected and exp_ok_mask is not None and exp_cov is not None:
                exp_cov_f = float(exp_cov)
                if raw.size >= 10 and sig_n >= 3 and exp_cov_f <= 0.55:
                    shortfall = max(0.0, (0.55 - exp_cov_f) / 0.55)

                    raw_pos = np.maximum(0.0, np.where(np.isfinite(raw), raw, 0.0))
                    s = float(np.sum(raw_pos))
                    if s > 0:
                        sh = raw_pos / s
                        # More aggressive flattening when coverage is very low.
                        p = float(np.clip(0.90 - 0.20 * shortfall, 0.75, 0.90))
                        sh2 = np.power(np.maximum(1e-12, sh), p)
                        sh2 = sh2 / float(np.sum(sh2))
                        raw = sh2 * s

                    miss = (~exp_ok_mask) & (np.isfinite(raw))
                    if bool(np.any(miss)):
                        raw = raw.copy()
                        # Apply a dynamic floor based on roster size to all players missing
                        # expected minutes. This prevents low priors (or low fallbacks) from
                        # starving the bench in true rotation-shock games.
                        avg = 240.0 / float(max(1, raw.size))
                        floor_uncertain = float(np.clip(max(10.0, 0.90 * avg), 10.0, 18.0))
                        raw[miss] = np.maximum(raw[miss], floor_uncertain)

                        # If we have priors for missing players, also respect them (useful
                        # for missing starters like Harden/Zubac).
                        try:
                            pri_ok = (pri_eff > 0.0) & np.isfinite(pri_eff)
                            use_pri = miss & pri_ok
                            if bool(np.any(use_pri)):
                                raw[use_pri] = np.maximum(raw[use_pri], pri_eff[use_pri])
                        except Exception:
                            pass

                    diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+exp_sparse"
        except Exception:
            pass

        exp_sparse_applied = bool(str(diag.get("minutes_source") or "").find("+exp_sparse") >= 0)

        # Rotation-shock tuning knobs (used by multiple downstream heuristics).
        try:
            alpha_rs = float(rotation_shock_alpha or 0.0)
        except Exception:
            alpha_rs = 0.0
        alpha_rs = float(np.clip(alpha_rs, 0.0, 1.0))
        diag["rotation_shock_alpha"] = float(alpha_rs)
        try:
            exp_cov = diag.get("minutes_expected_coverage")
            exp_cov_f = float(exp_cov) if exp_cov is not None and np.isfinite(float(exp_cov)) else None
        except Exception:
            exp_cov_f = None

        try:
            sig_n = int(diag.get("minutes_signal_n") or 0)
            cov = float(diag.get("minutes_prior_coverage") or 0.0)
            if dtd_n >= 3 and sig_n >= 8 and cov >= 0.80 and raw.size >= 8:
                diag["dtd_n"] = int(dtd_n)
                raw_pos = np.maximum(0.0, np.where(np.isfinite(raw), raw, 0.0))
                s = float(np.sum(raw_pos))
                if s > 0:
                    sh = raw_pos / s
                    rot_tag = None
                    top3_share = None
                    div0 = None
                    try:
                        sh_sorted = np.sort(np.maximum(0.0, np.where(np.isfinite(sh), sh, 0.0)))
                        top3_share = float(np.sum(sh_sorted[-3:]))
                        diag["minutes_top3_share"] = float(top3_share)
                        diag["minutes_top5_share"] = float(np.sum(sh_sorted[-5:]))
                    except Exception:
                        pass

                    # Pre-divergence vs priors (used for gating). If roll mins already disagree
                    # with priors, additional flattening can push minutes in the wrong direction.
                    try:
                        if pri_tag and pri_eff.size == raw.size and float(np.sum(pri_eff)) > 0.0:
                            pri_pos = np.maximum(0.0, np.where(np.isfinite(pri_eff), pri_eff, 0.0))
                            # Use the union support (raw OR priors) so this metric exists even
                            # when priors have zeros for some active players.
                            mm = (raw_pos > 0.0) | (pri_pos > 0.0)
                            if bool(np.any(mm)) and int(np.sum(mm)) >= 6:
                                rs = float(np.sum(raw_pos[mm]))
                                ps = float(np.sum(pri_pos[mm]))
                                if rs > 0 and ps > 0:
                                    raw_sh0 = raw_pos[mm] / rs
                                    pri_sh0 = pri_pos[mm] / ps
                                    div0 = float(np.mean(np.abs(raw_sh0 - pri_sh0)))
                                    diag["minutes_prior_divergence0"] = div0
                    except Exception:
                        pass
                    # Gentle flattening. Exponent < 1 boosts smaller shares.
                    # If rotation-shock conditions are met, strengthen the flattening
                    # within this single transform (avoid applying two separate power
                    # transforms which can over-flatten and regress p95/max).
                    p = 0.95
                    try:
                        if (
                            alpha_rs > 0.0
                            and (not using_expected)
                            and mins_col == "roll5_min"
                            and int(raw.size) >= 9
                            and exp_cov_f is not None
                            and exp_cov_f <= 0.60
                        ):
                            # Extra gating to avoid harming games where roll mins are already
                            # either very diffuse (flattening unnecessary) or extremely
                            # concentrated (flattening can push minutes to the wrong bench).
                            # Also require stronger injury/DTD uncertainty signal.
                            if int(dtd_n) < 4:
                                raise RuntimeError("rot_shock_skip_low_dtd")
                            if top3_share is not None:
                                if (top3_share < 0.46) or (top3_share > 0.58):
                                    raise RuntimeError("rot_shock_skip_top3_band")
                            if div0 is not None and div0 > 0.05:
                                raise RuntimeError("rot_shock_skip_div0")

                            # Stronger flattening when expected coverage is lower.
                            # Anchor at p0=0.95 when shortfall=0 so exp_cov==0.60 doesn't
                            # add any extra flatten beyond baseline.
                            shortfall = float(np.clip((0.60 - exp_cov_f) / 0.60, 0.0, 1.0))
                            diag["rotation_shock_shortfall"] = float(shortfall)
                            sf = float(np.sqrt(shortfall))
                            # Cap maximum flattening (avoid rare over-flatten tails).
                            p0 = float(np.clip(0.95 - 0.35 * sf, 0.82, 0.95))
                            p_rs = 1.0 - alpha_rs * (1.0 - p0)
                            # Only strengthen flattening beyond baseline.
                            if p_rs < (p - 1e-6):
                                p = float(p_rs)
                                diag["rotation_shock_detected"] = True
                                rot_tag = f"+rot_shock_flat[{alpha_rs:.2f}]"
                    except Exception:
                        pass

                    diag["minutes_flatten_p"] = float(p)
                    sh2 = np.power(np.maximum(1e-12, sh), p)
                    sh2 = sh2 / float(np.sum(sh2))
                    raw = sh2 * s
                    ms = str(diag.get("minutes_source") or mins_col or "none")
                    if "dtd_flat" not in ms:
                        ms = f"{ms}+dtd_flat"
                    if rot_tag and ("rot_shock_flat" not in ms):
                        ms = f"{ms}{rot_tag}"
                    diag["minutes_source"] = ms
        except Exception:
            pass

        # If the minutes signal is sparse (e.g., many NaNs/zeros), prefer priors as primary.
        try:
            sig_n = int(diag.get("minutes_signal_n") or 0)
            cov = float(diag.get("minutes_prior_coverage") or 0.0)
            # Don't override expected-minutes when we have it, even if partial coverage.
            if (not using_expected) and pri_tag and sig_n > 0 and sig_n < 8 and cov >= 0.60 and float(np.sum(pri_eff)) > 0.0:
                raw = np.where(pri_eff > 0.0, pri_eff, raw)
                diag["minutes_source"] = f"{mins_col or 'none'}+priors_primary"

                # Very short-rotation + very sparse roll signal: bias minutes toward the few
                # roll-signaled players and shrink priors for players with no roll signal.
                # This helps when priors include non-rotation players (trade deadline / call-ups)
                # and the props pool only covers a couple of actual high-minutes guys.
                try:
                    if mins_col == "roll5_min" and int(sig_n) <= 3 and raw.size >= 8 and raw.size <= 9:
                        v = pd.to_numeric(players.get(mins_col), errors="coerce").to_numpy(dtype=float)
                        ok = np.isfinite(v) & (v > 0)
                        miss = ~ok

                        pri = np.maximum(0.0, np.where(np.isfinite(pri_eff), pri_eff, 0.0))
                        base = np.maximum(0.0, np.where(np.isfinite(raw), raw, 0.0))

                        # Boost signaled players toward (slightly amplified) roll mins.
                        roll_boost = 1.20
                        base = np.where(ok, np.maximum(base, roll_boost * np.maximum(0.0, v)), base)
                        # Shrink missing-signal priors so they don't steal minutes.
                        shrink = 0.80
                        base = np.where(miss, shrink * base, base)

                        raw = np.maximum(0.0, base)
                        diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+sparse_roll_bias"
                except Exception:
                    pass
        except Exception:
            pass

        # If the chosen minutes signal strongly disagrees with priors on a well-covered roster,
        # softly align toward priors. This helps when roll mins are stale (injuries/trades) but
        # player-log priors are reliable. Keep gated to avoid broad regressions.
        try:
            cov = float(diag.get("minutes_prior_coverage") or 0.0)
            if pri_tag and cov >= 0.80 and pri_eff.size == raw.size and raw.size >= 8:
                raw_pos = np.maximum(0.0, np.where(np.isfinite(raw), raw, 0.0))
                pri_pos = np.maximum(0.0, np.where(np.isfinite(pri_eff), pri_eff, 0.0))
                m = (raw_pos > 0.0) & (pri_pos > 0.0)
                if bool(np.any(m)) and int(np.sum(m)) >= 6:
                    rs = float(np.sum(raw_pos[m]))
                    ps = float(np.sum(pri_pos[m]))
                    if rs > 0 and ps > 0:
                        raw_sh = raw_pos[m] / rs
                        pri_sh = pri_pos[m] / ps
                        div = float(np.mean(np.abs(raw_sh - pri_sh)))
                        diag["minutes_prior_divergence"] = div
                        if div >= 0.12:
                            blend = 0.65
                            raw = np.where(m, (1.0 - blend) * raw + blend * pri_eff, raw)
                            diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+align_{pri_tag}"
        except Exception:
            pass

        # Rotation shock: in some games (injuries/trades/rest), roll mins can be badly stale and
        # even with decent priors coverage we can end up allocating minutes to the wrong players.
        # Detect via low top-5 overlap + high share divergence vs priors, then blend toward priors.
        # Keep opt-in and tightly gated.
        # Rotation shock (priors disagreement): if roll minutes are stale relative to strong priors,
        # blend toward priors. This is a distinct trigger from the flattening regime above.
        try:
            if (
                alpha_rs > 0.0
                and (not using_expected)
                and pri_tag
                and pri_eff.size == raw.size
                and raw.size >= 8
                and float(np.sum(pri_eff)) > 0.0
            ):
                cov = float(diag.get("minutes_prior_coverage") or 0.0)
                minutes_source = str(diag.get("minutes_source") or "")
                # If we already switched to priors as primary, don't re-apply.
                if cov >= 0.55 and ("priors_primary" not in minutes_source):
                    raw_pos = np.maximum(0.0, np.where(np.isfinite(raw), raw, 0.0))
                    pri_pos = np.maximum(0.0, np.where(np.isfinite(pri_eff), pri_eff, 0.0))
                    m = (raw_pos > 0.0) & (pri_pos > 0.0)
                    if bool(np.any(m)) and int(np.sum(m)) >= 6:
                        rs = float(np.sum(raw_pos[m]))
                        ps = float(np.sum(pri_pos[m]))
                        if rs > 0 and ps > 0:
                            raw_sh = raw_pos[m] / rs
                            pri_sh = pri_pos[m] / ps
                            div = float(np.mean(np.abs(raw_sh - pri_sh)))
                            diag["rotation_shock_divergence"] = div

                            # Top-5 overlap (by minutes) between raw and priors.
                            top5_raw = set(np.argsort(-raw_pos)[:5].tolist())
                            top5_pri = set(np.argsort(-pri_pos)[:5].tolist())
                            overlap = int(len(top5_raw.intersection(top5_pri)))
                            diag["rotation_shock_top5_overlap"] = overlap

                            # Trigger thresholds: very different shapes and different top-5.
                            if div >= 0.18 and overlap <= 2:
                                # Blend strength scales with alpha and divergence.
                                # Keep bounded to avoid over-correcting.
                                div_factor = float(np.clip((div - 0.18) / 0.18, 0.0, 1.0))
                                blend = float(np.clip(0.35 + 0.25 * div_factor, 0.35, 0.60))
                                blend = float(np.clip(alpha_rs * blend, 0.10, 0.60))
                                raw = np.where(m, (1.0 - blend) * raw + blend * pri_eff, raw)
                                diag["rotation_shock_detected"] = True
                                diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+rot_shock_{pri_tag}[{alpha_rs:.2f}]"
        except Exception:
            pass

        # Fill missing/near-zero minutes from effective priors.
        try:
            use = (raw < 1.0) & (pri_eff > 0.0)
            if bool(np.any(use)):
                raw = np.where(use, pri_eff, raw)
                if pri_tag:
                    diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+{pri_tag}"
        except Exception:
            pass

        # Targeted strong prior blend: only when our primary minutes signal is weak (roll10/pred)
        # and priors cover most of the roster.
        try:
            cov = float(diag.get("minutes_prior_coverage") or 0.0)
            if (mins_col in ("roll10_min", "pred_min")) and pri_tag and cov >= 0.80:
                has = (raw >= 1.0) & (pri_eff > 0.0)
                if bool(np.any(has)):
                    blend = 0.80
                    raw = np.where(has, (1.0 - blend) * raw + blend * pri_eff, raw)
                    diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+strong_{pri_tag}"
        except Exception:
            pass
        # If all zeros, give a small default so we can still allocate a rotation.
        if float(np.sum(raw)) <= 0:
            raw = np.full(len(players), 24.0, dtype=float)

        # Rotation priors: adjust starter-vs-bench minute share based on expected first bench sub timing,
        # and (optionally) nudge the most-common first-sub bench entrant to look more like a real "6th man".
        try:
            team_u = str(team_label or "").strip().upper()
            prior = None
            if rotation_first_sub:
                prior = rotation_first_sub.get(team_u)

            sec = None
            top_enter_name = None
            top_enter_share = None
            if isinstance(prior, dict):
                sec = prior.get("elapsed_sec_mean")
                top_enter_name = prior.get("top_enter_player_name")
                top_enter_share = prior.get("top_enter_share")
            else:
                # Back-compat: allow dict[str,float] priors passed in by callers.
                sec = prior

            if sec is not None and np.isfinite(float(sec)) and len(raw) >= 8:
                sec_f = float(sec)
                diag["rotation_first_sub_elapsed_sec_mean"] = sec_f
                try:
                    alpha = float(coach_rotation_alpha or 0.0)
                except Exception:
                    alpha = 0.0
                alpha = float(np.clip(alpha, 0.0, 1.0))
                diag["coach_rotation_alpha"] = float(alpha)
                try:
                    diag["rotation_first_sub_top_enter_name"] = str(top_enter_name) if top_enter_name else None
                except Exception:
                    diag["rotation_first_sub_top_enter_name"] = None
                try:
                    sh = float(top_enter_share) if top_enter_share is not None and np.isfinite(float(top_enter_share)) else None
                    diag["rotation_first_sub_top_enter_share"] = sh
                except Exception:
                    diag["rotation_first_sub_top_enter_share"] = None

                # Map elapsed seconds to a target starter share (top 5 minutes / team minutes).
                # Earlier subs => more bench usage (lower starter share).
                early = 120.0  # 2:00 elapsed
                late = 360.0   # 6:00 elapsed
                z = (sec_f - early) / max(1e-6, (late - early))
                z = float(np.clip(z, 0.0, 1.0))
                if exp_sparse_applied:
                    # In rotation-shock games, bench usage can be materially higher than normal.
                    # Allow a lower starter share target when expected-minutes coverage was sparse.
                    starter_share_target = 0.58 + 0.08 * z  # [0.58, 0.66]
                else:
                    starter_share_target = 0.66 + 0.08 * z  # [0.66, 0.74]
                diag["rotation_starter_share_target"] = float(starter_share_target)

                total = float(np.sum(raw))
                if total > 0:
                    # Prefer explicit starters when provided.
                    top_idx = None
                    try:
                        if "is_starter" in players.columns:
                            st = players.get("is_starter")
                            if st is not None:
                                stv = st.astype(str).str.lower().str.strip()
                                m = stv.isin(["1", "true", "t", "yes", "y"]) | (pd.to_numeric(st, errors="coerce") == 1)
                                idx = np.flatnonzero(m.to_numpy(dtype=bool))
                                if idx.size >= 5:
                                    # If more than 5 flagged, take the 5 with highest raw.
                                    ord5 = idx[np.argsort(-raw[idx])][:5]
                                    top_idx = ord5
                    except Exception:
                        top_idx = None
                    if top_idx is None:
                        top_idx = np.argsort(-raw)[:5]
                    s_raw = float(np.sum(raw[top_idx]))
                    b_raw = max(1e-9, float(total - s_raw))
                    cur = s_raw / total if total > 0 else None
                    diag["rotation_starter_share_before"] = float(cur) if cur is not None else None

                    # Solve for multiplier f on starters to hit target share:
                    # (f*S) / (f*S + B) = target => f = target*B / (S*(1-target))
                    if alpha > 0.0 and s_raw > 0 and (1.0 - starter_share_target) > 1e-6:
                        f = (starter_share_target * b_raw) / (s_raw * (1.0 - starter_share_target))
                        # Scale effect by alpha (alpha=0 => no-op).
                        f = 1.0 + alpha * (f - 1.0)
                        if exp_sparse_applied:
                            f = float(np.clip(f, 0.65, 1.35))
                        else:
                            f = float(np.clip(f, 0.75, 1.25))
                        adj = raw.copy()
                        adj[top_idx] = adj[top_idx] * f
                        # Keep minute signals non-negative.
                        raw = np.maximum(0.0, adj)
                        total2 = float(np.sum(raw))
                        if total2 > 0:
                            s2 = float(np.sum(raw[top_idx]))
                            diag["rotation_starter_share_after"] = float(s2 / total2)

                    # Bench depth shaping: for teams that sub late (tight rotations), concentrate
                    # bench minutes into fewer players; for early subs (deeper rotations), spread
                    # bench minutes across more of the bench. Keep total bench minutes fixed.
                    try:
                        if alpha > 0.0:
                            top_set = set([int(x) for x in np.asarray(top_idx).tolist()])
                            bench_idx = np.array([i for i in range(len(raw)) if i not in top_set], dtype=int)
                            if bench_idx.size >= 3:
                                bench_total = float(np.sum(raw[bench_idx]))
                                if bench_total > 1e-6:
                                    # Map first-sub timing to a shaping exponent p.
                                    # p>1 concentrates; p<1 spreads.
                                    z2 = (sec_f - 120.0) / max(1e-6, (360.0 - 120.0))
                                    z2 = float(np.clip(z2, 0.0, 1.0))
                                    sh2 = float(diag.get("rotation_first_sub_top_enter_share") or 0.0)
                                    sh2 = float(np.clip(sh2, 0.0, 1.0))
                                    # If the first sub entrant is very consistent, rotations tend to
                                    # have a more defined 6th-man core (slightly more concentration).
                                    sh_boost = float(np.clip((sh2 - 0.50) / 0.50, 0.0, 1.0))
                                    if exp_sparse_applied:
                                        p0 = 0.80 + 0.35 * z2 + 0.08 * sh_boost  # ~[0.80, 1.23]
                                    else:
                                        p0 = 0.85 + 0.40 * z2 + 0.10 * sh_boost  # ~[0.85, 1.35]
                                    p0 = float(np.clip(p0, 0.80, 1.35))
                                    # Scale effect by alpha: alpha=0 => p=1 (no reshape).
                                    p = 1.0 + alpha * (p0 - 1.0)
                                    p = float(np.clip(p, 0.80, 1.35))

                                    b = np.maximum(0.0, np.where(np.isfinite(raw[bench_idx]), raw[bench_idx], 0.0))
                                    # If all bench minutes are zero/NaN, skip.
                                    if float(np.sum(b)) > 1e-9:
                                        w = np.power(np.maximum(1e-9, b), p)
                                        ws = float(np.sum(w))
                                        if ws > 1e-12:
                                            raw2 = raw.copy()
                                            raw2[bench_idx] = bench_total * (w / ws)
                                            raw = raw2
                                            diag["rotation_bench_shape_applied"] = True
                                            diag["rotation_bench_shape_p"] = float(p)
                    except Exception:
                        pass

                    # Bench anchor nudge: if we have a consistent first-sub entrant, concentrate
                    # a tiny bit of the bench minutes into that player (without changing starter share).
                    try:
                        enter_key = _norm_player_key(top_enter_name) if top_enter_name else ""
                        if enter_key:
                            keys = players.get("player_name").map(_norm_player_key).to_numpy(dtype=object)
                            m = np.flatnonzero(keys == enter_key)
                            if m.size > 0:
                                anchor_i = int(m[0])
                                top_set = set([int(x) for x in np.asarray(top_idx).tolist()])
                                if anchor_i not in top_set:
                                    bench_idx = np.array([i for i in range(len(raw)) if i not in top_set], dtype=int)
                                    if bench_idx.size > 0:
                                        bench_total = float(np.sum(raw[bench_idx]))
                                        if bench_total > 1e-6 and raw[anchor_i] > 0:
                                            z = (sec_f - 120.0) / max(1e-6, (360.0 - 120.0))
                                            z = float(np.clip(z, 0.0, 1.0))
                                            sh = float(diag.get("rotation_first_sub_top_enter_share") or 0.0)
                                            sh = float(np.clip(sh, 0.0, 1.0))
                                            share_factor = float(np.clip((sh - 0.25) / 0.75, 0.0, 1.0))
                                            mult = 1.0 + 0.10 * (1.0 - z) + 0.12 * share_factor
                                            # Scale effect by alpha (alpha=0 => mult=1.0).
                                            mult = 1.0 + alpha * (mult - 1.0)
                                            mult = float(np.clip(mult, 1.0, 1.22))

                                            others = np.array([i for i in bench_idx.tolist() if i != anchor_i and raw[i] > 0], dtype=int)
                                            if others.size > 0:
                                                # Keep bench total fixed by scaling other bench minutes.
                                                new_anchor = float(raw[anchor_i] * mult)
                                                new_anchor = min(new_anchor, 0.65 * bench_total)
                                                rem = max(1e-9, bench_total - new_anchor)
                                                other_sum = float(np.sum(raw[others]))
                                                if other_sum > 1e-9:
                                                    raw[anchor_i] = new_anchor
                                                    raw[others] = raw[others] * (rem / other_sum)
                                                    diag["rotation_bench_anchor_applied"] = True
                                                    diag["rotation_bench_anchor_mult"] = float(mult)
                    except Exception:
                        pass
        except Exception:
            pass

        diag["minutes_total_raw"] = float(np.sum(raw))

        # For very short rotations, scaling minutes up is realistic and the "fill bench" heuristic
        # can be counterproductive (it may lock players into the bench soft-cap when priors mis-rank
        # the top-5). In priors-primary mode with <=9 players, skip bench-filling and allow a
        # slightly higher cap so 40+ minute games are possible.
        minutes_source = str(diag.get("minutes_source") or "")
        cap_player_minutes = 40.0
        skip_bench_fill = False
        try:
            if ("priors_primary" in minutes_source) and int(len(raw)) <= 9:
                skip_bench_fill = True
                cap_player_minutes = 44.0
        except Exception:
            pass
        diag["minutes_cap"] = float(cap_player_minutes)

        # If the raw minutes sum is meaningfully below 240, scaling everyone up can create
        # unrealistic 40+ minute allocations. In that case, prefer to "fill" minutes into
        # bench players rather than inflating starters.
        try:
            raw2 = raw.copy()
            total_raw = float(np.sum(raw2))
            n_players = int(len(raw2))
            if (not skip_bench_fill) and n_players >= 8 and np.isfinite(total_raw) and total_raw > 0 and total_raw < 232.0:
                # Prefer explicit starters when provided.
                top5 = None
                try:
                    if "is_starter" in players.columns:
                        st = players.get("is_starter")
                        if st is not None:
                            stv = st.astype(str).str.lower().str.strip()
                            m = stv.isin(["1", "true", "t", "yes", "y"]) | (pd.to_numeric(st, errors="coerce") == 1)
                            idx = np.flatnonzero(m.to_numpy(dtype=bool))
                            if idx.size >= 5:
                                ord5 = idx[np.argsort(-raw2[idx])][:5]
                                top5 = set(ord5.tolist())
                except Exception:
                    top5 = None
                if top5 is None:
                    top5 = set(np.argsort(-raw2)[:5].tolist())
                bench_idx = np.array([i for i in range(n_players) if i not in top5], dtype=int)
                if bench_idx.size > 0:
                    bench_floor = 6.0
                    bench_soft_cap = 28.0
                    raw2[bench_idx] = np.maximum(raw2[bench_idx], bench_floor)
                    # Spread any remaining deficit across the bench up to a soft cap.
                    deficit = float(240.0 - float(np.sum(raw2)))
                    if deficit > 0:
                        headroom = np.maximum(0.0, bench_soft_cap - raw2)
                        headroom[list(top5)] = 0.0
                        hr_sum = float(np.sum(headroom))
                        if hr_sum > 0:
                            raw2 = raw2 + headroom * (deficit / hr_sum)
                raw = np.maximum(0.0, raw2)
        except Exception:
            pass

        sim_mins = _normalize_team_minutes(raw, total_minutes=240.0, cap_player_minutes=cap_player_minutes, floor_minutes=0.0)

        # Optional: garbage-time/blowout adjustment to minutes.
        try:
            # Gate tightly: blowouts are common-ish, so only act when our quarter sim
            # thinks a blowout is meaningfully likely.
            if gt_alpha > 0.0 and int(sim_mins.size) >= 8 and float(p_abs_blow) >= 0.39:
                p_abs = float(p_abs_blow)
                # Ramp from 0 at 0.35 -> full at 0.60.
                # Keep shifts small near the activation threshold.
                p_scale = float(np.clip((p_abs - 0.35) / 0.25, 0.0, 1.0))
                max_shift_minutes = 4.0
                shift_total = float(gt_alpha * p_scale * max_shift_minutes)

                if shift_total >= 0.50:
                    # Only apply to the likely blowout winner side; applying to the
                    # likely loser can be unrealistic and has caused regressions.
                    try:
                        tl = str(team_label or "").strip().upper()
                        ht = str(home_tri or "").strip().upper()
                        at = str(away_tri or "").strip().upper()
                        team_is_home = bool(tl and ht and tl == ht)
                        team_is_away = bool(tl and at and tl == at)
                        if team_is_home:
                            p_team_win_big = float(p_home_win_big)
                            p_opp_win_big = float(p_away_win_big)
                        elif team_is_away:
                            p_team_win_big = float(p_away_win_big)
                            p_opp_win_big = float(p_home_win_big)
                        else:
                            p_team_win_big = float(p_home_win_big)
                            p_opp_win_big = float(p_away_win_big)
                        if not (np.isfinite(p_team_win_big) and np.isfinite(p_opp_win_big)):
                            raise RuntimeError("garbage_time_skip_bad_probs")
                        # Only apply to the more-likely blowout winner side.
                        if p_team_win_big <= p_opp_win_big:
                            raise RuntimeError("garbage_time_skip_not_favored")
                        # Also require meaningful directional confidence.
                        # This blocks borderline "blowout-ish" distributions that have
                        # tended to create points regressions even with small minute shifts.
                        if p_team_win_big < 0.40:
                            raise RuntimeError("garbage_time_skip_low_dir_prob")
                    except RuntimeError:
                        raise
                    except Exception:
                        raise RuntimeError("garbage_time_skip_prob_logic")

                    ms0 = str(diag.get("minutes_source") or "")
                    if "priors_primary" in ms0:
                        raise RuntimeError("garbage_time_skip_priors_primary")

                    mins0 = np.maximum(0.0, np.where(np.isfinite(sim_mins), sim_mins, 0.0)).astype(float)
                    n_players = int(mins0.size)

                    # Only apply when minutes are fairly starter-heavy (otherwise there's
                    # little to gain and shifting can add noise).
                    top3_share_sim = None
                    try:
                        denom = float(np.sum(mins0))
                        if np.isfinite(denom) and denom > 1e-9 and int(mins0.size) >= 3:
                            sh_sorted = np.sort(np.maximum(0.0, mins0) / denom)
                            top3_share_sim = float(np.sum(sh_sorted[-3:]))
                    except Exception:
                        top3_share_sim = None
                    diag["garbage_time_top3_share_sim"] = float(top3_share_sim) if top3_share_sim is not None else None
                    if top3_share_sim is None or not (0.37 <= float(top3_share_sim) <= 0.45):
                        raise RuntimeError("garbage_time_skip_not_topheavy")

                    donors = None
                    try:
                        if "is_starter" in players.columns:
                            st = players.get("is_starter")
                            if st is not None:
                                stv = st.astype(str).str.lower().str.strip()
                                msk = stv.isin(["1", "true", "t", "yes", "y"]) | (pd.to_numeric(st, errors="coerce") == 1)
                                st_idx = np.flatnonzero(msk.to_numpy(dtype=bool))
                                if st_idx.size >= 5:
                                    donors = st_idx[np.argsort(-mins0[st_idx])][:5].astype(int)
                    except Exception:
                        donors = None
                    if donors is None:
                        donors = np.argsort(-mins0)[:5].astype(int)

                    donors_set = set(int(i) for i in donors.tolist())
                    recips_all = np.array([i for i in range(n_players) if i not in donors_set], dtype=int)
                    # Prefer shifting minutes to "real bench rotation" players rather than
                    # the deepest end-of-bench. This tends to preserve points realism.
                    recips = recips_all
                    try:
                        if recips_all.size > 0:
                            mins_rec = mins0[recips_all]
                            cand = recips_all[(mins_rec >= 4.0) & (mins_rec <= 26.0)]
                            if cand.size >= 3:
                                recips = cand[np.argsort(-mins0[cand])][:5].astype(int)
                    except Exception:
                        recips = recips_all

                    if recips.size > 0 and float(np.sum(mins0[donors])) > 1e-6:
                        donor_floor = np.maximum(18.0, 0.60 * mins0[donors])
                        recip_cap = 32.0

                        donor_room = np.maximum(0.0, mins0[donors] - donor_floor)
                        feasible = float(np.sum(donor_room))
                        amt = float(min(shift_total, feasible))
                        if amt >= 0.25:
                            w_take = np.maximum(1e-6, mins0[donors])
                            take = amt * (w_take / float(np.sum(w_take)))
                            for _ in range(6):
                                over = (mins0[donors] - take) < donor_floor
                                if not bool(np.any(over)):
                                    break
                                take[over] = np.maximum(0.0, mins0[donors][over] - donor_floor[over])
                                rem = float(amt - float(np.sum(take)))
                                if rem <= 1e-6:
                                    break
                                ok = ~over
                                if not bool(np.any(ok)):
                                    break
                                w2 = np.maximum(1e-6, mins0[donors][ok] - donor_floor[ok])
                                take[ok] = take[ok] + rem * (w2 / float(np.sum(w2)))

                            mins1 = mins0.copy()
                            mins1[donors] = np.maximum(0.0, mins1[donors] - take)

                            # Give minutes mainly to the selected bench rotation.
                            w_give = np.maximum(1e-6, mins1[recips])
                            give = amt * (w_give / float(np.sum(w_give)))
                            for _ in range(6):
                                over = (mins1[recips] + give) > recip_cap
                                if not bool(np.any(over)):
                                    break
                                give[over] = np.maximum(0.0, recip_cap - mins1[recips][over])
                                rem = float(amt - float(np.sum(give)))
                                if rem <= 1e-6:
                                    break
                                ok = ~over
                                if not bool(np.any(ok)):
                                    break
                                w2 = np.maximum(1e-6, mins1[recips][ok])
                                give[ok] = give[ok] + rem * (w2 / float(np.sum(w2)))
                            mins1[recips] = np.maximum(0.0, mins1[recips] + give)

                            sim_mins = _normalize_team_minutes(
                                mins1,
                                total_minutes=240.0,
                                cap_player_minutes=cap_player_minutes,
                                floor_minutes=0.0,
                            )

                            diag["garbage_time_shift_minutes"] = float(amt)
                            diag["garbage_time_p_scale"] = float(p_scale)
                            ms = str(diag.get("minutes_source") or mins_col or "none")
                            if "garbage_time" not in ms:
                                ms = f"{ms}+garbage_time[{gt_alpha:.2f}]"
                            diag["minutes_source"] = ms
        except Exception:
            pass

        # Optional: foul-trouble rotation disruption.
        # If a key rotation player is likely to reach 5+ fouls, cap their minutes
        # and reallocate to the bench rotation. Mean-preserving (renormalizes to 240).
        try:
            try:
                ft_alpha = float(foul_trouble_alpha or 0.0)
            except Exception:
                ft_alpha = 0.0
            ft_alpha = float(np.clip(ft_alpha, 0.0, 1.0))
            diag["foul_trouble_alpha"] = float(ft_alpha)
            diag["foul_trouble_shift_minutes"] = 0.0

            if ft_alpha <= 0.0:
                raise RuntimeError("foul_trouble_skip_alpha")
            if "_prior_pf_pm" not in players.columns:
                raise RuntimeError("foul_trouble_skip_no_pf_prior")

            mins0 = np.maximum(0.0, np.where(np.isfinite(sim_mins), sim_mins, 0.0)).astype(float)
            n_players = int(mins0.size)
            if n_players < 8:
                raise RuntimeError("foul_trouble_skip_small_roster")

            pf_pm = pd.to_numeric(players.get("_prior_pf_pm"), errors="coerce").to_numpy(dtype=float)
            if pf_pm.size != mins0.size:
                raise RuntimeError("foul_trouble_skip_pf_shape")

            rot = mins0 > 0.5
            ok = rot & np.isfinite(pf_pm) & (pf_pm > 0)
            cov = float(np.sum(ok)) / float(max(1, int(np.sum(rot))))
            diag["foul_trouble_pf_coverage"] = float(cov)
            if cov < 0.60:
                raise RuntimeError("foul_trouble_skip_low_pf_coverage")

            def _poisson_p_ge(lam: float, k: int) -> float:
                # P(X >= k) for Poisson(lam), computed via sum_{i=0..k-1}.
                if (not np.isfinite(lam)) or lam <= 0:
                    return 0.0
                kk = int(max(0, k))
                if kk <= 0:
                    return 1.0
                term = 1.0
                s = 1.0
                for i in range(1, kk):
                    term *= lam / float(i)
                    s += term
                p_lt = float(np.exp(-lam) * s)
                return float(max(0.0, min(1.0, 1.0 - p_lt)))

            # Evaluate foul-trouble risk among key rotation players.
            # IMPORTANT: avoid consuming RNG unless this actually applies.
            key = np.argsort(-mins0)[: min(5, n_players)].astype(int)
            p_ge5_base = np.zeros(int(key.size), dtype=float)
            for j, idx in enumerate(key.tolist()):
                lam = float(max(0.0, pf_pm[int(idx)] * mins0[int(idx)]))
                p_ge5_base[j] = float(_poisson_p_ge(lam, 5))
            p_max_base = float(np.max(p_ge5_base) if p_ge5_base.size else 0.0)

            # Probability that "real" foul trouble happens for the key rotation.
            # This keeps the effect rare (only in games with meaningfully elevated risk).
            event_prob = float(np.clip((p_max_base - 0.28) / 0.22, 0.0, 1.0))
            diag["foul_trouble_event_prob"] = float(event_prob)
            if event_prob <= 0.0:
                raise RuntimeError("foul_trouble_skip_event_prob")
            if float(rng.random()) > event_prob:
                raise RuntimeError("foul_trouble_skip_event_draw")

            # Team-level whistle intensity (mean 1.0), small variance.
            whistle_sigma = float(0.18 * ft_alpha)
            z = float(rng.normal(0.0, 1.0))
            whistle = float(np.exp(whistle_sigma * z - 0.5 * (whistle_sigma**2)))
            diag["foul_trouble_whistle"] = float(whistle)

            p_ge5 = np.zeros(int(key.size), dtype=float)
            for j, idx in enumerate(key.tolist()):
                lam = float(max(0.0, pf_pm[int(idx)] * mins0[int(idx)] * whistle))
                p_ge5[j] = float(_poisson_p_ge(lam, 5))
            p_max = float(np.max(p_ge5) if p_ge5.size else 0.0)
            diag["foul_trouble_p_ge5_max"] = float(p_max)

            # Convert risk into a small shift budget.
            p_scale = float(np.clip((p_max - 0.30) / 0.25, 0.0, 1.0))
            max_shift_minutes = 2.0
            shift_total = float(ft_alpha * p_scale * max_shift_minutes)
            if shift_total < 0.60:
                raise RuntimeError("foul_trouble_skip_small_shift")

            # Donors: high-minute, high-risk key rotation players.
            donors = []
            for j, idx in enumerate(key.tolist()):
                if float(p_ge5[j]) >= 0.30 and mins0[int(idx)] >= 24.0:
                    donors.append(int(idx))
            if not donors:
                raise RuntimeError("foul_trouble_skip_no_donors")
            donors = np.array(donors, dtype=int)

            # Recipients: bench rotation players (not top5), prefer realistic bench mins.
            top5 = set(int(i) for i in key.tolist())
            recips_all = np.array([i for i in range(n_players) if i not in top5], dtype=int)
            recips = recips_all
            try:
                if recips_all.size > 0:
                    mins_rec = mins0[recips_all]
                    cand = recips_all[(mins_rec >= 4.0) & (mins_rec <= 26.0)]
                    if cand.size >= 3:
                        recips = cand[np.argsort(-mins0[cand])][:5].astype(int)
            except Exception:
                recips = recips_all
            if recips.size == 0:
                raise RuntimeError("foul_trouble_skip_no_recipients")

            donor_floor = np.maximum(16.0, 0.60 * mins0[donors])
            recip_cap = 34.0
            donor_room = np.maximum(0.0, mins0[donors] - donor_floor)
            feasible = float(np.sum(donor_room))
            amt = float(min(shift_total, feasible))
            if amt < 0.25:
                raise RuntimeError("foul_trouble_skip_infeasible")

            # Take minutes from donors proportionally to foul-trouble risk.
            risk_w = np.zeros(int(donors.size), dtype=float)
            for j, idx in enumerate(donors.tolist()):
                lam = float(max(0.0, pf_pm[int(idx)] * mins0[int(idx)] * whistle))
                risk_w[j] = float(_poisson_p_ge(lam, 5) + 1e-6)
            take = amt * (risk_w / float(np.sum(risk_w)))
            for _ in range(6):
                over = (mins0[donors] - take) < donor_floor
                if not bool(np.any(over)):
                    break
                take[over] = np.maximum(0.0, mins0[donors][over] - donor_floor[over])
                rem = float(amt - float(np.sum(take)))
                if rem <= 1e-6:
                    break
                ok2 = ~over
                if not bool(np.any(ok2)):
                    break
                w2 = np.maximum(1e-6, mins0[donors][ok2] - donor_floor[ok2])
                take[ok2] = take[ok2] + rem * (w2 / float(np.sum(w2)))

            mins1 = mins0.copy()
            mins1[donors] = np.maximum(0.0, mins1[donors] - take)

            # Give minutes mainly to bench rotation.
            w_give = np.maximum(1e-6, mins1[recips])
            give = amt * (w_give / float(np.sum(w_give)))
            for _ in range(6):
                over = (mins1[recips] + give) > recip_cap
                if not bool(np.any(over)):
                    break
                give[over] = np.maximum(0.0, recip_cap - mins1[recips][over])
                rem = float(amt - float(np.sum(give)))
                if rem <= 1e-6:
                    break
                ok2 = ~over
                if not bool(np.any(ok2)):
                    break
                w2 = np.maximum(1e-6, mins1[recips][ok2])
                give[ok2] = give[ok2] + rem * (w2 / float(np.sum(w2)))
            mins1[recips] = np.maximum(0.0, mins1[recips] + give)

            sim_mins = _normalize_team_minutes(
                mins1,
                total_minutes=240.0,
                cap_player_minutes=cap_player_minutes,
                floor_minutes=0.0,
            )

            diag["foul_trouble_shift_minutes"] = float(amt)
            ms = str(diag.get("minutes_source") or mins_col or "none")
            if "foul_trouble" not in ms:
                ms = f"{ms}+foul_trouble[{ft_alpha:.2f}]"
            diag["minutes_source"] = ms
        except Exception:
            pass

        diag["minutes_total_sim"] = float(np.sum(sim_mins))
        out = players.copy()
        out["_sim_min"] = sim_mins
        return out, diag

    home_players, home_min_diag = _attach_sim_minutes(home_players, team_label=str(home_tri))
    away_players, away_min_diag = _attach_sim_minutes(away_players, team_label=str(away_tri))

    lineup_diag: Dict[str, Any] = {
        "enabled": bool(use_lineup_teammate_effects),
        "home": {"attempted": False, "applied": False},
        "away": {"attempted": False, "applied": False},
    }

    def _allocate_points(team_q_points: np.ndarray, team_players: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
        if team_players is None or team_players.empty:
            return pd.DataFrame(), np.zeros((team_q_points.shape[0], 0), dtype=int)

        # Improve points weights by blending pred_pts with player priors (pts/min) when available.
        try:
            tmp = team_players.copy()
            if "_prior_pts_pm" in tmp.columns:
                pm = pd.to_numeric(tmp.get("_prior_pts_pm"), errors="coerce")
                mins = pd.to_numeric(tmp.get("_sim_min"), errors="coerce")
                prior_pts = (pm.fillna(0.0) * mins.fillna(0.0)).to_numpy(dtype=float)
                pred_pts = pd.to_numeric(tmp.get("pred_pts"), errors="coerce").fillna(0.0).to_numpy(dtype=float) if "pred_pts" in tmp.columns else np.zeros(len(tmp), dtype=float)
                # Blend in prior usage signal; keep pred primary when present.
                blend = 0.45
                tmp["_pts_blend"] = (1.0 - blend) * pred_pts + blend * prior_pts
                base_probs = _dirichlet_weights(tmp, points_col="_pts_blend")
            else:
                base_probs = _dirichlet_weights(tmp)
        except Exception:
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

    def _build_mean_box(
        team_players: pd.DataFrame,
        team_total_pts_mu: float,
        scale_pts_mu: float,
        alloc_all: Optional[np.ndarray] = None,
    ) -> Dict[str, Any]:
        """Build a deterministic, aggregate box score (means) aligned to the simulated scoring environment.

        This is intentionally stable: it avoids grading props against a single 'rep' scenario and
        prevents extreme, one-sample outliers (e.g., a bench player with 10 assists) from being
        presented as the typical outcome.
        """
        if team_players is None or team_players.empty:
            return {"players": [], "team_total_pts": float(team_total_pts_mu)}

        tp = team_players.copy()
        mins = pd.to_numeric(tp.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)

        # Mean points per player: prefer the actual sampled allocations across all sims.
        # alloc_all shape is (n_samples, n_players, 4 quarters).
        pts_mu = None
        try:
            if alloc_all is not None and isinstance(alloc_all, np.ndarray) and alloc_all.ndim == 3:
                # Sum quarters then mean across samples.
                pts_mu = alloc_all.sum(axis=2).mean(axis=0).astype(float)
        except Exception:
            pts_mu = None

        # Fallback: deterministic expectation from weights.
        if pts_mu is None:
            try:
                if "_prior_pts_pm" in tp.columns:
                    pm = pd.to_numeric(tp.get("_prior_pts_pm"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                    prior_pts = np.maximum(0.0, pm) * np.maximum(0.0, mins)
                    pred_pts = pd.to_numeric(tp.get("pred_pts"), errors="coerce").fillna(0.0).to_numpy(dtype=float) if "pred_pts" in tp.columns else np.zeros(len(tp), dtype=float)
                    blend = 0.45
                    tp["_pts_blend"] = (1.0 - blend) * pred_pts + blend * prior_pts
                    p_pts = _dirichlet_weights(tp, points_col="_pts_blend")
                else:
                    p_pts = _dirichlet_weights(tp)
            except Exception:
                p_pts = _dirichlet_weights(tp)
            pts_mu = float(team_total_pts_mu) * p_pts

        def _prior_expected_team_total(stat: str) -> Optional[float]:
            k = f"_prior_{stat}_pm"
            if k not in tp.columns:
                return None
            try:
                pm = pd.to_numeric(tp.get(k), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                return float(np.sum(np.maximum(0.0, pm) * np.maximum(0.0, mins)))
            except Exception:
                return None

        def _team_total_mu_from_pred(col: str, power: float, prior_stat: Optional[str] = None) -> float:
            try:
                pred = float(pd.to_numeric(tp.get(col), errors="coerce").fillna(0.0).sum()) if col in tp.columns else 0.0
            except Exception:
                pred = 0.0
            prior_total = _prior_expected_team_total(prior_stat) if prior_stat else None
            pred_scaled = max(0.0, pred * (float(scale_pts_mu) ** float(power)))
            prior_scaled = max(0.0, float(prior_total) * (float(scale_pts_mu) ** float(power))) if prior_total is not None else None
            if prior_scaled is None:
                lam = pred_scaled
            elif pred_scaled <= 1e-9:
                lam = prior_scaled
            else:
                lam = 0.55 * pred_scaled + 0.45 * prior_scaled
            return float(max(0.0, lam))

        def _stat_probs(col: str, prior_stat: Optional[str] = None) -> np.ndarray:
            base = _weights_from_stat_and_minutes(tp, col)
            if not prior_stat:
                return base
            k = f"_prior_{prior_stat}_pm"
            if k not in tp.columns:
                return base
            try:
                pm = pd.to_numeric(tp.get(k), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                prior_tot = np.maximum(0.0, pm) * np.maximum(0.0, mins)
                s = float(np.sum(prior_tot))
                if not np.isfinite(s) or s <= 0:
                    return base
                prior = prior_tot / s
                blend = 0.40
                p = (1.0 - blend) * base + blend * prior
                p = np.maximum(0.0, p)
                ss = float(np.sum(p))
                return p / ss if ss > 0 else base
            except Exception:
                return base

        team_reb_mu = _team_total_mu_from_pred("pred_reb", power=0.55, prior_stat="reb")
        team_ast_mu = _team_total_mu_from_pred("pred_ast", power=0.75, prior_stat="ast")
        team_3pm_mu = _team_total_mu_from_pred("pred_threes", power=0.75, prior_stat="threes")

        reb_mu = float(team_reb_mu) * _stat_probs("pred_reb", prior_stat="reb")
        ast_mu = float(team_ast_mu) * _stat_probs("pred_ast", prior_stat="ast")
        thr_mu = float(team_3pm_mu) * _stat_probs("pred_threes", prior_stat="threes")

        players_out: list[dict[str, Any]] = []
        for i in range(len(tp)):
            p = tp.iloc[i]
            players_out.append(
                {
                    "player_name": _norm_name(p.get("player_name")),
                    "min": float(mins[i]) if np.isfinite(mins[i]) else None,
                    "pts": float(pts_mu[i]) if i < len(pts_mu) and np.isfinite(pts_mu[i]) else None,
                    "reb": float(reb_mu[i]) if i < len(reb_mu) and np.isfinite(reb_mu[i]) else None,
                    "ast": float(ast_mu[i]) if i < len(ast_mu) and np.isfinite(ast_mu[i]) else None,
                    "threes": float(thr_mu[i]) if i < len(thr_mu) and np.isfinite(thr_mu[i]) else None,
                }
            )

        players_out.sort(key=lambda r: ((r.get("min") or 0.0), (r.get("pts") or 0.0)), reverse=True)
        return {
            "players": players_out,
            "team_total_pts": float(team_total_pts_mu),
            "team_total_reb": float(team_reb_mu),
            "team_total_ast": float(team_ast_mu),
            "team_total_threes": float(team_3pm_mu),
        }

    # If we have a target quarter line, build a separate representative quarter scoreline
    # and allocation that exactly matches that target totals (after rounding).
    rep_home_q = None
    rep_away_q = None
    rep_h_alloc = None
    rep_a_alloc = None
    if t_home_qf is not None and t_away_qf is not None:
        try:
            hq = _round_quarters_to_total(t_home_qf, target_total=t_home_total)
            aq = _round_quarters_to_total(t_away_qf, target_total=t_away_total)

            # Break ties without changing total points (shift 1 point within the game).
            if int(sum(hq)) == int(sum(aq)):
                moved = False
                for qi in range(3, -1, -1):
                    if aq[qi] > 0:
                        aq[qi] -= 1
                        hq[qi] += 1
                        moved = True
                        break
                if not moved:
                    hq[-1] += 1
                    if hq[0] > 0:
                        hq[0] -= 1

            rep_home_q = np.array(hq, dtype=int)
            rep_away_q = np.array(aq, dtype=int)
        except Exception:
            rep_home_q = None
            rep_away_q = None

        # Allocation is best-effort; keep rep_*_q even if allocation fails.
        if rep_home_q is not None and rep_away_q is not None:
            try:
                def _draw_rep_alloc(team_players: pd.DataFrame, team_q_points_vec: np.ndarray) -> np.ndarray:
                    if team_players is None or team_players.empty:
                        return np.zeros((0, int(team_q_points_vec.shape[0])), dtype=int)
                    base_probs = _dirichlet_weights(team_players)
                    concentration = 180.0
                    alpha = np.maximum(0.05, base_probs * concentration)
                    p_game = rng.dirichlet(alpha)
                    out = np.zeros((len(base_probs), int(team_q_points_vec.shape[0])), dtype=int)
                    for q in range(int(team_q_points_vec.shape[0])):
                        out[:, q] = _multinomial_allocate(rng, int(team_q_points_vec[q]), p_game)
                    return out

                rep_h_alloc = _draw_rep_alloc(hp, rep_home_q)
                rep_a_alloc = _draw_rep_alloc(ap, rep_away_q)
            except Exception:
                rep_h_alloc = None
                rep_a_alloc = None

    def _build_box(team_players: pd.DataFrame, alloc: np.ndarray, team_q_points: np.ndarray, rep_alloc_override: Optional[np.ndarray] = None, rep_q_override: Optional[np.ndarray] = None) -> Dict[str, Any]:
        if team_players is None or team_players.empty:
            base_total = int((rep_q_override.sum() if rep_q_override is not None else team_q_points[idx].sum()) or 0)
            return {"players": [], "team_total_pts": base_total}

        if rep_alloc_override is not None and rep_q_override is not None:
            pts_by_player = rep_alloc_override.sum(axis=1)
            rep_team_q = rep_q_override
        else:
            if alloc.size == 0:
                return {"players": [], "team_total_pts": int(team_q_points[idx].sum())}
            pts_by_player = alloc[idx].sum(axis=1)  # shape (players,)
            rep_team_q = team_q_points[idx]

        # Apply lineup/teammate-conditioned nudges to priors before generating ALL other stats.
        try:
            if bool(use_lineup_teammate_effects):
                # Determine team tricode for this side (from team_players rows).
                team_label = None
                if "team" in team_players.columns and len(team_players) > 0:
                    team_label = str(team_players.iloc[0].get("team") or "").strip().upper()
                if team_label:
                    team_players = _apply_lineup_teammate_effects_to_priors(
                        team_players=team_players,
                        team_tri=team_label,
                        date_str=date_str,
                        home_tri=str(home_tri),
                        away_tri=str(away_tri),
                    )

                    try:
                        side = "home" if team_label == str(home_tri).strip().upper() else ("away" if team_label == str(away_tri).strip().upper() else str(team_label))
                        diag = dict(getattr(team_players, "attrs", {}).get("_lineup_effects", {}) or {})
                        if side in ("home", "away"):
                            lineup_diag[side] = diag
                        else:
                            lineup_diag[str(side)] = diag
                    except Exception:
                        pass
        except Exception:
            pass

        # Cap player points for realism/predictability, then redistribute to preserve team totals.
        try:
            team_total_pts = int(rep_team_q.sum())
            if team_total_pts > 0 and pts_by_player.size:
                # Redistribution probabilities: blend pred_pts and priors (pts/min) if available.
                base_probs = _dirichlet_weights(team_players)
                if "_prior_pts_pm" in team_players.columns:
                    pm = pd.to_numeric(team_players.get("_prior_pts_pm"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                    m = pd.to_numeric(team_players.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                    prior_pts = np.maximum(0.0, pm) * np.maximum(0.0, m)
                    s = float(prior_pts.sum())
                    if np.isfinite(s) and s > 0:
                        prior_probs = prior_pts / s
                        base_probs = (0.65 * base_probs) + (0.35 * prior_probs)
                        base_probs = np.maximum(0.0, base_probs)
                        ss = float(base_probs.sum())
                        base_probs = base_probs / ss if ss > 0 else _dirichlet_weights(team_players)

                # Expected points per player to derive soft caps.
                pred_pts_i = pd.to_numeric(team_players.get("pred_pts"), errors="coerce").fillna(0.0).to_numpy(dtype=float) if "pred_pts" in team_players.columns else np.zeros(len(team_players), dtype=float)
                # Scale to scoring environment.
                exp_pred = np.maximum(0.0, pred_pts_i * float(rep_team_q.sum() / max(1e-6, float(pred_pts_i.sum()) if float(pred_pts_i.sum()) > 0 else 1.0)))
                if "_prior_pts_pm" in team_players.columns:
                    pm = pd.to_numeric(team_players.get("_prior_pts_pm"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                    exp_prior = np.maximum(0.0, pm * np.maximum(0.0, mins))
                    mu = 0.60 * exp_pred + 0.40 * exp_prior
                else:
                    mu = exp_pred

                # Build per-player caps
                caps = []
                for i in range(len(mu)):
                    m = float(mu[i])
                    mi = float(mins[i]) if i < len(mins) and np.isfinite(mins[i]) else 0.0
                    cap = int(np.ceil(m + 2.2 * np.sqrt(m + 0.25)))
                    # minutes-based linear cap
                    cap = min(cap, int(np.floor(1.10 * mi + 6.0)))
                    # share cap
                    if team_total_pts >= 10:
                        cap = min(cap, max(1, int(np.floor(0.45 * float(team_total_pts)))))
                    cap = max(0, min(int(cap), 70))
                    caps.append(cap)

                caps_arr = np.array(caps, dtype=int)
                # ensure capacity
                deficit = int(max(0, team_total_pts - int(caps_arr.sum())))
                if deficit > 0:
                    ord_idx = np.argsort(-base_probs) if base_probs.size else np.arange(caps_arr.size)
                    for j in ord_idx.tolist():
                        if deficit <= 0:
                            break
                        ub = 70
                        if team_total_pts >= 10:
                            ub = min(ub, max(1, int(np.floor(0.45 * float(team_total_pts)))))
                        headroom = max(0, ub - int(caps_arr[j]))
                        if headroom <= 0:
                            continue
                        add = min(headroom, deficit)
                        caps_arr[j] = int(caps_arr[j]) + int(add)
                        deficit -= int(add)

                # enforce caps via redistribution
                alloc0 = pts_by_player.astype(int)
                overflow = np.maximum(0, alloc0 - caps_arr)
                excess = int(overflow.sum())
                alloc1 = np.minimum(alloc0, caps_arr)
                guard = 0
                while excess > 0 and guard < 50:
                    guard += 1
                    remaining = np.maximum(0, caps_arr - alloc1)
                    eligible = remaining > 0
                    if not bool(np.any(eligible)):
                        break
                    p = base_probs * eligible.astype(float)
                    if float(p.sum()) <= 0:
                        p = eligible.astype(float)
                    extra = _multinomial_allocate(rng, int(excess), p).astype(int)
                    extra_clipped = np.minimum(extra, remaining)
                    alloc1 += extra_clipped
                    excess = int((extra - extra_clipped).sum())
                # If still excess, relax into top-prob players without exceeding caps_arr
                if excess > 0 and alloc1.size:
                    ord_idx = np.argsort(-base_probs) if base_probs.size else np.arange(alloc1.size)
                    for j in ord_idx.tolist():
                        if excess <= 0:
                            break
                        headroom = max(0, int(caps_arr[j]) - int(alloc1[j]))
                        if headroom <= 0:
                            continue
                        add = min(headroom, excess)
                        alloc1[j] = int(alloc1[j]) + int(add)
                        excess -= int(add)

                pts_by_player = alloc1.astype(int)
        except Exception:
            pass

        # Scale other stats with team scoring vs predicted scoring, but enforce team totals.
        pred_team_pts = float(pd.to_numeric(team_players.get("pred_pts"), errors="coerce").fillna(0.0).sum())
        scale_pts = float(rep_team_q.sum() / max(1e-6, pred_team_pts)) if pred_team_pts > 0 else 1.0
        mins = pd.to_numeric(team_players.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)

        def _prior_expected_team_total(stat: str) -> Optional[float]:
            # stat is like 'reb','ast','threes','tov','stl','blk'
            k = f"_prior_{stat}_pm"
            if k not in team_players.columns:
                return None
            try:
                pm = pd.to_numeric(team_players.get(k), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                m = pd.to_numeric(team_players.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                return float(np.sum(np.maximum(0.0, pm) * np.maximum(0.0, m)))
            except Exception:
                return None

        def team_total_from_pred(col: str, power: float, prior_stat: Optional[str] = None) -> int:
            if col not in team_players.columns:
                pred = 0.0
            else:
                pred = float(pd.to_numeric(team_players.get(col), errors="coerce").fillna(0.0).sum())

            prior_total = None
            if prior_stat:
                prior_total = _prior_expected_team_total(prior_stat)

            # Scale both sources by scoring environment.
            pred_scaled = max(0.0, pred * (scale_pts**power))
            prior_scaled = max(0.0, float(prior_total) * (scale_pts**power)) if prior_total is not None else None

            # Blend: if pred is missing/zero, lean on priors.
            if prior_scaled is None:
                lam = pred_scaled
            elif pred_scaled <= 1e-9:
                lam = prior_scaled
            else:
                lam = 0.55 * pred_scaled + 0.45 * prior_scaled

            # Keep variance reasonable; Poisson is fine for a first pass.
            return int(rng.poisson(lam=lam)) if lam > 0 else 0

        def _stat_probs(col: str, prior_stat: Optional[str] = None) -> np.ndarray:
            # Base probs from model predictions.
            base = _weights_from_stat_and_minutes(team_players, col)
            if not prior_stat:
                return base
            k = f"_prior_{prior_stat}_pm"
            if k not in team_players.columns:
                return base
            try:
                pm = pd.to_numeric(team_players.get(k), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                m = pd.to_numeric(team_players.get("_sim_min"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                prior_tot = np.maximum(0.0, pm) * np.maximum(0.0, m)
                s = float(np.sum(prior_tot))
                if not np.isfinite(s) or s <= 0:
                    return base
                prior = prior_tot / s
                # Blend (keep model slightly primary).
                blend = 0.40
                p = (1.0 - blend) * base + blend * prior
                p = np.maximum(0.0, p)
                ss = float(np.sum(p))
                return p / ss if ss > 0 else base
            except Exception:
                return base

        def alloc_team_total(total_value: int, col: str, prior_stat: Optional[str] = None) -> List[int]:
            probs = _stat_probs(col, prior_stat=prior_stat)
            return list(_multinomial_allocate(rng, int(total_value), probs).astype(int))

        def alloc_team_total_capped(
            total_value: int,
            prior_stat: str,
            hard_cap_by_min: Optional[Tuple[float, int, float, int]] = None,
            linear_cap_by_min: Optional[Tuple[float, float]] = None,
            max_share_of_team: Optional[float] = None,
            max_cap: int = 18,
        ) -> List[int]:
            """Allocate a team total with per-player caps derived from prior expected values and minutes.

            hard_cap_by_min: (min1, cap1, min2, cap2) applies piecewise caps for low-minute players.
            linear_cap_by_min: (slope, intercept) applies cap <= floor(slope*minutes + intercept) for all players.
            """
            probs = _stat_probs(col=f"pred_{prior_stat}", prior_stat=prior_stat) if f"pred_{prior_stat}" in team_players.columns else _stat_probs(col="pred_min", prior_stat=prior_stat)
            pm = pd.to_numeric(team_players.get(f"_prior_{prior_stat}_pm"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
            mu_i = np.maximum(0.0, pm * (scale_pts**0.60) * np.maximum(0.0, mins))

            caps = []
            for i in range(len(mu_i)):
                m = float(mu_i[i])
                mi = float(mins[i]) if i < len(mins) and np.isfinite(mins[i]) else 0.0
                cap = int(np.ceil(m + 1.8 * np.sqrt(m + 0.25)))
                if hard_cap_by_min is not None:
                    m1, c1, m2, c2 = hard_cap_by_min
                    if mi < float(m1):
                        cap = min(cap, int(c1))
                    elif mi < float(m2):
                        cap = min(cap, int(c2))
                if linear_cap_by_min is not None:
                    try:
                        a, b = linear_cap_by_min
                        cap = min(cap, int(np.floor(float(a) * float(mi) + float(b))))
                    except Exception:
                        pass
                if max_share_of_team is not None and int(total_value) >= 3:
                    try:
                        share_cap = int(np.floor(float(max_share_of_team) * float(total_value)))
                        share_cap = max(1, share_cap)
                        cap = min(cap, share_cap)
                    except Exception:
                        pass
                cap = max(0, min(int(cap), int(max_cap)))
                caps.append(cap)

            caps_arr = np.array(caps, dtype=int)

            # If the modeled team total exceeds what this rotation can plausibly produce,
            # clamp it to the sum of per-player caps (keeps the allocator feasible and prevents spikes).
            try:
                total_eff = int(min(int(total_value), int(caps_arr.sum())))
            except Exception:
                total_eff = int(total_value)

            alloc0 = _multinomial_allocate(rng, int(total_eff), probs).astype(int)
            overflow = np.maximum(0, alloc0 - caps_arr)
            excess = int(overflow.sum())
            alloc1 = np.minimum(alloc0, caps_arr)

            guard = 0
            while excess > 0 and guard < 50:
                guard += 1
                remaining = np.maximum(0, caps_arr - alloc1)
                eligible = remaining > 0
                if not bool(np.any(eligible)):
                    # No capacity left; stop trying to redistribute.
                    break
                p = probs * eligible.astype(float)
                if float(p.sum()) <= 0:
                    p = eligible.astype(float)
                extra = _multinomial_allocate(rng, int(excess), p).astype(int)
                extra_clipped = np.minimum(extra, remaining)
                alloc1 += extra_clipped
                excess = int((extra - extra_clipped).sum())

            return list(alloc1.astype(int))

        def alloc_team_total_capped_threes(total_value: int) -> List[int]:
            # Use priors when available; otherwise fall back to model pred_threes.
            probs = _stat_probs(col="pred_threes", prior_stat="threes")
            if "_prior_threes_pm" in team_players.columns:
                pm = pd.to_numeric(team_players.get("_prior_threes_pm"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                mu_i = np.maximum(0.0, pm * (scale_pts**0.75) * np.maximum(0.0, mins))
            else:
                pred_threes = pd.to_numeric(team_players.get("pred_threes"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                mu_i = np.maximum(0.0, pred_threes * (scale_pts**0.75))

            caps = []
            for i in range(len(mu_i)):
                m = float(mu_i[i])
                mi = float(mins[i]) if i < len(mins) and np.isfinite(mins[i]) else 0.0
                cap = int(np.ceil(m + 1.5 * np.sqrt(m + 0.25)))
                # hard minute-based caps for low-minute players
                if mi < 6.0:
                    cap = min(cap, 0)
                elif mi < 10.0:
                    cap = min(cap, 1)
                elif mi < 16.0:
                    cap = min(cap, 3)
                # additional linear cap to prevent spikes
                cap = min(cap, int(np.floor(0.30 * mi + 1.0)))
                # max share of team threes to prevent concentration
                if int(total_value) >= 3:
                    cap = min(cap, max(1, int(np.floor(0.60 * float(total_value)))))
                cap = max(0, min(int(cap), 10))
                caps.append(cap)

            caps_arr = np.array(caps, dtype=int)

            try:
                total_eff = int(min(int(total_value), int(caps_arr.sum())))
            except Exception:
                total_eff = int(total_value)

            alloc0 = _multinomial_allocate(rng, int(total_eff), probs).astype(int)
            overflow = np.maximum(0, alloc0 - caps_arr)
            excess = int(overflow.sum())
            alloc1 = np.minimum(alloc0, caps_arr)

            guard = 0
            while excess > 0 and guard < 50:
                guard += 1
                remaining = np.maximum(0, caps_arr - alloc1)
                eligible = remaining > 0
                if not bool(np.any(eligible)):
                    break
                p = probs * eligible.astype(float)
                if float(p.sum()) <= 0:
                    p = eligible.astype(float)
                extra = _multinomial_allocate(rng, int(excess), p).astype(int)
                extra_clipped = np.minimum(extra, remaining)
                alloc1 += extra_clipped
                excess = int((extra - extra_clipped).sum())

            return list(alloc1.astype(int))

        # Team totals (constrained)
        team_reb = team_total_from_pred("pred_reb", power=0.55, prior_stat="reb")
        team_ast = team_total_from_pred("pred_ast", power=0.75, prior_stat="ast")
        team_3pm = team_total_from_pred("pred_threes", power=0.75, prior_stat="threes")
        team_tov = team_total_from_pred("pred_tov", power=0.70, prior_stat="tov")
        team_stl = team_total_from_pred("pred_stl", power=0.60, prior_stat="stl")
        team_blk = team_total_from_pred("pred_blk", power=0.60, prior_stat="blk")

        # ALL stats use priors + caps.
        if "_prior_reb_pm" in team_players.columns:
            reb = alloc_team_total_capped(
                team_reb,
                prior_stat="reb",
                hard_cap_by_min=(6.0, 1, 12.0, 2),
                linear_cap_by_min=(0.45, 2.0),
                max_share_of_team=0.55,
                max_cap=22,
            )
        else:
            reb = alloc_team_total(team_reb, "pred_reb")

        if "_prior_ast_pm" in team_players.columns:
            # Assists can spike unrealistically in moderate minutes; apply minutes + team-share caps.
            ast = alloc_team_total_capped(
                team_ast,
                prior_stat="ast",
                hard_cap_by_min=(6.0, 1, 12.0, 2),
                linear_cap_by_min=(0.44, 1.0),
                max_share_of_team=0.60,
                max_cap=18,
            )
        else:
            ast = alloc_team_total(team_ast, "pred_ast")

        threes = alloc_team_total_capped_threes(team_3pm) if ("_prior_threes_pm" in team_players.columns or "pred_threes" in team_players.columns) else alloc_team_total(team_3pm, "pred_threes")

        if "_prior_tov_pm" in team_players.columns:
            tov = alloc_team_total_capped(
                team_tov,
                prior_stat="tov",
                hard_cap_by_min=(6.0, 1, 12.0, 2),
                linear_cap_by_min=(0.30, 1.0),
                max_share_of_team=0.60,
                max_cap=10,
            )
        else:
            tov = alloc_team_total(team_tov, "pred_tov")

        if "_prior_stl_pm" in team_players.columns:
            stl = alloc_team_total_capped(
                team_stl,
                prior_stat="stl",
                hard_cap_by_min=(10.0, 0, 16.0, 1),
                linear_cap_by_min=(0.12, 1.0),
                max_share_of_team=0.75,
                max_cap=6,
            )
        else:
            stl = alloc_team_total(team_stl, "pred_stl")

        if "_prior_blk_pm" in team_players.columns:
            blk = alloc_team_total_capped(
                team_blk,
                prior_stat="blk",
                hard_cap_by_min=(10.0, 0, 16.0, 1),
                linear_cap_by_min=(0.10, 1.0),
                max_share_of_team=0.75,
                max_cap=6,
            )
        else:
            blk = alloc_team_total(team_blk, "pred_blk")

        # Shooting + fouls: derive attempts from priors, then pick makes subject to constraints
        # and (as much as possible) consistency with allocated points and threes.
        if "_prior_threes_att_pm" in team_players.columns:
            team_3pa = team_total_from_pred("pred_threes_att", power=0.70, prior_stat="threes_att")
        else:
            team_3pa = 0
        team_3pa = max(int(team_3pa), int(sum(threes)))

        if "_prior_fga_pm" in team_players.columns:
            team_fga = team_total_from_pred("pred_fga", power=0.60, prior_stat="fga")
        else:
            team_fga = 0

        if "_prior_fta_pm" in team_players.columns:
            team_fta = team_total_from_pred("pred_fta", power=0.55, prior_stat="fta")
        else:
            team_fta = 0

        if "_prior_pf_pm" in team_players.columns:
            team_pf = team_total_from_pred("pred_pf", power=0.45, prior_stat="pf")
        else:
            team_pf = 0

        if "_prior_threes_att_pm" in team_players.columns:
            fg3a = alloc_team_total_capped(
                team_3pa,
                prior_stat="threes_att",
                hard_cap_by_min=(6.0, 1, 12.0, 3),
                linear_cap_by_min=(0.50, 2.0),
                max_share_of_team=0.60,
                max_cap=18,
            )
        else:
            fg3a = [0 for _ in range(len(team_players))]

        if "_prior_fga_pm" in team_players.columns:
            # Attempts can be high-variance; keep ceilings minutes-driven.
            fga = alloc_team_total_capped(
                team_fga,
                prior_stat="fga",
                hard_cap_by_min=(6.0, 2, 12.0, 5),
                linear_cap_by_min=(0.75, 4.0),
                max_share_of_team=0.55,
                max_cap=30,
            )
        else:
            fga = [0 for _ in range(len(team_players))]

        if "_prior_fta_pm" in team_players.columns:
            fta = alloc_team_total_capped(
                team_fta,
                prior_stat="fta",
                hard_cap_by_min=(6.0, 1, 12.0, 3),
                linear_cap_by_min=(0.35, 2.0),
                max_share_of_team=0.70,
                max_cap=18,
            )
        else:
            fta = [0 for _ in range(len(team_players))]

        if "_prior_pf_pm" in team_players.columns:
            pf = alloc_team_total_capped(
                team_pf,
                prior_stat="pf",
                hard_cap_by_min=(10.0, 0, 16.0, 2),
                linear_cap_by_min=(0.18, 1.0),
                max_share_of_team=0.60,
                max_cap=6,
            )
        else:
            pf = [0 for _ in range(len(team_players))]

        # Convert attempts into makes using player priors (percentages), then nudge to match points.
        fgm: list[int] = []
        ftm: list[int] = []
        fg3m: list[int] = [int(x) for x in threes]

        for i in range(len(team_players)):
            pts_i = int(pts_by_player[i]) if i < len(pts_by_player) else 0
            mi = float(mins[i]) if i < len(mins) and np.isfinite(mins[i]) else 0.0

            fga_i = int(fga[i]) if i < len(fga) else 0
            fg3a_i = int(fg3a[i]) if i < len(fg3a) else 0
            fta_i = int(fta[i]) if i < len(fta) else 0
            pf_i = int(pf[i]) if i < len(pf) else 0
            fg3m_i = int(fg3m[i]) if i < len(fg3m) else 0

            # Minimal feasibility between made and attempts.
            if fg3a_i < fg3m_i:
                fg3a_i = fg3m_i
            if fga_i < fg3a_i:
                fga_i = fg3a_i
            # If a player scored but got 0 attempts across the board, give them at least one FGA.
            if pts_i > 0 and fga_i <= 0 and fta_i <= 0:
                fga_i = max(fga_i, 1)
            # Keep 3PA plausible relative to minutes.
            try:
                fg3a_i = min(fg3a_i, int(np.floor(0.50 * mi + 2.0)))
            except Exception:
                pass
            fg3a_i = max(fg3m_i, fg3a_i)

            # Estimate shooting percentages from priors when available.
            try:
                fga_pm = float(pd.to_numeric(team_players.get("_prior_fga_pm").iloc[i], errors="coerce")) if "_prior_fga_pm" in team_players.columns else float("nan")
                fgm_pm = float(pd.to_numeric(team_players.get("_prior_fgm_pm").iloc[i], errors="coerce")) if "_prior_fgm_pm" in team_players.columns else float("nan")
                fg_pct = (fgm_pm / fga_pm) if np.isfinite(fga_pm) and fga_pm > 0 and np.isfinite(fgm_pm) else float("nan")
            except Exception:
                fg_pct = float("nan")
            if not np.isfinite(fg_pct):
                fg_pct = 0.46
            fg_pct = float(max(0.25, min(0.75, fg_pct)))

            try:
                fta_pm = float(pd.to_numeric(team_players.get("_prior_fta_pm").iloc[i], errors="coerce")) if "_prior_fta_pm" in team_players.columns else float("nan")
                ftm_pm = float(pd.to_numeric(team_players.get("_prior_ftm_pm").iloc[i], errors="coerce")) if "_prior_ftm_pm" in team_players.columns else float("nan")
                ft_pct = (ftm_pm / fta_pm) if np.isfinite(fta_pm) and fta_pm > 0 and np.isfinite(ftm_pm) else float("nan")
            except Exception:
                ft_pct = float("nan")
            if not np.isfinite(ft_pct):
                ft_pct = 0.76
            ft_pct = float(max(0.45, min(0.95, ft_pct)))

            ftm_i = int(rng.binomial(int(fta_i), float(ft_pct))) if int(fta_i) > 0 else 0
            fgm_i = int(rng.binomial(int(fga_i), float(fg_pct))) if int(fga_i) > 0 else 0

            # Enforce makes constraints.
            if fgm_i < fg3m_i:
                fgm_i = fg3m_i
            if fgm_i > fga_i:
                fgm_i = fga_i
            if ftm_i > fta_i:
                ftm_i = fta_i
            if pf_i > 6:
                pf_i = 6
            if pf_i < 0:
                pf_i = 0

            # Nudge makes to better match allocated points.
            # Implied points: 2*FGM + FG3M + FTM (since FG3M already included in FGM).
            diff = int(pts_i - (2 * int(fgm_i) + int(fg3m_i) + int(ftm_i)))
            guard = 0
            while diff != 0 and guard < 30:
                guard += 1
                if diff >= 2 and fgm_i < fga_i:
                    fgm_i += 1
                    diff -= 2
                    continue
                if diff <= -2 and fgm_i > fg3m_i:
                    fgm_i -= 1
                    diff += 2
                    continue
                if diff >= 1 and ftm_i < fta_i:
                    ftm_i += 1
                    diff -= 1
                    continue
                if diff <= -1 and ftm_i > 0:
                    ftm_i -= 1
                    diff += 1
                    continue
                break

            # Final clamps + store back any adjusted attempts.
            fgm_i = max(int(fg3m_i), min(int(fgm_i), int(fga_i)))
            ftm_i = max(0, min(int(ftm_i), int(fta_i)))
            fga[i] = int(max(int(fga_i), int(fgm_i), int(fg3a_i)))
            fg3a[i] = int(max(int(fg3a_i), int(fg3m_i)))
            fta[i] = int(max(int(fta_i), int(ftm_i)))
            pf[i] = int(pf_i)

            fgm.append(int(fgm_i))
            ftm.append(int(ftm_i))

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
                    "fg3a": int(fg3a[i]) if i < len(fg3a) else 0,
                    "fg3m": int(threes[i]),
                    "fga": int(fga[i]) if i < len(fga) else 0,
                    "fgm": int(fgm[i]) if i < len(fgm) else 0,
                    "fta": int(fta[i]) if i < len(fta) else 0,
                    "ftm": int(ftm[i]) if i < len(ftm) else 0,
                    "pf": int(pf[i]) if i < len(pf) else 0,
                    "stl": int(stl[i]),
                    "blk": int(blk[i]),
                    "tov": int(tov[i]),
                }
            )

        # Sort by minutes then points
        players_out.sort(key=lambda r: ((r.get("min") or 0.0), r.get("pts") or 0), reverse=True)
        return {
            "players": players_out,
            "team_total_pts": int(rep_team_q.sum()),
            "team_total_reb": int(sum(reb)),
            "team_total_ast": int(sum(ast)),
            "team_total_threes": int(sum(threes)),
            "team_total_fg3a": int(sum(fg3a)) if isinstance(fg3a, list) else 0,
            "team_total_fga": int(sum(fga)) if isinstance(fga, list) else 0,
            "team_total_fgm": int(sum(fgm)) if isinstance(fgm, list) else 0,
            "team_total_fta": int(sum(fta)) if isinstance(fta, list) else 0,
            "team_total_ftm": int(sum(ftm)) if isinstance(ftm, list) else 0,
            "team_total_pf": int(sum(pf)) if isinstance(pf, list) else 0,
            "team_total_tov": int(sum(tov)),
            "team_total_stl": int(sum(stl)),
            "team_total_blk": int(sum(blk)),
        }

    # Representative quarter path for the box score
    rep_hq = rep_home_q if rep_home_q is not None else home_q[idx]
    rep_aq = rep_away_q if rep_away_q is not None else away_q[idx]

    def _blend_event_mix_into_base(base_box: Dict[str, Any], event_box: Dict[str, Any]) -> Dict[str, Any]:
        # Goal: keep point distribution from connected allocator (base_box), but adopt
        # event-level stat mix (attempts/FTA/TOV/etc.) for better realism.
        try:
            base_players = list((base_box or {}).get("players") or [])
            event_players = list((event_box or {}).get("players") or [])
        except Exception:
            return base_box

        if not base_players or not event_players:
            return base_box

        event_map: Dict[str, Dict[str, Any]] = {}
        for p in event_players:
            try:
                k = _norm_player_key(p.get("player_name"))
                if k:
                    event_map[k] = p
            except Exception:
                continue

        def _gi(d: Dict[str, Any], k: str) -> int:
            try:
                v = d.get(k)
                if v is None:
                    return 0
                if isinstance(v, bool):
                    return int(v)
                return int(float(v))
            except Exception:
                return 0

        out_players: List[Dict[str, Any]] = []
        for bp in base_players:
            try:
                k = _norm_player_key(bp.get("player_name"))
                ep = event_map.get(k) if k else None

                out = dict(bp)
                if ep is not None:
                    # Keep connected allocator's points-related fields.
                    pts = _gi(out, "pts")
                    threes = _gi(out, "threes")
                    fgm = _gi(out, "fgm")
                    ftm = _gi(out, "ftm")
                    fg3m = _gi(out, "fg3m")
                    if fg3m <= 0:
                        fg3m = threes

                    # Adopt event-level stat mix for attempts and non-points stats.
                    out["reb"] = _gi(ep, "reb")
                    out["ast"] = _gi(ep, "ast")
                    out["stl"] = _gi(ep, "stl")
                    out["blk"] = _gi(ep, "blk")
                    out["tov"] = _gi(ep, "tov")
                    out["pf"] = _gi(ep, "pf")

                    out["fga"] = max(int(fgm), _gi(ep, "fga"))
                    out["fg3a"] = max(int(fg3m), _gi(ep, "fg3a"))
                    out["fta"] = max(int(ftm), _gi(ep, "fta"))

                    # Restore points-related fields (in case event box had different values).
                    out["pts"] = int(pts)
                    out["threes"] = int(threes)
                    out["fgm"] = int(fgm)
                    out["ftm"] = int(ftm)
                    out["fg3m"] = int(fg3m)

                out_players.append(out)
            except Exception:
                out_players.append(dict(bp))

        # Recompute team totals.
        def _sum_int(col: str) -> int:
            return int(sum(_gi(p, col) for p in out_players))

        out_players.sort(key=lambda r: ((r.get("min") or 0.0), r.get("pts") or 0), reverse=True)
        out_box = dict(base_box)
        out_box["players"] = out_players
        out_box["team_total_pts"] = _sum_int("pts")
        out_box["team_total_reb"] = _sum_int("reb")
        out_box["team_total_ast"] = _sum_int("ast")
        out_box["team_total_threes"] = _sum_int("threes")
        out_box["team_total_fg3a"] = _sum_int("fg3a")
        out_box["team_total_fga"] = _sum_int("fga")
        out_box["team_total_fgm"] = _sum_int("fgm")
        out_box["team_total_fta"] = _sum_int("fta")
        out_box["team_total_ftm"] = _sum_int("ftm")
        out_box["team_total_pf"] = _sum_int("pf")
        out_box["team_total_tov"] = _sum_int("tov")
        out_box["team_total_stl"] = _sum_int("stl")
        out_box["team_total_blk"] = _sum_int("blk")
        return out_box

    # Base representative box score from the connected allocator (keeps points consistent).
    home_box_base = _build_box(hp, h_alloc, home_q, rep_alloc_override=rep_h_alloc, rep_q_override=rep_home_q)
    away_box_base = _build_box(ap, a_alloc, away_q, rep_alloc_override=rep_a_alloc, rep_q_override=rep_away_q)

    home_box = home_box_base
    away_box = away_box_base
    event_diag: Dict[str, Any] = {
        "enabled": bool(use_event_level_sim),
        "used": False,
        "mode": None,
        "error": None,
    }

    if use_event_level_sim:
        try:
            from .events import simulate_event_level_boxscore

            home_evt, away_evt = simulate_event_level_boxscore(
                rng=rng,
                home_players=hp,
                away_players=ap,
                home_q_pts=[int(x) for x in list(rep_hq)],
                away_q_pts=[int(x) for x in list(rep_aq)],
            )

            # Blend stat-mix from event-level into base box (preserve base points allocation).
            home_box = _blend_event_mix_into_base(home_box_base, home_evt)
            away_box = _blend_event_mix_into_base(away_box_base, away_evt)
            event_diag["used"] = True
            event_diag["mode"] = "stat_mix_blend"
        except Exception as e:
            event_diag["error"] = str(e)
            home_box = home_box_base
            away_box = away_box_base

    def _q_line(h: np.ndarray, a: np.ndarray) -> List[Dict[str, int]]:
        out = []
        hcum = 0
        acum = 0
        for qi in range(h.shape[0]):
            hcum += int(h[qi])
            acum += int(a[qi])
            out.append({"q": qi + 1, "home": int(h[qi]), "away": int(a[qi]), "home_cum": hcum, "away_cum": acum})
        return out

    q_rep = _q_line(rep_hq, rep_aq)

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

        # Roster coverage sanity (helps debug "missing players" complaints)
        try:
            for side, players_df in [("home", home_players), ("away", away_players)]:
                if not isinstance(players_df, pd.DataFrame):
                    continue
                attrs = getattr(players_df, "attrs", {}) or {}
                roster_n = int(attrs.get("_roster_n") or 0)
                missing_n = int(attrs.get("_roster_missing_n") or 0)
                if roster_n >= 10 and missing_n >= 5:
                    samp = attrs.get("_roster_missing_sample") or []
                    samp_s = ", ".join([str(x) for x in (samp[:6] if isinstance(samp, list) else []) if x])
                    extra = f" (e.g., {samp_s})" if samp_s else ""
                    _warn(f"{side}: {missing_n}/{roster_n} roster players excluded from pool{extra}.")
        except Exception:
            pass
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
        "scoring_correlation": scoring_corr_diag,
        "guardrails": guard_diag,
        "home_dedup_removed": int(getattr(home_players, "attrs", {}).get("_dedup_removed", 0)) if isinstance(home_players, pd.DataFrame) else 0,
        "away_dedup_removed": int(getattr(away_players, "attrs", {}).get("_dedup_removed", 0)) if isinstance(away_players, pd.DataFrame) else 0,
        "home_points_entropy": float(_shannon_entropy(_dirichlet_weights(home_players))) if isinstance(home_players, pd.DataFrame) and not home_players.empty else 0.0,
        "away_points_entropy": float(_shannon_entropy(_dirichlet_weights(away_players))) if isinstance(away_players, pd.DataFrame) and not away_players.empty else 0.0,
        "used_target_rep": bool(rep_home_q is not None and rep_away_q is not None),
        "target_home_total": int(t_home_total) if t_home_total is not None else None,
        "target_away_total": int(t_away_total) if t_away_total is not None else None,
        "lineup_effects": lineup_diag,
        "event_level": event_diag,
        "warnings": warnings,
    }

    rep_home_score = int(rep_home_q.sum()) if rep_home_q is not None else int(home_final[idx])
    rep_away_score = int(rep_away_q.sum()) if rep_away_q is not None else int(away_final[idx])
    try:
        rep_home_score = int((home_box or {}).get("team_total_pts") or rep_home_score)
    except Exception:
        pass
    try:
        rep_away_score = int((away_box or {}).get("team_total_pts") or rep_away_score)
    except Exception:
        pass

    # Aggregate (mean) box score aligned to the simulated scoring environment.
    try:
        pred_home_pts = float(pd.to_numeric(hp.get("pred_pts"), errors="coerce").fillna(0.0).sum()) if isinstance(hp, pd.DataFrame) and (not hp.empty) and ("pred_pts" in hp.columns) else 0.0
        pred_away_pts = float(pd.to_numeric(ap.get("pred_pts"), errors="coerce").fillna(0.0).sum()) if isinstance(ap, pd.DataFrame) and (not ap.empty) and ("pred_pts" in ap.columns) else 0.0
    except Exception:
        pred_home_pts = 0.0
        pred_away_pts = 0.0
    home_total_mu = float(np.mean(home_final))
    away_total_mu = float(np.mean(away_final))
    scale_home_mu = float(home_total_mu / max(1e-6, pred_home_pts)) if pred_home_pts > 0 else 1.0
    scale_away_mu = float(away_total_mu / max(1e-6, pred_away_pts)) if pred_away_pts > 0 else 1.0
    mean_home_box = _build_mean_box(hp, team_total_pts_mu=home_total_mu, scale_pts_mu=scale_home_mu, alloc_all=h_alloc)
    mean_away_box = _build_mean_box(ap, team_total_pts_mu=away_total_mu, scale_pts_mu=scale_away_mu, alloc_all=a_alloc)

    return {
        "home": home_tri,
        "away": away_tri,
        "rep": {
            "home_score": int(rep_home_score),
            "away_score": int(rep_away_score),
            "margin": int(rep_home_score - rep_away_score),
            "quarters": q_rep,
            "home_box": home_box,
            "away_box": away_box,
        },
        "means": {
            "home_score": float(home_total_mu),
            "away_score": float(away_total_mu),
            "margin": float(np.mean(margin)),
            "quarters": q_mean,
            "home_box": mean_home_box,
            "away_box": mean_away_box,
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

        # Use the same box-score basis as the chosen narrative basis.
        # If we're narrating means, use means boxes (aggregate), else rep boxes.
        if use_means:
            home_box = ((means.get("home_box") if isinstance(means, dict) else None) or {}).get("players") or []
            away_box = ((means.get("away_box") if isinstance(means, dict) else None) or {}).get("players") or []
        else:
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
            lines.append(f"Simulated outcome: {winner} {verb} {loser} {w_score}-{l_score} in a quarter-by-quarter grind.")
        else:
            lines.append(f"Simulated outcome: {home} and {away} played to a {h}-{a} draw through regulation.")
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
