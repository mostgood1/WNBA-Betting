from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .quarters import QuarterResult, sample_quarter_scores
from ..config import paths


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


def _load_rotation_first_sub_priors() -> dict[str, float]:
    """Load team-level first bench sub-in timing priors (seconds elapsed in Q1).

    File is written by nba_betting.rotation_priors.write_rotation_priors().
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
        out: dict[str, float] = {}
        for _, r in df.iterrows():
            team = str(r.get("team") or "").strip().upper()
            v = pd.to_numeric(r.get("elapsed_sec_mean"), errors="coerce")
            if team and np.isfinite(v):
                out[team] = float(v)
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
    player_priors: Optional[Dict[Tuple[str, str], Dict[str, float]]] = None,
    minutes_lookback_days: int = 21,
    n_samples: int = 1500,
    seed: Optional[int] = None,
    target_quarters: Optional[List[Dict[str, Any]]] = None,
    target_home_score: Optional[float] = None,
    target_away_score: Optional[float] = None,
    rotation_priors: Optional[Dict[str, float]] = None,
    date_str: Optional[str] = None,
    use_lineup_teammate_effects: bool = True,
    use_event_level_sim: bool = True,
) -> Dict[str, Any]:
    """Connected simulation: quarter team points + player box scores share the same scoring totals.

    - Samples integer quarter scores from the quarter distribution.
    - Allocates each quarter's team points across players via a Dirichlet-multinomial driven by pred_pts/minutes.
    - Generates a representative single-game box score (median margin) and also returns means.
    """
    rng = np.random.default_rng(seed)

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
    n = int(home_q.shape[0])
    if n == 0:
        return {"error": "no samples"}

    home_final = home_q.sum(axis=1)
    away_final = away_q.sum(axis=1)
    margin = home_final - away_final

    total = home_final + away_final

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
        # However, be tolerant of mismatches: if the filter would wipe out most rows,
        # keep the props_df-derived pool to avoid producing nonsense box scores.
        try:
            roster_names = [(_norm_player_key(x), _norm_name(x)) for x in (roster or []) if _norm_player_key(x)]
            if roster_names and ("player_name" in out.columns) and (not out.empty):
                allowed = set(k for k, _ in roster_names)
                before = int(len(out))
                filtered = out[out["player_name"].map(_norm_player_key).isin(allowed)].copy()
                after = int(len(filtered))
                # Keep restriction only if it still leaves a plausible rotation.
                if after >= 7 or (before > 0 and (after / before) >= 0.55):
                    out = filtered
        except Exception:
            pass

        # Deduplicate: keep the most-relevant row per player (highest minutes signal, then pred_pts)
        if not out.empty and "player_name" in out.columns:
            try:
                out = out.copy()
                out["_player_norm"] = out["player_name"].map(_norm_player_key)
                # pick best minutes feature available (based on non-empty data)
                mins_col = _pick_minutes_col(out, ("pred_min", "roll10_min", "roll5_min", "roll20_min", "roll30_min"))
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

            # Attach broader player priors (rates) if provided.
            try:
                if (not out.empty) and ("player_name" in out.columns) and player_priors:
                    out = out.copy()
                    out["_pkey"] = out["player_name"].map(_norm_player_key)
                    def _p(team_key: str, player_key: str, k: str) -> Optional[float]:
                        try:
                            v = (player_priors.get((team_key, player_key)) or {}).get(k)
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

            # After roster augmentation, attach priors for any newly-added players.
            # (The earlier attachment only covered props-derived players.)
            try:
                if (not out.empty) and ("player_name" in out.columns) and pri:
                    out = out.copy()
                    if "_prior_min" not in out.columns:
                        out["_prior_min"] = None
                    mins_map = out["player_name"].map(lambda nm: pri.get((team_u, _norm_player_key(nm))))
                    out["_prior_min"] = pd.to_numeric(out["_prior_min"], errors="coerce")
                    out["_prior_min"] = out["_prior_min"].where(out["_prior_min"].notna(), mins_map)
            except Exception:
                pass

            try:
                if (not out.empty) and ("player_name" in out.columns) and player_priors:
                    out = out.copy()
                    out["_pkey"] = out["player_name"].map(_norm_player_key)

                    def _p(team_key: str, player_key: str, k: str) -> Optional[float]:
                        try:
                            v = (player_priors.get((team_key, player_key)) or {}).get(k)
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
            "rotation_first_sub_elapsed_sec_mean": None,
            "rotation_starter_share_target": None,
            "rotation_starter_share_before": None,
            "rotation_starter_share_after": None,
        }
        if players is None or players.empty:
            return players, diag
        players = players.copy()
        diag["players"] = int(len(players))

        # No placeholder players. If minutes signals are missing, we'll normalize whatever is available.

        mins_col = _pick_minutes_col(players, ("pred_min", "roll10_min", "roll5_min", "roll20_min", "roll30_min"))
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

        # Second fallback: use player_logs-derived minutes mean if provided.
        try:
            if "_prior_min_mu" in players.columns:
                pri2 = pd.to_numeric(players.get("_prior_min_mu"), errors="coerce").fillna(0.0).to_numpy(dtype=float)
                use2 = (raw < 1.0) & (pri2 > 0.0)
                if bool(np.any(use2)):
                    raw = np.where(use2, pri2, raw)
                    diag["minutes_source"] = f"{diag.get('minutes_source') or mins_col or 'none'}+logs"
        except Exception:
            pass
        # If all zeros, give a small default so we can still allocate a rotation.
        if float(np.sum(raw)) <= 0:
            raw = np.full(len(players), 24.0, dtype=float)

        # Rotation prior: adjust starter-vs-bench minute share based on expected first bench sub timing.
        try:
            team_u = str(team_label or "").strip().upper()
            sec = None
            if rotation_first_sub:
                sec = rotation_first_sub.get(team_u)
            if sec is not None and np.isfinite(float(sec)) and len(raw) >= 8:
                sec_f = float(sec)
                diag["rotation_first_sub_elapsed_sec_mean"] = sec_f

                # Map elapsed seconds to a target starter share (top 5 minutes / team minutes).
                # Earlier subs => more bench usage (lower starter share).
                early = 120.0  # 2:00 elapsed
                late = 360.0   # 6:00 elapsed
                z = (sec_f - early) / max(1e-6, (late - early))
                z = float(np.clip(z, 0.0, 1.0))
                starter_share_target = 0.66 + 0.08 * z  # [0.66, 0.74]
                diag["rotation_starter_share_target"] = float(starter_share_target)

                total = float(np.sum(raw))
                if total > 0:
                    top_idx = np.argsort(-raw)[:5]
                    s_raw = float(np.sum(raw[top_idx]))
                    b_raw = max(1e-9, float(total - s_raw))
                    cur = s_raw / total if total > 0 else None
                    diag["rotation_starter_share_before"] = float(cur) if cur is not None else None

                    # Solve for multiplier f on starters to hit target share:
                    # (f*S) / (f*S + B) = target => f = target*B / (S*(1-target))
                    if s_raw > 0 and (1.0 - starter_share_target) > 1e-6:
                        f = (starter_share_target * b_raw) / (s_raw * (1.0 - starter_share_target))
                        f = float(np.clip(f, 0.75, 1.25))
                        adj = raw.copy()
                        adj[top_idx] = adj[top_idx] * f
                        # Keep minute signals non-negative.
                        raw = np.maximum(0.0, adj)
                        total2 = float(np.sum(raw))
                        if total2 > 0:
                            s2 = float(np.sum(raw[top_idx]))
                            diag["rotation_starter_share_after"] = float(s2 / total2)
        except Exception:
            pass

        diag["minutes_total_raw"] = float(np.sum(raw))

        # If the raw minutes sum is meaningfully below 240, scaling everyone up can create
        # unrealistic 40+ minute allocations. In that case, prefer to "fill" minutes into
        # bench players rather than inflating starters.
        try:
            raw2 = raw.copy()
            total_raw = float(np.sum(raw2))
            n_players = int(len(raw2))
            if n_players >= 8 and np.isfinite(total_raw) and total_raw > 0 and total_raw < 232.0:
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

        sim_mins = _normalize_team_minutes(raw, total_minutes=240.0, cap_player_minutes=40.0, floor_minutes=0.0)
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

    home_box = None
    away_box = None
    event_diag: Dict[str, Any] = {"enabled": bool(use_event_level_sim), "used": False, "error": None}
    if use_event_level_sim:
        try:
            from .events import simulate_event_level_boxscore

            home_box, away_box = simulate_event_level_boxscore(
                rng=rng,
                home_players=hp,
                away_players=ap,
                home_q_pts=[int(x) for x in list(rep_hq)],
                away_q_pts=[int(x) for x in list(rep_aq)],
            )
            event_diag["used"] = True
        except Exception as e:
            event_diag["error"] = str(e)
            home_box = None
            away_box = None

    if home_box is None or away_box is None:
        # Fallback to the aggregate allocator
        home_box = _build_box(hp, h_alloc, home_q, rep_alloc_override=rep_h_alloc, rep_q_override=rep_home_q)
        away_box = _build_box(ap, a_alloc, away_q, rep_alloc_override=rep_a_alloc, rep_q_override=rep_away_q)

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
