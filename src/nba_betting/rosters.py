from __future__ import annotations

import os
import pandas as pd
from pathlib import Path
import time
from typing import List

from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams as static_teams

from .config import paths


def _rosters_output_paths(season: str) -> tuple[Path, Path]:
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = season.replace("/", "-")
    return out_dir / f"rosters_{stem}.csv", out_dir / f"rosters_{stem}.parquet"


def _roster_file_team_count(path: Path) -> int:
    try:
        df = pd.read_csv(path, usecols=["TEAM_ABBREVIATION"])
        if not isinstance(df, pd.DataFrame) or df.empty:
            return 0
        return int(df["TEAM_ABBREVIATION"].dropna().astype(str).str.upper().str.strip().nunique())
    except Exception:
        return 0


def _pick_seed_roster_file(season: str, preferred_out_csv: Path) -> Path | None:
    out_dir = preferred_out_csv.parent
    candidates: list[Path] = []
    seen: set[Path] = set()

    def _add(path: Path) -> None:
        if path.exists() and path not in seen:
            seen.add(path)
            candidates.append(path)

    _add(preferred_out_csv)
    start_year = str(season).split("-", 1)[0].strip()
    if start_year:
        _add(out_dir / f"rosters_{start_year}.csv")
        for path in sorted(out_dir.glob(f"rosters_{start_year}*.csv")):
            _add(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: (_roster_file_team_count(p), p.stat().st_mtime if p.exists() else 0), reverse=True)
    return candidates[0]


def _load_existing_roster_frames(out_csv: Path) -> dict[str, pd.DataFrame]:
    if not out_csv.exists():
        return {}
    try:
        existing = pd.read_csv(out_csv)
    except Exception:
        return {}
    if not isinstance(existing, pd.DataFrame) or existing.empty or "TEAM_ABBREVIATION" not in existing.columns:
        return {}
    frames: dict[str, pd.DataFrame] = {}
    tmp = existing.copy()
    tmp["TEAM_ABBREVIATION"] = tmp["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()
    for tri, part in tmp.groupby("TEAM_ABBREVIATION", dropna=False):
        tri_key = str(tri or "").strip().upper()
        if tri_key:
            frames[tri_key] = part.copy()
    return frames


def _combine_roster_frames(team_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not team_frames:
        return pd.DataFrame()
    ordered = [team_frames[k] for k in sorted(team_frames)]
    out = pd.concat(ordered, ignore_index=True)
    if "TEAM_ABBREVIATION" in out.columns:
        out["TEAM_ABBREVIATION"] = out["TEAM_ABBREVIATION"].astype(str).str.upper().str.strip()
    return out


def _persist_roster_frames(team_frames: dict[str, pd.DataFrame], out_csv: Path, out_parq: Path) -> pd.DataFrame:
    out = _combine_roster_frames(team_frames)
    if out.empty:
        return out
    tmp_csv = out_csv.with_name(f"{out_csv.name}.tmp")
    out.to_csv(tmp_csv, index=False)
    os.replace(tmp_csv, out_csv)
    try:
        tmp_parq = out_parq.with_name(f"{out_parq.name}.tmp")
        out.to_parquet(tmp_parq, index=False)
        os.replace(tmp_parq, out_parq)
    except Exception:
        try:
            if 'tmp_parq' in locals() and tmp_parq.exists():
                tmp_parq.unlink()
        except Exception:
            pass
    return out


def fetch_rosters(
    season: str = "2025-26",
    rate_delay: float = 0.35,
    max_retries: int = 2,
    request_timeout: int = 10,
    persist_every: int = 1,
) -> pd.DataFrame:
    """Fetch all team rosters for a given season and save to processed folder.

    Parameters
    - season: NBA season string (e.g., '2025-26')

    Returns a DataFrame with concatenated rosters across all teams.
    """
    try:
        env_rate = os.environ.get("NBA_ROSTERS_RATE_DELAY", "").strip()
        if env_rate:
            rate_delay = float(env_rate)
    except Exception:
        pass
    try:
        env_retries = os.environ.get("NBA_ROSTERS_MAX_RETRIES", "").strip()
        if env_retries:
            max_retries = max(1, int(env_retries))
    except Exception:
        pass
    try:
        env_timeout = os.environ.get("NBA_ROSTERS_REQUEST_TIMEOUT_SEC", "").strip()
        if env_timeout:
            request_timeout = max(3, int(env_timeout))
    except Exception:
        pass
    try:
        env_persist = os.environ.get("NBA_ROSTERS_PERSIST_EVERY_TEAMS", "").strip()
        if env_persist:
            persist_every = max(1, int(env_persist))
    except Exception:
        pass

    team_list = static_teams.get_teams()
    out_csv, out_parq = _rosters_output_paths(season)
    seed_csv = _pick_seed_roster_file(season, out_csv)
    team_frames = _load_existing_roster_frames(seed_csv) if seed_csv is not None else {}
    failed: List[dict] = []
    refreshed: list[str] = []
    fetch_successes = 0
    total_teams = sum(1 for t in team_list if t.get('id'))
    try:
        print(
            f"[fetch_rosters] start season={season} teams={total_teams} "
            f"seed={None if seed_csv is None else seed_csv.name} "
            f"seed_teams={len(team_frames)} timeout={request_timeout}s retries={max_retries}",
            flush=True,
        )
    except Exception:
        pass
    for t in team_list:
        tid = t.get('id'); tri = t.get('abbreviation'); name = t.get('full_name')
        if not tid:
            continue
        tri = str(tri or "").strip().upper()
        last_err = None
        for attempt in range(int(max_retries)):
            try:
                print(
                    f"[fetch_rosters] team={tri or tid} attempt={attempt + 1}/{int(max_retries)}",
                    flush=True,
                )
                # nba_api uses requests under the hood; explicit per-request timeout prevents hangs.
                res = commonteamroster.CommonTeamRoster(team_id=tid, season=season, timeout=int(request_timeout))
                nd = res.get_normalized_dict()
                df = pd.DataFrame(nd.get('CommonTeamRoster', []))
                if df.empty:
                    last_err = "empty"
                    break
                df['TEAM_ID'] = tid
                df['TEAM_ABBREVIATION'] = tri
                df['TEAM_NAME'] = name
                df['SEASON'] = season
                team_frames[tri] = df
                refreshed.append(tri)
                fetch_successes += 1
                if fetch_successes == 1 or (fetch_successes % 5) == 0 or fetch_successes == total_teams:
                    print(
                        f"[fetch_rosters] progress refreshed={fetch_successes}/{total_teams} latest={tri}",
                        flush=True,
                    )
                if persist_every > 0 and (fetch_successes % persist_every) == 0:
                    _persist_roster_frames(team_frames, out_csv, out_parq)
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                try:
                    print(
                        f"[fetch_rosters] team={tri or tid} attempt={attempt + 1}/{int(max_retries)} failed: {last_err}",
                        flush=True,
                    )
                except Exception:
                    pass
                # NBA Stats API is rate-limited; backoff a bit.
                try:
                    time.sleep(float(rate_delay) * float(1 + attempt))
                except Exception:
                    pass
        if last_err is not None:
            failed.append(
                {
                    "TEAM_ID": tid,
                    "TEAM_ABBREVIATION": tri,
                    "TEAM_NAME": name,
                    "error": last_err,
                    "preserved_existing": bool(tri in team_frames),
                }
            )
        try:
            time.sleep(float(rate_delay))
        except Exception:
            pass
    if not team_frames:
        return pd.DataFrame()
    out = _persist_roster_frames(team_frames, out_csv, out_parq) if fetch_successes > 0 else _combine_roster_frames(team_frames)

    # Best-effort diagnostics (kept as prints so fetch_rosters can be used outside CLI).
    try:
        got = sorted(set(out.get('TEAM_ABBREVIATION', pd.Series(dtype=str)).astype(str).str.upper().str.strip().tolist()))
        expected = sorted([t.get('abbreviation') for t in team_list if t.get('abbreviation')])
        missing = sorted(set(expected) - set(got))
        if missing:
            print(f"[fetch_rosters] WARNING: missing {len(missing)} teams for season {season}: {missing}")
        print(
            f"[fetch_rosters] season={season} refreshed={len(set(refreshed))} "
            f"stored_teams={len(got)} seed={None if seed_csv is None else seed_csv.name} "
            f"timeout={request_timeout}s retries={max_retries}"
        )
        if failed:
            print(f"[fetch_rosters] failures: {len(failed)} teams (showing up to 8): {failed[:8]}")
    except Exception:
        pass
    return out
