from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import requests

from .config import paths
from .league import LEAGUE
from .roster_files import pick_rosters_file
from .teams import to_tricode


ESPN_SITE_ROOT = "https://site.web.api.espn.com/apis/site/v2"


def _headers() -> dict[str, str]:
    return {"Accept": "application/json", "User-Agent": LEAGUE.user_agent_product}


def _season_year(season: str) -> int:
    raw = str(season or "").strip()
    if not raw:
        return pd.Timestamp.utcnow().year
    head = raw.split("-", 1)[0].strip()
    try:
        return int(head)
    except Exception:
        return pd.Timestamp.utcnow().year


def _fetch_espn_teams() -> list[dict[str, object]]:
    url = f"{ESPN_SITE_ROOT}/{LEAGUE.espn_sport_path}/teams"
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    sports = payload.get("sports") or []
    leagues = (sports[0] or {}).get("leagues") if sports else []
    teams = (leagues[0] or {}).get("teams") if leagues else []
    out: list[dict[str, object]] = []
    for item in teams or []:
        team = item.get("team") or {}
        team_id = str(team.get("id") or "").strip()
        if not team_id:
            continue
        display_name = str(team.get("displayName") or team.get("shortDisplayName") or "").strip()
        tricode = to_tricode(display_name)
        if not tricode:
            tricode = to_tricode(str(team.get("abbreviation") or ""))
        if not display_name or not tricode:
            continue
        out.append(
            {
                "id": team_id,
                "display_name": display_name,
                "team_abbreviation": tricode,
            }
        )
    return out


def _fetch_espn_roster(team_id: str) -> list[dict[str, object]]:
    url = f"{ESPN_SITE_ROOT}/{LEAGUE.espn_sport_path}/teams/{team_id}/roster"
    resp = requests.get(url, headers=_headers(), timeout=30)
    resp.raise_for_status()
    payload = resp.json() or {}
    athletes = payload.get("athletes") or []
    return [ath for ath in athletes if isinstance(ath, dict)]


def _build_roster_frame(team: dict[str, object], season: str, athletes: list[dict[str, object]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    season_year = _season_year(season)
    tri = str(team.get("team_abbreviation") or "").strip().upper()
    team_name = str(team.get("display_name") or "").strip()
    team_id = str(team.get("id") or "").strip()
    for athlete in athletes:
        player_id_raw = str(athlete.get("id") or "").strip()
        player_id = int(player_id_raw) if player_id_raw.isdigit() else None
        first_name = str(athlete.get("firstName") or "").strip()
        last_name = str(athlete.get("lastName") or "").strip()
        player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
        position = (athlete.get("position") or {}).get("abbreviation")
        status = (athlete.get("status") or {}).get("name")
        experience = (athlete.get("experience") or {}).get("years")
        rows.append(
            {
                "PLAYER": player_name,
                "PLAYER_ID": player_id,
                "TEAM_ID": int(team_id) if team_id.isdigit() else team_id,
                "TEAM_ABBREVIATION": tri,
                "TEAM_NAME": team_name,
                "SEASON": season,
                "LEAGUE_SEASON": season_year,
                "NUM": athlete.get("jersey"),
                "POSITION": position,
                "HEIGHT": athlete.get("displayHeight"),
                "AGE": athlete.get("age"),
                "EXP": experience,
                "STATUS": status,
                "FIRST_NAME": first_name,
                "LAST_NAME": last_name,
                "BIRTH_DATE": athlete.get("dateOfBirth"),
            }
        )
    return pd.DataFrame(rows)


def _rosters_output_paths(season: str) -> tuple[Path, Path]:
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = season.replace("/", "-")
    return out_dir / f"rosters_{stem}.csv", out_dir / f"rosters_{stem}.parquet"


def _pick_seed_roster_file(season: str, preferred_out_csv: Path) -> Path | None:
    try:
        return pick_rosters_file(preferred_out_csv.parent, season=season)
    except Exception:
        return None


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
    season: str = "2026",
    rate_delay: float = 0.35,
    max_retries: int = 2,
    request_timeout: int = 10,
    persist_every: int = 1,
) -> pd.DataFrame:
    """Fetch all team rosters for a given season and save to processed folder.

    Parameters
    - season: season label used in the stored file name

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

    try:
        env_rate = os.environ.get("WNBA_ROSTERS_RATE_DELAY", env_rate if 'env_rate' in locals() else "").strip()
        if env_rate:
            rate_delay = float(env_rate)
    except Exception:
        pass
    try:
        env_retries = os.environ.get("WNBA_ROSTERS_MAX_RETRIES", env_retries if 'env_retries' in locals() else "").strip()
        if env_retries:
            max_retries = max(1, int(env_retries))
    except Exception:
        pass
    try:
        env_timeout = os.environ.get("WNBA_ROSTERS_REQUEST_TIMEOUT_SEC", env_timeout if 'env_timeout' in locals() else "").strip()
        if env_timeout:
            request_timeout = max(3, int(env_timeout))
    except Exception:
        pass
    try:
        env_persist = os.environ.get("WNBA_ROSTERS_PERSIST_EVERY_TEAMS", env_persist if 'env_persist' in locals() else "").strip()
        if env_persist:
            persist_every = max(1, int(env_persist))
    except Exception:
        pass

    team_list = _fetch_espn_teams()
    out_csv, out_parq = _rosters_output_paths(season)
    seed_csv = _pick_seed_roster_file(season, out_csv)
    team_frames = _load_existing_roster_frames(seed_csv) if seed_csv is not None else {}
    failed: dict[str, dict] = {}
    refreshed: list[str] = []
    fetch_successes = 0
    teams_to_fetch = [t for t in team_list if t.get("id") and str(t.get("team_abbreviation") or "").strip()]
    team_lookup = {str(t.get("team_abbreviation") or "").strip().upper(): t for t in teams_to_fetch}
    if team_frames:
        team_frames = {tri: frame for tri, frame in team_frames.items() if tri in team_lookup}
    total_teams = len(teams_to_fetch)
    try:
        print(
            f"[fetch_rosters] start season={season} teams={total_teams} "
            f"seed={None if seed_csv is None else seed_csv.name} "
            f"seed_teams={len(team_frames)} timeout={request_timeout}s retries={max_retries}",
            flush=True,
        )
    except Exception:
        pass

    def _refresh_team(t: dict, attempts: int, timeout_seconds: int) -> bool:
        nonlocal fetch_successes

        tid = t.get("id")
        tri = str(t.get("team_abbreviation") or "").strip().upper()
        name = t.get("display_name")
        if not tid or not tri:
            return False

        last_err = None
        for attempt in range(int(max(1, attempts))):
            try:
                print(
                    f"[fetch_rosters] team={tri or tid} attempt={attempt + 1}/{int(max(1, attempts))}",
                    flush=True,
                )
                athletes = _fetch_espn_roster(str(tid))
                df = _build_roster_frame(t, season=season, athletes=athletes)
                if df.empty:
                    last_err = "empty"
                    break
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
                failed.pop(tri, None)
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                try:
                    print(
                        f"[fetch_rosters] team={tri or tid} attempt={attempt + 1}/{int(max(1, attempts))} failed: {last_err}",
                        flush=True,
                    )
                except Exception:
                    pass
                try:
                    time.sleep(float(rate_delay) * float(1 + attempt))
                except Exception:
                    pass
        if last_err is not None:
            failed[tri] = {
                "TEAM_ID": tid,
                "TEAM_ABBREVIATION": tri,
                "TEAM_NAME": name,
                "error": last_err,
                "preserved_existing": bool(tri in team_frames),
            }
        try:
            time.sleep(float(rate_delay))
        except Exception:
            pass
        return last_err is None

    for t in teams_to_fetch:
        _refresh_team(t, attempts=int(max_retries), timeout_seconds=int(request_timeout))

    missing_after_first_pass = sorted(set(team_lookup) - set(team_frames))
    if missing_after_first_pass:
        retry_timeout = max(int(request_timeout), 15)
        retry_attempts = max(int(max_retries), 2)
        try:
            print(
                f"[fetch_rosters] retrying missing teams with timeout={retry_timeout}s: {missing_after_first_pass}",
                flush=True,
            )
        except Exception:
            pass
        for tri in missing_after_first_pass:
            t = team_lookup.get(tri)
            if t is not None:
                _refresh_team(t, attempts=retry_attempts, timeout_seconds=retry_timeout)

    if not team_frames:
        return pd.DataFrame()
    out = _persist_roster_frames(team_frames, out_csv, out_parq) if fetch_successes > 0 else _combine_roster_frames(team_frames)

    # Best-effort diagnostics (kept as prints so fetch_rosters can be used outside CLI).
    try:
        got = sorted(set(out.get('TEAM_ABBREVIATION', pd.Series(dtype=str)).astype(str).str.upper().str.strip().tolist()))
        expected = sorted(team_lookup)
        missing = sorted(set(expected) - set(got))
        if missing:
            print(f"[fetch_rosters] WARNING: missing {len(missing)} teams for season {season}: {missing}")
        print(
            f"[fetch_rosters] season={season} refreshed={len(set(refreshed))} "
            f"stored_teams={len(got)} seed={None if seed_csv is None else seed_csv.name} "
            f"timeout={request_timeout}s retries={max_retries}"
        )
        if failed:
            print(f"[fetch_rosters] failures: {len(failed)} teams (showing up to 8): {list(failed.values())[:8]}")
    except Exception:
        pass
    return out
