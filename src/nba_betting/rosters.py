from __future__ import annotations

import pandas as pd
from typing import List
import time

from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams as static_teams

from .config import paths


def fetch_rosters(season: str = "2025-26", rate_delay: float = 0.75, max_retries: int = 4) -> pd.DataFrame:
    """Fetch all team rosters for a given season and save to processed folder.

    Parameters
    - season: NBA season string (e.g., '2025-26')

    Returns a DataFrame with concatenated rosters across all teams.
    """
    team_list = static_teams.get_teams()
    rows = []
    failed: List[dict] = []
    for t in team_list:
        tid = t.get('id'); tri = t.get('abbreviation'); name = t.get('full_name')
        if not tid:
            continue
        last_err = None
        for attempt in range(int(max_retries)):
            try:
                res = commonteamroster.CommonTeamRoster(team_id=tid, season=season)
                nd = res.get_normalized_dict()
                df = pd.DataFrame(nd.get('CommonTeamRoster', []))
                if df.empty:
                    last_err = "empty"
                    break
                df['TEAM_ID'] = tid
                df['TEAM_ABBREVIATION'] = tri
                df['TEAM_NAME'] = name
                df['SEASON'] = season
                rows.append(df)
                last_err = None
                break
            except Exception as e:
                last_err = str(e)
                # NBA Stats API is rate-limited; backoff a bit.
                try:
                    time.sleep(float(rate_delay) * float(1 + attempt))
                except Exception:
                    pass
        if last_err is not None:
            failed.append({"TEAM_ID": tid, "TEAM_ABBREVIATION": tri, "TEAM_NAME": name, "error": last_err})
        try:
            time.sleep(float(rate_delay))
        except Exception:
            pass
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out_dir = paths.data_processed
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / f"rosters_{season.replace('/', '-')}.csv"
    out_parq = out_dir / f"rosters_{season.replace('/', '-')}.parquet"
    out.to_csv(out_csv, index=False)
    try:
        out.to_parquet(out_parq, index=False)
    except Exception:
        pass

    # Best-effort diagnostics (kept as prints so fetch_rosters can be used outside CLI).
    try:
        got = sorted(set(out.get('TEAM_ABBREVIATION', pd.Series(dtype=str)).astype(str).str.upper().str.strip().tolist()))
        expected = sorted([t.get('abbreviation') for t in team_list if t.get('abbreviation')])
        missing = sorted(set(expected) - set(got))
        if missing:
            print(f"[fetch_rosters] WARNING: missing {len(missing)} teams for season {season}: {missing}")
        if failed:
            print(f"[fetch_rosters] failures: {len(failed)} teams (showing up to 8): {failed[:8]}")
    except Exception:
        pass
    return out
