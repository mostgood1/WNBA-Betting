from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from typing import Optional, Dict, List
from .config import paths
from .teams import to_tricode
import datetime as _dt
import time as _time
import re as _re

@dataclass
class PlayerStatus:
    player_id: Optional[int]
    player_name: str
    team: str  # 3-letter tricode or '' for FA
    injury_status: str  # OUT/DOUBTFUL/QUESTIONABLE/PROBABLE/ACTIVE/UNKNOWN
    team_on_slate: bool
    playing_today: Optional[bool]  # True/False/None if unknown


def _today_slate_team_tricodes(date_str: str) -> set[str]:
    # Prefer standardized OddsAPI game_odds_<date>.csv
    tris: set[str] = set()
    go = paths.data_processed / f"game_odds_{date_str}.csv"
    if go.exists():
        try:
            df = pd.read_csv(go)
            if not df.empty:
                hcol = 'home_team' if 'home_team' in df.columns else None
                acol = 'visitor_team' if 'visitor_team' in df.columns else ('away_team' if 'away_team' in df.columns else None)
                if hcol and acol:
                    for _, r in df.iterrows():
                        h = to_tricode(str(r.get(hcol) or ''))
                        a = to_tricode(str(r.get(acol) or ''))
                        if h: tris.add(h)
                        if a: tris.add(a)
        except Exception:
            pass
    # Fallback: NBA Scoreboard
    if not tris:
        try:
            from nba_api.stats.endpoints import scoreboardv2
            sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=20)
            nd = sb.get_normalized_dict()
            ls = pd.DataFrame(nd.get('LineScore', []))
            if not ls.empty:
                c = {c.upper(): c for c in ls.columns}
                if 'TEAM_ABBREVIATION' in c:
                    for _, r in ls.iterrows():
                        tri = str(r[c['TEAM_ABBREVIATION']]).strip().upper()
                        if tri:
                            tris.add(tri)
        except Exception:
            pass
    return tris


def _season_for_date(date_str: str) -> str:
    try:
        d = pd.to_datetime(date_str).date()
    except Exception:
        d = _dt.date.today()
    start_year = d.year if d.month >= 7 else d.year - 1
    return f"{start_year}-{str(start_year+1)[-2:]}"


def _fetch_league_rosters_via_nba(date_str: str) -> pd.DataFrame:
    # Use nba_api teams -> CommonTeamRoster per team
    try:
        from nba_api.stats.static import teams as static_teams
        from nba_api.stats.endpoints import commonteamroster
        teams = static_teams.get_teams()
        season = _season_for_date(date_str)
        out: List[pd.DataFrame] = []
        for t in teams:
            try:
                tid = int(t.get('id'))
                tri = str(t.get('abbreviation') or '').strip().upper()
                if not tid or not tri:
                    continue
                # Explicit season to avoid cross-season leakage
                resp = commonteamroster.CommonTeamRoster(team_id=tid, season=season, timeout=20)
                nd = resp.get_normalized_dict()
                ply = pd.DataFrame(nd.get('CommonTeamRoster', []))
                if not ply.empty:
                    # Expected columns: PLAYER, PLAYER_ID
                    c = {c.upper(): c for c in ply.columns}
                    if 'PLAYER' in c and 'PLAYER_ID' in c:
                        part = ply[[c['PLAYER'], c['PLAYER_ID']]].copy()
                        part.rename(columns={c['PLAYER']: 'player_name', c['PLAYER_ID']: 'player_id'}, inplace=True)
                        part['team'] = tri
                        out.append(part)
            except Exception:
                continue
        if out:
            df = pd.concat(out, ignore_index=True)
            # Deduplicate by player_id+team (some endpoints duplicate two-way/waived entries)
            try:
                if {'player_id','team'}.issubset(df.columns):
                    df = df.drop_duplicates(subset=['player_id','team'])
            except Exception:
                pass
            return df
    except Exception:
        pass
    return pd.DataFrame(columns=['player_id','player_name','team'])


def _load_injuries_latest_upto(date_str: str) -> pd.DataFrame:
    inj = paths.data_raw / 'injuries.csv'
    out = pd.DataFrame(columns=['player','team','status','date'])
    if inj.exists():
        try:
            df = pd.read_csv(inj)
            if not df.empty and {'player','team','status','date'}.issubset(set(df.columns)):
                df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
                cutoff = pd.to_datetime(date_str).date()
                df = df[df['date'].notna()]
                df = df[df['date'] <= cutoff].copy()
                if not df.empty:
                    df = df.sort_values(['date'])
                    grp_cols = [c for c in ['player','team'] if c in df.columns]
                    if not grp_cols:
                        grp_cols = ['player']
                    latest = df.groupby(grp_cols, as_index=False).tail(1)
                    out = latest
        except Exception:
            pass
    # Merge in per-day overrides if any
    try:
        ovr = paths.data_raw / f"injuries_overrides_{date_str}.csv"
        if ovr.exists():
            odf = pd.read_csv(ovr)
            if not odf.empty and {'player','team','status'}.issubset(set(odf.columns)):
                out = pd.concat([out, odf], ignore_index=True) if not out.empty else odf
    except Exception:
        pass
    return out


def build_league_status(date_str: str) -> pd.DataFrame:
    # 0) Resolve league-wide teams via CommonPlayerInfo with caching (authoritative per-player)
    def _players_index_df() -> pd.DataFrame:
        try:
            from nba_api.stats.static import players as static_players
            plist = static_players.get_players()
            if plist:
                df = pd.DataFrame(plist)
                # expected columns: id, full_name, is_active
                c = {c.lower(): c for c in df.columns}
                need = {c.get('id'), c.get('full_name')}
                if all(need):
                    return df.rename(columns={c['id']: 'player_id', c['full_name']: 'player_name'})
        except Exception:
            pass
        return pd.DataFrame(columns=['player_id','player_name','is_active'])

    def _norm_name(s: str) -> str:
        s = (s or '').strip().lower()
        s = _re.sub(r"[^a-z0-9\s]", "", s)
        s = _re.sub(r"\s+", " ", s).strip()
        toks = [t for t in s.split(' ') if t not in {'jr','sr','ii','iii','iv','v'}]
        return ' '.join(toks)

    def _resolve_league_via_cpi(date_str: str) -> pd.DataFrame:
        idx = _players_index_df()
        if idx.empty:
            return pd.DataFrame(columns=['player_id','player_name','team'])
        # Candidate ids: prioritize active
        if 'is_active' in idx.columns:
            cand = idx[idx['is_active'] == True].copy()
        else:
            cand = idx.copy()
        # Add participants from odds snapshot for the day to ensure rookies/ten-days get covered
        try:
            from .config import paths as _paths
            raw_odds = _paths.data_raw / f"odds_nba_player_props_{date_str}.csv"
            if raw_odds.exists():
                od = pd.read_csv(raw_odds)
                name_col = next((c for c in od.columns if c.lower() in ('player','player_name','name')), None)
                if name_col:
                    od['_key'] = od[name_col].astype(str).map(_norm_name)
                    idx['_key'] = idx['player_name'].astype(str).map(_norm_name)
                    known = set(idx['_key'])
                    new_rows = od[~od['_key'].isin(known)][['_key']].drop_duplicates().copy()
                    if not new_rows.empty:
                        # Can't resolve id; skip adding unknowns to CPI list. We'll still rely on roster fallback for these.
                        pass
        except Exception:
            pass
        # Prepare cache and resolver
        cache_p = paths.data_processed / 'player_team_cache.csv'
        cache = {}
        if cache_p.exists():
            try:
                cdf = pd.read_csv(cache_p)
                if cdf is not None and not cdf.empty and {'player_id','team'}.issubset(set(cdf.columns)):
                    for _, r in cdf.iterrows():
                        try:
                            cache[int(pd.to_numeric(r['player_id'], errors='coerce'))] = str(r['team']).strip().upper()
                        except Exception:
                            continue
            except Exception:
                pass
        def _resolve(pid: int) -> str | None:
            if pid in cache:
                return cache[pid]
            try:
                from nba_api.stats.endpoints import commonplayerinfo as _cpi
                resp = _cpi.CommonPlayerInfo(player_id=int(pid), timeout=10)
                nd = resp.get_normalized_dict()
                rows = nd.get('CommonPlayerInfo', [])
                if rows:
                    tri = str(rows[0].get('TEAM_ABBREVIATION') or '').strip().upper()
                    if tri:
                        cache[pid] = tri
                        # Backoff a touch between calls
                        _time.sleep(0.2)
                        return tri
            except Exception:
                # small backoff on error to be polite
                _time.sleep(0.15)
                return None
            return None
        out_rows = []
        for _, r in cand.iterrows():
            try:
                pid = int(pd.to_numeric(r['player_id'], errors='coerce'))
                if not pid:
                    continue
                team = _resolve(pid) or ''
                name = str(r['player_name'])
                out_rows.append({'player_id': pid, 'player_name': name, 'team': team})
            except Exception:
                continue
        out = pd.DataFrame(out_rows)
        # Persist cache
        try:
            if cache:
                pd.DataFrame([(k, v) for k, v in cache.items()], columns=['player_id','team']).to_csv(cache_p, index=False)
        except Exception:
            pass
        return out

    # 1) Primary roster: resolve via CPI; fallback to team roster endpoint if CPI fails
    rost = _resolve_league_via_cpi(date_str)
    if rost is None or rost.empty:
        rost = _fetch_league_rosters_via_nba(date_str)
    if rost.empty:
        # fallback: latest processed roster file
        try:
            proc = paths.data_processed
            files = sorted(proc.glob('rosters_*.csv'))
            if files:
                df = pd.read_csv(files[-1])
                c = {c.upper(): c for c in df.columns}
                if {'PLAYER','PLAYER_ID'}.issubset(c.keys()):
                    if 'TEAM_ABBREVIATION' not in c:
                        df['TEAM_ABBREVIATION'] = None
                    rost = df[[c['PLAYER'], c['PLAYER_ID'], c['TEAM_ABBREVIATION']]].rename(columns={c['PLAYER']: 'player_name', c['PLAYER_ID']: 'player_id', c['TEAM_ABBREVIATION']: 'team'})
        except Exception:
            pass
    rost['team'] = rost['team'].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
    # Apply manual roster overrides if present (authoritative corrections)
    try:
        ov = paths.root / 'data' / 'overrides' / 'roster_overrides.csv'
        if ov.exists():
            odf = pd.read_csv(ov)
            if odf is not None and not odf.empty:
                c = {c.upper(): c for c in odf.columns}
                opid = c.get('PLAYER_ID'); oname = c.get('PLAYER'); otri = c.get('TEAM_ABBREVIATION')
                if otri and (opid or oname):
                    tmp = odf[[x for x in [opid,oname,otri] if x]].copy()
                    tmp[otri] = tmp[otri].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
                    if opid and ('player_id' in rost.columns):
                        tmp['pid'] = pd.to_numeric(tmp[opid], errors='coerce')
                        # join on player_id when possible
                        m = rost.merge(tmp[['pid', otri]].rename(columns={'pid':'player_id', otri:'team_override'}), on='player_id', how='left')
                        m['team'] = m['team_override'].where(m['team_override'].astype(str).str.len()>0, m['team']).fillna(m['team'])
                        rost = m.drop(columns=['team_override'], errors='ignore')
                    if oname:
                        # name-based fallback
                        def _nk(s: str) -> str:
                            s = (s or '').lower().strip()
                            s = _re.sub(r"[^a-z0-9\s]", '', s)
                            s = _re.sub(r"\s+", ' ', s).strip()
                            toks = [t for t in s.split(' ') if t not in {'jr','sr','ii','iii','iv','v'}]
                            return ' '.join(toks)
                        rost['_k'] = rost['player_name'].astype(str).map(_nk)
                        tmp['_k'] = tmp[oname].astype(str).map(_nk)
                        mm = rost.merge(tmp[['_k', otri]].rename(columns={otri:'team_override2'}), on='_k', how='left')
                        mm['team'] = mm['team_override2'].where(mm['team_override2'].astype(str).str.len()>0, mm['team']).fillna(mm['team'])
                        rost = mm.drop(columns=['_k','team_override2'], errors='ignore')
    except Exception:
        pass
    # Optional correction: if we have player logs, override team by latest team at or before the date (cross-season)
    try:
        logs_p = paths.data_processed / 'player_logs.csv'
        if logs_p.exists():
            logs = pd.read_csv(logs_p)
            if not logs.empty:
                c = {c.upper(): c for c in logs.columns}
                need = {'PLAYER_ID','TEAM_ABBREVIATION'}
                date_col = c.get('GAME_DATE') or c.get('GAME_DATE_EST') or None
                if need.issubset(c.keys()):
                    # Use all seasons, but limit by date <= anchor date when available
                    cutoff = pd.to_datetime(date_str, errors='coerce')
                    if date_col and cutoff is not None and not pd.isna(cutoff):
                        logs[date_col] = pd.to_datetime(logs[date_col], errors='coerce')
                        logs = logs[logs[date_col].notna()]
                        logs = logs[logs[date_col] <= cutoff]
                    if date_col:
                        # sort ascending, keep latest per player (up to cutoff)
                        logs = logs.sort_values([c['PLAYER_ID'], date_col])
                        latest = logs.groupby(c['PLAYER_ID'], as_index=False).tail(1)
                    else:
                        latest = logs.drop_duplicates(subset=[c['PLAYER_ID']], keep='last')
                    latest = latest[[c['PLAYER_ID'], c['TEAM_ABBREVIATION']]].rename(columns={c['PLAYER_ID']:'player_id', c['TEAM_ABBREVIATION']:'team_logs'})
                    latest['team_logs'] = latest['team_logs'].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
                    # Merge and override team when available
                    if not latest.empty and {'player_id','team'}.issubset(rost.columns):
                        tmp = rost.merge(latest, on='player_id', how='left')
                        tmp['team'] = tmp['team_logs'].where(tmp['team_logs'].astype(str).str.len()>0, tmp['team']).fillna(tmp['team'])
                        rost = tmp.drop(columns=['team_logs'], errors='ignore')
                        # Deduplicate again if override introduced dups
                        try:
                            rost = rost.drop_duplicates(subset=['player_id','team'])
                        except Exception:
                            pass
    except Exception:
        pass
    # 2) Injuries up to date
    inj = _load_injuries_latest_upto(date_str)
    # normalize injuries
    if not inj.empty:
        inj = inj.copy()
        inj['team'] = inj['team'].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
        inj['status_norm'] = inj['status'].astype(str).str.upper()
    # 3) Build slate teams
    tris = _today_slate_team_tricodes(date_str)
    # 4) Join roster + injuries by name (best-effort)
    def _norm_name(s: str) -> str:
        s = (s or '').strip().lower()
        s = _re.sub(r'[^a-z0-9\s]', '', s)
        s = _re.sub(r'\s+', ' ', s).strip()
        toks = [t for t in s.split(' ') if t not in {'jr','sr','ii','iii','iv','v'}]
        return ' '.join(toks)
    out = rost.copy()
    out['_name_key'] = out['player_name'].astype(str).map(_norm_name)
    if not inj.empty:
        inj = inj.copy()
        inj['_name_key'] = inj['player'].astype(str).map(_norm_name)
        keep_cols = ['_name_key', 'team', 'status_norm']
        inj_small = inj[[c for c in keep_cols if c in inj.columns]].drop_duplicates()

        # Prefer team-aware match (prevents cross-team contamination on trades).
        if {'_name_key', 'team', 'status_norm'}.issubset(set(inj_small.columns)):
            out = out.merge(inj_small, on=['_name_key', 'team'], how='left')
        else:
            out = out.merge(inj_small, on=['_name_key'], how='left')

        # Fallback to name-only match ONLY when the injuries row has no team.
        # If injuries have a team that doesn't match the player's current team, we must not apply it
        # (trades would otherwise incorrectly mark players OUT).
        try:
            if 'status_norm' in out.columns:
                missing = out['status_norm'].isna() | (out['status_norm'].astype(str).str.len() == 0)
            else:
                out['status_norm'] = None
                missing = out['status_norm'].isna()

            if bool(missing.any()) and {'_name_key', 'status_norm'}.issubset(set(inj_small.columns)):
                inj_name_only = None
                if 'team' in inj_small.columns:
                    tmp = inj_small.copy()
                    tmp['team'] = tmp['team'].fillna('').astype(str).str.upper().str.strip()
                    tmp = tmp[tmp['team'].astype(str).str.len() == 0]
                    if not tmp.empty:
                        inj_name_only = tmp[['_name_key', 'status_norm']].drop_duplicates(subset=['_name_key'])
                else:
                    inj_name_only = inj_small[['_name_key', 'status_norm']].drop_duplicates(subset=['_name_key'])

                if inj_name_only is not None and (not inj_name_only.empty):
                    out2 = out.loc[missing].merge(inj_name_only, on='_name_key', how='left', suffixes=('', '_byname'))
                    if 'status_norm_byname' in out2.columns:
                        out.loc[missing, 'status_norm'] = out2['status_norm_byname'].values
        except Exception:
            pass
    else:
        out['status_norm'] = None
    out['team'] = out['team'].fillna('').astype(str).str.upper()
    out['injury_status'] = out['status_norm'].fillna('')
    out['team_on_slate'] = out['team'].isin(tris)
    # playing_today: only for teams on slate and not obviously OUT
    def _playing(status: str, on_slate: bool) -> Optional[bool]:
        if not on_slate:
            return False
        s = str(status or '').upper()
        if s in {'OUT','INACTIVE','SUSPENDED','REST','DOUBTFUL'}:
            return False
        if s in {'QUESTIONABLE','PROBABLE','ACTIVE','DAY-TO-DAY',''}:
            return True
        return None
    out['playing_today'] = [ _playing(s, t) for s,t in zip(out['injury_status'], out['team_on_slate']) ]
    out = out.drop(columns=['_name_key','status_norm'], errors='ignore')
    # Save
    out_path = paths.data_processed / f'league_status_{date_str}.csv'
    out.to_csv(out_path, index=False)
    return out
