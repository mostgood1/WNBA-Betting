from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
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
    # Use standardized OddsAPI game_odds_<date>.csv when available,
    # but always try to union with NBA Scoreboard (odds snapshots can be incomplete).
    odds_tris: set[str] = set()
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
                        if h: odds_tris.add(h)
                        if a: odds_tris.add(a)
        except Exception:
            pass

    sb_tris: set[str] = set()
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
                        sb_tris.add(tri)
    except Exception:
        sb_tris = set()

    # Deterministic fallback: processed season schedule (local, stable).
    sched_tris: set[str] = set()
    try:
        season = _season_for_date(date_str)
        season_str = season.replace('-', '_')
        candidates = [
            paths.data_processed / f"schedule_{season_str}.json",
            paths.data_processed / "schedule_2025_26.json",
        ]
        sched_path = next((p for p in candidates if p.exists()), None)
        if sched_path is not None:
            import json as _json
            raw = _json.load(open(sched_path, 'r', encoding='utf-8'))
            if isinstance(raw, list) and raw:
                for g in raw:
                    try:
                        d = str(g.get('date_est') or g.get('date_utc') or '')
                        if not d:
                            continue
                        d0 = pd.to_datetime(d, errors='coerce')
                        if pd.isna(d0):
                            continue
                        if str(d0.date()) != str(date_str):
                            continue
                        ht = str(g.get('home_tricode') or '').strip().upper()
                        at = str(g.get('away_tricode') or '').strip().upper()
                        if ht:
                            sched_tris.add(ht)
                        if at:
                            sched_tris.add(at)
                    except Exception:
                        continue
    except Exception:
        sched_tris = set()

    return odds_tris | sb_tris | sched_tris


def _season_for_date(date_str: str) -> str:
    try:
        d = pd.to_datetime(date_str).date()
    except Exception:
        d = _dt.date.today()
    start_year = d.year if d.month >= 7 else d.year - 1
    return f"{start_year}-{str(start_year+1)[-2:]}"


def _pick_processed_roster_file(date_str: str | None) -> Path | None:
    proc = paths.data_processed
    files = list(proc.glob('rosters_*.csv'))
    if not files:
        return None

    def _team_count(path: Path) -> int:
        try:
            df = pd.read_csv(path, usecols=['TEAM_ABBREVIATION'])
            if not isinstance(df, pd.DataFrame) or df.empty:
                return 0
            return int(df['TEAM_ABBREVIATION'].dropna().astype(str).str.upper().str.strip().nunique())
        except Exception:
            return 0

    candidates: list[Path] = []
    seen: set[Path] = set()

    def _add(path: Path) -> None:
        if path.exists() and path not in seen:
            seen.add(path)
            candidates.append(path)

    if date_str:
        try:
            season = _season_for_date(date_str)
            _add(proc / f"rosters_{season}.csv")
            start_year = str(season).split('-', 1)[0].strip()
            if start_year:
                _add(proc / f"rosters_{start_year}.csv")
                for path in sorted(proc.glob(f"rosters_{start_year}*.csv")):
                    _add(path)
        except Exception:
            pass

    if candidates:
        candidates.sort(key=lambda p: (_team_count(p), p.stat().st_mtime if p.exists() else 0), reverse=True)
        return candidates[0]

    season_files = [f for f in files if '-' in f.stem]
    if season_files:
        season_files.sort(key=lambda p: (_team_count(p), p.stat().st_mtime if p.exists() else 0), reverse=True)
        return season_files[0]

    files.sort(key=lambda p: (_team_count(p), p.stat().st_mtime if p.exists() else 0), reverse=True)
    return files[0]


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
                    # Drop stale exclusion statuses (fixes RTP/feed-staleness issues).
                    try:
                        EXCL = {'OUT','DOUBTFUL','SUSPENDED','INACTIVE','REST'}
                        tmp = latest.copy()
                        tmp['status_norm'] = tmp['status'].astype(str).str.upper().str.strip()
                        tmp['date'] = pd.to_datetime(tmp['date'], errors='coerce').dt.date
                        is_excl = tmp['status_norm'].isin(EXCL)
                        is_season = (
                            tmp['status_norm'].astype(str).str.contains('SEASON', na=False)
                            | tmp['status_norm'].astype(str).str.contains('INDEFINITE', na=False)
                            | tmp['status_norm'].astype(str).str.contains('SEASON-ENDING', na=False)
                        )
                        days_old = None
                        try:
                            days_old = tmp['date'].map(lambda d: (cutoff - d).days if d is not None else 9999)
                        except Exception:
                            days_old = None
                        if days_old is not None:
                            stale_excl = is_excl & (~is_season) & (days_old > 3)
                            tmp = tmp[~stale_excl].copy()
                        out = tmp.drop(columns=['status_norm'], errors='ignore')
                    except Exception:
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
        # Prefer last-known team from player_logs (deterministic, local, and robust).
        logs_pid_to_tri: dict[int, str] = {}
        try:
            from .config import paths as _paths
            logs_csv = _paths.data_processed / 'player_logs.csv'
            logs_pq = _paths.data_processed / 'player_logs.parquet'
            logs = None
            if logs_csv.exists():
                logs = pd.read_csv(logs_csv)
            elif logs_pq.exists():
                try:
                    logs = pd.read_parquet(logs_pq)
                except Exception:
                    logs = None
            if isinstance(logs, pd.DataFrame) and not logs.empty:
                cols = {c.upper(): c for c in logs.columns}
                pid_c = cols.get('PLAYER_ID')
                tri_c = cols.get('TEAM_ABBREVIATION')
                date_c = cols.get('GAME_DATE') or cols.get('DATE')
                if pid_c and tri_c and date_c:
                    tmp = logs[[pid_c, tri_c, date_c]].copy()
                    tmp[pid_c] = pd.to_numeric(tmp[pid_c], errors='coerce')
                    tmp[tri_c] = tmp[tri_c].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
                    tmp[date_c] = pd.to_datetime(tmp[date_c], errors='coerce')
                    cut = pd.to_datetime(date_str, errors='coerce')
                    if pd.notna(cut):
                        tmp = tmp[tmp[date_c].notna() & (tmp[date_c] <= cut)]
                    tmp = tmp.dropna(subset=[pid_c])
                    tmp = tmp[tmp[tri_c].astype(str).str.len() > 0]
                    if not tmp.empty:
                        tmp = tmp.sort_values(date_c)
                        last = tmp.groupby(pid_c, as_index=False).tail(1)
                        for _, rr in last.iterrows():
                            try:
                                pid = int(rr[pid_c])
                                tri = str(rr[tri_c]).strip().upper()
                                if pid and tri:
                                    logs_pid_to_tri[pid] = tri
                            except Exception:
                                continue
        except Exception:
            logs_pid_to_tri = {}

        # Prepare a season-appropriate roster map (used to invalidate stale cache entries)
        roster_pid_to_tri: dict[int, str] = {}
        try:
            roster_file = _pick_processed_roster_file(date_str)
            if roster_file is not None and roster_file.exists():
                rdf = pd.read_csv(roster_file)
                if rdf is not None and not rdf.empty:
                    c = {c.upper(): c for c in rdf.columns}
                    if {'PLAYER_ID','TEAM_ABBREVIATION'}.issubset(c.keys()):
                        tmp = rdf[[c['PLAYER_ID'], c['TEAM_ABBREVIATION']]].copy()
                        tmp[c['PLAYER_ID']] = pd.to_numeric(tmp[c['PLAYER_ID']], errors='coerce')
                        tmp[c['TEAM_ABBREVIATION']] = tmp[c['TEAM_ABBREVIATION']].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
                        for _, rr in tmp.dropna().iterrows():
                            try:
                                pid = int(rr[c['PLAYER_ID']])
                                tri = str(rr[c['TEAM_ABBREVIATION']] or '').strip().upper()
                                if pid and tri:
                                    roster_pid_to_tri[pid] = tri
                            except Exception:
                                continue
        except Exception:
            roster_pid_to_tri = {}

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
            try:
                rtri = roster_pid_to_tri.get(int(pid))
                if rtri:
                    cache[pid] = rtri
                    return rtri
            except Exception:
                pass
            # Fall back to last-known team from logs at/before date when roster data is unavailable.
            try:
                ltri = logs_pid_to_tri.get(int(pid))
                if ltri:
                    cache[pid] = ltri
                    return ltri
            except Exception:
                pass
            if pid in cache:
                # If roster disagrees, treat roster as authoritative for this date.
                try:
                    rtri = roster_pid_to_tri.get(int(pid))
                    if rtri and rtri != cache.get(pid):
                        cache[pid] = rtri
                        return rtri
                except Exception:
                    pass
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
        # fallback: season-appropriate processed roster file
        try:
            roster_file = _pick_processed_roster_file(date_str)
            if roster_file is not None and roster_file.exists():
                df = pd.read_csv(roster_file)
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
                    # Logs are helpful when team is missing, but they can lag trades.
                    # Do not overwrite a roster-derived team with a stale latest-log team.
                    if not latest.empty and {'player_id','team'}.issubset(rost.columns):
                        tmp = rost.merge(latest, on='player_id', how='left')
                        team_missing = tmp['team'].fillna('').astype(str).str.len() == 0
                        logs_present = tmp['team_logs'].fillna('').astype(str).str.len() > 0
                        tmp['team'] = tmp['team_logs'].where(team_missing & logs_present, tmp['team']).fillna(tmp['team'])
                        rost = tmp.drop(columns=['team_logs'], errors='ignore')
                        # Deduplicate again if override introduced dups
                        try:
                            rost = rost.drop_duplicates(subset=['player_id','team'])
                        except Exception:
                            pass
    except Exception:
        pass

    # Authoritative correction: if the processed season roster says a player's current team differs
    # (common on trade days), prefer the roster team for the target date.
    try:
        roster_file = _pick_processed_roster_file(date_str)
        if (roster_file is not None) and roster_file.exists() and (rost is not None) and (not rost.empty) and ('player_id' in rost.columns):
            rdf = pd.read_csv(roster_file)
            c = {c.upper(): c for c in rdf.columns}
            if {'PLAYER_ID','TEAM_ABBREVIATION'}.issubset(c.keys()):
                tmp = rdf[[c['PLAYER_ID'], c['TEAM_ABBREVIATION']]].copy()
                tmp[c['PLAYER_ID']] = pd.to_numeric(tmp[c['PLAYER_ID']], errors='coerce')
                tmp[c['TEAM_ABBREVIATION']] = tmp[c['TEAM_ABBREVIATION']].astype(str).map(lambda x: (to_tricode(str(x)) or str(x).strip().upper()))
                tmp = tmp.dropna(subset=[c['PLAYER_ID']])
                tmp = tmp.drop_duplicates(subset=[c['PLAYER_ID']], keep='first')
                tmp.rename(columns={c['PLAYER_ID']: 'player_id', c['TEAM_ABBREVIATION']: 'team_roster'}, inplace=True)
                m = rost.merge(tmp, on='player_id', how='left')
                m['team_roster'] = m['team_roster'].fillna('').astype(str).str.upper().str.strip()
                m['team'] = m['team_roster'].where(m['team_roster'].astype(str).str.len() > 0, m['team']).fillna(m['team'])
                rost = m.drop(columns=['team_roster'], errors='ignore')
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

        # Fix ESPN/team-feed glitches: if the injury row's team doesn't match the player's
        # resolved roster team for the date, override the injury team to the roster team.
        # This keeps injuries effective even when the feed reports an incorrect team.
        try:
            if {'_name_key', 'team'}.issubset(set(inj_small.columns)) and (not out.empty):
                roster_team_by_key = (
                    out[['_name_key', 'team']]
                    .dropna()
                    .drop_duplicates(subset=['_name_key'], keep='first')
                    .set_index('_name_key')['team']
                    .astype(str)
                    .to_dict()
                )
                if roster_team_by_key:
                    def _clean_team(v: object) -> str:
                        s = str(v or '').strip().upper()
                        if s in {'NAN', 'NONE', 'NULL'}:
                            return ''
                        return s
                    inj_small = inj_small.copy()
                    inj_small['team'] = inj_small['team'].map(_clean_team)
                    inj_small['team'] = inj_small.apply(
                        lambda r: roster_team_by_key.get(str(r.get('_name_key') or ''), '') or str(r.get('team') or ''),
                        axis=1,
                    )
        except Exception:
            pass

        # Deterministic: if multiple injury rows exist for same player/team, keep the worst status.
        # This prevents duplicated league_status rows and unpredictable flips (e.g., OUT vs DAY-TO-DAY).
        def _sev(s: str) -> int:
            s = str(s or '').strip().upper()
            if s in {'OUT','INACTIVE','SUSPENDED','REST'}:
                return 50
            if s in {'DOUBTFUL'}:
                return 40
            if s in {'QUESTIONABLE'}:
                return 30
            if s in {'PROBABLE','DAY-TO-DAY'}:
                return 20
            if s in {'ACTIVE'}:
                return 10
            return 0
        try:
            if {'_name_key','team','status_norm'}.issubset(set(inj_small.columns)):
                inj_small['_sev'] = inj_small['status_norm'].map(_sev)
                inj_small['team'] = inj_small['team'].fillna('').astype(str)
                inj_small = inj_small.sort_values(['_name_key','team','_sev'], ascending=[True, True, False])
                inj_small = inj_small.drop_duplicates(subset=['_name_key','team'], keep='first').drop(columns=['_sev'], errors='ignore')
        except Exception:
            pass

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
                        # Keep worst status per name
                        try:
                            tmp['_sev'] = tmp['status_norm'].map(_sev)
                            tmp = tmp.sort_values(['_name_key','_sev'], ascending=[True, False])
                            inj_name_only = tmp[['_name_key', 'status_norm']].drop_duplicates(subset=['_name_key'], keep='first')
                        except Exception:
                            inj_name_only = tmp[['_name_key', 'status_norm']].drop_duplicates(subset=['_name_key'])
                else:
                    try:
                        tmp = inj_small.copy()
                        tmp['_sev'] = tmp['status_norm'].map(_sev)
                        tmp = tmp.sort_values(['_name_key','_sev'], ascending=[True, False])
                        inj_name_only = tmp[['_name_key', 'status_norm']].drop_duplicates(subset=['_name_key'], keep='first')
                    except Exception:
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

    # Final deterministic dedupe on player_id (keep worst injury status)
    try:
        if 'player_id' in out.columns and not out.empty:
            out['player_id'] = pd.to_numeric(out['player_id'], errors='coerce')
            out['_sev'] = out.get('injury_status', '').map(_sev)
            # Prefer on-slate rows (if any), then worst injury, then non-empty team
            out['_on'] = out.get('team_on_slate', False).fillna(False).astype(bool)
            out['_team_len'] = out.get('team', '').fillna('').astype(str).str.len()
            out = out.sort_values(['player_id','_on','_sev','_team_len'], ascending=[True, False, False, False])
            out = out.drop_duplicates(subset=['player_id'], keep='first').drop(columns=['_sev','_on','_team_len'], errors='ignore')
    except Exception:
        pass
    # Save
    out_path = paths.data_processed / f'league_status_{date_str}.csv'
    out.to_csv(out_path, index=False)
    return out
