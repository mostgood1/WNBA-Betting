from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List
from .config import paths
from .league import LEAGUE, season_label_from_date, season_year_from_date
from .teams import TEAM_TRICODES, to_tricode
import datetime as _dt
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
    # Prefer live slate sources (OddsAPI snapshots + NBA Scoreboard).
    # Only fall back to the season schedule when live sources are unavailable.
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
        # league_id is load-bearing: without it the stats API returns the NBA slate, and NBA ATL/GSW/IND/... were
        # flagged "on slate" (measured 2026-10-06).
        sb = scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, league_id=_WNBA_STATS_LEAGUE_ID, timeout=20)
        nd = sb.get_normalized_dict()
        ls = pd.DataFrame(nd.get('LineScore', []))
        if not ls.empty:
            c = {c.upper(): c for c in ls.columns}
            if 'TEAM_ABBREVIATION' in c:
                for _, r in ls.iterrows():
                    tri = _wnba_tricode(r[c['TEAM_ABBREVIATION']])
                    if tri:
                        sb_tris.add(tri)
    except Exception:
        sb_tris = set()

    if odds_tris or sb_tris:
        return odds_tris | sb_tris

    # Deterministic fallback: processed season schedule for the active league season.
    sched_tris: set[str] = set()
    try:
        season = _season_for_date(date_str)
        candidates = [
            paths.data_processed / f"schedule_{season}.csv",
            paths.data_processed / f"schedule_{season}.json",
        ]
        sched_path = next((p for p in candidates if p.exists()), None)
        if sched_path is not None:
            if sched_path.suffix.lower() == ".csv":
                df = pd.read_csv(sched_path)
            else:
                import json as _json
                raw = _json.load(open(sched_path, 'r', encoding='utf-8'))
                df = pd.DataFrame(raw if isinstance(raw, list) else [])
        else:
            df = pd.DataFrame()

        if df is not None and not df.empty:
            day_col = 'date_est' if 'date_est' in df.columns else ('date_utc' if 'date_utc' in df.columns else None)
            if day_col:
                day = df.copy()
                day[day_col] = pd.to_datetime(day[day_col], errors='coerce').dt.date
                day = day[day[day_col] == pd.to_datetime(date_str).date()].copy()
                for _, g in day.iterrows():
                    ht = str(g.get('home_tricode') or '').strip().upper()
                    at = str(g.get('away_tricode') or '').strip().upper()
                    if ht:
                        sched_tris.add(ht)
                    if at:
                        sched_tris.add(at)
    except Exception:
        sched_tris = set()

    if sched_tris:
        return sched_tris

    try:
        season = _season_for_date(date_str)
        candidates = [
            paths.data_processed / f"schedule_{season}.json",
            paths.data_processed / f"schedule_{season}.csv",
        ]
        sched_path = next((p for p in candidates if p.exists()), None)
        if sched_path is not None:
            if sched_path.suffix.lower() == ".csv":
                raw_df = pd.read_csv(sched_path)
                raw = raw_df.to_dict(orient='records') if raw_df is not None and not raw_df.empty else []
            else:
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

    return sched_tris


def _season_for_date(date_str: str) -> str:
    try:
        d = pd.to_datetime(date_str).date()
    except Exception:
        d = _dt.date.today()
    return str(season_label_from_date(d))


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


# The stats API's WNBA league id. Without it ScoreboardV2 defaults to "00", the NBA. (PR #9 adds the same value as
# LeagueConfig.stats_league_id; read it from there when present so the two cannot drift.)
_WNBA_STATS_LEAGUE_ID = getattr(LEAGUE, "stats_league_id", "10")
# The stats API spells two franchises differently from this package's tricodes.
_STATS_TRI_ALIASES = {"PHO": "PHX", "WAS": "WSH"}


def _wnba_tricode(value: object) -> str:
    """The WNBA tricode for a team string, or "" when it is not a WNBA team.

    Stricter than `to_tricode`, which upper-cases ANY 3-letter input (so NBA "BKN" or an exhibition "NIGER" row
    would pass through)."""
    tri = to_tricode(str(value or '').strip())
    tri = _STATS_TRI_ALIASES.get(tri, tri)
    return tri if tri in TEAM_TRICODES else ''


def _roster_from_player_logs(date_str: str) -> pd.DataFrame:
    """WNBA roster as of `date_str`: every player who logged a game this season on or before the date, on the team
    of their latest such game. Local and WNBA-native.

    Replaces `_fetch_league_rosters_via_nba`, which iterated nba_api's `static_teams.get_teams()` -- the NBA list --
    so with no processed rosters file every league_status_<date>.csv was the NBA roster (measured on the production
    data root 2026-10-06: 09-30..10-08 files ~600 rows, 30 NBA teams, no WNBA player).
    """
    empty = pd.DataFrame(columns=['player_id', 'player_name', 'team'])
    logs_p = paths.data_processed / 'player_logs.csv'
    if not logs_p.exists():
        return empty
    try:
        logs = pd.read_csv(logs_p, usecols=lambda col: str(col).upper() in {'GAME_DATE', 'PLAYER_NAME', 'PLAYER_ID', 'TEAM_ABBREVIATION'})
    except Exception:
        return empty
    c = {str(col).upper(): col for col in logs.columns}
    if not {'GAME_DATE', 'PLAYER_NAME', 'PLAYER_ID', 'TEAM_ABBREVIATION'}.issubset(c.keys()) or logs.empty:
        return empty
    cutoff = pd.to_datetime(date_str, errors='coerce')
    if pd.isna(cutoff):
        return empty
    season_start = pd.Timestamp(year=int(season_year_from_date(cutoff.date())), month=1, day=1)
    logs = logs.rename(columns={c['GAME_DATE']: 'game_date', c['PLAYER_NAME']: 'player_name', c['PLAYER_ID']: 'player_id', c['TEAM_ABBREVIATION']: 'team'})
    logs['game_date'] = pd.to_datetime(logs['game_date'], errors='coerce')
    logs['player_id'] = pd.to_numeric(logs['player_id'], errors='coerce')
    logs['team'] = logs['team'].map(_wnba_tricode)
    logs = logs[logs['game_date'].notna() & (logs['game_date'] >= season_start) & (logs['game_date'] <= cutoff)]
    logs = logs[logs['player_id'].notna() & (logs['team'].astype(str).str.len() > 0)]
    if logs.empty:
        return empty
    latest = logs.sort_values(['player_id', 'game_date']).groupby('player_id', as_index=False).tail(1)
    out = latest[['player_id', 'player_name', 'team']].copy()
    out['player_id'] = out['player_id'].astype(int)
    return out.reset_index(drop=True)


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
                    # The feed is a stack of DAILY SNAPSHOTS of the injury report and a returned player simply drops
                    # off the next one, so status comes from the LATEST SNAPSHOT on or before the date -- never a
                    # player's latest row, which kept returned players OUT (measured 2026-10-06: Jewell Loyd and
                    # Stephanie Talbot on the 10-05 snapshot, absent from 10-06).
                    counts = df['date'].value_counts().sort_index()
                    snapshot_day = counts.index[-1]
                    # A partial fetch would clear almost every exclusion: a latest snapshot under half the previous
                    # one's rows is not trusted.
                    if len(counts) >= 2 and counts.iloc[-1] < 0.5 * counts.iloc[-2]:
                        print(f"LEAGUE_STATUS_INJURY_SNAPSHOT_PARTIAL date={date_str} latest={snapshot_day} rows={int(counts.iloc[-1])} "
                              f"previous={counts.index[-2]} rows={int(counts.iloc[-2])} -- using the previous snapshot", flush=True)
                        snapshot_day = counts.index[-2]
                    out = df[df['date'] == snapshot_day].copy()
                    # A snapshot more than 3 days old counts only for its season-ending rows.
                    if (cutoff - snapshot_day).days > 3:
                        status_norm = out['status'].astype(str).str.upper().str.strip()
                        is_season = (
                            (status_norm.str.contains('SEASON', na=False) & status_norm.str.contains('OUT', na=False))
                            | status_norm.str.contains('INDEFINITE', na=False)
                            | status_norm.str.contains('SEASON-ENDING', na=False)
                        )
                        out = out[is_season].copy()
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
    # 1) Primary roster: season-appropriate processed roster file.
    # This is authoritative for WNBA and avoids cross-league contamination from CPI/NBA fallbacks.
    rost = pd.DataFrame()
    try:
        roster_file = _pick_processed_roster_file(date_str)
        if roster_file is not None and roster_file.exists():
            df = pd.read_csv(roster_file)
            c = {c.upper(): c for c in df.columns}
            if {'PLAYER','PLAYER_ID'}.issubset(c.keys()):
                if 'TEAM_ABBREVIATION' not in c:
                    df['TEAM_ABBREVIATION'] = None
                    c['TEAM_ABBREVIATION'] = 'TEAM_ABBREVIATION'
                rost = df[[c['PLAYER'], c['PLAYER_ID'], c['TEAM_ABBREVIATION']]].rename(columns={c['PLAYER']: 'player_name', c['PLAYER_ID']: 'player_id', c['TEAM_ABBREVIATION']: 'team'})
    except Exception:
        rost = pd.DataFrame()
    # 1b) No processed roster file: the season's WNBA player logs. NEVER nba_api's static team/player lists -- they
    # are the NBA's, and falling through to them wrote the whole NBA roster into every WNBA league_status file.
    if rost is None or rost.empty:
        rost = _roster_from_player_logs(date_str)
    if rost is None or rost.empty:
        print(f"LEAGUE_STATUS_NO_WNBA_ROSTER date={date_str}: no processed rosters file and no player_logs rows this season", flush=True)
        rost = pd.DataFrame(columns=['player_id', 'player_name', 'team'])
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
