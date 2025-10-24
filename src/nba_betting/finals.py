from __future__ import annotations

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Iterable

try:
    # Optional: nba_api may be unavailable in minimal environments
    from nba_api.stats.endpoints import scoreboardv2 as _scoreboardv2  # type: ignore
    from nba_api.stats.library import http as _nba_http  # type: ignore
except Exception:
    _scoreboardv2 = None
    _nba_http = None

from .config import paths


def _finals_from_stats(date_str: str) -> pd.DataFrame:
    """Fetch finals via nba_api ScoreboardV2 (happy path)."""
    try:
        if _scoreboardv2 is None:
            return pd.DataFrame()
        if _nba_http is not None:
            try:
                _nba_http.STATS_HEADERS.update({
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://www.nba.com',
                    'Referer': 'https://www.nba.com/stats/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
                    'Connection': 'keep-alive',
                })
            except Exception:
                pass
        sb = _scoreboardv2.ScoreboardV2(game_date=date_str, day_offset=0, timeout=10)
        nd = sb.get_normalized_dict()
        gh = pd.DataFrame(nd.get("GameHeader", []))
        ls = pd.DataFrame(nd.get("LineScore", []))
        if gh.empty or ls.empty:
            return pd.DataFrame()
        cgh = {c.upper(): c for c in gh.columns}
        cls = {c.upper(): c for c in ls.columns}
        team_rows: dict[int, dict[str, object]] = {}
        for _, r in ls.iterrows():
            try:
                tid = int(r[cls["TEAM_ID"]])
                tri = str(r[cls["TEAM_ABBREVIATION"]]).upper()
                pts = None
                if "PTS" in cls:
                    try:
                        pts = int(r[cls["PTS"]])
                    except Exception:
                        pts = None
                team_rows[tid] = {"tri": tri, "pts": pts}
            except Exception:
                continue
        out_rows: list[dict[str, object]] = []
        for _, g in gh.iterrows():
            try:
                hid = int(g[cgh["HOME_TEAM_ID"]]); vid = int(g[cgh["VISITOR_TEAM_ID"]])
                h = team_rows.get(hid, {}); v = team_rows.get(vid, {})
                htri = str(h.get("tri") or "").upper(); vtri = str(v.get("tri") or "").upper()
                hpts = h.get("pts"); vpts = v.get("pts")
                out_rows.append({"home_tri": htri, "away_tri": vtri, "home_pts": hpts, "visitor_pts": vpts})
            except Exception:
                continue
        return pd.DataFrame(out_rows)
    except Exception:
        return pd.DataFrame()


def _finals_from_cdn(date_str: str) -> pd.DataFrame:
    """Fetch finals via NBA public CDN (scoreboard.json)."""
    try:
        import requests as _rq  # type: ignore
    except Exception:
        return pd.DataFrame()
    try:
        ymd = date_str.replace('-', '')
        url = f"https://data.nba.com/data/10s/prod/v1/{ymd}/scoreboard.json"
        r = _rq.get(url, timeout=8)
        if r.status_code != 200:
            return pd.DataFrame()
        jd = r.json() or {}
        games = jd.get('games', []) if isinstance(jd, dict) else []
        rows: list[dict[str, object]] = []
        for g in games:
            try:
                htri = str((g.get('hTeam') or {}).get('triCode') or '').upper()
                vtri = str((g.get('vTeam') or {}).get('triCode') or '').upper()
                hs = (g.get('hTeam') or {}).get('score'); vs = (g.get('vTeam') or {}).get('score')
                hpts = int(hs) if (hs not in (None, '')) else None
                vpts = int(vs) if (vs not in (None, '')) else None
                rows.append({"home_tri": htri, "away_tri": vtri, "home_pts": hpts, "visitor_pts": vpts})
            except Exception:
                continue
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def _finals_from_espn(date_str: str) -> pd.DataFrame:
    """Fetch finals via ESPN scoreboard API."""
    try:
        import requests as _rq  # type: ignore
    except Exception:
        return pd.DataFrame()
    try:
        ymd = date_str.replace('-', '')
        url = f"https://site.web.api.espn.com/apis/v2/sports/basketball/nba/scoreboard?dates={ymd}"
        r = _rq.get(url, timeout=8)
        if r.status_code != 200:
            return pd.DataFrame()
        jd = r.json() or {}
        evs = jd.get('events', []) if isinstance(jd, dict) else []
        def espn_to_tri(abbr: str) -> str:
            s = str(abbr or '').upper()
            fix = { 'GS': 'GSW', 'NO': 'NOP', 'NY': 'NYK' }
            return fix.get(s, s)
        rows: list[dict[str, object]] = []
        for e in evs:
            try:
                comps = e.get('competitions', [])
                if not comps: continue
                c = comps[0]
                at = c.get('competitors', [])
                if len(at) < 2: continue
                home = next((t for t in at if str(t.get('homeAway'))=='home'), None)
                away = next((t for t in at if str(t.get('homeAway'))=='away'), None)
                if not home or not away: continue
                htri = espn_to_tri(((home.get('team') or {}).get('abbreviation')))
                vtri = espn_to_tri(((away.get('team') or {}).get('abbreviation')))
                try:
                    hpts = int(home.get('score')) if str(home.get('score') or '') != '' else None
                except Exception:
                    hpts = None
                try:
                    vpts = int(away.get('score')) if str(away.get('score') or '') != '' else None
                except Exception:
                    vpts = None
                rows.append({'home_tri': htri, 'away_tri': vtri, 'home_pts': hpts, 'visitor_pts': vpts})
            except Exception:
                continue
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def fetch_finals(date_str: str, include_adjacent: bool = True) -> pd.DataFrame:
    """Try multiple sources (stats -> CDN -> ESPN), with optional ±1 day fallback.

    Returns a DataFrame with columns [home_tri, away_tri, home_pts, visitor_pts] (may be empty).
    """
    # Primary order
    df = _finals_from_stats(date_str)
    if df is None or df.empty:
        df = _finals_from_cdn(date_str)
    if df is None or df.empty:
        df = _finals_from_espn(date_str)
    if include_adjacent and (df is None or df.empty):
        try:
            base = datetime.strptime(date_str, "%Y-%m-%d").date()
            parts: list[pd.DataFrame] = []
            for off in (-1, 1):
                d2 = (base + timedelta(days=off)).isoformat()
                d2_df = _finals_from_stats(d2)
                if d2_df is None or d2_df.empty:
                    d2_df = _finals_from_cdn(d2)
                if d2_df is None or d2_df.empty:
                    d2_df = _finals_from_espn(d2)
                if isinstance(d2_df, pd.DataFrame) and not d2_df.empty:
                    parts.append(d2_df)
            if parts:
                df = pd.concat(parts, ignore_index=True)
                keep = [c for c in ("home_tri","away_tri","home_pts","visitor_pts") if c in df.columns]
                if keep:
                    df = df[keep].drop_duplicates()
        except Exception:
            pass
    if not isinstance(df, pd.DataFrame):
        df = pd.DataFrame()
    # Normalize columns
    for col in ("home_tri","away_tri","home_pts","visitor_pts"):
        if col not in df.columns:
            df[col] = pd.NA
    return df[["home_tri","away_tri","home_pts","visitor_pts"]].copy()


def write_finals_csv(date_str: str, df: Optional[pd.DataFrame] = None) -> tuple[str, int]:
    """Write finals_<date>.csv to processed folder. If df is None, fetch first.

    Returns (path, row_count).
    """
    if df is None:
        df = fetch_finals(date_str)
    out = paths.data_processed / f"finals_{date_str}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    keep = ["home_tri","away_tri","home_pts","visitor_pts"]
    if df is None or df.empty:
        # Still write header (so downstream knows we attempted), but 0 rows
        pd.DataFrame(columns=["date"] + keep).to_csv(out, index=False)
        return str(out), 0
    df2 = df[keep].copy()
    df2.insert(0, "date", date_str)
    df2.to_csv(out, index=False)
    return str(out), int(len(df2))
